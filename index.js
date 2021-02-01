/* 
sfreportparser
Listens for changes at config.DOWNLOAD_PATH and parses csv into js objects, then sends to mongo
jwalker
*/
const config = require('./config');
const { MongoClient } = require('mongodb');
const puppeteer = require('puppeteer');
const dateFormat = require("dateformat");
const fs = require('fs');
const fsPromises = fs.promises;
const chokidar = require('chokidar');
const parse = require("csv-parse/lib/sync");
const winston = require('winston');
const { createLogger, format, transports } = require('winston');
const { combine, timestamp, label, printf } = format;
var nodemailer = require('nodemailer');

// TODO integrate with config.js / env variables
const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: config.EMAIL_FROM,
    pass: config.EMAIL_FROM_PASS
  }
});

const myFormat = printf(({ level, message, label, timestamp }) => {
  return `${timestamp} [${label}] ${level}: ${message}`;
});

const MONGO_ASSIGNED_COLLECTION = 'assigned';
const MONGO_UNASSIGNED_COLLECTION = 'unassigned';

var filesAdded = 0;
var lastUpdateTime;

const logger = winston.createLogger({
  level: config.LOG_LEVEL,
  format: combine(
    label({ label: 'sfreportparser' }),
    timestamp(),
    myFormat
  ),
  defaultMeta: { service: 'user-service' },
  transports: [
    // - Write all logs with level `error` and below to `error.log`
    // - Write all logs with level `info` and below to `combined.log`
    new winston.transports.File({ filename: 'error.log', level: 'error' }),
    new winston.transports.File({ filename: 'combined.log' }),
  ],
  exitOnError: false,
});
if (process.env.NODE_ENV !== 'production') {
  logger.add(new winston.transports.Console({
    format: winston.format.simple(),
  }));
}

var processingCSV = false;

var watcher = chokidar.watch(config.DOWNLOAD_PATH, {ignored: /\.crdownload/g, persistent: true});
watcher
  .on('add', async function(path) {
    /* On service startup watcher reads all files in the directory. The following initialization
       code skips to the newest file in the directory, then begins parsing. */
    logger.debug('File ' + path + ' was detected in directory ' + config.DOWNLOAD_PATH + '.');
    
    const numFiles = (await listDir(config.DOWNLOAD_PATH)).length;
    logger.debug(numFiles + ' files present in directory.');
    filesAdded++;
    logger.debug(filesAdded + ' total files added.'); 

    if (filesAdded < numFiles) { 
      return;
    } else {
      /* Exit function immediately if the previous csv file is still being processed. Occurs if many 
       ownership changes are queued */
      if (processingCSV) {
        logger.debug("Skipping csv processing as previous file has not been parsed.");
        return;
      }
      processingCSV = true;

      /* Initialization complete. Time to parse... */
      logger.info("Reading file " + path + ".");
      var latestReport = await readFile(path);

      var data = await parse(latestReport, {
        columns: true,
        skip_empty_lines: true
      })

      logger.debug("Loaded the following data from file: " + JSON.stringify(data));

      var now = new Date();
      var timestamp = dateFormat(now, "shortDate") + " " + dateFormat(now, "mediumTime");

      var sfCases = [];
      for (const row of data) {
        values = Object.values(row);
        url = `https://microfocus.lightning.force.com/lightning/r/Case/${values[1]}/view`
        urlPrintView = `https://microfocus.my.salesforce.com/${values[1]}/p`;
        sfCases.unshift({
          "logTime"          : timestamp,
          "_id"              : values[0], // case number - primary identifier in mongo
          "caseID"           : values[1],
          "caseOwner"        : values[2],
          "caseOwnerAlias"   : values[3],
          "caseDate"         : values[4],
          "subject"          : values[5],
          "type"             : values[6],
          "caseOrigin"       : values[7],
          "createdBy"        : values[8],
          "dateTimeOpened"   : values[9],
          "ageHours"         : values[10],
          "status"           : values[11],
          "milestoneStatus"  : values[12],
          "product"          : values[13],
          "supportProduct"   : values[14],
          "productGroup"     : values[15],
          "severity"         : values[16],
          "rdIncident"       : values[17],
          "rdChangeRequest"  : values[18],
          "contactName"      : values[19],
          "contactEmail"     : values[20],
          "contactPhone"     : values[21],
          "contactRegion"    : values[22],
          "country"          : values[23],
          "accountName"      : values[24],
          "businessHours"    : values[25],
          "description"      : values[26],
          "caseComments"     : values[27],
          "url"              : url,
          "urlPrintView"     : urlPrintView,
        });
      }
      logger.verbose("Created case objects: " + sfCases);

      logger.info("Sending cases to mongo.");
      await uploadToMongo(sfCases);

      lastUpdateTime = new Date();

      processingCSV = false;
    }    
  })
  .on('change', function(path) { logger.debug('File ' + path + ' has been changed.'); })
  .on('unlink', function(path) { logger.debug('File ' + path + ' has been removed.'); })
  .on('error', function(error) { logger.error('Error happened: ' + error); })


/* Accepts list of case objects to upload to mongo */
async function uploadToMongo(sfCases) {
  /* Connect to MongoDB */
  const client = new MongoClient(config.MONGO_URI, {useNewUrlParser: true, useUnifiedTopology: true});

  try {
    // Connect the client to the server
    logger.info("Connecting to mongo db \"" + config.MONGO_DB + "\" at url: " + config.MONGO_URI);
    await client.connect();
    // Establish and verify connection
    const database = client.db(config.MONGO_DB);
    await database.command({ ping: 1 });
    logger.debug("Connected successfully to mongo server.");

    const unassignedCollection = database.collection(MONGO_UNASSIGNED_COLLECTION);
    const assignedCollection = database.collection(MONGO_ASSIGNED_COLLECTION);

    /* Insert new records */
    logger.debug("Adding new cases...");
    for (const value of sfCases) {
      /* Query for the document */
      const query = { _id: value._id };
      const options = { upsert: true, }; // updates if exists, inserts if not

      const result = await unassignedCollection.replaceOne(query, value, options);

      if (result.modifiedCount === 0 && result.upsertedCount === 0) {
        logger.debug("No changes made to the collection.");
      } else {
        if (result.matchedCount === 1) {
          logger.debug("Matched " + result.matchedCount + " documents.");
        }
        if (result.modifiedCount === 1) {
          logger.debug("Updated one document.");
        }
        if (result.upsertedCount === 1) {
          logger.debug(
            "Inserted one new document with an _id of " + result.upsertedId._id
          );
        }
      }
    }

    /* Move reassigned records to 'moved' collection */
    const moveQueue = [];
    const previousCases = unassignedCollection.find();
    if ((await previousCases.count()) === 0) {
      logger.debug("No previous cases found in database.");
    }

    logger.debug("Updating existing cases in mongo that are no longer in the queue...");
    await previousCases.forEach(function(i) {
      let exists = false;

      for (const value of sfCases) {
        if (value._id === i._id) {
          logger.debug("Case " + i._id + " is still in the queue. Skipping...");
          exists = true;
        }
      }

      if (!exists) {
        logger.debug("Found a case that is no longer in the queue.");
        moveQueue.unshift(i);
      }
    });

    if (moveQueue.length > 0) {
      logger.debug("moveQueue contains objects.");

      /* Initiate the Puppeteer browser */
      const browser = await puppeteer.launch({
        // headless: false,
        // slowMo: 250,
        // defaultViewport: null,
        args: ['--no-sandbox'],
      });

      const context = browser.defaultBrowserContext();
      context.overridePermissions(url, ["notifications"]);
    
      logger.debug("Browser loaded.");

      const page = await salesforceLogin(browser, config.SF_LOGIN_URL);

      for (var _case of moveQueue) {
        if (!_case) {
          logger.error("null entry found in moveQueue. Skipping...");
          break;
        }
        logger.verbose("_case: " + JSON.stringify(_case));
        logger.debug("Looping through cases in moveQueue - querying for new owner for case " + _case._id + ".");
        /* spaghetti to account for older cases that don't have the newer urlPrintView attribute */
        if (!_case.urlPrintView) {
          _case.urlPrintView = urlPrintView = `https://microfocus.my.salesforce.com/${_case.caseID}/p`;
          logger.debug("Added urlPrintView: " + _case.urlPrintView);
        }
        let newFields = await updateFields(page, _case.urlPrintView);
        logger.debug("newFields: " + JSON.stringify(newFields));
        _case.caseOwner = newFields[0];
        _case.caseOwnerAlias = newFields[0];
        _case.product = newFields[1];
        _case.subject = newFields[2];
      }

      await browser.close();
      logger.debug("Finished querying new owners. Browser closed.");
    }

    await deleteDocuments(moveQueue, unassignedCollection);
    await insertDocuments(moveQueue, assignedCollection);

    logger.info("Mongo updated successfully.");
    
  } catch (e) {
    logger.error("Error caught: " + e);
  } finally {
    // Ensures that the client will close when you finish/error
    await client.close();
    logger.debug("Mongo connection closed.");
  }
}


/* "Query" Salesforce for new owner, subject, and product. 
   Requires authenticated page and _case.urlPrintView as argument. */
async function updateFields(page, url) {
  logger.debug("Updating fields for [" + url + "].");

  await page.goto(url, { waitUntil: 'networkidle2' });
  logger.debug("page.goto: " + url);

  // Evaluate 
  let newFields = await page.evaluate(() => {
    let owner = document.querySelector(
      "#mainTable > div.pbBody > div:nth-child(15) > table > tbody > tr:nth-child(4) > td.dataCol.last.col02"
    ).innerText;


    let product = document.querySelector(
      "#mainTable > div.pbBody > div:nth-child(7) > table > tbody > tr:nth-child(1) > td:nth-child(4)"
    ).innerText;

    let subject = document.querySelector(
      "#mainTable > div.pbBody > div:nth-child(3) > table > tbody > tr:nth-child(5) > td.dataCol.col02"
    ).innerText;

    return { owner, product, subject }
    
  });

  return newFields;
}


/* Logs into salesforce and returns the page */
async function salesforceLogin(browser, url) {
  const page = await browser.newPage();
  logger.debug("Blank page loaded.");

  /* Go to the page and wait for it to load */
  await page.goto(url, { waitUntil: 'networkidle2' });
  logger.debug("Salesforce initial auth page loaded.");

  /* Click on the SSO button */
  await Promise.all([
    page.click('#idp_section_buttons > button > span'),
    waitForNetworkIdle(page, 2000, 0),
    logger.debug("Navigating to SSO page."),
  ]);

  /* Enter username/password */
  await Promise.all([
    await page.type('#username', config.USER_LOGIN),
    await page.type('#password', config.PASS),
    await page.keyboard.press('Enter'),
    logger.info("Logged in to Salesforce. Please wait..."),
    await sleep(25000),
    waitForNetworkIdle(page, 1000, 0),
    logger.debug("Salesforce case page loaded."),
  ]);

  return page;
}


/* Sends an error if no new files have been detected in at least 2 minutes */
async function checkLastUpdateTime() {
  let finished = false;
  do {
    const currentTime = new Date();

    let diff = currentTime - lastUpdateTime;

    if (diff > 120000) { // two minutes
      let errorMessage = "Error: it has been " + Math.floor(diff / 1000 / 60) + " minutes since receiving any updates.\nYou may need to check on the sfexporter.";
      logger.error(errorMessage);

      const mailOptions = {
        from: config.EMAIL_FROM,
        to: config.EMAIL_TO,
        subject: 'Alert - [sfreportparser] error',
        text: errorMessage
      };

      transporter.sendMail(mailOptions, function(error, info){
        if (error) {
          console.log(error);
        } else {
          console.log('Email sent: ' + info.response);
        }
      });
      
    } else {
      logger.debug("Last update was received " + Math.floor(diff / 1000) + " seconds ago.");
    }
    
    await sleep(60000);
  } while (!finished);
}
checkLastUpdateTime();

/* Deletes objects from a collection. Takes an array of objects and a collection */
async function deleteDocuments(docs, collection) {
  for (const doc of docs) {
    const result = await collection.deleteOne({ _id: doc._id });
    logger.debug(
      `${result.deletedCount} document was deleted from ${collection.collectionName} collection with the _id: ${doc._id}`,
    );
  }
}


/* Inserts documentes into a collection. Takes an array of objects and a collection */
async function insertDocuments(docs, collection) {
  for (const doc of docs) {
    try {
      const result = await collection.insertOne(doc);
      logger.debug(
        `${result.insertedCount} document was inserted into ${collection.collectionName} collection with the _id: ${result.insertedId}`,
      );
    } catch (e) {
      logger.error("Error caught in insertDocuments: " + e);
    }
  }
}


/* Lists all files in specified directory */
async function listDir(path) {
  try {
    return fsPromises.readdir(path);
  } catch (err) {
    logger.error('Error occured while reading directory!', err);
  }
}


/* Reads file */
async function readFile(path) {
  try {
    return fs.readFileSync(path);
  } catch (err) {
    logger.error("Error caught in readFile: " + err);
  }
}

/* They promised me this would not be needed... */
function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
} 


/* Use if 500ms timeout of 'networkidleX' is insufficient */
function waitForNetworkIdle(page, timeout, maxInflightRequests = 0) {
  page.on('request', onRequestStarted);
  page.on('requestfinished', onRequestFinished);
  page.on('requestfailed', onRequestFinished);

  let inflight = 0;
  let fulfill;
  let promise = new Promise(x => fulfill = x);
  let timeoutId = setTimeout(onTimeoutDone, timeout);
  return promise;

  function onTimeoutDone() {
    page.removeListener('request', onRequestStarted);
    page.removeListener('requestfinished', onRequestFinished);
    page.removeListener('requestfailed', onRequestFinished);
    fulfill();
  }

  function onRequestStarted() {
    ++inflight;
    if (inflight > maxInflightRequests)
      clearTimeout(timeoutId);
  }
  
  function onRequestFinished() {
    if (inflight === 0)
      return;
    --inflight;
    if (inflight === maxInflightRequests)
      timeoutId = setTimeout(onTimeoutDone, timeout);
  }
}


/* They promised me this would not be needed... */
function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
} 