/* 
sfreportparser
Listens for changes at config.DOWNLOAD_PATH and parses csv into js objects, then sends to mongo
jwalker
*/
const config = require('./config');
const { MongoClient } = require('mongodb');
const puppeteer = require('puppeteer');
const fs = require('fs');
const fsPromises = fs.promises;
const chokidar = require('chokidar');
const parse = require('csv-parse/lib/sync');
const jwalkerLogger = require('jwalker-logger');
const nodemailer = require('nodemailer');
const sf = require('./jwalker-sf');
var emitter = require('events').EventEmitter;
const { ucs2 } = require('punycode');

// TODO integrate with config.js / env variables
const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: config.EMAIL_FROM,
    pass: config.EMAIL_FROM_PASS
  }
});

const MONGO_ALL_OPEN_COLLECTION = 'open';
const MONGO_UNASSIGNED_COLLECTION = 'unassigned';

var filesAdded = 0;
var lastUpdateTime;

const logger = jwalkerLogger.newLogger();

var processingCSV = false;

var casesUpdatedEmitter = new emitter();

var initialized = false;

casesUpdatedEmitter
  .on('openUpdated', async function() {
    logger.debug("Received 'openUpdated' event from casesUpdatedEmitter.");
    await updateUnassigned();
  })
  .on('unassignedUpdated', async function(mfiSupportCases) {
    logger.debug("Received 'unassignedUpdated' event from casesUpdatedEmitter.");
    await updateAssigned(mfiSupportCases);
  })
  .on('csvParsed', async function(filename) {
    logger.debug("Received 'csvParsed' event from casesUpdatedEmitter.");
    await deleteFile(filename);
  });

var watcher = chokidar.watch(config.DOWNLOAD_PATH, 
  {
    ignored: /\.crdownload$/g,
    ignoreInitial: /\.csv$/g,
    persistent: true,
    depth: 0
  });

watcher
  .on('ready', () => {
    logger.info('Initial scan complete. Ready for changes.');
    initialized = true;
  })
  .on('change', function(path) { logger.debug('File ' + path + ' has been changed.');   })
  .on('unlink', function(path) { logger.debug('File ' + path + ' has been removed.');   })
  .on('error' , function(e)    { logger.error(e.toLocaleString());                      })
  .on('add'   , async function(path) {
    /* Exit function immediately if not initialized or the previous csv file 
       is still being processed. */
    if (!initialized || processingCSV) return;

    processingCSV = true;

    /* Initialization complete. Time to parse... */
    logger.info("Reading file " + path + ".");
    var latestReport = await readFile(path);

    var data = await parse(latestReport, {
      columns: true,
      skip_empty_lines: true
    });

    // logger.silly("Loaded the following data from file: " + JSON.stringify(data));

    var timestamp = new Date()
      .toLocaleString('en-US', { timeZone: 'America/Denver'})
      .replace(',', '');
    logger.debug("logTime timestamp: " + timestamp);

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
        "FTSAccountName"   : values[28],
        "FTSPassword"      : values[29],
        "url"              : url,
        "urlPrintView"     : urlPrintView,
      });
    }
    // logger.silly("Created case objects: " + sfCases);

    logger.info("Sending cases to mongo.");
    await uploadToMongo(sfCases);

    lastUpdateTime = new Date();

    processingCSV = false;
    casesUpdatedEmitter.emit('csvParsed', path);
  });


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

    database.createCollection(MONGO_ALL_OPEN_COLLECTION, function (e) {
      if (e) logger.error(e.toLocaleString());
    });

    const allOpenCollection = database.collection(MONGO_ALL_OPEN_COLLECTION);

    await upsertToCollection(allOpenCollection, sfCases);

    casesUpdatedEmitter.emit('openUpdated');

    logger.info("Mongo updated successfully.");

  } catch (e) {
    logger.error(e.toLocaleString());
  } finally {
    // Ensures that the client will close when you finish/error
    await client.close();
    logger.debug("Mongo connection closed.");
  }
}


/* Query for all open cases with owner 'MFI Support' and add to unassigned collection */
async function updateUnassigned() {
  logger.debug("Updating unassigned collection.");

  const client = new MongoClient(config.MONGO_URI, {useNewUrlParser: true, useUnifiedTopology: true});

  try {
    // Connect the client to the server
    logger.info("Connecting to mongo db \"" + config.MONGO_DB + "\" at url: " + config.MONGO_URI);
    await client.connect();
    // Establish and verify connection
    const database = client.db(config.MONGO_DB);
    await database.command({ ping: 1 });
    logger.debug("Connected successfully to mongo server.");

    database.createCollection(MONGO_UNASSIGNED_COLLECTION, function (e) {
      if (e) logger.error(e.toLocaleString());
    });

    const allOpenCollection = database.collection(MONGO_ALL_OPEN_COLLECTION);
    const unassignedCollection = database.collection(MONGO_UNASSIGNED_COLLECTION);

    const query = { caseOwner: "MFI Support" };

    const options = { sort: { dateTimeOpened: 1 } };

    const mfiSupportCases = allOpenCollection.find(query, options);

    logger.debug(`Found ${await mfiSupportCases.count()} cases.`);

    await mfiSupportCases.forEach(function(i) {
      logger.silly(`mfiSupportCases: ${i._id}`);
    });

    const mfiSupportCasesArray = await mfiSupportCases.toArray();
    await upsertToCollection(unassignedCollection, mfiSupportCasesArray);

    casesUpdatedEmitter.emit('unassignedUpdated', mfiSupportCasesArray);

  } catch (e) {
    logger.error(e.toLocaleString());
  } finally {
    client.close();
    logger.debug("Mongo connection closed.");
  }
}


/* Compare new MFI Support cases against previous records and update fields for newly-assigned cases */
async function updateAssigned(mfiSupportCases) {
  logger.debug("Updating assigned cases.");
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

    const allOpenCollection = database.collection(MONGO_ALL_OPEN_COLLECTION);
    const unassignedCollection = database.collection(MONGO_UNASSIGNED_COLLECTION);

     /* Move reassigned records to 'moved' collection */
    const moveQueue = [];
    const previousCases = unassignedCollection.find();

    logger.debug("Updating existing cases in mongo that are no longer in the queue...");
    await previousCases.forEach(function(i) {
      let exists = false;

      for (const value of mfiSupportCases) {
        if (value._id === i._id) {
          logger.silly("Case " + i._id + " is still in the queue. Skipping...");
          exists = true;
        }
      }

      if (!exists) {
        logger.debug("Found a case that is no longer in the queue.");
        moveQueue.unshift(i);
      }
    });

    if (moveQueue.length > 0) {
      logger.debug(`Detected ${moveQueue.length} newly-assigned cases. Preparing to update documents.`);

      await updateFields(moveQueue);
      await deleteDocuments(moveQueue, unassignedCollection);
      await upsertToCollection(allOpenCollection, moveQueue);

      logger.info("Mongo updated successfully.");
    } else {
      logger.debug("No cases need to be updated at this time.");
    }
  } catch (e) {
    logger.error(e.toLocaleString());
  } finally {
    client.close();
    logger.debug("Mongo connection closed.");
  }
}

/* "Query" Salesforce for new owner, subject, and product. 
   Requires authenticated page and _case.urlPrintView as argument. */
async function updateFields(moveQueue) {
  logger.verbose("In updateFields() function.");
  try {
    /* Initiate the Puppeteer browser */
    var browser = await puppeteer.launch({
      // headless: false,
      // slowMo: 250,
      // defaultViewport: null,
      args: ['--no-sandbox'],
    });

    const context = browser.defaultBrowserContext();
    context.overridePermissions(url, ["notifications"]);

    logger.debug("Browser loaded.");

    const page = await sf.login(
      browser, 
      config.SF_LOGIN_URL, 
      config.USER_LOGIN, 
      config.PASS, 
      config.LOGIN_TIMEOUT, 
      logger
    );

    var count = 1;
    for (var _case of moveQueue) {
      logger.debug(`${count++} / ${moveQueue.length}`);
      if (!_case) {
        logger.error("null entry found in moveQueue. Skipping...");
        break;
      }

      logger.silly("_case: " + (JSON.stringify(_case))._id);
      logger.debug("Looping through cases in moveQueue - querying for new owner for case " + _case._id + ".");

      /* spaghetti to account for older cases that don't have the newer urlPrintView attribute */
      if (!_case.urlPrintView) {
        _case.urlPrintView = urlPrintView = `https://microfocus.my.salesforce.com/${_case.caseID}/p`;
        logger.debug("Added urlPrintView: " + _case.urlPrintView);
      }

      const newFields = await evaluatePage(page, _case.urlPrintView);

      logger.debug("newFields: " + JSON.stringify(newFields));

      if (!newFields.owner) { throw new Error("newFields empty. Skipping mongo update."); }

      _case.caseOwner      = newFields.owner;
      _case.caseOwnerAlias = newFields.owner;
      _case.product        = newFields.product;
      _case.subject        = newFields.subject;
    }
  } catch (e) {
    logger.error("Throwing error from updateFields().");
    throw (e);
  } finally {
    await browser.close();
    logger.debug("Browser closed.");
  }
}


/* Specific queries to get and return updated fields */
async function evaluatePage(page, url) {
  logger.debug("Updating fields for [" + url + "].");

  logger.debug("page.goto: " + url);
  await page.goto(url, { waitUntil: 'networkidle2' });
  logger.debug("calling sf.waitForNetworkIdle(2000)...");
  sf.waitForNetworkIdle(page, 5000, 0);
  logger.debug("done");

  // Evaluate 
  return await page.evaluate(() => {
    const owner = document.querySelector(
      "#mainTable > div.pbBody > div:nth-child(15) > table > tbody > tr:nth-child(4) > td.dataCol.last.col02"
    ).innerText;

    const product = document.querySelector(
      "#mainTable > div.pbBody > div:nth-child(7) > table > tbody > tr:nth-child(1) > td:nth-child(4)"
    ).innerText;

    const subject = document.querySelector(
      "#mainTable > div.pbBody > div:nth-child(3) > table > tbody > tr:nth-child(5) > td.dataCol.col02"
    ).innerText;

    return { owner, product, subject }
  });
}


async function upsertToCollection(collection, docs) {
  /* Insert new records */
  logger.debug(`Upserting documents to ${collection.collectionName}.`);

  const bulkOps = [];

  for (const doc of docs) {
    /* Query for the document */
    const filter = { _id: doc._id };
    const options = { upsert: true, }; // updates if exists, inserts if not

    bulkOps.push(
      { replaceOne :
        {
          "filter"      : filter,
          "replacement" : doc,
          "upsert"      : true
        }
      }
    );
  }

  const result = await collection.bulkWrite(bulkOps);

  logger.debug(`Matched ${result.nMatched} documents.`);
  logger.debug(`Updated ${result.nModified} documents.`);
  logger.debug(`Upserted ${result.nUpserted} documents.`);
}


/* Sends an error if no new files have been detected in at least 2 minutes */
async function checkLastUpdateTime() {
  let finished = false;
  do {
    const currentTime = new Date();

    let diff = currentTime - lastUpdateTime;

    if (diff > 300000) { // five minutes
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

      process.exit(5); // exit to restart docker container
      
    } else {
      logger.debug("Last update was received " + Math.floor(diff / 1000) + " seconds ago.");
    }
    
    await sleep(60000);
  } while (!finished);
}
checkLastUpdateTime();

/* Deletes objects from a collection. Takes an array of objects and a collection */
async function deleteDocuments(docs, collection) {
  logger.debug("Deleting documents...");
  for (const doc of docs) {
    const result = await collection.deleteOne({ _id: doc._id });
    logger.debug(
      `${result.deletedCount} document was deleted from ${collection.collectionName} collection with the _id: ${doc._id}`,
    );
  }
}


async function readFile(path) {
  try {
    return fs.readFileSync(path);
  } catch (e) {
    logger.error("Error caught in readFile: " + e.toLocaleString());
  }t
}


async function deleteFile(path) {
  try {
    logger.debug(`Deleting file ${path}`);
    fs.unlinkSync(path);
  } catch (e) {
    logger.error("Error caught in deleteFile(): " + e.toLocaleString());
  }
}


async function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}