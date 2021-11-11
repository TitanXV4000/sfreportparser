/* 
sfreportparser
Listens for changes at config.DOWNLOAD_PATH and parses csv into js objects, then sends to mongo
jwalker
*/
const config = require('./config');
const { MongoClient } = require('mongodb');
const puppeteer = require('puppeteer');
const fs = require('fs');
const chokidar = require('chokidar');
const parse = require('csv-parse/lib/sync');
const jwalkerLogger = require('tsanford-logger');
const nodemailer = require('nodemailer');
const sf = require('./jwalker-sf');
const emitter = require('events').EventEmitter;
const path = require('path');

// TODO integrate with config.js / env variables
const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: config.EMAIL_FROM,
    pass: config.EMAIL_FROM_PASS
  }
});

const MONGO_OPEN_COLLECTION = 'open';
const MONGO_CLOSED_COLLECTION = 'closed';
const MONGO_UNASSIGNED_COLLECTION = 'unassigned';

var lastUpdateTime;

const logger = jwalkerLogger.newLogger();
logger.info("Starting up.");

var processingCSV = false;

var casesUpdatedEmitter = new emitter();

var initialized = false;


casesUpdatedEmitter
  .on('open updated', async function(openCases) {
    logger.debug("Received 'open updated' event from casesUpdatedEmitter.");
    await cleanupOpenCases(openCases);
    // await updateUnassigned();
  })
  .on('closed updated', async function(closedCases) {
    logger.debug("Received 'closed updated' event from casesUpdatedEmitter.");
    await cleanupClosedCases(closedCases);
  })
  .on('unassigned updated', async function(mfiSupportCases) {
    logger.debug("Received 'unassigned updated' event from casesUpdatedEmitter.");
    // await updateAssigned(mfiSupportCases);
  })
  .on('csv parsed', async function(filename) {
    logger.debug("Received 'csv parsed' event from casesUpdatedEmitter.");
    processingCSV = false;
    await deleteFile(filename);
  })
  .on('cases parsed', async function(collection, cases) {
    logger.debug("Received 'cases parsed' event from casesUpdatedEmitter.");
    await uploadToMongo(collection, cases);
  })


var watcher = chokidar.watch(config.DOWNLOAD_PATH, 
  {
    ignored: /\.crdownload$/g,
    ignoreInitial: /\.csv$/g,
    persistent: true,
    depth: 0
  });
watcher
  .on('ready', () => {
    logger.info('Initial scan complete. Ready to process new reports.');
    initialized = true;
  })
  .on('change', function(filePath) { logger.debug('File ' + filePath + ' has been changed.'); })
  .on('unlink', function(filePath) { logger.debug('File ' + filePath + ' has been removed.'); })
  .on('error', function(e) { logger.error(e.toLocaleString()); })
  .on('add', async function(filePath) {
    /* Exit function immediately if not initialized or the previous csv file 
       is still being processed. */
    if (!initialized/* || processingCSV*/) return;

    /* Initialization complete. Time to parse... */
    processingCSV = true;

    /* Get the report tag from the beginnning of the basename to determine collection */
    const collectionName = ((path.basename(filePath)).split('_'))[0];
    logger.debug(`collectionName from reportTag: ${collectionName}`);
    
    logger.info(`Detected new file \'${filePath}\'. Begin parsing...`);
    var latestReport = await readFile(filePath);

    var data = await parse(latestReport, {
      columns: true,
      skip_empty_lines: true
    });

    logger.silly("Loaded the following data from file: " + JSON.stringify(data));

    var timestamp = new Date()
    //  .toLocaleString('en-US', { timeZone: 'America/Denver'})
    //  .replace(',', '');
    logger.debug("logTime timestamp: " + timestamp.toLocaleString('en-US', { timeZone: config.TIMEZONE}).replace(',', ''));
    //  .replace(',', '');
;

    var cases = [];
    for (const row of data) {
      values = Object.values(row);
      url = `https://microfocus.lightning.force.com/lightning/r/Case/${values[1]}/view`
      urlPrintView = `https://microfocus.my.salesforce.com/${values[1]}/p`;
      cases.unshift({
        "logTime"            : timestamp,
        "_id"                : values[0], // case number - primary identifier in mongo (indexed automatically)
        "caseID"             : values[1],
        "caseOwner"          : values[2],
        "caseOwnerAlias"     : values[3],
        "caseDate"           : values[4],
        "subject"            : values[5],
        "type"               : values[6],
        "caseOrigin"         : values[7],
        "createdBy"          : values[8],
        "dateTimeOpened"     : values[9],
        "ageHours"           : values[10],
        "status"             : values[11],
        "milestoneStatus"    : values[12],
        "product"            : values[13],
        "supportProduct"     : values[14],
        "productGroup"       : values[15],
        "severity"           : values[16],
        "rdIncident"         : values[17],
        "rdChangeRequest"    : values[18],
        "contactName"        : values[19],
        "contactEmail"       : values[20],
        "contactPhone"       : values[21],
        "contactRegion"      : values[22],
        "country"            : values[23],
        "accountName"        : values[24],
        "businessHours"      : values[25],
        "description"        : values[26],
        "caseComments"       : values[27],
        "FTSAccountName"     : values[28],
        "FTSPassword"        : values[29],
        "dateTimeClosed"     : values[30],
        "closureSummary"     : values[31],
        "closeCode"          : values[32],
        "caseLastModifiedBy" : values[33],
        "url"                : url,
        "urlPrintView"       : urlPrintView,
      });
    }
    logger.silly("Created case objects: " + cases);

    casesUpdatedEmitter.emit(`cases parsed`, collectionName, cases);

    lastUpdateTime = new Date();

    casesUpdatedEmitter.emit('csv parsed', filePath);
  });


/* Accepts list of case objects to upload to mongo */
async function uploadToMongo(collectionName, cases) {
  logger.info(`Sending cases to mongo collection \'${collectionName}\'.`);
  /* Connect to MongoDB */
  const client = new MongoClient(config.MONGO_URI, {useNewUrlParser: true, useUnifiedTopology: true});

  try {
    // Connect the client to the server
    logger.debug("Connecting to mongo db \"" + config.MONGO_DB + "\" at url: " + config.MONGO_URI);
    await client.connect();
    // Establish and verify connection
    const database = client.db(config.MONGO_DB);
    await database.command({ ping: 1 });
    logger.debug("Connected successfully to mongo server.");

    database.createCollection(collectionName, function (e) {
      if (e) logger.debug(e.toLocaleString());
    });

    const collection = database.collection(collectionName);

    await upsertToCollection(collection, cases);

    casesUpdatedEmitter.emit(`${collectionName} updated`, cases);

    logger.debug("Mongo updated successfully.");

  } catch (e) {
    logger.error(e.toLocaleString());
  } finally {
    // Ensures that the client will close when you finish/error
    await client.close();
    logger.debug("Mongo connection closed.");
  }
}


/* Uses bulkWrite to quickly upsert documents to the specified collection 
   Requires mongodb collection and array of documents */
async function upsertToCollection(collection, docs) {
  /* Insert new records */
  logger.debug(`Upserting documents to ${collection.collectionName}.`);

  const bulkOps = [];

  for (const doc of docs) {
    /* Query for the document */
    const filter = { _id: doc._id };

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

  //logger.info(`Matched ${result.nMatched} documents.`);
  logger.info(`Updated ${result.nModified} documents to ${collection.collectionName}.`);
  logger.info(`Upserted ${result.nUpserted} documents to ${collection.collectionName}.`);
}


/* Deletes objects from a collection. Takes an array of objects and a collection */
async function deleteDocuments(collection, docs) {
  try {
    logger.debug(`Deleting documents from collection \'${collection.collectionName}\'`);

    const bulkOps = [];

    for (const doc of docs) {
      /* Query for the document */
      const filter = { _id: doc._id };
      const options = { upsert: true, }; // updates if exists, inserts if not

      bulkOps.push(
        { deleteOne :
          {
            "filter" : filter
          }
        }
      );
    }

    const result = await collection.bulkWrite(bulkOps);

    logger.info(`Deleted ${result.nRemoved} documents from ${collection.collectionName}.`);

  } catch (e) {
    logger.error(`Caught error in deleteDocuments: ${e.toLocaleString()}`);
  }
}


/* Query for all open cases with owner 'MFI Support' and add to unassigned collection */
async function updateUnassigned() {
  logger.info("Updating unassigned collection (frontline).");

  const client = new MongoClient(config.MONGO_URI, {useNewUrlParser: true, useUnifiedTopology: true});

  try {
    // Connect the client to the server
    logger.debug("Connecting to mongo db \"" + config.MONGO_DB + "\" at url: " + config.MONGO_URI);
    await client.connect();
    // Establish and verify connection
    const database = client.db(config.MONGO_DB);
    await database.command({ ping: 1 });
    logger.debug("Connected successfully to mongo server.");

    database.createCollection(MONGO_UNASSIGNED_COLLECTION, function (e) {
      if (e) logger.debug(e.toLocaleString());
    });

    const openCollection = database.collection(MONGO_OPEN_COLLECTION);
    const unassignedCollection = database.collection(MONGO_UNASSIGNED_COLLECTION);

    const query = { caseOwner: "MFI Support" };

    const options = { sort: { dateTimeOpened: 1 } };

    const mfiSupportCases = openCollection.find(query, options);

    logger.debug(`Found ${await mfiSupportCases.count()} cases.`);

    await mfiSupportCases.forEach(function(i) {
      logger.silly(`mfiSupportCases: ${i._id}`);
    });

    const mfiSupportCasesArray = await mfiSupportCases.toArray();
    await upsertToCollection(unassignedCollection, mfiSupportCasesArray);

    casesUpdatedEmitter.emit('unassigned updated', mfiSupportCasesArray);

  } catch (e) {
    logger.error(e.toLocaleString());
  } finally {
    client.close();
    logger.debug("Mongo connection closed.");
  }
}


/* Compare new MFI Support cases against previous records and update fields for newly-assigned cases 
   TODO this function is redundant now that all open cases are constantly updated */
async function updateAssigned(mfiSupportCases) {
  logger.info("Updating fields of assigned cases.");
  /* Connect to MongoDB */
  const client = new MongoClient(config.MONGO_URI, {useNewUrlParser: true, useUnifiedTopology: true});

  try {
    // Connect the client to the server
    logger.debug("Connecting to mongo db \"" + config.MONGO_DB + "\" at url: " + config.MONGO_URI);
    await client.connect();
    // Establish and verify connection
    const database = client.db(config.MONGO_DB);
    await database.command({ ping: 1 });
    logger.debug("Connected successfully to mongo server.");

    const openCollection = database.collection(MONGO_OPEN_COLLECTION);
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
      await deleteDocuments(unassignedCollection, moveQueue);
      await upsertToCollection(openCollection, moveQueue);

      logger.debug("Mongo updated successfully.");
    } else {
      logger.info("No cases need to be updated at this time.");
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
  logger.debug("In updateFields() function.");
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
    logger.info('Finished updating new fields.');
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


/* Remove closed cases from 'open' collection */
async function cleanupClosedCases(closedCases) {
  logger.info('Cleaning up closed cases from \'open\' collection.');

  /* Connect to MongoDB */
  const client = new MongoClient(config.MONGO_URI, {useNewUrlParser: true, useUnifiedTopology: true});

  try {
    // Connect the client to the server
    logger.debug("Connecting to mongo db \"" + config.MONGO_DB + "\" at url: " + config.MONGO_URI);
    await client.connect();
    // Establish and verify connection
    const database = client.db(config.MONGO_DB);
    await database.command({ ping: 1 });
    logger.debug("Connected successfully to mongo server.");

    const openCollection = database.collection(MONGO_OPEN_COLLECTION);

    await deleteDocuments(openCollection, closedCases);
    
  } catch (e) {
    logger.error(e.toLocaleString());
  } finally {
    client.close();
    logger.debug("Mongo connection closed.");
  }
}


/* Remove open cases from 'closed' collection */
async function cleanupOpenCases(openCases) {
  logger.info('Cleaning up open cases from \'closed\' collection.');

  /* Connect to MongoDB */
  const client = new MongoClient(config.MONGO_URI, {useNewUrlParser: true, useUnifiedTopology: true});

  try {
    // Connect the client to the server
    logger.debug("Connecting to mongo db \"" + config.MONGO_DB + "\" at url: " + config.MONGO_URI);
    await client.connect();
    // Establish and verify connection
    const database = client.db(config.MONGO_DB);
    await database.command({ ping: 1 });
    logger.debug("Connected successfully to mongo server.");

    const closedCollection = database.collection(MONGO_CLOSED_COLLECTION);

    await deleteDocuments(closedCollection, openCases);
    
  } catch (e) {
    logger.error(e.toLocaleString());
  } finally {
    client.close();
    logger.debug("Mongo connection closed.");
  }
}


/* Sends an error if no new files have been detected in at least 2 minutes */
async function checkLastUpdateTime() {
  let finished = false;
  await sleep(120000); // wait 2 minutes before first run
  do {
    const currentTime = new Date();

    let diff = currentTime - lastUpdateTime;

    if (diff > 600000) { // ten minutes
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
