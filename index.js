/* 
sfreportparser
Listens for changes at config.DOWNLOAD_PATH and parses csv into js objects, then sends to mongo
jwalker
*/
const config = require('./config');
const { MongoClient } = require('mongodb');
const dateFormat = require("dateformat");
const fs = require('fs');
const fsPromises = fs.promises;
const chokidar = require('chokidar');
const parse = require("csv-parse/lib/sync");
const winston = require('winston');
const { createLogger, format, transports } = require('winston');
const { combine, timestamp, label, printf } = format;

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
        url = `https://microfocus.lightning.force.com/lightning/r/Case/${values[1]}/view`;
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
        });
      }
      logger.debug("Created case objects: " + sfCases);

      logger.info("Sending cases to mongo.");
      await uploadToMongo(sfCases);

      lastUpdateTime = new Date();
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

/* Sends an error if no new files have been detected in at least 2 minutes */
async function checkLastUpdateTime() {
  let finished = false;
  do {
    const currentTime = new Date();

    let diff = currentTime - lastUpdateTime;

    if (diff > 120000) { // two minutes
      logger.error("Error: it has been " + Math.floor(diff / 1000 / 60) + " minutes since receiving any updates.\nYou may need to check on the sfexporter.");
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

