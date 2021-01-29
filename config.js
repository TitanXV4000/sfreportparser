exports.MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/?poolSize=20&writeConcern=majorityw';
exports.MONGO_DB = process.env.MONGO_DB || 'sfwb';
exports.DOWNLOAD_PATH = process.env.DOWNLOAD_PATH || '/sfexports';
exports.NODE_ENV = process.env.NODE_ENV || 'dev';
exports.LOG_LEVEL = process.env.LOG_LEVEL || 'info';