const log = require("./lib/Logs")
const cmiImporter = require("./lib/CMIImporter")
const inpeImporter = require("./lib/INPE_Importer.js")
// const goesrlevel2Importer = require("./lib/GOESRLevel2Importer");

cmiImporter.init();
inpeImporter.init();
// goesrlevel2Importer.init();
log.info("GeoNetcast [0.17] importer initialized");