const log = require("./lib/Logs")
const cmiImporter = require("./lib/CMIImporter")
const goesrlevel2Importer = require("./lib/GOESRLevel2Importer");

cmiImporter.init();
goesrlevel2Importer.init();
log.info("GeoNetcast [0.17] importer initialized");