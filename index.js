const log = require("./lib/Logs")
const importer = require("./lib/CMIImporter")

importer.init();
log.info("GeoNetcast [0.07] importer initialized");