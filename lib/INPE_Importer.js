const fs = require("fs");
const log = require("./Logs");
const moment = require("moment-timezone");
const config = require("./Config");
const { exec } = require('child_process');

class INPEImporter {
    static get instance() {
        if (INPEImporter.singleton) return INPEImporter.singleton;
        INPEImporter.singleton = new INPEImporter();
        return INPEImporter.singleton;
    }

    get code() {return "gnc-inpe-importer"}
    getState() {        
        try {
            let j = fs.readFileSync(config.dataPath + "/download/" + this.code + "-state.json");
            if (j) return JSON.parse(j);
            return {};
        } catch (error) {
            return {}
        }
    }
    setState(state) {
        fs.writeFileSync(config.dataPath + "/download/" + this.code + "-state.json", JSON.stringify(state));
    }
    init() {
        this.running = false;
        try {
            fs.mkdirSync("/home/data/working/INPE");
        } catch(error) {            
        }
        this.callDaemon(500);
    }

    callDaemon(ms = 30000) {
        if (this.daemonTimer) clearTimeout(this.daemonTimer);
        this.daemonTimer = setTimeout(_ => {
            this.daemonTimer = null;
            this.daemon();
        }, ms)
    }

    async clearWorkingDir() {
        return new Promise((resolve, reject) => {
            fs.readdir("/home/data/working/INPE", {withFileTypes: true}, (err, files) => {
                if (err) {reject(err); return;}
                for (let d of files) {
                    if (!d.isDirectory() && (d.name.endsWith(".dbf") || d.name.endsWith(".prj") || d.name.endsWith(".shp") || d.name.endsWith(".shx"))) {
                        try {
                            fs.unlinkSync("/home/data/working/INPE/" + d.name);                            
                        } catch(error) {}
                    }
                }
                resolve();
            })
        })
    }

    getTime(st) {
        // 202206221600
        let time = moment.tz(st, "YYYYMMDDHHmm", "UTC");        
        return time;
    }
    getFileTime(name) {
        // INPE_MVF_202206221600.tar.gz        
        let st = name.substr(9, 12);
        return this.getTime(st).valueOf();
    }
    async getSourceFiles(state) {
        try {
            // Subproductss folder names
            let files = [];
            if (!state.time) state.time = Date.now() - 60000 * 60 * 24;
            let afterTime = state.time;
            let path = config.sourcePath + "/INPE";
            await new Promise((resolve, reject) => {                    
                fs.readdir(path, (err, list) => {
                    if (err) {reject(err);return;}
                    for (let f of list) {
                        if (f.endsWith(".tar.gz")) {
                            let t = this.getFileTime(f);
                            let onlyName = f.substring(0, f.length - 7);
                            if (t > afterTime) files.push({time:t, path:path + "/" + f, name:f, onlyName});
                        }
                    }
                    resolve();
                })
            });
            files.sort((f1, f2) => (f1.time - f2.time));
            return files;
        } catch (error) {
            throw error;
        }
    }

    exec(cmd) {
        return new Promise((resolve, reject) => {
            log.debug(" => " + cmd);
            exec(cmd, {maxBuffer:1024 * 1024}, (err, stdout, stderr) => {
                if (err) {
                    log.error(" ---->" + err);
                    reject(err);
                    return;
                }
                if (stderr) {
                    log.warn(" ---->" + stderr);
                    resolve(stderr);
                    return;
                }
                log.debug(" ---->" + stdout);
                resolve(stdout);
            });
        })
    }
    async importFile(f) {
        try {
            await this.clearWorkingDir();
            log.debug(" --> " + f.path + " [" + f.onlyName + "]");
            // Extraer en /home/data/working/INPE
            let cmd = "tar -xf " + f.path + " -C /home/data/working/INPE";
            log.debug("  - Descomprimiendo ...");
            await this.exec(cmd);
            log.debug("  - Extraido en /home/data/working/INPE");

            // Buscar archivo ".shp" creado
            let shpFile = await (
                new Promise((resolve, reject) => {
                    fs.readdir("/home/data/working/INPE", {withFileTypes: true}, (err, files) => {
                        if (err) {reject(err); return;}
                        for (let d of files) {
                            if (!d.isDirectory() && d.name.endsWith(".shp")) {
                                resolve(d.name);
                                return;
                            }
                        }
                        resolve(null);
                    })
                })
            )
            if (!shpFile) throw "No .shp file in tar.gz";            

            // normalize time to 1h block
            let fileTime = moment.tz(f.time, "UTC");
            fileTime.startOf("hour");

            let geoJsonName = "/home/data/working/INPE/" + f.onlyName + ".geojson";
            cmd = "ogr2ogr " + geoJsonName + " /home/data/working/INPE/" + shpFile + " -t_srs WGS84";
            await this.exec(cmd);

            let trgName = "/home/data/import/gnc-subp-inpe_inpe_" + fileTime.format("YYYY-MM-DD_HH_mm") + ".geojson";
            fs.renameSync(geoJsonName, trgName);            
        } catch (error) {
            console.error(error);
            log.error(error.toString())
            throw error;
        } finally {
            try {
                log.debug("  - limpiando ...");
                //await this.clearWorkingDir();
                log.debug("  - Listo");
            } catch(error) {
                console.error(error);
            }
        }
    }

    async daemon() {
        if (this.running) {
            console.log("Reentrance detected");
            return;
        }
        this.running = true;
        try {
            log.debug("INPE - Importer daemon running");
            let imported = false;
            do {
                let state = this.getState();
                if (!state.subproducts) state.subproducts = {};
                let files = await this.getSourceFiles(state);
                if (files.length) {
                    imported = true;
                    let f = files[0];
                    state = this.getState();
                    if (state.time < f.time) {
                        state.time = f.time;
                        this.setState(state);
                        try {
                            await this.importFile(f);
                        } catch(err) {
                            log.error(err);
                        }
                    } else {
                        log.warn("Archivo INPE tomado por otra hebra. Ignorando");
                    }
                } else {
                    imported = false;
                }
                this.setState(state);
            } while(imported);
            log.debug("INPE - Importer daemon finished");
        } catch(error) {
            console.error(error);
            log.error(error.toString());
        } finally {
            this.running = false;
            this.callDaemon();
        }
    }
}

module.exports = INPEImporter.instance;