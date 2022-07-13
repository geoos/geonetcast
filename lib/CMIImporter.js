const fs = require("fs");
const log = require("./Logs");
const moment = require("moment-timezone");
const config = require("./Config");
const { exec } = require('child_process');

class CMIImporter {
    static get instance() {
        if (CMIImporter.singleton) return CMIImporter.singleton;
        CMIImporter.singleton = new CMIImporter();
        return CMIImporter.singleton;
    }

    get code() {return "gnc-cmi"}
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
        this.callDaemon(500);
    }

    callDaemon(ms = 30000) {
        if (this.daemonTimer) clearTimeout(this.daemonTimer);
        this.daemonTimer = setTimeout(_ => {
            this.daemonTimer = null;
            this.daemon();
        }, ms)
    }

    getTime(st) {
        let yyyy = parseInt(st.substr(0,4));
        let ddd = parseInt(st.substr(4, 3));
        let hh = parseInt(st.substr(7, 2));
        let mm = parseInt(st.substr(9, 2));
        let sss = parseInt(st.substr(11, 3)) / 10;
        let m = moment.tz("UTC");
        m.year(yyyy); m.startOf("year");
        m.add(ddd - 1, "days");
        m.hour(hh); m.minute(mm); m.second(sss);

        return m.valueOf();
    }
    getFileTime(name) {
        // OR_ABI-L2-CMIPF-M6C01_G16_s20212451910204_e20212451919512_c20212451919581-134600_0.nc
        if (!name || !name.startsWith("OR_ABI-L2-CMIPF-") || !name.endsWith(".nc")) return 0;
        let p0 = name.indexOf("_s");
        if (p0 < 0) return 0;
        let t0 = this.getTime(name.substr(p0+2, 14));
        p0 = name.indexOf("_e");
        if (p0 < 0) return 0;
        let t1 = this.getTime(name.substr(p0+2, 14));
        return parseInt((t0 + t1) / 2);
    }
    async getSourceFiles(state) {
        try {
            let files = [];
            for (let band=1; band <= 16; band++) {
                let afterTime = state.bands[band] || 0;
                let path = config.sourcePath + "/GOES-R-CMI-Imagery/Band" + (band <10?"0":"") + band;
                await new Promise((resolve, reject) => {
                    fs.readdir(path, (err, list) => {
                        if (err) {
                            if (err.code == "ENOENT") resolve();
                            else reject(err);
                            return;
                        }
                        for (let f of list) {
                            let t = this.getFileTime(f);
                            if (t > afterTime) {
                                files.push({time:t, path:path + "/" + f, band:band, name:f});
                            }
                        }
                        resolve();
                    })
                })
            }
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
            log.debug(" --> " + f.path);
            let fileTime = moment.tz(f.time, "UTC");
            // normalize time to 120 mn. block
            let mm = fileTime.minutes();
            mm = 10 * parseInt(mm / 10);
            fileTime.minutes(mm);
            fileTime.startOf("minute");
            let varName = "CMI-" + (f.band < 10?"0":"") + f.band;
            let unpacked = "/home/data/working/" + f.name;
            let unpackCmd = "ncpdq -O --omp_num_threads 4 -U -v CMI " + f.path + " " + unpacked;
            await this.exec(unpackCmd);
            let fileName = "gnc-cmi_[" + varName + "]" + fileTime.format("YYYY-MM-DD_HH-mm") + ".nc";
            let warpped = "/home/data/working/" + fileName;
            let warpCmd = `gdalwarp -multi -s_srs "+proj=geos +lon_0=-75 +h=35786023 +sweep=x +a=6378137 +b=6356752.31414" NETCDF:${unpacked}:CMI -t_srs "+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0" -overwrite ${warpped}`;
            await this.exec(warpCmd);
            let importName = "/home/data/import/" + fileName;
            await new Promise((resolve, reject) => {
                fs.rename(warpped, importName, err => {
                    if (err) reject(err);
                    else {
                        log.debug("File moved to " + importName);
                        resolve();
                    }
                })
            });
            fs.unlinkSync(unpacked);
        } catch (error) {
            throw error;
        }
    }
    async daemon() {
        if (this.running) {
            console.log("Reentrance detected");
            return;
        }
        this.running = true;
        try {
            log.debug("GOES-R-CMI - Importer daemon running");
            let imported = false;
            do {
                let state = this.getState();
                if (!state.bands) state.bands = {};
                let files = await this.getSourceFiles(state);
                if (files.length) {
                    imported = true;
                    let f = files[0];
                    state = this.getState();
                    if (state.bands[f.band] < f.time) {
                        state.bands[f.band] = f.time;
                        this.setState(state);
                        try {
                            await this.importFile(f);
                        } catch(err) {
                            log.error(err);
                        }
                    } else {
                        log.warn("Archivo capturado por otra hebra. Buscando siguiente");
                    }
                } else {
                    imported = false;
                }                
            } while(imported);
            log.debug("GOES-R-CMI - Importer daemon finished");
        } catch(error) {
            console.error(error);
            log.error(error.toString());
        } finally {
            this.running = false;
            this.callDaemon();
        }
    }
}

module.exports = CMIImporter.instance;