const fs = require("fs");
const log = require("./Logs");
const moment = require("moment-timezone");
const config = require("./Config");
const { exec } = require('child_process');

class GOESRLevel2Importer {
    static get instance() {
        if (GOESRLevel2Importer.singleton) return GOESRLevel2Importer.singleton;
        GOESRLevel2Importer.singleton = new GOESRLevel2Importer();
        return GOESRLevel2Importer.singleton;
    }

    get code() {return "gnc-goesr-level2"}
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
        // OR_ABI-L2-FDCF-M6_G16_s20213331930209_e20213331939517_c20213331940039-134528_0.nc
        if (!name || !name.startsWith("OR_ABI-L2-FDCF-") || !name.endsWith(".nc")) return 0;
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
            // Subproductss folder names
            let subproducts = [{name:"FDCF", variable:"DQF"}]; 
            let files = [];
            for (let i=0; i<subproducts.length; i++) {
                let subproduct = subproducts[i];                
                let afterTime = state.subproducts[subproduct.name] || 0;
                let path = config.sourcePath + "/GOES-R-Level-2-Products/" + subproduct.name;
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
                                files.push({time:t, path:path + "/" + f, subproduct, name:f});
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
            // normalize time to 10 mn. block
            let mm = fileTime.minutes();
            mm = 10 * parseInt(mm / 10);
            fileTime.minutes(mm);
            fileTime.startOf("minute");
            let varName = f.subproduct.variable;
            let unpacked = "/home/data/working/" + f.name;
            let unpackCmd = "ncpdq -O --omp_num_threads 4 -U -v " + varName + " " + f.path + " " + unpacked;
            await this.exec(unpackCmd);
            let fileName = "gnc-goesrlevel2_[" + varName + "]" + fileTime.format("YYYY-MM-DD_HH-mm") + ".nc";
            let warpped = "/home/data/working/" + fileName;
            let warpCmd = `gdalwarp -multi -s_srs "+proj=geos +lon_0=-75 +h=35786023 +sweep=x +a=6378137 +b=6356752.31414" NETCDF:${unpacked}:${varName} -t_srs "+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0" -overwrite ${warpped}`;
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
            })
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
            log.debug("GOES-R-Level-2 - Importer daemon running");
            let imported = false;
            do {
                let state = this.getState();
                if (!state.subproducts) state.subproducts = {};
                let files = await this.getSourceFiles(state);
                if (files.length) {
                    imported = true;
                    let f = files[0];
                    state.subproducts[f.subproduct.name] = f.time;
                    try {
                        await this.importFile(f);
                    } catch(err) {
                        log.error(err);
                    }
                } else {
                    imported = false;
                }
                this.setState(state);
            } while(imported);
            log.debug("GOES-R-Level-2 - Importer daemon finished");
        } catch(error) {
            console.error(error);
            log.error(error.toString());
        } finally {
            this.running = false;
            this.callDaemon();
        }
    }
}

module.exports = GOESRLevel2Importer.instance;