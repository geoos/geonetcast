const fs = require("fs");
const log = require("./Logs");
const moment = require("moment-timezone");
const config = require("./Config");
const { exec } = require('child_process');
const readLine = require("readline");

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
        /* DBG:
        setTimeout( _ => {
            let postprocess = {
                subproduct:{name:"FDCF", variables:["Power", "Temp", "Area", "DQF"], postprocess:"hotpoints"}, 
                files:{
                    Area:"/home/data/working/pp_6171_gnc-goesrlevel2_[Area]2021-12-27_14-50.nc",
                    DQF:"/home/data/working/pp_6171_gnc-goesrlevel2_[DQF]2021-12-27_14-50.nc",
                    Power:"/home/data/working/pp_6171_gnc-goesrlevel2_[Power]2021-12-27_14-50.nc",
                    Temp:"/home/data/working/pp_6171_gnc-goesrlevel2_[Temp]2021-12-27_14-50.nc"
                }, 
                rndId:6171
            };
            this.ppHotPoints(postprocess);
        }, 1000);
        */
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
            let subproducts = [{name:"FDCF", variables:["Power", "Temp", "Area", "DQF"], postprocess:"hotpoints"}];
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
                                console.log("  => agregado");
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
            let warpedFiles = [];
            let postprocess = f.subproduct.postprocess?{subproduct:f.subproduct, files:{}, rndId:parseInt(9999 * Math.random())}:null;
            for (let varName of f.subproduct.variables) {
                // let varName = f.subproduct.variable;
                let unpacked = "/home/data/working/" + f.name;
                let unpackCmd = "ncpdq -O --omp_num_threads 4 -U -v " + varName + " " + f.path + " " + unpacked;
                await this.exec(unpackCmd);
                let fileName = "gnc-goesrlevel2_[" + varName + "]" + fileTime.format("YYYY-MM-DD_HH-mm") + ".nc";
                let warpped = "/home/data/working/" + fileName;
                let warpCmd = `gdalwarp -multi -s_srs "+proj=geos +lon_0=-75 +h=35786023 +sweep=x +a=6378137 +b=6356752.31414" NETCDF:${unpacked}:${varName} -t_srs "+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0" -overwrite ${warpped}`;
                await this.exec(warpCmd);
                if (postprocess) {
                    let ppName = "/home/data/working/pp_" + postprocess.rndId + "_"  + fileName;
                    await (
                        new Promise(resolve => {
                            fs.copyFile(warpped, ppName, err => {
                                if (err) {
                                    console.error(err);
                                    resolve();
                                } else {
                                    postprocess.files[varName] = ppName;
                                    resolve();
                                }
                            })
                        })
                    )
                }
                let importName = "/home/data/import/" + fileName;
                await new Promise((resolve, reject) => {
                    fs.rename(warpped, importName, err => {
                        if (err) reject(err);
                        else {
                            log.debug("File moved to " + importName);
                            warpedFiles.push(warpped);
                            resolve();
                        }
                    })
                })
                fs.unlinkSync(unpacked);
            }
            if (postprocess) {
                try {
                    await this.doPostProcess(postprocess);
                } catch(ppError) {
                    console.error(ppError);
                    log.error("PostProcess Error: " + ppError);
                }
            }
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

    doPostProcess(pp) {
        switch(pp.subproduct.postprocess) {
            case "hotpoints":
                return this.ppHotPoints(pp);
            default:
                throw "PostProcess '" + pp.subproduct.postprocess + "' no reconocido";
        }
    }

    deleteFile(path) {
        return new Promise(resolve => {
            fs.unlink(path, err => {
                if (err) {
                    console.error("No se puede boraar " + path);
                    console.error(err);
                    log.error(err.toString());
                }
                resolve();
            })
        })
    }

    async ppHotPoints(pp) {
        try {
            console.log("HOTPOINTS Vector Generator", pp);
            //console.log("exportando DQF");
            let cmd = "gdal_translate " + pp.files.DQF + " /home/data/working/dqf.txt -of AAIGrid -q";
            await this.exec(cmd);
            let lng0, lat0, cellSize, ncols, nrows;
            //console.log("procesando DQF");
            let hotPoints = {};
            let rlInterface = readLine.createInterface({input:fs.createReadStream("/home/data/working/dqf.txt")});
            let colNum, lineNum=0, noDataValue;
            await new Promise((resolve, reject) => {
                rlInterface.on("line", line => {
                    let fields = line.split(" ");
                    if (fields.length < 10) {
                        let key = fields[0];
                        let value = fields[fields.length - 1];
                        //console.log(key + " => " + value);
                        switch (key) {
                            case "ncols": ncols = parseInt(value); break;
                            case "nrows": nrows = parseInt(value); break;
                            case "xllcorner": lng0 = parseFloat(value); break;
                            case "yllcorner": lat0 = parseFloat(value); break;
                            case "cellsize": cellSize = parseFloat(value); break;
                            case "NODATA_value": noDataValue = parseFloat(value); break;
                        }
                    } else {
                        for (colNum=0; colNum < fields.length; colNum++) {
                            let v = parseInt(fields[colNum]);
                            if (v != noDataValue) {
                                if (v == 0) {
                                    let lat = lat0 + cellSize * lineNum;
                                    let lng = lng0 + cellSize * colNum;
                                    hotPoints[lineNum + "-" + colNum] = {dqf:v, lat:lat, lng:lng}                                    
                                }
                            }
                        }
                        lineNum++;
                    }
                })
                rlInterface.on("error", err => {
                    console.log("rlInterface Error", err);
                });
                rlInterface.on("close", _ => {
                    //console.log("rlInterface CLOSE");
                    resolve();
                });
            })

            //console.log("exportando Power");
            cmd = "gdal_translate " + pp.files.Power + " /home/data/working/power.txt -of AAIGrid -q";
            await this.exec(cmd);
            //console.log("procesando Power");
            rlInterface = readLine.createInterface({input:fs.createReadStream("/home/data/working/power.txt")});
            lineNum=0;
            await new Promise((resolve, reject) => {
                rlInterface.on("line", line => {
                    let fields = line.split(" ");
                    if (fields.length < 10) {
                        let key = fields[0];
                        let value = fields[fields.length - 1];
                        // console.log(key + " => " + value);
                        switch (key) {
                            case "ncols": ncols = parseInt(value); break;
                            case "nrows": nrows = parseInt(value); break;
                            case "xllcorner": lng0 = parseFloat(value); break;
                            case "yllcorner": lat0 = parseFloat(value); break;
                            case "cellsize": cellSize = parseFloat(value); break;
                            case "NODATA_value": noDataValue = parseFloat(value); break;
                        }
                    } else {
                        for (colNum=0; colNum < fields.length; colNum++) {
                            let v = parseFloat(fields[colNum]);
                            if (v != noDataValue) {
                                if (hotPoints[lineNum + "-" + colNum]) {                                
                                    hotPoints[lineNum + "-" + colNum].power = v;
                                }
                            }
                        }
                        lineNum++;
                    }
                })
                rlInterface.on("error", err => {
                    console.log("rlInterface Error", err);
                });
                rlInterface.on("close", _ => {
                    //console.log("rlInterface CLOSE");
                    resolve();
                });
            })

            // console.log("exportando Temp");
            cmd = "gdal_translate " + pp.files.Temp + " /home/data/working/temp.txt -of AAIGrid -q";
            await this.exec(cmd);
            // console.log("procesando Temp");
            rlInterface = readLine.createInterface({input:fs.createReadStream("/home/data/working/temp.txt")});
            lineNum=0;
            await new Promise((resolve, reject) => {
                rlInterface.on("line", line => {
                    let fields = line.split(" ");
                    if (fields.length < 10) {
                        let key = fields[0];
                        let value = fields[fields.length - 1];
                        // console.log(key + " => " + value);
                        switch (key) {
                            case "ncols": ncols = parseInt(value); break;
                            case "nrows": nrows = parseInt(value); break;
                            case "xllcorner": lng0 = parseFloat(value); break;
                            case "yllcorner": lat0 = parseFloat(value); break;
                            case "cellsize": cellSize = parseFloat(value); break;
                            case "NODATA_value": noDataValue = parseFloat(value); break;
                        }
                    } else {
                        for (colNum=0; colNum < fields.length; colNum++) {
                            let v = parseFloat(fields[colNum]);
                            if (v != noDataValue) {
                                if (hotPoints[lineNum + "-" + colNum]) {                                
                                    hotPoints[lineNum + "-" + colNum].temp = v;
                                }
                            }
                        }
                        lineNum++;
                    }

                })
                rlInterface.on("error", err => {
                    console.log("rlInterface Error", err);
                });
                rlInterface.on("close", _ => {
                    // console.log("rlInterface CLOSE");
                    resolve();
                });
            })

            // console.log("exportando Area");
            cmd = "gdal_translate " + pp.files.Area + " /home/data/working/area.txt -of AAIGrid -q";
            await this.exec(cmd);
            // console.log("procesando Area");
            rlInterface = readLine.createInterface({input:fs.createReadStream("/home/data/working/area.txt")});
            lineNum=0;
            await new Promise((resolve, reject) => {
                rlInterface.on("line", line => {
                    let fields = line.split(" ");
                    if (fields.length < 10) {
                        let key = fields[0];
                        let value = fields[fields.length - 1];
                        // console.log(key + " => " + value);
                        switch (key) {
                            case "ncols": ncols = parseInt(value); break;
                            case "nrows": nrows = parseInt(value); break;
                            case "xllcorner": lng0 = parseFloat(value); break;
                            case "yllcorner": lat0 = parseFloat(value); break;
                            case "cellsize": cellSize = parseFloat(value); break;
                            case "NODATA_value": noDataValue = parseFloat(value); break;
                        }
                    } else {
                        for (colNum=0; colNum < fields.length; colNum++) {
                            let v = parseFloat(fields[colNum]);
                            if (v != noDataValue) {
                                if (hotPoints[lineNum + "-" + colNum]) {                                
                                    hotPoints[lineNum + "-" + colNum].area = v;
                                }
                            }
                        }
                        lineNum++;
                    }

                })
                rlInterface.on("error", err => {
                    console.log("rlInterface Error", err);
                });
                rlInterface.on("close", _ => {
                    //console.log("rlInterface CLOSE");
                    resolve();
                });
            })

            //console.log("hotPoints", hotPoints);
            // Crear geojson
            let geojson = {type:"FeatureCollection", name:"HotPoints", crs:{
                type:"name", properties:{name:"urn:ogc:def:crs:OGC:1.3:CRS84"}
            }, features:[]}
            Object.keys(hotPoints).forEach(key => {
                let p = hotPoints[key];
                let f = {type:"Feature", properties:{dqf:p.dqf}, geometry:{type:"Point", coordinates:[p.lng, p.lat]}}
                if (p.power) f.properties.power = p.power;
                if (p.area) f.properties.area = p.area;
                if (p.temp) f.properties.temp = p.temp;
                geojson.features.push(f);
            });
            let dqfFile = pp.files.DQF;
            let p0 = dqfFile.lastIndexOf("]");
            let geojsonFileName = dqfFile.substr(p0+1);
            geojsonFileName = "/home/data/import/gnc-vector-goesrlevel2_hotpoints_" + geojsonFileName.substr(0, geojsonFileName.length - 3) + ".geojson";
            await new Promise(resolve => {
                fs.writeFile(geojsonFileName, JSON.stringify(geojson), err => {
                    if (err) console.error(err);
                    resolve();
                })
            });
            await this.deleteFile(pp.files.DQF);
            await this.deleteFile("/home/data/working/dqf.txt");
            await this.deleteFile("/home/data/working/dqf.prj");
            await this.deleteFile("/home/data/working/dqf.txt.aux.xml");

            await this.deleteFile(pp.files.Power);
            await this.deleteFile("/home/data/working/power.txt");
            await this.deleteFile("/home/data/working/power.prj");
            await this.deleteFile("/home/data/working/power.txt.aux.xml");

            await this.deleteFile(pp.files.Temp);
            await this.deleteFile("/home/data/working/temp.txt");
            await this.deleteFile("/home/data/working/temp.prj");
            await this.deleteFile("/home/data/working/temp.txt.aux.xml");

            await this.deleteFile(pp.files.Area);
            await this.deleteFile("/home/data/working/area.txt");
            await this.deleteFile("/home/data/working/area.prj");
            await this.deleteFile("/home/data/working/area.txt.aux.xml");

        } catch (error) {
            throw error;
        }
    }
}

module.exports = GOESRLevel2Importer.instance;