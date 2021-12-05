
class Config {
    static get instance() {
        if (Config.singleton) return Config.singleton;
        Config.singleton = new Config();
        return Config.singleton;
    }

    get timeZone() {return process.env.TIME_ZONE || "America/Santiago"}
    get logLevel() {return (process.env.LOG_LEVEL || "info").toLowerCase()}
    get logRetain() {return parseInt(process.env.LOG_RETAIN || "30")}
    get logPrefix() {return (process.env.LOG_PREFIX || "gnc-")}

    get downloaderActive() {
        let act = process.env.DOWNLOADER_ACTIVE;
        if (act && act.toLowerCase() == "false") return false;
        return true;
    }

    get dataPath() {return "/home/data"}
    get configPath() {return "/home/config"}
    get logPath() {return "/home/log"}

    get sourcePath() {return "/source"}

    get nParallelDownloads() {return parseInt(process.env.N_PARALLEL_DOWNLOADS || "5")}
}

module.exports = Config.instance;