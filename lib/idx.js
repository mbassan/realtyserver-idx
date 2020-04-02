const cfg = require('../cfg/cfg.json');
const cache = require('../util/cache');
const Log = require('./Log');
const Ftp = require('ftp');
const unzipper = require('unzipper');
const parse = require('csv-parse');
const fs = require('fs');

class Idx {
    constructor() {
        this.client = new Ftp();
    }

    async connect() {
        Log.start('Connecting to IDX server..');

        return new Promise((resolve, reject) => {
            this.client.connect({
                host: cfg.server_url,
                port: cfg.server_port || 21,
                user: cfg.server_user,
                password: cfg.server_pass
            });

            this.client.on('ready', () => {
                Log.info('Connected to IDX.');
                resolve(true);
            });

            this.client.on('error', (err) => {
                reject(err);
            });
        });
    }

    async getLatest() {
        try {
            let files = await this.ls();
            let latest = this.filterLatest(files);

            // nothing was found, exit
            if (!latest) {
                Log.warn('IDX: No files returned by server.');
                return false;
            }

            // download the latest files
            //await this.getFiles(latest);

            // read the data into memory
            let parsed = await this.parseFiles(latest);

        }
        catch (err) {
            Log.error('IDX error:', err);
        }
    }

    filterLatest(files) {
        if (!files || files.length == 0)
            return false;

        // in reverse order
        let latest = {
            res: null,
            ofc: null,
            com: null,
            agt: null
        }

        let cats = [ 'res', 'ofc', 'com', 'agt' ];
        let i = 0;

        // loop in reverse order
        for (let file of files.reverse()) {
            // all cats done
            if (!cats[i])
                break;

            if (!file || !file.name)
                continue;

            // skip headers
            if (file.name.indexOf('headers') >= 0)
                continue;

            // move on if category is done
            if (file.name.indexOf(cats[i]) < 0)
                continue;
            
            // first result is highest date
            latest[cats[i]] = file.name;
            i++;
        }

        return latest;
    }

    async getFiles(latest) {
        for (let cat in latest) {
            await new Promise((resolve) => {
                this.client.get('/' + latest[cat], (err, stream) => {
                    Log.info('Downloading ' + latest[cat] + '...');

                    if (err) {
                        Log.error('Error downloading ' + latest[cat] + ':', err);
                        resolve(false);
                    }

                    stream.on('error', (err) => {
                        Log.error('Error downloading ' + latest[cat] + ':', err);
                        resolve(false);
                    });

                    stream.once('data', () => {
                        stream.end();
                        resolve(true);
                    });

                    stream.pipe(fs.createWriteStream(__dirname + '/../files/' + cat + '.csv' , { flags: 'w' }));
                });
            });
        }
    }

    async parseFiles(latest) {
        let parsed = {};

        for (let cat in latest) {
            await new Promise((resolve) => {
                Log.info('Reading ' + latest[cat] + ' into memory...');

                parsed[cat] = [];
                const content = fs.readFileSync(__dirname + '/../files/' + cat + '.csv');
                const records = parse(content.toString(), {
                    columns: true,
                    skip_empty_lines: true,
                    trim: true
                });

                records.once('readable', () => {
                    let record
                    while (record = records.read()) {
                        parsed[cat].push(record);
                    }
                });

                records.once('error', (err) => {
                    Log.error('Error reading ' + latest[cat] + ':', err);
                    resolve(false);
                });

                records.once('end', () => {
                    resolve(true);
                });
            });
        }

        return parsed;
    }

    async ls() {
        return new Promise((resolve, reject) => {
            this.client.list('/', false, (err, data) => {
                if (err)
                    reject(err);
                else
                    resolve(data);
            });
        });
    }

    // date format: YYYYMMDD
    static async getLastDate(cat) {
        return await cache.get({
            key: 'last_date:' + cat
        });
    }

    // date format: YYYYMMDD
    static async setLastDate(cat, date) {
        return await cache.set({
            params: date,
            key: 'last_date:' + cat
        });
    }
}

module.exports = new Idx();