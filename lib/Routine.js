const idx = require('./idx');
const client = require('./integrations');
const Log = require('./Log');
const { sleep } = require('../util');

module.exports = class Routine {
    static init() {
        this.getLatest();
        this.refreshToken();
        return true;
    }

    static async getLatest() {
        Log.info('Getting latest files from server...');
        
        // get latest data from idx
        //let idx_data = await idx.getLatest();

        // wait a few seconds
        await sleep(5 * 1000);

        // do it again
        //process.nextTick(() => this.getLatest() );
    }

    static async refreshToken() {
        // wait until a minute before expiry
        await sleep((client.token_exp - 60) * 1000);
        await client.getToken();
        process.nextTick(() => this.getLatest() );
    }
}