const _ = require('lodash');
const moment = require('moment');
const Routine = require('./Routine');
const Log = require('./Log');
const cache = require('../util/cache');
const client = require('./integrations');
const idx = require('./idx');

module.exports = class Main {
    static async init() {
        // set globals
        global._ = _;
        global.moment = moment;

        // get startup info
        try {
            await Promise.all([
                client.connect(),
                idx.connect(),
                cache.connect()
            ]);
        }
        catch (err) {
            Log.error('An error occurred on startup! Exiting...', err);
            process.exit(1);
        }

        // start routines
        return Routine.init();
    }
}