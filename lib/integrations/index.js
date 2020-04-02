const INTEGRATIONS = require('./integrations');
const Log = require('../Log');
const cfg = require('../../cfg/cfg.json');

class Client {
    constructor() {
        if (!cfg 
            || !cfg.client_type_
            || typeof cfg.client_type_ != 'string')
            throw new Error('Config: `client_type_` not specified.');

        if (!INTEGRATIONS[cfg.client_type_])
            throw new Error('Config: unsupported type for `client_type_`: ' + cfg.client_type_ + '.');

        // initiate coind
        return new INTEGRATIONS[cfg.client_type_](cfg);
    }
}

module.exports = new Client();