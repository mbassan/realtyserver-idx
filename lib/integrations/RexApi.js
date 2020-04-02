const request = require('request-promise-native');
const Log = require('../Log');
const cfg = require('../../cfg/cfg.json');

module.exports = class RexApi {
    constructor() {
        this.type = cfg.client_type;
        this.url = cfg.client_url;
        this.key = cfg.client_key;
        this.secret = cfg.client_secret;
        this.region_id = cfg.client_region_id;
        this.token = null; 
        this.token_exp = 0;
    }

    async connect() {
        Log.start('Connecting to client service...');

        // attempt to get a token
        return await this.getToken();
    }

    async getToken() {
        let res = await this.request({
            method: 'POST',
            url: 'oauth/token'
        });

        this.token = res.access_token;
        this.token_exp = res.expires_in;

        return true;
    }

    async request(options) {
        let headers = {
            Accept: 'application/json; charset=utf-8',
            'Content-Type': 'application/x-www-form-urlencoded'
        };

        if (options.url.indexOf('oauth') < 0)
            headers.Authorization = this.token ? 'OAUTH oauth_token="' + this.token + '", api_key="' + this.key + '"' : ''

        let res = await request({
            uri: this.url + '/' + options.url,
            method: options.method || 'GET',
            headers,
            body: `=grant_type%3Dauthorization_code%26client_id%3D${this.key}%26code%3D${this.secret}`
        });

        if (!res)
            return false;

        return JSON.parse(res);
    }
}