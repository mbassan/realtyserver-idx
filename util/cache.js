const RedisClustr = require('redis-clustr');
const Redis = require('redis');
const Promise = require('bluebird');
const escape_quotes = require('escape-quotes');
const Log = require('../lib/Log');
const cfg = require('../cfg/cfg.json');
const { throwError, sleep, sortObject, indexObject } = require('../util');
const { ERRORS } = require('./constants');

class Cache {
    connect() {
        Log.start('Connecting to cache...');
        cfg.redis_port = cfg.redis_port || 6379;
        cfg.redis_host = cfg.redis_host || '127.0.0.1';

        // promisify redis
        Promise.promisifyAll(Redis.RedisClient.prototype);
        Promise.promisifyAll(Redis.Multi.prototype);

        // set redis
        let redis = new RedisClustr({
            servers: [{
                host: cfg.redis_host,
                port: cfg.redis_port,
                password: cfg.redis_pass,
                createClient: () => {
                    return Redis.createClient(cfg.redis_port, cfg.redis_host, { db: cfg.redis_db });
                }
            }]
        });

        //this._redis_io = redis.createClient(); // placeholder for pub/sub & socket.io
        this._redis = redis.createClient();

        Log.info('Connected to cache.');
        return true;
    }

    // check if something exists in the cache
    async exists({
        key,
        h_key_value,
        multi
    }) {
        let cached = null;
        let redis = multi || this._redis;

        try {
            if (!key)
                throwError('CACHE_ERROR', { error: 'Cache: `key` parameter is mandatory.' });

            if (h_key_value)
                cached = await redis.hexistsAsync(key,h_key_value) || null;
            else
                cached = await redis.existsAsync(key) || null;

        }
        catch (err) {
            throwError('CACHE_ERROR', { error: err.toString() });
        }

        return cached;
    }

    // key = [string, list, hash] (default is simple)
    // key format = class:method:namespace:id
    async get({ 
        key, 
        cache_type,
        params,
        h_key,
        h_key_value,
        s_key,
        s_key_value,
        start, 
        end,
        desc = false,
        limit,
        offset,
        with_scores,
        filter,
        multi,
        check_exists,
        index,
        until_sum // gets sorted set until var >= this
    }) {
        // check for h_key value
        if (cache_type == 'hash' && params)
            h_key_value = h_key_value || params[h_key];
        if (cache_type == 'sorted' && params)
            s_key_value = s_key_value || params[s_key];

        if (check_exists) {
            return this.exists({
                key,
                h_key_value,
                s_key_value,
                multi
            });
        }

        let cached = null;
        let redis = multi || this._redis;

        let range = this.getRange(cache_type, start, end, s_key_value);
        if (range[0])
            start = range[0];
        if (range[1])
            end = range[1];

        if (!redis)
            throwError('CACHE_ERROR', { error: 'Cache not available.' });

        if (!key)
            throwError('CACHE_ERROR', { error: 'Cache: `key` parameter is mandatory.' });

        try {
            if (cache_type == 'list')
                cached = await redis.lrangeAsync(key,start,end);
            else if (cache_type == 'hash' && h_key_value)
                cached = await redis.hgetAsync(key,h_key_value);
            else if (cache_type == 'hash') {
                let all = await redis.hgetallAsync(key);
                if (all) {
                    cached = [];
                    for (let k in all)
                        cached.push(this.unflatten(all[k]));
                }
            }
            else if (cache_type == 'sorted') {
                let params = [ key ];

                if (!desc) {
                    params.push(start);
                    params.push(end);
                }
                else {
                    params.push(end);
                    params.push(start);
                }

                if (with_scores)
                    params.push('WITHSCORES');

                if (limit) {
                    offset = offset ? offset : 0;
                    limit = limit + offset;
                    params.push('LIMIT');
                    params.push(offset);
                    params.push(limit);
                }

                if (filter) {
                    let params1 = [ key, '0', 'match', this.getFilter(filter) ];

                    // need to make this recursive because it doesn't always scan everything
                    let res = await this.scan(redis, params1);

                    if (res)
                        cached = res;
                }
                else if (until_sum) {
                    cached = await this.getWhileSubtract(key, until_sum, desc);
                    cached = this.parseScores(cached, with_scores);
                }
                else {
                    if (!desc)
                        cached = await redis.zrangebyscoreAsync(...params);
                    else
                        cached = await redis.zrevrangebyscoreAsync(...params);

                    cached = this.parseScores(cached, with_scores);
                }
            }
            else
                cached = await redis.getAsync(key);
        }
        catch (err) {
            throwError('CACHE_ERROR', { error: err.toString() });
        }

        if (cached && typeof cached == 'string')
            cached = this.unflatten(cached);

        if (index)
            cached = indexObject(cached, index);

        return cached;
    }

    async scan(redis, params, res) {
        res = res || [];
        let res1 = await redis.zscanAsync(...params);

        if (!res1)
            return null;

        if (res1[1]) {
            let c = 1;
            for (let i in res1[1]) {
                if (c % 2 != 0) {
                    res.push(this.unflatten(res1[1][i]));
                }
                c++;
            }
        }

        if (res1[0] === '0') {
            return _.uniqWith(res, _.isEqual);
        }
        else {
            params[1] = res1[0];
            return this.scan(redis, params, res);
        }
    }

    async set({ 
        key, 
        params, 
        cache_type, 
        l_max_length, 
        h_key,
        h_key_value,
        s_key,
        s_key_value,
        multi,
        expire_in
    }) {
        let res = false;
        let redis = multi || this._redis;

        if (!redis)
            throwError('CACHE_ERROR', { error: 'Cache not available.' });
        if (!key)
            throwError('CACHE_ERROR', { error: 'Cache: `key` parameter is mandatory.' });

        try {
            let flat = this.flatten(params);

            if (cache_type == 'list') {
                res = await redis.rpushAsync(key, flat);

                if (!isNaN(l_max_length)) {
                    let l = await redis.llenAsync(key);
                    if (l > l_max_length)
                        res = await redis.rpopAsync(key);
                }
            }
            else if (cache_type == 'hash') {
                let [ key_val ] = this.formatHash(params, h_key, h_key_value, cache_type);
                if (!key_val)
                    throwError('CACHE_ERROR', { error: 'Cache: `key` parameter is mandatory.' });

                res = await redis.hmsetAsync(key,key_val);
            }
            else if (cache_type == 'sorted') {
                let [ key_val ] = this.formatHash(params, s_key, s_key_value, cache_type);

                if (!key_val)
                    throwError('CACHE_ERROR', { error: 'Cache: `s_key` parameter is mandatory.' });
                
                let values = _.union([ key, 'NX' ], key_val);
                res = await redis.zaddAsync(...values);
            }
            else {
                if (!expire_in)
                    res = await redis.setAsync(key,flat);
                else
                    res = await redis.setexAsync(key,flat,expire_in);
            }
        }
        catch (err) {
            throwError('CACHE_ERROR', { error: err.toString() });
        }

        return res;
    }

    async delete({ 
        key,
        cache_type,
        h_key_value,
        s_key_values,
        start,
        end,
        multi
    }) {
        let res = false;
        let redis = multi || this._redis;
        [ start, end ] = this.getRange(cache_type, start, end);

        if (!redis)
            throwError('CACHE_ERROR', { error: 'Cache not available.' });

        if (!key)
            throwError('CACHE_ERROR', { error: 'Cache: `key` parameter is mandatory.' });

        try {
            if (cache_type == 'hash' && h_key_value) {
                res = await redis.hdelAsync(key,h_key_value);
            }
            else if (cache_type == 'sorted') {
                if (s_key_values && s_key_values.length > 0) {
                    let self = this;
                    let params = _.union([ key ], _.map(s_key_values, (item) => self.flatten(item)));
                    res = await redis.zremAsync(...params);
                }
                else if (start || end) {
                    if (!isNaN(start))
                        start = '(' + start;
                    if (!isNaN(end))
                        end = '(' + end;

                    let params = [ key, start, end ];
                    res = await redis.zremrangebyscoreAsync(...params);
                }
            }
            else
                res = await redis.delAsync(key);
        }
        catch (err) {
            throwError('CACHE_ERROR', { error: err.toString() });
        }

        return res;
    }

    // meant for updating rows of data
    async update({ 
        key, 
        params,
        old_params,
        cache_type, 
        h_key,
        h_key_value,
        s_key,
        s_key_value,
        multi,
        expire_in
    }) {
        if (cache_type == 'sorted' && !s_key) {
            throwError('CACHE_ERROR', { error: 'Cache: `s_key` parameter is mandatory for sorted sets.' });
            return false;
        }

        if (cache_type == 'hash' && !h_key) {
            throwError('CACHE_ERROR', { error: 'Cache: `h_key` parameter is mandatory for hashes.' });
            return false;
        }

        if (!old_params) {
            if (cache_type == 'hash')
                h_key_value = h_key_value || params[h_key];
            if (cache_type == 'sorted')
                s_key_value = s_key_value || params[s_key];

            old_params = await this.get({ 
                key, 
                cache_type,
                params,
                h_key,
                h_key_value,
                s_key,
                s_key_value,
                multi
            });

            if (cache_type == 'sorted')
                old_params = old_params[0];

            params = _.assign(_.cloneDeep(old_params), params);
        }

        if (cache_type == 'sorted') {
            await this.delete({ 
                key,
                cache_type,
                s_key_values: [ old_params ],
                multi
            });
        }

        return await this.set({ 
            key, 
            params, 
            cache_type, 
            h_key,
            h_key_value,
            s_key,
            s_key_value,
            multi,
            expire_in
        });
    }

    async incr({ 
        key, 
        value, 
        cache_type, 
        h_key_value,
        multi
    }) {
        let res = false;
        let redis = multi || this._redis;

        if (!redis)
            throwError('CACHE_ERROR', { error: 'Cache not available.' });

        if (!key)
            throwError('CACHE_ERROR', { error: 'Cache: `key` parameter is mandatory.' });

        try {
            if (cache_type == 'hash') {
                if (h_key_value)
                    res = await redis.hincrbyfloatAsync(key, h_key_value, value);
                else
                    throwError('CACHE_ERROR', { error: 'Cache: `h_key_value` parameter is mandatory.' });
            }
            else {
                res = await redis.incrbyfloatAsync(key, value);
            }
        }
        catch (err) {
            throwError('CACHE_ERROR', { error: err.toString() });
        }

        return res;
    }

    // mods = { key: increment, key: decrement, ...}
    async incDec(key, mods, multi) {
        if (typeof mods != 'object' || mods instanceof Array) {
            Log.warn('Invalid price object passed to `Orders.setDepth`.');
            return false;
        }

        let redis = multi || this._redis;

        let script = `
            local member = 0
            local res = {}
            local c = 1

            for i, v in ipairs(ARGV) do
                if (i % 2 == 0) then
                    res[c] = redis.call('zincrby', KEYS[1], v, member)

                    if (tonumber(res[c]) <= 0) then
                        redis.call('zrem', KEYS[1], member)
                    end

                    c = c + 1
                else
                    member = v
                end
            end

            return res
        `;

        let values = [];
        let params = [
            script,
            1,
            key
        ];

        for (let member in mods) {
            let inc = mods[member];
            params.push(member);
            params.push(inc);
            values.push(member);
        }

        let res = await redis.evalAsync(...params);
        if (res && typeof res == 'object')
            return _.zipObject(values, res);

        return false;
    }

    // will iterate over sorted set until amount <= 0 and return lowest value
    async getWhileSubtract(key, amount, desc, multi) {
        let redis = multi || this._redis;
        let func = desc ? 'zrevrangebyscore' : 'zrangebyscore';

        let script = `
            local function split(s, delimiter)
                local result = {}
                for match in (s..delimiter):gmatch("(.-)"..delimiter) do
                    table.insert(result, match)
                end
                return result
            end

            local result = {}
            local sum = 0
            local s_start = "-inf"
            local s_end = '+inf'
            local found = false

            if (ARGV[1] == 'true') then
                s_start = "+inf"
                s_end = "-inf"
            end

            repeat
                local res = redis.call('${func}', KEYS[1], s_start, s_end, 'WITHSCORES', 'LIMIT', 0, 10)
                for i, v in ipairs(res) do
                    if (i % 2 == 0) then
                        s_start = "("..v
                    else
                        local data = split(v, ",")
                        sum = sum + data[1]
                        table.insert(result, v)
                        found = true
                    end
                end
            until sum >= ${amount} or not found
            return result
        `;

        return await redis.evalAsync(script, 1, key, desc);
    }

    async start() {
        let multi = await this._redis.multiAsync();
        return multi;
    }

    async execute(multi) {
        return await multi.execAsync();
    }

    async discard(multi) {
        return await multi.discardAsync();
    }

    async getLock({ 
        key, 
        h_key, 
        timeout, 
        identifier,
        expire_in
    }, start, re) {
        if (!identifier)
            throw new Error(ERRORS['CACHE_LOCK_IDENTIFIER']);

        // if call is recursive, skip
        if (!re) {
            key = 'lock.' + key + (h_key ? '.' + h_key : '');
            start = start || moment().valueOf();
            timeout = timeout * 1000 || 3000;
            expire_in = expire_in * 1000 || 30000
        }

        // try to get lock
        let l = null;
        try {
            l = await this._redis.setAsync(key, identifier, 'PX', expire_in, 'NX');
        }
        catch (err) {
            console.log(err)
            throwError('CACHE_ERROR', { error: err.toString() });
        }

        if (!l) {
            await sleep(10);

            // if we could not get the lock before the `timeout`, return false
            if (moment().valueOf() - start > timeout)
                return false;

            return await this.getLock({ key, h_key, timeout, identifier, expire_in, re: true }, start, true);
        }

        return l;
    }

    async releaseLock({ 
        key, 
        h_key, 
        identifier 
    }) {
        if (!identifier)
            throw new Error(ERRORS['CACHE_LOCK_IDENTIFIER']);

        key = 'lock.' + key + (h_key ? '.' + h_key : '');
        let value = await this._redis.getAsync(key);
        let res = null;

        if (value == identifier)
            res = await this._redis.del(key);
        else
            Log.warn('Lock identifier mismatch.');

        return res;
    }

    async getMaxScore(key, multi) {
        let redis = multi || this._redis;
        let cached = await redis.zrevrangebyscoreAsync(key, '+inf', '-inf', 'WITHSCORES', 'LIMIT', 0, 1);

        if (!cached || !cached[1])
            return null;

        return parseInt(cached[1]);
    }

    async getMinScore(key, multi) {
        let redis = multi || this._redis;
        let cached = await redis.zrangebyscoreAsync(key, '-inf', '+inf', 'WITHSCORES', 'LIMIT', 0, 1);

        if (!cached || !cached[1])
            return null;

        return parseInt(cached[1]);
    }

    getRange(cache_type, start, end, s_key_value) {
        if (s_key_value)
            return [ s_key_value, s_key_value ];

        if (cache_type == 'list') {
            start = start || 0;
            end = end || -1;
        }
        else if (cache_type == 'sorted') {
            start = start || '-inf';
            end = end || '+inf';
        }

        return [ start, end ];
    }

    parseScores(res, with_scores, has_filter) {
        if (!(res instanceof Array))
            return res;

        let res_n = [];
        let res_y = {};
        let value = null;
        let c = 1;

        for (let row of res) {
            if (with_scores) {
                if (c % 2 == 0) {
                    res_y[row] = this.unflatten(value);
                }
                else {
                    value = row;
                }
            }
            else {
                if (!has_filter 
                    || (has_filter && c % 2 != 0)) {
                    res_n.push(this.unflatten(row));    
                }
            }

            c++;
        }

        return with_scores ? res_y : res_n;
    }

    formatHash(value, h_key, h_key_value, cache_type) {
        let indexed = {};
        let valid = false;
        let key_val = [];
        let min_score = null;
        let scores = [];

        // if value is string
        if (h_key_value) {
            return [[ h_key_value, this.flatten(value) ]];
        }

        // else if not an object, return false
        if (typeof value != 'object')
            return [];

        // check if is array of objects and try to index by key
        if (value instanceof Array 
            && value.length > 0) {
            indexed = indexObject(value, h_key);
            valid = typeof indexed == 'object' && !(indexed instanceof Array);
        }
        // if already indexed by key (object of objects)
        else if (Object.keys(value).length > 0 
            && typeof value[Object.keys(value)[0]] == 'object') {
            indexed = value;
            valid = true;
        }
        // if it is only one level and valid h_key
        else if (h_key && value[h_key]) {
            indexed[value[h_key]] = value;
            valid = true;
        }

        // exit if nothing to set
        if (!valid) {
            return [];
        }

        // if there is something to set
        for (let i in indexed) {
            key_val.push(i);
            key_val.push(this.flatten(indexed[i]));

            if (cache_type == 'sorted') {
                scores.push(i);

                if (i < min_score || !min_score)
                    min_score = i;
            }
        }

        return [ key_val, min_score, scores ];
    }

    flatten(object) {
        if (!object || typeof object != 'object')
            return object;

        if (object instanceof Array) {
            for (let i in object) {
                object[i] = sortObject(object[i]);
            }
        }
        else {
            object = sortObject(object);
        }

        return JSON.stringify(object);
    }

    unflatten(object) {
        try {
            return JSON.parse(object);
        }
        catch (err) {
            return object;
        }
    }

    getFilter(filter) {
        return '*' + escape_quotes(JSON.stringify(filter).replace('{','').replace('}','').trim(), `'"`) + '*';
    }
}

module.exports = new Cache();