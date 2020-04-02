const Log = require('../lib/Log');

module.exports = {
    indexObject: (thing, index) => {
        if (!index || !thing || !(thing instanceof Array))
            return thing;

        if (thing.length == 0)
            return {};

        if (index && thing[0] && !thing[0][index])
            return thing;

        let indexed = {};
        if (index) {
            for (let row of thing) {
                indexed[row[index]] = row;
            }
        }

        return indexed;
    },
    sleep: (ms) => {
        return new Promise(resolve => {
            setTimeout(resolve,ms)
        })
    },
    sortObject(unordered) {
        if (!unordered
            || typeof unordered != 'object' 
            || unordered instanceof Array)
            return unordered;

        let ordered = {};
        Object.keys(unordered).sort().forEach(function(key) {
            let value = unordered[key];
            if (typeof value == 'object' 
                && !(unordered instanceof Array))
                value = module.exports.sortObject(value);

          ordered[key] = unordered[key];
        });

        return ordered;
    },
    throwError: (err, replace, warn) => {
        if (typeof replace == 'object') {
            err = JSON.stringify({
                err_str: err,
                replace
            });
        }

        if (!warn)
            throw new Error(err);
        else
            Log.warn(err);
    }
}