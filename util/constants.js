module.exports = {
    ERRORS: {
        CACHE_ERROR: {
            code: 'CACHE_ERROR',
            message: '[error]',
            status: 500
        },
        CACHE_LOCK_IDENTIFIER: {
            code: 'DB_LOCK_IDENTIFIER',
            message: '`identifier` is necessary for locks.',
            status: 500
        },
        CACHE_LOCK_TIMEOUT: {
            code: 'DB_LOCK_TIMEOUT',
            message: 'Could not acquire lock - timeout.',
            status: 500
        },
        KEY_EXISTS: {
            code: 'KEY_EXISTS',
            message: 'Key file already exists.',
            status: 500
        },
        PARAM_INVALID: {
            code: 'PARAM_INVALID',
            message: 'Invalid parameter: `[param]`.',
            status: 500
        },
        PARAM_MISSING: {
            code: 'PARAM_MISSING',
            message: 'Missing parameter: `[param]`.',
            status: 500
        }
    }
};