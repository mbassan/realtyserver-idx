const colors = require('colors');
colors.enabled = true;

module.exports = class Log {
    static start(message) {
        message = this.stringify(message);
        console.log('\n[' + moment().format("YYYY-MM-DD HH:mm:ss") + ']', message.underline.cyan + '\n');
    }
    
    static info(message) {
        message = this.stringify(message);
        console.log( '[' + moment().format("YYYY-MM-DD HH:mm:ss") + ']',' Info '.bgCyan.white, message + '\n');
    }
    
    static success(message) {
        message = this.stringify(message);
        console.log( '[' + moment().format("YYYY-MM-DD HH:mm:ss") + ']', ' Success '.bgGreen.white, message + '\n');
    }
    
    static warn(message, err) {
        message = this.stringify(message);
        console.log( '[' + moment().format("YYYY-MM-DD HH:mm:ss") + ']', ' Warn '.bgYellow.white, message + '\n', err || '');
    }
    
    static error(message, err) {
        message = this.stringify(message);
        console.log( '[' + moment().format("YYYY-MM-DD HH:mm:ss") + ']', ' Error '.bgRed.white, message + '\n', err || '');
    }

    static stringify(message) {
        if (typeof message == 'object')
            message = JSON.stringify(message);

        return message;
    }
}