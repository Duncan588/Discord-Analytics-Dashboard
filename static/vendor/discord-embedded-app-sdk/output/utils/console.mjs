const consoleLevels = ['log', 'warn', 'debug', 'info', 'error'];
function wrapConsoleMethod(console, level, callback) {
    const _consoleMethod = console[level];
    const _console = console;
    if (!_consoleMethod) {
        return;
    }
    console[level] = function () {
        const args = [].slice.call(arguments);
        const message = '' + args.join(' ');
        callback(level, message);
        _consoleMethod.apply(_console, args);
    };
}

export { consoleLevels, wrapConsoleMethod };
