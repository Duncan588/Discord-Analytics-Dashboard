class SDKError extends Error {
    constructor(code, message = '') {
        super(message);
        this.code = code;
        this.message = message;
        this.name = 'Discord SDK Error';
    }
}

export { SDKError };
