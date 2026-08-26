/**
 * Assets x is statically unreachable at build-time,
 * and throws at runtime if data is dynamic.
 */
function assertUnreachable(_x, runtimeError) {
    throw runtimeError;
}

export { assertUnreachable as default };
