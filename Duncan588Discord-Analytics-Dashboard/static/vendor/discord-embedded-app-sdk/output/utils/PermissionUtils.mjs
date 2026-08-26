import BigFlagUtils from './BigFlagUtils.mjs';

function can(permission, permissions) {
    return BigFlagUtils.has(BigFlagUtils.deserialize(permissions), permission);
}
var PermissionUtils = {
    can,
};

export { PermissionUtils as default };
