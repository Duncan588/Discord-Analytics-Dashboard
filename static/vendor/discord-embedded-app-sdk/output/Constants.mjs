import BigFlagUtils from './utils/BigFlagUtils.mjs';

var RPCCloseCodes;
(function (RPCCloseCodes) {
    RPCCloseCodes[RPCCloseCodes["CLOSE_NORMAL"] = 1000] = "CLOSE_NORMAL";
    RPCCloseCodes[RPCCloseCodes["CLOSE_UNSUPPORTED"] = 1003] = "CLOSE_UNSUPPORTED";
    RPCCloseCodes[RPCCloseCodes["CLOSE_ABNORMAL"] = 1006] = "CLOSE_ABNORMAL";
    RPCCloseCodes[RPCCloseCodes["INVALID_CLIENTID"] = 4000] = "INVALID_CLIENTID";
    RPCCloseCodes[RPCCloseCodes["INVALID_ORIGIN"] = 4001] = "INVALID_ORIGIN";
    RPCCloseCodes[RPCCloseCodes["RATELIMITED"] = 4002] = "RATELIMITED";
    RPCCloseCodes[RPCCloseCodes["TOKEN_REVOKED"] = 4003] = "TOKEN_REVOKED";
    RPCCloseCodes[RPCCloseCodes["INVALID_VERSION"] = 4004] = "INVALID_VERSION";
    RPCCloseCodes[RPCCloseCodes["INVALID_ENCODING"] = 4005] = "INVALID_ENCODING";
})(RPCCloseCodes || (RPCCloseCodes = {}));
var RPCErrorCodes;
(function (RPCErrorCodes) {
    RPCErrorCodes[RPCErrorCodes["INVALID_PAYLOAD"] = 4000] = "INVALID_PAYLOAD";
    RPCErrorCodes[RPCErrorCodes["INVALID_COMMAND"] = 4002] = "INVALID_COMMAND";
    RPCErrorCodes[RPCErrorCodes["INVALID_GUILD"] = 4003] = "INVALID_GUILD";
    RPCErrorCodes[RPCErrorCodes["INVALID_EVENT"] = 4004] = "INVALID_EVENT";
    RPCErrorCodes[RPCErrorCodes["INVALID_CHANNEL"] = 4005] = "INVALID_CHANNEL";
    RPCErrorCodes[RPCErrorCodes["INVALID_PERMISSIONS"] = 4006] = "INVALID_PERMISSIONS";
    RPCErrorCodes[RPCErrorCodes["INVALID_CLIENTID"] = 4007] = "INVALID_CLIENTID";
    RPCErrorCodes[RPCErrorCodes["INVALID_ORIGIN"] = 4008] = "INVALID_ORIGIN";
    RPCErrorCodes[RPCErrorCodes["INVALID_TOKEN"] = 4009] = "INVALID_TOKEN";
    RPCErrorCodes[RPCErrorCodes["INVALID_USER"] = 4010] = "INVALID_USER";
})(RPCErrorCodes || (RPCErrorCodes = {}));
/**
 * @deprecated use OrientationTypeObject instead
 */
var Orientation;
(function (Orientation) {
    Orientation["LANDSCAPE"] = "landscape";
    Orientation["PORTRAIT"] = "portrait";
})(Orientation || (Orientation = {}));
var Platform;
(function (Platform) {
    Platform["MOBILE"] = "mobile";
    Platform["DESKTOP"] = "desktop";
})(Platform || (Platform = {}));
/** See https://discord.com/developers/docs/topics/permissions#permissions-bitwise-permission-flags for more Permissions details */
const Permissions = Object.freeze({
    CREATE_INSTANT_INVITE: BigFlagUtils.getFlag(0),
    KICK_MEMBERS: BigFlagUtils.getFlag(1),
    BAN_MEMBERS: BigFlagUtils.getFlag(2),
    ADMINISTRATOR: BigFlagUtils.getFlag(3),
    MANAGE_CHANNELS: BigFlagUtils.getFlag(4),
    MANAGE_GUILD: BigFlagUtils.getFlag(5),
    ADD_REACTIONS: BigFlagUtils.getFlag(6),
    VIEW_AUDIT_LOG: BigFlagUtils.getFlag(7),
    PRIORITY_SPEAKER: BigFlagUtils.getFlag(8),
    STREAM: BigFlagUtils.getFlag(9),
    VIEW_CHANNEL: BigFlagUtils.getFlag(10),
    SEND_MESSAGES: BigFlagUtils.getFlag(11),
    SEND_TTS_MESSAGES: BigFlagUtils.getFlag(12),
    MANAGE_MESSAGES: BigFlagUtils.getFlag(13),
    EMBED_LINKS: BigFlagUtils.getFlag(14),
    ATTACH_FILES: BigFlagUtils.getFlag(15),
    READ_MESSAGE_HISTORY: BigFlagUtils.getFlag(16),
    MENTION_EVERYONE: BigFlagUtils.getFlag(17),
    USE_EXTERNAL_EMOJIS: BigFlagUtils.getFlag(18),
    VIEW_GUILD_INSIGHTS: BigFlagUtils.getFlag(19),
    CONNECT: BigFlagUtils.getFlag(20),
    SPEAK: BigFlagUtils.getFlag(21),
    MUTE_MEMBERS: BigFlagUtils.getFlag(22),
    DEAFEN_MEMBERS: BigFlagUtils.getFlag(23),
    MOVE_MEMBERS: BigFlagUtils.getFlag(24),
    USE_VAD: BigFlagUtils.getFlag(25),
    CHANGE_NICKNAME: BigFlagUtils.getFlag(26),
    MANAGE_NICKNAMES: BigFlagUtils.getFlag(27),
    MANAGE_ROLES: BigFlagUtils.getFlag(28),
    MANAGE_WEBHOOKS: BigFlagUtils.getFlag(29),
    MANAGE_GUILD_EXPRESSIONS: BigFlagUtils.getFlag(30),
    USE_APPLICATION_COMMANDS: BigFlagUtils.getFlag(31),
    REQUEST_TO_SPEAK: BigFlagUtils.getFlag(32),
    MANAGE_EVENTS: BigFlagUtils.getFlag(33),
    MANAGE_THREADS: BigFlagUtils.getFlag(34),
    CREATE_PUBLIC_THREADS: BigFlagUtils.getFlag(35),
    CREATE_PRIVATE_THREADS: BigFlagUtils.getFlag(36),
    USE_EXTERNAL_STICKERS: BigFlagUtils.getFlag(37),
    SEND_MESSAGES_IN_THREADS: BigFlagUtils.getFlag(38),
    USE_EMBEDDED_ACTIVITIES: BigFlagUtils.getFlag(39),
    MODERATE_MEMBERS: BigFlagUtils.getFlag(40),
    VIEW_CREATOR_MONETIZATION_ANALYTICS: BigFlagUtils.getFlag(41),
    USE_SOUNDBOARD: BigFlagUtils.getFlag(42),
    CREATE_GUILD_EXPRESSIONS: BigFlagUtils.getFlag(43),
    CREATE_EVENTS: BigFlagUtils.getFlag(44),
    USE_EXTERNAL_SOUNDS: BigFlagUtils.getFlag(45),
    SEND_VOICE_MESSAGES: BigFlagUtils.getFlag(46),
    SEND_POLLS: BigFlagUtils.getFlag(49),
    USE_EXTERNAL_APPS: BigFlagUtils.getFlag(50),
});
const UNKNOWN_VERSION_NUMBER = -1;
const HANDSHAKE_SDK_VERSION_MINIMUM_MOBILE_VERSION = 250;

export { HANDSHAKE_SDK_VERSION_MINIMUM_MOBILE_VERSION, Orientation, Permissions, Platform, RPCCloseCodes, RPCErrorCodes, UNKNOWN_VERSION_NUMBER };
