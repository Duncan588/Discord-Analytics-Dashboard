import { object as objectType, string as stringType, array as arrayType, number as numberType, boolean as booleanType, bigint as bigIntType, null as nullType, optional as optionalType, nativeEnum as nativeEnumType } from '../lib/zod/lib/index.mjs';
import { GuildMember, UserVoiceState, Message, Channel, VoiceSettingsIO, VoiceSettingsMode, ShortcutKey, Sku, Entitlement, ReceiveFramePayload, Commands, ChannelTypesObject, Activity } from './common.mjs';
import { zodCoerceUnhandledValue, fallbackToDefault } from '../utils/zodUtils.mjs';
import { Schemas } from '../generated/schemas.mjs';
export { AuthenticateResponseSchema as AuthenticateResponse, InitiateImageUploadResponseSchema as InitiateImageUploadResponse } from '../generated/schemas.mjs';
import assertUnreachable from '../utils/assertUnreachable.mjs';

const EmptyResponse = objectType({}).nullable();
const AuthorizeResponse = objectType({
    code: stringType(),
});
const GetGuildsResponse = objectType({
    guilds: arrayType(objectType({
        id: stringType(),
        name: stringType(),
    })),
});
const GetGuildResponse = objectType({
    id: stringType(),
    name: stringType(),
    icon_url: stringType().optional(),
    members: arrayType(GuildMember),
});
const GetChannelResponse = objectType({
    id: stringType(),
    type: zodCoerceUnhandledValue(ChannelTypesObject),
    guild_id: stringType().optional().nullable(),
    name: stringType().optional().nullable(),
    topic: stringType().optional().nullable(),
    bitrate: numberType().optional().nullable(),
    user_limit: numberType().optional().nullable(),
    position: numberType().optional().nullable(),
    voice_states: arrayType(UserVoiceState),
    messages: arrayType(Message),
});
const GetChannelsResponse = objectType({
    channels: arrayType(Channel),
});
const NullableChannelResponse = GetChannelResponse.nullable();
const SelectVoiceChannelResponse = GetChannelResponse.nullable();
const SelectTextChannelResponse = GetChannelResponse.nullable();
const VoiceSettingsResponse = objectType({
    input: VoiceSettingsIO,
    output: VoiceSettingsIO,
    mode: VoiceSettingsMode,
    automatic_gain_control: booleanType(),
    echo_cancellation: booleanType(),
    noise_suppression: booleanType(),
    qos: booleanType(),
    silence_warning: booleanType(),
    deaf: booleanType(),
    mute: booleanType(),
});
const SubscribeResponse = objectType({
    evt: stringType(),
});
const CaptureShortcutResponse = objectType({ shortcut: ShortcutKey });
const SetActivityResponse = Activity;
const GetSkusResponse = objectType({ skus: arrayType(Sku) });
const GetEntitlementsResponse = objectType({ entitlements: arrayType(Entitlement) });
const StartPurchaseResponse = arrayType(Entitlement).nullable();
const SetConfigResponse = objectType({
    use_interactive_pip: booleanType(),
});
const UserSettingsGetLocaleResponse = objectType({
    locale: stringType(),
});
const EncourageHardwareAccelerationResponse = objectType({
    enabled: booleanType(),
});
const GetChannelPermissionsResponse = objectType({
    permissions: bigIntType().or(stringType()),
});
const OpenExternalLinkResponse = fallbackToDefault(objectType({ opened: booleanType().or(nullType()) }).default({ opened: null }));
/**
 * Because of the nature of Platform Behavior changes
 * every key/value is optional and may eventually be removed
 */
const GetPlatformBehaviorsResponse = objectType({
    iosKeyboardResizesView: optionalType(booleanType()),
});
const ResponseFrame = ReceiveFramePayload.extend({
    cmd: nativeEnumType(Commands),
    evt: nullType(),
});
function parseResponseData({ cmd, data }) {
    switch (cmd) {
        case Commands.AUTHORIZE:
            return AuthorizeResponse.parse(data);
        case Commands.CAPTURE_SHORTCUT:
            return CaptureShortcutResponse.parse(data);
        case Commands.ENCOURAGE_HW_ACCELERATION:
            return EncourageHardwareAccelerationResponse.parse(data);
        case Commands.GET_CHANNEL:
            return GetChannelResponse.parse(data);
        case Commands.GET_CHANNELS:
            return GetChannelsResponse.parse(data);
        case Commands.GET_CHANNEL_PERMISSIONS:
            return GetChannelPermissionsResponse.parse(data);
        case Commands.GET_GUILD:
            return GetGuildResponse.parse(data);
        case Commands.GET_GUILDS:
            return GetGuildsResponse.parse(data);
        case Commands.GET_PLATFORM_BEHAVIORS:
            return GetPlatformBehaviorsResponse.parse(data);
        case Commands.GET_CHANNEL:
            return GetChannelResponse.parse(data);
        case Commands.SELECT_TEXT_CHANNEL:
            return SelectTextChannelResponse.parse(data);
        case Commands.SELECT_VOICE_CHANNEL:
            return SelectVoiceChannelResponse.parse(data);
        case Commands.SET_ACTIVITY:
            return SetActivityResponse.parse(data);
        case Commands.GET_SKUS_EMBEDDED:
            return GetSkusResponse.parse(data);
        case Commands.GET_ENTITLEMENTS_EMBEDDED:
            return GetEntitlementsResponse.parse(data);
        case Commands.SET_CONFIG:
            return SetConfigResponse.parse(data);
        case Commands.START_PURCHASE:
            return StartPurchaseResponse.parse(data);
        case Commands.SUBSCRIBE:
        case Commands.UNSUBSCRIBE:
            return SubscribeResponse.parse(data);
        case Commands.USER_SETTINGS_GET_LOCALE:
            return UserSettingsGetLocaleResponse.parse(data);
        case Commands.OPEN_EXTERNAL_LINK:
            return OpenExternalLinkResponse.parse(data);
        // Empty Responses
        case Commands.SET_ORIENTATION_LOCK_STATE:
        case Commands.SET_CERTIFIED_DEVICES:
        case Commands.SEND_ANALYTICS_EVENT:
        case Commands.OPEN_INVITE_DIALOG:
        case Commands.CAPTURE_LOG:
        case Commands.GET_SKUS:
        case Commands.GET_ENTITLEMENTS:
            return EmptyResponse.parse(data);
        // START-GENERATED-SECTION
        case Commands.AUTHENTICATE:
        case Commands.GET_ACTIVITY_INSTANCE_CONNECTED_PARTICIPANTS:
        case Commands.GET_QUEST:
        case Commands.GET_QUEST_ENROLLMENT_STATUS:
        case Commands.GET_RELATIONSHIPS:
        case Commands.GET_USER:
        case Commands.INITIATE_IMAGE_UPLOAD:
        case Commands.INVITE_USER_EMBEDDED:
        case Commands.OPEN_SHARE_MOMENT_DIALOG:
        case Commands.QUEST_START_TIMER:
        case Commands.REQUEST_PROXY_TICKET_REFRESH:
        case Commands.SHARE_INTERACTION:
        case Commands.SHARE_LINK:
            // END-GENERATED-SECTION
            const { response } = Schemas[cmd];
            return response.parse(data);
        default:
            assertUnreachable(cmd, new Error(`Unrecognized command ${cmd}`));
    }
}
function parseResponsePayload(payload) {
    return Object.assign(Object.assign({}, payload), { data: parseResponseData(payload) });
}

export { AuthorizeResponse, CaptureShortcutResponse, EmptyResponse, EncourageHardwareAccelerationResponse, GetChannelPermissionsResponse, GetChannelResponse, GetChannelsResponse, GetEntitlementsResponse, GetGuildResponse, GetGuildsResponse, GetPlatformBehaviorsResponse, GetSkusResponse, NullableChannelResponse, OpenExternalLinkResponse, ResponseFrame, SelectTextChannelResponse, SelectVoiceChannelResponse, SetActivityResponse, SetConfigResponse, StartPurchaseResponse, SubscribeResponse, UserSettingsGetLocaleResponse, VoiceSettingsResponse, parseResponsePayload };
