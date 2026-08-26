import { nativeEnum as nativeEnumType, string as stringType, literal as literalType, object as objectType, number as numberType, union as unionType, boolean as booleanType } from '../lib/zod/lib/index.mjs';
import { Orientation } from '../Constants.mjs';
import { ReceiveFramePayload, DISPATCH, Commands, UserVoiceState, User, GuildMemberRPC, Entitlement, ThermalState, Relationship, LayoutModeTypeObject, OrientationTypeObject } from './common.mjs';
import { zodCoerceUnhandledValue } from '../utils/zodUtils.mjs';
import { GetActivityInstanceConnectedParticipantsResponseSchema } from '../generated/schemas.mjs';

// ERROR is sent as evt but is a special case, so is excluded from Events enum
const ERROR = 'ERROR';
var Events;
(function (Events) {
    Events["READY"] = "READY";
    Events["VOICE_STATE_UPDATE"] = "VOICE_STATE_UPDATE";
    Events["SPEAKING_START"] = "SPEAKING_START";
    Events["SPEAKING_STOP"] = "SPEAKING_STOP";
    Events["ACTIVITY_LAYOUT_MODE_UPDATE"] = "ACTIVITY_LAYOUT_MODE_UPDATE";
    Events["ORIENTATION_UPDATE"] = "ORIENTATION_UPDATE";
    Events["CURRENT_USER_UPDATE"] = "CURRENT_USER_UPDATE";
    Events["CURRENT_GUILD_MEMBER_UPDATE"] = "CURRENT_GUILD_MEMBER_UPDATE";
    Events["ENTITLEMENT_CREATE"] = "ENTITLEMENT_CREATE";
    Events["THERMAL_STATE_UPDATE"] = "THERMAL_STATE_UPDATE";
    Events["ACTIVITY_INSTANCE_PARTICIPANTS_UPDATE"] = "ACTIVITY_INSTANCE_PARTICIPANTS_UPDATE";
    Events["RELATIONSHIP_UPDATE"] = "RELATIONSHIP_UPDATE";
    Events["ACTIVITY_JOIN"] = "ACTIVITY_JOIN";
    Events["QUEST_ENROLLMENT_STATUS_UPDATE"] = "QUEST_ENROLLMENT_STATUS_UPDATE";
})(Events || (Events = {}));
const DispatchEventFrame = ReceiveFramePayload.extend({
    evt: nativeEnumType(Events),
    nonce: stringType().nullable(),
    cmd: literalType(DISPATCH),
    data: objectType({}).passthrough(),
});
const ErrorEvent = ReceiveFramePayload.extend({
    evt: literalType(ERROR),
    data: objectType({
        code: numberType(),
        message: stringType().optional(),
    })
        .passthrough(),
    cmd: nativeEnumType(Commands),
    nonce: stringType().nullable(),
});
const OtherEvent = DispatchEventFrame.extend({
    evt: stringType(),
});
const EventFrame = unionType([DispatchEventFrame, OtherEvent, ErrorEvent]);
function parseEventPayload(data) {
    const event = data.evt;
    if (!(event in Events)) {
        throw new Error(`Unrecognized event type ${data.evt}`);
    }
    const eventSchema = EventSchema[event];
    return eventSchema.payload.parse(data);
}
const EventSchema = {
    /**
     * @description
     * The READY event is emitted by Discord's RPC server in reply to a client
     * initiating the RPC handshake. The event includes information about
     * - the rpc server version
     * - the discord client configuration
     * - the (basic) user object
     *
     * Unlike other events, READY will only be omitted once, immediately after the
     * Embedded App SDK is initialized
     *
     * # Supported Platforms
     * | Web | iOS | Android |
     * |-----|-----|---------|
     * | ✅  | ✅  | ✅      |
     *
     * Required scopes: []
     *
     */
    [Events.READY]: {
        payload: DispatchEventFrame.extend({
            evt: literalType(Events.READY),
            data: objectType({
                v: numberType(),
                config: objectType({
                    cdn_host: stringType().optional(),
                    api_endpoint: stringType(),
                    environment: stringType(),
                }),
                user: objectType({
                    id: stringType(),
                    username: stringType(),
                    discriminator: stringType(),
                    avatar: stringType().optional(),
                })
                    .optional(),
            }),
        }),
    },
    [Events.VOICE_STATE_UPDATE]: {
        payload: DispatchEventFrame.extend({
            evt: literalType(Events.VOICE_STATE_UPDATE),
            data: UserVoiceState,
        }),
        subscribeArgs: objectType({
            channel_id: stringType(),
        }),
    },
    [Events.SPEAKING_START]: {
        payload: DispatchEventFrame.extend({
            evt: literalType(Events.SPEAKING_START),
            data: objectType({
                lobby_id: stringType().optional(),
                channel_id: stringType().optional(),
                user_id: stringType(),
            }),
        }),
        subscribeArgs: objectType({
            lobby_id: stringType().nullable().optional(),
            channel_id: stringType().nullable().optional(),
        }),
    },
    [Events.SPEAKING_STOP]: {
        payload: DispatchEventFrame.extend({
            evt: literalType(Events.SPEAKING_STOP),
            data: objectType({
                lobby_id: stringType().optional(),
                channel_id: stringType().optional(),
                user_id: stringType(),
            }),
        }),
        subscribeArgs: objectType({
            lobby_id: stringType().nullable().optional(),
            channel_id: stringType().nullable().optional(),
        }),
    },
    [Events.ACTIVITY_LAYOUT_MODE_UPDATE]: {
        payload: DispatchEventFrame.extend({
            evt: literalType(Events.ACTIVITY_LAYOUT_MODE_UPDATE),
            data: objectType({
                layout_mode: zodCoerceUnhandledValue(LayoutModeTypeObject),
            }),
        }),
    },
    [Events.ORIENTATION_UPDATE]: {
        payload: DispatchEventFrame.extend({
            evt: literalType(Events.ORIENTATION_UPDATE),
            data: objectType({
                screen_orientation: zodCoerceUnhandledValue(OrientationTypeObject),
                /**
                 * @deprecated use screen_orientation instead
                 */
                orientation: nativeEnumType(Orientation),
            }),
        }),
    },
    [Events.CURRENT_USER_UPDATE]: {
        payload: DispatchEventFrame.extend({
            evt: literalType(Events.CURRENT_USER_UPDATE),
            data: User,
        }),
    },
    [Events.CURRENT_GUILD_MEMBER_UPDATE]: {
        payload: DispatchEventFrame.extend({
            evt: literalType(Events.CURRENT_GUILD_MEMBER_UPDATE),
            data: GuildMemberRPC,
        }),
        subscribeArgs: objectType({
            guild_id: stringType(),
        }),
    },
    [Events.ENTITLEMENT_CREATE]: {
        payload: DispatchEventFrame.extend({
            evt: literalType(Events.ENTITLEMENT_CREATE),
            data: objectType({ entitlement: Entitlement }),
        }),
    },
    [Events.THERMAL_STATE_UPDATE]: {
        payload: DispatchEventFrame.extend({
            evt: literalType(Events.THERMAL_STATE_UPDATE),
            data: objectType({ thermal_state: ThermalState }),
        }),
    },
    [Events.ACTIVITY_INSTANCE_PARTICIPANTS_UPDATE]: {
        payload: DispatchEventFrame.extend({
            evt: literalType(Events.ACTIVITY_INSTANCE_PARTICIPANTS_UPDATE),
            data: objectType({
                participants: GetActivityInstanceConnectedParticipantsResponseSchema.shape.participants,
            }),
        }),
    },
    [Events.RELATIONSHIP_UPDATE]: {
        payload: DispatchEventFrame.extend({
            evt: literalType(Events.RELATIONSHIP_UPDATE),
            data: Relationship,
        }),
    },
    [Events.ACTIVITY_JOIN]: {
        payload: DispatchEventFrame.extend({
            evt: literalType(Events.ACTIVITY_JOIN),
            data: objectType({
                applicationId: stringType(),
                secret: stringType(),
            }),
        }),
    },
    [Events.QUEST_ENROLLMENT_STATUS_UPDATE]: {
        payload: DispatchEventFrame.extend({
            evt: literalType(Events.QUEST_ENROLLMENT_STATUS_UPDATE),
            data: objectType({
                quest_id: stringType(),
                is_enrolled: booleanType(),
                enrolled_at: stringType().date(),
            }),
        }),
    },
};

export { DispatchEventFrame, ERROR, ErrorEvent, EventFrame, EventSchema, Events, OtherEvent, parseEventPayload };
