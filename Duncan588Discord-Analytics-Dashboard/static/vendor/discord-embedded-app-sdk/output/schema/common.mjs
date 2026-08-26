import { object as objectType, string as stringType, unknown as unknownType, null as nullType, boolean as booleanType, number as numberType, array as arrayType, union as unionType } from '../lib/zod/lib/index.mjs';
import { zodCoerceUnhandledValue } from '../utils/zodUtils.mjs';
import { AuthenticateResponseSchema, GetRelationshipsResponseSchema } from '../generated/schemas.mjs';

// DISPATCH is sent as cmd but is a special case, so is excluded from Commands enum
const DISPATCH = 'DISPATCH';
var Commands;
(function (Commands) {
    Commands["AUTHORIZE"] = "AUTHORIZE";
    Commands["GET_GUILDS"] = "GET_GUILDS";
    Commands["GET_GUILD"] = "GET_GUILD";
    Commands["GET_CHANNEL"] = "GET_CHANNEL";
    Commands["GET_CHANNELS"] = "GET_CHANNELS";
    Commands["SELECT_VOICE_CHANNEL"] = "SELECT_VOICE_CHANNEL";
    Commands["SELECT_TEXT_CHANNEL"] = "SELECT_TEXT_CHANNEL";
    Commands["SUBSCRIBE"] = "SUBSCRIBE";
    Commands["UNSUBSCRIBE"] = "UNSUBSCRIBE";
    Commands["CAPTURE_SHORTCUT"] = "CAPTURE_SHORTCUT";
    Commands["SET_CERTIFIED_DEVICES"] = "SET_CERTIFIED_DEVICES";
    Commands["SET_ACTIVITY"] = "SET_ACTIVITY";
    Commands["GET_SKUS"] = "GET_SKUS";
    Commands["GET_ENTITLEMENTS"] = "GET_ENTITLEMENTS";
    Commands["GET_SKUS_EMBEDDED"] = "GET_SKUS_EMBEDDED";
    Commands["GET_ENTITLEMENTS_EMBEDDED"] = "GET_ENTITLEMENTS_EMBEDDED";
    Commands["START_PURCHASE"] = "START_PURCHASE";
    Commands["SET_CONFIG"] = "SET_CONFIG";
    Commands["SEND_ANALYTICS_EVENT"] = "SEND_ANALYTICS_EVENT";
    Commands["USER_SETTINGS_GET_LOCALE"] = "USER_SETTINGS_GET_LOCALE";
    Commands["OPEN_EXTERNAL_LINK"] = "OPEN_EXTERNAL_LINK";
    Commands["ENCOURAGE_HW_ACCELERATION"] = "ENCOURAGE_HW_ACCELERATION";
    Commands["CAPTURE_LOG"] = "CAPTURE_LOG";
    Commands["SET_ORIENTATION_LOCK_STATE"] = "SET_ORIENTATION_LOCK_STATE";
    Commands["OPEN_INVITE_DIALOG"] = "OPEN_INVITE_DIALOG";
    Commands["GET_PLATFORM_BEHAVIORS"] = "GET_PLATFORM_BEHAVIORS";
    Commands["GET_CHANNEL_PERMISSIONS"] = "GET_CHANNEL_PERMISSIONS";
    // START-GENERATED-SECTION
    Commands["AUTHENTICATE"] = "AUTHENTICATE";
    Commands["GET_ACTIVITY_INSTANCE_CONNECTED_PARTICIPANTS"] = "GET_ACTIVITY_INSTANCE_CONNECTED_PARTICIPANTS";
    Commands["GET_QUEST"] = "GET_QUEST";
    Commands["GET_QUEST_ENROLLMENT_STATUS"] = "GET_QUEST_ENROLLMENT_STATUS";
    Commands["GET_RELATIONSHIPS"] = "GET_RELATIONSHIPS";
    Commands["GET_USER"] = "GET_USER";
    Commands["INITIATE_IMAGE_UPLOAD"] = "INITIATE_IMAGE_UPLOAD";
    Commands["INVITE_USER_EMBEDDED"] = "INVITE_USER_EMBEDDED";
    Commands["OPEN_SHARE_MOMENT_DIALOG"] = "OPEN_SHARE_MOMENT_DIALOG";
    Commands["QUEST_START_TIMER"] = "QUEST_START_TIMER";
    Commands["REQUEST_PROXY_TICKET_REFRESH"] = "REQUEST_PROXY_TICKET_REFRESH";
    Commands["SHARE_INTERACTION"] = "SHARE_INTERACTION";
    Commands["SHARE_LINK"] = "SHARE_LINK";
    // END-GENERATED-SECTION
})(Commands || (Commands = {}));
const ReceiveFramePayload = objectType({
    cmd: stringType(),
    data: unknownType(),
    evt: nullType(),
    nonce: stringType(),
})
    .passthrough();
// TODO afgiel -- next breaking change release
// remove Scopes and ScopesObject in favor of Types.OAuthScopes
const ScopesObject = Object.assign(Object.assign({}, AuthenticateResponseSchema.shape.scopes.element.overlayType._def.innerType.options[0].Values), { UNHANDLED: -1 });
const Scopes = zodCoerceUnhandledValue(ScopesObject);
const Relationship = GetRelationshipsResponseSchema.shape.relationships.element;
const User = objectType({
    id: stringType(),
    username: stringType(),
    discriminator: stringType(),
    global_name: stringType().optional().nullable(),
    avatar: stringType().optional().nullable(),
    avatar_decoration_data: objectType({
        asset: stringType(),
        sku_id: stringType().optional(),
    })
        .nullable(),
    bot: booleanType(),
    flags: numberType().optional().nullable(),
    premium_type: numberType().optional().nullable(),
});
const GuildMember = objectType({
    user: User,
    nick: stringType().optional().nullable(),
    roles: arrayType(stringType()),
    joined_at: stringType(),
    deaf: booleanType(),
    mute: booleanType(),
});
const GuildMemberRPC = objectType({
    user_id: stringType(),
    nick: stringType().optional().nullable(),
    guild_id: stringType(),
    avatar: stringType().optional().nullable(),
    avatar_decoration_data: objectType({
        asset: stringType(),
        sku_id: stringType().optional().nullable(),
    })
        .optional()
        .nullable(),
    color_string: stringType().optional().nullable(),
});
const Emoji = objectType({
    id: stringType(),
    name: stringType().optional().nullable(),
    roles: arrayType(stringType()).optional().nullable(),
    user: User.optional().nullable(),
    require_colons: booleanType().optional().nullable(),
    managed: booleanType().optional().nullable(),
    animated: booleanType().optional().nullable(),
    available: booleanType().optional().nullable(),
});
const VoiceState = objectType({
    mute: booleanType(),
    deaf: booleanType(),
    self_mute: booleanType(),
    self_deaf: booleanType(),
    suppress: booleanType(),
});
const UserVoiceState = objectType({
    mute: booleanType(),
    nick: stringType(),
    user: User,
    voice_state: VoiceState,
    volume: numberType(),
});
const StatusObject = {
    UNHANDLED: -1,
    IDLE: 'idle',
    DND: 'dnd',
    ONLINE: 'online',
    OFFLINE: 'offline',
};
const Status = zodCoerceUnhandledValue(StatusObject);
const Activity = objectType({
    name: stringType(),
    type: numberType(),
    url: stringType().optional().nullable(),
    created_at: numberType().optional().nullable(),
    timestamps: objectType({
        start: numberType(),
        end: numberType(),
    })
        .partial()
        .optional()
        .nullable(),
    application_id: stringType().optional().nullable(),
    details: stringType().optional().nullable(),
    details_url: stringType().url().optional().nullable(),
    state: stringType().optional().nullable(),
    state_url: stringType().url().optional().nullable(),
    emoji: Emoji.optional().nullable(),
    party: objectType({
        id: stringType().optional().nullable(),
        size: arrayType(numberType()).optional().nullable(),
    })
        .optional()
        .nullable(),
    assets: objectType({
        large_image: stringType().nullable(),
        large_text: stringType().nullable(),
        large_url: stringType().url().optional().nullable(),
        small_image: stringType().nullable(),
        small_text: stringType().nullable(),
        small_url: stringType().url().optional().nullable(),
    })
        .partial()
        .optional()
        .nullable(),
    secrets: objectType({
        join: stringType(),
        match: stringType(),
    })
        .partial()
        .optional()
        .nullable(),
    instance: booleanType().optional().nullable(),
    flags: numberType().optional().nullable(),
});
const PermissionOverwriteTypeObject = {
    UNHANDLED: -1,
    ROLE: 0,
    MEMBER: 1,
};
const PermissionOverwrite = objectType({
    id: stringType(),
    type: zodCoerceUnhandledValue(PermissionOverwriteTypeObject),
    allow: stringType(),
    deny: stringType(),
});
const ChannelTypesObject = {
    UNHANDLED: -1,
    DM: 1,
    GROUP_DM: 3,
    GUILD_TEXT: 0,
    GUILD_VOICE: 2,
    GUILD_CATEGORY: 4,
    GUILD_ANNOUNCEMENT: 5,
    GUILD_STORE: 6,
    ANNOUNCEMENT_THREAD: 10,
    PUBLIC_THREAD: 11,
    PRIVATE_THREAD: 12,
    GUILD_STAGE_VOICE: 13,
    GUILD_DIRECTORY: 14,
    GUILD_FORUM: 15,
};
const Channel = objectType({
    id: stringType(),
    type: zodCoerceUnhandledValue(ChannelTypesObject),
    guild_id: stringType().optional().nullable(),
    position: numberType().optional().nullable(),
    permission_overwrites: arrayType(PermissionOverwrite).optional().nullable(),
    name: stringType().optional().nullable(),
    topic: stringType().optional().nullable(),
    nsfw: booleanType().optional().nullable(),
    last_message_id: stringType().optional().nullable(),
    bitrate: numberType().optional().nullable(),
    user_limit: numberType().optional().nullable(),
    rate_limit_per_user: numberType().optional().nullable(),
    recipients: arrayType(User).optional().nullable(),
    icon: stringType().optional().nullable(),
    owner_id: stringType().optional().nullable(),
    application_id: stringType().optional().nullable(),
    parent_id: stringType().optional().nullable(),
    last_pin_timestamp: stringType().optional().nullable(),
});
const PresenceUpdate = objectType({
    user: User,
    guild_id: stringType(),
    status: Status,
    activities: arrayType(Activity),
    client_status: objectType({
        desktop: Status,
        mobile: Status,
        web: Status,
    })
        .partial(),
});
const Role = objectType({
    id: stringType(),
    name: stringType(),
    color: numberType(),
    hoist: booleanType(),
    position: numberType(),
    permissions: stringType(),
    managed: booleanType(),
    mentionable: booleanType(),
});
const Guild = objectType({
    id: stringType(),
    name: stringType(),
    owner_id: stringType(),
    icon: stringType().nullable(),
    icon_hash: stringType().optional().nullable(),
    splash: stringType().nullable(),
    discovery_splash: stringType().nullable(),
    owner: booleanType().optional().nullable(),
    permissions: stringType().optional().nullable(),
    region: stringType(),
    afk_channel_id: stringType().nullable(),
    afk_timeout: numberType(),
    widget_enabled: booleanType().optional().nullable(),
    widget_channel_id: stringType().optional().nullable(),
    verification_level: numberType(),
    default_message_notifications: numberType(),
    explicit_content_filter: numberType(),
    roles: arrayType(Role),
    emojis: arrayType(Emoji),
    features: arrayType(stringType()),
    mfa_level: numberType(),
    application_id: stringType().nullable(),
    system_channel_id: stringType().nullable(),
    system_channel_flags: numberType(),
    rules_channel_id: stringType().nullable(),
    joined_at: stringType().optional().nullable(),
    large: booleanType().optional().nullable(),
    unavailable: booleanType().optional().nullable(),
    member_count: numberType().optional().nullable(),
    voice_states: arrayType(VoiceState).optional().nullable(),
    members: arrayType(GuildMember).optional().nullable(),
    channels: arrayType(Channel).optional().nullable(),
    presences: arrayType(PresenceUpdate).optional().nullable(),
    max_presences: numberType().optional().nullable(),
    max_members: numberType().optional().nullable(),
    vanity_url_code: stringType().nullable(),
    description: stringType().nullable(),
    banner: stringType().nullable(),
    premium_tier: numberType(),
    premium_subscription_count: numberType().optional().nullable(),
    preferred_locale: stringType(),
    public_updates_channel_id: stringType().nullable(),
    max_video_channel_users: numberType().optional().nullable(),
    approximate_member_count: numberType().optional().nullable(),
    approximate_presence_count: numberType().optional().nullable(),
});
const ChannelMention = objectType({
    id: stringType(),
    guild_id: stringType(),
    type: numberType(),
    name: stringType(),
});
const Attachment = objectType({
    id: stringType(),
    filename: stringType(),
    size: numberType(),
    url: stringType(),
    proxy_url: stringType(),
    height: numberType().optional().nullable(),
    width: numberType().optional().nullable(),
});
const EmbedFooter = objectType({
    text: stringType(),
    icon_url: stringType().optional().nullable(),
    proxy_icon_url: stringType().optional().nullable(),
});
const Image = objectType({
    url: stringType().optional().nullable(),
    proxy_url: stringType().optional().nullable(),
    height: numberType().optional().nullable(),
    width: numberType().optional().nullable(),
});
const Video = Image.omit({ proxy_url: true });
const EmbedProvider = objectType({
    name: stringType().optional().nullable(),
    url: stringType().optional().nullable(),
});
const EmbedAuthor = objectType({
    name: stringType().optional().nullable(),
    url: stringType().optional().nullable(),
    icon_url: stringType().optional().nullable(),
    proxy_icon_url: stringType().optional().nullable(),
});
const EmbedField = objectType({
    name: stringType(),
    value: stringType(),
    inline: booleanType(),
});
const Embed = objectType({
    title: stringType().optional().nullable(),
    type: stringType().optional().nullable(),
    description: stringType().optional().nullable(),
    url: stringType().optional().nullable(),
    timestamp: stringType().optional().nullable(),
    color: numberType().optional().nullable(),
    footer: EmbedFooter.optional().nullable(),
    image: Image.optional().nullable(),
    thumbnail: Image.optional().nullable(),
    video: Video.optional().nullable(),
    provider: EmbedProvider.optional().nullable(),
    author: EmbedAuthor.optional().nullable(),
    fields: arrayType(EmbedField).optional().nullable(),
});
const Reaction = objectType({
    count: numberType(),
    me: booleanType(),
    emoji: Emoji,
});
const MessageActivity = objectType({
    type: numberType(),
    party_id: stringType().optional().nullable(),
});
const MessageApplication = objectType({
    id: stringType(),
    cover_image: stringType().optional().nullable(),
    description: stringType(),
    icon: stringType().optional().nullable(),
    name: stringType(),
});
const MessageReference = objectType({
    message_id: stringType().optional().nullable(),
    channel_id: stringType().optional().nullable(),
    guild_id: stringType().optional().nullable(),
});
const Message = objectType({
    id: stringType(),
    channel_id: stringType(),
    guild_id: stringType().optional().nullable(),
    author: User.optional().nullable(),
    member: GuildMember.optional().nullable(),
    content: stringType(),
    timestamp: stringType(),
    edited_timestamp: stringType().optional().nullable(),
    tts: booleanType(),
    mention_everyone: booleanType(),
    mentions: arrayType(User),
    mention_roles: arrayType(stringType()),
    mention_channels: arrayType(ChannelMention),
    attachments: arrayType(Attachment),
    embeds: arrayType(Embed),
    reactions: arrayType(Reaction).optional().nullable(),
    nonce: unionType([stringType(), numberType()]).optional().nullable(),
    pinned: booleanType(),
    webhook_id: stringType().optional().nullable(),
    type: numberType(),
    activity: MessageActivity.optional().nullable(),
    application: MessageApplication.optional().nullable(),
    message_reference: MessageReference.optional().nullable(),
    flags: numberType().optional().nullable(),
    stickers: arrayType(unknownType()).optional().nullable(),
    // Cannot self reference, but this is possibly a Message
    referenced_message: unknownType().optional().nullable(),
});
const VoiceDevice = objectType({
    id: stringType(),
    name: stringType(),
});
const KeyTypesObject = {
    UNHANDLED: -1,
    KEYBOARD_KEY: 0,
    MOUSE_BUTTON: 1,
    KEYBOARD_MODIFIER_KEY: 2,
    GAMEPAD_BUTTON: 3,
};
const ShortcutKey = objectType({
    type: zodCoerceUnhandledValue(KeyTypesObject),
    code: numberType(),
    name: stringType(),
});
const VoiceSettingModeTypeObject = {
    UNHANDLED: -1,
    PUSH_TO_TALK: 'PUSH_TO_TALK',
    VOICE_ACTIVITY: 'VOICE_ACTIVITY',
};
const VoiceSettingsMode = objectType({
    type: zodCoerceUnhandledValue(VoiceSettingModeTypeObject),
    auto_threshold: booleanType(),
    threshold: numberType(),
    shortcut: arrayType(ShortcutKey),
    delay: numberType(),
});
const VoiceSettingsIO = objectType({
    device_id: stringType(),
    volume: numberType(),
    available_devices: arrayType(VoiceDevice),
});
const CertifiedDeviceTypeObject = {
    UNHANDLED: -1,
    AUDIO_INPUT: 'AUDIO_INPUT',
    AUDIO_OUTPUT: 'AUDIO_OUTPUT',
    VIDEO_INPUT: 'VIDEO_INPUT',
};
const CertifiedDevice = objectType({
    type: zodCoerceUnhandledValue(CertifiedDeviceTypeObject),
    id: stringType(),
    vendor: objectType({
        name: stringType(),
        url: stringType(),
    }),
    model: objectType({
        name: stringType(),
        url: stringType(),
    }),
    related: arrayType(stringType()),
    echo_cancellation: booleanType().optional().nullable(),
    noise_suppression: booleanType().optional().nullable(),
    automatic_gain_control: booleanType().optional().nullable(),
    hardware_mute: booleanType().optional().nullable(),
});
const SkuTypeObject = {
    UNHANDLED: -1,
    APPLICATION: 1,
    DLC: 2,
    CONSUMABLE: 3,
    BUNDLE: 4,
    SUBSCRIPTION: 5,
};
const Sku = objectType({
    id: stringType(),
    name: stringType(),
    type: zodCoerceUnhandledValue(SkuTypeObject),
    price: objectType({
        amount: numberType(),
        currency: stringType(),
    }),
    application_id: stringType(),
    flags: numberType(),
    release_date: stringType().nullable(),
});
const EntitlementTypesObject = {
    UNHANDLED: -1,
    PURCHASE: 1,
    PREMIUM_SUBSCRIPTION: 2,
    DEVELOPER_GIFT: 3,
    TEST_MODE_PURCHASE: 4,
    FREE_PURCHASE: 5,
    USER_GIFT: 6,
    PREMIUM_PURCHASE: 7,
};
const Entitlement = objectType({
    id: stringType(),
    sku_id: stringType(),
    application_id: stringType(),
    user_id: stringType(),
    gift_code_flags: numberType(),
    type: zodCoerceUnhandledValue(EntitlementTypesObject),
    gifter_user_id: stringType().optional().nullable(),
    branches: arrayType(stringType()).optional().nullable(),
    starts_at: stringType().optional().nullable(), // ISO string
    ends_at: stringType().optional().nullable(), // ISO string
    parent_id: stringType().optional().nullable(),
    consumed: booleanType().optional().nullable(),
    deleted: booleanType().optional().nullable(),
    gift_code_batch_id: stringType().optional().nullable(),
});
const OrientationLockStateTypeObject = {
    UNHANDLED: -1,
    UNLOCKED: 1,
    PORTRAIT: 2,
    LANDSCAPE: 3,
};
const OrientationLockState = zodCoerceUnhandledValue(OrientationLockStateTypeObject);
const ThermalStateTypeObject = {
    UNHANDLED: -1,
    NOMINAL: 0,
    FAIR: 1,
    SERIOUS: 2,
    CRITICAL: 3,
};
const ThermalState = zodCoerceUnhandledValue(ThermalStateTypeObject);
const OrientationTypeObject = {
    UNHANDLED: -1,
    PORTRAIT: 0,
    LANDSCAPE: 1,
};
const Orientation = zodCoerceUnhandledValue(OrientationTypeObject);
const LayoutModeTypeObject = {
    UNHANDLED: -1,
    FOCUSED: 0,
    PIP: 1,
    GRID: 2,
};
const LayoutMode = zodCoerceUnhandledValue(LayoutModeTypeObject);

export { Activity, Attachment, CertifiedDevice, CertifiedDeviceTypeObject, Channel, ChannelMention, ChannelTypesObject, Commands, DISPATCH, Embed, EmbedAuthor, EmbedField, EmbedFooter, EmbedProvider, Emoji, Entitlement, EntitlementTypesObject, Guild, GuildMember, GuildMemberRPC, Image, KeyTypesObject, LayoutMode, LayoutModeTypeObject, Message, MessageActivity, MessageApplication, MessageReference, Orientation, OrientationLockState, OrientationLockStateTypeObject, OrientationTypeObject, PermissionOverwrite, PermissionOverwriteTypeObject, PresenceUpdate, Reaction, ReceiveFramePayload, Relationship, Role, Scopes, ScopesObject, ShortcutKey, Sku, SkuTypeObject, Status, StatusObject, ThermalState, ThermalStateTypeObject, User, UserVoiceState, Video, VoiceDevice, VoiceSettingModeTypeObject, VoiceSettingsIO, VoiceSettingsMode, VoiceState };
