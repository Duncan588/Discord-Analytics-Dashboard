import { Commands } from '../schema/common.mjs';
import { GetChannelPermissionsResponse } from '../schema/responses.mjs';
import { commandFactory } from '../utils/commandFactory.mjs';

/**
 * Returns a bigint representing Permissions for the current user in the currently connected channel. Use PermissionsUtils to calculate if a user has a particular permission.
 * Always returns `0n` (no valid permissions) in a (G)DM context, so is unnecessary to call when discordSdk.guildId == null.
 */
const getChannelPermissions = (sendCommand) => commandFactory(sendCommand, Commands.GET_CHANNEL_PERMISSIONS, GetChannelPermissionsResponse);

export { getChannelPermissions };
