import { Activity, Commands } from '../schema/common.mjs';
import { SetActivityResponse } from '../schema/responses.mjs';
import { commandFactory } from '../utils/commandFactory.mjs';

Activity.pick({
    state: true,
    state_url: true,
    details: true,
    details_url: true,
    timestamps: true,
    assets: true,
    party: true,
    secrets: true,
    instance: true,
    type: true,
})
    .extend({
    type: Activity.shape.type.optional(),
    instance: Activity.shape.instance.optional(),
})
    .nullable();
/**
 *
 * @description
 * RPC documentation here: https://discord.com/developers/docs/topics/rpc#setactivity
 * Calling setActivity allows modifying how your activity's rich presence is displayed in the Discord App
 *
 * Supported Platforms
 * | Web | iOS | Android |
 * |-----|-----|---------|
 * | ✅  | ✅  | ✅      |
 *
 * Required scopes: [rpc.activities.write]
 *
 * @example
 * await discordSdk.commands.setActivity({
 *   activity: {
 *     type: 0,
 *     details: 'Details',
 *     state: 'Playing',
 *   },
 * });
 */
const setActivity = (sendCommand) => commandFactory(sendCommand, Commands.SET_ACTIVITY, SetActivityResponse);

export { setActivity };
