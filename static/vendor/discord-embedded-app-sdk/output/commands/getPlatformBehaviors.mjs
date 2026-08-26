import { Commands } from '../schema/common.mjs';
import { GetPlatformBehaviorsResponse } from '../schema/responses.mjs';
import { commandFactory } from '../utils/commandFactory.mjs';

/**
 * Returns an object with information about platform behaviors
 * This command can be utilized to inform and react to a breaking change in platform behavior
 *
 * @returns {GetPlatformBehaviorsPayload} payload - The command return value
 * @returns {boolean} payload.data.iosKeyboardResizesView - If on iOS the webview is resized when the keyboard is opened
 */
const getPlatformBehaviors = (sendCommand) => commandFactory(sendCommand, Commands.GET_PLATFORM_BEHAVIORS, GetPlatformBehaviorsResponse);

export { getPlatformBehaviors };
