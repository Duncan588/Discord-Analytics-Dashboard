import { Commands } from '../schema/common.mjs';
import { SetConfigResponse } from '../schema/responses.mjs';
import { commandFactory } from '../utils/commandFactory.mjs';

/**
 *
 */
const setConfig = (sendCommand) => commandFactory(sendCommand, Commands.SET_CONFIG, SetConfigResponse);

export { setConfig };
