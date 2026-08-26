import { Commands } from '../schema/common.mjs';
import { EmptyResponse } from '../schema/responses.mjs';
import { commandFactory } from '../utils/commandFactory.mjs';

/**
 *
 */
const captureLog = (sendCommand) => commandFactory(sendCommand, Commands.CAPTURE_LOG, EmptyResponse);

export { captureLog };
