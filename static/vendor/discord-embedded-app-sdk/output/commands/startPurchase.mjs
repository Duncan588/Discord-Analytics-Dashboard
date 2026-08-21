import { Commands } from '../schema/common.mjs';
import { StartPurchaseResponse } from '../schema/responses.mjs';
import { commandFactory } from '../utils/commandFactory.mjs';

/**
 *
 */
const startPurchase = (sendCommand) => commandFactory(sendCommand, Commands.START_PURCHASE, StartPurchaseResponse);

export { startPurchase };
