import { Commands } from '../schema/common.mjs';
import { GetEntitlementsResponse } from '../schema/responses.mjs';
import { commandFactory } from '../utils/commandFactory.mjs';

/**
 *
 */
const getEntitlements = (sendCommand) => commandFactory(sendCommand, Commands.GET_ENTITLEMENTS_EMBEDDED, GetEntitlementsResponse);

export { getEntitlements };
