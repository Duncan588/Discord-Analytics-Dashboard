import { Commands } from '../schema/common.mjs';
import { GetSkusResponse } from '../schema/responses.mjs';
import { commandFactory } from '../utils/commandFactory.mjs';

/*
 *
 */
const getSkus = (sendCommand) => commandFactory(sendCommand, Commands.GET_SKUS_EMBEDDED, GetSkusResponse);

export { getSkus };
