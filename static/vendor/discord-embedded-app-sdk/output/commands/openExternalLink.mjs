import { Commands } from '../schema/common.mjs';
import { OpenExternalLinkResponse } from '../schema/responses.mjs';
import { commandFactory } from '../utils/commandFactory.mjs';

/**
 *
 */
const openExternalLink = (sendCommand) => commandFactory(sendCommand, Commands.OPEN_EXTERNAL_LINK, OpenExternalLinkResponse);

export { openExternalLink };
