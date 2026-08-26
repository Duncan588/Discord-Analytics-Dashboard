import { Commands } from '../schema/common.mjs';
import { AuthorizeResponse } from '../schema/responses.mjs';
import { commandFactory } from '../utils/commandFactory.mjs';

/**
 * Should be called directly after a `ready` payload is received from the
 * Discord client. It includes a list of all scopes that your activity would
 * like to be authorized to use. If the user does not yet have a valid token
 * for all scopes requested, this command will open an OAuth modal. Once an
 * authorized token is available, it will be returned in the command response.
 */
const authorize = (sendCommand) => commandFactory(sendCommand, Commands.AUTHORIZE, AuthorizeResponse);

export { authorize };
