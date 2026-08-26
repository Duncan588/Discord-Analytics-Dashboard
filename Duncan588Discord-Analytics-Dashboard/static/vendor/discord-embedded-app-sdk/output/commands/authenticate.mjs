import { Command } from '../generated/schemas.mjs';
import { schemaCommandFactory } from '../utils/commandFactory.mjs';

/**
 * Authenticate Activity
 */
const authenticate = schemaCommandFactory(Command.AUTHENTICATE);

export { authenticate };
