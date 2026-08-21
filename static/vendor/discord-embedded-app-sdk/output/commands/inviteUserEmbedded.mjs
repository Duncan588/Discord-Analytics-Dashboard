import { Command } from '../generated/schemas.mjs';
import { schemaCommandFactory } from '../utils/commandFactory.mjs';

const inviteUserEmbedded = schemaCommandFactory(Command.INVITE_USER_EMBEDDED);

export { inviteUserEmbedded };
