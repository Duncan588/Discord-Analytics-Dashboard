import { Command } from '../generated/schemas.mjs';
import { schemaCommandFactory } from '../utils/commandFactory.mjs';

const shareInteraction = schemaCommandFactory(Command.SHARE_INTERACTION);

export { shareInteraction };
