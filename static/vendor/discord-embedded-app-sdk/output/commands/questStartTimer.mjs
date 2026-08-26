import { Command } from '../generated/schemas.mjs';
import { schemaCommandFactory } from '../utils/commandFactory.mjs';

const questStartTimer = schemaCommandFactory(Command.QUEST_START_TIMER);

export { questStartTimer };
