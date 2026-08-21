import { Command } from '../generated/schemas.mjs';
import { schemaCommandFactory } from '../utils/commandFactory.mjs';

const getQuest = schemaCommandFactory(Command.GET_QUEST);

export { getQuest };
