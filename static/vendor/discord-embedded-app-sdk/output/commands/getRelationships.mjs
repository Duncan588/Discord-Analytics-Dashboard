import { Command } from '../generated/schemas.mjs';
import { schemaCommandFactory } from '../utils/commandFactory.mjs';

const getRelationships = schemaCommandFactory(Command.GET_RELATIONSHIPS);

export { getRelationships };
