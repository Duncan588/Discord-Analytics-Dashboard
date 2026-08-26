import { Command } from '../generated/schemas.mjs';
import { schemaCommandFactory } from '../utils/commandFactory.mjs';

const getUser = schemaCommandFactory(Command.GET_USER);

export { getUser };
