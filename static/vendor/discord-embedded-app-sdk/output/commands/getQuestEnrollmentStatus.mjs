import { Command } from '../generated/schemas.mjs';
import { schemaCommandFactory } from '../utils/commandFactory.mjs';

const getQuestEnrollmentStatus = schemaCommandFactory(Command.GET_QUEST_ENROLLMENT_STATUS);

export { getQuestEnrollmentStatus };
