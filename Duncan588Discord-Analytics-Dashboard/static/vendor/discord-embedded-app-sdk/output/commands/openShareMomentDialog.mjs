import { Command } from '../generated/schemas.mjs';
import { schemaCommandFactory } from '../utils/commandFactory.mjs';

/**
 * Opens the Share Moment Dialog in the user's client, allowing them to share the media at mediaUrl as a message.
 *
 * @param {string} mediaUrl - a Discord CDN URL for the piece of media to be shared.
 * @returns {Promise<void>}
 */
const openShareMomentDialog = schemaCommandFactory(Command.OPEN_SHARE_MOMENT_DIALOG);

export { openShareMomentDialog };
