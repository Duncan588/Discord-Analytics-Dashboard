import { Command } from '../generated/schemas.mjs';
import { schemaCommandFactory } from '../utils/commandFactory.mjs';

/**
 * Opens a modal in the user's client to share the Activity link.
 *
 * @param {string} referrer_id
 * @param {string} custom_id
 * @param {string} message - message sent alongside link when shared.
 * @returns {Promise<{success: boolean>} whether or not the user shared the link to someone
 */
const shareLink = schemaCommandFactory(Command.SHARE_LINK);

export { shareLink };
