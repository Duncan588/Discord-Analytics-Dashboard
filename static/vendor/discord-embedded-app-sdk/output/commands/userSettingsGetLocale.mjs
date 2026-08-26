import { Commands } from '../schema/common.mjs';
import { UserSettingsGetLocaleResponse } from '../schema/responses.mjs';
import { commandFactory } from '../utils/commandFactory.mjs';

/**
 *
 */
const userSettingsGetLocale = (sendCommand) => commandFactory(sendCommand, Commands.USER_SETTINGS_GET_LOCALE, UserSettingsGetLocaleResponse);

export { userSettingsGetLocale };
