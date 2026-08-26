import { Commands } from '../schema/common.mjs';
import { EncourageHardwareAccelerationResponse } from '../schema/responses.mjs';
import { commandFactory } from '../utils/commandFactory.mjs';

/**
 *
 */
const encourageHardwareAcceleration = (sendCommand) => commandFactory(sendCommand, Commands.ENCOURAGE_HW_ACCELERATION, EncourageHardwareAccelerationResponse);

export { encourageHardwareAcceleration };
