import { Commands } from '../schema/common.mjs';
import { EmptyResponse } from '../schema/responses.mjs';
import compatCommandFactory from '../utils/compatCommandFactory.mjs';

const fallbackTransform = (args) => {
    return {
        lock_state: args.lock_state,
        picture_in_picture_lock_state: args.picture_in_picture_lock_state,
    };
};
const setOrientationLockState = (sendCommand) => compatCommandFactory({
    sendCommand,
    cmd: Commands.SET_ORIENTATION_LOCK_STATE,
    response: EmptyResponse,
    fallbackTransform,
});

export { setOrientationLockState };
