import { literal as literalType } from '../lib/zod/lib/index.mjs';
import { RPCErrorCodes } from '../Constants.mjs';
import { ReceiveFramePayload } from '../schema/common.mjs';

/**
 * @args - the primary args to send with the command.
 * @fallbackArgs - the args to try the command with in the case where an old Discord
 *  client doesn't support one of the new args.
 */
function compatCommandFactory({ sendCommand, cmd, response, fallbackTransform, transferTransform = () => undefined, }) {
    const payload = ReceiveFramePayload.extend({
        cmd: literalType(cmd),
        data: response,
    });
    return async (args) => {
        try {
            const reply = await sendCommand({ cmd, args, transfer: transferTransform(args) });
            const parsed = payload.parse(reply);
            return parsed.data;
        }
        catch (error) {
            if (error.code === RPCErrorCodes.INVALID_PAYLOAD) {
                const fallbackArgs = fallbackTransform(args);
                const reply = await sendCommand({ cmd, args: fallbackArgs, transfer: transferTransform(fallbackArgs) });
                const parsed = payload.parse(reply);
                return parsed.data;
            }
            else {
                throw error;
            }
        }
    };
}

export { compatCommandFactory as default };
