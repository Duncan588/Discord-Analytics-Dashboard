import { literal as literalType } from '../lib/zod/lib/index.mjs';
import { ReceiveFramePayload } from '../schema/common.mjs';
import { Schemas } from '../generated/schemas.mjs';

function commandFactory(sendCommand, cmd, response, transferTransform = () => undefined) {
    const payload = ReceiveFramePayload.extend({
        cmd: literalType(cmd),
        data: response,
    });
    return async (args) => {
        const reply = await sendCommand({ cmd, args, transfer: transferTransform(args) });
        const parsed = payload.parse(reply);
        return parsed.data;
    };
}
function schemaCommandFactory(cmd, transferTransform = () => undefined) {
    const response = Schemas[cmd].response;
    const payload = ReceiveFramePayload.extend({
        cmd: literalType(cmd),
        data: response,
    });
    return (sendCommand) => async (args) => {
        const reply = await sendCommand({
            // @ts-expect-error - Merge commands
            cmd: cmd,
            args,
            transfer: transferTransform(args),
        });
        const parsed = payload.parse(reply);
        return parsed.data;
    };
}

export { commandFactory, schemaCommandFactory };
