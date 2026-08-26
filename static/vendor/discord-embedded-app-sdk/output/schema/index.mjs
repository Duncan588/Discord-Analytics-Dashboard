import { object as objectType, string as stringType, nativeEnum as nativeEnumType, literal as literalType, number as numberType, unknown as unknownType } from '../lib/zod/lib/index.mjs';
import { ERROR, ErrorEvent, parseEventPayload, EventFrame } from './events.mjs';
import { parseResponsePayload, ResponseFrame } from './responses.mjs';
import * as responses from './responses.mjs';
export { responses as Responses };
import * as common from './common.mjs';
export { common as Common };
import { Platform } from '../Constants.mjs';

objectType({
    frame_id: stringType(),
    platform: nativeEnumType(Platform).optional().nullable(),
});
objectType({
    v: literalType(1),
    encoding: literalType('json').optional(),
    client_id: stringType(),
    frame_id: stringType(),
});
const ClosePayload = objectType({
    code: numberType(),
    message: stringType().optional(),
});
const IncomingPayload = objectType({
    evt: stringType().nullable(),
    nonce: stringType().nullable(),
    data: unknownType().nullable(),
    cmd: stringType(),
})
    .passthrough();
function parseIncomingPayload(payload) {
    const incoming = IncomingPayload.parse(payload);
    if (incoming.evt != null) {
        if (incoming.evt === ERROR) {
            return ErrorEvent.parse(incoming);
        }
        return parseEventPayload(EventFrame.parse(incoming));
    }
    else {
        return parseResponsePayload(ResponseFrame.passthrough().parse(incoming));
    }
}

export { ClosePayload, IncomingPayload, parseIncomingPayload };
