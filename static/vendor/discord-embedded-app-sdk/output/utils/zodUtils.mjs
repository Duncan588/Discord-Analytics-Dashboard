import { preprocess as preprocessType, string as stringType, number as numberType, custom } from '../lib/zod/lib/index.mjs';

/**
 * This is a helper function which coerces an unsupported arg value to the key/value UNHANDLED: -1
 * This is necessary to handle a scenario where a new enum value is added in the Discord Client,
 * so that the sdk will not throw an error when given a (newly) valid enum value.
 *
 * To remove the requirement for consumers of this sdk to import an enum when parsing data,
 * we instead use an object cast as const (readonly). This maintains parity with the previous
 * schema (which used zod.enum), and behaves more like a union type, i.e. 'foo' | 'bar' | -1
 *
 * @param inputObject This object must include the key/value pair UNHANDLED = -1
 */
function zodCoerceUnhandledValue(inputObject) {
    return preprocessType((arg) => {
        var _a;
        const [objectKey] = (_a = Object.entries(inputObject).find(([, value]) => value === arg)) !== null && _a !== void 0 ? _a : [];
        if (arg != null && objectKey === undefined) {
            return inputObject.UNHANDLED;
        }
        return arg;
    }, stringType().or(numberType()));
}
/**
 * Fallback to the default zod value if parsing fails.
 */
function fallbackToDefault(schema) {
    const transform = custom().transform((data) => {
        const res = schema.safeParse(data);
        if (res.success) {
            return res.data;
        }
        return schema._def.defaultValue();
    });
    // Must set this inner schema so inspection works correctly
    transform.overlayType = schema;
    // transform._def.schema = schema;
    return transform;
}

export { fallbackToDefault, zodCoerceUnhandledValue };
