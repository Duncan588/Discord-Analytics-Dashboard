import { getDefaultExportFromCjs } from './_commonjsHelpers.mjs';
import { __require as requireBigInteger } from '../lib/big-integer/BigInteger.mjs';

var BigIntegerExports = requireBigInteger();
var bigInt = /*@__PURE__*/getDefaultExportFromCjs(BigIntegerExports);

export { bigInt as default };
