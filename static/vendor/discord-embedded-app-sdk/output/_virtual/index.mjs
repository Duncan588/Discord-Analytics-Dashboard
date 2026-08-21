import { getDefaultExportFromCjs } from './_commonjsHelpers.mjs';
import { __require as requireEventemitter3 } from '../lib/eventemitter3/index2.mjs';

var eventemitter3Exports = requireEventemitter3();
var EventEmitter = /*@__PURE__*/getDefaultExportFromCjs(eventemitter3Exports);

export { EventEmitter as default };
