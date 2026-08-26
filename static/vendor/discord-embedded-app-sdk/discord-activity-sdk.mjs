/*
 * Minimal Discord Embedded App transport used by this site.
 *
 * The official SDK entry point imports the complete command/schema tree. In a
 * Discord Activity that becomes dozens of module requests through Discord's
 * proxy before authorize() can even be sent. This file keeps the same RPC
 * handshake and the two commands used by the site, but ships as one small
 * module. It is intentionally limited to Activity authentication.
 */
const OPCODE_HANDSHAKE = 0;
const OPCODE_FRAME = 1;
const DISPATCH = "DISPATCH";
const ERROR = "ERROR";
const SDK_VERSION = "2.5.0";

function nonce() {
  if (globalThis.crypto && typeof globalThis.crypto.randomUUID === "function") {
    return globalThis.crypto.randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

function rpcSource() {
  const source = window.parent && window.parent.opener
    ? window.parent.opener
    : window.parent;
  return [source, document.referrer || "*"];
}

class DiscordSDK {
  constructor(clientId) {
    const params = new URLSearchParams(window.location.search);
    this.frameId = params.get("frame_id");
    this.instanceId = params.get("instance_id");
    this.platform = params.get("platform");
    if (!this.frameId || !this.instanceId || !this.platform) {
      throw new Error("Discord Activity 参数不完整");
    }
    this.clientId = clientId;
    this.pending = new Map();
    this.readyPromise = new Promise((resolve) => { this.resolveReady = resolve; });
    [this.source, this.sourceOrigin] = rpcSource();
    this.handleMessage = this.handleMessage.bind(this);
    window.addEventListener("message", this.handleMessage);
    this.commands = {
      authorize: (args) => this.send("AUTHORIZE", args),
      authenticate: (args) => this.send("AUTHENTICATE", args),
      // Activities run in a sandboxed iframe: a plain <a target="_blank"> to an
      // outside origin (e.g. discord.com/channels/...) is blocked or silently
      // swallowed by Discord's client proxy. OPEN_EXTERNAL_LINK asks the
      // Discord client itself to open the URL outside the Activity frame.
      openExternalLink: (args) => this.send("OPEN_EXTERNAL_LINK", args),
    };
    this.handshake();
  }

  handshake() {
    const payload = {
      v: 1,
      encoding: "json",
      client_id: this.clientId,
      frame_id: this.frameId,
    };
    const mobileVersion = Number((new URLSearchParams(window.location.search)
      .get("mobile_app_version") || "").split(".")[0]);
    if (this.platform === "desktop" || mobileVersion >= 250) {
      payload.sdk_version = SDK_VERSION;
    }
    this.source.postMessage([OPCODE_HANDSHAKE, payload], this.sourceOrigin);
  }

  ready() {
    return this.readyPromise;
  }

  send(cmd, args) {
    const id = nonce();
    const promise = new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
    });
    this.source.postMessage([OPCODE_FRAME, { cmd, args, nonce: id }], this.sourceOrigin);
    return promise;
  }

  handleMessage(event) {
    if (event.source !== this.source) return;
    const tuple = event.data;
    if (!Array.isArray(tuple)) return;
    const [opcode, payload] = tuple;
    if (opcode !== OPCODE_FRAME || !payload) return;
    if (payload.cmd === DISPATCH && payload.evt === "READY") {
      this.resolveReady(payload.data);
      return;
    }
    if (!payload.nonce) return;
    const request = this.pending.get(payload.nonce);
    if (!request) return;
    this.pending.delete(payload.nonce);
    if (payload.evt === ERROR) {
      const error = new Error(payload.data && payload.data.message || "Discord RPC 请求失败");
      Object.assign(error, payload.data || {});
      request.reject(error);
      return;
    }
    request.resolve(payload.data);
  }
}

export { DiscordSDK };
