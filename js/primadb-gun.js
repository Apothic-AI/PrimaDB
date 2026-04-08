const SEA_PREFIX = "SEA";
const DEFAULT_CHANNEL = "primadb-sync";
const DEFAULT_RETRY_MS = 1500;
const textEncoder = new TextEncoder();

function noop() {}

function randomId(prefix = "id") {
  if (globalThis.crypto?.randomUUID) {
    return `${prefix}-${globalThis.crypto.randomUUID()}`;
  }
  return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

function nowMillis() {
  return Date.now();
}

function isPlainObject(value) {
  return (
    value !== null &&
    typeof value === "object" &&
    Object.getPrototypeOf(value) === Object.prototype
  );
}

function isGunLink(value) {
  return isPlainObject(value) && typeof value["#"] === "string" && Object.keys(value).length === 1;
}

function isPrimadbLink(value) {
  return (
    isPlainObject(value) &&
    typeof value.$link === "string" &&
    Object.keys(value).length === 1
  );
}

function stableStringify(value) {
  return JSON.stringify(value, (_key, candidate) => {
    if (Array.isArray(candidate) || !isPlainObject(candidate)) {
      return candidate;
    }
    return Object.fromEntries(Object.keys(candidate).sort().map((key) => [key, candidate[key]]));
  });
}

function splitPath(path) {
  return `${path ?? ""}`
    .split("/")
    .map((segment) => segment.trim())
    .filter(Boolean);
}

function joinPath(basePath, key) {
  return basePath ? `${basePath}/${key}` : key;
}

function ownerPubForPath(path) {
  const [root] = splitPath(path);
  if (!root || !root.startsWith("~") || root.startsWith("~@")) {
    return null;
  }
  return root.slice(1);
}

function convertToPrimadbValue(value) {
  if (value instanceof GunChain) {
    return { $link: value._soul() };
  }
  if (isGunLink(value)) {
    return { $link: value["#"] };
  }
  if (Array.isArray(value)) {
    return value.map((item) => convertToPrimadbValue(item));
  }
  if (!isPlainObject(value)) {
    return value;
  }
  return Object.fromEntries(
    Object.entries(value).map(([key, candidate]) => [key, convertToPrimadbValue(candidate)]),
  );
}

function convertToGunValue(value) {
  if (Array.isArray(value)) {
    return value.map((candidate) => convertToGunValue(candidate));
  }
  if (!isPlainObject(value)) {
    return value;
  }
  if (isPrimadbLink(value)) {
    return { "#": value.$link };
  }
  const next = {};
  for (const [key, candidate] of Object.entries(value)) {
    if (key === "$id") {
      next._id = candidate;
      continue;
    }
    next[key] = convertToGunValue(candidate);
  }
  return next;
}

function parseSeaEnvelope(value) {
  if (typeof value !== "string" || !value.startsWith(SEA_PREFIX)) {
    return null;
  }
  try {
    return JSON.parse(value.slice(SEA_PREFIX.length));
  } catch (_error) {
    return null;
  }
}

function encodeSeaEnvelope(payload) {
  return `${SEA_PREFIX}${JSON.stringify(payload)}`;
}

function ensureArray(value) {
  if (Array.isArray(value)) {
    return value;
  }
  if (value == null) {
    return [];
  }
  return [value];
}

function matchesLex(candidate, pattern) {
  if (pattern == null || pattern === "*" || pattern === "") {
    return true;
  }
  if (typeof pattern === "string") {
    if (pattern === candidate) {
      return true;
    }
    if (pattern.endsWith("*")) {
      return candidate.startsWith(pattern.slice(0, -1));
    }
    return candidate === pattern;
  }
  return false;
}

function matchesCertificatePath(path, policy) {
  if (policy == null) {
    return true;
  }
  const segments = splitPath(path);
  const key = segments.at(-1) ?? "";
  const soulPath = segments.slice(1, -1).join("/");
  const policies = Array.isArray(policy) ? policy : [policy];
  return policies.some((entry) => {
    if (typeof entry === "string") {
      return matchesLex(path, entry) || matchesLex(soulPath, entry) || matchesLex(key, entry);
    }
    if (!isPlainObject(entry)) {
      return false;
    }
    const pathRule = entry["#"];
    const fieldRule = entry["."];
    if (pathRule && fieldRule) {
      return matchesLex(soulPath, pathRule) && matchesLex(key, fieldRule);
    }
    if (pathRule) {
      return matchesLex(path, pathRule) || matchesLex(soulPath, pathRule);
    }
    if (fieldRule) {
      return matchesLex(key, fieldRule);
    }
    return false;
  });
}

function normalizeCertificants(certificants) {
  if (certificants == null) {
    return ["*"];
  }
  if (Array.isArray(certificants)) {
    return certificants.map((candidate) => `${candidate}`);
  }
  return [`${certificants}`];
}

function base64UrlEncode(bytes) {
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function tryDecodeBase64Url(value) {
  try {
    const normalized = `${value}`.replace(/-/g, "+").replace(/_/g, "/");
    const padded = normalized + "=".repeat((4 - (normalized.length % 4 || 4)) % 4);
    const binary = atob(padded);
    const bytes = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index += 1) {
      bytes[index] = binary.charCodeAt(index);
    }
    return bytes;
  } catch (_error) {
    return null;
  }
}

async function deriveProof(input, salt, iterations = 100_000) {
  const key = await globalThis.crypto.subtle.importKey(
    "raw",
    textEncoder.encode(`${input}`),
    "PBKDF2",
    false,
    ["deriveBits"],
  );
  const bits = await globalThis.crypto.subtle.deriveBits(
    {
      name: "PBKDF2",
      salt: textEncoder.encode(`${salt}`),
      iterations,
      hash: "SHA-256",
    },
    key,
    256,
  );
  return base64UrlEncode(new Uint8Array(bits));
}

async function normalizeCipherKey(seed) {
  const decoded = tryDecodeBase64Url(seed);
  if (decoded?.length === 32) {
    return seed;
  }
  const digest = await globalThis.crypto.subtle.digest(
    "SHA-256",
    textEncoder.encode(`${seed}`),
  );
  return base64UrlEncode(new Uint8Array(digest));
}

function createSea(bindings) {
  const {
    generateSeaPair,
    seaPairFromPrivateKeys,
    seaSign,
    seaVerify,
    seaSecret,
    seaEncrypt,
    seaDecrypt,
  } = bindings;

  if (
    !generateSeaPair ||
    !seaPairFromPrivateKeys ||
    !seaSign ||
    !seaVerify ||
    !seaSecret ||
    !seaEncrypt ||
    !seaDecrypt
  ) {
    throw new Error("Primadb Gun runtime requires the WASM build to include the `crypto` feature");
  }

  return {
    async pair() {
      return generateSeaPair();
    },

    async work(data, salt, opt = {}) {
      return deriveProof(data, salt, opt.iterations ?? 100_000);
    },

    sign(data, pair, cb, opt = {}) {
      const signed = seaSign(pair, data);
      const result = opt.raw ? signed : encodeSeaEnvelope({ kind: "signed", signed });
      cb?.(result);
      return result;
    },

    verify(data, pairOrPub, cb) {
      const envelope =
        typeof data === "string" ? parseSeaEnvelope(data)?.signed ?? parseSeaEnvelope(data) : data;
      if (!envelope?.signer || !envelope?.signature) {
        throw new Error("SEA.verify expected a signed payload");
      }
      const publicKey =
        typeof pairOrPub === "string"
          ? pairOrPub
          : pairOrPub?.pub ?? pairOrPub?.public_key ?? envelope.signer;
      const result = seaVerify(publicKey, envelope);
      cb?.(result);
      return result;
    },

    async encrypt(data, key, cb, opt = {}) {
      const cipherKey = await normalizeCipherKey(key);
      const encrypted = seaEncrypt(cipherKey, data);
      const result = opt.raw ? encrypted : encodeSeaEnvelope({ kind: "encrypted", encrypted });
      cb?.(result);
      return result;
    },

    async decrypt(data, key, cb) {
      const cipherKey = await normalizeCipherKey(key);
      const payload =
        typeof data === "string" ? parseSeaEnvelope(data)?.encrypted ?? parseSeaEnvelope(data) : data;
      if (!payload?.nonce || !payload?.ciphertext) {
        throw new Error("SEA.decrypt expected an encrypted payload");
      }
      const result = seaDecrypt(cipherKey, payload);
      cb?.(result);
      return result;
    },

    secret(epub, pair, cb) {
      const secret = seaSecret(pair, epub);
      cb?.(secret);
      return secret;
    },

    certify(certificants, policy, authority, cb, opt = {}) {
      const certificate = {
        c: normalizeCertificants(certificants),
        w: policy ?? "*",
        e: opt.expiry ?? null,
        wb: opt.writeBlock ?? null,
        iat: nowMillis(),
      };
      const signed = seaSign(authority, certificate);
      cb?.(signed);
      return signed;
    },

    pairFromPrivateKeys(secretKey, encryptionSecretKey) {
      return seaPairFromPrivateKeys(secretKey, encryptionSecretKey);
    },
  };
}

class DamConnection {
  constructor(runtime, url, options = {}) {
    this.runtime = runtime;
    this.url = url;
    this.channel = options.channel ?? DEFAULT_CHANNEL;
    this.retryIntervalMs = options.retryIntervalMs ?? DEFAULT_RETRY_MS;
    this.room = options.room ?? null;
    this.socket = null;
    this.closed = false;
    this.ready = false;
    this.knownPeers = new Map();
    this.inflight = new Map();
    this.signalListeners = new Set();
    this._retryTimer = null;
    this._connect();
  }

  _presencePayload() {
    return {
      peer_id: this.runtime._peerId,
      replica_id: this.runtime._db.replicaId(),
      transport: "websocket",
      capabilities: ["gun-runtime", "sync", "ack", "routing", "signal"],
      topics: this.room ? [this.room] : [],
      metadata: {
        app: "primadb-gun",
        state: "online",
      },
    };
  }

  _connect() {
    if (this.closed) {
      return;
    }
    this.socket = new WebSocket(this.url);
    this.socket.addEventListener("open", () => {
      this.ready = true;
      this._sendRoute({
        kind: "presence",
        peer: this._presencePayload(),
      });
      this.flushPending();
      this.retryInflight();
    });
    this.socket.addEventListener("message", (event) => {
      if (typeof event.data !== "string") {
        return;
      }
      this._handleMessage(event.data);
    });
    this.socket.addEventListener("close", () => {
      this.ready = false;
      this._scheduleReconnect();
    });
    this.socket.addEventListener("error", () => {
      this.ready = false;
      this._scheduleReconnect();
    });
  }

  _scheduleReconnect() {
    if (this.closed || this._retryTimer != null) {
      return;
    }
    this._retryTimer = globalThis.setTimeout(() => {
      this._retryTimer = null;
      this._connect();
    }, this.retryIntervalMs);
  }

  _route(target, payload) {
    return {
      route_id: `${this.runtime._peerId}/route/${randomId("dam")}`,
      from: this.runtime._peerId,
      channel: this.channel,
      target,
      ttl: 6,
      hops: 0,
      issued_at_millis: nowMillis(),
      payload,
    };
  }

  _sendRoute(payload, target = { kind: "broadcast" }) {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      return false;
    }
    const route = this._route(target, payload);
    this.socket.send(JSON.stringify(route));
    return true;
  }

  _handleMessage(raw) {
    let route;
    try {
      route = JSON.parse(raw);
    } catch (_error) {
      return;
    }

    switch (route.payload?.kind) {
      case "presence":
        this._handlePresence(route.payload.peer);
        return;
      case "signal":
        if (
          route.target?.kind === "peer" &&
          route.target.value === this.runtime._peerId &&
          route.payload.room === this.room
        ) {
          for (const listener of this.signalListeners) {
            listener(route.payload, route);
          }
        }
        return;
      case "sync":
        this._handleSync(route).catch((error) => console.error("sync apply failed", error));
        return;
      default:
        return;
    }
  }

  _handlePresence(peer) {
    if (!peer?.peer_id || peer.peer_id === this.runtime._peerId) {
      return;
    }
    if (peer.metadata?.state === "offline") {
      this.knownPeers.delete(peer.peer_id);
      return;
    }
    this.knownPeers.set(peer.peer_id, peer);
  }

  async _handleSync(route) {
    const payload = route.payload;
    if (payload.encoding !== "sync_frame") {
      return;
    }
    if (payload.payload?.type === "ack") {
      this.inflight.delete(payload.payload.message_id);
      return;
    }
    if (payload.payload?.type !== "sync") {
      return;
    }

    this.runtime._db.applyEnvelope({
      from: payload.payload.from,
      ops: payload.payload.ops,
    });
    this._sendRoute(
      {
        kind: "sync",
        encoding: "sync_frame",
        payload: {
          type: "ack",
          from: this.runtime._db.replicaId(),
          message_id: payload.payload.message_id,
          applied: payload.payload.ops.length,
        },
      },
      { kind: "peer", value: route.from },
    );
  }

  flushPending() {
    if (!this.ready) {
      return 0;
    }
    const envelope = this.runtime._db.drainPendingEnvelope();
    if (!envelope?.ops?.length) {
      return 0;
    }
    const messageId = `${this.runtime._db.replicaId()}/dam/${randomId("msg")}`;
    this.inflight.set(messageId, envelope);
    this._sendRoute({
      kind: "sync",
      encoding: "sync_frame",
      payload: {
        type: "sync",
        from: envelope.from,
        message_id: messageId,
        ops: envelope.ops,
      },
    });
    return envelope.ops.length;
  }

  retryInflight() {
    if (!this.ready) {
      return 0;
    }
    for (const [messageId, envelope] of this.inflight.entries()) {
      this._sendRoute({
        kind: "sync",
        encoding: "sync_frame",
        payload: {
          type: "sync",
          from: envelope.from,
          message_id: messageId,
          ops: envelope.ops,
        },
      });
    }
    return this.inflight.size;
  }

  sendSignal(toPeerId, payload) {
    return this._sendRoute(
      {
        kind: "signal",
        room: this.room,
        payload,
      },
      { kind: "peer", value: toPeerId },
    );
  }

  onSignal(listener) {
    this.signalListeners.add(listener);
    return () => this.signalListeners.delete(listener);
  }

  peerCount() {
    return this.knownPeers.size;
  }

  close() {
    this.closed = true;
    if (this._retryTimer != null) {
      clearTimeout(this._retryTimer);
      this._retryTimer = null;
    }
    try {
      this._sendRoute({
        kind: "presence",
        peer: {
          ...this._presencePayload(),
          metadata: {
            ...this._presencePayload().metadata,
            state: "offline",
          },
        },
      });
      this.socket?.close();
    } catch (_error) {
      // Ignore teardown failures during page unload.
    }
  }
}

class GunChain {
  constructor(runtime, config) {
    this._runtime = runtime;
    this._rootSoul = config.rootSoul ?? null;
    this._segments = config.segments ?? [];
    this._history = config.history ?? [];
    this._mode = config.mode ?? "value";
    this._source = config.source ?? null;
    this._userContext = config.userContext ?? null;
    this._listeners = new Set();
  }

  _clone(overrides = {}) {
    return new this.constructor(this._runtime, {
      rootSoul: overrides.rootSoul ?? this._rootSoul,
      segments: overrides.segments ?? this._segments,
      history: overrides.history ?? this._history,
      mode: overrides.mode ?? this._mode,
      source: overrides.source ?? this._source,
      userContext: overrides.userContext ?? this._userContext,
    });
  }

  _baseSoul() {
    if (this._userContext) {
      return this._userContext.baseSoul;
    }
    return this._rootSoul;
  }

  _resolveChain() {
    const soul = this._baseSoul();
    if (!soul) {
      throw new Error("gun user is not authenticated");
    }
    let chain = this._runtime._db.chain(soul);
    for (const segment of this._segments) {
      chain = chain.field(segment);
    }
    return chain;
  }

  _path() {
    const soul = this._baseSoul();
    if (!soul) {
      return this._segments.join("/");
    }
    return [soul, ...this._segments].join("/");
  }

  _soul() {
    if (this._segments.length === 0) {
      return this._baseSoul();
    }
    return this._path();
  }

  _lastKey() {
    return this._segments.at(-1) ?? this._baseSoul();
  }

  _allocateSoul() {
    const ownerPub = ownerPubForPath(this._path());
    if (ownerPub) {
      return `~${ownerPub}/${randomId("node")}`;
    }
    const root = splitPath(this._path())[0] ?? this._runtime._db.replicaId();
    return `${root}/${randomId("node")}`;
  }

  _currentUserPair() {
    return this._runtime._userContext.pair;
  }

  _prepareWrite(value, path, opt = {}) {
    if (value instanceof GunChain) {
      return { $link: value._soul() };
    }
    if (isGunLink(value)) {
      return { $link: value["#"] };
    }

    const ownerPub = ownerPubForPath(path);
    const user = this._runtime._userContext;
    const canSign =
      ownerPub &&
      user?.pair &&
      (user.pub === ownerPub || opt.cert);

    if (!canSign) {
      return convertToPrimadbValue(value);
    }

    if (Array.isArray(value) || !isPlainObject(value)) {
      return this._signLeaf(value, path, opt.cert);
    }

    const next = {};
    for (const [key, candidate] of Object.entries(value)) {
      const fieldPath = joinPath(path, key);
      if (candidate instanceof GunChain || isGunLink(candidate)) {
        next[key] = convertToPrimadbValue(candidate);
        continue;
      }
      if (Array.isArray(candidate) || !isPlainObject(candidate)) {
        next[key] = this._signLeaf(candidate, fieldPath, opt.cert);
        continue;
      }
      next[key] = this._prepareWrite(candidate, fieldPath, opt);
    }
    return next;
  }

  _signLeaf(value, path, cert) {
    const pair = this._currentUserPair();
    if (!pair) {
      throw new Error("writing to a SEA-backed user graph requires an authenticated user");
    }
    const signed = this._runtime.SEA.sign(
      {
        path,
        value,
        cert: cert ?? null,
      },
      pair,
      null,
      { raw: true },
    );
    if (signed instanceof Promise) {
      throw new Error("SEA.sign must be synchronous inside write preparation");
    }
    return encodeSeaEnvelope({ kind: "signed", signed });
  }

  _unwrapValue(value, path = this._path()) {
    const envelope = parseSeaEnvelope(value);
    if (envelope?.kind === "signed") {
      const signed = envelope.signed ?? envelope;
      const verified = this._runtime._verifySignedValue(signed, path);
      return verified;
    }

    if (Array.isArray(value)) {
      return value
        .map((candidate, index) => this._unwrapValue(candidate, joinPath(path, `${index}`)))
        .filter((candidate) => candidate !== undefined);
    }

    if (!isPlainObject(value)) {
      return value;
    }

    if (Array.isArray(value.$set) && Object.keys(value).length === 1) {
      return value.$set
        .map((candidate, index) => this._unwrapValue(candidate, joinPath(path, `${index}`)))
        .filter((candidate) => candidate !== undefined);
    }

    if (isPrimadbLink(value)) {
      return { "#": value.$link };
    }

    const next = {};
    for (const [key, candidate] of Object.entries(value)) {
      if (key === "$id") {
        next._id = candidate;
        continue;
      }
      const unwrapped = this._unwrapValue(candidate, joinPath(path, key));
      if (unwrapped !== undefined) {
        next[key] = unwrapped;
      }
    }
    return next;
  }

  _readCurrent() {
    const raw = this._resolveChain().once();
    return raw == null ? raw : this._unwrapValue(raw);
  }

  get(key, opt) {
    if (typeof key === "function") {
      return this.on(key, opt);
    }
    const nextKey = `${key}`;
    const history = [...this._history, this];
    return this._clone({
      segments: [...this._segments, nextKey],
      history,
      mode: "value",
      source: this,
    });
  }

  path() {
    return this._path();
  }

  back(depth = 1) {
    if (depth === -1) {
      return this._runtime;
    }
    if (typeof depth === "string") {
      if (depth === "user") {
        return this._runtime.user();
      }
      return null;
    }
    if (depth === 0) {
      return this;
    }
    return this._history.at(-depth) ?? null;
  }

  put(value, cb = noop, opt = {}) {
    this._runtime._whenReady()
      .then(async () => {
        try {
          const prepared = this._prepareWrite(value, this._path(), opt);
          const chain = this._resolveChain();
          if (prepared == null) {
            chain.unset();
          } else {
            chain.put(prepared);
          }
          this._runtime._flushNetwork();
          cb({ ok: 1, path: this._path() });
        } catch (error) {
          cb({ err: `${error}` });
        }
      })
      .catch((error) => cb({ err: `${error}` }));
    return this;
  }

  set(value, cb = noop, opt = {}) {
    const memberChain =
      value instanceof GunChain
        ? value
        : this._runtime.get(opt.soul ?? this._allocateSoul());

    if (!(value instanceof GunChain)) {
      memberChain.put(value, noop, opt);
    }

    this._runtime._whenReady()
      .then(async () => {
        try {
          this._resolveChain().set({ "#": memberChain._soul() });
          this._runtime._flushNetwork();
          cb({ ok: 1, soul: memberChain._soul() });
        } catch (error) {
          cb({ err: `${error}` });
        }
      })
      .catch((error) => cb({ err: `${error}` }));

    return memberChain;
  }

  once(cb, opt = {}) {
    const run = async () => {
      await this._runtime._whenReady();
      const data = this._readCurrent();
      if (this._mode === "map") {
        for (const [key, value] of this._mapEntries(data)) {
          cb?.call(this._source.get(key), value, key);
        }
        return data;
      }
      cb?.call(this, data, this._lastKey());
      return data;
    };

    if (typeof cb !== "function") {
      return run();
    }
    run().catch((error) => console.error("gun.once failed", error));
    return this;
  }

  on(cb, opt = {}) {
    const subscriptionRecord = {
      cancel: noop,
      lastMap: new Map(),
    };
    const eve = {
      off: () => {
        subscriptionRecord.cancel();
        this._listeners.delete(subscriptionRecord);
      },
    };

    this._runtime._whenReady()
      .then(async () => {
        const subscription = this._resolveChain().on((raw) => {
          const data = raw == null ? raw : this._unwrapValue(raw);
          if (this._mode === "map") {
            const nextEntries = new Map(this._mapEntries(data));
            for (const [key, value] of nextEntries.entries()) {
              const previous = subscriptionRecord.lastMap.get(key);
              if (previous !== undefined && stableStringify(previous) === stableStringify(value)) {
                continue;
              }
              cb.call(this._source.get(key), value, key, { put: value, get: key }, eve);
            }
            subscriptionRecord.lastMap = nextEntries;
            return;
          }
          cb.call(this, data, this._lastKey(), { put: data, get: this._lastKey() }, eve);
        });
        subscriptionRecord.cancel = () => subscription.cancel();
        this._listeners.add(subscriptionRecord);
      })
      .catch((error) => console.error("gun.on failed", error));
    return this;
  }

  off() {
    for (const listener of this._listeners) {
      listener.cancel();
    }
    this._listeners.clear();
    return this;
  }

  map(cb) {
    const mapped = this._clone({
      mode: "map",
      source: this,
    });
    if (typeof cb === "function") {
      return mapped.on((data, key, msg, eve) => {
        const next = cb.call(this.get(key), data, key, msg, eve);
        if (next !== undefined) {
          return next;
        }
        return undefined;
      });
    }
    return mapped;
  }

  open(cb, opt = {}) {
    let timer = null;
    return this.on((data, key, _msg, eve) => {
      clearTimeout(timer);
      timer = setTimeout(() => {
        cb.call(this, data, key, opt, eve);
        if (opt.off) {
          eve.off();
        }
      }, opt.wait ?? 9);
    }, opt);
  }

  load(cb, opt = {}) {
    return this.open(cb, { ...opt, off: true });
  }

  promise(cb) {
    const promise = this.then((put) => ({
      put,
      get: this._lastKey(),
      gun: this,
    }));
    return cb ? promise.then(cb) : promise;
  }

  then(cb) {
    const promise = this.once();
    return cb ? promise.then(cb) : promise;
  }

  not(cb) {
    this.once((data) => {
      if (data == null) {
        cb.call(this, this._lastKey(), noop);
      }
    });
    return this;
  }

  _mapEntries(data) {
    if (Array.isArray(data)) {
      return data.map((entry, index) => [entry?._id ?? entry?.["#"] ?? `${index}`, entry]);
    }
    if (isPlainObject(data)) {
      return Object.entries(data).filter(([key]) => key !== "_");
    }
    return [];
  }
}

class GunUser extends GunChain {
  constructor(runtime, userContext) {
    super(runtime, {
      rootSoul: null,
      segments: [],
      history: [],
      mode: "value",
      source: null,
      userContext,
    });
    this.is = null;
  }

  get(key, opt) {
    return new GunChain(this._runtime, {
      rootSoul: null,
      segments: [...this._segments, `${key}`],
      history: [...this._history, this],
      mode: "value",
      source: this,
      userContext: this._userContext,
    });
  }

  create(alias, password, cb = noop, opt = {}) {
    this._runtime._whenReady()
      .then(async () => {
        try {
          const existingSoul = await this._runtime._resolveAlias(alias);
          if (existingSoul && opt.already !== true) {
            cb({ err: "User already created!" });
            return;
          }
          const pair = await this._runtime.SEA.pair();
          const salt = randomId("salt");
          const proof = await this._runtime.SEA.work(password, salt, opt);
          const encrypted = await this._runtime.SEA.encrypt(
            {
              priv: pair.priv,
              epriv: pair.epriv,
            },
            proof,
            null,
            { raw: true },
          );
          const soul = `~${pair.pub}`;
          this._runtime._db.chain(soul).put({
            pub: pair.pub,
            epub: pair.epub,
            alias,
            auth: JSON.stringify({ ek: encrypted, s: salt }),
          });
          this._runtime._db.chain(`~@${alias}`).put({
            [soul]: { "#": soul },
          });
          this._runtime._flushNetwork();
          if (opt.autoAuth !== false) {
            await this._completeAuth(alias, pair, opt);
          }
          cb({ ok: 1, pub: pair.pub, alias });
        } catch (error) {
          cb({ err: `${error}` });
        }
      })
      .catch((error) => cb({ err: `${error}` }));
    return this;
  }

  auth(aliasOrPair, password, cb = noop, opt = {}) {
    this._runtime._whenReady()
      .then(async () => {
        try {
          let alias = typeof aliasOrPair === "string" ? aliasOrPair : aliasOrPair?.alias ?? null;
          let pair = null;
          if (typeof aliasOrPair === "object" && aliasOrPair?.priv && aliasOrPair?.epriv) {
            pair = aliasOrPair;
            alias = alias ?? aliasOrPair.pub;
          }
          if (!pair) {
            const soul = await this._runtime._resolveAlias(alias);
            if (!soul) {
              cb({ err: "Wrong user or password." });
              return;
            }
            const account = this._runtime.get(soul);
            const data = await account.once();
            const auth = JSON.parse(data?.auth ?? "{}");
            if (!auth.ek || !auth.s) {
              cb({ err: "User cannot be found!" });
              return;
            }
            const proof = await this._runtime.SEA.work(password, auth.s, opt);
            const decrypted = await this._runtime.SEA.decrypt(auth.ek, proof);
            pair = this._runtime.SEA.pairFromPrivateKeys(decrypted.priv, decrypted.epriv);
            if (!pair.pub) {
              pair.pub = data.pub;
            }
            if (!pair.epub) {
              pair.epub = data.epub;
            }
          }
          await this._completeAuth(alias ?? pair.pub, pair, opt);
          cb({ ok: 1, pub: pair.pub, alias: alias ?? pair.pub, epub: pair.epub });
        } catch (error) {
          cb({ err: `${error}` });
        }
      })
      .catch((error) => cb({ err: `${error}` }));
    return this;
  }

  async _completeAuth(alias, pair, opt) {
    this._userContext.baseSoul = `~${pair.pub}`;
    this._userContext.alias = alias;
    this._userContext.pub = pair.pub;
    this._userContext.epub = pair.epub;
    this._userContext.pair = pair;
    this.is = { pub: pair.pub, epub: pair.epub, alias };

    if (this._runtime._db.registerUser) {
      try {
        this._runtime._db.registerUser(alias, pair.pub, ["*"]);
      } catch (_error) {
        // Ignore duplicate registration attempts in the browser runtime.
      }
      this._runtime._db.authenticateLocalUser(alias, pair.priv, ["*"]);
    }

    if (opt.remember || this._runtime._options.remember) {
      sessionStorage.setItem(
        this._runtime._sessionKey(),
        JSON.stringify({
          alias,
          pair,
        }),
      );
    }
  }

  leave() {
    this._userContext.baseSoul = null;
    this._userContext.alias = null;
    this._userContext.pub = null;
    this._userContext.epub = null;
    this._userContext.pair = null;
    this.is = null;
    sessionStorage.removeItem(this._runtime._sessionKey());
    return this;
  }

  recall(opt = {}, cb = noop) {
    this._runtime._whenReady()
      .then(async () => {
        try {
          if (!opt.sessionStorage && !this._runtime._options.remember) {
            cb({ ok: 0 });
            return;
          }
          const remembered = sessionStorage.getItem(this._runtime._sessionKey());
          if (!remembered) {
            cb({ ok: 0 });
            return;
          }
          const parsed = JSON.parse(remembered);
          this.auth(parsed.pair, null, cb, opt);
        } catch (error) {
          cb({ err: `${error}` });
        }
      })
      .catch((error) => cb({ err: `${error}` }));
    return this;
  }

  pair() {
    return this._userContext.pair;
  }
}

class GunRoot {
  constructor(options = {}, SEA) {
    this._options = options;
    this.SEA = SEA;
    this._db = new options.Primadb(options.replicaId ?? `gun-${randomId("replica")}`);
    this._peerId = options.peerId ?? `browser:${this._db.replicaId()}:${randomId("peer")}`;
    this._userContext = {
      baseSoul: null,
      alias: null,
      pub: null,
      epub: null,
      pair: null,
    };
    this._user = new GunUser(this, this._userContext);
    this._damConnections = [];
    this._booting = true;
    this.ready = this._initialize().finally(() => {
      this._booting = false;
    });
  }

  async _initialize() {
    const { indexedDb, localStorageKey, peers, room, retryIntervalMs } = this._options;
    if (indexedDb) {
      try {
        await this._db.enableIndexedDbPersistence(
          indexedDb.databaseName,
          indexedDb.storeName ?? "snapshots",
          indexedDb.key ?? "main",
          indexedDb.loadExisting ?? true,
        );
      } catch (error) {
        if (localStorageKey) {
          this._db.useBrowserStorage(localStorageKey);
        } else {
          throw error;
        }
      }
    } else if (localStorageKey) {
      this._db.useBrowserStorage(localStorageKey);
    }

    for (const peer of ensureArray(peers)) {
      const connection = new DamConnection(this, peer, {
        channel: this._options.channel ?? DEFAULT_CHANNEL,
        room,
        retryIntervalMs,
      });
      this._damConnections.push(connection);
    }

    if (this._options.remember) {
      const remembered = sessionStorage.getItem(this._sessionKey());
      if (remembered) {
        const parsed = JSON.parse(remembered);
        await new Promise((resolve) => {
          this.user().auth(
            {
              ...parsed.pair,
              alias: parsed.alias,
            },
            null,
            () => resolve(),
            { remember: true },
          );
        });
      }
    }
  }

  _sessionKey() {
    return this._options.sessionStorageKey ?? "primadb-gun-user";
  }

  _whenReady() {
    return this._booting ? Promise.resolve() : this.ready;
  }

  _flushNetwork() {
    for (const connection of this._damConnections) {
      connection.flushPending();
    }
  }

  _verifySignedValue(signed, expectedPath) {
    const verified = this.SEA.verify(signed, signed.signer);
    if (verified instanceof Promise) {
      throw new Error("SEA.verify must be synchronous inside read unwrapping");
    }
    if (verified.path !== expectedPath) {
      return undefined;
    }
    const ownerPub = ownerPubForPath(expectedPath);
    if (ownerPub && signed.signer !== ownerPub) {
      const certificate = verified.cert;
      if (!certificate) {
        return undefined;
      }
      const certPayload = this.SEA.verify(certificate, ownerPub);
      if (certPayload instanceof Promise) {
        throw new Error("SEA.verify must be synchronous inside certificate validation");
      }
      if (certPayload.e && certPayload.e < nowMillis()) {
        return undefined;
      }
      const allowed = normalizeCertificants(certPayload.c);
      if (!allowed.includes("*") && !allowed.includes(signed.signer)) {
        return undefined;
      }
      if (!matchesCertificatePath(expectedPath, certPayload.w)) {
        return undefined;
      }
    }
    return verified.value;
  }

  async _resolveAlias(alias) {
    const node = await this.get(`~@${alias}`).once();
    if (!isPlainObject(node)) {
      return null;
    }
    for (const candidate of Object.values(node)) {
      if (isGunLink(candidate)) {
        return candidate["#"];
      }
    }
    return null;
  }

  get(key) {
    return new GunChain(this, {
      rootSoul: `${key}`,
      segments: [],
      history: [],
      mode: "value",
      source: null,
      userContext: null,
    });
  }

  user(pub) {
    if (pub) {
      return new GunChain(this, {
        rootSoul: `~${pub}`,
        segments: [],
        history: [],
        mode: "value",
        source: null,
        userContext: null,
      });
    }
    return this._user;
  }

  stats() {
    const peers = this._damConnections.reduce(
      (count, connection) => count + connection.peerCount(),
      0,
    );
    const inflight = this._damConnections.reduce(
      (count, connection) => count + connection.inflight.size,
      0,
    );
    return {
      replicaId: this._db.replicaId(),
      peers,
      inflight,
      pending: this._db.pendingOperations().length,
      authenticatedUser: this._user.is,
    };
  }

  close() {
    this._user.off();
    for (const connection of this._damConnections) {
      connection.close();
    }
  }
}

export function installPrimadbGunRuntime(bindings) {
  const SEA = createSea(bindings);

  function Gun(options = {}) {
    return new GunRoot(
      {
        ...options,
        Primadb: bindings.Primadb,
      },
      SEA,
    );
  }

  Gun.chain = GunChain.prototype;
  Gun.User = GunUser;
  Gun.SEA = SEA;
  Gun.state = nowMillis;
  Gun.text = {
    random: (length = 24) => randomId("text").slice(-length),
  };

  return Gun;
}
