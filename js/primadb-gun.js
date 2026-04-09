const SEA_PREFIX = "SEA";
const DEFAULT_CHANNEL = "primadb-sync";
const DEFAULT_RETRY_MS = 1500;
const MAX_ROUTE_CACHE = 4096;
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

function contentHashForPayload(payload) {
  const text = stableStringify(payload);
  let hash = 0x811c9dc5;
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash.toString(16).padStart(8, "0");
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

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function trimCache(map, maxSize = MAX_ROUTE_CACHE) {
  while (map.size > maxSize) {
    const oldest = map.keys().next();
    if (oldest.done) {
      break;
    }
    map.delete(oldest.value);
  }
}

function routeContentKey(route) {
  if (!route?.content_hash) {
    return null;
  }
  return `${route.from ?? ""}:${route.reply_to ?? ""}:${route.content_hash}`;
}

function sameJsonValue(left, right) {
  return stableStringify(left) === stableStringify(right);
}

function normalizeOpenValue(value, opt = {}, depth = 1, seen = new Map()) {
  if (value == null || typeof value !== "object") {
    return value;
  }

  if (Array.isArray(value)) {
    if (opt.depth && depth > opt.depth) {
      return value.slice();
    }
    return value.map((entry) => normalizeOpenValue(entry, opt, depth + 1, seen));
  }

  if (isGunLink(value)) {
    return { "#": value["#"] };
  }

  const seenId = typeof value._id === "string" ? value._id : null;
  if (seenId && seen.has(seenId)) {
    return seen.get(seenId);
  }

  const output = {};
  if (seenId) {
    seen.set(seenId, output);
  }

  for (const [key, candidate] of Object.entries(value)) {
    if (key === "_" && !opt.meta) {
      continue;
    }
    if (opt.depth && depth >= opt.depth && candidate && typeof candidate === "object") {
      output[key] = Array.isArray(candidate) ? candidate.slice() : { ...candidate };
      continue;
    }
    output[key] = normalizeOpenValue(candidate, opt, depth + 1, seen);
  }
  return output;
}

function isPlaceholderOpenValue(value) {
  if (value == null) {
    return false;
  }
  if (Array.isArray(value)) {
    return value.length === 0 || value.some((entry) => isPlaceholderOpenValue(entry));
  }
  if (!isPlainObject(value)) {
    return false;
  }
  const keys = Object.keys(value).filter((key) => key !== "_id");
  if (keys.length === 0) {
    return true;
  }
  return keys.every((key) => {
    const candidate = value[key];
    return candidate != null && typeof candidate === "object" && isPlaceholderOpenValue(candidate);
  });
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
    this.recommendedPeers = new Map();
    this.inflight = new Map();
    this.signalListeners = new Set();
    this.seenRoutes = new Map();
    this.seenContent = new Map();
    this.hydratedRootsByPeer = new Map();
    this.pendingSnapshots = new Map();
    this.pendingSnapshotKeys = new Map();
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
      this.knownPeers.clear();
      this.hydratedRootsByPeer.clear();
      this._failPendingSnapshots("connection closed");
      this._scheduleReconnect();
    });
    this.socket.addEventListener("error", () => {
      this.ready = false;
      this._failPendingSnapshots("connection errored");
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

  _route(target, payload, replyTo = null) {
    return {
      route_id: `${this.runtime._peerId}/route/${randomId("dam")}`,
      from: this.runtime._peerId,
      channel: this.channel,
      target,
      ttl: 6,
      hops: 0,
      issued_at_millis: nowMillis(),
      reply_to: replyTo,
      content_hash: contentHashForPayload(payload),
      seen_by: [this.runtime._peerId],
      payload,
    };
  }

  _sendRoute(payload, target = { kind: "broadcast" }, replyTo = null) {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      return null;
    }
    const route = this._route(target, payload, replyTo);
    this.socket.send(JSON.stringify(route));
    return route;
  }

  _handleMessage(raw) {
    let route;
    try {
      route = JSON.parse(raw);
    } catch (_error) {
      return;
    }

    if (!route?.payload || route.from === this.runtime._peerId) {
      return;
    }
    if (!this._isRouteForThisPeer(route) || this._isDuplicateRoute(route)) {
      return;
    }
    this._trackRoute(route);
    this._handlePayload(route.payload, route);
  }

  _handlePayload(payload, route) {
    switch (payload?.kind) {
      case "presence":
        this._handlePresence(payload.peer);
        return;
      case "peer_exchange":
        for (const recommendation of payload.peers ?? []) {
          if (recommendation?.peer?.peer_id && recommendation.peer.peer_id !== this.runtime._peerId) {
            this.recommendedPeers.set(recommendation.peer.peer_id, recommendation);
          }
        }
        return;
      case "batch":
        for (const item of payload.items ?? []) {
          this._handlePayload(item, route);
        }
        return;
      case "snapshot_request":
        this._handleSnapshotRequest(payload, route);
        return;
      case "snapshot_response":
        this._handleSnapshotResponse(payload, route);
        return;
      case "signal":
        if (
          route.target?.kind === "peer" &&
          route.target.value === this.runtime._peerId &&
          payload.room === this.room
        ) {
          for (const listener of this.signalListeners) {
            listener(payload, route);
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
    const isNewPeer = !this.knownPeers.has(peer.peer_id);
    if (peer.metadata?.state === "offline") {
      this.knownPeers.delete(peer.peer_id);
      this.recommendedPeers.delete(peer.peer_id);
      this.hydratedRootsByPeer.delete(peer.peer_id);
      return;
    }
    this.knownPeers.set(peer.peer_id, peer);
    this.recommendedPeers.set(peer.peer_id, {
      peer,
      relay_urls:
        typeof peer.metadata?.relay_url === "string"
          ? peer.metadata.relay_url
              .split(",")
              .map((candidate) => candidate.trim())
              .filter(Boolean)
          : [],
      score: 100 + Math.min(peer.capabilities?.length ?? 0, 8) * 5 + Math.min(peer.topics?.length ?? 0, 8) * 5,
      discovered_at_millis: nowMillis(),
    });
    if (isNewPeer) {
      this.runtime._replayLiveInterestsToPeer(this, peer.peer_id).catch((error) => {
        console.warn("live interest replay failed", error);
      });
    }
  }

  _handleSnapshotRequest(payload, route) {
    const snapshot = this.runtime._db.snapshotForRoot
      ? this.runtime._db.snapshotForRoot(payload.root ?? null)
      : this.runtime._db.snapshot();
    if (snapshot && typeof snapshot === "object") {
      snapshot.pending_ops = [];
    }
    this._sendRoute(
      {
        kind: "snapshot_response",
        root: payload.root ?? null,
        snapshot,
      },
      { kind: "peer", value: route.from },
      route.route_id,
    );
  }

  _handleSnapshotResponse(payload, route) {
    if (!route.reply_to) {
      return;
    }
    const pending = this.pendingSnapshots.get(route.reply_to);
    if (!pending) {
      return;
    }
    this.pendingSnapshots.delete(route.reply_to);
    this.pendingSnapshotKeys.delete(pending.key);
    try {
      const snapshot = {
        ...(payload.snapshot ?? {}),
        pending_ops: [],
      };
      this.runtime._db.mergeSnapshotJson(JSON.stringify(snapshot));
      this._markRootHydrated(pending.peerId, pending.root);
      pending.resolve(true);
    } catch (error) {
      pending.reject(error);
    }
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

  requestRootSnapshot(peerId, root = null, options = {}) {
    const normalizedRoot = root ?? null;
    const rootKey = normalizedRoot ?? "*";
    if (!peerId || !this.ready) {
      return Promise.resolve(false);
    }
    if (!options.force && this.hydratedRootsByPeer.get(peerId)?.has(rootKey)) {
      return Promise.resolve(false);
    }

    const pendingKey = `${peerId}:${rootKey}`;
    const existingRouteId = this.pendingSnapshotKeys.get(pendingKey);
    if (existingRouteId) {
      return this.pendingSnapshots.get(existingRouteId)?.promise ?? Promise.resolve(false);
    }

    const route = this._sendRoute(
      {
        kind: "snapshot_request",
        root: normalizedRoot,
      },
      { kind: "peer", value: peerId },
    );
    if (!route) {
      return Promise.resolve(false);
    }

    let resolve;
    let reject;
    const promise = new Promise((res, rej) => {
      resolve = res;
      reject = rej;
    });
    this.pendingSnapshots.set(route.route_id, {
      key: pendingKey,
      peerId,
      root: rootKey,
      resolve,
      reject,
      promise,
    });
    this.pendingSnapshotKeys.set(pendingKey, route.route_id);
    return promise;
  }

  requestRootSnapshotFromAnyPeer(root, options = {}) {
    const peerIds = [
      ...this.knownPeers.keys(),
      ...[...this.recommendedPeers.keys()].filter((peerId) => !this.knownPeers.has(peerId)),
    ];
    if (peerIds.length === 0) {
      return Promise.resolve(false);
    }
    const requests = peerIds.map((peerId) =>
      this.requestRootSnapshot(peerId, root, options).then((result) => {
        if (!result) {
          throw new Error("snapshot skipped");
        }
        return true;
      }),
    );
    return Promise.any(requests).catch(() => false);
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

  recommendationCount() {
    return this.recommendedPeers.size;
  }

  close() {
    this.closed = true;
    if (this._retryTimer != null) {
      clearTimeout(this._retryTimer);
      this._retryTimer = null;
    }
    this._failPendingSnapshots("connection closed");
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

  _isRouteForThisPeer(route) {
    if (route.channel && route.channel !== this.channel) {
      return false;
    }
    if (!route.target || route.target.kind === "broadcast") {
      return true;
    }
    if (route.target.kind === "peer") {
      return route.target.value === this.runtime._peerId;
    }
    if (route.target.kind === "topic") {
      return route.target.value === this.room || route.target.value === this.channel;
    }
    return false;
  }

  _isDuplicateRoute(route) {
    if (Array.isArray(route.seen_by) && route.seen_by.includes(this.runtime._peerId)) {
      return true;
    }
    if (this.seenRoutes.has(route.route_id)) {
      return true;
    }
    const contentKey = routeContentKey(route);
    return contentKey ? this.seenContent.has(contentKey) : false;
  }

  _trackRoute(route) {
    const seenAt = nowMillis();
    this.seenRoutes.set(route.route_id, seenAt);
    trimCache(this.seenRoutes);
    const contentKey = routeContentKey(route);
    if (contentKey) {
      this.seenContent.set(contentKey, seenAt);
      trimCache(this.seenContent);
    }
  }

  _markRootHydrated(peerId, rootKey) {
    if (!this.hydratedRootsByPeer.has(peerId)) {
      this.hydratedRootsByPeer.set(peerId, new Set());
    }
    this.hydratedRootsByPeer.get(peerId).add(rootKey);
  }

  _failPendingSnapshots(message) {
    for (const pending of this.pendingSnapshots.values()) {
      pending.reject(new Error(message));
    }
    this.pendingSnapshots.clear();
    this.pendingSnapshotKeys.clear();
  }
}

class GunChain {
  constructor(runtime, config) {
    this._runtime = runtime;
    this._rootSoul = config.rootSoul ?? null;
    this._segments = config.segments ?? [];
    this._history = config.history ?? [];
    this._mode = config.mode ?? "value";
    this._mapTransform = config.mapTransform ?? null;
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
      mapTransform: overrides.mapTransform ?? this._mapTransform,
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

  _interestRoot() {
    return this._baseSoul();
  }

  _prepareWrite(value, path, opt = {}) {
    void path;
    void opt;
    return convertToPrimadbValue(value);
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

  async _writeValue(value, opt = {}) {
    const prepared = this._prepareWrite(value, this._path(), opt);
    const chain = this._resolveChain();
    if (prepared == null) {
      chain.unset();
    } else if (
      typeof chain.putSigned === "function" &&
      ownerPubForPath(this._path()) &&
      this._runtime._userContext?.pair &&
      (this._runtime._userContext.pub === ownerPubForPath(this._path()) || opt.cert)
    ) {
      chain.putSigned(prepared, opt.cert ?? null);
    } else {
      chain.put(prepared);
    }
    this._runtime._flushNetwork();
    const expectedVisible = prepared == null ? null : this._unwrapValue(prepared, this._path());
    await this._runtime._waitForVisibleValue(this, expectedVisible);
    await delay(0);
    return { ok: 1, path: this._path() };
  }

  async _resolveMappedEntry(key, value, msg, eve) {
    const context = this._source?.get(key) ?? this.get(key);
    if (typeof this._mapTransform !== "function") {
      return {
        context,
        key,
        value,
        msg,
      };
    }

    const next = this._mapTransform.call(context, value, key, msg, eve);
    if (next === undefined) {
      return null;
    }
    if (next instanceof GunChain) {
      await this._runtime._ensureHydrated(next);
      return {
        context: next,
        key: next._lastKey(),
        value: next._readCurrent(),
        msg: { put: next._readCurrent(), get: next._lastKey() },
        chain: next,
      };
    }
    const resolved = next === value ? value : next;
    return {
      context,
      key,
      value: resolved,
      msg: {
        put: resolved,
        get: key,
      },
    };
  }

  _clearNestedMapSubscription(subscriptionRecord, key) {
    const nested = subscriptionRecord.nestedMap.get(key);
    if (!nested) {
      return;
    }
    nested.chain.off();
    subscriptionRecord.nestedMap.delete(key);
  }

  _clearAllNestedMapSubscriptions(subscriptionRecord) {
    if (!subscriptionRecord?.nestedMap) {
      return;
    }
    for (const key of subscriptionRecord.nestedMap.keys()) {
      this._clearNestedMapSubscription(subscriptionRecord, key);
    }
  }

  async _emitMapOnce(cb, data) {
    const mappedValues = [];
    for (const [key, value] of this._mapEntries(data)) {
      const resolved = await this._resolveMappedEntry(
        key,
        value,
        { put: value, get: key },
        null,
      );
      if (!resolved) {
        continue;
      }
      mappedValues.push(resolved.value);
      cb?.call(resolved.context, resolved.value, resolved.key);
    }
    return mappedValues;
  }

  _emitMapUpdate(cb, data, eve, subscriptionRecord) {
    const nextEntries = new Map(this._mapEntries(data));

    for (const key of subscriptionRecord.lastMap.keys()) {
      if (!nextEntries.has(key)) {
        subscriptionRecord.lastMap.delete(key);
        cb.call(this._source?.get(key) ?? this.get(key), null, key, { put: null, get: key }, eve);
      }
    }
    for (const key of subscriptionRecord.nestedMap.keys()) {
      if (!nextEntries.has(key)) {
        this._clearNestedMapSubscription(subscriptionRecord, key);
        cb.call(this._source?.get(key) ?? this.get(key), null, key, { put: null, get: key }, eve);
      }
    }

    for (const [key, value] of nextEntries.entries()) {
      Promise.resolve(
        this._resolveMappedEntry(key, value, { put: value, get: key }, eve),
      )
        .then((resolved) => {
          if (!resolved) {
            this._clearNestedMapSubscription(subscriptionRecord, key);
            subscriptionRecord.lastMap.delete(key);
            return;
          }

          if (resolved.chain instanceof GunChain) {
            const nestedPath = resolved.chain._path();
            const existing = subscriptionRecord.nestedMap.get(key);
            if (existing?.path === nestedPath) {
              return;
            }
            this._clearNestedMapSubscription(subscriptionRecord, key);
            const nestedChain = resolved.chain._clone();
            nestedChain.on((nestedValue, nestedKey, nestedMsg, nestedEve) => {
              cb.call(nestedChain, nestedValue, nestedKey, nestedMsg, nestedEve);
            });
            subscriptionRecord.nestedMap.set(key, {
              path: nestedPath,
              chain: nestedChain,
            });
            return;
          }

          this._clearNestedMapSubscription(subscriptionRecord, key);
          const previous = subscriptionRecord.lastMap.get(key);
          if (previous !== undefined && sameJsonValue(previous, resolved.value)) {
            return;
          }
          subscriptionRecord.lastMap.set(key, resolved.value);
          cb.call(resolved.context, resolved.value, resolved.key, resolved.msg, eve);
        })
        .catch((error) => console.error("gun.map update failed", error));
    }
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
    if (depth === -1 || depth === Infinity) {
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
    this._runtime._awaitReady()
      .then(async () => {
        try {
          cb(await this._writeValue(value, opt));
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

    this._runtime._awaitReady()
      .then(async () => {
        try {
          if (!(value instanceof GunChain)) {
            await memberChain._writeValue(value, opt);
          }
          this._resolveChain().set({ "#": memberChain._soul() });
          this._runtime._flushNetwork();
          await delay(0);
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
      await this._runtime._awaitReady();
      await this._runtime._ensureHydrated(this, { force: opt.force === true });
      const data = this._readCurrent();
      if (this._mode === "map") {
        const mapped = await this._emitMapOnce(cb, data);
        return typeof cb === "function" ? data : mapped;
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
      nestedMap: new Map(),
      interestOff: noop,
    };
    const eve = {
      off: () => {
        subscriptionRecord.interestOff();
        this._clearAllNestedMapSubscriptions(subscriptionRecord);
        subscriptionRecord.cancel();
        this._listeners.delete(subscriptionRecord);
      },
    };

    this._runtime._awaitReady()
      .then(async () => {
        subscriptionRecord.interestOff = this._runtime._registerInterest(this, {
          mode: this._mode,
        });
        await this._runtime._ensureHydrated(this);
        const subscription = this._resolveChain().on((raw) => {
          const data = raw == null ? raw : this._unwrapValue(raw);
          if (this._mode === "map") {
            this._emitMapUpdate(cb, data, eve, subscriptionRecord);
            return;
          }
          cb.call(this, data, this._lastKey(), { put: data, get: this._lastKey() }, eve);
        });
        subscriptionRecord.cancel = () => {
          this._clearAllNestedMapSubscriptions(subscriptionRecord);
          subscription.cancel();
        };
        this._listeners.add(subscriptionRecord);
      })
      .catch((error) => console.error("gun.on failed", error));
    return this;
  }

  off() {
    for (const listener of this._listeners) {
      listener.interestOff?.();
      this._clearAllNestedMapSubscriptions(listener);
      listener.cancel();
    }
    this._listeners.clear();
    return this;
  }

  map(cb) {
    return this._clone({
      mode: "map",
      mapTransform: typeof cb === "function" ? cb : null,
      source: this,
    });
  }

  open(cb, opt = {}) {
    let timer = null;
    let latestData = null;
    let latestKey = this._lastKey();
    let startedAt = nowMillis();
    return this.on((data, key, _msg, eve) => {
      latestData = data;
      latestKey = key;
      clearTimeout(timer);
      const waitMs = opt.wait ?? 9;
      const maxWaitMs = opt.maxWait ?? Math.max(waitMs * 8, 99);
      const fire = () => {
        const currentData = this._readCurrent();
        if (currentData !== undefined) {
          latestData = currentData;
        }
        const placeholder = isPlaceholderOpenValue(latestData);
        if (placeholder && nowMillis() - startedAt < maxWaitMs) {
          timer = setTimeout(fire, waitMs);
          return;
        }
        cb.call(this, normalizeOpenValue(latestData, opt), latestKey, opt, eve);
        if (opt.off) {
          eve.off();
        }
      };
      startedAt = nowMillis();
      timer = setTimeout(fire, waitMs);
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
    this._runtime._awaitReady()
      .then(async () => {
        await this._runtime._ensureHydrated(this, { force: true });
        const data = this._readCurrent();
        if (data == null) {
          cb.call(this, this._lastKey(), noop);
        }
      })
      .catch((error) => console.error("gun.not failed", error));
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
    this._activeInterests = new Map();
    this._nextInterestId = 0;
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

  _awaitReady() {
    return this.ready;
  }

  _flushNetwork() {
    for (const connection of this._damConnections) {
      connection.flushPending();
    }
  }

  _registerInterest(chain, options = {}) {
    const root = chain._interestRoot();
    if (!root) {
      return noop;
    }
    const id = `interest-${this._nextInterestId++}`;
    this._activeInterests.set(id, {
      id,
      root,
      mode: options.mode ?? chain._mode ?? "value",
    });
    for (const connection of this._damConnections) {
      connection.requestRootSnapshotFromAnyPeer(root).catch((error) => {
        console.warn("interest hydration failed", error);
      });
    }
    return () => {
      this._activeInterests.delete(id);
    };
  }

  async _ensureHydrated(chain, options = {}) {
    const root = chain._interestRoot();
    if (!root || this._damConnections.length === 0) {
      return false;
    }
    const attempts = this._damConnections
      .map((connection) => connection.requestRootSnapshotFromAnyPeer(root, options))
      .filter(Boolean);
    if (attempts.length === 0) {
      return false;
    }
    try {
      const result = await Promise.race([
        Promise.any(attempts),
        delay(options.timeoutMs ?? 250),
      ]);
      return result === true;
    } catch {
      return false;
    }
  }

  async _replayLiveInterestsToPeer(connection, peerId) {
    const roots = [...new Set([...this._activeInterests.values()].map((interest) => interest.root))];
    for (const root of roots) {
      try {
        await connection.requestRootSnapshot(peerId, root);
      } catch (error) {
        console.warn("peer hydration failed", error);
      }
    }
  }

  async _waitForVisibleValue(chain, expectedValue, timeoutMs = 250) {
    const deadline = nowMillis() + timeoutMs;
    while (nowMillis() < deadline) {
      const current = chain._readCurrent();
      if (expectedValue == null) {
        if (current == null) {
          return true;
        }
      } else if (sameJsonValue(current, expectedValue)) {
        return true;
      } else if (isPlainObject(expectedValue) && isPlainObject(current)) {
        const expectedKeys = Object.keys(expectedValue);
        if (
          expectedKeys.length > 0 &&
          expectedKeys.every((key) => sameJsonValue(current[key], expectedValue[key]))
        ) {
          return true;
        }
      } else if (Array.isArray(expectedValue) && Array.isArray(current)) {
        if (current.length >= expectedValue.length && current.length > 0) {
          return true;
        }
      }
      await delay(10);
    }
    return false;
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
    const recommendations = this._damConnections.reduce(
      (count, connection) => count + connection.recommendationCount(),
      0,
    );
    return {
      replicaId: this._db.replicaId(),
      peers,
      recommendations,
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
