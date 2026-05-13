import * as Moq from "@moq/lite";

function hasPendingOps(envelope) {
  return Boolean(
    envelope &&
      typeof envelope === "object" &&
      Array.isArray(envelope.ops) &&
      envelope.ops.length > 0,
  );
}

function asPath(path) {
  return Moq.Path.from(path);
}

const DEFAULT_ROUTE_TRACK = "routes";
const DEFAULT_CHANNEL = "primadb-sync";

function nowMillis() {
  return Date.now();
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function broadcastTarget() {
  return { kind: "broadcast" };
}

function stableJson(value) {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value) ?? "null";
  }
  if (Array.isArray(value)) {
    return `[${value.map(stableJson).join(",")}]`;
  }
  const entries = Object.entries(value).sort(([left], [right]) => left.localeCompare(right));
  return `{${entries.map(([key, item]) => `${JSON.stringify(key)}:${stableJson(item)}`).join(",")}}`;
}

function contentHash(value) {
  const text = stableJson(value);
  let hash = 0x811c9dc5;
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return `fnv1a32:${hash.toString(16).padStart(8, "0")}`;
}

function drainPendingEnvelopeJson(db) {
  if (typeof db.drainPendingEnvelopeJson === "function") {
    return db.drainPendingEnvelopeJson();
  }
  return JSON.stringify(db.drainPendingEnvelope());
}

function applyMoqPayload(db, message) {
  if (typeof message.envelopeJson === "string") {
    if (typeof db.applyOperationsJson === "function") {
      return db.applyOperationsJson(message.envelopeJson);
    }
    return db.applyEnvelope(JSON.parse(message.envelopeJson));
  }
  return db.applyEnvelope(message.envelope);
}

function applySyncFramePayload(db, frame) {
  if (typeof frame.envelopeJson === "string") {
    if (typeof db.applyOperationsJson === "function") {
      return db.applyOperationsJson(frame.envelopeJson);
    }
    return db.applyEnvelope(JSON.parse(frame.envelopeJson));
  }
  if (typeof db.applyOperationsJson === "function") {
    return db.applyOperationsJson(
      JSON.stringify({
        type: "sync",
        from: frame.from,
        message_id: frame.message_id,
        ops: frame.ops ?? [],
      }),
    );
  }
  return db.applyEnvelope({ from: frame.from, ops: frame.ops ?? [] });
}

export function moqRuntimeSupport() {
  return {
    webTransport: typeof globalThis.WebTransport === "function",
    nodeWebTransportProvider: true,
    webSocket: typeof globalThis.WebSocket === "function",
    websocketFallback: typeof globalThis.WebSocket === "function",
  };
}

export async function connectPrimadbMoq(db, options) {
  const url = new URL(String(options.url));
  const ownsTransport = !options.transport;
  const transport = options.transport ?? (await createDefaultNodeWebTransport(url, options));
  let connection;
  try {
    connection = await Moq.Connection.connect(url, {
      transport,
      webtransport: options.webtransport,
      websocket:
        options.websocket === false
          ? { enabled: false }
          : {
              enabled: true,
              url: options.websocketUrl ? new URL(String(options.websocketUrl)) : undefined,
            },
    });
  } catch (error) {
    if (ownsTransport) {
      try {
        transport?.close();
      } catch {}
    }
    throw error;
  }

  const session = new PrimadbMoqSession(db, connection, {
    ...options,
    closeConnection: options.closeConnection ?? true,
  });
  if (options.publish !== false) {
    session.publish();
  }
  for (const path of options.subscribe ?? []) {
    session.subscribe(path);
  }
  session.startAutoFlush();
  return session;
}

async function createDefaultNodeWebTransport(url, options) {
  if (
    options.nodeWebTransport === false ||
    options.webtransport === false ||
    typeof globalThis.WebTransport === "function" ||
    !/^https?:$/.test(url.protocol)
  ) {
    return undefined;
  }

  const { WebTransport } = await import("@webtransport-bun/webtransport");
  const transport = new WebTransport(String(url), normalizeNodeWebTransportOptions(options));
  transport.closed.catch(() => {});
  await transport.ready;
  return transport;
}

function normalizeNodeWebTransportOptions(options) {
  const source =
    isRecord(options.nodeWebTransportOptions)
      ? options.nodeWebTransportOptions
      : isRecord(options.webtransport)
        ? options.webtransport
        : {};
  const normalized = { ...source };
  if (options.tlsDisableVerify === true || envFlag("PRIMADB_MOQ_TLS_DISABLE_VERIFY")) {
    normalized.tls = {
      ...(isRecord(normalized.tls) ? normalized.tls : {}),
      insecureSkipVerify: true,
    };
  }
  return normalized;
}

function envFlag(name) {
  const value = globalThis.process?.env?.[name];
  return value === "1" || value === "true" || value === "yes";
}

function isRecord(value) {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}

function closeConnectionSoon(connection) {
  const timer = setTimeout(() => {
    try {
      connection.close();
    } catch {}
  }, 250);
  timer.unref?.();
}

export const PRIMADB_APPLICATION_STREAM_NAMESPACE = "primadb.applicationStream";
export const PRIMADB_APPLICATION_STREAM_PROTOCOL_V1 = "primadb.applicationStream.v1";

const DEFAULT_OVERLAY_POLICY = {
  preferredTransports: ["web_rtc", "moq", "web_socket", "broadcast_channel", "in_memory"],
  sendMode: "first_success",
  directFirst: true,
  allowDirect: true,
  allowRelay: true,
  requireDirect: false,
};

export class PrimadbApplicationRouteSubscription {
  #queue = [];
  #waiters = [];
  #closed = false;
  #onClose;

  constructor(filter, onClose) {
    this.filter = filter;
    this.#onClose = onClose;
  }

  next() {
    if (this.#queue.length > 0) {
      return Promise.resolve(this.#queue.shift() ?? null);
    }
    if (this.#closed) {
      return Promise.resolve(null);
    }
    return new Promise((resolve) => {
      this.#waiters.push(resolve);
    });
  }

  tryNext() {
    return this.#queue.shift() ?? null;
  }

  drain() {
    const events = this.#queue;
    this.#queue = [];
    return events;
  }

  close() {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    this.#onClose();
    for (const waiter of this.#waiters.splice(0)) {
      waiter(null);
    }
    this.#queue = [];
  }

  enqueue(event) {
    if (this.#closed || !applicationRouteMatchesFilter(event, this.filter)) {
      return;
    }
    const waiter = this.#waiters.shift();
    if (waiter) {
      waiter(event);
      return;
    }
    this.#queue.push(event);
    trimQueue(this.#queue);
  }
}

export class PrimadbRouteOverlaySession {
  #policy;
  #underlays = new Map();
  #seenRoutes = new Set();
  #seenApplicationEvents = new Set();
  #routeSeq = 0;
  #applicationEvents = [];
  #applicationSubscriptions = new Set();
  #applicationWaiters = [];
  #streamBuffers = new Map();
  #streamEvents = [];

  constructor(options) {
    this.peerId = options.peerId;
    this.channel = options.channel ?? DEFAULT_CHANNEL;
    this.ttl = options.ttl ?? 6;
    this.#policy = normalizeOverlayPolicy(options.policy);
  }

  policy() {
    return { ...this.#policy, preferredTransports: [...this.#policy.preferredTransports] };
  }

  setPolicy(policy) {
    this.#policy = normalizeOverlayPolicy(policy);
  }

  addUnderlay(underlay) {
    this.#underlays.set(underlay.info().id, underlay);
  }

  removeUnderlay(id) {
    const underlay = this.#underlays.get(id);
    if (!underlay) {
      return null;
    }
    this.#underlays.delete(id);
    underlay.close?.();
    return normalizeUnderlayInfo(underlay.info());
  }

  underlays() {
    return [...this.#underlays.values()].map((underlay) => normalizeUnderlayInfo(underlay.info()));
  }

  createRoute(payload, target = broadcastTarget(), replyTo = null) {
    this.#routeSeq += 1;
    return {
      route_id: `${this.peerId}/overlay/${this.#routeSeq.toString(16)}`,
      from: this.peerId,
      channel: this.channel,
      target,
      ttl: this.ttl,
      hops: 0,
      issued_at_millis: nowMillis(),
      reply_to: replyTo,
      content_hash: contentHash(payload),
      seen_by: [this.peerId],
      payload,
    };
  }

  async publishApplication(message, target = broadcastTarget()) {
    return this.sendRoute(
      this.createRoute(
        {
          kind: "application",
          message: normalizeApplicationMessage(message),
        },
        target,
      ),
    );
  }

  async sendApplication(namespace, protocol, topic, body, metadata = {}, target = broadcastTarget()) {
    return this.publishApplication({ namespace, protocol, topic: topic ?? null, body, metadata }, target);
  }

  async sendRoute(route) {
    const attempts = [];
    const deliveredUnderlayIds = [];
    const failedUnderlayIds = [];
    const underlays = this.#orderedUnderlays();

    if (underlays.length === 0) {
      return {
        route,
        attempts,
        deliveredUnderlayIds,
        failedUnderlayIds,
        deliveredPeerIds: [],
        fallbackReason: "no connected underlay matched the route overlay policy",
        duplicateSuppressed: 0,
      };
    }

    for (const underlay of underlays) {
      const info = normalizeUnderlayInfo(underlay.info());
      try {
        const sent = await underlay.sendRoute(route);
        if (sent <= 0) {
          throw new Error("underlay did not accept the route");
        }
        deliveredUnderlayIds.push(info.id);
        attempts.push({
          underlay: info,
          attemptedAtMillis: nowMillis(),
          success: true,
        });
        if (this.#policy.sendMode === "first_success") {
          break;
        }
      } catch (error) {
        failedUnderlayIds.push(info.id);
        attempts.push({
          underlay: info,
          attemptedAtMillis: nowMillis(),
          success: false,
          message: error instanceof Error ? error.message : String(error),
        });
      }
    }

    return {
      route,
      attempts,
      deliveredUnderlayIds,
      failedUnderlayIds,
      deliveredPeerIds:
        route.target.kind === "peer" && deliveredUnderlayIds.length > 0 ? [route.target.value] : [],
      fallbackReason:
        deliveredUnderlayIds.length === 0
          ? "all attempted route underlays failed"
          : failedUnderlayIds.length > 0
            ? "one or more preferred underlays failed before a later underlay delivered"
            : null,
      duplicateSuppressed: 0,
    };
  }

  subscribeApplications(filter = {}) {
    let subscription;
    subscription = new PrimadbApplicationRouteSubscription(filter, () => {
      this.#applicationSubscriptions.delete(subscription);
    });
    this.#applicationSubscriptions.add(subscription);
    return subscription;
  }

  nextApplication(filter = {}) {
    const index = this.#applicationEvents.findIndex((event) =>
      applicationRouteMatchesFilter(event, filter),
    );
    if (index >= 0) {
      const [event] = this.#applicationEvents.splice(index, 1);
      return Promise.resolve(event ?? null);
    }
    return new Promise((resolve) => {
      this.#applicationWaiters.push({ filter, resolve });
    });
  }

  tryNextApplication(filter = {}) {
    const index = this.#applicationEvents.findIndex((event) =>
      applicationRouteMatchesFilter(event, filter),
    );
    if (index < 0) {
      return null;
    }
    const [event] = this.#applicationEvents.splice(index, 1);
    return event ?? null;
  }

  drainApplications(filter = {}) {
    const drained = [];
    this.#applicationEvents = this.#applicationEvents.filter((event) => {
      if (!applicationRouteMatchesFilter(event, filter)) {
        return true;
      }
      drained.push(event);
      return false;
    });
    return drained;
  }

  pump() {
    const report = {
      receivedRoutes: 0,
      deliveredApplicationRoutes: 0,
      deliveredStreamEvents: 0,
      duplicateSuppressed: 0,
      underlayIds: [],
    };
    for (const underlay of this.#underlays.values()) {
      const drainRoutes = underlay.drainRoutes;
      if (!drainRoutes) {
        continue;
      }
      const info = normalizeUnderlayInfo(underlay.info());
      const routes = drainRoutes();
      if (routes.length > 0) {
        report.underlayIds.push(info.id);
      }
      for (const route of routes) {
        report.receivedRoutes += 1;
        if (this.#acceptRoute(info, route)) {
          report.deliveredApplicationRoutes += 1;
          report.deliveredStreamEvents = this.#streamEvents.length;
        } else {
          report.duplicateSuppressed += 1;
        }
      }
    }
    return report;
  }

  async sendApplicationStream(options) {
    const streamId = `${this.peerId}/stream/${nowMillis().toString(16)}`;
    const target = options.target ?? broadcastTarget();
    const body = JSON.stringify(options.body);
    if (body === undefined) {
      throw new TypeError("application stream body must be JSON-serializable");
    }
    const chunks = chunkString(body, Math.max(1, options.maxChunkChars ?? 16 * 1024));
    const frameReports = [];

    frameReports.push(
      await this.publishApplication(
        streamFrameMessage({
          streamId,
          sequence: 0,
          kind: "open",
          namespace: options.namespace,
          protocol: options.protocol,
          topic: options.topic ?? null,
          finalChunk: chunks.length === 0,
          metadata: options.metadata ?? {},
        }),
        target,
      ),
    );

    for (let index = 0; index < chunks.length; index += 1) {
      frameReports.push(
        await this.publishApplication(
          streamFrameMessage({
            streamId,
            sequence: index + 1,
            kind: "data",
            namespace: options.namespace,
            protocol: options.protocol,
            topic: options.topic ?? null,
            chunk: chunks[index] ?? "",
            finalChunk: index + 1 === chunks.length,
            metadata: options.metadata ?? {},
          }),
          target,
        ),
      );
    }

    frameReports.push(
      await this.publishApplication(
        streamFrameMessage({
          streamId,
          sequence: chunks.length + 1,
          kind: "close",
          namespace: options.namespace,
          protocol: options.protocol,
          topic: options.topic ?? null,
          finalChunk: true,
          metadata: options.metadata ?? {},
        }),
        target,
      ),
    );

    return { streamId, frameReports };
  }

  drainStreamEvents() {
    const events = this.#streamEvents;
    this.#streamEvents = [];
    return events;
  }

  close() {
    for (const underlay of this.#underlays.values()) {
      underlay.close?.();
    }
    this.#underlays.clear();
    for (const waiter of this.#applicationWaiters.splice(0)) {
      waiter.resolve(null);
    }
    for (const subscription of [...this.#applicationSubscriptions]) {
      subscription.close();
    }
    this.#applicationEvents = [];
    this.#streamEvents = [];
    this.#streamBuffers.clear();
  }

  #orderedUnderlays() {
    const underlays = [...this.#underlays.values()].filter((underlay) => {
      const info = normalizeUnderlayInfo(underlay.info());
      return (
        info.connected &&
        (!this.#policy.requireDirect || info.direct) &&
        (this.#policy.allowDirect || !info.direct) &&
        (this.#policy.allowRelay || !info.relayRouted)
      );
    });
    underlays.sort((left, right) =>
      compareOverlayUnderlays(
        normalizeUnderlayInfo(left.info()),
        normalizeUnderlayInfo(right.info()),
        this.#policy,
      ),
    );
    return underlays;
  }

  #acceptRoute(underlay, route) {
    if (
      route.from === this.peerId ||
      route.seen_by?.includes(this.peerId) ||
      this.#seenRoutes.has(route.route_id) ||
      !routeTargetsPeer(route, new Set([this.peerId]), this.channel)
    ) {
      return false;
    }
    this.#seenRoutes.add(route.route_id);
    trimSeenSet(this.#seenRoutes);
    return this.#acceptRoutePayload(underlay, route, route.payload);
  }

  #acceptRoutePayload(underlay, route, payload) {
    if (payload.kind === "batch" && Array.isArray(payload.items)) {
      let delivered = false;
      for (const item of payload.items) {
        delivered = this.#acceptRoutePayload(underlay, route, item) || delivered;
      }
      return delivered;
    }
    if (payload.kind !== "application" || !isApplicationRouteMessage(payload.message)) {
      return false;
    }
    const message = normalizeApplicationMessage(payload.message);
    const eventKey = `${route.route_id}:${message.namespace}:${message.protocol}:${message.topic ?? ""}`;
    if (this.#seenApplicationEvents.has(eventKey)) {
      return false;
    }
    this.#seenApplicationEvents.add(eventKey);
    trimSeenSet(this.#seenApplicationEvents);
    const event = {
      routeId: route.route_id,
      from: route.from,
      channel: route.channel,
      target: route.target,
      issuedAtMillis: route.issued_at_millis,
      receivedAtMillis: nowMillis(),
      transport: underlay.transport,
      verifiedIdentity: null,
      context: {
        sourcePeerId: route.from,
        transport: underlay.transport,
        underlayId: underlay.id,
        direct: underlay.direct,
        relayRouted: underlay.relayRouted,
        gatewayRouted: false,
        gatewayPeerId: null,
        authStatus: "unknown",
        provenance: [underlay.transport],
      },
      message,
    };
    const streamEvent = this.#acceptStreamEvent(event);
    if (streamEvent) {
      this.#streamEvents.push(streamEvent);
      trimQueue(this.#streamEvents);
    }
    this.#enqueueApplicationRoute(event);
    return true;
  }

  #enqueueApplicationRoute(event) {
    const waiterIndex = this.#applicationWaiters.findIndex((waiter) =>
      applicationRouteMatchesFilter(event, waiter.filter),
    );
    if (waiterIndex >= 0) {
      const [waiter] = this.#applicationWaiters.splice(waiterIndex, 1);
      waiter?.resolve(event);
    } else {
      this.#applicationEvents.push(event);
      trimQueue(this.#applicationEvents);
    }
    for (const subscription of this.#applicationSubscriptions) {
      subscription.enqueue(event);
    }
  }

  #acceptStreamEvent(event) {
    if (
      event.message.namespace !== PRIMADB_APPLICATION_STREAM_NAMESPACE ||
      event.message.protocol !== PRIMADB_APPLICATION_STREAM_PROTOCOL_V1 ||
      !isApplicationStreamFrame(event.message.body)
    ) {
      return null;
    }
    return this.#acceptStreamFrame(event, event.message.body);
  }

  #acceptStreamFrame(event, frame) {
    if (frame.kind === "open") {
      this.#streamBuffers.set(frame.streamId, {
        from: event.from,
        transport: event.transport,
        namespace: frame.namespace,
        protocol: frame.protocol,
        topic: frame.topic ?? null,
        metadata: frame.metadata ?? {},
        chunks: new Map(),
      });
      return null;
    }
    if (frame.kind === "data") {
      const buffer =
        this.#streamBuffers.get(frame.streamId) ??
        {
          from: event.from,
          transport: event.transport,
          namespace: frame.namespace,
          protocol: frame.protocol,
          topic: frame.topic ?? null,
          metadata: frame.metadata ?? {},
          chunks: new Map(),
        };
      if (frame.chunk != null) {
        buffer.chunks.set(frame.sequence, frame.chunk);
      }
      if (frame.finalChunk) {
        buffer.finalSequence = frame.sequence;
      }
      this.#streamBuffers.set(frame.streamId, buffer);
      return this.#tryCompleteStream(frame.streamId);
    }
    if (frame.kind === "close") {
      const buffer = this.#streamBuffers.get(frame.streamId);
      if (buffer && buffer.finalSequence == null && frame.sequence > 0) {
        buffer.finalSequence = frame.sequence - 1;
      }
      return this.#tryCompleteStream(frame.streamId);
    }
    return null;
  }

  #tryCompleteStream(streamId) {
    const buffer = this.#streamBuffers.get(streamId);
    if (!buffer || buffer.finalSequence == null) {
      return null;
    }
    let joined = "";
    for (let sequence = 1; sequence <= buffer.finalSequence; sequence += 1) {
      const chunk = buffer.chunks.get(sequence);
      if (chunk == null) {
        return null;
      }
      joined += chunk;
    }
    this.#streamBuffers.delete(streamId);
    return {
      streamId,
      from: buffer.from,
      transport: buffer.transport,
      namespace: buffer.namespace,
      protocol: buffer.protocol,
      topic: buffer.topic,
      body: JSON.parse(joined),
      metadata: buffer.metadata,
    };
  }
}

export function primadbMoqOverlayUnderlay(id, session, options = {}) {
  const queue = [];
  const unsubscribe = session.onRoute((route) => {
    queue.push(route);
    trimQueue(queue, options.maxQueue ?? 1024);
  });
  return {
    info() {
      return {
        id,
        transport: "moq",
        direct: false,
        relayRouted: true,
        connected: true,
        priority: options.priority ?? 0,
        metadata: {
          path: session.path,
          track: session.track,
          channel: session.channel,
          ...(options.metadata ?? {}),
        },
      };
    },
    sendRoute(route) {
      return session.sendRoute(route);
    },
    drainRoutes() {
      return queue.splice(0);
    },
    close() {
      unsubscribe();
      queue.splice(0);
    },
  };
}

export class PrimadbMoqSession {
  #published;
  #publishTracks = new Set();
  #subscribedTracks = new Set();
  #subscribedPaths = new Set();
  #routeHandlers = new Set();
  #applicationSubscriptions = new Set();
  #applicationEvents = [];
  #applicationWaiters = [];
  #seenRoutes = new Set();
  #knownPeers = new Map();
  #recommendations = new Map();
  #acceptedPeerIds = new Set();
  #interval;
  #closed = false;
  #closeConnection;
  #target;
  #nextRouteSeq = 0;

  constructor(db, connection, options) {
    this.db = db;
    this.connection = connection;
    this.path = options.path;
    this.track = options.track ?? DEFAULT_ROUTE_TRACK;
    this.channel = options.channel ?? DEFAULT_CHANNEL;
    this.peerId = options.peerId ?? `moq:${db.replicaId()}`;
    this.intervalMs = Math.max(25, options.intervalMs ?? 250);
    this.retryIntervalMs = Math.max(100, options.retryIntervalMs ?? 1000);
    this.#closeConnection = options.closeConnection ?? false;
    this.#target = options.target ?? broadcastTarget();
    this.#acceptedPeerIds.add(this.peerId);
  }

  publish() {
    if (this.#published) {
      return this.#published;
    }
    const broadcast = new Moq.Broadcast();
    this.#published = broadcast;
    this.connection.publish(asPath(this.path), broadcast);
    void this.#serveRequestedTracks(broadcast);
    return broadcast;
  }

  subscribe(path = this.path) {
    if (this.#subscribedPaths.has(path)) {
      return path;
    }
    this.#subscribedPaths.add(path);
    void this.#subscribePath(path);
    return path;
  }

  startAutoFlush() {
    if (this.#interval || this.#closed) {
      return;
    }
    this.#interval = setInterval(() => {
      void this.flushPending().catch(() => {});
    }, this.intervalMs);
  }

  onRoute(handler) {
    this.#routeHandlers.add(handler);
    return () => {
      this.#routeHandlers.delete(handler);
    };
  }

  publishApplication(message, target = this.#target) {
    return this.sendRoute(
      this.createRoute(
        {
          kind: "application",
          message: normalizeApplicationMessage(message),
        },
        target,
      ),
    );
  }

  sendApplication(namespace, protocol, topic, body, metadata = {}, target = this.#target) {
    return this.publishApplication(
      {
        namespace,
        protocol,
        topic: topic ?? null,
        body,
        metadata,
      },
      target,
    );
  }

  subscribeApplications(filter = {}) {
    let subscription;
    subscription = new PrimadbApplicationRouteSubscription(filter, () => {
      this.#applicationSubscriptions.delete(subscription);
    });
    this.#applicationSubscriptions.add(subscription);
    return subscription;
  }

  nextApplication(filter = {}) {
    const index = this.#applicationEvents.findIndex((event) =>
      applicationRouteMatchesFilter(event, filter),
    );
    if (index >= 0) {
      const [event] = this.#applicationEvents.splice(index, 1);
      return Promise.resolve(event ?? null);
    }
    if (this.#closed) {
      return Promise.resolve(null);
    }
    return new Promise((resolve) => {
      this.#applicationWaiters.push({ filter, resolve });
    });
  }

  tryNextApplication(filter = {}) {
    const index = this.#applicationEvents.findIndex((event) =>
      applicationRouteMatchesFilter(event, filter),
    );
    if (index < 0) {
      return null;
    }
    const [event] = this.#applicationEvents.splice(index, 1);
    return event ?? null;
  }

  drainApplications(filter = {}) {
    const drained = [];
    this.#applicationEvents = this.#applicationEvents.filter((event) => {
      if (!applicationRouteMatchesFilter(event, filter)) {
        return true;
      }
      drained.push(event);
      return false;
    });
    return drained;
  }

  addAcceptedPeerId(peerId) {
    this.#acceptedPeerIds.add(peerId);
    return () => {
      if (peerId !== this.peerId) {
        this.#acceptedPeerIds.delete(peerId);
      }
    };
  }

  knownPeers() {
    return [...this.#knownPeers.values()];
  }

  recommendedPeers() {
    return [...this.#recommendations.values()];
  }

  createRoute(payload, target = this.#target, replyTo = null) {
    this.#nextRouteSeq += 1;
    return {
      route_id: `${this.peerId}/route/${this.#nextRouteSeq.toString(16)}`,
      from: this.peerId,
      channel: this.channel,
      target,
      ttl: 6,
      hops: 0,
      issued_at_millis: nowMillis(),
      reply_to: replyTo,
      content_hash: contentHash(payload),
      seen_by: [this.peerId],
      payload,
    };
  }

  sendRoute(route) {
    if (this.#closed || this.#publishTracks.size === 0) {
      return 0;
    }

    const payload = {
      type: "primadb.route.v1",
      from: this.peerId,
      sentAt: nowMillis(),
      route,
    };

    let sent = 0;
    for (const track of this.#publishTracks) {
      track.writeJson(payload);
      sent += 1;
    }
    return sent;
  }

  announcePresence() {
    return this.sendRoute(
      this.createRoute({
        kind: "presence",
        peer: {
          peer_id: this.peerId,
          replica_id: this.db.replicaId(),
          transport: "moq",
          capabilities: ["sync", "ack", "routing", "batch", "peer_exchange", "application_routes"],
          topics: [this.channel],
          metadata: {
            moq_path: this.path,
            moq_track: this.track,
          },
        },
      }),
    );
  }

  async flushPending() {
    if (this.#closed || this.#publishTracks.size === 0) {
      return 0;
    }
    if (!hasPendingOps(this.db.pendingEnvelope())) {
      return 0;
    }

    const envelopeJson = drainPendingEnvelopeJson(this.db);
    if (!hasPendingOps(JSON.parse(envelopeJson))) {
      return 0;
    }

    const envelope = JSON.parse(envelopeJson);
    const messageId = `${this.peerId}/sync/${(this.#nextRouteSeq + 1).toString(16)}`;
    return this.sendRoute(
      this.createRoute({
        kind: "sync",
        encoding: "sync_frame",
        payload: {
          type: "sync",
          from: envelope.from ?? this.db.replicaId(),
          message_id: messageId,
          envelopeJson,
          ops: envelope.ops ?? [],
        },
      }),
    );
  }

  close() {
    this.#closed = true;
    if (this.#interval) {
      clearInterval(this.#interval);
      this.#interval = undefined;
    }
    for (const track of this.#publishTracks) {
      track.close();
    }
    for (const track of this.#subscribedTracks) {
      track.close();
    }
    for (const waiter of this.#applicationWaiters.splice(0)) {
      waiter.resolve(null);
    }
    for (const subscription of [...this.#applicationSubscriptions]) {
      subscription.close();
    }
    this.#applicationEvents = [];
    this.#published?.close();
    if (this.#closeConnection) {
      closeConnectionSoon(this.connection);
    }
  }

  async #subscribePath(path) {
    while (!this.#closed && this.#subscribedPaths.has(path)) {
      let track;
      try {
        const remote = this.connection.consume(asPath(path));
        track = remote.subscribe(this.track, 0);
        this.#subscribedTracks.add(track);
        const read = this.#readTrack(track);
        read.catch(() => {});
        await Promise.race([
          read,
          track.closed.then((error) => {
            if (error) {
              throw error;
            }
          }),
        ]);
      } catch (error) {
        if (!this.#closed) {
          console.warn(
            `PrimaDB MoQ subscribe failed for path=${path} track=${this.track}; retrying`,
            error,
          );
        }
      } finally {
        if (track) {
          this.#subscribedTracks.delete(track);
          try {
            track.close();
          } catch {}
        }
      }
      if (!this.#closed && this.#subscribedPaths.has(path)) {
        await wait(this.retryIntervalMs);
      }
    }
  }

  async #serveRequestedTracks(broadcast) {
    while (!this.#closed) {
      const request = await broadcast.requested();
      if (!request) {
        break;
      }
      if (request.track.name !== this.track) {
        request.track.close(new Error(`unsupported PrimaDB MoQ track: ${request.track.name}`));
        continue;
      }
      this.#publishTracks.add(request.track);
      void request.track.closed.finally(() => {
        this.#publishTracks.delete(request.track);
      });
      this.announcePresence();
      void this.flushPending().catch(() => {});
    }
  }

  async #readTrack(track) {
    try {
      while (!this.#closed) {
        const message = await track.readJson();
        if (!message) {
          break;
        }
        if (isPrimadbMoqRoutePayload(message)) {
          this.#acceptRoute(message.route);
        } else if (isPrimadbRouteEnvelope(message)) {
          this.#acceptRoute(message);
        } else if (isPrimadbMoqSyncPayload(message)) {
          if (message.from === this.db.replicaId()) {
            continue;
          }
          applyMoqPayload(this.db, message);
        }
      }
    } finally {
      this.#subscribedTracks.delete(track);
    }
  }

  #acceptRoute(route) {
    if (
      route.from === this.peerId ||
      route.seen_by?.includes(this.peerId) ||
      this.#seenRoutes.has(route.route_id) ||
      !routeTargetsPeer(route, this.#acceptedPeerIds, this.channel)
    ) {
      return;
    }
    this.#seenRoutes.add(route.route_id);
    if (this.#seenRoutes.size > 4096) {
      const [oldest] = this.#seenRoutes;
      this.#seenRoutes.delete(oldest);
    }

    for (const handler of this.#routeHandlers) {
      handler(route);
    }
    this.#acceptRoutePayload(route, route.payload);
  }

  #acceptRoutePayload(route, payload) {
    if (payload.kind === "batch" && Array.isArray(payload.items)) {
      for (const item of payload.items) {
        this.#acceptRoutePayload(route, item);
      }
      return;
    }
    if (payload.kind === "application" && isApplicationRouteMessage(payload.message)) {
      this.#enqueueApplicationRoute({
        routeId: route.route_id,
        from: route.from,
        channel: route.channel,
        target: route.target,
        issuedAtMillis: route.issued_at_millis,
        receivedAtMillis: nowMillis(),
        transport: "moq",
        verifiedIdentity: null,
        context: {
          sourcePeerId: route.from,
          transport: "moq",
          underlayId: this.path,
          direct: false,
          relayRouted: true,
          gatewayRouted: false,
          gatewayPeerId: null,
          authStatus: "unknown",
          provenance: ["moq"],
        },
        message: normalizeApplicationMessage(payload.message),
      });
      return;
    }
    if (payload.kind === "presence" && isPeerPresence(payload.peer)) {
      if (payload.peer.metadata?.state === "offline") {
        this.#knownPeers.delete(payload.peer.peer_id);
      } else {
        this.#knownPeers.set(payload.peer.peer_id, payload.peer);
      }
      return;
    }
    if (payload.kind === "peer_exchange") {
      for (const recommendation of payload.peers ?? []) {
        if (isPeerPresence(recommendation.peer)) {
          this.#recommendations.set(recommendation.peer.peer_id, recommendation);
        }
      }
      return;
    }
    if (payload.kind !== "sync") {
      return;
    }

    if (payload.encoding !== "sync_frame" || !isSyncFrame(payload.payload)) {
      return;
    }
    if (payload.payload.type === "sync") {
      const applied = applySyncFramePayload(this.db, payload.payload);
      this.sendRoute(
        this.createRoute(
          {
            kind: "sync",
            encoding: "sync_frame",
            payload: {
              type: "ack",
              from: this.db.replicaId(),
              message_id: payload.payload.message_id,
              applied,
            },
          },
          { kind: "peer", value: route.from },
          route.route_id,
        ),
      );
    }
  }

  #enqueueApplicationRoute(event) {
    const waiterIndex = this.#applicationWaiters.findIndex((waiter) =>
      applicationRouteMatchesFilter(event, waiter.filter),
    );
    if (waiterIndex >= 0) {
      const [waiter] = this.#applicationWaiters.splice(waiterIndex, 1);
      waiter?.resolve(event);
    } else {
      this.#applicationEvents.push(event);
      trimQueue(this.#applicationEvents);
    }
    for (const subscription of this.#applicationSubscriptions) {
      subscription.enqueue(event);
    }
  }
}

export async function createPrimadbMoqLoopback(options) {
  const url = new URL(String(options.url ?? "https://primadb.local/moq-loopback"));
  const pair = createMockTransportPair(options.protocol ?? "");
  const [subscriberConnection, publisherConnection] = await Promise.all([
    Moq.Connection.connect(url, { transport: pair.client }),
    Moq.Connection.accept(pair.server, url),
  ]);

  const publisher = new PrimadbMoqSession(options.publisherDb, publisherConnection, {
    path: options.path,
    track: options.track,
    channel: options.channel,
    intervalMs: options.intervalMs,
    closeConnection: true,
  });
  const subscriber = new PrimadbMoqSession(options.subscriberDb, subscriberConnection, {
    path: options.path,
    track: options.track,
    channel: options.channel,
    intervalMs: options.intervalMs,
    closeConnection: true,
  });

  publisher.publish();
  subscriber.subscribe(options.path);
  publisher.startAutoFlush();
  publisher.announcePresence();

  return {
    publisher,
    subscriber,
    flush: () => publisher.flushPending(),
    close() {
      publisher.close();
      subscriber.close();
    },
  };
}

function isPrimadbMoqSyncPayload(value) {
  return Boolean(
    value &&
      typeof value === "object" &&
      value.type === "primadb.sync.v1" &&
      typeof value.from === "string" &&
      ("envelopeJson" in value || "envelope" in value),
  );
}

function routeTargetsPeer(route, peerIds, channel) {
  switch (route.target?.kind) {
    case "broadcast":
      return route.channel === channel;
    case "topic":
      return route.target.value === channel || route.channel === route.target.value;
    case "peer":
      return peerIds.has(route.target.value);
    default:
      return false;
  }
}

function isPrimadbRouteEnvelope(value) {
  return Boolean(
    value &&
      typeof value === "object" &&
      typeof value.route_id === "string" &&
      typeof value.from === "string" &&
      typeof value.channel === "string" &&
      typeof value.payload === "object",
  );
}

function isPrimadbMoqRoutePayload(value) {
  return Boolean(
    value &&
      typeof value === "object" &&
      value.type === "primadb.route.v1" &&
      typeof value.from === "string" &&
      isPrimadbRouteEnvelope(value.route),
  );
}

function isPeerPresence(value) {
  return Boolean(
    value &&
      typeof value === "object" &&
      typeof value.peer_id === "string" &&
      typeof value.replica_id === "string" &&
      typeof value.transport === "string",
  );
}

function isApplicationRouteMessage(value) {
  return Boolean(
    value &&
      typeof value === "object" &&
      typeof value.namespace === "string" &&
      typeof value.protocol === "string" &&
      "body" in value,
  );
}

function normalizeApplicationMessage(message) {
  return {
    namespace: message.namespace,
    protocol: message.protocol,
    topic: message.topic ?? null,
    body: message.body,
    metadata: message.metadata ?? {},
  };
}

function applicationRouteMatchesFilter(event, filter) {
  return (
    (filter.namespace == null || filter.namespace === event.message.namespace) &&
    (filter.protocol == null || filter.protocol === event.message.protocol) &&
    (filter.topic == null || filter.topic === event.message.topic)
  );
}

function normalizeOverlayPolicy(policy = {}) {
  return {
    ...DEFAULT_OVERLAY_POLICY,
    ...policy,
    preferredTransports: policy.preferredTransports ?? DEFAULT_OVERLAY_POLICY.preferredTransports,
  };
}

function normalizeUnderlayInfo(info) {
  return {
    id: info.id,
    transport: info.transport,
    direct: info.direct ?? false,
    relayRouted: info.relayRouted ?? !info.direct,
    connected: info.connected ?? true,
    priority: info.priority ?? 0,
    metadata: info.metadata ?? {},
  };
}

function compareOverlayUnderlays(left, right, policy) {
  const leftPreferred = policy.preferredTransports.indexOf(left.transport);
  const rightPreferred = policy.preferredTransports.indexOf(right.transport);
  const leftRank = leftPreferred < 0 ? policy.preferredTransports.length + 16 : leftPreferred;
  const rightRank = rightPreferred < 0 ? policy.preferredTransports.length + 16 : rightPreferred;
  const leftDirect = policy.directFirst && left.direct ? 0 : 1;
  const rightDirect = policy.directFirst && right.direct ? 0 : 1;
  return (
    leftDirect - rightDirect ||
    leftRank - rightRank ||
    left.priority - right.priority ||
    left.id.localeCompare(right.id)
  );
}

function trimSeenSet(seen, max = 4096) {
  while (seen.size > max) {
    const oldest = seen.values().next().value;
    if (oldest == null) {
      break;
    }
    seen.delete(oldest);
  }
}

function chunkString(value, maxChars) {
  if (value.length === 0) {
    return [""];
  }
  const chunks = [];
  let current = "";
  for (const character of value) {
    current += character;
    if ([...current].length >= maxChars) {
      chunks.push(current);
      current = "";
    }
  }
  if (current.length > 0) {
    chunks.push(current);
  }
  return chunks;
}

function streamFrameMessage(frame) {
  return {
    namespace: PRIMADB_APPLICATION_STREAM_NAMESPACE,
    protocol: PRIMADB_APPLICATION_STREAM_PROTOCOL_V1,
    topic: frame.topic ?? null,
    body: frame,
    metadata: {},
  };
}

function isApplicationStreamFrame(value) {
  return Boolean(
    value &&
      typeof value === "object" &&
      typeof value.streamId === "string" &&
      typeof value.sequence === "number" &&
      typeof value.kind === "string" &&
      typeof value.namespace === "string" &&
      typeof value.protocol === "string",
  );
}

function trimQueue(queue, max = 1024) {
  while (queue.length > max) {
    queue.shift();
  }
}

function isSyncFrame(value) {
  return Boolean(
    value &&
      typeof value === "object" &&
      ((value.type === "sync" &&
        typeof value.from === "string" &&
        typeof value.message_id === "string" &&
        (Array.isArray(value.ops) || typeof value.envelopeJson === "string")) ||
        (value.type === "ack" &&
          typeof value.from === "string" &&
          typeof value.message_id === "string")),
  );
}

function createMockTransportPair(protocol = "") {
  const client = new MockTransport(protocol);
  const server = new MockTransport(protocol);
  client.setPeer(server);
  server.setPeer(client);
  return { client, server };
}

function streamPair() {
  return new TransformStream({
    transform(chunk, controller) {
      controller.enqueue(new Uint8Array(chunk));
    },
  });
}

class MockTransport {
  ready = Promise.resolve();
  datagrams = {
    readable: new ReadableStream(),
    writable: new WritableStream(),
    incomingHighWaterMark: 0,
    outgoingHighWaterMark: 0,
    incomingMaxAge: null,
    outgoingMaxAge: null,
    maxDatagramSize: 0,
  };
  congestionControl = "default";
  reliability = "supports-unreliable";
  #peer;
  #closeResolve;
  #bidiController;
  #uniController;

  constructor(protocol) {
    this.protocol = protocol;
    this.closed = new Promise((resolve) => {
      this.#closeResolve = resolve;
    });
    this.incomingBidirectionalStreams = new ReadableStream({
      start: (controller) => {
        this.#bidiController = controller;
      },
    });
    this.incomingUnidirectionalStreams = new ReadableStream({
      start: (controller) => {
        this.#uniController = controller;
      },
    });
  }

  setPeer(peer) {
    this.#peer = peer;
  }

  async createBidirectionalStream() {
    if (!this.#peer) {
      throw new Error("mock MoQ transport has no peer");
    }
    const outbound = streamPair();
    const inbound = streamPair();
    this.#peer.#bidiController.enqueue({
      readable: outbound.readable,
      writable: inbound.writable,
    });
    return {
      readable: inbound.readable,
      writable: outbound.writable,
    };
  }

  async createUnidirectionalStream() {
    if (!this.#peer) {
      throw new Error("mock MoQ transport has no peer");
    }
    const stream = streamPair();
    this.#peer.#uniController.enqueue(stream.readable);
    return stream.writable;
  }

  close(info) {
    const closeInfo = info ?? { closeCode: 0, reason: "" };
    this.#closeResolve(closeInfo);
    try {
      this.#bidiController.close();
    } catch {}
    try {
      this.#uniController.close();
    } catch {}
    if (this.#peer) {
      this.#peer.#closeResolve(closeInfo);
    }
  }

  async getStats() {
    return {};
  }
}
