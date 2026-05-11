import * as Moq from "@moq/lite";

export interface PrimadbLike {
  replicaId(): string;
  pendingEnvelope(): unknown;
  drainPendingEnvelope(): unknown;
  drainPendingEnvelopeJson?(): string;
  drainPendingOperationsJson?(): string;
  applyEnvelope(envelope: unknown): number;
  applyOperationsJson?(payload: string): number;
}

export interface PrimadbMoqSyncPayload {
  type: "primadb.sync.v1";
  from: string;
  sentAt: number;
  envelope?: unknown;
  envelopeJson?: string;
}

export type PrimadbRouteTarget =
  | { kind: "broadcast" }
  | { kind: "peer"; value: string }
  | { kind: "topic"; value: string };

export type PrimadbRoutePayload =
  | { kind: "sync"; encoding: string; payload: unknown }
  | { kind: "presence"; peer: PrimadbPeerPresence }
  | { kind: "peer_exchange"; peers: PrimadbPeerRecommendation[] }
  | { kind: "batch"; items: PrimadbRouteBatchItem[] }
  | { kind: string; [key: string]: unknown };

export type PrimadbRouteBatchItem =
  | { kind: "sync"; encoding: string; payload: unknown }
  | { kind: "presence"; peer: PrimadbPeerPresence }
  | { kind: "peer_exchange"; peers: PrimadbPeerRecommendation[] }
  | { kind: string; [key: string]: unknown };

export interface PrimadbPeerPresence {
  peer_id: string;
  replica_id: string;
  transport: string;
  identity?: unknown;
  capabilities?: string[];
  topics?: string[];
  metadata?: Record<string, string>;
}

export interface PrimadbPeerRecommendation {
  peer: PrimadbPeerPresence;
  relay_urls?: string[];
  score?: number;
  discovered_at_millis?: number;
}

export interface PrimadbRouteEnvelope {
  route_id: string;
  from: string;
  channel: string;
  target: PrimadbRouteTarget;
  ttl: number;
  hops: number;
  issued_at_millis: number;
  reply_to?: string | null;
  content_hash?: string | null;
  seen_by: string[];
  payload: PrimadbRoutePayload;
}

export interface PrimadbMoqRoutePayload {
  type: "primadb.route.v1";
  from: string;
  sentAt: number;
  route: PrimadbRouteEnvelope;
}

export type PrimadbRouteHandler = (route: PrimadbRouteEnvelope) => void;

export interface PrimadbMoqSessionOptions {
  path: string;
  track?: string;
  channel?: string;
  peerId?: string;
  target?: PrimadbRouteTarget;
  intervalMs?: number;
  publish?: boolean;
  subscribe?: string[];
  closeConnection?: boolean;
}

export interface ConnectPrimadbMoqOptions extends PrimadbMoqSessionOptions {
  url: string | URL;
  websocketUrl?: string | URL;
  websocket?: boolean;
  webtransport?: WebTransportOptions;
  transport?: WebTransport;
}

export interface PrimadbMoqLoopbackOptions {
  publisherDb: PrimadbLike;
  subscriberDb: PrimadbLike;
  path: string;
  track?: string;
  channel?: string;
  intervalMs?: number;
  url?: string | URL;
  protocol?: string;
}

export interface PrimadbMoqLoopback {
  publisher: PrimadbMoqSession;
  subscriber: PrimadbMoqSession;
  flush(): Promise<number>;
  close(): void;
}

type MoqConnection = Awaited<ReturnType<typeof Moq.Connection.connect>>;
type MoqBroadcast = Moq.Broadcast;
type MoqTrack = ReturnType<MoqBroadcast["subscribe"]>;

const DEFAULT_ROUTE_TRACK = "routes";
const DEFAULT_CHANNEL = "primadb-sync";

function hasPendingOps(envelope: unknown): boolean {
  return Boolean(
    envelope &&
      typeof envelope === "object" &&
      Array.isArray((envelope as { ops?: unknown }).ops) &&
      (envelope as { ops: unknown[] }).ops.length > 0,
  );
}

function asPath(path: string): ReturnType<typeof Moq.Path.from> {
  return Moq.Path.from(path);
}

function nowMillis(): number {
  return Date.now();
}

function broadcastTarget(): PrimadbRouteTarget {
  return { kind: "broadcast" };
}

function stableJson(value: unknown): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value) ?? "null";
  }
  if (Array.isArray(value)) {
    return `[${value.map(stableJson).join(",")}]`;
  }
  const entries = Object.entries(value as Record<string, unknown>).sort(([left], [right]) =>
    left.localeCompare(right),
  );
  return `{${entries.map(([key, item]) => `${JSON.stringify(key)}:${stableJson(item)}`).join(",")}}`;
}

function contentHash(value: unknown): string {
  const text = stableJson(value);
  let hash = 0x811c9dc5;
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return `fnv1a32:${hash.toString(16).padStart(8, "0")}`;
}

function drainPendingEnvelopeJson(db: PrimadbLike): string {
  if (typeof db.drainPendingEnvelopeJson === "function") {
    return db.drainPendingEnvelopeJson();
  }
  if (typeof db.drainPendingOperationsJson === "function") {
    return db.drainPendingOperationsJson();
  }
  return JSON.stringify(db.drainPendingEnvelope());
}

function applyMoqPayload(db: PrimadbLike, message: PrimadbMoqSyncPayload): number {
  if (typeof message.envelopeJson === "string") {
    return db.applyEnvelope(JSON.parse(message.envelopeJson));
  }
  return db.applyEnvelope(message.envelope);
}

export function moqRuntimeSupport(): {
  webTransport: boolean;
  webSocket: boolean;
  websocketFallback: boolean;
} {
  return {
    webTransport: typeof globalThis.WebTransport === "function",
    webSocket: typeof globalThis.WebSocket === "function",
    websocketFallback: typeof globalThis.WebSocket === "function",
  };
}

export async function connectPrimadbMoq(
  db: PrimadbLike,
  options: ConnectPrimadbMoqOptions,
): Promise<PrimadbMoqSession> {
  const url = new URL(String(options.url));
  const connection = await Moq.Connection.connect(url, {
    transport: options.transport,
    webtransport: options.webtransport,
    websocket:
      options.websocket === false
        ? { enabled: false }
        : {
            enabled: true,
            url: options.websocketUrl ? new URL(String(options.websocketUrl)) : undefined,
          },
  });

  const session = new PrimadbMoqSession(db, connection, options);
  if (options.publish !== false) {
    session.publish();
  }
  for (const path of options.subscribe ?? []) {
    session.subscribe(path);
  }
  session.startAutoFlush();
  return session;
}

export class PrimadbMoqSession {
  readonly db: PrimadbLike;
  readonly connection: MoqConnection;
  readonly path: string;
  readonly track: string;
  readonly channel: string;
  readonly peerId: string;
  readonly intervalMs: number;

  #published?: MoqBroadcast;
  #publishTracks = new Set<MoqTrack>();
  #subscribedTracks = new Set<MoqTrack>();
  #routeHandlers = new Set<PrimadbRouteHandler>();
  #seenRoutes = new Set<string>();
  #knownPeers = new Map<string, PrimadbPeerPresence>();
  #recommendations = new Map<string, PrimadbPeerRecommendation>();
  #interval: ReturnType<typeof setInterval> | undefined;
  #closed = false;
  #closeConnection: boolean;
  #target: PrimadbRouteTarget;
  #nextRouteSeq = 0;

  constructor(db: PrimadbLike, connection: MoqConnection, options: PrimadbMoqSessionOptions) {
    this.db = db;
    this.connection = connection;
    this.path = options.path;
    this.track = options.track ?? DEFAULT_ROUTE_TRACK;
    this.channel = options.channel ?? DEFAULT_CHANNEL;
    this.peerId = options.peerId ?? `moq:${db.replicaId()}`;
    this.intervalMs = Math.max(25, options.intervalMs ?? 250);
    this.#closeConnection = options.closeConnection ?? false;
    this.#target = options.target ?? broadcastTarget();
  }

  publish(): MoqBroadcast {
    if (this.#published) {
      return this.#published;
    }

    const broadcast = new Moq.Broadcast();
    this.#published = broadcast;
    this.connection.publish(asPath(this.path), broadcast);
    void this.#serveRequestedTracks(broadcast);
    return broadcast;
  }

  subscribe(path: string = this.path): MoqTrack {
    const remote = this.connection.consume(asPath(path));
    const track = remote.subscribe(this.track, 0);
    this.#subscribedTracks.add(track);
    void this.#readTrack(track);
    return track;
  }

  startAutoFlush(): void {
    if (this.#interval || this.#closed) {
      return;
    }
    this.#interval = setInterval(() => {
      void this.flushPending().catch(() => {});
    }, this.intervalMs);
  }

  onRoute(handler: PrimadbRouteHandler): () => void {
    this.#routeHandlers.add(handler);
    return () => {
      this.#routeHandlers.delete(handler);
    };
  }

  knownPeers(): PrimadbPeerPresence[] {
    return [...this.#knownPeers.values()];
  }

  recommendedPeers(): PrimadbPeerRecommendation[] {
    return [...this.#recommendations.values()];
  }

  createRoute(
    payload: PrimadbRoutePayload,
    target: PrimadbRouteTarget = this.#target,
    replyTo?: string | null,
  ): PrimadbRouteEnvelope {
    this.#nextRouteSeq += 1;
    return {
      route_id: `${this.peerId}/route/${this.#nextRouteSeq.toString(16)}`,
      from: this.peerId,
      channel: this.channel,
      target,
      ttl: 6,
      hops: 0,
      issued_at_millis: nowMillis(),
      reply_to: replyTo ?? null,
      content_hash: contentHash(payload),
      seen_by: [this.peerId],
      payload,
    };
  }

  sendRoute(route: PrimadbRouteEnvelope): number {
    if (this.#closed || this.#publishTracks.size === 0) {
      return 0;
    }

    const payload: PrimadbMoqRoutePayload = {
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

  announcePresence(): number {
    return this.sendRoute(
      this.createRoute({
        kind: "presence",
        peer: {
          peer_id: this.peerId,
          replica_id: this.db.replicaId(),
          transport: "moq",
          capabilities: ["sync", "ack", "routing", "batch", "peer_exchange"],
          topics: [this.channel],
          metadata: {
            moq_path: this.path,
            moq_track: this.track,
          },
        },
      }),
    );
  }

  async flushPending(): Promise<number> {
    if (this.#closed || this.#publishTracks.size === 0) {
      return 0;
    }

    const pending = this.db.pendingEnvelope();
    if (!hasPendingOps(pending)) {
      return 0;
    }

    const envelopeJson = drainPendingEnvelopeJson(this.db);
    if (!hasPendingOps(JSON.parse(envelopeJson))) {
      return 0;
    }

    const envelope = JSON.parse(envelopeJson) as { from?: string; ops?: unknown[] };
    const messageId = `${this.peerId}/sync/${(this.#nextRouteSeq + 1).toString(16)}`;
    return this.sendRoute(
      this.createRoute({
        kind: "sync",
        encoding: "sync_frame",
        payload: {
          type: "sync",
          from: envelope.from ?? this.db.replicaId(),
          message_id: messageId,
          ops: envelope.ops ?? [],
        },
      }),
    );
  }

  close(): void {
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
    this.#published?.close();
    if (this.#closeConnection) {
      this.connection.close();
    }
  }

  async #serveRequestedTracks(broadcast: MoqBroadcast): Promise<void> {
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

  async #readTrack(track: MoqTrack): Promise<void> {
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

  #acceptRoute(route: PrimadbRouteEnvelope): void {
    if (
      route.from === this.peerId ||
      route.seen_by?.includes(this.peerId) ||
      this.#seenRoutes.has(route.route_id) ||
      !routeTargetsPeer(route, this.peerId, this.channel)
    ) {
      return;
    }
    this.#seenRoutes.add(route.route_id);
    if (this.#seenRoutes.size > 4096) {
      const oldest = this.#seenRoutes.values().next().value;
      if (oldest) {
        this.#seenRoutes.delete(oldest);
      }
    }

    for (const handler of this.#routeHandlers) {
      handler(route);
    }
    this.#acceptRoutePayload(route.from, route.route_id, route.payload);
  }

  #acceptRoutePayload(from: string, routeId: string, payload: PrimadbRoutePayload): void {
    if (payload.kind === "batch" && Array.isArray((payload as { items?: unknown }).items)) {
      for (const item of (payload as { items: PrimadbRouteBatchItem[] }).items) {
        this.#acceptRoutePayload(from, routeId, item as PrimadbRoutePayload);
      }
      return;
    }
    if (payload.kind === "presence" && isPeerPresence((payload as { peer?: unknown }).peer)) {
      const peer = (payload as { peer: PrimadbPeerPresence }).peer;
      if (peer.metadata?.state === "offline") {
        this.#knownPeers.delete(peer.peer_id);
      } else {
        this.#knownPeers.set(peer.peer_id, peer);
      }
      return;
    }
    if (payload.kind === "peer_exchange") {
      for (const recommendation of (payload as { peers?: PrimadbPeerRecommendation[] }).peers ?? []) {
        if (isPeerPresence(recommendation.peer)) {
          this.#recommendations.set(recommendation.peer.peer_id, recommendation);
        }
      }
      return;
    }
    if (payload.kind !== "sync") {
      return;
    }

    const sync = payload as { encoding?: string; payload?: unknown };
    if (sync.encoding !== "sync_frame" || !isSyncFrame(sync.payload)) {
      return;
    }
    if (sync.payload.type === "sync") {
      const applied = this.db.applyEnvelope({ from: sync.payload.from, ops: sync.payload.ops });
      this.sendRoute(
        this.createRoute(
          {
            kind: "sync",
            encoding: "sync_frame",
            payload: {
              type: "ack",
              from: this.db.replicaId(),
              message_id: sync.payload.message_id,
              applied,
            },
          },
          { kind: "peer", value: from },
          routeId,
        ),
      );
    }
  }
}

export async function createPrimadbMoqLoopback(
  options: PrimadbMoqLoopbackOptions,
): Promise<PrimadbMoqLoopback> {
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

function routeTargetsPeer(route: PrimadbRouteEnvelope, peerId: string, channel: string): boolean {
  switch (route.target.kind) {
    case "broadcast":
      return route.channel === channel;
    case "topic":
      return route.target.value === channel || route.channel === route.target.value;
    case "peer":
      return route.target.value === peerId;
    default:
      return false;
  }
}

function isPrimadbRouteEnvelope(value: unknown): value is PrimadbRouteEnvelope {
  return Boolean(
    value &&
      typeof value === "object" &&
      typeof (value as { route_id?: unknown }).route_id === "string" &&
      typeof (value as { from?: unknown }).from === "string" &&
      typeof (value as { channel?: unknown }).channel === "string" &&
      typeof (value as { payload?: unknown }).payload === "object",
  );
}

function isPrimadbMoqRoutePayload(value: unknown): value is PrimadbMoqRoutePayload {
  return Boolean(
    value &&
      typeof value === "object" &&
      (value as { type?: unknown }).type === "primadb.route.v1" &&
      typeof (value as { from?: unknown }).from === "string" &&
      isPrimadbRouteEnvelope((value as { route?: unknown }).route),
  );
}

function isPrimadbMoqSyncPayload(value: unknown): value is PrimadbMoqSyncPayload {
  return Boolean(
    value &&
      typeof value === "object" &&
      (value as { type?: unknown }).type === "primadb.sync.v1" &&
      typeof (value as { from?: unknown }).from === "string" &&
      ("envelopeJson" in value || "envelope" in value),
  );
}

function isPeerPresence(value: unknown): value is PrimadbPeerPresence {
  return Boolean(
    value &&
      typeof value === "object" &&
      typeof (value as { peer_id?: unknown }).peer_id === "string" &&
      typeof (value as { replica_id?: unknown }).replica_id === "string" &&
      typeof (value as { transport?: unknown }).transport === "string",
  );
}

function isSyncFrame(value: unknown): value is
  | { type: "sync"; from: string; message_id: string; ops: unknown[] }
  | { type: "ack"; from: string; message_id: string; applied: number } {
  return Boolean(
    value &&
      typeof value === "object" &&
      (((value as { type?: unknown }).type === "sync" &&
        typeof (value as { from?: unknown }).from === "string" &&
        typeof (value as { message_id?: unknown }).message_id === "string" &&
        Array.isArray((value as { ops?: unknown }).ops)) ||
        ((value as { type?: unknown }).type === "ack" &&
          typeof (value as { from?: unknown }).from === "string" &&
          typeof (value as { message_id?: unknown }).message_id === "string")),
  );
}

function createMockTransportPair(protocol = ""): { client: WebTransport; server: WebTransport } {
  const client = new MockTransport(protocol);
  const server = new MockTransport(protocol);
  client.setPeer(server);
  server.setPeer(client);
  return {
    client: client as unknown as WebTransport,
    server: server as unknown as WebTransport,
  };
}

function streamPair(): TransformStream<Uint8Array, Uint8Array> {
  return new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
      controller.enqueue(new Uint8Array(chunk));
    },
  });
}

class MockTransport {
  readonly protocol: string;
  readonly ready = Promise.resolve();
  readonly closed: Promise<WebTransportCloseInfo>;
  readonly incomingBidirectionalStreams: ReadableStream<unknown>;
  readonly incomingUnidirectionalStreams: ReadableStream<unknown>;
  readonly datagrams = {
    readable: new ReadableStream<Uint8Array>(),
    writable: new WritableStream<Uint8Array>(),
    incomingHighWaterMark: 0,
    outgoingHighWaterMark: 0,
    incomingMaxAge: null,
    outgoingMaxAge: null,
    maxDatagramSize: 0,
  };
  readonly congestionControl = "default";
  readonly reliability = "supports-unreliable";

  #peer?: MockTransport;
  #closeResolve!: (info: WebTransportCloseInfo) => void;
  #bidiController!: ReadableStreamDefaultController<unknown>;
  #uniController!: ReadableStreamDefaultController<unknown>;

  constructor(protocol: string) {
    this.protocol = protocol;
    this.closed = new Promise<WebTransportCloseInfo>((resolve) => {
      this.#closeResolve = resolve;
    });
    this.incomingBidirectionalStreams = new ReadableStream<unknown>({
      start: (controller) => {
        this.#bidiController = controller;
      },
    });
    this.incomingUnidirectionalStreams = new ReadableStream<unknown>({
      start: (controller) => {
        this.#uniController = controller;
      },
    });
  }

  setPeer(peer: MockTransport): void {
    this.#peer = peer;
  }

  async createBidirectionalStream(): Promise<{ readable: ReadableStream; writable: WritableStream }> {
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

  async createUnidirectionalStream(): Promise<WritableStream> {
    if (!this.#peer) {
      throw new Error("mock MoQ transport has no peer");
    }
    const stream = streamPair();
    this.#peer.#uniController.enqueue(stream.readable);
    return stream.writable;
  }

  close(info?: WebTransportCloseInfo): void {
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

  async getStats(): Promise<Record<string, never>> {
    return {};
  }
}
