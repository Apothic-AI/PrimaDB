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

export interface PrimadbMoqSessionOptions {
  path: string;
  track?: string;
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
    if (typeof db.applyOperationsJson === "function") {
      return db.applyOperationsJson(message.envelopeJson);
    }
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
  readonly intervalMs: number;

  #published?: MoqBroadcast;
  #publishTracks = new Set<MoqTrack>();
  #subscribedTracks = new Set<MoqTrack>();
  #interval: ReturnType<typeof setInterval> | undefined;
  #closed = false;
  #closeConnection: boolean;

  constructor(db: PrimadbLike, connection: MoqConnection, options: PrimadbMoqSessionOptions) {
    this.db = db;
    this.connection = connection;
    this.path = options.path;
    this.track = options.track ?? "ops";
    this.intervalMs = Math.max(25, options.intervalMs ?? 250);
    this.#closeConnection = options.closeConnection ?? false;
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

    const payload: PrimadbMoqSyncPayload = {
      type: "primadb.sync.v1",
      from: this.db.replicaId(),
      sentAt: nowMillis(),
      envelopeJson,
    };

    let sent = 0;
    for (const track of this.#publishTracks) {
      track.writeJson(payload);
      sent += 1;
    }
    return sent;
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
        if (!isPrimadbMoqSyncPayload(message)) {
          continue;
        }
        if (message.from === this.db.replicaId()) {
          continue;
        }
        applyMoqPayload(this.db, message);
      }
    } finally {
      this.#subscribedTracks.delete(track);
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
    intervalMs: options.intervalMs,
    closeConnection: true,
  });
  const subscriber = new PrimadbMoqSession(options.subscriberDb, subscriberConnection, {
    path: options.path,
    track: options.track,
    intervalMs: options.intervalMs,
    closeConnection: true,
  });

  publisher.publish();
  subscriber.subscribe(options.path);
  publisher.startAutoFlush();

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

function isPrimadbMoqSyncPayload(value: unknown): value is PrimadbMoqSyncPayload {
  return Boolean(
    value &&
      typeof value === "object" &&
      (value as { type?: unknown }).type === "primadb.sync.v1" &&
      typeof (value as { from?: unknown }).from === "string" &&
      ("envelopeJson" in value || "envelope" in value),
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
