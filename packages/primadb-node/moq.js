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

export function moqRuntimeSupport() {
  return {
    webTransport: typeof globalThis.WebTransport === "function",
    webSocket: typeof globalThis.WebSocket === "function",
    websocketFallback: typeof globalThis.WebSocket === "function",
  };
}

export async function connectPrimadbMoq(db, options) {
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
  #published;
  #publishTracks = new Set();
  #subscribedTracks = new Set();
  #interval;
  #closed = false;
  #closeConnection;

  constructor(db, connection, options) {
    this.db = db;
    this.connection = connection;
    this.path = options.path;
    this.track = options.track ?? "ops";
    this.intervalMs = Math.max(25, options.intervalMs ?? 250);
    this.#closeConnection = options.closeConnection ?? false;
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
    const remote = this.connection.consume(asPath(path));
    const track = remote.subscribe(this.track, 0);
    this.#subscribedTracks.add(track);
    void this.#readTrack(track);
    return track;
  }

  startAutoFlush() {
    if (this.#interval || this.#closed) {
      return;
    }
    this.#interval = setInterval(() => {
      void this.flushPending().catch(() => {});
    }, this.intervalMs);
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

    const payload = {
      type: "primadb.sync.v1",
      from: this.db.replicaId(),
      sentAt: Date.now(),
      envelopeJson,
    };

    let sent = 0;
    for (const track of this.#publishTracks) {
      track.writeJson(payload);
      sent += 1;
    }
    return sent;
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
    this.#published?.close();
    if (this.#closeConnection) {
      this.connection.close();
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

function isPrimadbMoqSyncPayload(value) {
  return Boolean(
    value &&
      typeof value === "object" &&
      value.type === "primadb.sync.v1" &&
      typeof value.from === "string" &&
      ("envelopeJson" in value || "envelope" in value),
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
