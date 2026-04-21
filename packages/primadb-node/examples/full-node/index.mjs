#!/usr/bin/env node
import { mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { Primadb, RelayServer } from "../../index.js";

function parseArgs(argv) {
  const values = {
    room: "package-full-node",
    relayBind: "127.0.0.1:9010",
    relayUrl: null,
    iceServers: [],
    name: `node-full-${process.pid}`,
    title: null,
    message: "",
    durationMs: null,
  };

  for (let index = 2; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];
    if (arg === "--room" && next) {
      values.room = next;
      index += 1;
    } else if (arg === "--relay-bind" && next) {
      values.relayBind = next;
      index += 1;
    } else if (arg === "--relay-url" && next) {
      values.relayUrl = next;
      index += 1;
    } else if (arg === "--ice-server" && next) {
      values.iceServers.push(parseIceServerSpec(next));
      index += 1;
    } else if (arg === "--name" && next) {
      values.name = next;
      index += 1;
    } else if (arg === "--title" && next) {
      values.title = next;
      index += 1;
    } else if (arg === "--message" && next) {
      values.message = next;
      index += 1;
    } else if (arg === "--duration-ms" && next) {
      const parsed = Number.parseInt(next, 10);
      if (Number.isFinite(parsed) && parsed > 0) {
        values.durationMs = parsed;
      }
      index += 1;
    }
  }

  return values;
}

function parseIceServerSpec(spec) {
  const trimmed = String(spec).trim();
  if (trimmed.startsWith("{")) {
    const value = JSON.parse(trimmed);
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      throw new Error("--ice-server JSON must decode to an object");
    }
    return value;
  }
  if (trimmed.startsWith("stun:") || trimmed.startsWith("turn:") || trimmed.startsWith("turns:")) {
    return { urls: trimmed };
  }
  throw new Error(`invalid --ice-server value \`${trimmed}\`; use a STUN/TURN URL or JSON object`);
}

function defaultExampleIceServers() {
  return [{ urls: "stun:stun.cloudflare.com:3478" }];
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function stableStringify(value) {
  return JSON.stringify(value, null, 2);
}

function localRelayUrl(relayBind) {
  const lastColon = relayBind.lastIndexOf(":");
  if (lastColon === -1) {
    throw new Error(`invalid --relay-bind value \`${relayBind}\``);
  }
  const host = relayBind.slice(0, lastColon);
  const port = relayBind.slice(lastColon + 1);
  const normalizedHost = host === "0.0.0.0" ? "127.0.0.1" : host;
  return `ws://${normalizedHost}:${port}`;
}

const options = parseArgs(process.argv);
const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, ".data", "full-node", options.name);
mkdirSync(root, { recursive: true });

const relay = await RelayServer.listen({ bind: options.relayBind });
const relayUrl = options.relayUrl ?? relay.url();

const db = new Primadb(`node-full-${options.name}`);
db.openDurableStorage({
  kind: "segment_files",
  directory: root,
  journalRetention: 8,
});

const mesh = await db.connectMesh({
  room: options.room,
  relayUrl,
  retryIntervalMs: 750,
  iceServers: options.iceServers.length > 0 ? options.iceServers : defaultExampleIceServers(),
});
const notes = db.chain("full_nodes").field(options.room).field("notes");

if (options.message) {
  notes.set({
    author: options.name,
    title: options.title ?? `${options.name} ${new Date().toISOString().slice(11, 19)}`,
    body: options.message,
    role: "full-node",
    updated_at: Date.now(),
  });
  await mesh.flushPending();
}

let shuttingDown = false;
async function shutdown(exitCode = 0) {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  try {
    await mesh.close();
  } finally {
    await relay.close();
    process.exit(exitCode);
  }
}

process.on("SIGINT", () => {
  void shutdown(0);
});
process.on("SIGTERM", () => {
  void shutdown(0);
});

let previous = "";
const deadline = options.durationMs == null ? Number.POSITIVE_INFINITY : Date.now() + options.durationMs;
while (Date.now() < deadline) {
  const snapshot = {
    role: "full-node",
    name: options.name,
    room: options.room,
    relayBind: relay.bindAddr(),
    relayUrl,
    relayClients: relay.clientCount(),
    relayPeers: relay.peerCount(),
    peerId: mesh.peerId(),
    signaling: mesh.signalingMode(),
    relayConnected: mesh.relayConnected(),
    openPeers: await mesh.openPeerCount(),
    peers: await mesh.peerCount(),
    inflight: await mesh.inflightCount(),
    notes: notes.query({
      order: { path: "updated_at", direction: "desc" },
      limit: 5,
    }),
  };
  const encoded = stableStringify(snapshot);
  if (encoded !== previous) {
    previous = encoded;
    console.log(encoded);
  }
  await sleep(1000);
}

await shutdown(0);
