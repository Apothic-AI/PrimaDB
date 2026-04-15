#!/usr/bin/env node
import { mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { Primadb } from "../../index.js";

function parseArgs(argv) {
  const values = {
    room: "package-mesh",
    relay: "ws://127.0.0.1:9010",
    name: `node-${process.pid}`,
    message: "",
    durationMs: 15_000,
  };

  for (let index = 2; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];
    if (arg === "--room" && next) {
      values.room = next;
      index += 1;
    } else if (arg === "--relay" && next) {
      values.relay = next;
      index += 1;
    } else if (arg === "--name" && next) {
      values.name = next;
      index += 1;
    } else if (arg === "--message" && next) {
      values.message = next;
      index += 1;
    } else if (arg === "--duration-ms" && next) {
      values.durationMs = Number.parseInt(next, 10) || values.durationMs;
      index += 1;
    }
  }

  return values;
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function stableStringify(value) {
  return JSON.stringify(value, null, 2);
}

const options = parseArgs(process.argv);
const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, ".data", options.name);
mkdirSync(root, { recursive: true });

const db = new Primadb(`node-mesh-${options.name}`);
db.openDurableStorage({
  kind: "segment_files",
  directory: root,
  journalRetention: 4,
});

const mesh = await db.connectMesh({
  room: options.room,
  relayUrl: options.relay,
  retryIntervalMs: 750,
});
const notes = db.chain("package_examples").field("mesh").field(options.room).field("notes");

if (options.message) {
  notes.set({
    author: options.name,
    title: `${options.name} ${new Date().toISOString().slice(11, 19)}`,
    body: options.message,
    updated_at: Date.now(),
  });
  await mesh.flushPending();
}

let previous = "";
let lastRelayConnected = null;
const deadline = Date.now() + options.durationMs;
while (Date.now() < deadline) {
  const relayConnected = mesh.relayConnected();
  if (relayConnected !== lastRelayConnected) {
    if (relayConnected) {
      console.error(`relay ${mesh.relayUrl()} connected; mesh signaling is active`);
    } else {
      console.error(
        `relay ${mesh.relayUrl()} unavailable; continuing offline and retrying in background`,
      );
    }
    lastRelayConnected = relayConnected;
  }
  const snapshot = {
    peerId: mesh.peerId(),
    signaling: mesh.signalingMode(),
    relayUrl: mesh.relayUrl(),
    relayConnected,
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

await mesh.close();
