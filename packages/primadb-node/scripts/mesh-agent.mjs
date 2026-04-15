#!/usr/bin/env node
import { Primadb } from "../index.js";
import { mkdirSync } from "node:fs";
import process from "node:process";

function parseArgs(argv) {
  const options = {
    action: "live",
    relay: "ws://127.0.0.1:9010",
    iceServers: [],
    room: "primadb-mesh-agent",
    replica: `node-agent-${process.pid}`,
    storageDir: undefined,
    writeTitle: undefined,
    writeBody: "node mesh agent",
    expectTitles: [],
    minPeers: 1,
    timeoutMs: 60_000,
    holdMs: 2_000,
    writeDelayMs: 0,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = () => argv[++index];
    switch (arg) {
      case "--action":
        options.action = next();
        break;
      case "--relay":
        options.relay = next();
        break;
      case "--ice-server":
        options.iceServers.push(parseIceServerSpec(next()));
        break;
      case "--room":
        options.room = next();
        break;
      case "--replica":
        options.replica = next();
        break;
      case "--storage-dir":
        options.storageDir = next();
        break;
      case "--write-title":
        options.writeTitle = next();
        break;
      case "--write-body":
        options.writeBody = next();
        break;
      case "--expect-titles":
        options.expectTitles = next()
          .split(",")
          .map((value) => value.trim())
          .filter(Boolean);
        break;
      case "--min-peers":
        options.minPeers = Number(next());
        break;
      case "--timeout-ms":
        options.timeoutMs = Number(next());
        break;
      case "--hold-ms":
        options.holdMs = Number(next());
        break;
      case "--write-delay-ms":
        options.writeDelayMs = Number(next());
        break;
      default:
        throw new Error(`Unknown argument: ${arg}`);
    }
  }

  return options;
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

function testIceServers() {
  return [{ urls: "stun:stun.cloudflare.com:3478" }];
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate, timeoutMs, errorMessage) {
  const deadline = Date.now() + timeoutMs;
  let lastValue = null;
  while (Date.now() < deadline) {
    const value = await predicate();
    if (value) {
      return value;
    }
    lastValue = value;
    await wait(100);
  }
  throw new Error(`${errorMessage}. Last value: ${JSON.stringify(lastValue)}`);
}

function collectTitles(db, room) {
  return db
    .chain("boards")
    .field(room)
    .field("notes")
    .query({
      order: { path: "updated_at", direction: "asc" },
      limit: 1000,
    })
    .map((entry) => entry?.value?.title)
    .filter(Boolean);
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const db = new Primadb(options.replica);
  let storage = null;
  let mesh = null;

  try {
    if (options.storageDir) {
      mkdirSync(options.storageDir, { recursive: true });
      storage = db.openDurableStorage({
        kind: "segment_files",
        directory: options.storageDir,
      });
    }

    if (options.action === "verify-stored") {
      const titles = collectTitles(db, options.room);
      const missing = options.expectTitles.filter((title) => !titles.includes(title));
      if (missing.length > 0) {
        throw new Error(`Stored data missing titles: ${missing.join(", ")}`);
      }
      console.log(JSON.stringify({
        action: options.action,
        replica: options.replica,
        storage,
        storedTitles: titles,
        node_package_storage_confirmed: true,
      }, null, 2));
      return;
    }

    mesh = await db.connectMesh({
      room: options.room,
      relayUrl: options.relay,
      retryIntervalMs: 500,
      iceServers: options.iceServers.length > 0 ? options.iceServers : testIceServers(),
    });

    await waitFor(
      async () => (await mesh.openPeerCount()) >= options.minPeers,
      options.timeoutMs,
      `Timed out waiting for ${options.minPeers} open mesh peers`,
    );

    if (options.writeTitle) {
      if (options.writeDelayMs > 0) {
        await wait(options.writeDelayMs);
      }
      const now = Date.now();
      db.chain("boards").field(options.room).field("notes").set({
        title: options.writeTitle,
        body: options.writeBody,
        done: false,
        archived: false,
        created_at: now,
        updated_at: now,
      });
      await mesh.flushPending();
    }

    const titles = await waitFor(
      async () => {
        const current = collectTitles(db, options.room);
        return options.expectTitles.every((title) => current.includes(title)) ? current : null;
      },
      options.timeoutMs,
      "Timed out waiting for expected mesh titles",
    );

    console.log(JSON.stringify({
      action: options.action,
      replica: options.replica,
      storage,
      relay: options.relay,
      room: options.room,
      peerId: mesh.peerId(),
      signaling: mesh.signalingMode(),
      relayConnected: mesh.relayConnected(),
      openPeerCount: await mesh.openPeerCount(),
      titles,
      node_package_mesh_agent_confirmed: true,
    }, null, 2));
    if (options.holdMs > 0) {
      await wait(options.holdMs);
    }
  } finally {
    if (mesh) {
      await mesh.close();
    }
  }
}

main().catch((error) => {
  console.error(error.stack || String(error));
  process.exitCode = 1;
});
