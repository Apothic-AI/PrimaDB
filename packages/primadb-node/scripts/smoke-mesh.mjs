#!/usr/bin/env node
import { spawn } from "node:child_process";
import net from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import { Primadb } from "../index.js";

const relayAddress = "127.0.0.1:9022";
const relayUrl = `ws://${relayAddress}`;
const room = `node-mesh-${Date.now()}`;
const relay = spawn("cargo", ["run", "--example", "ws_relay_server", "--", relayAddress], {
  cwd: "/home/bitnom/Code/gunport/primadb",
  stdio: "pipe",
  env: process.env,
});

async function waitForRelay(timeoutMs = 20_000) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const connected = await new Promise((resolve) => {
      const socket = net.connect({ host: "127.0.0.1", port: 9022 });
      socket.once("connect", () => {
        socket.destroy();
        resolve(true);
      });
      socket.once("error", () => {
        socket.destroy();
        resolve(false);
      });
    });
    if (connected) {
      return;
    }
    await delay(200);
  }
  throw new Error(`Timed out waiting for relay on ${relayAddress}`);
}

async function waitFor(predicate, timeoutMs = 30_000) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const value = await predicate();
    if (value) {
      return value;
    }
    await delay(250);
  }
  throw new Error("Timed out waiting for native mesh replication");
}

try {
  await waitForRelay();

  const dbA = new Primadb("node-mesh-a");
  const dbB = new Primadb("node-mesh-b");
  const meshA = await dbA.connectMesh({
    room,
    relayUrl,
    retryIntervalMs: 500,
  });
  const meshB = await dbB.connectMesh({
    room,
    relayUrl,
    retryIntervalMs: 500,
  });

  await waitFor(async () => {
    const [openA, openB] = await Promise.all([meshA.openPeerCount(), meshB.openPeerCount()]);
    return openA > 0 && openB > 0;
  });

  const title = `Node mesh ${Date.now()}`;
  dbA.chain("mesh-demo").field("notes").set({
    title,
    body: "replicated over native node mesh",
    createdAt: new Date().toISOString(),
  });
  await meshA.flushPending();

  const replicated = await waitFor(async () => {
    const entries = dbB.chain("mesh-demo").field("notes").query({
      filters: [{ kind: "eq", path: "title", value: title }],
    });
    return Array.isArray(entries) && entries.length > 0 ? entries : null;
  });

  await meshA.close();
  await meshB.close();

  console.log(
    JSON.stringify(
      {
        relayUrl,
        room,
        title,
        replicatedCount: replicated.length,
        node_package_mesh_confirmed: true,
      },
      null,
      2,
    ),
  );
} finally {
  if (relay.exitCode == null && !relay.killed) {
    relay.kill("SIGTERM");
    await delay(200);
  }
}
