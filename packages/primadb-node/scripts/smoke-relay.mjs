#!/usr/bin/env node
import { spawn } from "node:child_process";
import net from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import { Primadb } from "../index.js";

const relayAddress = "127.0.0.1:9021";
const relayUrl = `ws://${relayAddress}`;
const relay = spawn("cargo", ["run", "--example", "ws_relay_server", "--", relayAddress], {
  cwd: "/home/bitnom/Code/gunport/primadb",
  stdio: "pipe",
  env: process.env,
});

async function waitForRelay(timeoutMs = 20_000) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const connected = await new Promise((resolve) => {
      const socket = net.connect({ host: "127.0.0.1", port: 9021 });
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

async function waitFor(predicate, timeoutMs = 20_000) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const value = await predicate();
    if (value) {
      return value;
    }
    await delay(250);
  }
  throw new Error("Timed out waiting for relay replication");
}

try {
  await waitForRelay();

  const dbA = new Primadb("node-relay-a");
  const dbB = new Primadb("node-relay-b");
  const syncA = await dbA.connectRelay({ url: relayUrl, retryIntervalMs: 500 });
  const syncB = await dbB.connectRelay({ url: relayUrl, retryIntervalMs: 500 });
  const title = `Node relay ${Date.now()}`;

  dbA.chain("relay-demo").field("notes").set({
    title,
    body: "replicated over native node relay",
    createdAt: new Date().toISOString(),
  });
  await syncA.flushPending();

  const replicated = await waitFor(async () => {
    const entries = dbB.chain("relay-demo").field("notes").query({
      filters: [{ kind: "eq", path: "title", value: title }],
    });
    return Array.isArray(entries) && entries.length > 0 ? entries : null;
  });

  syncA.close();
  syncB.close();

  console.log(
    JSON.stringify(
      {
        relayUrl,
        title,
        replicatedCount: replicated.length,
        node_package_relay_confirmed: true,
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
