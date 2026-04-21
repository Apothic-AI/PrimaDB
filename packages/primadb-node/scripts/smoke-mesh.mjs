#!/usr/bin/env node
import { setTimeout as delay } from "node:timers/promises";
import { Primadb, RelayServer } from "../index.js";

const relayAddress = "127.0.0.1:9022";
const relayUrl = `ws://${relayAddress}`;
const room = `node-mesh-${Date.now()}`;
const iceServers = [{ urls: "stun:stun.cloudflare.com:3478" }];

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

const relay = await RelayServer.listen({ bind: relayAddress });

try {
  const dbA = new Primadb("node-mesh-a");
  const dbB = new Primadb("node-mesh-b");
  const meshA = await dbA.connectMesh({
    room,
    relayUrl,
    retryIntervalMs: 500,
    iceServers,
  });
  const meshB = await dbB.connectMesh({
    room,
    relayUrl,
    retryIntervalMs: 500,
    iceServers,
  });

  await waitFor(async () => {
    const [openA, openB] = await Promise.all([meshA.openPeerCount(), meshB.openPeerCount()]);
    return openA > 0 && openB > 0;
  });

  const title = `Node mesh ${Date.now()}`;
  const watch = await meshB.watchRemoteQuery(
    meshA.peerId(),
    { anchor: "mesh-demo", segments: ["notes"] },
    {
      filters: [{ kind: "eq", path: "title", value: title }],
      limit: 1,
    },
  );
  const initialWatch = await watch.next();

  dbA.chain("mesh-demo").field("notes").set({
    title,
    body: "replicated over native node mesh",
    createdAt: new Date().toISOString(),
  });
  await meshA.flushPending();

  const watchUpdate = await waitFor(async () => {
    const message = await watch.next();
    return Array.isArray(message.value) && message.value.length > 0 ? message : null;
  });

  const replicated = await waitFor(async () => {
    const entries = dbB.chain("mesh-demo").field("notes").query({
      filters: [{ kind: "eq", path: "title", value: title }],
    });
    return Array.isArray(entries) && entries.length > 0 ? entries : null;
  });

  watch.close();
  await meshA.close();
  await meshB.close();

  console.log(
    JSON.stringify(
      {
        relayUrl,
        room,
        title,
        initialWatch,
        watchUpdate,
        replicatedCount: replicated.length,
        node_package_mesh_confirmed: true,
      },
      null,
      2,
    ),
  );
} finally {
  await relay.close();
}
