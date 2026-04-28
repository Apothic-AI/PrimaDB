#!/usr/bin/env node
import { setTimeout as delay } from "node:timers/promises";
import { Primadb, RelayServer } from "../index.js";

const relayAddress = "127.0.0.1:9021";
const relayUrl = `ws://${relayAddress}`;

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

const relay = await RelayServer.listen({ bind: relayAddress });

try {
  const dbA = new Primadb("node-relay-a");
  const dbB = new Primadb("node-relay-b");
  const syncA = await dbA.connectRelay({ url: relayUrl, retryIntervalMs: 500 });
  const syncB = await dbB.connectRelay({ url: relayUrl, retryIntervalMs: 500 });
  const title = `Node relay ${Date.now()}`;
  dbA.scope("relay-ledger").configure({
    consistency: "coordinated",
    authority: { kind: "full_node", peerId: "native:node-relay-a" },
  });

  const targetPeer = await waitFor(async () => {
    const peers = syncB.recommendedPeers();
    return peers.find((entry) => entry?.peer?.replica_id === dbA.replicaId())?.peer?.peer_id ?? null;
  });
  const transactionReport = await syncB.remoteTransaction(targetPeer, "relay-ledger", [
    {
      kind: "increment",
      path: { anchor: "alice", segments: ["balance"] },
      by: 7,
    },
  ]);
  const remoteBalance = await syncB.remoteGet(targetPeer, {
    anchor: "relay-ledger",
    segments: ["alice", "balance"],
  });
  const watch = syncB.watchRemoteQuery(
    targetPeer,
    { anchor: "relay-demo", segments: ["notes"] },
    {
      filters: [{ kind: "eq", path: "title", value: title }],
      limit: 1,
    },
  );
  const initialWatch = await watch.next();

  dbA.chain("relay-demo").field("notes").set({
    title,
    body: "replicated over native node relay",
    createdAt: new Date().toISOString(),
  });
  await syncA.flushPending();

  const watchUpdate = await waitFor(async () => {
    const message = await watch.next();
    return Array.isArray(message.value) && message.value.length > 0 ? message : null;
  });

  const replicated = await waitFor(async () => {
    const entries = dbB.chain("relay-demo").field("notes").query({
      filters: [{ kind: "eq", path: "title", value: title }],
    });
    return Array.isArray(entries) && entries.length > 0 ? entries : null;
  });

  watch.close();
  syncA.close();
  syncB.close();

  console.log(
    JSON.stringify(
      {
        relayUrl,
        title,
        targetPeer,
        initialWatch,
        watchUpdate,
        transactionReport,
        remoteBalance,
        replicatedCount: replicated.length,
        node_package_relay_confirmed:
          transactionReport.status === "committed" && remoteBalance === 7 && replicated.length > 0,
      },
      null,
      2,
    ),
  );
} finally {
  await relay.close();
}
