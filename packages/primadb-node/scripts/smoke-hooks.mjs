#!/usr/bin/env node
import { setTimeout as delay } from "node:timers/promises";
import { Primadb, RelayServer } from "../index.js";

const relayAddress = "127.0.0.1:9024";
const relayUrl = `ws://${relayAddress}`;

async function waitFor(predicate, timeoutMs = 20_000) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const value = await predicate();
    if (value) {
      return value;
    }
    await delay(200);
  }
  throw new Error("Timed out waiting for hook propagation");
}

const relay = await RelayServer.listen({ bind: relayAddress });

try {
  const serverDb = new Primadb("node-hook-server");
  const clientDb = new Primadb("node-hook-client");

  serverDb.setNetworkHooks({
    onPull(context) {
      if (context.request.kind === "get" && context.request.path.anchor === "private") {
        return "private root denied";
      }
      return undefined;
    },
    onServeResult(_context, result) {
      if (result.kind === "get") {
        return { kind: "get", value: { masked: true } };
      }
      return undefined;
    },
  });

  const server = await serverDb.connectRelay({ url: relayUrl, retryIntervalMs: 500 });
  const client = await clientDb.connectRelay({ url: relayUrl, retryIntervalMs: 500 });

  const targetPeer = await waitFor(async () => {
    const peers = client.recommendedPeers();
    return peers.find((entry) => entry?.peer?.replica_id === serverDb.replicaId())?.peer?.peer_id ?? null;
  });

  serverDb.chain("docs").field("profile").put({ title: "Hooked profile", visible: true });
  serverDb.chain("private").field("secret").put({ title: "Forbidden profile", visible: false });
  await server.flushPending();

  const masked = await waitFor(async () => {
    const value = await client.remoteGet(targetPeer, { anchor: "docs", segments: ["profile"] });
    return value?.masked === true ? value : null;
  });

  let denied = null;
  try {
    await client.remoteGet(targetPeer, { anchor: "private", segments: ["secret"] });
  } catch (error) {
    denied = String(error);
  }
  if (!denied || !denied.includes("private root denied")) {
    throw new Error(`Expected denied pull from network hook, got: ${denied}`);
  }

  serverDb.clearNetworkHooks();
  const unmasked = await waitFor(async () => {
    const value = await client.remoteGet(targetPeer, { anchor: "docs", segments: ["profile"] });
    return value?.title === "Hooked profile" ? value : null;
  });

  server.close();
  client.close();

  console.log(
    JSON.stringify(
      {
        relayUrl,
        targetPeer,
        masked,
        denied,
        unmasked,
        node_package_hooks_confirmed: true,
      },
      null,
      2,
    ),
  );
} finally {
  await relay.close();
}
