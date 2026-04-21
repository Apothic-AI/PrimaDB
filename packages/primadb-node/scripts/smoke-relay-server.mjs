#!/usr/bin/env node
import { Primadb, RelayServer } from "../index.js";

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function waitFor(condition, timeoutMs, description) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await condition()) {
      return;
    }
    await sleep(100);
  }
  throw new Error(`timed out waiting for ${description}`);
}

const relay = await RelayServer.listen({ bind: "127.0.0.1:9031" });
const leftDb = new Primadb("node-relay-server-left");
const rightDb = new Primadb("node-relay-server-right");

const left = await leftDb.connectRelay({ url: relay.url(), retryIntervalMs: 200 });
const right = await rightDb.connectRelay({ url: relay.url(), retryIntervalMs: 200 });

await waitFor(() => left.isConnected() && right.isConnected(), 10_000, "both relay clients to connect");
await waitFor(() => relay.clientCount() >= 2, 10_000, "relay client count to reach 2");

const payload = {
  bindAddr: relay.bindAddr(),
  url: relay.url(),
  clientCount: relay.clientCount(),
  peerCount: relay.peerCount(),
  relayServerApiConfirmed: true,
};
console.log(JSON.stringify(payload, null, 2));

left.close();
right.close();
await relay.close();
