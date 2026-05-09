---
title: Relay, Full Node, And Mesh
sidebar_position: 2
---

PrimaDB peers are offline-first. A relay is just a peer process that accepts WebSocket connections
and forwards routed traffic for other peers. A full node is a peer that combines local graph state,
relay serving, and optional mesh participation in one long-running process.

## Relay-Only Topology

Use relay-only mode when you want simple connectivity and do not need direct peer-to-peer data
channels.

```bash
cargo run --features native-websocket --example ws_relay_server -- 127.0.0.1:9010
```

Browser or package clients connect to it:

```ts
const sync = await db.connectRelay({
  url: "ws://127.0.0.1:9010",
  retryIntervalMs: 1500,
});
```

If the relay is unavailable, SDK mesh/relay clients continue local reads and writes and retry in the
background where the transport supports disconnected startup.

## Full Node Topology

Use a full node when you want a stable anchor peer that can hold durable state, accept relay
connections, and participate in mesh discovery.

Rust:

```bash
cargo run --features native-webrtc --example full_node -- \
  --relay-bind 127.0.0.1:9010 \
  --room demo \
  --message "hello from the anchor node"
```

Node:

```bash
cd packages/primadb-node
pnpm install
pnpm run build
node ./examples/full-node/index.mjs --room package-full-node --name node-anchor --message "hello from the anchor node"
```

Python:

```bash
cd packages/primadb-python/examples/full_node
uv sync
uv run python main.py
```

## Mesh Topology

Mesh mode uses relay-backed signaling for discovery and then attempts direct WebRTC links. Sharing a
relay alone does not automatically make relay traffic direct peer-to-peer traffic; use
`connectMesh(...)` when you want direct data channels.

```ts
const mesh = await db.connectMesh({
  room: "demo",
  signaling: "relay",
  relayUrl: "ws://127.0.0.1:9010",
  iceServers: [{ urls: "stun:stun.cloudflare.com:3478" }],
});
```

PrimaDB core and packages do not hard-code STUN servers. Examples may pass public STUN servers
explicitly.

## Remote Reads And Watches

Relay and mesh transports support peer-agnostic remote interests once peers are connected. Relay
sync clients can pull with `get(...)`, `query(...)`, `lex(...)`, `records(...)`, `node(...)`, and
`snapshot(...)`; relay and mesh handles can watch with `watchGet(...)`, `watchMap(...)`,
`watchQuery(...)`, `watchLex(...)`, `watchRecords(...)`, `watchNode(...)`, and
`watchSnapshot(...)`.

The default policy is "any connected/recommended peer." Pass a `RemoteInterestPolicy` only when
needed:

```ts
const result = await sync.records({ prefix: "threads/" });

const authorityResult = await sync.records(
  { prefix: "ledger/" },
  { target: "peer", peerId: "native:ledger", requireCapability: true },
);
```

The explicit peer-targeting methods remain available:

- `remoteGet(...)`
- `remoteQuery(...)`
- `remoteLex(...)`
- `remoteRecords(...)`
- `remoteNode(...)`
- `remoteSnapshot(...)`
- `watchRemoteGet(...)`
- `watchRemoteMap(...)`
- `watchRemoteQuery(...)`
- `watchRemoteLex(...)`
- `watchRemoteRecords(...)`
- `watchRemoteNode(...)`
- `watchRemoteSnapshot(...)`

Strict-scope `remoteTransaction(...)` still targets a concrete authority peer.

Mesh traversal can fetch missing linked nodes on demand without pulling the entire graph first.
Prefer `watchTraverse(...)` when a UI should update as those fetched nodes arrive.

## When To Use Each Mode

- Use relay-only for predictable deployment and simple firewall behavior.
- Use full nodes for always-on anchors, durable authority peers, or application relays.
- Use mesh when direct peer channels matter for latency, bandwidth, or relay load.
- Use strict `sessionAuth` plus hooks when serving should require verified peer identity.

See also:

- [Routing and mesh](../concepts/routing-and-mesh)
- [Running examples](../examples/running-examples)
- [Network hooks](../reference/network-hooks)
