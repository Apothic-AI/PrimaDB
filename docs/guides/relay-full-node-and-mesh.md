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

MoQ can also be used as a signaling underlay when the browser package is loaded with the
`primadb/moq` helper:

```ts
import { connectMeshViaMoq } from "primadb/moq";

const mesh = await connectMeshViaMoq(db, {
  url: "https://relay.example.com/anon",
  path: `primadb/mesh/demo/${db.replicaId()}`,
  subscribe: ["primadb/mesh/demo/known-peer"],
  room: "demo",
  iceServers: [{ urls: "stun:stun.cloudflare.com:3478" }],
});
```

This uses MoQ only for WebRTC signaling. Data still moves over WebRTC data channels after peers
connect. A PrimaDB-aware gateway/full node is still required when MoQ route traffic must be bridged
to WebSocket/WebRTC peers that are not participating in the same MoQ route namespace.

MoQ signaling does not automatically fall back to PrimaDB WebSocket relay signaling when the MoQ
session fails. Applications that need resilient browser connectivity should make fallback explicit:
try MoQ-backed signaling first, then normal WebSocket relay-backed `connectMesh(...)`, then
BroadcastChannel/local signaling where appropriate. The JS MoQ helper's WebSocket option is a
MoQ-over-WebSocket fallback for compatible MoQ endpoints, not the PrimaDB relay protocol.

## Remote Reads And Watches

Relay and mesh transports support peer-agnostic remote interests once peers are connected. Relay
sync clients can pull with `get(...)`, `query(...)`, `lex(...)`, `records(...)`, `node(...)`, and
`snapshot(...)`; relay and mesh handles can watch with `watchGet(...)`, `watchMap(...)`,
`watchQuery(...)`, `watchLex(...)`, `watchRecords(...)`, `watchNode(...)`, and
`watchSnapshot(...)`.

When a caller needs every reachable policy-matching peer instead of one selected peer, use
`recordsFanIn(...)` / `records_fan_in(...)` and `watchRecordsFanIn(...)` /
`watch_records_fan_in(...)`. Fan-in responses preserve source peer metadata, merged records,
conflict metadata, and partial failures.

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

## Application Overlay Traffic

Use application routes for caller-defined protocols such as mesh channels, trust proposals, vault
proposals, or memory coordination. The payload remains inside `RouteEnvelope` and is delivered by
the same relay/mesh/MoQ routing machinery as sync, pulls, watches, and signaling.

For one transport handle:

```ts
const sub = sync.subscribeApplications({ namespace: "starla.mesh" });

await sync.sendApplication(
  "starla.mesh",
  "channel.v1",
  "general",
  { text: "hello" },
  {},
  { kind: "broadcast" },
);

const event = await sub.next();
console.log(event?.context.sourcePeerId, event?.context.transport);
```

For a multi-underlay route policy, use the overlay session APIs. Rust exposes
`RouteOverlaySession`; browser and Node MoQ helpers expose `PrimadbRouteOverlaySession`. Native
WebSocket, native MoQ, and native WebRTC handles expose route-overlay underlay adapters, so a caller
can send through a direct-first/fallback policy and receive delivery diagnostics instead of
manually fanning out through each transport.

Application streams are chunked application-route messages for larger trusted payloads. They carry
stream id, sequence, chunk, final, close/error, ack/nack, and metadata fields and reassemble into a
completed stream event on the receiver.

## When To Use Each Mode

- Use relay-only for predictable deployment and simple firewall behavior.
- Use full nodes for always-on anchors, durable authority peers, or application relays.
- Use mesh when direct peer channels matter for latency, bandwidth, or relay load.
- Use strict `sessionAuth` plus hooks when serving should require verified peer identity.

See also:

- [Routing and mesh](../concepts/routing-and-mesh)
- [Running examples](../examples/running-examples)
- [Network hooks](../reference/network-hooks)
