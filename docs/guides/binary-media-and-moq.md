---
title: Binary Data, Media, And MoQ
sidebar_position: 3
---

PrimaDB can store binary data directly in the graph and can store larger payloads as
content-addressed blobs. The examples also include media-oriented demos and experimental MoQ helper
flows.

## Bytes In The Graph

Use bytes fields for small binary values or live chunks that should replicate as graph data.

Browser:

```ts
const bytes = new Uint8Array([1, 2, 3, 5, 8, 13]);
db.chain("assets").field("avatar").putBytes(bytes);
const restored = db.chain("assets").field("avatar").onceBytes();
```

Node:

```ts
db.chain("assets").field("avatar").putBytes(Buffer.from([1, 2, 3, 5, 8, 13]));
const restored = db.chain("assets").field("avatar").onceBytes();
```

Python:

```python
db.chain("assets").field("avatar").put_bytes(bytes([1, 2, 3, 5, 8, 13]))
restored = db.chain("assets").field("avatar").once_bytes()
```

## Content-Addressed Blobs

Use blob storage for larger binary payloads. Blob IDs are BLAKE3-prefixed content IDs.

Node:

```ts
db.openBlobStorage({
  kind: "files",
  directory: "/tmp/primadb-blobs",
  durability: "full",
});

const ref = db
  .chain("assets")
  .field("archive")
  .putBlob(Buffer.from([1, 2, 3]), "application/octet-stream");

console.log(ref.id); // blake3:...
```

Python exposes the same flow through `open_blob_storage(...)`, `put_blob(...)`, `blob_ref()`, and
`get_blob()`. Browser blob storage can use the memory backend or IndexedDB through
`openBlobStorage(...)` / `enableIndexedDbBlobStorage(...)`.

The graph stores a blob reference, while the bytes live in the configured blob backend. Vacuum and
blob GC remove unreferenced native blobs.

## Media Examples

The browser package includes demos that use `MediaRecorder` chunks as PrimaDB byte payloads:

- `packages/primadb/examples/binary-stream-room`
- `packages/primadb/examples/text-voice-chat`

These demos are intentionally examples, not a claim that graph replication is the best transport for
all realtime media. For serious media transport, use them to evaluate latency, chunk size, backpressure,
and peer topology.

## MoQ Route Helpers

The MoQ helpers model path/track/sequence frames on top of PrimaDB package surfaces. They now carry
PrimaDB `RouteEnvelope` objects (`primadb.route.v1`) rather than a standalone MoQ-only sync
protocol:

- browser: `primadb/moq`
- Node: `primadb-node/moq`
- Python: `primadb.moq`
- Rust/native: optional `native-moq` feature with `NativeMoqRouteClient` and `NativeMoqSync`

Generic MoQ relays can fan out these frames to subscribers that know the same route-mode path and
track. A PrimaDB-aware gateway/full node is still needed to bridge that MoQ route traffic to
WebSocket/WebRTC sessions, enforce hooks/auth, and host durable state.

Generic public relays may not support broadcast discovery/ANNOUNCE. When using a relay such as
Cloudflare MoQ, prefer one publish path per peer and configure each peer with the remote paths it
should subscribe to, or route through a PrimaDB-aware gateway that can bridge and advertise peers.

## MoQ-Backed WebRTC Signaling

Browser WebRTC mesh can use a MoQ route session for signaling while keeping WebRTC data channels as
the direct data underlay:

```ts
import { connectMeshViaMoq } from "primadb/moq";

const mesh = await connectMeshViaMoq(db, {
  url: "https://relay.example.com/anon",
  path: `primadb/mesh/demo/${db.replicaId()}`,
  subscribe: ["primadb/mesh/demo/peer-b"],
  room: "demo",
  iceServers: [{ urls: "stun:stun.cloudflare.com:3478" }],
});
```

`connectMeshViaMoq(...)` creates a route-mode `PrimadbMoqSession`, constructs the WASM mesh with
external signaling, forwards outgoing `RoutePayload::Signal` routes into MoQ, and injects incoming
MoQ routes back into the mesh. MoQ-only peers are still relay-routed; they only form direct WebRTC
links if they also run the WebRTC mesh layer.

Use the package-local examples when evaluating them:

```bash
cd packages/primadb/examples
pnpm run smoke:moq
pnpm run smoke:moq-live # optional; gated by MOQ_RELAY, MOQ_DRAFT14_RELAY, or MOQ_DRAFT07_RELAY

cd ../primadb-node
pnpm run smoke:moq
pnpm run smoke:moq-live # optional; requires a Node WebTransport polyfill for WebTransport-only relays

cd ../primadb-python
uv run python examples/moq_sync/main.py

cargo run --features native-moq --example native_moq_live_probe
cargo run --features "native-websocket native-moq" --example native_gateway_moq_ws_live_probe
scripts/smoke-native-moq-ietf-local.sh # starts a local Cloudflare moq-rs relay first
```

Observed Cloudflare interop caveats:

- Native Rust uses Cloudflare `moq-rs` (`moq-transport`/`moq-native-ietf`) for draft-14/latest
  route-mode sessions and a renamed Cloudflare `moq-rs` draft-07 branch backend for
  `MoqDraft::Draft07`. Both native/native and native gateway probes pass against the configured
  Cloudflare draft-14 and draft-07 endpoints in this environment.
- Browser and Node use `@moq/lite` for the JS stack. Browser/browser, browser/Node, and browser
  WebRTC-over-MoQ signaling pass against Cloudflare draft-14. The current JS stack still does not
  negotiate the Cloudflare draft-07 endpoint, so browser/Node draft-07 probes report connection
  loss or `E_SESSION_CLOSED`.
- Node v26.1.0 in this workspace has built-in WebSocket but still no built-in WebTransport.
  `primadb-node/moq` now uses `@webtransport-bun/webtransport` as its Node-only provider when no
  explicit transport is supplied. Node/Node route exchange passes through Cloudflare draft-14 with
  this provider; draft-07 still reports `E_SESSION_CLOSED` because it shares the JS MoQ stack.

See also:

- [Data model](../concepts/data-model)
- [Storage and durability](../concepts/storage)
- [Examples overview](../examples/overview)
