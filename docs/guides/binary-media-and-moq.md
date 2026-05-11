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

Use the package-local examples when evaluating them:

```bash
cd packages/primadb/examples
pnpm run smoke:moq

cd ../primadb-node
pnpm run smoke:moq

cd ../primadb-python
uv run python examples/moq_sync/main.py
```

See also:

- [Data model](../concepts/data-model)
- [Storage and durability](../concepts/storage)
- [Examples overview](../examples/overview)
