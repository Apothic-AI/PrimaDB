# primadb

`primadb` is a Rust-native, local-first graph database inspired by Gun, but built around explicit versioned operations instead of Gun's implicit event mesh. The current codebase focuses on deterministic merge behavior, browser compatibility, and a clean replication boundary that can be driven by WebSockets, WebRTC, service workers, or any other transport you want to layer on top.

## Current Capabilities

- Graph-shaped documents with nested object writes.
- Gun-style path traversal through a chain API.
- Query layer with filter/order/limit support over node fields and set members.
- Per-field last-write-wins conflict resolution with hybrid logical revisions.
- Set membership via `set()` / `remove()` or `{"$set": [...]}` markers.
- Link references via `{"$link": "node-id"}` markers.
- First-class small/medium binary fields via `put_bytes()` / `putBytes(...)` and `{"$bytes": "..."}` markers.
- Separate content-addressed blob storage for larger binary payloads, with blob refs stored in-graph via `{"$blob": {...}}`.
- Reactive subscriptions.
- Database-level change subscriptions for persistence/sync hooks.
- Explicit outbound replication log via `pending_operations()` / `drain_pending_operations()`.
- Sync envelopes and JSON wire helpers for custom transports.
- Snapshot import/export.
- Native file persistence.
- Browser persistence via `localStorage`.
- Async IndexedDB save/load helpers in the WASM bindings.
- Automatic IndexedDB persistence hook in the WASM bindings.
- Browser WebSocket sync helper with ack/retry/requeue behavior.
- Routed transport envelopes with presence, signaling, remote pull requests/responses, batch payloads, chunked replies, reply correlation, content hashes, seen-by hints, TTL, and dedupe.
- Remote live watches for `get` / `map` / `query` / `lex` / `snapshot` over relay and mesh transports, with initial snapshots, streamed updates, chunked watch events, and active-interest replay when peers appear.
- Browser WebRTC mesh sync with both local `BroadcastChannel` signaling and relay-backed signaling for cross-browser peers.
- Native WebRTC mesh starts offline, keeps local reads/writes/durable state available, and retries relay signaling in the background until a relay peer appears.
- Optional native WebSocket sync adapter behind the `native-websocket` feature, with disconnected startup and background relay retry on native.
- Integrated auth/user policies behind the `crypto` feature, including trusted users, local user sessions, signed sync, encrypted sync, and encrypted snapshot persistence.
- Data-level auth in the core database for signed user-owned fields, certificate-authorized delegated writes, and read-time signature verification/unwrapping.
- Gun-compatible browser runtime in [js/primadb-gun.js](/home/bitnom/Code/gunport/primadb/js/primadb-gun.js) with current-style `get`, `put`, `set`, `on`, `once`, `open`, `load`, `map`, `then`, `back`, `not`, and `user` flows.
- Merge-safe snapshot import for peer catch-up without clobbering local state, plus root snapshot traversal that includes reachable linked/set-member nodes instead of only prefix-matched node IDs.
- SEA-style browser crypto surface with pair generation, password work, sign/verify, encrypt/decrypt, shared-secret derivation, and certificates.
- Storage adapter ecosystem with an in-memory adapter, snapshot-file adapter, and RADisk-style append-log file adapter.
- Incremental segment-backed native storage with lazy node restore, canonical node/index records, manifest metadata, direct-scalar indexes, and journaled transactions.
- Lexical/range traversal via `chain.lex()` / `chain.scan(...)`.
- Gun compatibility surface with `Gun` / `GunChain`, Gun link markers, and Gun graph import/export helpers.
- Runtime stats and limit controls for transport and queue hardening.
- Optional Rayon-backed query filtering/sorting, chunk construction, and log replay on native targets and on the opt-in `wasm-threads` browser build.
- `wasm-bindgen` bindings that compile on `wasm32-unknown-unknown`.

## Design Notes

This is intentionally not a 1:1 port of Gun internals.

- Primadb stores version markers per field and tombstone.
- Set membership tracks both add and remove markers so concurrent `set()` / `remove()` operations converge.
- Writes are turned into explicit operations.
- Replication is transport-agnostic.
- Nested objects become linked graph nodes with deterministic path-derived IDs, so replicas converge on the same intermediate graph structure.
- Browser auto-persistence ignores the transient “drained for transport” state so in-flight writes are not silently lost on reload before ack.
- Persisted snapshot loads preserve the local replica identity and do not replay another tab's pending queue.
- Browser support stays on stable `wasm32-unknown-unknown` patterns instead of assuming newer WebAssembly proposals are enabled by default.
- Threaded WASM is an explicit opt-in path layered on top of the default browser build instead of changing the default toolchain or hosting requirements.
- Native storage no longer needs full snapshot hydration up front: the incremental store can restore clock/pending metadata first and lazy-load nodes on demand.

That gives the project a more inspectable merge model and makes it easier to test and evolve without carrying over Gun's event-routing bugs.

## Planning

- [docs/scale-and-storage-maturity-plan.md](/home/bitnom/Code/gunport/primadb/docs/scale-and-storage-maturity-plan.md): Storage-boundary redesign plan for incremental reads, page/segment persistence, query pushdown, browser/native backends, compaction, and scale-oriented testing.
- [docs/cross-platform-parity-plan.md](/home/bitnom/Code/gunport/primadb/docs/cross-platform-parity-plan.md): Cross-platform parity plan for shared relay, durable storage, and browser/native WebRTC mesh.

## Rust Example

```rust
use primadb::Primadb;
use serde_json::json;

fn main() -> primadb::Result<()> {
    let db = Primadb::with_replica_id("desktop-a");

    db.root("users").field("alice").put(json!({
        "name": "Alice",
        "profile": {
            "timezone": "America/New_York"
        }
    }))?;

    db.root("rooms")
        .field("general")
        .field("members")
        .set(json!({"$link": "users/alice"}))?;

    let snapshot = db.root("users").field("alice").once_json()?;
    println!("{}", serde_json::to_string_pretty(&snapshot)?);

    db.root("rooms")
        .field("general")
        .field("members")
        .remove(json!({"$link": "users/alice"}))?;

    let boston_users = db
        .root("users")
        .find()
        .where_eq("profile.timezone", "America/New_York")?
        .order_by("name", primadb::QueryDirection::Asc)
        .run()?;
    println!("{}", boston_users.len());

    Ok(())
}
```

## Browser Example

The canonical browser build entrypoint is now:

```bash
./build-wasm.sh
```

That produces a package in `./pkg` by default. Example-specific `build.sh` scripts are now just
thin wrappers around this main build path.

After building with `wasm-pack build --target web` or an equivalent toolchain, the generated bindings expose `Primadb`, `Chain`, and `Subscription`.

```js
import init, { Primadb } from "./pkg/primadb.js";

await init();

const db = new Primadb("browser-a");
db.useBrowserStorage("primadb-demo");

const persistence = await db.enableIndexedDbPersistence(
  "primadb-demo",
  "snapshots",
  "main",
  true,
);
db.openBlobStorage({
  kind: "indexed_db",
  databaseName: "primadb-demo",
  storeName: "blobs",
  namespace: "main",
});

const user = db.chain("users").field("alice");
user.put({
  name: "Alice",
  profile: { timezone: "America/New_York" },
});

const sub = user.on((value) => {
  console.log("update", value);
});

db.chain("assets").field("avatar").putBytes(new Uint8Array([1, 2, 3, 4]));
const blobRef = await db
  .chain("assets")
  .field("archive")
  .putBlob(new Uint8Array([5, 6, 7, 8]), "application/octet-stream");
console.log(blobRef);

const matches = db.chain("users").query({
  filters: [{ kind: "prefix", path: "name", value: "A" }],
  order: { path: "name", direction: "asc" },
  limit: 10,
});

const peer = db.connectRelay({
  url: "ws://127.0.0.1:9010",
  retryIntervalMs: 2000,
});
peer.flushPending();

sub.cancel();
persistence.close();
peer.close();
```

## Threaded WASM

The default browser build remains the current stable `wasm32-unknown-unknown` path.

If you want Rayon-backed parallel work in the browser, Primadb now has a separate `wasm-threads`
build path. That path is intentionally opt-in because it requires:

- the `wasm-threads` feature
- a nightly toolchain with `-Z build-std`
- shared-memory linker flags
- `SharedArrayBuffer`
- COOP/COEP headers at runtime

The canonical threaded build entrypoint is:

```bash
./build-wasm-threads.sh
```

That produces a threaded package in `./pkg` by default. Example-level threaded `build.sh` scripts
wrap this same path with example-specific output directories.

The dedicated example in [examples/browser-threaded-query/README.md](/home/bitnom/Code/gunport/primadb/examples/browser-threaded-query/README.md)
shows the intended JS bootstrap pattern:

```js
import init, * as primadb from "./pkg/primadb.js";

await init();
await primadb.initThreadPool(Math.max(2, navigator.hardwareConcurrency || 4));

const db = new primadb.Primadb("threaded-browser");
console.log(primadb.parallelEnabled(), primadb.parallelThreadCount());
```

For a full threaded browser P2P example on top of the same bootstrap path, see
[examples/browser-threaded-mesh-notes/README.md](/home/bitnom/Code/gunport/primadb/examples/browser-threaded-mesh-notes/README.md).

## TypeScript Package

Primadb now also has an in-repo TypeScript package in
[packages/primadb](/home/bitnom/Code/gunport/primadb/packages/primadb). It wraps the existing
Rust/WASM browser runtime instead of reimplementing it.

Build it from the repo with:

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb
npm install
npm run build
```

That package exposes three browser-facing entrypoints:

- `primadb`: default browser build
- `primadb/threads`: threaded browser build
- `primadb/gun`: Gun-compatible browser runtime

Example usage:

```ts
import { Primadb, initPrimadb } from "primadb";

await initPrimadb();

const db = new Primadb("browser-a");
const mesh = db.connectMesh({
  room: "demo-room",
  relayUrl: "ws://127.0.0.1:9010",
});
```

See [packages/primadb/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb/README.md)
for the full package-specific flow.

For a real browser app that consumes the package through npm and Vite, see
[examples/browser-package-notes-vite/README.md](/home/bitnom/Code/gunport/primadb/examples/browser-package-notes-vite/README.md).

For runnable package-local browser examples, see
[packages/primadb/examples/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb/examples/README.md).

## Native Node Package

Primadb also now has a native Node.js package in
[packages/primadb-node](/home/bitnom/Code/gunport/primadb/packages/primadb-node). Unlike the
browser package, this one wraps the native Rust runtime directly through a Node addon.

Build it with:

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb-node
npm install
npm run build
```

Example usage:

```js
import { Primadb } from "primadb-node";

const db = new Primadb("node-a");
db.openDurableStorage({
  kind: "segment_files",
  directory: "/tmp/primadb-node-demo",
});

db.chain("notes").field("items").set({
  title: "Native Node note",
  body: "Stored through the Node addon",
  createdAt: new Date().toISOString(),
});
```

See [packages/primadb-node/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-node/README.md)
for the full package-specific flow.

For runnable package-local Node examples, see
[packages/primadb-node/examples/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-node/examples/README.md).

## Native Python Package

Primadb also now has a native Python package in
[packages/primadb-python](/home/bitnom/Code/gunport/primadb/packages/primadb-python). Like the
Node package, it wraps the native Rust runtime directly instead of the browser WASM layer.

Install it locally with:

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb-python
uv sync
```

Example usage:

```python
from primadb import Primadb

db = Primadb("python-a")
db.open_durable_storage(
    {
        "kind": "segment_files",
        "directory": "/tmp/primadb-python-demo",
    }
)

db.chain("notes").field("items").set(
    {
        "title": "Native Python note",
        "body": "Stored through the Python extension",
    }
)
```

Smoke it with:

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb-python
uv sync
uv run python scripts/smoke_core.py
uv run python scripts/smoke_relay.py
uv run python scripts/smoke_mesh.py
uv run python scripts/pack_check.py
```

See [packages/primadb-python/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-python/README.md)
for the package-specific flow.

For runnable package-local Python examples, see
[packages/primadb-python/examples/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-python/examples/README.md).

## Replication Contract

Primadb does not hide the wire format from you.

1. Local writes append to `pending_operations()`.
2. `sync_envelope()` / `export_pending_operations_json()` packages them for transport.
3. Peers call `apply_sync_envelope()`, `apply_operations_json()`, or `apply_operations()` with the received payload.

That makes replication easy to test and keeps transport policy outside the core database.

The browser `WebSocketSync` helper adds:

- `sync` frames with message IDs.
- `ack` frames with applied counts.
- typed remote `get` / `query` / `lex` / `snapshot` pull requests and responses.
- batched route payloads and chunked reply assembly for large query and snapshot results.
- `reply_to`, content-hash, and `seen_by` metadata for better relay dedupe and loop suppression.
- peer recommendation exchange alongside presence.
- automatic resend of unacked messages on an interval.
- requeue of in-flight operations if the socket closes or send fails.

The Gun-compatible runtime layers a DAM-style browser relay client on top of those same primitives:

- peer presence and discovery over the relay
- targeted routing and signal payloads
- sync/ack over the relay without `BroadcastChannel`
- replay of active browser chain interests to newly discovered peers via root snapshot hydration
- current Gun-style session recall and browser `user()` flows

## Examples

- [examples/browser-notes/README.md](/home/bitnom/Code/gunport/primadb/examples/browser-notes/README.md): Browser-only local-first board with IndexedDB persistence and cross-tab sync over `BroadcastChannel`.
- [examples/browser-segment-notes/README.md](/home/bitnom/Code/gunport/primadb/examples/browser-segment-notes/README.md): Browser-only local-first board using the canonical node/index segment records in IndexedDB plus cross-tab sync over `BroadcastChannel`.
- [examples/browser-relay-notes/README.md](/home/bitnom/Code/gunport/primadb/examples/browser-relay-notes/README.md): Browser board using Primadb's relay client API, automatic IndexedDB persistence, and the included relay server.
- [examples/browser-mesh-notes/README.md](/home/bitnom/Code/gunport/primadb/examples/browser-mesh-notes/README.md): Default browser mesh board using Primadb's shared `connectMesh(...)` facade, relay-backed signaling by default, optional `BroadcastChannel` fallback, and browser/native smoke coverage.
- [examples/browser-gun-notes/README.md](/home/bitnom/Code/gunport/primadb/examples/browser-gun-notes/README.md): Gun-compatible browser app using `js/primadb-gun.js`, SEA-style users, the DAM relay, and a browser runtime smoke test for `load/not/map/back`.
- [examples/browser-package-notes-vite/README.md](/home/bitnom/Code/gunport/primadb/examples/browser-package-notes-vite/README.md): Vite browser app that installs the local `primadb` npm package, exercises IndexedDB-backed persistence, and can optionally join the relay-signaled mesh through query params.
- [packages/primadb/examples/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb/examples/README.md): package-local browser demos for the default and threaded npm entrypoints.
- [packages/primadb-node/examples/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-node/examples/README.md): package-local native Node demos for durable local storage and relay-signaled mesh peers.
- [packages/primadb-python/examples/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-python/examples/README.md): package-local native Python demos for durable local storage and relay-signaled mesh peers.
- [examples/browser-threaded-query/README.md](/home/bitnom/Code/gunport/primadb/examples/browser-threaded-query/README.md): Opt-in `wasm-threads` browser demo that initializes `initThreadPool(...)` and exercises the Rayon-backed query path under COOP/COEP.
- [examples/browser-threaded-mesh-notes/README.md](/home/bitnom/Code/gunport/primadb/examples/browser-threaded-mesh-notes/README.md): Opt-in `wasm-threads` browser P2P demo using `WebRtcMesh`, relay-backed signaling by default, COOP/COEP serving, configurable ICE servers, and a threaded shared-query workload over WebRTC-synced notes.
- [examples/ws_relay_server.rs](/home/bitnom/Code/gunport/primadb/examples/ws_relay_server.rs): DAM-style Rust WebSocket relay with peer presence, targeted routing, and signaling, runnable with `cargo run --example ws_relay_server -- 127.0.0.1:9010`.
- [examples/native_relay_client.rs](/home/bitnom/Code/gunport/primadb/examples/native_relay_client.rs): Native relay client, runnable with `cargo run --features native-websocket --example native_relay_client -- ws://127.0.0.1:9010`.
- [examples/native_relay_probe.rs](/home/bitnom/Code/gunport/primadb/examples/native_relay_probe.rs): Native relay probe used by the browser/native and native/native relay smoke tests.
- [examples/native_mesh_probe.rs](/home/bitnom/Code/gunport/primadb/examples/native_mesh_probe.rs): Native WebRTC mesh probe interoperable with the browser relay-signaled mesh.
- [examples/native_mesh_agent.rs](/home/bitnom/Code/gunport/primadb/examples/native_mesh_agent.rs): Native mesh/storage agent used by the mixed-target end-to-end suite.
- [examples/test-all-targets-mesh-e2e.sh](/home/bitnom/Code/gunport/primadb/examples/test-all-targets-mesh-e2e.sh): Cross-target suite that builds and runs the default WASM demo, threaded WASM demo, npm browser app, native Node package, native Python package, and Rust native mesh together.
- [examples/native_parallel_query.rs](/home/bitnom/Code/gunport/primadb/examples/native_parallel_query.rs): Native Rayon verification example, runnable with `cargo run --example native_parallel_query`.
- [examples/crypto_foundation.rs](/home/bitnom/Code/gunport/primadb/examples/crypto_foundation.rs): Signing and encryption primitives, runnable with `cargo run --features crypto --example crypto_foundation`.
- [examples/authenticated_sync.rs](/home/bitnom/Code/gunport/primadb/examples/authenticated_sync.rs): Signed and encrypted sync policy demo, runnable with `cargo run --features crypto --example authenticated_sync`.
- [examples/radisk_storage.rs](/home/bitnom/Code/gunport/primadb/examples/radisk_storage.rs): Incremental segment-backed native storage demo through the current `use_radisk_storage(...)` entrypoint, runnable with `cargo run --example radisk_storage`.
- [examples/gun_compat.rs](/home/bitnom/Code/gunport/primadb/examples/gun_compat.rs): Gun-compatible API demo, runnable with `cargo run --example gun_compat`.

## Running Examples

Standard browser build:

```bash
cd /home/bitnom/Code/gunport/primadb
./build-wasm.sh
```

Threaded browser build:

```bash
cd /home/bitnom/Code/gunport/primadb
./build-wasm-threads.sh
```

Vite browser app consuming the npm package:

```bash
cd /home/bitnom/Code/gunport/primadb/examples/browser-package-notes-vite
npm install
npm run dev
```

Open:

```text
http://127.0.0.1:4182/
```

To run that same package app in the shared mesh:

```text
http://127.0.0.1:4182/?room=demo-room&signal=relay&relay=ws://127.0.0.1:9010
```

Local-only browser board:

```bash
cd /home/bitnom/Code/gunport/primadb
./examples/browser-notes/build.sh
./examples/browser-notes/serve.sh
```

Open:

```text
http://127.0.0.1:4173/examples/browser-notes/
```

Relay-backed browser board:

```bash
cd /home/bitnom/Code/gunport/primadb
./examples/browser-relay-notes/build.sh
cargo run --example ws_relay_server -- 127.0.0.1:9010
```

In a second terminal:

```bash
cd /home/bitnom/Code/gunport/primadb
./examples/browser-relay-notes/serve.sh
```

Open:

```text
http://127.0.0.1:4173/examples/browser-relay-notes/
```

Default relay-signaled WebRTC mesh example:

```bash
cd /home/bitnom/Code/gunport/primadb
./examples/browser-mesh-notes/build.sh
cargo run --example ws_relay_server -- 127.0.0.1:9010
```

In a second terminal:

```bash
cd /home/bitnom/Code/gunport/primadb
./examples/browser-mesh-notes/serve.sh
```

Open:

```text
http://127.0.0.1:4173/examples/browser-mesh-notes/
```

Gun-style relay example:

```bash
cd /home/bitnom/Code/gunport/primadb
./examples/browser-gun-notes/build.sh
cargo run --example ws_relay_server -- 127.0.0.1:9010
```

In a second terminal:

```bash
cd /home/bitnom/Code/gunport/primadb
./examples/browser-gun-notes/serve.sh
```

Open:

```text
http://127.0.0.1:4173/examples/browser-gun-notes/
```

Threaded relay-signaled WebRTC mesh example:

```bash
cd /home/bitnom/Code/gunport/primadb
./examples/browser-threaded-mesh-notes/build.sh
cargo run --example ws_relay_server -- 127.0.0.1:9010
```

In a second terminal:

```bash
cd /home/bitnom/Code/gunport/primadb
./examples/browser-threaded-mesh-notes/serve.sh
```

Open:

```text
http://127.0.0.1:4175/examples/browser-threaded-mesh-notes/
```

Cross-target mesh and storage suite:

```bash
cd /home/bitnom/Code/gunport/primadb
bash examples/test-all-targets-mesh-e2e.sh
```

For faster reruns while iterating on the harness itself:

```bash
cd /home/bitnom/Code/gunport/primadb
PRIMADB_E2E_SKIP_BUILD=1 bash examples/test-all-targets-mesh-e2e.sh
```

Browser smoke tests:

```bash
cd /home/bitnom/Code/gunport/primadb
bash examples/browser-segment-notes/test-live-sync.sh
bash examples/browser-relay-notes/test-browser-native-smoke.sh
bash examples/browser-mesh-notes/test-two-page-smoke.sh
bash examples/browser-mesh-notes/test-browser-native-smoke.sh
bash examples/browser-threaded-mesh-notes/test-two-page-smoke.sh
bash examples/browser-threaded-mesh-notes/test-browser-native-smoke.sh
bash examples/browser-threaded-mesh-notes/test-cross-browser-smoke.sh
bash examples/browser-gun-notes/test-runtime-smoke.sh
bash examples/test-native-relay-smoke.sh
bash examples/test-native-mesh-smoke.sh
```

Package browser smoke:

```bash
cd /home/bitnom/Code/gunport/primadb/examples/browser-package-notes-vite
npm run smoke
```

## Query Layer

Rust:

```rust
let results = db
    .root("users")
    .find()
    .where_eq("profile.city", "Boston")?
    .where_gte("age", 30)?
    .order_by("name", primadb::QueryDirection::Desc)
    .limit(10)
    .run()?;
```

Browser:

```js
const results = db.chain("users").query({
  filters: [
    { kind: "eq", path: "profile.city", value: "Boston" },
    { kind: "gte", path: "age", value: 30 },
  ],
  order: { path: "name", direction: "desc" },
  limit: 10,
});
```

Supported filters:

- `eq`, `ne`
- `gt`, `gte`, `lt`, `lte`
- `prefix`, `contains`
- `exists`

Use `"$key"` to filter or sort by the entry key, and `"$value"` for the full materialized value.

Lexical traversal:

```rust
let entries = db
    .root("users")
    .field("alice")
    .lex()
    .follow_links(true)
    .depth(3)
    .prefix("p")
    .run()?;
```

## Auth and User Policies

Enable the `crypto` feature for identities, trusted users, signed sync, encrypted sync, and encrypted snapshot persistence:

```bash
cargo run --features crypto --example crypto_foundation
```

Available pieces include:

- `Identity` / `PublicIdentity` for Ed25519 signing and verification.
- `SeaPair` for Gun-style `{ pub, epub, priv, epriv }` key material.
- `SignedPayload<T>` for signed JSON payloads such as `SyncFrame`.
- `SignedValueClaims` and `DataCertificate` for field-level auth in owned user graphs.
- `SecretBoxKey` / `EncryptedPayload` for XChaCha20-Poly1305 JSON encryption.
- `register_user(...)` / `authenticate_local_user(...)` on `Primadb`.
- `Chain::put_signed(...)`, `Chain::set_signed(...)`, and `create_write_certificate(...)`.
- `set_require_signed_sync(...)`, `set_transport_encryption_key(...)`, and `set_snapshot_encryption_key(...)`.
- browser `generateSeaPair()`, `seaPairFromPrivateKeys()`, `seaSign()`, `seaVerify()`, `seaEncrypt()`, `seaDecrypt()`, and `seaSecret()` WASM exports behind `crypto`

## Routing and Mesh

Primadb now routes transport messages through `RouteEnvelope` with:

- peer presence announcements
- peer recommendation exchange
- routed sync payloads
- routed pull request/response payloads
- batch payloads for grouped route delivery
- signaling payloads
- snapshot request/response
- reply correlation, content hashes, seen-by hints, TTL, and dedupe tracking

The browser build includes three network convenience entrypoints:

- `connectRelay(...)` for relay-backed sync and remote pull/query
- `connectMesh(...)` for the shared mesh facade, including relay signaling and configurable ICE servers
- `connectWebRtcMesh(...)` / `connectWebRtcMeshViaRelay(...)` as narrower browser aliases over the same mesh surface

Both paths support configurable ICE servers, and default to a small STUN set when you do not
provide one explicitly.

The included relay example upgrades that into a networked DAM-style path:

- browsers announce presence to the relay
- new clients receive the current peer set plus peer recommendations on connect
- targeted `peer` routes are forwarded only to the addressed client
- disconnects broadcast offline presence so peer lists converge

## Native Sync And Mesh

```bash
cargo run --features native-websocket --example native_relay_client -- ws://127.0.0.1:9010
```

```bash
cargo run --features native-webrtc --example native_mesh_probe -- --relay ws://127.0.0.1:9010 --room demo --action status
```

## Storage Adapters

Primadb supports:

- snapshot file persistence with `use_file_persistence(...)`
- browser `localStorage` persistence with `use_browser_storage(...)`
- explicit IndexedDB persistence hooks in WASM
- shared durable storage configuration via `open_durable_storage(...)` / `openDurableStorage(...)`
- storage adapters via `attach_storage_adapter(...)`
- a RADisk-style file adapter with `use_radisk_storage(...)`

## Data Markers

- `{"$link": "node-id"}` sets a field to an explicit graph link.
- `{"#": "node-id"}` is accepted as a Gun-compatible link marker.
- `{"$set": [ ... ]}` sets a field to a membership set.
- Materialized nodes include `"$id"`.
- Cycles are represented as `{"$ref": "node-id"}`.

## Verification

```bash
cargo test
cargo test --features "crypto native-websocket"
cargo check --target wasm32-unknown-unknown --features crypto
cargo check --example ws_relay_server --features crypto
./examples/browser-relay-notes/build.sh
./examples/browser-gun-notes/build.sh
```

The browser mesh example was verified in a clean two-tab browser run:

- both tabs reached `connected to 1 peer over WebRTC`
- a note created in one tab appeared in the other
- both tabs remained responsive after sync

The Gun runtime demo was verified in the built-in browser context as well:

- a second same-origin client joined through the relay and both clients reported discovered peers
- a note created by the second client appeared in the first client's `gun.get(...).get(...).set(...)` collection
- a signed profile written through the Gun runtime was stored as SEA envelopes in the raw Primadb snapshot and still read back correctly through the runtime
- `Gun.SEA.sign(...)` and `Gun.SEA.verify(...)` succeeded in-browser during the same run

The relay example was verified in the built-in browser context with a hidden same-origin second client:

- both clients discovered each other through peer recommendations
- `remoteGet()`, `remoteQuery()`, `remoteLex()`, and `remoteSnapshot()` succeeded over the relay
- query and full-database snapshot responses crossed the chunk thresholds and were reassembled correctly in-browser

## Coverage

The originally identified gaps are now covered by concrete subsystems in this repo:

- Gun-compatible browser runtime
- SEA-style `user()` and browser crypto surface
- DAM-style relay routing and internet-scale peer discovery/signaling
- remote pull/query over the wire
- batched routes plus chunked query/snapshot replies
- reply correlation and content-hash dedupe hints
- data-level signed field auth and delegated certificates in the core
- browser relay sync without `BroadcastChannel`
- lexical/range traversal
- RADisk-style storage adapters
- browser and native example integrations
