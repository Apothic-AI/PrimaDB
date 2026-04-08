# primadb

`primadb` is a Rust-native, local-first graph database inspired by Gun, but built around explicit versioned operations instead of Gun's implicit event mesh. The current codebase focuses on deterministic merge behavior, browser compatibility, and a clean replication boundary that can be driven by WebSockets, WebRTC, service workers, or any other transport you want to layer on top.

## Current Capabilities

- Graph-shaped documents with nested object writes.
- Gun-style path traversal through a chain API.
- Query layer with filter/order/limit support over node fields and set members.
- Per-field last-write-wins conflict resolution with hybrid logical revisions.
- Set membership via `set()` / `remove()` or `{"$set": [...]}` markers.
- Link references via `{"$link": "node-id"}` markers.
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
- Routed transport envelopes with presence, signaling, snapshot request/response, TTL, and dedupe.
- Browser peer discovery over `BroadcastChannel` plus direct WebRTC mesh sync.
- Optional native WebSocket sync adapter behind the `native-websocket` feature.
- Integrated auth/user policies behind the `crypto` feature, including trusted users, local user sessions, signed sync, encrypted sync, and encrypted snapshot persistence.
- Storage adapter ecosystem with an in-memory adapter, snapshot-file adapter, and RADisk-style append-log file adapter.
- Lexical/range traversal via `chain.lex()` / `chain.scan(...)`.
- Gun compatibility surface with `Gun` / `GunChain`, Gun link markers, and Gun graph import/export helpers.
- Runtime stats and limit controls for transport and queue hardening.
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

That gives the project a more inspectable merge model and makes it easier to test and evolve without carrying over Gun's event-routing bugs.

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

const user = db.chain("users").field("alice");
user.put({
  name: "Alice",
  profile: { timezone: "America/New_York" },
});

const sub = user.on((value) => {
  console.log("update", value);
});

const matches = db.chain("users").query({
  filters: [{ kind: "prefix", path: "name", value: "A" }],
  order: { path: "name", direction: "asc" },
  limit: 10,
});

const peer = db.connectWebSocket("ws://127.0.0.1:9010", 2000);
peer.flushPending();

sub.cancel();
persistence.close();
peer.close();
```

## Replication Contract

Primadb does not hide the wire format from you.

1. Local writes append to `pending_operations()`.
2. `sync_envelope()` / `export_pending_operations_json()` packages them for transport.
3. Peers call `apply_sync_envelope()`, `apply_operations_json()`, or `apply_operations()` with the received payload.

That makes replication easy to test and keeps transport policy outside the core database.

The browser `WebSocketSync` helper adds:

- `sync` frames with message IDs.
- `ack` frames with applied counts.
- automatic resend of unacked messages on an interval.
- requeue of in-flight operations if the socket closes or send fails.

## Examples

- [examples/browser-notes/README.md](/home/bitnom/Code/gunport/primadb/examples/browser-notes/README.md): Browser-only local-first board with IndexedDB persistence and cross-tab sync over `BroadcastChannel`.
- [examples/browser-relay-notes/README.md](/home/bitnom/Code/gunport/primadb/examples/browser-relay-notes/README.md): Browser board using Primadb's `WebSocketSync` API and the included relay server.
- [examples/browser-mesh-notes/README.md](/home/bitnom/Code/gunport/primadb/examples/browser-mesh-notes/README.md): Browser board using Primadb's `WebRtcMesh` API, peer discovery over `BroadcastChannel`, and direct WebRTC data-channel sync.
- [examples/ws_relay_server.rs](/home/bitnom/Code/gunport/primadb/examples/ws_relay_server.rs): Minimal Rust WebSocket relay, runnable with `cargo run --example ws_relay_server -- 127.0.0.1:9010`.
- [examples/native_relay_client.rs](/home/bitnom/Code/gunport/primadb/examples/native_relay_client.rs): Native relay client, runnable with `cargo run --features native-websocket --example native_relay_client -- ws://127.0.0.1:9010`.
- [examples/crypto_foundation.rs](/home/bitnom/Code/gunport/primadb/examples/crypto_foundation.rs): Signing and encryption primitives, runnable with `cargo run --features crypto --example crypto_foundation`.
- [examples/authenticated_sync.rs](/home/bitnom/Code/gunport/primadb/examples/authenticated_sync.rs): Signed and encrypted sync policy demo, runnable with `cargo run --features crypto --example authenticated_sync`.
- [examples/radisk_storage.rs](/home/bitnom/Code/gunport/primadb/examples/radisk_storage.rs): RADisk-style append-log storage demo, runnable with `cargo run --example radisk_storage`.
- [examples/gun_compat.rs](/home/bitnom/Code/gunport/primadb/examples/gun_compat.rs): Gun-compatible API demo, runnable with `cargo run --example gun_compat`.

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
- `SignedPayload<T>` for signed JSON payloads such as `SyncFrame`.
- `SecretBoxKey` / `EncryptedPayload` for XChaCha20-Poly1305 JSON encryption.
- `register_user(...)` / `authenticate_local_user(...)` on `Primadb`.
- `set_require_signed_sync(...)`, `set_transport_encryption_key(...)`, and `set_snapshot_encryption_key(...)`.

## Routing and Mesh

Primadb now routes transport messages through `RouteEnvelope` with:

- peer presence announcements
- routed sync payloads
- signaling payloads
- snapshot request/response
- TTL and dedupe tracking

The browser build also includes `connectWebRtcMesh(...)`, which uses `BroadcastChannel` for local peer discovery/signaling and WebRTC data channels for direct sync.

## Native Sync

```bash
cargo run --features native-websocket --example native_relay_client -- ws://127.0.0.1:9010
```

## Storage Adapters

Primadb supports:

- snapshot file persistence with `use_file_persistence(...)`
- browser `localStorage` persistence with `use_browser_storage(...)`
- explicit IndexedDB persistence hooks in WASM
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
cargo check --target wasm32-unknown-unknown
cargo test --features "crypto native-websocket"
```

The browser mesh example was also verified in a clean two-tab browser run:

- both tabs reached `connected to 1 peer over WebRTC`
- a note created in one tab appeared in the other
- both tabs remained responsive after sync

## Coverage

The originally identified gaps are now covered by concrete subsystems in this repo:

- integrated auth/user policies
- routed networking
- browser peer discovery and WebRTC mesh sync
- RADisk-style storage adapters
- lexical/range traversal
- Gun compatibility helpers
- hardening controls and example integrations
