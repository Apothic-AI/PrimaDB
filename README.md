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
- Optional native WebSocket sync adapter behind the `native-websocket` feature.
- Optional crypto/auth groundwork behind the `crypto` feature.
- `wasm-bindgen` bindings that compile on `wasm32-unknown-unknown`.

## Design Notes

This is intentionally not a 1:1 port of Gun internals.

- Primadb stores version markers per field and tombstone.
- Set membership tracks both add and remove markers so concurrent `set()` / `remove()` operations converge.
- Writes are turned into explicit operations.
- Replication is transport-agnostic.
- Nested objects become linked graph nodes with deterministic path-derived IDs, so replicas converge on the same intermediate graph structure.
- Browser auto-persistence ignores the transient “drained for transport” state so in-flight writes are not silently lost on reload before ack.
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
- [examples/ws_relay_server.rs](/home/bitnom/Code/gunport/primadb/examples/ws_relay_server.rs): Minimal Rust WebSocket relay, runnable with `cargo run --example ws_relay_server -- 127.0.0.1:9010`.
- [examples/native_relay_client.rs](/home/bitnom/Code/gunport/primadb/examples/native_relay_client.rs): Native relay client, runnable with `cargo run --features native-websocket --example native_relay_client -- ws://127.0.0.1:9010`.
- [examples/crypto_foundation.rs](/home/bitnom/Code/gunport/primadb/examples/crypto_foundation.rs): Signing and encryption demo, runnable with `cargo run --features crypto --example crypto_foundation`.

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

## Auth Groundwork

Enable the `crypto` feature for identity, signing, and envelope-encryption primitives:

```bash
cargo run --features crypto --example crypto_foundation
```

Available primitives include:

- `Identity` / `PublicIdentity` for Ed25519 signing and verification.
- `SignedPayload<T>` for signed JSON payloads such as `SyncFrame`.
- `SecretBoxKey` / `EncryptedPayload` for XChaCha20-Poly1305 JSON encryption.

## Native Sync

Enable the `native-websocket` feature for a runtime-backed native relay client:

```bash
cargo run --features native-websocket --example native_relay_client -- ws://127.0.0.1:9010
```

## Data Markers

- `{"$link": "node-id"}` sets a field to an explicit graph link.
- `{"$set": [ ... ]}` sets a field to a membership set.
- Materialized nodes include `"$id"`.
- Cycles are represented as `{"$ref": "node-id"}`.

## Verification

```bash
cargo test
cargo check --target wasm32-unknown-unknown
cargo test --features "crypto native-websocket"
```

## Near-Term Gaps

The foundation is in place, but this is not full Gun parity yet.

- No SEA/auth/encryption layer yet beyond the current identity/signing/encryption groundwork.
- No peer discovery or WebRTC transport yet.
- No lexical graph/range traversal engine yet beyond the current filter-based query layer.
- The crypto module is groundwork only; it is not yet enforced by the sync adapters.
