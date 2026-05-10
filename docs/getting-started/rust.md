---
title: Rust Quickstart
sidebar_position: 2
---

The Rust crate is the canonical core surface. It is the best place to understand PrimaDB’s actual
merge model, query model, and storage behavior.

## Minimal Example

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

    let snapshot = db.root("users").field("alice").once_json()?;
    println!("{}", serde_json::to_string_pretty(&snapshot)?);

    Ok(())
}
```

## Queries

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

## Durable Storage

Native storage is available through:

- `use_file_persistence(...)`
- `use_segment_storage(...)`
- `open_durable_storage(...)`
- `open_blob_storage(...)`

PrimaDB’s current native storage path is incremental and segment-backed rather than a pure
append-only replay-everything model.

## Native Relay And Mesh

Relay:

```bash
cargo run --features native-websocket --example native_relay_client -- ws://127.0.0.1:9010
```

Mesh:

```bash
cargo run --features native-webrtc --example native_mesh_probe -- --relay ws://127.0.0.1:9010 --room demo --action status
```

## Source Of Truth

If you want the underlying implementation details, start in:

- [src/db.rs](https://github.com/Apothic-AI/PrimaDB/tree/master/src/db.rs)
- [src/router.rs](https://github.com/Apothic-AI/PrimaDB/tree/master/src/router.rs)
- [src/sync.rs](https://github.com/Apothic-AI/PrimaDB/tree/master/src/sync.rs)
- [src/auth.rs](https://github.com/Apothic-AI/PrimaDB/tree/master/src/auth.rs)
