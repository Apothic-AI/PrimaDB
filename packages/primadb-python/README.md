# `primadb-python`

`primadb-python` is a native Python package for Primadb. Like
[packages/primadb-node](/home/bitnom/Code/gunport/primadb/packages/primadb-node), it wraps the
native Rust runtime directly instead of going through the browser WASM layer.

Current surface:

- `Primadb` and `Chain` for local graph operations
- durable native storage through `open_durable_storage(...)`
- content-addressed native blob storage through `open_blob_storage(...)`
- first-class binary helpers through `put_bytes()`, `once_bytes()`, `put_blob()`, and `get_blob()`
- subscriptions
- native relay sync through `connect_relay(...)`
- native WebRTC mesh through `connect_mesh(...)`
- live remote watches through `watch_remote_get(...)`, `watch_remote_map(...)`, `watch_remote_query(...)`, `watch_remote_lex(...)`, and `watch_remote_snapshot(...)`

## Package Examples

Runnable package-local examples live under [examples/](/home/bitnom/Code/gunport/primadb/packages/primadb-python/examples):

- [examples/local_notes/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-python/examples/local_notes/README.md)
- [examples/mesh_peer/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-python/examples/mesh_peer/README.md)

## Install

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb-python
uv sync
```

Then run commands through `uv run`:

```bash
uv run python -c "from primadb import Primadb; print(Primadb().replica_id())"
```

## Example

```python
from primadb import Primadb

db = Primadb("py-a")
db.open_durable_storage(
    {
        "kind": "segment_files",
        "directory": "/tmp/primadb-python-demo",
    }
)
db.open_blob_storage(
    {
        "kind": "files",
        "directory": "/tmp/primadb-python-demo-blobs",
    }
)

db.chain("notes").field("items").set(
    {
        "title": "Python note",
        "body": "Stored through the native extension",
    }
)
db.chain("assets").field("avatar").put_bytes(b"\x01\x02\x03\x04")
db.chain("assets").field("archive").put_blob(
    b"\x05\x06\x07\x08",
    "application/octet-stream",
)
```

## Smoke Tests

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb-python
uv sync
uv run python scripts/smoke_core.py
uv run python scripts/smoke_relay.py
uv run python scripts/smoke_mesh.py
uv run python scripts/pack_check.py
```
