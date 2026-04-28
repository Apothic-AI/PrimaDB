# `primadb-python`

`primadb-python` is a native Python package for Primadb. Like
[packages/primadb-node](/home/bitnom/Code/gunport/primadb/packages/primadb-node), it wraps the
native Rust runtime directly instead of going through the browser WASM layer.

Current surface:

- `Primadb` and `Chain` for local graph operations
- `Scope` and step-based transactions for local ACID writes and coordinated strict-scope proposals
- durable native storage through `open_durable_storage(...)`
- content-addressed native blob storage through `open_blob_storage(...)`
- first-class binary helpers through `put_bytes()`, `once_bytes()`, `put_blob()`, and `get_blob()`
- subscriptions
- native relay server hosting through `RelayServer.listen(...)`
- native relay sync through `connect_relay(...)`, including disconnected startup with background relay retry
- remote strict-scope transaction submission through `remote_transaction(...)` on relay sync clients
- native WebRTC mesh through `connect_mesh(...)`, including disconnected startup with background relay retry
- live remote watches through `watch_remote_get(...)`, `watch_remote_map(...)`, `watch_remote_query(...)`, `watch_remote_lex(...)`, and `watch_remote_snapshot(...)`
- network-boundary hooks through `set_network_hooks(...)` / `clear_network_hooks()`
- experimental MoQ path/track/frame helpers through `primadb.moq`

## Package Examples

Runnable package-local examples live under [examples/](/home/bitnom/Code/gunport/primadb/packages/primadb-python/examples):

- [examples/local_notes/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-python/examples/local_notes/README.md)
- [examples/mesh_peer/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-python/examples/mesh_peer/README.md)
- [examples/full_node/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-python/examples/full_node/README.md)
- [examples/moq_sync/README.md](/home/bitnom/Code/gunport/primadb/packages/primadb-python/examples/moq_sync/README.md)

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
db.scope("ledger").configure(
    {
        "consistency": "coordinated",
        "authority": {"kind": "full_node", "peerId": "native:py-a"},
    }
)
db.scope("ledger").transaction(
    [
        {
            "kind": "increment",
            "path": {"anchor": "alice", "segments": ["balance"]},
            "by": 10,
        }
    ]
)
db.chain("assets").field("avatar").put_bytes(b"\x01\x02\x03\x04")
db.chain("assets").field("archive").put_blob(
    b"\x05\x06\x07\x08",
    "application/octet-stream",
)


class PrivateDocsHooks:
    def on_pull(self, context):
        request = context["request"]
        if request["kind"] == "get" and request["path"]["anchor"] == "private":
            return "private root denied"
        return None

    def on_serve_result(self, _context, result):
        if result["kind"] == "get":
            return {"kind": "get", "value": {"masked": True}}
        return None


db.set_network_hooks(PrivateDocsHooks())
```

## Smoke Tests

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb-python
uv sync
uv run python scripts/smoke_core.py
uv run python scripts/smoke_hooks.py
uv run python scripts/smoke_relay_server.py
uv run python scripts/smoke_relay.py
uv run python scripts/smoke_relay_offline.py
uv run python scripts/smoke_mesh.py
uv run python scripts/pack_check.py
```
