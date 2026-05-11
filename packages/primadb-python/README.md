# `primadb-python`

`primadb-python` is a native Python package for Primadb. Like
[packages/primadb-node](../primadb-node), it wraps the
native Rust runtime directly instead of going through the browser WASM layer.

Current surface:

- `Primadb` and `Chain` for local graph operations
- `Scope` and step-based transactions for local ACID writes and coordinated strict-scope proposals
- durable native storage through `open_durable_storage(...)`
- explicit SegmentFiles sync/recovery/close helpers through `sync_storage()`, `storage_recovery_report()`, and `close_durable_storage()`
- content-addressed native blob storage through `open_blob_storage(...)`
- first-class binary helpers through `put_bytes()`, `once_bytes()`, `put_blob()`, and `get_blob()`
- graph-native keyed records through `put_record(...)`, `put_record_bytes(...)`, `put_record_blob(...)`, `get_record(...)`, `scan_records(...)`, `watch_records(...)`, `apply_record_batch(...)`, and `delete_record(...)`
- subscriptions
- native relay server hosting through `RelayServer.listen(...)`
- native relay sync through `connect_relay(...)`, including disconnected startup with background relay retry
- remote strict-scope transaction submission through `remote_transaction(...)` on relay sync clients
- native WebRTC mesh through `connect_mesh(...)`, including disconnected startup with background relay retry
- peer-agnostic relay pulls through `get(...)`, `query(...)`, `lex(...)`, `records(...)`, `node(...)`, and `snapshot(...)`, with optional `RemoteInterestPolicy` constraints
- peer-agnostic relay/mesh watches through `watch_get(...)`, `watch_map(...)`, `watch_query(...)`, `watch_lex(...)`, `watch_records(...)`, `watch_node(...)`, and `watch_snapshot(...)`
- application RouteEnvelope payloads through `publish_application(...)`, `send_application(...)`, and `subscribe_applications(...)`
- source-tagged multi-peer record fan-in through `records_fan_in(...)` and `watch_records_fan_in(...)`
- explicit peer-targeted pulls and watches through `remote_get(...)`, `remote_query(...)`, `remote_lex(...)`, `remote_records(...)`, `remote_node(...)`, `remote_snapshot(...)`, `watch_remote_get(...)`, `watch_remote_map(...)`, `watch_remote_query(...)`, `watch_remote_lex(...)`, `watch_remote_records(...)`, `watch_remote_node(...)`, and `watch_remote_snapshot(...)`
- authenticated relay/mesh session identity through `generate_identity()`, `authenticate_local_user(...)`, `sessionAuth` config, and `context["verifiedIdentity"]`
- Argon2id password-derived secret-box keys through `derive_password_key(...)`, usable with `set_snapshot_encryption_key(...)` and `set_transport_encryption_key(...)`
- node-attached scripting through `attach_node_script(...)`, `node_scripts(...)`, `remove_node_script(...)`, and `execute_node_scripts(...)`
- network-boundary hooks through `set_network_hooks(...)` / `clear_network_hooks()`
- experimental MoQ path/track/frame helpers through `primadb.moq`

## Package Examples

Runnable package-local examples live under [examples/](examples):

- [examples/local_notes/README.md](examples/local_notes/README.md)
- [examples/mesh_peer/README.md](examples/mesh_peer/README.md)
- [examples/full_node/README.md](examples/full_node/README.md)
- [examples/moq_sync/README.md](examples/moq_sync/README.md)

## Install

```bash
cd /path/to/primadb/packages/primadb-python
uv sync
```

Then run commands through `uv run`:

```bash
uv run python -c "from primadb import Primadb; print(Primadb().replica_id())"
```

## Example

```python
from primadb import Primadb, derive_password_key

db = Primadb("py-a")
key = derive_password_key(
    "correct horse battery staple",
    {"memoryCostKiB": 64 * 1024, "timeCost": 3, "parallelism": 1},
)
db.set_snapshot_encryption_key(key["keyBase64"])
db.open_durable_storage(
    {
        "kind": "segment_files",
        "directory": "/tmp/primadb-python-demo",
        "durability": "full",
        "lockMode": {"kind": "exclusive"},
    }
)
db.open_blob_storage(
    {
        "kind": "files",
        "directory": "/tmp/primadb-python-demo-blobs",
        "durability": "full",
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

db.put_record("agentfs/inodes/1", {"mode": "file", "size": 4})
db.put_record_bytes("agentfs/chunks/1/000000", b"\x01\x02\x03\x04")
chunks = db.scan_records({"prefix": "agentfs/chunks/1/", "limit": 100})
print(len(chunks["entries"]))
db.apply_record_batch(
    {
        "preconditions": [{"kind": "exists", "key": "agentfs/inodes/1"}],
        "mutations": [],
    }
)
db.sync_storage()

script_path = {"anchor": "notes", "segments": ["scripted"]}
script_capabilities = {
    "read": [{"root": "notes", "recursive": True}],
    "write": [{"root": "derived", "recursive": True}],
    "transaction": [{"root": "derived", "recursive": True}],
}
db.chain("notes").field("scripted").put({"title": "Scripted note"})
db.attach_node_script(
    script_path,
    {
        "id": "derive-title",
        "source": """
            fn main(ctx) {
                let note = db_get("notes/scripted");
                db_put("derived/scripted", #{ title: note.title, source: ctx.path.display });
                return #{ title: note.title };
            }
        """,
        "capabilities": script_capabilities,
    },
)
db.execute_node_scripts(script_path, {"capabilities": script_capabilities})


class PrivateDocsHooks:
    def on_pull(self, context):
        if context.get("verifiedIdentity", {}).get("alias") == "team-a":
            return None
        request = context["request"]
        if request["kind"] == "get" and request["path"]["anchor"] == "private":
            return "private root denied"
        return None

    def on_serve_result(self, _context, result):
        if result["kind"] == "get":
            return {"kind": "get", "value": {"masked": True}}
        return None


db.set_network_hooks(PrivateDocsHooks())
db.close_durable_storage()
```

## Smoke Tests

```bash
cd /path/to/primadb/packages/primadb-python
uv sync
uv run python scripts/smoke_core.py
uv run python scripts/smoke_hooks.py
uv run python scripts/smoke_relay_server.py
uv run python scripts/smoke_relay.py
uv run python scripts/smoke_relay_offline.py
uv run python scripts/smoke_mesh.py
uv run python scripts/pack_check.py
```
