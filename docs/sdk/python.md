---
title: Python Package
sidebar_position: 3
---

`primadb-python` is the native Python package. It wraps the Rust runtime through PyO3 and is now
documented around a `uv`-first workflow.

Source:

- [packages/primadb-python](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb-python)

Full API reference:

- [Python package API](../api/python-package)

## Install

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb-python
uv sync
```

## Example

```python
from primadb import Primadb, derive_password_key

db = Primadb("py-a")
key = derive_password_key("correct horse battery staple")
db.set_snapshot_encryption_key(key["keyBase64"])

db.open_durable_storage(
    {
        "kind": "segment_files",
        "directory": "/tmp/primadb-python-demo",
        "durability": "full",
        "lockMode": {"kind": "exclusive"},
    }
)

db.put_record("agentfs/inodes/1", {"mode": "file", "size": 4})
db.put_record_bytes("agentfs/chunks/1/000000", b"\x01\x02\x03\x04")
print(db.scan_records({"prefix": "agentfs/chunks/1/", "limit": 100})["entries"])
db.apply_record_batch(
    {
        "preconditions": [{"kind": "exists", "key": "agentfs/inodes/1"}],
        "mutations": [],
    }
)
db.sync_storage()

db.set_network_hooks(
    {
        "on_pull": lambda context: "private root denied"
        if context["request"]["kind"] == "get"
        and context["request"]["path"]["anchor"] == "private"
        else None
    }
)

db.close_durable_storage()
```

## Transactions And Strict Scopes

```python
ledger = db.scope("ledger")
ledger.configure(
    {
        "consistency": "coordinated",
        "authority": {"kind": "full_node", "peerId": "native:py-a"},
        "offlineWrites": "reject",
    }
)

report = ledger.transaction(
    [
        {
            "kind": "increment",
            "path": {"anchor": "alice", "segments": ["balance"]},
            "by": 10,
        }
    ]
)
print(report["status"])
```

When a different peer is the authority, submit over a relay sync client:

```python
sync = db.connect_relay({"url": "ws://127.0.0.1:9010"})
sync.remote_transaction(
    "native:ledger",
    "ledger",
    [
        {
            "kind": "increment",
            "path": {"anchor": "alice", "segments": ["balance"]},
            "by": 10,
        }
    ],
)
```

## Guides

- [Auth, encryption, and password keys](../guides/auth-encryption)
- [Relay, full node, and mesh](../guides/relay-full-node-and-mesh)
- [Transactions and strict scopes](../guides/transactions-and-strict-scopes)
- [Binary data, media, and MoQ](../guides/binary-media-and-moq)
- [Node-attached scripting](../guides/scripting)

## Package Examples

- [local_notes](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb-python/examples/local_notes)
- [mesh_peer](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb-python/examples/mesh_peer)
- [full_node](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb-python/examples/full_node)
- [moq_sync](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb-python/examples/moq_sync)
