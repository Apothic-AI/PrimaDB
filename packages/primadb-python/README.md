# `primadb-python`

`primadb-python` is a native Python package for Primadb. Like
[packages/primadb-node](/home/bitnom/Code/gunport/primadb/packages/primadb-node), it wraps the
native Rust runtime directly instead of going through the browser WASM layer.

Current surface:

- `Primadb` and `Chain` for local graph operations
- durable native storage through `open_durable_storage(...)`
- subscriptions
- native relay sync through `connect_relay(...)`
- native WebRTC mesh through `connect_mesh(...)`

## Install

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb-python
python3 -m pip install -e .
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

db.chain("notes").field("items").set(
    {
        "title": "Python note",
        "body": "Stored through the native extension",
    }
)
```

## Smoke Tests

```bash
python3 scripts/smoke_core.py
python3 scripts/smoke_relay.py
python3 scripts/smoke_mesh.py
python3 scripts/pack_check.py
```
