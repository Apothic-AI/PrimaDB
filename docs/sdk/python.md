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
from primadb import Primadb

db = Primadb("py-a")
db.open_durable_storage(
    {
        "kind": "segment_files",
        "directory": "/tmp/primadb-python-demo",
    }
)

db.set_network_hooks(
    {
        "on_pull": lambda context: "private root denied"
        if context["request"]["kind"] == "get"
        and context["request"]["path"]["anchor"] == "private"
        else None
    }
)
```

## Package Examples

- [local_notes](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb-python/examples/local_notes)
- [mesh_peer](https://github.com/Apothic-AI/PrimaDB/tree/master/packages/primadb-python/examples/mesh_peer)
