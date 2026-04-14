#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

from primadb import Primadb


ROOT = Path(__file__).resolve().parent / ".data"
SEGMENTS = ROOT / "segments"
BLOBS = ROOT / "blobs"
SEGMENTS.mkdir(parents=True, exist_ok=True)
BLOBS.mkdir(parents=True, exist_ok=True)

db = Primadb("python-example-local")
durable = db.open_durable_storage(
    {
        "kind": "segment_files",
        "directory": str(SEGMENTS),
    }
)
blob_binding = db.open_blob_storage(
    {
        "kind": "files",
        "directory": str(BLOBS),
    }
)

notes = db.chain("package_examples").field("python_local").field("notes")
binary = db.chain("package_examples").field("python_local").field("avatar_bytes")
blob_chain = db.chain("package_examples").field("python_local").field("archive_blob")

notes.set(
    {
        "title": "Python package example",
        "body": "Stored through the native Python example",
        "updated_at": 1_775_700_000_000,
    }
)
payload = bytes([2, 4, 6, 8, 10, 12])
binary.put_bytes(payload)
blob_ref = blob_chain.put_blob(payload, "application/octet-stream")

entries = notes.query(
    {
        "order": {"path": "updated_at", "direction": "desc"},
        "limit": 5,
    }
)

print(
    json.dumps(
        {
            "durable": durable,
            "blobBinding": blob_binding,
            "entryCount": len(entries),
            "latest": entries[0] if entries else None,
            "bytes": list(binary.once_bytes() or b""),
            "blobRef": blob_ref,
            "blob": list(blob_chain.get_blob() or b""),
        },
        indent=2,
    )
)
