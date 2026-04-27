#!/usr/bin/env python3
from __future__ import annotations

import json
import time

from primadb import Primadb, create_primadb_moq_loopback


room = f"python-moq-{int(time.time() * 1000)}"
path = f"primadb/examples/moq/{room}"
publisher_db = Primadb(f"python-moq-pub-{room}")
subscriber_db = Primadb(f"python-moq-sub-{room}")

publisher_notes = (
    publisher_db.chain("package_examples").field("python_moq").field(room).field("notes")
)
subscriber_notes = (
    subscriber_db.chain("package_examples").field("python_moq").field(room).field("notes")
)

link = create_primadb_moq_loopback(
    publisher_db=publisher_db,
    subscriber_db=subscriber_db,
    path=path,
)

publisher_notes.set(
    {
        "title": "MoQ Python sync",
        "body": "This record moved through a MoQ-shaped track.",
        "updated_at": int(time.time() * 1000),
    }
)
sent = link.flush()
entries = subscriber_notes.query(
    {
        "order": {"path": "updated_at", "direction": "desc"},
        "limit": 5,
    }
)
payload = {
    "path": path,
    "track": "ops",
    "sentTracks": sent,
    "replicated": len(entries) > 0,
    "subscriberEntries": entries,
}
print(json.dumps(payload, indent=2))
link.close()

if not payload["replicated"]:
    raise SystemExit(1)
