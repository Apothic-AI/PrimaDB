#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shutil
import tempfile
import time

from primadb import Primadb


def main() -> None:
    root = tempfile.mkdtemp(prefix="primadb-python-core-")
    try:
        db = Primadb("python-core-a")
        binding = db.open_durable_storage(
            {
                "kind": "segment_files",
                "directory": root,
            }
        )

        notes = db.chain("notes").field("items")
        subscription = notes.subscribe()

        note_id = notes.set(
            {
                "title": "Python package note",
                "body": "stored through the native Python package",
                "done": False,
            }
        )

        event = None
        deadline = time.time() + 10
        while time.time() < deadline:
            candidate = subscription.try_next()
            if candidate["value"] is not None:
                event = candidate
                break
            time.sleep(0.05)

        restored = Primadb("python-core-b")
        restored_binding = restored.open_durable_storage(
            {
                "kind": "segment_files",
                "directory": root,
            }
        )

        results = restored.chain("notes").field("items").query(
            {
                "filters": [{"kind": "eq", "path": "title", "value": "Python package note"}],
                "limit": 10,
            }
        )

        print(
            json.dumps(
                {
                    "binding": binding,
                    "restoredBinding": restored_binding,
                    "noteId": note_id,
                    "subscriptionEvent": event,
                    "restoredCount": len(results),
                    "python_package_core_confirmed": len(results) == 1,
                },
                indent=2,
            )
        )

        subscription.close()
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
