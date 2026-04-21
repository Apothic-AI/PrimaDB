#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import time

from primadb import Primadb, RelayServer

RELAY_ADDR = "127.0.0.1:9010"
RELAY_URL = f"ws://{RELAY_ADDR}"
TITLE = os.environ.get("PRIMADB_PYTHON_RELAY_TITLE", f"Python relay smoke {int(time.time() * 1000)}")


def wait(ms: int) -> None:
    time.sleep(ms / 1000)

def main() -> None:
    relay = RelayServer.listen({"bind": RELAY_ADDR})
    try:
        waiter_db = Primadb("python-relay-waiter")
        writer_db = Primadb("python-relay-writer")

        waiter = waiter_db.connect_relay({"url": RELAY_URL, "retryIntervalMs": 1500})
        writer = writer_db.connect_relay({"url": RELAY_URL, "retryIntervalMs": 1500})

        deadline = time.time() + 20
        while time.time() < deadline:
            if waiter.is_connected() and writer.is_connected():
                break
            wait(100)
        else:
            raise RuntimeError("Timed out waiting for relay connections")

        deadline = time.time() + 20
        target_peer = None
        while time.time() < deadline:
            peers = waiter.recommended_peers()
            target_peer = next(
                (
                    entry["peer"]["peer_id"]
                    for entry in peers
                    if entry.get("peer", {}).get("replica_id") == writer_db.replica_id()
                ),
                None,
            )
            if target_peer:
                break
            wait(100)
        else:
            raise RuntimeError("Timed out waiting for relay peer discovery")

        watch = waiter.watch_remote_query(
            target_peer,
            {"anchor": "boards", "segments": ["shared", "notes"]},
            {"filters": [{"kind": "eq", "path": "title", "value": TITLE}], "limit": 1},
        )
        initial_watch = watch.next()

        notes = writer_db.chain("boards").field("shared").field("notes")
        note_id = notes.set(
            {
                "title": TITLE,
                "body": "python/python relay smoke",
                "done": False,
                "archived": False,
            }
        )
        writer.flush_pending()

        deadline = time.time() + 20
        watch_update = None
        while time.time() < deadline:
            candidate = watch.try_next()
            if candidate.get("value"):
                watch_update = candidate
                break
            wait(100)
        else:
            raise RuntimeError(f"Timed out waiting for watch update {TITLE}")

        deadline = time.time() + 30
        matches = []
        while time.time() < deadline:
            matches = waiter_db.chain("boards").field("shared").field("notes").query(
                {
                    "filters": [{"kind": "eq", "path": "title", "value": TITLE}],
                    "limit": 1,
                }
            )
            if matches:
                break
            wait(150)
        else:
            raise RuntimeError(f"Timed out waiting for remote note {TITLE}")

        print(
            json.dumps(
                {
                    "relay": RELAY_URL,
                    "title": TITLE,
                    "noteId": note_id,
                    "targetPeer": target_peer,
                    "initialWatch": initial_watch,
                    "watchUpdate": watch_update,
                    "knownPeers": {
                        "waiter": waiter.known_peer_count(),
                        "writer": writer.known_peer_count(),
                    },
                    "matches": matches,
                    "python_package_relay_confirmed": True,
                },
                indent=2,
            )
        )

        watch.close()
        waiter.close()
        writer.close()
    finally:
        relay.close()


if __name__ == "__main__":
    main()
