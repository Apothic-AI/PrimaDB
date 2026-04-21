#!/usr/bin/env python3
from __future__ import annotations

import json
import time

from primadb import Primadb, RelayServer

RELAY_ADDR = "127.0.0.1:9031"
RELAY_URL = f"ws://{RELAY_ADDR}"
TITLE = f"Python relay offline retry {int(time.time() * 1000)}"


def wait(ms: int) -> None:
    time.sleep(ms / 1000)

def wait_for(predicate, timeout_ms: int, message: str):
    deadline = time.time() + (timeout_ms / 1000)
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        wait(100)
    raise RuntimeError(message)


def main() -> None:
    relay = None
    writer_db = Primadb("python-relay-offline-writer")
    waiter_db = Primadb("python-relay-offline-waiter")

    writer = writer_db.connect_relay({"url": RELAY_URL, "retryIntervalMs": 500})
    if writer.is_connected():
        raise RuntimeError("writer relay unexpectedly connected before relay startup")

    notes = writer_db.chain("boards").field("offline-retry").field("notes")
    offline_note = notes.set(
        {
            "title": "offline local note",
            "body": "created before relay startup",
            "done": False,
            "archived": False,
        }
    )
    writer.flush_pending()
    local_matches = notes.query(
        {"filters": [{"kind": "eq", "path": "title", "value": "offline local note"}], "limit": 1}
    )
    if not local_matches:
        raise RuntimeError("local offline note was not readable before relay startup")

    try:
        relay = RelayServer.listen({"bind": RELAY_ADDR})

        wait_for(
            writer.is_connected,
            20_000,
            "writer did not reconnect after relay startup",
        )

        waiter = waiter_db.connect_relay({"url": RELAY_URL, "retryIntervalMs": 500})
        wait_for(
            lambda: waiter.is_connected() and writer.is_connected(),
            20_000,
            "writer/waiter did not connect after relay startup",
        )

        target_peer = wait_for(
            lambda: next(
                (
                    entry["peer"]["peer_id"]
                    for entry in waiter.recommended_peers()
                    if entry.get("peer", {}).get("replica_id") == writer_db.replica_id()
                ),
                None,
            ),
            20_000,
            "timed out waiting for relay peer discovery after reconnect",
        )

        watch = waiter.watch_remote_query(
            target_peer,
            {"anchor": "boards", "segments": ["offline-retry", "notes"]},
            {"filters": [{"kind": "eq", "path": "title", "value": TITLE}], "limit": 1},
        )
        initial_watch = watch.next()

        live_note = notes.set(
            {
                "title": TITLE,
                "body": "created after relay reconnect",
                "done": False,
                "archived": False,
            }
        )
        writer.flush_pending()

        watch_update = wait_for(
            lambda: next(
                (
                    candidate
                    for candidate in [watch.try_next()]
                    if candidate.get("value")
                ),
                None,
            ),
            20_000,
            "timed out waiting for live relay update after reconnect",
        )

        print(
            json.dumps(
                {
                    "relay": RELAY_URL,
                    "offlineNote": offline_note,
                    "liveNote": live_note,
                    "targetPeer": target_peer,
                    "initialWatch": initial_watch,
                    "watchUpdate": watch_update,
                    "python_relay_offline_retry_confirmed": True,
                },
                indent=2,
            )
        )

        watch.close()
        waiter.close()
        writer.close()
    finally:
        if relay is not None:
            relay.close()


if __name__ == "__main__":
    main()
