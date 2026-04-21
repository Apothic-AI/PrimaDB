#!/usr/bin/env python3
from __future__ import annotations

import json
import time

from primadb import Primadb, RelayServer

RELAY_ADDR = "127.0.0.1:9010"
RELAY_URL = f"ws://{RELAY_ADDR}"
ROOM = f"python-mesh-{int(time.time() * 1000)}"
TITLE = f"Python mesh smoke {int(time.time() * 1000)}"
ICE_SERVERS = [{"urls": "stun:stun.cloudflare.com:3478"}]


def wait(ms: int) -> None:
    time.sleep(ms / 1000)

def main() -> None:
    relay = RelayServer.listen({"bind": RELAY_ADDR})
    try:
        waiter_db = Primadb("python-mesh-waiter")
        writer_db = Primadb("python-mesh-writer")

        waiter = waiter_db.connect_mesh({"room": ROOM, "relayUrl": RELAY_URL, "iceServers": ICE_SERVERS})
        writer = writer_db.connect_mesh({"room": ROOM, "relayUrl": RELAY_URL, "iceServers": ICE_SERVERS})

        deadline = time.time() + 45
        while time.time() < deadline:
            if waiter.open_peer_count() >= 1 and writer.open_peer_count() >= 1:
                break
            wait(100)
        else:
            raise RuntimeError("Timed out waiting for open mesh peers")

        watch = waiter.watch_remote_query(
            writer.peer_id(),
            {"anchor": "boards", "segments": [ROOM, "notes"]},
            {"filters": [{"kind": "eq", "path": "title", "value": TITLE}], "limit": 1},
        )
        initial_watch = watch.next()

        notes = writer_db.chain("boards").field(ROOM).field("notes")
        note_id = notes.set(
            {
                "title": TITLE,
                "body": "python/python mesh smoke",
                "done": False,
                "archived": False,
            }
        )
        writer.flush_pending()

        deadline = time.time() + 30
        watch_update = None
        while time.time() < deadline:
            candidate = watch.try_next()
            if candidate.get("value"):
                watch_update = candidate
                break
            wait(100)
        else:
            raise RuntimeError(f"Timed out waiting for mesh watch {TITLE}")

        deadline = time.time() + 45
        matches = []
        while time.time() < deadline:
            matches = waiter_db.chain("boards").field(ROOM).field("notes").query(
                {
                    "filters": [{"kind": "eq", "path": "title", "value": TITLE}],
                    "limit": 1,
                }
            )
            if matches:
                break
            wait(100)
        else:
            raise RuntimeError(f"Timed out waiting for mesh note {TITLE}")

        print(
            json.dumps(
                {
                    "relay": RELAY_URL,
                    "room": ROOM,
                    "title": TITLE,
                    "noteId": note_id,
                    "initialWatch": initial_watch,
                    "watchUpdate": watch_update,
                    "waiter": {
                        "peerId": waiter.peer_id(),
                        "signaling": waiter.signaling_mode(),
                        "relayConnected": waiter.relay_connected(),
                        "openPeerCount": waiter.open_peer_count(),
                    },
                    "writer": {
                        "peerId": writer.peer_id(),
                        "signaling": writer.signaling_mode(),
                        "relayConnected": writer.relay_connected(),
                        "openPeerCount": writer.open_peer_count(),
                    },
                    "matches": matches,
                    "python_package_mesh_confirmed": True,
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
