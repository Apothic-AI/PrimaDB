#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import time

from primadb import Primadb


ROOT = os.environ.get("PRIMADB_ROOT", "/home/bitnom/Code/gunport/primadb")
RELAY_ADDR = os.environ.get("PRIMADB_PYTHON_MESH_RELAY_ADDR", "127.0.0.1:9010")
RELAY_URL = os.environ.get("PRIMADB_PYTHON_MESH_RELAY_URL", f"ws://{RELAY_ADDR}")
ROOM = os.environ.get("PRIMADB_PYTHON_MESH_ROOM", f"python-mesh-{int(time.time() * 1000)}")
TITLE = os.environ.get("PRIMADB_PYTHON_MESH_TITLE", f"Python mesh smoke {int(time.time() * 1000)}")
PORT = int(RELAY_ADDR.rsplit(":", 1)[-1])


def wait(ms: int) -> None:
    time.sleep(ms / 1000)


def is_port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.5)
        try:
            sock.connect(("127.0.0.1", port))
        except OSError:
            return False
        return True


def ensure_relay() -> tuple[subprocess.Popen[bytes] | None, bool]:
    if is_port_open(PORT):
        return None, False

    child = subprocess.Popen(
        ["cargo", "run", "--example", "ws_relay_server", "--", RELAY_ADDR],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )

    deadline = time.time() + 30
    while time.time() < deadline:
        if is_port_open(PORT):
            return child, True
        wait(200)

    raise RuntimeError(f"Timed out waiting for relay on {RELAY_ADDR}")


def close_detached(child: subprocess.Popen[bytes] | None) -> None:
    if child is None:
        return
    try:
        os.killpg(child.pid, signal.SIGTERM)
    except OSError:
        pass


def main() -> None:
    relay_proc, started = ensure_relay()
    try:
        waiter_db = Primadb("python-mesh-waiter")
        writer_db = Primadb("python-mesh-writer")

        waiter = waiter_db.connect_mesh({"room": ROOM, "relayUrl": RELAY_URL})
        writer = writer_db.connect_mesh({"room": ROOM, "relayUrl": RELAY_URL})

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
        if started:
            close_detached(relay_proc)


if __name__ == "__main__":
    main()
