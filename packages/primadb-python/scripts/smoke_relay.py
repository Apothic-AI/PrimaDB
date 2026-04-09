#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time

from primadb import Primadb


ROOT = os.environ.get("PRIMADB_ROOT", "/home/bitnom/Code/gunport/primadb")
RELAY_ADDR = os.environ.get("PRIMADB_PYTHON_RELAY_ADDR", "127.0.0.1:9010")
RELAY_URL = os.environ.get("PRIMADB_PYTHON_RELAY_URL", f"ws://{RELAY_ADDR}")
TITLE = os.environ.get("PRIMADB_PYTHON_RELAY_TITLE", f"Python relay smoke {int(time.time() * 1000)}")
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

        waiter.close()
        writer.close()
    finally:
        if started:
            close_detached(relay_proc)


if __name__ == "__main__":
    main()
