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
RELAY_ADDR = os.environ.get("PRIMADB_PYTHON_HOOK_RELAY_ADDR", "127.0.0.1:9025")
RELAY_URL = os.environ.get("PRIMADB_PYTHON_HOOK_RELAY_URL", f"ws://{RELAY_ADDR}")
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


def wait_for(predicate, timeout_ms: int = 20_000):
    deadline = time.time() + timeout_ms / 1000
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        wait(200)
    raise RuntimeError("Timed out waiting for hook smoke condition")


class HookPolicy:
    def on_pull(self, context):
        request = context["request"]
        if request["kind"] == "get" and request["path"]["anchor"] == "private":
            return "private root denied"
        return None

    def on_serve_result(self, _context, result):
        if result["kind"] == "get":
            return {"kind": "get", "value": {"masked": True}}
        return None


def main() -> None:
    relay_proc, started = ensure_relay()
    try:
        server_db = Primadb("python-hook-server")
        client_db = Primadb("python-hook-client")

        server_db.set_network_hooks(HookPolicy())

        server = server_db.connect_relay({"url": RELAY_URL, "retryIntervalMs": 500})
        client = client_db.connect_relay({"url": RELAY_URL, "retryIntervalMs": 500})

        target_peer = wait_for(
            lambda: next(
                (
                    entry["peer"]["peer_id"]
                    for entry in client.recommended_peers()
                    if entry.get("peer", {}).get("replica_id") == server_db.replica_id()
                ),
                None,
            )
        )

        server_db.chain("docs").field("profile").put({"title": "Hooked profile", "visible": True})
        server_db.chain("private").field("secret").put({"title": "Forbidden profile", "visible": False})
        server.flush_pending()

        masked = wait_for(
            lambda: (
                value
                if (value := client.remote_get(target_peer, {"anchor": "docs", "segments": ["profile"]})).get("masked") is True
                else None
            )
        )

        denied = None
        try:
            client.remote_get(target_peer, {"anchor": "private", "segments": ["secret"]})
        except Exception as error:  # noqa: BLE001
            denied = str(error)
        if denied is None or "private root denied" not in denied:
            raise RuntimeError(f"Expected denied pull from network hook, got: {denied}")

        server_db.clear_network_hooks()

        unmasked = wait_for(
            lambda: (
                value
                if (value := client.remote_get(target_peer, {"anchor": "docs", "segments": ["profile"]})).get("title")
                == "Hooked profile"
                else None
            )
        )

        print(
            json.dumps(
                {
                    "relay": RELAY_URL,
                    "targetPeer": target_peer,
                    "masked": masked,
                    "denied": denied,
                    "unmasked": unmasked,
                    "python_package_hooks_confirmed": True,
                },
                indent=2,
            )
        )

        server.close()
        client.close()
    finally:
        if started:
            close_detached(relay_proc)


if __name__ == "__main__":
    main()
