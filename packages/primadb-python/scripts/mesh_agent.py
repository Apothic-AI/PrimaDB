#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time

from primadb import Primadb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--action", default="live", choices=["live", "verify-stored"])
    parser.add_argument("--relay", default="ws://127.0.0.1:9010")
    parser.add_argument("--ice-server", action="append", default=[])
    parser.add_argument("--room", default="primadb-mesh-agent")
    parser.add_argument("--replica", default=f"python-agent-{os.getpid()}")
    parser.add_argument("--storage-dir")
    parser.add_argument("--write-title")
    parser.add_argument("--write-body", default="python mesh agent")
    parser.add_argument("--expect-titles", default="")
    parser.add_argument("--min-peers", type=int, default=1)
    parser.add_argument("--timeout-ms", type=int, default=60_000)
    parser.add_argument("--hold-ms", type=int, default=2_000)
    parser.add_argument("--write-delay-ms", type=int, default=0)
    return parser.parse_args()


def parse_ice_server_specs(specs: list[str]) -> list[dict[str, object]]:
    servers: list[dict[str, object]] = []
    for spec in specs:
        trimmed = spec.strip()
        if trimmed.startswith("{"):
            value = json.loads(trimmed)
            if not isinstance(value, dict):
                raise ValueError("--ice-server JSON must decode to an object")
            servers.append(value)
        else:
            if not trimmed.startswith(("stun:", "turn:", "turns:")):
                raise ValueError(
                    f"invalid --ice-server value `{trimmed}`; use a STUN/TURN URL or JSON object"
                )
            servers.append({"urls": trimmed})
    return servers


def test_ice_servers() -> list[dict[str, object]]:
    return [{"urls": "stun:stun.cloudflare.com:3478"}]


def wait(ms: int) -> None:
    time.sleep(ms / 1000)


def collect_titles(db: Primadb, room: str) -> list[str]:
    entries = db.chain("boards").field(room).field("notes").query(
        {
            "order": {"path": "updated_at", "direction": "asc"},
            "limit": 1000,
        }
    )
    return [entry["value"]["title"] for entry in entries if entry.get("value", {}).get("title")]


def wait_for(predicate, timeout_ms: int, message: str):
    deadline = time.time() + (timeout_ms / 1000)
    last_value = None
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        last_value = value
        wait(100)
    raise RuntimeError(f"{message}. Last value: {last_value!r}")


def main() -> None:
    options = parse_args()
    expected_titles = [title for title in options.expect_titles.split(",") if title]
    db = Primadb(options.replica)
    storage = None

    if options.storage_dir:
      os.makedirs(options.storage_dir, exist_ok=True)
      storage = db.open_durable_storage(
          {
              "kind": "segment_files",
              "directory": options.storage_dir,
          }
      )

    if options.action == "verify-stored":
        titles = collect_titles(db, options.room)
        missing = [title for title in expected_titles if title not in titles]
        if missing:
            raise RuntimeError(f"Stored data missing titles: {', '.join(missing)}")
        print(
            json.dumps(
                {
                    "action": options.action,
                    "replica": options.replica,
                    "storage": storage,
                    "storedTitles": titles,
                    "python_package_storage_confirmed": True,
                },
                indent=2,
            )
        )
        return

    mesh = db.connect_mesh(
        {
            "room": options.room,
            "relayUrl": options.relay,
            "retryIntervalMs": 500,
            "iceServers": (
                parse_ice_server_specs(options.ice_server)
                if options.ice_server
                else test_ice_servers()
            ),
        }
    )

    try:
        wait_for(
            lambda: mesh.open_peer_count() >= options.min_peers,
            options.timeout_ms,
            f"Timed out waiting for {options.min_peers} open mesh peers",
        )

        if options.write_title:
            if options.write_delay_ms > 0:
                wait(options.write_delay_ms)
            now = int(time.time() * 1000)
            db.chain("boards").field(options.room).field("notes").set(
                {
                    "title": options.write_title,
                    "body": options.write_body,
                    "done": False,
                    "archived": False,
                    "created_at": now,
                    "updated_at": now,
                }
            )
            mesh.flush_pending()

        titles = wait_for(
            lambda: (
                current := collect_titles(db, options.room),
                current if all(title in current for title in expected_titles) else None,
            )[1],
            options.timeout_ms,
            "Timed out waiting for expected mesh titles",
        )

        print(
            json.dumps(
                {
                    "action": options.action,
                    "replica": options.replica,
                    "storage": storage,
                    "relay": options.relay,
                    "room": options.room,
                    "peerId": mesh.peer_id(),
                    "signaling": mesh.signaling_mode(),
                    "relayConnected": mesh.relay_connected(),
                    "openPeerCount": mesh.open_peer_count(),
                    "titles": titles,
                    "python_package_mesh_agent_confirmed": True,
                },
                indent=2,
            )
        )
        if options.hold_ms > 0:
            wait(options.hold_ms)
    finally:
        mesh.close()


if __name__ == "__main__":
    main()
