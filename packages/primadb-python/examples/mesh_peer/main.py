#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from primadb import Primadb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--room", default="package-mesh")
    parser.add_argument("--relay", default="ws://127.0.0.1:9010")
    parser.add_argument("--ice-server", action="append", default=[])
    parser.add_argument("--name", default=f"py-{int(time.time())}")
    parser.add_argument("--message", default="")
    parser.add_argument("--duration-ms", type=int, default=15_000)
    return parser.parse_args()


def parse_ice_server_specs(specs: list[str]) -> list[dict[str, object]]:
    if not specs:
        return [{"urls": "stun:stun.cloudflare.com:3478"}]
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


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parent / ".data" / args.name
    root.mkdir(parents=True, exist_ok=True)

    db = Primadb(f"python-mesh-{args.name}")
    db.open_durable_storage(
        {
            "kind": "segment_files",
            "directory": str(root),
        }
    )

    mesh = db.connect_mesh(
        {
            "room": args.room,
            "relayUrl": args.relay,
            "retryIntervalMs": 750,
            "iceServers": parse_ice_server_specs(args.ice_server),
        }
    )
    notes = db.chain("package_examples").field("mesh").field(args.room).field("notes")

    if args.message:
        notes.set(
            {
                "author": args.name,
                "title": f"{args.name} {time.strftime('%H:%M:%S')}",
                "body": args.message,
                "updated_at": int(time.time() * 1000),
            }
        )
        mesh.flush_pending()

    previous = ""
    last_relay_connected: bool | None = None
    deadline = time.time() + (args.duration_ms / 1000)
    while time.time() < deadline:
        relay_connected = mesh.relay_connected()
        if relay_connected != last_relay_connected:
            if relay_connected:
                print(
                    f"relay {mesh.relay_url()} connected; mesh signaling is active",
                    file=sys.stderr,
                )
            else:
                print(
                    f"relay {mesh.relay_url()} unavailable; continuing offline and retrying in background",
                    file=sys.stderr,
                )
            last_relay_connected = relay_connected
        payload = {
            "peerId": mesh.peer_id(),
            "signaling": mesh.signaling_mode(),
            "relayUrl": mesh.relay_url(),
            "relayConnected": relay_connected,
            "openPeers": mesh.open_peer_count(),
            "peers": mesh.peer_count(),
            "inflight": mesh.inflight_count(),
            "notes": notes.query(
                {
                    "order": {"path": "updated_at", "direction": "desc"},
                    "limit": 5,
                }
            ),
        }
        encoded = json.dumps(payload, indent=2, sort_keys=True)
        if encoded != previous:
            previous = encoded
            print(encoded)
        time.sleep(1)

    mesh.close()


if __name__ == "__main__":
    main()
