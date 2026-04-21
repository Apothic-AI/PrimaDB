#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from primadb import Primadb, RelayServer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--room", default="package-full-node")
    parser.add_argument("--relay-bind", default="127.0.0.1:9010")
    parser.add_argument("--relay-url", default=None)
    parser.add_argument("--ice-server", action="append", default=[])
    parser.add_argument("--name", default=f"py-full-{int(time.time())}")
    parser.add_argument("--title", default=None)
    parser.add_argument("--message", default="")
    parser.add_argument("--duration-ms", type=int, default=None)
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
            continue
        if not trimmed.startswith(("stun:", "turn:", "turns:")):
            raise ValueError(
                f"invalid --ice-server value `{trimmed}`; use a STUN/TURN URL or JSON object"
            )
        servers.append({"urls": trimmed})
    return servers


def local_relay_url(relay_bind: str) -> str:
    host, port = relay_bind.rsplit(":", 1)
    if host == "0.0.0.0":
        host = "127.0.0.1"
    return f"ws://{host}:{port}"


def main() -> None:
    args = parse_args()
    here = Path(__file__).resolve().parent
    storage_root = here / ".data" / "full_node" / args.name
    storage_root.mkdir(parents=True, exist_ok=True)

    relay = RelayServer.listen({"bind": args.relay_bind})
    relay_url = args.relay_url or relay.url()
    try:
        db = Primadb(f"python-full-{args.name}")
        db.open_durable_storage(
            {
                "kind": "segment_files",
                "directory": str(storage_root),
                "journalRetention": 8,
            }
        )

        mesh = db.connect_mesh(
            {
                "room": args.room,
                "relayUrl": relay_url,
                "retryIntervalMs": 750,
                "iceServers": parse_ice_server_specs(args.ice_server),
            }
        )
        notes = db.chain("full_nodes").field(args.room).field("notes")

        if args.message:
            notes.set(
                {
                    "author": args.name,
                    "title": args.title or f"{args.name} {time.strftime('%H:%M:%S')}",
                    "body": args.message,
                    "role": "full-node",
                    "updated_at": int(time.time() * 1000),
                }
            )
            mesh.flush_pending()

        previous = ""
        deadline = None
        if args.duration_ms is not None and args.duration_ms > 0:
            deadline = time.time() + (args.duration_ms / 1000)

        while deadline is None or time.time() < deadline:
            payload = {
                "role": "full-node",
                "name": args.name,
                "room": args.room,
                "relayBind": relay.bind_addr(),
                "relayUrl": relay_url,
                "relayClients": relay.client_count(),
                "relayPeers": relay.peer_count(),
                "peerId": mesh.peer_id(),
                "signaling": mesh.signaling_mode(),
                "relayConnected": mesh.relay_connected(),
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
    except KeyboardInterrupt:
        pass
    finally:
        relay.close()


if __name__ == "__main__":
    main()
