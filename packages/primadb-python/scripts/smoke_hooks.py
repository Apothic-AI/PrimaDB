#!/usr/bin/env python3
from __future__ import annotations

import json
import time

from primadb import Primadb, RelayServer, generate_identity

RELAY_ADDR = "127.0.0.1:9025"
RELAY_URL = f"ws://{RELAY_ADDR}"


def wait(ms: int) -> None:
    time.sleep(ms / 1000)

def wait_for(predicate, timeout_ms: int = 20_000):
    deadline = time.time() + timeout_ms / 1000
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        wait(200)
    raise RuntimeError("Timed out waiting for hook smoke condition")


class HookPolicy:
    def __init__(self):
        self.verified_alias = None

    def on_pull(self, context):
        identity = context.get("verifiedIdentity") or {}
        self.verified_alias = identity.get("alias")
        if self.verified_alias != "client":
            return "verified client identity required"
        request = context["request"]
        if request["kind"] == "get" and request["path"]["anchor"] == "private":
            return "private root denied"
        return None

    def on_serve_result(self, _context, result):
        if result["kind"] == "get":
            return {"kind": "get", "value": {"masked": True}}
        return None


def main() -> None:
    relay = RelayServer.listen({"bind": RELAY_ADDR})
    try:
        server_db = Primadb("python-hook-server")
        client_db = Primadb("python-hook-client")
        grants = [{"root": "*", "read": True, "write": True}]
        server_identity = generate_identity()
        client_identity = generate_identity()
        server_db.register_user("server", server_identity["publicKey"], grants)
        server_db.register_user("client", client_identity["publicKey"], grants)
        server_db.authenticate_local_user("server", server_identity["secretKey"], grants)
        client_db.register_user("server", server_identity["publicKey"], grants)
        client_db.register_user("client", client_identity["publicKey"], grants)
        client_db.authenticate_local_user("client", client_identity["secretKey"], grants)

        hook_policy = HookPolicy()
        server_db.set_network_hooks(hook_policy)

        server = server_db.connect_relay(
            {
                "url": RELAY_URL,
                "retryIntervalMs": 500,
                "sessionAuth": {
                    "requireAuthenticatedPeers": True,
                    "trustedAliases": ["client"],
                },
            }
        )
        client = client_db.connect_relay(
            {
                "url": RELAY_URL,
                "retryIntervalMs": 500,
                "sessionAuth": {
                    "trustedAliases": ["server"],
                },
            }
        )

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

        def read_masked_profile():
            try:
                value = client.remote_get(target_peer, {"anchor": "docs", "segments": ["profile"]})
            except Exception:  # noqa: BLE001
                return None
            return value if value.get("masked") is True else None

        masked = wait_for(read_masked_profile)

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
                    "verifiedAlias": hook_policy.verified_alias,
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
        relay.close()


if __name__ == "__main__":
    main()
