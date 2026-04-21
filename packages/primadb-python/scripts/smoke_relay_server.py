from __future__ import annotations

import json
import time

from primadb import Primadb, RelayServer


def wait_for(condition, timeout_ms: int, description: str) -> None:
    deadline = time.time() + (timeout_ms / 1000)
    while time.time() < deadline:
        if condition():
            return
        time.sleep(0.1)
    raise RuntimeError(f"timed out waiting for {description}")


def main() -> None:
    relay = RelayServer.listen({"bind": "127.0.0.1:9032"})
    left_db = Primadb("python-relay-server-left")
    right_db = Primadb("python-relay-server-right")

    left = left_db.connect_relay({"url": relay.url(), "retryIntervalMs": 200})
    right = right_db.connect_relay({"url": relay.url(), "retryIntervalMs": 200})

    wait_for(lambda: left.is_connected() and right.is_connected(), 10_000, "both relay clients to connect")
    wait_for(lambda: relay.client_count() >= 2, 10_000, "relay client count to reach 2")

    print(
        json.dumps(
            {
                "bindAddr": relay.bind_addr(),
                "url": relay.url(),
                "clientCount": relay.client_count(),
                "peerCount": relay.peer_count(),
                "relayServerApiConfirmed": True,
            },
            indent=2,
            sort_keys=True,
        )
    )

    left.close()
    right.close()
    relay.close()


if __name__ == "__main__":
    main()
