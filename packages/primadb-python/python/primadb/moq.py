from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional


DEFAULT_ROUTE_TRACK = "routes"
DEFAULT_CHANNEL = "primadb-sync"


def _has_pending_ops(envelope: Any) -> bool:
    return isinstance(envelope, dict) and isinstance(envelope.get("ops"), list) and len(envelope["ops"]) > 0


def _drain_pending_envelope_json(db: Any) -> str:
    if hasattr(db, "drain_pending_envelope_json"):
        return str(db.drain_pending_envelope_json())
    return json.dumps(db.drain_pending_envelope(), separators=(",", ":"))


def _apply_moq_payload(db: Any, message: dict[str, Any]) -> int:
    envelope_json = message.get("envelopeJson")
    if isinstance(envelope_json, str):
        if hasattr(db, "apply_operations_json"):
            return int(db.apply_operations_json(envelope_json))
        return int(db.apply_envelope(json.loads(envelope_json)))
    return int(db.apply_envelope(message["envelope"]))


def _apply_sync_frame_payload(db: Any, frame: dict[str, Any]) -> int:
    envelope_json = frame.get("envelopeJson")
    if isinstance(envelope_json, str):
        if hasattr(db, "apply_operations_json"):
            return int(db.apply_operations_json(envelope_json))
        return int(db.apply_envelope(json.loads(envelope_json)))
    if hasattr(db, "apply_operations_json"):
        return int(
            db.apply_operations_json(
                json.dumps(
                    {
                        "type": "sync",
                        "from": frame.get("from"),
                        "message_id": frame.get("message_id"),
                        "ops": frame.get("ops", []),
                    },
                    separators=(",", ":"),
                )
            )
        )
    return int(db.apply_envelope({"from": frame.get("from"), "ops": frame.get("ops", [])}))


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _content_hash(value: Any) -> str:
    hash_value = 0x811C9DC5
    for char in _stable_json(value):
        hash_value ^= ord(char)
        hash_value = (hash_value * 0x01000193) & 0xFFFFFFFF
    return f"fnv1a32:{hash_value:08x}"


def _now_millis() -> int:
    return int(time.time() * 1000)


def _is_route_envelope(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and isinstance(value.get("route_id"), str)
        and isinstance(value.get("from"), str)
        and isinstance(value.get("channel"), str)
        and isinstance(value.get("payload"), dict)
    )


def _route_targets_peer(route: dict[str, Any], peer_ids: set[str], channel: str) -> bool:
    target = route.get("target")
    if not isinstance(target, dict):
        return False
    kind = target.get("kind")
    if kind == "broadcast":
        return route.get("channel") == channel
    if kind == "topic":
        return target.get("value") == channel or route.get("channel") == target.get("value")
    if kind == "peer":
        return target.get("value") in peer_ids
    return False


def _is_application_message(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and isinstance(value.get("namespace"), str)
        and isinstance(value.get("protocol"), str)
        and "body" in value
    )


def _normalize_application_message(message: dict[str, Any]) -> dict[str, Any]:
    return {
        "namespace": message["namespace"],
        "protocol": message["protocol"],
        "topic": message.get("topic"),
        "body": message.get("body"),
        "metadata": message.get("metadata", {}),
    }


def _application_matches_filter(event: dict[str, Any], filter: dict[str, Any]) -> bool:
    message = event.get("message", {})
    return (
        (filter.get("namespace") is None or filter.get("namespace") == message.get("namespace"))
        and (filter.get("protocol") is None or filter.get("protocol") == message.get("protocol"))
        and (filter.get("topic") is None or filter.get("topic") == message.get("topic"))
    )


def _trim_queue(queue: list[Any], max_size: int = 1024) -> None:
    del queue[: max(0, len(queue) - max_size)]


@dataclass
class PrimadbMoqFrame:
    path: str
    track: str
    sequence: int
    payload: bytes

    def json(self) -> Any:
        return json.loads(self.payload.decode("utf-8"))


class PrimadbApplicationRouteSubscription:
    def __init__(self, filter: dict[str, Any], on_close: Callable[[], None]) -> None:
        self.filter = filter
        self._on_close = on_close
        self._queue: list[dict[str, Any]] = []
        self._closed = False

    def next(self) -> Optional[dict[str, Any]]:
        return self.try_next()

    def try_next(self) -> Optional[dict[str, Any]]:
        if not self._queue:
            return None
        return self._queue.pop(0)

    def drain(self) -> list[dict[str, Any]]:
        events = self._queue
        self._queue = []
        return events

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._queue = []
        self._on_close()

    def enqueue(self, event: dict[str, Any]) -> None:
        if self._closed or not _application_matches_filter(event, self.filter):
            return
        self._queue.append(event)
        _trim_queue(self._queue)


class PrimadbMoqSession:
    """PrimaDB MoQ track adapter.

    The Python package currently exposes a deterministic loopback adapter because the available
    Python MoQ bindings do not yet expose stable generic byte tracks on Python 3.14. The API mirrors
    the browser/Node helpers so Python examples can exercise the same path/track/object mapping.
    """

    def __init__(
        self,
        db: Any,
        *,
        path: str,
        track: str = DEFAULT_ROUTE_TRACK,
        channel: str = DEFAULT_CHANNEL,
        peer_id: Optional[str] = None,
    ) -> None:
        self.db = db
        self.path = path
        self.track = track
        self.channel = channel
        self.peer_id = peer_id or f"moq:{db.replica_id()}"
        self._sequence = 0
        self._route_sequence = 0
        self._subscribers: list[PrimadbMoqSession] = []
        self._route_handlers: list[Callable[[dict[str, Any]], None]] = []
        self._seen_routes: set[str] = set()
        self._known_peers: dict[str, dict[str, Any]] = {}
        self._recommendations: dict[str, dict[str, Any]] = {}
        self._accepted_peer_ids: set[str] = {self.peer_id}
        self._application_subscriptions: list[PrimadbApplicationRouteSubscription] = []
        self._application_events: list[dict[str, Any]] = []
        self._closed = False

    def subscribe_from(self, publisher: "PrimadbMoqSession") -> None:
        if self._closed:
            raise RuntimeError("MoQ session is closed")
        publisher._subscribers.append(self)

    def on_route(self, handler: Callable[[dict[str, Any]], None]) -> Callable[[], None]:
        self._route_handlers.append(handler)

        def unsubscribe() -> None:
            if handler in self._route_handlers:
                self._route_handlers.remove(handler)

        return unsubscribe

    def add_accepted_peer_id(self, peer_id: str) -> Callable[[], None]:
        self._accepted_peer_ids.add(peer_id)

        def unregister() -> None:
            if peer_id != self.peer_id:
                self._accepted_peer_ids.discard(peer_id)

        return unregister

    def known_peers(self) -> list[dict[str, Any]]:
        return list(self._known_peers.values())

    def recommended_peers(self) -> list[dict[str, Any]]:
        return list(self._recommendations.values())

    def publish_application(self, message: dict[str, Any], target: Optional[dict[str, Any]] = None) -> int:
        return self.send_route(
            self.create_route(
                {
                    "kind": "application",
                    "message": _normalize_application_message(message),
                },
                target,
            )
        )

    def send_application(
        self,
        namespace: str,
        protocol: str,
        topic: Optional[str],
        body: Any,
        metadata: Optional[dict[str, Any]] = None,
        target: Optional[dict[str, Any]] = None,
    ) -> int:
        return self.publish_application(
            {
                "namespace": namespace,
                "protocol": protocol,
                "topic": topic,
                "body": body,
                "metadata": metadata or {},
            },
            target,
        )

    def subscribe_applications(
        self, filter: Optional[dict[str, Any]] = None
    ) -> PrimadbApplicationRouteSubscription:
        subscription: PrimadbApplicationRouteSubscription

        def on_close() -> None:
            if subscription in self._application_subscriptions:
                self._application_subscriptions.remove(subscription)

        subscription = PrimadbApplicationRouteSubscription(filter or {}, on_close)
        self._application_subscriptions.append(subscription)
        return subscription

    def next_application(self, filter: Optional[dict[str, Any]] = None) -> Optional[dict[str, Any]]:
        return self.try_next_application(filter)

    def try_next_application(self, filter: Optional[dict[str, Any]] = None) -> Optional[dict[str, Any]]:
        filter = filter or {}
        for index, event in enumerate(self._application_events):
            if _application_matches_filter(event, filter):
                return self._application_events.pop(index)
        return None

    def drain_applications(self, filter: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
        filter = filter or {}
        drained: list[dict[str, Any]] = []
        retained: list[dict[str, Any]] = []
        for event in self._application_events:
            if _application_matches_filter(event, filter):
                drained.append(event)
            else:
                retained.append(event)
        self._application_events = retained
        return drained

    def create_route(
        self,
        payload: dict[str, Any],
        target: Optional[dict[str, Any]] = None,
        reply_to: Optional[str] = None,
    ) -> dict[str, Any]:
        self._route_sequence += 1
        return {
            "route_id": f"{self.peer_id}/route/{self._route_sequence:x}",
            "from": self.peer_id,
            "channel": self.channel,
            "target": target or {"kind": "broadcast"},
            "ttl": 6,
            "hops": 0,
            "issued_at_millis": _now_millis(),
            "reply_to": reply_to,
            "content_hash": _content_hash(payload),
            "seen_by": [self.peer_id],
            "payload": payload,
        }

    def send_route(self, route: dict[str, Any]) -> int:
        if self._closed or not self._subscribers:
            return 0
        payload = {
            "type": "primadb.route.v1",
            "from": self.peer_id,
            "sentAt": _now_millis(),
            "route": route,
        }
        frame = PrimadbMoqFrame(
            path=self.path,
            track=self.track,
            sequence=self._sequence,
            payload=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        )
        self._sequence += 1
        for subscriber in list(self._subscribers):
            subscriber.receive_frame(frame)
        return len(self._subscribers)

    def announce_presence(self) -> int:
        return self.send_route(
            self.create_route(
                {
                    "kind": "presence",
                    "peer": {
                        "peer_id": self.peer_id,
                        "replica_id": self.db.replica_id(),
                        "transport": "moq",
                        "capabilities": ["sync", "ack", "routing", "batch", "peer_exchange", "application_routes"],
                        "topics": [self.channel],
                        "metadata": {
                            "moq_path": self.path,
                            "moq_track": self.track,
                        },
                    },
                }
            )
        )

    def flush_pending(self) -> int:
        if self._closed or not self._subscribers:
            return 0
        if not _has_pending_ops(self.db.pending_envelope()):
            return 0
        envelope_json = _drain_pending_envelope_json(self.db)
        if not _has_pending_ops(json.loads(envelope_json)):
            return 0

        envelope = json.loads(envelope_json)
        return self.send_route(
            self.create_route(
                {
                    "kind": "sync",
                    "encoding": "sync_frame",
                    "payload": {
                        "type": "sync",
                        "from": envelope.get("from", self.db.replica_id()),
                        "message_id": f"{self.peer_id}/sync/{self._route_sequence + 1:x}",
                        "envelopeJson": envelope_json,
                        "ops": envelope.get("ops", []),
                    },
                }
            )
        )

    def receive_frame(self, frame: PrimadbMoqFrame) -> int:
        if self._closed:
            return 0
        if frame.path != self.path or frame.track != self.track:
            return 0
        message = frame.json()
        if not isinstance(message, dict):
            return 0
        if message.get("type") == "primadb.route.v1" and _is_route_envelope(message.get("route")):
            return self._accept_route(message["route"])
        if _is_route_envelope(message):
            return self._accept_route(message)
        if message.get("type") == "primadb.sync.v1":
            if message.get("from") == self.db.replica_id():
                return 0
            return _apply_moq_payload(self.db, message)
        return 0

    def _accept_route(self, route: dict[str, Any]) -> int:
        if (
            route.get("from") == self.peer_id
            or self.peer_id in route.get("seen_by", [])
            or route["route_id"] in self._seen_routes
            or not _route_targets_peer(route, self._accepted_peer_ids, self.channel)
        ):
            return 0
        self._seen_routes.add(route["route_id"])
        if len(self._seen_routes) > 4096:
            self._seen_routes.pop()
        for handler in list(self._route_handlers):
            handler(route)
        return self._accept_route_payload(route, route["payload"])

    def _accept_route_payload(self, route: dict[str, Any], payload: dict[str, Any]) -> int:
        if payload.get("kind") == "batch" and isinstance(payload.get("items"), list):
            total = 0
            for item in payload["items"]:
                if isinstance(item, dict):
                    total += self._accept_route_payload(route, item)
            return total
        if payload.get("kind") == "application" and _is_application_message(payload.get("message")):
            self._enqueue_application_route(
                {
                    "routeId": route["route_id"],
                    "from": route["from"],
                    "channel": route["channel"],
                    "target": route["target"],
                    "issuedAtMillis": route["issued_at_millis"],
                    "receivedAtMillis": _now_millis(),
                    "transport": "moq",
                    "verifiedIdentity": None,
                    "message": _normalize_application_message(payload["message"]),
                }
            )
            return 0
        if payload.get("kind") == "presence" and isinstance(payload.get("peer"), dict):
            peer = payload["peer"]
            if peer.get("metadata", {}).get("state") == "offline":
                self._known_peers.pop(peer.get("peer_id"), None)
            elif isinstance(peer.get("peer_id"), str):
                self._known_peers[peer["peer_id"]] = peer
            return 0
        if payload.get("kind") == "peer_exchange":
            for recommendation in payload.get("peers", []):
                peer = recommendation.get("peer") if isinstance(recommendation, dict) else None
                if isinstance(peer, dict) and isinstance(peer.get("peer_id"), str):
                    self._recommendations[peer["peer_id"]] = recommendation
            return 0
        if payload.get("kind") != "sync" or payload.get("encoding") != "sync_frame":
            return 0
        frame = payload.get("payload")
        if not isinstance(frame, dict):
            return 0
        if frame.get("type") != "sync":
            return 0
        applied = _apply_sync_frame_payload(self.db, frame)
        self.send_route(
            self.create_route(
                {
                    "kind": "sync",
                    "encoding": "sync_frame",
                    "payload": {
                        "type": "ack",
                        "from": self.db.replica_id(),
                        "message_id": frame.get("message_id"),
                        "applied": applied,
                    },
                },
                {"kind": "peer", "value": route["from"]},
                route["route_id"],
            )
        )
        return applied

    def _enqueue_application_route(self, event: dict[str, Any]) -> None:
        self._application_events.append(event)
        _trim_queue(self._application_events)
        for subscription in list(self._application_subscriptions):
            subscription.enqueue(event)

    def close(self) -> None:
        self._closed = True
        self._subscribers.clear()
        self._application_events.clear()
        for subscription in list(self._application_subscriptions):
            subscription.close()


class PrimadbMoqLoopback:
    def __init__(self, publisher: PrimadbMoqSession, subscriber: PrimadbMoqSession) -> None:
        self.publisher = publisher
        self.subscriber = subscriber
        self.subscriber.subscribe_from(self.publisher)
        self.publisher.announce_presence()

    def flush(self) -> int:
        return self.publisher.flush_pending()

    def close(self) -> None:
        self.publisher.close()
        self.subscriber.close()


def create_primadb_moq_loopback(
    *,
    publisher_db: Any,
    subscriber_db: Any,
    path: str,
    track: str = DEFAULT_ROUTE_TRACK,
    channel: str = DEFAULT_CHANNEL,
) -> PrimadbMoqLoopback:
    publisher = PrimadbMoqSession(publisher_db, path=path, track=track, channel=channel)
    subscriber = PrimadbMoqSession(subscriber_db, path=path, track=track, channel=channel)
    return PrimadbMoqLoopback(publisher, subscriber)
