from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any


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


@dataclass
class PrimadbMoqFrame:
    path: str
    track: str
    sequence: int
    payload: bytes

    def json(self) -> Any:
        return json.loads(self.payload.decode("utf-8"))


class PrimadbMoqSession:
    """PrimaDB MoQ track adapter.

    The Python package currently exposes a deterministic loopback adapter because the available
    Python MoQ bindings do not yet expose stable generic byte tracks on Python 3.14. The API mirrors
    the browser/Node helpers so Python examples can exercise the same path/track/object mapping.
    """

    def __init__(self, db: Any, *, path: str, track: str = "ops") -> None:
        self.db = db
        self.path = path
        self.track = track
        self._sequence = 0
        self._subscribers: list[PrimadbMoqSession] = []
        self._closed = False

    def subscribe_from(self, publisher: "PrimadbMoqSession") -> None:
        if self._closed:
            raise RuntimeError("MoQ session is closed")
        publisher._subscribers.append(self)

    def flush_pending(self) -> int:
        if self._closed or not self._subscribers:
            return 0
        if not _has_pending_ops(self.db.pending_envelope()):
            return 0
        envelope_json = _drain_pending_envelope_json(self.db)
        if not _has_pending_ops(json.loads(envelope_json)):
            return 0

        payload = {
            "type": "primadb.sync.v1",
            "from": self.db.replica_id(),
            "sentAt": int(time.time() * 1000),
            "envelopeJson": envelope_json,
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

    def receive_frame(self, frame: PrimadbMoqFrame) -> int:
        if self._closed:
            return 0
        if frame.path != self.path or frame.track != self.track:
            return 0
        message = frame.json()
        if not isinstance(message, dict) or message.get("type") != "primadb.sync.v1":
            return 0
        if message.get("from") == self.db.replica_id():
            return 0
        return _apply_moq_payload(self.db, message)

    def close(self) -> None:
        self._closed = True
        self._subscribers.clear()


class PrimadbMoqLoopback:
    def __init__(self, publisher: PrimadbMoqSession, subscriber: PrimadbMoqSession) -> None:
        self.publisher = publisher
        self.subscriber = subscriber
        self.subscriber.subscribe_from(self.publisher)

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
    track: str = "ops",
) -> PrimadbMoqLoopback:
    publisher = PrimadbMoqSession(publisher_db, path=path, track=track)
    subscriber = PrimadbMoqSession(subscriber_db, path=path, track=track)
    return PrimadbMoqLoopback(publisher, subscriber)
