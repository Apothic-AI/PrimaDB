#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shutil
import tempfile
import time

from primadb import Primadb


def main() -> None:
    root = tempfile.mkdtemp(prefix="primadb-python-core-")
    try:
        db = Primadb("python-core-a")
        binding = db.open_durable_storage(
            {
                "kind": "segment_files",
                "directory": root,
            }
        )
        blob_binding = db.open_blob_storage(
            {
                "kind": "files",
                "directory": os.path.join(root, "blobs"),
            }
        )

        notes = db.chain("notes").field("items")
        binary = db.chain("assets").field("bytes")
        blob_chain = db.chain("assets").field("blob")
        graph_alice = db.chain("graph").field("alice")
        ledger = db.scope("ledger")
        offline_ledger = db.scope("offline-ledger")
        subscription = notes.subscribe()
        payload = bytes([1, 2, 3, 5, 8, 13])

        note_id = notes.set(
            {
                "title": "Python package note",
                "body": "stored through the native Python package",
                "done": False,
            }
        )
        graph_alice.put({"name": "Alice", "friend": {"$link": "graph/bob"}})
        ledger.configure(
            {
                "consistency": "coordinated",
                "authority": {"kind": "full_node", "peerId": "native:python-core-a"},
            }
        )
        ledger_report = ledger.transaction(
            [
                {
                    "kind": "increment",
                    "path": {"anchor": "alice", "segments": ["balance"]},
                    "by": 10,
                }
            ]
        )
        offline_ledger.configure(
            {
                "consistency": "coordinated",
                "authority": {"kind": "full_node", "peerId": "native:missing-ledger"},
                "offlineWrites": "queue_provisional",
            }
        )
        provisional_report = offline_ledger.transaction(
            [
                {
                    "kind": "increment",
                    "path": {"anchor": "alice", "segments": ["balance"]},
                    "by": 10,
                }
            ]
        )
        db.chain("graph").field("bob").put({"name": "Bob"})
        binary.put_bytes(payload)
        blob_ref = blob_chain.put_blob(payload, "application/octet-stream")
        round_trip_bytes = binary.once_bytes()
        round_trip_blob = blob_chain.get_blob()

        event = None
        deadline = time.time() + 10
        while time.time() < deadline:
            candidate = subscription.try_next()
            if candidate["value"] is not None:
                event = candidate
                break
            time.sleep(0.05)

        restored = Primadb("python-core-b")
        restored_binding = restored.open_durable_storage(
            {
                "kind": "segment_files",
                "directory": root,
            }
        )
        restored_blob_binding = restored.open_blob_storage(
            {
                "kind": "files",
                "directory": os.path.join(root, "blobs"),
            }
        )

        results = restored.chain("notes").field("items").query(
            {
                "filters": [{"kind": "eq", "path": "title", "value": "Python package note"}],
                "limit": 10,
            }
        )
        restored_bytes = restored.chain("assets").field("bytes").once_bytes()
        restored_blob = restored.chain("assets").field("blob").get_blob()
        traversal = restored.chain("graph").field("alice").traverse(
            {
                "maxDepth": 1,
                "includeValues": True,
            }
        )
        traversal_watch = restored.chain("graph").field("alice").watch_traverse(
            {
                "maxDepth": 1,
                "includeValues": True,
            }
        )
        traversal_initial = traversal_watch.next()
        restored.chain("graph").field("bob").put({"name": "Robert"})
        traversal_update = traversal_watch.next()
        traversal_watch.close()
        restored_ledger_balance = restored.chain("ledger").field("alice").field("balance").once()
        provisional_canonical = (
            db.chain("offline-ledger").field("alice").field("balance").once()
        )

        print(
            json.dumps(
                {
                    "binding": binding,
                    "blobBinding": blob_binding,
                    "restoredBinding": restored_binding,
                    "restoredBlobBinding": restored_blob_binding,
                    "noteId": note_id,
                    "blobRef": blob_ref,
                    "roundTripBytes": list(round_trip_bytes) if round_trip_bytes is not None else None,
                    "roundTripBlob": list(round_trip_blob) if round_trip_blob is not None else None,
                    "restoredBytes": list(restored_bytes) if restored_bytes is not None else None,
                    "restoredBlob": list(restored_blob) if restored_blob is not None else None,
                    "traversal": traversal,
                    "traversalInitial": traversal_initial,
                    "traversalUpdate": traversal_update,
                    "ledgerReport": ledger_report,
                    "provisionalReport": provisional_report,
                    "restoredLedgerBalance": restored_ledger_balance,
                    "provisionalCanonical": provisional_canonical,
                    "offlineProposals": offline_ledger.proposals(),
                    "subscriptionEvent": event,
                    "restoredCount": len(results),
                    "python_package_core_confirmed": (
                        len(results) == 1
                        and any(
                            entry["nodeId"] == "graph/bob" and entry["value"]["name"] == "Bob"
                            for entry in traversal["entries"]
                        )
                        and any(
                            entry["nodeId"] == "graph/bob" and entry["value"]["name"] == "Robert"
                            for entry in traversal_update["value"]["entries"]
                        )
                        and round_trip_bytes == payload
                        and round_trip_blob == payload
                        and restored_bytes == payload
                        and restored_blob == payload
                        and ledger_report["status"] == "committed"
                        and restored_ledger_balance == 10
                        and provisional_report["status"] == "provisional"
                        and provisional_canonical is None
                        and len(offline_ledger.proposals()) == 1
                    ),
                },
                indent=2,
            )
        )

        subscription.close()
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
