#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shutil
import tempfile
import time

from primadb import Primadb, derive_password_key


def main() -> None:
    root = tempfile.mkdtemp(prefix="primadb-python-core-")
    try:
        db = Primadb("python-core-a")
        password_key = derive_password_key(
            "python smoke password",
            {
                "saltBase64": "MTIzNDU2Nzg5MGFiY2RlZg",
                "memoryCostKiB": 32,
                "timeCost": 1,
                "parallelism": 1,
            },
        )
        db.set_snapshot_encryption_key(password_key["keyBase64"])
        db.set_transport_encryption_key(password_key["keyBase64"])
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
        script_path = {"anchor": "notes", "segments": ["scripted"]}
        script_capabilities = {
            "read": [{"root": "notes", "recursive": True}],
            "write": [{"root": "derived", "recursive": True}],
            "transaction": [{"root": "derived", "recursive": True}],
        }

        note_id = notes.set(
            {
                "title": "Python package note",
                "body": "stored through the native Python package",
                "done": False,
            }
        )
        db.chain("notes").field("scripted").put({"title": "Scripted note"})
        db.attach_node_script(
            script_path,
            {
                "id": "derive-title",
                "source": """
                    fn main(ctx) {
                        let note = db_get("notes/scripted");
                        db_put("derived/scripted", #{ title: note.title, source: ctx.path.display });
                        return #{ title: note.title };
                    }
                """,
                "capabilities": script_capabilities,
            },
        )
        script_results = db.execute_node_scripts(
            script_path,
            {"capabilities": script_capabilities},
        )
        scripted = db.chain("derived").field("scripted").once()
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
        db.put_record("agentfs/inode/1", {"kind": "file", "size": len(payload)})
        db.put_record_bytes("agentfs/chunk/1/000000", payload)
        record_scan = db.scan_records({"prefix": "agentfs/"})
        storage_sync = db.sync_storage()
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

        db.close_durable_storage()
        restored = Primadb("python-core-b")
        restored.set_snapshot_encryption_key(password_key["keyBase64"])
        restored.set_transport_encryption_key(password_key["keyBase64"])
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
        restored_record = restored.get_record("agentfs/inode/1")
        restored_record_scan = restored.scan_records({"prefix": "agentfs/chunk/1/"})
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
                    "passwordKey": {
                        "algorithm": password_key["algorithm"],
                        "saltBase64": password_key["saltBase64"],
                        "memoryCostKiB": password_key["params"]["memoryCostKiB"],
                    },
                    "restoredBinding": restored_binding,
                    "restoredBlobBinding": restored_blob_binding,
                    "noteId": note_id,
                    "blobRef": blob_ref,
                    "roundTripBytes": list(round_trip_bytes) if round_trip_bytes is not None else None,
                    "roundTripBlob": list(round_trip_blob) if round_trip_blob is not None else None,
                    "restoredBytes": list(restored_bytes) if restored_bytes is not None else None,
                    "restoredBlob": list(restored_blob) if restored_blob is not None else None,
                    "recordScan": record_scan,
                    "storageSync": storage_sync,
                    "restoredRecord": restored_record,
                    "restoredRecordScan": restored_record_scan,
                    "traversal": traversal,
                    "traversalInitial": traversal_initial,
                    "traversalUpdate": traversal_update,
                    "scriptResults": script_results,
                    "scripted": scripted,
                    "ledgerReport": ledger_report,
                    "provisionalReport": provisional_report,
                    "restoredLedgerBalance": restored_ledger_balance,
                    "provisionalCanonical": provisional_canonical,
                    "offlineProposals": offline_ledger.proposals(),
                    "subscriptionEvent": event,
                    "restoredCount": len(results),
                    "python_package_core_confirmed": (
                        len(results) == 1
                        and script_results[0]["report"]["status"] == "committed"
                        and scripted["title"] == "Scripted note"
                        and scripted["source"] == "notes/scripted"
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
                        and storage_sync["synced"] is True
                        and len(record_scan["entries"]) == 2
                        and restored_record["value"]["value"]["size"] == len(payload)
                        and len(restored_record_scan["entries"]) == 1
                        and ledger_report["status"] == "committed"
                        and restored_ledger_balance == 10
                        and provisional_report["status"] == "provisional"
                        and provisional_canonical is None
                        and len(offline_ledger.proposals()) == 1
                        and password_key["algorithm"] == "argon2id-v1.3"
                        and password_key["saltBase64"] == "MTIzNDU2Nzg5MGFiY2RlZg"
                        and isinstance(password_key["keyBase64"], str)
                        and len(password_key["keyBase64"]) > 0
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
