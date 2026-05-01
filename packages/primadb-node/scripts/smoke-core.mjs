#!/usr/bin/env node
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Primadb, derivePasswordKey } from "../index.js";

const tempDir = mkdtempSync(join(tmpdir(), "primadb-node-core-"));

try {
  const db = new Primadb("node-core-a");
  const passwordKey = derivePasswordKey("node smoke password", {
    saltBase64: "MTIzNDU2Nzg5MGFiY2RlZg",
    memoryCostKiB: 32,
    timeCost: 1,
    parallelism: 1,
  });
  db.setSnapshotEncryptionKey(passwordKey.keyBase64);
  db.setTransportEncryptionKey(passwordKey.keyBase64);
  const binding = db.openDurableStorage({
    kind: "segment_files",
    directory: tempDir,
    journalRetention: 4,
  });
  const blobBinding = db.openBlobStorage({
    kind: "files",
    directory: `${tempDir}/blobs`,
  });

  const notes = db.chain("notes").field("items");
  const binary = db.chain("assets").field("bytes");
  const blobChain = db.chain("assets").field("blob");
  const graphAlice = db.chain("graph").field("alice");
  const ledger = db.scope("ledger");
  const offlineLedger = db.scope("offline-ledger");
  const subscription = notes.subscribe();
  const title = `Node core ${Date.now()}`;
  const payload = Buffer.from([1, 2, 3, 5, 8, 13]);

  notes.set({
    title,
    body: "native addon smoke",
    createdAt: new Date().toISOString(),
  });
  graphAlice.put({
    name: "Alice",
    friend: { $link: "graph/bob" },
  });
  ledger.configure({
    consistency: "coordinated",
    authority: { kind: "full_node", peerId: "native:node-core-a" },
  });
  const ledgerReport = ledger.transaction([
    {
      kind: "increment",
      path: { anchor: "alice", segments: ["balance"] },
      by: 10,
    },
  ]);
  offlineLedger.configure({
    consistency: "coordinated",
    authority: { kind: "full_node", peerId: "native:missing-ledger" },
    offlineWrites: "queue_provisional",
  });
  const provisionalReport = offlineLedger.transaction([
    {
      kind: "increment",
      path: { anchor: "alice", segments: ["balance"] },
      by: 10,
    },
  ]);
  db.chain("graph").field("bob").put({
    name: "Bob",
  });
  binary.putBytes(payload);
  const blobRef = blobChain.putBlob(payload, "application/octet-stream");
  const roundTripBytes = binary.onceBytes();
  const roundTripBlob = blobChain.getBlob();

  let message = await subscription.next();
  if (message?.value == null && !message?.done) {
    message = await subscription.next();
  }
  const entries = notes.query({
    filters: [{ kind: "prefix", path: "title", value: "Node core" }],
    order: { path: "createdAt", direction: "desc" },
  });

  const restored = new Primadb("node-core-b");
  restored.setSnapshotEncryptionKey(passwordKey.keyBase64);
  restored.setTransportEncryptionKey(passwordKey.keyBase64);
  const restoredBinding = restored.openDurableStorage({
    kind: "segment_files",
    directory: tempDir,
    journalRetention: 4,
  });
  const restoredBlobBinding = restored.openBlobStorage({
    kind: "files",
    directory: `${tempDir}/blobs`,
  });
  const restoredEntries = restored.chain("notes").field("items").query({
    filters: [{ kind: "prefix", path: "title", value: "Node core" }],
  });
  const restoredBytes = restored.chain("assets").field("bytes").onceBytes();
  const restoredBlob = restored.chain("assets").field("blob").getBlob();
  const traversal = restored.chain("graph").field("alice").traverse({
    maxDepth: 1,
    includeValues: true,
  });
  const traversalWatch = restored.chain("graph").field("alice").watchTraverse({
    maxDepth: 1,
    includeValues: true,
  });
  const traversalInitial = await traversalWatch.next();
  restored.chain("graph").field("bob").put({
    name: "Robert",
  });
  const traversalUpdate = await traversalWatch.next();
  traversalWatch.close();
  const restoredLedgerBalance = restored.chain("ledger").field("alice").field("balance").once();
  const provisionalCanonical = db
    .chain("offline-ledger")
    .field("alice")
    .field("balance")
    .once();

  console.log(
    JSON.stringify(
      {
        binding,
        blobBinding,
        passwordKey: {
          algorithm: passwordKey.algorithm,
          saltBase64: passwordKey.saltBase64,
          memoryCostKiB: passwordKey.params.memoryCostKiB,
        },
        restoredBinding,
        restoredBlobBinding,
        subscriptionMessage: message,
        entryCount: Array.isArray(entries) ? entries.length : null,
        restoredCount: Array.isArray(restoredEntries) ? restoredEntries.length : null,
        blobRef,
        roundTripBytes: roundTripBytes ? Array.from(roundTripBytes) : null,
        roundTripBlob: roundTripBlob ? Array.from(roundTripBlob) : null,
        restoredBytes: restoredBytes ? Array.from(restoredBytes) : null,
        restoredBlob: restoredBlob ? Array.from(restoredBlob) : null,
        traversal,
        traversalInitial,
        traversalUpdate,
        ledgerReport,
        provisionalReport,
        restoredLedgerBalance,
        provisionalCanonical,
        offlineProposals: offlineLedger.proposals(),
        node_package_core_confirmed:
          Array.isArray(entries) &&
          entries.length >= 1 &&
          traversal?.entries?.some?.((entry) => entry.nodeId === "graph/bob" && entry.value?.name === "Bob") &&
          traversalUpdate?.value?.entries?.some?.(
            (entry) => entry.nodeId === "graph/bob" && entry.value?.name === "Robert",
          ) &&
          JSON.stringify(roundTripBytes ? Array.from(roundTripBytes) : null) === JSON.stringify(Array.from(payload)) &&
          JSON.stringify(roundTripBlob ? Array.from(roundTripBlob) : null) === JSON.stringify(Array.from(payload)) &&
          JSON.stringify(restoredBytes ? Array.from(restoredBytes) : null) === JSON.stringify(Array.from(payload)) &&
          JSON.stringify(restoredBlob ? Array.from(restoredBlob) : null) === JSON.stringify(Array.from(payload)) &&
          ledgerReport.status === "committed" &&
          restoredLedgerBalance === 10 &&
          provisionalReport.status === "provisional" &&
          provisionalCanonical === null &&
          offlineLedger.proposals().length === 1 &&
          passwordKey.algorithm === "argon2id-v1.3" &&
          passwordKey.saltBase64 === "MTIzNDU2Nzg5MGFiY2RlZg" &&
          typeof passwordKey.keyBase64 === "string" &&
          passwordKey.keyBase64.length > 0,
      },
      null,
      2,
    ),
  );
} finally {
  rmSync(tempDir, { recursive: true, force: true });
}
