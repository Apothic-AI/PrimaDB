#!/usr/bin/env node
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Primadb } from "../index.js";

const tempDir = mkdtempSync(join(tmpdir(), "primadb-node-core-"));

try {
  const db = new Primadb("node-core-a");
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
  const subscription = notes.subscribe();
  const title = `Node core ${Date.now()}`;
  const payload = Buffer.from([1, 2, 3, 5, 8, 13]);

  notes.set({
    title,
    body: "native addon smoke",
    createdAt: new Date().toISOString(),
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

  console.log(
    JSON.stringify(
      {
        binding,
        blobBinding,
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
        node_package_core_confirmed:
          Array.isArray(entries) &&
          entries.length >= 1 &&
          JSON.stringify(roundTripBytes ? Array.from(roundTripBytes) : null) === JSON.stringify(Array.from(payload)) &&
          JSON.stringify(roundTripBlob ? Array.from(roundTripBlob) : null) === JSON.stringify(Array.from(payload)) &&
          JSON.stringify(restoredBytes ? Array.from(restoredBytes) : null) === JSON.stringify(Array.from(payload)) &&
          JSON.stringify(restoredBlob ? Array.from(restoredBlob) : null) === JSON.stringify(Array.from(payload)),
      },
      null,
      2,
    ),
  );
} finally {
  rmSync(tempDir, { recursive: true, force: true });
}
