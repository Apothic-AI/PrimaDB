#!/usr/bin/env node
import { mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { Primadb } from "../../index.js";

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, ".data");
const segmentsDir = join(root, "segments");
const blobsDir = join(root, "blobs");

mkdirSync(segmentsDir, { recursive: true });
mkdirSync(blobsDir, { recursive: true });

const db = new Primadb("node-example-local");
const durable = db.openDurableStorage({
  kind: "segment_files",
  directory: segmentsDir,
  journalRetention: 4,
});
const blobs = db.openBlobStorage({
  kind: "files",
  directory: blobsDir,
});

const notes = db.chain("package_examples").field("node_local").field("notes");
const bytes = db.chain("package_examples").field("node_local").field("avatar_bytes");
const blob = db.chain("package_examples").field("node_local").field("archive_blob");

notes.set({
  title: `Node package example ${new Date().toISOString()}`,
  body: "Stored through the native addon example",
  created_at: Date.now(),
  updated_at: Date.now(),
});

const payload = Buffer.from([1, 3, 3, 7, 9, 21]);
bytes.putBytes(payload);
const blobRef = blob.putBlob(Buffer.from("primadb-node-example"), "application/octet-stream");

const noteEntries = notes.query({
  order: { path: "updated_at", direction: "desc" },
  limit: 5,
});

console.log(
  JSON.stringify(
    {
      durable,
      blobs,
      noteCount: Array.isArray(noteEntries) ? noteEntries.length : null,
      latestNote: Array.isArray(noteEntries) && noteEntries[0] ? noteEntries[0] : null,
      bytes: bytes.onceBytes() ? Array.from(bytes.onceBytes()) : null,
      blobRef,
      blob: blob.getBlob() ? Array.from(blob.getBlob()) : null,
    },
    null,
    2,
  ),
);
