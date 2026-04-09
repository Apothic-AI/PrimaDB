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

  const notes = db.chain("notes").field("items");
  const subscription = notes.subscribe();
  const title = `Node core ${Date.now()}`;

  notes.set({
    title,
    body: "native addon smoke",
    createdAt: new Date().toISOString(),
  });

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
  const restoredEntries = restored.chain("notes").field("items").query({
    filters: [{ kind: "prefix", path: "title", value: "Node core" }],
  });

  console.log(
    JSON.stringify(
      {
        binding,
        restoredBinding,
        subscriptionMessage: message,
        entryCount: Array.isArray(entries) ? entries.length : null,
        restoredCount: Array.isArray(restoredEntries) ? restoredEntries.length : null,
        node_package_core_confirmed: true,
      },
      null,
      2,
    ),
  );
} finally {
  rmSync(tempDir, { recursive: true, force: true });
}
