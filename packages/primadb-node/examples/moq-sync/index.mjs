#!/usr/bin/env node
import { Primadb } from "../../index.js";
import { createPrimadbMoqLoopback, moqRuntimeSupport } from "../../moq.js";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const room = process.argv.includes("--room")
  ? process.argv[process.argv.indexOf("--room") + 1]
  : `node-moq-${process.pid}`;
const path = `primadb/examples/moq/${room}`;
const publisherDb = new Primadb(`node-moq-pub-${room}`);
const subscriberDb = new Primadb(`node-moq-sub-${room}`);
const publisherNotes = publisherDb.chain("package_examples").field("node_moq").field(room).field("notes");
const subscriberNotes = subscriberDb.chain("package_examples").field("node_moq").field(room).field("notes");

const link = await createPrimadbMoqLoopback({
  publisherDb,
  subscriberDb,
  path,
  intervalMs: 5000,
});

await sleep(100);

publisherNotes.set({
  title: "MoQ Node sync",
  body: "This record moved through a MoQ track.",
  updated_at: Date.now(),
});

let sent = 0;
for (let attempt = 0; attempt < 10 && sent === 0; attempt += 1) {
  sent = await link.flush();
  await sleep(100);
}

const subscriberEntries = subscriberNotes.query({
  order: { path: "updated_at", direction: "desc" },
  limit: 5,
});

const result = {
  runtime: moqRuntimeSupport(),
  path,
  track: "ops",
  sentTracks: sent,
  replicated: Array.isArray(subscriberEntries) && subscriberEntries.length > 0,
  subscriberEntries,
};

console.log(JSON.stringify(result, null, 2));
link.close();

if (!result.replicated) {
  process.exitCode = 1;
}
