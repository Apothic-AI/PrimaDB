import { createPrimadb } from "primadb";
import { connectPrimadbMoq, createPrimadbMoqLoopback, moqRuntimeSupport } from "primadb/moq";

const publisherEl = document.querySelector("#publisher");
const subscriberEl = document.querySelector("#subscriber");
const moqEl = document.querySelector("#moq");
const params = new URLSearchParams(globalThis.location.search);

function render(element, value) {
  element.textContent = JSON.stringify(value, null, 2);
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function waitFor(predicate, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) {
      return true;
    }
    await sleep(50);
  }
  return false;
}

const room = params.get("room") || `browser-moq-${Date.now()}`;
const path = `primadb/examples/moq/${room}`;
const publisherDb = await createPrimadb(`browser-moq-pub-${room}`);
const subscriberDb = await createPrimadb(`browser-moq-sub-${room}`);
const notesPath = ["moq_sync", room, "notes"];
const publisherNotes = notesPath.reduce((chain, key) => chain.field(key), publisherDb.chain("package_examples"));
const subscriberNotes = notesPath.reduce((chain, key) => chain.field(key), subscriberDb.chain("package_examples"));

const link = await createPrimadbMoqLoopback({
  publisherDb,
  subscriberDb,
  path,
  intervalMs: 5000,
});

await sleep(100);

publisherNotes.set({
  title: "MoQ browser sync",
  body: "This record moved through a MoQ track.",
  updated_at: Date.now(),
});

let sent = 0;
for (let attempt = 0; attempt < 10 && sent === 0; attempt += 1) {
  sent = await link.flush();
  await sleep(100);
}

await waitFor(() => subscriberNotes.map().length > 0);

const publisherEntries = publisherNotes.query({
  order: { path: "updated_at", direction: "desc" },
  limit: 5,
});
const subscriberEntries = subscriberNotes.query({
  order: { path: "updated_at", direction: "desc" },
  limit: 5,
});

render(publisherEl, {
  replicaId: publisherDb.replicaId(),
  entries: publisherEntries,
});
render(subscriberEl, {
  replicaId: subscriberDb.replicaId(),
  entries: subscriberEntries,
});
render(moqEl, {
  runtime: moqRuntimeSupport(),
  path,
  track: "ops",
  sentTracks: sent,
  replicated: subscriberEntries.length > 0,
});

Object.assign(globalThis, {
  primadbMoqExample: {
    publisherDb,
    subscriberDb,
    link,
    path,
    publisherEntries,
    subscriberEntries,
    sent,
  },
  primadbMoqApi: {
    connectPrimadbMoq,
  },
});
