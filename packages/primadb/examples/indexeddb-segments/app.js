import { Primadb, initPrimadb } from "primadb";

const status = document.querySelector("#status");

await initPrimadb();
status.textContent = "ready";

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) {
      return;
    }
    await wait(25);
  }
  throw new Error("timed out waiting for IndexedDB segment persistence");
}

async function runIndexedDbSegmentRegression(options = {}) {
  const namespace =
    options.namespace ?? `indexeddb-segments-regression-${Date.now().toString(36)}`;
  const seedCount = options.seedCount ?? 32;
  const seedSize = options.seedSize ?? 4096;
  const iterations = options.iterations ?? 8;
  const checkpointSize = options.checkpointSize ?? 64 * 1024;

  const db = new Primadb(`idb-segments-${Date.now().toString(36)}`);
  const seed = db.chain("seed");
  for (let index = 0; index < seedCount; index += 1) {
    seed.field(`doc-${index}`).put(`${index}:${"s".repeat(seedSize)}`);
  }

  const hook = await db.enableIndexedDbSegmentPersistence(
    "primadb-indexeddb-segments-regression",
    "segments",
    namespace,
    false,
  );

  const checkpoint = db.chain("checkpoint").field("active");
  for (let index = 0; index < iterations; index += 1) {
    const before = hook.stats().incrementalTransactions;
    checkpoint.put(`${index}:${"z".repeat(checkpointSize)}`);
    await waitFor(() => hook.stats().incrementalTransactions > before);
  }

  const stats = hook.stats();
  const estimate = await hook.estimateStorage();
  hook.close();
  return {
    namespace,
    seedCount,
    iterations,
    stats,
    estimate,
  };
}

Object.assign(globalThis, {
  indexedDbSegmentsRegression: {
    run: runIndexedDbSegmentRegression,
  },
});
