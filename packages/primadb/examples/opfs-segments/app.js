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
  throw new Error("timed out waiting for OPFS segment persistence");
}

function assertOpfsAvailable() {
  if (globalThis.navigator?.storage?.getDirectory == null) {
    throw new Error("OPFS navigator.storage.getDirectory is unavailable");
  }
}

async function runOpfsSegmentRegression(options = {}) {
  assertOpfsAvailable();
  const namespace = options.namespace ?? `opfs-segments-regression-${Date.now().toString(36)}`;
  const directory = options.directory ?? "primadb-opfs-segments-regression";
  const seedCount = options.seedCount ?? 32;
  const seedSize = options.seedSize ?? 4096;
  const iterations = options.iterations ?? 8;
  const checkpointSize = options.checkpointSize ?? 64 * 1024;

  const db = new Primadb(`opfs-segments-${Date.now().toString(36)}`);
  const seed = db.chain("seed");
  for (let index = 0; index < seedCount; index += 1) {
    seed.field(`doc-${index}`).put(`${index}:${"s".repeat(seedSize)}`);
  }

  const hook = await db.enableOpfsSegmentPersistence(directory, namespace, false);

  const checkpoint = db.chain("checkpoint").field("active");
  for (let index = 0; index < iterations; index += 1) {
    const before = hook.stats().incrementalTransactions;
    checkpoint.put(`${index}:${"z".repeat(checkpointSize)}`);
    await waitFor(() => hook.stats().incrementalTransactions > before);
  }

  const stats = hook.stats();
  const estimate = await hook.estimateStorage();
  hook.close();

  const restored = new Primadb(`opfs-restored-${Date.now().toString(36)}`);
  const restoredLoaded = await restored.loadOpfsSegments(directory, namespace);
  const restoredCheckpoint = restored.chain("checkpoint").field("active").once();

  return {
    directory,
    namespace,
    seedCount,
    iterations,
    stats,
    estimate,
    restoredLoaded,
    restoredCheckpointPrefix:
      typeof restoredCheckpoint === "string" ? restoredCheckpoint.slice(0, 16) : restoredCheckpoint,
    restoredCheckpointLength:
      typeof restoredCheckpoint === "string" ? restoredCheckpoint.length : null,
  };
}

Object.assign(globalThis, {
  opfsSegmentsRegression: {
    run: runOpfsSegmentRegression,
  },
});
