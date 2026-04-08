import init, * as primadb from "./pkg/primadb.js";

const state = {
  db: null,
  seeded: 0,
};

const buildMode = document.querySelector("#build-mode");
const threadCount = document.querySelector("#thread-count");
const seedCount = document.querySelector("#seed-count");
const queryTiming = document.querySelector("#query-timing");
const resultOutput = document.querySelector("#result-output");
const seedButton = document.querySelector("#seed-button");
const queryButton = document.querySelector("#query-button");

async function bootstrap() {
  await init();

  if (typeof primadb.initThreadPool !== "function") {
    throw new Error("This package was not built with the wasm-threads feature.");
  }

  const requestedThreads = Math.max(2, Math.min(navigator.hardwareConcurrency || 4, 8));
  await primadb.initThreadPool(requestedThreads);

  state.db = new primadb.Primadb("threaded-browser");

  buildMode.textContent = primadb.parallelEnabled() ? "wasm-threads" : "fallback";
  threadCount.textContent = String(primadb.parallelThreadCount());
  seedCount.textContent = "0";
  queryTiming.textContent = "ready";
  resultOutput.textContent = "Thread pool initialized. Seed the database to run a parallel query.";

  seedButton.addEventListener("click", () => {
    void seedNotes();
  });
  queryButton.addEventListener("click", runQuery);
}

async function seedNotes() {
  if (!state.db) {
    return;
  }

  seedButton.disabled = true;
  queryButton.disabled = true;
  resultOutput.textContent = "Seeding 4,000 notes...";

  const notes = state.db.chain("notes");
  const total = 4000;
  const batchSize = 500;

  for (let start = 0; start < total; start += batchSize) {
    const end = Math.min(start + batchSize, total);
    for (let index = start; index < end; index += 1) {
      notes.field(`note-${String(index).padStart(5, "0")}`).put({
        title: `Note ${String(index).padStart(5, "0")}`,
        category: index % 3 === 0 ? "ops" : "product",
        archived: index % 11 === 0,
        priority: index % 5,
      });
    }
    resultOutput.textContent = `Seeding ${end.toLocaleString()} / ${total.toLocaleString()} notes...`;
    await yieldToBrowser();
  }

  state.seeded = total;
  seedCount.textContent = String(state.seeded);
  resultOutput.textContent = "Seed complete. Run the parallel query to exercise Rayon in the threaded WASM pool.";
  seedButton.disabled = false;
  queryButton.disabled = false;
}

function runQuery() {
  if (!state.db) {
    return;
  }

  const start = performance.now();
  const entries = state.db.chain("notes").query({
    filters: [
      { kind: "eq", path: "archived", value: false },
      { kind: "gte", path: "priority", value: 2 },
      { kind: "prefix", path: "title", value: "Note 0" },
    ],
    order: { path: "title", direction: "asc" },
    limit: 1200,
  });
  const elapsed = performance.now() - start;

  queryTiming.textContent = `${elapsed.toFixed(1)} ms`;
  resultOutput.textContent = JSON.stringify(
    {
      parallelEnabled: primadb.parallelEnabled(),
      parallelThreadCount: primadb.parallelThreadCount(),
      seeded: state.seeded,
      queryMatches: entries.length,
      firstKey: entries[0]?.key ?? null,
      lastKey: entries.at(-1)?.key ?? null,
    },
    null,
    2,
  );
}

bootstrap().catch((error) => {
  buildMode.textContent = "failed";
  queryTiming.textContent = "error";
  resultOutput.textContent = error.stack || String(error);
  seedButton.disabled = true;
  queryButton.disabled = true;
});

function yieldToBrowser() {
  return new Promise((resolve) => {
    requestAnimationFrame(() => resolve());
  });
}
