import init, * as primadb from "./pkg/primadb.js";

const DEFAULT_SNAPSHOT_DB = "primadb-browser-threaded-mesh-notes";
const SNAPSHOT_STORE = "snapshots";
const SNAPSHOT_KEY = "main";
const DEFAULT_MESH_ROOM = "primadb-browser-threaded-mesh-notes";
const SEEDED_NOTE_TOTAL = 320;

const session = createSessionConfig();

const state = {
  db: null,
  listChain: null,
  subscription: null,
  persistence: null,
  mesh: null,
  statusTimer: null,
  refreshTimer: null,
  refreshRunning: false,
  refreshQueued: false,
  filter: "all",
  search: "",
  seeded: 0,
};

const elements = {
  replicaId: document.getElementById("replica-id"),
  buildMode: document.getElementById("build-mode"),
  threadCount: document.getElementById("thread-count"),
  persistenceStatus: document.getElementById("persistence-status"),
  peerCount: document.getElementById("peer-count"),
  meshQueue: document.getElementById("mesh-queue"),
  meshDetail: document.getElementById("mesh-detail"),
  noteForm: document.getElementById("note-form"),
  noteTitle: document.getElementById("note-title"),
  noteBody: document.getElementById("note-body"),
  filterGroup: document.getElementById("filter-group"),
  searchInput: document.getElementById("search-input"),
  seedButton: document.getElementById("seed-button"),
  queryButton: document.getElementById("query-button"),
  seedCount: document.getElementById("seed-count"),
  queryTiming: document.getElementById("query-timing"),
  queryOutput: document.getElementById("query-output"),
  emptyState: document.getElementById("empty-state"),
  noteList: document.getElementById("note-list"),
  noteTemplate: document.getElementById("note-template"),
};

main().catch((error) => {
  console.error(error);
  elements.buildMode.textContent = "failed";
  elements.queryTiming.textContent = "error";
  elements.queryOutput.textContent = error.stack || String(error);
  elements.meshDetail.textContent = "threaded mesh startup failed";
});

async function main() {
  await init();

  if (typeof primadb.initThreadPool !== "function") {
    throw new Error("This package was not built with the wasm-threads feature.");
  }

  const requestedThreads = Math.max(2, Math.min(navigator.hardwareConcurrency || 4, 4));
  await primadb.initThreadPool(requestedThreads);

  const replicaId = `threaded-mesh-${globalThis.crypto?.randomUUID?.() ?? Date.now()}`;
  const db = new primadb.Primadb(replicaId);
  state.db = db;
  state.listChain = db.chain("boards").field(session.room).field("notes");

  elements.replicaId.textContent = replicaId;
  elements.buildMode.textContent = primadb.parallelEnabled() ? "wasm-threads" : "fallback";
  elements.threadCount.textContent = String(primadb.parallelThreadCount());

  await setupPersistence();
  state.mesh = db.connectWebRtcMesh(session.room, 1500);
  bindUi();
  startStatusLoop();

  state.subscription = state.listChain.on(() => {
    scheduleUiRefresh();
  });

  await ensureSeedNote();
  await render();
  updateSeedCount();
  runQuery();

  globalThis.threadedMeshDemo = state;

  globalThis.addEventListener("beforeunload", () => {
    if (state.statusTimer) {
      clearInterval(state.statusTimer);
    }
    if (state.refreshTimer) {
      clearTimeout(state.refreshTimer);
    }
    try {
      state.subscription?.cancel();
    } catch (error) {
      console.warn("subscription cleanup failed", error);
    }
    try {
      state.persistence?.close();
    } catch (error) {
      console.warn("persistence cleanup failed", error);
    }
    try {
      state.mesh?.close();
    } catch (error) {
      console.warn("mesh cleanup failed", error);
    }
  });
}

function createSessionConfig() {
  const params = new URLSearchParams(globalThis.location.search);
  const room = params.get("room")?.trim() || DEFAULT_MESH_ROOM;
  return {
    room,
    snapshotDb: `${DEFAULT_SNAPSHOT_DB}-${room}`,
  };
}

async function setupPersistence() {
  try {
    state.persistence = await state.db.enableIndexedDbPersistence(
      session.snapshotDb,
      SNAPSHOT_STORE,
      SNAPSHOT_KEY,
      true,
    );
    elements.persistenceStatus.textContent = "indexeddb";
  } catch (error) {
    console.warn("IndexedDB unavailable, falling back to localStorage", error);
    state.db.useBrowserStorage(`${session.snapshotDb}-fallback`);
    elements.persistenceStatus.textContent = "localStorage";
  }
}

function bindUi() {
  elements.noteForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const title = elements.noteTitle.value.trim();
    const body = elements.noteBody.value.trim();
    if (!title) {
      return;
    }

    const now = Date.now();
    state.listChain.set({
      title,
      body,
      done: false,
      archived: false,
      created_at: now,
      updated_at: now,
    });

    elements.noteForm.reset();
    await flushMesh("saved");
    updateSeedCount();
    await render();
  });

  elements.filterGroup.addEventListener("click", async (event) => {
    const button = event.target.closest("[data-filter]");
    if (!button) {
      return;
    }
    state.filter = button.dataset.filter;
    for (const candidate of elements.filterGroup.querySelectorAll("[data-filter]")) {
      candidate.classList.toggle("is-active", candidate === button);
    }
    await render();
  });

  elements.searchInput.addEventListener("input", async (event) => {
    state.search = event.target.value.trim();
    await render();
  });

  elements.seedButton.addEventListener("click", () => {
    void seedSharedLoad();
  });

  elements.queryButton.addEventListener("click", () => {
    runQuery();
  });
}

function startStatusLoop() {
  const update = () => {
    const peerCount = state.mesh ? state.mesh.peerCount() : 0;
    const openPeerCount = state.mesh ? state.mesh.openPeerCount() : 0;
    const inflight = state.mesh ? state.mesh.inflightCount() : 0;
    elements.peerCount.textContent = String(peerCount);
    elements.meshQueue.textContent = String(inflight);
    if (!state.mesh) {
      elements.meshDetail.textContent = "mesh unavailable";
    } else if (peerCount === 0) {
      elements.meshDetail.textContent = "waiting for another tab to join the room";
    } else if (openPeerCount === 0) {
      elements.meshDetail.textContent = "peer discovered, waiting for the WebRTC data channel to open";
    } else {
      elements.meshDetail.textContent = `connected to ${openPeerCount} peer${openPeerCount === 1 ? "" : "s"} over WebRTC`;
    }
  };

  update();
  state.statusTimer = setInterval(update, 1000);
}

async function ensureSeedNote() {
  const existing = state.listChain.query({
    filters: [{ kind: "eq", path: "archived", value: false }],
    limit: 1,
  });

  if (existing.length > 0) {
    return;
  }

  const now = Date.now();
  const noteId = seedNoteId();
  state.db.chain(noteId).put({
    title: "Open this page in a second tab",
    body: "This room uses WebRTC for direct peer-to-peer sync and wasm-threads for query work.",
    done: false,
    archived: false,
    created_at: now,
    updated_at: now,
  });
  state.listChain.set({ $link: noteId });
  await flushMesh("seeded");
}

async function seedSharedLoad() {
  if (!state.listChain) {
    return;
  }

  updateSeedCount();
  if (state.seeded >= SEEDED_NOTE_TOTAL) {
    elements.queryOutput.textContent = "Shared load already present. Running the threaded query again.";
    runQuery();
    return;
  }

  elements.seedButton.disabled = true;
  elements.queryButton.disabled = true;
  elements.queryOutput.textContent = `Seeding ${SEEDED_NOTE_TOTAL.toLocaleString()} shared notes...`;

  if (state.mesh && state.mesh.peerCount() > 0) {
    const deadline = Date.now() + 8_000;
    while (state.mesh.openPeerCount() === 0 && Date.now() < deadline) {
      elements.meshDetail.textContent = "waiting for the WebRTC data channel before seeding";
      await yieldToBrowser();
    }
  }

  const batchSize = 40;
  const baseTime = Date.now();
  for (let start = 0; start < SEEDED_NOTE_TOTAL; start += batchSize) {
    const end = Math.min(start + batchSize, SEEDED_NOTE_TOTAL);
    for (let index = start; index < end; index += 1) {
      state.listChain.set({
        title: `Mesh note ${String(index).padStart(4, "0")}`,
        body: `Generated workload item ${index}`,
        done: index % 5 === 0,
        archived: false,
        created_at: baseTime + index,
        updated_at: baseTime + index,
      });
    }
    state.mesh?.flushPending();
    elements.queryOutput.textContent = `Seeding ${end.toLocaleString()} / ${SEEDED_NOTE_TOTAL.toLocaleString()} shared notes...`;
    await yieldToBrowser();
  }

  await flushMesh("seeded workload");
  updateSeedCount();
  elements.seedButton.disabled = false;
  elements.queryButton.disabled = false;
  await render();
  runQuery();
}

async function render() {
  const query = {
    filters: [{ kind: "eq", path: "archived", value: false }],
    order: { path: "created_at", direction: "desc" },
    limit: 250,
  };

  if (state.filter === "open") {
    query.filters.push({ kind: "eq", path: "done", value: false });
  } else if (state.filter === "done") {
    query.filters.push({ kind: "eq", path: "done", value: true });
  }

  let entries = state.listChain.query(query);
  if (state.search) {
    const lowered = state.search.toLowerCase();
    entries = entries.filter((entry) => {
      const title = `${entry.value.title ?? ""}`.toLowerCase();
      const body = `${entry.value.body ?? ""}`.toLowerCase();
      return title.includes(lowered) || body.includes(lowered);
    });
  }

  elements.noteList.replaceChildren();
  elements.emptyState.hidden = entries.length > 0;
  for (const entry of entries) {
    elements.noteList.append(renderNote(entry));
  }
}

function scheduleUiRefresh() {
  state.refreshQueued = true;
  if (state.refreshRunning || state.refreshTimer) {
    return;
  }

  state.refreshTimer = setTimeout(() => {
    state.refreshTimer = null;
    void flushUiRefresh();
  }, 40);
}

async function flushUiRefresh() {
  if (state.refreshRunning) {
    return;
  }

  state.refreshRunning = true;
  try {
    while (state.refreshQueued) {
      state.refreshQueued = false;
      updateSeedCount();
      await render();
      runQuery();
      await yieldToBrowser();
    }
  } finally {
    state.refreshRunning = false;
    if (state.refreshQueued && !state.refreshTimer) {
      scheduleUiRefresh();
    }
  }
}

function renderNote(entry) {
  const fragment = elements.noteTemplate.content.cloneNode(true);
  const item = fragment.querySelector(".note-card");
  const title = fragment.querySelector(".note-title");
  const body = fragment.querySelector(".note-body");
  const timestamp = fragment.querySelector(".note-timestamp");
  const toggleButton = fragment.querySelector(".toggle-button");
  const archiveButton = fragment.querySelector(".archive-button");

  const note = entry.value;
  const noteId = note.$id ?? entry.key;
  const done = Boolean(note.done);

  item.classList.toggle("is-done", done);
  title.textContent = note.title ?? "Untitled";
  body.textContent = note.body?.trim() ? note.body : "No body";
  timestamp.textContent = formatTimestamp(note.updated_at ?? note.created_at);

  toggleButton.addEventListener("click", async () => {
    updateNote(noteId, {
      ...note,
      done: !done,
      archived: false,
      updated_at: Date.now(),
    });
    await flushMesh(done ? "reopened" : "completed");
    await render();
  });

  archiveButton.addEventListener("click", async () => {
    state.listChain.remove({ $link: noteId });
    await flushMesh("archived");
    updateSeedCount();
    await render();
  });

  return fragment;
}

function updateNote(noteId, note) {
  state.db.chain(noteId).put({
    title: note.title ?? "",
    body: note.body ?? "",
    done: Boolean(note.done),
    archived: Boolean(note.archived),
    created_at: note.created_at ?? Date.now(),
    updated_at: note.updated_at ?? Date.now(),
  });
}

async function flushMesh(action) {
  await state.persistence?.flush?.();
  state.mesh?.flushPending();
  elements.meshDetail.textContent = `${action} locally`;
}

function updateSeedCount() {
  const entries = state.listChain.query({
    filters: [{ kind: "eq", path: "archived", value: false }],
    limit: 5000,
  });
  state.seeded = entries.length;
  elements.seedCount.textContent = String(state.seeded);
}

function runQuery() {
  if (!state.db) {
    return;
  }

  const start = performance.now();
  const entries = state.listChain.query({
    filters: [
      { kind: "eq", path: "archived", value: false },
      { kind: "gte", path: "created_at", value: 1 },
    ],
    order: { path: "title", direction: "asc" },
    limit: 320,
  });
  const elapsed = performance.now() - start;

  elements.queryTiming.textContent = `${elapsed.toFixed(1)} ms`;
  elements.queryOutput.textContent = JSON.stringify(
    {
      parallelEnabled: primadb.parallelEnabled(),
      parallelThreadCount: primadb.parallelThreadCount(),
      peers: state.mesh?.peerCount() ?? 0,
      openPeers: state.mesh?.openPeerCount?.() ?? 0,
      seeded: state.seeded,
      queryMatches: entries.length,
      firstKey: entries[0]?.key ?? null,
      lastKey: entries.at(-1)?.key ?? null,
    },
    null,
    2,
  );
}

function formatTimestamp(value) {
  if (!value) {
    return "unknown";
  }
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

function seedNoteId() {
  return `threaded-mesh-room/${session.room}/welcome-note`;
}

function yieldToBrowser() {
  return new Promise((resolve) => {
    let settled = false;
    const finish = () => {
      if (settled) {
        return;
      }
      settled = true;
      resolve();
    };

    const timeoutId = setTimeout(finish, 16);
    if (typeof requestAnimationFrame === "function" && document.visibilityState === "visible") {
      requestAnimationFrame(() => {
        clearTimeout(timeoutId);
        finish();
      });
    }
  });
}
