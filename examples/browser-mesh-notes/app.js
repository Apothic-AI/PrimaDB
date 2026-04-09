import init, { Primadb } from "./pkg/primadb.js";

const SNAPSHOT_DB = "primadb-browser-mesh-notes";
const SNAPSHOT_STORE = "snapshots";
const SNAPSHOT_KEY = "main";
const DEFAULT_ROOM = "primadb-browser-mesh-notes";

const session = createSessionConfig();

const state = {
  db: null,
  listChain: null,
  subscription: null,
  persistence: null,
  mesh: null,
  statusTimer: null,
  filter: "all",
  search: "",
};

const elements = {
  replicaId: document.getElementById("replica-id"),
  persistenceStatus: document.getElementById("persistence-status"),
  peerCount: document.getElementById("peer-count"),
  meshQueue: document.getElementById("mesh-queue"),
  meshDetail: document.getElementById("mesh-detail"),
  noteForm: document.getElementById("note-form"),
  noteTitle: document.getElementById("note-title"),
  noteBody: document.getElementById("note-body"),
  filterGroup: document.getElementById("filter-group"),
  searchInput: document.getElementById("search-input"),
  emptyState: document.getElementById("empty-state"),
  noteList: document.getElementById("note-list"),
  noteTemplate: document.getElementById("note-template"),
};

main().catch((error) => {
  console.error(error);
  elements.meshDetail.textContent = "mesh startup failed";
});

async function main() {
  await init();

  const replicaId = `mesh-${globalThis.crypto?.randomUUID?.() ?? Date.now()}`;
  const db = new Primadb(replicaId);
  state.db = db;
  state.listChain = db.chain("boards").field("mesh").field("notes");
  elements.replicaId.textContent = replicaId;

  await setupPersistence();
  state.mesh = db.connectWebRtcMesh(session.room, 1500);
  bindUi();
  startStatusLoop();

  state.subscription = state.listChain.on(() => {
    render().catch((error) => console.error(error));
  });

  await ensureSeedNote();
  await render();

  globalThis.addEventListener("beforeunload", () => {
    if (state.statusTimer) {
      clearInterval(state.statusTimer);
    }
    state.subscription?.cancel();
    state.persistence?.close();
    state.mesh?.close();
  });
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

function createSessionConfig() {
  const params = new URLSearchParams(globalThis.location.search);
  const room = params.get("room")?.trim() || DEFAULT_ROOM;
  return {
    room,
    snapshotDb: `${SNAPSHOT_DB}-${room}`,
  };
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
}

function startStatusLoop() {
  const update = () => {
    const peerCount = state.mesh ? state.mesh.peerCount() : 0;
    const inflight = state.mesh ? state.mesh.inflightCount() : 0;
    elements.peerCount.textContent = String(peerCount);
    elements.meshQueue.textContent = String(inflight);
    if (!state.mesh) {
      elements.meshDetail.textContent = "mesh unavailable";
    } else if (peerCount === 0) {
      elements.meshDetail.textContent = "waiting for another tab to join the room";
    } else {
      elements.meshDetail.textContent = `connected to ${peerCount} peer${peerCount === 1 ? "" : "s"} over WebRTC`;
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
    body: "Primadb will discover the peer through BroadcastChannel and sync directly over WebRTC.",
    done: false,
    archived: false,
    created_at: now,
    updated_at: now,
  });
  state.listChain.set({ $link: noteId });
  await flushMesh("seeded");
}

async function render() {
  const query = {
    filters: [{ kind: "eq", path: "archived", value: false }],
    order: { path: "created_at", direction: "desc" },
    limit: 200,
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
  return `mesh-room/${session.room}/welcome-note`;
}
