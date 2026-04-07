import init, { Primadb } from "./pkg/primadb.js";

const SNAPSHOT_DB = "primadb-relay-notes";
const SNAPSHOT_STORE = "snapshots";
const SNAPSHOT_KEY = "main";

const state = {
  db: null,
  listChain: null,
  subscription: null,
  persistence: null,
  relay: null,
  filter: "all",
  search: "",
};

const elements = {
  replicaId: document.getElementById("replica-id"),
  persistenceStatus: document.getElementById("persistence-status"),
  relayStatus: document.getElementById("relay-status"),
  inflightCount: document.getElementById("inflight-count"),
  relayUrl: document.getElementById("relay-url"),
  connectButton: document.getElementById("connect-button"),
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
  elements.relayStatus.textContent = "failed";
});

async function main() {
  await init();

  const replicaId = `relay-${globalThis.crypto?.randomUUID?.() ?? Date.now()}`;
  const db = new Primadb(replicaId);
  state.db = db;
  state.listChain = db.chain("boards").field("shared").field("notes");
  elements.replicaId.textContent = replicaId;

  await setupPersistence();
  bindUi();

  state.subscription = state.listChain.on(() => {
    render().catch((error) => console.error(error));
  });

  await ensureSeedNote();
  await render();
  updateRelayStatus();

  globalThis.addEventListener("beforeunload", () => {
    state.subscription?.cancel();
    state.persistence?.close();
    state.relay?.close();
  });
}

async function setupPersistence() {
  try {
    state.persistence = await state.db.enableIndexedDbPersistence(
      SNAPSHOT_DB,
      SNAPSHOT_STORE,
      SNAPSHOT_KEY,
      true,
    );
    elements.persistenceStatus.textContent = "indexeddb";
  } catch (error) {
    console.warn("IndexedDB unavailable, falling back to localStorage", error);
    state.db.useBrowserStorage("primadb-relay-notes-fallback");
    elements.persistenceStatus.textContent = "localStorage";
  }
}

function bindUi() {
  elements.connectButton.addEventListener("click", connectRelay);

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
    await flushPersistence();
    flushRelay();
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

async function connectRelay() {
  try {
    state.relay?.close();
  } catch (error) {
    console.warn("closing previous relay failed", error);
  }

  state.relay = state.db.connectWebSocket(elements.relayUrl.value.trim(), 1500);
  updateRelayStatus();
  flushRelay();
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
  state.listChain.set({
    title: "Connect a second browser to the relay",
    body: "Run cargo run --example ws_relay_server and open this page twice.",
    done: false,
    archived: false,
    created_at: now,
    updated_at: now,
  });

  await flushPersistence();
}

async function render() {
  const query = {
    filters: [{ kind: "eq", path: "archived", value: false }],
    order: { path: "created_at", direction: "desc" },
    limit: 200,
  };

  if (state.filter === "active") {
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

  elements.inflightCount.textContent = state.relay ? String(state.relay.inflightCount()) : "0";
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
  const updated = fragment.querySelector(".note-updated");
  const toggleButton = fragment.querySelector(".toggle-button");
  const archiveButton = fragment.querySelector(".archive-button");

  const note = entry.value;
  const noteId = note.$id ?? entry.key;
  const done = Boolean(note.done);

  item.classList.toggle("is-done", done);
  title.textContent = note.title ?? "Untitled";
  body.textContent = note.body?.trim() ? note.body : "No body";
  updated.textContent = formatTimestamp(note.updated_at ?? note.created_at);
  toggleButton.title = done ? "Mark active" : "Mark done";

  toggleButton.addEventListener("click", async () => {
    updateNote(noteId, {
      ...note,
      done: !done,
      archived: false,
      updated_at: Date.now(),
    });
    await flushPersistence();
    flushRelay();
    await render();
  });

  archiveButton.addEventListener("click", async () => {
    updateNote(noteId, {
      ...note,
      archived: true,
      updated_at: Date.now(),
    });
    await flushPersistence();
    flushRelay();
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

async function flushPersistence() {
  if (state.persistence) {
    await state.persistence.flush();
  }
}

function flushRelay() {
  if (!state.relay) {
    updateRelayStatus();
    return;
  }
  try {
    state.relay.flushPending();
  } catch (error) {
    console.error("relay flush failed", error);
  }
  updateRelayStatus();
}

function updateRelayStatus() {
  if (!state.relay) {
    elements.relayStatus.textContent = "not connected";
    elements.inflightCount.textContent = "0";
    return;
  }

  const readyState = state.relay.readyState();
  const label =
    readyState === WebSocket.CONNECTING
      ? "connecting"
      : readyState === WebSocket.OPEN
        ? "connected"
        : readyState === WebSocket.CLOSING
          ? "closing"
          : "closed";

  elements.relayStatus.textContent = `${label} / pending ${state.relay.pendingCount()}`;
  elements.inflightCount.textContent = String(state.relay.inflightCount());
}

function formatTimestamp(value) {
  if (!value) {
    return "updated just now";
  }
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}
