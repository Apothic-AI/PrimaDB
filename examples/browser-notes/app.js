import init, { Primadb } from "./pkg/primadb.js";

const SYNC_CHANNEL = "primadb-browser-notes-sync";
const SNAPSHOT_DB = "primadb-browser-notes";
const SNAPSHOT_STORE = "snapshots";
const SNAPSHOT_KEY = "main";

const state = {
  db: null,
  listChain: null,
  subscription: null,
  persistence: null,
  channel: null,
  filter: "all",
  search: "",
  lastSyncLabel: "idle",
  persistenceLabel: "starting",
};

const elements = {
  replicaId: document.getElementById("replica-id"),
  persistenceStatus: document.getElementById("persistence-status"),
  syncStatus: document.getElementById("sync-status"),
  taskCount: document.getElementById("task-count"),
  taskForm: document.getElementById("task-form"),
  taskTitle: document.getElementById("task-title"),
  taskNote: document.getElementById("task-note"),
  filterGroup: document.getElementById("filter-group"),
  searchInput: document.getElementById("search-input"),
  emptyState: document.getElementById("empty-state"),
  taskList: document.getElementById("task-list"),
  taskTemplate: document.getElementById("task-template"),
};

main().catch((error) => {
  console.error(error);
  elements.syncStatus.textContent = "failed";
});

async function main() {
  await init();

  const replicaId = `browser-${globalThis.crypto?.randomUUID?.() ?? Date.now()}`;
  const db = new Primadb(replicaId);
  state.db = db;
  state.listChain = db.chain("lists").field("main").field("items");
  elements.replicaId.textContent = replicaId;

  await setupPersistence();
  setupBroadcastSync();
  bindUi();

  state.subscription = state.listChain.on(() => {
    render().catch((error) => console.error(error));
  });

  await ensureSeedTask();
  await render();
}

async function setupPersistence() {
  try {
    state.persistence = await state.db.enableIndexedDbPersistence(
      SNAPSHOT_DB,
      SNAPSHOT_STORE,
      SNAPSHOT_KEY,
      true,
    );
    state.persistenceLabel = "indexeddb";
  } catch (error) {
    console.warn("IndexedDB unavailable, falling back to localStorage", error);
    state.db.useBrowserStorage("primadb-browser-notes-fallback");
    state.persistenceLabel = "localStorage";
  }
  elements.persistenceStatus.textContent = state.persistenceLabel;
}

function setupBroadcastSync() {
  const channel = new BroadcastChannel(SYNC_CHANNEL);
  channel.onmessage = async (event) => {
    const { from, payload } = event.data ?? {};
    if (!payload || from === state.db.replicaId()) {
      return;
    }
    try {
      const applied = state.db.applyOperationsJson(payload);
      if (applied > 0) {
        state.lastSyncLabel = `received from ${from}`;
        elements.syncStatus.textContent = state.lastSyncLabel;
        await render();
      }
    } catch (error) {
      console.error("Failed to apply broadcast payload", error);
    }
  };
  state.channel = channel;

  globalThis.addEventListener("beforeunload", () => {
    state.subscription?.cancel();
    state.persistence?.close();
    channel.close();
  });
}

function bindUi() {
  elements.taskForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const title = elements.taskTitle.value.trim();
    const note = elements.taskNote.value.trim();
    if (!title) {
      return;
    }

    const now = Date.now();
    state.listChain.set({
      title,
      note,
      done: false,
      archived: false,
      created_at: now,
      updated_at: now,
    });

    elements.taskForm.reset();
    await flushLocalOps("saved");
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

async function ensureSeedTask() {
  const existing = state.listChain.query({
    filters: [{ kind: "eq", path: "archived", value: false }],
    limit: 1,
  });

  if (existing.length > 0) {
    return;
  }

  const now = Date.now();
  state.listChain.set({
    title: "Open this page in a second tab",
    note: "The example uses BroadcastChannel for instant sync and IndexedDB for persistence.",
    done: false,
    archived: false,
    created_at: now,
    updated_at: now,
  });
  await flushLocalOps("seeded");
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

  if (state.search) {
    query.filters.push({ kind: "contains", path: "title", value: state.search });
  }

  let entries = state.listChain.query(query);
  if (state.search) {
    const lowered = state.search.toLowerCase();
    entries = entries.filter((entry) => {
      const note = `${entry.value.note ?? ""}`.toLowerCase();
      return note.includes(lowered) || `${entry.value.title ?? ""}`.toLowerCase().includes(lowered);
    });
  }

  elements.taskCount.textContent = String(entries.length);
  elements.syncStatus.textContent = state.lastSyncLabel;
  elements.taskList.replaceChildren();
  elements.emptyState.hidden = entries.length > 0;

  for (const entry of entries) {
    elements.taskList.append(renderTask(entry));
  }
}

function renderTask(entry) {
  const fragment = elements.taskTemplate.content.cloneNode(true);
  const item = fragment.querySelector(".task-card");
  const title = fragment.querySelector(".task-title");
  const note = fragment.querySelector(".task-note");
  const timestamp = fragment.querySelector(".task-timestamp");
  const toggleButton = fragment.querySelector(".toggle-button");
  const archiveButton = fragment.querySelector(".archive-button");

  const task = entry.value;
  const taskId = task.$id ?? entry.key;
  const done = Boolean(task.done);

  item.dataset.taskId = taskId;
  item.classList.toggle("is-done", done);
  title.textContent = task.title ?? "Untitled";
  note.textContent = task.note?.trim() ? task.note : "No note";
  timestamp.textContent = formatTimestamp(task.updated_at ?? task.created_at);
  toggleButton.title = done ? "Mark as open" : "Mark as done";
  archiveButton.title = "Archive task";

  toggleButton.addEventListener("click", async () => {
    updateTask(taskId, {
      ...task,
      done: !done,
      archived: false,
      updated_at: Date.now(),
    });
    await flushLocalOps(done ? "reopened" : "completed");
    await render();
  });

  archiveButton.addEventListener("click", async () => {
    updateTask(taskId, {
      ...task,
      archived: true,
      updated_at: Date.now(),
    });
    await flushLocalOps("archived");
    await render();
  });

  return fragment;
}

function updateTask(taskId, task) {
  state.db.chain(taskId).put({
    title: task.title ?? "",
    note: task.note ?? "",
    done: Boolean(task.done),
    archived: Boolean(task.archived),
    created_at: task.created_at ?? Date.now(),
    updated_at: task.updated_at ?? Date.now(),
  });
}

async function flushLocalOps(label) {
  const envelope = state.db.drainPendingEnvelope();
  if (!envelope.ops.length) {
    return;
  }

  state.lastSyncLabel = `${label} locally`;
  elements.syncStatus.textContent = state.lastSyncLabel;
  state.channel.postMessage({
    from: state.db.replicaId(),
    payload: JSON.stringify(envelope),
  });

  if (state.persistence) {
    await state.persistence.flush();
  }
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
