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
  statusTimer: null,
  filter: "all",
  search: "",
};

const elements = {
  replicaId: document.getElementById("replica-id"),
  persistenceStatus: document.getElementById("persistence-status"),
  relayStatus: document.getElementById("relay-status"),
  relayDetail: document.getElementById("relay-detail"),
  pendingCount: document.getElementById("pending-count"),
  inflightCount: document.getElementById("inflight-count"),
  peerCount: document.getElementById("peer-count"),
  peerDetail: document.getElementById("peer-detail"),
  relayUrl: document.getElementById("relay-url"),
  connectButton: document.getElementById("connect-button"),
  seedBulkButton: document.getElementById("seed-bulk-button"),
  probeRemoteButton: document.getElementById("probe-remote-button"),
  remoteResult: document.getElementById("remote-result"),
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
  startStatusLoop();
  await connectRelay();
  globalThis.primadbRelayDemo = state;

  globalThis.addEventListener("beforeunload", () => {
    if (state.statusTimer) {
      clearInterval(state.statusTimer);
    }
    try {
      state.subscription?.cancel();
    } catch (error) {
      console.warn("subscription teardown failed", error);
    }
    try {
      state.persistence?.close();
    } catch (error) {
      console.warn("persistence teardown failed", error);
    }
    try {
      state.relay?.close();
    } catch (error) {
      console.warn("relay teardown failed", error);
    }
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
  elements.seedBulkButton.addEventListener("click", seedBulkNotes);
  elements.probeRemoteButton.addEventListener("click", probeRemotePeer);

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
  elements.connectButton.disabled = true;
  try {
    state.relay?.close();
  } catch (error) {
    console.warn("closing previous relay failed", error);
  }

  try {
    state.relay = state.db.connectWebSocket(elements.relayUrl.value.trim(), 1500);
    updateRelayStatus();
    flushRelay();
  } catch (error) {
    console.error("relay connect failed", error);
    state.relay = null;
    updateRelayStatus();
  } finally {
    elements.connectButton.disabled = false;
  }
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

async function seedBulkNotes() {
  const start = Date.now();
  for (let index = 0; index < 90; index += 1) {
    state.listChain.set({
      title: `Chunk note ${index + 1}`,
      body: `Remote query and snapshot chunk proof ${index + 1}`,
      done: index % 3 === 0,
      archived: false,
      created_at: start + index,
      updated_at: start + index,
    });
  }
  await flushPersistence();
  flushRelay();
  await render();
  elements.remoteResult.textContent =
    "Seeded 90 local notes. Open a second client and use “Probe remote peer” to force chunked query and snapshot replies.";
}

async function probeRemotePeer() {
  if (!state.relay) {
    elements.remoteResult.textContent = "Connect to the relay first.";
    return;
  }

  const recommendations = state.relay.recommendedPeers();
  const target =
    recommendations.find(
      (candidate) =>
        candidate?.peer?.capabilities?.includes("pull_query") &&
        candidate?.peer?.topics?.includes("primadb-sync"),
    ) ?? recommendations.find((candidate) => candidate?.peer?.peer_id);
  if (!target?.peer?.peer_id) {
    elements.remoteResult.textContent =
      "No recommended peer is available yet. Open a second client on the same relay first.";
    return;
  }

  const path = { anchor: "boards", segments: ["shared", "notes"] };

  try {
    const remoteValue = await state.relay.remoteGet(target.peer.peer_id, path);
    const remoteQuery = await state.relay.remoteQuery(target.peer.peer_id, path, {
      filters: [{ kind: "eq", path: "archived", value: false }],
      order: { path: "created_at", direction: "desc" },
      limit: 200,
    });
    const remoteLex = await state.relay.remoteLex(target.peer.peer_id, path, {
      follow_links: true,
      depth: 2,
      limit: 200,
    });
    const remoteSnapshot = await state.relay.remoteSnapshot(target.peer.peer_id, "boards");

    elements.remoteResult.textContent = JSON.stringify(
      {
        target_peer: target.peer.peer_id,
        target_replica: target.peer.replica_id,
        relay_urls: target.relay_urls ?? [],
        remote_get_items: Array.isArray(remoteValue?.$set) ? remoteValue.$set.length : null,
        remote_query_count: Array.isArray(remoteQuery) ? remoteQuery.length : 0,
        remote_lex_count: Array.isArray(remoteLex) ? remoteLex.length : 0,
        remote_snapshot_nodes: Object.keys(remoteSnapshot?.nodes ?? {}).length,
      },
      null,
      2,
    );
  } catch (error) {
    console.error("remote probe failed", error);
    elements.remoteResult.textContent = `Remote probe failed: ${error}`;
  }
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
    state.listChain.remove({ $link: noteId });
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

function startStatusLoop() {
  if (state.statusTimer) {
    clearInterval(state.statusTimer);
  }
  state.statusTimer = setInterval(() => {
    updateRelayStatus();
  }, 500);
}

function updateRelayStatus() {
  if (!state.relay) {
    elements.relayStatus.textContent = "offline";
    elements.relayDetail.textContent = "Not connected to the relay. Local changes stay in this browser until you connect.";
    elements.pendingCount.textContent = String(state.db ? state.db.pendingOperations().length : 0);
    elements.inflightCount.textContent = "0";
    elements.peerCount.textContent = "0";
    elements.peerDetail.textContent = "Connect a second client to discover peers.";
    elements.connectButton.textContent = "Connect";
    elements.connectButton.classList.remove("is-live");
    return;
  }

  const readyState = state.relay.readyState();
  const pending = state.relay.pendingCount();
  const inflight = state.relay.inflightCount();
  const recommendations = state.relay.recommendedPeers();

  if (readyState === WebSocket.CONNECTING) {
    elements.relayStatus.textContent = "connecting";
    elements.relayDetail.textContent =
      "The browser has opened a WebSocket and is waiting for the relay handshake to finish.";
    elements.connectButton.textContent = "Connecting...";
    elements.connectButton.classList.remove("is-live");
  } else if (readyState === WebSocket.OPEN) {
    elements.relayStatus.textContent = "connected";
    if (inflight > 0) {
      elements.relayDetail.textContent =
        inflight === 1
          ? "1 message has been sent to the relay and is waiting for another client to acknowledge it."
          : `${inflight} messages have been sent to the relay and are waiting for peer acknowledgments.`;
    } else if (pending > 0) {
      elements.relayDetail.textContent =
        pending === 1
          ? "1 local change is queued to be sent."
          : `${pending} local changes are queued to be sent.`;
    } else {
      elements.relayDetail.textContent = "Relay link is live and there are no unsynced changes right now.";
    }
    elements.connectButton.textContent = "Reconnect";
    elements.connectButton.classList.add("is-live");
  } else if (readyState === WebSocket.CLOSING) {
    elements.relayStatus.textContent = "closing";
    elements.relayDetail.textContent = "The relay connection is shutting down.";
    elements.connectButton.textContent = "Reconnect";
    elements.connectButton.classList.remove("is-live");
  } else {
    elements.relayStatus.textContent = "closed";
    elements.relayDetail.textContent =
      "The relay socket is closed. Any unsynced changes stay local and will send again after reconnecting.";
    elements.connectButton.textContent = "Reconnect";
    elements.connectButton.classList.remove("is-live");
  }

  elements.pendingCount.textContent = String(pending);
  elements.inflightCount.textContent = String(state.relay.inflightCount());
  elements.peerCount.textContent = String(recommendations.length);
  elements.peerDetail.textContent =
    recommendations.length > 0
      ? `Best match: ${(recommendations.find(
          (candidate) =>
            candidate?.peer?.capabilities?.includes("pull_query") &&
            candidate?.peer?.topics?.includes("primadb-sync"),
        ) ?? recommendations[0]).peer.peer_id} over ${(recommendations.find(
          (candidate) =>
            candidate?.peer?.capabilities?.includes("pull_query") &&
            candidate?.peer?.topics?.includes("primadb-sync"),
        ) ?? recommendations[0]).peer.transport}.`
      : "Connect a second client to discover peers.";
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
