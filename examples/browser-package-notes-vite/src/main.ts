import "./styles.css";
import { Primadb, initPrimadb } from "primadb";

type NoteRecord = {
  title?: string;
  body?: string;
  createdAt?: string;
  created_at?: number | string;
  updated_at?: number | string;
  done?: boolean;
  archived?: boolean;
  $id?: string;
};

type QueryEntry = {
  key: string;
  value: NoteRecord;
};

const app = {
  replica: document.querySelector<HTMLElement>("#replica-id")!,
  storage: document.querySelector<HTMLElement>("#storage-backend")!,
  signaling: document.querySelector<HTMLElement>("#mesh-signaling")!,
  relay: document.querySelector<HTMLElement>("#mesh-relay")!,
  peers: document.querySelector<HTMLElement>("#mesh-peers")!,
  count: document.querySelector<HTMLElement>("#note-count")!,
  status: document.querySelector<HTMLElement>("#status-line")!,
  meshDetail: document.querySelector<HTMLElement>("#mesh-detail")!,
  list: document.querySelector<HTMLUListElement>("#notes-list")!,
  form: document.querySelector<HTMLFormElement>("#note-form")!,
  title: document.querySelector<HTMLInputElement>("#note-title")!,
  body: document.querySelector<HTMLTextAreaElement>("#note-body")!,
};

const session = createSession();

const replicaId =
  globalThis.crypto?.randomUUID?.() != null
    ? `vite-${globalThis.crypto.randomUUID().slice(0, 8)}`
    : `vite-${Date.now().toString(36)}`;

await initPrimadb();

const db = new Primadb(replicaId);
const durable = await db.openDurableStorage({
  kind: "indexed_db_segments",
  databaseName: `primadb-package-vite-${session.room}`,
  storeName: "segments",
  namespace: session.room,
  loadExisting: true,
  autoPersist: true,
});
const notes = session.meshEnabled
  ? db.chain("boards").field(session.room).field("notes")
  : db.chain("package-demo").field("notes");
const mesh = session.meshEnabled
  ? db.connectMesh({
      room: session.room,
      signaling: session.signal === "broadcast" ? "broadcast_channel" : "relay",
      relayUrl: session.signal === "broadcast" ? undefined : session.relayUrl,
      retryIntervalMs: 1500,
    })
  : null;

app.replica.textContent = db.replicaId();
app.storage.textContent = `${durable.backend} / incremental=${durable.incremental}`;
startMeshStatusLoop();

function uniqueTitle(title: string) {
  if (session.exactTitle) {
    return title.trim();
  }
  return `${title.trim()} ${new Date().toISOString().slice(11, 19)}`;
}

function escapeHtml(value: string) {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}

function renderNotes() {
  const entries = notes.query({
    order: {
      path: "updated_at",
      direction: "desc",
    },
  }) as QueryEntry[];

  app.count.textContent = String(entries.length);
  app.status.textContent = entries.length
    ? `${entries.length} note${entries.length === 1 ? "" : "s"} stored through the npm package`
    : "No notes yet. Add one below.";

  app.list.innerHTML = entries
    .map(({ key, value }) => {
      const title = escapeHtml(value.title ?? key);
      const body = escapeHtml(value.body ?? "");
      const createdAt = escapeHtml(formatCreatedAt(value));
      return `
        <li class="note-card" data-note-key="${escapeHtml(key)}" data-note-title="${title}">
          <div class="note-meta">${createdAt}</div>
          <h3>${title}</h3>
          <p>${body}</p>
        </li>
      `;
    })
    .join("");
}

notes.on(() => {
  renderNotes();
});

renderNotes();

app.form.addEventListener("submit", (event) => {
  event.preventDefault();

  const title = uniqueTitle(app.title.value);
  const body = app.body.value.trim();
  if (!title || !body) {
    return;
  }

  notes.set({
    title,
    body,
    done: false,
    archived: false,
    created_at: Date.now(),
    updated_at: Date.now(),
  });
  renderNotes();
  mesh?.flushPending();

  app.form.reset();
  app.title.focus();
});

globalThis.addEventListener("beforeunload", () => {
  try {
    mesh?.close();
  } catch (_error) {}
});

function createSession() {
  const params = new URLSearchParams(globalThis.location.search);
  const room = params.get("room")?.trim() || "package-demo";
  const signal = (params.get("signal")?.trim() || "relay").toLowerCase();
  const protocol = globalThis.location.protocol === "https:" ? "wss" : "ws";
  const relayUrl =
    params.get("relay")?.trim() || `${protocol}://${globalThis.location.hostname}:9010`;
  const meshEnabled = params.has("room") || params.has("relay") || params.has("signal");
  return {
    room,
    signal: signal === "broadcast" ? "broadcast" : "relay",
    relayUrl,
    meshEnabled,
    exactTitle: params.get("exactTitle") === "1",
  };
}

function formatCreatedAt(value: NoteRecord) {
  const candidate =
    value.updated_at ??
    value.created_at ??
    value.createdAt;
  if (candidate == null) {
    return "";
  }
  const date =
    typeof candidate === "number"
      ? new Date(candidate)
      : new Date(candidate);
  return Number.isNaN(date.getTime()) ? String(candidate) : date.toISOString();
}

function startMeshStatusLoop() {
  if (!mesh) {
    app.signaling.textContent = "disabled";
    app.relay.textContent = "disabled";
    app.meshDetail.textContent = "Mesh disabled. Add ?room=... to join a shared room.";
    return;
  }

  const update = () => {
    const signalingMode = mesh.signalingMode().replace("_", " ");
    const readyState = mesh.signalingReadyState();
    const peerCount = mesh.peerCount();
    const openPeerCount = mesh.openPeerCount();
    app.signaling.textContent = signalingMode;
    app.peers.textContent = String(peerCount);
    app.relay.textContent =
      readyState == null
        ? "local"
        : readyState === WebSocket.OPEN
          ? "connected"
          : readyState === WebSocket.CONNECTING
            ? "connecting"
            : readyState === WebSocket.CLOSING
              ? "closing"
              : "closed";

    if (peerCount === 0 && signalingMode.includes("relay")) {
      app.meshDetail.textContent =
        readyState === WebSocket.OPEN
          ? `waiting for another peer via ${session.relayUrl}`
          : `connecting to signaling relay ${session.relayUrl}`;
    } else if (peerCount === 0) {
      app.meshDetail.textContent = "waiting for another browser context to join the room";
    } else if (openPeerCount === 0) {
      app.meshDetail.textContent = "peer discovered, waiting for the WebRTC data channel";
    } else {
      app.meshDetail.textContent = `connected to ${openPeerCount} peer${openPeerCount === 1 ? "" : "s"} over WebRTC`;
    }
  };

  update();
  globalThis.setInterval(update, 1000);
}
