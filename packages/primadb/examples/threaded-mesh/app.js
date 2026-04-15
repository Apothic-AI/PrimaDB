import {
  createThreadedPrimadb,
  parallelEnabled,
  parallelThreadCount,
} from "../../dist/threads.js";

const dom = {
  buildStatus: document.querySelector("#build-status"),
  roomStatus: document.querySelector("#room-status"),
  meshStatus: document.querySelector("#mesh-status"),
  peerStatus: document.querySelector("#peer-status"),
  cardCount: document.querySelector("#card-count"),
  cardsList: document.querySelector("#cards-list"),
  form: document.querySelector("#card-form"),
  title: document.querySelector("#card-title"),
  body: document.querySelector("#card-body"),
};

const params = new URLSearchParams(globalThis.location.search);

function parseIceServerSpec(spec) {
  const trimmed = String(spec).trim();
  if (trimmed.startsWith("{")) {
    const value = JSON.parse(trimmed);
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      throw new Error("ice query parameter JSON must decode to an object");
    }
    return value;
  }
  if (trimmed.startsWith("stun:") || trimmed.startsWith("turn:") || trimmed.startsWith("turns:")) {
    return { urls: trimmed };
  }
  throw new Error(`invalid ice query parameter \`${trimmed}\`; use a STUN/TURN URL or JSON object`);
}

function defaultExampleIceServers() {
  return [{ urls: "stun:stun.cloudflare.com:3478" }];
}

const session = {
  room: params.get("room") || "package-threaded-mesh",
  relayUrl: params.get("relay") || "ws://127.0.0.1:9010",
  signal: params.get("signal") === "relay" ? "relay" : "broadcast_channel",
  threads: Math.max(2, Number.parseInt(params.get("threads") || "", 10) || 4),
  iceServers: (() => {
    const parsed = params.getAll("ice").map(parseIceServerSpec);
    return parsed.length > 0 ? parsed : defaultExampleIceServers();
  })(),
  replicaId: globalThis.crypto?.randomUUID
    ? `pkg-threaded-${globalThis.crypto.randomUUID().slice(0, 8)}`
    : `pkg-threaded-${Date.now().toString(36)}`,
};

const db = await createThreadedPrimadb(session.replicaId, { threads: session.threads });
await db.openDurableStorage({
  kind: "indexed_db_segments",
  databaseName: `primadb-package-threaded-${session.room}`,
  storeName: "segments",
  namespace: session.room,
  loadExisting: true,
  autoPersist: true,
});

const cards = db.chain("package_examples").field("threaded_mesh").field(session.room).field("cards");
const mesh = db.connectMesh({
  room: session.room,
  signaling: session.signal,
  relayUrl: session.signal === "relay" ? session.relayUrl : undefined,
  retryIntervalMs: 1500,
  iceServers: session.iceServers,
});

dom.buildStatus.textContent = `${parallelEnabled() ? "wasm-threads" : "single-thread"} / ${parallelThreadCount()} workers`;
dom.roomStatus.textContent = `${session.room} / signaling=${session.signal}`;

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}

function renderCards() {
  const entries = cards.query({
    order: {
      path: "updated_at",
      direction: "desc",
    },
  });

  dom.cardCount.textContent = String(entries.length);
  dom.cardsList.innerHTML = entries
    .map(({ key, value }) => {
      const title = escapeHtml(value.title ?? key);
      const body = escapeHtml(value.body ?? "");
      const stamp = escapeHtml(new Date(value.updated_at ?? Date.now()).toLocaleString());
      const author = escapeHtml(value.author ?? "unknown");
      return `
        <li class="card">
          <div class="card-meta">${stamp} / ${author}</div>
          <h3>${title}</h3>
          <p>${body}</p>
        </li>
      `;
    })
    .join("");
}

cards.on(renderCards);
renderCards();

async function refreshMeshStatus() {
  const [peerCount, openPeerCount, inflight] = await Promise.all([
    mesh.peerCount(),
    mesh.openPeerCount(),
    mesh.inflightCount(),
  ]);
  dom.meshStatus.textContent =
    session.signal === "relay" ? `relay=${mesh.relayConnected() ? "connected" : "waiting"}` : "broadcast ready";
  dom.peerStatus.textContent = `peers=${peerCount} / open=${openPeerCount} / inflight=${inflight}`;
}

setInterval(() => {
  void refreshMeshStatus();
}, 1000);
void refreshMeshStatus();

dom.form.addEventListener("submit", (event) => {
  event.preventDefault();

  const title = dom.title.value.trim();
  const body = dom.body.value.trim();
  if (!title || !body) {
    return;
  }

  cards.set({
    title,
    body,
    author: db.replicaId(),
    updated_at: Date.now(),
  });

  dom.form.reset();
});

Object.assign(globalThis, {
  threadedPackageDemo: {
    db,
    cards,
    mesh,
  },
});
