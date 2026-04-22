import {
  createThreadedPrimadb,
  parallelEnabled,
  parallelThreadCount,
  wbg_rayon_start_worker,
} from "primadb/threads";

// Keep the rayon worker bootstrap export in the Vite bundle. The threaded runtime
// dynamically imports the `primadb/threads` module inside worker contexts.
globalThis.__primadbRayonWorkerBootstrap = wbg_rayon_start_worker;

const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;

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
  replInput: document.querySelector("#repl-input"),
  replHighlight: document.querySelector("#repl-highlight"),
  replStatus: document.querySelector("#repl-status"),
  runRepl: document.querySelector("#run-repl"),
  clearLogs: document.querySelector("#clear-logs"),
  replLogs: document.querySelector("#repl-logs"),
  logCount: document.querySelector("#log-count"),
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

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}

function formatLogValue(value) {
  if (typeof value === "string") {
    return value;
  }
  if (value instanceof Error) {
    return value.stack || `${value.name}: ${value.message}`;
  }
  try {
    return JSON.stringify(value, null, 2);
  } catch (_error) {
    return String(value);
  }
}

function appendLog(level, ...values) {
  const item = document.createElement("article");
  item.className = `log-entry log-${level}`;

  const meta = document.createElement("header");
  meta.className = "log-meta";
  meta.textContent = `${new Date().toLocaleTimeString()} / ${level}`;

  const body = document.createElement("pre");
  body.className = "log-body";
  body.textContent = values.map(formatLogValue).join("\n");

  item.append(meta, body);
  dom.replLogs.prepend(item);
  dom.logCount.textContent = String(dom.replLogs.childElementCount);
}

function clearLogs() {
  dom.replLogs.innerHTML = "";
  dom.logCount.textContent = "0";
}

const paramsSession = {
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

const session = paramsSession;
const durableStorageConfig = {
  kind: "indexed_db_segments",
  databaseName: `primadb-package-threaded-${session.room}`,
  storeName: "segments",
  namespace: session.room,
  loadExisting: true,
  autoPersist: true,
};

const db = await createThreadedPrimadb(session.replicaId, { threads: session.threads });
let storageStatus = {
  ready: false,
  error: null,
};
try {
  await db.openDurableStorage(durableStorageConfig);
  storageStatus = {
    ready: true,
    error: null,
  };
} catch (error) {
  storageStatus = {
    ready: false,
    error: formatLogValue(error),
  };
  appendLog("error", "durable storage init failed; continuing in memory", error);
}

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

const threadedPackageDemo = {
  db,
  cards,
  mesh,
  session,
  storageStatus,
  lastPersist: null,
};

async function persistDurableState(reason = "manual") {
  if (!storageStatus.ready) {
    const result = {
      ok: false,
      skipped: true,
      reason,
      at: Date.now(),
      error: storageStatus.error ?? "durable storage unavailable",
    };
    threadedPackageDemo.lastPersist = result;
    return result;
  }

  try {
    await db.saveIndexedDbSegments(
      durableStorageConfig.databaseName,
      durableStorageConfig.storeName,
      durableStorageConfig.namespace,
    );
    const result = {
      ok: true,
      skipped: false,
      reason,
      at: Date.now(),
    };
    threadedPackageDemo.lastPersist = result;
    return result;
  } catch (error) {
    const result = {
      ok: false,
      skipped: false,
      reason,
      at: Date.now(),
      error: formatLogValue(error),
    };
    threadedPackageDemo.lastPersist = result;
    appendLog("error", "durable segment flush failed", error);
    return result;
  }
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

async function meshSnapshot() {
  return {
    replicaId: db.replicaId(),
    peerId: mesh.peerId(),
    peers: await mesh.peerCount(),
    openPeers: await mesh.openPeerCount(),
    inflight: mesh.inflightCount(),
    signaling: mesh.signalingMode(),
    signalingReadyState: mesh.signalingReadyState?.() ?? null,
    relayUrl: mesh.relayUrl?.() ?? null,
    room: session.room,
    storageStatus,
  };
}

dom.form.addEventListener("submit", async (event) => {
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
  await persistDurableState("form_submit");

  dom.form.reset();
});

const keywordTokens = new Set([
  "await",
  "const",
  "let",
  "var",
  "return",
  "if",
  "else",
  "for",
  "while",
  "try",
  "catch",
  "finally",
  "async",
  "true",
  "false",
  "null",
  "undefined",
  "new",
  "throw",
  "typeof",
]);

const builtinTokens = new Set([
  "db",
  "cards",
  "mesh",
  "session",
  "log",
  "clearLogs",
  "renderCards",
  "refreshMeshStatus",
  "persistNow",
  "threadedPackageDemo",
  "Date",
  "JSON",
  "Math",
  "console",
]);

function highlightJavaScript(code) {
  const tokens = [];
  let index = 0;

  while (index < code.length) {
    const current = code[index];
    const next = code[index + 1];

    if (current === "/" && next === "/") {
      let end = index + 2;
      while (end < code.length && code[end] !== "\n") {
        end += 1;
      }
      tokens.push({ type: "comment", value: code.slice(index, end) });
      index = end;
      continue;
    }

    if (current === "/" && next === "*") {
      let end = index + 2;
      while (end < code.length && !(code[end] === "*" && code[end + 1] === "/")) {
        end += 1;
      }
      end = Math.min(code.length, end + 2);
      tokens.push({ type: "comment", value: code.slice(index, end) });
      index = end;
      continue;
    }

    if (current === "\"" || current === "'" || current === "`") {
      const quote = current;
      let end = index + 1;
      while (end < code.length) {
        if (code[end] === "\\") {
          end += 2;
          continue;
        }
        if (code[end] === quote) {
          end += 1;
          break;
        }
        end += 1;
      }
      tokens.push({ type: "string", value: code.slice(index, end) });
      index = end;
      continue;
    }

    if (/\d/.test(current)) {
      let end = index + 1;
      while (end < code.length && /[\d._xXa-fA-F]/.test(code[end])) {
        end += 1;
      }
      tokens.push({ type: "number", value: code.slice(index, end) });
      index = end;
      continue;
    }

    if (/[A-Za-z_$]/.test(current)) {
      let end = index + 1;
      while (end < code.length && /[A-Za-z0-9_$]/.test(code[end])) {
        end += 1;
      }
      const value = code.slice(index, end);
      let type = "plain";
      if (keywordTokens.has(value)) {
        type = "keyword";
      } else if (builtinTokens.has(value)) {
        type = "builtin";
      }
      tokens.push({ type, value });
      index = end;
      continue;
    }

    tokens.push({ type: "plain", value: current });
    index += 1;
  }

  return tokens
    .map((token) => {
      const escaped = escapeHtml(token.value);
      if (token.type === "plain") {
        return escaped;
      }
      return `<span class="tok-${token.type}">${escaped}</span>`;
    })
    .join("");
}

function syncEditorHighlight() {
  dom.replHighlight.innerHTML = `${highlightJavaScript(dom.replInput.value)}<span class="tok-plain">\n</span>`;
  dom.replHighlight.scrollTop = dom.replInput.scrollTop;
  dom.replHighlight.scrollLeft = dom.replInput.scrollLeft;
}

function setReplStatus(text, level = "idle") {
  dom.replStatus.textContent = text;
  dom.replStatus.dataset.state = level;
}

const defaultReplSource = `// Live threaded mesh REPL.
// Available bindings: db, cards, mesh, session, log, clearLogs, renderCards, refreshMeshStatus, meshSnapshot, persistNow

const entries = cards.query({
  order: { path: "updated_at", direction: "desc" },
  limit: 5,
});

log("latest cards", entries);
log("mesh status", await meshSnapshot());
log("last persist", threadedPackageDemo.lastPersist);

return {
  count: entries.length,
  firstCard: entries[0] ?? null,
};`;

dom.replInput.value = defaultReplSource;
syncEditorHighlight();

dom.replInput.addEventListener("input", syncEditorHighlight);
dom.replInput.addEventListener("scroll", syncEditorHighlight);
dom.replInput.addEventListener("keydown", (event) => {
  if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
    event.preventDefault();
    void runRepl();
  }
});

async function runRepl() {
  setReplStatus("running", "running");
  try {
    const runner = new AsyncFunction(
      "db",
      "cards",
      "mesh",
      "session",
      "log",
      "clearLogs",
      "renderCards",
      "refreshMeshStatus",
      "meshSnapshot",
      "persistNow",
      "threadedPackageDemo",
      dom.replInput.value,
    );

    const result = await runner(
      db,
      cards,
      mesh,
      session,
      (...values) => appendLog("info", ...values),
      clearLogs,
      renderCards,
      refreshMeshStatus,
      meshSnapshot,
      persistDurableState,
      threadedPackageDemo,
    );

    if (result !== undefined) {
      appendLog("result", result);
    }
    setReplStatus("completed", "success");
  } catch (error) {
    appendLog("error", error);
    setReplStatus("error", "error");
  }
}

dom.runRepl.addEventListener("click", () => {
  void runRepl();
});
dom.clearLogs.addEventListener("click", clearLogs);

Object.assign(globalThis, {
  threadedPackageDemo: Object.assign(threadedPackageDemo, {
    log: (...values) => appendLog("info", ...values),
    appendLog: (...values) => appendLog("info", ...values),
    clearLogs,
    renderCards,
    refreshMeshStatus,
    meshSnapshot,
    persistNow: persistDurableState,
    runRepl,
  }),
});

appendLog("info", "threaded mesh demo ready", {
  replicaId: db.replicaId(),
  room: session.room,
  signaling: session.signal,
  storageStatus,
});
