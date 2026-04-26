import { createPrimadb } from "primadb";

const dom = {
  meshStatus: document.querySelector("#mesh-status"),
  roomStatus: document.querySelector("#room-status"),
  storageStatus: document.querySelector("#storage-status"),
  publishStatus: document.querySelector("#publish-status"),
  localVideo: document.querySelector("#local-video"),
  captureMode: document.querySelector("#capture-mode"),
  chunkMs: document.querySelector("#chunk-ms"),
  windowMs: document.querySelector("#window-ms"),
  startButton: document.querySelector("#start-button"),
  stopButton: document.querySelector("#stop-button"),
  localChunks: document.querySelector("#local-chunks"),
  remoteChunks: document.querySelector("#remote-chunks"),
  localBytes: document.querySelector("#local-bytes"),
  remoteBytes: document.querySelector("#remote-bytes"),
  peerStatus: document.querySelector("#peer-status"),
  bufferStatus: document.querySelector("#buffer-status"),
  participantCount: document.querySelector("#participant-count"),
  participantList: document.querySelector("#participant-list"),
  remoteCount: document.querySelector("#remote-count"),
  remoteStreams: document.querySelector("#remote-streams"),
  clearLog: document.querySelector("#clear-log"),
  eventLog: document.querySelector("#event-log"),
};

const params = new URLSearchParams(globalThis.location.search);

function parsePositiveInt(value, fallback, min) {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.max(min, parsed);
}

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

function formatBytes(bytes) {
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  if (bytes < 1024 * 1024) {
    return `${(bytes / 1024).toFixed(1)} KiB`;
  }
  return `${(bytes / 1024 / 1024).toFixed(2)} MiB`;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}

function appendLog(level, message, detail) {
  const row = document.createElement("pre");
  const suffix = detail === undefined ? "" : `\n${typeof detail === "string" ? detail : JSON.stringify(detail, null, 2)}`;
  row.textContent = `${new Date().toLocaleTimeString()} / ${level} / ${message}${suffix}`;
  dom.eventLog.prepend(row);
}

const session = {
  room: params.get("room") || "package-binary-stream",
  relayUrl: params.get("relay") || "ws://127.0.0.1:9010",
  signal:
    params.get("signal") === "broadcast" || params.get("signal") === "broadcast_channel"
      ? "broadcast_channel"
      : "relay",
  name: params.get("name") || `browser-${Math.floor(Math.random() * 10000)}`,
  captureMode: params.get("capture") || "camera",
  chunkMs: parsePositiveInt(params.get("chunkMs"), 500, 150),
  windowMs: parsePositiveInt(params.get("windowMs"), 8000, 1000),
  videoBitsPerSecond: parsePositiveInt(params.get("bitrate"), 260000, 64000),
  iceServers: (() => {
    const parsed = params.getAll("ice").map(parseIceServerSpec);
    return parsed.length > 0 ? parsed : defaultExampleIceServers();
  })(),
};

dom.captureMode.value = session.captureMode === "synthetic" ? "synthetic" : "camera";
dom.chunkMs.value = String(session.chunkMs);
dom.windowMs.value = String(session.windowMs);
dom.roomStatus.textContent = `${session.room} / signaling=${session.signal}`;

const db = await createPrimadb(
  globalThis.crypto?.randomUUID
    ? `pkg-stream-${globalThis.crypto.randomUUID().slice(0, 8)}`
    : `pkg-stream-${Date.now().toString(36)}`,
);

let storageStatus = { ready: false, error: null };
try {
  const durable = await db.openDurableStorage({
    kind: "indexed_db_segments",
    databaseName: "primadb-package-binary-stream",
    storeName: "segments",
    namespace: session.room,
    loadExisting: true,
    autoPersist: true,
  });
  storageStatus = { ready: true, error: `${durable.backend} / incremental=${durable.incremental}` };
} catch (error) {
  storageStatus = { ready: false, error: String(error?.message ?? error) };
  appendLog("warn", "durable storage unavailable; continuing in memory", storageStatus.error);
}

dom.storageStatus.textContent = storageStatus.ready ? "ready" : "memory";

const peerKey = db.replicaId().replace(/[^A-Za-z0-9_-]/g, "_");
const roomRoot = db.chain("package_examples").field("binary_stream_room").field(session.room);
const participants = roomRoot.field("participants");
const streams = roomRoot.field("streams");
const localParticipant = participants.field(peerKey);
const localStreamRoot = streams.field(peerKey);
const localChunksRoot = localStreamRoot.field("chunks");

const mesh = db.connectMesh({
  room: session.room,
  signaling: session.signal,
  relayUrl: session.signal === "relay" ? session.relayUrl : undefined,
  retryIntervalMs: 1500,
  iceServers: session.iceServers,
});

const metrics = {
  localChunks: 0,
  remoteChunks: 0,
  localBytes: 0,
  remoteBytes: 0,
  droppedChunks: 0,
};

let localMediaStream = null;
let syntheticTimer = null;
let recorder = null;
let seq = 0;
let activeMimeType = null;
const localChunkWindow = [];
const remotePlayers = new Map();
let refreshQueued = false;

function updateMetrics() {
  dom.localChunks.textContent = String(metrics.localChunks);
  dom.remoteChunks.textContent = String(metrics.remoteChunks);
  dom.localBytes.textContent = formatBytes(metrics.localBytes);
  dom.remoteBytes.textContent = formatBytes(metrics.remoteBytes);
  dom.bufferStatus.textContent = `${localChunkWindow.length} local chunks`;
}

function updatePresence(extra = {}) {
  localParticipant.put({
    peerKey,
    replicaId: db.replicaId(),
    name: session.name,
    updatedAt: Date.now(),
    streaming: recorder?.state === "recording",
    mimeType: activeMimeType,
    captureMode: dom.captureMode.value,
    ...extra,
  });
}

function chooseMimeType(stream) {
  const hasAudio = stream.getAudioTracks().length > 0;
  const candidates = hasAudio
    ? ["video/webm;codecs=vp8,opus", "video/webm;codecs=vp8", "video/webm"]
    : ["video/webm;codecs=vp8", "video/webm"];
  return candidates.find((candidate) => MediaRecorder.isTypeSupported(candidate)) || "";
}

function createSyntheticStream() {
  const canvas = document.createElement("canvas");
  canvas.width = 640;
  canvas.height = 360;
  const context = canvas.getContext("2d");
  if (!context || typeof canvas.captureStream !== "function") {
    throw new Error("synthetic capture requires canvas.captureStream support");
  }

  const startedAt = performance.now();
  function draw() {
    const t = (performance.now() - startedAt) / 1000;
    context.fillStyle = "#102027";
    context.fillRect(0, 0, canvas.width, canvas.height);
    context.fillStyle = "#9de2d0";
    context.fillRect(28 + ((t * 60) % 460), 58, 120, 120);
    context.fillStyle = "#f7efe0";
    context.font = "32px IBM Plex Sans, sans-serif";
    context.fillText("PrimaDB byte stream", 32, 260);
    context.font = "22px IBM Plex Mono, monospace";
    context.fillText(`${session.name} / ${new Date().toLocaleTimeString()}`, 32, 304);
  }

  draw();
  syntheticTimer = globalThis.setInterval(draw, 100);
  return canvas.captureStream(10);
}

async function getCaptureStream() {
  if (dom.captureMode.value === "synthetic") {
    return createSyntheticStream();
  }

  return navigator.mediaDevices.getUserMedia({
    video: {
      width: { ideal: 640 },
      height: { ideal: 360 },
      frameRate: { ideal: 12, max: 20 },
    },
    audio: true,
  });
}

async function publishChunk(blob) {
  if (!blob || blob.size === 0) {
    return;
  }

  const id = `${String(seq).padStart(10, "0")}-${Date.now()}`;
  const createdAt = Date.now();
  const bytes = new Uint8Array(await blob.arrayBuffer());
  const meta = {
    id,
    peerKey,
    seq,
    createdAt,
    mimeType: activeMimeType || blob.type || "video/webm",
    size: bytes.byteLength,
    durationMs: Number(dom.chunkMs.value),
  };

  localChunksRoot.field(id).field("payload").putBytes(bytes);
  localChunksRoot.field(id).put(meta);
  localStreamRoot.field("state").put({
    peerKey,
    latestSeq: seq,
    updatedAt: createdAt,
    mimeType: meta.mimeType,
    windowMs: Number(dom.windowMs.value),
  });

  localChunkWindow.push({ id, seq, createdAt });
  seq += 1;
  metrics.localChunks += 1;
  metrics.localBytes += bytes.byteLength;
  pruneLocalWindow();
  updateMetrics();
}

function pruneLocalWindow() {
  const cutoff = Date.now() - Number(dom.windowMs.value);
  while (localChunkWindow.length > 0) {
    const first = localChunkWindow[0];
    if (first.seq === 0 || first.createdAt >= cutoff) {
      break;
    }
    localChunkWindow.shift();
    localChunksRoot.field(first.id).field("payload").unset();
    localChunksRoot.field(first.id).unset();
  }
}

async function startPublishing() {
  if (recorder?.state === "recording") {
    return;
  }

  const chunkMs = parsePositiveInt(dom.chunkMs.value, session.chunkMs, 150);
  const windowMs = parsePositiveInt(dom.windowMs.value, session.windowMs, 1000);
  dom.chunkMs.value = String(chunkMs);
  dom.windowMs.value = String(Math.max(windowMs, chunkMs * 2));

  localMediaStream = await getCaptureStream();
  dom.localVideo.srcObject = localMediaStream;
  activeMimeType = chooseMimeType(localMediaStream);
  recorder = new MediaRecorder(localMediaStream, {
    mimeType: activeMimeType || undefined,
    videoBitsPerSecond: session.videoBitsPerSecond,
  });
  seq = 0;
  localChunkWindow.length = 0;

  recorder.addEventListener("dataavailable", (event) => {
    void publishChunk(event.data).catch((error) => {
      metrics.droppedChunks += 1;
      appendLog("error", "chunk publish failed", String(error?.message ?? error));
    });
  });
  recorder.addEventListener("stop", () => {
    updatePresence({ streaming: false });
  });
  recorder.start(chunkMs);

  dom.startButton.disabled = true;
  dom.stopButton.disabled = false;
  dom.publishStatus.textContent = `streaming ${activeMimeType || "video/webm"}`;
  updatePresence({ streaming: true, mimeType: activeMimeType });
  appendLog("info", "publisher started", { mode: dom.captureMode.value, chunkMs, windowMs: Number(dom.windowMs.value) });
}

function stopPublishing() {
  if (recorder && recorder.state !== "inactive") {
    recorder.stop();
  }
  recorder = null;
  if (localMediaStream) {
    for (const track of localMediaStream.getTracks()) {
      track.stop();
    }
  }
  localMediaStream = null;
  if (syntheticTimer != null) {
    globalThis.clearInterval(syntheticTimer);
    syntheticTimer = null;
  }
  dom.localVideo.srcObject = null;
  dom.startButton.disabled = false;
  dom.stopButton.disabled = true;
  dom.publishStatus.textContent = "idle";
  updatePresence({ streaming: false });
  appendLog("info", "publisher stopped");
}

class RemotePlayer {
  constructor(peer) {
    this.peer = peer;
    this.seen = new Set();
    this.queue = [];
    this.chunks = [];
    this.bytes = 0;
    this.sourceBuffer = null;
    this.mediaSource = null;
    this.usingMediaSource = false;
    this.objectUrl = null;

    this.element = document.createElement("article");
    this.element.className = "remote-card";
    this.title = document.createElement("strong");
    this.video = document.createElement("video");
    this.video.autoplay = true;
    this.video.muted = true;
    this.video.controls = true;
    this.video.playsInline = true;
    this.meta = document.createElement("div");
    this.meta.className = "stream-meta";
    this.element.append(this.title, this.video, this.meta);
    dom.remoteStreams.append(this.element);
    this.updatePeer(peer);
  }

  updatePeer(peer) {
    this.peer = peer;
    this.title.textContent = peer.name || peer.peerKey;
    this.meta.textContent = `${this.seen.size} chunks / ${formatBytes(this.bytes)}`;
  }

  ensureMediaSource(mimeType) {
    const MediaSourceImpl = globalThis.MediaSource || globalThis.ManagedMediaSource;
    if (
      this.usingMediaSource ||
      !MediaSourceImpl ||
      typeof MediaSourceImpl.isTypeSupported !== "function" ||
      !MediaSourceImpl.isTypeSupported(mimeType)
    ) {
      return false;
    }

    this.mediaSource = new MediaSourceImpl();
    this.objectUrl = URL.createObjectURL(this.mediaSource);
    this.video.src = this.objectUrl;
    this.mediaSource.addEventListener("sourceopen", () => {
      try {
        this.sourceBuffer = this.mediaSource.addSourceBuffer(mimeType);
        this.sourceBuffer.mode = "sequence";
        this.sourceBuffer.addEventListener("updateend", () => this.drainQueue());
        this.drainQueue();
      } catch (error) {
        appendLog("warn", "remote MediaSource unavailable; using blob playback", String(error?.message ?? error));
        this.usingMediaSource = false;
      }
    }, { once: true });
    this.usingMediaSource = true;
    return true;
  }

  enqueue(meta, bytes) {
    if (this.seen.has(meta.id)) {
      return;
    }
    this.seen.add(meta.id);
    this.bytes += bytes.byteLength;
    this.chunks.push({ meta, bytes });
    this.prune();

    if (this.ensureMediaSource(meta.mimeType)) {
      this.queue.push(bytes.slice().buffer);
      this.drainQueue();
    } else if (this.usingMediaSource) {
      this.queue.push(bytes.slice().buffer);
      this.drainQueue();
    } else {
      this.renderBlobFallback(meta.mimeType);
    }

    this.meta.textContent = `${this.seen.size} chunks / ${formatBytes(this.bytes)}`;
  }

  drainQueue() {
    if (!this.sourceBuffer || this.sourceBuffer.updating || this.queue.length === 0) {
      return;
    }
    try {
      this.sourceBuffer.appendBuffer(this.queue.shift());
      void this.video.play().catch(() => {});
    } catch (error) {
      appendLog("warn", "remote append failed; using blob playback", String(error?.message ?? error));
      this.usingMediaSource = false;
      this.queue.length = 0;
      this.renderBlobFallback(this.chunks.at(-1)?.meta.mimeType || "video/webm");
    }
  }

  renderBlobFallback(mimeType) {
    if (this.objectUrl) {
      URL.revokeObjectURL(this.objectUrl);
    }
    const blob = new Blob(this.chunks.map((entry) => entry.bytes), { type: mimeType });
    this.objectUrl = URL.createObjectURL(blob);
    this.video.src = this.objectUrl;
    void this.video.play().catch(() => {});
  }

  prune() {
    const cutoff = Date.now() - Number(dom.windowMs.value);
    this.chunks = this.chunks.filter((entry) => entry.meta.seq === 0 || entry.meta.createdAt >= cutoff);
  }
}

function participantEntries() {
  return participants
    .map()
    .filter(({ value }) => value && typeof value === "object" && typeof value.peerKey === "string")
    .sort((a, b) => String(a.value.name || a.key).localeCompare(String(b.value.name || b.key)));
}

function renderParticipants(entries = participantEntries()) {
  dom.participantCount.textContent = String(entries.length);
  dom.participantList.innerHTML = entries
    .map(({ value }) => {
      const age = Math.max(0, Math.round((Date.now() - Number(value.updatedAt || 0)) / 1000));
      const state = value.streaming ? "streaming" : "present";
      return `
        <li>
          <strong>${escapeHtml(value.name || value.peerKey)}</strong>
          <span class="card-meta">${escapeHtml(state)} / ${age}s ago</span>
        </li>
      `;
    })
    .join("");
}

async function refreshRemoteStreams() {
  const entries = participantEntries();
  renderParticipants(entries);

  for (const { value: peer } of entries) {
    if (peer.peerKey === peerKey || !peer.streaming) {
      continue;
    }
    let player = remotePlayers.get(peer.peerKey);
    if (!player) {
      player = new RemotePlayer(peer);
      remotePlayers.set(peer.peerKey, player);
    } else {
      player.updatePeer(peer);
    }

    const chunkRoot = streams.field(peer.peerKey).field("chunks");
    const metas = chunkRoot
      .map()
      .map(({ value }) => value)
      .filter((value) => value && typeof value === "object" && typeof value.id === "string")
      .sort((a, b) => Number(a.seq) - Number(b.seq));

    for (const meta of metas) {
      if (player.seen.has(meta.id)) {
        continue;
      }
      const bytes = chunkRoot.field(meta.id).field("payload").onceBytes();
      if (!bytes || bytes.byteLength === 0) {
        continue;
      }
      player.enqueue(meta, bytes);
      metrics.remoteChunks += 1;
      metrics.remoteBytes += bytes.byteLength;
    }
  }

  dom.remoteCount.textContent = String(remotePlayers.size);
  updateMetrics();
}

function queueRefresh() {
  if (refreshQueued) {
    return;
  }
  refreshQueued = true;
  globalThis.setTimeout(() => {
    refreshQueued = false;
    void refreshRemoteStreams().catch((error) => {
      appendLog("error", "remote refresh failed", String(error?.message ?? error));
    });
  }, 120);
}

roomRoot.on(queueRefresh);
globalThis.setInterval(queueRefresh, 700);
globalThis.setInterval(() => {
  updatePresence();
  pruneLocalWindow();
}, 2000);
async function refreshMeshStatus() {
  const [peerCount, openPeerCount, inflight] = await Promise.all([
    mesh.peerCount(),
    mesh.openPeerCount(),
    mesh.inflightCount(),
  ]);
  const readyState = mesh.signalingReadyState?.();
  const relayStatus =
    session.signal === "relay" ? `relay=${readyState === WebSocket.OPEN ? "connected" : "waiting"}` : "broadcast";
  dom.meshStatus.textContent = relayStatus;
  dom.peerStatus.textContent = `${peerCount} / ${openPeerCount} / ${inflight}`;
}

globalThis.setInterval(() => {
  void refreshMeshStatus();
}, 1000);
void refreshMeshStatus();

dom.startButton.addEventListener("click", () => {
  void startPublishing().catch((error) => {
    appendLog("error", "publisher start failed", String(error?.message ?? error));
    stopPublishing();
  });
});
dom.stopButton.addEventListener("click", stopPublishing);
dom.clearLog.addEventListener("click", () => {
  dom.eventLog.innerHTML = "";
});

updatePresence();
renderParticipants();
updateMetrics();
appendLog("info", "binary stream room ready", {
  room: session.room,
  peerKey,
  signaling: session.signal,
  relayUrl: session.relayUrl,
});

Object.assign(globalThis, {
  binaryStreamDemo: {
    db,
    mesh,
    roomRoot,
    participants,
    streams,
    session,
    metrics,
    peerKey,
    startPublishing,
    stopPublishing,
    refreshRemoteStreams,
    refreshMeshStatus,
    updatePresence,
  },
});

if (params.get("autostart") === "1") {
  void startPublishing().catch((error) => {
    appendLog("error", "autostart failed", String(error?.message ?? error));
  });
}
