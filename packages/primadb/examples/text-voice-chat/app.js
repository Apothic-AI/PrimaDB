import { createPrimadb } from "primadb";

const dom = {
  meshStatus: document.querySelector("#mesh-status"),
  roomStatus: document.querySelector("#room-status"),
  voiceStatus: document.querySelector("#voice-status"),
  messageCount: document.querySelector("#message-count"),
  messageList: document.querySelector("#message-list"),
  messageForm: document.querySelector("#message-form"),
  messageInput: document.querySelector("#message-input"),
  displayName: document.querySelector("#display-name"),
  captureMode: document.querySelector("#capture-mode"),
  chunkMs: document.querySelector("#chunk-ms"),
  windowMs: document.querySelector("#window-ms"),
  startVoice: document.querySelector("#start-voice"),
  stopVoice: document.querySelector("#stop-voice"),
  participantCount: document.querySelector("#participant-count"),
  participantList: document.querySelector("#participant-list"),
  remoteCount: document.querySelector("#remote-count"),
  remoteVoiceList: document.querySelector("#remote-voice-list"),
  sentChunks: document.querySelector("#sent-chunks"),
  receivedChunks: document.querySelector("#received-chunks"),
  sentBytes: document.querySelector("#sent-bytes"),
  receivedBytes: document.querySelector("#received-bytes"),
  peerStatus: document.querySelector("#peer-status"),
  bufferStatus: document.querySelector("#buffer-status"),
};

const params = new URLSearchParams(globalThis.location.search);

function parsePositiveInt(value, fallback, min) {
  const parsed = Number.parseInt(value ?? "", 10);
  return Number.isFinite(parsed) ? Math.max(min, parsed) : fallback;
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

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
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

function formatTime(value) {
  return new Date(Number(value) || Date.now()).toLocaleTimeString();
}

const session = {
  room: params.get("room") || "package-text-voice-chat",
  relayUrl: params.get("relay") || "ws://127.0.0.1:9010",
  signal:
    params.get("signal") === "broadcast" || params.get("signal") === "broadcast_channel"
      ? "broadcast_channel"
      : "relay",
  name: params.get("name") || `speaker-${Math.floor(Math.random() * 10000)}`,
  captureMode: params.get("capture") || "microphone",
  chunkMs: parsePositiveInt(params.get("chunkMs"), 350, 100),
  windowMs: parsePositiveInt(params.get("windowMs"), 6000, 1000),
  audioBitsPerSecond: parsePositiveInt(params.get("bitrate"), 48000, 12000),
  iceServers: (() => {
    const parsed = params.getAll("ice").map(parseIceServerSpec);
    return parsed.length > 0 ? parsed : defaultExampleIceServers();
  })(),
};

dom.displayName.value = session.name;
dom.captureMode.value = session.captureMode === "synthetic" ? "synthetic" : "microphone";
dom.chunkMs.value = String(session.chunkMs);
dom.windowMs.value = String(session.windowMs);
dom.roomStatus.textContent = `${session.room} / signaling=${session.signal}`;

const db = await createPrimadb(
  globalThis.crypto?.randomUUID
    ? `pkg-chat-${globalThis.crypto.randomUUID().slice(0, 8)}`
    : `pkg-chat-${Date.now().toString(36)}`,
);

const peerKey = db.replicaId().replace(/[^A-Za-z0-9_-]/g, "_");
const roomRoot = db.chain("package_examples").field("text_voice_chat").field(session.room);
const messages = roomRoot.field("messages");
const participants = roomRoot.field("participants");
const voices = roomRoot.field("voices");
const localParticipant = participants.field(peerKey);
const localVoiceRoot = voices.field(peerKey);
const localChunksRoot = localVoiceRoot.field("chunks");

const mesh = db.connectMesh({
  room: session.room,
  signaling: session.signal,
  relayUrl: session.signal === "relay" ? session.relayUrl : undefined,
  retryIntervalMs: 1500,
  iceServers: session.iceServers,
});

const metrics = {
  sentChunks: 0,
  receivedChunks: 0,
  sentBytes: 0,
  receivedBytes: 0,
  droppedChunks: 0,
};

let recorder = null;
let audioStream = null;
let activeMimeType = null;
let seq = 0;
let syntheticVoiceTimer = null;
let refreshQueued = false;
const localChunkWindow = [];
const voicePlayers = new Map();

function chooseAudioMimeType() {
  const candidates = ["audio/webm;codecs=opus", "audio/webm", "audio/ogg;codecs=opus", "audio/ogg"];
  return candidates.find((candidate) => MediaRecorder.isTypeSupported(candidate)) || "";
}

function updateMetrics() {
  dom.sentChunks.textContent = String(metrics.sentChunks);
  dom.receivedChunks.textContent = String(metrics.receivedChunks);
  dom.sentBytes.textContent = formatBytes(metrics.sentBytes);
  dom.receivedBytes.textContent = formatBytes(metrics.receivedBytes);
  dom.bufferStatus.textContent = `${localChunkWindow.length} chunks`;
}

function isVoiceActive() {
  return recorder?.state === "recording" || syntheticVoiceTimer !== null;
}

function updatePresence(extra = {}) {
  localParticipant.put({
    peerKey,
    replicaId: db.replicaId(),
    name: dom.displayName.value.trim() || session.name,
    updatedAt: Date.now(),
    voiceActive: isVoiceActive(),
    captureMode: dom.captureMode.value,
    mimeType: activeMimeType,
    ...extra,
  });
}

async function getAudioStream() {
  return navigator.mediaDevices.getUserMedia({
    audio: {
      echoCancellation: true,
      noiseSuppression: true,
      autoGainControl: true,
    },
    video: false,
  });
}

async function publishVoiceBytes(bytes, mimeType, durationMs) {
  if (!bytes || bytes.byteLength === 0) {
    return;
  }

  const currentSeq = seq;
  const id = `${String(currentSeq).padStart(10, "0")}-${Date.now()}`;
  const createdAt = Date.now();
  const meta = {
    id,
    peerKey,
    seq: currentSeq,
    createdAt,
    mimeType,
    size: bytes.byteLength,
    durationMs,
  };

  localChunksRoot.field(id).field("payload").putBytes(bytes);
  localChunksRoot.field(id).put(meta);
  localVoiceRoot.field("state").put({
    peerKey,
    latestSeq: currentSeq,
    updatedAt: createdAt,
    mimeType: meta.mimeType,
    windowMs: Number(dom.windowMs.value),
  });

  localChunkWindow.push({ id, seq: currentSeq, createdAt });
  seq += 1;
  metrics.sentChunks += 1;
  metrics.sentBytes += bytes.byteLength;
  pruneLocalVoiceWindow();
  updateMetrics();
}

async function publishVoiceChunk(blob) {
  if (!blob || blob.size === 0) {
    return;
  }

  const bytes = new Uint8Array(await blob.arrayBuffer());
  await publishVoiceBytes(bytes, activeMimeType || blob.type || "audio/webm", Number(dom.chunkMs.value));
}

function makeSyntheticVoiceBytes(nextSeq, durationMs) {
  const length = Math.max(256, Math.floor(durationMs * 18));
  const bytes = new Uint8Array(length);
  for (let index = 0; index < bytes.length; index += 1) {
    const carrier = Math.sin((nextSeq * 31 + index) / 9);
    const envelope = Math.sin((nextSeq + index / bytes.length) * Math.PI);
    bytes[index] = (128 + Math.round(82 * carrier * Math.max(0.25, Math.abs(envelope)))) & 255;
  }
  return bytes;
}

function startSyntheticVoice(chunkMs) {
  activeMimeType = "application/octet-stream";
  seq = 0;
  localChunkWindow.length = 0;

  const publish = () => {
    const bytes = makeSyntheticVoiceBytes(seq, chunkMs);
    void publishVoiceBytes(bytes, activeMimeType, chunkMs).catch((error) => {
      metrics.droppedChunks += 1;
      console.error(error);
    });
  };

  publish();
  syntheticVoiceTimer = globalThis.setInterval(publish, chunkMs);
  dom.startVoice.disabled = true;
  dom.stopVoice.disabled = false;
  dom.voiceStatus.textContent = "streaming synthetic transport bytes";
  updatePresence({ voiceActive: true, mimeType: activeMimeType });
}

function pruneLocalVoiceWindow() {
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

async function startVoice() {
  if (isVoiceActive()) {
    return;
  }

  const chunkMs = parsePositiveInt(dom.chunkMs.value, session.chunkMs, 100);
  const windowMs = parsePositiveInt(dom.windowMs.value, session.windowMs, 1000);
  dom.chunkMs.value = String(chunkMs);
  dom.windowMs.value = String(Math.max(windowMs, chunkMs * 3));

  if (dom.captureMode.value === "synthetic") {
    startSyntheticVoice(chunkMs);
    return;
  }

  audioStream = await getAudioStream();
  activeMimeType = chooseAudioMimeType();
  recorder = new MediaRecorder(audioStream, {
    mimeType: activeMimeType || undefined,
    audioBitsPerSecond: session.audioBitsPerSecond,
  });
  seq = 0;
  localChunkWindow.length = 0;

  recorder.addEventListener("dataavailable", (event) => {
    void publishVoiceChunk(event.data).catch((error) => {
      metrics.droppedChunks += 1;
      console.error(error);
    });
  });
  recorder.addEventListener("stop", () => {
    updatePresence({ voiceActive: false });
  });
  recorder.start(chunkMs);

  dom.startVoice.disabled = true;
  dom.stopVoice.disabled = false;
  dom.voiceStatus.textContent = `streaming ${activeMimeType || "audio/webm"}`;
  updatePresence({ voiceActive: true, mimeType: activeMimeType });
}

function stopVoice() {
  if (syntheticVoiceTimer) {
    globalThis.clearInterval(syntheticVoiceTimer);
  }
  syntheticVoiceTimer = null;
  if (recorder && recorder.state !== "inactive") {
    recorder.stop();
  }
  recorder = null;
  if (audioStream) {
    for (const track of audioStream.getTracks()) {
      track.stop();
    }
  }
  audioStream = null;
  dom.startVoice.disabled = false;
  dom.stopVoice.disabled = true;
  dom.voiceStatus.textContent = "idle";
  updatePresence({ voiceActive: false });
}

class VoicePlayer {
  constructor(peer) {
    this.peer = peer;
    this.seen = new Set();
    this.queue = [];
    this.chunks = [];
    this.bytes = 0;
    this.mediaSource = null;
    this.sourceBuffer = null;
    this.objectUrl = null;
    this.usingMediaSource = false;

    this.element = document.createElement("section");
    this.element.className = "voice-card";
    this.title = document.createElement("strong");
    this.audio = document.createElement("audio");
    this.audio.controls = true;
    this.audio.autoplay = true;
    this.audio.playsInline = true;
    this.meta = document.createElement("span");
    this.meta.className = "voice-meta";
    this.element.append(this.title, this.audio, this.meta);
    dom.remoteVoiceList.append(this.element);
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
    this.audio.src = this.objectUrl;
    this.mediaSource.addEventListener("sourceopen", () => {
      try {
        this.sourceBuffer = this.mediaSource.addSourceBuffer(mimeType);
        this.sourceBuffer.mode = "sequence";
        this.sourceBuffer.addEventListener("updateend", () => this.drainQueue());
        this.drainQueue();
      } catch {
        this.usingMediaSource = false;
        this.renderBlobFallback(mimeType);
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

    const playable = String(meta.mimeType || "").startsWith("audio/");
    if (!playable) {
      this.audio.removeAttribute("src");
    } else if (this.ensureMediaSource(meta.mimeType) || this.usingMediaSource) {
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
      void this.audio.play().catch(() => {});
    } catch {
      this.usingMediaSource = false;
      this.queue.length = 0;
      this.renderBlobFallback(this.chunks.at(-1)?.meta.mimeType || "audio/webm");
    }
  }

  renderBlobFallback(mimeType) {
    if (this.objectUrl) {
      URL.revokeObjectURL(this.objectUrl);
    }
    const blob = new Blob(this.chunks.map((entry) => entry.bytes), { type: mimeType });
    this.objectUrl = URL.createObjectURL(blob);
    this.audio.src = this.objectUrl;
    void this.audio.play().catch(() => {});
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
      return `
        <li>
          <strong>${escapeHtml(value.name || value.peerKey)}</strong>
          <span>${value.voiceActive ? "voice active" : "present"} / ${age}s ago</span>
        </li>
      `;
    })
    .join("");
}

function renderMessages() {
  const entries = messages.query({
    order: { path: "createdAt", direction: "asc" },
    limit: 200,
  });
  dom.messageCount.textContent = String(entries.length);
  dom.messageList.innerHTML = entries
    .map(({ value }) => {
      const local = value.peerKey === peerKey ? " local" : "";
      return `
        <li class="message${local}">
          <strong>${escapeHtml(value.name || value.peerKey || "unknown")}</strong>
          <span class="message-meta">${escapeHtml(formatTime(value.createdAt))}</span>
          <p>${escapeHtml(value.text || "")}</p>
        </li>
      `;
    })
    .join("");
  dom.messageList.scrollTop = dom.messageList.scrollHeight;
}

async function refreshRemoteVoice() {
  const entries = participantEntries();
  renderParticipants(entries);
  for (const { value: peer } of entries) {
    if (peer.peerKey === peerKey || !peer.voiceActive) {
      continue;
    }
    let player = voicePlayers.get(peer.peerKey);
    if (!player) {
      player = new VoicePlayer(peer);
      voicePlayers.set(peer.peerKey, player);
    } else {
      player.updatePeer(peer);
    }

    const chunksRoot = voices.field(peer.peerKey).field("chunks");
    const metas = chunksRoot
      .map()
      .map(({ value }) => value)
      .filter((value) => value && typeof value === "object" && typeof value.id === "string")
      .sort((a, b) => Number(a.seq) - Number(b.seq));

    for (const meta of metas) {
      if (player.seen.has(meta.id)) {
        continue;
      }
      const bytes = chunksRoot.field(meta.id).field("payload").onceBytes();
      if (!bytes || bytes.byteLength === 0) {
        continue;
      }
      player.enqueue(meta, bytes);
      metrics.receivedChunks += 1;
      metrics.receivedBytes += bytes.byteLength;
    }
  }
  dom.remoteCount.textContent = String(voicePlayers.size);
  updateMetrics();
}

function queueRefresh() {
  if (refreshQueued) {
    return;
  }
  refreshQueued = true;
  globalThis.setTimeout(() => {
    refreshQueued = false;
    renderMessages();
    void refreshRemoteVoice();
  }, 100);
}

async function refreshMeshStatus() {
  const [peerCount, openPeerCount, inflight] = await Promise.all([
    mesh.peerCount(),
    mesh.openPeerCount(),
    mesh.inflightCount(),
  ]);
  const readyState = mesh.signalingReadyState?.();
  const status =
    session.signal === "relay" ? `relay=${readyState === WebSocket.OPEN ? "connected" : "waiting"}` : "broadcast";
  dom.meshStatus.textContent = status;
  dom.peerStatus.textContent = `${peerCount} / ${openPeerCount} / ${inflight}`;
}

dom.messageForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const text = dom.messageInput.value.trim();
  if (!text) {
    return;
  }
  messages.set({
    peerKey,
    replicaId: db.replicaId(),
    name: dom.displayName.value.trim() || session.name,
    text,
    createdAt: Date.now(),
  });
  dom.messageInput.value = "";
  updatePresence();
});

dom.displayName.addEventListener("input", () => updatePresence());
dom.startVoice.addEventListener("click", () => {
  void startVoice().catch((error) => {
    console.error(error);
    stopVoice();
  });
});
dom.stopVoice.addEventListener("click", stopVoice);

roomRoot.on(queueRefresh);
globalThis.setInterval(queueRefresh, 700);
globalThis.setInterval(() => {
  updatePresence();
  pruneLocalVoiceWindow();
}, 2000);
globalThis.setInterval(() => {
  void refreshMeshStatus();
}, 1000);

updatePresence();
renderMessages();
renderParticipants();
updateMetrics();
void refreshMeshStatus();

Object.assign(globalThis, {
  textVoiceChatDemo: {
    db,
    mesh,
    roomRoot,
    messages,
    participants,
    voices,
    session,
    metrics,
    peerKey,
    startVoice,
    stopVoice,
    renderMessages,
    refreshRemoteVoice,
    refreshMeshStatus,
  },
});

if (params.get("message")) {
  messages.set({
    peerKey,
    replicaId: db.replicaId(),
    name: dom.displayName.value.trim() || session.name,
    text: params.get("message"),
    createdAt: Date.now(),
  });
}

if (params.get("autostart") === "1") {
  void startVoice().catch((error) => {
    console.error(error);
  });
}
