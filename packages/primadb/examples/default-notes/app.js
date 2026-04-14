import { createPrimadb } from "../../dist/index.js";

const dom = {
  replicaId: document.querySelector("#replica-id"),
  storageStatus: document.querySelector("#storage-status"),
  binaryStatus: document.querySelector("#binary-status"),
  noteCount: document.querySelector("#note-count"),
  notesList: document.querySelector("#notes-list"),
  form: document.querySelector("#note-form"),
  title: document.querySelector("#note-title"),
  body: document.querySelector("#note-body"),
  binaryButton: document.querySelector("#binary-button"),
};

const session = {
  replicaId: globalThis.crypto?.randomUUID
    ? `pkg-default-${globalThis.crypto.randomUUID().slice(0, 8)}`
    : `pkg-default-${Date.now().toString(36)}`,
  namespace: "package-default-notes",
};

const db = await createPrimadb(session.replicaId);
const durable = await db.openDurableStorage({
  kind: "indexed_db_segments",
  databaseName: "primadb-package-default-segments",
  storeName: "segments",
  namespace: session.namespace,
  loadExisting: true,
  autoPersist: true,
});
db.openBlobStorage({
  kind: "indexed_db",
  databaseName: "primadb-package-default-blobs",
  storeName: "blobs",
  namespace: session.namespace,
});

const notes = db.chain("package_examples").field("default_notes").field("items");
const bytesChain = db.chain("package_examples").field("default_notes").field("avatar_bytes");
const blobChain = db.chain("package_examples").field("default_notes").field("archive_blob");

dom.replicaId.textContent = db.replicaId();
dom.storageStatus.textContent = `${durable.backend} / incremental=${durable.incremental}`;

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}

function formatWhen(value) {
  if (typeof value === "number") {
    return new Date(value).toLocaleString();
  }
  if (typeof value === "string") {
    const parsed = Date.parse(value);
    if (!Number.isNaN(parsed)) {
      return new Date(parsed).toLocaleString();
    }
    return value;
  }
  return "unknown";
}

function renderNotes() {
  const entries = notes.query({
    order: {
      path: "updated_at",
      direction: "desc",
    },
  });

  dom.noteCount.textContent = String(entries.length);
  dom.notesList.innerHTML = entries
    .map(({ key, value }) => {
      const title = escapeHtml(value.title ?? key);
      const body = escapeHtml(value.body ?? "");
      const stamp = escapeHtml(formatWhen(value.updated_at ?? value.created_at ?? value.createdAt));
      return `
        <li class="note-card">
          <div class="note-meta">${stamp}</div>
          <h3>${title}</h3>
          <p>${body}</p>
        </li>
      `;
    })
    .join("");
}

notes.on(renderNotes);
renderNotes();

dom.form.addEventListener("submit", (event) => {
  event.preventDefault();

  const title = dom.title.value.trim();
  const body = dom.body.value.trim();
  if (!title || !body) {
    return;
  }

  notes.set({
    title,
    body,
    created_at: Date.now(),
    updated_at: Date.now(),
  });

  dom.form.reset();
});

dom.binaryButton.addEventListener("click", async () => {
  const bytes = new Uint8Array([7, 14, 21, 28, Date.now() % 251]);
  bytesChain.putBytes(bytes);
  const blobRef = await blobChain.putBlob(
    new Uint8Array([3, 1, 4, 1, 5, 9, Date.now() % 251]),
    "application/octet-stream",
  );
  const restoredBytes = bytesChain.onceBytes();
  const restoredBlob = await blobChain.getBlob();
  dom.binaryStatus.textContent =
    `bytes=${restoredBytes?.length ?? 0} / blob=${restoredBlob?.length ?? 0} / ${blobRef.id.slice(0, 12)}`;
});

Object.assign(globalThis, {
  defaultPackageDemo: {
    db,
    notes,
    bytesChain,
    blobChain,
  },
});
