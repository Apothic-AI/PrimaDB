import "./styles.css";
import { Primadb, initPrimadb } from "primadb";

type NoteRecord = {
  title?: string;
  body?: string;
  createdAt?: string;
  $id?: string;
};

type QueryEntry = {
  key: string;
  value: NoteRecord;
};

const app = {
  replica: document.querySelector<HTMLElement>("#replica-id")!,
  storage: document.querySelector<HTMLElement>("#storage-backend")!,
  count: document.querySelector<HTMLElement>("#note-count")!,
  status: document.querySelector<HTMLElement>("#status-line")!,
  list: document.querySelector<HTMLUListElement>("#notes-list")!,
  form: document.querySelector<HTMLFormElement>("#note-form")!,
  title: document.querySelector<HTMLInputElement>("#note-title")!,
  body: document.querySelector<HTMLTextAreaElement>("#note-body")!,
};

const replicaId =
  globalThis.crypto?.randomUUID?.() != null
    ? `vite-${globalThis.crypto.randomUUID().slice(0, 8)}`
    : `vite-${Date.now().toString(36)}`;

await initPrimadb();

const db = new Primadb(replicaId);
const durable = await db.openDurableStorage({
  kind: "indexed_db_segments",
  databaseName: "primadb-package-vite",
  storeName: "segments",
  namespace: "notes",
  loadExisting: true,
  autoPersist: true,
});
const notes = db.chain("package-demo").field("notes");

app.replica.textContent = db.replicaId();
app.storage.textContent = `${durable.backend} / incremental=${durable.incremental}`;

function uniqueTitle(title: string) {
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
      path: "createdAt",
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
      const createdAt = escapeHtml(value.createdAt ?? "");
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
    createdAt: new Date().toISOString(),
  });

  app.form.reset();
  app.title.focus();
});
