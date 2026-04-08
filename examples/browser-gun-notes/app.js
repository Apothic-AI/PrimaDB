import init, {
  Primadb,
  generateSeaPair,
  seaDecrypt,
  seaEncrypt,
  seaPairFromPrivateKeys,
  seaSecret,
  seaSign,
  seaVerify,
} from "./pkg/primadb.js";
import { installPrimadbGunRuntime } from "../../js/primadb-gun.js";

const state = {
  Gun: null,
  gun: null,
  user: null,
  notes: null,
  notesData: [],
  profileSubscription: null,
  notesSubscription: null,
  statusTimer: null,
};

const elements = {
  replicaId: document.getElementById("replica-id"),
  peerCount: document.getElementById("peer-count"),
  pendingCount: document.getElementById("pending-count"),
  inflightCount: document.getElementById("inflight-count"),
  authStatus: document.getElementById("auth-status"),
  authDetail: document.getElementById("auth-detail"),
  aliasInput: document.getElementById("alias-input"),
  passwordInput: document.getElementById("password-input"),
  createButton: document.getElementById("create-button"),
  loginButton: document.getElementById("login-button"),
  recallButton: document.getElementById("recall-button"),
  logoutButton: document.getElementById("logout-button"),
  profileForm: document.getElementById("profile-form"),
  profileState: document.getElementById("profile-state"),
  displayNameInput: document.getElementById("display-name-input"),
  taglineInput: document.getElementById("tagline-input"),
  relayUrl: document.getElementById("relay-url"),
  noteForm: document.getElementById("note-form"),
  noteTitle: document.getElementById("note-title"),
  noteBody: document.getElementById("note-body"),
  notesState: document.getElementById("notes-state"),
  runtimeState: document.getElementById("runtime-state"),
  noteList: document.getElementById("note-list"),
  emptyState: document.getElementById("empty-state"),
  noteTemplate: document.getElementById("note-template"),
};

main().catch((error) => {
  console.error(error);
  elements.runtimeState.textContent = `runtime failed: ${error}`;
});

async function main() {
  await init();

  const Gun = installPrimadbGunRuntime({
    Primadb,
    generateSeaPair,
    seaPairFromPrivateKeys,
    seaSign,
    seaVerify,
    seaSecret,
    seaEncrypt,
    seaDecrypt,
  });

  const gun = Gun({
    replicaId: `gun-demo-${globalThis.crypto?.randomUUID?.() ?? Date.now()}`,
    peers: [elements.relayUrl.value.trim()],
    room: "browser-gun-notes",
    remember: true,
    indexedDb: {
      databaseName: "primadb-browser-gun-notes",
      storeName: "snapshots",
      key: "main",
    },
    localStorageKey: "primadb-browser-gun-notes-fallback",
  });

  state.Gun = Gun;
  state.gun = gun;
  state.user = gun.user();
  state.notes = gun.get("rooms").get("lobby").get("notes");

  await gun.ready;

  elements.replicaId.textContent = gun.stats().replicaId;
  bindUi();
  subscribeToNotes();
  syncProfileSubscription();
  await ensureSeedNote();
  await refreshNotes();
  await refreshProfile();
  updateUi();

  state.statusTimer = setInterval(async () => {
    await refreshNotes();
    await refreshProfile();
    updateUi();
  }, 1000);
  globalThis.primadbGunDemo = {
    Gun,
    gun,
    user: state.user,
    notes: state.notes,
  };
  globalThis.addEventListener("beforeunload", teardown, { once: true });
}

function bindUi() {
  elements.createButton.addEventListener("click", async () => {
    const ack = await createUser();
    if (!ack.err) {
      elements.authDetail.textContent = `created ${ack.alias ?? elements.aliasInput.value.trim()}`;
      syncProfileSubscription();
      await refreshProfile();
      updateUi();
    }
  });

  elements.loginButton.addEventListener("click", async () => {
    const ack = await loginUser();
    if (!ack.err) {
      elements.authDetail.textContent = `authenticated ${ack.alias ?? elements.aliasInput.value.trim()}`;
      syncProfileSubscription();
      await refreshProfile();
      updateUi();
    }
  });

  elements.recallButton.addEventListener("click", async () => {
    const ack = await recallUser();
    if (ack.err) {
      elements.authDetail.textContent = ack.err;
      return;
    }
    if (ack.ok) {
      elements.authDetail.textContent = `recalled ${ack.alias ?? state.user.is?.alias ?? "session"}`;
    } else {
      elements.authDetail.textContent = "no stored session";
    }
    syncProfileSubscription();
    await refreshProfile();
    updateUi();
  });

  elements.logoutButton.addEventListener("click", () => {
    state.user.leave();
    elements.authDetail.textContent = "session cleared";
    if (state.profileSubscription) {
      state.profileSubscription.off();
      state.profileSubscription = null;
    }
    elements.displayNameInput.value = "";
    elements.taglineInput.value = "";
    updateUi();
  });

  elements.profileForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!state.user.is) {
      elements.authDetail.textContent = "sign in before saving profile data";
      return;
    }
    await new Promise((resolve) => {
      state.user.get("profile").put(
        {
          display_name: elements.displayNameInput.value.trim(),
          tagline: elements.taglineInput.value.trim(),
          updated_at: Date.now(),
        },
        (ack) => {
          elements.profileState.textContent = ack.err ? ack.err : "profile saved";
          resolve();
        },
      );
    });
    const profile = await state.user.get("profile").once();
    elements.displayNameInput.value = profile?.display_name ?? "";
    elements.taglineInput.value = profile?.tagline ?? "";
    updateUi();
  });

  elements.noteForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const title = elements.noteTitle.value.trim();
    const body = elements.noteBody.value.trim();
    if (!title) {
      return;
    }

    const author = state.user.is?.alias ?? "anonymous";
    state.notes.set({
      title,
      body,
      author,
      owner: state.user.is?.pub ?? null,
      done: false,
      created_at: Date.now(),
      updated_at: Date.now(),
    });
    elements.noteForm.reset();
    await refreshNotes();
    elements.runtimeState.textContent = "queued shared note";
    updateUi();
  });
}

function subscribeToNotes() {
  state.notesSubscription = state.notes.open(
    (notes) => {
      state.notesData = Array.isArray(notes) ? notes : [];
      renderNotes();
      elements.notesState.textContent = `${state.notesData.length} shared note${state.notesData.length === 1 ? "" : "s"}`;
    },
    { wait: 25 },
  );
}

function syncProfileSubscription() {
  if (state.profileSubscription) {
    state.profileSubscription.off();
    state.profileSubscription = null;
  }

  if (!state.user.is) {
    elements.profileState.textContent = "waiting for login";
    return;
  }

  state.profileSubscription = state.user.get("profile").open(
    (profile) => {
      elements.displayNameInput.value = profile?.display_name ?? "";
      elements.taglineInput.value = profile?.tagline ?? "";
      elements.profileState.textContent = profile?.display_name
        ? "signed profile loaded"
        : "profile ready";
    },
    { wait: 25 },
  );
}

async function ensureSeedNote() {
  const existing = await state.notes.once();
  if (Array.isArray(existing) && existing.length > 0) {
    return;
  }
  state.notes.set({
    title: "Open this page in another browser tab",
    body: "The Gun-style runtime discovers peers through the DAM relay and syncs through Primadb.",
    author: "primadb",
    owner: null,
    done: false,
    created_at: Date.now(),
    updated_at: Date.now(),
  });
}

async function refreshNotes() {
  const notes = await state.notes.once();
  state.notesData = Array.isArray(notes) ? notes : [];
  renderNotes();
  elements.notesState.textContent = `${state.notesData.length} shared note${state.notesData.length === 1 ? "" : "s"}`;
}

async function refreshProfile() {
  if (!state.user.is) {
    return;
  }
  const profile = await state.user.get("profile").once();
  elements.displayNameInput.value = profile?.display_name ?? "";
  elements.taglineInput.value = profile?.tagline ?? "";
}

function renderNotes() {
  const notes = [...state.notesData].sort(
    (left, right) => (right.created_at ?? 0) - (left.created_at ?? 0),
  );

  elements.noteList.replaceChildren();
  elements.emptyState.hidden = notes.length > 0;

  for (const note of notes) {
    const fragment = elements.noteTemplate.content.cloneNode(true);
    const item = fragment.querySelector(".note-card");
    const title = fragment.querySelector(".note-title");
    const author = fragment.querySelector(".note-author");
    const body = fragment.querySelector(".note-body");
    const time = fragment.querySelector(".note-time");
    const toggle = fragment.querySelector(".toggle-note");

    item.classList.toggle("is-done", Boolean(note.done));
    title.textContent = note.title ?? "Untitled";
    author.textContent = note.author ? `by ${note.author}` : "anonymous";
    body.textContent = note.body || "No body";
    time.textContent = formatTimestamp(note.updated_at ?? note.created_at);
    toggle.textContent = note.done ? "Mark Open" : "Mark Done";
    toggle.addEventListener("click", () => {
      if (!note._id) {
        return;
      }
      state.gun.get(note._id).put({
        ...note,
        done: !note.done,
        updated_at: Date.now(),
      });
      elements.runtimeState.textContent = "updated note";
    });

    elements.noteList.append(fragment);
  }
}

async function createUser() {
  return new Promise((resolve) => {
    state.user.create(
      elements.aliasInput.value.trim(),
      elements.passwordInput.value,
      (ack) => resolve(ack),
      { remember: true },
    );
  });
}

async function loginUser() {
  return new Promise((resolve) => {
    state.user.auth(
      elements.aliasInput.value.trim(),
      elements.passwordInput.value,
      (ack) => resolve(ack),
      { remember: true },
    );
  });
}

async function recallUser() {
  return new Promise((resolve) => {
    state.user.recall({ sessionStorage: true }, (ack) => resolve(ack));
  });
}

function updateUi() {
  const stats = state.gun.stats();
  elements.peerCount.textContent = `${stats.peers}`;
  elements.pendingCount.textContent = `${stats.pending}`;
  elements.inflightCount.textContent = `${stats.inflight}`;
  elements.authStatus.textContent = state.user.is
    ? `${state.user.is.alias} (${state.user.is.pub.slice(0, 12)}...)`
    : "signed out";
  elements.runtimeState.textContent = stats.peers
    ? `connected to ${stats.peers} discovered peer${stats.peers === 1 ? "" : "s"}`
    : "waiting for another browser";
  elements.profileForm
    .querySelectorAll("input, textarea, button")
    .forEach((element) => (element.disabled = !state.user.is));
  elements.noteForm
    .querySelectorAll("input, textarea, button")
    .forEach((element) => (element.disabled = !state.user.is));
}

function teardown() {
  if (state.statusTimer) {
    clearInterval(state.statusTimer);
  }
  state.profileSubscription?.off();
  state.notesSubscription?.off();
  state.gun?.close();
}

function formatTimestamp(value) {
  if (!value) {
    return "unknown time";
  }
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}
