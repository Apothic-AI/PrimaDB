#!/usr/bin/env node

import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import process from "node:process";

const require = createRequire(import.meta.url);

const ROOT_URL =
  process.env.PRIMADB_MESH_URL ??
  "http://127.0.0.1:4173/examples/browser-mesh-notes/";
const SERVER_PORT = Number(process.env.PRIMADB_MESH_PORT ?? "4173");
const RELAY_ADDR = process.env.PRIMADB_MESH_RELAY_ADDR ?? "127.0.0.1:9010";
const RELAY_URL = process.env.PRIMADB_MESH_RELAY_URL ?? `ws://${RELAY_ADDR}`;
const RELAY_PORT = Number(RELAY_ADDR.split(":").at(-1) ?? "9010");
const SERVER_ROOT =
  process.env.PRIMADB_MESH_ROOT ?? "/home/bitnom/Code/gunport/primadb";
const CHROME_PATH =
  process.env.PLAYWRIGHT_BROWSER_PATH ?? "/usr/bin/google-chrome-stable";
const ROOM = process.env.PRIMADB_MESH_ROOM ?? `mesh-smoke-${Date.now()}`;

async function loadPlaywright() {
  const override = process.env.PLAYWRIGHT_MODULE_PATH;
  const candidates = [
    override,
    "playwright",
    "/tmp/primadb-playwright/node_modules/playwright",
  ].filter(Boolean);
  for (const candidate of candidates) {
    try {
      return require(candidate);
    } catch {
      continue;
    }
  }
  throw new Error(
    "Could not resolve the `playwright` module. Set PLAYWRIGHT_MODULE_PATH or install playwright.",
  );
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function isPortOpen(port) {
  const net = await import("node:net");
  return await new Promise((resolve) => {
    const socket = net.createConnection({ host: "127.0.0.1", port });
    socket.once("connect", () => {
      socket.destroy();
      resolve(true);
    });
    socket.once("error", () => resolve(false));
  });
}

async function ensureServer() {
  if (await isPortOpen(SERVER_PORT)) {
    return { process: null, started: false };
  }

  const child = spawn("python3", ["-m", "http.server", String(SERVER_PORT)], {
    cwd: SERVER_ROOT,
    stdio: "ignore",
    detached: true,
  });
  child.unref();

  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    if (await isPortOpen(SERVER_PORT)) {
      return { process: child, started: true };
    }
    await wait(100);
  }

  throw new Error(`Timed out waiting for mesh server on port ${SERVER_PORT}`);
}

async function ensureRelay() {
  if (await isPortOpen(RELAY_PORT)) {
    return { process: null, started: false };
  }

  const child = spawn(
    "cargo",
    ["run", "--features", "native-websocket", "--example", "ws_relay_server", "--", RELAY_ADDR],
    {
      cwd: SERVER_ROOT,
      stdio: "ignore",
      detached: true,
    },
  );
  child.unref();

  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if (await isPortOpen(RELAY_PORT)) {
      return { process: child, started: true };
    }
    await wait(200);
  }

  throw new Error(`Timed out waiting for relay on ${RELAY_ADDR}`);
}

async function maybeBuild() {
  if (existsSync(`${SERVER_ROOT}/examples/browser-mesh-notes/pkg/primadb.js`)) {
    return;
  }

  await new Promise((resolve, reject) => {
    const child = spawn("bash", ["examples/browser-mesh-notes/build.sh"], {
      cwd: SERVER_ROOT,
      stdio: "inherit",
    });
    child.once("exit", (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`mesh build failed with exit code ${code}`));
      }
    });
  });
}

async function main() {
  await maybeBuild();
  const { chromium } = await loadPlaywright();
  const server = await ensureServer();
  const relay = await ensureRelay();

  if (!existsSync(CHROME_PATH)) {
    throw new Error(`Browser executable not found at ${CHROME_PATH}`);
  }

  const browser = await chromium.launch({
    headless: true,
    executablePath: CHROME_PATH,
  });

  try {
    const context = await browser.newContext();
    const page1 = await context.newPage();
    const page2 = await context.newPage();
    const url = `${ROOT_URL}?room=${encodeURIComponent(ROOM)}&signal=relay&relay=${encodeURIComponent(RELAY_URL)}`;
    const noteTitle = `Mesh note ${Date.now()}`;

    const waitForReady = async (page) => {
      await page.goto(url, { waitUntil: "networkidle" });
      await page.waitForFunction(() => {
        const persistence = document.querySelector("#persistence-status")?.textContent ?? "";
        const signaling = document.querySelector("#signaling-mode")?.textContent ?? "";
        const relay = document.querySelector("#relay-status")?.textContent ?? "";
        const count = document.querySelectorAll(".note-title").length;
        return persistence.length > 0 && signaling.includes("relay") && relay === "connected" && count >= 1;
      }, { timeout: 30_000 });
    };

    await waitForReady(page1);
    await waitForReady(page2);

    await Promise.all([
      page1.waitForFunction(() => Number(document.querySelector("#peer-count")?.textContent ?? "0") >= 1, { timeout: 30_000 }),
      page2.waitForFunction(() => Number(document.querySelector("#peer-count")?.textContent ?? "0") >= 1, { timeout: 30_000 }),
      page1.waitForFunction(() => (document.querySelector("#mesh-detail")?.textContent ?? "").includes("connected to 1 peer"), { timeout: 30_000 }),
      page2.waitForFunction(() => (document.querySelector("#mesh-detail")?.textContent ?? "").includes("connected to 1 peer"), { timeout: 30_000 }),
    ]);

    const before = await page2.locator(".note-title").count();

    await page1.locator("#note-title").fill(noteTitle);
    await page1.locator("#note-body").fill("Two-page default mesh smoke test");
    await page1.getByRole("button", { name: "Add note" }).click();

    await page2.waitForFunction((expectedTitle) => {
      return [...document.querySelectorAll(".note-title")].some(
        (node) => node.textContent === expectedTitle,
      );
    }, noteTitle, { timeout: 30_000 });

    const binaryPayload = [0, 9, 42, 255, 7];
    await page1.evaluate(async ({ room, payload }) => {
      const demo = globalThis.meshDemo;
      const chain = demo.db.chain("binary").field(room).field("bytes");
      chain.putBytes(new Uint8Array(payload));
      demo.mesh.flushPending();
    }, { room: ROOM, payload: binaryPayload });
    await page2.waitForFunction(
      ({ room, payload }) => {
        const demo = globalThis.meshDemo;
        const bytes = demo.db.chain("binary").field(room).field("bytes").onceBytes();
        return bytes != null && JSON.stringify(Array.from(bytes)) === JSON.stringify(payload);
      },
      { room: ROOM, payload: binaryPayload },
      { timeout: 30_000 },
    );

    const blobPayload = [13, 37, 99, 4, 77, 18];
    const blobReference = await page1.evaluate(async ({ room, payload }) => {
      const demo = globalThis.meshDemo;
      demo.db.enableIndexedDbBlobStorage("primadb-browser-mesh-binary", "blobs", room);
      return await demo.db
        .chain("binary")
        .field(room)
        .field("blob")
        .putBlob(new Uint8Array(payload), "application/octet-stream");
    }, { room: ROOM, payload: blobPayload });
    await page2.waitForFunction(
      (room) => globalThis.meshDemo.db.chain("binary").field(room).field("blob").blobRef() != null,
      ROOM,
      { timeout: 30_000 },
    );
    const restoredBlob = await page2.evaluate(async ({ room }) => {
      const demo = globalThis.meshDemo;
      demo.db.enableIndexedDbBlobStorage("primadb-browser-mesh-binary", "blobs", room);
      const bytes = await demo.db.chain("binary").field(room).field("blob").getBlob();
      return bytes == null ? null : Array.from(bytes);
    }, { room: ROOM });

    const after = await page2.locator(".note-title").count();
    const result = {
      url,
      room: ROOM,
      persistence1: await page1.locator("#persistence-status").textContent(),
      persistence2: await page2.locator("#persistence-status").textContent(),
      signaling1: await page1.locator("#signaling-mode").textContent(),
      signaling2: await page2.locator("#signaling-mode").textContent(),
      relay1: await page1.locator("#relay-status").textContent(),
      relay2: await page2.locator("#relay-status").textContent(),
      peers1: Number(await page1.locator("#peer-count").textContent()),
      peers2: Number(await page2.locator("#peer-count").textContent()),
      before,
      after,
      title: noteTitle,
      binaryPayload,
      blobReference,
      blobPayload,
      blobRestored: restoredBlob,
      live_note_replicated: after >= before + 1,
      bytes_replicated: JSON.stringify(restoredBlob == null ? [] : restoredBlob) === JSON.stringify(blobPayload),
      default_p2p_confirmed: after >= before + 1,
    };

    result.bytes_replicated =
      JSON.stringify(
        await page2.evaluate(
          (room) => {
            const bytes = globalThis.meshDemo.db.chain("binary").field(room).field("bytes").onceBytes();
            return bytes == null ? null : Array.from(bytes);
          },
          ROOM,
        ),
      ) === JSON.stringify(binaryPayload);
    result.blob_restored = JSON.stringify(restoredBlob) === JSON.stringify(blobPayload);

    console.log(JSON.stringify(result, null, 2));

    if (!result.default_p2p_confirmed || !result.bytes_replicated || !result.blob_restored) {
      throw new Error("Default mesh binary smoke did not complete successfully");
    }

  } finally {
    await browser.close();
    if (relay.started && relay.process?.pid) {
      process.kill(-relay.process.pid, "SIGTERM");
    }
    if (server.started && server.process?.pid) {
      process.kill(-server.process.pid, "SIGTERM");
    }
  }
}

main().catch((error) => {
  console.error(error.stack || String(error));
  process.exitCode = 1;
});
