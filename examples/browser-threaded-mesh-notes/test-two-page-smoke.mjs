#!/usr/bin/env node

import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import process from "node:process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));

const ROOT_URL =
  process.env.PRIMADB_THREADED_MESH_URL ??
  "http://127.0.0.1:4175/examples/browser-threaded-mesh-notes/";
const SERVER_PORT = Number(process.env.PRIMADB_THREADED_MESH_PORT ?? "4175");
const RELAY_ADDR = process.env.PRIMADB_THREADED_MESH_RELAY_ADDR ?? "127.0.0.1:9010";
const RELAY_URL = process.env.PRIMADB_THREADED_MESH_RELAY_URL ?? `ws://${RELAY_ADDR}`;
const RELAY_PORT = Number(RELAY_ADDR.split(":").at(-1) ?? "9010");
const SERVER_ROOT =
  process.env.PRIMADB_THREADED_MESH_ROOT ?? resolve(SCRIPT_DIR, "../..");
const CHROME_PATH =
  process.env.PLAYWRIGHT_BROWSER_PATH ?? "/usr/bin/google-chrome-stable";
const ROOM = process.env.PRIMADB_THREADED_MESH_ROOM ?? `smoke-${Date.now()}`;

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

  const child = spawn("python3", ["examples/browser-threaded-mesh-notes/serve.py"], {
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

  throw new Error(`Timed out waiting for threaded mesh server on port ${SERVER_PORT}`);
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
  if (existsSync(`${SERVER_ROOT}/examples/browser-threaded-mesh-notes/pkg/primadb.js`)) {
    return;
  }

  await new Promise((resolve, reject) => {
    const child = spawn("bash", ["examples/browser-threaded-mesh-notes/build.sh"], {
      cwd: SERVER_ROOT,
      stdio: "inherit",
    });
    child.once("exit", (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`threaded mesh build failed with exit code ${code}`));
      }
    });
  });
}

function parseJsonBlock(text) {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

function killDetached(child) {
  if (child?.pid) {
    try {
      process.kill(-child.pid, "SIGTERM");
    } catch {}
  }
}

async function main() {
  await maybeBuild();
  const { chromium } = await loadPlaywright();
  const server = await ensureServer();
  const relay = await ensureRelay();
  let browser = null;

  if (!existsSync(CHROME_PATH)) {
    throw new Error(`Browser executable not found at ${CHROME_PATH}`);
  }

  browser = await chromium.launch({
    headless: true,
    executablePath: CHROME_PATH,
  });

  try {
    const context = await browser.newContext();
    const page1 = await context.newPage();
    const page2 = await context.newPage();
    const url = `${ROOT_URL}?room=${encodeURIComponent(ROOM)}&signal=relay&relay=${encodeURIComponent(RELAY_URL)}`;
    const noteTitle = `Threaded mesh note ${Date.now()}`;

    const waitForReady = async (page) => {
      await page.goto(url, { waitUntil: "networkidle" });
      await page.waitForFunction(() => {
        const build = document.querySelector("#build-mode")?.textContent ?? "";
        const signaling = document.querySelector("#signaling-mode")?.textContent ?? "";
        const relay = document.querySelector("#relay-status")?.textContent ?? "";
        const threads = Number(document.querySelector("#thread-count")?.textContent ?? "0");
        const persistence = document.querySelector("#persistence-status")?.textContent ?? "";
        const seeded = Number(document.querySelector("#seed-count")?.textContent ?? "0");
        return (
          build === "wasm-threads" &&
          signaling.includes("relay") &&
          relay === "connected" &&
          threads >= 1 &&
          persistence.length > 0 &&
          seeded >= 1 &&
          globalThis.crossOriginIsolated === true
        );
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

    await page1.locator("#note-title").fill(noteTitle);
    await page1.locator("#note-body").fill("Two-page threaded mesh smoke test");
    await page1.getByRole("button", { name: "Add note" }).click();

    await page2.waitForFunction((expectedTitle) => {
      return [...document.querySelectorAll(".note-title")].some(
        (node) => node.textContent === expectedTitle,
      );
    }, noteTitle, { timeout: 30_000 });
    const liveReplicated = (await page2.locator(".note-title", { hasText: noteTitle }).count()) >= 1;

    const binaryPayload = [5, 8, 13, 21, 34];
    await page1.evaluate(async ({ room, payload }) => {
      const demo = globalThis.threadedMeshDemo;
      demo.db.chain("binary").field(room).field("bytes").putBytes(new Uint8Array(payload));
      demo.mesh.flushPending();
    }, { room: ROOM, payload: binaryPayload });
    await page2.waitForFunction(
      ({ room, payload }) => {
        const bytes = globalThis.threadedMeshDemo.db.chain("binary").field(room).field("bytes").onceBytes();
        return bytes != null && JSON.stringify(Array.from(bytes)) === JSON.stringify(payload);
      },
      { room: ROOM, payload: binaryPayload },
      { timeout: 30_000 },
    );

    const blobPayload = [144, 1, 2, 3, 5, 8, 13];
    const blobReference = await page1.evaluate(async ({ room, payload }) => {
      const demo = globalThis.threadedMeshDemo;
      demo.db.enableIndexedDbBlobStorage("primadb-threaded-mesh-binary", "blobs", room);
      return await demo.db
        .chain("binary")
        .field(room)
        .field("blob")
        .putBlob(new Uint8Array(payload), "application/octet-stream");
    }, { room: ROOM, payload: blobPayload });
    await page2.waitForFunction(
      (room) => globalThis.threadedMeshDemo.db.chain("binary").field(room).field("blob").blobRef() != null,
      ROOM,
      { timeout: 30_000 },
    );
    const restoredBlob = await page2.evaluate(async ({ room }) => {
      const demo = globalThis.threadedMeshDemo;
      demo.db.enableIndexedDbBlobStorage("primadb-threaded-mesh-binary", "blobs", room);
      const bytes = await demo.db.chain("binary").field(room).field("blob").getBlob();
      return bytes == null ? null : Array.from(bytes);
    }, { room: ROOM });

    await page1.getByRole("button", { name: "Seed Shared Load" }).click();
    await page2.waitForFunction(
      (expectedSeeded) => Number(document.querySelector("#seed-count")?.textContent ?? "0") >= expectedSeeded,
      321,
      { timeout: 60_000 },
    );

    await page2.getByRole("button", { name: "Run Parallel Query" }).click();
    await page2.waitForFunction(() => {
      const raw = document.querySelector("#query-output")?.textContent ?? "";
      try {
        const parsed = JSON.parse(raw);
        return (
          parsed.parallelEnabled === true &&
          Number(parsed.parallelThreadCount) >= 1 &&
          Number(parsed.openPeers) >= 1 &&
          Number(parsed.seeded) >= 321 &&
          Number(parsed.queryMatches) >= 320
        );
      } catch {
        return false;
      }
    }, { timeout: 30_000 });

    const result = {
      url,
      room: ROOM,
      build1: await page1.locator("#build-mode").textContent(),
      build2: await page2.locator("#build-mode").textContent(),
      signaling1: await page1.locator("#signaling-mode").textContent(),
      signaling2: await page2.locator("#signaling-mode").textContent(),
      relay1: await page1.locator("#relay-status").textContent(),
      relay2: await page2.locator("#relay-status").textContent(),
      threads1: Number(await page1.locator("#thread-count").textContent()),
      threads2: Number(await page2.locator("#thread-count").textContent()),
      peers1: Number(await page1.locator("#peer-count").textContent()),
      peers2: Number(await page2.locator("#peer-count").textContent()),
      seeded2: Number(await page2.locator("#seed-count").textContent()),
      live_note_replicated: liveReplicated,
      bytes_replicated: JSON.stringify(
        await page2.evaluate(
          (room) => {
            const bytes = globalThis.threadedMeshDemo.db.chain("binary").field(room).field("bytes").onceBytes();
            return bytes == null ? null : Array.from(bytes);
          },
          ROOM,
        ),
      ) === JSON.stringify(binaryPayload),
      blob_reference: blobReference,
      blob_restored: JSON.stringify(restoredBlob) === JSON.stringify(blobPayload),
      query: parseJsonBlock(await page2.locator("#query-output").textContent()),
      threaded_p2p_confirmed: liveReplicated,
    };

    console.log(JSON.stringify(result, null, 2));

    if (!result.threaded_p2p_confirmed || !result.bytes_replicated || !result.blob_restored) {
      throw new Error("Threaded mesh binary smoke did not complete successfully");
    }

  } finally {
    if (browser) {
      await browser.close();
    }
    if (relay.started) {
      killDetached(relay.process);
    }
    if (server.started) {
      killDetached(server.process);
    }
  }
}

main().catch((error) => {
  console.error(error.stack || String(error));
  process.exitCode = 1;
});
