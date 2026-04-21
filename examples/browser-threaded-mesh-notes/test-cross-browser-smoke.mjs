#!/usr/bin/env node

import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import process from "node:process";

const require = createRequire(import.meta.url);

const ROOT_URL =
  process.env.PRIMADB_THREADED_MESH_URL ??
  "http://127.0.0.1:4175/examples/browser-threaded-mesh-notes/";
const SERVER_PORT = Number(process.env.PRIMADB_THREADED_MESH_PORT ?? "4175");
const RELAY_ADDR = process.env.PRIMADB_THREADED_MESH_RELAY_ADDR ?? "127.0.0.1:9010";
const RELAY_URL = process.env.PRIMADB_THREADED_MESH_RELAY_URL ?? `ws://${RELAY_ADDR}`;
const RELAY_PORT = Number(RELAY_ADDR.split(":").at(-1) ?? "9010");
const SERVER_ROOT =
  process.env.PRIMADB_THREADED_MESH_ROOT ??
  "/home/bitnom/Code/gunport/primadb";
const CHROME_PATH =
  process.env.PLAYWRIGHT_BROWSER_PATH ?? "/usr/bin/google-chrome-stable";
const FIREFOX_PATH = process.env.PLAYWRIGHT_FIREFOX_PATH;
const ROOM = process.env.PRIMADB_THREADED_MESH_ROOM ?? `cross-browser-${Date.now()}`;

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
  const { chromium, firefox } = await loadPlaywright();
  const server = await ensureServer();
  const relay = await ensureRelay();
  let chrome = null;
  let fox = null;

  try {
    const chromiumOptions = { headless: true };
    if (existsSync(CHROME_PATH)) {
      chromiumOptions.executablePath = CHROME_PATH;
    }
    chrome = await chromium.launch(chromiumOptions);
    const firefoxOptions = { headless: true };
    if (FIREFOX_PATH && existsSync(FIREFOX_PATH)) {
      firefoxOptions.executablePath = FIREFOX_PATH;
    }
    fox = await firefox.launch(firefoxOptions);

    const chromiumContext = await chrome.newContext();
    const firefoxContext = await fox.newContext();
    const chromePage = await chromiumContext.newPage();
    const firefoxPage = await firefoxContext.newPage();
    const url = `${ROOT_URL}?room=${encodeURIComponent(ROOM)}&signal=relay&relay=${encodeURIComponent(RELAY_URL)}`;
    const noteTitle = `Cross-browser note ${Date.now()}`;

    const waitForReady = async (page) => {
      await page.goto(url, { waitUntil: "networkidle" });
      await page.waitForFunction(() => {
        const build = document.querySelector("#build-mode")?.textContent ?? "";
        const signaling = document.querySelector("#signaling-mode")?.textContent ?? "";
        const relay = document.querySelector("#relay-status")?.textContent ?? "";
        const threads = Number(document.querySelector("#thread-count")?.textContent ?? "0");
        const persistence = document.querySelector("#persistence-status")?.textContent ?? "";
        return (
          build === "wasm-threads" &&
          signaling.includes("relay") &&
          relay === "connected" &&
          threads >= 1 &&
          persistence.length > 0 &&
          globalThis.crossOriginIsolated === true
        );
      }, { timeout: 45_000 });
    };

    await waitForReady(chromePage);
    await waitForReady(firefoxPage);

    await Promise.all([
      chromePage.waitForFunction(() => Number(document.querySelector("#peer-count")?.textContent ?? "0") >= 1, { timeout: 45_000 }),
      firefoxPage.waitForFunction(() => Number(document.querySelector("#peer-count")?.textContent ?? "0") >= 1, { timeout: 45_000 }),
      chromePage.waitForFunction(() => (document.querySelector("#mesh-detail")?.textContent ?? "").includes("connected to 1 peer"), { timeout: 45_000 }),
      firefoxPage.waitForFunction(() => (document.querySelector("#mesh-detail")?.textContent ?? "").includes("connected to 1 peer"), { timeout: 45_000 }),
    ]);

    await chromePage.locator("#note-title").fill(noteTitle);
    await chromePage.locator("#note-body").fill("Cross-browser threaded mesh smoke test");
    await chromePage.getByRole("button", { name: "Add note" }).click();

    await firefoxPage.waitForFunction((expectedTitle) => {
      return [...document.querySelectorAll(".note-title")].some(
        (node) => node.textContent === expectedTitle,
      );
    }, noteTitle, { timeout: 45_000 });

    await firefoxPage.getByRole("button", { name: "Run Parallel Query" }).click();
    await firefoxPage.waitForFunction(() => {
      const raw = document.querySelector("#query-output")?.textContent ?? "";
      try {
        const parsed = JSON.parse(raw);
        return (
          parsed.parallelEnabled === true &&
          Number(parsed.parallelThreadCount) >= 1 &&
          Number(parsed.openPeers) >= 1 &&
          Number(parsed.queryMatches) >= 1
        );
      } catch {
        return false;
      }
    }, { timeout: 30_000 });

    const result = {
      url,
      room: ROOM,
      chromium: {
        signaling: await chromePage.locator("#signaling-mode").textContent(),
        relay: await chromePage.locator("#relay-status").textContent(),
        peers: Number(await chromePage.locator("#peer-count").textContent()),
      },
      firefox: {
        signaling: await firefoxPage.locator("#signaling-mode").textContent(),
        relay: await firefoxPage.locator("#relay-status").textContent(),
        peers: Number(await firefoxPage.locator("#peer-count").textContent()),
        query: parseJsonBlock(await firefoxPage.locator("#query-output").textContent()),
      },
      cross_browser_live_replication: true,
    };

    console.log(JSON.stringify(result, null, 2));
  } finally {
    if (chrome) {
      await chrome.close();
    }
    if (fox) {
      await fox.close();
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
