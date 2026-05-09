#!/usr/bin/env node

import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import process from "node:process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));

const EXAMPLES_ROOT =
  process.env.PRIMADB_PACKAGE_EXAMPLES_ROOT ?? resolve(SCRIPT_DIR, "..");
const PACKAGE_ROOT =
  process.env.PRIMADB_PACKAGE_ROOT ?? resolve(SCRIPT_DIR, "../..");
const ROOT_URL =
  process.env.PRIMADB_PACKAGE_TEXT_VOICE_URL ??
  "http://127.0.0.1:4181/text-voice-chat/";
const SERVER_PORT = Number(process.env.PRIMADB_PACKAGE_PORT ?? "4181");
const CHROME_PATH =
  process.env.PLAYWRIGHT_BROWSER_PATH ?? "/usr/bin/google-chrome-stable";
const ROOM = process.env.PRIMADB_PACKAGE_TEXT_VOICE_ROOM ?? `package-chat-${Date.now()}`;

async function loadPlaywright() {
  const override = process.env.PLAYWRIGHT_MODULE_PATH;
  const candidates = [
    override,
    "playwright",
    "playwright-core",
    "/tmp/primadb-playwright/node_modules/playwright",
    "/tmp/primadb-playwright/node_modules/playwright-core",
  ].filter(Boolean);
  for (const candidate of candidates) {
    try {
      return require(candidate);
    } catch {
      continue;
    }
  }
  throw new Error(
    "Could not resolve the `playwright` module. Set PLAYWRIGHT_MODULE_PATH or install the examples dependencies.",
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

async function ensureBuild() {
  if (
    existsSync(`${PACKAGE_ROOT}/dist/index.js`) &&
    existsSync(`${PACKAGE_ROOT}/dist/vendor/default/primadb.js`)
  ) {
    return;
  }

  await new Promise((resolve, reject) => {
    const child = spawn("pnpm", ["--dir", "..", "run", "build"], {
      cwd: EXAMPLES_ROOT,
      stdio: "inherit",
    });
    child.once("exit", (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`package build failed with exit code ${code}`));
      }
    });
  });
}

async function ensurePreviewServer() {
  if (await isPortOpen(SERVER_PORT)) {
    if (process.env.PRIMADB_PACKAGE_REUSE_SERVER === "1") {
      return { process: null, started: false };
    }
    throw new Error(
      `Port ${SERVER_PORT} is already in use. Pick a free PRIMADB_PACKAGE_PORT or set PRIMADB_PACKAGE_REUSE_SERVER=1 to reuse the existing server.`,
    );
  }

  const child = spawn(
    "pnpm",
    ["exec", "vite", "preview", "--host", "127.0.0.1", "--port", String(SERVER_PORT), "--strictPort"],
    {
      cwd: EXAMPLES_ROOT,
      stdio: "ignore",
      detached: true,
    },
  );
  child.unref();

  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if (await isPortOpen(SERVER_PORT)) {
      return { process: child, started: true };
    }
    await wait(100);
  }

  throw new Error(`Timed out waiting for Vite preview server on port ${SERVER_PORT}`);
}

function killDetached(child) {
  if (child?.pid) {
    try {
      process.kill(-child.pid, "SIGTERM");
    } catch {}
  }
}

async function main() {
  const { chromium } = await loadPlaywright();
  await ensureBuild();
  const server = await ensurePreviewServer();

  if (!existsSync(CHROME_PATH)) {
    throw new Error(`Browser executable not found at ${CHROME_PATH}`);
  }

  const browser = await chromium.launch({
    headless: true,
    executablePath: CHROME_PATH,
  });

  try {
    const context = await browser.newContext();
    const alice = await context.newPage();
    const bob = await context.newPage();
    const sharedParams = `room=${encodeURIComponent(ROOM)}&signal=broadcast&capture=synthetic&chunkMs=200&windowMs=2500`;
    const aliceUrl = `${ROOT_URL}?${sharedParams}&name=alice&autostart=1&message=${encodeURIComponent("hello from alice")}`;
    const bobUrl = `${ROOT_URL}?${sharedParams}&name=bob&message=${encodeURIComponent("hello from bob")}`;

    await bob.goto(bobUrl, { waitUntil: "networkidle" });
    await alice.goto(aliceUrl, { waitUntil: "networkidle" });

    await bob.waitForFunction(
      () => [...document.querySelectorAll(".message p")].some((node) => node.textContent === "hello from alice"),
      { timeout: 30_000 },
    );
    await alice.waitForFunction(
      () => [...document.querySelectorAll(".message p")].some((node) => node.textContent === "hello from bob"),
      { timeout: 30_000 },
    );

    await alice.waitForFunction(
      () => globalThis.textVoiceChatDemo?.metrics.sentChunks >= 3,
      { timeout: 30_000 },
    );
    await bob.waitForFunction(
      () => globalThis.textVoiceChatDemo?.metrics.receivedChunks >= 2,
      { timeout: 45_000 },
    );

    const result = await Promise.all([
      alice.evaluate(() => ({
        mesh: document.querySelector("#mesh-status")?.textContent ?? "",
        messages: document.querySelectorAll(".message").length,
        sentChunks: globalThis.textVoiceChatDemo.metrics.sentChunks,
        sentBytes: globalThis.textVoiceChatDemo.metrics.sentBytes,
      })),
      bob.evaluate(() => ({
        mesh: document.querySelector("#mesh-status")?.textContent ?? "",
        messages: document.querySelectorAll(".message").length,
        receivedChunks: globalThis.textVoiceChatDemo.metrics.receivedChunks,
        receivedBytes: globalThis.textVoiceChatDemo.metrics.receivedBytes,
        remoteVoiceCards: document.querySelectorAll(".voice-card").length,
      })),
    ]);

    console.log(JSON.stringify({ room: ROOM, alice: result[0], bob: result[1] }, null, 2));
  } finally {
    await browser.close();
    if (server.started) {
      killDetached(server.process);
    }
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
