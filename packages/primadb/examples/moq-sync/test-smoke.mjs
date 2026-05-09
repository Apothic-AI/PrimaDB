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
const ROOT_URL = process.env.PRIMADB_PACKAGE_MOQ_URL ?? "http://127.0.0.1:4181/moq-sync/";
const SERVER_PORT = Number(process.env.PRIMADB_PACKAGE_PORT ?? "4181");
const CHROME_PATH = process.env.PLAYWRIGHT_BROWSER_PATH ?? "/usr/bin/google-chrome-stable";

async function loadPlaywright() {
  for (const candidate of [
    process.env.PLAYWRIGHT_MODULE_PATH,
    "playwright",
    "playwright-core",
    "/tmp/primadb-playwright/node_modules/playwright",
    "/tmp/primadb-playwright/node_modules/playwright-core",
  ].filter(Boolean)) {
    try {
      return require(candidate);
    } catch {}
  }
  throw new Error("Could not resolve playwright. Install example dependencies or set PLAYWRIGHT_MODULE_PATH.");
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function isPortOpen(port) {
  const net = await import("node:net");
  return new Promise((resolve) => {
    const socket = net.createConnection({ host: "127.0.0.1", port });
    socket.once("connect", () => {
      socket.destroy();
      resolve(true);
    });
    socket.once("error", () => resolve(false));
  });
}

async function ensurePreviewServer() {
  if (await isPortOpen(SERVER_PORT)) {
    if (process.env.PRIMADB_PACKAGE_REUSE_SERVER === "1") {
      return null;
    }
    throw new Error(`Port ${SERVER_PORT} is already in use. Set PRIMADB_PACKAGE_REUSE_SERVER=1 to reuse it.`);
  }
  const child = spawn(
    "pnpm",
    ["exec", "vite", "preview", "--host", "127.0.0.1", "--port", String(SERVER_PORT), "--strictPort"],
    { cwd: EXAMPLES_ROOT, stdio: "ignore", detached: true },
  );
  child.unref();
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if (await isPortOpen(SERVER_PORT)) {
      return child;
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
  const server = await ensurePreviewServer();
  if (!existsSync(CHROME_PATH)) {
    throw new Error(`Browser executable not found at ${CHROME_PATH}`);
  }
  const browser = await chromium.launch({ headless: true, executablePath: CHROME_PATH });
  try {
    const page = await browser.newPage();
    const room = `moq-smoke-${Date.now()}`;
    await page.goto(`${ROOT_URL}?room=${encodeURIComponent(room)}`, { waitUntil: "networkidle" });
    await page.waitForFunction(
      () => globalThis.primadbMoqExample?.subscriberEntries?.length > 0,
      { timeout: 30_000 },
    );
    const result = await page.evaluate(() => ({
      path: globalThis.primadbMoqExample.path,
      sent: globalThis.primadbMoqExample.sent,
      replicated: globalThis.primadbMoqExample.subscriberEntries.length,
    }));
    console.log(JSON.stringify(result, null, 2));
  } finally {
    await browser.close();
    killDetached(server);
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
