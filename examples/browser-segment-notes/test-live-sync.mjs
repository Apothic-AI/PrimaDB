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
  process.env.PRIMADB_SEGMENT_NOTES_URL ??
  "http://127.0.0.1:4176/examples/browser-segment-notes/";
const SERVER_PORT = Number(process.env.PRIMADB_SEGMENT_NOTES_PORT ?? "4176");
const SERVER_ROOT =
  process.env.PRIMADB_SEGMENT_NOTES_ROOT ?? resolve(SCRIPT_DIR, "../..");
const CHROME_PATH =
  process.env.PLAYWRIGHT_BROWSER_PATH ?? "/usr/bin/google-chrome-stable";

async function loadPlaywright() {
  const override = process.env.PLAYWRIGHT_MODULE_PATH;
  const fallbackPaths = [
    override,
    "playwright",
    "/tmp/primadb-playwright/node_modules/playwright",
  ].filter(Boolean);

  for (const candidate of fallbackPaths) {
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

  throw new Error(`Timed out waiting for local server on port ${SERVER_PORT}`);
}

async function main() {
  const { chromium } = await loadPlaywright();
  const server = await ensureServer();

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
    const title = `Segment live sync ${Date.now()}`;

    const waitForReady = async (page) => {
      await page.goto(ROOT_URL, { waitUntil: "networkidle" });
      await page.waitForFunction(() => {
        const persistence =
          document.querySelector("#persistence-status")?.textContent ?? "";
        const count = Number(
          document.querySelector("#task-count")?.textContent ?? "0",
        );
        return persistence.includes("indexeddb") && count >= 1;
      });
    };

    await waitForReady(page1);
    await waitForReady(page2);

    const before = Number(await page2.locator("#task-count").textContent());

    await page1.locator("#task-title").fill(title);
    await page1.locator("#task-note").fill("Two-page Playwright live sync check");
    await page1.getByRole("button", { name: "Add task" }).click();

    await page2.waitForFunction((expectedTitle) => {
      return [...document.querySelectorAll(".task-title")].some(
        (node) => node.textContent === expectedTitle,
      );
    }, title);

    const after = Number(await page2.locator("#task-count").textContent());
    const persistence = await page2.locator("#persistence-status").textContent();
    const restored = await page2
      .locator(".task-title", { hasText: title })
      .count();

    const result = {
      url: ROOT_URL,
      before,
      after,
      persistence,
      restored,
      title,
      live_delivery_confirmed: restored === 1 && after >= before + 1,
    };

    console.log(JSON.stringify(result, null, 2));

    if (!result.live_delivery_confirmed) {
      throw new Error("Live cross-tab delivery was not confirmed");
    }

    if (server.started && server.process?.pid) {
      process.kill(-server.process.pid, "SIGTERM");
    }
  } finally {
    await browser.close();
  }
}

main().catch((error) => {
  console.error(error.stack || String(error));
  process.exitCode = 1;
});
