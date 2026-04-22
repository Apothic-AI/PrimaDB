#!/usr/bin/env node

import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import process from "node:process";

const require = createRequire(import.meta.url);

const EXAMPLES_ROOT =
  process.env.PRIMADB_PACKAGE_EXAMPLES_ROOT ??
  "/home/bitnom/Code/gunport/primadb/packages/primadb/examples";
const PACKAGE_ROOT =
  process.env.PRIMADB_PACKAGE_ROOT ?? "/home/bitnom/Code/gunport/primadb/packages/primadb";
const ROOT_URL =
  process.env.PRIMADB_PACKAGE_DEFAULT_NOTES_URL ??
  "http://127.0.0.1:4181/default-notes/";
const SERVER_PORT = Number(process.env.PRIMADB_PACKAGE_PORT ?? "4181");
const CHROME_PATH =
  process.env.PLAYWRIGHT_BROWSER_PATH ?? "/usr/bin/google-chrome-stable";

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
    "Could not resolve the `playwright` or `playwright-core` module. Set PLAYWRIGHT_MODULE_PATH or install the examples dependencies.",
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
        reject(new Error(`example build failed with exit code ${code}`));
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
    const page = await context.newPage();
    const title = `Default notes smoke ${Date.now()}`;

    async function waitForReady() {
      await page.goto(ROOT_URL, { waitUntil: "networkidle" });
      await page.waitForFunction(() => {
        const demo = globalThis.defaultPackageDemo;
        const storage = document.querySelector("#storage-status")?.textContent ?? "";
        return demo != null && storage.includes("indexeddb_segments");
      }, { timeout: 30_000 });
    }

    await waitForReady();

    await page.locator("#note-title").fill(title);
    await page.locator("#note-body").fill("Persist me through IndexedDB segments");
    await page.getByRole("button", { name: "Add note" }).click();

    await page.waitForFunction(
      (expectedTitle) =>
        [...document.querySelectorAll("#notes-list h3")].some(
          (node) => node.textContent === expectedTitle,
        ),
      title,
      { timeout: 30_000 },
    );

    await page.reload({ waitUntil: "networkidle" });
    await page.waitForFunction(
      (expectedTitle) =>
        [...document.querySelectorAll("#notes-list h3")].some(
          (node) => node.textContent === expectedTitle,
        ),
      title,
      { timeout: 30_000 },
    );

    const result = await page.evaluate(() => ({
      replicaId: document.querySelector("#replica-id")?.textContent ?? "",
      storageStatus: document.querySelector("#storage-status")?.textContent ?? "",
      noteCount: document.querySelector("#note-count")?.textContent ?? "0",
    }));
    console.log(JSON.stringify({ url: ROOT_URL, title, persisted: true, result }, null, 2));
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
