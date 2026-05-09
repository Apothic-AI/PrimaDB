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
  process.env.PRIMADB_PACKAGE_THREADED_MESH_URL ??
  "http://127.0.0.1:4181/threaded-mesh/";
const SERVER_PORT = Number(process.env.PRIMADB_PACKAGE_PORT ?? "4181");
const CHROME_PATH =
  process.env.PLAYWRIGHT_BROWSER_PATH ?? "/usr/bin/google-chrome-stable";
const ROOM = process.env.PRIMADB_PACKAGE_THREADED_MESH_ROOM ?? `package-threaded-${Date.now()}`;

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

async function ensureBuild() {
  if (
    existsSync(`${PACKAGE_ROOT}/dist/threads.js`) &&
    existsSync(`${PACKAGE_ROOT}/dist/vendor/threads/primadb.js`)
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
    const page = await context.newPage();
    const url = `${ROOT_URL}?room=${encodeURIComponent(ROOM)}`;
    const title = `Package threaded smoke ${Date.now()}`;

    async function waitForReady() {
      await page.goto(url, { waitUntil: "networkidle" });
      await page.waitForFunction(() => {
        const build = document.querySelector("#build-status")?.textContent ?? "";
        const mesh = document.querySelector("#mesh-status")?.textContent ?? "";
        const demo = globalThis.threadedPackageDemo;
        return (
          build.includes("wasm-threads") &&
          mesh.length > 0 &&
          demo != null &&
          demo.storageStatus?.ready === true &&
          globalThis.crossOriginIsolated === true
        );
      }, { timeout: 30_000 });
    }

    await waitForReady();

    const initialLogText = await page.locator("#repl-logs").innerText();
    if (initialLogText.includes("durable storage init failed")) {
      throw new Error("threaded package example reported durable storage init failure");
    }

    await page.locator("#card-title").fill(title);
    await page.locator("#card-body").fill("IndexedDB segment persistence smoke");
    await page.getByRole("button", { name: "Add Shared Card" }).click();

    await page.waitForFunction(
      (expectedTitle) => {
        const demo = globalThis.threadedPackageDemo;
        return (
          [...document.querySelectorAll("#cards-list h3")].some(
            (node) => node.textContent === expectedTitle,
          ) &&
          demo?.lastPersist?.ok === true
        );
      },
      title,
      { timeout: 30_000 },
    );

    await page.reload({ waitUntil: "networkidle" });
    await page.waitForFunction(
      (expectedTitle) => {
        const demo = globalThis.threadedPackageDemo;
        return (
          demo?.storageStatus?.ready === true &&
          [...document.querySelectorAll("#cards-list h3")].some(
            (node) => node.textContent === expectedTitle,
          )
        );
      },
      title,
      { timeout: 30_000 },
    );

    const reloadedLogText = await page.locator("#repl-logs").innerText();
    if (reloadedLogText.includes("durable storage init failed")) {
      throw new Error("threaded package example reported durable storage init failure after reload");
    }

    const result = await page.evaluate(() => ({
      build: document.querySelector("#build-status")?.textContent ?? "",
      mesh: document.querySelector("#mesh-status")?.textContent ?? "",
      peerStatus: document.querySelector("#peer-status")?.textContent ?? "",
      cardCount: document.querySelector("#card-count")?.textContent ?? "0",
      storageStatus: globalThis.threadedPackageDemo?.storageStatus ?? null,
    }));
    console.log(JSON.stringify({ url, room: ROOM, title, persisted: true, result }, null, 2));
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
