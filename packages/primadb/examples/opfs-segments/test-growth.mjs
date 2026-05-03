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
  process.env.PRIMADB_PACKAGE_OPFS_SEGMENTS_URL ?? "http://127.0.0.1:4181/opfs-segments/";
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
  const packageBuilt =
    existsSync(`${PACKAGE_ROOT}/dist/index.js`) &&
    existsSync(`${PACKAGE_ROOT}/dist/vendor/default/primadb.js`);
  if (!packageBuilt) {
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

  if (existsSync(`${EXAMPLES_ROOT}/dist/opfs-segments/index.html`)) {
    return;
  }

  await new Promise((resolve, reject) => {
    const child = spawn("pnpm", ["run", "build"], {
      cwd: EXAMPLES_ROOT,
      stdio: "inherit",
    });
    child.once("exit", (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`examples build failed with exit code ${code}`));
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
    const page = await browser.newPage();
    await page.goto(ROOT_URL, { waitUntil: "networkidle" });
    await page.waitForFunction(() => globalThis.opfsSegmentsRegression != null, {
      timeout: 30_000,
    });

    const namespace = `growth-${Date.now().toString(36)}`;
    const options = {
      namespace,
      seedCount: 32,
      seedSize: 4096,
      iterations: 8,
      checkpointSize: 64 * 1024,
    };
    const result = await page.evaluate((options) => {
      return globalThis.opfsSegmentsRegression.run(options);
    }, options);

    if (result.stats.fullReplacements !== 1) {
      throw new Error(
        `expected exactly one full replacement, saw ${result.stats.fullReplacements}`,
      );
    }
    if (result.stats.incrementalTransactions < result.iterations) {
      throw new Error(
        `expected at least ${result.iterations} incremental transactions, saw ${result.stats.incrementalTransactions}`,
      );
    }
    if (result.stats.failedWrites !== 0) {
      throw new Error(`expected zero failed writes, saw ${result.stats.failedWrites}`);
    }
    if (result.restoredLoaded !== true) {
      throw new Error("expected OPFS restore to load existing segment state");
    }
    const expectedPrefix = `${options.iterations - 1}:`;
    if (
      typeof result.restoredCheckpointPrefix !== "string" ||
      !result.restoredCheckpointPrefix.startsWith(expectedPrefix)
    ) {
      throw new Error(
        `restored checkpoint mismatch: expected prefix ${expectedPrefix}, saw ${result.restoredCheckpointPrefix}`,
      );
    }
    if (result.restoredCheckpointLength !== String(options.iterations - 1).length + 1 + options.checkpointSize) {
      throw new Error(
        `restored checkpoint length mismatch: saw ${result.restoredCheckpointLength}`,
      );
    }
    if (result.stats.lastEntriesWritten > 12) {
      throw new Error(
        `last incremental write touched too many entries: ${result.stats.lastEntriesWritten}`,
      );
    }
    if (result.stats.lastEntriesDeleted > 6) {
      throw new Error(
        `last incremental write deleted too many entries: ${result.stats.lastEntriesDeleted}`,
      );
    }
    if (result.stats.lastEstimatedBytesWritten > options.checkpointSize * 2) {
      throw new Error(
        `last incremental write was too large: ${result.stats.lastEstimatedBytesWritten}`,
      );
    }
    if (
      result.estimate.estimatedBytes >
      options.seedCount * options.seedSize + options.checkpointSize * 2
    ) {
      throw new Error(
        `logical OPFS namespace estimate grew too large: ${result.estimate.estimatedBytes}`,
      );
    }

    console.log(JSON.stringify({ url: ROOT_URL, result }, null, 2));
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
