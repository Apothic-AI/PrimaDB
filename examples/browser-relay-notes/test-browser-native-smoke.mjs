#!/usr/bin/env node

import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import process from "node:process";

const require = createRequire(import.meta.url);

const ROOT_URL =
  process.env.PRIMADB_RELAY_URL ??
  "http://127.0.0.1:4173/examples/browser-relay-notes/";
const SERVER_PORT = Number(process.env.PRIMADB_RELAY_PORT ?? "4173");
const RELAY_ADDR = process.env.PRIMADB_RELAY_WS_ADDR ?? "127.0.0.1:9010";
const RELAY_URL = process.env.PRIMADB_RELAY_WS_URL ?? `ws://${RELAY_ADDR}`;
const RELAY_PORT = Number(RELAY_ADDR.split(":").at(-1) ?? "9010");
const SERVER_ROOT =
  process.env.PRIMADB_RELAY_ROOT ?? "/home/bitnom/Code/gunport/primadb";
const CHROME_PATH =
  process.env.PLAYWRIGHT_BROWSER_PATH ?? "/usr/bin/google-chrome-stable";

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

  throw new Error(`Timed out waiting for relay example server on port ${SERVER_PORT}`);
}

async function ensureRelay() {
  if (await isPortOpen(RELAY_PORT)) {
    return { process: null, started: false };
  }

  const child = spawn(
    "cargo",
    ["run", "--example", "ws_relay_server", "--", RELAY_ADDR],
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
  if (existsSync(`${SERVER_ROOT}/examples/browser-relay-notes/pkg/primadb.js`)) {
    return;
  }

  await new Promise((resolve, reject) => {
    const child = spawn("bash", ["examples/browser-relay-notes/build.sh"], {
      cwd: SERVER_ROOT,
      stdio: "inherit",
    });
    child.once("exit", (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`relay build failed with exit code ${code}`));
      }
    });
  });
}

async function runNativeRelayProbe(args) {
  return await new Promise((resolve, reject) => {
    const child = spawn(
      "cargo",
      [
        "run",
        "--quiet",
        "--features",
        "native-websocket",
        "--example",
        "native_relay_probe",
        "--",
        ...args,
      ],
      {
        cwd: SERVER_ROOT,
        stdio: ["ignore", "pipe", "pipe"],
      },
    );

    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    child.once("exit", (code) => {
      if (code === 0) {
        try {
          resolve(JSON.parse(stdout));
        } catch (error) {
          reject(new Error(`native relay probe did not produce JSON: ${stdout}\n${error}`));
        }
      } else {
        reject(new Error(`native relay probe failed with ${code}\n${stderr}`));
      }
    });
  });
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
    const nativeToBrowserTitle = `Native->Browser relay ${Date.now()}`;

    await page.goto(ROOT_URL, { waitUntil: "networkidle" });
    await page.waitForFunction(() => {
      const persistence = document.querySelector("#persistence-status")?.textContent ?? "";
      const relay = document.querySelector("#relay-status")?.textContent ?? "";
      return persistence.length > 0 && relay === "connected";
    }, { timeout: 30_000 });

    await page.waitForFunction(
      () => Number(document.querySelector("#peer-count")?.textContent ?? "0") >= 1,
      { timeout: 45_000 },
    );

    const nativeWriteResult = await runNativeRelayProbe([
      "--relay", RELAY_URL,
      "--board", "shared",
      "--replica", "native-relay-writer",
      "--action", "write-note",
      "--title", nativeToBrowserTitle,
      "--body", "native to browser relay smoke",
      "--timeout-ms", "45000",
      "--hold-ms", "10000",
      "--expected-peers", "0",
    ]);

    await page.waitForFunction((expectedTitle) => {
      return [...document.querySelectorAll(".note-title")].some(
        (node) => node.textContent === expectedTitle,
      );
    }, nativeToBrowserTitle, { timeout: 45_000 });

    const result = {
      url: ROOT_URL,
      browser: {
        relay: await page.locator("#relay-status").textContent(),
        peers: Number(await page.locator("#peer-count").textContent()),
      },
      native_to_browser: nativeWriteResult,
      cross_platform_relay_confirmed: true,
    };

    console.log(JSON.stringify(result, null, 2));
  } finally {
    await browser.close();
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
