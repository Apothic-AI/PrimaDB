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
const ROOM = process.env.PRIMADB_THREADED_MESH_ROOM ?? `threaded-native-${Date.now()}`;

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

async function runNativeMeshProbe(args) {
  return await new Promise((resolve, reject) => {
    const child = spawn(
      "cargo",
      [
        "run",
        "--quiet",
        "--features",
        "native-webrtc",
        "--example",
        "native_mesh_probe",
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
          reject(new Error(`native mesh probe did not produce JSON: ${stdout}\n${error}`));
        }
      } else {
        reject(new Error(`native mesh probe failed with ${code}\n${stderr}`));
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
    const url = `${ROOT_URL}?room=${encodeURIComponent(ROOM)}&signal=relay&relay=${encodeURIComponent(RELAY_URL)}`;
    const browserToNativeTitle = `Threaded Browser->Native ${Date.now()}`;
    const nativeToBrowserTitle = `Threaded Native->Browser ${Date.now()}`;

    await page.goto(url, { waitUntil: "networkidle" });
    await page.waitForFunction(() => {
      const build = document.querySelector("#build-mode")?.textContent ?? "";
      const signaling = document.querySelector("#signaling-mode")?.textContent ?? "";
      const relay = document.querySelector("#relay-status")?.textContent ?? "";
      const threads = Number(document.querySelector("#thread-count")?.textContent ?? "0");
      return (
        build === "wasm-threads" &&
        signaling.includes("relay") &&
        relay === "connected" &&
        threads >= 1 &&
        globalThis.crossOriginIsolated === true
      );
    }, { timeout: 45_000 });

    const nativeWait = runNativeMeshProbe([
      "--relay", RELAY_URL,
      "--room", ROOM,
      "--replica", "native-threaded-waiter",
      "--action", "wait-note",
      "--title", browserToNativeTitle,
      "--timeout-ms", "45000",
      "--hold-ms", "0",
      "--expected-peers", "1",
    ]);

    await page.waitForFunction(
      () => Number(document.querySelector("#peer-count")?.textContent ?? "0") >= 1,
      { timeout: 45_000 },
    );
    await page.waitForFunction(
      () => (document.querySelector("#mesh-detail")?.textContent ?? "").includes("connected to 1 peer"),
      { timeout: 45_000 },
    );

    await page.locator("#note-title").fill(browserToNativeTitle);
    await page.locator("#note-body").fill("threaded browser to native mesh smoke");
    await page.getByRole("button", { name: "Add note" }).click();

    const nativeWaitResult = await nativeWait;

    const nativeWriteResult = await runNativeMeshProbe([
      "--relay", RELAY_URL,
      "--room", ROOM,
      "--replica", "native-threaded-writer",
      "--action", "write-note",
      "--title", nativeToBrowserTitle,
      "--body", "threaded native to browser mesh smoke",
      "--timeout-ms", "45000",
      "--hold-ms", "1500",
      "--expected-peers", "1",
    ]);

    await page.waitForFunction((expectedTitle) => {
      return [...document.querySelectorAll(".note-title")].some(
        (node) => node.textContent === expectedTitle,
      );
    }, nativeToBrowserTitle, { timeout: 45_000 });

    await page.getByRole("button", { name: "Run Parallel Query" }).click();
    await page.waitForFunction(() => {
      const raw = document.querySelector("#query-output")?.textContent ?? "";
      try {
        const parsed = JSON.parse(raw);
        return parsed.parallelEnabled === true && Number(parsed.parallelThreadCount) >= 1;
      } catch {
        return false;
      }
    }, { timeout: 30_000 });

    const result = {
      url,
      room: ROOM,
      browser: {
        build: await page.locator("#build-mode").textContent(),
        signaling: await page.locator("#signaling-mode").textContent(),
        relay: await page.locator("#relay-status").textContent(),
        peers: Number(await page.locator("#peer-count").textContent()),
      },
      browser_to_native: nativeWaitResult,
      native_to_browser: nativeWriteResult,
      query: JSON.parse(await page.locator("#query-output").textContent()),
      threaded_cross_platform_mesh_confirmed: true,
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
