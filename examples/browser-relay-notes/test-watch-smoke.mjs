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
  process.env.PRIMADB_BROWSER_RELAY_URL ??
  "http://127.0.0.1:4173/examples/browser-relay-notes/";
const SERVER_PORT = Number(process.env.PRIMADB_BROWSER_RELAY_PORT ?? "4173");
const RELAY_ADDR = process.env.PRIMADB_BROWSER_RELAY_ADDR ?? "127.0.0.1:9010";
const RELAY_URL = process.env.PRIMADB_BROWSER_RELAY_URL_WS ?? `ws://${RELAY_ADDR}`;
const RELAY_PORT = Number(RELAY_ADDR.split(":").at(-1) ?? "9010");
const SERVER_ROOT =
  process.env.PRIMADB_BROWSER_RELAY_ROOT ?? resolve(SCRIPT_DIR, "../..");
const CHROME_PATH =
  process.env.PLAYWRIGHT_BROWSER_PATH ?? "/usr/bin/google-chrome-stable";

async function loadPlaywright() {
  const override = process.env.PLAYWRIGHT_MODULE_PATH;
  const candidates = [override, "playwright", "/tmp/primadb-playwright/node_modules/playwright"].filter(Boolean);
  for (const candidate of candidates) {
    try {
      return require(candidate);
    } catch {}
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

  throw new Error(`Timed out waiting for browser relay server on port ${SERVER_PORT}`);
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
        reject(new Error(`browser relay build failed with exit code ${code}`));
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

    const waitForReady = async (page) => {
      await page.goto(ROOT_URL, { waitUntil: "networkidle" });
      await page.waitForFunction(() => {
        const demo = globalThis.primadbRelayDemo;
        return demo != null && demo.relay != null && demo.db != null && demo.relay.readyState() === 1;
      }, { timeout: 30_000 });
    };

    await waitForReady(page1);
    await waitForReady(page2);

    const page2Replica = await page2.evaluate(() => globalThis.primadbRelayDemo.db.replicaId());
    const targetPeer = await page1.waitForFunction(
      (targetReplica) => {
        const peers = globalThis.primadbRelayDemo.relay.recommendedPeers();
        return peers.find((entry) => entry?.peer?.replica_id === targetReplica)?.peer?.peer_id ?? null;
      },
      page2Replica,
      { timeout: 30_000 },
    );

    const targetPeerId = await targetPeer.jsonValue();
    const noteTitle = `Relay watch ${Date.now()}`;
    const statusTitle = `status-${Date.now()}`;

    const initialGet = await page1.evaluate(async ({ targetPeerId }) => {
      const demo = globalThis.primadbRelayDemo;
      const watch = demo.relay.watchRemoteGet(targetPeerId, {
        anchor: "watch-demo",
        segments: ["status"],
      });
      globalThis.__primadbWatchGet = watch;
      return await watch.next();
    }, { targetPeerId });

    const initialQuery = await page1.evaluate(async ({ targetPeerId, noteTitle }) => {
      const demo = globalThis.primadbRelayDemo;
      const watch = demo.relay.watchRemoteQuery(
        targetPeerId,
        {
          anchor: "boards",
          segments: ["shared", "notes"],
        },
        {
          filters: [{ kind: "eq", path: "title", value: noteTitle }],
          limit: 1,
        },
      );
      globalThis.__primadbWatchQuery = watch;
      return await watch.next();
    }, { targetPeerId, noteTitle });

    await page2.evaluate(async ({ noteTitle, statusTitle }) => {
      const demo = globalThis.primadbRelayDemo;
      demo.db.chain("watch-demo").field("status").put({
        title: statusTitle,
        updated_at: Date.now(),
      });
      demo.listChain.set({
        title: noteTitle,
        body: "remote watch smoke",
        done: false,
        archived: false,
        created_at: Date.now(),
        updated_at: Date.now(),
      });
      await demo.persistence?.flush?.();
      demo.relay.flushPending();
    }, { noteTitle, statusTitle });

    const getUpdate = await page1.evaluate(async () => {
      return await globalThis.__primadbWatchGet.next();
    });
    const queryUpdate = await page1.evaluate(async () => {
      return await globalThis.__primadbWatchQuery.next();
    });

    await page1.evaluate(() => {
      globalThis.__primadbWatchGet.cancel();
      globalThis.__primadbWatchQuery.cancel();
    });

    console.log(
      JSON.stringify(
        {
          targetPeerId,
          initialGet,
          initialQuery,
          getUpdate,
          queryUpdate,
          browser_relay_watch_confirmed:
            getUpdate?.kind === "get" &&
            getUpdate?.value?.title === statusTitle &&
            queryUpdate?.kind === "query" &&
            Array.isArray(queryUpdate?.value) &&
            queryUpdate.value.some((entry) => entry?.value?.title === noteTitle),
        },
        null,
        2,
      ),
    );
  } finally {
    await browser?.close();
    if (server.started) {
      killDetached(server.process);
    }
    if (relay.started) {
      killDetached(relay.process);
    }
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
