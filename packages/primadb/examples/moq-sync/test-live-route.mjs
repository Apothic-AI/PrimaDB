#!/usr/bin/env node

import { spawn } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import process from "node:process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { connectPrimadbMoq as connectNodePrimadbMoq } from "../../../primadb-node/moq.js";

const require = createRequire(import.meta.url);
const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const EXAMPLES_ROOT =
  process.env.PRIMADB_PACKAGE_EXAMPLES_ROOT ?? resolve(SCRIPT_DIR, "..");
const PRIMADB_ROOT = resolve(SCRIPT_DIR, "../../../..");
const ROOT_URL = process.env.PRIMADB_PACKAGE_MOQ_URL ?? "http://127.0.0.1:4181/moq-sync/";
const SERVER_PORT = Number(process.env.PRIMADB_PACKAGE_PORT ?? "4181");
const CHROME_PATH = process.env.PLAYWRIGHT_BROWSER_PATH ?? "/usr/bin/google-chrome-stable";

loadDotEnv(resolve(PRIMADB_ROOT, ".env"));

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

function loadDotEnv(path) {
  if (!existsSync(path)) {
    return;
  }
  for (const line of readFileSync(path, "utf8").split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }
    const match = /^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/.exec(trimmed);
    if (!match || process.env[match[1]] !== undefined) {
      continue;
    }
    process.env[match[1]] = match[2].replace(/^['"]|['"]$/g, "");
  }
}

function relayCandidates() {
  const selected = process.env.MOQ_RELAY ?? process.env.PRIMADB_MOQ_RELAY;
  if (selected) {
    return [{ draft: process.env.MOQ_RELAY_DRAFT ?? "selected", relay: selected }];
  }
  return [
    { draft: "draft_14", relay: process.env.MOQ_DRAFT14_RELAY },
    { draft: "draft_07", relay: process.env.MOQ_DRAFT07_RELAY },
  ].filter((candidate) => candidate.relay);
}

function normalizeRelayUrl(value) {
  if (/^https?:\/\//.test(value)) {
    return value;
  }
  return `https://${value}`;
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate, timeoutMs = 20_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await predicate();
    if (value) {
      return value;
    }
    await wait(100);
  }
  return null;
}

async function cloudflareIceServers(label) {
  const servers = [];
  if (process.env.CLOUDFLARE_STUN) {
    servers.push({ urls: `stun:${process.env.CLOUDFLARE_STUN}:3478` });
  }
  const tokenId = process.env.CLOUDFLARE_TURN_TOKEN_ID;
  const apiToken = process.env.CLOUDFLARE_TURN_API_TOKEN;
  if (tokenId && apiToken) {
    const response = await fetch(
      `https://rtc.live.cloudflare.com/v1/turn/keys/${tokenId}/credentials/generate`,
      {
        method: "POST",
        headers: {
          authorization: `Bearer ${apiToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          ttl: 600,
          customIdentifier: `primadb-moq-mesh-${label}`,
        }),
      },
    );
    if (!response.ok) {
      throw new Error(`Cloudflare TURN credential request failed with HTTP ${response.status}`);
    }
    const payload = await response.json();
    if (payload?.iceServers) {
      servers.push(payload.iceServers);
    }
  } else if (process.env.CLOUDFLARE_TURN) {
    servers.push({ urls: `turn:${process.env.CLOUDFLARE_TURN}:3478?transport=udp` });
  }
  return servers;
}

function withTimeout(promise, timeoutMs, description) {
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      setTimeout(() => reject(new Error(`Timed out waiting for ${description}`)), timeoutMs);
    }),
  ]);
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

function createDb(name) {
  return {
    replicaId() {
      return name;
    },
    pendingEnvelope() {
      return { ops: [] };
    },
    drainPendingEnvelope() {
      return { ops: [] };
    },
    applyEnvelope() {
      return 0;
    },
  };
}

async function runBrowserMeshViaMoq(page, url, draft) {
  const token = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const room = `primadb-live-moq-mesh-${token}`;
  const channel = `mesh:${room}`;
  const pathA = `primadb/live/mesh/${token}/a`;
  const pathB = `primadb/live/mesh/${token}/b`;
  const iceServers = await cloudflareIceServers(token);

  await withTimeout(
    page.evaluate(
      async ({ url, room, channel, pathA, pathB, iceServers }) => {
        const { connectMeshViaMoq, createPrimadb } = globalThis.primadbMoqApi;
        const dbA = await createPrimadb(`browser-mesh-a-${room}`);
        const dbB = await createPrimadb(`browser-mesh-b-${room}`);
        const a = await connectMeshViaMoq(dbA, {
          url,
          path: pathA,
          channel,
          subscribe: [pathB],
          room,
          intervalMs: 60_000,
          retryIntervalMs: 500,
          iceServers,
        });
        const b = await connectMeshViaMoq(dbB, {
          url,
          path: pathB,
          channel,
          subscribe: [pathA],
          room,
          intervalMs: 60_000,
          retryIntervalMs: 500,
          iceServers,
        });
        globalThis.primadbLiveMoqMesh = { a, b, room, channel, iceServers };
      },
      { url, room, channel, pathA, pathB, iceServers },
    ),
    25_000,
    `browser WebRTC mesh MoQ signaling connect via ${draft}`,
  );

  const result = await waitFor(
    async () =>
      page.evaluate(() => {
        const state = globalThis.primadbLiveMoqMesh;
        state.a.mesh.announceSignalingPresence();
        state.b.mesh.announceSignalingPresence();
        const aOpen = state.a.mesh.openPeerCount();
        const bOpen = state.b.mesh.openPeerCount();
        return aOpen > 0 && bOpen > 0
          ? {
              room: state.room,
              channel: state.channel,
              aPeerId: state.a.mesh.peerId(),
              bPeerId: state.b.mesh.peerId(),
              aPeerCount: state.a.mesh.peerCount(),
              bPeerCount: state.b.mesh.peerCount(),
              aOpenPeerCount: aOpen,
              bOpenPeerCount: bOpen,
              iceServerCount: state.iceServers.length,
            }
          : null;
      }),
    30_000,
  );

  await page.evaluate(() => {
    globalThis.primadbLiveMoqMesh?.a.close();
    globalThis.primadbLiveMoqMesh?.b.close();
    delete globalThis.primadbLiveMoqMesh;
  });
  if (!result) {
    throw new Error(`Timed out waiting for browser WebRTC mesh via MoQ signaling (${draft})`);
  }
  return result;
}

async function runBrowserPair(page, url, draft) {
  const token = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const channel = `primadb-live-browser-${token}`;
  const pathA = `primadb/live/browser/${token}/a`;
  const pathB = `primadb/live/browser/${token}/b`;
  await withTimeout(
    page.evaluate(
      async ({ url, channel, pathA, pathB }) => {
      const { connectPrimadbMoq } = globalThis.primadbMoqApi;
      const db = (name) => ({
        replicaId: () => name,
        pendingEnvelope: () => ({ ops: [] }),
        drainPendingEnvelope: () => ({ ops: [] }),
        applyEnvelope: () => 0,
      });
      const receivedA = [];
      const receivedB = [];
      const a = await connectPrimadbMoq(db("browser-a"), {
        url,
        path: pathA,
        channel,
        subscribe: [pathB],
        intervalMs: 60_000,
      });
      const b = await connectPrimadbMoq(db("browser-b"), {
        url,
        path: pathB,
        channel,
        subscribe: [pathA],
        intervalMs: 60_000,
      });
      a.onRoute((route) => receivedA.push(route));
      b.onRoute((route) => receivedB.push(route));
      globalThis.primadbLiveMoqBrowserPair = { a, b, receivedA, receivedB, channel };
    },
      { url, channel, pathA, pathB },
    ),
    20_000,
    `browser/browser MoQ connect via ${draft}`,
  );

  const result = await waitFor(async () => {
    await page.evaluate((draft) => {
      const pair = globalThis.primadbLiveMoqBrowserPair;
      pair.a.sendRoute(
        pair.a.createRoute({
          kind: "signal",
          room: "moq-live-interop",
          payload: { kind: "interop_probe", draft, fromLabel: "browser-a" },
        }),
      );
      pair.b.sendRoute(
        pair.b.createRoute({
          kind: "signal",
          room: "moq-live-interop",
          payload: { kind: "interop_probe", draft, fromLabel: "browser-b" },
        }),
      );
    }, draft);
    return page.evaluate(() => {
      const pair = globalThis.primadbLiveMoqBrowserPair;
      const a = pair.receivedA.find((route) => route.payload?.payload?.fromLabel === "browser-b");
      const b = pair.receivedB.find((route) => route.payload?.payload?.fromLabel === "browser-a");
      return a && b ? { a: a.route_id, b: b.route_id, channel: pair.channel } : null;
    });
  });

  await page.evaluate(() => {
    globalThis.primadbLiveMoqBrowserPair?.a.close();
    globalThis.primadbLiveMoqBrowserPair?.b.close();
    delete globalThis.primadbLiveMoqBrowserPair;
  });
  if (!result) {
    throw new Error(`Timed out waiting for browser/browser MoQ route exchange via ${draft}`);
  }
  return result;
}

async function runBrowserNodePair(page, url, draft) {
  const token = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const channel = `primadb-live-browser-node-${token}`;
  const browserPath = `primadb/live/browser-node/${token}/browser`;
  const nodePath = `primadb/live/browser-node/${token}/node`;
  const nodeReceived = [];
  const nodeSession = await withTimeout(
    connectNodePrimadbMoq(createDb("node"), {
      url,
      path: nodePath,
      channel,
      subscribe: [browserPath],
      intervalMs: 60_000,
    }),
    15_000,
    `Node side MoQ connect via ${draft}`,
  );
  nodeSession.onRoute((route) => nodeReceived.push(route));

  try {
    await withTimeout(
      page.evaluate(
        async ({ url, channel, browserPath, nodePath }) => {
        const { connectPrimadbMoq } = globalThis.primadbMoqApi;
        const db = {
          replicaId: () => "browser",
          pendingEnvelope: () => ({ ops: [] }),
          drainPendingEnvelope: () => ({ ops: [] }),
          applyEnvelope: () => 0,
        };
        const received = [];
        const session = await connectPrimadbMoq(db, {
          url,
          path: browserPath,
          channel,
          subscribe: [nodePath],
          intervalMs: 60_000,
        });
        session.onRoute((route) => received.push(route));
        globalThis.primadbLiveMoqBrowserNode = { session, received, channel };
      },
        { url, channel, browserPath, nodePath },
      ),
      20_000,
      `browser side MoQ connect via ${draft}`,
    );

    const result = await waitFor(async () => {
      nodeSession.sendRoute(
        nodeSession.createRoute({
          kind: "signal",
          room: "moq-live-interop",
          payload: { kind: "interop_probe", draft, fromLabel: "node" },
        }),
      );
      await page.evaluate((draft) => {
        const state = globalThis.primadbLiveMoqBrowserNode;
        state.session.sendRoute(
          state.session.createRoute({
            kind: "signal",
            room: "moq-live-interop",
            payload: { kind: "interop_probe", draft, fromLabel: "browser" },
          }),
        );
      }, draft);
      const browserGotNode = await page.evaluate(() => {
        const state = globalThis.primadbLiveMoqBrowserNode;
        return state.received.find((route) => route.payload?.payload?.fromLabel === "node")?.route_id ?? null;
      });
      const nodeGotBrowser =
        nodeReceived.find((route) => route.payload?.payload?.fromLabel === "browser")?.route_id ?? null;
      return browserGotNode && nodeGotBrowser
        ? { browserGotNode, nodeGotBrowser, channel }
        : null;
    });
    if (!result) {
      throw new Error(`Timed out waiting for browser/Node MoQ route exchange via ${draft}`);
    }
    return result;
  } finally {
    await page.evaluate(() => {
      globalThis.primadbLiveMoqBrowserNode?.session.close();
      delete globalThis.primadbLiveMoqBrowserNode;
    });
    nodeSession.close();
  }
}

async function main() {
  const candidates = relayCandidates();
  if (candidates.length === 0) {
    console.log("Skipping live MoQ browser smoke: MOQ_DRAFT14_RELAY/MOQ_DRAFT07_RELAY are not set.");
    return;
  }
  const { chromium } = await loadPlaywright();
  const server = await ensurePreviewServer();
  if (!existsSync(CHROME_PATH)) {
    throw new Error(`Browser executable not found at ${CHROME_PATH}`);
  }
  const browser = await chromium.launch({ headless: true, executablePath: CHROME_PATH });
  const results = [];
  try {
    const page = await browser.newPage();
    if (process.env.PRIMADB_MOQ_LIVE_BROWSER_LOGS === "1") {
      page.on("console", (message) => {
        console.log(`[browser:${message.type()}] ${message.text()}`);
      });
      page.on("pageerror", (error) => {
        console.log(`[browser:pageerror] ${error.stack ?? error.message}`);
      });
    }
    await page.goto(ROOT_URL, { waitUntil: "networkidle" });
    await page.waitForFunction(
      () => globalThis.primadbMoqApi?.connectPrimadbMoq && globalThis.primadbMoqApi?.connectMeshViaMoq,
      {
      timeout: 30_000,
      },
    );
    for (const candidate of candidates) {
      const url = normalizeRelayUrl(candidate.relay);
      const result = { draft: candidate.draft, url };
      try {
        result.browserBrowser = await runBrowserPair(page, url, candidate.draft);
      } catch (error) {
        result.browserBrowser = {
          ok: false,
          error: error instanceof Error ? error.message : String(error),
        };
      }
      try {
        result.browserNode = await runBrowserNodePair(page, url, candidate.draft);
      } catch (error) {
        result.browserNode = {
          ok: false,
          error: error instanceof Error ? error.message : String(error),
        };
      }
      try {
        result.browserMesh = await runBrowserMeshViaMoq(page, url, candidate.draft);
      } catch (error) {
        result.browserMesh = {
          ok: false,
          error: error instanceof Error ? error.message : String(error),
        };
      }
      result.ok = Boolean(
        result.browserBrowser?.channel &&
          result.browserNode?.channel &&
          result.browserMesh?.channel,
      );
      results.push(result);
    }
  } finally {
    await browser.close();
    killDetached(server);
  }
  console.log(JSON.stringify(results, null, 2));
  if (!results.some((result) => result.ok)) {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
