#!/usr/bin/env node

import { existsSync, readFileSync } from "node:fs";
import process from "node:process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { connectPrimadbMoq } from "../moq.js";

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const PRIMADB_ROOT = resolve(SCRIPT_DIR, "../../..");

loadDotEnv(resolve(PRIMADB_ROOT, ".env"));

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
    const value = predicate();
    if (value) {
      return value;
    }
    await wait(100);
  }
  return null;
}

function withTimeout(promise, timeoutMs, description) {
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      setTimeout(() => reject(new Error(`Timed out waiting for ${description}`)), timeoutMs);
    }),
  ]);
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

async function probeNodePair(candidate) {
  const url = normalizeRelayUrl(candidate.relay);
  const token = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const channel = `primadb-live-node-${token}`;
  const pathA = `primadb/live/node/${token}/a`;
  const pathB = `primadb/live/node/${token}/b`;
  const receivedA = [];
  const receivedB = [];
  const [a, b] = await withTimeout(
    Promise.all([
      connectPrimadbMoq(createDb("node-a"), {
        url,
        path: pathA,
        channel,
        subscribe: [pathB],
        intervalMs: 60_000,
      }),
      connectPrimadbMoq(createDb("node-b"), {
        url,
        path: pathB,
        channel,
        subscribe: [pathA],
        intervalMs: 60_000,
      }),
    ]),
    15_000,
    `Node/Node MoQ connect via ${candidate.draft}`,
  );
  a.onRoute((route) => receivedA.push(route));
  b.onRoute((route) => receivedB.push(route));
  try {
    const result = await waitFor(() => {
      a.sendRoute(
        a.createRoute({
          kind: "signal",
          room: "moq-live-interop",
          payload: { kind: "interop_probe", draft: candidate.draft, fromLabel: "node-a" },
        }),
      );
      b.sendRoute(
        b.createRoute({
          kind: "signal",
          room: "moq-live-interop",
          payload: { kind: "interop_probe", draft: candidate.draft, fromLabel: "node-b" },
        }),
      );
      const gotA = receivedA.find((route) => route.payload?.payload?.fromLabel === "node-b");
      const gotB = receivedB.find((route) => route.payload?.payload?.fromLabel === "node-a");
      return gotA && gotB
        ? {
            draft: candidate.draft,
            url,
            channel,
            nodeAGotNodeB: gotA.route_id,
            nodeBGotNodeA: gotB.route_id,
          }
        : null;
    });
    if (!result) {
      throw new Error(`Timed out waiting for Node/Node MoQ route exchange via ${candidate.draft}`);
    }
    return result;
  } finally {
    a.close();
    b.close();
  }
}

async function main() {
  const candidates = relayCandidates();
  if (candidates.length === 0) {
    console.log("Skipping live MoQ Node smoke: MOQ_DRAFT14_RELAY/MOQ_DRAFT07_RELAY are not set.");
    return;
  }
  const results = [];
  for (const candidate of candidates) {
    const result = { draft: candidate.draft, url: normalizeRelayUrl(candidate.relay) };
    try {
      result.exchange = await probeNodePair(candidate);
      result.ok = true;
    } catch (error) {
      result.ok = false;
      result.error = error instanceof Error ? error.message : String(error);
    }
    results.push(result);
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
