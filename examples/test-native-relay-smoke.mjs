#!/usr/bin/env node

import { spawn } from "node:child_process";
import process from "node:process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const ROOT = process.env.PRIMADB_ROOT ?? resolve(SCRIPT_DIR, "..");
const RELAY_ADDR = process.env.PRIMADB_NATIVE_RELAY_ADDR ?? "127.0.0.1:9010";
const RELAY_URL = process.env.PRIMADB_NATIVE_RELAY_URL ?? `ws://${RELAY_ADDR}`;
const RELAY_PORT = Number(RELAY_ADDR.split(":").at(-1) ?? "9010");
const TITLE = process.env.PRIMADB_NATIVE_RELAY_TITLE ?? `Native relay smoke ${Date.now()}`;

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

async function ensureRelay() {
  if (await isPortOpen(RELAY_PORT)) {
    return { process: null, started: false };
  }

  const child = spawn(
    "cargo",
    ["run", "--features", "native-websocket", "--example", "ws_relay_server", "--", RELAY_ADDR],
    {
      cwd: ROOT,
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

async function runProbe(args) {
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
        cwd: ROOT,
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
  const relay = await ensureRelay();
  try {
    const waiter = runProbe([
      "--relay", RELAY_URL,
      "--board", "shared",
      "--replica", "native-relay-waiter",
      "--action", "wait-note",
      "--title", TITLE,
      "--timeout-ms", "45000",
      "--hold-ms", "0",
      "--expected-peers", "0",
    ]);

    await wait(750);

    const writer = await runProbe([
      "--relay", RELAY_URL,
      "--board", "shared",
      "--replica", "native-relay-writer",
      "--action", "write-note",
      "--title", TITLE,
      "--body", "native/native relay smoke",
      "--timeout-ms", "45000",
      "--hold-ms", "1000",
      "--expected-peers", "0",
    ]);

    const waited = await waiter;
    console.log(JSON.stringify({
      relay: RELAY_URL,
      title: TITLE,
      writer,
      waited,
      native_native_relay_confirmed: true,
    }, null, 2));
  } finally {
    if (relay.started) {
      killDetached(relay.process);
    }
  }
}

main().catch((error) => {
  console.error(error.stack || String(error));
  process.exitCode = 1;
});
