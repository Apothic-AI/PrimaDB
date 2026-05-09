#!/usr/bin/env node

import { spawn } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const ROOT = process.env.PRIMADB_ROOT ?? resolve(SCRIPT_DIR, "..");
const require = createRequire(import.meta.url);

const PORTS = {
  relayAddr: process.env.PRIMADB_E2E_RELAY_ADDR ?? "127.0.0.1:9031",
  rootServer: Number(process.env.PRIMADB_E2E_ROOT_PORT ?? "4193"),
  threadedServer: Number(process.env.PRIMADB_E2E_THREADED_PORT ?? "4195"),
  packageServer: Number(process.env.PRIMADB_E2E_PACKAGE_PORT ?? "4184"),
};

const RELAY_URL = `ws://${PORTS.relayAddr}`;
const ROOM = process.env.PRIMADB_E2E_ROOM ?? `all-targets-${Date.now()}`;
const CHROME_PATH =
  process.env.PLAYWRIGHT_BROWSER_PATH ??
  process.env.PRIMADB_BROWSER_EXECUTABLE ??
  "/usr/bin/google-chrome-stable";
const PACKAGE_EXAMPLE_DIR = `${ROOT}/examples/browser-package-notes-vite`;

const titles = {
  browserDefault: `AllTargets Browser Default ${Date.now()}`,
  browserThreaded: `AllTargets Browser Threaded ${Date.now()}`,
  browserPackage: `AllTargets Browser Package ${Date.now()}`,
  node: `AllTargets Node ${Date.now()}`,
  python: `AllTargets Python ${Date.now()}`,
  rust: `AllTargets Rust ${Date.now()}`,
};
const allTitles = Object.values(titles);

async function loadPlaywright() {
  const candidates = [
    process.env.PLAYWRIGHT_MODULE_PATH,
    "playwright",
    "/tmp/primadb-playwright/node_modules/playwright",
  ].filter(Boolean);
  for (const candidate of candidates) {
    try {
      return require(candidate);
    } catch {}
  }
  throw new Error("Could not resolve the `playwright` module.");
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function killDetached(child) {
  if (child?.pid) {
    try {
      process.kill(-child.pid, "SIGTERM");
    } catch {}
  }
}

function capturePromise(label, promise) {
  return promise.then(
    (value) => ({ ok: true, label, value }),
    (error) => ({ ok: false, label, error }),
  );
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

async function waitForPort(port, timeoutMs, description) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await isPortOpen(port)) {
      return;
    }
    await wait(100);
  }
  throw new Error(`Timed out waiting for ${description} on port ${port}`);
}

function spawnPromise(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: options.cwd ?? ROOT,
      stdio: options.stdio ?? ["ignore", "pipe", "pipe"],
      env: options.env ?? process.env,
      detached: options.detached ?? false,
    });
    let stdout = "";
    let stderr = "";
    if (child.stdout) {
      child.stdout.on("data", (chunk) => {
        stdout += chunk.toString();
      });
    }
    if (child.stderr) {
      child.stderr.on("data", (chunk) => {
        stderr += chunk.toString();
      });
    }
    child.once("exit", (code) => {
      if (code === 0) {
        resolve({ stdout, stderr });
      } else {
        reject(new Error(`${command} ${args.join(" ")} exited with ${code}\n${stderr}`));
      }
    });
  });
}

async function runJson(command, args, options = {}) {
  const { stdout } = await spawnPromise(command, args, options);
  try {
    return JSON.parse(stdout);
  } catch (error) {
    throw new Error(`Expected JSON from ${command} ${args.join(" ")}\n${stdout}\n${error}`);
  }
}

async function run(command, args, options = {}) {
  await spawnPromise(command, args, {
    ...options,
    stdio: options.stdio ?? "inherit",
  });
}

async function ensureBuilds() {
  if (process.env.PRIMADB_E2E_SKIP_BUILD === "1") {
    return;
  }
  await run("bash", ["examples/browser-mesh-notes/build.sh"]);
  await run("bash", ["examples/browser-threaded-mesh-notes/build.sh"]);
  await run("npm", ["install"], { cwd: `${ROOT}/packages/primadb-node` });
  await run("npm", ["run", "build"], { cwd: `${ROOT}/packages/primadb-node` });
  await run("npm", ["install"], { cwd: PACKAGE_EXAMPLE_DIR });
  await run("npm", ["run", "build"], { cwd: PACKAGE_EXAMPLE_DIR });

  const venvDir = `${ROOT}/packages/primadb-python/.venv`;
  if (!existsSync(`${venvDir}/bin/python`)) {
    await run("python3", ["-m", "venv", venvDir]);
  }
  await run(`${venvDir}/bin/python`, ["-m", "pip", "install", "--upgrade", "pip"]);
  await run(`${venvDir}/bin/python`, ["-m", "pip", "install", "-e", `${ROOT}/packages/primadb-python`]);

  await run("cargo", ["build", "--features", "native-webrtc", "--example", "native_mesh_agent"]);
}

async function ensureRootServer() {
  if (await isPortOpen(PORTS.rootServer)) {
    return { process: null, started: false };
  }
  const child = spawn("python3", ["-m", "http.server", String(PORTS.rootServer)], {
    cwd: ROOT,
    stdio: "ignore",
    detached: true,
  });
  child.unref();
  await waitForPort(PORTS.rootServer, 15_000, "root server");
  return { process: child, started: true };
}

async function ensureThreadedServer() {
  if (await isPortOpen(PORTS.threadedServer)) {
    return { process: null, started: false };
  }
  const child = spawn("python3", ["examples/browser-threaded-mesh-notes/serve.py"], {
    cwd: ROOT,
    stdio: "ignore",
    detached: true,
    env: {
      ...process.env,
      PRIMADB_PORT: String(PORTS.threadedServer),
    },
  });
  child.unref();
  await waitForPort(PORTS.threadedServer, 15_000, "threaded server");
  return { process: child, started: true };
}

async function ensurePackageServer() {
  if (await isPortOpen(PORTS.packageServer)) {
    return { process: null, started: false };
  }
  const child = spawn(
    "npm",
    ["run", "preview", "--", "--host", "127.0.0.1", "--port", String(PORTS.packageServer), "--strictPort"],
    {
      cwd: PACKAGE_EXAMPLE_DIR,
      stdio: "ignore",
      detached: true,
    },
  );
  child.unref();
  await waitForPort(PORTS.packageServer, 20_000, "package preview");
  return { process: child, started: true };
}

async function ensureRelay() {
  const relayPort = Number(PORTS.relayAddr.split(":").at(-1) ?? "9031");
  if (await isPortOpen(relayPort)) {
    return { process: null, started: false };
  }
  const child = spawn("cargo", ["run", "--features", "native-websocket", "--example", "ws_relay_server", "--", PORTS.relayAddr], {
    cwd: ROOT,
    stdio: "ignore",
    detached: true,
  });
  child.unref();
  await waitForPort(relayPort, 30_000, "relay");
  return { process: child, started: true };
}

function spawnNodeAgent(storageDir) {
  return runJson("node", [
    `${ROOT}/packages/primadb-node/scripts/mesh-agent.mjs`,
    "--action", "live",
    "--relay", RELAY_URL,
    "--room", ROOM,
    "--replica", "node-all-targets",
    "--storage-dir", storageDir,
    "--write-title", titles.node,
    "--write-body", "node package cross-target mesh",
    "--expect-titles", allTitles.join(","),
    "--min-peers", "1",
    "--timeout-ms", "90000",
    "--hold-ms", "5000",
    "--write-delay-ms", "5000",
  ]);
}

function spawnPythonAgent(storageDir) {
  return runJson(`${ROOT}/packages/primadb-python/.venv/bin/python`, [
    `${ROOT}/packages/primadb-python/scripts/mesh_agent.py`,
    "--action", "live",
    "--relay", RELAY_URL,
    "--room", ROOM,
    "--replica", "python-all-targets",
    "--storage-dir", storageDir,
    "--write-title", titles.python,
    "--write-body", "python package cross-target mesh",
    "--expect-titles", allTitles.join(","),
    "--min-peers", "1",
    "--timeout-ms", "90000",
    "--hold-ms", "5000",
    "--write-delay-ms", "5000",
  ]);
}

function spawnRustAgent(storageDir) {
  return runJson("cargo", [
    "run",
    "--quiet",
    "--features",
    "native-webrtc",
    "--example",
    "native_mesh_agent",
    "--",
    "--action", "live",
    "--relay", RELAY_URL,
    "--room", ROOM,
    "--replica", "rust-all-targets",
    "--storage-dir", storageDir,
    "--write-title", titles.rust,
    "--write-body", "rust native cross-target mesh",
    "--expect-titles", allTitles.join(","),
    "--min-peers", "1",
    "--timeout-ms", "90000",
    "--hold-ms", "5000",
    "--write-delay-ms", "5000",
  ]);
}

async function verifyStored(storageDir, kind) {
  if (kind === "node") {
    return await runJson("node", [
      `${ROOT}/packages/primadb-node/scripts/mesh-agent.mjs`,
      "--action", "verify-stored",
      "--room", ROOM,
      "--replica", "node-all-targets-verify",
      "--storage-dir", storageDir,
      "--expect-titles", allTitles.join(","),
    ]);
  }
  if (kind === "python") {
    return await runJson(`${ROOT}/packages/primadb-python/.venv/bin/python`, [
      `${ROOT}/packages/primadb-python/scripts/mesh_agent.py`,
      "--action", "verify-stored",
      "--room", ROOM,
      "--replica", "python-all-targets-verify",
      "--storage-dir", storageDir,
      "--expect-titles", allTitles.join(","),
    ]);
  }
  return await runJson("cargo", [
    "run",
    "--quiet",
    "--features",
    "native-webrtc",
    "--example",
    "native_mesh_agent",
    "--",
    "--action", "verify-stored",
    "--room", ROOM,
    "--replica", "rust-all-targets-verify",
    "--storage-dir", storageDir,
    "--expect-titles", allTitles.join(","),
  ]);
}

async function waitForTitlesInPage(page, titlesToFind, selector) {
  try {
    await page.waitForFunction(
      ({ titlesToFind, selector }) => {
        const values = [...document.querySelectorAll(selector)].map((node) => node.textContent?.trim() ?? "");
        return titlesToFind.every((title) => values.includes(title));
      },
      { titlesToFind, selector },
      { timeout: 90_000 },
    );
  } catch (error) {
    const values = await page.$$eval(selector, (nodes) =>
      nodes.map((node) => node.textContent?.trim() ?? ""),
    );
    throw new Error(
      `Timed out waiting for titles [${titlesToFind.join(", ")}] using selector ${selector}. Current values: [${values.join(", ")}]. ${error}`,
    );
  }
}

async function main() {
  if (!existsSync(CHROME_PATH)) {
    throw new Error(`Browser executable not found at ${CHROME_PATH}`);
  }

  await ensureBuilds();
  const [rootServer, threadedServer, packageServer, relay] = await Promise.all([
    ensureRootServer(),
    ensureThreadedServer(),
    ensurePackageServer(),
    ensureRelay(),
  ]);

  const tempRoot = mkdtempSync(join(tmpdir(), "primadb-all-targets-"));
  const storageDirs = {
    node: join(tempRoot, "node-store"),
    python: join(tempRoot, "python-store"),
    rust: join(tempRoot, "rust-store"),
  };
  for (const dir of Object.values(storageDirs)) {
    mkdirSync(dir, { recursive: true });
  }

  const { chromium } = await loadPlaywright();
  const browser = await chromium.launch({
    headless: true,
    executablePath: CHROME_PATH,
  });

  try {
    console.error("[e2e] launching browser pages");
    const context = await browser.newContext();
    const defaultPage = await context.newPage();
    const threadedPage = await context.newPage();
    const packagePage = await context.newPage();

    await Promise.all([
      defaultPage.goto(
        `http://127.0.0.1:${PORTS.rootServer}/examples/browser-mesh-notes/?room=${encodeURIComponent(ROOM)}&signal=relay&relay=${encodeURIComponent(RELAY_URL)}`,
        { waitUntil: "networkidle" },
      ),
      threadedPage.goto(
        `http://127.0.0.1:${PORTS.threadedServer}/examples/browser-threaded-mesh-notes/?room=${encodeURIComponent(ROOM)}&signal=relay&relay=${encodeURIComponent(RELAY_URL)}`,
        { waitUntil: "networkidle" },
      ),
      packagePage.goto(
        `http://127.0.0.1:${PORTS.packageServer}/?room=${encodeURIComponent(ROOM)}&signal=relay&relay=${encodeURIComponent(RELAY_URL)}&exactTitle=1`,
        { waitUntil: "networkidle" },
      ),
    ]);

    const nodeAgent = capturePromise("node", spawnNodeAgent(storageDirs.node));
    const pythonAgent = capturePromise("python", spawnPythonAgent(storageDirs.python));
    const rustAgent = capturePromise("rust", spawnRustAgent(storageDirs.rust));

    console.error("[e2e] waiting for browser mesh peers");
    await Promise.all([
      defaultPage.waitForFunction(() => {
        const relay = document.querySelector("#relay-status")?.textContent ?? "";
        const persistence = document.querySelector("#persistence-status")?.textContent ?? "";
        const peers = Number(document.querySelector("#peer-count")?.textContent ?? "0");
        return persistence.length > 0 && (relay === "connected" || relay === "connecting") && peers >= 1;
      }, { timeout: 45_000 }),
      threadedPage.waitForFunction(() => {
        const build = document.querySelector("#build-mode")?.textContent ?? "";
        const relay = document.querySelector("#relay-status")?.textContent ?? "";
        const threads = Number(document.querySelector("#thread-count")?.textContent ?? "0");
        const peers = Number(document.querySelector("#peer-count")?.textContent ?? "0");
        return build === "wasm-threads" && threads >= 1 && (relay === "connected" || relay === "connecting") && peers >= 1;
      }, { timeout: 60_000 }),
      packagePage.waitForFunction(() => {
        const storage = document.querySelector("#storage-backend")?.textContent ?? "";
        const signaling = document.querySelector("#mesh-signaling")?.textContent ?? "";
        const peers = Number(document.querySelector("#mesh-peers")?.textContent ?? "0");
        return storage.includes("incremental=true") && signaling.includes("relay") && peers >= 1;
      }, { timeout: 45_000 }),
    ]);

    console.error("[e2e] submitting browser notes");
    await defaultPage.locator("#note-title").fill(titles.browserDefault);
    await defaultPage.locator("#note-body").fill("raw wasm default browser mesh");
    await defaultPage.getByRole("button", { name: "Add note" }).click();
    await waitForTitlesInPage(defaultPage, [titles.browserDefault], ".note-title");

    await threadedPage.locator("#note-title").fill(titles.browserThreaded);
    await threadedPage.locator("#note-body").fill("raw wasm threaded browser mesh");
    await threadedPage.getByRole("button", { name: "Add note" }).click();
    await waitForTitlesInPage(threadedPage, [titles.browserThreaded], ".note-title");

    await packagePage.locator("#note-title").fill(titles.browserPackage);
    await packagePage.locator("#note-body").fill("package browser mesh");
    await packagePage.locator("#note-submit").click();
    await waitForTitlesInPage(packagePage, [titles.browserPackage], "#notes-list h3");

    console.error("[e2e] waiting for all browser pages to converge");
    await Promise.all([
      waitForTitlesInPage(defaultPage, allTitles, ".note-title"),
      waitForTitlesInPage(threadedPage, allTitles, ".note-title"),
      waitForTitlesInPage(packagePage, allTitles, "#notes-list h3"),
    ]);

    console.error("[e2e] collecting native agent results");
    const [nodeResult, pythonResult, rustResult] = await Promise.all([
      nodeAgent,
      pythonAgent,
      rustAgent,
    ]);
    const nativeFailures = [nodeResult, pythonResult, rustResult].filter((result) => !result.ok);
    if (nativeFailures.length > 0) {
      throw new Error(
        nativeFailures
          .map((result) => `[${result.label}] ${result.error?.stack || String(result.error)}`)
          .join("\n\n"),
      );
    }

    console.error("[e2e] verifying persistence after relay shutdown");
    await wait(5000);
    if (relay.started) {
      killDetached(relay.process);
    }

    await Promise.all([
      defaultPage.reload({ waitUntil: "networkidle" }),
      threadedPage.reload({ waitUntil: "networkidle" }),
    ]);

    await Promise.all([
      waitForTitlesInPage(defaultPage, allTitles, ".note-title"),
      waitForTitlesInPage(threadedPage, allTitles, ".note-title"),
    ]);

    const [nodeStored, pythonStored, rustStored] = await Promise.all([
      verifyStored(storageDirs.node, "node"),
      verifyStored(storageDirs.python, "python"),
      verifyStored(storageDirs.rust, "rust"),
    ]);
    await run("node", ["./scripts/smoke.mjs"], { cwd: PACKAGE_EXAMPLE_DIR });

    const result = {
      room: ROOM,
      relayUrl: RELAY_URL,
      titles,
      browsers: {
        default: {
          persistence: await defaultPage.locator("#persistence-status").textContent(),
          peers: Number(await defaultPage.locator("#peer-count").textContent()),
        },
        threaded: {
          build: await threadedPage.locator("#build-mode").textContent(),
          persistence: await threadedPage.locator("#persistence-status").textContent(),
          peers: Number(await threadedPage.locator("#peer-count").textContent()),
          query: await (async () => {
            await threadedPage.getByRole("button", { name: "Run Parallel Query" }).click();
            await threadedPage.waitForFunction(() => {
              const raw = document.querySelector("#query-output")?.textContent ?? "";
              try {
                const parsed = JSON.parse(raw);
                return parsed.parallelEnabled === true && Number(parsed.parallelThreadCount) >= 1;
              } catch {
                return false;
              }
            }, { timeout: 30_000 });
            return JSON.parse(await threadedPage.locator("#query-output").textContent());
          })(),
        },
        package: {
          storage: await packagePage.locator("#storage-backend").textContent(),
          signaling: await packagePage.locator("#mesh-signaling").textContent(),
          peers: Number(await packagePage.locator("#mesh-peers").textContent()),
          dedicatedPersistenceSmokePassed: true,
        },
      },
      native: {
        nodeLive: nodeResult.value,
        pythonLive: pythonResult.value,
        rustLive: rustResult.value,
        nodeStored,
        pythonStored,
        rustStored,
      },
      all_targets_mesh_and_storage_confirmed: true,
    };

    console.log(JSON.stringify(result, null, 2));
  } finally {
    await browser.close();
    if (relay.started) {
      killDetached(relay.process);
    }
    if (packageServer.started) {
      killDetached(packageServer.process);
    }
    if (threadedServer.started) {
      killDetached(threadedServer.process);
    }
    if (rootServer.started) {
      killDetached(rootServer.process);
    }
    rmSync(tempRoot, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error(error.stack || String(error));
  process.exitCode = 1;
});
