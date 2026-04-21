#!/usr/bin/env node

import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import process from "node:process";

const require = createRequire(import.meta.url);

const ROOT_URL =
  process.env.PRIMADB_GUN_URL ??
  "http://127.0.0.1:4173/examples/browser-gun-notes/";
const SERVER_PORT = Number(process.env.PRIMADB_GUN_PORT ?? "4173");
const RELAY_PORT = Number(process.env.PRIMADB_GUN_RELAY_PORT ?? "9010");
const SERVER_ROOT =
  process.env.PRIMADB_GUN_ROOT ?? "/home/bitnom/Code/gunport/primadb";
const RELAY_ADDR =
  process.env.PRIMADB_GUN_RELAY_ADDR ?? `127.0.0.1:${RELAY_PORT}`;
const CHROME_PATH =
  process.env.PLAYWRIGHT_BROWSER_PATH ?? "/usr/bin/google-chrome-stable";
const ROOM = process.env.PRIMADB_GUN_ROOM ?? `gun-smoke-${Date.now()}`;

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

async function ensureStaticServer() {
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
  throw new Error(`Timed out waiting for static server on port ${SERVER_PORT}`);
}

async function ensureRelay() {
  if (await isPortOpen(RELAY_PORT)) {
    return { process: null, started: false };
  }

  const child = spawn("cargo", ["run", "--features", "native-websocket", "--example", "ws_relay_server", "--", RELAY_ADDR], {
    cwd: SERVER_ROOT,
    stdio: "ignore",
    detached: true,
  });
  child.unref();

  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    if (await isPortOpen(RELAY_PORT)) {
      return { process: child, started: true };
    }
    await wait(200);
  }
  throw new Error(`Timed out waiting for relay on ${RELAY_ADDR}`);
}

async function maybeBuild() {
  if (existsSync(`${SERVER_ROOT}/examples/browser-gun-notes/pkg/primadb.js`)) {
    return;
  }

  await new Promise((resolve, reject) => {
    const child = spawn("bash", ["examples/browser-gun-notes/build.sh"], {
      cwd: SERVER_ROOT,
      stdio: "inherit",
    });
    child.once("exit", (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`gun runtime build failed with exit code ${code}`));
      }
    });
  });
}

async function main() {
  await maybeBuild();
  const { chromium } = await loadPlaywright();
  const relay = await ensureRelay();
  const server = await ensureStaticServer();

  if (!existsSync(CHROME_PATH)) {
    throw new Error(`Browser executable not found at ${CHROME_PATH}`);
  }

  const browser = await chromium.launch({
    headless: true,
    executablePath: CHROME_PATH,
  });

  try {
    const context = await browser.newContext();
    const page1 = await context.newPage();
    const page2 = await context.newPage();
    const url = `${ROOT_URL}?room=${encodeURIComponent(ROOM)}`;
    const alias1 = `alice-${ROOM}`;
    const alias2 = `bob-${ROOM}`;
    const password = "correct horse battery staple";
    const sharedTitle = `Gun runtime note ${Date.now()}`;

    const waitForReady = async (page) => {
      await page.goto(url, { waitUntil: "networkidle" });
      await page.waitForFunction(() => {
        const replica = document.querySelector("#replica-id")?.textContent ?? "";
        return replica.length > 0 && replica !== "loading";
      }, { timeout: 30_000 });
    };

    await waitForReady(page1);
    await waitForReady(page2);

    const createUser = async (page, alias) => {
      await page.locator("#alias-input").fill(alias);
      await page.locator("#password-input").fill(password);
      await page.getByRole("button", { name: "Create" }).click();
      await page.waitForFunction(
        (expectedAlias) =>
          (document.querySelector("#auth-status")?.textContent ?? "").includes(expectedAlias),
        alias,
        { timeout: 30_000 },
      );
    };

    await createUser(page1, alias1);
    await createUser(page2, alias2);

    await Promise.all([
      page1.waitForFunction(() => Number(document.querySelector("#peer-count")?.textContent ?? "0") >= 1, { timeout: 30_000 }),
      page2.waitForFunction(() => Number(document.querySelector("#peer-count")?.textContent ?? "0") >= 1, { timeout: 30_000 }),
    ]);

    await page1.locator("#note-title").fill(sharedTitle);
    await page1.locator("#note-body").fill("Gun runtime relay sync smoke test");
    await page1.getByRole("button", { name: "Add Shared Note" }).click();

    await page2.waitForFunction((expectedTitle) => {
      return [...document.querySelectorAll(".note-title")].some(
        (node) => node.textContent === expectedTitle,
      );
    }, sharedTitle, { timeout: 30_000 });

    const runtimeCheck = await page1.evaluate(async ({ room }) => {
      const demo = globalThis.primadbGunDemo;
      const gun = demo.gun;
      const root = gun.get("runtime-smoke").get(room);

      const notKey = await new Promise((resolve) => {
        root.get("missing").not((key) => resolve(key));
      });

      await new Promise((resolve) => {
        root.get("doc").put({ alpha: 1, nested: { beta: 2 } }, () => resolve());
      });

      const loaded = await new Promise((resolve) => {
        root.get("doc").load((data, key) => resolve({ data, key }));
      });

      await new Promise((resolve) => {
        root.get("collection").set({ title: "Mapped A" }, () => resolve());
      });
      await new Promise((resolve) => {
        root.get("collection").set({ title: "Mapped B" }, () => resolve());
      });

      const mapped = await root
        .get("collection")
        .map((data) => data?.title)
        .once();

      return {
        notKey,
        loaded,
        mapped,
        backToRoot: root.back(-1) === gun,
      };
    }, { room: ROOM });

    const result = {
      url,
      room: ROOM,
      peers1: Number(await page1.locator("#peer-count").textContent()),
      peers2: Number(await page2.locator("#peer-count").textContent()),
      shared_note_replicated: (await page2.locator(".note-title", { hasText: sharedTitle }).count()) >= 1,
      runtime: runtimeCheck,
      gun_runtime_confirmed:
        runtimeCheck.notKey === "missing" &&
        runtimeCheck.loaded?.data?.nested?.beta === 2 &&
        Array.isArray(runtimeCheck.mapped) &&
        runtimeCheck.mapped.includes("Mapped A") &&
        runtimeCheck.mapped.includes("Mapped B") &&
        runtimeCheck.backToRoot === true,
    };

    console.log(JSON.stringify(result, null, 2));

    if (!result.gun_runtime_confirmed || !result.shared_note_replicated) {
      throw new Error("Gun runtime browser smoke test failed");
    }

    if (server.started && server.process?.pid) {
      process.kill(-server.process.pid, "SIGTERM");
    }
    if (relay.started && relay.process?.pid) {
      process.kill(-relay.process.pid, "SIGTERM");
    }
  } finally {
    await browser.close();
  }
}

main().catch((error) => {
  console.error(error.stack || String(error));
  process.exitCode = 1;
});
