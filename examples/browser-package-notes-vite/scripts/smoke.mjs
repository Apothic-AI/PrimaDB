#!/usr/bin/env node
import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright-core";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const exampleDir = resolve(scriptDir, "..");
const browserCandidates = [
  process.env.PRIMADB_BROWSER_EXECUTABLE,
  "/home/bitnom/.cache/ms-playwright/chromium-1200/chrome-linux64/chrome",
  "/usr/bin/chromium",
  "/usr/bin/google-chrome",
  "/usr/bin/google-chrome-stable",
].filter(Boolean);

const executablePath = browserCandidates.find((candidate) => existsSync(candidate));
if (!executablePath) {
  console.error("No Chromium executable found for smoke test");
  process.exit(1);
}

function run(command, args, cwd) {
  return new Promise((resolvePromise, rejectPromise) => {
    const child = spawn(command, args, {
      cwd,
      stdio: "inherit",
      env: process.env,
    });
    child.on("exit", (code) => {
      if (code === 0) {
        resolvePromise();
        return;
      }
      rejectPromise(new Error(`${command} ${args.join(" ")} exited with ${code}`));
    });
  });
}

async function waitForServer(url, timeoutMs = 15_000) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    try {
      const response = await fetch(url);
      if (response.ok) {
        return;
      }
    } catch (_error) {}
    await new Promise((resolvePromise) => setTimeout(resolvePromise, 250));
  }
  throw new Error(`Timed out waiting for ${url}`);
}

await run("npm", ["run", "build"], exampleDir);

const preview = spawn("npm", ["run", "preview"], {
  cwd: exampleDir,
  stdio: "inherit",
  env: process.env,
});

try {
  await waitForServer("http://127.0.0.1:4182");

  const browser = await chromium.launch({
    executablePath,
    headless: true,
  });
  const page = await browser.newPage();
  await page.goto("http://127.0.0.1:4182", { waitUntil: "networkidle" });

  const unique = `Vite package smoke ${Date.now()}`;
  await page.fill("#note-title", unique);
  await page.fill("#note-body", "Installed from the local primadb package.");
  await page.click("#note-submit");
  await page.waitForSelector(`[data-note-title^="${unique}"]`);

  await page.reload({ waitUntil: "networkidle" });
  await page.waitForSelector(`[data-note-title^="${unique}"]`);

  const backend = await page.textContent("#storage-backend");
  const count = await page.textContent("#note-count");
  await browser.close();

  console.log(
    JSON.stringify(
      {
        url: "http://127.0.0.1:4182",
        executablePath,
        unique,
        backend,
        count,
        package_browser_confirmed: true,
      },
      null,
      2,
    ),
  );
} finally {
  preview.kill("SIGTERM");
}
