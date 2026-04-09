#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { copyFileSync, cpSync, mkdirSync, readdirSync, rmSync, statSync, unlinkSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const packageDir = resolve(scriptDir, "..");
const repoRoot = resolve(packageDir, "../..");
const vendorDir = resolve(packageDir, "vendor");
const distDir = resolve(packageDir, "dist");

function run(command, args, cwd) {
  const result = spawnSync(command, args, {
    cwd,
    stdio: "inherit",
    shell: false,
  });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

function removeFilesNamed(rootDir, filename) {
  for (const entry of readdirSync(rootDir)) {
    const fullPath = resolve(rootDir, entry);
    const stats = statSync(fullPath);
    if (stats.isDirectory()) {
      removeFilesNamed(fullPath, filename);
      continue;
    }
    if (entry === filename) {
      unlinkSync(fullPath);
    }
  }
}

rmSync(vendorDir, { recursive: true, force: true });
rmSync(distDir, { recursive: true, force: true });
mkdirSync(resolve(distDir, "runtime"), { recursive: true });

run("./build-wasm.sh", ["--out-dir", "packages/primadb/vendor/default", "--features", "crypto"], repoRoot);
run(
  "./build-wasm-threads.sh",
  ["--out-dir", "packages/primadb/vendor/threads", "--features", "crypto"],
  repoRoot,
);

run("npm", ["exec", "--", "tsc", "-p", "tsconfig.json"], packageDir);

cpSync(vendorDir, resolve(distDir, "vendor"), { recursive: true });
copyFileSync(
  resolve(repoRoot, "js/primadb-gun.js"),
  resolve(distDir, "runtime/primadb-gun.js"),
);
removeFilesNamed(resolve(distDir, "vendor"), ".gitignore");

console.log(`Built TypeScript package in ${distDir}`);
