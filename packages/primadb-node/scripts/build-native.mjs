#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { copyFileSync, existsSync, readdirSync, rmSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { currentBindingName, currentCargoLibraryName } from "../binding-name.js";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const packageDir = resolve(scriptDir, "..");

function run(command, args, cwd) {
  const result = spawnSync(command, args, {
    cwd,
    stdio: "inherit",
    env: process.env,
  });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

for (const entry of readdirSync(packageDir)) {
  if (entry.endsWith(".node")) {
    rmSync(resolve(packageDir, entry), { force: true });
  }
}

run("cargo", ["build", "--release"], packageDir);

const source = resolve(packageDir, "target/release", currentCargoLibraryName());
if (!existsSync(source)) {
  console.error(`Missing built library: ${source}`);
  process.exit(1);
}

const output = resolve(packageDir, currentBindingName());
copyFileSync(source, output);

console.log(`Built ${output}`);
