#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "..");

function usage() {
  console.error(`Usage:
  node ./scripts/cut-release.mjs <version>

Behavior:
  - verifies the git worktree is clean
  - syncs Cargo.toml and package versions to <version>
  - creates a release commit if manifest versions changed
  - creates an annotated tag v<version>

After it succeeds, push with:
  git push --follow-tags origin master`);
}

function run(command, args, options = {}) {
  return execFileSync(command, args, {
    cwd: repoRoot,
    encoding: "utf8",
    stdio: ["inherit", "pipe", "pipe"],
    ...options,
  }).trim();
}

function runStreaming(command, args) {
  execFileSync(command, args, {
    cwd: repoRoot,
    stdio: "inherit",
  });
}

function runQuiet(command, args) {
  execFileSync(command, args, {
    cwd: repoRoot,
    stdio: ["inherit", "ignore", "inherit"],
  });
}

function ensureCleanWorktree() {
  const status = run("git", ["status", "--porcelain"]);
  if (status.length > 0) {
    throw new Error("git worktree is not clean; commit or stash changes before cutting a release");
  }
}

function ensureTagAbsent(tagName) {
  try {
    run("git", ["rev-parse", "--verify", "--quiet", `refs/tags/${tagName}`]);
    throw new Error(`tag ${tagName} already exists`);
  } catch (error) {
    if (error.message === `tag ${tagName} already exists`) {
      throw error;
    }
  }
}

function currentVersion() {
  const output = run("node", ["./scripts/version-sync.mjs", "print"]);
  const line = output.split("\n").find((entry) => entry.startsWith("Cargo.toml "));
  if (!line) {
    throw new Error("could not determine current Cargo.toml version");
  }
  return line.replace("Cargo.toml ", "");
}

function main() {
  const [, , version] = process.argv;
  if (!version || version === "--help" || version === "-h") {
    usage();
    process.exit(version ? 0 : 1);
  }

  ensureCleanWorktree();
  const before = currentVersion();
  const tagName = `v${version}`;
  ensureTagAbsent(tagName);

  if (before !== version) {
    runStreaming("node", ["./scripts/version-sync.mjs", "set", version]);
    runQuiet("cargo", ["metadata", "--format-version", "1", "--no-deps"]);
    runStreaming("git", [
      "add",
      "Cargo.toml",
      "Cargo.lock",
      "packages/primadb/package.json",
      "packages/primadb-node/package.json",
      "packages/primadb-python/pyproject.toml",
    ]);
    runStreaming("git", ["commit", "-m", `Release ${version}`]);
  }

  runStreaming("git", ["tag", "-a", tagName, "-m", `Release ${version}`]);
  console.log(`Created ${tagName}.`);
  console.log("Push with: git push --follow-tags origin master");
}

try {
  main();
} catch (error) {
  console.error(error.message);
  process.exit(1);
}
