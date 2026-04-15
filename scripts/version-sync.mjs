#!/usr/bin/env node
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "..");

const cargoManifestPath = resolve(repoRoot, "Cargo.toml");
const browserPackagePath = resolve(repoRoot, "packages/primadb/package.json");
const nodePackagePath = resolve(repoRoot, "packages/primadb-node/package.json");
const pythonProjectPath = resolve(repoRoot, "packages/primadb-python/pyproject.toml");

const packageTargets = [
  { id: "packages/primadb", path: browserPackagePath, kind: "package-json" },
  { id: "packages/primadb-node", path: nodePackagePath, kind: "package-json" },
  { id: "packages/primadb-python", path: pythonProjectPath, kind: "toml-project" },
];

function usage() {
  console.error(`Usage:
  node ./scripts/version-sync.mjs check [--expect <version>]
  node ./scripts/version-sync.mjs sync
  node ./scripts/version-sync.mjs set <version>
  node ./scripts/version-sync.mjs print

Behavior:
  - Cargo.toml is the source of truth.
  - check validates that all package manifests match Cargo.toml.
  - sync rewrites package manifests to match Cargo.toml.
  - set updates Cargo.toml and all package manifests to the provided version.`);
}

function readText(path) {
  return readFileSync(path, "utf8");
}

function writeText(path, text) {
  writeFileSync(path, text, "utf8");
}

function findTomlSectionRange(text, sectionName) {
  const lines = text.split("\n");
  const header = `[${sectionName}]`;
  const start = lines.findIndex((line) => line.trim() === header);
  if (start === -1) {
    throw new Error(`missing TOML section ${header}`);
  }
  let end = lines.length;
  for (let index = start + 1; index < lines.length; index += 1) {
    const trimmed = lines[index].trim();
    if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
      end = index;
      break;
    }
  }
  return { lines, start, end };
}

function readTomlVersion(path, sectionName) {
  const text = readText(path);
  const { lines, start, end } = findTomlSectionRange(text, sectionName);
  for (let index = start + 1; index < end; index += 1) {
    const match = lines[index].match(/^\s*version\s*=\s*"([^"]+)"\s*$/);
    if (match) {
      return match[1];
    }
  }
  throw new Error(`missing version in [${sectionName}] of ${path}`);
}

function replaceTomlVersion(path, sectionName, version) {
  const text = readText(path);
  const { lines, start, end } = findTomlSectionRange(text, sectionName);
  for (let index = start + 1; index < end; index += 1) {
    if (/^\s*version\s*=/.test(lines[index])) {
      lines[index] = `version = "${version}"`;
      writeText(path, `${lines.join("\n")}${text.endsWith("\n") ? "\n" : ""}`);
      return;
    }
  }
  throw new Error(`missing version in [${sectionName}] of ${path}`);
}

function readPackageJsonVersion(path) {
  return JSON.parse(readText(path)).version;
}

function replacePackageJsonVersion(path, version) {
  const parsed = JSON.parse(readText(path));
  parsed.version = version;
  writeText(path, `${JSON.stringify(parsed, null, 2)}\n`);
}

function readVersion(target) {
  switch (target.kind) {
    case "cargo-package":
      return readTomlVersion(target.path, "package");
    case "toml-project":
      return readTomlVersion(target.path, "project");
    case "package-json":
      return readPackageJsonVersion(target.path);
    default:
      throw new Error(`unsupported target kind ${target.kind}`);
  }
}

function writeVersion(target, version) {
  switch (target.kind) {
    case "cargo-package":
      replaceTomlVersion(target.path, "package", version);
      return;
    case "toml-project":
      replaceTomlVersion(target.path, "project", version);
      return;
    case "package-json":
      replacePackageJsonVersion(target.path, version);
      return;
    default:
      throw new Error(`unsupported target kind ${target.kind}`);
  }
}

function assertVersionFormat(version) {
  if (!/^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/.test(version)) {
    throw new Error(`invalid version '${version}'`);
  }
}

function collectVersions() {
  const cargo = { id: "Cargo.toml", path: cargoManifestPath, kind: "cargo-package" };
  return {
    cargo,
    cargoVersion: readVersion(cargo),
    packageVersions: packageTargets.map((target) => ({
      ...target,
      version: readVersion(target),
    })),
  };
}

function runCheck(expectVersion) {
  const { cargoVersion, packageVersions } = collectVersions();
  const mismatches = [];

  if (expectVersion && cargoVersion !== expectVersion) {
    mismatches.push(
      `Cargo.toml version ${cargoVersion} does not match expected tag version ${expectVersion}`,
    );
  }

  for (const target of packageVersions) {
    if (target.version !== cargoVersion) {
      mismatches.push(`${target.id} version ${target.version} does not match Cargo.toml ${cargoVersion}`);
    }
  }

  if (mismatches.length > 0) {
    console.error("Version drift detected:");
    for (const mismatch of mismatches) {
      console.error(`- ${mismatch}`);
    }
    process.exit(1);
  }

  console.log(`All manifest versions are aligned at ${cargoVersion}.`);
}

function runSync() {
  const { cargoVersion, packageVersions } = collectVersions();
  const changed = [];
  for (const target of packageVersions) {
    if (target.version !== cargoVersion) {
      writeVersion(target, cargoVersion);
      changed.push(target.id);
    }
  }

  if (changed.length === 0) {
    console.log(`All package manifests already match Cargo.toml ${cargoVersion}.`);
    return;
  }

  console.log(`Synced ${changed.length} manifest(s) to ${cargoVersion}:`);
  for (const id of changed) {
    console.log(`- ${id}`);
  }
}

function runSet(version) {
  assertVersionFormat(version);
  const cargo = { id: "Cargo.toml", path: cargoManifestPath, kind: "cargo-package" };
  const targets = [cargo, ...packageTargets];
  const changed = [];

  for (const target of targets) {
    if (readVersion(target) !== version) {
      writeVersion(target, version);
      changed.push(target.id);
    }
  }

  if (changed.length === 0) {
    console.log(`All manifests already use ${version}.`);
    return;
  }

  console.log(`Updated ${changed.length} manifest(s) to ${version}:`);
  for (const id of changed) {
    console.log(`- ${id}`);
  }
}

function runPrint() {
  const { cargoVersion, packageVersions } = collectVersions();
  console.log(`Cargo.toml ${cargoVersion}`);
  for (const target of packageVersions) {
    console.log(`${target.id} ${target.version}`);
  }
}

const args = process.argv.slice(2);
const command = args[0];

if (!command || command === "--help" || command === "-h") {
  usage();
  process.exit(command ? 0 : 1);
}

try {
  switch (command) {
    case "check": {
      let expectVersion;
      for (let index = 1; index < args.length; index += 1) {
        if (args[index] === "--expect") {
          expectVersion = args[index + 1];
          index += 1;
          continue;
        }
        throw new Error(`unknown argument '${args[index]}'`);
      }
      if (expectVersion) {
        assertVersionFormat(expectVersion);
      }
      runCheck(expectVersion);
      break;
    }
    case "sync":
      if (args.length !== 1) {
        throw new Error("sync does not accept extra arguments");
      }
      runSync();
      break;
    case "set": {
      const version = args[1];
      if (!version || args.length !== 2) {
        throw new Error("set requires exactly one version argument");
      }
      runSet(version);
      break;
    }
    case "print":
      if (args.length !== 1) {
        throw new Error("print does not accept extra arguments");
      }
      runPrint();
      break;
    default:
      throw new Error(`unknown command '${command}'`);
  }
} catch (error) {
  console.error(error.message);
  process.exit(1);
}
