#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import {
  copyFileSync,
  cpSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
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

function patchThreadWorkerHelper(rootDir) {
  const snippetsDir = resolve(rootDir, "vendor/threads/snippets");
  const helperDir = readdirSync(snippetsDir, { withFileTypes: true }).find(
    (entry) => entry.isDirectory() && entry.name.startsWith("wasm-bindgen-rayon-"),
  );
  if (!helperDir) {
    throw new Error(`Could not find wasm-bindgen-rayon snippet directory under ${snippetsDir}`);
  }

  const helperPath = resolve(
    snippetsDir,
    helperDir.name,
    "src/workerHelpers.no-bundler.js",
  );
  const current = readFileSync(helperPath, "utf8");
  if (current.includes("const workerBootstrapSource = `")) {
    return;
  }

  const replacement = `/*
 * Copyright 2022 Google Inc. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

function waitForMsgType(target, type) {
  return new Promise((resolve) => {
    target.addEventListener("message", function onMsg({ data }) {
      if (data == null || data.type !== type) {
        return;
      }
      target.removeEventListener("message", onMsg);
      resolve(data);
    });
  });
}

const workerBootstrapSource = \`
function waitForMsgType(target, type) {
  return new Promise((resolve) => {
    target.addEventListener("message", function onMsg({ data }) {
      if (data == null || data.type !== type) {
        return;
      }
      target.removeEventListener("message", onMsg);
      resolve(data);
    });
  });
}

waitForMsgType(self, "wasm_bindgen_worker_init").then(async (data) => {
  const pkg = await import(data.mainJS);
  await pkg.default(data.module, data.memory);
  postMessage({ type: "wasm_bindgen_worker_ready" });
  pkg.wbg_rayon_start_worker(data.receiver);
});
\`;

let _workers;

export async function startWorkers(module, memory, builder) {
  if (builder.numThreads() === 0) {
    throw new Error(\`num_threads must be > 0.\`);
  }

  const workerInit = {
    type: "wasm_bindgen_worker_init",
    module,
    memory,
    receiver: builder.receiver(),
    mainJS: builder.mainJS(),
  };

  _workers = await Promise.all(
    Array.from({ length: builder.numThreads() }, async () => {
      const scriptBlob = new Blob([workerBootstrapSource], {
        type: "text/javascript",
      });
      const url = URL.createObjectURL(scriptBlob);
      const worker = new Worker(url, {
        type: "module",
      });
      worker.postMessage(workerInit);
      await waitForMsgType(worker, "wasm_bindgen_worker_ready");
      URL.revokeObjectURL(url);
      return worker;
    }),
  );
  builder.build();
}
`;
  writeFileSync(helperPath, replacement);
}

rmSync(vendorDir, { recursive: true, force: true });
rmSync(distDir, { recursive: true, force: true });
mkdirSync(resolve(distDir, "runtime"), { recursive: true });

run(
  "./build-wasm.sh",
  ["--out-dir", "packages/primadb/vendor/default", "--features", "crypto,scripting"],
  repoRoot,
);
run(
  "./build-wasm-threads.sh",
  ["--out-dir", "packages/primadb/vendor/threads", "--features", "crypto,scripting"],
  repoRoot,
);

run("pnpm", ["exec", "tsc", "-p", "tsconfig.json"], packageDir);

cpSync(vendorDir, resolve(distDir, "vendor"), { recursive: true });
copyFileSync(
  resolve(repoRoot, "js/primadb-gun.js"),
  resolve(distDir, "runtime/primadb-gun.js"),
);
removeFilesNamed(resolve(distDir, "vendor"), ".gitignore");
patchThreadWorkerHelper(distDir);

console.log(`Built TypeScript package in ${distDir}`);
