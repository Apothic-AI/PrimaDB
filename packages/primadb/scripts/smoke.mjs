#!/usr/bin/env node
import { readFileSync } from "node:fs";

const base = await import("../dist/index.js");
const gun = await import("../dist/gun.js");

if (typeof globalThis.self === "undefined") {
  globalThis.self = {
    addEventListener() {},
    removeEventListener() {},
  };
}

const threads = await import("../dist/threads.js");
const wasm = readFileSync(new URL("../dist/vendor/default/primadb_bg.wasm", import.meta.url));

await base.initPrimadb({ module_or_path: wasm });
const db = new base.Primadb("browser-script-smoke");
db.chain("notes").field("scripted").put({ title: "Browser scripted note" });
const scriptPath = { anchor: "notes", segments: ["scripted"] };
const scriptCapabilities = {
  read: [{ root: "notes", recursive: true }],
  write: [{ root: "derived", recursive: true }],
  transaction: [{ root: "derived", recursive: true }],
};
db.attachNodeScript(scriptPath, {
  id: "derive-title",
  source: `
    fn main(ctx) {
      let note = db_get("notes/scripted");
      db_put("derived/scripted", #{ title: note.title, source: ctx.path.display });
      return #{ title: note.title };
    }
  `,
  capabilities: scriptCapabilities,
});
const scriptResults = db.executeNodeScripts(scriptPath, { capabilities: scriptCapabilities });
const scripted = db.chain("derived").field("scripted").once();

const report = {
  base: {
    initPrimadb: typeof base.initPrimadb === "function",
    createPrimadb: typeof base.createPrimadb === "function",
    Primadb: typeof base.Primadb === "function",
    derivePasswordKey: typeof base.derivePasswordKey === "function",
    scope: typeof base.Primadb?.prototype?.scope === "function",
    transaction: typeof base.Primadb?.prototype?.transaction === "function",
    attachNodeScript: typeof base.Primadb?.prototype?.attachNodeScript === "function",
    nodeScripts: typeof base.Primadb?.prototype?.nodeScripts === "function",
    executeNodeScripts: typeof base.Primadb?.prototype?.executeNodeScripts === "function",
    Scope: typeof base.Scope === "function",
    setNetworkHooks: typeof base.setNetworkHooks === "function",
    clearNetworkHooks: typeof base.clearNetworkHooks === "function",
    scriptingRoundTrip:
      Array.isArray(scriptResults) &&
      scriptResults[0]?.report?.status === "committed" &&
      scripted?.title === "Browser scripted note" &&
      scripted?.source === "notes/scripted",
  },
  threads: {
    initPrimadbThreads: typeof threads.initPrimadbThreads === "function",
    bootstrapPrimadbThreads: typeof threads.bootstrapPrimadbThreads === "function",
    derivePasswordKey: typeof threads.derivePasswordKey === "function",
    initThreadPool: typeof threads.initThreadPool === "function",
    executeNodeScripts: typeof threads.Primadb?.prototype?.executeNodeScripts === "function",
    setNetworkHooks: typeof threads.setNetworkHooks === "function",
    clearNetworkHooks: typeof threads.clearNetworkHooks === "function",
  },
  gun: {
    initPrimadbGun: typeof gun.initPrimadbGun === "function",
    installPrimadbGunRuntime: typeof gun.installPrimadbGunRuntime === "function",
  },
};

for (const section of Object.values(report)) {
  for (const value of Object.values(section)) {
    if (!value) {
      console.error(JSON.stringify(report, null, 2));
      process.exit(1);
    }
  }
}

console.log(JSON.stringify(report, null, 2));
