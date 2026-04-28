#!/usr/bin/env node
const base = await import("../dist/index.js");
const gun = await import("../dist/gun.js");

if (typeof globalThis.self === "undefined") {
  globalThis.self = {
    addEventListener() {},
    removeEventListener() {},
  };
}

const threads = await import("../dist/threads.js");

const report = {
  base: {
    initPrimadb: typeof base.initPrimadb === "function",
    createPrimadb: typeof base.createPrimadb === "function",
    Primadb: typeof base.Primadb === "function",
    scope: typeof base.Primadb?.prototype?.scope === "function",
    transaction: typeof base.Primadb?.prototype?.transaction === "function",
    Scope: typeof base.Scope === "function",
    setNetworkHooks: typeof base.setNetworkHooks === "function",
    clearNetworkHooks: typeof base.clearNetworkHooks === "function",
  },
  threads: {
    initPrimadbThreads: typeof threads.initPrimadbThreads === "function",
    bootstrapPrimadbThreads: typeof threads.bootstrapPrimadbThreads === "function",
    initThreadPool: typeof threads.initThreadPool === "function",
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
