import "./globals.js";
import initWasm, { Primadb, initThreadPool } from "./vendor/threads/primadb.js";

export * from "./vendor/threads/primadb.js";

export type PrimadbThreadsInitInput = Parameters<typeof initWasm>[0];
export type PrimadbThreadsInitOutput = Awaited<ReturnType<typeof initWasm>>;

export interface ThreadedPrimadbInitOptions {
  input?: PrimadbThreadsInitInput;
  threads?: number;
}

export { initWasm as initWasmThreads };

export function suggestedThreadCount(fallback = 4): number {
  if (typeof navigator !== "undefined" && typeof navigator.hardwareConcurrency === "number") {
    return Math.max(2, navigator.hardwareConcurrency);
  }
  return Math.max(2, fallback);
}

export async function initPrimadbThreads(
  input?: PrimadbThreadsInitInput,
): Promise<PrimadbThreadsInitOutput> {
  return initWasm(input);
}

export async function bootstrapPrimadbThreads(
  options: ThreadedPrimadbInitOptions = {},
): Promise<PrimadbThreadsInitOutput> {
  const output = await initPrimadbThreads(options.input);
  await initThreadPool(options.threads ?? suggestedThreadCount());
  return output;
}

export async function createThreadedPrimadb(
  replicaId?: string | null,
  options: ThreadedPrimadbInitOptions = {},
): Promise<Primadb> {
  await bootstrapPrimadbThreads(options);
  return new Primadb(replicaId ?? undefined);
}

export default bootstrapPrimadbThreads;
