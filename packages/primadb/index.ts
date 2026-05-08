import "./globals.js";
import initWasm, { Primadb } from "./vendor/default/primadb.js";

export * from "./vendor/default/primadb.js";
export * from "./hooks.js";
export * from "./types.js";

export type PrimadbInitInput = Parameters<typeof initWasm>[0];
export type PrimadbInitOutput = Awaited<ReturnType<typeof initWasm>>;

export { initWasm };

export async function initPrimadb(input?: PrimadbInitInput): Promise<PrimadbInitOutput> {
  return initWasm(input);
}

export async function createPrimadb(
  replicaId?: string | null,
  input?: PrimadbInitInput,
): Promise<Primadb> {
  await initPrimadb(input);
  return new Primadb(replicaId ?? undefined);
}

export default initPrimadb;
