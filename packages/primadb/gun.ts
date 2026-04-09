import "./globals.js";
import * as bindings from "./vendor/default/primadb.js";
import { initPrimadb } from "./index.js";
import { installPrimadbGunRuntime } from "./runtime/primadb-gun.js";

export type PrimadbGunInitInput = Parameters<typeof initPrimadb>[0];
export type PrimadbGun = ReturnType<typeof installPrimadbGunRuntime>;

export { installPrimadbGunRuntime };

export async function initPrimadbGun(input?: PrimadbGunInitInput): Promise<PrimadbGun> {
  await initPrimadb(input);
  return installPrimadbGunRuntime(bindings);
}

export default initPrimadbGun;
