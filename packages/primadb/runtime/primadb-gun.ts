import type * as PrimadbBindings from "../vendor/default/primadb.js";

export interface PrimadbGunBindings {
  Primadb: typeof PrimadbBindings.Primadb;
  Chain: typeof PrimadbBindings.Chain;
  Subscription: typeof PrimadbBindings.Subscription;
  WebSocketSync: typeof PrimadbBindings.WebSocketSync;
  WebRtcMesh: typeof PrimadbBindings.WebRtcMesh;
  derivePasswordKey: typeof PrimadbBindings.derivePasswordKey;
  generateSeaPair: typeof PrimadbBindings.generateSeaPair;
  seaDecrypt: typeof PrimadbBindings.seaDecrypt;
  seaEncrypt: typeof PrimadbBindings.seaEncrypt;
  seaPairFromPrivateKeys: typeof PrimadbBindings.seaPairFromPrivateKeys;
  seaSecret: typeof PrimadbBindings.seaSecret;
  seaSign: typeof PrimadbBindings.seaSign;
  seaVerify: typeof PrimadbBindings.seaVerify;
}

export interface PrimadbGunStatic {
  (options?: Record<string, unknown>): any;
  chain: any;
  User: any;
  SEA: any;
  state(): number;
  text: {
    random(length?: number): string;
  };
}

export function installPrimadbGunRuntime(_bindings: PrimadbGunBindings): PrimadbGunStatic {
  throw new Error("installPrimadbGunRuntime is replaced with the built runtime artifact");
}
