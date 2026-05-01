---
title: Gun Runtime API
sidebar_position: 5
---

This page covers the browser Gun-compatible entrypoint and the typed runtime installer contract used by `primadb/gun`.

> This page is generated from the current package source declarations.

## `packages/primadb/gun.ts`

Public `primadb/gun` entrypoint.

### Direct exports

#### `PrimadbGunInitInput`

Kind: type alias

```ts
export type PrimadbGunInitInput = Parameters<typeof initPrimadb>[0];
```

#### `PrimadbGun`

Kind: type alias

```ts
export type PrimadbGun = ReturnType<typeof installPrimadbGunRuntime>;
```

#### `initPrimadbGun`

Kind: function

```ts
export declare function initPrimadbGun(input?: PrimadbGunInitInput): Promise<PrimadbGun>;
```

### Re-exports

```ts
export { installPrimadbGunRuntime };
```

```ts
export default initPrimadbGun;
```

## `packages/primadb/runtime/primadb-gun.ts`

Typed runtime installer surface.

### Direct exports

#### `PrimadbGunBindings`

Kind: interface

```ts
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
```

#### `PrimadbGunStatic`

Kind: interface

```ts
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
```

#### `installPrimadbGunRuntime`

Kind: function

```ts
export declare function installPrimadbGunRuntime(_bindings: PrimadbGunBindings): PrimadbGunStatic;
```
