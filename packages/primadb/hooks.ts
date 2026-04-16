import type { Primadb } from "./vendor/default/primadb.js";

export interface ConnectHookContext {
  peer: {
    peerId: string;
    replicaId: string;
    transport: string;
    capabilities?: string[];
    topics?: string[];
    metadata?: Record<string, string>;
  };
  transport: "relay" | "mesh";
  relayUrl?: string | null;
}

export interface RoomHookContext {
  peerId: string;
  room: string;
  transport: "relay" | "mesh";
  peer?: ConnectHookContext["peer"] | null;
}

export type PullRequestKind =
  | { kind: "get"; path: { anchor: string; segments?: string[] } }
  | { kind: "map"; path: { anchor: string; segments?: string[] } }
  | { kind: "query"; path: { anchor: string; segments?: string[] }; spec: Record<string, unknown> }
  | { kind: "lex"; path: { anchor: string; segments?: string[] }; spec: Record<string, unknown> }
  | { kind: "snapshot"; root?: string | null };

export type RemoteResult =
  | { kind: "get"; value: unknown | null }
  | { kind: "map"; entries: unknown[] }
  | { kind: "query"; entries: unknown[] }
  | { kind: "lex"; entries: unknown[] }
  | { kind: "snapshot"; snapshot: unknown };

export interface ServeRequestContext {
  peerId: string;
  transport: "relay" | "mesh";
  requestId?: string | null;
  watchId?: string | null;
  request: PullRequestKind;
}

export interface ServeResultContext {
  peerId: string;
  transport: "relay" | "mesh";
  requestId?: string | null;
  watchId?: string | null;
  request: PullRequestKind;
  initial: boolean;
}

export type VoidHookDecision =
  | boolean
  | string
  | {
      allow?: boolean;
      message?: string;
    }
  | null
  | undefined;

export type RequestHookDecision =
  | VoidHookDecision
  | PullRequestKind
  | {
      allow?: boolean;
      message?: string;
      request?: PullRequestKind;
    };

export type ResultHookDecision =
  | VoidHookDecision
  | RemoteResult
  | {
      allow?: boolean;
      message?: string;
      result?: RemoteResult;
    };

export interface NetworkHooks {
  onConnect?(context: ConnectHookContext): VoidHookDecision;
  onJoinRoom?(context: RoomHookContext): VoidHookDecision;
  onPull?(context: ServeRequestContext): RequestHookDecision;
  onWatch?(context: ServeRequestContext): RequestHookDecision;
  onServeResult?(context: ServeResultContext, result: RemoteResult): ResultHookDecision;
}

type HookablePrimadb = Primadb & {
  setNetworkHooks(hooks: unknown): void;
  clearNetworkHooks(): void;
};

export function setNetworkHooks(db: Primadb, hooks: NetworkHooks | null | undefined): void {
  const target = db as HookablePrimadb;
  if (hooks == null) {
    target.clearNetworkHooks();
    return;
  }
  target.setNetworkHooks(hooks);
}

export function clearNetworkHooks(db: Primadb): void {
  (db as HookablePrimadb).clearNetworkHooks();
}
