import type { Primadb } from "./vendor/default/primadb.js";
import type { RecordScan, RecordScanResult } from "./types.js";

export interface PresenceIdentity {
  publicKey: string;
  alias?: string | null;
  keyScheme?: string;
  sessionId: string;
  claims?: Record<string, string>;
  issuedAtMillis?: number;
  expiresAtMillis?: number | null;
}

export type IdentityTrust = "verified" | "trusted_public_key" | "trusted_alias";

export interface VerifiedIdentity {
  publicKey: string;
  alias?: string | null;
  peerId: string;
  replicaId: string;
  transport: string;
  sessionId: string;
  claims?: Record<string, string>;
  issuedAtMillis: number;
  expiresAtMillis?: number | null;
  trust: IdentityTrust;
}

export interface ConnectHookContext {
  peer: {
    peerId: string;
    replicaId: string;
    transport: string;
    identity?: PresenceIdentity | null;
    capabilities?: string[];
    topics?: string[];
    metadata?: Record<string, string>;
  };
  transport: "relay" | "mesh";
  relayUrl?: string | null;
  verifiedIdentity?: VerifiedIdentity | null;
}

export interface RoomHookContext {
  peerId: string;
  room: string;
  transport: "relay" | "mesh";
  peer?: ConnectHookContext["peer"] | null;
  verifiedIdentity?: VerifiedIdentity | null;
}

export type PullRequestKind =
  | { kind: "get"; path: { anchor: string; segments?: string[] } }
  | { kind: "map"; path: { anchor: string; segments?: string[] } }
  | { kind: "query"; path: { anchor: string; segments?: string[] }; spec: Record<string, unknown> }
  | { kind: "lex"; path: { anchor: string; segments?: string[] }; spec: Record<string, unknown> }
  | { kind: "records"; scan: RecordScan }
  | { kind: "node"; id: string }
  | { kind: "snapshot"; root?: string | null };

export type RemoteResult =
  | { kind: "get"; value: unknown | null }
  | { kind: "map"; entries: unknown[] }
  | { kind: "query"; entries: unknown[] }
  | { kind: "lex"; entries: unknown[] }
  | { kind: "records"; result: RecordScanResult }
  | { kind: "node"; node: unknown | null }
  | { kind: "snapshot"; snapshot: unknown };

export interface ServeRequestContext {
  peerId: string;
  transport: "relay" | "mesh";
  requestId?: string | null;
  watchId?: string | null;
  request: PullRequestKind;
  verifiedIdentity?: VerifiedIdentity | null;
}

export interface ServeResultContext {
  peerId: string;
  transport: "relay" | "mesh";
  requestId?: string | null;
  watchId?: string | null;
  request: PullRequestKind;
  initial: boolean;
  verifiedIdentity?: VerifiedIdentity | null;
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
