import type { Primadb } from "./index.js";

export interface PrimadbMoqSyncPayload {
  type: "primadb.sync.v1";
  from: string;
  sentAt: number;
  envelope?: unknown;
  envelopeJson?: string;
}

export interface PrimadbMoqSessionOptions {
  path: string;
  track?: string;
  intervalMs?: number;
  publish?: boolean;
  subscribe?: string[];
  closeConnection?: boolean;
}

export interface ConnectPrimadbMoqOptions extends PrimadbMoqSessionOptions {
  url: string | URL;
  websocketUrl?: string | URL;
  websocket?: boolean;
  webtransport?: unknown;
  transport?: unknown;
}

export interface PrimadbMoqLoopbackOptions {
  publisherDb: Primadb;
  subscriberDb: Primadb;
  path: string;
  track?: string;
  intervalMs?: number;
  url?: string | URL;
  protocol?: string;
}

export declare function moqRuntimeSupport(): {
  webTransport: boolean;
  webSocket: boolean;
  websocketFallback: boolean;
};

export declare function connectPrimadbMoq(
  db: Primadb,
  options: ConnectPrimadbMoqOptions,
): Promise<PrimadbMoqSession>;

export declare class PrimadbMoqSession {
  readonly db: Primadb;
  readonly connection: unknown;
  readonly path: string;
  readonly track: string;
  readonly intervalMs: number;
  publish(): unknown;
  subscribe(path?: string): unknown;
  startAutoFlush(): void;
  flushPending(): Promise<number>;
  close(): void;
}

export interface PrimadbMoqLoopback {
  publisher: PrimadbMoqSession;
  subscriber: PrimadbMoqSession;
  flush(): Promise<number>;
  close(): void;
}

export declare function createPrimadbMoqLoopback(
  options: PrimadbMoqLoopbackOptions,
): Promise<PrimadbMoqLoopback>;
