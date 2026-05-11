import type { Primadb } from "./index.js";

export interface PrimadbMoqSyncPayload {
  type: "primadb.sync.v1";
  from: string;
  sentAt: number;
  envelope?: unknown;
  envelopeJson?: string;
}

export type PrimadbRouteTarget =
  | { kind: "broadcast" }
  | { kind: "peer"; value: string }
  | { kind: "topic"; value: string };

export type PrimadbRoutePayload =
  | { kind: "sync"; encoding: string; payload: unknown }
  | { kind: "presence"; peer: PrimadbPeerPresence }
  | { kind: "peer_exchange"; peers: PrimadbPeerRecommendation[] }
  | { kind: "batch"; items: PrimadbRouteBatchItem[] }
  | { kind: string; [key: string]: unknown };

export type PrimadbRouteBatchItem =
  | { kind: "sync"; encoding: string; payload: unknown }
  | { kind: "presence"; peer: PrimadbPeerPresence }
  | { kind: "peer_exchange"; peers: PrimadbPeerRecommendation[] }
  | { kind: string; [key: string]: unknown };

export interface PrimadbPeerPresence {
  peer_id: string;
  replica_id: string;
  transport: string;
  identity?: unknown;
  capabilities?: string[];
  topics?: string[];
  metadata?: Record<string, string>;
}

export interface PrimadbPeerRecommendation {
  peer: PrimadbPeerPresence;
  relay_urls?: string[];
  score?: number;
  discovered_at_millis?: number;
}

export interface PrimadbRouteEnvelope {
  route_id: string;
  from: string;
  channel: string;
  target: PrimadbRouteTarget;
  ttl: number;
  hops: number;
  issued_at_millis: number;
  reply_to?: string | null;
  content_hash?: string | null;
  seen_by: string[];
  payload: PrimadbRoutePayload;
}

export interface PrimadbMoqRoutePayload {
  type: "primadb.route.v1";
  from: string;
  sentAt: number;
  route: PrimadbRouteEnvelope;
}

export type PrimadbRouteHandler = (route: PrimadbRouteEnvelope) => void;

export interface PrimadbMoqSessionOptions {
  path: string;
  track?: string;
  channel?: string;
  peerId?: string;
  target?: PrimadbRouteTarget;
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
  channel?: string;
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
  readonly channel: string;
  readonly peerId: string;
  readonly intervalMs: number;
  publish(): unknown;
  subscribe(path?: string): unknown;
  startAutoFlush(): void;
  onRoute(handler: PrimadbRouteHandler): () => void;
  knownPeers(): PrimadbPeerPresence[];
  recommendedPeers(): PrimadbPeerRecommendation[];
  createRoute(
    payload: PrimadbRoutePayload,
    target?: PrimadbRouteTarget,
    replyTo?: string | null,
  ): PrimadbRouteEnvelope;
  sendRoute(route: PrimadbRouteEnvelope): number;
  announcePresence(): number;
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
