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

export type PrimadbRouteTransportKind =
  | "web_socket"
  | "moq"
  | "web_rtc"
  | "broadcast_channel"
  | "in_memory";

export type PrimadbRoutePayload =
  | { kind: "application"; message: PrimadbApplicationRouteMessage }
  | { kind: "sync"; encoding: string; payload: unknown }
  | { kind: "presence"; peer: PrimadbPeerPresence }
  | { kind: "peer_exchange"; peers: PrimadbPeerRecommendation[] }
  | { kind: "batch"; items: PrimadbRouteBatchItem[] }
  | { kind: string; [key: string]: unknown };

export type PrimadbRouteBatchItem =
  | { kind: "application"; message: PrimadbApplicationRouteMessage }
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

export interface PrimadbApplicationRouteMessage {
  namespace: string;
  protocol: string;
  topic?: string | null;
  body: unknown;
  metadata?: Record<string, unknown>;
}

export type PrimadbApplicationRouteAuthStatus =
  | "unknown"
  | "not_required"
  | "unauthenticated"
  | "authenticated"
  | "required_but_missing";

export interface PrimadbApplicationRouteContext {
  sourcePeerId: string;
  transport: PrimadbRouteTransportKind;
  underlayId?: string | null;
  direct: boolean;
  relayRouted: boolean;
  gatewayRouted: boolean;
  gatewayPeerId?: string | null;
  authStatus: PrimadbApplicationRouteAuthStatus;
  provenance: string[];
}

export interface PrimadbApplicationRouteEvent {
  routeId: string;
  from: string;
  channel: string;
  target: PrimadbRouteTarget;
  issuedAtMillis: number;
  receivedAtMillis: number;
  transport: PrimadbRouteTransportKind;
  verifiedIdentity: null;
  context: PrimadbApplicationRouteContext;
  message: PrimadbApplicationRouteMessage;
}

export interface PrimadbApplicationRouteFilter {
  namespace?: string | null;
  protocol?: string | null;
  topic?: string | null;
}

export type PrimadbRouteOverlaySendMode = "first_success" | "fan_out";

export interface PrimadbRouteOverlayPolicy {
  preferredTransports?: PrimadbRouteTransportKind[];
  sendMode?: PrimadbRouteOverlaySendMode;
  directFirst?: boolean;
  allowDirect?: boolean;
  allowRelay?: boolean;
  requireDirect?: boolean;
}

export interface PrimadbRouteOverlayUnderlayInfo {
  id: string;
  transport: PrimadbRouteTransportKind;
  direct?: boolean;
  relayRouted?: boolean;
  connected?: boolean;
  priority?: number;
  metadata?: Record<string, string>;
}

export interface PrimadbRouteOverlayUnderlay {
  info(): PrimadbRouteOverlayUnderlayInfo;
  sendRoute(route: PrimadbRouteEnvelope): number | Promise<number>;
  drainRoutes?(): PrimadbRouteEnvelope[];
  close?(): void;
}

export interface PrimadbRouteOverlayDeliveryAttempt {
  underlay: PrimadbRouteOverlayUnderlayInfo;
  attemptedAtMillis: number;
  success: boolean;
  message?: string | null;
}

export interface PrimadbRouteOverlaySendReport {
  route: PrimadbRouteEnvelope;
  attempts: PrimadbRouteOverlayDeliveryAttempt[];
  deliveredUnderlayIds: string[];
  failedUnderlayIds: string[];
  deliveredPeerIds: string[];
  fallbackReason?: string | null;
  duplicateSuppressed: number;
}

export interface PrimadbRouteOverlayPumpReport {
  receivedRoutes: number;
  deliveredApplicationRoutes: number;
  deliveredStreamEvents: number;
  duplicateSuppressed: number;
  underlayIds: string[];
}

export type PrimadbApplicationStreamFrameKind =
  | "open"
  | "data"
  | "ack"
  | "nack"
  | "close"
  | "error";

export interface PrimadbApplicationStreamFrame {
  streamId: string;
  sequence: number;
  kind: PrimadbApplicationStreamFrameKind;
  namespace: string;
  protocol: string;
  topic?: string | null;
  chunk?: string | null;
  finalChunk?: boolean;
  ackSequence?: number | null;
  error?: string | null;
  metadata?: Record<string, unknown>;
}

export interface PrimadbApplicationStreamEvent {
  streamId: string;
  from: string;
  transport: PrimadbRouteTransportKind;
  namespace: string;
  protocol: string;
  topic?: string | null;
  body: unknown;
  metadata: Record<string, unknown>;
}

export interface PrimadbApplicationStreamSendOptions {
  namespace: string;
  protocol: string;
  topic?: string | null;
  body: unknown;
  metadata?: Record<string, unknown>;
  target?: PrimadbRouteTarget;
  maxChunkChars?: number;
}

export interface PrimadbApplicationStreamSendReport {
  streamId: string;
  frameReports: PrimadbRouteOverlaySendReport[];
}

export interface PrimadbMoqSessionOptions {
  path: string;
  track?: string;
  channel?: string;
  peerId?: string;
  target?: PrimadbRouteTarget;
  intervalMs?: number;
  retryIntervalMs?: number;
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
  nodeWebTransport?: boolean;
  nodeWebTransportOptions?: unknown;
  tlsDisableVerify?: boolean;
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
  nodeWebTransportProvider: boolean;
  webSocket: boolean;
  websocketFallback: boolean;
};

export declare function connectPrimadbMoq(
  db: Primadb,
  options: ConnectPrimadbMoqOptions,
): Promise<PrimadbMoqSession>;

export declare class PrimadbApplicationRouteSubscription {
  readonly filter: PrimadbApplicationRouteFilter;
  next(): Promise<PrimadbApplicationRouteEvent | null>;
  tryNext(): PrimadbApplicationRouteEvent | null;
  drain(): PrimadbApplicationRouteEvent[];
  close(): void;
}

export declare const PRIMADB_APPLICATION_STREAM_NAMESPACE: "primadb.applicationStream";
export declare const PRIMADB_APPLICATION_STREAM_PROTOCOL_V1: "primadb.applicationStream.v1";

export declare class PrimadbRouteOverlaySession {
  readonly peerId: string;
  readonly channel: string;
  readonly ttl: number;
  constructor(options: {
    peerId: string;
    channel?: string;
    ttl?: number;
    policy?: PrimadbRouteOverlayPolicy;
  });
  policy(): Required<PrimadbRouteOverlayPolicy>;
  setPolicy(policy: PrimadbRouteOverlayPolicy): void;
  addUnderlay(underlay: PrimadbRouteOverlayUnderlay): void;
  removeUnderlay(id: string): PrimadbRouteOverlayUnderlayInfo | null;
  underlays(): PrimadbRouteOverlayUnderlayInfo[];
  createRoute(payload: PrimadbRoutePayload, target?: PrimadbRouteTarget, replyTo?: string | null): PrimadbRouteEnvelope;
  publishApplication(
    message: PrimadbApplicationRouteMessage,
    target?: PrimadbRouteTarget,
  ): Promise<PrimadbRouteOverlaySendReport>;
  sendApplication(
    namespace: string,
    protocol: string,
    topic: string | null | undefined,
    body: unknown,
    metadata?: Record<string, unknown>,
    target?: PrimadbRouteTarget,
  ): Promise<PrimadbRouteOverlaySendReport>;
  sendRoute(route: PrimadbRouteEnvelope): Promise<PrimadbRouteOverlaySendReport>;
  subscribeApplications(filter?: PrimadbApplicationRouteFilter): PrimadbApplicationRouteSubscription;
  nextApplication(filter?: PrimadbApplicationRouteFilter): Promise<PrimadbApplicationRouteEvent | null>;
  tryNextApplication(filter?: PrimadbApplicationRouteFilter): PrimadbApplicationRouteEvent | null;
  drainApplications(filter?: PrimadbApplicationRouteFilter): PrimadbApplicationRouteEvent[];
  pump(): PrimadbRouteOverlayPumpReport;
  sendApplicationStream(
    options: PrimadbApplicationStreamSendOptions,
  ): Promise<PrimadbApplicationStreamSendReport>;
  drainStreamEvents(): PrimadbApplicationStreamEvent[];
  close(): void;
}

export declare function primadbMoqOverlayUnderlay(
  id: string,
  session: PrimadbMoqSession,
  options?: { priority?: number; maxQueue?: number; metadata?: Record<string, string> },
): PrimadbRouteOverlayUnderlay;

export declare class PrimadbMoqSession {
  readonly db: Primadb;
  readonly connection: unknown;
  readonly path: string;
  readonly track: string;
  readonly channel: string;
  readonly peerId: string;
  readonly intervalMs: number;
  readonly retryIntervalMs: number;
  publish(): unknown;
  subscribe(path?: string): unknown;
  startAutoFlush(): void;
  onRoute(handler: PrimadbRouteHandler): () => void;
  publishApplication(message: PrimadbApplicationRouteMessage, target?: PrimadbRouteTarget): number;
  sendApplication(
    namespace: string,
    protocol: string,
    topic: string | null | undefined,
    body: unknown,
    metadata?: Record<string, unknown>,
    target?: PrimadbRouteTarget,
  ): number;
  subscribeApplications(filter?: PrimadbApplicationRouteFilter): PrimadbApplicationRouteSubscription;
  nextApplication(filter?: PrimadbApplicationRouteFilter): Promise<PrimadbApplicationRouteEvent | null>;
  tryNextApplication(filter?: PrimadbApplicationRouteFilter): PrimadbApplicationRouteEvent | null;
  drainApplications(filter?: PrimadbApplicationRouteFilter): PrimadbApplicationRouteEvent[];
  addAcceptedPeerId(peerId: string): () => void;
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
