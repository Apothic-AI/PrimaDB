export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

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

export interface RelayClientConfig {
  url: string;
  retryIntervalMs?: number;
}

export interface RelayServerConfig {
  bind: string;
}

export type MeshSignalingMode = "relay" | "broadcast_channel";

export interface IceServerConfig {
  urls: string | string[];
  username?: string | null;
  credential?: string | null;
}

export interface MeshConfig {
  room: string;
  signaling?: MeshSignalingMode;
  relayUrl?: string | null;
  retryIntervalMs?: number;
  iceServers?: IceServerConfig[];
}

export type DurableStorageConfig =
  | {
      kind: "snapshot_file";
      path: string;
    }
  | {
      kind: "segment_files";
      directory: string;
      journalRetention?: number;
    };

export interface DurableStorageBinding {
  backend: string;
  incremental: boolean;
  loadedExisting: boolean;
  autoPersist: boolean;
}

export interface QueryOrder {
  path: string;
  direction?: "asc" | "desc";
}

export type QueryFilter =
  | { kind: "eq"; path: string; value: JsonValue }
  | { kind: "ne"; path: string; value: JsonValue }
  | { kind: "gt"; path: string; value: JsonValue }
  | { kind: "gte"; path: string; value: JsonValue }
  | { kind: "lt"; path: string; value: JsonValue }
  | { kind: "lte"; path: string; value: JsonValue }
  | { kind: "prefix"; path: string; value: string }
  | { kind: "contains"; path: string; value: string }
  | { kind: "exists"; path: string };

export interface QuerySpec {
  filters?: QueryFilter[];
  order?: QueryOrder | null;
  limit?: number | null;
  offset?: number;
}

export interface LexSpec {
  prefix?: string | null;
  startAt?: string | null;
  startAfter?: string | null;
  endAt?: string | null;
  endBefore?: string | null;
  reverse?: boolean;
  limit?: number | null;
  depth?: number;
  followLinks?: boolean;
}

export type TraversalDirection = "outbound" | "inbound" | "both";
export type TraversalStrategy = "bfs" | "dfs";
export type TraversalEdgeKind = "link" | "set_member";

export interface TraversalEdge {
  source: string;
  field: string;
  target: string;
  kind: TraversalEdgeKind;
}

export interface TraversalSpec {
  direction?: TraversalDirection;
  strategy?: TraversalStrategy;
  maxDepth?: number;
  limit?: number | null;
  edgeFields?: string[] | null;
  followLinks?: boolean;
  followSets?: boolean;
  includeStart?: boolean;
  includeValues?: boolean;
  filters?: QueryFilter[];
  fetchMissing?: boolean;
  maxFetches?: number;
}

export interface TraversalEntry {
  nodeId: string;
  depth: number;
  path: string[];
  via?: TraversalEdge | null;
  value?: JsonValue | null;
}

export interface TraversalResult {
  entries: TraversalEntry[];
  complete: boolean;
  timedOut: boolean;
  depthLimitReached: boolean;
  resultLimitReached: boolean;
  fetched: number;
  missing: string[];
  denied: string[];
}

export interface RemotePath {
  anchor: string;
  segments?: string[];
}

export type PullRequestKind =
  | { kind: "get"; path: { anchor: string; segments?: string[] } }
  | { kind: "map"; path: { anchor: string; segments?: string[] } }
  | { kind: "query"; path: { anchor: string; segments?: string[] }; spec: QuerySpec }
  | { kind: "lex"; path: { anchor: string; segments?: string[] }; spec: LexSpec }
  | { kind: "node"; id: string }
  | { kind: "snapshot"; root?: string | null };

export type RemoteResult =
  | { kind: "get"; value: JsonValue | null }
  | { kind: "map"; entries: JsonValue[] }
  | { kind: "query"; entries: JsonValue[] }
  | { kind: "lex"; entries: JsonValue[] }
  | { kind: "node"; node: JsonValue | null }
  | { kind: "snapshot"; snapshot: JsonValue };

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

export interface SubscriptionMessage {
  done: boolean;
  value?: JsonValue | null;
}

export interface RemoteWatchMessage {
  done: boolean;
  initial?: boolean;
  kind?: "get" | "map" | "query" | "lex" | "node" | "snapshot" | null;
  value?: JsonValue | null;
  error?: string | null;
}

export declare class Primadb {
  constructor(replicaId?: string | null);
  replicaId(): string;
  chain(root: string): Chain;
  snapshot(): JsonValue;
  snapshotForRoot(root?: string | null): JsonValue;
  nodeState(id: string): JsonValue | null;
  applyNodeState(node: JsonValue): boolean;
  exportSnapshotJson(): string;
  importSnapshotJson(payload: string): void;
  mergeSnapshotJson(payload: string): void;
  pendingOperations(): JsonValue;
  pendingEnvelope(): JsonValue;
  exportPendingOperationsJson(): string;
  drainPendingOperations(): JsonValue;
  drainPendingEnvelope(): JsonValue;
  drainPendingEnvelopeJson(): string;
  applyOperations(operations: JsonValue): number;
  applyEnvelope(envelope: JsonValue): number;
  applyOperationsJson(payload: string): number;
  openDurableStorage(config: DurableStorageConfig): DurableStorageBinding;
  connectRelay(config: RelayClientConfig): Promise<WebSocketSync>;
  connectMesh(config: MeshConfig): Promise<WebRtcMesh>;
  setNetworkHooks(hooks: NetworkHooks): void;
  clearNetworkHooks(): void;
}

export declare class Chain {
  field(key: string): Chain;
  path(): string;
  put(value: JsonValue): void;
  putSigned(value: JsonValue, certificate?: string | null): void;
  once(): JsonValue | null;
  unset(): void;
  set(value: JsonValue): string;
  setSigned(value: JsonValue, certificate?: string | null): string;
  remove(value: JsonValue): string;
  map(): JsonValue;
  query(spec: QuerySpec): JsonValue;
  firstQuery(spec: QuerySpec): JsonValue | null;
  scan(spec: LexSpec): JsonValue;
  traverse(spec: TraversalSpec): TraversalResult;
  subscribe(): Subscription;
  watchTraverse(spec: TraversalSpec): TraversalSubscription;
}

export declare class Subscription {
  next(): Promise<SubscriptionMessage>;
  tryNext(): SubscriptionMessage;
  close(): void;
}

export declare class TraversalSubscription {
  next(): Promise<{ done: boolean; value?: TraversalResult | null }>;
  tryNext(): { done: boolean; value?: TraversalResult | null };
  close(): void;
}

export declare class RelayServer {
  static listen(config: RelayServerConfig): Promise<RelayServer>;
  bindAddr(): string;
  url(): string;
  clientCount(): number;
  peerCount(): number;
  close(): Promise<void>;
}

export declare class RemoteWatch {
  next(): Promise<RemoteWatchMessage>;
  tryNext(): RemoteWatchMessage;
  close(): void;
}

export declare class WebSocketSync {
  isConnected(): boolean;
  pendingCount(): number;
  inflightCount(): number;
  knownPeerCount(): number;
  recommendedPeers(): JsonValue;
  remoteGet(peerId: string, path: RemotePath): Promise<JsonValue | null>;
  remoteQuery(peerId: string, path: RemotePath, spec: QuerySpec): Promise<JsonValue>;
  remoteLex(peerId: string, path: RemotePath, spec: LexSpec): Promise<JsonValue>;
  remoteNode(peerId: string, id: string): Promise<JsonValue | null>;
  remoteSnapshot(peerId: string, root?: string | null): Promise<JsonValue>;
  watchRemoteGet(peerId: string, path: RemotePath): RemoteWatch;
  watchRemoteMap(peerId: string, path: RemotePath): RemoteWatch;
  watchRemoteQuery(peerId: string, path: RemotePath, spec: QuerySpec): RemoteWatch;
  watchRemoteLex(peerId: string, path: RemotePath, spec: LexSpec): RemoteWatch;
  watchRemoteNode(peerId: string, id: string): RemoteWatch;
  watchRemoteSnapshot(peerId: string, root?: string | null): RemoteWatch;
  flushPending(): Promise<number>;
  retryInflight(): Promise<number>;
  close(): void;
}

export declare class WebRtcMesh {
  peerId(): string;
  signalingMode(): string;
  relayUrl(): string | undefined;
  relayConnected(): boolean;
  peerCount(): Promise<number>;
  openPeerCount(): Promise<number>;
  inflightCount(): Promise<number>;
  recommendedPeers(): Promise<JsonValue>;
  watchRemoteGet(peerId: string, path: RemotePath): Promise<RemoteWatch>;
  watchRemoteMap(peerId: string, path: RemotePath): Promise<RemoteWatch>;
  watchRemoteQuery(peerId: string, path: RemotePath, spec: QuerySpec): Promise<RemoteWatch>;
  watchRemoteLex(peerId: string, path: RemotePath, spec: LexSpec): Promise<RemoteWatch>;
  watchRemoteNode(peerId: string, id: string): Promise<RemoteWatch>;
  watchRemoteSnapshot(peerId: string, root?: string | null): Promise<RemoteWatch>;
  flushPending(): Promise<number>;
  retryInflight(): Promise<number>;
  close(): Promise<void>;
}
