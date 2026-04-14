export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

export interface RelayClientConfig {
  url: string;
  retryIntervalMs?: number;
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

export interface RemotePath {
  root: string;
  segments?: string[];
}

export interface SubscriptionMessage {
  done: boolean;
  value?: JsonValue | null;
}

export interface RemoteWatchMessage {
  done: boolean;
  initial?: boolean;
  kind?: "get" | "map" | "query" | "lex" | "snapshot" | null;
  value?: JsonValue | null;
  error?: string | null;
}

export declare class Primadb {
  constructor(replicaId?: string | null);
  replicaId(): string;
  chain(root: string): Chain;
  snapshot(): JsonValue;
  snapshotForRoot(root?: string | null): JsonValue;
  exportSnapshotJson(): string;
  importSnapshotJson(payload: string): void;
  mergeSnapshotJson(payload: string): void;
  pendingOperations(): JsonValue;
  pendingEnvelope(): JsonValue;
  exportPendingOperationsJson(): string;
  drainPendingOperations(): JsonValue;
  drainPendingEnvelope(): JsonValue;
  applyOperations(operations: JsonValue): number;
  applyEnvelope(envelope: JsonValue): number;
  applyOperationsJson(payload: string): number;
  openDurableStorage(config: DurableStorageConfig): DurableStorageBinding;
  connectRelay(config: RelayClientConfig): Promise<WebSocketSync>;
  connectMesh(config: MeshConfig): Promise<WebRtcMesh>;
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
  subscribe(): Subscription;
}

export declare class Subscription {
  next(): Promise<SubscriptionMessage>;
  tryNext(): SubscriptionMessage;
  close(): void;
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
  remoteSnapshot(peerId: string, root?: string | null): Promise<JsonValue>;
  watchRemoteGet(peerId: string, path: RemotePath): RemoteWatch;
  watchRemoteMap(peerId: string, path: RemotePath): RemoteWatch;
  watchRemoteQuery(peerId: string, path: RemotePath, spec: QuerySpec): RemoteWatch;
  watchRemoteLex(peerId: string, path: RemotePath, spec: LexSpec): RemoteWatch;
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
  watchRemoteSnapshot(peerId: string, root?: string | null): Promise<RemoteWatch>;
  flushPending(): Promise<number>;
  retryInflight(): Promise<number>;
  close(): Promise<void>;
}
