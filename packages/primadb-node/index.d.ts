export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

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

export interface IdentityKeyPair {
  publicKey: string;
  secretKey: string;
}

export interface PasswordKeyDerivationParams {
  memoryCostKiB?: number;
  timeCost?: number;
  parallelism?: number;
}

export interface PasswordKeyDerivationOptions extends PasswordKeyDerivationParams {
  saltBase64?: string | null;
}

export interface PasswordDerivedKey {
  algorithm: "argon2id-v1.3";
  keyBase64: string;
  saltBase64: string;
  params: Required<PasswordKeyDerivationParams>;
}

export interface UserGrant {
  root: string;
  read?: boolean;
  write?: boolean;
}

export declare function generateIdentity(): IdentityKeyPair;
export declare function derivePasswordKey(
  password: string,
  options?: PasswordKeyDerivationOptions | null,
): PasswordDerivedKey;

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

export interface SessionAuthConfig {
  requireAuthenticatedPeers?: boolean;
  trustedPublicKeys?: string[];
  trustedAliases?: string[];
  challengeTimeoutMs?: number;
  sessionTtlMs?: number;
  allowUnauthenticatedPresence?: boolean;
}

export interface RelayClientConfig {
  url: string;
  retryIntervalMs?: number;
  sessionAuth?: SessionAuthConfig;
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
  sessionAuth?: SessionAuthConfig;
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
      durability?: SegmentDurability;
      lockMode?: SegmentLockMode;
    };

export type SegmentDurability = "full" | "data" | "relaxed";

export type SegmentLockMode =
  | { kind: "exclusive" }
  | { kind: "wait"; timeoutMillis: number }
  | { kind: "disabled" };

export interface DurableStorageBinding {
  backend: string;
  incremental: boolean;
  loadedExisting: boolean;
  autoPersist: boolean;
  durability?: SegmentDurability;
  lockMode?: SegmentLockMode;
}

export type BlobStorageConfig =
  | { kind: "memory" }
  | {
      kind: "files";
      directory: string;
      durability?: SegmentDurability;
    };

export interface BlobStorageBinding {
  backend: string;
  contentAddressed: boolean;
  durability?: SegmentDurability;
}

export interface BlobRef {
  id: string;
  bytes: number;
  mediaType?: string | null;
}

export type RecordValue =
  | { kind: "json"; value: JsonValue }
  | { kind: "bytes"; value: string }
  | { kind: "blob"; value: BlobRef };

export interface RecordEntry {
  key: string;
  value: RecordValue;
}

export interface RecordScan {
  prefix?: string | null;
  startAt?: string | null;
  startAfter?: string | null;
  endAt?: string | null;
  endBefore?: string | null;
  reverse?: boolean;
  limit?: number | null;
  cursor?: string | null;
}

export interface RecordScanResult {
  entries: RecordEntry[];
  nextCursor?: string | null;
}

export type RecordMutation =
  | { kind: "put"; key: string; value: RecordValue }
  | { kind: "delete"; key: string }
  | { kind: "delete_range"; scan: RecordScan };

export type RecordPrecondition =
  | { kind: "exists"; key: string }
  | { kind: "absent"; key: string }
  | { kind: "value"; key: string; value: RecordValue };

export interface RecordBatch {
  preconditions?: RecordPrecondition[];
  mutations?: RecordMutation[];
}

export interface RecordBatchReport {
  preconditions: number;
  puts: number;
  deletes: number;
  rangeDeletes: number;
  operationCount: number;
}

export interface StorageSyncReport {
  backend: string;
  durability: string;
  synced: boolean;
}

export interface StorageRecoveryReport {
  appliedTransactions: number;
  skippedTransactions: number;
  removedPendingFiles: number;
  removedTempFiles: number;
  quarantinedFiles: number;
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

export type ScopeConsistency = "eventual" | "local_transactional" | "coordinated";
export type ScopeOfflineWrites = "reject" | "queue_provisional";
export type ScopeIsolation = "serializable";
export type ScopeReadMode = "cached" | "authority" | "quorum";

export type ScopeAuthority =
  | { kind: "peer"; peerId: string }
  | { kind: "full_node"; peerId: string }
  | { kind: "quorum"; peers: string[]; threshold: number };

export interface ScopePolicy {
  consistency?: ScopeConsistency;
  authority?: ScopeAuthority | null;
  isolation?: ScopeIsolation;
  readMode?: ScopeReadMode;
  offlineWrites?: ScopeOfflineWrites;
}

export interface TransactionOptions {
  offline?: ScopeOfflineWrites | null;
}

export type TransactionStep =
  | { kind: "put"; path: RemotePath; value: JsonValue }
  | { kind: "unset"; path: RemotePath }
  | { kind: "set"; path: RemotePath; value: JsonValue }
  | { kind: "remove"; path: RemotePath; value: JsonValue }
  | { kind: "assert_exists"; path: RemotePath }
  | { kind: "assert_absent"; path: RemotePath }
  | { kind: "assert_value"; path: RemotePath; value: JsonValue }
  | { kind: "assert_revision"; path: RemotePath; revision?: JsonValue | null }
  | { kind: "increment"; path: RemotePath; by: number };

export interface TransactionReport {
  status: "committed" | "provisional";
  operationCount: number;
  memberIds?: string[];
  proposalId?: string | null;
}

export interface ProvisionalTransaction {
  id: string;
  scope: string;
  createdAtMillis: number;
  steps: TransactionStep[];
  options?: TransactionOptions;
}

export type ScriptRuntime = "rhai";

export interface ScriptPathGrant {
  root: string;
  segments?: string[];
  recursive?: boolean;
}

export interface ScriptCapabilities {
  read?: ScriptPathGrant[];
  query?: ScriptPathGrant[];
  traverse?: ScriptPathGrant[];
  write?: ScriptPathGrant[];
  transaction?: ScriptPathGrant[];
}

export interface ScriptLimits {
  maxOperations?: number;
  maxCallLevels?: number;
  maxVariables?: number;
  maxFunctions?: number;
  maxModules?: number;
  maxExpressionDepth?: number;
  maxStringBytes?: number;
  maxArraySize?: number;
  maxMapSize?: number;
}

export interface NodeScript {
  id: string;
  runtime?: ScriptRuntime;
  entry?: string;
  source: string;
  sourceHash?: string | null;
  author?: string | null;
  signature?: string | null;
  capabilities?: ScriptCapabilities;
  metadata?: JsonValue;
}

export interface ScriptExecutionOptions {
  args?: JsonValue;
  capabilities?: ScriptCapabilities;
  applyWrites?: boolean;
  limits?: ScriptLimits;
}

export interface ScriptExecutionResult {
  scriptId: string;
  runtime: ScriptRuntime;
  sourceHash: string;
  value: JsonValue;
  steps: TransactionStep[];
  report?: TransactionReport | null;
}

export type PullRequestKind =
  | { kind: "get"; path: { anchor: string; segments?: string[] } }
  | { kind: "map"; path: { anchor: string; segments?: string[] } }
  | { kind: "query"; path: { anchor: string; segments?: string[] }; spec: QuerySpec }
  | { kind: "lex"; path: { anchor: string; segments?: string[] }; spec: LexSpec }
  | { kind: "records"; scan: RecordScan }
  | { kind: "node"; id: string }
  | { kind: "snapshot"; root?: string | null }
  | {
      kind: "transaction";
      scope: string;
      steps: TransactionStep[];
      options?: TransactionOptions;
    };

export type RemoteResult =
  | { kind: "get"; value: JsonValue | null }
  | { kind: "map"; entries: JsonValue[] }
  | { kind: "query"; entries: JsonValue[] }
  | { kind: "lex"; entries: JsonValue[] }
  | { kind: "records"; result: RecordScanResult }
  | { kind: "node"; node: JsonValue | null }
  | { kind: "snapshot"; snapshot: JsonValue }
  | { kind: "transaction"; report: TransactionReport };

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

export interface SubscriptionMessage {
  done: boolean;
  value?: JsonValue | null;
}

export interface RemoteWatchMessage {
  done: boolean;
  initial?: boolean;
  kind?: "get" | "map" | "query" | "lex" | "records" | "node" | "snapshot" | "transaction" | null;
  value?: JsonValue | null;
  error?: string | null;
}

export declare class Primadb {
  constructor(replicaId?: string | null);
  replicaId(): string;
  chain(root: string): Chain;
  scope(root: string): Scope;
  transaction(steps: TransactionStep[]): TransactionReport;
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
  openBlobStorage(config: BlobStorageConfig): BlobStorageBinding;
  closeDurableStorage(): void;
  syncStorage(): StorageSyncReport;
  storageRecoveryReport(): StorageRecoveryReport | null;
  putRecord(key: string, value: JsonValue): void;
  putRecordBytes(key: string, value: Uint8Array): void;
  putRecordBlob(key: string, value: Uint8Array, mediaType?: string | null): BlobRef;
  getRecord(key: string): RecordEntry | null;
  scanRecords(scan: RecordScan): RecordScanResult;
  watchRecords(scan: RecordScan): RecordWatchSubscription;
  applyRecordBatch(batch: RecordBatch): RecordBatchReport;
  deleteRecord(key: string): void;
  attachNodeScript(path: RemotePath, script: NodeScript): void;
  removeNodeScript(path: RemotePath, scriptId: string): void;
  nodeScripts(path: RemotePath): NodeScript[];
  executeNodeScripts(path: RemotePath, options?: ScriptExecutionOptions | null): ScriptExecutionResult[];
  registerUser(alias: string, publicKey: string, grants: UserGrant[]): void;
  authenticateLocalUser(alias: string, secretKey: string, grants: UserGrant[]): void;
  setRequireSignedSync(required: boolean): void;
  setSnapshotEncryptionKey(keyBase64: string): void;
  setTransportEncryptionKey(keyBase64: string): void;
  connectRelay(config: RelayClientConfig): Promise<WebSocketSync>;
  connectMesh(config: MeshConfig): Promise<WebRtcMesh>;
  setNetworkHooks(hooks: NetworkHooks): void;
  clearNetworkHooks(): void;
}

export declare class Scope {
  root(): string;
  configure(policy: ScopePolicy): void;
  policy(): ScopePolicy | null;
  proposals(): ProvisionalTransaction[];
  transaction(steps: TransactionStep[], options?: TransactionOptions | null): TransactionReport;
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
  putBytes(value: Uint8Array): void;
  onceBytes(): Uint8Array | null;
  putBlob(value: Uint8Array, mediaType?: string | null): JsonValue;
  blobRef(): JsonValue | null;
  getBlob(): Uint8Array | null;
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

export declare class RecordWatchSubscription {
  next(): Promise<{ done: boolean; value?: RecordScanResult | null }>;
  tryNext(): { done: boolean; value?: RecordScanResult | null };
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
  remoteRecords(peerId: string, scan: RecordScan): Promise<RecordScanResult>;
  remoteNode(peerId: string, id: string): Promise<JsonValue | null>;
  remoteSnapshot(peerId: string, root?: string | null): Promise<JsonValue>;
  remoteTransaction(
    peerId: string,
    scope: string,
    steps: TransactionStep[],
    options?: TransactionOptions | null,
  ): Promise<TransactionReport>;
  watchRemoteGet(peerId: string, path: RemotePath): RemoteWatch;
  watchRemoteMap(peerId: string, path: RemotePath): RemoteWatch;
  watchRemoteQuery(peerId: string, path: RemotePath, spec: QuerySpec): RemoteWatch;
  watchRemoteLex(peerId: string, path: RemotePath, spec: LexSpec): RemoteWatch;
  watchRemoteRecords(peerId: string, scan: RecordScan): RemoteWatch;
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
  watchRemoteRecords(peerId: string, scan: RecordScan): Promise<RemoteWatch>;
  watchRemoteNode(peerId: string, id: string): Promise<RemoteWatch>;
  watchRemoteSnapshot(peerId: string, root?: string | null): Promise<RemoteWatch>;
  flushPending(): Promise<number>;
  retryInflight(): Promise<number>;
  close(): Promise<void>;
}
