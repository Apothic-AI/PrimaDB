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

export type MoqDraft = "draft07" | "draft14" | "draft_latest";

export interface MoqRelayClientConfig {
  url: string;
  path: string;
  track?: string;
  channel?: string;
  subscribe?: string[];
  draft?: MoqDraft;
  retryIntervalMs?: number;
  tlsDisableVerify?: boolean;
  sessionAuth?: SessionAuthConfig;
}

export type RelayEndpointConfig =
  | { kind: "web_socket"; url: string; retryIntervalMs?: number; sessionAuth?: SessionAuthConfig }
  | ({ kind: "moq" } & MoqRelayClientConfig);

export interface RelayServerConfig {
  bind: string;
  moq?: MoqRelayClientConfig | null;
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
  relayEndpoint?: RelayEndpointConfig | null;
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

export type VectorMetric = "cosine" | "l2" | "dot";
export type VectorBackendKind = "exact" | "edgevec";
export type VectorManagerState = "ready" | "catching_up" | "rebuilding" | "stale" | "failed";
export type VectorStalePolicy = "fallback_exact" | "allow_stale" | "error";

export interface VectorHnswConfig {
  m?: number | null;
  efConstruction?: number | null;
  efSearch?: number | null;
  tombstoneRebuildRatio?: number | null;
}

export interface VectorChunkingConfig {
  chunkBytes: number;
}

export interface VectorCollectionConfig {
  dim: number;
  metric?: VectorMetric;
  backend?: VectorBackendKind | null;
  hnsw?: VectorHnswConfig | null;
  chunking?: VectorChunkingConfig;
}

export interface VectorEntry {
  id: string;
  vector: number[];
  metadata?: JsonValue | null;
  writeId: string;
  checksum: string;
}

export interface VectorMetadataFilter {
  eq?: Record<string, JsonValue>;
  prefix?: Record<string, string>;
  exists?: string[];
}

export interface VectorFilter {
  idPrefix?: string | null;
  ids?: string[];
  metadata?: VectorMetadataFilter | null;
}

export interface VectorSearchSpec {
  limit: number;
  ef?: number | null;
  filter?: VectorFilter | null;
  includeVector?: boolean;
  includeMetadata?: boolean;
  exact?: boolean;
  stalePolicy?: VectorStalePolicy;
}

export interface VectorMatch {
  id: string;
  distance: number;
  metadata?: JsonValue | null;
  vector?: number[] | null;
}

export interface VectorSearchResult {
  matches: VectorMatch[];
  exact: boolean;
  backend: VectorBackendKind;
  state: VectorManagerState;
  stale: boolean;
  approximateReason?: string | null;
}

export interface TextFieldConfig {
  name: string;
  weight?: number;
  indexed?: boolean;
  stored?: boolean;
}

export interface TextAnalyzerConfig {
  kind?: "simple";
  lowercase?: boolean;
  unicodeNormalization?: string | null;
  stopwords?: string | null;
  stemming?: string | null;
  version?: number;
}

export interface TextCollectionConfig {
  fields?: TextFieldConfig[];
  analyzer?: TextAnalyzerConfig;
  k1?: number;
  b?: number;
  metadata?: Record<string, JsonValue>;
}

export interface TextDocument {
  id: string;
  fields?: Record<string, string>;
  metadata?: Record<string, JsonValue>;
}

export type TextSearchSource =
  | string
  | { kind: "collection"; collection: string }
  | { kind: "query"; path: RemotePath; spec: QuerySpec }
  | { kind: "records"; scan: RecordScan };

export interface TextSearchSpec {
  limit?: number | null;
  offset?: number | null;
  fields?: string[] | null;
  includeMetadata?: boolean;
  includeSnippets?: boolean;
  explain?: boolean;
  exact?: boolean;
  stalePolicy?: "allow" | "refresh" | "reject";
  candidateLimit?: number | null;
  candidatePolicy?: "reject_paginated_query" | "allow_preselected_candidates";
}

export interface TextSearchMatch {
  id: string;
  score: number;
  fieldHits: Array<{ field: string; terms: string[]; score: number }>;
  metadata?: Record<string, JsonValue> | null;
  snippets?: Array<{ field: string; text: string }> | null;
  explanation?: string | null;
}

export interface TextSearchResult {
  source: JsonValue;
  query: string;
  matches: TextSearchMatch[];
  backend: "exact";
  exact: boolean;
  stale: boolean;
  candidateCount: number;
  searchedCount: number;
  truncatedCandidates: boolean;
  scoreScope: "collection" | "candidate_set" | "peer_local";
}

export interface TextIndexStats {
  documentCount: number;
  deletedCount: number;
  termCount: number;
  totalTerms: number;
  averageFieldLength: number;
  state: "ready" | "rebuilding" | "stale" | "failed";
  sourceHash: string;
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

export type RemoteInterestTarget = "any" | "peer" | "peers";

export interface RemoteInterestPolicy {
  target?: RemoteInterestTarget;
  peerId?: string | null;
  peers?: string[];
  requireCapability?: boolean;
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
  | { kind: "vector_search"; collection: string; query: number[]; spec: VectorSearchSpec }
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
  | { kind: "vector_search"; result: VectorSearchResult }
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

export type RouteTarget =
  | { kind: "broadcast" }
  | { kind: "peer"; value: string }
  | { kind: "topic"; value: string };

export type RouteTransportKind =
  | "web_socket"
  | "moq"
  | "web_rtc"
  | "broadcast_channel"
  | "in_memory";

export interface ApplicationRouteMessage {
  namespace: string;
  protocol: string;
  topic?: string | null;
  body: JsonValue;
  metadata?: Record<string, JsonValue>;
}

export type ApplicationRouteAuthStatus =
  | "unknown"
  | "not_required"
  | "unauthenticated"
  | "authenticated"
  | "required_but_missing";

export interface ApplicationRouteContext {
  sourcePeerId: string;
  transport: RouteTransportKind;
  underlayId?: string | null;
  direct: boolean;
  relayRouted: boolean;
  gatewayRouted: boolean;
  gatewayPeerId?: string | null;
  authStatus: ApplicationRouteAuthStatus;
  provenance: string[];
}

export interface ApplicationRouteEvent {
  routeId: string;
  from: string;
  channel: string;
  target: RouteTarget;
  issuedAtMillis: number;
  receivedAtMillis: number;
  transport: RouteTransportKind;
  verifiedIdentity?: VerifiedIdentity | null;
  context: ApplicationRouteContext;
  message: ApplicationRouteMessage;
}

export interface ApplicationRouteFilter {
  namespace?: string | null;
  protocol?: string | null;
  topic?: string | null;
}

export interface RemotePeerFailure {
  peerId: string;
  transport: RouteTransportKind;
  message: string;
}

export interface RemotePeerRecords {
  peerId: string;
  transport: RouteTransportKind;
  result: RecordScanResult;
}

export interface RemotePeerTextSearch {
  peerId: string;
  transport: RouteTransportKind;
  result: TextSearchResult;
}

export interface RemoteRecordConflictSource {
  peerId: string;
  transport: RouteTransportKind;
  contentHash: string;
}

export interface RemoteRecordConflict {
  key: string;
  winnerPeerId: string;
  winnerHash: string;
  sources: RemoteRecordConflictSource[];
}

export interface RemoteRecordsFanIn {
  requestId: string;
  records: RemotePeerRecords[];
  failures: RemotePeerFailure[];
  merged: RecordScanResult;
  conflicts: RemoteRecordConflict[];
}

export interface RemoteTextSearchFanIn {
  requestId: string;
  results: RemotePeerTextSearch[];
  failures: RemotePeerFailure[];
  merged: TextSearchResult;
}

export type RemoteFanInWatchEvent =
  | {
      kind: "update";
      peerId: string;
      transport: RouteTransportKind;
      initial: boolean;
      sequence: number;
      result: RemoteResult;
    }
  | {
      kind: "failure";
      peerId: string;
      transport: RouteTransportKind;
      message: string;
      terminal: boolean;
    }
  | { kind: "closed" };

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
  createVectorCollection(name: string, config: VectorCollectionConfig): void;
  putVector(collection: string, id: string, vector: number[], metadata?: JsonValue | null): void;
  deleteVector(collection: string, id: string): void;
  getVector(collection: string, id: string): VectorEntry | null;
  searchVectors(collection: string, query: number[], spec: VectorSearchSpec): VectorSearchResult;
  watchVectorSearch(
    collection: string,
    query: number[],
    spec: VectorSearchSpec,
  ): VectorWatchSubscription;
  createTextCollection(name: string, config: TextCollectionConfig): void;
  putTextDocument(collection: string, document: TextDocument): void;
  deleteTextDocument(collection: string, id: string): void;
  getTextDocument(collection: string, id: string): TextDocument | null;
  textSearch(source: TextSearchSource, query: string, spec: TextSearchSpec): TextSearchResult;
  watchTextSearch(
    source: TextSearchSource,
    query: string,
    spec: TextSearchSpec,
  ): TextWatchSubscription;
  textIndexStats(collection: string): TextIndexStats;
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

export declare class VectorWatchSubscription {
  next(): Promise<{ done: boolean; value?: VectorSearchResult | null }>;
  tryNext(): { done: boolean; value?: VectorSearchResult | null };
  close(): void;
}

export declare class TextWatchSubscription {
  next(): Promise<{ done: boolean; value?: TextSearchResult | null }>;
  tryNext(): { done: boolean; value?: TextSearchResult | null };
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

export declare class ApplicationRouteSubscription {
  next(): Promise<ApplicationRouteEvent | null>;
  tryNext(): ApplicationRouteEvent | null;
  drain(): ApplicationRouteEvent[];
  close(): void;
}

export declare class RemoteFanInWatch {
  next(): Promise<RemoteFanInWatchEvent | null>;
  tryNext(): RemoteFanInWatchEvent | null;
  drain(): RemoteFanInWatchEvent[];
  close(): void;
}

export declare class WebSocketSync {
  isConnected(): boolean;
  pendingCount(): number;
  inflightCount(): number;
  knownPeerCount(): number;
  recommendedPeers(): JsonValue;
  publishApplication(message: ApplicationRouteMessage, target?: RouteTarget | null): JsonValue;
  sendApplication(
    namespace: string,
    protocol: string,
    topic: string | null | undefined,
    body: JsonValue,
    metadata?: Record<string, JsonValue> | null,
    target?: RouteTarget | null,
  ): JsonValue;
  sendRouteEnvelope(route: JsonValue): JsonValue;
  subscribeApplications(filter?: ApplicationRouteFilter | null): ApplicationRouteSubscription;
  get(path: RemotePath, policy?: RemoteInterestPolicy | null): Promise<JsonValue | null>;
  query(
    path: RemotePath,
    spec: QuerySpec,
    policy?: RemoteInterestPolicy | null,
  ): Promise<JsonValue>;
  lex(path: RemotePath, spec: LexSpec, policy?: RemoteInterestPolicy | null): Promise<JsonValue>;
  records(scan: RecordScan, policy?: RemoteInterestPolicy | null): Promise<RecordScanResult>;
  vectorSearch(
    collection: string,
    query: number[],
    spec: VectorSearchSpec,
    policy?: RemoteInterestPolicy | null,
  ): Promise<VectorSearchResult>;
  textSearch(
    source: TextSearchSource,
    query: string,
    spec: TextSearchSpec,
    policy?: RemoteInterestPolicy | null,
  ): Promise<TextSearchResult>;
  textSearchFanIn(
    source: TextSearchSource,
    query: string,
    spec: TextSearchSpec,
    policy?: RemoteInterestPolicy | null,
  ): Promise<RemoteTextSearchFanIn>;
  node(id: string, policy?: RemoteInterestPolicy | null): Promise<JsonValue | null>;
  snapshot(root?: string | null, policy?: RemoteInterestPolicy | null): Promise<JsonValue>;
  recordsFanIn(scan: RecordScan, policy?: RemoteInterestPolicy | null): Promise<RemoteRecordsFanIn>;
  remoteGet(peerId: string, path: RemotePath): Promise<JsonValue | null>;
  remoteQuery(peerId: string, path: RemotePath, spec: QuerySpec): Promise<JsonValue>;
  remoteLex(peerId: string, path: RemotePath, spec: LexSpec): Promise<JsonValue>;
  remoteRecords(peerId: string, scan: RecordScan): Promise<RecordScanResult>;
  remoteVectorSearch(
    peerId: string,
    collection: string,
    query: number[],
    spec: VectorSearchSpec,
  ): Promise<VectorSearchResult>;
  remoteTextSearch(
    peerId: string,
    source: TextSearchSource,
    query: string,
    spec: TextSearchSpec,
  ): Promise<TextSearchResult>;
  remoteNode(peerId: string, id: string): Promise<JsonValue | null>;
  remoteSnapshot(peerId: string, root?: string | null): Promise<JsonValue>;
  remoteTransaction(
    peerId: string,
    scope: string,
    steps: TransactionStep[],
    options?: TransactionOptions | null,
  ): Promise<TransactionReport>;
  watchGet(path: RemotePath, policy?: RemoteInterestPolicy | null): RemoteWatch;
  watchMap(path: RemotePath, policy?: RemoteInterestPolicy | null): RemoteWatch;
  watchQuery(
    path: RemotePath,
    spec: QuerySpec,
    policy?: RemoteInterestPolicy | null,
  ): RemoteWatch;
  watchLex(path: RemotePath, spec: LexSpec, policy?: RemoteInterestPolicy | null): RemoteWatch;
  watchRecords(scan: RecordScan, policy?: RemoteInterestPolicy | null): RemoteWatch;
  watchRecordsFanIn(scan: RecordScan, policy?: RemoteInterestPolicy | null): RemoteFanInWatch;
  watchVectorSearch(
    collection: string,
    query: number[],
    spec: VectorSearchSpec,
    policy?: RemoteInterestPolicy | null,
  ): RemoteWatch;
  watchTextSearch(
    source: TextSearchSource,
    query: string,
    spec: TextSearchSpec,
    policy?: RemoteInterestPolicy | null,
  ): RemoteWatch;
  watchTextSearchFanIn(
    source: TextSearchSource,
    query: string,
    spec: TextSearchSpec,
    policy?: RemoteInterestPolicy | null,
  ): RemoteFanInWatch;
  watchRemoteTextSearch(
    peerId: string,
    source: TextSearchSource,
    query: string,
    spec: TextSearchSpec,
  ): RemoteWatch;
  watchNode(id: string, policy?: RemoteInterestPolicy | null): RemoteWatch;
  watchSnapshot(root?: string | null, policy?: RemoteInterestPolicy | null): RemoteWatch;
  watchRemoteGet(peerId: string, path: RemotePath): RemoteWatch;
  watchRemoteMap(peerId: string, path: RemotePath): RemoteWatch;
  watchRemoteQuery(peerId: string, path: RemotePath, spec: QuerySpec): RemoteWatch;
  watchRemoteLex(peerId: string, path: RemotePath, spec: LexSpec): RemoteWatch;
  watchRemoteRecords(peerId: string, scan: RecordScan): RemoteWatch;
  watchRemoteVectorSearch(
    peerId: string,
    collection: string,
    query: number[],
    spec: VectorSearchSpec,
  ): RemoteWatch;
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
  publishApplication(message: ApplicationRouteMessage, target?: RouteTarget | null): Promise<JsonValue>;
  sendApplication(
    namespace: string,
    protocol: string,
    topic: string | null | undefined,
    body: JsonValue,
    metadata?: Record<string, JsonValue> | null,
    target?: RouteTarget | null,
  ): Promise<JsonValue>;
  sendRouteEnvelope(route: JsonValue): Promise<JsonValue>;
  subscribeApplications(filter?: ApplicationRouteFilter | null): ApplicationRouteSubscription;
  recordsFanIn(scan: RecordScan, policy?: RemoteInterestPolicy | null): Promise<RemoteRecordsFanIn>;
  textSearchFanIn(
    source: TextSearchSource,
    query: string,
    spec: TextSearchSpec,
    policy?: RemoteInterestPolicy | null,
  ): Promise<RemoteTextSearchFanIn>;
  watchGet(path: RemotePath, policy?: RemoteInterestPolicy | null): Promise<RemoteWatch>;
  watchMap(path: RemotePath, policy?: RemoteInterestPolicy | null): Promise<RemoteWatch>;
  watchQuery(
    path: RemotePath,
    spec: QuerySpec,
    policy?: RemoteInterestPolicy | null,
  ): Promise<RemoteWatch>;
  watchLex(
    path: RemotePath,
    spec: LexSpec,
    policy?: RemoteInterestPolicy | null,
  ): Promise<RemoteWatch>;
  watchRecords(scan: RecordScan, policy?: RemoteInterestPolicy | null): Promise<RemoteWatch>;
  watchRecordsFanIn(scan: RecordScan, policy?: RemoteInterestPolicy | null): Promise<RemoteFanInWatch>;
  watchVectorSearch(
    collection: string,
    query: number[],
    spec: VectorSearchSpec,
    policy?: RemoteInterestPolicy | null,
  ): Promise<RemoteWatch>;
  watchTextSearch(
    source: TextSearchSource,
    query: string,
    spec: TextSearchSpec,
    policy?: RemoteInterestPolicy | null,
  ): Promise<RemoteWatch>;
  watchTextSearchFanIn(
    source: TextSearchSource,
    query: string,
    spec: TextSearchSpec,
    policy?: RemoteInterestPolicy | null,
  ): Promise<RemoteFanInWatch>;
  watchNode(id: string, policy?: RemoteInterestPolicy | null): Promise<RemoteWatch>;
  watchSnapshot(root?: string | null, policy?: RemoteInterestPolicy | null): Promise<RemoteWatch>;
  watchRemoteGet(peerId: string, path: RemotePath): Promise<RemoteWatch>;
  watchRemoteMap(peerId: string, path: RemotePath): Promise<RemoteWatch>;
  watchRemoteQuery(peerId: string, path: RemotePath, spec: QuerySpec): Promise<RemoteWatch>;
  watchRemoteLex(peerId: string, path: RemotePath, spec: LexSpec): Promise<RemoteWatch>;
  watchRemoteRecords(peerId: string, scan: RecordScan): Promise<RemoteWatch>;
  watchRemoteVectorSearch(
    peerId: string,
    collection: string,
    query: number[],
    spec: VectorSearchSpec,
  ): Promise<RemoteWatch>;
  watchRemoteTextSearch(
    peerId: string,
    source: TextSearchSource,
    query: string,
    spec: TextSearchSpec,
  ): Promise<RemoteWatch>;
  watchRemoteNode(peerId: string, id: string): Promise<RemoteWatch>;
  watchRemoteSnapshot(peerId: string, root?: string | null): Promise<RemoteWatch>;
  flushPending(): Promise<number>;
  retryInflight(): Promise<number>;
  close(): Promise<void>;
}
