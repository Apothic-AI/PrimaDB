export type JsonPrimitive = string | number | boolean | null;

export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

export type SegmentDurability = "full" | "data" | "relaxed";

export type SegmentLockMode =
  | { kind: "exclusive" }
  | { kind: "wait"; timeoutMillis: number }
  | { kind: "disabled" };

export type DurableStorageConfig =
  | {
      kind: "indexed_db_snapshots";
      databaseName: string;
      storeName: string;
      key: string;
      loadExisting?: boolean;
      autoPersist?: boolean;
    }
  | {
      kind: "indexed_db_segments";
      databaseName: string;
      storeName: string;
      namespace: string;
      loadExisting?: boolean;
      autoPersist?: boolean;
    }
  | {
      kind: "opfs_segments";
      directory: string;
      namespace: string;
      loadExisting?: boolean;
      autoPersist?: boolean;
    };

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
      kind: "indexed_db";
      databaseName: string;
      storeName: string;
      namespace: string;
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

export interface RemotePath {
  anchor: string;
  segments?: string[];
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

export type RemoteInterestTarget = "any" | "peer" | "peers";

export interface RemoteInterestPolicy {
  target?: RemoteInterestTarget;
  peerId?: string | null;
  peers?: string[];
  requireCapability?: boolean;
}

export type RouteTransportKind =
  | "web_socket"
  | "moq"
  | "web_rtc"
  | "broadcast_channel"
  | "memory"
  | "unknown";

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

export interface RemoteRecordsFanIn {
  requestId: string;
  records: RemotePeerRecords[];
  failures: RemotePeerFailure[];
  merged: RecordScanResult;
  conflicts: Array<Record<string, JsonValue>>;
}

export interface RemoteTextSearchFanIn {
  requestId: string;
  results: RemotePeerTextSearch[];
  failures: RemotePeerFailure[];
  merged: TextSearchResult;
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
