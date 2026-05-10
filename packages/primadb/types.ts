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

export type RemoteInterestTarget = "any" | "peer" | "peers";

export interface RemoteInterestPolicy {
  target?: RemoteInterestTarget;
  peerId?: string | null;
  peers?: string[];
  requireCapability?: boolean;
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
