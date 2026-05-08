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

export type RecordMutation =
  | { kind: "put"; key: string; value: RecordValue }
  | { kind: "delete"; key: string }
  | { kind: "delete_range"; scan: RecordScan };

export interface RecordBatch {
  mutations?: RecordMutation[];
}

export interface RecordBatchReport {
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
