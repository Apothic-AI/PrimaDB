use crate::clock::HybridClock;
use crate::consistency::{ProvisionalTransaction, ScopePolicy};
#[cfg(not(target_arch = "wasm32"))]
use crate::durable::{SegmentDurability, SegmentFileStoreOptions, SegmentLockMode};
use crate::error::{PrimadbError, Result};
use crate::operation::Operation;
use crate::query::QueryDirection;
use crate::record::{RecordEntry, RecordScan, RecordValue};
use crate::snapshot::DatabaseSnapshot;
use crate::value::{FieldValue, NodeId, NodeState};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
#[cfg(not(target_arch = "wasm32"))]
use std::io::Write;
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Mutex;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::{AtomicU64, Ordering};

const STORAGE_SCHEMA_VERSION: u32 = 1;
#[cfg(not(target_arch = "wasm32"))]
const SEGMENT_COMMIT_SCHEMA_VERSION: u32 = 1;
#[cfg(not(target_arch = "wasm32"))]
const DIRECT_INDEX_LITERAL_COMPONENT_LIMIT: usize = 120;
#[cfg(not(target_arch = "wasm32"))]
const DIRECT_INDEX_LITERAL_PREFIX: &str = "v_";
#[cfg(not(target_arch = "wasm32"))]
const DIRECT_INDEX_HASH_PREFIX: &str = "h_";
#[cfg(not(target_arch = "wasm32"))]
const RECORD_KEY_CHUNK_HEX: usize = 64;
#[cfg(not(target_arch = "wasm32"))]
const RECORD_KEY_INDEX_HEX_LIMIT: usize = 512;
#[cfg(not(target_arch = "wasm32"))]
const RECORD_ENTRY_FILE: &str = "entry.json";
#[cfg(not(target_arch = "wasm32"))]
const RECORD_EMPTY_KEY_COMPONENT: &str = "_empty";
#[cfg(not(target_arch = "wasm32"))]
const RECORD_OVERFLOW_COMPONENT: &str = "_overflow";
#[cfg(not(target_arch = "wasm32"))]
static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
std::thread_local! {
    static STORAGE_MATERIALIZATION_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static STORAGE_DIRECT_INDEX_BUCKET_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_storage_materialization_visit_count() {
    STORAGE_MATERIALIZATION_VISITS.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn storage_materialization_visit_count() -> usize {
    STORAGE_MATERIALIZATION_VISITS.with(std::cell::Cell::get)
}

#[cfg(test)]
pub(crate) fn reset_storage_direct_index_bucket_read_count() {
    STORAGE_DIRECT_INDEX_BUCKET_READS.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn storage_direct_index_bucket_read_count() -> usize {
    STORAGE_DIRECT_INDEX_BUCKET_READS.with(std::cell::Cell::get)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StorageMetadata {
    pub schema_version: u32,
    pub clock: HybridClock,
    pub pending_ops: Vec<Operation>,
    pub next_tx_id: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub last_materialized_tx_id: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub scope_policies: BTreeMap<String, ScopePolicy>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub provisional_transactions: BTreeMap<String, ProvisionalTransaction>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub next_provisional_transaction_id: u64,
}

impl StorageMetadata {
    pub fn new(clock: HybridClock, pending_ops: Vec<Operation>, next_tx_id: u64) -> Self {
        Self {
            schema_version: STORAGE_SCHEMA_VERSION,
            clock,
            pending_ops,
            next_tx_id,
            last_materialized_tx_id: next_tx_id.saturating_sub(1),
            scope_policies: BTreeMap::new(),
            provisional_transactions: BTreeMap::new(),
            next_provisional_transaction_id: 0,
        }
    }
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DirectScalarIndexEntry {
    pub node_id: NodeId,
    pub path: String,
    pub value: JsonValue,
    pub sortable_key: String,
}

#[derive(Debug, Clone)]
struct DirectScalarIndexFragment {
    path: String,
    value: Arc<JsonValue>,
    sortable_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct DirectIndexScan {
    #[serde(default)]
    pub exact_sortable_key: Option<String>,
    #[serde(default)]
    pub prefix_sortable_key: Option<String>,
    #[serde(default)]
    pub start_at: Option<String>,
    #[serde(default)]
    pub start_after: Option<String>,
    #[serde(default)]
    pub end_at: Option<String>,
    #[serde(default)]
    pub end_before: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub offset: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_ids: Option<BTreeSet<NodeId>>,
}

impl DirectIndexScan {
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    fn matches_sortable_key(&self, candidate: &str) -> bool {
        if let Some(exact) = &self.exact_sortable_key {
            return candidate == exact;
        }
        if let Some(prefix) = &self.prefix_sortable_key
            && !candidate.starts_with(prefix)
        {
            return false;
        }
        if let Some(start_at) = &self.start_at
            && candidate < start_at.as_str()
        {
            return false;
        }
        if let Some(start_after) = &self.start_after
            && candidate <= start_after.as_str()
        {
            return false;
        }
        if let Some(end_at) = &self.end_at
            && candidate > end_at.as_str()
        {
            return false;
        }
        if let Some(end_before) = &self.end_before
            && candidate >= end_before.as_str()
        {
            return false;
        }
        true
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn matches_candidate(&self, node_id: &str) -> bool {
        self.candidate_ids
            .as_ref()
            .is_none_or(|candidate_ids| candidate_ids.contains(node_id))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct StoredAuthFieldMeta {
    pub signer: String,
    pub certificate: Option<String>,
    pub owner: Option<String>,
    pub verified: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AuthNodeMeta {
    pub signed_fields: BTreeMap<String, StoredAuthFieldMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct NodeIndexManifest {
    pub direct_index_keys: Vec<String>,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
struct DirectIndexBucket {
    entries: BTreeMap<String, DirectScalarIndexEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StorageTransaction {
    pub id: u64,
    pub metadata: StorageMetadata,
    pub nodes: BTreeMap<NodeId, NodeState>,
    pub node_indexes: BTreeMap<NodeId, NodeIndexManifest>,
    pub direct_indexes: BTreeMap<String, DirectScalarIndexEntry>,
    #[serde(default)]
    pub records: BTreeMap<String, RecordEntry>,
    #[serde(default)]
    pub deleted_records: BTreeSet<String>,
    pub auth_meta: BTreeMap<NodeId, AuthNodeMeta>,
    pub journal_ops: Vec<Operation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct StorageSyncReport {
    pub backend: String,
    pub durability: String,
    pub synced: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct StorageRecoveryReport {
    pub applied_transactions: usize,
    pub skipped_transactions: usize,
    pub removed_pending_files: usize,
    pub removed_temp_files: usize,
    pub quarantined_files: usize,
}

pub trait IncrementalStore: Debug + Send + Sync {
    fn name(&self) -> &str;
    fn load_metadata(&self) -> Result<Option<StorageMetadata>>;
    fn apply_transaction(&self, transaction: &StorageTransaction) -> Result<()>;
    fn get_node(&self, node_id: &str) -> Result<Option<NodeState>>;
    fn export_snapshot(&self, root: Option<&str>) -> Result<DatabaseSnapshot>;
    fn scan_direct_index_entries(
        &self,
        path: &str,
        direction: QueryDirection,
        scan: &DirectIndexScan,
    ) -> Result<Vec<DirectScalarIndexEntry>>;
    fn list_direct_index_entries(
        &self,
        path: &str,
        direction: QueryDirection,
    ) -> Result<Vec<DirectScalarIndexEntry>> {
        self.scan_direct_index_entries(path, direction, &DirectIndexScan::default())
    }
    fn scan_record_entries(&self, scan: &RecordScan) -> Result<Option<Vec<RecordEntry>>> {
        let _ = scan;
        Ok(None)
    }
    fn sync(&self) -> Result<StorageSyncReport> {
        Ok(StorageSyncReport {
            backend: self.name().to_owned(),
            durability: "unspecified".to_owned(),
            synced: false,
        })
    }
    fn recovery_report(&self) -> Option<StorageRecoveryReport> {
        None
    }
    fn vacuum(&self, transaction: &StorageTransaction) -> Result<StorageVacuumReport> {
        let _ = transaction;
        Ok(StorageVacuumReport::default())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct StorageVacuumReport {
    pub removed_node_files: usize,
    pub removed_auth_files: usize,
    pub removed_index_manifests: usize,
    pub removed_direct_index_files: usize,
    pub removed_empty_index_dirs: usize,
    pub pruned_journal_files: usize,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
pub struct SegmentFileStore {
    root: std::path::PathBuf,
    journal_retention: usize,
    durability: SegmentDurability,
    _lock: Option<Arc<SegmentStoreLock>>,
    recovery_report: Arc<Mutex<StorageRecoveryReport>>,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
struct SegmentStoreLock {
    _file: std::fs::File,
}

#[cfg(not(target_arch = "wasm32"))]
impl SegmentStoreLock {
    fn acquire(root: &std::path::Path, mode: &SegmentLockMode) -> Result<Option<Arc<Self>>> {
        if matches!(mode, SegmentLockMode::Disabled) {
            return Ok(None);
        }
        std::fs::create_dir_all(root)?;
        let path = root.join(".primadb.lock");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        match mode {
            SegmentLockMode::Exclusive => {
                fs2::FileExt::try_lock_exclusive(&file).map_err(|error| {
                    PrimadbError::Message(format!(
                        "segment store `{}` is already open by another process: {error}",
                        root.display()
                    ))
                })?;
            }
            SegmentLockMode::Wait { timeout_millis } => {
                let deadline =
                    std::time::Instant::now() + std::time::Duration::from_millis(*timeout_millis);
                loop {
                    match fs2::FileExt::try_lock_exclusive(&file) {
                        Ok(()) => break,
                        Err(error) if std::time::Instant::now() >= deadline => {
                            return Err(PrimadbError::Message(format!(
                                "timed out waiting for segment store lock `{}`: {error}",
                                root.display()
                            )));
                        }
                        Err(_) => std::thread::sleep(std::time::Duration::from_millis(25)),
                    }
                }
            }
            SegmentLockMode::Disabled => unreachable!("disabled mode returned early"),
        }
        Ok(Some(Arc::new(Self { _file: file })))
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SegmentCommitPayload {
    schema_version: u32,
    transaction: StorageTransaction,
    direct_index_removals: Vec<String>,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SegmentCommitRecord {
    payload: SegmentCommitPayload,
    checksum: String,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SegmentFaultPoint {
    AfterJournalWrite,
    AfterNodeWrites,
    AfterIndexWrites,
    AfterManifestWrite,
    BeforeJournalFinalize,
}

#[cfg(all(test, not(target_arch = "wasm32")))]
static SEGMENT_FAULT_POINTS: Mutex<BTreeMap<std::path::PathBuf, SegmentFaultPoint>> =
    Mutex::new(BTreeMap::new());

#[cfg(all(test, not(target_arch = "wasm32")))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SegmentWriteMetrics {
    pub file_writes: usize,
    pub bytes_written: usize,
    pub direct_index_writes: usize,
    pub file_syncs: usize,
    pub directory_syncs: usize,
    pub direct_index_directory_syncs: usize,
}

#[cfg(all(test, not(target_arch = "wasm32")))]
static SEGMENT_WRITE_METRICS: Mutex<BTreeMap<std::path::PathBuf, SegmentWriteMetrics>> =
    Mutex::new(BTreeMap::new());

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) fn set_segment_fault_point_for_test(
    root: impl Into<std::path::PathBuf>,
    point: SegmentFaultPoint,
) {
    SEGMENT_FAULT_POINTS
        .lock()
        .unwrap()
        .insert(root.into(), point);
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) fn reset_segment_write_metrics_for_test(root: impl Into<std::path::PathBuf>) {
    SEGMENT_WRITE_METRICS
        .lock()
        .unwrap()
        .insert(root.into(), SegmentWriteMetrics::default());
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) fn segment_write_metrics_for_test(
    root: impl Into<std::path::PathBuf>,
) -> SegmentWriteMetrics {
    SEGMENT_WRITE_METRICS
        .lock()
        .unwrap()
        .get(&root.into())
        .copied()
        .unwrap_or_default()
}

#[cfg(all(not(target_arch = "wasm32"), not(windows)))]
fn replace_file(from: &std::path::Path, to: &std::path::Path) -> Result<()> {
    std::fs::rename(from, to)?;
    Ok(())
}

#[cfg(windows)]
fn replace_file(from: &std::path::Path, to: &std::path::Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;

    const MOVEFILE_REPLACE_EXISTING: u32 = 0x1;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x8;

    unsafe extern "system" {
        fn MoveFileExW(
            existing_file_name: *const u16,
            new_file_name: *const u16,
            flags: u32,
        ) -> i32;
    }

    let from_wide = from
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let to_wide = to
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let ok = unsafe {
        MoveFileExW(
            from_wide.as_ptr(),
            to_wide.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if ok == 0 {
        Err(std::io::Error::last_os_error().into())
    } else {
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl SegmentFileStore {
    pub fn new(root: impl Into<std::path::PathBuf>, journal_retention: usize) -> Result<Self> {
        Self::with_options(root, journal_retention, SegmentFileStoreOptions::default())
    }

    pub fn with_options(
        root: impl Into<std::path::PathBuf>,
        journal_retention: usize,
        options: SegmentFileStoreOptions,
    ) -> Result<Self> {
        let root = root.into();
        let lock = SegmentStoreLock::acquire(&root, &options.lock_mode)?;
        let store = Self {
            root,
            journal_retention: journal_retention.max(1),
            durability: options.durability,
            _lock: lock,
            recovery_report: Arc::new(Mutex::new(StorageRecoveryReport::default())),
        };
        store.ensure_layout()?;
        Ok(store)
    }

    fn ensure_layout(&self) -> Result<()> {
        let paths = [
            self.root.join("nodes"),
            self.root.join("auth"),
            self.root.join("node_indexes"),
            self.root.join("indexes").join("direct"),
            self.record_entries_root(),
            self.root.join("journal"),
        ];
        let mut sync_dirs = BTreeSet::new();
        for path in paths {
            sync_dirs.extend(Self::ensure_parent_dirs(&path)?);
        }
        if matches!(self.durability, SegmentDurability::Full) {
            for path in sync_dirs {
                self.sync_dir(&path)?;
            }
        }
        Ok(())
    }

    fn manifest_path(&self) -> std::path::PathBuf {
        self.root.join("manifest.json")
    }

    fn node_path(&self, node_id: &str) -> std::path::PathBuf {
        self.root
            .join("nodes")
            .join(format!("{}.json", encode_component(node_id)))
    }

    fn auth_meta_path(&self, node_id: &str) -> std::path::PathBuf {
        self.root
            .join("auth")
            .join(format!("{}.json", encode_component(node_id)))
    }

    fn node_index_manifest_path(&self, node_id: &str) -> std::path::PathBuf {
        self.root
            .join("node_indexes")
            .join(format!("{}.json", encode_component(node_id)))
    }

    fn direct_index_root(&self, path: &str) -> std::path::PathBuf {
        self.root
            .join("indexes")
            .join("direct")
            .join(safe_direct_index_component(&encode_component(path)))
    }

    fn direct_index_path(&self, key: &str) -> std::path::PathBuf {
        if let Some((encoded_path, sortable_key, encoded_node_id)) = direct_index_key_parts(key) {
            return self
                .root
                .join("indexes")
                .join("direct")
                .join(safe_direct_index_component(encoded_path))
                .join(safe_direct_index_component(sortable_key))
                .join(format!(
                    "{}.json",
                    safe_direct_index_component(encoded_node_id)
                ));
        }

        let mut path = self.root.join("indexes");
        for segment in key.split('/') {
            path.push(safe_direct_index_component(segment));
        }
        path.set_extension("json");
        path
    }

    fn records_root(&self) -> std::path::PathBuf {
        self.root.join("records")
    }

    fn record_entries_root(&self) -> std::path::PathBuf {
        self.records_root().join("by_key")
    }

    fn record_entry_path(&self, key: &str) -> std::path::PathBuf {
        let mut path = self.record_entries_root();
        let encoded = encode_component(key);
        if encoded.is_empty() {
            path.push(RECORD_EMPTY_KEY_COMPONENT);
        } else {
            let indexed_len = encoded.len().min(RECORD_KEY_INDEX_HEX_LIMIT);
            for chunk in encoded[..indexed_len]
                .as_bytes()
                .chunks(RECORD_KEY_CHUNK_HEX)
            {
                path.push(std::str::from_utf8(chunk).expect("hex record key chunk"));
            }
            if encoded.len() > RECORD_KEY_INDEX_HEX_LIMIT {
                path.push(RECORD_OVERFLOW_COMPONENT);
                path.push(blake3::hash(key.as_bytes()).to_hex().to_string());
            }
        }
        path.join(RECORD_ENTRY_FILE)
    }

    fn read_record_entry_path(path: &std::path::Path) -> Result<RecordEntry> {
        Ok(serde_json::from_str(&std::fs::read_to_string(path)?)?)
    }

    fn upsert_record_entry_batched(
        &self,
        entry: &RecordEntry,
        sync_dirs: &mut BTreeSet<std::path::PathBuf>,
    ) -> Result<()> {
        let path = self.record_entry_path(&entry.key);
        self.write_json_file_batched(&path, entry, sync_dirs)
    }

    fn remove_record_entry_batched(
        &self,
        key: &str,
        sync_dirs: &mut BTreeSet<std::path::PathBuf>,
    ) -> Result<bool> {
        let path = self.record_entry_path(key);
        if !self.remove_file_batched(&path, sync_dirs)? {
            return Ok(false);
        }
        if let Some(parent) = path.parent() {
            self.prune_empty_record_dirs_batched(parent, sync_dirs)?;
        }
        Ok(true)
    }

    fn read_direct_index_bucket_path(path: &std::path::Path) -> Result<DirectIndexBucket> {
        if !path.exists() {
            return Ok(DirectIndexBucket::default());
        }
        Ok(serde_json::from_str(&std::fs::read_to_string(path)?)?)
    }

    fn write_direct_index_bucket_path(
        &self,
        path: &std::path::Path,
        bucket: &DirectIndexBucket,
    ) -> Result<()> {
        self.write_json_file(path, bucket)
    }

    fn write_direct_index_bucket_path_batched(
        &self,
        path: &std::path::Path,
        bucket: &DirectIndexBucket,
        sync_dirs: &mut BTreeSet<std::path::PathBuf>,
    ) -> Result<()> {
        self.write_json_file_batched(path, bucket, sync_dirs)
    }

    fn write_json_file<T: Serialize>(&self, path: &std::path::Path, value: &T) -> Result<()> {
        self.write_file(path, &serde_json::to_vec(value)?)
    }

    fn write_json_file_batched<T: Serialize>(
        &self,
        path: &std::path::Path,
        value: &T,
        sync_dirs: &mut BTreeSet<std::path::PathBuf>,
    ) -> Result<()> {
        self.write_file_batched(path, &serde_json::to_vec(value)?, sync_dirs)
    }

    fn write_file(&self, path: &std::path::Path, bytes: &[u8]) -> Result<()> {
        let mut sync_dirs = BTreeSet::new();
        self.write_file_batched(path, bytes, &mut sync_dirs)?;
        for path in sync_dirs {
            self.sync_dir(&path)?;
        }
        Ok(())
    }

    fn write_file_batched(
        &self,
        path: &std::path::Path,
        bytes: &[u8],
        sync_dirs: &mut BTreeSet<std::path::PathBuf>,
    ) -> Result<()> {
        let parent = path.parent().ok_or_else(|| {
            PrimadbError::Message(format!("path `{}` has no parent directory", path.display()))
        })?;
        let created_dir_parents = Self::ensure_parent_dirs(parent)?;

        if matches!(self.durability, SegmentDurability::Relaxed) {
            std::fs::write(path, bytes)?;
            #[cfg(test)]
            self.record_file_write_for_test(path, bytes.len());
            return Ok(());
        }

        let temp_path = self.temp_path_for(path);
        {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temp_path)?;
            file.write_all(bytes)?;
            file.flush()?;
            match self.durability {
                SegmentDurability::Full => {
                    #[cfg(test)]
                    self.record_file_sync_for_test();
                    file.sync_all()?
                }
                SegmentDurability::Data => {
                    #[cfg(test)]
                    self.record_file_sync_for_test();
                    file.sync_data()?
                }
                SegmentDurability::Relaxed => {}
            }
        }
        replace_file(&temp_path, path)?;
        #[cfg(test)]
        self.record_file_write_for_test(path, bytes.len());
        if matches!(self.durability, SegmentDurability::Full) {
            sync_dirs.extend(created_dir_parents);
            sync_dirs.insert(parent.to_path_buf());
        }
        Ok(())
    }

    fn ensure_parent_dirs(parent: &std::path::Path) -> Result<BTreeSet<std::path::PathBuf>> {
        let mut missing = Vec::new();
        let mut current = parent;
        while !current.exists() {
            missing.push(current.to_path_buf());
            let Some(next) = current.parent() else {
                break;
            };
            current = next;
        }
        std::fs::create_dir_all(parent)?;
        Ok(missing
            .into_iter()
            .filter_map(|path| path.parent().map(std::path::Path::to_path_buf))
            .collect())
    }

    fn remove_file_durable(&self, path: &std::path::Path) -> Result<()> {
        if !path.exists() {
            return Ok(());
        }
        let parent = path.parent().map(std::path::Path::to_path_buf);
        std::fs::remove_file(path)?;
        if matches!(self.durability, SegmentDurability::Full)
            && let Some(parent) = parent
        {
            self.sync_dir(&parent)?;
        }
        Ok(())
    }

    fn remove_dir_durable(&self, path: &std::path::Path) -> Result<()> {
        let parent = path.parent().map(std::path::Path::to_path_buf);
        std::fs::remove_dir(path)?;
        if matches!(self.durability, SegmentDurability::Full)
            && let Some(parent) = parent
        {
            self.sync_dir(&parent)?;
        }
        Ok(())
    }

    fn temp_path_for(&self, path: &std::path::Path) -> std::path::PathBuf {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("primadb");
        let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let suffix = format!(
            ".tmp-{}-{}-{}",
            std::process::id(),
            crate::clock::now_millis(),
            counter
        );
        path.with_file_name(format!("{name}{suffix}"))
    }

    fn sync_dir(&self, path: &std::path::Path) -> Result<()> {
        let file = std::fs::File::open(path)?;
        file.sync_all()?;
        #[cfg(test)]
        self.record_directory_sync_for_test(path);
        Ok(())
    }

    #[cfg(test)]
    fn record_file_write_for_test(&self, path: &std::path::Path, bytes: usize) {
        let mut metrics = SEGMENT_WRITE_METRICS.lock().unwrap();
        let entry = metrics.entry(self.root.clone()).or_default();
        entry.file_writes += 1;
        entry.bytes_written += bytes;
        if path.starts_with(self.root.join("indexes").join("direct")) {
            entry.direct_index_writes += 1;
        }
    }

    #[cfg(test)]
    fn record_file_sync_for_test(&self) {
        SEGMENT_WRITE_METRICS
            .lock()
            .unwrap()
            .entry(self.root.clone())
            .or_default()
            .file_syncs += 1;
    }

    #[cfg(test)]
    fn record_directory_sync_for_test(&self, path: &std::path::Path) {
        let mut metrics = SEGMENT_WRITE_METRICS.lock().unwrap();
        let entry = metrics.entry(self.root.clone()).or_default();
        entry.directory_syncs += 1;
        if path.starts_with(self.root.join("indexes").join("direct")) {
            entry.direct_index_directory_syncs += 1;
        }
    }

    #[cfg(test)]
    fn maybe_fail(&self, point: SegmentFaultPoint) -> Result<()> {
        let mut faults = SEGMENT_FAULT_POINTS.lock().unwrap();
        if faults.get(&self.root).is_some_and(|fault| fault == &point) {
            faults.remove(&self.root);
            return Err(PrimadbError::Message(format!(
                "injected segment fault at {point:?}"
            )));
        }
        Ok(())
    }

    #[cfg(not(test))]
    fn maybe_fail(&self, _point: SegmentFaultPoint) -> Result<()> {
        Ok(())
    }

    fn direct_index_scan_sortable_dirs(
        &self,
        root: &std::path::Path,
        scan: &DirectIndexScan,
    ) -> Result<Vec<(std::path::PathBuf, Option<String>)>> {
        if let Some(exact) = &scan.exact_sortable_key {
            let dir = root.join(safe_direct_index_component(exact));
            return Ok(dir
                .is_dir()
                .then_some((dir, Some(exact.clone())))
                .into_iter()
                .collect());
        }

        let mut dirs = Vec::new();
        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(literal) = literal_direct_index_component(&name)
                && !scan.matches_sortable_key(literal)
            {
                continue;
            }
            dirs.push((
                path,
                literal_direct_index_component(&name).map(ToOwned::to_owned),
            ));
        }
        // Literal directory names retain sortable-key order. Hashed names are
        // kept after them and handled by the correctness fallback below.
        dirs.sort_by(|(left_path, left_key), (right_path, right_key)| {
            left_key
                .is_none()
                .cmp(&right_key.is_none())
                .then_with(|| left_key.cmp(right_key))
                .then_with(|| left_path.cmp(right_path))
        });
        Ok(dirs)
    }

    fn collect_direct_index_entries_from_bucket(
        file: &std::path::Path,
        path: &str,
        scan: &DirectIndexScan,
        remaining_offset: &mut usize,
        entries: &mut Vec<DirectScalarIndexEntry>,
    ) -> Result<()> {
        let bucket = Self::read_direct_index_bucket_path(file)?;
        #[cfg(test)]
        STORAGE_DIRECT_INDEX_BUCKET_READS.with(|count| count.set(count.get() + 1));
        let bucket_entries = bucket
            .entries
            .values()
            .filter(|entry| {
                entry.path == path
                    && scan.matches_sortable_key(&entry.sortable_key)
                    && scan.matches_candidate(&entry.node_id)
            })
            .collect::<Vec<_>>();
        for entry in bucket_entries {
            if *remaining_offset > 0 {
                *remaining_offset -= 1;
                continue;
            }
            entries.push(entry.clone());
            if scan.limit.is_some_and(|limit| entries.len() >= limit) {
                break;
            }
        }
        Ok(())
    }

    fn record_scan_root(&self, scan: &RecordScan) -> (std::path::PathBuf, Option<String>) {
        let Some(prefix) = record_scan_key_prefix(scan) else {
            return (self.record_entries_root(), None);
        };
        let encoded = encode_component(&prefix);
        if encoded.is_empty() {
            return (self.record_entries_root(), None);
        }

        let indexed_len = encoded.len().min(RECORD_KEY_INDEX_HEX_LIMIT);
        let full_chunks_len = indexed_len / RECORD_KEY_CHUNK_HEX * RECORD_KEY_CHUNK_HEX;
        let mut root = self.record_entries_root();
        for chunk in encoded[..full_chunks_len]
            .as_bytes()
            .chunks(RECORD_KEY_CHUNK_HEX)
        {
            root.push(std::str::from_utf8(chunk).expect("hex record scan chunk"));
        }

        let partial = (full_chunks_len < indexed_len)
            .then(|| encoded[full_chunks_len..indexed_len].to_owned());
        (root, partial)
    }

    fn collect_record_entries(
        dir: &std::path::Path,
        encoded_key: &str,
        partial_component_prefix: Option<&str>,
        scan: &RecordScan,
        entries: &mut Vec<RecordEntry>,
    ) -> Result<bool> {
        if !dir.exists() {
            return Ok(false);
        }

        let mut children = std::fs::read_dir(dir)?
            .map(|entry| entry.map_err(Into::into))
            .collect::<Result<Vec<_>>>()?;
        children.sort_by_key(|entry| entry.file_name());
        let record_file = children
            .iter()
            .find(|entry| entry.file_name() == RECORD_ENTRY_FILE)
            .map(|entry| entry.path());
        let mut directories = children
            .into_iter()
            .filter(|entry| entry.path().is_dir())
            .collect::<Vec<_>>();

        if !scan.reverse
            && let Some(path) = record_file.as_deref()
            && Self::collect_record_entry_file(path, encoded_key, scan, entries)?
        {
            return Ok(true);
        }

        if scan.reverse {
            directories.reverse();
        }
        for entry in directories {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(partial) = partial_component_prefix
                && !name.starts_with(partial)
            {
                continue;
            }

            let next_encoded_key = if encoded_key.is_empty() {
                name
            } else {
                format!("{encoded_key}{name}")
            };
            if Self::collect_record_entries(&path, &next_encoded_key, None, scan, entries)? {
                return Ok(true);
            }
        }

        if scan.reverse
            && let Some(path) = record_file.as_deref()
            && Self::collect_record_entry_file(path, encoded_key, scan, entries)?
        {
            return Ok(true);
        }
        Ok(false)
    }

    fn collect_record_entry_file(
        path: &std::path::Path,
        encoded_key: &str,
        scan: &RecordScan,
        entries: &mut Vec<RecordEntry>,
    ) -> Result<bool> {
        let candidate = if encoded_key.is_empty() || encoded_key == RECORD_EMPTY_KEY_COMPONENT {
            Some(String::new())
        } else if encoded_key.ends_with(RECORD_OVERFLOW_COMPONENT) {
            None
        } else {
            decode_component(encoded_key).ok()
        };

        let entry = match candidate {
            Some(key) if scan.matches_key(&key) => Self::read_record_entry_path(path)?,
            Some(_) => return Ok(false),
            None => {
                let entry = Self::read_record_entry_path(path)?;
                if !scan.matches_key(&entry.key) {
                    return Ok(false);
                }
                entry
            }
        };
        entries.push(entry);
        Ok(scan.limit.is_some_and(|limit| entries.len() >= limit))
    }

    fn prune_empty_record_dirs_batched(
        &self,
        start: &std::path::Path,
        sync_dirs: &mut BTreeSet<std::path::PathBuf>,
    ) -> Result<usize> {
        let root = self.record_entries_root();
        let mut current = start.to_path_buf();
        let mut removed = 0;
        while current != root {
            match std::fs::read_dir(&current) {
                Ok(entries) => {
                    let mut entries = entries;
                    if entries.next().is_some() {
                        break;
                    }
                    self.remove_dir_batched(&current, sync_dirs)?;
                    removed += 1;
                }
                _ => break,
            }
            let Some(parent) = current.parent() else {
                break;
            };
            current = parent.to_path_buf();
        }
        Ok(removed)
    }

    fn journal_pending_path(&self, tx_id: u64) -> std::path::PathBuf {
        self.root
            .join("journal")
            .join(format!("pending-{tx_id:020}.json"))
    }

    fn journal_final_path(&self, tx_id: u64) -> std::path::PathBuf {
        self.root
            .join("journal")
            .join(format!("tx-{tx_id:020}.json"))
    }

    fn journal_file_key(path: &std::path::Path) -> Option<(u64, bool)> {
        let name = path.file_name().and_then(|name| name.to_str())?;
        let (raw_id, pending) = if let Some(raw_id) = name
            .strip_prefix("pending-")
            .and_then(|name| name.strip_suffix(".json"))
        {
            (raw_id, true)
        } else if let Some(raw_id) = name
            .strip_prefix("tx-")
            .and_then(|name| name.strip_suffix(".json"))
        {
            (raw_id, false)
        } else {
            return None;
        };
        raw_id.parse::<u64>().ok().map(|tx_id| (tx_id, pending))
    }

    fn build_commit_payload(
        &self,
        transaction: &StorageTransaction,
    ) -> Result<SegmentCommitPayload> {
        let mut direct_index_removals = Vec::new();
        for (node_id, manifest) in &transaction.node_indexes {
            let previous = self.load_node_index_manifest(node_id)?;
            let next_keys: BTreeSet<_> = manifest.direct_index_keys.iter().cloned().collect();
            direct_index_removals.extend(
                previous
                    .direct_index_keys
                    .into_iter()
                    .filter(|key| !next_keys.contains(key)),
            );
        }
        direct_index_removals.sort();
        direct_index_removals.dedup();
        Ok(SegmentCommitPayload {
            schema_version: SEGMENT_COMMIT_SCHEMA_VERSION,
            transaction: transaction.clone(),
            direct_index_removals,
        })
    }

    fn commit_record_for_payload(payload: SegmentCommitPayload) -> Result<SegmentCommitRecord> {
        let bytes = serde_json::to_vec(&payload)?;
        Ok(SegmentCommitRecord {
            payload,
            checksum: blake3::hash(&bytes).to_hex().to_string(),
        })
    }

    fn validate_commit_record(record: SegmentCommitRecord) -> Result<SegmentCommitPayload> {
        if record.payload.schema_version != SEGMENT_COMMIT_SCHEMA_VERSION {
            return Err(PrimadbError::Message(format!(
                "segment commit schema version {} is unsupported",
                record.payload.schema_version
            )));
        }
        let expected = blake3::hash(&serde_json::to_vec(&record.payload)?)
            .to_hex()
            .to_string();
        if expected != record.checksum {
            return Err(PrimadbError::Message(
                "segment commit checksum mismatch".to_owned(),
            ));
        }
        Ok(record.payload)
    }

    fn read_commit_record(path: &std::path::Path) -> Result<SegmentCommitPayload> {
        let record: SegmentCommitRecord = serde_json::from_str(&std::fs::read_to_string(path)?)?;
        Self::validate_commit_record(record)
    }

    fn write_pending_commit(&self, payload: SegmentCommitPayload) -> Result<std::path::PathBuf> {
        let record = Self::commit_record_for_payload(payload)?;
        let pending_path = self.journal_pending_path(record.payload.transaction.id);
        self.write_json_file(&pending_path, &record)?;
        Ok(pending_path)
    }

    fn materialize_commit_payload(&self, payload: &SegmentCommitPayload) -> Result<()> {
        let transaction = &payload.transaction;
        let mut sync_dirs = BTreeSet::new();

        for (node_id, node_state) in &transaction.nodes {
            self.write_json_file_batched(&self.node_path(node_id), node_state, &mut sync_dirs)?;
        }

        self.maybe_fail(SegmentFaultPoint::AfterNodeWrites)?;

        for (node_id, auth_meta) in &transaction.auth_meta {
            self.write_json_file_batched(&self.auth_meta_path(node_id), auth_meta, &mut sync_dirs)?;
        }

        self.materialize_direct_index_changes(payload, &mut sync_dirs)?;
        for (node_id, manifest) in &transaction.node_indexes {
            self.write_json_file_batched(
                &self.node_index_manifest_path(node_id),
                manifest,
                &mut sync_dirs,
            )?;
        }

        for key in &transaction.deleted_records {
            let _ = self.remove_record_entry_batched(key, &mut sync_dirs)?;
        }
        for entry in transaction.records.values() {
            self.upsert_record_entry_batched(entry, &mut sync_dirs)?;
        }

        for path in sync_dirs {
            self.sync_dir(&path)?;
        }

        self.maybe_fail(SegmentFaultPoint::AfterIndexWrites)?;

        self.write_json_file(&self.manifest_path(), &transaction.metadata)?;
        self.maybe_fail(SegmentFaultPoint::AfterManifestWrite)?;
        Ok(())
    }

    fn materialize_direct_index_changes(
        &self,
        payload: &SegmentCommitPayload,
        sync_dirs: &mut BTreeSet<std::path::PathBuf>,
    ) -> Result<()> {
        let transaction = &payload.transaction;
        let mut buckets: BTreeMap<std::path::PathBuf, (DirectIndexBucket, bool)> = BTreeMap::new();

        for key in &payload.direct_index_removals {
            let path = self.direct_index_path(key);
            if !buckets.contains_key(&path) {
                buckets.insert(
                    path.clone(),
                    (Self::read_direct_index_bucket_path(&path)?, false),
                );
            }
            let bucket = buckets
                .get_mut(&path)
                .expect("inserted direct index bucket");
            if bucket.0.entries.remove(key).is_some() {
                bucket.1 = true;
            }
        }

        for manifest in transaction.node_indexes.values() {
            for key in &manifest.direct_index_keys {
                let Some(entry) = transaction.direct_indexes.get(key) else {
                    continue;
                };
                let path = self.direct_index_path(key);
                if !buckets.contains_key(&path) {
                    buckets.insert(
                        path.clone(),
                        (Self::read_direct_index_bucket_path(&path)?, false),
                    );
                }
                let bucket = buckets
                    .get_mut(&path)
                    .expect("inserted direct index bucket");
                if bucket.0.entries.get(key) != Some(entry) {
                    bucket.0.entries.insert(key.clone(), entry.clone());
                    bucket.1 = true;
                }
            }
        }

        for (path, (bucket, changed)) in buckets {
            if !changed {
                continue;
            }
            if bucket.entries.is_empty() {
                if path.exists() {
                    self.remove_file_batched(&path, sync_dirs)?;
                }
            } else {
                self.write_direct_index_bucket_path_batched(&path, &bucket, sync_dirs)?;
            }
        }
        Ok(())
    }

    fn remove_file_batched(
        &self,
        path: &std::path::Path,
        sync_dirs: &mut BTreeSet<std::path::PathBuf>,
    ) -> Result<bool> {
        if !path.exists() {
            return Ok(false);
        }
        std::fs::remove_file(path)?;
        if matches!(self.durability, SegmentDurability::Full)
            && let Some(parent) = path.parent()
        {
            sync_dirs.insert(parent.to_path_buf());
        }
        Ok(true)
    }

    fn remove_dir_batched(
        &self,
        path: &std::path::Path,
        sync_dirs: &mut BTreeSet<std::path::PathBuf>,
    ) -> Result<()> {
        let parent = path.parent().map(std::path::Path::to_path_buf);
        std::fs::remove_dir(path)?;
        sync_dirs.remove(path);
        if matches!(self.durability, SegmentDurability::Full)
            && let Some(parent) = parent
        {
            sync_dirs.insert(parent);
        }
        Ok(())
    }

    fn recover(&self) -> Result<StorageRecoveryReport> {
        self.ensure_layout()?;
        let mut report = StorageRecoveryReport::default();
        let mut materialized_last = self
            .read_manifest_unrecovered()?
            .map(|metadata| metadata.last_materialized_tx_id)
            .unwrap_or(0);

        let mut journal_files = Vec::new();
        for entry in std::fs::read_dir(self.root.join("journal"))? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if name.contains(".tmp-") {
                self.remove_file_durable(&path)?;
                report.removed_temp_files += 1;
                continue;
            }
            if let Some((tx_id, pending)) = Self::journal_file_key(&path) {
                journal_files.push((tx_id, pending, path));
            }
        }
        journal_files.sort_by_key(|(tx_id, pending, _)| (*tx_id, *pending));

        for (_, _, path) in journal_files {
            let payload = match Self::read_commit_record(&path) {
                Ok(payload) => payload,
                Err(_) => {
                    let quarantine = path.with_extension("corrupt");
                    let _ = std::fs::rename(&path, quarantine);
                    report.quarantined_files += 1;
                    continue;
                }
            };
            if payload.transaction.id <= materialized_last {
                if path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("pending-"))
                {
                    self.remove_file_durable(&path)?;
                    report.removed_pending_files += 1;
                }
                report.skipped_transactions += 1;
                continue;
            }
            self.materialize_commit_payload(&payload)?;
            materialized_last = materialized_last.max(payload.transaction.id);
            report.applied_transactions += 1;
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("pending-"))
            {
                let final_path = self.journal_final_path(payload.transaction.id);
                if final_path.exists() {
                    self.remove_file_durable(&path)?;
                } else {
                    replace_file(&path, &final_path)?;
                    if matches!(self.durability, SegmentDurability::Full) {
                        self.sync_dir(&self.root.join("journal"))?;
                    }
                }
            }
        }

        *self.recovery_report.lock().unwrap() = report.clone();
        Ok(report)
    }

    fn read_manifest_unrecovered(&self) -> Result<Option<StorageMetadata>> {
        let path = self.manifest_path();
        if !path.exists() {
            return Ok(None);
        }
        let metadata: StorageMetadata = serde_json::from_str(&std::fs::read_to_string(path)?)?;
        if metadata.schema_version != STORAGE_SCHEMA_VERSION {
            return Err(PrimadbError::Message(format!(
                "storage schema version {} is unsupported",
                metadata.schema_version
            )));
        }
        Ok(Some(metadata))
    }

    fn load_node_index_manifest(&self, node_id: &str) -> Result<NodeIndexManifest> {
        let path = self.node_index_manifest_path(node_id);
        if !path.exists() {
            return Ok(NodeIndexManifest::default());
        }
        Ok(serde_json::from_str(&std::fs::read_to_string(path)?)?)
    }

    fn prune_journal(&self) -> Result<()> {
        let _ = self.prune_journal_with_report()?;
        Ok(())
    }

    fn prune_journal_with_report(&self) -> Result<usize> {
        let mut entries: Vec<_> = std::fs::read_dir(self.root.join("journal"))?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let path = entry.path();
                let name = path.file_name()?.to_str()?;
                name.starts_with("tx-").then_some(path)
            })
            .collect();
        entries.sort();
        if entries.len() <= self.journal_retention {
            return Ok(0);
        }
        let remove_count = entries.len() - self.journal_retention;
        let mut sync_dirs = BTreeSet::new();
        for path in entries.into_iter().take(remove_count) {
            let _ = self.remove_file_batched(&path, &mut sync_dirs);
        }
        for path in sync_dirs {
            self.sync_dir(&path)?;
        }
        Ok(remove_count)
    }

    fn collect_live_index_paths(
        transaction: &StorageTransaction,
    ) -> (
        BTreeSet<String>,
        BTreeSet<String>,
        BTreeSet<String>,
        BTreeSet<String>,
    ) {
        let live_nodes = transaction.nodes.keys().cloned().collect::<BTreeSet<_>>();
        let live_auth = transaction
            .auth_meta
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let live_manifests = transaction
            .node_indexes
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let live_direct = transaction
            .direct_indexes
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        (live_nodes, live_auth, live_manifests, live_direct)
    }

    fn vacuum_files_for_dir(
        &self,
        dir: std::path::PathBuf,
        live_stems: &BTreeSet<String>,
    ) -> Result<usize> {
        if !dir.exists() {
            return Ok(0);
        }
        let mut removed = 0;
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            let Ok(decoded) = decode_component(stem) else {
                continue;
            };
            if !live_stems.contains(&decoded) {
                self.remove_file_durable(&path)?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    fn prune_empty_index_dirs(&self, root: &std::path::Path) -> Result<usize> {
        if !root.exists() {
            return Ok(0);
        }
        let mut removed = 0;
        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                removed += self.prune_empty_index_dirs(&path)?;
                if std::fs::read_dir(&path)?.next().is_none() {
                    self.remove_dir_durable(&path)?;
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl IncrementalStore for SegmentFileStore {
    fn name(&self) -> &str {
        "segment_file"
    }

    fn load_metadata(&self) -> Result<Option<StorageMetadata>> {
        self.ensure_layout()?;
        let _ = self.recover()?;
        self.read_manifest_unrecovered()
    }

    fn apply_transaction(&self, transaction: &StorageTransaction) -> Result<()> {
        self.ensure_layout()?;
        let payload = self.build_commit_payload(transaction)?;
        let pending_path = self.write_pending_commit(payload.clone())?;
        self.maybe_fail(SegmentFaultPoint::AfterJournalWrite)?;

        self.materialize_commit_payload(&payload)?;

        let final_path = self.journal_final_path(transaction.id);
        self.maybe_fail(SegmentFaultPoint::BeforeJournalFinalize)?;
        replace_file(&pending_path, &final_path)?;
        if matches!(self.durability, SegmentDurability::Full) {
            self.sync_dir(&self.root.join("journal"))?;
        }
        self.prune_journal()?;
        Ok(())
    }

    fn get_node(&self, node_id: &str) -> Result<Option<NodeState>> {
        self.ensure_layout()?;
        let path = self.node_path(node_id);
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(serde_json::from_str(&std::fs::read_to_string(path)?)?))
    }

    fn export_snapshot(&self, root: Option<&str>) -> Result<DatabaseSnapshot> {
        self.ensure_layout()?;
        let metadata = self
            .load_metadata()?
            .ok_or_else(|| PrimadbError::Message("storage manifest is missing".to_owned()))?;
        let mut nodes = BTreeMap::new();
        for entry in std::fs::read_dir(self.root.join("nodes"))? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            let node_id = decode_component(stem)?;
            if root.is_some_and(|root| !node_matches_root(&node_id, root)) {
                continue;
            }
            let node_state: NodeState = serde_json::from_str(&std::fs::read_to_string(path)?)?;
            nodes.insert(node_id, node_state);
        }
        let pending_ops = match root {
            Some(root) => metadata
                .pending_ops
                .into_iter()
                .filter(|op| operation_matches_root(op, root))
                .collect(),
            None => metadata.pending_ops,
        };
        Ok(DatabaseSnapshot {
            clock: metadata.clock,
            nodes,
            pending_ops,
            scope_policies: metadata.scope_policies,
            provisional_transactions: metadata.provisional_transactions,
            next_provisional_transaction_id: metadata.next_provisional_transaction_id,
        })
    }

    fn scan_direct_index_entries(
        &self,
        path: &str,
        direction: QueryDirection,
        scan: &DirectIndexScan,
    ) -> Result<Vec<DirectScalarIndexEntry>> {
        self.ensure_layout()?;
        let root = self.direct_index_root(path);
        if !root.exists() {
            return Ok(Vec::new());
        }

        if scan.limit == Some(0) {
            return Ok(Vec::new());
        }

        let dirs = self.direct_index_scan_sortable_dirs(&root, scan)?;
        let has_hashed_dirs = dirs.iter().any(|(_, key)| key.is_none());
        let mut entries = Vec::new();
        let mut remaining_offset = scan.offset;
        if has_hashed_dirs {
            // Hashed physical names do not encode logical order. Read these
            // uncommon long-key buckets through the exact same predicate,
            // then sort the combined result to retain deterministic semantics.
            let mut uncapped_scan = scan.clone();
            uncapped_scan.offset = 0;
            uncapped_scan.limit = None;
            let mut ignored_offset = 0;
            for (dir, _) in &dirs {
                let mut files = Vec::new();
                collect_files(dir, &mut files)?;
                files.sort();
                for file in files {
                    Self::collect_direct_index_entries_from_bucket(
                        &file,
                        path,
                        &uncapped_scan,
                        &mut ignored_offset,
                        &mut entries,
                    )?;
                }
            }
            sort_direct_index_entries(&mut entries, direction);
            apply_direct_index_window(&mut entries, scan.offset, scan.limit);
            return Ok(entries);
        }

        let dirs = if matches!(direction, QueryDirection::Desc) {
            dirs.into_iter().rev().collect::<Vec<_>>()
        } else {
            dirs
        };
        for (dir, _) in dirs {
            let mut files = Vec::new();
            collect_files(&dir, &mut files)?;
            files.sort();
            if matches!(direction, QueryDirection::Desc) {
                files.reverse();
            }
            for file in files {
                Self::collect_direct_index_entries_from_bucket(
                    &file,
                    path,
                    scan,
                    &mut remaining_offset,
                    &mut entries,
                )?;
                if scan.limit.is_some_and(|limit| entries.len() >= limit) {
                    return Ok(entries);
                }
            }
        }
        Ok(entries)
    }

    fn scan_record_entries(&self, scan: &RecordScan) -> Result<Option<Vec<RecordEntry>>> {
        self.ensure_layout()?;
        let (root, partial_component_prefix) = self.record_scan_root(scan);
        if !root.exists() {
            return Ok(Some(Vec::new()));
        }
        let mut entries = Vec::new();
        let encoded_root = root
            .strip_prefix(self.record_entries_root())
            .ok()
            .map(|relative| {
                relative
                    .components()
                    .map(|component| component.as_os_str().to_string_lossy())
                    .collect::<String>()
            })
            .unwrap_or_default();
        Self::collect_record_entries(
            &root,
            &encoded_root,
            partial_component_prefix.as_deref(),
            scan,
            &mut entries,
        )?;
        Ok(Some(entries))
    }

    fn sync(&self) -> Result<StorageSyncReport> {
        self.ensure_layout()?;
        if !matches!(self.durability, SegmentDurability::Relaxed) {
            self.sync_dir(&self.root)?;
            self.sync_dir(&self.root.join("journal"))?;
            self.sync_dir(&self.root.join("nodes"))?;
            self.sync_dir(&self.root.join("auth"))?;
            self.sync_dir(&self.root.join("node_indexes"))?;
            self.sync_dir(&self.root.join("records"))?;
            self.sync_dir(&self.root.join("indexes").join("direct"))?;
        }
        Ok(StorageSyncReport {
            backend: self.name().to_owned(),
            durability: format!("{:?}", self.durability).to_lowercase(),
            synced: !matches!(self.durability, SegmentDurability::Relaxed),
        })
    }

    fn recovery_report(&self) -> Option<StorageRecoveryReport> {
        Some(self.recovery_report.lock().unwrap().clone())
    }

    fn vacuum(&self, transaction: &StorageTransaction) -> Result<StorageVacuumReport> {
        self.ensure_layout()?;
        let (live_nodes, live_auth, live_manifests, live_direct) =
            Self::collect_live_index_paths(transaction);

        let mut report = StorageVacuumReport::default();
        report.removed_node_files =
            self.vacuum_files_for_dir(self.root.join("nodes"), &live_nodes)?;
        report.removed_auth_files =
            self.vacuum_files_for_dir(self.root.join("auth"), &live_auth)?;
        report.removed_index_manifests =
            self.vacuum_files_for_dir(self.root.join("node_indexes"), &live_manifests)?;

        let direct_root = self.root.join("indexes").join("direct");
        if direct_root.exists() {
            let mut files = Vec::new();
            collect_files(&direct_root, &mut files)?;
            for file in files {
                let mut bucket = Self::read_direct_index_bucket_path(&file)?;
                let before = bucket.entries.len();
                bucket.entries.retain(|key, _| live_direct.contains(key));
                if bucket.entries.is_empty() {
                    self.remove_file_durable(&file)?;
                    report.removed_direct_index_files += 1;
                } else if bucket.entries.len() != before {
                    self.write_direct_index_bucket_path(&file, &bucket)?;
                }
            }
            report.removed_empty_index_dirs = self.prune_empty_index_dirs(&direct_root)?;
        }

        report.pruned_journal_files = self.prune_journal_with_report()?;
        Ok(report)
    }
}

pub fn build_storage_metadata(
    clock: HybridClock,
    pending_ops: Vec<Operation>,
    next_tx_id: u64,
) -> StorageMetadata {
    StorageMetadata::new(clock, pending_ops, next_tx_id)
}

pub fn build_storage_transaction(
    id: u64,
    metadata: StorageMetadata,
    nodes: BTreeMap<NodeId, NodeState>,
) -> StorageTransaction {
    let mut node_indexes = BTreeMap::new();
    let mut direct_indexes = BTreeMap::new();
    let mut materialization_cache: BTreeMap<_, Arc<[_]>> = BTreeMap::new();
    let mut records = BTreeMap::new();
    let mut deleted_records = BTreeSet::new();
    let mut auth_meta = BTreeMap::new();

    for (node_id, node_state) in &nodes {
        let direct = direct_scalar_indexes(node_id, &nodes, &mut materialization_cache);
        let manifest = NodeIndexManifest {
            direct_index_keys: direct.keys().cloned().collect(),
        };
        node_indexes.insert(node_id.clone(), manifest);
        direct_indexes.extend(direct);
        if is_record_node_id(node_id)
            && let Some(record_key) = record_key_from_node_state(node_state)
        {
            if let Some(entry) = record_entry_from_node_state(node_state) {
                records.insert(entry.key.clone(), entry);
            } else {
                deleted_records.insert(record_key);
            }
        }
        auth_meta.insert(node_id.clone(), auth_node_meta(node_id, node_state));
    }

    StorageTransaction {
        id,
        metadata,
        nodes,
        node_indexes,
        direct_indexes,
        records,
        deleted_records,
        auth_meta,
        journal_ops: Vec::new(),
    }
}

pub fn build_storage_transaction_from_ops(
    id: u64,
    metadata: StorageMetadata,
    nodes: &BTreeMap<NodeId, NodeState>,
    ops: &[Operation],
) -> StorageTransaction {
    let touched = touched_storage_nodes(nodes, ops);
    let materialized_nodes = touched
        .into_iter()
        .filter_map(|node_id| nodes.get(&node_id).cloned().map(|state| (node_id, state)))
        .collect();
    let mut transaction = build_storage_transaction(id, metadata, materialized_nodes);
    transaction.journal_ops = ops.to_vec();
    transaction
}

pub fn touched_nodes(ops: &[Operation]) -> BTreeSet<NodeId> {
    let mut touched = BTreeSet::new();
    for op in ops {
        match &op.action {
            crate::operation::OperationAction::SetField { node, .. }
            | crate::operation::OperationAction::AddSetMember { node, .. }
            | crate::operation::OperationAction::RemoveSetMember { node, .. }
            | crate::operation::OperationAction::DeleteField { node, .. } => {
                touched.insert(node.clone());
            }
        }
    }
    touched
}

pub fn touched_storage_nodes(
    nodes: &BTreeMap<NodeId, NodeState>,
    ops: &[Operation],
) -> BTreeSet<NodeId> {
    let mut touched = touched_nodes(ops);
    let direct = touched.clone();
    for node_id in direct {
        let mut current = node_id.as_str();
        while let Some((parent, _)) = current.rsplit_once('/') {
            if nodes.contains_key(parent) {
                touched.insert(parent.to_owned());
            }
            current = parent;
        }
    }
    touched
}

pub fn sortable_scalar_key(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(value) => Some(format!("s_{}", encode_component(value))),
        JsonValue::Number(value) => {
            let number = value.as_f64()?;
            let bits = number.to_bits();
            let sortable = if bits >> 63 == 0 {
                bits ^ (1_u64 << 63)
            } else {
                !bits
            };
            Some(format!("n_{sortable:016x}"))
        }
        JsonValue::Bool(value) => Some(format!("b_{}", if *value { 1 } else { 0 })),
        JsonValue::Null => Some("z_null".to_owned()),
        _ => None,
    }
}

pub fn direct_index_key(path: &str, sortable_key: &str, node_id: &str) -> String {
    format!(
        "direct/{}/{}/{}",
        encode_component(path),
        sortable_key,
        encode_component(node_id)
    )
}

#[cfg(not(target_arch = "wasm32"))]
fn direct_index_key_parts(key: &str) -> Option<(&str, &str, &str)> {
    let mut parts = key.split('/');
    match (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) {
        (Some("direct"), Some(path), Some(sortable_key), Some(node_id), None) => {
            Some((path, sortable_key, node_id))
        }
        _ => None,
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn sort_direct_index_entries(entries: &mut [DirectScalarIndexEntry], direction: QueryDirection) {
    entries.sort_by(|left, right| {
        let sortable_order = left.sortable_key.cmp(&right.sortable_key);
        let sortable_order = if matches!(direction, QueryDirection::Desc) {
            sortable_order.reverse()
        } else {
            sortable_order
        };
        sortable_order
            .then_with(|| left.node_id.cmp(&right.node_id))
            .then_with(|| left.path.cmp(&right.path))
    });
}

#[cfg(not(target_arch = "wasm32"))]
fn apply_direct_index_window(
    entries: &mut Vec<DirectScalarIndexEntry>,
    offset: usize,
    limit: Option<usize>,
) {
    if offset >= entries.len() {
        entries.clear();
        return;
    }
    if offset > 0 {
        entries.drain(..offset);
    }
    if let Some(limit) = limit {
        entries.truncate(limit);
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn safe_direct_index_component(component: &str) -> String {
    if component.len() <= DIRECT_INDEX_LITERAL_COMPONENT_LIMIT {
        return format!("{DIRECT_INDEX_LITERAL_PREFIX}{component}");
    }
    format!(
        "{DIRECT_INDEX_HASH_PREFIX}{}",
        blake3::hash(component.as_bytes()).to_hex()
    )
}

#[cfg(not(target_arch = "wasm32"))]
fn literal_direct_index_component(component: &str) -> Option<&str> {
    component.strip_prefix(DIRECT_INDEX_LITERAL_PREFIX)
}

#[cfg(not(target_arch = "wasm32"))]
fn record_scan_key_prefix(scan: &RecordScan) -> Option<String> {
    if let Some(prefix) = &scan.prefix {
        return Some(prefix.clone());
    }

    let lower = if scan.reverse {
        scan.start_at.as_deref().or(scan.start_after.as_deref())
    } else {
        scan.cursor
            .as_deref()
            .or(scan.start_at.as_deref())
            .or(scan.start_after.as_deref())
    };
    let upper = if scan.reverse {
        scan.cursor
            .as_deref()
            .or(scan.end_at.as_deref())
            .or(scan.end_before.as_deref())
    } else {
        scan.end_at.as_deref().or(scan.end_before.as_deref())
    };

    let common = common_string_prefix(lower?, upper?);
    (!common.is_empty()).then_some(common)
}

#[cfg(not(target_arch = "wasm32"))]
fn common_string_prefix(left: &str, right: &str) -> String {
    let mut output = String::new();
    for (left_char, right_char) in left.chars().zip(right.chars()) {
        if left_char != right_char {
            break;
        }
        output.push(left_char);
    }
    output
}

pub fn encode_component(input: &str) -> String {
    let mut output = String::with_capacity(input.len() * 2);
    for byte in input.as_bytes() {
        use std::fmt::Write;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

pub fn direct_index_encode_prefix(input: &str) -> String {
    encode_component(input)
}

pub fn decode_component(input: &str) -> Result<String> {
    if input.len() % 2 != 0 {
        return Err(PrimadbError::Message(format!(
            "invalid storage key component `{input}`"
        )));
    }
    let bytes = (0..input.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&input[index..index + 2], 16).map_err(|error| {
                PrimadbError::Message(format!("invalid storage key component `{input}`: {error}"))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    String::from_utf8(bytes).map_err(|error| {
        PrimadbError::Message(format!(
            "invalid utf-8 storage key component `{input}`: {error}"
        ))
    })
}

pub fn node_matches_root(node_id: &str, root: &str) -> bool {
    node_id == root || node_id.starts_with(&format!("{root}/"))
}

pub fn operation_matches_root(op: &Operation, root: &str) -> bool {
    match &op.action {
        crate::operation::OperationAction::SetField { node, .. }
        | crate::operation::OperationAction::AddSetMember { node, .. }
        | crate::operation::OperationAction::RemoveSetMember { node, .. }
        | crate::operation::OperationAction::DeleteField { node, .. } => {
            node_matches_root(node, root)
        }
    }
}

pub fn record_node_id(key: &str) -> String {
    format!(
        "__primadb_records/{}",
        blake3::hash(key.as_bytes()).to_hex()
    )
}

pub fn is_record_node_id(node_id: &str) -> bool {
    node_id.starts_with("__primadb_records/")
}

pub fn record_key_from_node_state(node_state: &NodeState) -> Option<String> {
    let Some(field) = node_state.fields.get("key") else {
        return None;
    };
    match &field.value {
        FieldValue::Scalar(JsonValue::String(key)) => Some(key.clone()),
        _ => None,
    }
}

pub fn record_entry_from_node_state(node_state: &NodeState) -> Option<RecordEntry> {
    let key = record_key_from_node_state(node_state)?;
    let value = node_state.fields.get("value")?;
    let value = match &value.value {
        FieldValue::Scalar(value) => RecordValue::Json(value.clone()),
        FieldValue::Bytes(bytes) => RecordValue::Bytes(bytes.clone()),
        FieldValue::Blob(blob) => RecordValue::Blob(blob.clone()),
        FieldValue::Link(_) | FieldValue::Set(_) => return None,
    };
    Some(RecordEntry { key, value })
}

fn direct_scalar_indexes(
    node_id: &str,
    nodes: &BTreeMap<NodeId, NodeState>,
    materialization_cache: &mut BTreeMap<NodeId, Arc<[DirectScalarIndexFragment]>>,
) -> BTreeMap<String, DirectScalarIndexEntry> {
    let mut indexes = BTreeMap::new();
    let (fragments, _) = storage_collect_scalar_fragments(
        node_id,
        nodes,
        &mut BTreeSet::new(),
        materialization_cache,
    );
    for fragment in fragments.iter() {
        let key = direct_index_key(&fragment.path, &fragment.sortable_key, node_id);
        indexes.insert(
            key,
            DirectScalarIndexEntry {
                node_id: node_id.to_owned(),
                path: fragment.path.clone(),
                value: (*fragment.value).clone(),
                sortable_key: fragment.sortable_key.clone(),
            },
        );
    }
    indexes
}

fn storage_collect_scalar_fragments(
    node_id: &str,
    nodes: &BTreeMap<NodeId, NodeState>,
    visited: &mut BTreeSet<NodeId>,
    materialization_cache: &mut BTreeMap<NodeId, Arc<[DirectScalarIndexFragment]>>,
) -> (Arc<[DirectScalarIndexFragment]>, bool) {
    if let Some(fragments) = materialization_cache.get(node_id) {
        return (Arc::clone(fragments), false);
    }
    if !visited.insert(node_id.to_owned()) {
        return (Arc::from([]), true);
    }
    #[cfg(test)]
    STORAGE_MATERIALIZATION_VISITS.with(|count| count.set(count.get() + 1));
    let mut fragments = Vec::new();
    let mut contains_cycle = false;
    if let Some(node_state) = nodes.get(node_id) {
        for (field, state) in &node_state.fields {
            match &state.value {
                FieldValue::Scalar(value) => {
                    if let Some(value) = storage_materialized_scalar(node_id, field, value)
                        && let Some(sortable_key) = sortable_scalar_key(&value)
                    {
                        fragments.push(DirectScalarIndexFragment {
                            path: field.clone(),
                            value: Arc::new(value),
                            sortable_key,
                        });
                    }
                }
                FieldValue::Link(target) => {
                    let (child_fragments, child_contains_cycle) = storage_collect_scalar_fragments(
                        target,
                        nodes,
                        visited,
                        materialization_cache,
                    );
                    contains_cycle |= child_contains_cycle;
                    for fragment in child_fragments.iter() {
                        fragments.push(DirectScalarIndexFragment {
                            path: if fragment.path.is_empty() {
                                field.clone()
                            } else {
                                format!("{field}.{}", fragment.path)
                            },
                            value: Arc::clone(&fragment.value),
                            sortable_key: fragment.sortable_key.clone(),
                        });
                    }
                }
                FieldValue::Set(_) | FieldValue::Bytes(_) | FieldValue::Blob(_) => {}
            }
        }
    }
    visited.remove(node_id);
    // Cyclic materialization depends on the root's active path, so only completed
    // acyclic subgraphs are safe to reuse across direct-index roots.
    let fragments: Arc<[DirectScalarIndexFragment]> = fragments.into();
    if !contains_cycle {
        materialization_cache.insert(node_id.to_owned(), Arc::clone(&fragments));
    }
    (fragments, contains_cycle)
}

fn auth_node_meta(node_id: &str, node_state: &NodeState) -> AuthNodeMeta {
    let mut meta = AuthNodeMeta::default();
    for (field, state) in &node_state.fields {
        let FieldValue::Scalar(value) = &state.value else {
            continue;
        };
        let path = format!("{node_id}/{field}");
        if let Some(meta_value) = inspect_signed_scalar(&path, value) {
            meta.signed_fields.insert(field.clone(), meta_value);
        }
    }
    meta
}

fn storage_materialized_scalar(node_id: &str, field: &str, value: &JsonValue) -> Option<JsonValue> {
    #[cfg(feature = "crypto")]
    {
        let inspected =
            crate::inspect_signed_field_value(&format!("{node_id}/{field}"), value).ok()?;
        if let Some(inspected) = inspected {
            return inspected.unwrapped_value;
        }
        return Some(value.clone());
    }

    #[cfg(not(feature = "crypto"))]
    {
        let _ = node_id;
        let _ = field;
        Some(value.clone())
    }
}

fn inspect_signed_scalar(path: &str, value: &JsonValue) -> Option<StoredAuthFieldMeta> {
    #[cfg(feature = "crypto")]
    {
        crate::inspect_signed_field_value(path, value)
            .ok()
            .flatten()
            .map(|inspected| inspected.meta)
    }

    #[cfg(not(feature = "crypto"))]
    {
        let _ = path;
        let _ = value;
        None
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn collect_files(root: &std::path::Path, output: &mut Vec<std::path::PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, output)?;
        } else if path.is_file() {
            output.push(path);
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{Revision, VersionMarker};
    use crate::value::FieldState;
    use serde_json::json;
    #[cfg(not(target_arch = "wasm32"))]
    use std::time::{SystemTime, UNIX_EPOCH};

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn direct_index_bucket_changes_are_materialized_once_per_path() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("primadb-engine-bucket-batch-{unique}"));
        let store = SegmentFileStore::new(&root, 8)?;
        let key = direct_index_key("value", "s_76616c7565", "node");
        let entry = DirectScalarIndexEntry {
            node_id: "node".to_owned(),
            path: "value".to_owned(),
            value: JsonValue::String("value".to_owned()),
            sortable_key: "s_76616c7565".to_owned(),
        };
        let transaction = StorageTransaction {
            id: 1,
            metadata: StorageMetadata::new(HybridClock::with_actor("test"), Vec::new(), 2),
            nodes: BTreeMap::new(),
            node_indexes: BTreeMap::from([
                (
                    "first".to_owned(),
                    NodeIndexManifest {
                        direct_index_keys: vec![key.clone()],
                    },
                ),
                (
                    "second".to_owned(),
                    NodeIndexManifest {
                        direct_index_keys: vec![key.clone()],
                    },
                ),
            ]),
            direct_indexes: BTreeMap::from([(key, entry)]),
            records: BTreeMap::new(),
            deleted_records: BTreeSet::new(),
            auth_meta: BTreeMap::new(),
            journal_ops: Vec::new(),
        };
        let payload = SegmentCommitPayload {
            schema_version: SEGMENT_COMMIT_SCHEMA_VERSION,
            transaction,
            direct_index_removals: vec![direct_index_key("value", "s_76616c7565", "node")],
        };

        reset_segment_write_metrics_for_test(root.clone());
        store.materialize_commit_payload(&payload)?;
        let metrics = segment_write_metrics_for_test(root.clone());
        assert_eq!(metrics.direct_index_writes, 1);
        assert!(metrics.direct_index_directory_syncs > 0);

        let bucket_path = store.direct_index_path(
            payload
                .transaction
                .direct_indexes
                .keys()
                .next()
                .expect("test transaction direct index key"),
        );
        let bucket = SegmentFileStore::read_direct_index_bucket_path(&bucket_path)?;
        assert_eq!(bucket.entries.len(), 1);

        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    fn field(value: FieldValue) -> FieldState {
        FieldState {
            value,
            version: VersionMarker {
                revision: Revision {
                    millis: 1,
                    counter: 0,
                    actor: "test".to_owned(),
                },
                op_id: "test/op".to_owned(),
            },
        }
    }

    fn node(id: &str, fields: impl IntoIterator<Item = (&'static str, FieldValue)>) -> NodeState {
        let mut node = NodeState::new(id);
        node.fields.extend(
            fields
                .into_iter()
                .map(|(name, value)| (name.to_owned(), field(value))),
        );
        node
    }

    #[test]
    fn full_direct_index_build_reuses_shared_acyclic_subgraphs() {
        let nodes = BTreeMap::from([
            (
                "root-a".to_owned(),
                node("root-a", [("child", FieldValue::Link("shared".to_owned()))]),
            ),
            (
                "root-b".to_owned(),
                node("root-b", [("child", FieldValue::Link("shared".to_owned()))]),
            ),
            (
                "shared".to_owned(),
                node("shared", [("value", FieldValue::Scalar(json!(42)))]),
            ),
        ]);

        reset_storage_materialization_visit_count();
        let transaction = build_storage_transaction(
            1,
            build_storage_metadata(crate::clock::HybridClock::with_actor("test"), vec![], 2),
            nodes,
        );

        // Each root is visited once; the shared child is visited only once.
        assert_eq!(storage_materialization_visit_count(), 3);
        for node_id in ["root-a", "root-b"] {
            let entry = transaction
                .direct_indexes
                .values()
                .find(|entry| entry.node_id == node_id && entry.path == "child.value")
                .expect("shared child should be indexed for every root");
            assert_eq!(entry.value, json!(42));
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn direct_index_scan_is_ordered_bounded_and_candidate_aware() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("primadb-engine-range-scan-{unique}"));
        let store = SegmentFileStore::new(&root, 8)?;
        let nodes = (0..64)
            .map(|index| {
                let node_id = format!("node-{index:03}");
                (
                    node_id.clone(),
                    node(&node_id, [("rank", FieldValue::Scalar(json!(index)))]),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let transaction = build_storage_transaction(
            1,
            build_storage_metadata(crate::clock::HybridClock::with_actor("test"), vec![], 2),
            nodes,
        );
        store.apply_transaction(&transaction)?;

        reset_storage_direct_index_bucket_read_count();
        let scan = DirectIndexScan {
            offset: 7,
            limit: Some(4),
            candidate_ids: Some(
                (10..40)
                    .filter(|index| index % 2 == 0)
                    .map(|index| format!("node-{index:03}"))
                    .collect(),
            ),
            ..DirectIndexScan::default()
        };
        let entries = store.scan_direct_index_entries("rank", QueryDirection::Asc, &scan)?;
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.value.as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![24, 26, 28, 30]
        );
        assert!(storage_direct_index_bucket_read_count() < 64);

        let range = DirectIndexScan {
            start_at: Some(sortable_scalar_key(&json!(20)).unwrap()),
            end_before: Some(sortable_scalar_key(&json!(24)).unwrap()),
            ..DirectIndexScan::default()
        };
        let entries = store.scan_direct_index_entries("rank", QueryDirection::Asc, &range)?;
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.value.as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![20, 21, 22, 23]
        );

        let descending = DirectIndexScan {
            offset: 2,
            limit: Some(3),
            ..DirectIndexScan::default()
        };
        let entries = store.scan_direct_index_entries("rank", QueryDirection::Desc, &descending)?;
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.value.as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![61, 60, 59]
        );

        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn direct_index_scan_keeps_long_key_collisions_and_ties_ordered() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("primadb-engine-range-collision-{unique}"));
        let store = SegmentFileStore::new(&root, 8)?;
        let shared_prefix = "x".repeat(120);
        let low = format!("{shared_prefix}a");
        let high = format!("{shared_prefix}b");
        let nodes = BTreeMap::from([
            (
                "node-a".to_owned(),
                node("node-a", [("value", FieldValue::Scalar(json!(low)))]),
            ),
            (
                "node-b".to_owned(),
                node("node-b", [("value", FieldValue::Scalar(json!(low)))]),
            ),
            (
                "node-c".to_owned(),
                node("node-c", [("value", FieldValue::Scalar(json!(high)))]),
            ),
        ]);
        let transaction = build_storage_transaction(
            1,
            build_storage_metadata(crate::clock::HybridClock::with_actor("test"), vec![], 2),
            nodes,
        );
        store.apply_transaction(&transaction)?;
        let entries = store.scan_direct_index_entries(
            "value",
            QueryDirection::Desc,
            &DirectIndexScan {
                limit: Some(3),
                ..DirectIndexScan::default()
            },
        )?;
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.node_id.as_str())
                .collect::<Vec<_>>(),
            vec!["node-c", "node-a", "node-b"]
        );

        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[test]
    fn full_direct_index_build_visits_large_shared_graph_once_per_node() {
        const ROOT_COUNT: usize = 512;
        const SHARED_DEPTH: usize = 24;
        let mut nodes = BTreeMap::new();
        for depth in 0..SHARED_DEPTH {
            let node_id = format!("shared-{depth:02}");
            let value = if depth + 1 == SHARED_DEPTH {
                FieldValue::Scalar(json!("leaf"))
            } else {
                FieldValue::Link(format!("shared-{:02}", depth + 1))
            };
            nodes.insert(node_id.clone(), node(&node_id, [("next", value)]));
        }
        for index in 0..ROOT_COUNT {
            let node_id = format!("root-{index:03}");
            nodes.insert(
                node_id.clone(),
                node(
                    &node_id,
                    [("child", FieldValue::Link("shared-00".to_owned()))],
                ),
            );
        }
        let node_count = nodes.len();

        reset_storage_materialization_visit_count();
        let transaction = build_storage_transaction(
            1,
            build_storage_metadata(crate::clock::HybridClock::with_actor("test"), vec![], 2),
            nodes,
        );

        assert_eq!(storage_materialization_visit_count(), node_count);
        assert_eq!(transaction.node_indexes.len(), node_count);
        assert_eq!(transaction.direct_indexes.len(), node_count);
    }

    #[test]
    fn full_direct_index_build_preserves_cycle_truncation_per_root() {
        let nodes = BTreeMap::from([
            (
                "cycle-a".to_owned(),
                node(
                    "cycle-a",
                    [
                        ("name", FieldValue::Scalar(json!("a"))),
                        ("other", FieldValue::Link("cycle-b".to_owned())),
                    ],
                ),
            ),
            (
                "cycle-b".to_owned(),
                node(
                    "cycle-b",
                    [
                        ("name", FieldValue::Scalar(json!("b"))),
                        ("other", FieldValue::Link("cycle-a".to_owned())),
                    ],
                ),
            ),
        ]);

        let transaction = build_storage_transaction(
            1,
            build_storage_metadata(crate::clock::HybridClock::with_actor("test"), vec![], 2),
            nodes,
        );

        for (node_id, own_name, other_name) in [("cycle-a", "a", "b"), ("cycle-b", "b", "a")] {
            assert!(transaction.direct_indexes.values().any(|entry| {
                entry.node_id == node_id && entry.path == "name" && entry.value == json!(own_name)
            }));
            assert!(transaction.direct_indexes.values().any(|entry| {
                entry.node_id == node_id
                    && entry.path == "other.name"
                    && entry.value == json!(other_name)
            }));
            assert!(
                !transaction
                    .direct_indexes
                    .values()
                    .any(|entry| entry.node_id == node_id && entry.path == "other.other.name")
            );
        }
    }

    #[cfg(feature = "crypto")]
    #[test]
    fn full_direct_index_build_keeps_signed_scalar_verification() {
        let identity = crate::Identity::from_secret_key_bytes([7; 32]);
        let mut security = crate::SecurityState::default();
        security
            .set_local_user("test", identity, vec![crate::UserGrant::write_root("*")])
            .unwrap();
        let signed = security
            .sign_data_value("secure/value", json!("secret"), None)
            .unwrap();
        let nodes = BTreeMap::from([(
            "secure".to_owned(),
            node("secure", [("value", FieldValue::Scalar(signed))]),
        )]);

        let transaction = build_storage_transaction(
            1,
            build_storage_metadata(crate::clock::HybridClock::with_actor("test"), vec![], 2),
            nodes,
        );

        let entry = transaction
            .direct_indexes
            .values()
            .find(|entry| entry.node_id == "secure" && entry.path == "value")
            .expect("signed scalar should be indexed");
        assert_eq!(entry.value, json!("secret"));
        assert!(
            transaction
                .auth_meta
                .get("secure")
                .unwrap()
                .signed_fields
                .get("value")
                .unwrap()
                .verified
        );
    }
}
