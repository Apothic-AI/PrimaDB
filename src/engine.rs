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
use serde_json::Map as JsonMap;
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
#[cfg(not(target_arch = "wasm32"))]
use std::io::Write;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(target_arch = "wasm32"))]
use std::sync::{Arc, Mutex};

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
pub(crate) fn set_segment_fault_point_for_test(
    root: impl Into<std::path::PathBuf>,
    point: SegmentFaultPoint,
) {
    SEGMENT_FAULT_POINTS
        .lock()
        .unwrap()
        .insert(root.into(), point);
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
        std::fs::create_dir_all(self.root.join("nodes"))?;
        std::fs::create_dir_all(self.root.join("auth"))?;
        std::fs::create_dir_all(self.root.join("node_indexes"))?;
        std::fs::create_dir_all(self.root.join("indexes").join("direct"))?;
        std::fs::create_dir_all(self.record_entries_root())?;
        std::fs::create_dir_all(self.root.join("journal"))?;
        if matches!(self.durability, SegmentDurability::Full) {
            self.sync_dir(&self.root)?;
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

    fn upsert_record_entry(&self, entry: &RecordEntry) -> Result<()> {
        let path = self.record_entry_path(&entry.key);
        self.write_json_file(&path, entry)
    }

    fn remove_record_entry(&self, key: &str) -> Result<bool> {
        let path = self.record_entry_path(key);
        if !path.exists() {
            return Ok(false);
        }
        self.remove_file_durable(&path)?;
        if let Some(parent) = path.parent() {
            self.prune_empty_record_dirs(parent)?;
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

    fn remove_direct_index_entry(&self, key: &str) -> Result<bool> {
        let path = self.direct_index_path(key);
        if !path.exists() {
            return Ok(false);
        }
        let mut bucket = Self::read_direct_index_bucket_path(&path)?;
        let removed = bucket.entries.remove(key).is_some();
        if bucket.entries.is_empty() {
            self.remove_file_durable(&path)?;
        } else if removed {
            self.write_direct_index_bucket_path(&path, &bucket)?;
        }
        Ok(removed)
    }

    fn upsert_direct_index_entry(&self, key: &str, entry: &DirectScalarIndexEntry) -> Result<()> {
        let path = self.direct_index_path(key);
        let mut bucket = Self::read_direct_index_bucket_path(&path)?;
        bucket.entries.insert(key.to_owned(), entry.clone());
        self.write_direct_index_bucket_path(&path, &bucket)
    }

    fn write_json_file<T: Serialize>(&self, path: &std::path::Path, value: &T) -> Result<()> {
        self.write_file(path, &serde_json::to_vec(value)?)
    }

    fn write_file(&self, path: &std::path::Path, bytes: &[u8]) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        if matches!(self.durability, SegmentDurability::Relaxed) {
            std::fs::write(path, bytes)?;
            return Ok(());
        }

        let parent = path.parent().ok_or_else(|| {
            PrimadbError::Message(format!("path `{}` has no parent directory", path.display()))
        })?;
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
                SegmentDurability::Full => file.sync_all()?,
                SegmentDurability::Data => file.sync_data()?,
                SegmentDurability::Relaxed => {}
            }
        }
        replace_file(&temp_path, path)?;
        if matches!(self.durability, SegmentDurability::Full) {
            self.sync_dir(parent)?;
        }
        Ok(())
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
        Ok(())
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
    ) -> Result<Vec<std::path::PathBuf>> {
        if let Some(exact) = &scan.exact_sortable_key {
            let dir = root.join(safe_direct_index_component(exact));
            return Ok(dir.is_dir().then_some(dir).into_iter().collect());
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
            dirs.push(path);
        }
        dirs.sort();
        Ok(dirs)
    }

    fn collect_direct_index_entries_from_bucket(
        file: &std::path::Path,
        path: &str,
        scan: &DirectIndexScan,
        entries: &mut Vec<DirectScalarIndexEntry>,
    ) -> Result<()> {
        let bucket = Self::read_direct_index_bucket_path(file)?;
        for entry in bucket.entries.values() {
            if entry.path == path && scan.matches_sortable_key(&entry.sortable_key) {
                entries.push(entry.clone());
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

    fn collect_record_entry_files(
        dir: &std::path::Path,
        partial_component_prefix: Option<&str>,
        files: &mut Vec<std::path::PathBuf>,
    ) -> Result<()> {
        if !dir.exists() {
            return Ok(());
        }
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            if path.is_file() {
                if name == RECORD_ENTRY_FILE {
                    files.push(path);
                }
                continue;
            }
            if !path.is_dir() {
                continue;
            }
            if let Some(partial) = partial_component_prefix
                && !name.starts_with(partial)
            {
                continue;
            }
            Self::collect_record_entry_files(&path, None, files)?;
        }
        Ok(())
    }

    fn prune_empty_record_dirs(&self, start: &std::path::Path) -> Result<usize> {
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
                    self.remove_dir_durable(&current)?;
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
        if matches!(self.durability, SegmentDurability::Full) {
            self.sync_dir(&self.root.join("journal"))?;
        }
        Ok(pending_path)
    }

    fn materialize_commit_payload(&self, payload: &SegmentCommitPayload) -> Result<()> {
        let transaction = &payload.transaction;

        for (node_id, node_state) in &transaction.nodes {
            self.write_json_file(&self.node_path(node_id), node_state)?;
        }

        self.maybe_fail(SegmentFaultPoint::AfterNodeWrites)?;

        for (node_id, auth_meta) in &transaction.auth_meta {
            self.write_json_file(&self.auth_meta_path(node_id), auth_meta)?;
        }

        for stale_key in &payload.direct_index_removals {
            let _ = self.remove_direct_index_entry(stale_key)?;
        }
        for (node_id, manifest) in &transaction.node_indexes {
            for key in &manifest.direct_index_keys {
                let Some(entry) = transaction.direct_indexes.get(key) else {
                    continue;
                };
                self.upsert_direct_index_entry(key, entry)?;
            }
            self.write_json_file(&self.node_index_manifest_path(node_id), manifest)?;
        }

        for key in &transaction.deleted_records {
            let _ = self.remove_record_entry(key)?;
        }
        for entry in transaction.records.values() {
            self.upsert_record_entry(entry)?;
        }

        self.maybe_fail(SegmentFaultPoint::AfterIndexWrites)?;

        self.write_json_file(&self.manifest_path(), &transaction.metadata)?;
        self.maybe_fail(SegmentFaultPoint::AfterManifestWrite)?;
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
        for path in entries.into_iter().take(remove_count) {
            let _ = self.remove_file_durable(&path);
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

        let mut entries = Vec::new();
        for dir in self.direct_index_scan_sortable_dirs(&root, scan)? {
            let mut files = Vec::new();
            collect_files(&dir, &mut files)?;
            files.sort();
            for file in files {
                Self::collect_direct_index_entries_from_bucket(&file, path, scan, &mut entries)?;
            }
        }
        entries.sort_by(|left, right| {
            left.sortable_key
                .cmp(&right.sortable_key)
                .then_with(|| left.node_id.cmp(&right.node_id))
                .then_with(|| left.path.cmp(&right.path))
        });
        if matches!(direction, QueryDirection::Desc) {
            entries.reverse();
        }
        if let Some(limit) = scan.limit
            && entries.len() > limit
        {
            entries.truncate(limit);
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
        let mut files = Vec::new();
        Self::collect_record_entry_files(&root, partial_component_prefix.as_deref(), &mut files)?;
        files.sort();
        for file in files {
            let entry = Self::read_record_entry_path(&file)?;
            if scan.matches_key(&entry.key) {
                entries.push(entry);
            }
        }
        entries.sort_by(|left, right| left.key.cmp(&right.key));
        if scan.reverse {
            entries.reverse();
        }
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
    let mut records = BTreeMap::new();
    let mut deleted_records = BTreeSet::new();
    let mut auth_meta = BTreeMap::new();

    for (node_id, node_state) in &nodes {
        let direct = direct_scalar_indexes(node_id, &nodes);
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
) -> BTreeMap<String, DirectScalarIndexEntry> {
    let mut indexes = BTreeMap::new();
    let materialized = storage_materialize_node(node_id, nodes, &mut BTreeSet::new());
    collect_direct_scalar_indexes(node_id, "", &materialized, &mut indexes);
    indexes
}

fn storage_materialize_node(
    node_id: &str,
    nodes: &BTreeMap<NodeId, NodeState>,
    visited: &mut BTreeSet<NodeId>,
) -> JsonValue {
    if !visited.insert(node_id.to_owned()) {
        return JsonValue::Null;
    }
    let value = nodes
        .get(node_id)
        .map(|node_state| {
            let mut object = JsonMap::new();
            for (field, state) in &node_state.fields {
                match &state.value {
                    FieldValue::Scalar(value) => {
                        if let Some(value) = storage_materialized_scalar(node_id, field, value) {
                            object.insert(field.clone(), value);
                        }
                    }
                    FieldValue::Link(target) => {
                        let value = storage_materialize_node(target, nodes, visited);
                        if !value.is_null() {
                            object.insert(field.clone(), value);
                        }
                    }
                    FieldValue::Set(_) | FieldValue::Bytes(_) | FieldValue::Blob(_) => {}
                }
            }
            JsonValue::Object(object)
        })
        .unwrap_or(JsonValue::Null);
    visited.remove(node_id);
    value
}

fn collect_direct_scalar_indexes(
    node_id: &str,
    path: &str,
    value: &JsonValue,
    indexes: &mut BTreeMap<String, DirectScalarIndexEntry>,
) {
    match value {
        JsonValue::Object(object) => {
            for (field, value) in object {
                let next_path = if path.is_empty() {
                    field.clone()
                } else {
                    format!("{path}.{field}")
                };
                collect_direct_scalar_indexes(node_id, &next_path, value, indexes);
            }
        }
        JsonValue::String(_) | JsonValue::Number(_) | JsonValue::Bool(_) | JsonValue::Null => {
            let Some(sortable_key) = sortable_scalar_key(value) else {
                return;
            };
            let key = direct_index_key(path, &sortable_key, node_id);
            indexes.insert(
                key,
                DirectScalarIndexEntry {
                    node_id: node_id.to_owned(),
                    path: path.to_owned(),
                    value: value.clone(),
                    sortable_key,
                },
            );
        }
        JsonValue::Array(_) => {}
    }
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
