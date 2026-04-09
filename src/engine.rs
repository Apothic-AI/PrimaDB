use crate::clock::HybridClock;
use crate::error::{PrimadbError, Result};
use crate::operation::Operation;
use crate::query::QueryDirection;
use crate::snapshot::DatabaseSnapshot;
use crate::value::{FieldValue, NodeId, NodeState};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

const STORAGE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StorageMetadata {
    pub schema_version: u32,
    pub clock: HybridClock,
    pub pending_ops: Vec<Operation>,
    pub next_tx_id: u64,
}

impl StorageMetadata {
    pub fn new(clock: HybridClock, pending_ops: Vec<Operation>, next_tx_id: u64) -> Self {
        Self {
            schema_version: STORAGE_SCHEMA_VERSION,
            clock,
            pending_ops,
            next_tx_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DirectScalarIndexEntry {
    pub node_id: NodeId,
    pub path: String,
    pub value: JsonValue,
    pub sortable_key: String,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StorageTransaction {
    pub id: u64,
    pub metadata: StorageMetadata,
    pub nodes: BTreeMap<NodeId, NodeState>,
    pub node_indexes: BTreeMap<NodeId, NodeIndexManifest>,
    pub direct_indexes: BTreeMap<String, DirectScalarIndexEntry>,
    pub auth_meta: BTreeMap<NodeId, AuthNodeMeta>,
    pub journal_ops: Vec<Operation>,
}

pub trait IncrementalStore: Debug + Send + Sync {
    fn name(&self) -> &str;
    fn load_metadata(&self) -> Result<Option<StorageMetadata>>;
    fn apply_transaction(&self, transaction: &StorageTransaction) -> Result<()>;
    fn get_node(&self, node_id: &str) -> Result<Option<NodeState>>;
    fn export_snapshot(&self, root: Option<&str>) -> Result<DatabaseSnapshot>;
    fn list_direct_index_entries(
        &self,
        path: &str,
        direction: QueryDirection,
    ) -> Result<Vec<DirectScalarIndexEntry>>;
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
pub struct SegmentFileStore {
    root: std::path::PathBuf,
    journal_retention: usize,
}

#[cfg(not(target_arch = "wasm32"))]
impl SegmentFileStore {
    pub fn new(root: impl Into<std::path::PathBuf>, journal_retention: usize) -> Self {
        Self {
            root: root.into(),
            journal_retention: journal_retention.max(1),
        }
    }

    fn ensure_layout(&self) -> Result<()> {
        std::fs::create_dir_all(self.root.join("nodes"))?;
        std::fs::create_dir_all(self.root.join("auth"))?;
        std::fs::create_dir_all(self.root.join("node_indexes"))?;
        std::fs::create_dir_all(self.root.join("indexes").join("direct"))?;
        std::fs::create_dir_all(self.root.join("journal"))?;
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
            .join(encode_component(path))
    }

    fn direct_index_path(&self, key: &str) -> std::path::PathBuf {
        let mut path = self.root.join("indexes");
        for segment in key.split('/') {
            path.push(segment);
        }
        path.set_extension("json");
        path
    }

    fn journal_pending_path(&self, tx_id: u64) -> std::path::PathBuf {
        self.root
            .join("journal")
            .join(format!("pending-{tx_id:020}.json"))
    }

    fn journal_final_path(&self, tx_id: u64) -> std::path::PathBuf {
        self.root.join("journal").join(format!("tx-{tx_id:020}.json"))
    }

    fn load_node_index_manifest(&self, node_id: &str) -> Result<NodeIndexManifest> {
        let path = self.node_index_manifest_path(node_id);
        if !path.exists() {
            return Ok(NodeIndexManifest::default());
        }
        Ok(serde_json::from_str(&std::fs::read_to_string(path)?)?)
    }

    fn prune_journal(&self) -> Result<()> {
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
            return Ok(());
        }
        let remove_count = entries.len() - self.journal_retention;
        for path in entries.into_iter().take(remove_count) {
            let _ = std::fs::remove_file(path);
        }
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl IncrementalStore for SegmentFileStore {
    fn name(&self) -> &str {
        "segment_file"
    }

    fn load_metadata(&self) -> Result<Option<StorageMetadata>> {
        self.ensure_layout()?;
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

    fn apply_transaction(&self, transaction: &StorageTransaction) -> Result<()> {
        self.ensure_layout()?;

        let pending_path = self.journal_pending_path(transaction.id);
        std::fs::write(&pending_path, serde_json::to_string_pretty(transaction)?)?;

        for (node_id, node_state) in &transaction.nodes {
            std::fs::write(
                self.node_path(node_id),
                serde_json::to_string_pretty(node_state)?,
            )?;
        }

        for (node_id, auth_meta) in &transaction.auth_meta {
            std::fs::write(
                self.auth_meta_path(node_id),
                serde_json::to_string_pretty(auth_meta)?,
            )?;
        }

        for (node_id, manifest) in &transaction.node_indexes {
            let previous = self.load_node_index_manifest(node_id)?;
            let next_keys: BTreeSet<_> = manifest.direct_index_keys.iter().cloned().collect();
            for stale_key in previous
                .direct_index_keys
                .into_iter()
                .filter(|key| !next_keys.contains(key))
            {
                let stale_path = self.direct_index_path(&stale_key);
                if stale_path.exists() {
                    let _ = std::fs::remove_file(&stale_path);
                }
            }
            for key in &manifest.direct_index_keys {
                let Some(entry) = transaction.direct_indexes.get(key) else {
                    continue;
                };
                let path = self.direct_index_path(key);
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(path, serde_json::to_string_pretty(entry)?)?;
            }
            std::fs::write(
                self.node_index_manifest_path(node_id),
                serde_json::to_string_pretty(manifest)?,
            )?;
        }

        std::fs::write(
            self.manifest_path(),
            serde_json::to_string_pretty(&transaction.metadata)?,
        )?;

        let final_path = self.journal_final_path(transaction.id);
        std::fs::rename(&pending_path, &final_path)?;
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
        })
    }

    fn list_direct_index_entries(
        &self,
        path: &str,
        direction: QueryDirection,
    ) -> Result<Vec<DirectScalarIndexEntry>> {
        self.ensure_layout()?;
        let root = self.direct_index_root(path);
        if !root.exists() {
            return Ok(Vec::new());
        }
        let mut files = Vec::new();
        collect_files(&root, &mut files)?;
        let mut entries = Vec::with_capacity(files.len());
        for file in files {
            let entry: DirectScalarIndexEntry = serde_json::from_str(&std::fs::read_to_string(file)?)?;
            entries.push(entry);
        }
        entries.sort_by(|left, right| {
            left.sortable_key
                .cmp(&right.sortable_key)
                .then_with(|| left.node_id.cmp(&right.node_id))
        });
        if matches!(direction, QueryDirection::Desc) {
            entries.reverse();
        }
        Ok(entries)
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
    let mut auth_meta = BTreeMap::new();

    for (node_id, node_state) in &nodes {
        let direct = direct_scalar_indexes(node_id, node_state);
        let manifest = NodeIndexManifest {
            direct_index_keys: direct.keys().cloned().collect(),
        };
        node_indexes.insert(node_id.clone(), manifest);
        direct_indexes.extend(direct);
        auth_meta.insert(node_id.clone(), auth_node_meta(node_id, node_state));
    }

    StorageTransaction {
        id,
        metadata,
        nodes,
        node_indexes,
        direct_indexes,
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
    let touched = touched_nodes(ops);
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

pub fn encode_component(input: &str) -> String {
    let mut output = String::with_capacity(input.len() * 2);
    for byte in input.as_bytes() {
        use std::fmt::Write;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
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
                PrimadbError::Message(format!(
                    "invalid storage key component `{input}`: {error}"
                ))
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
        | crate::operation::OperationAction::DeleteField { node, .. } => node_matches_root(node, root),
    }
}

fn direct_scalar_indexes(
    node_id: &str,
    node_state: &NodeState,
) -> BTreeMap<String, DirectScalarIndexEntry> {
    let mut indexes = BTreeMap::new();
    for (field, state) in &node_state.fields {
        let FieldValue::Scalar(value) = &state.value else {
            continue;
        };
        let Some(value) = storage_materialized_scalar(node_id, field, value) else {
            continue;
        };
        let Some(sortable_key) = sortable_scalar_key(&value) else {
            continue;
        };
        let key = direct_index_key(field, &sortable_key, node_id);
        indexes.insert(
            key,
            DirectScalarIndexEntry {
                node_id: node_id.to_owned(),
                path: field.clone(),
                value,
                sortable_key,
            },
        );
    }
    indexes
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
        let inspected = crate::inspect_signed_field_value(&format!("{node_id}/{field}"), value).ok()?;
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
