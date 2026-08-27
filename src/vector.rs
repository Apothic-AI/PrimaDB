use crate::binary::BinaryBytes;
use crate::engine::decode_component;
use crate::error::{PrimadbError, Result};
use crate::record::{RecordEntry, RecordValue};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
#[cfg(feature = "vector-edgevec")]
use std::sync::Arc;

pub const VECTOR_RECORD_PREFIX: &str = "__primadb_vectors";
pub const VECTOR_ENCODING_F32_LE: &str = "f32_le";
#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
pub const VECTOR_CACHE_FORMAT_VERSION: u32 = 1;

const DEFAULT_VECTOR_CHUNK_BYTES: usize = 64 * 1024;
const CHUNK_MAGIC: &[u8] = b"PRIMADB_VECTOR_CHUNK_V1\n";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum VectorMetric {
    #[default]
    Cosine,
    L2,
    Dot,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum VectorBackendKind {
    #[default]
    Exact,
    Edgevec,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct VectorHnswConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub m: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ef_construction: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ef_search: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tombstone_rebuild_ratio: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VectorChunkingConfig {
    pub chunk_bytes: usize,
}

impl Default for VectorChunkingConfig {
    fn default() -> Self {
        Self {
            chunk_bytes: DEFAULT_VECTOR_CHUNK_BYTES,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct VectorCollectionConfig {
    pub dim: usize,
    #[serde(default)]
    pub metric: VectorMetric,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend: Option<VectorBackendKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hnsw: Option<VectorHnswConfig>,
    #[serde(default)]
    pub chunking: VectorChunkingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct VectorItemMeta {
    pub id: String,
    pub write_id: String,
    pub dim: usize,
    pub encoding: String,
    pub byte_length: usize,
    pub checksum: String,
    pub chunk_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<JsonValue>,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VectorChunkHeader {
    pub write_id: String,
    pub chunk_index: usize,
    pub chunk_count: usize,
    pub byte_offset: usize,
    pub checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct VectorEntry {
    pub id: String,
    pub vector: Vec<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<JsonValue>,
    pub write_id: String,
    pub checksum: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum VectorStalePolicy {
    #[default]
    FallbackExact,
    AllowStale,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct VectorMetadataFilter {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub eq: BTreeMap<String, JsonValue>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub prefix: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exists: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct VectorFilter {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<VectorMetadataFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct VectorSearchSpec {
    pub limit: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ef: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<VectorFilter>,
    #[serde(default)]
    pub include_vector: bool,
    #[serde(default)]
    pub include_metadata: bool,
    #[serde(default)]
    pub exact: bool,
    #[serde(default)]
    pub stale_policy: VectorStalePolicy,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum VectorManagerState {
    #[default]
    Ready,
    CatchingUp,
    Rebuilding,
    Stale,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct VectorMatch {
    pub id: String,
    pub distance: f32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<Vec<f32>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct VectorSearchResult {
    pub matches: Vec<VectorMatch>,
    pub exact: bool,
    pub backend: VectorBackendKind,
    pub state: VectorManagerState,
    pub stale: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approximate_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct VectorIndexStats {
    pub vector_count: usize,
    pub deleted_count: usize,
    pub incomplete_count: usize,
    pub dim: usize,
    pub metric: VectorMetric,
    pub state: VectorManagerState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct VectorCacheManifest {
    pub collection: String,
    pub dim: usize,
    pub metric: VectorMetric,
    pub vector_count: usize,
    pub deleted_count: usize,
    pub incomplete_count: usize,
    pub record_prefix: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<u64>,
    pub source_hash: String,
    pub source_hash_mode: String,
    pub backend: VectorBackendKind,
    pub backend_version: String,
    pub cache_format_version: u32,
    pub manager_state: VectorManagerState,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VectorCacheKeyRecord {
    pub id: String,
    pub write_id: String,
    pub checksum: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorCacheFiles {
    pub manifest: VectorCacheManifest,
    pub vectors_f32: Vec<u8>,
    pub keys_bin: Vec<u8>,
    pub metadata_bin: Vec<u8>,
    pub backend_edgevec: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VectorCacheEntry {
    pub vector: Vec<f32>,
    pub metadata: Option<JsonValue>,
    pub write_id: String,
    pub checksum: String,
}

#[derive(Debug, Clone)]
pub(crate) struct VectorCollectionCache {
    pub config: VectorCollectionConfig,
    pub entries: BTreeMap<String, VectorCacheEntry>,
    pub deleted_count: usize,
    pub incomplete_count: usize,
    pub state: VectorManagerState,
    pub dirty: bool,
    pub source_hash: String,
    #[cfg(feature = "vector-edgevec")]
    pub ann: Option<EdgeVecVectorIndex>,
}

struct PreparedVectorFilter<'a> {
    filter: Option<&'a VectorFilter>,
    ids: Option<BTreeSet<&'a str>>,
}

struct ExactCandidate<'a> {
    id: &'a str,
    entry: &'a VectorCacheEntry,
    distance: f32,
}

impl PartialEq for ExactCandidate<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.distance.total_cmp(&other.distance) == std::cmp::Ordering::Equal && self.id == other.id
    }
}

impl Eq for ExactCandidate<'_> {}

impl PartialOrd for ExactCandidate<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ExactCandidate<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.id.cmp(other.id))
    }
}

#[cfg(feature = "vector-edgevec")]
#[derive(Clone)]
pub(crate) struct EdgeVecVectorIndex {
    inner: Arc<EdgeVecVectorIndexInner>,
}

#[cfg(feature = "vector-edgevec")]
struct EdgeVecVectorIndexInner {
    index: edgevec::HnswIndex,
    storage: edgevec::VectorStorage,
    vector_id_to_key: BTreeMap<u64, String>,
    metric: VectorMetric,
}

#[cfg(feature = "vector-edgevec")]
impl std::fmt::Debug for EdgeVecVectorIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EdgeVecVectorIndex")
            .field("vector_count", &self.inner.vector_id_to_key.len())
            .field("metric", &self.inner.metric)
            .finish_non_exhaustive()
    }
}

#[allow(dead_code)]
pub(crate) trait VectorIndexBackend {
    fn upsert(&mut self, key: &str, vector: &[f32], metadata: Option<&JsonValue>) -> Result<()>;
    fn delete(&mut self, key: &str) -> Result<()>;
    fn search(&self, query: &[f32], spec: &VectorSearchSpec) -> Result<VectorSearchResult>;
    fn rebuild(&mut self, entries: Vec<VectorEntry>) -> Result<()>;
    fn load_cache(&mut self, _manifest: &VectorCacheManifest) -> Result<()> {
        Err(PrimadbError::Message(
            "vector backend cache loading is not implemented for this backend".to_owned(),
        ))
    }
    fn write_cache(&self, _manifest: &VectorCacheManifest) -> Result<()> {
        Err(PrimadbError::Message(
            "vector backend cache writing is not implemented for this backend".to_owned(),
        ))
    }
    fn stats(&self) -> VectorIndexStats;
}

#[allow(dead_code)]
pub(crate) struct ExactVectorIndex {
    cache: VectorCollectionCache,
}

#[allow(dead_code)]
impl ExactVectorIndex {
    pub fn new(config: VectorCollectionConfig) -> Self {
        Self {
            cache: VectorCollectionCache::empty(config),
        }
    }
}

impl VectorIndexBackend for ExactVectorIndex {
    fn upsert(&mut self, key: &str, vector: &[f32], metadata: Option<&JsonValue>) -> Result<()> {
        validate_vector(vector, self.cache.config.dim)?;
        let bytes = encode_f32_le(vector);
        self.cache.entries.insert(
            key.to_owned(),
            VectorCacheEntry {
                vector: vector.to_vec(),
                metadata: metadata.cloned(),
                write_id: String::new(),
                checksum: checksum_bytes(&bytes),
            },
        );
        Ok(())
    }

    fn delete(&mut self, key: &str) -> Result<()> {
        self.cache.entries.remove(key);
        self.cache.deleted_count = self.cache.deleted_count.saturating_add(1);
        Ok(())
    }

    fn search(&self, query: &[f32], spec: &VectorSearchSpec) -> Result<VectorSearchResult> {
        exact_search(&self.cache, query, spec)
    }

    fn rebuild(&mut self, entries: Vec<VectorEntry>) -> Result<()> {
        self.cache.entries.clear();
        for entry in entries {
            self.upsert(&entry.id, &entry.vector, entry.metadata.as_ref())?;
        }
        self.cache.state = VectorManagerState::Ready;
        self.cache.dirty = false;
        Ok(())
    }

    fn stats(&self) -> VectorIndexStats {
        self.cache.stats()
    }
}

impl VectorCollectionCache {
    pub fn empty(config: VectorCollectionConfig) -> Self {
        Self {
            config,
            entries: BTreeMap::new(),
            deleted_count: 0,
            incomplete_count: 0,
            state: VectorManagerState::Ready,
            dirty: false,
            source_hash: String::new(),
            #[cfg(feature = "vector-edgevec")]
            ann: None,
        }
    }

    pub fn stats(&self) -> VectorIndexStats {
        VectorIndexStats {
            vector_count: self.entries.len(),
            deleted_count: self.deleted_count,
            incomplete_count: self.incomplete_count,
            dim: self.config.dim,
            metric: self.config.metric,
            state: self.state,
        }
    }
}

pub(crate) fn build_vector_cache_files(
    collection: &str,
    cache: &VectorCollectionCache,
    created_at: String,
) -> Result<VectorCacheFiles> {
    let keys = cache
        .entries
        .iter()
        .map(|(id, entry)| VectorCacheKeyRecord {
            id: id.clone(),
            write_id: entry.write_id.clone(),
            checksum: entry.checksum.clone(),
        })
        .collect::<Vec<_>>();
    let mut vectors = Vec::with_capacity(cache.entries.len() * cache.config.dim * 4);
    let mut metadata = Vec::with_capacity(cache.entries.len());
    for key in &keys {
        let Some(entry) = cache.entries.get(&key.id) else {
            return Err(PrimadbError::Message(format!(
                "vector cache entry `{}` disappeared while exporting",
                key.id
            )));
        };
        vectors.extend_from_slice(&encode_f32_le(&entry.vector));
        metadata.push(entry.metadata.clone());
    }

    let backend = cache.config.backend.unwrap_or(VectorBackendKind::Exact);
    let manifest = VectorCacheManifest {
        collection: collection.to_owned(),
        dim: cache.config.dim,
        metric: cache.config.metric,
        vector_count: cache.entries.len(),
        deleted_count: cache.deleted_count,
        incomplete_count: cache.incomplete_count,
        record_prefix: vector_collection_items_prefix(collection),
        source_revision: None,
        source_hash: cache.source_hash.clone(),
        source_hash_mode: "rebuild_scan".to_owned(),
        backend,
        backend_version: vector_backend_version(backend),
        cache_format_version: VECTOR_CACHE_FORMAT_VERSION,
        manager_state: VectorManagerState::Ready,
        created_at,
    };

    Ok(VectorCacheFiles {
        manifest,
        vectors_f32: vectors,
        keys_bin: serde_json::to_vec(&keys)?,
        metadata_bin: serde_json::to_vec(&metadata)?,
        backend_edgevec: (backend == VectorBackendKind::Edgevec)
            .then(|| br#"{"kind":"edgevec","cacheOnly":true,"formatVersion":1}"#.to_vec()),
    })
}

pub(crate) fn collection_cache_from_cache_files(
    config: VectorCollectionConfig,
    files: VectorCacheFiles,
    expected_source_hash: &str,
) -> Result<VectorCollectionCache> {
    let manifest = &files.manifest;
    if manifest.cache_format_version != VECTOR_CACHE_FORMAT_VERSION {
        return Err(PrimadbError::Message(format!(
            "vector cache format mismatch: expected {}, got {}",
            VECTOR_CACHE_FORMAT_VERSION, manifest.cache_format_version
        )));
    }
    if manifest.dim != config.dim || manifest.metric != config.metric {
        return Err(PrimadbError::Message(
            "vector cache manifest does not match collection config".to_owned(),
        ));
    }
    if manifest.source_hash != expected_source_hash {
        return Err(PrimadbError::Message(
            "vector cache source hash does not match authoritative records".to_owned(),
        ));
    }
    if manifest.backend_version != vector_backend_version(manifest.backend) {
        return Err(PrimadbError::Message(
            "vector cache backend version does not match this build".to_owned(),
        ));
    }

    let keys: Vec<VectorCacheKeyRecord> = serde_json::from_slice(&files.keys_bin)?;
    let metadata: Vec<Option<JsonValue>> = serde_json::from_slice(&files.metadata_bin)?;
    if keys.len() != manifest.vector_count || metadata.len() != manifest.vector_count {
        return Err(PrimadbError::Message(
            "vector cache side tables have inconsistent lengths".to_owned(),
        ));
    }
    let expected_vector_bytes = manifest
        .vector_count
        .checked_mul(manifest.dim)
        .and_then(|value| value.checked_mul(4))
        .ok_or_else(|| PrimadbError::Message("vector cache byte length overflow".to_owned()))?;
    if files.vectors_f32.len() != expected_vector_bytes {
        return Err(PrimadbError::Message(format!(
            "vector cache vector slab has wrong byte length: expected {expected_vector_bytes}, got {}",
            files.vectors_f32.len()
        )));
    }

    let mut cache = VectorCollectionCache::empty(config);
    cache.deleted_count = manifest.deleted_count;
    cache.incomplete_count = manifest.incomplete_count;
    cache.source_hash = manifest.source_hash.clone();
    cache.state = VectorManagerState::Ready;
    cache.dirty = false;

    for (index, key) in keys.into_iter().enumerate() {
        let start = index * manifest.dim * 4;
        let end = start + manifest.dim * 4;
        let vector_bytes = &files.vectors_f32[start..end];
        if checksum_bytes(vector_bytes) != key.checksum {
            return Err(PrimadbError::Message(format!(
                "vector cache checksum mismatch for `{}`",
                key.id
            )));
        }
        let vector = decode_f32_le(vector_bytes)?;
        validate_vector(&vector, manifest.dim)?;
        cache.entries.insert(
            key.id,
            VectorCacheEntry {
                vector,
                metadata: metadata[index].clone(),
                write_id: key.write_id,
                checksum: key.checksum,
            },
        );
    }
    let _ = build_vector_ann(&mut cache);
    Ok(cache)
}

pub(crate) fn build_vector_ann(cache: &mut VectorCollectionCache) -> Result<()> {
    #[cfg(feature = "vector-edgevec")]
    {
        cache.ann = None;
        if cache.config.backend.unwrap_or(VectorBackendKind::Exact) == VectorBackendKind::Edgevec {
            cache.ann = Some(EdgeVecVectorIndex::from_cache(cache)?);
        }
    }
    #[cfg(not(feature = "vector-edgevec"))]
    {
        let _ = cache;
    }
    Ok(())
}

pub(crate) fn search_vector_collection(
    cache: &VectorCollectionCache,
    query: &[f32],
    spec: &VectorSearchSpec,
) -> Result<VectorSearchResult> {
    if spec.exact || spec.filter.is_some() {
        return exact_search(cache, query, spec);
    }
    if cache.config.backend.unwrap_or(VectorBackendKind::Exact) == VectorBackendKind::Edgevec {
        #[cfg(feature = "vector-edgevec")]
        if let Some(ann) = &cache.ann {
            return ann.search(cache, query, spec);
        }
    }
    exact_search(cache, query, spec)
}

pub(crate) fn vector_backend_version(kind: VectorBackendKind) -> String {
    match kind {
        VectorBackendKind::Exact => "primadb-exact-v1".to_owned(),
        VectorBackendKind::Edgevec => {
            #[cfg(feature = "vector-edgevec")]
            {
                format!("primadb-edgevec-{}-v1", edgevec::version())
            }
            #[cfg(not(feature = "vector-edgevec"))]
            {
                "primadb-edgevec-unavailable-v1".to_owned()
            }
        }
    }
}

#[cfg(feature = "vector-edgevec")]
impl EdgeVecVectorIndex {
    pub fn from_cache(cache: &VectorCollectionCache) -> Result<Self> {
        let config = edgevec_hnsw_config(&cache.config)?;
        let mut storage = edgevec::VectorStorage::new(&config, None);
        let mut index = edgevec::HnswIndex::new(config, &storage).map_err(|error| {
            PrimadbError::Message(format!("edgevec index init failed: {error}"))
        })?;
        let mut vector_id_to_key = BTreeMap::new();

        for (key, entry) in &cache.entries {
            let vector = edgevec_vector_for_metric(cache.config.metric, &entry.vector)?;
            let vector_id = index.insert(&vector, &mut storage).map_err(|error| {
                PrimadbError::Message(format!("edgevec insert failed for `{key}`: {error}"))
            })?;
            vector_id_to_key.insert(vector_id.0, key.clone());
        }

        Ok(Self {
            inner: Arc::new(EdgeVecVectorIndexInner {
                index,
                storage,
                vector_id_to_key,
                metric: cache.config.metric,
            }),
        })
    }

    pub fn search(
        &self,
        cache: &VectorCollectionCache,
        query: &[f32],
        spec: &VectorSearchSpec,
    ) -> Result<VectorSearchResult> {
        validate_vector(query, cache.config.dim)?;
        validate_search_spec(spec)?;
        let query_for_index = edgevec_vector_for_metric(cache.config.metric, query)?;
        let search_k = spec.ef.unwrap_or(spec.limit).max(spec.limit);
        let candidates = self
            .inner
            .index
            .search(&query_for_index, search_k, &self.inner.storage)
            .map_err(|error| PrimadbError::Message(format!("edgevec search failed: {error}")))?;

        let filter = prepare_vector_filter(spec.filter.as_ref());
        let mut matches = Vec::with_capacity(spec.limit);
        for candidate in candidates {
            let Some(key) = self.inner.vector_id_to_key.get(&candidate.vector_id.0) else {
                continue;
            };
            let Some(entry) = cache.entries.get(key) else {
                continue;
            };
            if !vector_filter_matches(key, entry.metadata.as_ref(), &filter) {
                continue;
            }
            matches.push(VectorMatch {
                id: key.clone(),
                distance: vector_distance(self.inner.metric, query, &entry.vector),
                metadata: spec
                    .include_metadata
                    .then(|| entry.metadata.clone())
                    .flatten(),
                vector: spec.include_vector.then(|| entry.vector.clone()),
            });
            if matches.len() >= spec.limit {
                break;
            }
        }
        matches.sort_by(|left, right| {
            left.distance
                .total_cmp(&right.distance)
                .then_with(|| left.id.cmp(&right.id))
        });
        matches.truncate(spec.limit);

        Ok(VectorSearchResult {
            matches,
            exact: false,
            backend: VectorBackendKind::Edgevec,
            state: cache.state,
            stale: cache.state != VectorManagerState::Ready,
            approximate_reason: Some("edgevec_hnsw".to_owned()),
        })
    }
}

#[cfg(feature = "vector-edgevec")]
fn edgevec_hnsw_config(config: &VectorCollectionConfig) -> Result<edgevec::HnswConfig> {
    let dimensions = u32::try_from(config.dim).map_err(|_| {
        PrimadbError::Message("vector dimension exceeds EdgeVec u32 limit".to_owned())
    })?;
    let mut hnsw = edgevec::HnswConfig::new(dimensions);
    hnsw.metric = match config.metric {
        VectorMetric::L2 => edgevec::HnswConfig::METRIC_L2_SQUARED,
        VectorMetric::Cosine | VectorMetric::Dot => edgevec::HnswConfig::METRIC_DOT_PRODUCT,
    };
    if let Some(options) = &config.hnsw {
        if let Some(m) = options.m {
            hnsw.m = m.max(2);
            hnsw.m0 = hnsw.m.saturating_mul(2).max(hnsw.m);
        }
        if let Some(ef_construction) = options.ef_construction {
            hnsw.ef_construction = ef_construction.max(1);
        }
        if let Some(ef_search) = options.ef_search {
            hnsw.ef_search = ef_search.max(1);
        }
    }
    Ok(hnsw)
}

#[cfg(feature = "vector-edgevec")]
fn edgevec_vector_for_metric(metric: VectorMetric, vector: &[f32]) -> Result<Vec<f32>> {
    if metric != VectorMetric::Cosine {
        return Ok(vector.to_vec());
    }
    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm == 0.0 {
        return Ok(vector.to_vec());
    }
    Ok(vector.iter().map(|value| value / norm).collect())
}

pub fn validate_collection_config(config: &VectorCollectionConfig) -> Result<()> {
    if config.dim == 0 {
        return Err(PrimadbError::Message(
            "vector collection dimension must be greater than zero".to_owned(),
        ));
    }
    if config.chunking.chunk_bytes == 0 {
        return Err(PrimadbError::Message(
            "vector chunkBytes must be greater than zero".to_owned(),
        ));
    }
    Ok(())
}

pub fn validate_vector(vector: &[f32], dim: usize) -> Result<()> {
    if vector.len() != dim {
        return Err(PrimadbError::Message(format!(
            "vector dimension mismatch: expected {dim}, got {}",
            vector.len()
        )));
    }
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(PrimadbError::Message(
            "vectors must contain only finite f32 values".to_owned(),
        ));
    }
    Ok(())
}

pub fn validate_search_spec(spec: &VectorSearchSpec) -> Result<()> {
    if spec.limit == 0 {
        return Err(PrimadbError::Message(
            "vector search limit must be greater than zero".to_owned(),
        ));
    }
    Ok(())
}

pub fn vector_collection_meta_key(collection: &str) -> String {
    format!(
        "{}/{}/meta",
        VECTOR_RECORD_PREFIX,
        crate::encode_component(collection)
    )
}

pub fn vector_collection_items_prefix(collection: &str) -> String {
    format!(
        "{}/{}/items/",
        VECTOR_RECORD_PREFIX,
        crate::encode_component(collection)
    )
}

pub fn vector_item_meta_key(collection: &str, id: &str) -> String {
    format!(
        "{}{}/meta",
        vector_collection_items_prefix(collection),
        crate::encode_component(id)
    )
}

pub fn vector_item_chunks_prefix(collection: &str, id: &str) -> String {
    format!(
        "{}{}/chunks/",
        vector_collection_items_prefix(collection),
        crate::encode_component(id)
    )
}

pub fn vector_item_chunk_key(collection: &str, id: &str, chunk_index: usize) -> String {
    format!(
        "{}{}",
        vector_item_chunks_prefix(collection, id),
        chunk_index
    )
}

pub fn vector_collection_from_record_key(key: &str) -> Option<String> {
    let suffix = key.strip_prefix(VECTOR_RECORD_PREFIX)?.strip_prefix('/')?;
    let encoded = suffix.split('/').next()?;
    decode_component(encoded).ok()
}

pub fn vector_item_id_from_record_key(collection: &str, key: &str) -> Option<String> {
    let suffix = key.strip_prefix(&vector_collection_items_prefix(collection))?;
    let encoded = suffix.split('/').next()?;
    decode_component(encoded).ok()
}

pub fn encode_f32_le(vector: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(vector.len() * 4);
    for value in vector {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

pub fn decode_f32_le(bytes: &[u8]) -> Result<Vec<f32>> {
    let chunks = bytes.chunks_exact(4);
    if !chunks.remainder().is_empty() {
        return Err(PrimadbError::Message(
            "f32_le vector bytes length is not divisible by four".to_owned(),
        ));
    }
    Ok(chunks
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect())
}

pub fn checksum_bytes(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

pub fn encode_vector_chunk(header: &VectorChunkHeader, payload: &[u8]) -> Result<BinaryBytes> {
    let header_json = serde_json::to_vec(header)?;
    let header_len = u32::try_from(header_json.len()).map_err(|_| {
        PrimadbError::Message("vector chunk header is too large to encode".to_owned())
    })?;
    let mut bytes = Vec::with_capacity(CHUNK_MAGIC.len() + 4 + header_json.len() + payload.len());
    bytes.extend_from_slice(CHUNK_MAGIC);
    bytes.extend_from_slice(&header_len.to_le_bytes());
    bytes.extend_from_slice(&header_json);
    bytes.extend_from_slice(payload);
    Ok(BinaryBytes::from(bytes))
}

pub fn decode_vector_chunk(bytes: &[u8]) -> Result<(VectorChunkHeader, Vec<u8>)> {
    if !bytes.starts_with(CHUNK_MAGIC) {
        return Err(PrimadbError::Message(
            "vector chunk has an invalid header".to_owned(),
        ));
    }
    let offset = CHUNK_MAGIC.len();
    if bytes.len() < offset + 4 {
        return Err(PrimadbError::Message(
            "vector chunk is shorter than its header length".to_owned(),
        ));
    }
    let header_len = u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ]) as usize;
    let header_start = offset + 4;
    let header_end = header_start + header_len;
    if bytes.len() < header_end {
        return Err(PrimadbError::Message(
            "vector chunk header length exceeds payload length".to_owned(),
        ));
    }
    let header = serde_json::from_slice(&bytes[header_start..header_end])?;
    Ok((header, bytes[header_end..].to_vec()))
}

pub fn records_source_hash(entries: &[RecordEntry]) -> String {
    let mut hasher = blake3::Hasher::new();
    let mut sorted = entries.to_vec();
    sorted.sort_by(|left, right| left.key.cmp(&right.key));
    for entry in sorted {
        hasher.update(entry.key.as_bytes());
        hasher.update(&[0]);
        if let Ok(bytes) = serde_json::to_vec(&entry.value) {
            hasher.update(&bytes);
        }
        hasher.update(&[0xff]);
    }
    format!("blake3:{}", hasher.finalize().to_hex())
}

pub(crate) fn exact_search(
    cache: &VectorCollectionCache,
    query: &[f32],
    spec: &VectorSearchSpec,
) -> Result<VectorSearchResult> {
    validate_vector(query, cache.config.dim)?;
    validate_search_spec(spec)?;

    let filter = prepare_vector_filter(spec.filter.as_ref());
    let mut candidates = BinaryHeap::with_capacity(spec.limit.min(cache.entries.len()));
    for (id, entry) in &cache.entries {
        if !vector_filter_matches(id, entry.metadata.as_ref(), &filter) {
            continue;
        }
        let candidate = ExactCandidate {
            id,
            entry,
            distance: vector_distance(cache.config.metric, query, &entry.vector),
        };
        if candidates.len() < spec.limit {
            candidates.push(candidate);
        } else if candidate
            < *candidates
                .peek()
                .expect("a full top-k heap must have a root")
        {
            candidates.pop();
            candidates.push(candidate);
        }
    }

    let mut candidates = candidates.into_vec();
    candidates.sort_unstable_by(|left, right| {
        left.distance
            .total_cmp(&right.distance)
            .then_with(|| left.id.cmp(right.id))
    });
    let matches = candidates
        .into_iter()
        .map(|candidate| VectorMatch {
            id: candidate.id.to_owned(),
            distance: candidate.distance,
            metadata: spec
                .include_metadata
                .then(|| candidate.entry.metadata.clone())
                .flatten(),
            vector: spec.include_vector.then(|| candidate.entry.vector.clone()),
        })
        .collect();

    let stale = cache.state != VectorManagerState::Ready;
    Ok(VectorSearchResult {
        matches,
        exact: true,
        backend: VectorBackendKind::Exact,
        state: cache.state,
        stale,
        approximate_reason: None,
    })
}

pub fn vector_distance(metric: VectorMetric, query: &[f32], vector: &[f32]) -> f32 {
    match metric {
        VectorMetric::L2 => query
            .iter()
            .zip(vector)
            .map(|(left, right)| {
                let delta = left - right;
                delta * delta
            })
            .sum::<f32>()
            .sqrt(),
        VectorMetric::Dot => -query
            .iter()
            .zip(vector)
            .map(|(left, right)| left * right)
            .sum::<f32>(),
        VectorMetric::Cosine => {
            let mut dot = 0.0_f32;
            let mut left_norm = 0.0_f32;
            let mut right_norm = 0.0_f32;
            for (left, right) in query.iter().zip(vector) {
                dot += left * right;
                left_norm += left * left;
                right_norm += right * right;
            }
            if left_norm == 0.0 || right_norm == 0.0 {
                return 1.0;
            }
            1.0 - dot / (left_norm.sqrt() * right_norm.sqrt())
        }
    }
}

fn prepare_vector_filter(filter: Option<&VectorFilter>) -> PreparedVectorFilter<'_> {
    PreparedVectorFilter {
        filter,
        ids: filter
            .filter(|filter| !filter.ids.is_empty())
            .map(|filter| filter.ids.iter().map(String::as_str).collect()),
    }
}

fn vector_filter_matches(
    id: &str,
    metadata: Option<&JsonValue>,
    prepared: &PreparedVectorFilter<'_>,
) -> bool {
    let Some(filter) = prepared.filter else {
        return true;
    };
    if let Some(prefix) = &filter.id_prefix
        && !id.starts_with(prefix)
    {
        return false;
    }
    if let Some(ids) = &prepared.ids {
        if !ids.contains(id) {
            return false;
        }
    }
    let Some(metadata_filter) = &filter.metadata else {
        return true;
    };
    let Some(JsonValue::Object(object)) = metadata else {
        return false;
    };
    for (key, expected) in &metadata_filter.eq {
        if object.get(key) != Some(expected) {
            return false;
        }
    }
    for (key, prefix) in &metadata_filter.prefix {
        match object.get(key) {
            Some(JsonValue::String(value)) if value.starts_with(prefix) => {}
            _ => return false,
        }
    }
    metadata_filter
        .exists
        .iter()
        .all(|key| object.contains_key(key))
}

pub(crate) fn item_meta_from_record(entry: &RecordEntry) -> Result<VectorItemMeta> {
    match &entry.value {
        RecordValue::Json(value) => Ok(serde_json::from_value(value.clone())?),
        _ => Err(PrimadbError::Message(format!(
            "vector item meta record `{}` is not JSON",
            entry.key
        ))),
    }
}

pub(crate) fn collection_config_from_record(entry: &RecordEntry) -> Result<VectorCollectionConfig> {
    match &entry.value {
        RecordValue::Json(value) => {
            let config: VectorCollectionConfig = serde_json::from_value(value.clone())?;
            validate_collection_config(&config)?;
            Ok(config)
        }
        _ => Err(PrimadbError::Message(format!(
            "vector collection meta record `{}` is not JSON",
            entry.key
        ))),
    }
}

pub(crate) fn chunk_from_record(entry: &RecordEntry) -> Result<(VectorChunkHeader, Vec<u8>)> {
    match &entry.value {
        RecordValue::Bytes(bytes) => decode_vector_chunk(bytes.as_slice()),
        _ => Err(PrimadbError::Message(format!(
            "vector chunk record `{}` is not bytes",
            entry.key
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::{VectorCollectionCache, exact_search};
    use crate::{VectorCollectionConfig, VectorFilter, VectorMetric, VectorSearchSpec};
    use serde_json::json;

    fn cache_with_entries() -> VectorCollectionCache {
        let mut cache = VectorCollectionCache::empty(VectorCollectionConfig {
            dim: 1,
            metric: VectorMetric::L2,
            backend: None,
            hnsw: None,
            chunking: Default::default(),
        });
        for (id, value, group) in [
            ("alpha", 1.0, "keep"),
            ("beta", 1.0, "keep"),
            ("far", 5.0, "keep"),
            ("zero", 0.0, "drop"),
        ] {
            cache.entries.insert(
                id.to_owned(),
                super::VectorCacheEntry {
                    vector: vec![value],
                    metadata: Some(json!({"group": group})),
                    write_id: String::new(),
                    checksum: String::new(),
                },
            );
        }
        cache
    }

    #[test]
    fn exact_top_k_preserves_distance_and_id_tie_order_with_filters() {
        let cache = cache_with_entries();
        let result = exact_search(
            &cache,
            &[0.0],
            &VectorSearchSpec {
                limit: 2,
                ef: None,
                filter: Some(VectorFilter {
                    ids: vec!["far".to_owned(), "alpha".to_owned(), "beta".to_owned()],
                    ..Default::default()
                }),
                include_vector: true,
                include_metadata: true,
                exact: true,
                stale_policy: Default::default(),
            },
        )
        .unwrap();

        assert_eq!(
            result
                .matches
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            ["alpha", "beta"]
        );
        assert_eq!(result.matches[0].distance, 1.0);
        assert_eq!(result.matches[0].vector, Some(vec![1.0]));
        assert_eq!(
            result.matches[0].metadata.as_ref().unwrap()["group"],
            "keep"
        );
    }

    #[test]
    fn exact_top_k_scans_large_corpus_without_materializing_non_matches() {
        let mut cache = VectorCollectionCache::empty(VectorCollectionConfig {
            dim: 1,
            metric: VectorMetric::L2,
            backend: None,
            hnsw: None,
            chunking: Default::default(),
        });
        for index in 0..20_000 {
            cache.entries.insert(
                format!("item-{index:05}"),
                super::VectorCacheEntry {
                    vector: vec![index as f32 + 1.0],
                    metadata: Some(json!({"index": index})),
                    write_id: String::new(),
                    checksum: String::new(),
                },
            );
        }

        let result = exact_search(
            &cache,
            &[0.0],
            &VectorSearchSpec {
                limit: 3,
                ef: None,
                filter: None,
                include_vector: false,
                include_metadata: false,
                exact: true,
                stale_policy: Default::default(),
            },
        )
        .unwrap();

        assert_eq!(result.matches.len(), 3);
        assert_eq!(result.matches[0].id, "item-00000");
        assert_eq!(result.matches[2].id, "item-00002");
        assert!(
            result
                .matches
                .iter()
                .all(|item| item.metadata.is_none() && item.vector.is_none())
        );
    }
}
