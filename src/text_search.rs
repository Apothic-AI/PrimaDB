use crate::MapEntry;
use crate::engine::decode_component;
use crate::error::{PrimadbError, Result};
use crate::query::QuerySpec;
use crate::record::{RecordEntry, RecordScan, RecordValue};
use crate::sync::RemotePath;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

pub const TEXT_RECORD_PREFIX: &str = "__primadb_text";
#[allow(dead_code)]
pub const TEXT_CACHE_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TextAnalyzerKind {
    #[default]
    Simple,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TextAnalyzerConfig {
    #[serde(default)]
    pub kind: TextAnalyzerKind,
    #[serde(default = "default_true")]
    pub lowercase: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unicode_normalization: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopwords: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stemming: Option<String>,
    #[serde(default = "default_analyzer_version")]
    pub version: u32,
}

impl Default for TextAnalyzerConfig {
    fn default() -> Self {
        Self {
            kind: TextAnalyzerKind::Simple,
            lowercase: true,
            unicode_normalization: None,
            stopwords: None,
            stemming: None,
            version: default_analyzer_version(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TextFieldConfig {
    pub name: String,
    #[serde(default = "default_field_weight")]
    pub weight: f32,
    #[serde(default = "default_true")]
    pub indexed: bool,
    #[serde(default = "default_true")]
    pub stored: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TextCollectionConfig {
    #[serde(default)]
    pub fields: Vec<TextFieldConfig>,
    #[serde(default)]
    pub analyzer: TextAnalyzerConfig,
    #[serde(default = "default_k1")]
    pub k1: f32,
    #[serde(default = "default_b")]
    pub b: f32,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

impl Default for TextCollectionConfig {
    fn default() -> Self {
        Self {
            fields: Vec::new(),
            analyzer: TextAnalyzerConfig::default(),
            k1: default_k1(),
            b: default_b(),
            metadata: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TextDocument {
    pub id: String,
    #[serde(default)]
    pub fields: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SearchStalePolicy {
    Allow,
    #[default]
    Refresh,
    Reject,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TextCandidatePolicy {
    #[default]
    RejectPaginatedQuery,
    AllowPreselectedCandidates,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TextSearchBackend {
    #[default]
    Exact,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TextIndexState {
    #[default]
    Ready,
    Rebuilding,
    Stale,
    Failed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TextScoreScope {
    #[default]
    Collection,
    CandidateSet,
    PeerLocal,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TextSearchMode {
    #[default]
    Ambient,
    LocalOnly,
    RemoteAny,
    FanIn,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TextSearchSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
    #[serde(default)]
    pub include_metadata: bool,
    #[serde(default)]
    pub include_snippets: bool,
    #[serde(default)]
    pub explain: bool,
    #[serde(default = "default_true")]
    pub exact: bool,
    #[serde(default)]
    pub stale_policy: SearchStalePolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_limit: Option<usize>,
    #[serde(default)]
    pub candidate_policy: TextCandidatePolicy,
}

impl Default for TextSearchSpec {
    fn default() -> Self {
        Self {
            limit: Some(10),
            offset: None,
            fields: None,
            include_metadata: false,
            include_snippets: false,
            explain: false,
            exact: true,
            stale_policy: SearchStalePolicy::Refresh,
            candidate_limit: None,
            candidate_policy: TextCandidatePolicy::RejectPaginatedQuery,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TextSearchSource {
    Collection {
        collection: String,
    },
    #[serde(rename = "query")]
    GraphQuery {
        path: RemotePath,
        spec: QuerySpec,
    },
    Records {
        scan: RecordScan,
    },
}

impl TextSearchSource {
    pub fn collection(collection: impl Into<String>) -> Self {
        Self::Collection {
            collection: collection.into(),
        }
    }
}

impl From<&str> for TextSearchSource {
    fn from(value: &str) -> Self {
        Self::collection(value)
    }
}

impl From<String> for TextSearchSource {
    fn from(value: String) -> Self {
        Self::collection(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TextSearchSourceSummary {
    Collection {
        collection: String,
    },
    #[serde(rename = "query")]
    GraphQuery {
        path: RemotePath,
    },
    Records {
        prefix: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TextFieldHit {
    pub field: String,
    pub terms: Vec<String>,
    pub score: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TextSnippet {
    pub field: String,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TextSearchMatch {
    pub id: String,
    pub score: f32,
    #[serde(default)]
    pub field_hits: Vec<TextFieldHit>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<BTreeMap<String, JsonValue>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snippets: Option<Vec<TextSnippet>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explanation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TextSearchResult {
    pub source: TextSearchSourceSummary,
    pub query: String,
    pub matches: Vec<TextSearchMatch>,
    pub backend: TextSearchBackend,
    pub exact: bool,
    pub stale: bool,
    pub candidate_count: usize,
    pub searched_count: usize,
    pub truncated_candidates: bool,
    pub score_scope: TextScoreScope,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TextIndexStats {
    pub document_count: usize,
    pub deleted_count: usize,
    pub term_count: usize,
    pub total_terms: usize,
    pub average_field_length: usize,
    pub state: TextIndexState,
    pub source_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TextCacheManifest {
    pub collection: String,
    pub document_count: usize,
    pub term_count: usize,
    pub record_prefix: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<u64>,
    pub source_hash: String,
    pub source_hash_mode: String,
    pub backend: TextSearchBackend,
    pub backend_version: String,
    pub cache_format_version: u32,
    pub manager_state: TextIndexState,
    pub analyzer_version: u32,
    pub config_hash: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TextCacheFiles {
    pub manifest: TextCacheManifest,
    pub terms_bin: Vec<u8>,
    pub postings_bin: Vec<u8>,
    pub docs_bin: Vec<u8>,
    pub metadata_bin: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TextCollectionCache {
    pub config: TextCollectionConfig,
    pub documents: BTreeMap<String, TextDocument>,
    pub deleted_count: usize,
    pub state: TextIndexState,
    pub dirty: bool,
    pub source_hash: String,
    index: TextIndex,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
struct TextIndex {
    postings: BTreeMap<String, BTreeMap<String, BTreeMap<String, usize>>>,
    doc_field_lengths: BTreeMap<String, BTreeMap<String, usize>>,
    doc_lengths: BTreeMap<String, usize>,
    doc_terms: BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
    term_doc_freq: BTreeMap<String, usize>,
    total_terms: usize,
    #[serde(skip)]
    dense_doc_ids: Vec<String>,
    #[serde(skip)]
    dense_doc_lengths: Vec<usize>,
    #[serde(skip)]
    dense_field_names: Vec<String>,
    #[serde(skip)]
    dense_postings: BTreeMap<String, Vec<DensePosting>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DensePosting {
    doc_index: usize,
    field_index: usize,
    term_frequency: usize,
}

#[derive(Debug, Clone, Copy)]
struct CandidatePosting {
    doc_index: usize,
    field_index: usize,
    term_frequency: usize,
}

struct TextMatchDocument<'a> {
    id: &'a str,
    fields: &'a BTreeMap<String, String>,
    metadata: &'a BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TextCandidate {
    pub id: String,
    pub fields: BTreeMap<String, String>,
    pub metadata: BTreeMap<String, JsonValue>,
}

impl TextCollectionCache {
    pub fn from_documents(
        config: TextCollectionConfig,
        documents: BTreeMap<String, TextDocument>,
        source_hash: String,
    ) -> Result<Self> {
        validate_text_collection_config(&config)?;
        let mut cache = Self {
            config,
            documents,
            deleted_count: 0,
            state: TextIndexState::Ready,
            dirty: false,
            source_hash,
            index: TextIndex::default(),
        };
        rebuild_text_index(&mut cache)?;
        Ok(cache)
    }

    pub fn stats(&self) -> TextIndexStats {
        let average_field_length = if self.index.doc_lengths.is_empty() {
            0
        } else {
            self.index.total_terms / self.index.doc_lengths.len()
        };
        TextIndexStats {
            document_count: self.documents.len(),
            deleted_count: self.deleted_count,
            term_count: self.index.term_doc_freq.len(),
            total_terms: self.index.total_terms,
            average_field_length,
            state: self.state,
            source_hash: self.source_hash.clone(),
        }
    }
}

pub fn validate_text_collection_config(config: &TextCollectionConfig) -> Result<()> {
    if !(config.k1.is_finite() && config.k1 > 0.0) {
        return Err(PrimadbError::Message(
            "text collection k1 must be finite and greater than zero".to_owned(),
        ));
    }
    if !(config.b.is_finite() && (0.0..=1.0).contains(&config.b)) {
        return Err(PrimadbError::Message(
            "text collection b must be finite and between 0 and 1".to_owned(),
        ));
    }
    for field in &config.fields {
        if field.name.trim().is_empty() {
            return Err(PrimadbError::Message(
                "text collection field names must not be empty".to_owned(),
            ));
        }
        if !(field.weight.is_finite() && field.weight >= 0.0) {
            return Err(PrimadbError::Message(format!(
                "text field `{}` weight must be finite and non-negative",
                field.name
            )));
        }
    }
    if config.analyzer.version == 0 {
        return Err(PrimadbError::Message(
            "text analyzer version must be greater than zero".to_owned(),
        ));
    }
    Ok(())
}

pub fn analyze_text(config: &TextAnalyzerConfig, input: &str) -> Vec<String> {
    let text = if config.lowercase {
        input.to_lowercase()
    } else {
        input.to_owned()
    };
    let mut tokens = Vec::new();
    let mut current = String::new();
    for ch in text.chars() {
        if ch.is_alphanumeric() || ch == '_' {
            current.push(ch);
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

pub(crate) fn rebuild_text_index(cache: &mut TextCollectionCache) -> Result<()> {
    validate_text_collection_config(&cache.config)?;
    let mut index = TextIndex::default();
    for document in cache.documents.values() {
        let field_names = indexed_field_names(&cache.config, document);
        let mut doc_term_union = BTreeSet::new();
        for field_name in field_names {
            let Some(text) = document.fields.get(&field_name) else {
                continue;
            };
            let tokens = analyze_text(&cache.config.analyzer, text);
            if tokens.is_empty() {
                continue;
            }
            index
                .doc_field_lengths
                .entry(document.id.clone())
                .or_default()
                .insert(field_name.clone(), tokens.len());
            *index.doc_lengths.entry(document.id.clone()).or_default() += tokens.len();
            index.total_terms += tokens.len();

            let mut field_terms = BTreeSet::new();
            for token in tokens {
                field_terms.insert(token.clone());
                doc_term_union.insert(token.clone());
                *index
                    .postings
                    .entry(token)
                    .or_default()
                    .entry(document.id.clone())
                    .or_default()
                    .entry(field_name.clone())
                    .or_default() += 1;
            }
            index
                .doc_terms
                .entry(document.id.clone())
                .or_default()
                .insert(field_name, field_terms);
        }
        for term in doc_term_union {
            *index.term_doc_freq.entry(term).or_default() += 1;
        }
    }
    cache.index = index;
    rebuild_dense_text_index(&mut cache.index);
    cache.state = TextIndexState::Ready;
    cache.dirty = false;
    Ok(())
}

pub(crate) fn search_text_collection(
    collection: &str,
    cache: &TextCollectionCache,
    query: &str,
    spec: &TextSearchSpec,
) -> Result<TextSearchResult> {
    validate_text_search_spec(spec)?;
    let query_terms = analyze_text(&cache.config.analyzer, query);
    let mut matches = score_index(cache, &query_terms, spec, TextScoreScope::Collection);
    let searched_count = cache.index.doc_lengths.len();
    paginate_matches(&mut matches, spec);
    Ok(TextSearchResult {
        source: TextSearchSourceSummary::Collection {
            collection: collection.to_owned(),
        },
        query: query.to_owned(),
        matches,
        backend: TextSearchBackend::Exact,
        exact: true,
        stale: cache.state != TextIndexState::Ready || cache.dirty,
        candidate_count: cache.documents.len(),
        searched_count,
        truncated_candidates: false,
        score_scope: TextScoreScope::Collection,
    })
}

pub(crate) fn search_text_candidates(
    source: TextSearchSourceSummary,
    query: &str,
    spec: &TextSearchSpec,
    candidates: Vec<TextCandidate>,
    truncated_candidates: bool,
    score_scope: TextScoreScope,
) -> Result<TextSearchResult> {
    validate_text_search_spec(spec)?;
    let candidate_count = candidates.len();
    let candidates = candidates
        .into_iter()
        .take(spec.candidate_limit.unwrap_or(usize::MAX))
        .collect::<Vec<_>>();
    let mut unique_candidates = Vec::with_capacity(candidates.len());
    let mut candidate_positions = BTreeMap::new();
    for candidate in candidates {
        if let Some(position) = candidate_positions.get(&candidate.id).copied() {
            unique_candidates[position] = candidate;
        } else {
            candidate_positions.insert(candidate.id.clone(), unique_candidates.len());
            unique_candidates.push(candidate);
        }
    }
    let candidates = unique_candidates;
    // Keep the dense order identical to the old BTreeMap scorer for stable
    // floating-point accumulation and deterministic equal-score selection.
    let mut candidates = candidates;
    candidates.sort_unstable_by(|left, right| left.id.cmp(&right.id));
    let effective_truncation = truncated_candidates
        || spec
            .candidate_limit
            .is_some_and(|limit| candidate_count > limit);
    let config = fields_for_candidate_documents(&candidates);
    let query_terms = analyze_text(&config.analyzer, query);
    let (mut matches, searched_count) =
        score_one_shot_candidates(&candidates, &config, &query_terms, spec, score_scope);
    paginate_matches(&mut matches, spec);
    Ok(TextSearchResult {
        source,
        query: query.to_owned(),
        matches,
        backend: TextSearchBackend::Exact,
        exact: true,
        stale: false,
        candidate_count,
        searched_count,
        truncated_candidates: effective_truncation,
        score_scope,
    })
}

pub(crate) fn text_candidates_from_map_entries(
    entries: &[MapEntry],
    fields: Option<&[String]>,
) -> Vec<TextCandidate> {
    entries
        .iter()
        .filter_map(|entry| {
            let extracted = extract_json_text_fields(&entry.value, fields);
            (!extracted.is_empty()).then(|| TextCandidate {
                id: entry.key.clone(),
                fields: extracted,
                metadata: BTreeMap::new(),
            })
        })
        .collect()
}

pub(crate) fn text_candidates_from_record_entries(
    entries: &[RecordEntry],
    fields: Option<&[String]>,
) -> Vec<TextCandidate> {
    entries
        .iter()
        .filter_map(|entry| match &entry.value {
            RecordValue::Json(value) => {
                let extracted = extract_json_text_fields(value, fields);
                (!extracted.is_empty()).then(|| TextCandidate {
                    id: entry.key.clone(),
                    fields: extracted,
                    metadata: BTreeMap::new(),
                })
            }
            RecordValue::Bytes(_) | RecordValue::Blob(_) => None,
        })
        .collect()
}

pub fn validate_text_search_spec(spec: &TextSearchSpec) -> Result<()> {
    if spec.limit == Some(0) {
        return Err(PrimadbError::Message(
            "text search limit must be greater than zero".to_owned(),
        ));
    }
    if spec.candidate_limit == Some(0) {
        return Err(PrimadbError::Message(
            "text search candidateLimit must be greater than zero".to_owned(),
        ));
    }
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn text_cache_files(
    collection: &str,
    cache: &TextCollectionCache,
    created_at: String,
) -> Result<TextCacheFiles> {
    let manifest = TextCacheManifest {
        collection: collection.to_owned(),
        document_count: cache.documents.len(),
        term_count: cache.index.term_doc_freq.len(),
        record_prefix: text_collection_docs_prefix(collection),
        source_revision: None,
        source_hash: cache.source_hash.clone(),
        source_hash_mode: "rebuild_scan".to_owned(),
        backend: TextSearchBackend::Exact,
        backend_version: text_backend_version(TextSearchBackend::Exact),
        cache_format_version: TEXT_CACHE_FORMAT_VERSION,
        manager_state: cache.state,
        analyzer_version: cache.config.analyzer.version,
        config_hash: stable_json_hash(&cache.config)?,
        created_at,
    };
    Ok(TextCacheFiles {
        manifest,
        terms_bin: serde_json::to_vec(&cache.index.term_doc_freq)?,
        postings_bin: serde_json::to_vec(&cache.index.postings)?,
        docs_bin: serde_json::to_vec(&cache.documents)?,
        metadata_bin: serde_json::to_vec(&cache.config)?,
    })
}

#[allow(dead_code)]
pub(crate) fn collection_cache_from_text_cache_files(
    config: TextCollectionConfig,
    files: TextCacheFiles,
    expected_source_hash: &str,
) -> Result<TextCollectionCache> {
    let manifest = &files.manifest;
    if manifest.cache_format_version != TEXT_CACHE_FORMAT_VERSION {
        return Err(PrimadbError::Message(format!(
            "text cache format mismatch: expected {}, got {}",
            TEXT_CACHE_FORMAT_VERSION, manifest.cache_format_version
        )));
    }
    if manifest.backend_version != text_backend_version(manifest.backend) {
        return Err(PrimadbError::Message(
            "text cache backend version does not match this build".to_owned(),
        ));
    }
    if manifest.manager_state != TextIndexState::Ready {
        return Err(PrimadbError::Message(
            "text cache manifest is not ready for restoration".to_owned(),
        ));
    }
    if manifest.source_hash_mode != "rebuild_scan" || manifest.created_at.is_empty() {
        return Err(PrimadbError::Message(
            "text cache manifest has invalid source metadata".to_owned(),
        ));
    }
    if manifest.collection.trim().is_empty()
        || manifest.record_prefix != text_collection_docs_prefix(&manifest.collection)
    {
        return Err(PrimadbError::Message(
            "text cache manifest has an invalid collection record prefix".to_owned(),
        ));
    }
    if manifest.source_hash != expected_source_hash {
        return Err(PrimadbError::Message(
            "text cache source hash does not match authoritative records".to_owned(),
        ));
    }
    let serialized_config: TextCollectionConfig = serde_json::from_slice(&files.metadata_bin)?;
    validate_text_collection_config(&serialized_config)?;
    if serialized_config != config
        || manifest.analyzer_version != config.analyzer.version
        || manifest.config_hash != stable_json_hash(&config)?
    {
        return Err(PrimadbError::Message(
            "text cache manifest does not match collection config".to_owned(),
        ));
    }
    let documents: BTreeMap<String, TextDocument> = serde_json::from_slice(&files.docs_bin)?;
    if documents.len() != manifest.document_count {
        return Err(PrimadbError::Message(
            "text cache document table has inconsistent length".to_owned(),
        ));
    }
    for (id, document) in &documents {
        if id != &document.id || document.id.trim().is_empty() {
            return Err(PrimadbError::Message(format!(
                "text cache document table has inconsistent id `{id}`"
            )));
        }
    }

    let term_doc_freq: BTreeMap<String, usize> = serde_json::from_slice(&files.terms_bin)?;
    if term_doc_freq.len() != manifest.term_count {
        return Err(PrimadbError::Message(
            "text cache term table has inconsistent length".to_owned(),
        ));
    }
    let postings: BTreeMap<String, BTreeMap<String, BTreeMap<String, usize>>> =
        serde_json::from_slice(&files.postings_bin)?;
    if postings.len() != manifest.term_count || postings.len() != term_doc_freq.len() {
        return Err(PrimadbError::Message(
            "text cache postings and term tables have inconsistent lengths".to_owned(),
        ));
    }
    if postings.keys().ne(term_doc_freq.keys()) {
        return Err(PrimadbError::Message(
            "text cache postings and term tables contain different terms".to_owned(),
        ));
    }

    let mut index = TextIndex {
        postings,
        term_doc_freq,
        ..TextIndex::default()
    };
    for (term, document_postings) in &index.postings {
        if term.is_empty() || document_postings.is_empty() {
            return Err(PrimadbError::Message(
                "text cache postings contain an empty term bucket".to_owned(),
            ));
        }
        let analyzed_term = analyze_text(&config.analyzer, term);
        if analyzed_term.len() != 1 || analyzed_term[0] != *term {
            return Err(PrimadbError::Message(format!(
                "text cache contains an invalid analyzed term `{term}`"
            )));
        }
        let Some(expected_doc_freq) = index.term_doc_freq.get(term) else {
            return Err(PrimadbError::Message(format!(
                "text cache postings contain term `{term}` missing from term table"
            )));
        };
        if *expected_doc_freq == 0 || *expected_doc_freq != document_postings.len() {
            return Err(PrimadbError::Message(format!(
                "text cache document frequency is inconsistent for term `{term}`"
            )));
        }
        for (doc_id, field_postings) in document_postings {
            if field_postings.is_empty() {
                return Err(PrimadbError::Message(format!(
                    "text cache postings contain an empty field bucket for document `{doc_id}`"
                )));
            }
            let Some(document) = documents.get(doc_id) else {
                return Err(PrimadbError::Message(format!(
                    "text cache postings reference unknown document `{doc_id}`"
                )));
            };
            for (field, term_frequency) in field_postings {
                if *term_frequency == 0 {
                    return Err(PrimadbError::Message(format!(
                        "text cache posting has zero frequency for term `{term}` in document `{doc_id}`"
                    )));
                }
                if !document.fields.contains_key(field)
                    || !indexed_field_names(&config, document)
                        .iter()
                        .any(|name| name == field)
                {
                    return Err(PrimadbError::Message(format!(
                        "text cache posting references non-indexed field `{field}` in document `{doc_id}`"
                    )));
                }
                index
                    .doc_field_lengths
                    .entry(doc_id.clone())
                    .or_default()
                    .entry(field.clone())
                    .and_modify(|length| *length += *term_frequency)
                    .or_insert(*term_frequency);
                *index.doc_lengths.entry(doc_id.clone()).or_default() += *term_frequency;
                index
                    .doc_terms
                    .entry(doc_id.clone())
                    .or_default()
                    .entry(field.clone())
                    .or_default()
                    .insert(term.clone());
                index.total_terms += *term_frequency;
            }
        }
    }
    rebuild_dense_text_index(&mut index);

    Ok(TextCollectionCache {
        config,
        documents,
        deleted_count: 0,
        state: TextIndexState::Ready,
        dirty: false,
        source_hash: manifest.source_hash.clone(),
        index,
    })
}

#[allow(dead_code)]
pub fn text_backend_version(kind: TextSearchBackend) -> String {
    match kind {
        TextSearchBackend::Exact => "primadb-bm25-exact-v1".to_owned(),
    }
}

pub(crate) fn collection_config_from_record(entry: &RecordEntry) -> Result<TextCollectionConfig> {
    match &entry.value {
        RecordValue::Json(value) => {
            let config: TextCollectionConfig = serde_json::from_value(value.clone())?;
            validate_text_collection_config(&config)?;
            Ok(config)
        }
        _ => Err(PrimadbError::Message(format!(
            "text collection config record `{}` is not JSON",
            entry.key
        ))),
    }
}

pub(crate) fn text_document_from_record(entry: &RecordEntry) -> Result<TextDocument> {
    match &entry.value {
        RecordValue::Json(value) => Ok(serde_json::from_value(value.clone())?),
        _ => Err(PrimadbError::Message(format!(
            "text document record `{}` is not JSON",
            entry.key
        ))),
    }
}

pub fn text_collection_config_key(collection: &str) -> String {
    format!(
        "{}/{}/config",
        TEXT_RECORD_PREFIX,
        crate::encode_component(collection)
    )
}

pub fn text_collection_docs_prefix(collection: &str) -> String {
    format!(
        "{}/{}/docs/",
        TEXT_RECORD_PREFIX,
        crate::encode_component(collection)
    )
}

pub fn text_document_key(collection: &str, id: &str) -> String {
    format!(
        "{}{}",
        text_collection_docs_prefix(collection),
        crate::encode_component(id)
    )
}

pub fn text_collection_from_record_key(key: &str) -> Option<String> {
    let suffix = key.strip_prefix(TEXT_RECORD_PREFIX)?.strip_prefix('/')?;
    let encoded = suffix.split('/').next()?;
    decode_component(encoded).ok()
}

pub fn text_document_id_from_record_key(collection: &str, key: &str) -> Option<String> {
    let suffix = key.strip_prefix(&text_collection_docs_prefix(collection))?;
    let encoded = suffix.split('/').next()?;
    decode_component(encoded).ok()
}

#[allow(dead_code)]
pub fn stable_json_hash(value: impl Serialize) -> Result<String> {
    Ok(format!(
        "blake3:{}",
        blake3::hash(&serde_json::to_vec(&value)?).to_hex()
    ))
}

fn rebuild_dense_text_index(index: &mut TextIndex) {
    index.dense_doc_ids = index.doc_lengths.keys().cloned().collect();
    index.dense_doc_lengths = index.doc_lengths.values().copied().collect();
    index.dense_field_names = index
        .postings
        .values()
        .flat_map(|documents| documents.values())
        .flat_map(|fields| fields.keys())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    let doc_positions = index
        .dense_doc_ids
        .iter()
        .enumerate()
        .map(|(position, id)| (id.as_str(), position))
        .collect::<BTreeMap<_, _>>();
    let field_positions = index
        .dense_field_names
        .iter()
        .enumerate()
        .map(|(position, name)| (name.as_str(), position))
        .collect::<BTreeMap<_, _>>();
    index.dense_postings = index
        .postings
        .iter()
        .map(|(term, documents)| {
            let postings = documents
                .iter()
                .flat_map(|(doc_id, fields)| {
                    fields.iter().filter_map(|(field, term_frequency)| {
                        Some(DensePosting {
                            doc_index: *doc_positions.get(doc_id.as_str())?,
                            field_index: *field_positions.get(field.as_str())?,
                            term_frequency: *term_frequency,
                        })
                    })
                })
                .collect();
            (term.clone(), postings)
        })
        .collect();
}

fn score_index(
    cache: &TextCollectionCache,
    query_terms: &[String],
    spec: &TextSearchSpec,
    _scope: TextScoreScope,
) -> Vec<TextSearchMatch> {
    if query_terms.is_empty() || cache.index.dense_doc_ids.is_empty() {
        return Vec::new();
    }
    let query_terms = query_terms
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let doc_count = cache.index.dense_doc_ids.len() as f32;
    let avg_doc_len = cache.index.total_terms as f32 / doc_count.max(1.0);
    let field_weights = field_weights(&cache.config);
    let selected_fields = spec
        .fields
        .as_ref()
        .map(|fields| fields.iter().cloned().collect::<BTreeSet<_>>());
    let mut scores = BTreeMap::new();

    for term in query_terms {
        let Some(postings) = cache.index.dense_postings.get(term) else {
            continue;
        };
        let df = *cache.index.term_doc_freq.get(term).unwrap_or(&0) as f32;
        if df <= 0.0 {
            continue;
        }
        let idf = ((doc_count - df + 0.5) / (df + 0.5) + 1.0).ln();
        for posting in postings {
            let field = &cache.index.dense_field_names[posting.field_index];
            if selected_fields
                .as_ref()
                .is_some_and(|fields| !fields.contains(field))
            {
                continue;
            }
            let doc_len = cache.index.dense_doc_lengths[posting.doc_index] as f32;
            if doc_len <= 0.0 {
                continue;
            }
            let tf = posting.term_frequency as f32;
            let weight = field_weights.get(field).copied().unwrap_or(1.0);
            if weight == 0.0 {
                continue;
            }
            let numerator = tf * (cache.config.k1 + 1.0);
            let denominator = tf
                + cache.config.k1 * (1.0 - cache.config.b + cache.config.b * doc_len / avg_doc_len);
            let score = idf * numerator / denominator * weight;
            let (total, hits) = scores
                .entry(posting.doc_index)
                .or_insert_with(|| (0.0, BTreeMap::new()));
            *total += score;
            let hit = hits.entry(field.clone()).or_insert_with(|| TextFieldHit {
                field: field.clone(),
                terms: Vec::new(),
                score: 0.0,
            });
            hit.terms.push(term.to_owned());
            hit.score += score;
        }
    }

    let documents = scores
        .keys()
        .filter_map(|&index| {
            let id = cache.index.dense_doc_ids.get(index)?;
            let document = cache
                .documents
                .get(id)
                .expect("dense text index document must be present");
            Some((
                index,
                TextMatchDocument {
                    id: document.id.as_str(),
                    fields: &document.fields,
                    metadata: &document.metadata,
                },
            ))
        })
        .collect::<BTreeMap<_, _>>();
    build_top_matches_dense(scores, &documents, spec)
}

fn score_one_shot_candidates(
    documents: &[TextCandidate],
    config: &TextCollectionConfig,
    query_terms: &[String],
    spec: &TextSearchSpec,
    _scope: TextScoreScope,
) -> (Vec<TextSearchMatch>, usize) {
    if documents.is_empty() {
        return (Vec::new(), 0);
    }

    let query_terms = query_terms
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let field_names = documents
        .iter()
        .flat_map(|document| document.fields.keys())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let field_positions = field_names
        .iter()
        .enumerate()
        .map(|(position, name)| (name.as_str(), position))
        .collect::<BTreeMap<_, _>>();
    let mut document_lengths = vec![0_usize; documents.len()];
    let mut term_doc_freq = BTreeMap::new();
    let mut postings = BTreeMap::<String, Vec<CandidatePosting>>::new();
    let mut total_terms = 0;
    let mut searched_count = 0;

    for (doc_index, document) in documents.iter().enumerate() {
        let mut matching_terms = BTreeSet::new();
        for field_name in indexed_field_names_for_fields(config, &document.fields) {
            let Some(text) = document.fields.get(&field_name) else {
                continue;
            };
            let tokens = analyze_text(&config.analyzer, text);
            document_lengths[doc_index] += tokens.len();
            let mut term_frequencies = BTreeMap::<String, usize>::new();
            for token in tokens {
                if query_terms.contains(token.as_str()) {
                    *term_frequencies.entry(token).or_default() += 1;
                }
            }
            let Some(&field_index) = field_positions.get(field_name.as_str()) else {
                continue;
            };
            for (term, term_frequency) in term_frequencies {
                matching_terms.insert(term.clone());
                postings.entry(term).or_default().push(CandidatePosting {
                    doc_index,
                    field_index,
                    term_frequency,
                });
            }
        }
        if document_lengths[doc_index] == 0 {
            continue;
        }
        searched_count += 1;
        total_terms += document_lengths[doc_index];
        for term in matching_terms {
            *term_doc_freq.entry(term).or_default() += 1;
        }
    }

    if searched_count == 0 {
        return (Vec::new(), 0);
    }
    let doc_count = searched_count as f32;
    let avg_doc_len = total_terms as f32 / doc_count.max(1.0);
    let field_weights = field_weights(config);
    let selected_fields = spec
        .fields
        .as_ref()
        .map(|fields| fields.iter().cloned().collect::<BTreeSet<_>>());
    let mut scores = BTreeMap::new();

    for term in query_terms {
        let df = *term_doc_freq.get(term).unwrap_or(&0) as f32;
        if df <= 0.0 {
            continue;
        }
        let idf = ((doc_count - df + 0.5) / (df + 0.5) + 1.0).ln();
        let Some(term_postings) = postings.get(term) else {
            continue;
        };
        for posting in term_postings {
            let field = &field_names[posting.field_index];
            if selected_fields
                .as_ref()
                .is_some_and(|fields| !fields.contains(field))
            {
                continue;
            }
            let doc_len = document_lengths[posting.doc_index] as f32;
            let tf = posting.term_frequency as f32;
            let weight = field_weights.get(field).copied().unwrap_or(1.0);
            if weight == 0.0 {
                continue;
            }
            let numerator = tf * (config.k1 + 1.0);
            let denominator = tf + config.k1 * (1.0 - config.b + config.b * doc_len / avg_doc_len);
            let score = idf * numerator / denominator * weight;
            let (total, hits) = scores
                .entry(posting.doc_index)
                .or_insert_with(|| (0.0, BTreeMap::new()));
            *total += score;
            let hit = hits.entry(field.clone()).or_insert_with(|| TextFieldHit {
                field: field.clone(),
                terms: Vec::new(),
                score: 0.0,
            });
            hit.terms.push(term.to_owned());
            hit.score += score;
        }
    }

    let match_documents = scores
        .keys()
        .filter_map(|&index| {
            let document = documents.get(index)?;
            Some((
                index,
                TextMatchDocument {
                    id: document.id.as_str(),
                    fields: &document.fields,
                    metadata: &document.metadata,
                },
            ))
        })
        .collect::<BTreeMap<_, _>>();
    (
        build_top_matches_dense(scores, &match_documents, spec),
        searched_count,
    )
}

fn build_top_matches_dense(
    scores: BTreeMap<usize, (f32, BTreeMap<String, TextFieldHit>)>,
    documents: &BTreeMap<usize, TextMatchDocument<'_>>,
    spec: &TextSearchSpec,
) -> Vec<TextSearchMatch> {
    let max_matches = spec
        .limit
        .map(|limit| spec.offset.unwrap_or(0).saturating_add(limit))
        .unwrap_or(usize::MAX);
    let selected = select_top_score_indices(&scores, max_matches);
    let mut matches = scores
        .into_iter()
        .filter_map(|(index, score)| {
            let (score, field_hits) = score;
            if !selected.contains(&index) || score <= 0.0 {
                return None;
            }
            let document = documents.get(&index)?;
            Some(TextSearchMatch {
                id: document.id.to_owned(),
                score,
                field_hits: field_hits.into_values().collect(),
                metadata: spec.include_metadata.then(|| document.metadata.clone()),
                snippets: spec
                    .include_snippets
                    .then(|| snippets_for_fields(document.fields, spec)),
                explanation: spec.explain.then(|| {
                    "exact BM25 over PrimaDB text index; scores are scoped by result scoreScope"
                        .to_owned()
                }),
            })
        })
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.id.cmp(&right.id))
    });
    matches
}

#[derive(Debug, PartialEq)]
struct DenseWorstScore {
    index: usize,
    score: f32,
}

impl Eq for DenseWorstScore {}

impl Ord for DenseWorstScore {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .score
            .total_cmp(&self.score)
            .then_with(|| self.index.cmp(&other.index))
    }
}

impl PartialOrd for DenseWorstScore {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn select_top_score_indices(
    scores: &BTreeMap<usize, (f32, BTreeMap<String, TextFieldHit>)>,
    max_matches: usize,
) -> BTreeSet<usize> {
    if max_matches == usize::MAX {
        return scores.keys().copied().collect();
    }
    let mut heap = BinaryHeap::new();
    for (&index, (score, _)) in scores {
        if *score <= 0.0 {
            continue;
        }
        let candidate = DenseWorstScore {
            index,
            score: *score,
        };
        if heap.len() < max_matches {
            heap.push(candidate);
        } else if candidate < *heap.peek().expect("non-empty bounded heap") {
            heap.pop();
            heap.push(candidate);
        }
    }
    let mut indices = BTreeSet::new();
    for entry in heap {
        indices.insert(entry.index);
    }
    indices
}

fn paginate_matches(matches: &mut Vec<TextSearchMatch>, spec: &TextSearchSpec) {
    let offset = spec.offset.unwrap_or(0);
    let limit = spec.limit.unwrap_or(usize::MAX);
    if offset > 0 {
        if offset >= matches.len() {
            matches.clear();
        } else {
            matches.drain(0..offset);
        }
    }
    matches.truncate(limit);
}

fn indexed_field_names(config: &TextCollectionConfig, document: &TextDocument) -> Vec<String> {
    indexed_field_names_for_fields(config, &document.fields)
}

fn indexed_field_names_for_fields(
    config: &TextCollectionConfig,
    fields: &BTreeMap<String, String>,
) -> Vec<String> {
    if config.fields.is_empty() {
        return fields.keys().cloned().collect();
    }
    config
        .fields
        .iter()
        .filter(|field| field.indexed)
        .filter(|field| fields.contains_key(&field.name))
        .map(|field| field.name.clone())
        .collect()
}

fn field_weights(config: &TextCollectionConfig) -> BTreeMap<String, f32> {
    config
        .fields
        .iter()
        .map(|field| (field.name.clone(), field.weight))
        .collect()
}

fn fields_for_candidate_documents(documents: &[TextCandidate]) -> TextCollectionConfig {
    let mut names = BTreeSet::new();
    for document in documents {
        names.extend(document.fields.keys().cloned());
    }
    TextCollectionConfig {
        fields: names
            .into_iter()
            .map(|name| TextFieldConfig {
                name,
                weight: 1.0,
                indexed: true,
                stored: true,
            })
            .collect(),
        ..TextCollectionConfig::default()
    }
}

fn snippets_for_fields(
    document_fields: &BTreeMap<String, String>,
    spec: &TextSearchSpec,
) -> Vec<TextSnippet> {
    let selected = spec
        .fields
        .as_ref()
        .map(|fields| fields.iter().cloned().collect::<BTreeSet<_>>());
    document_fields
        .iter()
        .filter(|(field, _)| {
            selected
                .as_ref()
                .is_none_or(|fields| fields.contains(*field))
        })
        .take(3)
        .map(|(field, value)| TextSnippet {
            field: field.clone(),
            text: value.chars().take(240).collect(),
        })
        .collect()
}

fn extract_json_text_fields(
    value: &JsonValue,
    fields: Option<&[String]>,
) -> BTreeMap<String, String> {
    let mut output = BTreeMap::new();
    if let Some(fields) = fields {
        for field in fields {
            if let Some(value) = json_path_value(value, field)
                && let Some(text) = json_value_text(value)
            {
                output.insert(field.clone(), text);
            }
        }
        return output;
    }
    collect_string_leaves(value, "$value", &mut output);
    output
}

fn json_path_value<'a>(value: &'a JsonValue, path: &str) -> Option<&'a JsonValue> {
    if path.is_empty() || path == "$value" {
        return Some(value);
    }
    let mut current = value;
    for segment in path.split('.') {
        current = match current {
            JsonValue::Object(object) => object.get(segment)?,
            JsonValue::Array(array) => {
                let index = segment.parse::<usize>().ok()?;
                array.get(index)?
            }
            _ => return None,
        };
    }
    Some(current)
}

fn json_value_text(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(value) => Some(value.clone()),
        JsonValue::Object(_) | JsonValue::Array(_) => {
            let mut fields = BTreeMap::new();
            collect_string_leaves(value, "$value", &mut fields);
            (!fields.is_empty()).then(|| fields.into_values().collect::<Vec<_>>().join(" "))
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) => None,
    }
}

fn collect_string_leaves(value: &JsonValue, path: &str, output: &mut BTreeMap<String, String>) {
    match value {
        JsonValue::String(text) => {
            output.insert(path.to_owned(), text.clone());
        }
        JsonValue::Array(array) => {
            for (index, value) in array.iter().enumerate() {
                collect_string_leaves(value, &format!("{path}.{index}"), output);
            }
        }
        JsonValue::Object(object) => {
            for (key, value) in object {
                let next = if path == "$value" {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                collect_string_leaves(value, &next, output);
            }
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) => {}
    }
}

fn default_true() -> bool {
    true
}

fn default_analyzer_version() -> u32 {
    1
}

fn default_field_weight() -> f32 {
    1.0
}

fn default_k1() -> f32 {
    1.2
}

fn default_b() -> f32 {
    0.75
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn doc(id: &str, title: &str, body: &str) -> TextDocument {
        TextDocument {
            id: id.to_owned(),
            fields: BTreeMap::from([
                ("title".to_owned(), title.to_owned()),
                ("body".to_owned(), body.to_owned()),
            ]),
            metadata: BTreeMap::new(),
        }
    }

    // This deliberately keeps the pre-optimization document/term scan as a
    // correctness oracle for the postings-first implementations above.
    fn reference_one_shot_matches(
        documents: &[TextDocument],
        config: &TextCollectionConfig,
        query: &str,
        spec: &TextSearchSpec,
    ) -> (Vec<TextSearchMatch>, usize) {
        let analyzed_query = analyze_text(&config.analyzer, query);
        let query_terms = analyzed_query
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let mut document_stats = Vec::with_capacity(documents.len());
        let mut term_doc_freq = BTreeMap::new();
        let mut total_terms = 0;

        for document in documents {
            let mut doc_len = 0;
            let mut matching_fields = BTreeMap::new();
            let mut matching_terms = BTreeSet::new();
            for field_name in indexed_field_names(config, document) {
                let Some(text) = document.fields.get(&field_name) else {
                    continue;
                };
                let tokens = analyze_text(&config.analyzer, text);
                doc_len += tokens.len();
                let mut term_frequencies: BTreeMap<String, usize> = BTreeMap::new();
                for token in tokens {
                    if query_terms.contains(token.as_str()) {
                        *term_frequencies.entry(token.clone()).or_default() += 1;
                        matching_terms.insert(token);
                    }
                }
                if !term_frequencies.is_empty() {
                    matching_fields.insert(field_name, term_frequencies);
                }
            }
            if doc_len == 0 {
                continue;
            }
            total_terms += doc_len;
            for term in matching_terms {
                *term_doc_freq.entry(term).or_default() += 1;
            }
            document_stats.push((document, doc_len, matching_fields));
        }

        let doc_count = document_stats.len();
        if doc_count == 0 {
            return (Vec::new(), 0);
        }
        let avg_doc_len = total_terms as f32 / (doc_count as f32).max(1.0);
        let field_weights = field_weights(config);
        let selected_fields = spec
            .fields
            .as_ref()
            .map(|fields| fields.iter().cloned().collect::<BTreeSet<_>>());
        let mut scores = BTreeMap::new();

        for term in query_terms {
            let df = *term_doc_freq.get(term).unwrap_or(&0) as f32;
            if df <= 0.0 {
                continue;
            }
            let idf = ((doc_count as f32 - df + 0.5) / (df + 0.5) + 1.0).ln();
            for (document, doc_len, fields) in &document_stats {
                for (field, frequencies) in fields {
                    let Some(tf) = frequencies.get(term) else {
                        continue;
                    };
                    if selected_fields
                        .as_ref()
                        .is_some_and(|fields| !fields.contains(field))
                    {
                        continue;
                    }
                    let weight = field_weights.get(field).copied().unwrap_or(1.0);
                    if weight == 0.0 {
                        continue;
                    }
                    let tf = *tf as f32;
                    let numerator = tf * (config.k1 + 1.0);
                    let denominator = tf
                        + config.k1 * (1.0 - config.b + config.b * *doc_len as f32 / avg_doc_len);
                    let score = idf * numerator / denominator * weight;
                    let (total, hits) = scores
                        .entry(document.id.clone())
                        .or_insert_with(|| (0.0, BTreeMap::<String, TextFieldHit>::new()));
                    *total += score;
                    let hit = hits.entry(field.clone()).or_insert_with(|| TextFieldHit {
                        field: field.clone(),
                        terms: Vec::new(),
                        score: 0.0,
                    });
                    hit.terms.push(term.to_owned());
                    hit.score += score;
                }
            }
        }

        let document_by_id = documents
            .iter()
            .map(|document| (document.id.as_str(), document))
            .collect::<BTreeMap<_, _>>();
        let mut matches = scores
            .into_iter()
            .filter_map(|(id, (score, field_hits))| {
                if score <= 0.0 {
                    return None;
                }
                let document = document_by_id.get(id.as_str())?;
                Some(TextSearchMatch {
                    id,
                    score,
                    field_hits: field_hits.into_values().collect(),
                    metadata: spec.include_metadata.then(|| document.metadata.clone()),
                    snippets: spec
                        .include_snippets
                        .then(|| snippets_for_fields(&document.fields, spec)),
                    explanation: spec.explain.then(|| {
                        "exact BM25 over PrimaDB text index; scores are scoped by result scoreScope"
                            .to_owned()
                    }),
                })
            })
            .collect::<Vec<_>>();
        matches.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.id.cmp(&right.id))
        });
        paginate_matches(&mut matches, spec);
        (matches, doc_count)
    }

    fn reference_candidate_matches(
        candidates: &[TextCandidate],
        config: &TextCollectionConfig,
        query: &str,
        spec: &TextSearchSpec,
    ) -> (Vec<TextSearchMatch>, usize) {
        let mut documents = BTreeMap::new();
        for candidate in candidates
            .iter()
            .take(spec.candidate_limit.unwrap_or(usize::MAX))
        {
            documents.insert(
                candidate.id.clone(),
                TextDocument {
                    id: candidate.id.clone(),
                    fields: candidate.fields.clone(),
                    metadata: candidate.metadata.clone(),
                },
            );
        }
        let documents = documents.into_values().collect::<Vec<_>>();
        reference_one_shot_matches(&documents, config, query, spec)
    }

    #[test]
    fn analyzer_is_deterministic() {
        let config = TextAnalyzerConfig::default();
        assert_eq!(
            analyze_text(&config, "Secure mesh-routing, v2"),
            vec!["secure", "mesh", "routing", "v2"]
        );
    }

    #[test]
    fn exact_bm25_ranks_relevant_documents_first() -> Result<()> {
        let mut documents = BTreeMap::new();
        documents.insert(
            "a".to_owned(),
            doc("a", "secure mesh routing", "routing routing trust"),
        );
        documents.insert("b".to_owned(), doc("b", "unrelated", "plain notes"));
        let cache = TextCollectionCache::from_documents(
            TextCollectionConfig::default(),
            documents,
            "test".to_owned(),
        )?;
        let result = search_text_collection("docs", &cache, "secure routing", &Default::default())?;
        assert_eq!(
            result.matches.first().map(|item| item.id.as_str()),
            Some("a")
        );
        assert_eq!(result.score_scope, TextScoreScope::Collection);
        Ok(())
    }

    #[test]
    fn candidate_postings_match_reference_across_hit_rates_and_queries() -> Result<()> {
        for (hit_rate, query) in [("all", "common"), ("half", "half"), ("rare", "rare signal")] {
            let candidates = (0..120)
                .map(|index| {
                    let body = match hit_rate {
                        "all" => format!("common signal document {index}"),
                        "half" if index % 2 == 0 => format!("half signal document {index}"),
                        "rare" if index % 20 == 0 => format!("rare signal document {index}"),
                        _ => format!("ordinary document {index}"),
                    };
                    TextCandidate {
                        id: format!("doc-{index:03}"),
                        fields: BTreeMap::from([
                            ("title".to_owned(), format!("entry {index}")),
                            ("body".to_owned(), body),
                        ]),
                        metadata: BTreeMap::from([(String::from("rank"), json!(index))]),
                    }
                })
                .collect::<Vec<_>>();
            let config = fields_for_candidate_documents(&candidates);
            let spec = TextSearchSpec {
                limit: Some(7),
                offset: Some(3),
                fields: Some(vec!["body".to_owned()]),
                include_metadata: true,
                include_snippets: true,
                explain: true,
                candidate_limit: Some(83),
                ..Default::default()
            };
            let expected = reference_candidate_matches(&candidates, &config, query, &spec);
            let actual = search_text_candidates(
                TextSearchSourceSummary::Records { prefix: None },
                query,
                &spec,
                candidates,
                false,
                TextScoreScope::CandidateSet,
            )?;

            assert_eq!(actual.matches, expected.0, "hit rate {hit_rate}");
            assert_eq!(actual.searched_count, expected.1, "hit rate {hit_rate}");
            assert_eq!(actual.candidate_count, 120);
            assert!(actual.truncated_candidates);
            assert!(actual.matches.len() <= 7);
            assert!(actual.matches.iter().all(|item| item.metadata.is_some()));
            assert!(actual.matches.iter().all(|item| item.snippets.is_some()));
            assert!(actual.matches.iter().all(|item| item.explanation.is_some()));
        }
        Ok(())
    }

    #[test]
    fn collection_dense_postings_match_reference_with_weights_and_details() -> Result<()> {
        let config = TextCollectionConfig {
            fields: vec![
                TextFieldConfig {
                    name: "title".to_owned(),
                    weight: 4.0,
                    indexed: true,
                    stored: true,
                },
                TextFieldConfig {
                    name: "body".to_owned(),
                    weight: 1.0,
                    indexed: true,
                    stored: true,
                },
                TextFieldConfig {
                    name: "ignored".to_owned(),
                    weight: 20.0,
                    indexed: false,
                    stored: true,
                },
            ],
            ..Default::default()
        };
        let documents = BTreeMap::from([
            (
                "a".to_owned(),
                TextDocument {
                    id: "a".to_owned(),
                    fields: BTreeMap::from([
                        ("title".to_owned(), "secure routing".to_owned()),
                        ("body".to_owned(), "ordinary notes".to_owned()),
                        ("ignored".to_owned(), "secure".to_owned()),
                    ]),
                    metadata: BTreeMap::from([(String::from("kind"), json!("title"))]),
                },
            ),
            (
                "b".to_owned(),
                TextDocument {
                    id: "b".to_owned(),
                    fields: BTreeMap::from([
                        ("title".to_owned(), "ordinary".to_owned()),
                        ("body".to_owned(), "secure routing routing".to_owned()),
                    ]),
                    metadata: BTreeMap::from([(String::from("kind"), json!("body"))]),
                },
            ),
            ("c".to_owned(), doc("c", "unrelated", "plain notes")),
            ("d".to_owned(), doc("d", "routing", "secure")),
        ]);
        let cache = TextCollectionCache::from_documents(
            config.clone(),
            documents.clone(),
            "test".to_owned(),
        )?;
        for spec in [
            TextSearchSpec {
                limit: None,
                fields: Some(vec!["title".to_owned()]),
                include_metadata: true,
                include_snippets: true,
                explain: true,
                ..Default::default()
            },
            TextSearchSpec {
                limit: Some(2),
                offset: Some(1),
                fields: Some(vec!["title".to_owned(), "body".to_owned()]),
                ..Default::default()
            },
        ] {
            let ordered_documents = documents.values().cloned().collect::<Vec<_>>();
            let expected =
                reference_one_shot_matches(&ordered_documents, &config, "secure routing", &spec);
            let actual = search_text_collection("docs", &cache, "secure routing", &spec)?;
            assert_eq!(actual.matches, expected.0);
            assert_eq!(actual.searched_count, expected.1);
        }
        Ok(())
    }

    #[test]
    fn field_weight_changes_score() -> Result<()> {
        let mut documents = BTreeMap::new();
        documents.insert("a".to_owned(), doc("a", "mesh", "none"));
        documents.insert("b".to_owned(), doc("b", "none", "mesh mesh"));
        let config = TextCollectionConfig {
            fields: vec![
                TextFieldConfig {
                    name: "title".to_owned(),
                    weight: 5.0,
                    indexed: true,
                    stored: true,
                },
                TextFieldConfig {
                    name: "body".to_owned(),
                    weight: 1.0,
                    indexed: true,
                    stored: true,
                },
            ],
            ..Default::default()
        };
        let cache = TextCollectionCache::from_documents(config, documents, "test".to_owned())?;
        let result = search_text_collection("docs", &cache, "mesh", &Default::default())?;
        assert_eq!(
            result.matches.first().map(|item| item.id.as_str()),
            Some("a")
        );
        Ok(())
    }

    #[test]
    fn cache_rejects_analyzer_mismatch() -> Result<()> {
        let documents = BTreeMap::from([("a".to_owned(), doc("a", "mesh", "routing"))]);
        let config = TextCollectionConfig::default();
        let cache =
            TextCollectionCache::from_documents(config.clone(), documents, "source".to_owned())?;
        let files = text_cache_files("docs", &cache, "now".to_owned())?;
        let mut changed = config;
        changed.analyzer.version = 2;
        assert!(collection_cache_from_text_cache_files(changed, files, "source").is_err());
        Ok(())
    }

    #[test]
    fn cache_restore_uses_serialized_postings_without_rebuilding_documents() -> Result<()> {
        let documents = BTreeMap::from([
            ("a".to_owned(), doc("a", "mesh", "secure routing")),
            ("b".to_owned(), doc("b", "other", "plain notes")),
        ]);
        let config = TextCollectionConfig::default();
        let cache =
            TextCollectionCache::from_documents(config.clone(), documents, "source".to_owned())?;
        let mut files = text_cache_files("docs", &cache, "now".to_owned())?;
        let mut changed_documents: BTreeMap<String, TextDocument> =
            serde_json::from_slice(&files.docs_bin)?;
        changed_documents
            .get_mut("a")
            .expect("exported document exists")
            .fields
            .insert("body".to_owned(), "rewritten document".to_owned());
        files.docs_bin = serde_json::to_vec(&changed_documents)?;

        let restored = collection_cache_from_text_cache_files(config, files, "source")?;
        let old_term = search_text_collection("docs", &restored, "routing", &Default::default())?;
        assert_eq!(
            old_term.matches.first().map(|item| item.id.as_str()),
            Some("a")
        );
        let new_term = search_text_collection("docs", &restored, "rewritten", &Default::default())?;
        assert!(new_term.matches.is_empty());
        assert_eq!(restored.stats().document_count, 2);
        assert_eq!(restored.stats().term_count, cache.stats().term_count);
        Ok(())
    }

    #[test]
    fn cache_restore_rejects_inconsistent_postings() -> Result<()> {
        let documents = BTreeMap::from([("a".to_owned(), doc("a", "mesh", "routing"))]);
        let config = TextCollectionConfig::default();
        let cache =
            TextCollectionCache::from_documents(config.clone(), documents, "source".to_owned())?;
        let mut files = text_cache_files("docs", &cache, "now".to_owned())?;
        let mut postings: BTreeMap<String, BTreeMap<String, BTreeMap<String, usize>>> =
            serde_json::from_slice(&files.postings_bin)?;
        postings
            .get_mut("routing")
            .expect("routing posting exists")
            .get_mut("a")
            .expect("document posting exists")
            .insert("body".to_owned(), 0);
        files.postings_bin = serde_json::to_vec(&postings)?;
        assert!(collection_cache_from_text_cache_files(config, files, "source").is_err());
        Ok(())
    }

    #[test]
    fn candidate_search_reports_candidate_scope() -> Result<()> {
        let result = search_text_candidates(
            TextSearchSourceSummary::Records {
                prefix: Some("memory/".to_owned()),
            },
            "trust proposal",
            &Default::default(),
            vec![TextCandidate {
                id: "memory/1".to_owned(),
                fields: BTreeMap::from([(
                    "$value".to_owned(),
                    "trust proposal in the mesh".to_owned(),
                )]),
                metadata: BTreeMap::new(),
            }],
            false,
            TextScoreScope::CandidateSet,
        )?;
        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.score_scope, TextScoreScope::CandidateSet);
        Ok(())
    }

    #[test]
    fn one_shot_candidate_search_preserves_empty_query_search_count() -> Result<()> {
        let candidates = vec![TextCandidate {
            id: "memory/1".to_owned(),
            fields: BTreeMap::from([("$value".to_owned(), "secure routing notes".to_owned())]),
            metadata: BTreeMap::new(),
        }];
        let result = search_text_candidates(
            TextSearchSourceSummary::Records { prefix: None },
            "",
            &TextSearchSpec::default(),
            candidates,
            false,
            TextScoreScope::CandidateSet,
        )?;
        assert!(result.matches.is_empty());
        assert_eq!(result.candidate_count, 1);
        assert_eq!(result.searched_count, 1);
        Ok(())
    }

    #[test]
    fn one_shot_candidate_search_handles_large_bounded_workloads() -> Result<()> {
        let candidates = (0..2_000)
            .map(|index| TextCandidate {
                id: format!("doc-{index}"),
                fields: BTreeMap::from([(
                    "body".to_owned(),
                    format!("routing common-term-{index}"),
                )]),
                metadata: BTreeMap::new(),
            })
            .collect();
        let result = search_text_candidates(
            TextSearchSourceSummary::Records { prefix: None },
            "routing",
            &TextSearchSpec {
                limit: Some(3),
                ..Default::default()
            },
            candidates,
            false,
            TextScoreScope::CandidateSet,
        )?;
        assert_eq!(result.searched_count, 2_000);
        assert_eq!(result.matches.len(), 3);
        assert_eq!(
            result
                .matches
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            ["doc-0", "doc-1", "doc-10"]
        );
        Ok(())
    }

    #[test]
    fn candidate_search_matches_indexed_scores_and_result_details() -> Result<()> {
        let documents = BTreeMap::from([
            (
                "alpha".to_owned(),
                TextDocument {
                    id: "alpha".to_owned(),
                    fields: BTreeMap::from([
                        ("title".to_owned(), "secure routing".to_owned()),
                        ("body".to_owned(), "secure mesh proposal".to_owned()),
                    ]),
                    metadata: BTreeMap::from([(String::from("kind"), json!("note"))]),
                },
            ),
            (
                "beta".to_owned(),
                TextDocument {
                    id: "beta".to_owned(),
                    fields: BTreeMap::from([
                        ("title".to_owned(), "routing".to_owned()),
                        ("body".to_owned(), "unrelated notes".to_owned()),
                    ]),
                    metadata: BTreeMap::from([(String::from("kind"), json!("other"))]),
                },
            ),
            ("gamma".to_owned(), doc("gamma", "unrelated", "plain notes")),
        ]);
        let cache = TextCollectionCache::from_documents(
            TextCollectionConfig::default(),
            documents.clone(),
            "test".to_owned(),
        )?;
        let spec = TextSearchSpec {
            limit: None,
            include_metadata: true,
            include_snippets: true,
            explain: true,
            ..Default::default()
        };
        let indexed = search_text_collection("docs", &cache, "secure routing", &spec)?;
        let candidates = documents
            .values()
            .cloned()
            .map(|document| TextCandidate {
                id: document.id,
                fields: document.fields,
                metadata: document.metadata,
            })
            .collect();
        let one_shot = search_text_candidates(
            TextSearchSourceSummary::Records { prefix: None },
            "secure routing",
            &spec,
            candidates,
            false,
            TextScoreScope::CandidateSet,
        )?;

        assert_eq!(indexed.matches, one_shot.matches);
        assert_eq!(indexed.searched_count, one_shot.searched_count);
        assert_eq!(indexed.candidate_count, one_shot.candidate_count);
        assert_eq!(indexed.matches[0].id, "alpha");
        assert_eq!(
            indexed.matches[0].metadata.as_ref().unwrap()["kind"],
            json!("note")
        );
        assert!(
            indexed.matches[0]
                .snippets
                .as_ref()
                .is_some_and(|items| !items.is_empty())
        );
        assert!(indexed.matches[0].explanation.is_some());
        Ok(())
    }

    #[test]
    fn collection_top_k_preserves_score_order_and_ties() -> Result<()> {
        let documents = BTreeMap::from([
            ("a".to_owned(), doc("a", "mesh", "")),
            ("b".to_owned(), doc("b", "mesh", "")),
            ("c".to_owned(), doc("c", "mesh", "")),
            ("d".to_owned(), doc("d", "unrelated", "")),
        ]);
        let cache = TextCollectionCache::from_documents(
            TextCollectionConfig::default(),
            documents,
            "test".to_owned(),
        )?;
        let all = search_text_collection(
            "docs",
            &cache,
            "mesh",
            &TextSearchSpec {
                limit: None,
                ..Default::default()
            },
        )?;
        let page = search_text_collection(
            "docs",
            &cache,
            "mesh",
            &TextSearchSpec {
                limit: Some(2),
                offset: Some(1),
                ..Default::default()
            },
        )?;
        assert_eq!(
            page.matches
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            all.matches
                .iter()
                .skip(1)
                .take(2)
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>()
        );
        assert_eq!(page.matches[0].score, all.matches[1].score);
        assert_eq!(page.matches[1].score, all.matches[2].score);
        Ok(())
    }
}
