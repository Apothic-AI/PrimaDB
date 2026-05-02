use crate::{
    NodeState, Primadb, PrimadbError, QuerySpec, RemotePath, Result, TransactionReport,
    TransactionStep, TraversalSpec,
};
use base64ct::{Base64UrlUnpadded, Encoding};
use rhai::{Dynamic, Engine, EvalAltResult, Scope as RhaiScope};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

const SCRIPT_ATTACHMENTS_ROOT: &str = "__primadb_scripts";
const DEFAULT_SCRIPT_ENTRY: &str = "main";
const STORED_SCRIPT_CHUNK_BYTES: usize = 96;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ScriptRuntime {
    #[default]
    Rhai,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ScriptPathGrant {
    pub root: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub segments: Vec<String>,
    #[serde(default)]
    pub recursive: bool,
}

impl ScriptPathGrant {
    pub fn root(root: impl Into<String>) -> Self {
        Self {
            root: root.into(),
            segments: Vec::new(),
            recursive: true,
        }
    }

    pub fn exact(root: impl Into<String>, segments: Vec<String>) -> Self {
        Self {
            root: root.into(),
            segments,
            recursive: false,
        }
    }

    pub fn matches(&self, path: &RemotePath) -> bool {
        if self.root != "*" && self.root != path.anchor {
            return false;
        }
        if self.recursive {
            path.segments.starts_with(&self.segments)
        } else {
            self.segments == path.segments
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ScriptCapabilities {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub read: Vec<ScriptPathGrant>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub query: Vec<ScriptPathGrant>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub traverse: Vec<ScriptPathGrant>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub write: Vec<ScriptPathGrant>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transaction: Vec<ScriptPathGrant>,
}

impl ScriptCapabilities {
    pub fn none() -> Self {
        Self::default()
    }

    pub fn all_for_root(root: impl Into<String>) -> Self {
        let grant = ScriptPathGrant::root(root);
        Self {
            read: vec![grant.clone()],
            query: vec![grant.clone()],
            traverse: vec![grant.clone()],
            write: vec![grant.clone()],
            transaction: vec![grant],
        }
    }

    pub fn read_root(root: impl Into<String>) -> Self {
        Self {
            read: vec![ScriptPathGrant::root(root)],
            ..Self::default()
        }
    }

    fn allows(&self, operation: ScriptOperation, path: &RemotePath) -> bool {
        operation
            .grants(self)
            .iter()
            .any(|grant| grant.matches(path))
    }

    fn declares(&self, operation: ScriptOperation) -> bool {
        !operation.grants(self).is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ScriptLimits {
    pub max_operations: u64,
    pub max_call_levels: usize,
    pub max_variables: usize,
    pub max_functions: usize,
    pub max_modules: usize,
    pub max_expression_depth: usize,
    pub max_string_bytes: usize,
    pub max_array_size: usize,
    pub max_map_size: usize,
}

impl Default for ScriptLimits {
    fn default() -> Self {
        Self {
            max_operations: 50_000,
            max_call_levels: 32,
            max_variables: 256,
            max_functions: 64,
            max_modules: 8,
            max_expression_depth: 64,
            max_string_bytes: 64 * 1024,
            max_array_size: 4096,
            max_map_size: 4096,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeScript {
    pub id: String,
    #[serde(default)]
    pub runtime: ScriptRuntime,
    #[serde(default = "default_script_entry")]
    pub entry: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(default)]
    pub capabilities: ScriptCapabilities,
    #[serde(default)]
    pub metadata: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredNodeScript {
    format: String,
    encoding: String,
    chunks: BTreeMap<String, String>,
}

impl NodeScript {
    pub fn rhai(id: impl Into<String>, source: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            runtime: ScriptRuntime::Rhai,
            entry: DEFAULT_SCRIPT_ENTRY.to_owned(),
            source: source.into(),
            source_hash: None,
            author: None,
            signature: None,
            capabilities: ScriptCapabilities::default(),
            metadata: JsonValue::Null,
        }
    }

    pub fn computed_source_hash(&self) -> String {
        script_source_hash(&self.source)
    }

    fn normalize_for_attach(&mut self) -> Result<()> {
        validate_script_id(&self.id)?;
        if self.entry.trim().is_empty() {
            self.entry = DEFAULT_SCRIPT_ENTRY.to_owned();
        }
        let computed = self.computed_source_hash();
        if let Some(source_hash) = self.source_hash.as_deref()
            && source_hash != computed
        {
            return Err(PrimadbError::Message(format!(
                "script `{}` source hash mismatch: expected `{source_hash}`, computed `{computed}`",
                self.id
            )));
        }
        self.source_hash = Some(computed);
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ScriptExecutionOptions {
    #[serde(default)]
    pub args: JsonValue,
    #[serde(default)]
    pub capabilities: ScriptCapabilities,
    #[serde(default = "default_apply_writes")]
    pub apply_writes: bool,
    #[serde(default)]
    pub limits: ScriptLimits,
}

impl Default for ScriptExecutionOptions {
    fn default() -> Self {
        Self {
            args: JsonValue::Null,
            capabilities: ScriptCapabilities::default(),
            apply_writes: true,
            limits: ScriptLimits::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ScriptExecutionContext {
    pub path: ScriptPathContext,
    pub node: Option<JsonValue>,
    pub node_state: Option<NodeState>,
    pub edges: Vec<ScriptNodeEdge>,
    pub script: NodeScript,
    #[serde(default)]
    pub args: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ScriptPathContext {
    pub anchor: String,
    pub segments: Vec<String>,
    pub display: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ScriptNodeEdge {
    pub field: String,
    pub target: String,
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ScriptExecutionResult {
    pub script_id: String,
    pub runtime: ScriptRuntime,
    pub source_hash: String,
    pub value: JsonValue,
    #[serde(default)]
    pub steps: Vec<TransactionStep>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report: Option<TransactionReport>,
}

#[derive(Debug, Clone, Copy)]
enum ScriptOperation {
    Read,
    Query,
    Traverse,
    Write,
    Transaction,
}

impl ScriptOperation {
    fn grants(self, capabilities: &ScriptCapabilities) -> &[ScriptPathGrant] {
        match self {
            ScriptOperation::Read => &capabilities.read,
            ScriptOperation::Query => &capabilities.query,
            ScriptOperation::Traverse => &capabilities.traverse,
            ScriptOperation::Write => &capabilities.write,
            ScriptOperation::Transaction => &capabilities.transaction,
        }
    }

    fn label(self) -> &'static str {
        match self {
            ScriptOperation::Read => "read",
            ScriptOperation::Query => "query",
            ScriptOperation::Traverse => "traverse",
            ScriptOperation::Write => "write",
            ScriptOperation::Transaction => "transaction",
        }
    }
}

struct ScriptExecutionState {
    db: Primadb,
    local_capabilities: ScriptCapabilities,
    script_capabilities: ScriptCapabilities,
    steps: Vec<TransactionStep>,
}

impl ScriptExecutionState {
    fn assert_allowed(&self, operation: ScriptOperation, path: &RemotePath) -> Result<()> {
        if !self.local_capabilities.allows(operation, path) {
            return Err(PrimadbError::Message(format!(
                "script {} capability denied for `{}`",
                operation.label(),
                path.path()
            )));
        }
        if self.script_capabilities.declares(operation)
            && !self.script_capabilities.allows(operation, path)
        {
            return Err(PrimadbError::Message(format!(
                "script manifest does not request {} capability for `{}`",
                operation.label(),
                path.path()
            )));
        }
        Ok(())
    }

    fn assert_transaction_allowed(&self) -> Result<()> {
        for step in &self.steps {
            let path = transaction_step_path(step);
            self.assert_allowed(ScriptOperation::Transaction, path)?;
        }
        Ok(())
    }
}

impl Primadb {
    pub fn attach_node_script(&self, path: RemotePath, mut script: NodeScript) -> Result<()> {
        script.normalize_for_attach()?;
        script_attachment_script_chain(self, &path, &script.id)
            .put(encode_stored_node_script(&script)?)
    }

    pub fn remove_node_script(&self, path: &RemotePath, script_id: &str) -> Result<()> {
        validate_script_id(script_id)?;
        script_attachment_script_chain(self, path, script_id).unset()
    }

    pub fn node_scripts(&self, path: &RemotePath) -> Result<Vec<NodeScript>> {
        let mut scripts = script_attachment_scripts_chain(self, path)
            .map()?
            .into_iter()
            .map(|entry| decode_stored_node_script(entry.value))
            .collect::<Result<Vec<NodeScript>>>()?;
        scripts.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(scripts)
    }

    pub fn execute_node_scripts(
        &self,
        path: RemotePath,
        options: ScriptExecutionOptions,
    ) -> Result<Vec<ScriptExecutionResult>> {
        let scripts = self.node_scripts(&path)?;
        let mut results = Vec::with_capacity(scripts.len());
        for script in scripts {
            results.push(self.execute_node_script(&path, script, &options)?);
        }
        Ok(results)
    }

    fn execute_node_script(
        &self,
        path: &RemotePath,
        script: NodeScript,
        options: &ScriptExecutionOptions,
    ) -> Result<ScriptExecutionResult> {
        match script.runtime {
            ScriptRuntime::Rhai => self.execute_rhai_node_script(path, script, options),
        }
    }

    fn execute_rhai_node_script(
        &self,
        path: &RemotePath,
        script: NodeScript,
        options: &ScriptExecutionOptions,
    ) -> Result<ScriptExecutionResult> {
        let context = self.script_execution_context(path, &script, &options.args)?;
        let state = Arc::new(Mutex::new(ScriptExecutionState {
            db: self.clone(),
            local_capabilities: options.capabilities.clone(),
            script_capabilities: script.capabilities.clone(),
            steps: Vec::new(),
        }));

        let mut engine = rhai_engine(&options.limits);
        register_db_facade(&mut engine, state.clone());

        let ast = engine.compile(&script.source).map_err(|error| {
            PrimadbError::Message(format!("script `{}` compile error: {error}", script.id))
        })?;
        let mut scope = RhaiScope::new();
        let context_dynamic = json_to_dynamic(&context).map_err(primadb_script_error)?;
        let value = engine
            .call_fn::<Dynamic>(&mut scope, &ast, &script.entry, (context_dynamic,))
            .map_err(|error| {
                PrimadbError::Message(format!("script `{}` execution error: {error}", script.id))
            })?;
        let value = dynamic_to_json(&value).map_err(primadb_script_error)?;
        let steps = state.lock().unwrap().steps.clone();
        let report = if options.apply_writes && !steps.is_empty() {
            state.lock().unwrap().assert_transaction_allowed()?;
            Some(self.apply_transaction_steps(steps.clone())?)
        } else {
            None
        };

        let source_hash = script.computed_source_hash();
        Ok(ScriptExecutionResult {
            script_id: script.id,
            runtime: script.runtime.clone(),
            source_hash,
            value,
            steps,
            report,
        })
    }

    fn script_execution_context(
        &self,
        path: &RemotePath,
        script: &NodeScript,
        args: &JsonValue,
    ) -> Result<ScriptExecutionContext> {
        let node = self.get_path(path)?;
        let node_id = path.path();
        let node_state = self.node_state(&node_id)?;
        let edges = node_state.as_ref().map(node_edges).unwrap_or_default();
        Ok(ScriptExecutionContext {
            path: ScriptPathContext {
                anchor: path.anchor.clone(),
                segments: path.segments.clone(),
                display: path.path(),
            },
            node,
            node_state,
            edges,
            script: script.clone(),
            args: args.clone(),
        })
    }
}

fn rhai_engine(limits: &ScriptLimits) -> Engine {
    let mut engine = Engine::new();
    engine.set_max_operations(limits.max_operations);
    engine.set_max_call_levels(limits.max_call_levels);
    engine.set_max_variables(limits.max_variables);
    engine.set_max_functions(limits.max_functions);
    engine.set_max_modules(limits.max_modules);
    engine.set_max_expr_depths(limits.max_expression_depth, limits.max_expression_depth);
    engine.set_max_string_size(limits.max_string_bytes);
    engine.set_max_array_size(limits.max_array_size);
    engine.set_max_map_size(limits.max_map_size);
    engine
}

fn register_db_facade(engine: &mut Engine, state: Arc<Mutex<ScriptExecutionState>>) {
    let get_state = state.clone();
    engine.register_fn(
        "db_get",
        move |path: String| -> std::result::Result<Dynamic, Box<EvalAltResult>> {
            let path = parse_script_path(&path)?;
            let state = get_state.lock().unwrap();
            state.assert_allowed(ScriptOperation::Read, &path)?;
            json_to_dynamic(&state.db.get_path(&path)?)
        },
    );

    let map_state = state.clone();
    engine.register_fn(
        "db_map",
        move |path: String| -> std::result::Result<Dynamic, Box<EvalAltResult>> {
            let path = parse_script_path(&path)?;
            let state = map_state.lock().unwrap();
            state.assert_allowed(ScriptOperation::Read, &path)?;
            json_to_dynamic(&state.db.map_path(&path)?)
        },
    );

    let query_state = state.clone();
    engine.register_fn(
        "db_query",
        move |path: String, spec: Dynamic| -> std::result::Result<Dynamic, Box<EvalAltResult>> {
            let path = parse_script_path(&path)?;
            let spec: QuerySpec = dynamic_to_type(&spec)?;
            let state = query_state.lock().unwrap();
            state.assert_allowed(ScriptOperation::Query, &path)?;
            json_to_dynamic(&state.db.query_path(&path, &spec)?)
        },
    );

    let traverse_state = state.clone();
    engine.register_fn(
        "db_traverse",
        move |path: String, spec: Dynamic| -> std::result::Result<Dynamic, Box<EvalAltResult>> {
            let path = parse_script_path(&path)?;
            let spec: TraversalSpec = dynamic_to_type(&spec)?;
            let state = traverse_state.lock().unwrap();
            state.assert_allowed(ScriptOperation::Traverse, &path)?;
            json_to_dynamic(&state.db.traverse_path(&path, &spec)?)
        },
    );

    let put_state = state.clone();
    engine.register_fn(
        "db_put",
        move |path: String, value: Dynamic| -> std::result::Result<(), Box<EvalAltResult>> {
            let path = parse_script_path(&path)?;
            let value = dynamic_to_json(&value)?;
            let mut state = put_state.lock().unwrap();
            state.assert_allowed(ScriptOperation::Write, &path)?;
            state.steps.push(TransactionStep::Put { path, value });
            Ok(())
        },
    );

    let unset_state = state.clone();
    engine.register_fn(
        "db_unset",
        move |path: String| -> std::result::Result<(), Box<EvalAltResult>> {
            let path = parse_script_path(&path)?;
            let mut state = unset_state.lock().unwrap();
            state.assert_allowed(ScriptOperation::Write, &path)?;
            state.steps.push(TransactionStep::Unset { path });
            Ok(())
        },
    );

    let set_state = state.clone();
    engine.register_fn(
        "db_set",
        move |path: String, value: Dynamic| -> std::result::Result<(), Box<EvalAltResult>> {
            let path = parse_script_path(&path)?;
            let value = dynamic_to_json(&value)?;
            let mut state = set_state.lock().unwrap();
            state.assert_allowed(ScriptOperation::Write, &path)?;
            state.steps.push(TransactionStep::Set { path, value });
            Ok(())
        },
    );

    let remove_state = state.clone();
    engine.register_fn(
        "db_remove",
        move |path: String, value: Dynamic| -> std::result::Result<(), Box<EvalAltResult>> {
            let path = parse_script_path(&path)?;
            let value = dynamic_to_json(&value)?;
            let mut state = remove_state.lock().unwrap();
            state.assert_allowed(ScriptOperation::Write, &path)?;
            state.steps.push(TransactionStep::Remove { path, value });
            Ok(())
        },
    );

    engine.register_fn(
        "db_increment",
        move |path: String, by: rhai::FLOAT| -> std::result::Result<(), Box<EvalAltResult>> {
            let path = parse_script_path(&path)?;
            let mut state = state.lock().unwrap();
            state.assert_allowed(ScriptOperation::Write, &path)?;
            state.steps.push(TransactionStep::Increment { path, by });
            Ok(())
        },
    );
}

fn script_attachment_scripts_chain(db: &Primadb, path: &RemotePath) -> crate::Chain {
    db.root(SCRIPT_ATTACHMENTS_ROOT)
        .field(script_attachment_key(path))
        .field("scripts")
}

fn script_attachment_script_chain(
    db: &Primadb,
    path: &RemotePath,
    script_id: &str,
) -> crate::Chain {
    script_attachment_scripts_chain(db, path).field(script_id)
}

fn script_attachment_key(path: &RemotePath) -> String {
    blake3::hash(path.path().as_bytes()).to_hex().to_string()
}

fn script_source_hash(source: &str) -> String {
    format!("blake3:{}", blake3::hash(source.as_bytes()).to_hex())
}

fn default_script_entry() -> String {
    DEFAULT_SCRIPT_ENTRY.to_owned()
}

fn default_apply_writes() -> bool {
    true
}

fn validate_script_id(script_id: &str) -> Result<()> {
    if script_id.trim().is_empty()
        || script_id.contains('/')
        || script_id == "."
        || script_id == ".."
    {
        return Err(PrimadbError::Message(format!(
            "invalid script id `{script_id}`; ids must be non-empty path-safe field names"
        )));
    }
    Ok(())
}

fn decode_stored_node_script(value: JsonValue) -> Result<NodeScript> {
    let mut stored: StoredNodeScript = serde_json::from_value(value)?;
    if stored.format != "primadb.script.v1" {
        return Err(PrimadbError::Message(format!(
            "unsupported stored script format `{}`",
            stored.format
        )));
    }
    if stored.encoding != "base64url_unpadded_json_chunks" {
        return Err(PrimadbError::Message(format!(
            "unsupported stored script encoding `{}`",
            stored.encoding
        )));
    }
    stored.chunks.retain(|key, _| key.starts_with("chunk"));
    let encoded = stored.chunks.into_values().collect::<String>();
    let bytes = Base64UrlUnpadded::decode_vec(&encoded)
        .map_err(|error| PrimadbError::Message(format!("invalid stored script base64: {error}")))?;
    serde_json::from_slice(&bytes).map_err(PrimadbError::from)
}

fn encode_stored_node_script(script: &NodeScript) -> Result<StoredNodeScript> {
    let script_json = serde_json::to_vec(script)?;
    let encoded = Base64UrlUnpadded::encode_string(&script_json);
    let chunks = encoded
        .as_bytes()
        .chunks(STORED_SCRIPT_CHUNK_BYTES)
        .enumerate()
        .map(|(index, chunk)| {
            (
                format!("chunk{index:04}"),
                String::from_utf8(chunk.to_vec()).expect("base64 chunks are valid UTF-8"),
            )
        })
        .collect();
    Ok(StoredNodeScript {
        format: "primadb.script.v1".to_owned(),
        encoding: "base64url_unpadded_json_chunks".to_owned(),
        chunks,
    })
}

fn parse_script_path(path: &str) -> std::result::Result<RemotePath, Box<EvalAltResult>> {
    let parts = path
        .split('/')
        .filter(|part| !part.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let Some((anchor, segments)) = parts.split_first() else {
        return Err("script path must include a root".into());
    };
    Ok(RemotePath::new(anchor.clone(), segments.to_vec()))
}

fn transaction_step_path(step: &TransactionStep) -> &RemotePath {
    match step {
        TransactionStep::Put { path, .. }
        | TransactionStep::Unset { path }
        | TransactionStep::Set { path, .. }
        | TransactionStep::Remove { path, .. }
        | TransactionStep::AssertExists { path }
        | TransactionStep::AssertAbsent { path }
        | TransactionStep::AssertValue { path, .. }
        | TransactionStep::AssertRevision { path, .. }
        | TransactionStep::Increment { path, .. } => path,
    }
}

fn json_to_dynamic<T: Serialize>(value: T) -> std::result::Result<Dynamic, Box<EvalAltResult>> {
    rhai::serde::to_dynamic(value).map_err(|error| error.to_string().into())
}

fn dynamic_to_json(value: &Dynamic) -> std::result::Result<JsonValue, Box<EvalAltResult>> {
    rhai::serde::from_dynamic(value).map_err(|error| error.to_string().into())
}

fn dynamic_to_type<'de, T>(value: &'de Dynamic) -> std::result::Result<T, Box<EvalAltResult>>
where
    T: Deserialize<'de>,
{
    rhai::serde::from_dynamic(value).map_err(|error| error.to_string().into())
}

fn primadb_script_error(error: Box<EvalAltResult>) -> PrimadbError {
    PrimadbError::Message(error.to_string())
}

impl From<PrimadbError> for Box<EvalAltResult> {
    fn from(error: PrimadbError) -> Self {
        error.to_string().into()
    }
}

fn node_edges(node: &NodeState) -> Vec<ScriptNodeEdge> {
    node.fields
        .iter()
        .flat_map(|(field, state)| match &state.value {
            crate::FieldValue::Link(target) => vec![ScriptNodeEdge {
                field: field.clone(),
                target: target.clone(),
                kind: "link".to_owned(),
            }],
            crate::FieldValue::Set(set) => set
                .members
                .keys()
                .map(|target| ScriptNodeEdge {
                    field: field.clone(),
                    target: target.clone(),
                    kind: "set_member".to_owned(),
                })
                .collect(),
            crate::FieldValue::Scalar(_)
            | crate::FieldValue::Bytes(_)
            | crate::FieldValue::Blob(_) => Vec::new(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn grant(root: &str) -> ScriptPathGrant {
        ScriptPathGrant::root(root)
    }

    #[test]
    fn node_scripts_are_attached_and_listed_by_path() -> Result<()> {
        let db = Primadb::with_replica_id("script-test");
        let path = RemotePath::new("notes", vec!["welcome".to_owned()]);
        let script = NodeScript::rhai("derive", "fn main(ctx) { ctx.path.display }");

        db.attach_node_script(path.clone(), script)?;
        let scripts = db.node_scripts(&path)?;

        assert_eq!(scripts.len(), 1);
        assert_eq!(scripts[0].id, "derive");
        assert!(
            scripts[0]
                .source_hash
                .as_deref()
                .is_some_and(|hash| hash.starts_with("blake3:"))
        );
        Ok(())
    }

    #[test]
    fn scripts_execute_with_capability_scoped_reads_and_transactional_writes() -> Result<()> {
        let db = Primadb::with_replica_id("script-test");
        db.root("notes")
            .field("welcome")
            .put(json!({ "title": "Welcome" }))?;

        let path = RemotePath::new("notes", vec!["welcome".to_owned()]);
        let mut script = NodeScript::rhai(
            "derive",
            r#"
                fn main(ctx) {
                    let source = db_get("notes/welcome");
                    db_put("derived/welcome", #{
                        title: source.title,
                        from: ctx.path.display
                    });
                    return #{ ok: true, title: source.title };
                }
            "#,
        );
        script.capabilities.read.push(grant("notes"));
        script.capabilities.write.push(grant("derived"));
        script.capabilities.transaction.push(grant("derived"));
        db.attach_node_script(path.clone(), script)?;

        let results = db.execute_node_scripts(
            path,
            ScriptExecutionOptions {
                capabilities: ScriptCapabilities {
                    read: vec![grant("notes")],
                    write: vec![grant("derived")],
                    transaction: vec![grant("derived")],
                    ..ScriptCapabilities::default()
                },
                ..ScriptExecutionOptions::default()
            },
        )?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].value["title"], "Welcome");
        assert!(results[0].report.is_some());
        let derived = db.root("derived").field("welcome").once_json()?.unwrap();
        assert_eq!(derived["title"], "Welcome");
        assert_eq!(derived["from"], "notes/welcome");
        Ok(())
    }

    #[test]
    fn local_capabilities_are_required_even_when_script_requests_access() -> Result<()> {
        let db = Primadb::with_replica_id("script-test");
        db.root("notes")
            .field("welcome")
            .put(json!({ "title": "Welcome" }))?;
        let path = RemotePath::new("notes", vec!["welcome".to_owned()]);
        let mut script = NodeScript::rhai(
            "leak",
            r#"
                fn main(ctx) {
                    let source = db_get("notes/welcome");
                    db_put("private/copy", source);
                    return true;
                }
            "#,
        );
        script.capabilities.read.push(grant("notes"));
        script.capabilities.write.push(grant("private"));
        script.capabilities.transaction.push(grant("private"));
        db.attach_node_script(path.clone(), script)?;

        let denied = db
            .execute_node_scripts(
                path,
                ScriptExecutionOptions {
                    capabilities: ScriptCapabilities {
                        read: vec![grant("notes")],
                        write: vec![grant("derived")],
                        transaction: vec![grant("derived")],
                        ..ScriptCapabilities::default()
                    },
                    ..ScriptExecutionOptions::default()
                },
            )
            .unwrap_err()
            .to_string();

        assert!(denied.contains("script write capability denied"));
        assert_eq!(db.root("private").field("copy").once_json()?, None);
        Ok(())
    }

    #[test]
    fn dry_run_returns_steps_without_applying_them() -> Result<()> {
        let db = Primadb::with_replica_id("script-test");
        let path = RemotePath::new("notes", vec!["welcome".to_owned()]);
        let mut script = NodeScript::rhai(
            "plan",
            r#"
                fn main(ctx) {
                    db_increment("metrics/runs", 1.0);
                    return "planned";
                }
            "#,
        );
        script.capabilities.write.push(grant("metrics"));
        db.attach_node_script(path.clone(), script)?;

        let results = db.execute_node_scripts(
            path,
            ScriptExecutionOptions {
                apply_writes: false,
                capabilities: ScriptCapabilities {
                    write: vec![grant("metrics")],
                    ..ScriptCapabilities::default()
                },
                ..ScriptExecutionOptions::default()
            },
        )?;

        assert_eq!(results[0].steps.len(), 1);
        assert!(results[0].report.is_none());
        assert_eq!(db.root("metrics").field("runs").once_json()?, None);
        Ok(())
    }
}
