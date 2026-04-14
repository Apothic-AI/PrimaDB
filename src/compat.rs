use crate::error::{PrimadbError, Result};
use crate::{Chain, FieldValue, MapEntry, Primadb, QuerySpec, Subscription};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct GunCompatOptions {
    #[serde(default = "default_true")]
    pub null_as_unset: bool,
    #[serde(default = "default_true")]
    pub gun_link_markers: bool,
}

#[derive(Debug, Clone)]
pub struct Gun {
    db: Primadb,
    options: GunCompatOptions,
}

#[derive(Debug, Clone)]
pub struct GunChain {
    chain: Chain,
    history: Vec<Chain>,
    options: GunCompatOptions,
}

impl Gun {
    pub fn new(replica_id: impl Into<String>) -> Self {
        Self {
            db: Primadb::with_replica_id(replica_id),
            options: GunCompatOptions::default(),
        }
    }

    pub fn from_db(db: Primadb) -> Self {
        Self {
            db,
            options: GunCompatOptions::default(),
        }
    }

    pub fn with_options(db: Primadb, options: GunCompatOptions) -> Self {
        Self { db, options }
    }

    pub fn db(&self) -> &Primadb {
        &self.db
    }

    pub fn get(&self, key: impl Into<String>) -> GunChain {
        GunChain {
            chain: self.db.root(key.into()),
            history: Vec::new(),
            options: self.options.clone(),
        }
    }

    pub fn export_gun_graph_json(&self) -> Result<String> {
        let snapshot = self.db.snapshot();
        let mut graph = Map::new();
        for (node_id, state) in snapshot.nodes {
            let mut node = Map::new();
            let mut metadata = Map::new();
            metadata.insert("#".to_owned(), JsonValue::String(node_id.clone()));
            let mut versions = Map::new();
            for (field, field_state) in &state.fields {
                versions.insert(
                    field.clone(),
                    JsonValue::String(format!(
                        "{}:{}:{}",
                        field_state.version.revision.millis,
                        field_state.version.revision.counter,
                        field_state.version.op_id
                    )),
                );
                node.insert(field.clone(), field_value_to_gun(&field_state.value));
            }
            metadata.insert(">".to_owned(), JsonValue::Object(versions));
            node.insert("_".to_owned(), JsonValue::Object(metadata));
            graph.insert(node_id, JsonValue::Object(node));
        }
        Ok(serde_json::to_string_pretty(&JsonValue::Object(graph))?)
    }

    pub fn import_gun_graph_json(&self, payload: &str) -> Result<()> {
        let graph: JsonValue = serde_json::from_str(payload)?;
        let JsonValue::Object(nodes) = graph else {
            return Err(PrimadbError::ExpectedObject {
                path: "gun graph".to_owned(),
            });
        };
        for (node_id, raw_node) in nodes {
            let JsonValue::Object(fields) = raw_node else {
                continue;
            };
            for (field, value) in fields {
                if field == "_" {
                    continue;
                }
                self.db
                    .root(node_id.clone())
                    .field(field)
                    .put(gun_to_primadb(value))?;
            }
        }
        Ok(())
    }
}

impl GunChain {
    pub fn get(&self, key: impl Into<String>) -> Self {
        let mut history = self.history.clone();
        history.push(self.chain.clone());
        Self {
            chain: self.chain.field(key.into()),
            history,
            options: self.options.clone(),
        }
    }

    pub fn back(&self, steps: Option<usize>) -> Option<Self> {
        let steps = steps.unwrap_or(1);
        if steps == 0 {
            return Some(self.clone());
        }
        let len = self.history.len();
        let target = len.checked_sub(steps)?;
        Some(Self {
            chain: self.history[target].clone(),
            history: self.history[..target].to_vec(),
            options: self.options.clone(),
        })
    }

    pub fn put<T: Serialize>(&self, value: T) -> Result<()> {
        let value = serde_json::to_value(value)?;
        if self.options.null_as_unset && value.is_null() {
            self.chain.unset()
        } else {
            self.chain.put(gun_to_primadb(value))
        }
    }

    pub fn set<T: Serialize>(&self, value: T) -> Result<String> {
        self.chain.set(gun_to_primadb(serde_json::to_value(value)?))
    }

    pub fn once(&self) -> Result<Option<JsonValue>> {
        self.chain
            .once_json()
            .map(|value| value.map(primadb_to_gun))
    }

    pub fn query(&self, spec: QuerySpec) -> Result<Vec<MapEntry>> {
        self.chain.query(spec)
    }

    pub fn map(&self) -> Result<Vec<MapEntry>> {
        self.chain.map()
    }

    pub fn on(&self) -> Result<Subscription> {
        self.chain.subscribe()
    }
}

fn gun_to_primadb(value: JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(mut object) => {
            if object.len() == 1 {
                if let Some(JsonValue::String(target)) = object.remove("#") {
                    return JsonValue::Object(Map::from_iter([(
                        "$link".to_owned(),
                        JsonValue::String(target),
                    )]));
                }
            }
            JsonValue::Object(
                object
                    .into_iter()
                    .map(|(key, value)| (key, gun_to_primadb(value)))
                    .collect(),
            )
        }
        JsonValue::Array(items) => {
            JsonValue::Array(items.into_iter().map(gun_to_primadb).collect())
        }
        other => other,
    }
}

fn primadb_to_gun(value: JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(mut object) => {
            if object.len() == 1 {
                if let Some(JsonValue::String(target)) = object.remove("$ref") {
                    return JsonValue::Object(Map::from_iter([(
                        "#".to_owned(),
                        JsonValue::String(target),
                    )]));
                }
            }
            JsonValue::Object(
                object
                    .into_iter()
                    .map(|(key, value)| (key, primadb_to_gun(value)))
                    .collect(),
            )
        }
        JsonValue::Array(items) => {
            JsonValue::Array(items.into_iter().map(primadb_to_gun).collect())
        }
        other => other,
    }
}

fn primadb_value_to_gun(value: &JsonValue) -> JsonValue {
    primadb_to_gun(value.clone())
}

fn field_value_to_gun(value: &FieldValue) -> JsonValue {
    match value {
        FieldValue::Scalar(value) => primadb_value_to_gun(value),
        FieldValue::Bytes(bytes) => JsonValue::Object(Map::from_iter([(
            "$bytes".to_owned(),
            JsonValue::String(bytes.to_base64()),
        )])),
        FieldValue::Blob(reference) => JsonValue::Object(Map::from_iter([(
            "$blob".to_owned(),
            serde_json::to_value(reference).unwrap_or(JsonValue::Null),
        )])),
        FieldValue::Link(target) => JsonValue::Object(Map::from_iter([(
            "#".to_owned(),
            JsonValue::String(target.clone()),
        )])),
        FieldValue::Set(set) => JsonValue::Object(Map::from_iter([(
            "$set".to_owned(),
            JsonValue::Array(
                set.members
                    .keys()
                    .map(|member| {
                        JsonValue::Object(Map::from_iter([(
                            "#".to_owned(),
                            JsonValue::String(member.clone()),
                        )]))
                    })
                    .collect(),
            ),
        )])),
    }
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::Gun;
    use serde_json::json;

    #[test]
    fn gun_compat_supports_get_put_and_once() {
        let gun = Gun::new("compat-test");
        gun.get("users")
            .get("alice")
            .put(json!({"name": "Alice"}))
            .unwrap();

        let alice = gun.get("users").get("alice").once().unwrap().unwrap();
        assert_eq!(alice["name"], "Alice");
    }

    #[test]
    fn gun_graph_import_accepts_hash_links() {
        let gun = Gun::new("compat-import");
        gun.import_gun_graph_json(
            &json!({
                "users": {
                    "_": { "#": "users" },
                    "alice": { "#": "users/alice" }
                },
                "users/alice": {
                    "_": { "#": "users/alice" },
                    "name": "Alice"
                }
            })
            .to_string(),
        )
        .unwrap();

        let alice = gun.get("users").get("alice").once().unwrap().unwrap();
        assert_eq!(alice["name"], "Alice");
    }
}
