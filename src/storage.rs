use crate::error::{PrimadbError, Result};
use crate::{DatabaseSnapshot, Operation, Primadb};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

pub trait StorageAdapter: Debug + Send + Sync {
    fn name(&self) -> &str;
    fn load_snapshot(&self) -> Result<Option<DatabaseSnapshot>>;
    fn flush(&self, ops: &[Operation], snapshot: &DatabaseSnapshot) -> Result<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct StorageReport {
    pub adapter: String,
    pub replayed_ops: usize,
    pub compacted: bool,
}

#[derive(Debug, Clone, Default)]
pub struct MemoryStorageAdapter {
    snapshot: Arc<Mutex<Option<DatabaseSnapshot>>>,
    log: Arc<Mutex<Vec<Operation>>>,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
pub struct SnapshotFileAdapter {
    path: std::path::PathBuf,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
pub struct RadiskFileAdapter {
    directory: std::path::PathBuf,
    replica_id: String,
    compaction_threshold: usize,
}

impl MemoryStorageAdapter {
    pub fn log_len(&self) -> usize {
        self.log.lock().unwrap().len()
    }
}

impl StorageAdapter for MemoryStorageAdapter {
    fn name(&self) -> &str {
        "memory"
    }

    fn load_snapshot(&self) -> Result<Option<DatabaseSnapshot>> {
        Ok(self.snapshot.lock().unwrap().clone())
    }

    fn flush(&self, ops: &[Operation], snapshot: &DatabaseSnapshot) -> Result<()> {
        self.log.lock().unwrap().extend_from_slice(ops);
        *self.snapshot.lock().unwrap() = Some(snapshot.clone());
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl SnapshotFileAdapter {
    pub fn new(path: impl Into<std::path::PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl StorageAdapter for SnapshotFileAdapter {
    fn name(&self) -> &str {
        "snapshot_file"
    }

    fn load_snapshot(&self) -> Result<Option<DatabaseSnapshot>> {
        if !self.path.exists() {
            return Ok(None);
        }
        let payload = std::fs::read_to_string(&self.path)?;
        Ok(Some(serde_json::from_str(&payload)?))
    }

    fn flush(&self, _ops: &[Operation], snapshot: &DatabaseSnapshot) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&self.path, serde_json::to_string_pretty(snapshot)?)?;
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl RadiskFileAdapter {
    pub fn new(
        directory: impl Into<std::path::PathBuf>,
        replica_id: impl Into<String>,
        compaction_threshold: usize,
    ) -> Self {
        Self {
            directory: directory.into(),
            replica_id: replica_id.into(),
            compaction_threshold: compaction_threshold.max(1),
        }
    }

    fn checkpoint_path(&self) -> std::path::PathBuf {
        self.directory.join("checkpoint.json")
    }

    fn log_path(&self) -> std::path::PathBuf {
        self.directory.join("ops.jsonl")
    }

    fn read_log(&self) -> Result<Vec<Operation>> {
        let path = self.log_path();
        if !path.exists() {
            return Ok(Vec::new());
        }
        let payload = std::fs::read_to_string(path)?;
        payload
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).map_err(Into::into))
            .collect()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl StorageAdapter for RadiskFileAdapter {
    fn name(&self) -> &str {
        "radisk_file"
    }

    fn load_snapshot(&self) -> Result<Option<DatabaseSnapshot>> {
        std::fs::create_dir_all(&self.directory)?;
        let checkpoint = if self.checkpoint_path().exists() {
            let payload = std::fs::read_to_string(self.checkpoint_path())?;
            Some(serde_json::from_str::<DatabaseSnapshot>(&payload)?)
        } else {
            None
        };

        let log = self.read_log()?;
        if checkpoint.is_none() && log.is_empty() {
            return Ok(None);
        }

        let temp = Primadb::with_replica_id(
            checkpoint
                .as_ref()
                .map(|snapshot| snapshot.clock.actor().to_owned())
                .unwrap_or_else(|| self.replica_id.clone()),
        );
        if let Some(snapshot) = checkpoint {
            temp.load_snapshot(snapshot)?;
        }
        if !log.is_empty() {
            temp.apply_operations(log)?;
        }
        Ok(Some(temp.snapshot()))
    }

    fn flush(&self, ops: &[Operation], snapshot: &DatabaseSnapshot) -> Result<()> {
        std::fs::create_dir_all(&self.directory)?;
        if !ops.is_empty() {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.log_path())?;
            for op in ops {
                use std::io::Write;
                writeln!(file, "{}", serde_json::to_string(op)?)?;
            }
        }

        let log_len = self.read_log()?.len();
        if log_len >= self.compaction_threshold || !self.checkpoint_path().exists() {
            std::fs::write(self.checkpoint_path(), serde_json::to_string_pretty(snapshot)?)?;
            std::fs::write(self.log_path(), "")?;
        }
        Ok(())
    }
}

#[allow(dead_code)]
pub fn attach_adapter(db: &Primadb, adapter: Arc<dyn StorageAdapter>) -> Result<bool> {
    db.attach_storage_adapter(adapter)
}

#[allow(dead_code)]
pub fn adapter_error(name: &str, detail: impl ToString) -> PrimadbError {
    PrimadbError::Message(format!("storage adapter `{name}` failed: {}", detail.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{MemoryStorageAdapter, StorageAdapter};
    use crate::Primadb;
    use serde_json::json;

    #[test]
    fn memory_adapter_round_trips_snapshot() {
        let db = Primadb::with_replica_id("adapter-a");
        db.root("docs").field("post").put(json!({"title": "Hello"})).unwrap();
        let adapter = MemoryStorageAdapter::default();
        adapter.flush(&db.pending_operations(), &db.snapshot()).unwrap();
        let snapshot = adapter.load_snapshot().unwrap().unwrap();
        assert_eq!(snapshot.nodes.len(), 2);
    }
}
