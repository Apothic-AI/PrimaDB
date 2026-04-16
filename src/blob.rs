use crate::binary::BinaryBytes;
#[cfg(not(target_arch = "wasm32"))]
use crate::error::PrimadbError;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BlobRef {
    pub id: String,
    pub bytes: usize,
    #[serde(default)]
    pub media_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StoredBlob {
    pub reference: BlobRef,
    pub data: BinaryBytes,
}

pub trait BlobStore: Debug + Send + Sync {
    fn name(&self) -> &str;
    fn put_blob(&self, data: &[u8], media_type: Option<&str>) -> Result<BlobRef>;
    fn get_blob(&self, blob_id: &str) -> Result<Option<StoredBlob>>;
    fn has_blob(&self, blob_id: &str) -> Result<bool> {
        Ok(self.get_blob(blob_id)?.is_some())
    }
    fn delete_unreferenced(&self, live_blob_ids: &BTreeSet<String>) -> Result<usize> {
        let _ = live_blob_ids;
        Ok(0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum BlobStorageConfig {
    Memory,
    #[cfg(not(target_arch = "wasm32"))]
    Files {
        directory: String,
    },
    #[cfg(target_arch = "wasm32")]
    IndexedDb {
        database_name: String,
        store_name: String,
        namespace: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BlobStorageBinding {
    pub backend: String,
    pub content_addressed: bool,
}

#[derive(Debug, Clone, Default)]
pub struct MemoryBlobStore {
    blobs: Arc<Mutex<std::collections::BTreeMap<String, StoredBlob>>>,
}

impl MemoryBlobStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl BlobStore for MemoryBlobStore {
    fn name(&self) -> &str {
        "memory"
    }

    fn put_blob(&self, data: &[u8], media_type: Option<&str>) -> Result<BlobRef> {
        let reference = blob_ref_for_data(data, media_type);
        let blob = StoredBlob {
            reference: reference.clone(),
            data: BinaryBytes::from(data),
        };
        self.blobs
            .lock()
            .unwrap()
            .insert(reference.id.clone(), blob);
        Ok(reference)
    }

    fn get_blob(&self, blob_id: &str) -> Result<Option<StoredBlob>> {
        Ok(self.blobs.lock().unwrap().get(blob_id).cloned())
    }

    fn delete_unreferenced(&self, live_blob_ids: &BTreeSet<String>) -> Result<usize> {
        let mut blobs = self.blobs.lock().unwrap();
        let before = blobs.len();
        blobs.retain(|blob_id, _| live_blob_ids.contains(blob_id));
        Ok(before.saturating_sub(blobs.len()))
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
pub struct FileBlobStore {
    root: std::path::PathBuf,
}

#[cfg(not(target_arch = "wasm32"))]
impl FileBlobStore {
    pub fn new(root: impl Into<std::path::PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn ensure_layout(&self) -> Result<()> {
        std::fs::create_dir_all(self.root.join("blobs"))?;
        Ok(())
    }

    fn blob_dir(&self, blob_id: &str) -> std::path::PathBuf {
        self.root.join("blobs").join(blob_id.replace(':', "_"))
    }

    fn blob_meta_path(&self, blob_id: &str) -> std::path::PathBuf {
        self.blob_dir(blob_id).join("meta.json")
    }

    fn blob_data_path(&self, blob_id: &str) -> std::path::PathBuf {
        self.blob_dir(blob_id).join("data.bin")
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl BlobStore for FileBlobStore {
    fn name(&self) -> &str {
        "files"
    }

    fn put_blob(&self, data: &[u8], media_type: Option<&str>) -> Result<BlobRef> {
        self.ensure_layout()?;
        let reference = blob_ref_for_data(data, media_type);
        let dir = self.blob_dir(&reference.id);
        std::fs::create_dir_all(&dir)?;
        std::fs::write(self.blob_data_path(&reference.id), data)?;
        std::fs::write(
            self.blob_meta_path(&reference.id),
            serde_json::to_vec(&reference)?,
        )?;
        Ok(reference)
    }

    fn get_blob(&self, blob_id: &str) -> Result<Option<StoredBlob>> {
        self.ensure_layout()?;
        let meta_path = self.blob_meta_path(blob_id);
        let data_path = self.blob_data_path(blob_id);
        if !meta_path.exists() || !data_path.exists() {
            return Ok(None);
        }
        let reference: BlobRef = serde_json::from_str(&std::fs::read_to_string(meta_path)?)?;
        let data = std::fs::read(data_path)?;
        if reference.bytes != data.len() {
            return Err(PrimadbError::Message(format!(
                "blob `{blob_id}` metadata length {} does not match stored bytes {}",
                reference.bytes,
                data.len()
            )));
        }
        Ok(Some(StoredBlob {
            reference,
            data: BinaryBytes::from(data),
        }))
    }

    fn delete_unreferenced(&self, live_blob_ids: &BTreeSet<String>) -> Result<usize> {
        self.ensure_layout()?;
        let root = self.root.join("blobs");
        if !root.exists() {
            return Ok(0);
        }
        let mut removed = 0;
        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let blob_id = name.replace('_', ":");
            if !live_blob_ids.contains(&blob_id) {
                std::fs::remove_dir_all(path)?;
                removed += 1;
            }
        }
        Ok(removed)
    }
}

pub fn blob_ref_for_data(data: &[u8], media_type: Option<&str>) -> BlobRef {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let digest = hasher.finalize();
    BlobRef {
        id: format!("sha256:{digest:x}"),
        bytes: data.len(),
        media_type: media_type.map(str::to_owned),
    }
}
