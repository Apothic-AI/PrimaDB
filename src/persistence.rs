#[cfg(target_arch = "wasm32")]
use crate::error::PrimadbError;
use crate::error::Result;
use crate::snapshot::DatabaseSnapshot;

#[derive(Debug, Clone)]
pub enum PersistenceTarget {
    #[cfg(not(target_arch = "wasm32"))]
    File(std::path::PathBuf),
    #[cfg(target_arch = "wasm32")]
    BrowserStorage(String),
}

pub fn load_snapshot(target: &PersistenceTarget) -> Result<Option<DatabaseSnapshot>> {
    match target {
        #[cfg(not(target_arch = "wasm32"))]
        PersistenceTarget::File(path) => {
            if !path.exists() {
                return Ok(None);
            }
            let data = std::fs::read_to_string(path)?;
            Ok(Some(serde_json::from_str(&data)?))
        }
        #[cfg(target_arch = "wasm32")]
        PersistenceTarget::BrowserStorage(key) => {
            let storage = browser_storage()?;
            let payload = storage
                .get_item(key)
                .map_err(|err| PrimadbError::Message(format!("{err:?}")))?;
            match payload {
                Some(json) => Ok(Some(serde_json::from_str(&json)?)),
                None => Ok(None),
            }
        }
    }
}

pub fn store_snapshot(target: &PersistenceTarget, snapshot: &DatabaseSnapshot) -> Result<()> {
    let payload = serde_json::to_string_pretty(snapshot)?;
    match target {
        #[cfg(not(target_arch = "wasm32"))]
        PersistenceTarget::File(path) => {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(path, payload)?;
            Ok(())
        }
        #[cfg(target_arch = "wasm32")]
        PersistenceTarget::BrowserStorage(key) => {
            let storage = browser_storage()?;
            storage
                .set_item(key, &payload)
                .map_err(|err| PrimadbError::Message(format!("{err:?}")))?;
            Ok(())
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn browser_storage() -> Result<web_sys::Storage> {
    let window = web_sys::window().ok_or(PrimadbError::BrowserWindowUnavailable)?;
    let storage = window
        .local_storage()
        .map_err(|err| PrimadbError::Message(format!("{err:?}")))?
        .ok_or(PrimadbError::BrowserStorageUnavailable)?;
    Ok(storage)
}
