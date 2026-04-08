fn main() -> anyhow::Result<()> {
    use primadb::Primadb;
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis();
    let directory = std::env::temp_dir().join(format!("primadb-radisk-example-{suffix}"));

    let db = Primadb::with_replica_id("radisk-demo");
    db.use_radisk_storage(&directory, 4)?;
    db.root("docs")
        .field("welcome")
        .put(json!({"title": "Durable", "status": "persisted"}))?;

    let reopened = Primadb::with_replica_id("radisk-demo");
    reopened.use_radisk_storage(&directory, 4)?;
    let restored = reopened.root("docs").field("welcome").once_json()?.unwrap();

    println!("{}", serde_json::to_string_pretty(&restored)?);
    Ok(())
}
