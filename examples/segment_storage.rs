fn main() -> anyhow::Result<()> {
    use primadb::Primadb;
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let directory = std::env::temp_dir().join(format!("primadb-segment-example-{suffix}"));

    let db = Primadb::with_replica_id("segment-demo");
    db.use_segment_storage(&directory, 4)?;
    db.root("docs")
        .field("welcome")
        .put(json!({"title": "Durable", "status": "persisted"}))?;
    db.close_durable_storage();

    let reopened = Primadb::with_replica_id("segment-demo");
    reopened.use_segment_storage(&directory, 4)?;
    let restored = reopened.root("docs").field("welcome").once_json()?.unwrap();

    println!("{}", serde_json::to_string_pretty(&restored)?);
    Ok(())
}
