#[cfg(feature = "crypto")]
fn main() -> anyhow::Result<()> {
    use primadb::{Identity, Primadb, SecretBoxKey, SyncFrame, UserGrant};
    use serde_json::json;

    let alice = Identity::generate();
    let transport_key = SecretBoxKey::generate();

    let writer = Primadb::with_replica_id("writer");
    let reader = Primadb::with_replica_id("reader");

    let grants = vec![UserGrant::write_root("docs")];
    writer.set_require_signed_sync(true);
    reader.set_require_signed_sync(true);
    writer.register_user("alice", alice.public_identity(), grants.clone())?;
    reader.register_user("alice", alice.public_identity(), grants.clone())?;
    writer.authenticate_local_user("alice", alice.clone(), grants.clone())?;
    reader.set_transport_encryption_key(transport_key.clone());
    writer.set_transport_encryption_key(transport_key);

    writer
        .root("docs")
        .field("post")
        .put(json!({"title": "Signed", "body": "Encrypted transport"}))?;

    let envelope = writer.drain_sync_envelope()?;
    let frame = SyncFrame::Sync {
        from: envelope.from,
        message_id: "example-sync-1".to_owned(),
        ops: envelope.ops,
    };
    let secure = writer.secure_sync_frame(frame)?;
    reader.apply_secure_sync_frame(secure)?;

    let snapshot = reader.root("docs").field("post").once_json()?.unwrap();
    println!("{}", serde_json::to_string_pretty(&snapshot)?);
    Ok(())
}

#[cfg(not(feature = "crypto"))]
fn main() {
    eprintln!("Run with: cargo run --features crypto --example authenticated_sync");
}
