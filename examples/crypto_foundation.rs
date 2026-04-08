#[cfg(not(feature = "crypto"))]
fn main() {
    eprintln!("Run with: cargo run --features crypto --example crypto_foundation");
}

#[cfg(feature = "crypto")]
fn main() -> anyhow::Result<()> {
    use primadb::{Identity, SecretBoxKey, SyncFrame};
    use serde_json::json;

    let identity = Identity::generate();
    let signed = identity.sign_sync_frame(SyncFrame::Ack {
        from: identity.public_key_base64(),
        message_id: "demo-message".to_owned(),
        applied: 1,
    })?;
    let verified = signed.verify()?;

    let secret_box = SecretBoxKey::generate();
    let encrypted = secret_box.encrypt_json(&json!({
        "title": "Encrypted note",
        "body": "This payload can be stored or transported separately from Primadb state."
    }))?;
    let decrypted: serde_json::Value = secret_box.decrypt_json(&encrypted)?;

    println!("public key: {}", identity.public_key_base64());
    println!(
        "verified frame: {}",
        serde_json::to_string_pretty(&verified)?
    );
    println!(
        "encrypted payload: {}",
        serde_json::to_string_pretty(&encrypted)?
    );
    println!(
        "decrypted payload: {}",
        serde_json::to_string_pretty(&decrypted)?
    );
    Ok(())
}
