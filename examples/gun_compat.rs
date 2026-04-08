fn main() -> anyhow::Result<()> {
    use primadb::Gun;
    use serde_json::json;

    let gun = Gun::new("compat-demo");
    let users = gun.get("users");
    users
        .get("alice")
        .put(json!({"name": "Alice", "profile": {"timezone": "UTC"}}))?;
    users
        .get("alice")
        .get("friends")
        .set(json!({"name": "Bob"}))?;

    let alice = users.get("alice").once()?.unwrap();
    println!("{}", serde_json::to_string_pretty(&alice)?);
    Ok(())
}
