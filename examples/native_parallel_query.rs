use primadb::{Primadb, QueryDirection, parallel_enabled, parallel_thread_count};
use serde_json::json;

fn main() -> primadb::Result<()> {
    let db = Primadb::with_replica_id("native-parallel");
    let total = 1_500;

    for index in 0..total {
        db.root("notes")
            .field(format!("note-{index:05}"))
            .put(json!({
                "title": format!("Note {index:05}"),
                "priority": index % 5,
                "archived": index % 7 == 0,
            }))?;
    }

    let matches = db
        .root("notes")
        .find()
        .where_eq("archived", false)?
        .where_gte("priority", 2)?
        .order_by("title", QueryDirection::Asc)
        .limit(256)
        .run()?;

    println!("parallel_enabled={}", parallel_enabled());
    println!("parallel_threads={}", parallel_thread_count());
    println!("result_count={}", matches.len());
    println!(
        "first_key={}",
        matches
            .first()
            .map(|entry| entry.key.as_str())
            .unwrap_or("<none>")
    );

    Ok(())
}
