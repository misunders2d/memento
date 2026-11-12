use super::*;
use memento_core::Store;
use memento_domain::{Graph, processing::Processing};
use std::{fs, os::unix::fs::PermissionsExt};

fn capture(store: &Store, conversation: &str, event: &str, kind: &str, metadata: Value) -> (String, i64) {
    let id = store
        .capture(serde_json::from_value(json!({"source":"native:fixture:pi","event_id":event,"conversation_id":conversation,"kind":kind,"text":format!("Synthetic {event} text"),"metadata":metadata})).unwrap())
        .unwrap()
        .id;
    let seq = store
        .read(|tx| Ok(tx.sql().query_row("SELECT seq FROM episodes WHERE id=?", [&id], |r| r.get(0))?))
        .unwrap();
    (id, seq)
}
fn summary(store: &Store, instant: chrono::DateTime<chrono::Utc>, conversations: usize, records: i64) -> Value {
    let indexing = json!({"available":false});
    store
        .read(|tx| bounded_summary(tx.sql(), &indexing, 0., instant, conversations, records))
        .unwrap()
}

#[test]
fn counts_extraction_input_messages_by_admission_jobs_and_parking() {
    let dir = tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap();
    let store = Store::open(dir.path()).unwrap();
    let (user, _) = capture(&store, "thread", "user", "user", json!({}));
    let (_, assistant) = capture(&store, "thread", "assistant", "assistant", json!({}));
    capture(&store, "thread", "tool", "tool", json!({}));
    capture(&store, "thread", "raw", "native_record", json!({"extraction_mode":"skip"}));
    capture(&store, "thread", "forgotten", "user", json!({}));
    store
        .write(|tx| {
            tx.sql().execute("INSERT INTO forgotten_sources(source,event_id,target,commit_id) VALUES('native:fixture:pi','forgotten','t','c')", [])?;
            Ok(())
        })
        .unwrap();
    let now = chrono::Utc::now();
    let idle = now + chrono::Duration::hours(1);

    // Active conversation without a compaction marker: everything waits.
    let value = summary(&store, now, 64, 1000);
    let remaining = &value["extraction"]["remaining"];
    assert_eq!(remaining["admissible_messages"], 0);
    assert_eq!(remaining["waiting_messages"], 2);
    assert_eq!(remaining["waiting_other"], 1);
    assert_eq!(remaining["counted_conversations"], 1);
    assert_eq!(remaining["partial"], false);
    assert!(value["extraction"].get("progress").is_none());
    assert_eq!(value["extraction"]["pending_conversations"], 1);
    assert_eq!(value["extraction"]["conversations"]["tracked"], 1);
    assert_eq!(value["extraction"]["conversations"]["up_to_date"], 0);

    // After 30 idle minutes the whole remainder is admissible.
    let remaining = summary(&store, idle, 64, 1000)["extraction"]["remaining"].clone();
    assert_eq!(remaining["admissible_messages"], 2);
    assert_eq!(remaining["admissible_other"], 1);
    assert_eq!(remaining["waiting_messages"], 0);

    // A compaction marker admits only through its sequence.
    store
        .write(|tx| {
            tx.sql().execute("UPDATE processing_progress SET compact_through=?", [assistant])?;
            Ok(())
        })
        .unwrap();
    let remaining = summary(&store, now, 64, 1000)["extraction"]["remaining"].clone();
    assert_eq!(remaining["admissible_messages"], 2);
    assert_eq!(remaining["admissible_other"], 0);
    assert_eq!(remaining["waiting_other"], 1);

    // The record bound reports partial instead of guessing.
    let remaining = summary(&store, now, 64, 2)["extraction"]["remaining"].clone();
    assert_eq!(remaining["partial"], true);
    assert_eq!(remaining["counted_conversations"], 0);
    let remaining = summary(&store, now, 0, 1000)["extraction"]["remaining"].clone();
    assert_eq!(remaining["partial"], true);

    // Batched messages count distinct episodes, not windows.
    let payload = json!({"source":"native:fixture:pi","conversation_id":"thread","windows":[{"id":"a","episode_id":user,"start":0,"end":5},{"id":"b","episode_id":user,"start":4,"end":9}]});
    store
        .write(|tx| {
            tx.sql().execute("INSERT INTO jobs(id,job_key,kind,payload,state,available_at,attempts) VALUES('live','live','extract',?,'leased',0,1)", [payload.to_string()])?;
            Ok(())
        })
        .unwrap();
    let value = summary(&store, now, 64, 1000);
    assert_eq!(value["extraction"]["batched"]["leased_live"], json!({"jobs":1,"messages":1,"other":0}));
    assert_eq!(value["extraction"]["batched"]["pending_live"], json!({"jobs":0,"messages":0,"other":0}));
    assert_eq!(value["extraction"]["conversations"]["in_live_job"], 1);
    assert_eq!(value["extraction"]["leased_jobs"], 1);

    // Parking moves the unextracted range out of live work.
    store
        .write(|tx| {
            tx.sql().execute("DELETE FROM jobs", [])?;
            Ok(())
        })
        .unwrap();
    Processing::new(Graph::new(store.clone())).park_history().unwrap();
    let value = summary(&store, idle, 64, 1000);
    assert_eq!(value["extraction"]["pending_conversations"], 0);
    assert_eq!(value["extraction"]["parked_conversations"], 1);
    assert_eq!(value["extraction"]["conversations"]["parked_live_up_to_date"], 1);
    assert_eq!(value["extraction"]["remaining"]["admissible_messages"], 0);
    assert_eq!(value["historical"]["parked_conversations"], 1);
    assert_eq!(value["historical"]["active"], true);
}
