use super::*;
use crate::processing::Release;

fn claim(f: &Fixture, hour: u32) -> Value {
    f.processing.claim_at(Claim {
        agent: "fixture".into(), timezone_name: "UTC".into(), compaction: None,
        exclude: vec![], consolidate_now: false, kinds: None,
        consolidation_start: Some("01:00".into()), consolidation_end: Some("08:00".into()),
        continuous: true, batch_size: 16,
    }, chrono::DateTime::parse_from_rfc3339(&format!("2026-11-09T{hour:02}:00:00Z")).unwrap().to_utc()).unwrap()
}
fn empty(f: &Fixture, work: &Value) {
    f.complete(work, json!({"windows":work["input"]["windows"].as_array().unwrap().iter()
        .map(|w|json!({"id":w["id"],"candidates":[]})).collect::<Vec<_>>()})).unwrap();
}

#[test]
fn historical_nightly_retry_yields_to_fresh_same_conversation_and_preserves_both_cursors() {
    let f = Fixture::new();
    let old = f.seed();
    f.processing.park_history().unwrap();
    assert!(claim(&f, 0)["work"].as_array().unwrap().is_empty());
    let first = claim(&f, 1)["work"][0].clone();
    assert_eq!(first["kind"], "extract");
    assert_eq!(first["input"]["windows"][0]["text"], FACT);
    assert_eq!(f.stored::<i64>("SELECT seq FROM processing_progress"), 0);
    f.processing.release(Release { id: first["id"].as_str().unwrap().into(),
        owner: first["owner"].as_str().unwrap().into(), error: "service_rejected".into() }).unwrap();
    f.graph.store.write(|tx| { tx.sql().execute("UPDATE jobs SET available_at=0 WHERE state='pending'", [])?; Ok(()) }).unwrap();
    let outside = claim(&f, 8);
    assert!(outside["work"].as_array().unwrap().is_empty());
    assert_eq!(outside["backlog"]["claimable"]["extract"], 0);

    let fresh = f.capture("fresh", "thread", "Fresh evidence must go first.", "user", "contract", json!({}), "2026-11-08T15:00:00Z");
    let live = claim(&f, 2)["work"][0].clone();
    assert_eq!(live["input"]["windows"][0]["text"], "Fresh evidence must go first.");
    assert_ne!(live["id"], first["id"]);
    empty(&f, &live);
    assert_eq!(f.stored::<i64>("SELECT seq FROM processing_progress"), 0);
    let live_cursor = f.stored::<i64>("SELECT live_seq FROM historical_parking");
    let resumed = claim(&f, 2)["work"][0].clone();
    assert_eq!(resumed["id"], first["id"]);
    empty(&f, &resumed);
    assert_eq!(f.stored::<i64>("SELECT live_seq FROM historical_parking"), live_cursor);
    assert_eq!(f.processing.history_status().unwrap()["pending_conversations"], 0);
    assert_eq!(f.processing.history_status().unwrap()["extracted_conversations"], 1);
    assert!(f.graph.store.read(|tx| tx.original_text(&old)).unwrap().is_some());
    assert!(f.graph.store.read(|tx| tx.original_text(&fresh)).unwrap().is_some());

    let Fixture { _dir: dir, graph, processing } = f;
    drop(processing); drop(graph);
    let reopened = Processing::new(Graph::new(Store::open(dir.path()).unwrap()));
    assert_eq!(reopened.history_status().unwrap()["pending_conversations"], 0);
    assert_eq!(reopened.history_status().unwrap()["extracted_conversations"], 1);
}

#[test]
fn historical_nightly_waits_for_fresh_consolidation_and_skips_excluded_input() {
    let f = Fixture::new();
    f.seed();
    f.capture("skip", "thread", "Excluded synthetic record.", "native_record", "contract", json!({"extraction_mode":"skip"}), "2026-10-18T12:00:00Z");
    f.processing.park_history().unwrap();
    f.capture("new", "fresh", "Copper shields are durable.", "user", "contract", json!({}), "2026-11-08T15:00:00Z");
    f.extract("contract", "fresh", "Copper shields are durable.", "knowledge", false);
    let consolidation = claim(&f, 2);
    assert_eq!(consolidation["work"].as_array().unwrap().len(), 1);
    assert_eq!(consolidation["work"][0]["kind"], "consolidate");
    assert_eq!(f.processing.history_status().unwrap()["queued_jobs"], 0);
    let work = &consolidation["work"][0];
    let actions = work["input"]["candidates"].as_array().unwrap().iter().map(|c| action(c, "reject")).collect::<Vec<_>>();
    f.complete(work, json!({"actions":actions})).unwrap();
    let history = claim(&f, 2)["work"][0].clone();
    assert_eq!(history["input"]["windows"].as_array().unwrap().len(), 1);
    assert_eq!(history["input"]["windows"][0]["text"], FACT);
    empty(&f, &history);
    assert_eq!(f.processing.history_status().unwrap()["pending_conversations"], 0);
}

#[test]
fn historical_nightly_partial_batch_resumes_after_reopen_while_active_chat_waits() {
    let f = Fixture::new();
    let text = "Synthetic copper shielding evidence. ".repeat(2000);
    f.capture("long", "thread", &text, "user", "contract", json!({}), "2026-10-18T12:00:00Z");
    f.processing.park_history().unwrap();
    f.capture("active", "active", "Still discussing a fresh topic.", "user", "contract", json!({}), "2026-11-09T01:59:00Z");
    f.graph.store.write(|tx| { tx.sql().execute("UPDATE processing_progress SET latest_at='2026-11-09T01:59:00Z' WHERE conversation_id='active'", [])?; Ok(()) }).unwrap();
    let first = claim(&f, 2)["work"][0].clone();
    assert!(first["input"]["windows"].as_array().unwrap().iter().all(|w| w["text"].as_str().unwrap().contains("shielding")));
    empty(&f, &first);
    let offset = f.stored::<i64>("SELECT offset FROM processing_progress WHERE conversation_id='thread'");
    assert!(offset > 0);
    assert_eq!(f.processing.history_status().unwrap()["pending_conversations"], 1);
    let Fixture { _dir: dir, graph, processing } = f;
    drop(processing); drop(graph);
    let graph = Graph::new(Store::open(dir.path()).unwrap());
    let reopened = Fixture { processing: Processing::new(graph.clone()), graph, _dir: dir };
    let next = claim(&reopened, 2)["work"][0].clone();
    assert_ne!(first["id"], next["id"]);
    let start = reopened.stored::<i64>("SELECT json_extract(payload,'$.windows[0].start') FROM jobs WHERE state='leased'");
    assert_eq!(start, offset);
    empty(&reopened, &next);
    assert_eq!(reopened.processing.history_status().unwrap()["pending_conversations"], 0);
    assert_eq!(reopened.stored::<i64>("SELECT seq FROM processing_progress WHERE conversation_id='active'"), 0);
}
