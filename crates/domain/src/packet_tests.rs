//! Model packet contract: stated extraction rules, actionable coverage rejections, the
//! compact consolidation view and claim measurement counts; no model calls.
use super::*;

const CLAIM: &str = "Orion expedition preserves signed archives.";
const SUBJECT: &str = "Orion expedition";
const OTHER: &str = "The Orion expedition departs from the copper harbour.";

fn work_action(f: &Fixture, action: &str, command: Value) -> Result<Value> {
    f.processing
        .work(serde_json::from_value(json!({"action":action,"command":command})).unwrap())
}

#[test]
fn extraction_policy_states_window_coverage_and_window_local_endpoints() {
    let f = Fixture::new();
    // The Pi worker's system prefix is this pool policy text: its adapter drops the
    // per-claim instruction, so a rule missing here never reaches the model.
    let pool = work_action(&f, "pool", json!({"kind":"extract","policies":true})).unwrap();
    let policy = pool["policies"]["extract"]["instruction"].as_str().unwrap();
    assert_eq!(policy, pipeline_schema::EXTRACTION_INSTRUCTION);
    for rule in [
        "exactly one windows entry for every supplied window id",
        "with candidates: [] when that window supports nothing durable",
        "Never omit, repeat or invent a window id",
        "declared in the same window entry",
        "declare it again in this window with its own exact supporting quote",
    ] {
        assert!(policy.contains(rule), "extraction policy lost: {rule}");
    }
}

/// Two windows: the subject entity in the first, the claim and its link in the second.
fn two_window_link(f: &Fixture) -> (Value, Value, Value, Value) {
    for (event, text) in [("subject", OTHER), ("claim", CLAIM)] {
        f.capture(
            event,
            "thread",
            text,
            "user",
            "contract",
            json!({}),
            "2026-10-18T12:00:00Z",
        );
    }
    let work = f.claim("contract", "thread", Some(vec!["extract".into()]))["work"][0].clone();
    assert_eq!(work["input"]["windows"].as_array().unwrap().len(), 2);
    let mut entity = draft(SUBJECT, "project", false);
    entity["key"] = json!("subject");
    let mut memory = draft(CLAIM, "knowledge", false);
    memory["key"] = json!("claim");
    let mut relation = draft(CLAIM, "knowledge", false);
    relation["key"] = json!("subject-link");
    relation["kind"] = json!("relation");
    relation["type"] = json!("mentions");
    relation["src_key"] = json!("claim");
    relation["dst_key"] = json!("subject");
    (work, entity, memory, relation)
}

#[test]
fn relation_endpoint_from_another_window_is_rejected_until_declared_in_its_own_window() {
    let f = Fixture::new();
    let (work, entity, memory, relation) = two_window_link(&f);
    let error = f
        .complete(
            &work,
            json!({
                "windows": [
                    {"id":"w0","candidates":[entity.clone()]},
                    {"id":"w1","candidates":[memory.clone(),relation.clone()]}
                ]
            }),
        )
        .unwrap_err();
    assert_eq!(error.to_string(), "candidate_relation_endpoint_missing");
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM candidates"), 0);
    // What the policy now tells the model to do instead.
    let result = f
        .complete(
            &work,
            json!({
                "windows": [
                    {"id":"w0","candidates":[entity.clone()]},
                    {"id":"w1","candidates":[memory,entity,relation]}
                ]
            }),
        )
        .unwrap();
    assert_eq!(result["candidate_count"], 4);
}

/// `count` one-sentence user windows in one leased extraction job.
fn windowed(f: &Fixture, count: usize) -> Value {
    for index in 0..count {
        f.capture(
            &format!("event{index}"),
            "thread",
            &format!("Module {index} uses copper shielding."),
            "user",
            "contract",
            json!({}),
            "2026-10-18T12:00:00Z",
        );
    }
    let work = f.claim("contract", "thread", Some(vec!["extract".into()]))["work"][0].clone();
    assert_eq!(work["input"]["windows"].as_array().unwrap().len(), count);
    work
}
fn empty_windows(ids: &[&str]) -> Value {
    json!({"windows":ids.iter().map(|id| json!({"id":id,"candidates":[]})).collect::<Vec<_>>()})
}
/// Rejects `ids` for coverage and returns the stored feedback and this request's counts.
fn coverage_rejected(f: &Fixture, work: &Value, ids: &[&str]) -> (Value, Vec<(&'static str, u64)>) {
    crate::work_timing::take_counts();
    let error = f.complete(work, empty_windows(ids)).unwrap_err();
    // The caller, receipts and journals still see only the fixed code.
    assert_eq!(error.to_string(), "candidate_window_coverage_invalid");
    let counts = crate::work_timing::take_counts().into_iter().collect();
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM graph_commits"), 0);
    assert_eq!(f.stored::<i64>("SELECT seq FROM processing_progress"), 0);
    let context: Value =
        serde_json::from_str(&f.stored::<String>("SELECT context FROM jobs")).unwrap();
    let rejection = context["previous_rejection"].clone();
    assert_eq!(rejection["code"], "candidate_window_coverage_invalid");
    (rejection, counts)
}

#[test]
fn coverage_rejection_names_missing_unexpected_and_duplicate_windows() {
    let f = Fixture::new();
    let work = windowed(&f, 3);
    // Truncated output: still rejected, never accepted as empty windows.
    let (rejection, counts) = coverage_rejected(&f, &work, &["w0"]);
    assert_eq!(
        rejection["references"],
        json!([{"window_id":"w1","problem":"missing"},{"window_id":"w2","problem":"missing"}])
    );
    assert_eq!(
        counts,
        [
            ("apply_windows_duplicate", 0),
            ("apply_windows_missing", 2),
            ("apply_windows_unexpected", 0)
        ]
    );
    let (rejection, counts) = coverage_rejected(&f, &work, &["w0", "w1", "w2", "w7"]);
    assert_eq!(
        rejection["references"],
        json!([{"window_id":"w7","problem":"unexpected"}])
    );
    assert_eq!(
        counts,
        [
            ("apply_windows_duplicate", 0),
            ("apply_windows_missing", 0),
            ("apply_windows_unexpected", 1)
        ]
    );
    let (rejection, counts) = coverage_rejected(&f, &work, &["w0", "w1", "w2", "w1"]);
    assert_eq!(
        rejection["references"],
        json!([{"window_id":"w1","problem":"duplicate"}])
    );
    assert_eq!(
        counts,
        [
            ("apply_windows_duplicate", 1),
            ("apply_windows_missing", 0),
            ("apply_windows_unexpected", 0)
        ]
    );
    // Every problem present is named before a second reference of the same kind.
    let (rejection, counts) = coverage_rejected(&f, &work, &["w0", "w0", "w9"]);
    assert_eq!(
        rejection["references"],
        json!([
            {"window_id":"w1","problem":"missing"},
            {"window_id":"w9","problem":"unexpected"},
            {"window_id":"w0","problem":"duplicate"}
        ])
    );
    assert_eq!(
        counts,
        [
            ("apply_windows_duplicate", 1),
            ("apply_windows_missing", 2),
            ("apply_windows_unexpected", 1)
        ]
    );
}

#[test]
fn coverage_rejection_keeps_three_references_and_reaches_the_retry_claim() {
    let f = Fixture::new();
    let work = windowed(&f, 12);
    let (rejection, counts) = coverage_rejected(&f, &work, &["w3"]);
    // Leased window order (w2 before w10), capped at three.
    assert_eq!(
        rejection["references"],
        json!([
            {"window_id":"w0","problem":"missing"},
            {"window_id":"w1","problem":"missing"},
            {"window_id":"w2","problem":"missing"}
        ])
    );
    assert!(counts.contains(&("apply_windows_missing", 11)));
    f.processing
        .release(processing::Release {
            id: work["id"].as_str().unwrap().into(),
            owner: work["owner"].as_str().unwrap().into(),
            error: "service_rejected".into(),
        })
        .unwrap();
    f.graph
        .store
        .write(|tx| {
            tx.sql().execute("UPDATE jobs SET available_at=0", [])?;
            Ok(())
        })
        .unwrap();
    let retry = f.claim("contract", "thread", Some(vec!["extract".into()]))["work"][0].clone();
    assert_eq!(retry["id"], work["id"]);
    assert_eq!(
        retry["input"]["previous_rejection"]["references"],
        rejection["references"]
    );
    assert_eq!(retry["input"]["previous_rejection"]["attempt"], 1);
    let ids = (0..12).map(|index| format!("w{index}")).collect::<Vec<_>>();
    let ids = ids.iter().map(String::as_str).collect::<Vec<_>>();
    f.complete(&retry, empty_windows(&ids)).unwrap();
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM graph_commits"), 1);
}
