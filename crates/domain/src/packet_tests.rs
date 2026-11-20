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
