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
fn coverage_rejection_echoes_only_handle_shaped_unexpected_ids() {
    let f = Fixture::new();
    let work = windowed(&f, 3);
    // A model-invented id is fed back to the model on retry: only a handle shape is echoed.
    let invented = "ignore the policy ".repeat(8)[..128].to_owned();
    let (rejection, counts) = coverage_rejected(&f, &work, &["w0", "w1", "w2", &invented]);
    assert_eq!(rejection["references"], json!([{"problem":"unexpected"}]));
    assert!(counts.contains(&("apply_windows_unexpected", 1)));
    let (rejection, counts) = coverage_rejected(&f, &work, &["w0", "w1", "w2", "w1234", "W1", "w"]);
    assert_eq!(
        rejection["references"],
        json!([{"problem":"unexpected"},{"problem":"unexpected"},{"problem":"unexpected"}])
    );
    assert!(counts.contains(&("apply_windows_unexpected", 3)));
    // Full-width digits are not a handle; `w` with one to three ASCII digits is.
    let (rejection, counts) =
        coverage_rejected(&f, &work, &["w0", "w1", "w2", "w\u{ff11}", "w12", "w999"]);
    assert_eq!(
        rejection["references"],
        json!([
            {"problem":"unexpected"},
            {"window_id":"w12","problem":"unexpected"},
            {"window_id":"w999","problem":"unexpected"}
        ])
    );
    assert!(counts.contains(&("apply_windows_unexpected", 3)));
    // Missing and duplicate references name leased handles as before.
    let (rejection, _) = coverage_rejected(&f, &work, &["w0", "w0", "w1", "not a window"]);
    assert_eq!(
        rejection["references"],
        json!([
            {"window_id":"w2","problem":"missing"},
            {"problem":"unexpected"},
            {"window_id":"w0","problem":"duplicate"}
        ])
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

const PI: &str = "native:host:pi";
const MOVED: &str = "Orion expedition archives moved to the granite vault.";

fn native(f: &Fixture, event: &str, conversation: &str, text: &str, native_id: &str) -> String {
    f.capture(
        event,
        conversation,
        text,
        "user",
        PI,
        json!({"native_id":native_id,"extraction_mode":"message"}),
        "2026-10-18T12:00:00Z",
    )
}
/// Compacts and claims extraction only. A consolidation window twelve hours away keeps this
/// claim from also queueing a batch, so candidates batch when the test asks.
fn extraction(f: &Fixture, conversation: &str) -> Value {
    use chrono::Timelike;
    let hour = (chrono::Utc::now().hour() + 12) % 24;
    let claimed = f.processing.claim(Claim {
        agent: "fixture".into(),
        timezone_name: "UTC".into(),
        compaction: Some(Compaction {
            source: PI.into(),
            conversation_id: conversation.into(),
        }),
        exclude: vec![],
        consolidate_now: false,
        kinds: Some(vec!["extract".into()]),
        consolidation_start: Some(format!("{hour:02}:00")),
        consolidation_end: Some(format!("{hour:02}:01")),
        continuous: false,
        batch_size: 16,
    });
    claimed.unwrap()["work"][0].clone()
}
fn consolidation(f: &Fixture, size: usize) -> Value {
    let command = json!({
        "agent":"fixture",
        "timezone":"UTC",
        "kinds":["consolidate"],
        "consolidate_now":true,
        "continuous":true,
        "batch_size":size
    });
    work_action(f, "claim", command).unwrap()["work"][0].clone()
}
/// The leased context stored for the one live consolidation lease.
fn leased_context(f: &Fixture) -> Value {
    serde_json::from_str(
        &f.stored::<String>("SELECT context FROM jobs WHERE kind='consolidate' AND state='leased'"),
    )
    .unwrap()
}
/// Every supplied source reference in packet order: candidate, node and relation evidence, then raw windows.
fn source_references(packet: &Value) -> Vec<&Value> {
    let mut found = vec![];
    for section in ["candidates", "existing_nodes", "existing_relations"] {
        for item in packet[section].as_array().unwrap() {
            found.extend(item["evidence"].as_array().unwrap());
        }
    }
    found.extend(packet["raw_windows"].as_array().unwrap());
    found
}
/// An existing claim -> subject relation from conversation A, then three candidates:
/// two projections of one native record in conversation B and one more from A.
fn mixed_packet(f: &Fixture) -> Value {
    native(f, "message:a1", "conversation-a", CLAIM, "native-a1");
    let work = extraction(f, "conversation-a");
    let mut memory = draft(CLAIM, "knowledge", false);
    memory["key"] = json!("claim");
    let mut entity = draft(SUBJECT, "project", false);
    entity["key"] = json!("subject");
    let mut relation = draft(CLAIM, "knowledge", false);
    relation["key"] = json!("subject-link");
    relation["kind"] = json!("relation");
    relation["type"] = json!("mentions");
    relation["src_key"] = json!("claim");
    relation["dst_key"] = json!("subject");
    f.complete(
        &work,
        json!({"windows":[{"id":"w0","candidates":[memory,entity,relation]}]}),
    )
    .unwrap();
    let work = consolidation(f, 16);
    let candidates = work["input"]["candidates"].as_array().unwrap();
    let claim = candidates.iter().find(|c| c["kind"] == "memory").unwrap();
    let subject = candidates.iter().find(|c| c["kind"] == "entity").unwrap();
    let relation = candidates.iter().find(|c| c["kind"] == "relation").unwrap();
    let mut link = action(relation, "link");
    link["type"] = Value::Null;
    link["text"] = Value::Null;
    link["scope"] = Value::Null;
    link["src"] = claim["id"].clone();
    link["dst"] = subject["id"].clone();
    link["relation"] = json!("mentions");
    f.complete(
        &work,
        json!({"actions":[link,action(claim,"remember"),action(subject,"remember")]}),
    )
    .unwrap();
    native(f, "message:b1", "conversation-b", OTHER, "native-b1");
    native(f, "output:b1:0", "conversation-b", MOVED, "native-b1");
    let work = extraction(f, "conversation-b");
    f.complete(
        &work,
        json!({
            "windows": [
                {"id":"w0","candidates":[draft(SUBJECT,"project",false)]},
                {"id":"w1","candidates":[draft(MOVED,"knowledge",false)]}
            ]
        }),
    )
    .unwrap();
    native(f, "message:a2", "conversation-a", OTHER, "native-a2");
    let work = extraction(f, "conversation-a");
    f.complete(
        &work,
        json!({"windows":[{"id":"w0","candidates":[draft(OTHER,"knowledge",false)]}]}),
    )
    .unwrap();
    consolidation(f, 16)
}

#[test]
fn consolidation_view_drops_bookkeeping_and_keeps_the_leased_context_whole() {
    let f = Fixture::new();
    let work = mixed_packet(&f);
    let (view, leased) = (&work["input"], leased_context(&f));
    assert_eq!(view["candidates"].as_array().unwrap().len(), 3);
    assert!(!view["existing_nodes"].as_array().unwrap().is_empty());
    assert!(!view["existing_relations"].as_array().unwrap().is_empty());
    // The server-side context that every validator reads keeps every field.
    assert_eq!(leased["loop_policy_version"], consolidation::VERSION);
    for (section, fields) in [
        ("candidates", &["run_id", "position", "decision_id"][..]),
        (
            "existing_nodes",
            &["commit_id", "superseded_by", "state"][..],
        ),
        ("existing_relations", &["commit_id", "state"][..]),
    ] {
        let supplied = view[section].as_array().unwrap();
        let stored = leased[section].as_array().unwrap();
        assert_eq!(supplied.len(), stored.len());
        for (shown, kept) in supplied.iter().zip(stored) {
            for field in fields {
                assert!(kept.get(*field).is_some(), "leased {section} lost {field}");
                assert!(shown.get(*field).is_none(), "{section} still shows {field}");
            }
            if section != "candidates" {
                assert_eq!(kept["state"], "active");
            }
            // Everything else is supplied unchanged.
            for (key, value) in kept.as_object().unwrap() {
                if !fields.contains(&key.as_str()) && key != "evidence" {
                    assert_eq!(&shown[key], value, "{section}.{key}");
                }
            }
            for (shown, kept) in shown["evidence"]
                .as_array()
                .unwrap()
                .iter()
                .zip(kept["evidence"].as_array().unwrap())
            {
                for (key, value) in kept.as_object().unwrap() {
                    if !["conversation_id", "event_id", "native_id"].contains(&key.as_str()) {
                        assert_eq!(&shown[key], value, "{section}.evidence.{key}");
                    }
                }
            }
        }
    }
    assert!(view.get("loop_policy_version").is_none());
    for key in [
        "raw_windows",
        "resolved_candidates",
        "behavior_exposures",
        "maintenance_ids",
        "context_truncated",
        "retirement_ready",
        "timezone",
    ] {
        assert!(view.get(key).is_some(), "view lost {key}");
    }
    assert!(
        view.as_object()
            .unwrap()
            .keys()
            .all(|key| !key.starts_with('_'))
    );
    // A snapshot carrying a date or choice is supplied whole; an all-null one says nothing.
    let stored = leased["loop_snapshots"].as_object().unwrap();
    assert!(!stored.is_empty());
    for (id, snapshot) in stored {
        if snapshot.as_object().unwrap().values().all(Value::is_null) {
            assert!(view["loop_snapshots"].get(id).is_none());
        } else {
            assert_eq!(&view["loop_snapshots"][id], snapshot);
        }
    }
}

#[test]
fn consolidation_view_labels_keep_source_identity_equality_across_the_packet() {
    let f = Fixture::new();
    let work = mixed_packet(&f);
    let leased = leased_context(&f);
    let (shown, kept) = (
        source_references(&work["input"]),
        source_references(&leased),
    );
    assert_eq!(shown.len(), kept.len());
    for (key, prefix) in [
        ("conversation_id", 'k'),
        ("event_id", 'e'),
        ("native_id", 'n'),
    ] {
        let mut labels = std::collections::BTreeMap::new();
        let mut originals = std::collections::BTreeMap::new();
        for (shown, kept) in shown.iter().zip(&kept) {
            assert_eq!(shown["episode_id"], kept["episode_id"]);
            // A native id names a record only within its source conversation.
            let original = if key == "native_id" {
                json!([kept["source"], kept["conversation_id"], kept[key]]).to_string()
            } else {
                kept[key].as_str().unwrap().to_owned()
            };
            let label = shown[key].as_str().unwrap();
            assert!(
                label
                    .strip_prefix(prefix)
                    .is_some_and(|number| number.parse::<usize>().is_ok()),
                "{key} label {label}"
            );
            // Equal originals share one label and different originals never do.
            assert_eq!(*labels.entry(original.clone()).or_insert(label), label);
            assert_eq!(
                *originals.entry(label).or_insert(original.clone()),
                original
            );
        }
        let expected = match key {
            // Conversations A and B.
            "conversation_id" => 2,
            // Four source episodes; b1's two projections share one native record.
            "event_id" => 4,
            _ => 3,
        };
        assert_eq!(labels.len(), expected, "{key}");
        // Numbered by first appearance in packet order.
        assert_eq!(shown[0][key], json!(format!("{prefix}1")));
    }
    // The policy explains the labels it now receives.
    let policy = pipeline_schema::CONSOLIDATION_INSTRUCTION
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        policy.contains(
            "labels local to this input (conversation_id k1…, event_id e1…, native_id n1…)"
        )
    );
    // MCP read pages rebuild the same view from the stored context.
    let mut text = String::new();
    loop {
        let page = work_action(
            &f,
            "read",
            json!({"id":work["id"],"owner":work["owner"],"after":text.chars().count()}),
        )
        .unwrap();
        text.push_str(page["text"].as_str().unwrap());
        if page["next"].is_null() {
            break;
        }
    }
    let read: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(read["input"], work["input"]);
    // A plan citing evidence by real episode_id and quote still validates and commits.
    let candidates = work["input"]["candidates"].as_array().unwrap();
    let subject = work["input"]["existing_nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["text"] == SUBJECT)
        .unwrap();
    let actions = candidates
        .iter()
        .map(|candidate| {
            if candidate["kind"] == "entity" {
                let mut duplicate = action(candidate, "duplicate");
                duplicate["target"] = subject["id"].clone();
                duplicate
            } else {
                action(candidate, "remember")
            }
        })
        .collect::<Vec<_>>();
    let result = f.complete(&work, json!({"actions":actions})).unwrap();
    assert_eq!(result["candidate_count"], 3);
}

#[test]
fn consolidation_view_native_labels_never_join_two_conversations() {
    // Some harnesses number native records per conversation (`step:3`), so equal native
    // ids from another conversation or source are different records.
    let item = |source: Option<&str>, conversation: &str, event: &str| {
        let mut item = json!({"episode_id":event,"conversation_id":conversation,"event_id":event,"native_id":"step:3"});
        if let Some(source) = source {
            item["source"] = json!(source);
        }
        item
    };
    let (pi, claude) = (Some("native:host:pi"), Some("native:host:claude"));
    let context = json!({
        "candidates":[{"id":"c_1","evidence":[item(pi,"thread-a","message:a3")]}],
        "existing_nodes":[{"id":"n_1","evidence":[item(pi,"thread-b","message:b3")]}],
        "existing_relations":[],
        "raw_windows":[
            // The same native record as the candidate's evidence: another projection of it.
            item(pi,"thread-a","output:a3:0"),
            item(claude,"thread-a","message:c3"),
            item(None,"thread-b","message:x3")
        ],
        // A loop event id is not a source event, even when the strings are equal.
        "loop_snapshots":{"n_1":{"event_id":"message:a3","state":"open","pending_source_at":null,"manual_choice_at":null}}
    });
    let view = consolidation::Consolidator::model_input(&context);
    let shown = source_references(&view);
    let labels = |key: &str| {
        shown
            .iter()
            .map(|item| item[key].clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        labels("native_id"),
        json!(["n1", "n2", "n1", "n3", "n4"])
            .as_array()
            .unwrap()
            .clone()
    );
    // Conversation and event labels keep keying on the stored id alone.
    assert_eq!(
        labels("conversation_id"),
        json!(["k1", "k2", "k1", "k1", "k2"])
            .as_array()
            .unwrap()
            .clone()
    );
    assert_eq!(
        labels("event_id"),
        json!(["e1", "e2", "e3", "e4", "e5"])
            .as_array()
            .unwrap()
            .clone()
    );
    assert_eq!(view["loop_snapshots"], context["loop_snapshots"]);
    // The policy says where the labels appear and what equal labels mean.
    let policy = pipeline_schema::CONSOLIDATION_INSTRUCTION
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    assert!(policy.contains(
        "For this purpose evidence references and raw windows show their source event/conversation/native identities as \
        labels local to this input (conversation_id k1…, event_id e1…, native_id n1…): equal conversation or event labels \
        with the same source mean the same original conversation or event, and equal native labels mean the same native \
        record of one source conversation."
    ));
    assert_eq!(view, consolidation::Consolidator::model_input(&context));
}

#[test]
fn consolidation_view_keeps_anything_that_is_not_plain_bookkeeping() {
    let reference = |conversation: Value, native: Value| json!({"episode_id":"e","quote":"q","conversation_id":conversation,"event_id":"message:x","native_id":native});
    let context = json!({
        "loop_policy_version":"consolidate-v0",
        "candidates":[{"id":"c_1","run_id":"job/window","position":7,"decision_id":"d","evidence":[reference(json!("thread"),Value::Null)]}],
        "existing_nodes":[
            {"id":"n1","commit_id":"x","state":"active","superseded_by":null,"evidence":[reference(json!("thread"),json!("record"))]},
            {"id":"n2","commit_id":"x","state":"refined","superseded_by":"n1","evidence":[]}
        ],
        "loop_snapshots":{
            "c_1":{"event_id":null,"state":null,"pending_source_at":null,"manual_choice_at":null},
            "n1":{"event_id":"loop-event","state":"open","pending_source_at":null,"manual_choice_at":null},
            "n2":{"event_id":null,"state":null,"pending_source_at":"2026-10-18T12:00:00Z","manual_choice_at":null}
        },
        "_deferred":["c_2"]
    });
    let view = consolidation::Consolidator::model_input(&context);
    assert_eq!(
        view,
        json!({
            "candidates":[{"id":"c_1","evidence":[{"episode_id":"e","quote":"q","conversation_id":"k1","event_id":"e1","native_id":null,"local_time":null}]}],
            "existing_nodes":[
                {"id":"n1","evidence":[{"episode_id":"e","quote":"q","conversation_id":"k1","event_id":"e1","native_id":"n1"}]},
                // Never produced by preparation; shown if it ever is.
                {"id":"n2","state":"refined","superseded_by":"n1","evidence":[]}
            ],
            // A loop event id is a different identity from a source event and stays as stored.
            "loop_snapshots":{
                "n1":context["loop_snapshots"]["n1"],
                "n2":context["loop_snapshots"]["n2"]
            }
        })
    );
    // The same stored context always yields the same view.
    assert_eq!(view, consolidation::Consolidator::model_input(&context));
}

/// A 16-candidate update batch against 48 existing assertions with uuid-sized source
/// identities and ~900-character messages, the shape a production packet has.
fn update_packet(f: &Fixture) -> Value {
    let (c, v) = (b"bdfgklmnprstvz", b"aeiou");
    let names = (0..48)
        .map(|i| {
            format!(
                "{}{}{}{}lor",
                (c[i % 14] as char).to_ascii_uppercase(),
                v[i / 14 % 5] as char,
                c[(i * 5 + 3) % 14] as char,
                v[i % 5] as char
            )
        })
        .collect::<Vec<_>>();
    let extract = |facts: &[String]| {
        let conversation = util::id();
        for (index, fact) in facts.iter().enumerate() {
            let filler =
                format!("Routine scheduling note {index} about unrelated logistics and travel. ")
                    .repeat(6);
            native(
                f,
                &format!("message:{}", util::id()),
                &conversation,
                &format!("{filler}{fact} {filler}"),
                &util::id(),
            );
        }
        let work = extraction(f, &conversation);
        let windows = work["input"]["windows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|window| {
                let text = window["text"].as_str().unwrap();
                let fact = facts
                    .iter()
                    .find(|fact| text.contains(fact.as_str()))
                    .unwrap();
                json!({"id":window["id"],"candidates":[draft(fact,"knowledge",false)]})
            })
            .collect::<Vec<_>>();
        f.complete(&work, json!({"windows":windows})).unwrap();
    };
    // Three projects per guild, so each update candidate discovers three distinct targets.
    let guilds = [
        "amber", "birch", "cedar", "delta", "ember", "flint", "grove", "heron", "iris", "jade",
        "kelp", "larch", "maple", "nori", "opal", "pine",
    ];
    let fact = |index: usize, month: &str| {
        format!(
            "Project {} of the {} guild ships in {month}.",
            names[index],
            guilds[index % 16]
        )
    };
    for chunk in (0..48).collect::<Vec<_>>().chunks(12) {
        let facts = chunk
            .iter()
            .map(|index| fact(*index, "March"))
            .collect::<Vec<_>>();
        extract(&facts);
    }
    let work = consolidation(f, 64);
    let candidates = work["input"]["candidates"].as_array().unwrap();
    assert_eq!(candidates.len(), 48);
    let actions = candidates
        .iter()
        .map(|candidate| action(candidate, "remember"))
        .collect::<Vec<_>>();
    f.complete(&work, json!({"actions":actions})).unwrap();
    let facts = (0..16)
        .map(|index| fact(index, "April"))
        .collect::<Vec<_>>();
    extract(&facts);
    consolidation(f, 16)
}

#[test]
fn consolidation_view_is_smaller_than_the_leased_context_it_is_built_from() {
    let f = Fixture::new();
    let work = update_packet(&f);
    let leased = leased_context(&f);
    assert_eq!(work["input"]["candidates"].as_array().unwrap().len(), 16);
    assert_eq!(
        work["input"]["existing_nodes"].as_array().unwrap().len(),
        48
    );
    let public = Value::Object(
        leased
            .as_object()
            .unwrap()
            .iter()
            .filter(|(key, _)| !key.starts_with('_'))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    );
    let bytes = |value: &Value| serde_json::to_vec(value).unwrap().len();
    let (view, full) = (bytes(&work["input"]), bytes(&public));
    eprintln!(
        "PACKET consolidation candidates=16 existing_nodes=48 view_bytes={view} leased_public_bytes={full}"
    );
    assert!(view * 100 < full * 90, "view {view} of {full} bytes");
}

#[test]
fn claim_timing_counts_model_input_bytes_and_tool_versus_message_windows() {
    let f = Fixture::new();
    f.seed();
    // Tool output is projected to its first 2,000 characters; a tool_output record of
    // another kind is capped and counted the same way.
    for (event, kind) in [("result", "tool"), ("embedded", "user")] {
        f.capture(
            event,
            "thread",
            &"copper telemetry line. ".repeat(200),
            kind,
            "contract",
            json!({"extraction_mode":"tool_output"}),
            "2026-10-18T12:00:00Z",
        );
    }
    let bytes = |work: &Value| serde_json::to_vec(&work["input"]).unwrap().len() as u64;
    crate::work_timing::take_counts();
    let work = f.claim("contract", "thread", Some(vec!["extract".into()]))["work"][0].clone();
    let counts = crate::work_timing::take_counts();
    assert_eq!(
        counts
            .iter()
            .filter(|(name, _)| name.starts_with("claim_extract_")
                || name.starts_with("claim_consolidate_"))
            .map(|(name, count)| (*name, *count))
            .collect::<Vec<_>>(),
        [
            ("claim_extract_input_bytes", bytes(&work)),
            ("claim_extract_message_chars", FACT.chars().count() as u64),
            ("claim_extract_message_windows", 1),
            ("claim_extract_tool_chars", 4000),
            ("claim_extract_tool_windows", 2)
        ]
    );
    // The model view is unchanged by the measurement.
    assert_eq!(
        work["input"]["windows"][1],
        json!({
            "id":"w1",
            "kind":"tool",
            "local_time":"2026-10-18T12:00:00+00:00 Sunday",
            "text":"copper telemetry line. ".repeat(200).chars().take(2000).collect::<String>()
        })
    );
    let windows = work["input"]["windows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|window| {
            let candidates = if window["text"] == FACT {
                vec![draft(FACT, "knowledge", false)]
            } else {
                vec![]
            };
            json!({"id":window["id"],"candidates":candidates})
        })
        .collect::<Vec<_>>();
    f.complete(&work, json!({"windows":windows})).unwrap();
    crate::work_timing::take_counts();
    let work = f.consolidate_work();
    let counts = crate::work_timing::take_counts();
    assert_eq!(counts["claim_consolidate_input_bytes"], bytes(&work));
    assert!(!counts.contains_key("claim_extract_input_bytes"));
    // A claim that hands out no work measures none.
    f.complete(
        &work,
        json!({"actions":[action(&work["input"]["candidates"][0],"reject")]}),
    )
    .unwrap();
    crate::work_timing::take_counts();
    let idle = f.claim("contract", "thread", None);
    assert!(idle["work"].as_array().unwrap().is_empty());
    assert!(
        crate::work_timing::take_counts()
            .keys()
            .all(|name| !name.ends_with("_input_bytes") && !name.ends_with("_windows"))
    );
}
