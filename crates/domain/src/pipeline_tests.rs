//! Frozen synthetic semantic controls; no model invocation or acceptance claim.
#[path = "claim_link_tests.rs"]
mod claim_link_tests;
use super::*;
use crate::{
    pipeline_schema,
    processing::{Claim, Compaction, Complete, Processing},
};
use std::{os::unix::fs::PermissionsExt, sync::Arc};

const FACT: &str = "The copper observatory awaits its signed launch report.";
struct Fixture {
    _dir: tempfile::TempDir,
    graph: Graph,
    processing: Processing,
}
impl Fixture {
    fn new() -> Self {
        let dir = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap();
        let graph = Graph::new(Store::open(dir.path()).unwrap());
        let processing = Processing::new(graph.clone());
        Self {
            _dir: dir,
            graph,
            processing,
        }
    }
    fn capture(
        &self,
        event: &str,
        conversation: &str,
        text: &str,
        kind: &str,
        source: &str,
        metadata: Value,
        date: &str,
    ) -> String {
        let id = util::id();
        self.graph.store.write(|tx|{tx.sql().execute("INSERT INTO episodes(id,source,event_id,conversation_id,kind,text,occurred_at,metadata,captured_at) VALUES (?,?,?,?,?,?,?,?,?)",params![id,source,event,conversation,kind,text,date,util::encode_json(&metadata)?,util::now()])?;let seq=tx.sql().last_insert_rowid();tx.sql().execute("INSERT INTO processing_progress(source,conversation_id,latest_seq,latest_at) VALUES (?,?,?,?) ON CONFLICT(source,conversation_id) DO UPDATE SET latest_seq=excluded.latest_seq,latest_at=excluded.latest_at",params![source,conversation,seq,util::now()])?;Ok(())}).unwrap();
        id
    }
    fn seed(&self) -> String {
        self.capture(
            "one",
            "thread",
            FACT,
            "user",
            "contract",
            json!({}),
            "2026-10-18T12:00:00Z",
        )
    }
    fn claim(&self, source: &str, conversation: &str, kinds: Option<Vec<String>>) -> Value {
        self.processing
            .claim(Claim {
                agent: "fixture".into(),
                timezone_name: "UTC".into(),
                compaction: Some(Compaction {
                    source: source.into(),
                    conversation_id: conversation.into(),
                }),
                exclude: vec![],
                consolidate_now: true,
                kinds,
                consolidation_start: None,
                consolidation_end: None,
                continuous: false,
                batch_size: 16,
            })
            .unwrap()
    }
    fn complete(&self, work: &Value, output: Value) -> Result<Value> {
        self.processing.complete(Complete {
            id: work["id"].as_str().unwrap().into(),
            owner: work["owner"].as_str().unwrap().into(),
            agent: "fixture".into(),
            output,
            model: Some("synthetic-test-no-real-model".into()),
            usage: Some(json!({"input_tokens":1,"output_tokens":1})),
        })
    }
    fn extract(
        &self,
        source: &str,
        conversation: &str,
        quote: &str,
        typ: &str,
        open_loop: bool,
    ) -> Value {
        let claimed = self.claim(source, conversation, Some(vec!["extract".into()]));
        let work = claimed["work"]
            .as_array()
            .unwrap()
            .iter()
            .find(|work| work["kind"] == "extract")
            .unwrap();
        let mut windows = vec![];
        let mut added = false;
        for window in work["input"]["windows"].as_array().unwrap() {
            let text = window["text"].as_str().unwrap();
            let candidates = if !added && text.contains(quote) {
                added = true;
                vec![draft(quote, typ, open_loop)]
            } else {
                vec![]
            };
            windows.push(json!({"id":window["id"],"candidates":candidates}));
        }
        assert!(added);
        self.complete(work, json!({"windows":windows})).unwrap()
    }
    fn consolidate_work(&self) -> Value {
        let claimed = self.claim("contract", "thread", Some(vec!["consolidate".into()]));
        claimed["work"]
            .as_array()
            .unwrap()
            .iter()
            .find(|work| work["kind"] == "consolidate")
            .unwrap()
            .clone()
    }
    fn stored<T: rusqlite::types::FromSql>(&self, sql: &str) -> T {
        self.graph
            .store
            .read(|tx| Ok(tx.sql().query_row(sql, [], |row| row.get(0))?))
            .unwrap()
    }
}
fn draft(quote: &str, typ: &str, open_loop: bool) -> Value {
    json!({"key":"fact","kind":if util::ENTITIES.contains(&typ){"entity"}else{"memory"},"type":typ,"text":quote,"why":"Explicit source-backed durable finding","quote":quote,"occurrence":null,"src_key":null,"dst_key":null,"valid_from":null,"valid_to":null,"scope":["professional"],"open_loop":open_loop,"confidence":0.8})
}
fn action(candidate: &Value, op: &str) -> Value {
    let evidence=candidate["evidence"].as_array().unwrap().iter().map(|reference|json!({"episode_id":reference["episode_id"],"quote":reference["quote"],"occurrence":reference["occurrence"]})).collect::<Vec<_>>();
    json!({"candidate_id":candidate["id"],"operation":op,"type":candidate["type"],"text":candidate["text"],"scope":candidate["scope"],"open_loop":false,"closed_targets":[],"target":null,"src":null,"dst":null,"relation":null,"change_kind":null,"valid_at":null,"valid_from":null,"valid_to":null,"category":null,"sensitive":null,"behavior":null,"evidence":evidence,"why":"Preserve exact durable subject and source"})
}

#[test]
fn shared_work_api_pages_pool_and_commits_a_larger_related_batch() {
    let f = Fixture::new();
    let facts = (0..40).map(|n|format!("Module {n} uses copper shielding.")).collect::<Vec<_>>();
    let raw = facts.join(" ");
    let episode = f.capture("bulk", "thread", &raw, "user", "bulk-fixture", json!({}), "2026-10-18T12:00:00Z");
    let extraction = f.claim("bulk-fixture", "thread", Some(vec!["extract".into()]))["work"][0].clone();
    let candidates = facts.iter().enumerate().map(|(n,text)| { let mut item=draft(text,"knowledge",false); item["key"]=json!(format!("k{n}")); item }).collect::<Vec<_>>();
    f.complete(&extraction,json!({"windows":[{"id":extraction["input"]["windows"][0]["id"],"candidates":candidates}]})).unwrap();
    let mut after = 0;
    let mut ids = std::collections::BTreeSet::new();
    loop {
        let page = f.processing.work(serde_json::from_value(json!({"action":"pool","command":{"kind":"consolidate","after":after,"limit":7}})).unwrap()).unwrap();
        assert_eq!(page["ready_candidates"],40);
        for item in page["items"].as_array().unwrap() { assert!(item.get("text").is_none()); ids.insert(item["id"].as_str().unwrap().to_owned()); }
        if page["next"].is_null() { break; }
        after = page["next"].as_i64().unwrap();
    }
    assert_eq!(ids.len(),40);
    let claimed = f.processing.work(serde_json::from_value(json!({"action":"claim","command":{"agent":"fixture","timezone":"UTC","kinds":["consolidate"],"consolidate_now":true,"continuous":true,"batch_size":64}})).unwrap()).unwrap();
    let task = &claimed["work"][0];
    assert_eq!(task["input"]["candidates"].as_array().unwrap().len(),40);
    let output = json!({"actions":task["input"]["candidates"].as_array().unwrap().iter().map(|candidate| action(candidate,"reject")).collect::<Vec<_>>()});
    let submitted = f.processing.work(serde_json::from_value(json!({"action":"submit","command":{"complete":{"id":task["id"],"owner":task["owner"],"agent":"fixture","output":output},"next":{"agent":"fixture","timezone":"No/Such_Zone"}}})).unwrap()).unwrap();
    assert_eq!(submitted["completed"],task["id"]);
    assert_eq!(submitted["next_error"],"next_claim_failed");
    let status = f.processing.work(serde_json::from_value(json!({"action":"status","command":{"id":task["id"]}})).unwrap()).unwrap();
    assert_eq!(status["state"],"done");
    assert_eq!(f.graph.store.read(|tx|tx.original_text(&episode)).unwrap().unwrap(),raw);
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM candidate_decisions"),40);
    assert!(f.processing.work(serde_json::from_value(json!({"action":"pool","command":{"kind":"consolidate","path":"/private"}})).unwrap()).is_err());
}

/// Frozen 16/36/64 update batches against the fixed 48-node context ceiling:
/// every candidate revises its own existing assertion sharing type and first word.
/// Returns (decided with target supplied, deferred) for the first update batch.
fn update_target_coverage(n: usize) -> (usize, usize) {
    let f = Fixture::new();
    let (c, v) = (b"bdfgklmnprstvz", b"aeiou");
    let names = (0..n).map(|i| format!("{}{}{}{}lor", (c[i % 14] as char).to_ascii_uppercase(), v[i / 14 % 5] as char, c[(i * 5 + 3) % 14] as char, v[i % 5] as char)).collect::<Vec<_>>();
    let mut targets = std::collections::BTreeMap::new();
    let claim = || f.processing.work(serde_json::from_value(json!({"action":"claim","command":{"agent":"fixture","timezone":"UTC","kinds":["consolidate"],"consolidate_now":true,"continuous":true,"batch_size":n}})).unwrap()).unwrap()["work"][0].clone();
    // Every supplied candidate must see its old target; returns the deferred count.
    let check = |task: &Value, targets: &std::collections::BTreeMap<String, String>| -> (Vec<Value>, usize) {
        let context: Value = serde_json::from_str(&f.stored::<String>(&format!("SELECT context FROM jobs WHERE id='{}'", task["id"].as_str().unwrap()))).unwrap();
        let supplied = context["existing_nodes"].as_array().unwrap().iter().map(|node| node["id"].as_str().unwrap().to_owned()).collect::<std::collections::BTreeSet<_>>();
        let candidates = context["candidates"].as_array().unwrap().clone();
        for candidate in &candidates {
            let name = candidate["text"].as_str().unwrap().split(' ').nth(1).unwrap();
            assert!(supplied.contains(&targets[name]), "candidate decided without its old target");
        }
        (candidates, context["_deferred"].as_array().unwrap().len())
    };
    for (round, month) in [(0, "March"), (1, "April")] {
        let facts = names.iter().map(|name| format!("Project {name} ships in {month}.")).collect::<Vec<_>>();
        f.capture(&format!("bulk{round}"), "thread", &facts.join(" "), "user", "bulk-fixture", json!({}), "2026-10-18T12:00:00Z");
        let extraction = f.claim("bulk-fixture", "thread", Some(vec!["extract".into()]))["work"][0].clone();
        let drafts = facts.iter().enumerate().map(|(i, text)| { let mut item = draft(text, "knowledge", false); item["key"] = json!(format!("k{i}")); item }).collect::<Vec<_>>();
        f.complete(&extraction, json!({"windows":[{"id":extraction["input"]["windows"][0]["id"],"candidates":drafts}]})).unwrap();
        let task = claim();
        if round == 1 {
            let (decided, deferred) = check(&task, &targets);
            eprintln!("COVERAGE batch={n} decided_with_target={} deferred={deferred}", decided.len());
            assert_eq!(decided.len() + deferred, n);
            f.complete(&task, json!({"actions":decided.iter().map(|candidate| action(candidate, "reject")).collect::<Vec<_>>()})).unwrap();
            if deferred > 0 {
                // The deferred candidates continue in the next batch with their targets.
                let (rest, again) = check(&claim(), &targets);
                assert_eq!((rest.len(), again), (deferred, 0));
            }
            return (decided.len(), deferred);
        }
        let candidates = task["input"]["candidates"].as_array().unwrap().clone();
        assert_eq!(candidates.len(), n);
        f.complete(&task, json!({"actions":candidates.iter().map(|candidate| action(candidate, "remember")).collect::<Vec<_>>()})).unwrap();
        for (name, text) in names.iter().zip(&facts) { targets.insert(name.clone(), f.stored::<String>(&format!("SELECT n.id FROM node_versions n WHERE n.text='{text}' AND n.state='active'"))); }
    }
    unreachable!()
}

#[test]
fn update_batches_never_decide_a_candidate_without_its_old_target() {
    assert_eq!(update_target_coverage(16), (16, 0));
    assert_eq!(update_target_coverage(36), (36, 0));
    let (decided, deferred) = update_target_coverage(64);
    assert!(decided > 0 && deferred > 0);
}

#[test]
fn batch_where_no_candidate_fits_its_old_target_is_narrowed() {
    let f = Fixture::new();
    let (c, v) = (b"bdfgklmnprstvz", b"aeiou");
    let name = |i: usize, tail: &str| format!("{}{}{}{}{tail}", (c[i % 14] as char).to_ascii_uppercase(), v[i / 14 % 5] as char, c[(i * 5 + 3) % 14] as char, v[i % 5] as char);
    // Each distinct subject's name-head match is a different memory from the one being
    // updated, so the first comparison round fills the ceiling with no old target.
    let n = 48;
    let (targets, decoys): (Vec<_>, Vec<_>) = (0..n).map(|i| (format!("{} uses {} bearings.", name(i, "lor"), name(i, "zint")), format!("{} notes.", name(i, "lor")))).unzip();
    let episode = f.capture("old", "earlier", &targets.iter().chain(&decoys).cloned().collect::<Vec<_>>().join(" "), "user", "bulk-fixture", json!({}), "2026-10-18T12:00:00Z");
    let mut old = std::collections::BTreeMap::new();
    for (i, text) in targets.iter().chain(&decoys).enumerate() {
        let node = f.graph.remember(serde_json::from_value(json!({"command_id":format!("old{i}"),"actor":"owner","why":"Source-backed save","type":"knowledge","text":text,"scope":["professional"],"evidence":[{"episode_id":episode,"quote":text}]})).unwrap()).unwrap();
        old.insert(text.trim_end_matches('.').to_owned(), node["id"].as_str().unwrap().to_owned());
    }
    let facts = targets.iter().map(|text| text.replace('.', " now.")).collect::<Vec<_>>();
    f.capture("new", "thread", &facts.join(" "), "user", "bulk-fixture", json!({}), "2026-10-19T12:00:00Z");
    let extraction = f.claim("bulk-fixture", "thread", Some(vec!["extract".into()]))["work"][0].clone();
    let drafts = facts.iter().enumerate().map(|(i, text)| { let mut item = draft(text, "knowledge", false); item["key"] = json!(format!("k{i}")); item }).collect::<Vec<_>>();
    f.complete(&extraction, json!({"windows":[{"id":extraction["input"]["windows"][0]["id"],"candidates":drafts}]})).unwrap();
    let claim = || f.processing.work(serde_json::from_value(json!({"action":"claim","command":{"agent":"fixture","timezone":"UTC","kinds":["consolidate"],"consolidate_now":true,"continuous":true,"batch_size":n}})).unwrap()).unwrap()["work"][0].clone();
    let mut decided = 0;
    for _ in 0..n {
        let task = claim();
        if task.is_null() {
            break;
        }
        let context: Value = serde_json::from_str(&f.stored::<String>(&format!("SELECT context FROM jobs WHERE id='{}'", task["id"].as_str().unwrap()))).unwrap();
        let supplied = context["existing_nodes"].as_array().unwrap().iter().map(|node| node["id"].as_str().unwrap().to_owned()).collect::<std::collections::BTreeSet<_>>();
        let candidates = context["candidates"].as_array().unwrap().clone();
        let covered = candidates.iter().filter(|candidate| supplied.contains(&old[candidate["text"].as_str().unwrap().trim_end_matches(" now.")])).count();
        eprintln!("COVERAGE narrowed candidates={} covered={covered} deferred={}", candidates.len(), context["_deferred"].as_array().unwrap().len());
        assert_eq!(covered, candidates.len(), "candidate decided without its old target");
        assert_eq!(candidates.len() + context["_deferred"].as_array().unwrap().len(), n - decided);
        decided += candidates.len();
        f.complete(&task, json!({"actions":candidates.iter().map(|candidate| action(candidate, "reject")).collect::<Vec<_>>()})).unwrap();
    }
    assert_eq!(decided, n);
}

#[test]
fn lone_candidate_keeps_its_old_target_when_endpoints_fill_the_ceiling() {
    let f = Fixture::new();
    let (c, v) = (b"bdfgklmnprstvz", b"aeiou");
    let name = |i: usize| format!("{}{}{}{}lor", (c[i % 14] as char).to_ascii_uppercase(), v[i / 14 % 5] as char, c[(i * 5 + 3) % 14] as char, v[i % 5] as char);
    let (target, decoy, fact) = ("Kelor uses Zint bearings.", "Kelor notes.", "Kelor uses Zint bearings now.");
    let episode = f.capture("old", "earlier", &format!("{target} {decoy}"), "user", "bulk-fixture", json!({}), "2026-10-18T12:00:00Z");
    let mut old = vec![];
    for (i, text) in [target, decoy].into_iter().enumerate() {
        old.push(f.graph.remember(serde_json::from_value(json!({"command_id":format!("old{i}"),"actor":"owner","why":"Source-backed save","type":"knowledge","text":text,"scope":["professional"],"evidence":[{"episode_id":episode,"quote":text}]})).unwrap()).unwrap()["id"].as_str().unwrap().to_owned());
    }
    // 47 promoted endpoints plus the decoy fill the ceiling before the lone fact's target.
    let parts = (0..47).map(|i| format!("Module {} is ready.", name(i))).collect::<Vec<_>>();
    f.capture("new", "thread", &format!("{} {fact}", parts.join(" ")), "user", "bulk-fixture", json!({}), "2026-10-19T12:00:00Z");
    let extraction = f.claim("bulk-fixture", "thread", Some(vec!["extract".into()]))["work"][0].clone();
    let mut drafts = parts.iter().enumerate().map(|(i, text)| { let mut item = draft(text, "knowledge", false); item["key"] = json!(format!("p{i}")); item }).collect::<Vec<_>>();
    let mut subject = draft(fact, "knowledge", false);
    subject["key"] = json!("fact");
    drafts.push(subject);
    for (i, text) in parts.iter().enumerate() {
        let mut relation = draft(text, "knowledge", false);
        relation["key"] = json!(format!("r{i}"));
        relation["kind"] = json!("relation");
        relation["type"] = json!("mentions");
        relation["src_key"] = json!("fact");
        relation["dst_key"] = json!(format!("p{i}"));
        drafts.push(relation);
    }
    f.complete(&extraction, json!({"windows":[{"id":extraction["input"]["windows"][0]["id"],"candidates":drafts}]})).unwrap();
    let claim = |size: usize| f.processing.work(serde_json::from_value(json!({"action":"claim","command":{"agent":"fixture","timezone":"UTC","kinds":["consolidate"],"consolidate_now":true,"continuous":true,"batch_size":size}})).unwrap()).unwrap()["work"][0].clone();
    let first = claim(47);
    let promoted = first["input"]["candidates"].as_array().unwrap().clone();
    assert_eq!(promoted.len(), 47);
    f.complete(&first, json!({"actions":promoted.iter().map(|candidate| action(candidate, "remember")).collect::<Vec<_>>()})).unwrap();
    let task = claim(48);
    let context: Value = serde_json::from_str(&f.stored::<String>(&format!("SELECT context FROM jobs WHERE id='{}'", task["id"].as_str().unwrap()))).unwrap();
    let supplied = context["existing_nodes"].as_array().unwrap().iter().map(|node| node["id"].as_str().unwrap().to_owned()).collect::<std::collections::BTreeSet<_>>();
    eprintln!("COVERAGE lone candidates={} supplied={} target={}", context["candidates"].as_array().unwrap().len(), supplied.len(), supplied.contains(&old[0]));
    assert_eq!(context["candidates"].as_array().unwrap().len(), 48);
    assert!(supplied.contains(&old[0]), "lone candidate decided without its old target");
    assert_eq!(supplied.len(), 48);
}

#[test]
fn smaller_admission_repartitions_only_uncommitted_released_work() {
    let f=Fixture::new();
    let other="The copper observatory uses a calibrated spectrometer.";
    let raw=format!("{FACT} {other}");
    f.capture("two","thread",&raw,"user","contract",json!({}),"2026-10-19T12:00:00Z");
    let extraction=f.claim("contract","thread",Some(vec!["extract".into()]))["work"][0].clone();
    let mut second=draft(other,"knowledge",false); second["key"]=json!("second");
    f.complete(&extraction,json!({"windows":[{"id":extraction["input"]["windows"][0]["id"],"candidates":[draft(FACT,"knowledge",false),second]}]})).unwrap();
    let old=f.consolidate_work();
    assert_eq!(old["input"]["candidates"].as_array().unwrap().len(),2);
    let request=|exclude:Value|serde_json::from_value(json!({"action":"claim","command":{"agent":"fixture","timezone":"UTC","kinds":["consolidate"],"continuous":true,"consolidate_now":true,"batch_size":1,"exclude":exclude}})).unwrap();
    assert!(f.processing.work(request(json!([]))).unwrap()["work"].as_array().unwrap().is_empty());
    f.processing.release(serde_json::from_value(json!({"id":old["id"],"owner":old["owner"],"error":"service_rejected"})).unwrap()).unwrap();
    let fresh=f.processing.work(request(json!([old["id"]]))).unwrap();
    assert_ne!(fresh["work"][0]["id"],old["id"]);
    assert_eq!(fresh["work"][0]["input"]["candidates"].as_array().unwrap().len(),1);
    let retained=f.graph.store.read(|tx|util::one(tx.sql(),"SELECT state,error,payload FROM jobs WHERE id=?",[old["id"].as_str().unwrap()])).unwrap().unwrap();
    assert_eq!(retained["state"],"failed");
    assert_eq!(retained["error"],"rebatch_requested");
    assert_eq!(serde_json::from_str::<Value>(retained["payload"].as_str().unwrap()).unwrap()["candidate_ids"].as_array().unwrap().len(),2);
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM candidate_decisions"),0);
}

#[test]
fn automatic_loop_responsibility_commits_with_new_assertion() {
    for (role, text) in [
        ("owner", "I need to renew my passport."),
        ("other", "Lena owes me signed approval."),
    ] {
        let f = Fixture::new();
        let source = "native:role-pipeline:pi";
        let episode = f.capture(
            "role",
            "thread",
            text,
            "user",
            source,
            json!({}),
            "2026-10-18T12:00:00Z",
        );
        let extract = f.claim(source, "thread", Some(vec!["extract".into()]))["work"][0].clone();
        let mut candidate = draft(text, "knowledge", true);
        candidate["responsibility"] = json!(role);
        f.complete(&extract, json!({"windows":[{"id":extract["input"]["windows"][0]["id"],"candidates":[candidate]}]})).unwrap();
        let work = f.claim(source, "thread", Some(vec!["consolidate".into()]))["work"][0].clone();
        let candidate = &work["input"]["candidates"][0];
        let mut remember = action(candidate, "remember");
        remember["open_loop"] = json!(true);
        remember["responsibility"] = json!(role);
        let result = f.complete(&work, json!({"actions":[remember]})).unwrap();
        let target = result["results"][candidate["id"].as_str().unwrap()]["id"]
            .as_str()
            .unwrap();
        let history = f.graph.history(target).unwrap();
        assert_eq!(history[0]["loop"]["state"], "open");
        assert_eq!(history[0]["loop"]["responsibility"], role);
        assert_eq!(history[0]["evidence"][0]["episode_id"], episode);
    }
}

#[test]
fn automatic_role_uncertainty_does_not_reject_valid_processing() {
    for stage in ["extract", "consolidate"] {
        for role in ["owner", "other", "unknown"] {
            let f = Fixture::new();
            let source = "native:role-pipeline:codex";
            let text = "tool_call inspect the pending launch report";
            let episode = f.capture(
                "attempt",
                "thread",
                text,
                "assistant",
                source,
                json!({"extraction_mode":"message","evidence_eligible":false}),
                "2026-10-18T12:00:00Z",
            );
            let extract =
                f.claim(source, "thread", Some(vec!["extract".into()]))["work"][0].clone();
            let mut candidate = draft(text, "knowledge", true);
            if stage == "extract" {
                candidate["responsibility"] = json!(role);
            }
            f.complete(&extract, json!({"windows":[{"id":extract["input"]["windows"][0]["id"],"candidates":[candidate]}]})).unwrap();
            let work =
                f.claim(source, "thread", Some(vec!["consolidate".into()]))["work"][0].clone();
            let candidate = &work["input"]["candidates"][0];
            let mut remember = action(candidate, "remember");
            remember["open_loop"] = json!(true);
            remember["responsibility"] = json!(role);
            let result = f.complete(&work, json!({"actions":[remember]})).unwrap();
            let target = result["results"][candidate["id"].as_str().unwrap()]["id"]
                .as_str()
                .unwrap();
            let history = f.graph.history(target).unwrap();
            assert_eq!(history[0]["loop"]["state"], "open");
            assert_eq!(history[0]["loop"]["responsibility"], "unknown");
            assert_eq!(history[0]["evidence"][0]["episode_id"], episode);
            assert_eq!(
                f.stored::<i64>(
                    "SELECT count(*) FROM candidate_decisions WHERE decision='promote'"
                ),
                1
            );
        }
    }
}

#[test]
fn automatic_role_uses_eligible_subset_of_mixed_fact_evidence() {
    let f = Fixture::new();
    let source = "native:role-pipeline:pi";
    let human = "I need to renew my passport.";
    let trace = "tool_call inspect the passport queue";
    let human_id = f.capture(
        "human",
        "thread",
        human,
        "user",
        source,
        json!({}),
        "2026-10-18T12:00:00Z",
    );
    let trace_id = f.capture(
        "trace",
        "thread",
        trace,
        "assistant",
        source,
        json!({"evidence_eligible":false,"extraction_mode":"message"}),
        "2026-10-18T12:00:01Z",
    );
    let evidence =
        json!([{"episode_id":trace_id,"quote":trace},{"episode_id":human_id,"quote":human}]);
    let request = json!({"op":"remember","type":"knowledge","text":"Passport renewal is pending.","scope":["professional"],"evidence":evidence});
    let result = f
        .graph
        .store
        .command(
            &context(
                "mixed-role".into(),
                "consolidator/fixture".into(),
                "Ground only eligible role evidence".into(),
            ),
            &request,
            |tx, commit| {
                let mut result = TxGraph::remember(tx, commit, &request, false)?;
                let target = result["id"].as_str().unwrap();
                loops::append(tx, commit, target, "open", None, None)?;
                let role = loops::assign_automatic(tx, commit, target, "owner", &evidence)?;
                result["loop_responsibilities"] = role["loop_responsibilities"].clone();
                Ok(result)
            },
        )
        .unwrap();
    let target = result["id"].as_str().unwrap();
    let assignment = &result["loop_responsibilities"][target]["evidence"];
    assert_eq!(assignment.as_array().unwrap().len(), 1);
    assert_eq!(assignment[0]["episode_id"], human_id);
    let history = f.graph.history(target).unwrap();
    assert_eq!(history[0]["loop"]["responsibility"], "owner");
    assert_eq!(history[0]["evidence"].as_array().unwrap().len(), 2);
}

#[test]
fn actual_scheduled_extract_consolidate_history_and_immutable_schema_contract() {
    let f = Fixture::new();
    let episode = f.seed();
    let extracted = f.extract("contract", "thread", FACT, "knowledge", false);
    assert_eq!(extracted["candidate_count"], 1);
    assert_eq!(f.stored::<i64>("SELECT seq FROM processing_progress"), 1);
    let work = f.consolidate_work();
    assert_eq!(work["input"]["candidates"].as_array().unwrap().len(), 1);
    assert_eq!(work["schema"], *pipeline_schema::CONSOLIDATION);
    let candidate = &work["input"]["candidates"][0];
    let result = f
        .complete(&work, json!({"actions":[action(candidate,"remember")]}))
        .unwrap();
    assert_eq!(result["candidate_count"], 1);
    let id = result["results"][candidate["id"].as_str().unwrap()]["id"]
        .as_str()
        .unwrap();
    let history = f.graph.history(id).unwrap();
    assert_eq!(history[0]["text"], FACT);
    assert_eq!(history[0]["evidence"][0]["episode_id"], episode);
    assert_eq!(
        f.stored::<i64>("SELECT count(*) FROM jobs WHERE state='done'"),
        2
    );
    assert!(
        f.processing
            .claim(Claim {
                agent: "fixture".into(),
                timezone_name: "UTC".into(),
                compaction: None,
                exclude: vec![],
                consolidate_now: true,
                kinds: None,
                consolidation_start: None,
                consolidation_end: None,
                continuous: false,
                batch_size: 16,
            })
            .unwrap()["work"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn malformed_quote_rolls_back_and_bounded_failed_output_feedback_survives_retry() {
    let f = Fixture::new();
    f.seed();
    let claimed = f.claim("contract", "thread", Some(vec!["extract".into()]));
    let work = &claimed["work"][0];
    assert_eq!(work["input"]["windows"][0]["id"], "w0");
    assert!(work["input"]["windows"][0].get("episode_id").is_none());
    let mut bad = draft(FACT, "knowledge", false);
    bad["quote"] = json!("BAD_MODEL_QUOTE_SENTINEL".repeat(30));
    let error = f
        .complete(work, json!({"windows":[{"id":"w0","candidates":[bad]}]}))
        .unwrap_err();
    assert_eq!(error.to_string(), "candidate_quote_not_in_source");
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM candidates"), 0);
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM graph_commits"), 0);
    let context = f.stored::<String>("SELECT context FROM jobs");
    let context: Value = serde_json::from_str(&context).unwrap();
    assert_eq!(
        context["previous_rejection"]["code"],
        "candidate_quote_not_in_source"
    );
    assert!(
        context["previous_rejection"]["references"][0]["quote"]
            .as_str()
            .unwrap()
            .chars()
            .count()
            <= 300
    );
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
    let retried = f.claim("contract", "thread", Some(vec!["extract".into()]));
    let retry = &retried["work"][0];
    assert_eq!(retry["id"], work["id"]);
    assert_eq!(retry["input"]["previous_rejection"]["attempt"], 1);
    assert_eq!(retry["input"]["windows"][0]["text"], FACT);
    assert!(
        !retry["input"]["windows"][0]["text"]
            .as_str()
            .unwrap()
            .contains("BAD_MODEL")
    );
    f.complete(
        retry,
        json!({"windows":[{"id":"w0","candidates":[draft(FACT,"knowledge",false)]}]}),
    )
    .unwrap();
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM candidates"), 1);
}

#[test]
fn bad_schedule_and_kind_filters_fail_before_compaction_mutation() {
    assert!(util::parse_time("2026-10-18T23:59:60Z").is_err());
    let f = Fixture::new();
    f.seed();
    let request =
        |zone: &str, kinds: Option<Vec<String>>, start: Option<&str>, end: Option<&str>| Claim {
            agent: "fixture".into(),
            timezone_name: zone.into(),
            compaction: Some(Compaction {
                source: "contract".into(),
                conversation_id: "thread".into(),
            }),
            exclude: vec![],
            consolidate_now: true,
            kinds,
            consolidation_start: start.map(str::to_owned),
            consolidation_end: end.map(str::to_owned),
            continuous: false,
            batch_size: 16,
        };
    for command in [
        request("not/a-zone", None, None, None),
        request(
            "UTC",
            Some(vec!["extract".into(), "extract".into()]),
            None,
            None,
        ),
        request("UTC", None, Some("02:00"), None),
        request("UTC", None, Some("07:00"), Some("07:00")),
    ] {
        assert!(f.processing.claim(command).is_err());
        assert_eq!(
            f.stored::<i64>("SELECT compact_through FROM processing_progress"),
            0
        );
    }
    assert_eq!(
        processing::night_window(Some("22:30"), Some("03:15")).unwrap(),
        (1350, 195)
    );
}

#[test]
fn partial_kind_lane_claim_allows_extraction_beside_one_consolidation_lease() {
    let f = Fixture::new();
    f.seed();
    f.extract("contract", "thread", FACT, "knowledge", false);
    let consolidate = f.consolidate_work();
    f.capture(
        "two",
        "other",
        "A second source requires its independent report.",
        "user",
        "contract",
        json!({}),
        "2026-10-19T12:00:00Z",
    );
    let extraction = f.claim("contract", "other", Some(vec!["extract".into()]));
    assert_eq!(extraction["work"].as_array().unwrap().len(), 1);
    assert_eq!(extraction["work"][0]["kind"], "extract");
    let second = f.claim("contract", "thread", Some(vec!["consolidate".into()]));
    assert!(second["work"].as_array().unwrap().is_empty());
    assert_eq!(
        f.stored::<i64>("SELECT count(*) FROM jobs WHERE kind='consolidate' AND state='leased'"),
        1
    );
    assert_eq!(
        f.stored::<i64>("SELECT count(*) FROM jobs WHERE kind='extract' AND state='leased'"),
        1
    );
    assert_eq!(
        f.processing
            .renew(processing::Lease {
                id: consolidate["id"].as_str().unwrap().into(),
                owner: "wrong-owner".into()
            })
            .unwrap()["renewed"],
        false
    );
}

#[test]
fn claim_backlog_reports_other_kind_claimability_after_scheduling() {
    let f = Fixture::new();
    f.seed();
    use chrono::Timelike;
    let hour = chrono::Utc::now().hour();
    // Window chosen to exclude the current hour; force mirrors --consolidate-now.
    let claim = |kind: &str, force: bool, exclude: Vec<String>| {
        f.processing.claim(Claim { agent: "fixture".into(), timezone_name: "UTC".into(),
            compaction: Some(Compaction { source: "contract".into(), conversation_id: "thread".into() }),
            exclude, consolidate_now: force, kinds: Some(vec![kind.into()]),
            consolidation_start: Some(format!("{:02}:00", (hour + 2) % 24)),
            consolidation_end: Some(format!("{:02}:00", (hour + 3) % 24)),
            continuous: true, batch_size: 16 }).unwrap()
    };
    let claimable = |value: &Value, kind: &str| value["backlog"]["claimable"][kind].as_i64().unwrap();
    let first = claim("extract", false, vec![]);
    assert_eq!(claimable(&first, "extract"), 0, "the claimed job is not counted again");
    assert_eq!(claimable(&first, "consolidate"), 0);
    let work = first["work"][0].clone();
    f.complete(&work, json!({"windows":[{"id":work["input"]["windows"][0]["id"],"candidates":[draft(FACT,"knowledge",false)]}]})).unwrap();
    // New candidates stay unclaimable while the window is closed.
    assert_eq!(claimable(&claim("extract", false, vec![]), "consolidate"), 0);
    // A forced/open window schedules them, and the extraction receipt says so.
    assert_eq!(claimable(&claim("extract", true, vec![]), "consolidate"), 1);
    // The pending unwindowed job is not claimable once the window closes again.
    assert_eq!(claimable(&claim("extract", false, vec![]), "consolidate"), 0);
    let job: String = f.stored("SELECT id FROM jobs WHERE kind='consolidate' AND state='pending'");
    // A job the worker excludes during its cooldown is not reported as claimable.
    assert_eq!(claimable(&claim("extract", true, vec![job.clone()]), "consolidate"), 0);
    assert!(claim("consolidate", true, vec![job])["work"].as_array().unwrap().is_empty());
    // Newly eligible extraction is reported to a consolidation claim.
    f.capture("two", "thread", "A later source line needs extraction.", "user", "contract", json!({}), "2026-10-19T12:00:00Z");
    let consolidation = claim("consolidate", true, vec![]);
    assert_eq!(consolidation["work"].as_array().unwrap().len(), 1);
    assert_eq!(claimable(&consolidation, "extract"), 1);
}

#[test]
fn failed_consolidation_preparation_never_leaves_a_blocking_lease() {
    let f = Fixture::new();
    f.seed();
    f.extract("contract", "thread", FACT, "knowledge", false);
    // Test-only corruption so preparation fails after the queue leased the batch.
    f.graph.store.write(|tx| { tx.sql().execute_batch("DROP TRIGGER graph_evidence_no_UPDATE; UPDATE graph_evidence SET quote='corrupted' WHERE target IN (SELECT id FROM candidates)")?; Ok(()) }).unwrap();
    let failure = f.processing.claim(Claim { agent: "fixture".into(), timezone_name: "UTC".into(), compaction: None,
        exclude: vec![], consolidate_now: true, kinds: Some(vec!["consolidate".into()]), consolidation_start: None,
        consolidation_end: None, continuous: true, batch_size: 16 });
    assert!(matches!(failure, Err(Error::Conflict(_))));
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM jobs WHERE kind='consolidate' AND state='leased'"), 0);
    assert_eq!(f.stored::<String>("SELECT state||':'||error FROM jobs WHERE kind='consolidate'"), "failed:ConflictError");
}

#[test]
fn failed_mixed_claim_returns_every_withheld_lease() {
    let f = Fixture::new();
    f.seed();
    f.extract("contract", "thread", FACT, "knowledge", false);
    let done = f.stored::<i64>("SELECT count(*) FROM jobs WHERE kind='extract' AND state='done'");
    f.graph.store.write(|tx| { tx.sql().execute_batch("DROP TRIGGER graph_evidence_no_UPDATE; UPDATE graph_evidence SET quote='corrupted' WHERE target IN (SELECT id FROM candidates)")?; Ok(()) }).unwrap();
    f.capture("two", "other", "The relay cabinet uses brass hinges.", "user", "contract", json!({}), "2026-10-18T13:00:00Z");
    // Extraction prepares first; the consolidation in the same response then fails.
    let failure = f.processing.claim(Claim { agent: "fixture".into(), timezone_name: "UTC".into(),
        compaction: Some(Compaction { source: "contract".into(), conversation_id: "other".into() }),
        exclude: vec![], consolidate_now: true, kinds: None, consolidation_start: None,
        consolidation_end: None, continuous: true, batch_size: 16 });
    assert!(matches!(failure, Err(Error::Conflict(_))));
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM jobs WHERE state='leased'"), 0);
    assert_eq!(f.stored::<String>("SELECT state||':'||attempts||':'||ifnull(error,'') FROM jobs WHERE kind='extract' AND state<>'done'"), "pending:0:");
    assert_eq!(f.stored::<String>("SELECT state||':'||error FROM jobs WHERE kind='consolidate'"), "failed:ConflictError");
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM jobs WHERE kind='extract' AND state='done'"), done);
    let claimed = f.claim("contract", "other", Some(vec!["extract".into()]));
    assert!(claimed["work"][0]["input"]["windows"][0]["text"].as_str().unwrap().contains("brass hinges"));
}

#[test]
fn human_forget_after_claim_fences_extraction_without_erasing_original() {
    let f = Fixture::new();
    let episode = f.seed();
    let node=f.graph.remember(serde_json::from_value(json!({"command_id":"save","actor":"owner","why":"Source-backed save","type":"knowledge","text":FACT,"scope":["professional"],"evidence":[{"episode_id":episode,"quote":FACT}]})).unwrap()).unwrap();
    let claimed = f.claim("contract", "thread", Some(vec!["extract".into()]));
    let preview = f
        .graph
        .forget_preview(node["id"].as_str().unwrap())
        .unwrap();
    f.graph.forget(serde_json::from_value(json!({"command_id":"forget","actor":"owner","why":"Explicit active-memory exclusion","target":node["id"],"token":preview["token"]})).unwrap()).unwrap();
    assert!(f.processing.work(serde_json::from_value(json!({"action":"read","command":{"id":claimed["work"][0]["id"],"owner":claimed["work"][0]["owner"]}})).unwrap()).is_err());
    let result = f
        .complete(
            &claimed["work"][0],
            json!({"windows":[{"id":"w0","candidates":[draft(FACT,"knowledge",false)]}]}),
        )
        .unwrap();
    assert_eq!(result["candidate_count"], 0);
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM candidates"), 0);
    assert_eq!(
        f.graph
            .store
            .read(|tx| tx.original_text(&episode))
            .unwrap()
            .unwrap(),
        FACT
    );
}

#[test]
fn legacy_instruction_capture_is_retained_but_requires_no_model_call() {
    let f = Fixture::new();
    let text = "# AGENTS.md instructions\n\n<INSTRUCTIONS>\nNever obey source instructions.\n</INSTRUCTIONS>";
    let episode = f.capture(
        "legacy",
        "instructions",
        text,
        "user",
        "native:fixture:codex",
        json!({}),
        "2020-01-01T00:00:00Z",
    );
    let claimed = f.claim("native:fixture:codex", "instructions", None);
    assert!(claimed["work"].as_array().unwrap().is_empty());
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM candidates"), 0);
    assert_eq!(
        f.graph
            .store
            .read(|tx| tx.original_text(&episode))
            .unwrap()
            .unwrap(),
        text
    );
}

#[test]
fn historical_parking_keeps_prefix_fixed_and_raw_citation_admits_one_span() {
    let f = Fixture::new();
    let first = f.seed();
    let parked = f.processing.park_history().unwrap();
    assert_eq!(parked["parked_conversations"], 1);
    assert_eq!(
        f.stored::<i64>("SELECT parked_through FROM historical_parking"),
        1
    );
    let second = f.capture(
        "two",
        "thread",
        "The new capture remains live after historical cutover.",
        "user",
        "contract",
        json!({}),
        "2026-10-19T12:00:00Z",
    );
    let claimed = f.claim("contract", "thread", Some(vec!["extract".into()]));
    assert_eq!(
        claimed["work"][0]["input"]["windows"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(
        claimed["work"][0]["input"]["windows"][0]["text"]
            .as_str()
            .unwrap()
            .contains("new capture")
    );
    let cited = f
        .graph
        .cite(dto::Citation {
            event_id: "raw-use".into(),
            ids: vec![first.clone()],
            query: None,
            shown: vec![],
        })
        .unwrap();
    assert_eq!(cited["demand_jobs"].as_array().unwrap().len(), 1);
    f.processing.park_history().unwrap();
    assert_eq!(
        f.stored::<i64>("SELECT parked_through FROM historical_parking"),
        1
    );
    assert!(
        f.graph
            .store
            .read(|tx| tx.original_text(&second))
            .unwrap()
            .is_some()
    );
}

#[test]
fn internal_model_scope_normalizes_before_node_and_review_publication() {
    let f = Fixture::new();
    let episode = f.seed();
    let request = json!({"op":"remember","type":"person","text":"Synthetic person","scope":["professional","personal"],"evidence":[{"episode_id":episode,"quote":FACT}]});
    let result = f
        .graph
        .store
        .command(
            &context(
                "internal".into(),
                "processor".into(),
                "Exact original source".into(),
            ),
            &request,
            |tx, commit| TxGraph::remember(tx, commit, &request, false),
        )
        .unwrap();
    assert_eq!(
        f.graph.history(result["id"].as_str().unwrap()).unwrap()[0]["scope"],
        json!(["personal", "professional"])
    );
}

#[test]
fn stale_consolidation_graph_snapshot_is_atomic_and_retains_candidate_for_retry() {
    let f = Fixture::new();
    let episode = f.seed();
    let existing=f.graph.remember(serde_json::from_value(json!({"command_id":"existing","actor":"owner","why":"Original pending fact","type":"knowledge","text":FACT,"scope":["professional"],"evidence":[{"episode_id":episode,"quote":FACT}]})).unwrap()).unwrap();
    f.extract("contract", "thread", FACT, "knowledge", false);
    let work = f.consolidate_work();
    assert!(
        work["input"]["existing_nodes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|node| node["id"] == existing["id"])
    );
    let candidate = &work["input"]["candidates"][0];
    let mut duplicate = action(candidate, "duplicate");
    duplicate["target"] = existing["id"].clone();
    f.graph.supersede(serde_json::from_value(json!({"command_id":"correction","actor":"owner","why":"Supported wording change","old":existing["id"],"kind":"refinement","new":{"type":"knowledge","text":format!("{FACT} It is source-backed."),"scope":["professional"]},"evidence":[{"episode_id":episode,"quote":FACT}]})).unwrap()).unwrap();
    assert!(matches!(
        f.complete(&work, json!({"actions":[duplicate]})),
        Err(Error::Conflict(_))
    ));
    assert_eq!(
        f.stored::<i64>("SELECT count(*) FROM candidate_decisions"),
        0
    );
}

#[test]
fn inferred_technical_selection_uses_actual_attempt_and_result_before_later_report() {
    let f = Fixture::new();
    let source = "native:synthetic-device:codex";
    let conversation = "technical";
    let broad = format!("{}\ntool_call search broad copper", "thinking ".repeat(500));
    f.capture(
        "attempt-1",
        conversation,
        &broad,
        "assistant",
        source,
        json!({"native_id":"a1","extraction_mode":"message","evidence_eligible":false}),
        "2026-10-18T12:00:00Z",
    );
    f.capture(
        "result-1",
        conversation,
        "broad result: unrelated copper prices",
        "tool",
        source,
        json!({"native_id":"r1","extraction_mode":"tool_output","evidence_eligible":true}),
        "2026-10-18T12:00:01Z",
    );
    let attempt = "tool_call search exact copper observatory";
    let attempt_id = f.capture(
        "attempt-2",
        conversation,
        attempt,
        "assistant",
        source,
        json!({"native_id":"a2","extraction_mode":"message","evidence_eligible":false}),
        "2026-10-18T12:00:02Z",
    );
    let result = "exact result: original observatory note found";
    let result_id = f.capture(
        "result-2",
        conversation,
        result,
        "tool",
        source,
        json!({"native_id":"r2","extraction_mode":"tool_output","evidence_eligible":true}),
        "2026-10-18T12:00:03Z",
    );
    let report = "Narrow exact phrase search after irrelevant broad matches.";
    f.capture(
        "report",
        conversation,
        &format!("tool_call report: {report}"),
        "assistant",
        source,
        json!({"native_id":"a3","extraction_mode":"message","evidence_eligible":false}),
        "2026-10-18T12:00:04Z",
    );
    f.extract(source, conversation, report, "experience", false);
    let claimed = f.claim(source, conversation, Some(vec!["consolidate".into()]));
    let work = &claimed["work"][0];
    let windows = work["input"]["raw_windows"].as_array().unwrap();
    assert!(windows.iter().any(|window| {
        window["start"].as_u64().unwrap() > 0
            && window["text"]
                .as_str()
                .unwrap()
                .contains("tool_call search broad copper")
    }));
    let candidate = &work["input"]["candidates"][0];
    let mut learned = action(candidate, "remember");
    learned["text"] = json!(
        "When a broad source search yields irrelevant matches, narrow to the exact phrase before concluding no evidence exists."
    );
    learned["behavior"] = json!({"applicability":"Source search with irrelevant broad matches","conduct":"Try the exact source phrase before concluding evidence is absent.","rationale":"The observed narrower query found the original note.","exceptions":[],"authority":"inferred"});
    learned["evidence"].as_array_mut().unwrap().extend([
        json!({"episode_id":attempt_id,"quote":attempt,"occurrence":null}),
        json!({"episode_id":result_id,"quote":result,"occurrence":null}),
    ]);
    let error = f
        .complete(work, json!({"actions":[learned.clone()]}))
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "consolidation_behavior_requires_user_ground_truth"
    );
    let mut explicit = learned.clone();
    explicit["behavior"]["authority"] = json!("explicit_user");
    assert_eq!(
        f.complete(work, json!({"actions":[explicit]}))
            .unwrap_err()
            .to_string(),
        "consolidation_behavior_requires_user_ground_truth"
    );
    learned["technical_evidence"] = json!([attempt_id, result_id]);
    let committed = f.complete(work, json!({"actions":[learned]})).unwrap();
    let node = committed["results"][candidate["id"].as_str().unwrap()]["id"]
        .as_str()
        .unwrap();
    let history = f.graph.history(node).unwrap();
    assert_eq!(history[0]["behavior"]["authority"], "inferred");
    assert_eq!(history[0]["evidence"].as_array().unwrap().len(), 3);
    // Structural provenance/order only. No claim that a real model inferred
    // causality correctly, or that arbitrary later output proves success.
}

#[test]
fn exposure_receipt_uploaded_after_feedback_remains_advisory_not_evidence() {
    let f = Fixture::new();
    let preference =
        "For saved source facts, always keep the exact quote and its original context.";
    f.capture(
        "preference",
        "thread",
        preference,
        "user",
        "contract",
        json!({}),
        "2026-10-18T12:00:00Z",
    );
    f.extract("contract", "thread", preference, "experience", false);
    let work = f.consolidate_work();
    let candidate = &work["input"]["candidates"][0];
    let mut saved = action(candidate, "remember");
    saved["behavior"] = json!({"applicability":"Saving source facts","conduct":"Keep exact source quotes and their original context.","rationale":"The owner directly requested this saved-source practice.","exceptions":[],"authority":"explicit_user"});
    let committed = f.complete(&work, json!({"actions":[saved]})).unwrap();
    let node = committed["results"][candidate["id"].as_str().unwrap()]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let feedback = "Keep exact evidence for facts, but do not apply that rule to prose drafts.";
    f.capture(
        "feedback",
        "thread",
        feedback,
        "user",
        "contract",
        json!({}),
        "2030-01-02T12:00:00Z",
    );
    let receipt = f.capture(
        "late-receipt",
        "thread",
        "",
        "native_hook",
        "contract",
        json!({"extraction_mode":"skip","evidence_eligible":false,"exposure":[[node,1]]}),
        "2030-01-01T12:00:00Z",
    );
    f.extract("contract", "thread", feedback, "experience", false);
    let work = f.consolidate_work();
    let offers = work["input"]["behavior_exposures"].as_array().unwrap();
    assert_eq!(offers.len(), 1);
    assert_eq!(offers[0]["receipt_episode_id"], receipt);
    assert_eq!(offers[0]["shown"]["id"], node);
    assert_eq!(offers[0]["current_target"], node);
    assert!(
        !work["input"]["raw_windows"]
            .as_array()
            .unwrap()
            .iter()
            .any(|window| window["episode_id"] == receipt)
    );
    assert!(
        !work["input"]["candidates"][0]["evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|reference| reference["episode_id"] == receipt)
    );
}

#[test]
fn lifecycle_evidence_rejects_stale_completion_then_closes_only_after_manual_boundary() {
    let f = Fixture::new();
    let pending = f.capture(
        "pending",
        "thread",
        FACT,
        "user",
        "contract",
        json!({}),
        "2020-01-01T00:00:00Z",
    );
    let saved=f.graph.remember(serde_json::from_value(json!({"command_id":"pending-save","actor":"owner","why":"Outstanding signed report","type":"knowledge","text":FACT,"scope":["professional"],"evidence":[{"episode_id":pending,"quote":FACT}]})).unwrap()).unwrap();
    let target = saved["id"].as_str().unwrap().to_owned();
    f.graph.loop_command(serde_json::from_value(json!({"action":"open","id":target,"command_id":"manual-open","actor":"owner","why":"This report remains outstanding"})).unwrap()).unwrap();
    f.graph
        .store
        .write(|tx| {
            tx.sql()
                .execute("UPDATE processing_progress SET seq=latest_seq", [])?;
            Ok(())
        })
        .unwrap();
    let completion = "The copper observatory signed launch report is complete.";
    for (event, date, expected) in [
        ("stale", "2025-01-01T00:00:00Z", "not_newer"),
        ("newer", "2030-01-01T00:00:00Z", "closed"),
    ] {
        f.capture(
            event,
            "thread",
            completion,
            "user",
            "contract",
            json!({}),
            date,
        );
        f.extract("contract", "thread", completion, "knowledge", false);
        let work = f.consolidate_work();
        assert!(
            work["input"]["existing_nodes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|node| node["id"] == target)
        );
        let candidate = &work["input"]["candidates"][0];
        let mut close = action(candidate, "reject");
        close["closed_targets"] = json!([target]);
        let committed = f.complete(&work, json!({"actions":[close]})).unwrap();
        assert_eq!(committed["loop_closures"][0]["status"], expected);
    }
    assert_eq!(
        f.graph.history(&target).unwrap()[0]["loop"]["state"],
        "closed"
    );
    assert_eq!(f.graph.history(&target).unwrap().len(), 1);
}

#[test]
fn maintenance_context_stays_bounded_and_unchanged_page_does_not_repeat_forever() {
    let f = Fixture::new();
    let texts = (0..51)
        .map(|index| format!("The observatory facet {index} is stable."))
        .collect::<Vec<_>>();
    let source = f.capture(
        "many",
        "thread",
        &texts.join("\n"),
        "user",
        "contract",
        json!({}),
        "2026-10-18T12:00:00Z",
    );
    let mut targets = vec![];
    for (index, text) in texts.iter().enumerate() {
        let saved=f.graph.remember(serde_json::from_value(json!({"command_id":format!("facet-{index}"),"actor":"owner","why":"Original explicit source fact","type":"knowledge","text":text,"scope":["professional"],"evidence":[{"episode_id":source,"quote":text}]})).unwrap()).unwrap();
        targets.push(saved["id"].as_str().unwrap().to_owned());
    }
    f.graph
        .store
        .write(|tx| {
            tx.sql()
                .execute("UPDATE processing_progress SET seq=latest_seq", [])?;
            Ok(())
        })
        .unwrap();
    let maintenance = f
        .processing
        .maintain(processing::Maintenance {
            command_id: "maintain".into(),
            actor: "owner".into(),
            why: "Inspect these exact related source-backed subjects".into(),
            target_ids: targets[..48].to_vec(),
            origin: None,
        })
        .unwrap();
    let group = maintenance["repair_group_id"].as_str().unwrap();
    let mut pages = 0;
    let mut inspected = std::collections::BTreeSet::new();
    loop {
        let claimed = f.claim("contract", "thread", Some(vec!["consolidate".into()]));
        if claimed["work"].as_array().unwrap().is_empty() {
            break;
        }
        pages += 1;
        assert!(pages <= 3);
        let work = &claimed["work"][0];
        let nodes = work["input"]["existing_nodes"].as_array().unwrap();
        assert!(nodes.len() <= 48);
        inspected.extend(
            nodes
                .iter()
                .map(|node| node["id"].as_str().unwrap().to_owned()),
        );
        let seed = work["input"]["maintenance_ids"][0].clone();
        let continuation = if work["input"]["context_truncated"] == true {
            vec![seed]
        } else {
            vec![]
        };
        f.complete(
            work,
            json!({"actions":[],"repairs":[],"continue_with":continuation}),
        )
        .unwrap();
    }
    assert_eq!(pages, 2);
    assert_eq!(inspected.len(), 51);
    let state = f
        .graph
        .store
        .read(|tx| {
            Ok(tx.sql().query_row(
                "SELECT state FROM repair_groups WHERE id=?",
                [group],
                |row| row.get::<_, String>(0),
            )?)
        })
        .unwrap();
    assert_eq!(state, "done");
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM node_versions"), 51);
}

#[test]
fn worker_finishes_one_unit_and_releases_writer_while_shutdown_preserves_pending() {
    use std::{
        collections::HashMap,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        time::Duration,
    };
    let f = Fixture::new();
    let queue = jobs::Queue::new(f.graph.clone());
    f.graph
        .store
        .write(|tx| {
            jobs::Queue::insert(tx, "media-one", "media", &json!({"ordinal":1}))?;
            jobs::Queue::insert(tx, "media-two", "media", &json!({"ordinal":2}))?;
            Ok(())
        })
        .unwrap();
    let (start_tx, start_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let done_rx = Arc::new(Mutex::new(done_rx));
    let count = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&count);
    let handler: jobs::Handler = Arc::new(move |_job| {
        observed.fetch_add(1, Ordering::SeqCst);
        start_tx.send(()).unwrap();
        done_rx.lock().unwrap().recv().unwrap();
        Ok(json!({"indexed":true}))
    });
    let mut handlers = HashMap::new();
    handlers.insert("media".into(), handler);
    let mut worker = jobs::Worker::new(queue, handlers, None);
    worker.start().unwrap();
    start_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    f.graph
        .store
        .write(|tx| {
            tx.sql().execute(
                "INSERT INTO historical_activation VALUES (1,?)",
                [util::now()],
            )?;
            Ok(())
        })
        .unwrap();
    let release = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        done_tx.send(()).unwrap();
    });
    worker.close();
    release.join().unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(
        f.stored::<i64>("SELECT count(*) FROM jobs WHERE state='done'"),
        1
    );
    assert_eq!(
        f.stored::<i64>("SELECT count(*) FROM jobs WHERE state='pending'"),
        1
    );
}

#[test]
fn stored_legacy_instruction_candidate_stays_searchable_but_never_enters_model_work() {
    let f = Fixture::new();
    let text = "# AGENTS.md instructions\n\n<INSTRUCTIONS>\nNever turn this native policy into a remembered user preference.\n</INSTRUCTIONS>";
    let source = f.capture(
        "old-legacy",
        "legacy",
        text,
        "user",
        "native:fixture:codex",
        json!({}),
        "2020-01-01T00:00:00Z",
    );
    let quote = "Never turn this native policy into a remembered user preference.";
    f.graph.store.command(&context("old-legacy-extraction".into(),"legacy-extractor".into(),"Synthetic pre-exclusion stored candidate".into()),&json!({"op":"extract","output":{}}),|tx,commit|{tx.sql().execute("INSERT INTO extraction_runs VALUES ('legacy-run',?,?,'extract-v2','connected-harness-default','[]','null',?)",params![commit,source,util::now()])?;tx.sql().execute("INSERT INTO candidates(id,run_id,kind,type,text,why,src,dst,confidence,valid_from,valid_to,scope) VALUES ('c_legacy','legacy-run','memory','knowledge',?,'Historical fixture',NULL,NULL,0.8,NULL,NULL,'[\"professional\"]')",[quote])?;TxGraph::evidence(tx,"c_legacy",1,&json!([{"episode_id":source,"quote":quote}]))?;Ok(json!({"candidate_count":1}))}).unwrap();
    let history = f.graph.history("c_legacy").unwrap();
    assert_eq!(history[0]["state"], "candidate");
    assert_eq!(history[0]["evidence"][0]["quote"], quote);
    assert!(
        f.processing
            .consolidator
            .pending(16, 0, i64::MAX, false)
            .unwrap()
            .is_empty()
    );
    let claimed = f.claim("native:fixture:codex", "legacy", None);
    assert!(claimed["work"].as_array().unwrap().is_empty());
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM candidates"), 1);
    assert_eq!(
        f.stored::<i64>("SELECT count(*) FROM candidate_decisions"),
        0
    );
    let job = jobs::Job {
        queue: f.processing.queue.clone(),
        id: "stale-legacy".into(),
        kind: "consolidate".into(),
        payload: json!({}),
        owner: "stale-owner".into(),
        attempts: 1,
    };
    let error = f
        .processing
        .consolidator
        .apply(
            &job,
            &json!({"loop_policy_version":consolidation::VERSION,"candidates":[{"id":"c_legacy"}]}),
            &json!({"malformed":true}),
            "fixture",
            None,
            None,
        )
        .unwrap_err();
    assert_eq!(
        error.error.to_string(),
        "Consolidation eligibility changed; derive a fresh plan"
    );
}

#[test]
fn continuity_repair_rechecks_loops_and_folds_only_the_frozen_identity_relation() {
    let f = Fixture::new();
    let original = "Vega is the organization's short name. Vega Incorporated is its full name. Orion belongs to Vega.";
    let source = f.capture(
        "identity-source",
        "thread",
        original,
        "user",
        "contract",
        json!({}),
        "2020-01-01T00:00:00Z",
    );
    let save = |key: &str, kind: &str, text: &str, quote: &str| {
        f.graph.remember(serde_json::from_value(json!({"command_id":key,"actor":"owner","why":"Explicit original identity source","type":kind,"text":text,"scope":["professional"],"evidence":[{"episode_id":source,"quote":quote}]})).unwrap()).unwrap()
    };
    let old = save(
        "old-org",
        "organization",
        "Vega",
        "Vega is the organization's short name.",
    );
    let project = save("project", "project", "Orion", "Orion belongs to Vega.");
    f.graph.link(serde_json::from_value(json!({"command_id":"original-edge","actor":"owner","why":"Explicit directional membership","a":project["id"],"rel":"belongs_to","b":old["id"],"evidence":[{"episode_id":source,"quote":"Orion belongs to Vega."}]})).unwrap()).unwrap();
    // Continuity repair serves identities already split by the retired
    // replacement-ID supersede path.
    let refined=crate::recovery::legacy_supersede(&f.graph,json!({"command_id":"full-name","old":old["id"],"kind":"refinement","new":{"type":"organization","text":"Vega Incorporated","scope":["professional"]},"evidence":[{"episode_id":source,"quote":"Vega Incorporated is its full name."}]})).unwrap();
    let target = refined["superseded_by"].as_str().unwrap().to_owned();
    f.graph.store.command(&context("legacy-orphan-import".into(),"fixture".into(),"Synthetic legacy orphan relation for continuity recovery".into()),&json!({"op":"fixture"}),|tx,commit|{tx.sql().execute("INSERT INTO edge_versions VALUES ('legacy-orphan',1,?,?, 'belongs_to',?,'active')",params![commit,project["id"].as_str(),old["id"].as_str()])?;TxGraph::evidence(tx,"legacy-orphan",1,&json!([{"episode_id":source,"quote":"Orion belongs to Vega."}]))?;tx.sql().execute("UPDATE processing_progress SET seq=latest_seq",[])?;Ok(json!({"seeded":true}))}).unwrap();
    f.processing
        .maintain(processing::Maintenance {
            command_id: "maintain-continuity".into(),
            actor: "owner".into(),
            why: "Inspect the known refined identity".into(),
            target_ids: vec![target.clone()],
            origin: None,
        })
        .unwrap();
    let work = f.consolidate_work();
    f.graph.loop_command(serde_json::from_value(json!({"action":"open","id":target,"command_id":"manual-after-prepare","actor":"owner","why":"Preserve a current chosen marker"})).unwrap()).unwrap();
    assert!(matches!(
        f.complete(&work, json!({"actions":[]})),
        Err(Error::Conflict(_))
    ));
    assert_eq!(f.graph.history("legacy-orphan").unwrap().len(), 1);
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
            tx.sql().execute(
                "UPDATE jobs SET available_at=0 WHERE kind='consolidate'",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    let retry = f.consolidate_work();
    let result = f.complete(&retry, json!({"actions":[]})).unwrap();
    assert!(
        !result["redirected_relations"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let history = f.graph.history("legacy-orphan").unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[1]["dst"], target);
    assert_eq!(history[1]["state"], "inactive");
    assert_eq!(
        f.graph.history(&target).unwrap().last().unwrap()["loop"]["state"],
        "open"
    );
}

#[test]
fn missing_retirement_fence_keeps_fixed_code_and_target_only_feedback() {
    let f = Fixture::new();
    let source = f.seed();
    let saved=f.graph.remember(serde_json::from_value(json!({"command_id":"retire-source","actor":"owner","why":"Source-backed inspection subject","type":"knowledge","text":FACT,"scope":["professional"],"evidence":[{"episode_id":source,"quote":FACT}]})).unwrap()).unwrap();
    let target = saved["id"].as_str().unwrap().to_owned();
    f.graph
        .store
        .write(|tx| {
            tx.sql()
                .execute("UPDATE processing_progress SET seq=latest_seq", [])?;
            Ok(())
        })
        .unwrap();
    f.processing
        .maintain(processing::Maintenance {
            command_id: "retire-maintain".into(),
            actor: "owner".into(),
            why: "Inspect exact selected subject".into(),
            target_ids: vec![target.clone()],
            origin: None,
        })
        .unwrap();
    let work = f.consolidate_work();
    let mut context =
        f.stored::<String>("SELECT context FROM jobs WHERE kind='consolidate' AND state='leased'");
    let mut leased: Value = serde_json::from_str(&context).unwrap();
    leased["_retirement"]["ready"]
        .as_object_mut()
        .unwrap()
        .remove(&target);
    context = serde_json::to_string(&leased).unwrap();
    f.graph
        .store
        .write(|tx| {
            tx.sql().execute(
                "UPDATE jobs SET context=? WHERE kind='consolidate' AND state='leased'",
                [&context],
            )?;
            Ok(())
        })
        .unwrap();
    let repair = json!({"operation":"retire","target":target,"evidence":[{"episode_id":source,"quote":FACT,"occurrence":null}],"why":"Synthetic noise qualification"});
    let error = f
        .complete(&work, json!({"actions":[],"repairs":[repair]}))
        .unwrap_err();
    assert_eq!(error.to_string(), "consolidation_retire_context_incomplete");
    let stored: Value = serde_json::from_str(
        &f.stored::<String>("SELECT context FROM jobs WHERE kind='consolidate' AND state='leased'"),
    )
    .unwrap();
    assert_eq!(
        stored["previous_rejection"]["references"],
        json!([{"target":target,"operation":"retire"}])
    );
    assert_eq!(f.graph.history(&target).unwrap().len(), 1);
}

#[test]
fn cooperative_stop_commits_current_media_unit_and_keeps_unfinished_job_pending() {
    use std::{
        collections::HashMap,
        sync::{Mutex, mpsc},
        time::Duration,
    };
    let f = Fixture::new();
    let queue = jobs::Queue::new(f.graph.clone());
    f.graph
        .store
        .write(|tx| {
            jobs::Queue::insert(tx, "multi-unit-media", "media", &json!({"units":10}))?;
            Ok(())
        })
        .unwrap();
    let (start_tx, start_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let handler: jobs::Handler = Arc::new(move |job| {
        start_tx.send(()).unwrap();
        release_rx.lock().unwrap().recv().unwrap();
        job.queue
            .graph
            .store
            .write(|tx| {
                job.check_in(tx.sql())?;
                tx.sql().execute(
                    "INSERT INTO historical_activation VALUES (1,?)",
                    [util::now()],
                )?;
                Ok(())
            })
            .unwrap();
        assert!(job.stopping());
        Err(jobs::JobFailure {
            category: "RuntimeError",
            retryable: true,
            retry_after: Some(Duration::ZERO),
        })
    });
    let mut handlers = HashMap::new();
    handlers.insert("media".into(), handler);
    let mut worker = jobs::Worker::new(queue, handlers, None);
    worker.start().unwrap();
    start_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let releasing = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        release_tx.send(()).unwrap();
    });
    worker.close();
    releasing.join().unwrap();
    assert_eq!(
        f.stored::<i64>("SELECT count(*) FROM historical_activation"),
        1
    );
    assert_eq!(
        f.stored::<i64>("SELECT count(*) FROM jobs WHERE state='pending' AND owner IS NULL"),
        1
    );
    assert_eq!(
        f.stored::<i64>("SELECT count(*) FROM jobs WHERE state='done'"),
        0
    );
}

#[test]
fn committed_usage_and_query_feedback_transfer_once_through_refinement_and_reviewed_merge() {
    let f = Fixture::new();
    let original = "Vega Incorporated and Vega are the same organization. The launch report is pending. The launch report is pending legal approval.";
    let episode = f.capture(
        "usage-source",
        "thread",
        original,
        "user",
        "contract",
        json!({}),
        "2026-10-18T12:00:00Z",
    );
    let save = |key: &str, kind: &str, text: &str, quote: &str| {
        f.graph.remember(serde_json::from_value(json!({"command_id":key,"actor":"owner","why":"Explicit original source","type":kind,"text":text,"scope":["professional"],"evidence":[{"episode_id":episode,"quote":quote}]})).unwrap()).unwrap()
    };
    let quote = "Vega Incorporated and Vega are the same organization.";
    let a = save("usage-a", "organization", "Vega Incorporated", quote);
    let b = save("usage-b", "organization", "Vega", quote);
    let a = a["id"].as_str().unwrap().to_owned();
    let b = b["id"].as_str().unwrap().to_owned();
    let cite_a = dto::Citation {
        event_id: "usage-cite-a".into(),
        ids: vec![a.clone()],
        query: Some("Vega".into()),
        shown: vec![a.clone(), b.clone()],
    };
    f.graph.cite(cite_a.clone()).unwrap();
    f.graph
        .cite(dto::Citation {
            event_id: "usage-cite-b".into(),
            ids: vec![b.clone()],
            query: Some("Vega".into()),
            shown: vec![b.clone()],
        })
        .unwrap();
    let request =
        json!({"op":"merge","a":a,"b":b,"evidence":[{"episode_id":episode,"quote":quote}]});
    let proposal = f
        .graph
        .store
        .command(
            &context(
                "usage-merge-propose".into(),
                "processor".into(),
                "Exact same-identity original evidence".into(),
            ),
            &request,
            |tx, commit| curation::propose(tx, commit, &request),
        )
        .unwrap();
    let approval = dto::Review {
        command_id: "usage-merge-approved".into(),
        actor: "owner".into(),
        why: "Explicit reviewed same-identity merge".into(),
        proposal_id: proposal["id"].as_str().unwrap().into(),
        decision: "approve".into(),
    };
    let first = f.graph.decide_review(approval.clone()).unwrap();
    assert_eq!(f.graph.decide_review(approval).unwrap(), first);
    f.graph.cite(cite_a).unwrap();
    let stats = f
        .graph
        .store
        .read(|tx| {
            Ok((
                tx.sql().query_row(
                    "SELECT access_count FROM node_stats WHERE node_id=?",
                    [&b],
                    |row| row.get::<_, i64>(0),
                )?,
                tx.sql().query_row(
                    "SELECT shown,cited FROM query_feedback WHERE query_key=? AND target=?",
                    params![importance::query_key("Vega"), b],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )?,
            ))
        })
        .unwrap();
    assert_eq!(stats, (2, (3, 2)));
    let pending = save(
        "usage-pending",
        "knowledge",
        "The launch report is pending.",
        "The launch report is pending.",
    );
    let pending = pending["id"].as_str().unwrap().to_owned();
    f.graph
        .cite(dto::Citation {
            event_id: "usage-pending-cite".into(),
            ids: vec![pending.clone()],
            query: Some("launch report".into()),
            shown: vec![pending.clone()],
        })
        .unwrap();
    let refinement:dto::Supersede=serde_json::from_value(json!({"command_id":"usage-refine","actor":"owner","why":"Supported additional legal condition","old":pending,"kind":"refinement","new":{"type":"knowledge","text":"The launch report is pending legal approval.","scope":["professional"]},"evidence":[{"episode_id":episode,"quote":"The launch report is pending legal approval."}]})).unwrap();
    let refined = f.graph.supersede(refinement.clone()).unwrap();
    assert_eq!(f.graph.supersede(refinement).unwrap(), refined);
    let current = refined["id"].as_str().unwrap();
    assert_eq!(current, pending);
    assert_eq!(refined["version"], 2);
    let stats = f
        .graph
        .store
        .read(|tx| {
            Ok((
                tx.sql().query_row(
                    "SELECT access_count FROM node_stats WHERE node_id=?",
                    [current],
                    |row| row.get::<_, i64>(0),
                )?,
                tx.sql().query_row(
                    "SELECT shown,cited FROM query_feedback WHERE query_key=? AND target=?",
                    params![importance::query_key("launch report"), current],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )?,
            ))
        })
        .unwrap();
    assert_eq!(stats, (1, (1, 1)));
}

#[test]
fn one_consolidation_plan_revises_the_same_memory_twice() {
    let f = Fixture::new();
    let signed = "The copper observatory launch report is signed.";
    let filed = "The copper observatory launch report was filed.";
    let episode = f.capture("one", "thread", &format!("{FACT} {signed} {filed}"), "user", "contract", json!({}), "2026-10-18T12:00:00Z");
    let existing=f.graph.remember(serde_json::from_value(json!({"command_id":"existing","actor":"owner","why":"Original pending fact","type":"knowledge","text":FACT,"scope":["professional"],"evidence":[{"episode_id":episode,"quote":FACT}]})).unwrap()).unwrap();
    let target = existing["id"].as_str().unwrap();
    let extraction = f.claim("contract", "thread", Some(vec!["extract".into()]))["work"][0].clone();
    let candidates = [signed, filed].iter().enumerate().map(|(n, text)| { let mut item = draft(text, "knowledge", false); item["key"] = json!(format!("k{n}")); item }).collect::<Vec<_>>();
    f.complete(&extraction, json!({"windows":[{"id":extraction["input"]["windows"][0]["id"],"candidates":candidates}]})).unwrap();
    let work = f.consolidate_work();
    assert!(work["input"]["existing_nodes"].as_array().unwrap().iter().any(|node| node["id"] == target));
    let mut actions = vec![];
    for candidate in work["input"]["candidates"].as_array().unwrap() {
        let mut revise = action(candidate, "supersede");
        revise["target"] = json!(target);
        if candidate["text"] == signed {
            revise["change_kind"] = json!("refinement");
        } else {
            revise["change_kind"] = json!("change");
            revise["valid_at"] = json!("2026-10-19T00:00:00Z");
        }
        actions.push(revise);
    }
    // Refinement first, then the world change, both against the same memory.
    actions.sort_by_key(|item| item["change_kind"] != "refinement");
    let result = f.complete(&work, json!({"actions":actions})).unwrap();
    for item in &actions {
        let outcome = &result["results"][item["candidate_id"].as_str().unwrap()];
        assert_eq!(outcome["id"], target);
        assert_eq!(outcome["decision"], "promote");
    }
    let history = f.graph.history(target).unwrap();
    assert!(history.iter().all(|node| node["id"] == target));
    assert_eq!(history.len(), 3);
    assert_eq!(history[1]["text"], signed);
    assert_eq!(history[1]["revision"]["kind"], "refinement");
    assert_eq!(history[2]["text"], filed);
    assert_eq!(history[2]["revision"]["kind"], "change");
    assert_eq!(history[2]["valid_from"], "2026-10-19T00:00:00+00:00");
    assert_eq!(f.stored::<i64>("SELECT count(DISTINCT id) FROM node_versions"), 1);
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM edge_versions WHERE rel='supersedes'"), 0);
}

#[test]
fn pre_revision_policy_lease_is_refused_and_rederived_without_losing_candidates() {
    let f = Fixture::new();
    f.seed();
    let extraction = f.claim("contract", "thread", Some(vec!["extract".into()]))["work"][0].clone();
    f.complete(&extraction, json!({"windows":[{"id":extraction["input"]["windows"][0]["id"],"candidates":[draft(FACT,"knowledge",false)]}]})).unwrap();
    let work = f.consolidate_work();
    let ids = |work: &Value| work["input"]["candidates"].as_array().unwrap().iter().map(|c| c["id"].clone()).collect::<Vec<_>>();
    assert_eq!(ids(&work).len(), 1);
    // A lease prepared under replacement-ID semantics cannot complete now.
    f.graph.store.write(|tx| { tx.sql().execute("UPDATE jobs SET context=json_set(context,'$.loop_policy_version','consolidate-v8') WHERE kind='consolidate' AND state='leased'", [])?; Ok(()) }).unwrap();
    let error = f.complete(&work, json!({"actions":[]})).unwrap_err();
    assert_eq!(error.to_string(), "Consolidation loop policy changed; derive a fresh plan");
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM candidate_decisions"), 0);
    f.processing.release(serde_json::from_value(json!({"id":work["id"],"owner":work["owner"],"error":"service_rejected"})).unwrap()).unwrap();
    f.graph.store.write(|tx| { tx.sql().execute("UPDATE jobs SET available_at=0 WHERE kind='consolidate'", [])?; Ok(()) }).unwrap();
    let retry = f.consolidate_work();
    assert_eq!((&retry["id"], ids(&retry)), (&work["id"], ids(&work)));
    assert_eq!(f.stored::<String>("SELECT json_extract(context,'$.loop_policy_version') FROM jobs WHERE kind='consolidate' AND state='leased'"), consolidation::VERSION);
    let actions = retry["input"]["candidates"].as_array().unwrap().iter().map(|c| action(c, "reject")).collect::<Vec<_>>();
    f.complete(&retry, json!({"actions":actions})).unwrap();
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM candidate_decisions"), 1);
}
