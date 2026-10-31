use crate::{
    operational_status::{self, RuntimeStatus},
    query_rows::rows,
    viewer::*,
};
use memento_core::{CommandContext, Error, Store};
use memento_domain::{Graph, processing::Processing};
use rusqlite::params;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{fs, os::unix::fs::PermissionsExt, sync::Arc};
const NOW: &str = "2026-10-26T00:00:00+00:00";
fn parse<T: DeserializeOwned>(v: Value) -> T {
    serde_json::from_value(v).unwrap()
}
struct F {
    _dir: tempfile::TempDir,
    store: Arc<Store>,
    graph: Graph,
    episode: String,
    a: String,
    b: String,
    c: String,
}
impl F {
    fn new() -> Self {
        let dir = tempfile::Builder::new()
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap();
        let store = Store::open(dir.path()).unwrap();
        let episode=store.capture(parse(json!({"source":"native:fixture:pi","event_id":"one","conversation_id":"thread","kind":"user","text":"<img src=x onerror=alert(1)> Ирина prefers tea. Atlas launch. Cedar budget."}))).unwrap().id;
        let graph = Graph::new(store.clone());
        let mut ids = Vec::new();
        for (key, text, scope) in [
            (
                "a",
                "<img src=x onerror=alert(1)> Ирина prefers tea",
                "personal",
            ),
            ("b", "Atlas launch", "professional"),
            ("c", "Cedar budget", "personal"),
        ] {
            ids.push(graph.remember(parse(json!({"command_id":key,"actor":"synthetic","why":"Exact source","type":"knowledge","text":text,"scope":[scope],"evidence":[{"episode_id":episode,"quote":text}]}))).unwrap()["id"].as_str().unwrap().to_owned());
        }
        Self {
            _dir: dir,
            store,
            graph,
            episode,
            a: ids[0].clone(),
            b: ids[1].clone(),
            c: ids[2].clone(),
        }
    }
    fn link(&self) -> String {
        self.graph.link(parse(json!({"command_id":"link","actor":"synthetic","why":"Related","a":self.a,"b":self.b,"rel":"related_to","evidence":[{"episode_id":self.episode,"quote":"Atlas launch"}]}))).unwrap()["id"].as_str().unwrap().into()
    }
    fn candidate(&self, id: &str, episode: &str) {
        self.candidate_scope(id,episode,"personal");
    }
    fn candidate_scope(&self, id: &str, episode: &str, scope: &str) {
        self.store.command(&CommandContext{command_id:format!("candidate-{id}"),actor:"fixture".into(),why:"Synthetic extraction".into()},&json!({"fixture":id}),|tx,commit|{let run=format!("run-{id}");tx.sql().execute("INSERT INTO extraction_runs VALUES(?,?,?,'extract-v2','fixture','[]','{}',?)",params![run,commit,episode,NOW])?;tx.sql().execute("INSERT INTO candidates VALUES(?,?,'memory','knowledge','Renew Zephyr passport','Synthetic exact source',NULL,NULL,0.9,NULL,NULL,?)",params![id,run,json!([scope]).to_string()])?;tx.sql().execute("INSERT INTO graph_evidence VALUES(?,1,?,0,5,'Exact')",params![id,episode])?;Ok(json!({"id":id}))}).unwrap();
    }
}
#[test]
fn graph_scope_views_loop_counts_xss_and_forgotten_boundaries() {
    let f = F::new();
    let edge = f.link();
    for (id, key) in [(&f.c, "open-c"), (&f.b, "open-b")] {
        f.graph
            .loop_command(parse(
                json!({"action":"open","id":id,"command_id":key,"actor":"owner","why":"Manual"}),
            ))
            .unwrap();
    }
    f.graph.loop_command(parse(json!({"action":"remind","id":f.c,"command_id":"due","actor":"owner","why":"Manual","due_at":"2000-01-01T00:00:00Z"}))).unwrap();
    f.graph.loop_command(parse(json!({"action":"later","id":f.b,"command_id":"later","actor":"owner","why":"Manual","days":3650}))).unwrap();
    f.store
        .read(|tx| {
            let all = graph(tx.sql(), &GraphQuery::default(), NOW)?;
            assert_eq!(all["total_nodes"], 3);
            assert_eq!(all["total_edges"], 1);
            assert_eq!(all["loops"]["open"], 1);
            assert_eq!(all["loops"]["deferred"], 1);
            assert!(
                all["nodes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|n| n["label"] == "<img src=x onerror=alert(1)> Ирина prefers tea")
            );
            let personal = graph(tx.sql(), &parse(json!({"scope":"personal"})), NOW)?;
            assert_eq!(personal["total_nodes"], 2);
            assert_eq!(personal["edges"], json!([]));
            let around = graph(tx.sql(), &parse(json!({"view":"around","id":f.a})), NOW)?;
            assert_eq!(around["nodes"].as_array().unwrap().len(), 2);
            assert_eq!(node(tx.sql(), &f.a, NOW)?["links"][0]["outgoing"], true);
            Ok(())
        })
        .unwrap();
    f.store
        .write(|tx| {
            tx.sql().execute(
                "INSERT INTO forgotten_targets VALUES(?,'fixture','edge')",
                [edge],
            )?;
            Ok(())
        })
        .unwrap();
    f.store
        .read(|tx| {
            assert_eq!(
                graph(tx.sql(), &GraphQuery::default(), NOW)?["edges"],
                json!([])
            );
            assert_eq!(node(tx.sql(), &f.a, NOW)?["links"], json!([]));
            Ok(())
        })
        .unwrap();
}
#[test]
fn complete_loop_view_includes_candidates_beyond_default_cap_with_scope_and_forgetting() {
    let f = F::new();
    for index in 0..103 {
        let id = format!("loop-candidate-{index}");
        f.candidate_scope(&id, &f.episode, if index==101 {"professional"} else {"personal"});
        f.graph.loop_command(parse(json!({"action":"open","id":id,"command_id":format!("open-{index}")}))).unwrap();
    }
    f.store.write(|tx| {
        tx.sql().execute("INSERT INTO forgotten_targets VALUES('loop-candidate-102','fixture','candidate')", [])?;
        Ok(())
    }).unwrap();
    f.store.read(|tx| {
        let bounded = graph(tx.sql(), &parse(json!({"view":"loops"})), NOW)?;
        assert_eq!(bounded["loops"]["candidates"]["total"], 102);
        assert_eq!(bounded["loops"]["candidates"]["items"].as_array().unwrap().len(), 100);
        let complete = graph(tx.sql(), &parse(json!({"view":"loops","all":true})), NOW)?;
        assert_eq!(complete["too_large"], false);
        assert_eq!(complete["loops"]["open"], 102);
        assert_eq!(complete["loops"]["candidates"]["items"].as_array().unwrap().len(), 102);
        let personal = graph(tx.sql(), &parse(json!({"view":"loops","all":true,"scope":"personal"})), NOW)?;
        assert_eq!(personal["loops"]["candidates"]["total"], 101);
        assert_eq!(personal["loops"]["candidates"]["items"].as_array().unwrap().len(), 101);
        assert!(personal["loops"]["candidates"]["items"].as_array().unwrap().iter().all(|item|item["id"]!="loop-candidate-101" && item["id"]!="loop-candidate-102"));
        assert_eq!(graph(tx.sql(), &parse(json!({"all":true})), NOW)?["loops"]["candidates"]["items"].as_array().unwrap().len(), 100);
        Ok(())
    }).unwrap();
}
#[test]
fn literal_search_paging_conversation_unicode_and_unavailable_text() {
    let f = F::new();
    let long = "🧠".repeat(4001);
    f.store.capture(parse(json!({"source":"other","event_id":"long","conversation_id":"c","kind":"user","text":long}))).unwrap();
    f.store
        .read(|tx| {
            assert_eq!(
                search(tx.sql(), &parse(json!({"q":"Atlas"})))?["results"][0]["id"],
                f.b
            );
            assert_eq!(
                search(tx.sql(), &parse(json!({"q":"Atlas","scope":"personal"})))?["results"],
                json!([])
            );
            assert!(search(tx.sql(), &parse(json!({"q":"\" OR NEAR("}))).is_ok());
            assert!(
                conversations(tx.sql(), &parse(json!({})))?["total"]
                    .as_i64()
                    .unwrap()
                    >= 2
            );
            let q = parse(json!({"source":"other","conversation_id":"c"}));
            let value = conversation(tx.sql(), &q, |r| {
                tx.original_text(r["id"].as_str().unwrap())
            })?;
            assert_eq!(
                value["items"][0]["text"].as_str().unwrap().chars().count(),
                4000
            );
            assert_eq!(value["items"][0]["truncated"], true);
            let unavailable = conversation(tx.sql(), &q, |_| {
                Err(Error::Unavailable("Original unavailable"))
            })?;
            assert_eq!(unavailable["items"][0]["text"], Value::Null);
            assert_eq!(unavailable["items"][0]["truncated"], false);
            Ok(())
        })
        .unwrap();
}
#[test]
fn candidate_status_legacy_exclusion_parking_and_private_error_sanitization() {
    let f = F::new();
    f.candidate("candidate", &f.episode);
    let developer=f.store.capture(parse(json!({"source":"native:fixture:codex","event_id":"developer","conversation_id":"instructions","kind":"developer","text":"Synthetic developer instruction"}))).unwrap().id;
    f.candidate("excluded", &developer);
    f.store.write(|tx|{tx.sql().execute("INSERT INTO jobs(id,job_key,kind,payload,state,available_at,attempts,error) VALUES('retry','retry','extract',?,'pending',0,1,'PRIVATE SOURCE MUST NOT LEAK')",[json!({"source":"native:fixture:pi"}).to_string()])?;Ok(())}).unwrap();
    f.store.write(|tx| { tx.sql().execute("INSERT INTO jobs(id,job_key,kind,payload,state,available_at,attempts,error) VALUES('rebatch','rebatch','consolidate','{}','failed',0,1,'rebatch_requested')", [])?; Ok(()) }).unwrap();
    let runtime = RuntimeStatus::disabled();
    f.store
        .read(|tx| {
            let value = operational_status::operational_status(tx.sql(), None, &runtime, 0.)?;
            assert_eq!(value["consolidation"]["rebatched_jobs"], 1);
            assert_eq!(value["consolidation"]["failed_jobs"], 0);
            assert_eq!(value["consolidation"]["errors"], json!([]));
            assert_eq!(value["consolidation"]["pending_candidates"], 1);
            assert_eq!(value["consolidation"]["ready_candidates"], 1);
            assert_eq!(value["consolidation"]["excluded_legacy_candidates"], 1);
            assert_eq!(value["extraction"]["retrying_jobs"], 1);
            assert_eq!(
                value["extraction"]["errors"][0]["category"],
                "unknown_error"
            );
            assert!(!value.to_string().contains("PRIVATE SOURCE"));
            assert_eq!(candidates(tx.sql(), &parse(json!({})))?["total"], 2);
            assert_eq!(
                candidates(tx.sql(), &parse(json!({"q":"Zephyr"})))?["total"],
                2
            );
            Ok(())
        })
        .unwrap();
    Processing::new(f.graph.clone()).park_history().unwrap();
    f.store
        .read(|tx| {
            let value = operational_status::operational_status(tx.sql(), None, &runtime, 0.)?;
            assert_eq!(value["consolidation"]["parked_candidates"], 1);
            assert_eq!(value["consolidation"]["pending_candidates"], 0);
            assert_eq!(value["historical"]["scope"], "global");
            assert_eq!(value["historical"]["active"], true);
            Ok(())
        })
        .unwrap();
}
#[test]
fn status_source_counts_global_verified_artifacts_and_disabled_runtime() {
    let f = F::new();
    f.store.capture(parse(json!({"source":"other","event_id":"one","conversation_id":"different","kind":"native_record","text":"raw Ω","metadata":{"extraction_mode":"skip"}}))).unwrap();
    let info = f
        .store
        .blobs()
        .put(&mut &b"synthetic artifact"[..], 100)
        .unwrap();
    f.store
        .write(|tx| {
            tx.register_blob(&info, "image/png")?;
            Ok(())
        })
        .unwrap();
    let runtime = RuntimeStatus::disabled();
    f.store
        .read(|tx| {
            let first =
                operational_status::operational_status(tx.sql(), Some("other"), &runtime, 0.)?;
            assert_eq!(first["captured"]["episodes"], 1);
            assert_eq!(first["captured"]["conversations"], 1);
            assert_eq!(first["raw_storage"]["inline_raw_bytes"], 6);
            assert_eq!(
                first["indexing"],
                json!({"available":false,"reason":"embedding_runtime_disabled"})
            );
            assert_eq!(first["artifact_storage"]["registered"]["objects"], 1);
            assert_eq!(first["artifact_storage"]["scope"], "global");
            let unrelated =
                operational_status::operational_status(tx.sql(), Some("unrelated"), &runtime, 0.)?;
            assert_eq!(unrelated["captured"]["episodes"], 0);
            assert_eq!(unrelated["artifact_storage"], first["artifact_storage"]);
            Ok(())
        })
        .unwrap();
}
#[test]
fn every_observation_preserves_authority_counters() {
    let f = F::new();
    f.candidate("candidate", &f.episode);
    let fingerprint = || {
        f.store.read(|tx|rows(tx.sql(),"SELECT (SELECT count(*) FROM graph_commits) AS commits,(SELECT count(*) FROM node_versions) AS versions,(SELECT count(*) FROM loop_events) AS loops,(SELECT count(*) FROM candidate_decisions) AS decisions,(SELECT sum(access_count) FROM node_stats) AS accesses,(SELECT count(*) FROM jobs) AS jobs",[])).unwrap()
    };
    let before = fingerprint();
    f.store
        .read(|tx| {
            graph(tx.sql(), &GraphQuery::default(), NOW)?;
            node(tx.sql(), &f.a, NOW)?;
            search(tx.sql(), &parse(json!({"q":"Atlas"})))?;
            conversations(tx.sql(), &parse(json!({})))?;
            conversation(
                tx.sql(),
                &parse(json!({"source":"native:fixture:pi","conversation_id":"thread"})),
                |r| tx.original_text(r["id"].as_str().unwrap()),
            )?;
            candidates(tx.sql(), &parse(json!({})))?;
            operational_status::operational_status(tx.sql(), None, &RuntimeStatus::disabled(), 0.)?;
            Ok(())
        })
        .unwrap();
    assert_eq!(fingerprint(), before);
}
#[test]
fn query_validation_host_allowlist_static_bundle_and_plain_connect() {
    let f = F::new();
    f.store
        .read(|tx| {
            assert!(search(tx.sql(), &parse(json!({"q":""}))).is_err());
            assert!(search(tx.sql(), &parse(json!({"q":"Atlas","scope":"invalid"}))).is_err());
            assert!(conversations(tx.sql(), &parse(json!({"limit":101}))).is_err());
            assert!(matches!(
                graph(
                    tx.sql(),
                    &parse(json!({"view":"around","id":"unknown"})),
                    NOW
                ),
                Err(Error::NotFound(_))
            ));
            let bad = RuntimeStatus {
                indexing: Value::Null,
                drive: Default::default(),
            };
            assert!(operational_status::operational_status(tx.sql(), None, &bad, 0.).is_err());
            Ok(())
        })
        .unwrap();
    assert!(serde_json::from_value::<GraphQuery>(json!({"unknown":1})).is_err());
    for (host, allowed) in [
        ("evil.example", false),
        ("127.0.0.1:9", true),
        ("[::1]:8765", true),
        ("zen.example.ts.net", true),
        ("zen.example.ts.net.evil.example", false),
    ] {
        assert_eq!(host_allowed(host, &["zen.example.ts.net".into()]), allowed);
    }
    assert!(!STATIC_FILES.contains(&"index.html"));
    assert!(CSP.contains("frame-ancestors 'none'"));
    let guide = crate::connect_guide::connect_guide();
    assert!(guide.starts_with("Memento: connect"));
    assert!(guide.contains("memento agent add HARNESS --url SERVICE_ORIGIN"));
    assert!(guide.contains("memento-drive configure"));
    assert!(!guide.contains("<html"));
}
#[test]
fn behavior_commit_mapping_preserves_full_record_and_focus() {
    let f = F::new();
    let behavior = json!({"authority":"inferred","trigger":"Repeated manual setup","action":"Use shipped installer","outcome":"Fewer setup errors","mechanism":"Reuse validated configuration","exceptions":[]});
    f.store.command(&CommandContext{command_id:"behavior-commit".into(),actor:"synthetic-model".into(),why:"Stored lesson fixture".into()},&json!({"op":"consolidate","output":{"actions":[{"candidate_id":"lesson-candidate","operation":"remember","type":"experience","behavior":behavior}]}}),|tx,commit|{tx.sql().execute("INSERT INTO node_versions VALUES('lesson',1,?,'experience','Use shipped installer','active',NULL,NULL,NULL,0,'[\"personal\"]')",[commit])?;Ok(json!({"results":{"lesson-candidate":{"id":"lesson"}}}))}).unwrap();
    f.store
        .read(|tx| {
            assert_eq!(node(tx.sql(), "lesson", NOW)?["behavior"], behavior);
            let value = graph(tx.sql(), &parse(json!({"view":"behaviors"})), NOW)?;
            assert_eq!(value["nodes"].as_array().unwrap().len(), 1);
            assert_eq!(value["nodes"][0]["id"], "lesson");
            assert_eq!(value["nodes"][0]["behavior"], "inferred");
            assert_eq!(value["nodes"][0]["focus"], true);
            Ok(())
        })
        .unwrap();
}
