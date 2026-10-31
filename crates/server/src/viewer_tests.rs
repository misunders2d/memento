use crate::{
    operational_status::{self, RuntimeStatus},
    query_rows::{self, ACTIVE_CANDIDATE, rows},
    viewer::*,
};
use memento_core::{CommandContext, Error, Result, Store};
use memento_domain::{Graph, loops, processing::Processing};
use rusqlite::{Connection, params};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fs,
    os::unix::fs::PermissionsExt,
    sync::Arc,
};
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

/// Verbatim pre-change `graph()` (whole-graph materialization), kept as the selection oracle.
fn legacy_graph(db: &Connection, q: &GraphQuery, now: &str, cap: usize) -> Result<Value> {
    scope(q.scope.as_deref())?;
    if q.responsibility.as_deref().is_some_and(|value|!loops::valid_responsibility(value)) {
        return Err(Error::Validation("Invalid loop responsibility"));
    }
    if !["memories", "behaviors", "loops", "around"].contains(&q.view.as_str()) {
        return Err(Error::Validation("Invalid graph view"));
    }
    let mut nodes = HashMap::new();
    let mut order = Vec::new();
    for row in rows(
        db,
        &format!(
            "SELECT n.id,n.type,substr(n.text,1,240) AS text,n.scope,n.pinned,c.recorded_at,coalesce(s.importance,0.5)*coalesce(s.decay,1) AS weight FROM node_versions n JOIN graph_commits c ON c.id=n.commit_id LEFT JOIN node_stats s ON s.node_id=n.id WHERE {CURRENT_NODE}"
        ),
        [],
    )? {
        let id = row["id"].as_str().unwrap().to_owned();
        let weight = if row["pinned"].as_i64().unwrap_or(0) != 0 {
            1.0
        } else {
            format!(
                "{:.4}",
                row["weight"]
                    .as_f64()
                    .ok_or(Error::Unavailable("Invalid viewer weight"))?
            )
            .parse::<f64>()
            .map_err(|_| Error::Unavailable("Invalid viewer weight"))?
        };
        nodes.insert(id.clone(),json!({"id":id,"type":row["type"],"label":label(row["text"].as_str().unwrap()),"scope":query_rows::array(&row["scope"] )?,"weight":weight,"at":row["recorded_at"],"behavior":null,"loop":null}));
        order.push(id);
    }
    let mut edges = rows(
        db,
        &format!("SELECT e.id,e.src,e.rel,e.dst FROM edge_versions e WHERE {CURRENT_EDGE}"),
        [],
    )?
    .into_iter()
    .filter(|r| {
        nodes.contains_key(r["src"].as_str().unwrap())
            && nodes.contains_key(r["dst"].as_str().unwrap())
    })
    .collect::<Vec<_>>();
    for edge in &mut edges {
        edge["loop"] = Value::Null;
    }
    let every = rows(db, "SELECT DISTINCT id FROM node_versions", [])?.into_iter().map(|r| r["id"].as_str().unwrap().to_owned()).collect::<Vec<_>>();
    for (id, behavior) in behavior_map(db, &every)? {
        if let Some(node) = nodes.get_mut(&id) {
            node["behavior"] = behavior["authority"].clone();
        }
    }
    let markers = rows(db,&format!("SELECT l.target,l.due_at,l.deferred_until FROM loop_events l WHERE {OPEN_LOOP}"),[])?;
    let roles = loops::responsibilities(db,&markers.iter().map(|row|row["target"].as_str().unwrap().to_owned()).collect::<Vec<_>>())?;
    for mut row in markers {
        let target = row["target"].as_str().unwrap().to_owned();
        let role=roles.get(&target).and_then(|value|value["responsibility"].as_str()).unwrap_or("unknown");
        if q.responsibility.as_deref().is_some_and(|wanted|wanted!=role) { continue; }
        row["responsibility"]=json!(role);
        if let Some(node) = nodes.get_mut(&target) {
            node["loop"] = loop_value(&row, now);
        } else if let Some(edge) = edges.iter_mut().find(|e| e["id"] == target) {
            edge["loop"] = loop_value(&row, now);
        }
    }
    let candidates=loops::visible_loop_targets(db,rows(db,&format!("SELECT c.id,c.kind AS candidate_kind,c.src AS loop_src,c.dst AS loop_dst,substr(c.text,1,240) AS text,c.scope,l.due_at,l.deferred_until FROM loop_events l JOIN candidates c ON c.id=l.target WHERE {OPEN_LOOP} AND {ACTIVE_CANDIDATE} ORDER BY l.due_at IS NULL,l.due_at,c.rowid DESC"),[])?)?.into_iter().map(|mut row|{row["responsibility"]=json!(roles.get(row["id"].as_str().unwrap()).and_then(|value|value["responsibility"].as_str()).unwrap_or("unknown"));Ok(json!({"id":row["id"],"label":label(row["text"].as_str().unwrap()),"scope":query_rows::array(&row["scope"] )?,"loop":loop_value(&row,now)}))}).collect::<Result<Vec<_>>>()?.into_iter().filter(|r|within(r,q.scope.as_deref())&&q.responsibility.as_deref().is_none_or(|wanted|r["loop"]["responsibility"]==wanted)).collect::<Vec<_>>();
    let mut looped = nodes
        .values()
        .filter(|n| within(n, q.scope.as_deref()) && !n["loop"].is_null())
        .map(|n| n["loop"].clone())
        .collect::<Vec<_>>();
    looped.extend(
        edges
            .iter()
            .filter(|e| {
                !e["loop"].is_null()
                    && (within(&nodes[e["src"].as_str().unwrap()], q.scope.as_deref())
                        || within(&nodes[e["dst"].as_str().unwrap()], q.scope.as_deref()))
            })
            .map(|e| e["loop"].clone()),
    );
    looped.extend(candidates.iter().map(|c| c["loop"].clone()));
    let full_candidate_loops = q.all && q.view == "loops";
    let candidate_overflow = full_candidate_loops && candidates.len() > cap;
    let candidate_limit = if candidate_overflow {
        0
    } else if full_candidate_loops {
        cap
    } else {
        100
    };
    let loops = json!({"open":looped.iter().filter(|l|l["deferred"]!=true).count(),"deferred":looped.iter().filter(|l|l["deferred"]==true).count(),"candidates":{"total":candidates.len(),"items":candidates.iter().take(candidate_limit).cloned().collect::<Vec<_>>()}});
    nodes.retain(|_, n| within(n, q.scope.as_deref()));
    order.retain(|id| nodes.contains_key(id));
    let mut focus = HashSet::new();
    match q.view.as_str() {
        "behaviors" => focus.extend(
            nodes
                .iter()
                .filter(|(_, n)| !n["behavior"].is_null())
                .map(|(id, _)| id.clone()),
        ),
        "loops" => {
            focus.extend(
                nodes
                    .iter()
                    .filter(|(_, n)| !n["loop"].is_null())
                    .map(|(id, _)| id.clone()),
            );
            for edge in &edges {
                if !edge["loop"].is_null() {
                    for endpoint in ["src", "dst"] {
                        let id = edge[endpoint].as_str().unwrap();
                        if nodes.contains_key(id) {
                            focus.insert(id.to_owned());
                        }
                    }
                }
            }
        }
        "around" => {
            let id =
                q.id.as_ref()
                    .filter(|id| nodes.contains_key(*id))
                    .ok_or(Error::NotFound("Memory not found in this scope"))?;
            focus.insert(id.clone());
        }
        _ => (),
    }
    let chosen = if q.all && !(q.view == "loops" && q.responsibility.is_some()) {
        nodes.keys().cloned().collect::<HashSet<_>>()
    } else if q.view == "memories" {
        let mut ranked = order.clone();
        ranked.sort_by(|a, b| {
            nodes[b]["weight"]
                .as_f64()
                .unwrap()
                .total_cmp(&nodes[a]["weight"].as_f64().unwrap())
                .then(a.cmp(b))
        });
        let mut recent = order;
        recent.sort_by(|a, b| nodes[b]["at"].as_str().cmp(&nodes[a]["at"].as_str()));
        ranked
            .into_iter()
            .take(200)
            .chain(recent.into_iter().take(100))
            .collect()
    } else {
        let mut chosen = focus.clone();
        for edge in &edges {
            let a = edge["src"].as_str().unwrap();
            let b = edge["dst"].as_str().unwrap();
            if focus.contains(a) && nodes.contains_key(b) {
                chosen.insert(b.into());
            }
            if focus.contains(b) && nodes.contains_key(a) {
                chosen.insert(a.into());
            }
        }
        chosen
    };
    let too_large = chosen.len() > cap || candidate_overflow;
    let mut result = json!({"view":q.view,"total_nodes":nodes.len(),"total_edges":edges.iter().filter(|e|nodes.contains_key(e["src"].as_str().unwrap())&&nodes.contains_key(e["dst"].as_str().unwrap())).count(),"loops":loops,"too_large":too_large,"nodes":[],"edges":[]});
    if too_large {
        return Ok(result);
    }
    let sorted = chosen.iter().cloned().collect::<BTreeSet<_>>();
    result["nodes"] = json!(
        sorted
            .into_iter()
            .map(|id| {
                let mut node = nodes.remove(&id).unwrap();
                node["focus"] = json!(focus.contains(&id));
                node
            })
            .collect::<Vec<_>>()
    );
    result["edges"] = json!(
        edges
            .into_iter()
            .filter(|e| chosen.contains(e["src"].as_str().unwrap())
                && chosen.contains(e["dst"].as_str().unwrap()))
            .collect::<Vec<_>>()
    );
    Ok(result)
}
/// A deterministic graph with LIMIT-sized ties, pins, revisions, forgetting,
/// scopes, node/edge/candidate loops with roles, and consolidated behaviors.
fn synthetic(f: &F, n: usize) -> Vec<String> {
    let ctx = |id: String| CommandContext{command_id:id,actor:"fixture".into(),why:"Synthetic graph".into()};
    let types = ["knowledge", "person", "concept", "experience", "decision"];
    let scopes = [json!(["personal"]), json!(["professional"]), json!(["personal","professional"])];
    let id = |i: usize| format!("n{i:06}");
    for chunk in 0..n.div_ceil(23) {
        let range = chunk * 23..n.min((chunk + 1) * 23);
        let mut actions = Vec::new();
        let mut results = serde_json::Map::new();
        for i in range.clone() {
            let mut action = json!({"candidate_id":format!("cand-{i}"),"operation":"remember","type":types[i % 5],"text":format!("Synthetic memory {i} with a realistic amount of consolidated text to parse")});
            if types[i % 5] == "experience" && i % 3 != 2 {
                action["behavior"] = json!({"authority":if i % 2 == 0 {"inferred"} else {"explicit_user"},"applicability":"Synthetic trigger","conduct":"Synthetic conduct","rationale":"Synthetic rationale","exceptions":[]});
                if i % 7 == 0 {
                    action["behavior"].as_object_mut().unwrap().remove("authority");
                }
            }
            actions.push(action);
            results.insert(format!("cand-{i}"), json!({"id":id(i)}));
        }
        f.store.command(&ctx(format!("bulk-{chunk}")), &json!({"op":"consolidate","output":{"actions":actions}}), |tx, commit| {
            for i in range {
                tx.sql().execute("INSERT INTO node_versions VALUES(?,1,?,?,?,'active',NULL,NULL,NULL,?,?)", params![id(i), commit, types[i % 5], format!("Synthetic   memory\n{i}"), (i % 97 == 0) as i64, scopes[i % 3].to_string()])?;
                if i % 2 == 0 {
                    tx.sql().execute("INSERT INTO node_stats(node_id,importance,decay) VALUES(?,?,?)", params![id(i), [0.5, 0.65, 0.9, 0.3][i / 2 % 4], [1.0, 0.865, 0.5, 0.25, 0.123456][i / 2 % 5]])?;
                }
            }
            Ok(json!({"results":results}))
        }).unwrap();
    }
    let edges = (n / 9).max(150);
    let step = (n / 150).clamp(7, 211);
    let edge = |k: usize| format!("e{k:06}");
    f.store.command(&ctx("revise".into()), &json!({"fixture":"revise"}), |tx, commit| {
        for i in 0..n {
            if i % 11 == 0 || i % 13 == 0 {
                tx.sql().execute("INSERT INTO node_versions VALUES(?,2,?,?,?,?,NULL,NULL,NULL,0,?)", params![id(i), commit, types[i % 5], format!("Revised memory {i}"), if i % 13 == 0 {"deleted"} else {"active"}, scopes[(i + 1) % 3].to_string()])?;
            }
            if i % 31 == 0 {
                tx.sql().execute("INSERT INTO forgotten_targets VALUES(?,?,'node')", params![id(i), commit])?;
            }
        }
        for k in 0..edges {
            tx.sql().execute("INSERT INTO edge_versions VALUES(?,1,?,?,'related_to',?,'active')", params![edge(k), commit, id((k * 7919 + 13) % n), id((k * 104_729 + 7) % n)])?;
            if k % 17 == 0 {
                tx.sql().execute("INSERT INTO edge_versions VALUES(?,2,?,?,'related_to',?,'deleted')", params![edge(k), commit, id((k * 7919 + 13) % n), id((k * 104_729 + 7) % n)])?;
            }
            if k % 19 == 0 {
                tx.sql().execute("INSERT INTO forgotten_targets VALUES(?,?,'edge')", params![edge(k), commit])?;
            }
        }
        Ok(json!({}))
    }).unwrap();
    let candidates = (n / 130).clamp(5, 250);
    for c in 0..candidates {
        f.candidate_scope(&format!("loop-cand-{c}"), &f.episode, if c % 4 == 3 {"professional"} else {"personal"});
    }
    let mut targets = (0..n).filter(|i| i % step == 5).map(id).collect::<Vec<_>>();
    targets.extend((0..edges).filter(|k| k % step == 3).map(edge));
    targets.extend((0..candidates).map(|c| format!("loop-cand-{c}")));
    let mut roles = serde_json::Map::new();
    for (index, target) in targets.iter().enumerate() {
        if index % 3 < 2 {
            roles.insert(target.clone(), json!({"responsibility":if index % 3 == 0 {"owner"} else {"other"},"manual":true,"evidence":[]}));
        }
    }
    f.store.command(&ctx("loops".into()), &json!({"fixture":"loops"}), |tx, commit| {
        for (index, target) in targets.iter().enumerate() {
            let (due, deferred) = match index % 4 { 0 => (Some("2000-01-01T00:00:00Z"), None), 1 => (None, Some("2999-01-01T00:00:00Z")), 2 => (Some("2999-02-01T00:00:00Z"), None), _ => (None, None) };
            tx.sql().execute("INSERT INTO loop_events VALUES(?,?,?,'open',?,?)", params![format!("open-{target}"), target, commit, due, deferred])?;
        }
        Ok(json!({"loop_responsibilities":roles}))
    }).unwrap();
    f.store.command(&ctx("close".into()), &json!({"fixture":"close"}), |tx, commit| {
        for target in targets.iter().step_by(5) {
            tx.sql().execute("INSERT INTO loop_events VALUES(?,?,?,'closed',NULL,NULL)", params![format!("close-{target}"), target, commit])?;
        }
        Ok(json!({}))
    }).unwrap();
    // Around anchors: a behavior experience, a loop node, a forgotten and an inactive node.
    vec![id(3), id(5 + step), id(31), id(13), f.a.clone()]
}
#[test]
fn every_view_selects_exactly_what_whole_graph_materialization_selected() {
    let f = F::new();
    let anchors = synthetic(&f, 600);
    f.store.read(|tx| {
        let db = tx.sql();
        let mut views = ["memories", "behaviors", "loops"].map(|view| json!({"view":view})).to_vec();
        views.extend(anchors.iter().map(|id| json!({"view":"around","id":id})));
        let mut compared = 0;
        for cap in [MAX_NODES, 120, 3] {
            for view in &views {
                for scope in [None, Some("personal"), Some("professional")] {
                    for responsibility in [None, Some("owner"), Some("unknown")] {
                        for all in [false, true] {
                            let mut query = view.clone();
                            query["all"] = json!(all);
                            if let Some(scope) = scope { query["scope"] = json!(scope); }
                            if let Some(role) = responsibility { query["responsibility"] = json!(role); }
                            let q: GraphQuery = parse(query.clone());
                            match (legacy_graph(db, &q, NOW, cap), graph_capped(db, &q, NOW, cap)) {
                                (Ok(old), Ok(new)) => {
                                    assert_eq!(new, old, "{query} cap={cap}");
                                    compared += 1;
                                }
                                (Err(old), Err(new)) => assert_eq!(new.to_string(), old.to_string(), "{query}"),
                                (old, new) => panic!("{query} cap={cap}: legacy {:?} new {:?}", old.map(|_| ()), new.map(|_| ())),
                            }
                        }
                    }
                }
            }
        }
        // The fixture really exercises every limit and every focus kind.
        let memories = graph(db, &GraphQuery::default(), NOW)?;
        let shown = memories["nodes"].as_array().unwrap().len();
        assert!(memories["total_nodes"].as_u64().unwrap() > 500 && shown > 200 && shown <= 300, "{shown}");
        assert!(memories["nodes"].as_array().unwrap().iter().any(|n| n["weight"] == 1.0));
        assert!(memories["edges"].as_array().unwrap().len() > 5);
        let behaviors = graph(db, &parse(json!({"view":"behaviors"})), NOW)?;
        assert!(behaviors["nodes"].as_array().unwrap().iter().any(|n| n["focus"] == true && n["behavior"] == "inferred"));
        assert!(behaviors["nodes"].as_array().unwrap().iter().any(|n| n["focus"] == false));
        let loops = graph(db, &parse(json!({"view":"loops","responsibility":"owner"})), NOW)?;
        assert!(loops["nodes"].as_array().unwrap().iter().any(|n| n["loop"]["responsibility"] == "owner"));
        assert!(loops["edges"].as_array().unwrap().iter().any(|e| e["loop"]["responsibility"] == "owner"));
        assert!(loops["loops"]["deferred"].as_u64().unwrap() > 0);
        assert!(loops["loops"]["candidates"]["total"].as_u64().unwrap() > 0);
        assert!(compared > 200);
        Ok(())
    }).unwrap();
}
#[test]
fn show_all_above_the_cap_is_refused_from_the_count_without_reading_nodes() {
    let f = F::new();
    f.link();
    // Any attempt to read this current node's fields fails.
    f.store.command(&CommandContext{command_id:"unreadable".into(),actor:"fixture".into(),why:"Unreadable scope".into()},&json!({"fixture":"unreadable"}),|tx,commit|{tx.sql().execute("INSERT INTO node_versions VALUES('unreadable',1,?,'knowledge','Unreadable scope','active',NULL,NULL,NULL,0,'not json')",[commit])?;Ok(json!({}))}).unwrap();
    f.store.read(|tx| {
        let all: GraphQuery = parse(json!({"all":true}));
        assert!(graph(tx.sql(), &all, NOW).is_err());
        let refused = graph_capped(tx.sql(), &all, NOW, 3)?;
        assert_eq!(refused["too_large"], true);
        assert_eq!(refused["total_nodes"], 4);
        assert_eq!(refused["total_edges"], 1);
        assert_eq!(refused["nodes"], json!([]));
        assert_eq!(refused["edges"], json!([]));
        assert_eq!(graph_capped(tx.sql(), &parse(json!({"all":true,"view":"behaviors"})), NOW, 3)?["too_large"], true);
        Ok(())
    }).unwrap();
}
/// Synthetic scale timings; run with MEMENTO_VIEWER_BENCH_NODES and `--ignored --nocapture`.
#[test]
#[ignore]
fn viewer_scale_timings() {
    let n = std::env::var("MEMENTO_VIEWER_BENCH_NODES").ok().and_then(|v| v.parse().ok()).unwrap_or(32_000);
    let f = F::new();
    let started = std::time::Instant::now();
    let anchors = synthetic(&f, n);
    println!("fixture {n} nodes built in {:?}", started.elapsed());
    f.store.read(|tx| {
        let db = tx.sql();
        let time = |label: &str, run: &dyn Fn() -> Result<Value>| -> Result<()> {
            let started = std::time::Instant::now();
            let value = run()?;
            println!("{label}: {:?} ({} bytes)", started.elapsed(), value.to_string().len());
            Ok(())
        };
        let every = rows(db, "SELECT DISTINCT id FROM node_versions", [])?.into_iter().map(|r| r["id"].as_str().unwrap().to_owned()).collect::<Vec<_>>();
        time("behavior_map(all experiences)", &|| Ok(json!(behavior_map(db, &every)?.len())))?;
        let targets = rows(db, &format!("SELECT l.target FROM loop_events l WHERE {OPEN_LOOP}"), [])?.into_iter().map(|r| r["target"].as_str().unwrap().to_owned()).collect::<Vec<_>>();
        time(&format!("responsibilities({} open loops)", targets.len()), &|| Ok(json!(memento_domain::loops::responsibilities(db, &targets)?.len())))?;
        for query in [json!({}), json!({"view":"behaviors"}), json!({"view":"loops"}), json!({"view":"loops","responsibility":"owner"}), json!({"view":"around","id":anchors[1]}), json!({"all":true})] {
            let q: GraphQuery = parse(query.clone());
            time(&format!("legacy {query}"), &|| legacy_graph(db, &q, NOW, MAX_NODES))?;
            time(&format!("new    {query}"), &|| graph(db, &q, NOW))?;
        }
        Ok(())
    }).unwrap();
}
