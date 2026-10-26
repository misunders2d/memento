use crate::{
    indexing,
    operational_status::{self, RuntimeStatus},
};
use memento_core::{CommandContext, F32Blob, Store, Tier, VectorKind, VectorWrite};
use rusqlite::{Connection, params};
use serde_json::json;

fn counts(db: &Connection, source: Option<&str>) -> Vec<i64> {
    let actual = indexing::status(db, source, false, true, false).unwrap();
    [
        "total",
        "indexed",
        "pending",
        "failed",
        "unprepared_episodes",
    ]
    .map(|key| actual[key].as_i64().unwrap())
    .to_vec()
}
fn command(id: &str) -> CommandContext {
    CommandContext {
        command_id: id.into(),
        actor: "owner".into(),
        why: "Exact synthetic status fixture".into(),
    }
}
#[test]
fn exact_unit_counts_preserve_sources_missing_vectors_failures_versions_and_snapshot_fences() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("data")).unwrap();
    let text = "Synthetic exact source quote";
    let mut episodes = Vec::new();
    for source in ["synthetic:a", "synthetic:b", "synthetic:forgotten"] {
        episodes.push(store.capture(serde_json::from_value(json!({"source":source,"event_id":"source","conversation_id":"source","kind":"user","text":text,"metadata":{}})).unwrap()).unwrap().id);
    }
    let mut vector = [0.; 768];
    vector[0] = 1.;
    store.command(&command("seed"),&json!({"seed":true}),|tx,commit|{
        for n in 0..8 {
            let id=format!("n{n}");let state=if n==4{"retired"}else{"active"};
            tx.sql().execute("INSERT INTO node_versions(id,version,commit_id,type,text,state,pinned,scope) VALUES(?,1,?,'knowledge','Exact fixture',?,0,'[\"personal\"]')",params![id,commit,state])?;
            if n!=7 { tx.sql().execute("INSERT INTO graph_evidence VALUES(?,1,?,0,?,?)",params![id,episodes[usize::from(n==6)],text.len() as i64,text])?; }
            if n==0 {continue;}
            if n==1 { tx.sql().execute("INSERT INTO vector_items VALUES(0,?,'node',1)",[&id])?;continue; }
            tx.put_vector(&VectorWrite {seq:if n==7{7}else{n-1},target:id,kind:VectorKind::Node,version:1,current:n!=2,tier:Tier::Hot,embedding:F32Blob::from_values(&vector)?})?;
        }
        tx.sql().execute("INSERT INTO forgotten_targets VALUES('n5',?,'2026-10-26T00:00:00Z')",[commit])?;
        for target in ["n0","n3"] {tx.sql().execute("INSERT INTO index_failures VALUES(?,1,1,'Fixture',0,0)",[target])?;}
        tx.sql().execute("INSERT INTO episode_chunks VALUES('chunk-a',?,0,?)",params![episodes[0],text.len() as i64])?;
        tx.sql().execute("INSERT INTO episode_chunks VALUES('chunk-forgotten',?,0,?)",params![episodes[2],text.len() as i64])?;
        tx.sql().execute("INSERT INTO forgotten_sources VALUES('synthetic:forgotten','source','forgotten',?)",[commit])?;
        tx.sql().execute("INSERT INTO graph_evidence VALUES('n3',1,?,1,?,?)",params![episodes[0],text.len() as i64,&text[1..]])?;
        tx.put_vector(&VectorWrite{seq:6,target:"chunk-a".into(),kind:VectorKind::Episode,version:1,current:true,tier:Tier::Hot,embedding:F32Blob::from_values(&vector)?})?;
        Ok(json!({}))
    }).unwrap();
    store.read(|tx|{
        assert_eq!(counts(tx.sql(),None),vec![7,4,2,1,1]);
        assert_eq!(counts(tx.sql(),Some("synthetic:a")),vec![5,2,2,1,0]);
        assert_eq!(counts(tx.sql(),Some("synthetic:b")),vec![1,1,0,0,1]);
        assert_eq!(counts(tx.sql(),Some("synthetic:absent")),vec![0,0,0,0,0]);
        // A metadata-only new current version must not inherit an old vector.
        store.command(&command("new-version"),&json!({"new":true}),|write,commit|{
            write.touch_target("n3")?;
            write.sql().execute("INSERT INTO node_versions(id,version,commit_id,type,text,state,pinned,scope) VALUES('n3',2,?,'knowledge','Latest fixture','active',0,'[\"personal\"]')",[commit])?;
            write.sql().execute("INSERT INTO graph_evidence VALUES('n3',2,?,0,?,?)",params![episodes[0],text.len() as i64,text])?;
            write.sql().execute("INSERT INTO vector_items VALUES(8,'n3','node',2)",[])?;
            Ok(json!({}))
        })?;
        assert_eq!(counts(tx.sql(),None),vec![7,4,2,1,1]);
        Ok(())
    }).unwrap();
    store
        .read(|tx| {
            assert_eq!(counts(tx.sql(), None), vec![7, 3, 3, 1, 1]);
            Ok(())
        })
        .unwrap();
    store
        .write(|tx| {
            tx.sql().execute(
                "INSERT INTO episode_chunks VALUES('chunk-a2',?,1,?)",
                params![episodes[0], text.len() as i64],
            )?;
            for (id, start) in [("chunk-b1", 0), ("chunk-b2", 1)] {
                tx.sql().execute(
                    "INSERT INTO episode_chunks VALUES(?,?,?,?)",
                    params![id, episodes[1], start, text.len() as i64],
                )?;
            }
            Ok(())
        })
        .unwrap();
    store.read(|tx|{
        assert_eq!(counts(tx.sql(),None),vec![10,3,6,1,0]);
        assert_eq!(counts(tx.sql(),Some("synthetic:a")),vec![6,1,4,1,0]);
        assert_eq!(counts(tx.sql(),Some("synthetic:b")),vec![3,1,2,0,0]);
        store.command(&command("new-candidate"),&json!({"candidate":true}),|write,commit|{
            write.sql().execute("INSERT INTO extraction_runs VALUES('new-run',?,?,'test','test','[]','{}','2026-10-26T00:00:00Z')",params![commit,episodes[0]])?;
            write.sql().execute("INSERT INTO candidates(id,run_id,kind,type,text,why,confidence,scope) VALUES('new','new-run','memory','knowledge','New candidate','Exact fixture',1,'[\"personal\"]')",[])?;
            write.sql().execute("INSERT INTO graph_evidence VALUES('new',1,?,0,?,?)",params![episodes[0],text.len() as i64,text])?;
            Ok(json!({}))
        })?;
        assert_eq!(counts(tx.sql(),None),vec![10,3,6,1,0]);
        Ok(())
    }).unwrap();
    store
        .read(|tx| {
            assert_eq!(counts(tx.sql(), None), vec![11, 3, 7, 1, 0]);
            Ok(())
        })
        .unwrap();
    store.command(&command("old-provenance-and-segment"),&json!({"provenance":true}),|tx,commit|{
        // Source membership is not narrowed to the current assertion version.
        tx.sql().execute("INSERT INTO graph_evidence VALUES('n3',1,?,0,?,?)",params![episodes[1],text.len() as i64,text])?;
        tx.sql().execute("INSERT INTO node_versions(id,version,commit_id,type,text,state,pinned,scope) VALUES('media',1,?,'media','Media fixture','active',0,'[\"personal\"]')",[commit])?;
        tx.sql().execute("INSERT INTO graph_evidence VALUES('media',1,?,0,?,?)",params![episodes[1],text.len() as i64,text])?;
        tx.sql().execute("INSERT INTO segments(id,node_id,modality,text,artifact_sha256) VALUES('segment','media','image','Segment fixture','synthetic')",[])?;
        Ok(json!({}))
    }).unwrap();
    store
        .read(|tx| {
            assert_eq!(counts(tx.sql(), None), vec![12, 3, 8, 1, 0]);
            assert_eq!(counts(tx.sql(), Some("synthetic:a")), vec![7, 1, 5, 1, 0]);
            assert_eq!(counts(tx.sql(), Some("synthetic:b")), vec![5, 1, 4, 0, 0]);
            Ok(())
        })
        .unwrap();
    let legacy=store.capture(serde_json::from_value(json!({"source":"native:fixture:codex","event_id":"legacy","conversation_id":"legacy","kind":"developer","text":"Retained synthetic developer instruction","metadata":{}})).unwrap()).unwrap();
    store.command(&command("candidate-evidence-shapes"),&json!({"candidates":true}),|tx,commit|{
        for id in ["legacy-only","mixed","no-evidence"] {
            let origin=if id=="legacy-only"{&legacy.id}else{&episodes[0]};
            tx.sql().execute("INSERT INTO extraction_runs VALUES(?,?,?,'test','test','[]','{}','2026-10-26T00:00:00Z')",params![id,commit,origin])?;
            tx.sql().execute("INSERT INTO candidates(id,run_id,kind,type,text,why,confidence,scope) VALUES(?,?,'memory','knowledge','Candidate fixture','Exact fixture',1,'[\"personal\"]')",params![id,id])?;
            if id!="no-evidence" {tx.sql().execute("INSERT INTO graph_evidence VALUES(?,1,?,0,8,'Retained')",params![id,legacy.id])?;}
            if id=="mixed" {tx.sql().execute("INSERT INTO graph_evidence VALUES(?,1,?,0,?,?)",params![id,episodes[0],text.len() as i64,text])?;}
        }
        Ok(json!({}))
    }).unwrap();
    store
        .read(|tx| {
            // Index-unit counts preserve all active candidates independently of the
            // consolidation classifier; no-evidence candidates have no source membership.
            assert_eq!(counts(tx.sql(), None), vec![15, 3, 11, 1, 1]);
            assert_eq!(counts(tx.sql(), Some("synthetic:a")), vec![8, 1, 6, 1, 0]);
            assert_eq!(
                counts(tx.sql(), Some("native:fixture:codex")),
                vec![2, 0, 2, 0, 1]
            );
            Ok(())
        })
        .unwrap();
    store
        .command(
            &command("reject-new"),
            &json!({"reject":true}),
            |tx, commit| {
                tx.sql().execute(
                    "INSERT INTO candidate_decisions VALUES('reject','new',?,'reject',NULL,NULL)",
                    [commit],
                )?;
                Ok(json!({}))
            },
        )
        .unwrap();
    store
        .read(|tx| {
            assert_eq!(counts(tx.sql(), None), vec![14, 3, 10, 1, 1]);
            Ok(())
        })
        .unwrap();
}

#[test]
fn narrow_raw_counts_follow_real_offload_compression_and_source_filters() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("data")).unwrap();
    let raw = |source| {
        store
            .read(|tx| {
                Ok(operational_status::operational_status(
                    tx.sql(),
                    source,
                    &RuntimeStatus::disabled(),
                    0.,
                )?["raw_storage"]
                    .clone())
            })
            .unwrap()
    };
    assert_eq!(raw(None)["inline_raw_episodes"], 0);
    for (event, source, kind, text, mode) in [
        (
            "inline",
            "synthetic:a",
            "native_record",
            "Рядок αβ".to_owned(),
            "skip",
        ),
        (
            "offload",
            "synthetic:a",
            "native_live_record",
            "A".repeat(65536),
            "skip",
        ),
        (
            "hook",
            "synthetic:b",
            "native_hook",
            "Original second source".to_owned(),
            "skip",
        ),
        (
            "message",
            "synthetic:b",
            "user",
            "Excluded normal evidence".to_owned(),
            "message",
        ),
        (
            "raw-message",
            "synthetic:b",
            "native_record",
            "Excluded raw message mode".to_owned(),
            "message",
        ),
    ] {
        store.capture(serde_json::from_value(json!({"source":source,"event_id":event,"conversation_id":"raw","kind":kind,"text":text,"metadata":{"extraction_mode":mode}})).unwrap()).unwrap();
    }
    let original_bytes = "Рядок αβ".len() + "Original second source".len();
    assert_eq!(raw(None)["inline_raw_episodes"], 2);
    assert_eq!(raw(None)["inline_raw_bytes"], original_bytes);
    assert_eq!(raw(None)["offloaded_raw_episodes"], 1);
    assert_eq!(raw(Some("synthetic:a"))["inline_raw_episodes"], 1);
    assert_eq!(raw(Some("synthetic:a"))["offloaded_raw_episodes"], 1);
    assert_eq!(raw(Some("synthetic:b"))["inline_raw_episodes"], 1);
    assert_eq!(raw(Some("absent"))["inline_raw_episodes"], 0);
    for _ in 0..8 {
        if !store.compress_raw_step().unwrap() {
            break;
        }
    }
    let after = raw(None);
    assert_eq!(after["inline_raw_episodes"], 0);
    assert_eq!(after["inline_raw_bytes"], 0);
    assert_eq!(after["offloaded_raw_episodes"], 1);
    assert_eq!(after["compressed_raw_episodes"], 2);
    assert_eq!(after["compressed_original_bytes"], original_bytes);
    assert!(after["compressed_raw_bytes"].as_i64().unwrap() > 0);
    assert_eq!(raw(Some("synthetic:a"))["compressed_raw_episodes"], 1);
    assert_eq!(raw(Some("synthetic:b"))["compressed_raw_episodes"], 1);
}
