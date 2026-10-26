// Frozen pre-optimization query is the semantic parity oracle.
const ORIGINAL: &str = r#"WITH RECURSIVE current_nodes AS (SELECT n.* FROM node_versions n WHERE n.state='active' AND n.version=(SELECT max(v.version) FROM node_versions v WHERE v.id=n.id) AND NOT EXISTS(SELECT 1 FROM forgotten_targets f WHERE f.target=n.id) AND {visible} AND (?1 IS NULL OR EXISTS(SELECT 1 FROM json_each(n.scope) WHERE value=?1))),current_edges AS (SELECT e.* FROM edge_versions e WHERE e.state='active' AND e.version=(SELECT max(v.version) FROM edge_versions v WHERE v.id=e.id)),adjacency AS (SELECT src AS origin,dst AS target,rel,'out' AS direction,id FROM current_edges UNION ALL SELECT dst,src,rel,'in',id FROM current_edges),bounded AS (SELECT a.*,row_number() OVER (PARTITION BY origin ORDER BY a.id) AS fanout FROM adjacency a JOIN current_nodes n ON n.id=a.target),walk(id,depth,via,relation,direction,edge_id) AS (VALUES(?2,0,NULL,NULL,NULL,NULL) UNION ALL SELECT a.target,w.depth+1,w.id,a.rel,a.direction,a.id FROM walk w JOIN bounded a ON a.origin=w.id WHERE w.depth<?3 AND a.fanout<=8),reached AS (SELECT n.id,n.type,substr(n.text,1,160) AS one_line,n.valid_from,n.valid_to,n.scope,coalesce((SELECT importance FROM node_stats WHERE node_id=n.id),0.5) AS importance,w.depth AS hops,w.via,w.relation,w.direction,w.edge_id,row_number() OVER(PARTITION BY n.id ORDER BY w.depth,w.edge_id,w.via) AS choice FROM walk w JOIN current_nodes n ON n.id=w.id WHERE w.depth>0 AND n.id<>?2) SELECT id,type,one_line,valid_from,valid_to,scope,importance,hops,via,relation,direction,edge_id FROM reached WHERE choice=1 ORDER BY hops,importance DESC,id LIMIT 16"#;
use super::*;
use memento_core::Store;
use std::sync::atomic::{AtomicU64, Ordering};

fn fixture() -> (tempfile::TempDir, std::sync::Arc<Store>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("data")).unwrap();
    store.write(|tx|{tx.sql().execute("INSERT INTO graph_commits VALUES('manual','manual','{}','{}','owner','Explicit manual relation fixture','2026-10-26T00:00:00Z')",[])?;Ok(())}).unwrap();
    (dir, store)
}
fn node(db: &Connection, id: &str, state: &str, scope: &str, importance: f64) {
    db.execute("INSERT INTO node_versions VALUES(?1,1,'manual','person',?1,?2,'2020-01-01T00:00:00Z',NULL,NULL,0,?3)",params![id,state,scope]).unwrap();
    db.execute(
        "INSERT INTO node_stats(node_id,importance) VALUES(?,?)",
        params![id, importance],
    )
    .unwrap();
}
fn edge(db: &Connection, id: &str, src: &str, dst: &str) {
    db.execute(
        "INSERT INTO edge_versions VALUES(?,1,'manual',?,'related_to',?,'active')",
        params![id, src, dst],
    )
    .unwrap();
}
fn original(db: &Connection, id: &str, hops: usize, scope: Option<&str>) -> Vec<Value> {
    let sql = ORIGINAL.replace("{visible}", &visible_sql("n.id"));
    let mut result = rows(db, &sql, params![scope, id, hops]).unwrap();
    for row in &mut result {
        super::scope(row).unwrap();
        row["loop"] = json!(loops::current(db, row["id"].as_str().unwrap()).unwrap());
    }
    result
}
fn parity(db: &Connection, id: &str, hops: usize, scope: Option<&str>) -> Vec<Value> {
    let actual = related(db, id, hops, scope).unwrap();
    assert_eq!(
        actual,
        original(db, id, hops, scope),
        "origin={id} hops={hops} scope={scope:?}"
    );
    actual
}

#[test]
fn targeted_related_matches_current_history_direction_scope_forgetting_loops_and_cycles() {
    let (_dir, store) = fixture();
    let source=store.capture(serde_json::from_value(json!({"source":"synthetic:media","event_id":"event","conversation_id":"media","kind":"user","text":"Exact synthetic source","metadata":{}})).unwrap()).unwrap();
    store.write(|tx|{let db=tx.sql();
        for(id,state,scope,importance)in [
            ("root","active","[\"personal\",\"professional\"]",0.5),
            ("a","active","[\"personal\"]",0.7),("b","active","[\"professional\"]",0.9),
            ("c","active","[\"personal\",\"professional\"]",0.3),
            ("d","retired","[\"personal\"]",1.),("e","active","[\"personal\"]",1.),
            ("f","active","[\"personal\"]",0.8),("g","retired","[\"personal\"]",0.4),
            ("h","active","[\"personal\"]",1.),("i","active","[\"professional\"]",0.9),
            ("j","active","[\"personal\"]",0.)]{node(db,id,state,scope,importance);}
        db.execute("INSERT INTO node_versions VALUES('media',1,'manual','media','Captured media','active',NULL,NULL,NULL,0,'[\"personal\"]')",[])?;
        db.execute("INSERT INTO media_files VALUES('media','sha','synthetic.png','image','manual')",[])?;
        db.execute("INSERT INTO node_versions SELECT id,2,commit_id,type,'Ω😀 latest',CASE id WHEN 'f' THEN 'superseded' ELSE 'active' END,valid_from,valid_to,superseded_by,pinned,scope FROM node_versions WHERE id IN ('f','g')",[])?;
        db.execute("INSERT INTO forgotten_targets VALUES('e','manual','node')",[])?;
        db.execute("INSERT INTO captured_media VALUES('sha','image','synthetic.png','indexed',NULL,'media',NULL)",[])?;
        db.execute("INSERT INTO captured_media_refs VALUES('sha',?,0,5,'Exact')",[&source.id])?;
        db.execute("INSERT INTO forgotten_sources VALUES('synthetic:media','event','media','manual')",[])?;
        for(id,a,b)in [
            ("00-retired","root","d"),("01-forgotten","root","e"),("02-superseded","root","f"),("03-hidden","root","media"),
            ("10-out","root","a"),("11-in","c","root"),("12-prof","root","b"),("13-current","root","g"),
            ("14-duplicate","root","a"),("15-old-edge","root","h"),("16-rewired","root","h"),("17-self","root","root"),
            ("18-cycle","a","c"),("19-back","a","root"),("19-direct","root","j"),
            ("20-path","a","h"),("21-path","c","h"),("22-prof","a","i"),("23-cycle","h","a"),("24-self","a","a")]{edge(db,id,a,b);}
        db.execute("INSERT INTO edge_versions SELECT id,2,commit_id,src,rel,dst,'forgotten' FROM edge_versions WHERE id='15-old-edge'",[])?;
        db.execute("INSERT INTO edge_versions SELECT id,2,commit_id,'i',rel,'j','active' FROM edge_versions WHERE id='16-rewired'",[])?;
        db.execute("INSERT INTO loop_events VALUES('loop-a','a','manual','open','2027-01-01T00:00:00Z','2026-12-01T00:00:00Z')",[])?;
        Ok(())
    }).unwrap();
    store.read(|tx|{
        for origin in ["root","a","g","h","absent"]{for hops in [1,2]{for scope in [None,Some("personal"),Some("professional")]{parity(tx.sql(),origin,hops,scope);}}}
        let personal=parity(tx.sql(),"root",1,Some("personal"));
        assert!(personal.iter().all(|r|!matches!(r["id"].as_str(),Some("b"|"d"|"e"|"f"|"media"))));
        let a=personal.iter().find(|r|r["id"]=="a").unwrap();assert_eq!(a["loop"]["state"],"open");assert_eq!(a["edge_id"],"10-out");assert_eq!(a["direction"],"out");
        let c=personal.iter().find(|r|r["id"]=="c").unwrap();assert_eq!(c["direction"],"in");
        assert!(matches!(related(tx.sql(),"root",0,None),Err(Error::Validation(_))));
        assert!(matches!(related(tx.sql(),"root",3,None),Err(Error::Validation(_))));
        // The entire traversal retains the caller's old cut after a current-version change.
        store.write(|write|{write.sql().execute("INSERT INTO node_versions SELECT id,3,commit_id,type,text,'retired',valid_from,valid_to,superseded_by,pinned,scope FROM node_versions WHERE id='g' AND version=2",[])?;Ok(())})?;
        assert_eq!(related(tx.sql(),"root",1,Some("personal"))?,personal);
        Ok(())
    }).unwrap();
    store
        .read(|tx| {
            let current = parity(tx.sql(), "root", 1, Some("personal"));
            assert!(current.iter().all(|r| r["id"] != "g"));
            Ok(())
        })
        .unwrap();
    // A new genuine reference makes captured media visible without changing any edge.
    let genuine=store.capture(serde_json::from_value(json!({"source":"synthetic:genuine","event_id":"event","conversation_id":"media","kind":"user","text":"Exact second source","metadata":{}})).unwrap()).unwrap();
    store
        .write(|tx| {
            tx.sql().execute(
                "INSERT INTO captured_media_refs VALUES('sha',?,0,5,'Exact')",
                [&genuine.id],
            )?;
            Ok(())
        })
        .unwrap();
    store
        .read(|tx| {
            assert!(
                parity(tx.sql(), "root", 1, Some("personal"))
                    .iter()
                    .any(|r| r["id"] == "media")
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn targeted_related_preserves_eight_edge_fanout_shortest_choice_and_sixteen_result_order() {
    let (_dir, store) = fixture();
    store
        .write(|tx| {
            let db = tx.sql();
            node(db, "root", "active", "[\"personal\"]", 0.5);
            for i in 0..10 {
                let id = format!("a{i}");
                node(db, &id, "active", "[\"personal\"]", 0.5);
                edge(db, &format!("10_root{i}"), "root", &id);
                for j in 0..8 {
                    let k = i * 8 + j;
                    let leaf = format!("leaf{k:02}");
                    node(db, &leaf, "active", "[\"personal\"]", k as f64 / 80.);
                    edge(db, &format!("00_{i}_{j}"), &id, &leaf);
                }
            }
            Ok(())
        })
        .unwrap();
    store
        .read(|tx| {
            let one = parity(tx.sql(), "root", 1, None);
            assert_eq!(one.len(), 8);
            assert_eq!(
                one.iter()
                    .map(|r| r["id"].as_str().unwrap())
                    .collect::<Vec<_>>(),
                vec!["a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7"]
            );
            let two = parity(tx.sql(), "root", 2, None);
            assert_eq!(two.len(), 16);
            assert_eq!(two[8]["id"], "leaf63");
            assert_eq!(two[15]["id"], "leaf56");
            Ok(())
        })
        .unwrap();
    store
        .write(|tx| {
            edge(tx.sql(), "00_self", "root", "root");
            Ok(())
        })
        .unwrap();
    store
        .read(|tx| {
            assert_eq!(parity(tx.sql(), "root", 1, None).len(), 6);
            parity(tx.sql(), "root", 2, None);
            Ok(())
        })
        .unwrap();
}

#[derive(Default)]
struct Work {
    fullscan: AtomicU64,
    vm: AtomicU64,
}
unsafe extern "C" fn profile(
    event: u32,
    context: *mut std::ffi::c_void,
    statement: *mut std::ffi::c_void,
    _: *mut std::ffi::c_void,
) -> i32 {
    if event == rusqlite::ffi::SQLITE_TRACE_PROFILE {
        let w = unsafe { &*context.cast::<Work>() };
        let s = statement.cast();
        w.fullscan.fetch_add(
            unsafe {
                rusqlite::ffi::sqlite3_stmt_status(
                    s,
                    rusqlite::ffi::SQLITE_STMTSTATUS_FULLSCAN_STEP,
                    1,
                )
            }
            .max(0) as u64,
            Ordering::Relaxed,
        );
        w.vm.fetch_add(
            unsafe {
                rusqlite::ffi::sqlite3_stmt_status(s, rusqlite::ffi::SQLITE_STMTSTATUS_VM_STEP, 1)
            }
            .max(0) as u64,
            Ordering::Relaxed,
        );
    }
    0
}
fn measured(db: &Connection) -> (Vec<Value>, u64, u64) {
    let mut work = Work::default();
    unsafe {
        rusqlite::ffi::sqlite3_trace_v2(
            db.handle(),
            rusqlite::ffi::SQLITE_TRACE_PROFILE,
            Some(profile),
            (&mut work as *mut Work).cast(),
        )
    };
    let result = related(db, "root", 2, None).unwrap();
    unsafe { rusqlite::ffi::sqlite3_trace_v2(db.handle(), 0, None, std::ptr::null_mut()) };
    (
        result,
        work.fullscan.load(Ordering::Relaxed),
        work.vm.load(Ordering::Relaxed),
    )
}
#[test]
fn disconnected_graph_growth_does_not_add_traversal_scans_or_vm_work() {
    let (_dir, store) = fixture();
    store
        .write(|tx| {
            node(tx.sql(), "root", "active", "[\"personal\"]", 0.5);
            node(tx.sql(), "a", "active", "[\"personal\"]", 0.5);
            node(tx.sql(), "b", "active", "[\"personal\"]", 0.5);
            edge(tx.sql(), "one", "root", "a");
            edge(tx.sql(), "two", "b", "a");
            Ok(())
        })
        .unwrap();
    let before = store.read(|tx| Ok(measured(tx.sql()))).unwrap();
    store
        .write(|tx| {
            for i in 0..500 {
                let a = format!("disconnected-{i:03}");
                node(tx.sql(), &a, "active", "[\"personal\"]", 0.5);
                if i > 0 {
                    edge(
                        tx.sql(),
                        &format!("unrelated-{i:03}"),
                        &a,
                        &format!("disconnected-{:03}", i - 1),
                    );
                }
            }
            Ok(())
        })
        .unwrap();
    store
        .read(|tx| {
            let after = measured(tx.sql());
            assert_eq!(after.0, before.0);
            assert_eq!(before.1, 0);
            assert_eq!(after.1, 0);
            // Exact opcode counts can vary with tree layout. Five hundred
            // unrelated nodes must not add even one VM step per added node.
            assert!(after.2 < before.2 + 500);
            assert_eq!(after.0, original(tx.sql(), "root", 2, None));
            Ok(())
        })
        .unwrap();
}
