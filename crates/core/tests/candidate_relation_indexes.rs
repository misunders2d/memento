use memento_core::{Error, F32Blob, Store, Tier, VectorKind, VectorWrite};
use rusqlite::{Connection, StatementStatus, params};

const INDEXES: &str = include_str!("../src/candidate_relation_indexes.sql");
const DROP: &str = "DROP INDEX search_candidate_relation_src_v1; DROP INDEX search_candidate_relation_dst_v1;";
const COUNT: &str = "SELECT count(*) FROM sqlite_schema WHERE type='index' AND name IN ('search_candidate_relation_src_v1','search_candidate_relation_dst_v1')";
// Frozen caller predicates. Source assertions below catch any eligibility drift.
const ACTIVE: &str = "NOT EXISTS (SELECT 1 FROM forgotten_targets f WHERE f.target=c.id) AND NOT EXISTS (SELECT 1 FROM candidate_decisions d WHERE d.candidate_id=c.id AND d.rowid=(SELECT max(x.rowid) FROM candidate_decisions x WHERE x.candidate_id=c.id) AND d.decision IN ('promote','reject')) AND NOT EXISTS (SELECT 1 FROM extraction_runs r JOIN episodes e ON e.id=r.episode_id JOIN forgotten_sources f ON f.source=e.source AND f.event_id=e.event_id WHERE r.id=c.run_id)";
const ORIGINAL: &str = "SELECT c.id,c.type AS relation,c.src,c.dst FROM candidates c WHERE c.kind='relation' AND (c.src=?1 OR c.dst=?1) AND {ACTIVE_CANDIDATE} AND (?2 IS NULL OR EXISTS(SELECT 1 FROM json_each(c.scope) WHERE value=?2)) ORDER BY c.id LIMIT 32";
const TEMPLATE: &str = "SELECT c.id,c.type AS relation,c.src,c.dst FROM candidates c WHERE c.kind='relation' AND c.id IN (SELECT id FROM candidates WHERE kind='relation' AND src=?1 UNION SELECT id FROM candidates WHERE kind='relation' AND dst=?1) AND {ACTIVE_CANDIDATE} AND (?2 IS NULL OR EXISTS(SELECT 1 FROM json_each(c.scope) WHERE value=?2)) ORDER BY c.id LIMIT 32";
type Row = (String, String, Option<String>, Option<String>);
fn query() -> String {
    assert!(include_str!("../../server/src/anchors.rs").contains(TEMPLATE));
    assert!(include_str!("../../ann/src/eligibility.rs").contains(ACTIVE));
    TEMPLATE.replace("{ACTIVE_CANDIDATE}", ACTIVE)
}
fn measured_sql(db: &Connection, sql: &str, target: &str, scope: Option<&str>) -> (Vec<Row>, i32, i32) {
    let mut statement = db.prepare(sql).unwrap();
    let result = statement.query_map(params![target, scope], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap().collect::<rusqlite::Result<Vec<Row>>>().unwrap();
    (result, statement.get_status(StatementStatus::FullscanStep), statement.get_status(StatementStatus::VmStep))
}
fn measured(db: &Connection, target: &str, scope: Option<&str>) -> (Vec<Row>, i32, i32) {
    measured_sql(db, &query(), target, scope)
}
fn seed(db: &Connection) {
    db.execute("INSERT INTO graph_commits VALUES('fixture','fixture','{}','{}','owner','Relation index fixture','2026-10-27T00:00:00Z')", []).unwrap();
    db.execute("INSERT INTO episodes VALUES(1,'episode','source','event','conversation','user','Exact original source',NULL,'{}','2026-10-27',NULL,NULL,NULL)", []).unwrap();
    db.execute("INSERT INTO episodes VALUES(2,'forgotten-episode','forgotten-source','event','conversation','user','Exact forgotten source',NULL,'{}','2026-10-27',NULL,NULL,NULL)", []).unwrap();
    for (id, episode) in [("run", "episode"), ("forgotten-run", "forgotten-episode")] {
        db.execute("INSERT INTO extraction_runs VALUES(?,'fixture',?,'test','test','[]','{}','2026-10-27')", params![id, episode]).unwrap();
    }
    db.execute("INSERT INTO forgotten_sources VALUES('forgotten-source','event','conversation','fixture')", []).unwrap();
    // These sort before matching rows, so the pre-index ID-order scan cannot stop early.
    for i in 0..6000 {
        db.execute("INSERT INTO candidates VALUES(?,'run','relation','related_to','Unrelated','Fixture',?,'elsewhere',1,NULL,NULL,'[\"personal\"]')",
            params![format!("000-filler-{i:05}"), format!("other-{i}")]).unwrap();
    }
    for i in 0..45 {
        let id = format!("r{i:02}");
        let (src, dst) = if i == 8 { (Some("anchor"), Some("anchor")) }
            else if i % 2 == 0 { (Some("anchor"), Some("other")) } else { (Some("other"), Some("anchor")) };
        let scope = if i == 4 { "[\"professional\"]" } else { "[\"personal\"]" };
        let run = if i == 5 { "missing-run" } else if i == 6 { "forgotten-run" } else { "run" };
        db.execute("INSERT INTO candidates VALUES(?,?,'relation','related_to','Relation','Fixture',?,?,1,NULL,NULL,?)", params![id, run, src, dst, scope]).unwrap();
    }
    // Null endpoints and non-relation kinds must not become false matches.
    db.execute("INSERT INTO candidates VALUES('nulls','run','relation','related_to','Nulls','Fixture',NULL,NULL,1,NULL,NULL,'[\"personal\"]')", []).unwrap();
    db.execute("INSERT INTO candidates VALUES('entity','run','entity','person','Anchor','Fixture','anchor','anchor',1,NULL,NULL,'[\"personal\"]')", []).unwrap();
    db.execute("INSERT INTO forgotten_targets VALUES('r03','fixture','candidate')", []).unwrap();
    for (id, target, decision) in [("d0", "r00", "reject"), ("d1", "r01", "promote"), ("d2", "r02", "reject"), ("d3", "r02", "review")] {
        db.execute("INSERT INTO candidate_decisions VALUES(?,?,'fixture',?,'durable',NULL)", params![id, target, decision]).unwrap();
    }
}
#[test]
fn pending_relation_indexes_keep_exact_results_and_target_both_endpoints() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("data")).unwrap();
    store.write(|tx| {
        let db=tx.sql(); db.execute_batch(DROP)?; seed(db);
        let scopes=[None,Some("personal"),Some("professional")];
        let original=ORIGINAL.replace("{ACTIVE_CANDIDATE}",ACTIVE);
        let before=scopes.map(|scope| measured_sql(db,&original,"anchor",scope).0);
        let absent_before=measured_sql(db,&original,"absent",None);
        assert!(absent_before.1>=6000);
        db.execute_batch(INDEXES)?;
        for (scope, expected) in scopes.into_iter().zip(before) {
            let actual=measured(db,"anchor",scope).0; assert_eq!(actual,expected);
            if scope!=Some("professional") {
                assert_eq!(actual.len(),32);
                assert!(actual.iter().any(|r|r.0=="r02"));
                assert!(actual.iter().any(|r|r.0=="r05"));
                assert_eq!(actual.iter().filter(|r|r.0=="r08").count(),1);
                assert!(actual.iter().all(|r| !["r00","r01","r03","r06"].contains(&r.0.as_str())));
            }
        }
        assert_eq!(measured(db,"anchor",Some("professional")).0.len(),1);
        let mut statement=db.prepare(&format!("EXPLAIN QUERY PLAN {}",query()))?;
        let plan=statement.query_map(params!["absent",Option::<&str>::None],|r|r.get::<_,String>(3))?.collect::<rusqlite::Result<Vec<_>>>()?;
        assert!(plan.iter().any(|s|s.contains("SEARCH") && s.contains("search_candidate_relation_src_v1")),"{plan:?}");
        assert!(plan.iter().any(|s|s.contains("SEARCH") && s.contains("search_candidate_relation_dst_v1")),"{plan:?}");
        let absent_after=measured(db,"absent",None);
        assert_eq!(absent_after.0,absent_before.0); assert_eq!(absent_after.1,0);
        assert!(absent_after.2*20<absent_before.2,"before={} after={}",absent_before.2,absent_after.2);
        Ok(())
    }).unwrap();
}
#[test]
fn relation_index_upgrade_preserves_authority_fences_and_is_idempotent() {
    let dir=tempfile::tempdir().unwrap();let data=dir.path().join("data");let store=Store::open(&data).unwrap();
    store.write(|tx| {seed(tx.sql());let mut v=[0.;768];v[0]=1.;v[1]=-0.;tx.put_vector(&VectorWrite{seq:0,target:"entity".into(),kind:VectorKind::Candidate,version:1,current:true,tier:Tier::Cold,embedding:F32Blob::from_values(&v)?})?;Ok(())}).unwrap();
    let snapshot=|s:&Store|s.read(|tx|Ok((tx.source_cut().clone(),tx.fence(Tier::Cold)?,tx.sql().query_row("SELECT embedding FROM vector_blobs WHERE seq=0",[],|r|r.get::<_,Vec<u8>>(0))?,tx.sql().query_row("SELECT text FROM episodes WHERE id='episode'",[],|r|r.get::<_,String>(0))?,tx.sql().query_row("SELECT count(*) FROM candidates",[],|r|r.get::<_,i64>(0))?))).unwrap();
    let before=snapshot(&store);store.write(|tx|{tx.sql().execute_batch(DROP)?;Ok(())}).unwrap();
    assert!(matches!(Store::open(&data),Err(Error::Conflict(_))));
    store.read(|tx|{assert_eq!(tx.sql().query_row(COUNT,[],|r|r.get::<_,i64>(0))?,0);Ok(())}).unwrap();drop(store);
    let reopened=Store::open(&data).unwrap();assert_eq!(snapshot(&reopened),before);
    let version=reopened.read(|tx|{assert_eq!(tx.sql().query_row(COUNT,[],|r|r.get::<_,i64>(0))?,2);assert_eq!(tx.sql().query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0))?,5);Ok(tx.sql().query_row("PRAGMA schema_version",[],|r|r.get::<_,i64>(0))?)}).unwrap();drop(reopened);
    let healthy=Store::open(&data).unwrap();assert_eq!(snapshot(&healthy),before);healthy.read(|tx|{assert_eq!(tx.sql().query_row("PRAGMA schema_version",[],|r|r.get::<_,i64>(0))?,version);Ok(())}).unwrap();
}
#[test]
fn unknown_header_cannot_install_relation_indexes() {
    let dir=tempfile::tempdir().unwrap();let data=dir.path().join("data");drop(Store::open(&data).unwrap());let path=data.join("memory.sqlite");
    let db=Connection::open(&path).unwrap();db.execute_batch(DROP).unwrap();db.pragma_update(None,"user_version",99).unwrap();drop(db);let before=std::fs::read(&path).unwrap();
    assert!(matches!(Store::open(&data),Err(Error::UnsupportedHeader(99))));assert_eq!(std::fs::read(&path).unwrap(),before);
    let db=Connection::open_with_flags(path,rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();assert_eq!(db.query_row(COUNT,[],|r|r.get::<_,i64>(0)).unwrap(),0);
}
