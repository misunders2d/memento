use memento_core::{CommandContext, Error, F32Blob, Store, Tier, VectorKind, VectorWrite};
use rusqlite::Connection;
use serde_json::json;

const DROP: &str = "DROP INDEX vector_blobs_status_current_v1;DROP INDEX episodes_status_indexable_v1;DROP INDEX episodes_status_raw_v1;";
const COUNT: &str = "SELECT count(*) FROM sqlite_schema WHERE type='index' AND name IN ('vector_blobs_status_current_v1','episodes_status_indexable_v1','episodes_status_raw_v1')";

#[test]
fn additive_status_indexes_preserve_authority_and_respect_writer_and_reopen_fences() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let store = Store::open(&data).unwrap();
    let episode=store.capture(serde_json::from_value(json!({"source":"synthetic","event_id":"event","conversation_id":"thread","kind":"user","text":"Ω😀 exact original source","metadata":{}})).unwrap()).unwrap();
    let mut values = [0.; 768];
    values[0] = -0.;
    values[1] = 1.;
    store.command(&CommandContext{command_id:"seed".into(),actor:"owner".into(),why:"Index upgrade preservation fixture".into()},&json!({"seed":true}),|tx,commit|{
        tx.sql().execute("INSERT INTO node_versions VALUES('node',1,?,'knowledge','Exact assertion','active',NULL,NULL,NULL,0,'[\"personal\"]')",[commit])?;
        tx.sql().execute("INSERT INTO graph_evidence VALUES('node',1,?,0,2,'Ω😀')",[&episode.id])?;
        tx.put_vector(&VectorWrite{seq:0,target:"node".into(),kind:VectorKind::Node,version:1,current:true,tier:Tier::Hot,embedding:F32Blob::from_values(&values)?})?;Ok(json!({}))
    }).unwrap();
    let snapshot = |s: &Store| {
        s.read(|tx| {
            Ok((
                tx.source_cut().clone(),
                tx.fence(Tier::Hot)?,
                tx.sql()
                    .query_row("SELECT embedding FROM vector_blobs WHERE seq=0", [], |r| {
                        r.get::<_, Vec<u8>>(0)
                    })?,
                tx.sql().query_row(
                    "SELECT quote FROM graph_evidence WHERE target='node'",
                    [],
                    |r| r.get::<_, String>(0),
                )?,
                tx.episode(&episode.id)?.unwrap(),
            ))
        })
        .unwrap()
    };
    let before = snapshot(&store);
    store
        .write(|tx| {
            assert_eq!(tx.sql().query_row(COUNT, [], |r| r.get::<_, i64>(0))?, 3);
            tx.sql().execute_batch(DROP)?;
            Ok(())
        })
        .unwrap();
    // A second process may not rebuild even derived indexes before owning the lease.
    assert!(matches!(Store::open(&data), Err(Error::Conflict(_))));
    store
        .read(|tx| {
            assert_eq!(tx.sql().query_row(COUNT, [], |r| r.get::<_, i64>(0))?, 0);
            Ok(())
        })
        .unwrap();
    drop(store);
    let reopened = Store::open(&data).unwrap();
    assert_eq!(snapshot(&reopened), before);
    let version = reopened
        .read(|tx| {
            assert_eq!(tx.sql().query_row(COUNT, [], |r| r.get::<_, i64>(0))?, 3);
            assert_eq!(
                tx.sql()
                    .query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))?,
                5
            );
            Ok(tx
                .sql()
                .query_row("PRAGMA schema_version", [], |r| r.get::<_, i64>(0))?)
        })
        .unwrap();
    drop(reopened);
    let healthy = Store::open(&data).unwrap();
    assert_eq!(snapshot(&healthy), before);
    healthy
        .read(|tx| {
            assert_eq!(
                tx.sql()
                    .query_row("PRAGMA schema_version", [], |r| r.get::<_, i64>(0))?,
                version
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn unknown_header_with_missing_status_indexes_fails_before_upgrade() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    drop(Store::open(&data).unwrap());
    let path = data.join("memory.sqlite");
    let db = Connection::open(&path).unwrap();
    db.execute_batch(DROP).unwrap();
    db.pragma_update(None, "user_version", 99).unwrap();
    drop(db);
    let before = std::fs::read(&path).unwrap();
    assert!(matches!(
        Store::open(&data),
        Err(Error::UnsupportedHeader(99))
    ));
    assert_eq!(std::fs::read(&path).unwrap(), before);
    let db =
        Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    assert_eq!(db.query_row(COUNT, [], |r| r.get::<_, i64>(0)).unwrap(), 0);
}
