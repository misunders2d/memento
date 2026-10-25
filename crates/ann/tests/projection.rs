use memento_ann::{Pool, Projection};
use memento_core::{F32Blob, ModelSignature, Store, Tier, VectorKind, VectorWrite};
use rusqlite::params;
use std::{
    os::unix::fs::PermissionsExt,
    sync::{Arc, atomic::AtomicBool},
};
fn fixture() -> (tempfile::TempDir, Arc<Store>, Arc<Projection>) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let store = Store::open(dir.path()).unwrap();
    let index = Projection::open(store.clone(), ModelSignature::pinned()).unwrap();
    (dir, store, index)
}
fn vector(cos: f32) -> F32Blob {
    let mut values = [0.0; 768];
    values[0] = cos;
    values[1] = (1.0 - cos * cos).sqrt();
    F32Blob::from_values(&values).unwrap()
}
fn query() -> [f32; 768] {
    let mut v = [0.0; 768];
    v[0] = 1.;
    v
}
fn node(
    store: &Store,
    seq: i64,
    id: &str,
    cos: f32,
    scope: &str,
    kind: &str,
    current: bool,
    tier: Tier,
) {
    store.write(|tx|{tx.sql().execute("INSERT INTO graph_commits SELECT 'test','test','{}','{}','test','test','2026-01-01T00:00:00+00:00' WHERE NOT EXISTS(SELECT 1 FROM graph_commits WHERE id='test')",[])?;tx.sql().execute("INSERT INTO node_versions VALUES(?,1,'test',?,'A meaningful stored assertion','active',NULL,NULL,NULL,0,?)",params![id,kind,format!("[\"{scope}\"]")])?;tx.put_vector(&VectorWrite{seq,target:id.into(),kind:VectorKind::Node,version:1,current,tier,embedding:vector(cos)})?;Ok(())}).unwrap()
}
fn hits(index: &Projection, tier: Tier) -> Vec<String> {
    index
        .snapshot(tier == Tier::Hot, tier == Tier::Cold, |_, view| {
            Ok(view
                .search(
                    tier,
                    &query(),
                    1,
                    "node",
                    Pool::Ordinary,
                    Some("personal"),
                    Some("person"),
                )?
                .into_iter()
                .map(|v| v.target)
                .collect())
        })
        .unwrap()
}
#[test]
fn filters_apply_before_top_k_and_unoptimized_mutations_are_immediately_visible() {
    let (_dir, store, index) = fixture();
    node(
        &store,
        0,
        "wanted",
        0.7,
        "personal",
        "person",
        true,
        Tier::Hot,
    );
    for i in 1..=64 {
        let (scope, kind, current) = match i % 4 {
            0 => ("professional", "person", true),
            1 => ("personal", "tool", true),
            2 => ("personal", "person", false),
            _ => ("personal", "person", true),
        };
        let id = format!("forbidden{i}");
        node(&store, i, &id, 0.99, scope, kind, current, Tier::Hot);
        if i % 4 == 3 {
            store.write(|tx|{tx.touch_target(&id)?;tx.sql().execute("INSERT INTO node_versions SELECT id,version+1,commit_id,type,text,'retired',valid_from,valid_to,superseded_by,pinned,scope FROM node_versions WHERE id=?",[&id])?;Ok(())}).unwrap();
        }
    }
    index.rebuild(Tier::Hot, &AtomicBool::new(false)).unwrap();
    assert_eq!(hits(&index, Tier::Hot), ["wanted"]);
    node(
        &store,
        -1,
        "fresh",
        0.9,
        "personal",
        "person",
        true,
        Tier::Hot,
    );
    assert_eq!(hits(&index, Tier::Hot), ["fresh"]);
    store
        .write(|tx| {
            tx.put_vector(&VectorWrite {
                seq: -1,
                target: "fresh".into(),
                kind: VectorKind::Node,
                version: 1,
                current: true,
                tier: Tier::Hot,
                embedding: vector(0.5),
            })?;
            Ok(())
        })
        .unwrap();
    assert_eq!(hits(&index, Tier::Hot), ["wanted"]);
    store
        .write(|tx| {
            tx.touch_vector(0, Tier::Hot)?;
            tx.sql()
                .execute("DELETE FROM vector_blobs WHERE seq=0", [])?;
            tx.sql()
                .execute("DELETE FROM vector_items WHERE seq=0", [])?;
            Ok(())
        })
        .unwrap();
    assert_eq!(hits(&index, Tier::Hot), ["fresh"]);
}
#[test]
fn eligibility_without_vector_write_changes_search_and_snapshot_keeps_newer_event() {
    let (_dir, store, index) = fixture();
    node(
        &store,
        1,
        "first",
        0.9,
        "personal",
        "person",
        true,
        Tier::Hot,
    );
    node(
        &store,
        2,
        "second",
        0.8,
        "personal",
        "person",
        true,
        Tier::Hot,
    );
    index.rebuild(Tier::Hot, &AtomicBool::new(false)).unwrap();
    index
        .snapshot(true, false, |tx, view| {
            store.write(|write| {
                write.touch_target("first")?;
                write.sql().execute(
                    "INSERT INTO forgotten_targets VALUES('first','test','2026-01-02')",
                    [],
                )?;
                Ok(())
            })?;
            assert_eq!(
                view.search(
                    Tier::Hot,
                    &query(),
                    1,
                    "node",
                    Pool::Ordinary,
                    Some("personal"),
                    None
                )?[0]
                    .target,
                "first"
            );
            assert_eq!(
                tx.sql()
                    .query_row("SELECT count(*) FROM forgotten_targets", [], |r| r
                        .get::<_, i64>(0))?,
                0
            );
            Ok(())
        })
        .unwrap();
    assert_eq!(hits(&index, Tier::Hot), ["second"]);
    assert!(
        store
            .read(|tx| Ok(tx.dirty(Tier::Cold, i64::MAX)?.len()))
            .unwrap()
            > 0
    );
}
#[test]
fn reopen_preserves_extreme_ids_and_blob_bytes_and_missing_index_rebuilds() {
    let (dir, store, index) = fixture();
    node(
        &store,
        i64::MIN,
        "low",
        0.8,
        "personal",
        "person",
        true,
        Tier::Hot,
    );
    node(
        &store,
        i64::MAX,
        "high",
        0.7,
        "personal",
        "person",
        true,
        Tier::Cold,
    );
    index.rebuild(Tier::Hot, &AtomicBool::new(false)).unwrap();
    index.rebuild(Tier::Cold, &AtomicBool::new(false)).unwrap();
    let before = store
        .read(|tx| {
            Ok(tx.sql().query_row(
                "SELECT hex(embedding) FROM vector_blobs WHERE seq=?",
                [i64::MIN],
                |r| r.get::<_, String>(0),
            )?)
        })
        .unwrap();
    drop(index);
    let index = Projection::open(store.clone(), ModelSignature::pinned()).unwrap();
    assert_eq!(hits(&index, Tier::Hot), ["low"]);
    assert_eq!(hits(&index, Tier::Cold), ["high"]);
    drop(index);
    std::fs::remove_dir_all(dir.path().join("search/hot")).unwrap();
    let index = Projection::open(store.clone(), ModelSignature::pinned()).unwrap();
    assert!(index.snapshot(true, false, |_, _| Ok(())).is_err());
    index.rebuild(Tier::Hot, &AtomicBool::new(false)).unwrap();
    assert_eq!(hits(&index, Tier::Hot), ["low"]);
    assert_eq!(
        store
            .read(|tx| Ok(tx.sql().query_row(
                "SELECT hex(embedding) FROM vector_blobs WHERE seq=?",
                [i64::MIN],
                |r| r.get::<_, String>(0)
            )?))
            .unwrap(),
        before
    );
}
#[test]
fn overlapping_requests_wait_within_existing_budget_and_capture_remains_free() {
    let (_dir, store, index) = fixture();
    node(
        &store,
        1,
        "fact",
        0.9,
        "personal",
        "person",
        true,
        Tier::Hot,
    );
    index.rebuild(Tier::Hot, &AtomicBool::new(false)).unwrap();
    let (send, recv) = std::sync::mpsc::channel();
    let other = index.clone();
    let first = std::thread::spawn(move || {
        other.snapshot(true, false, |_, _| {
            send.send(()).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(180));
            Ok(())
        })
    });
    recv.recv().unwrap();
    let start = std::time::Instant::now();
    store.write(|tx|{tx.sql().execute("INSERT INTO graph_commits VALUES('capture','capture','{}','{}','test','test','2026')",[])?;Ok(())}).unwrap();
    assert!(start.elapsed() < std::time::Duration::from_millis(100));
    assert_eq!(hits(&index, Tier::Hot), ["fact"]);
    first.join().unwrap().unwrap();
}
#[test]
fn cold_rebuild_stop_does_not_disable_hot_and_wrong_model_fails_closed() {
    let (_dir, store, index) = fixture();
    node(
        &store,
        1,
        "fact",
        0.9,
        "personal",
        "person",
        true,
        Tier::Hot,
    );
    index.rebuild(Tier::Hot, &AtomicBool::new(false)).unwrap();
    assert!(index.rebuild(Tier::Cold, &AtomicBool::new(true)).is_err());
    assert_eq!(hits(&index, Tier::Hot), ["fact"]);
    let mut bad = ModelSignature::pinned();
    bad.revision = "wrong".into();
    assert!(Projection::open(store, bad).is_err());
}

#[test]
fn callback_validation_preserves_index_readiness_and_caller_deadline_is_used() {
    let (_dir, store, index) = fixture();
    node(
        &store,
        1,
        "fact",
        0.9,
        "personal",
        "person",
        true,
        Tier::Hot,
    );
    index.rebuild(Tier::Hot, &AtomicBool::new(false)).unwrap();
    let failure: memento_core::Result<()> = index.snapshot(true, false, |_, _| {
        Err(memento_core::Error::Validation("Invalid query"))
    });
    assert!(failure.is_err());
    assert!(index.ready(Tier::Hot));
    assert_eq!(hits(&index, Tier::Hot), ["fact"]);
    let result = index.snapshot_until(
        true,
        false,
        std::time::Instant::now() + std::time::Duration::from_millis(100),
        |_, _| {
            std::thread::sleep(std::time::Duration::from_millis(130));
            Ok(())
        },
    );
    assert!(result.is_err());
    assert!(index.ready(Tier::Hot));
    assert!(
        index
            .snapshot_until(
                true,
                false,
                std::time::Instant::now() + std::time::Duration::from_millis(300),
                |_, _| {
                    std::thread::sleep(std::time::Duration::from_millis(130));
                    Ok(())
                }
            )
            .is_ok()
    );
}
