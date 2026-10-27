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
fn exact_filtered_updates_flush_and_reopen_without_building_hnsw() {
    let (_dir, store, index) = fixture();
    for (seq, id, cos, scope, kind, current) in [
        (0, "wanted", 0.7, "personal", "person", true),
        (1, "other-scope", 0.99, "professional", "person", true),
        (2, "other-type", 0.99, "personal", "tool", true),
        (3, "inactive", 0.99, "personal", "person", false),
    ] {
        node(&store, seq, id, cos, scope, kind, current, Tier::Hot);
    }
    // Cross two 10,000-KiB thresholds so real Cold conversion is exercised.
    // HNSW-labelled storage may report indexed vectors with both graphs off.
    store
        .write(|tx| {
            let embedding = vector(0.95);
            for seq in 4..7004 {
                tx.put_vector(&VectorWrite {
                    seq,
                    target: format!("inactive-padding-{seq}"),
                    kind: VectorKind::Node,
                    version: 1,
                    current: false,
                    tier: Tier::Hot,
                    embedding: embedding.clone(),
                })?;
            }
            Ok(())
        })
        .unwrap();
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
    // The ordinary snapshot catches up the pending upsert and durably flushes it.
    assert_eq!(hits(&index, Tier::Hot), ["fresh"]);
    drop(index);
    let shard =
        qdrant_edge::EdgeShard::load(&store.data_dir().join("search/hot/edge"), None).unwrap();
    let info = shard.info().unwrap();
    assert_eq!(info.points_count, 7005);
    let mut live_segments = 0;
    let mut cold_segments = 0;
    for entry in std::fs::read_dir(store.data_dir().join("search/hot/edge/segments")).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy();
        if !path.is_dir() || name.starts_with('.') || name.ends_with(".deleted")
            || !path.join("version.info").is_file() { continue; }
        live_segments += 1;
        let state:qdrant_edge::SegmentState = serde_json::from_slice(
            &std::fs::read(path.join("segment.json")).unwrap()).unwrap();
        let vector = &state.config.vector_data[qdrant_edge::DEFAULT_VECTOR_NAME];
        let actual = serde_json::to_value(vector).unwrap();
        assert_eq!(actual["datatype"], "float32");
        assert!(vector.quantization_config.is_none());
        if !state.config.is_appendable() {
            cold_segments += 1;
            assert_eq!(actual["storage_type"], "Mmap");
            assert_eq!(actual["index"]["type"], "hnsw");
            assert_eq!(actual["index"]["options"]["m"], 0);
            assert_eq!(actual["index"]["options"]["payload_m"], 0);
            let graph:serde_json::Value = serde_json::from_slice(
                &std::fs::read(path.join("vector_index/hnsw_config.json")).unwrap()).unwrap();
            for key in ["m", "m0", "payload_m", "payload_m0"] {
                assert_eq!(graph[key], 0, "built graph parameter {key}");
            }
        }
    }
    assert_eq!(live_segments, info.segments_count);
    assert!(cold_segments > 0, "fixture did not prove actual nonappendable Cold storage");
    eprintln!("native indexed-vector storage count: {}", info.indexed_vectors_count);
    for field in ["kind", "current", "ordinary", "personal", "professional"] {
        assert!(
            info.payload_schema
                .keys()
                .any(|key| key.to_string() == field)
        );
    }
    drop(shard);
    let index = Projection::open(store, ModelSignature::pinned()).unwrap();
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

#[test]
fn caught_up_read_does_not_acquire_held_capture_writer() {
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
    let (entered, wait) = std::sync::mpsc::channel();
    let (release, ready) = std::sync::mpsc::channel();
    let other = store.clone();
    let writer = std::thread::spawn(move || {
        other.write(|_tx| {
            entered.send(()).unwrap();
            let _ = ready.recv_timeout(std::time::Duration::from_millis(500));
            Ok(())
        })
    });
    wait.recv().unwrap();
    let started = std::time::Instant::now();
    assert_eq!(hits(&index, Tier::Hot), ["fact"]);
    let elapsed = started.elapsed();
    let _ = release.send(());
    writer.join().unwrap().unwrap();
    assert!(
        elapsed < std::time::Duration::from_millis(150),
        "healthy read waited for capture writer: {elapsed:?}"
    );
}

#[test]
fn parking_moves_raw_point_between_shards_without_changing_bytes() {
    let (_dir, store, index) = fixture();
    store.write(|tx|{
        tx.sql().execute("INSERT INTO episodes(seq,id,source,event_id,conversation_id,kind,text,metadata,captured_at) VALUES(1,'episode','fixture','event','conversation','user','A preserved historical passage','{}','2026')",[])?;
        tx.sql().execute("INSERT INTO episode_chunks VALUES('chunk','episode',0,30)",[])?;tx.put_vector(&VectorWrite{seq:30,target:"chunk".into(),kind:VectorKind::Episode,version:1,current:true,tier:Tier::Hot,embedding:vector(0.9)})?;Ok(())}).unwrap();
    index.rebuild(Tier::Hot, &AtomicBool::new(false)).unwrap();
    index.rebuild(Tier::Cold, &AtomicBool::new(false)).unwrap();
    let original = store
        .read(|tx| {
            Ok(tx.sql().query_row(
                "SELECT hex(embedding) FROM vector_blobs WHERE seq=30",
                [],
                |r| r.get::<_, String>(0),
            )?)
        })
        .unwrap();
    store.write(|tx|{tx.sql().execute("INSERT INTO historical_parking(source,conversation_id,parked_through,live_seq,live_offset) VALUES('fixture','conversation',1,1,0)",[])?;tx.touch_conversation("fixture","conversation")?;Ok(())}).unwrap();
    index
        .snapshot(true, true, |_, view| {
            assert!(
                view.search(
                    Tier::Hot,
                    &query(),
                    1,
                    "episode",
                    Pool::Ordinary,
                    None,
                    None
                )?
                .is_empty()
            );
            assert_eq!(
                view.search(
                    Tier::Cold,
                    &query(),
                    1,
                    "episode",
                    Pool::Historical,
                    None,
                    None
                )?[0]
                    .target,
                "chunk"
            );
            Ok(())
        })
        .unwrap();
    assert_eq!(
        store
            .read(|tx| Ok(tx.sql().query_row(
                "SELECT hex(embedding) FROM vector_blobs WHERE seq=30",
                [],
                |r| r.get::<_, String>(0)
            )?))
            .unwrap(),
        original
    );
}

#[test]
fn raw_pool_upgrade_retiers_in_batches_preserves_rows_and_rebuilds_old_payload_format() {
    let (_dir,store,index)=fixture();
    node(&store,50000,"curated",0.8,"personal","person",true,Tier::Hot);
    let ids=std::iter::once(i64::MIN).chain(0..255).chain(std::iter::once(i64::MAX)).collect::<Vec<_>>();
    store.write(|tx| {
        tx.sql().execute("INSERT INTO episodes(seq,id,source,event_id,conversation_id,kind,text,metadata,captured_at) VALUES(1,'raw-source','fixture','raw-source','raw-source','user',?,'{}','2026')",["Retained raw source passage ".repeat(300)])?;
        for (position,seq) in ids.iter().enumerate() {
            let chunk=format!("raw-{position}");
            tx.sql().execute("INSERT INTO episode_chunks VALUES(?,'raw-source',?,?)",params![chunk,position as i64,position as i64+1])?;
            tx.put_vector(&VectorWrite{seq:*seq,target:chunk,kind:VectorKind::Episode,version:1,current:*seq!=0,tier:Tier::Hot,embedding:vector(0.9)})?;
        }
        Ok(())
    }).unwrap();
    let preserved=|| -> Vec<(i64,String,String,i64,Vec<u8>,bool)> {
        store.read(|tx| {
            let mut query=tx.sql().prepare("SELECT i.seq,i.target,i.kind,i.version,b.embedding,b.current FROM vector_items i JOIN vector_blobs b ON b.seq=i.seq ORDER BY i.seq")?;
            Ok(query.query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)))?.collect::<rusqlite::Result<Vec<_>>>()?)
        }).unwrap()
    };
    let state=|| -> (i64,i64) {store.read(|tx|Ok((tx.sql().query_row("SELECT generation FROM runtime_state",[],|r|r.get(0))?,tx.sql().query_row("SELECT count(*) FROM projection_events",[],|r|r.get(0))?))).unwrap()};
    let original=preserved();
    index.rebuild(Tier::Hot,&AtomicBool::new(false)).unwrap();
    index.rebuild(Tier::Cold,&AtomicBool::new(false)).unwrap();
    for tier in ["hot","cold"] {
        let path=store.data_dir().join("search").join(tier).join("checkpoint.json");
        let mut checkpoint:serde_json::Value=serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        checkpoint["format"]=serde_json::json!(1);
        std::fs::write(path,serde_json::to_vec(&checkpoint).unwrap()).unwrap();
    }
    let before=state();
    drop(index);
    let index=Projection::open(store.clone(),ModelSignature::pinned()).unwrap();
    let migrated=state();
    assert_eq!(migrated.0,before.0+2); //257 raw rows in existing256-row transactions.
    assert_eq!(migrated.1,before.1+2*ids.len() as i64);
    assert_eq!(preserved(),original);
    assert!(index.synchronize(Tier::Hot).is_err()); //Old payload format cannot serve.
    drop(index);
    let index=Projection::open(store.clone(),ModelSignature::pinned()).unwrap();
    assert_eq!(state(),migrated); //No marker/counter is needed for an idempotent reopen.
    index.rebuild(Tier::Hot,&AtomicBool::new(false)).unwrap();
    assert!(!index.ready(Tier::Cold));
    assert_eq!(hits(&index,Tier::Hot),["curated"]);
    index.rebuild(Tier::Cold,&AtomicBool::new(false)).unwrap();
    index.snapshot(true,true,|_,view| {
        assert!(view.search(Tier::Hot,&query(),300,"episode",Pool::Ordinary,None,None)?.is_empty());
        let raw=view.search(Tier::Cold,&query(),300,"episode",Pool::RawFallback,None,None)?;
        assert_eq!(raw.len(),256);
        assert!(raw.iter().any(|r|r.seq==i64::MIN));
        assert!(raw.iter().any(|r|r.seq==i64::MAX));
        assert!(raw.iter().all(|r|r.seq!=0));
        assert!(view.search(Tier::Cold,&query(),300,"episode",Pool::Historical,None,None)?.is_empty());
        Ok(())
    }).unwrap();
    assert_eq!(preserved(),original);
}
