use memento_core::*;
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::{cell::Cell, os::unix::fs::OpenOptionsExt, sync::mpsc, thread};
fn store() -> (tempfile::TempDir, std::sync::Arc<Store>) {
    let temp = tempfile::tempdir().unwrap();
    let store = Store::open(temp.path().join("data")).unwrap();
    (temp, store)
}
fn context(id: &str) -> CommandContext {
    CommandContext {
        command_id: id.into(),
        actor: "owner".into(),
        why: "Synthetic verification".into(),
    }
}
fn vector() -> F32Blob {
    let mut values = vec![0.0; EMBED_DIM];
    values[..4].copy_from_slice(&[-0.0, 0.25, f32::from_bits(1), -0.75]);
    F32Blob::from_values(&values).unwrap()
}
fn put(tx: &WriteTxn<'_, '_>) -> Result<EventFence> {
    tx.put_vector(&VectorWrite {
        seq: 0,
        target: "node".into(),
        kind: VectorKind::Node,
        version: 1,
        current: true,
        tier: Tier::Hot,
        embedding: vector(),
    })
}
fn seed(tx: &WriteTxn<'_, '_>, commit: &str) -> Result<()> {
    tx.sql().execute("INSERT INTO episodes(seq,id,source,event_id,conversation_id,kind,text,occurred_at,metadata,captured_at) VALUES(1,'episode','fixture','event','chat','user','Ω😀 synthetic source',NULL,'{}','2026-10-20T12:00:00+00:00')",[])?;
    tx.sql().execute("INSERT INTO node_versions VALUES('node',1,?,'knowledge','Supported fixture','active',NULL,NULL,NULL,0,'[\"personal\"]')",[commit])?;
    tx.sql()
        .execute("INSERT INTO node_stats(node_id) VALUES('node')", [])?;
    tx.sql().execute(
        "INSERT INTO graph_evidence VALUES('node',1,'episode',0,2,'Ω😀')",
        [],
    )?;
    Ok(())
}
#[test]
fn fresh_header5_wal_full_and_one_writer() {
    let (_temp, store) = store();
    let identity = store
        .read(|tx| {
            assert_eq!(
                tx.sql()
                    .query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))?,
                5
            );
            assert_eq!(
                tx.sql()
                    .query_row("PRAGMA journal_mode", [], |r| r.get::<_, String>(0))?,
                "wal"
            );
            assert!(
                tx.sql()
                    .prepare("SELECT embedding FROM memory_vectors")
                    .is_err()
            );
            assert_eq!(tx.source_cut().generation, 0);
            Ok(tx.source_cut().db_uuid.clone())
        })
        .unwrap();
    assert!(matches!(
        Store::open(store.data_dir()),
        Err(Error::Conflict(_))
    ));
    let path = store.data_dir().to_owned();
    drop(store);
    let reopened = Store::open(path).unwrap();
    assert_eq!(
        reopened
            .read(|tx| Ok(tx.source_cut().db_uuid.clone()))
            .unwrap(),
        identity
    );
}
#[test]
fn unsupported_header_refused_before_any_mutation() {
    for header in [1, 2, 3, 4, 99] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("memory.sqlite");
        let db = Connection::open(&path).unwrap();
        db.pragma_update(None, "user_version", header).unwrap();
        drop(db);
        let before = std::fs::read(&path).unwrap();
        assert!(matches!(Store::open(temp.path()),Err(Error::UnsupportedHeader(v)) if v==header));
        assert_eq!(std::fs::read(path).unwrap(), before);
        assert!(!temp.path().join("writer.lock").exists());
    }
}
#[test]
fn python_canonical_spacing_unicode_numbers_and_defaults() {
    let value: Value = serde_json::from_str(r#"{"z":"Ω😀\n","a":{"b":1e-6,"a":-0.0}}"#).unwrap();
    assert_eq!(
        canonical_json(&value).unwrap(),
        "{\"a\": {\"a\": -0.0, \"b\": 1e-06}, \"z\": \"Ω😀\\n\"}"
    );
    for (source, expected) in [
        ("0.0", "0.0"),
        ("-0.0", "-0.0"),
        ("1e-6", "1e-06"),
        ("1e-4", "0.0001"),
        ("1e15", "1000000000000000.0"),
        ("1e16", "1e+16"),
        ("1e20", "1e+20"),
        ("5e-324", "5e-324"),
        ("1.2345e-7", "1.2345e-07"),
        (
            "340282366920938463463374607431768211455",
            "340282366920938463463374607431768211455",
        ),
    ] {
        assert_eq!(
            canonical_json(&serde_json::from_str(source).unwrap()).unwrap(),
            expected,
            "{source}"
        );
    }
    assert!(canonical_json(&serde_json::from_str("1e400").unwrap()).is_err());
}
#[test]
fn exact_old_command_retry_does_not_reapply_or_hide_changes() {
    let (_temp, store) = store();
    let ctx = CommandContext {
        command_id: "legacy".into(),
        actor: "ø".into(),
        why: "réessai".into(),
    };
    let old = r#"{"actor": "ø", "command": {"n": 1e-06, "text": "Ω😀"}, "why": "réessai"}"#;
    store.write(|tx|{tx.sql().execute("INSERT INTO graph_commits VALUES('old','legacy',?,'{\"id\":\"already\"}','ø','réessai','2026-10-20T12:00:00+00:00')",[old])?;Ok(())}).unwrap();
    let applied = Cell::new(false);
    let request: Value = serde_json::from_str(r#"{"text":"Ω😀","n":0.000001}"#).unwrap();
    let result = store
        .command(&ctx, &request, |_, _| {
            applied.set(true);
            Ok(json!({"id":"wrong"}))
        })
        .unwrap();
    assert_eq!(result, json!({"id":"already"}));
    assert!(!applied.get());
    assert!(matches!(
        store.command(&ctx, &json!({"n":1e-6,"text":"different"}), |_, _| Ok(
            json!({})
        )),
        Err(Error::Conflict(_))
    ));
}
#[test]
fn authority_history_is_immutable_and_failed_command_rolls_back_events() {
    let (_temp, store) = store();
    store
        .command(&context("seed"), &json!({"op":"seed"}), |tx, id| {
            seed(tx, id)?;
            put(tx)?;
            Ok(json!({"id":"node"}))
        })
        .unwrap();
    let before = store
        .read(|tx| Ok((tx.source_cut().generation, tx.fence(Tier::Hot)?)))
        .unwrap();
    let result = store.command(&context("failed"), &json!({"op":"failed"}), |tx, _| {
        tx.touch_target("node")?;
        tx.sql()
            .execute("UPDATE node_versions SET text='not permitted'", [])?;
        Ok(json!({}))
    });
    assert!(result.is_err());
    store
        .read(|tx| {
            assert_eq!(tx.source_cut().generation, before.0);
            assert_eq!(tx.fence(Tier::Hot)?, before.1);
            assert_eq!(
                tx.sql().query_row(
                    "SELECT count(*) FROM graph_commits WHERE command_id='failed'",
                    [],
                    |r| r.get::<_, i64>(0)
                )?,
                0
            );
            assert_eq!(
                tx.sql()
                    .query_row("SELECT text FROM node_versions WHERE id='node'", [], |r| {
                        r.get::<_, String>(0)
                    })?,
                "Supported fixture"
            );
            Ok(())
        })
        .unwrap();
}
#[test]
fn eligibility_only_write_is_explicitly_dirty_and_old_cut_ack_retains_newer_same_target() {
    let (_temp, store) = store();
    store
        .command(&context("seed"), &json!({"op":"seed"}), |tx, id| {
            seed(tx, id)?;
            put(tx)?;
            Ok(json!({}))
        })
        .unwrap();
    store
        .read(|old| {
            let cut = old.fence(Tier::Hot)?;
            let bytes =
                old.sql()
                    .query_row("SELECT embedding FROM vector_blobs WHERE seq=0", [], |r| {
                        r.get::<_, Vec<u8>>(0)
                    })?;
            store.write(|tx| {
                tx.sql().execute(
                    "INSERT INTO forgotten_targets VALUES('node','fixture-forget','node')",
                    [],
                )?;
                tx.touch_forgotten_target("node")?;
                Ok(())
            })?;
            assert_eq!(
                old.sql()
                    .query_row("SELECT count(*) FROM forgotten_targets", [], |r| r
                        .get::<_, i64>(0))?,
                0,
                "Pinned old WAL cut must remain coherent"
            );
            store.write(|tx| tx.acknowledge(Tier::Hot, &cut))?;
            store.read(|new| {
                assert_eq!(
                    new.sql()
                        .query_row("SELECT count(*) FROM forgotten_targets", [], |r| r
                            .get::<_, i64>(0))?,
                    1
                );
                assert_eq!(
                    new.sql().query_row(
                        "SELECT embedding FROM vector_blobs WHERE seq=0",
                        [],
                        |r| r.get::<_, Vec<u8>>(0)
                    )?,
                    bytes
                );
                let hot = new.dirty(Tier::Hot, new.fence(Tier::Hot)?.id)?;
                assert_eq!(hot.len(), 1);
                assert!(hot[0].fence.id > cut.id);
                let cold = new.dirty(Tier::Cold, new.fence(Tier::Cold)?.id)?;
                assert_eq!(cold.len(), 1);
                assert_eq!(hot[0].fence.generation, cold[0].fence.generation);
                assert_eq!(new.source_cut().generation, 2);
                Ok(())
            })?;
            Ok(())
        })
        .unwrap();
}
#[test]
fn dependency_selectors_dirty_evidence_source_type_and_metadata_only_vectors() {
    let (_temp, store) = store();
    store
        .command(&context("seed"), &json!({"op":"seed"}), |tx, id| {
            seed(tx, id)?;
            put(tx)?;
            tx.sql().execute(
                "INSERT INTO episode_chunks VALUES('chunk','episode',0,2)",
                [],
            )?;
            tx.sql()
                .execute("INSERT INTO vector_items VALUES(7,'chunk','episode',1)", [])?;
            Ok(json!({}))
        })
        .unwrap();
    store
        .write(|tx| {
            let source = tx.touch_source("fixture", "event")?;
            assert_eq!(
                source.len(),
                4,
                "Node evidence and metadata-only raw chunk each need both tiers"
            );
            assert_eq!(tx.touch_type("knowledge")?.len(), 2);
            assert_eq!(tx.touch_conversation("fixture", "chat")?.len(), 4);
            Ok(())
        })
        .unwrap();
}
#[test]
fn write_lock_is_not_held_by_read_snapshot() {
    let (_temp, store) = store();
    let shared = store.clone();
    let (send, recv) = mpsc::channel();
    store
        .read(|_| {
            let child = thread::spawn(move || {
                let result = shared.write(|tx| {
                    tx.sql().execute(
                        "INSERT INTO processing_progress(source,conversation_id) VALUES('s','c')",
                        [],
                    )?;
                    Ok(())
                });
                send.send(result.is_ok()).unwrap();
            });
            assert!(
                recv.recv_timeout(std::time::Duration::from_secs(2))
                    .unwrap()
            );
            child.join().unwrap();
            Ok(())
        })
        .unwrap();
}
#[test]
fn f32_blob_preserves_bits_and_codepoint_spans_keep_overlap() {
    let values = vector();
    assert_eq!(F32Blob::from_values(&values.values()).unwrap(), values);
    assert_eq!(values.values()[0].to_bits(), (-0.0f32).to_bits());
    assert_eq!(values.values()[2].to_bits(), 1);
    let text = "Ω😀 e\u{301} :: café 🧠 / café 🧠";
    let span = source_span(text, "café 🧠", Some(1)).unwrap();
    assert_eq!(span.slice(text).unwrap(), "café 🧠");
    assert_ne!(text.get(span.start..span.end), Some("café 🧠"));
    assert_eq!(
        source_span("aaaa", "aa", Some(1)).unwrap(),
        CharSpan { start: 1, end: 3 }
    );
    assert!(source_span("aaaa", "aa", None).is_err());
    assert!(F32Blob::from_bytes(vec![0; 2]).is_err());
}
#[test]
fn utc_date_normalization_and_legacy_context_guard() {
    assert_eq!(
        utc_time("2026-10-20T13:30:00.123456789+01:30").unwrap(),
        "2026-10-20T12:00:00.123456+00:00"
    );
    assert!(utc_time("2026-10-20T12:00:00").is_err());
    let text = "# AGENTS.md instructions\n<INSTRUCTIONS>fixture</INSTRUCTIONS>";
    assert!(raw::legacy_native_context(
        "native:fixture:codex",
        "user",
        Some(text),
        Some(&json!({}))
    ));
    assert!(!raw::legacy_native_context(
        "native:fixture:codex",
        "user",
        Some(text),
        Some(&json!({"extraction_mode":null}))
    ));
}
#[test]
fn local_original_raw_frame_and_opaque_blob_hydrate_without_nested_writer() {
    use std::io::Write;
    let (_temp, store) = store();
    let original = "Результат 🧠 Ω😀";
    let sha = raw::sha256(original.as_bytes());
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(original.as_bytes()).unwrap();
    let compressed = encoder.finish().unwrap();
    store.write(|tx|{tx.sql().execute("INSERT INTO episodes(seq,id,source,event_id,conversation_id,kind,text,metadata,captured_at) VALUES(1,'frame','native:fixture:pi','f','raw','native_record',?,'{\"extraction_mode\":\"skip\"}','2026-10-20T12:00:00+00:00')",[original])?;
        tx.sql().execute("INSERT INTO raw_frames VALUES(1,'native:fixture:pi','raw',?,1,?,?)",params![original.len(),sha,compressed])?;
        let refs=format!("[[1,0,{}]]",original.len());tx.authorize_frame(1,&refs,&sha)?;
        tx.sql().execute("UPDATE episodes SET text='',raw_frame=?,text_sha256=? WHERE id='frame'",params![refs,sha])?;
        assert_eq!(tx.original_text("frame")?,Some(original.into()));Ok(())}).unwrap();
    store
        .read(|tx| {
            assert_eq!(tx.original_text("frame")?, Some(original.into()));
            Ok(())
        })
        .unwrap();
    // This read does not invoke a model/network client, and no data is fabricated on loss.
    store
        .write(|tx| {
            assert!(
                tx.sql()
                    .execute("UPDATE episodes SET text='changed' WHERE id='frame'", [])
                    .is_err()
            );
            Ok(())
        })
        .unwrap();
    let payload = "A".repeat(65536);
    let payload_sha = raw::sha256(payload.as_bytes());
    let prefix = "Ω😀 before ";
    let suffix = " after e\u{301}";
    let full = format!("{prefix}{payload}{suffix}");
    let full_sha = raw::sha256(full.as_bytes());
    let directory = store.data_dir().join("blobs").join(&payload_sha[..2]);
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join(&payload_sha[2..]);
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&path)
        .unwrap();
    file.write_all(payload.as_bytes()).unwrap();
    file.sync_all().unwrap();
    drop(file);
    store.write(|tx|{let runs=serde_json::to_string(&json!([[prefix.chars().count(),payload_sha,payload.len()]]))?;
        tx.sql().execute("INSERT INTO episodes(seq,id,source,event_id,conversation_id,kind,text,metadata,captured_at,text_runs,text_sha256) VALUES(2,'opaque','native:fixture:pi','o','raw','native_record',?,'{\"extraction_mode\":\"skip\"}','2026-10-20T12:00:00+00:00',?,?)",params![format!("{prefix}{suffix}"),runs,full_sha])?;
        assert_eq!(tx.original_text("opaque")?,Some(full.clone()));Ok(())}).unwrap();
    store
        .read(|tx| {
            assert_eq!(tx.original_text("opaque")?, Some(full));
            Ok(())
        })
        .unwrap();
    std::fs::remove_file(path).unwrap();
    assert!(matches!(
        store.read(|tx| tx.original_text("opaque")),
        Err(Error::Unavailable(_))
    ));
}

#[test]
fn offline_validation_checks_unreferenced_frames_with_bounded_decode() {
    use std::io::Write;
    let (_dir, store) = store();
    let bytes = b"retained original raw";
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).unwrap();
    let compressed = encoder.finish().unwrap();
    store
        .write(|tx| {
            tx.sql().execute(
                "INSERT INTO raw_frames VALUES(1,'fixture','chat',?,1,?,?)",
                params![bytes.len(), raw::sha256(bytes), compressed],
            )?;
            raw::validate_frames(tx.sql())?;
            tx.sql().execute(
                "INSERT INTO raw_frames VALUES(2,'fixture','chat',?,1,'wrong-checksum',?)",
                params![bytes.len(), compressed],
            )?;
            assert!(matches!(
                raw::validate_frames(tx.sql()),
                Err(Error::Unavailable(_))
            ));
            Ok(())
        })
        .unwrap();
}

#[test]
fn derived_media_stage_upgrade_is_transactional_and_healthy_reopen_has_no_ddl() {
    let (dir, store) = store();
    let path = store.data_dir().to_owned();
    let original = store
        .read(|tx| {
            Ok(tx
                .sql()
                .query_row("PRAGMA schema_version", [], |r| r.get::<_, i64>(0))?)
        })
        .unwrap();
    drop(store);
    let reopened = Store::open(&path).unwrap();
    assert_eq!(
        reopened
            .read(|tx| Ok(tx
                .sql()
                .query_row("PRAGMA schema_version", [], |r| r.get::<_, i64>(0))?))
            .unwrap(),
        original
    );
    drop(reopened);
    let db = Connection::open(path.join("memory.sqlite")).unwrap();
    db.execute_batch("DROP TABLE media_stage_units").unwrap();
    drop(db);
    let upgraded = Store::open(&path).unwrap();
    let version = upgraded
        .read(|tx| {
            assert_eq!(
                tx.sql()
                    .query_row("SELECT count(*) FROM media_stage_units", [], |r| r
                        .get::<_, i64>(0))?,
                0
            );
            Ok(tx
                .sql()
                .query_row("PRAGMA schema_version", [], |r| r.get::<_, i64>(0))?)
        })
        .unwrap();
    assert_eq!(version, original + 4);
    drop(upgraded);
    let again = Store::open(&path).unwrap();
    assert_eq!(
        again
            .read(|tx| Ok(tx
                .sql()
                .query_row("PRAGMA schema_version", [], |r| r.get::<_, i64>(0))?))
            .unwrap(),
        version
    );
    drop(again);
    drop(dir);
}

#[test]
fn health_uses_separate_bounded_writer_probe_and_never_recreates_missing_database() {
    let (_dir, store) = store();
    let (entered, wait) = mpsc::channel();
    let (release, ready) = mpsc::channel();
    let other = store.clone();
    let writer = thread::spawn(move || {
        other.write(|_| {
            entered.send(()).unwrap();
            let _ = ready.recv_timeout(std::time::Duration::from_secs(1));
            Ok(())
        })
    });
    wait.recv().unwrap();
    let began = std::time::Instant::now();
    assert!(store.health().is_err());
    assert!(began.elapsed() < std::time::Duration::from_millis(700));
    release.send(()).unwrap();
    writer.join().unwrap().unwrap();
    store.health().unwrap();
    let database = store.data_dir().join("memory.sqlite");
    std::fs::remove_file(&database).unwrap();
    assert!(store.health().is_err());
    assert!(!database.exists());
}
