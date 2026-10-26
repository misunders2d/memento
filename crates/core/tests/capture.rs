use memento_core::{Error, Store, capture::EpisodeInput, raw};
use serde_json::{Value, json};
use std::{
    io::{Cursor, Read},
    sync::{Arc, Barrier},
};
fn fixture() -> (tempfile::TempDir, Arc<Store>) {
    let temp = tempfile::tempdir().unwrap();
    let store = Store::open(temp.path().join("data")).unwrap();
    (temp, store)
}
fn episode(id: &str, text: &str) -> EpisodeInput {
    serde_json::from_value(json!({"source":"native:fixture:pi","event_id":id,"conversation_id":"thread","kind":"user","text":text,"metadata":{}})).unwrap()
}
#[test]
fn capture_retry_preserves_source_offset_unicode_metadata_and_literal_search() {
    let (_temp, store) = fixture();
    let mut input = episode("one", "Ω🧠 fixed AND NEAR  phrase");
    input.occurred_at = Some("2026-01-02T12:00:00.123456+02:00".into());
    input.metadata = serde_json::from_str("{\"z\":1e-5,\"a\":{\"雪\":\"x, y: z\"}}").unwrap();
    let first = store.capture(input.clone()).unwrap();
    assert!(first.created);
    let mut retry = input.clone();
    retry.metadata = serde_json::from_str("{\"a\":{\"雪\":\"x, y: z\"},\"z\":1e-5}").unwrap();
    let second = store.capture(retry).unwrap();
    assert!(!second.created);
    assert_eq!(first.id, second.id);
    let stored = store.episode(&first.id).unwrap().unwrap();
    assert_eq!(stored["text"], input.text);
    assert_eq!(stored["occurred_at"], "2026-01-02T12:00:00.123456+02:00");
    assert!(stored.get("seq").is_none());
    assert!(stored.get("text_sha256").is_none());
    assert_eq!(
        store
            .read(|tx| Ok(tx
                .sql()
                .query_row("SELECT metadata FROM episodes", [], |r| r
                    .get::<_, String>(0))?))
            .unwrap(),
        "{\"a\":{\"雪\":\"x, y: z\"},\"z\":1e-05}"
    );
    assert_eq!(
        store
            .search_episodes(Some("AND NEAR"), 20, None, None, None)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .search_episodes(
                Some("phrase"),
                20,
                None,
                None,
                Some("2026-01-02T10:00:00+00:00")
            )
            .unwrap()
            .len(),
        1
    );
    assert!(
        store
            .search_episodes(None, 20, None, None, Some("2026-01-02T10:00:01+00:00"))
            .unwrap()
            .is_empty()
    );
    let mut changed = input;
    changed.text.push('!');
    assert!(matches!(store.capture(changed), Err(Error::Conflict(_))));
}
#[test]
fn skipped_native_opaque_runs_are_durable_before_reference_and_retry_is_lossless() {
    let (_temp, store) = fixture();
    let original = format!("前🧠 prefix {}== suffix", "A".repeat(65536));
    let mut input = episode("native", &original);
    input.kind = "native_record".into();
    input
        .metadata
        .insert("extraction_mode".into(), json!("skip"));
    let result = store.capture(input.clone()).unwrap();
    let runs = store
        .read(|tx| {
            let row: (String, String, String) =
                tx.sql()
                    .query_row("SELECT text,text_runs,text_sha256 FROM episodes", [], |r| {
                        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                    })?;
            assert_eq!(row.0, "前🧠 prefix  suffix");
            assert_eq!(row.2, raw::sha256(original.as_bytes()));
            let runs: Value = serde_json::from_str(&row.1)?;
            assert_eq!(runs[0][0], "前🧠 prefix ".chars().count());
            let sha = runs[0][1].as_str().unwrap();
            assert!(store.blobs().verify(sha)?.exists());
            let mime: String =
                tx.sql()
                    .query_row("SELECT mime FROM blobs WHERE sha256=?", [sha], |r| r.get(0))?;
            assert_eq!(mime, "application/x-memento-raw-run");
            Ok(runs)
        })
        .unwrap();
    assert_eq!(
        store.episode(&result.id).unwrap().unwrap()["text"],
        original
    );
    assert!(!store.capture(input.clone()).unwrap().created);
    let mut changed = input;
    changed.text.push_str(&"B".repeat(65536));
    assert!(matches!(store.capture(changed), Err(Error::Conflict(_))));
    assert_eq!(
        store
            .read(|tx| Ok(tx
                .sql()
                .query_row("SELECT count(*) FROM blobs", [], |r| r.get::<_, i64>(0))?))
            .unwrap(),
        1
    );
    let sha = runs[0][1].as_str().unwrap();
    assert_eq!(
        store
            .blobs()
            .reader(sha)
            .unwrap()
            .read_to_end(&mut Vec::new())
            .unwrap(),
        65538
    );
}
#[test]
fn concurrent_identical_capture_commits_once_and_conflicting_input_remains_pending() {
    let (_temp, store) = fixture();
    let barrier = Arc::new(Barrier::new(3));
    let mut threads = Vec::new();
    for _ in 0..2 {
        let store = store.clone();
        let barrier = barrier.clone();
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            store.capture(episode("same", "One exact capture"))
        }));
    }
    barrier.wait();
    let results = threads
        .into_iter()
        .map(|t| t.join().unwrap().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results[0].id, results[1].id);
    assert_eq!(results.iter().filter(|r| r.created).count(), 1);
    assert_eq!(
        store
            .read(|tx| Ok(tx
                .sql()
                .query_row("SELECT count(*) FROM episodes", [], |r| r.get::<_, i64>(0))?))
            .unwrap(),
        1
    );
    let mut invalid = episode("bad", "payload");
    invalid.source.clear();
    assert!(store.capture(invalid).is_err());
    let mut nonfinite = episode("finite", "payload");
    nonfinite.metadata = serde_json::from_str("{\"n\":1e999}").unwrap();
    assert!(store.capture(nonfinite).is_err());
}
#[test]
fn captured_artifact_offsets_wake_after_upload_and_instruction_context_is_excluded() {
    let (_temp, store) = fixture();
    let bytes = b"synthetic image bytes";
    let sha = raw::sha256(bytes);
    let text = format!("前🧠 original artifact {sha}");
    let mut input = episode("media", &text);
    input.metadata.insert("artifacts".into(),json!([{ "type":"image","sha256":sha,"filename":"../actual.png","artifact_status":"ready","mime":"image/png"}]));
    let captured = store.capture(input.clone()).unwrap();
    store
        .read(|tx| {
            let (state, filename): (String, String) =
                tx.sql()
                    .query_row("SELECT state,filename FROM captured_media", [], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })?;
            assert_eq!(state, "waiting");
            assert_eq!(filename, "actual.png");
            let (start, end, quote): (usize, usize, String) = tx.sql().query_row(
                "SELECT start,end,quote FROM captured_media_refs WHERE episode_id=?",
                [&captured.id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            assert_eq!(
                (start, end),
                (
                    "前🧠 original artifact ".chars().count(),
                    "前🧠 original artifact ".chars().count() + 64
                )
            );
            assert_eq!(quote, sha);
            Ok(())
        })
        .unwrap();
    let info = store.blobs().put(&mut Cursor::new(bytes), 1024).unwrap();
    store
        .write(|tx| {
            let registered = tx.register_blob(&info, "image/png; charset=utf-8")?;
            assert_eq!(registered["mime"], "image/png");
            Ok(())
        })
        .unwrap();
    assert_eq!(
        store
            .read(|tx| Ok(tx
                .sql()
                .query_row("SELECT state FROM captured_media", [], |r| r
                    .get::<_, String>(0))?))
            .unwrap(),
        "ready"
    );
    input.event_id = "context".into();
    input.metadata.insert(
        "native_context_kinds".into(),
        json!(["hooks.additional_context"]),
    );
    store.capture(input).unwrap();
    assert_eq!(
        store
            .read(|tx| Ok(tx.sql().query_row(
                "SELECT count(*) FROM captured_media_refs",
                [],
                |r| r.get::<_, i64>(0)
            )?))
            .unwrap(),
        1
    );
}
#[test]
fn blob_publication_repairs_same_address_and_reader_lease_blocks_eviction() {
    let (_temp, store) = fixture();
    let bytes = b"one immutable artifact";
    let info = store.blobs().put(&mut Cursor::new(bytes), 1024).unwrap();
    assert_eq!(
        store
            .blobs()
            .put(&mut Cursor::new(bytes), 1024)
            .unwrap()
            .sha256,
        info.sha256
    );
    let reader = store.blobs().reader(&info.sha256).unwrap();
    assert!(store.blobs().in_use(&info.sha256).unwrap());
    assert!(!store.blobs().evict_if_unused(&info.sha256).unwrap());
    drop(reader);
    assert!(store.blobs().evict_if_unused(&info.sha256).unwrap());
    store.blobs().put(&mut Cursor::new(bytes), 1024).unwrap();
    std::fs::write(
        store.blobs().path(&info.sha256).unwrap(),
        b"corrupted bytes",
    )
    .unwrap();
    assert!(store.blobs().reader(&info.sha256).is_err());
    store.blobs().put(&mut Cursor::new(bytes), 1024).unwrap();
    assert!(store.blobs().verify(&info.sha256).is_ok());
    assert!(store.blobs().put(&mut Cursor::new(bytes), 1).is_err());
    assert!(store.blobs().path("../outside").is_err());
    assert!(
        store
            .write(|tx| tx.register_blob(&info, "invalid MIME"))
            .is_err()
    );
    store
        .write(|tx| {
            tx.register_blob(&info, "image/png")?;
            assert_eq!(tx.register_blob(&info, "audio/wav")?["mime"], "image/png");
            let mut wrong = info.clone();
            wrong.size += 1;
            assert!(tx.register_blob(&wrong, "image/png").is_err());
            Ok(())
        })
        .unwrap();
}

#[test]
fn native_raw_frame_compression_keeps_exact_unicode_retries_and_normal_messages() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("data")).unwrap();
    let text = "Ω🧠б, line\r\n".repeat(30000);
    let input:memento_core::capture::EpisodeInput=serde_json::from_value(serde_json::json!({"source":"native:synthetic:pi","event_id":"raw","conversation_id":"thread","kind":"native_record","text":text,"metadata":{"extraction_mode":"skip"}})).unwrap();
    let first = store.capture(input.clone()).unwrap();
    let normal=store.capture(serde_json::from_value(serde_json::json!({"source":"native:synthetic:pi","event_id":"normal","conversation_id":"thread","kind":"user","text":"Unchanged searchable projection Ω","metadata":{}})).unwrap()).unwrap();
    while store.compress_raw_step().unwrap() {}
    let restored = store.episode(&first.id).unwrap().unwrap();
    assert_eq!(restored["text"], text);
    assert_eq!(store.capture(input).unwrap().id, first.id);
    assert_eq!(
        store.episode(&normal.id).unwrap().unwrap()["text"],
        "Unchanged searchable projection Ω"
    );
    assert_eq!(
        store
            .search_episodes(Some("searchable"), 10, None, None, None)
            .unwrap()
            .len(),
        1
    );
    store
        .read(|tx| {
            let (skeleton, refs): (String, String) = tx.sql().query_row(
                "SELECT text,raw_frame FROM episodes WHERE id=?",
                [&first.id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            assert!(skeleton.is_empty());
            assert!(
                serde_json::from_str::<serde_json::Value>(&refs)?
                    .as_array()
                    .unwrap()
                    .len()
                    > 1
            );
            memento_core::raw::validate_frames(tx.sql())?;
            assert!(
                tx.sql()
                    .execute("UPDATE episodes SET text='forged' WHERE id=?", [&first.id])
                    .is_err()
            );
            Ok(())
        })
        .unwrap();
    let path = store.data_dir().to_owned();
    drop(store);
    let reopened = Store::open(path).unwrap();
    while reopened.compress_raw_step().unwrap() {}
    assert_eq!(reopened.episode(&first.id).unwrap().unwrap()["text"], text);
}
#[test]
fn frame_compression_keeps_conversation_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("data")).unwrap();
    for i in 0..3 {
        store.capture(serde_json::from_value(serde_json::json!({"source":"native:synthetic:pi","event_id":format!("r{i}"),"conversation_id":format!("thread{i}"),"kind":"native_record","text":format!("private synthetic {i}"),"metadata":{"extraction_mode":"skip"}})).unwrap()).unwrap();
    }
    assert!(store.compress_raw_step().unwrap());
    let count = store
        .read(|tx| {
            Ok(tx
                .sql()
                .query_row("SELECT count(*) FROM raw_frames", [], |r| {
                    r.get::<_, i64>(0)
                })?)
        })
        .unwrap();
    assert_eq!(count, 1);
    while store.compress_raw_step().unwrap() {}
    store
        .read(|tx| {
            assert_eq!(
                tx.sql().query_row(
                    "SELECT count(DISTINCT conversation_id) FROM raw_frames",
                    [],
                    |r| r.get::<_, i64>(0)
                )?,
                3
            );
            memento_core::raw::validate_frames(tx.sql())?;
            Ok(())
        })
        .unwrap();
}

#[test]
fn failed_frame_publication_rolls_back_all_frames_and_cursor_then_retries() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("data")).unwrap();
    let original = "Ω exact retained raw text";
    let episode=store.capture(serde_json::from_value(serde_json::json!({"source":"native:synthetic:pi","event_id":"rollback","conversation_id":"thread","kind":"native_record","text":original,"metadata":{"extraction_mode":"skip"}})).unwrap()).unwrap();
    store.write(|tx|{tx.sql().execute_batch("CREATE TRIGGER synthetic_reject BEFORE UPDATE ON episodes WHEN NEW.raw_frame IS NOT NULL BEGIN SELECT RAISE(ABORT,'synthetic rollback'); END;")?;Ok(())}).unwrap();
    assert!(store.compress_raw_step().is_err());
    store
        .read(|tx| {
            assert_eq!(
                tx.sql()
                    .query_row("SELECT count(*) FROM raw_frames", [], |r| r
                        .get::<_, i64>(0))?,
                0
            );
            assert_eq!(tx.episode(&episode.id)?.unwrap()["text"], original);
            Ok(())
        })
        .unwrap();
    store
        .write(|tx| {
            tx.sql().execute_batch("DROP TRIGGER synthetic_reject")?;
            Ok(())
        })
        .unwrap();
    assert!(store.compress_raw_step().unwrap());
    assert_eq!(
        store.episode(&episode.id).unwrap().unwrap()["text"],
        original
    );
}

#[test]
fn captured_missing_document_filename_uses_baseline_mime_suffix_for_real_decoder_selection() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("data")).unwrap();
    for (i, (mime, suffix)) in [
        ("text/html", ".html"),
        ("text/markdown", ".md"),
        ("text/rtf", ".rtf"),
        (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            ".docx",
        ),
        (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".xlsx",
        ),
        (
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            ".pptx",
        ),
        ("application/vnd.oasis.opendocument.text", ".odt"),
        ("application/vnd.oasis.opendocument.spreadsheet", ".ods"),
        ("application/vnd.oasis.opendocument.presentation", ".odp"),
    ]
    .into_iter()
    .enumerate()
    {
        let sha = format!("{i:064x}");
        let input=serde_json::from_value(serde_json::json!({"source":"native:synthetic:pi","event_id":format!("doc{i}"),"conversation_id":"thread","kind":"user","text":sha,"metadata":{"artifacts":[{"type":"file","artifact_status":"captured","sha256":sha,"mime":mime}]}})).unwrap();
        store.capture(input).unwrap();
        let filename = store
            .read(|tx| {
                Ok(tx.sql().query_row(
                    "SELECT filename FROM captured_media WHERE sha256=?",
                    [&sha],
                    |r| r.get::<_, String>(0),
                )?)
            })
            .unwrap();
        assert_eq!(filename, format!("{sha}{suffix}"));
    }
}
