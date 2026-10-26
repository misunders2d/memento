//! Admission controls only: no synthetic or real model inference is needed.
use super::captured::CapturedMedia;
use memento_core::{Store, blobs::BlobInfo, capture::EpisodeInput};
use memento_domain::{Graph, jobs::Queue};
use rusqlite::params;
use serde_json::{Value, json};
use std::{io::Cursor, sync::Arc};

struct Fixture {
    _dir: tempfile::TempDir,
    store: Arc<Store>,
    queue: Queue,
    captured: CapturedMedia,
}
impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data")).unwrap();
        store.write(|tx| { tx.sql().execute("INSERT INTO vector_model(singleton,model_id,revision,dimensions,text_encoding) VALUES(1,?,?,768,1)",params![memento_embedding::MODEL_ID,memento_embedding::MODEL_REVISION])?; Ok(()) }).unwrap();
        let queue = Queue::new(Graph::new(store.clone()));
        Self {
            _dir: dir,
            store,
            captured: CapturedMedia::new(queue.clone()),
            queue,
        }
    }
    fn blob(&self, content: &str, register: bool) -> BlobInfo {
        let info = self
            .store
            .blobs()
            .put(&mut Cursor::new(content.as_bytes()), 10000)
            .unwrap();
        if register {
            self.register(&info);
        }
        info
    }
    fn register(&self, info: &BlobInfo) {
        self.store
            .write(|tx| {
                tx.register_blob(info, "text/plain")?;
                Ok(())
            })
            .unwrap();
    }
    fn capture(&self, event: &str, text: &str, metadata: Value) -> String {
        self.store.capture(serde_json::from_value::<EpisodeInput>(json!({"source":"native:pi:fixture","event_id":event,"conversation_id":"thread","kind":"user","text":text,"metadata":metadata})).unwrap()).unwrap().id
    }
    fn carrier(&self, event: &str, info: &BlobInfo) -> String {
        self.capture(event,&format!("Ω😀 {} {}",info.sha256,info.sha256),json!({"artifacts":[{"type":"file","artifact_status":"available","mime":"text/plain","filename":"source.txt","sha256":info.sha256}]}))
    }
    fn count(&self, table: &str) -> i64 {
        self.store
            .read(|tx| {
                Ok(tx
                    .sql()
                    .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?)
            })
            .unwrap()
    }
    fn state(&self, sha: &str) -> (String, Option<String>, Option<String>) {
        self.store
            .read(|tx| {
                Ok(tx.sql().query_row(
                    "SELECT state,job_id,error FROM captured_media WHERE sha256=?",
                    [sha],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )?)
            })
            .unwrap()
    }
}

#[test]
fn captured_discovery_backfills_64_at_a_time_using_core_parser() {
    let f = Fixture::new();
    let one = f.blob("body", false);
    let two = f.blob("metadata", false);
    let body=json!([{"type":"file","artifact_status":"available","mime":"text/plain","sha256":one.sha256}]).to_string();
    f.capture("0", &body, json!({}));
    for n in 1..64 {
        f.capture(&n.to_string(), "ordinary projected text", json!({}));
    }
    // A too-large body still carries an exact metadata stub; no JSON fragment is parsed.
    f.capture(
        "64",
        &"x".repeat(1024 * 1024 + 2),
        json!({"artifacts":[{"type":"file","artifact_status":"available","sha256":two.sha256}]}),
    );
    // Simulate imported history that predates synchronous capture-reference registration.
    f.store
        .write(|tx| {
            tx.sql()
                .execute_batch("DELETE FROM captured_media_refs; DELETE FROM captured_media;")?;
            Ok(())
        })
        .unwrap();
    assert!(f.captured.discover().unwrap());
    assert_eq!(f.count("captured_media_refs"), 1);
    assert_eq!(
        f.store
            .read(|tx| Ok(tx.sql().query_row(
                "SELECT seq FROM captured_media_progress",
                [],
                |r| r.get::<_, i64>(0)
            )?))
            .unwrap(),
        64
    );
    assert!(f.captured.discover().unwrap());
    assert_eq!(f.count("captured_media_refs"), 2);
    assert!(!f.captured.discover().unwrap());
    assert_eq!(f.state(&one.sha256).0, "waiting");
    assert_eq!(f.state(&two.sha256).0, "waiting");
}

#[test]
fn captured_skip_context_never_creates_attachment_authority() {
    let f = Fixture::new();
    let info = f.blob("context", true);
    let metadata = json!({"extraction_mode":"skip","artifacts":[{"type":"file","artifact_status":"available","sha256":info.sha256}]});
    f.capture("skip", "not human evidence", metadata);
    assert!(f.captured.step().unwrap());
    assert_eq!(f.count("captured_media"), 0);
    assert_eq!(f.count("jobs"), 0);
}

#[test]
fn captured_admission_caps_pending_and_leased_then_makes_progress() {
    let f = Fixture::new();
    for n in 0..3 {
        let b = f.blob(&format!("file{n}"), true);
        f.carrier(&format!("e{n}"), &b);
    }
    assert!(f.captured.admit().unwrap());
    assert_eq!(f.count("jobs"), 2);
    let job = f.queue.claim(&["media".into()], &[]).unwrap().unwrap();
    assert!(!f.captured.admit().unwrap());
    assert_eq!(f.count("jobs"), 2);
    f.queue
        .complete(&job, &json!({"control":"admission only"}))
        .unwrap();
    assert!(f.captured.admit().unwrap());
    assert_eq!(f.count("jobs"), 3);
    let outstanding:i64=f.store.read(|tx|Ok(tx.sql().query_row("SELECT count(*) FROM captured_media cm JOIN jobs j ON j.id=cm.job_id WHERE j.state IN ('pending','leased')",[],|r|r.get(0))?)).unwrap();
    assert_eq!(outstanding, 2);
}

#[test]
fn captured_exact_latest_reference_and_identity_retry_are_idempotent() {
    let f = Fixture::new();
    let b = f.blob("exact", true);
    f.carrier("old", &b);
    let newest = f.carrier("new", &b);
    assert!(f.captured.admit().unwrap());
    let id = f.state(&b.sha256).1.unwrap();
    let evidence: (String, i64, i64, String) = f
        .store
        .read(|tx| {
            Ok(tx.sql().query_row(
                "SELECT episode_id,start,end,quote FROM graph_evidence WHERE target=?",
                [&id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )?)
        })
        .unwrap();
    assert_eq!(evidence, (newest, 3, 67, b.sha256.clone()));
    f.store
        .write(|tx| {
            tx.sql().execute(
                "UPDATE captured_media SET state='ready',job_id=NULL WHERE sha256=?",
                [&b.sha256],
            )?;
            Ok(())
        })
        .unwrap();
    assert!(f.captured.admit().unwrap());
    assert_eq!(f.state(&b.sha256).1, Some(id));
    assert_eq!(f.count("jobs"), 1);
    assert_eq!(f.count("graph_evidence"), 1);
}

#[test]
fn captured_waits_for_upload_and_revives_source_failure_only_on_new_carrier() {
    let f = Fixture::new();
    let b = f.blob("late", false);
    f.carrier("first", &b);
    assert!(!f.captured.admit().unwrap());
    assert_eq!(f.state(&b.sha256).0, "waiting");
    f.register(&b);
    assert!(f.captured.admit().unwrap());
    let id = f.state(&b.sha256).1.unwrap();
    f.store
        .write(|tx| {
            tx.sql().execute(
                "UPDATE jobs SET state='failed',error='CapturedSourceUnavailable' WHERE id=?",
                [&id],
            )?;
            Ok(())
        })
        .unwrap();
    f.captured.discover().unwrap();
    assert_eq!(f.queue.get(&id).unwrap().unwrap()["state"], "failed");
    f.carrier("new-carrier", &b);
    assert_eq!(f.queue.get(&id).unwrap().unwrap()["state"], "pending");
    assert_eq!(f.count("jobs"), 1);
}

#[test]
fn captured_source_and_target_forgetting_fence_admission() {
    let f = Fixture::new();
    let b = f.blob("forgotten", true);
    f.carrier("first", &b);
    f.store.write(|tx|{tx.sql().execute("INSERT INTO forgotten_sources VALUES('native:pi:fixture','first','gone','control')",[])?;Ok(())}).unwrap();
    assert!(!f.captured.admit().unwrap());
    assert_eq!(
        f.state(&b.sha256),
        ("forgotten".into(), None, Some("forgotten_source".into()))
    );
    f.carrier("new-carrier", &b);
    assert_eq!(f.state(&b.sha256).0, "ready");
    f.store
        .write(|tx| {
            tx.sql().execute(
                "INSERT INTO media_files VALUES('old-target',?,'source.txt','document','control')",
                [&b.sha256],
            )?;
            tx.sql().execute(
                "INSERT INTO forgotten_targets VALUES('old-target','control','node')",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    assert!(!f.captured.admit().unwrap());
    assert_eq!(f.state(&b.sha256).2, Some("forgotten_target".into()));
    f.carrier("another-carrier", &b);
    assert_eq!(f.state(&b.sha256).0, "forgotten");
    assert_eq!(f.count("jobs"), 0);
}

#[test]
fn captured_model_signature_failure_rolls_back_admission() {
    let f = Fixture::new();
    let b = f.blob("model-pin", true);
    f.carrier("first", &b);
    f.store
        .write(|tx| {
            tx.sql()
                .execute("UPDATE vector_model SET revision='different'", [])?;
            Ok(())
        })
        .unwrap();
    assert!(f.captured.admit().is_err());
    assert_eq!(f.state(&b.sha256), ("ready".into(), None, None));
    assert_eq!(f.count("jobs"), 0);
    assert_eq!(f.count("graph_evidence"), 0);
}
