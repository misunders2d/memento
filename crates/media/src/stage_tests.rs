//! Transaction/lease controls only. Unit vectors are explicitly synthetic;
//! real model numerical acceptance is in the separately frozen8 raw fixtures.
use super::stage::*;
use memento_core::{Store, blobs::BlobInfo, capture::EpisodeInput};
use memento_domain::{
    Graph,
    jobs::{Job, JobFailure, Queue},
};
use serde_json::json;
use std::{io::Cursor, sync::Arc, time::Duration};
pub(crate) struct Fixture {
    _dir: tempfile::TempDir,
    pub(crate) store: Arc<Store>,
    pub(crate) queue: Queue,
    pub(crate) job: Job,
    pub(crate) identity: String,
    pub(crate) blob: BlobInfo,
}
impl Fixture {
    pub(crate) fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data")).unwrap();
        store.write(|tx|{tx.sql().execute("INSERT OR REPLACE INTO vector_model(singleton,model_id,revision,dimensions,text_encoding) VALUES (1,?,?,768,1)",rusqlite::params![memento_embedding::MODEL_ID,memento_embedding::MODEL_REVISION])?;Ok(())}).unwrap();
        let episode:EpisodeInput=serde_json::from_value(json!({"source":"media-test","event_id":"one","conversation_id":"thread","kind":"user","text":"Exact synthetic media evidence Ω😀","metadata":{}})).unwrap();
        let saved = store.capture(episode).unwrap();
        let blob = store
            .blobs()
            .put(&mut Cursor::new(b"unit0 unit1"), 100)
            .unwrap();
        store
            .write(|tx| {
                tx.register_blob(&blob, "text/plain")?;
                Ok(())
            })
            .unwrap();
        let queue = Queue::new(Graph::new(store.clone()));
        queue.submit_media(&json!({"command_id":"save-media","actor":"fixture","why":"source contract","sha256":blob.sha256,"kind":"document","filename":"source.txt","scope":["personal"],"evidence":[{"episode_id":saved.id,"quote":"Exact synthetic media evidence Ω😀"}]})).unwrap();
        let job = queue.claim(&["media".into()], &[]).unwrap().unwrap();
        let identity = representation(&store, "document", "source.txt").unwrap();
        Self {
            _dir: dir,
            store,
            queue,
            job,
            identity,
            blob,
        }
    }
    pub(crate) fn unit(&self, n: usize) -> Unit {
        let mut vector = [0f32; 768];
        vector[n] = 1.0;
        Unit {
            metadata: Metadata {
                modality: "text".into(),
                text: format!("unit{n}"),
                artifact_sha256: self.blob.sha256.clone(),
                start_s: None,
                end_s: None,
                page: Some(1),
                char_start: Some(n * 6),
                char_end: Some(n * 6 + 5),
                source_sha256: Some(self.blob.sha256.clone()),
            },
            vector,
            artifacts: vec![(self.blob.clone(), "text/plain".into())],
        }
    }
    pub(crate) fn count(&self, table: &str) -> i64 {
        self.store
            .read(|tx| {
                Ok(tx
                    .sql()
                    .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?)
            })
            .unwrap()
    }
}
#[test]
fn staged_units_resume_with_new_lease_then_publish_once_with_exact_vectors() {
    let f = Fixture::new();
    persist(&f.store, &f.job, &f.identity, 0, f.unit(0)).unwrap();
    assert_eq!(f.count("node_versions"), 0);
    assert_eq!(f.count("segments"), 0);
    assert_eq!(f.count("vector_blobs"), 0);
    f.queue
        .fail(
            &f.job,
            JobFailure {
                category: "InterruptedError",
                retryable: true,
                retry_after: Some(Duration::ZERO),
            },
        )
        .unwrap();
    let resumed = f.queue.claim(&["media".into()], &[]).unwrap().unwrap();
    assert_ne!(resumed.owner, f.job.owner);
    assert_eq!(cursor(&f.store, &resumed, &f.identity).unwrap(), 1);
    assert!(persist(&f.store, &f.job, &f.identity, 1, f.unit(1)).is_err());
    persist(&f.store, &resumed, &f.identity, 1, f.unit(1)).unwrap();
    let result = finish(&f.store, &resumed, &f.identity, 2).unwrap();
    assert_eq!(result["segments"].as_array().unwrap().len(), 2);
    assert_eq!(f.count("media_stage_units"), 0);
    assert_eq!(f.count("vector_blobs"), 2);
    let bytes: Vec<Vec<u8>> = f
        .store
        .read(|tx| {
            let mut q = tx
                .sql()
                .prepare("SELECT embedding FROM vector_blobs ORDER BY seq")?;
            Ok(q.query_map([], |r| r.get(0))?
                .collect::<std::result::Result<_, _>>()?)
        })
        .unwrap();
    assert_eq!(
        bytes[0],
        f.unit(0)
            .vector
            .iter()
            .flat_map(|x| x.to_le_bytes())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        bytes[1],
        f.unit(1)
            .vector
            .iter()
            .flat_map(|x| x.to_le_bytes())
            .collect::<Vec<_>>()
    );
    assert_eq!(previous(&f.store, &resumed).unwrap().unwrap(), result);
    f.queue.complete(&resumed, &result).unwrap();
    assert_eq!(f.queue.get(&resumed.id).unwrap().unwrap()["state"], "done");
}
#[test]
fn final_failure_rolls_back_graph_and_preserves_every_staged_unit() {
    let f = Fixture::new();
    for n in 0..2 {
        persist(&f.store, &f.job, &f.identity, n, f.unit(n)).unwrap();
    }
    f.store.write(|tx|{tx.sql().execute_batch("CREATE TEMP TRIGGER reject_second BEFORE INSERT ON segments WHEN NEW.text='unit1' BEGIN SELECT RAISE(ABORT,'injected'); END;")?;Ok(())}).unwrap();
    assert!(finish(&f.store, &f.job, &f.identity, 2).is_err());
    for table in [
        "node_versions",
        "segments",
        "vector_blobs",
        "media_representations",
    ] {
        assert_eq!(f.count(table), 0);
    }
    assert_eq!(f.count("media_stage_units"), 2);
    assert!(previous(&f.store, &f.job).unwrap().is_none());
    f.store
        .write(|tx| {
            tx.sql().execute_batch("DROP TRIGGER reject_second")?;
            Ok(())
        })
        .unwrap();
    finish(&f.store, &f.job, &f.identity, 2).unwrap();
    assert_eq!(f.count("segments"), 2);
    assert_eq!(f.count("media_stage_units"), 0);
}
#[test]
fn a_model_signature_change_cannot_publish_or_extend_old_stages() {
    let f = Fixture::new();
    persist(&f.store, &f.job, &f.identity, 0, f.unit(0)).unwrap();
    f.store
        .write(|tx| {
            tx.sql()
                .execute("UPDATE vector_model SET revision='different'", [])?;
            Ok(())
        })
        .unwrap();
    assert!(persist(&f.store, &f.job, &f.identity, 1, f.unit(1)).is_err());
    assert!(finish(&f.store, &f.job, &f.identity, 1).is_err());
    assert_eq!(f.count("media_stage_units"), 1);
    assert_eq!(f.count("node_versions"), 0);
    assert_eq!(f.count("vector_blobs"), 0);
}
