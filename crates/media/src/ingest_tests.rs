//! Real decoder/blob/job/transaction paths; synthetic encoder controls only.
use crate::{
    Result,
    ingest::{Encoder, MediaIngest},
    stage,
    stage_tests::Fixture,
};
use memento_domain::{
    Graph,
    jobs::{Handler, Job, JobFailure, Queue, Worker},
};
use memento_embedding::TextChunk;
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};
struct Synthetic {
    calls: AtomicUsize,
    wait: Mutex<Option<(mpsc::Sender<()>, Arc<Mutex<Option<Job>>>)>>,
}
impl Synthetic {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            wait: Mutex::new(None),
        }
    }
}
impl Encoder for Synthetic {
    fn chunk_text(&self, text: &str) -> Result<Vec<TextChunk>> {
        let mut offset = 0;
        Ok(text
            .split_whitespace()
            .map(|s| {
                let chunk = TextChunk {
                    text: s.into(),
                    start: offset,
                    end: offset + s.chars().count(),
                };
                offset = chunk.end + 1;
                chunk
            })
            .collect())
    }
    fn encode_document(&self, text: &str, _title: Option<&str>) -> Result<[f32; 768]> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some((ready, live)) = self.wait.lock().unwrap().take() {
            ready.send(()).unwrap();
            let start = Instant::now();
            while !live.lock().unwrap().as_ref().unwrap().stopping() {
                assert!(
                    start.elapsed() < Duration::from_secs(3),
                    "shutdown was not signalled"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        let mut vector = [0f32; 768];
        let n = text.strip_prefix("unit").unwrap().parse::<usize>().unwrap();
        vector[n] = 1.0;
        Ok(vector)
    }
    fn encode_rgb(&self, _w: usize, _h: usize, _rgb: &[u8], _budget: u32) -> Result<[f32; 768]> {
        panic!("not an image control")
    }
    fn encode_audio(&self, _s: &[f32], _rate: u32) -> Result<[f32; 768]> {
        panic!("not an audio control")
    }
}
#[test]
fn actual_handler_shutdown_finishes_current_unit_then_resumes_without_reencoding() {
    let f = Fixture::new();
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
    let model = Arc::new(Synthetic::new());
    let live = Arc::new(Mutex::new(None));
    let (tx, rx) = mpsc::channel();
    *model.wait.lock().unwrap() = Some((tx, live.clone()));
    let ingest = MediaIngest::for_test(Graph::new(f.store.clone()), model.clone());
    let observed = live.clone();
    let handler: Handler = Arc::new(move |job| {
        *observed.lock().unwrap() = Some(job.clone());
        ingest.handle(job)
    });
    let mut worker = Worker::new(
        f.queue.clone(),
        HashMap::from([("media".into(), handler)]),
        None,
    );
    worker.start().unwrap();
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
    worker.close();
    assert_eq!(f.queue.get(&f.job.id).unwrap().unwrap()["state"], "pending");
    assert_eq!(f.count("media_stage_units"), 1);
    assert_eq!(f.count("node_versions"), 0);
    let queue = Queue::new(Graph::new(f.store.clone()));
    let resumed = queue.claim(&["media".into()], &[]).unwrap().unwrap();
    let ingest = MediaIngest::for_test(Graph::new(f.store.clone()), model.clone());
    let result = ingest.handle(&resumed).unwrap();
    queue.complete(&resumed, &result).unwrap();
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(f.count("segments"), 2);
    assert_eq!(f.count("media_stage_units"), 0);
    assert_eq!(result["segments"][1]["char_start"], 6);
    assert_eq!(result["segments"][1]["char_end"], 11);
    assert_eq!(ingest.handle(&resumed).unwrap(), result);
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
}
#[test]
fn completed_compatible_representation_reuses_exact_units_without_encoder_calls() {
    let f = Fixture::new();
    let model = Arc::new(Synthetic::new());
    let ingest = MediaIngest::for_test(Graph::new(f.store.clone()), model.clone());
    let first = ingest.handle(&f.job).unwrap();
    f.queue.complete(&f.job, &first).unwrap();
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    let mut command = f.job.payload.clone();
    command["command_id"] = json!("second-media");
    command["scope"] = json!(["professional"]);
    f.queue.submit_media(&command).unwrap();
    let second = f.queue.claim(&["media".into()], &[]).unwrap().unwrap();
    let result = ingest.handle(&second).unwrap();
    assert_ne!(result["id"], first["id"]);
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(f.count("segments"), 4);
    assert_eq!(f.count("media_stage_units"), 0);
    let values = f
        .store
        .read(|tx| {
            let mut q = tx
                .sql()
                .prepare("SELECT embedding FROM vector_blobs ORDER BY seq")?;
            Ok(q.query_map([], |r| r.get::<_, Vec<u8>>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?)
        })
        .unwrap();
    assert_eq!(values[0], values[2]);
    assert_eq!(values[1], values[3]);
}
#[test]
fn captured_source_forgetting_blocks_more_stages_and_final_publication() {
    let f = Fixture::new();
    let mut payload = f.job.payload.clone();
    payload["captured"] = json!(true);
    payload["scope"] = json!([]);
    payload["evidence"] = json!([]);
    let episode = f.job.payload["evidence"][0]["episode_id"].as_str().unwrap();
    let id=f.store.write(|tx|{tx.sql().execute("INSERT INTO captured_media_refs(sha256,episode_id,start,end,quote) VALUES (?,?,0,35,?)",rusqlite::params![f.blob.sha256,episode,"Exact synthetic media evidence Ω😀"])?;Queue::insert(tx,"captured-control","media",&payload)}).unwrap();
    let captured = f.queue.claim(&["media".into()], &[]).unwrap().unwrap();
    assert_eq!(captured.id, id);
    stage::persist(&f.store, &captured, &f.identity, 0, f.unit(0)).unwrap();
    f.store.write(|tx|{tx.sql().execute("INSERT INTO forgotten_sources(source,event_id,target,commit_id) VALUES ('media-test','one','control-target','control-commit')",[])?;Ok(())}).unwrap();
    assert!(stage::persist(&f.store, &captured, &f.identity, 1, f.unit(1)).is_err());
    assert!(stage::finish(&f.store, &captured, &f.identity, 1).is_err());
    let model = Arc::new(Synthetic::new());
    let ingest = MediaIngest::for_test(Graph::new(f.store.clone()), model.clone());
    let failure = ingest.handle(&captured).unwrap_err();
    assert_eq!(failure.category, "CapturedSourceUnavailable");
    f.queue.fail(&captured, failure).unwrap();
    assert_eq!(f.count("media_stage_units"), 1);
    assert_eq!(f.count("node_versions"), 0);
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert!(f.store.blobs().reader(&f.blob.sha256).is_ok());
}
#[test]
fn expired_unchanged_owner_returns_pending_without_publishing() {
    let f = Fixture::new();
    f.store
        .write(|tx| {
            tx.sql()
                .execute("UPDATE jobs SET lease_until=0 WHERE id=?", [&f.job.id])?;
            Ok(())
        })
        .unwrap();
    let model = Arc::new(Synthetic::new());
    let ingest = MediaIngest::for_test(Graph::new(f.store.clone()), model.clone());
    let failure = ingest.handle(&f.job).unwrap_err();
    assert_eq!(failure.category, "ConflictError");
    assert!(failure.retryable);
    assert_eq!(failure.retry_after, Some(Duration::ZERO));
    f.queue.fail(&f.job, failure).unwrap();
    assert_eq!(f.queue.get(&f.job.id).unwrap().unwrap()["state"], "pending");
    assert_eq!(f.count("node_versions"), 0);
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
}
#[test]
fn stale_owner_failure_cannot_release_a_reassigned_job() {
    let f = Fixture::new();
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
    let current = f.queue.claim(&["media".into()], &[]).unwrap().unwrap();
    let model = Arc::new(Synthetic::new());
    let ingest = MediaIngest::for_test(Graph::new(f.store.clone()), model.clone());
    let failure = ingest.handle(&f.job).unwrap_err();
    assert!(failure.retryable);
    f.queue.fail(&f.job, failure).unwrap();
    let owner: String = f
        .store
        .read(|tx| {
            Ok(tx
                .sql()
                .query_row("SELECT owner FROM jobs WHERE id=?", [&f.job.id], |r| {
                    r.get(0)
                })?)
        })
        .unwrap();
    assert_eq!(owner, current.owner);
    assert_eq!(f.queue.get(&f.job.id).unwrap().unwrap()["state"], "leased");
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
}
#[test]
fn undecodable_original_is_retained_and_never_reported_complete() {
    let f = Fixture::new();
    let binary = f
        .store
        .blobs()
        .put(&mut std::io::Cursor::new(b"wrong\0binary"), 100)
        .unwrap();
    f.store
        .write(|tx| {
            tx.register_blob(&binary, "text/plain")?;
            Ok(())
        })
        .unwrap();
    let mut command = f.job.payload.clone();
    command["command_id"] = json!("bad-source");
    command["sha256"] = json!(binary.sha256);
    f.queue.submit_media(&command).unwrap();
    let job = f.queue.claim(&["media".into()], &[]).unwrap().unwrap();
    let model = Arc::new(Synthetic::new());
    let ingest = MediaIngest::for_test(Graph::new(f.store.clone()), model.clone());
    let failure = ingest.handle(&job).unwrap_err();
    assert_eq!(failure.category, "ValueError");
    assert!(!failure.retryable);
    f.queue.fail(&job, failure).unwrap();
    assert_eq!(f.queue.get(&job.id).unwrap().unwrap()["state"], "failed");
    assert!(f.store.blobs().reader(&binary.sha256).is_ok());
    assert_eq!(f.count("node_versions"), 0);
    assert_eq!(f.count("media_stage_units"), 0);
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
}
