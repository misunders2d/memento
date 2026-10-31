use super::*;
use crate::{Recall, RecallRequest, router};
use axum::{body::{Body, to_bytes}, http::{Request, StatusCode}};
use std::{
    sync::{Condvar, Mutex, atomic::AtomicUsize},
    time::{Duration, Instant},
};
use tower::ServiceExt;

#[derive(Clone)]
struct Gate(Arc<(Mutex<bool>, Condvar)>);
impl Gate {
    fn new() -> Self { Self(Arc::new((Mutex::new(false), Condvar::new()))) }
    fn wait(&self) {
        let (lock, changed) = &*self.0;
        let _guard = changed.wait_while(lock.lock().unwrap(), |released| !*released).unwrap();
    }
    fn release(&self) {
        let (lock, changed) = &*self.0;
        *lock.lock().unwrap() = true;
        changed.notify_all();
    }
}
struct ReleaseOnDrop(Gate);
impl Drop for ReleaseOnDrop { fn drop(&mut self) { self.0.release(); } }
struct StubRecall;
impl Recall for StubRecall {
    fn recall(&self, _: &RecallRequest, _: Instant) -> Result<Vec<Value>> {
        Ok(vec![json!({"id":"status-admission-fixture"})])
    }
    fn ready(&self) -> bool { true }
}
async fn request(state: AppState, path: &'static str) -> (StatusCode, Value) {
    let response = router(state).oneshot(Request::get(path)
        .header("host","127.0.0.1:8765").body(Body::empty()).unwrap()).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 2 * 1024 * 1024).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

#[test]
fn canceled_status_keeps_admission_without_starving_health_or_recall() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2).max_blocking_threads(2).enable_all().build().unwrap();
    runtime.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let store = memento_core::Store::open(dir.path().join("data")).unwrap();
        let mut state = AppState::new(store).unwrap();
        state.recall = Some(Arc::new(StubRecall));
        let gate = Gate::new();
        let _release_on_failure = ReleaseOnDrop(gate.clone());
        let began = Arc::new(AtomicUsize::new(0));
        let (entered, started) = tokio::sync::oneshot::channel();
        let held = gate.clone();
        let counter = began.clone();
        let first = tokio::spawn(admitted_blocking(state.status_admission.clone(), move || {
            counter.fetch_add(1, Ordering::SeqCst);
            let _ = entered.send(());
            held.wait();
            Ok(())
        }));
        tokio::time::timeout(Duration::from_secs(2), started).await.unwrap().unwrap();
        first.abort();
        assert!(first.await.is_err());

        let mut waiting = Vec::new();
        for _ in 0..40 {
            let held = gate.clone();
            let counter = began.clone();
            waiting.push(tokio::spawn(admitted_blocking(state.status_admission.clone(), move || {
                counter.fetch_add(1, Ordering::SeqCst);
                held.wait();
                Ok(())
            })));
        }
        // The actual HTTP status route must use the same admission gate.
        let mut queued_http = tokio::spawn(request(state.clone(), "/status"));
        assert!(tokio::time::timeout(Duration::from_millis(100), &mut queued_http).await.is_err());
        let (health, recall) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(request(state.clone(), "/health"), request(state.clone(), "/recall?q=fixture"))
        }).await.expect("status waiters must leave the second blocking worker free");
        assert_eq!(health.0, StatusCode::OK);
        assert_eq!(recall.0, StatusCode::OK);
        assert_eq!(recall.1["memories"][0]["id"], "status-admission-fixture");
        assert_eq!(began.load(Ordering::SeqCst), 1);

        queued_http.abort();
        assert!(queued_http.await.is_err());
        for task in &waiting { task.abort(); }
        for task in waiting { assert!(task.await.is_err()); }
        assert_eq!(began.load(Ordering::SeqCst), 1);
        gate.release();
        let permit = tokio::time::timeout(Duration::from_secs(2), state.status_admission.clone().acquire_owned()).await.unwrap().unwrap();
        drop(permit);
        state.store.capture(serde_json::from_value(json!({"source":"synthetic","event_id":"after-admission","conversation_id":"fixture","kind":"user","text":"Fresh count after admission","metadata":{}})).unwrap()).unwrap();
        let fresh = tokio::time::timeout(Duration::from_secs(2), request(state.clone(), "/status")).await.unwrap();
        assert_eq!(fresh.0, StatusCode::OK);
        assert_eq!(fresh.1["captured"]["episodes"], 1);
        assert_eq!(state.status_admission.available_permits(), 1);
    });
}

#[tokio::test]
async fn status_admission_returns_after_sql_error_or_worker_panic() {
    let admission = Arc::new(Semaphore::new(1));
    assert!(admitted_blocking::<()>(admission.clone(), || Err(Error::Unavailable("Fixture SQL error"))).await.is_err());
    assert_eq!(admission.available_permits(), 1);
    assert!(admitted_blocking::<()>(admission.clone(), || panic!("Fixture worker panic")).await.is_err());
    assert_eq!(admission.available_permits(), 1);
    let result = admitted_blocking(admission.clone(), || Ok(42)).await.unwrap_or_else(|_| panic!("admission must remain usable"));
    assert_eq!(result, 42);
    assert_eq!(admission.available_permits(), 1);
}

#[test]
fn busy_viewer_reads_queue_without_starving_recall() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2).max_blocking_threads(3).enable_all().build().unwrap();
    runtime.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let store = memento_core::Store::open(dir.path().join("data")).unwrap();
        let mut state = AppState::new(store).unwrap();
        state.recall = Some(Arc::new(StubRecall));
        let gate = Gate::new();
        let _release_on_failure = ReleaseOnDrop(gate.clone());
        let mut held = Vec::new();
        for _ in 0..2 {
            let (entered, started) = tokio::sync::oneshot::channel();
            let wait = gate.clone();
            held.push(tokio::spawn(admitted_blocking(state.viewer_admission.clone(), move || {
                let _ = entered.send(());
                wait.wait();
                Ok(())
            })));
            tokio::time::timeout(Duration::from_secs(2), started).await.unwrap().unwrap();
        }
        // Two viewer reads hold both permits; further UI requests wait without a worker.
        let mut graphs = (0..8).map(|_| tokio::spawn(request(state.clone(), "/ui/api/graph"))).collect::<Vec<_>>();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(graphs.iter().all(|task| !task.is_finished()));
        let (health, recall) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(request(state.clone(), "/health"), request(state.clone(), "/recall?q=fixture"))
        }).await.expect("queued viewer reads must leave the third blocking worker free");
        assert_eq!(health.0, StatusCode::OK);
        assert_eq!(recall.0, StatusCode::OK);
        // Stale client requests are dropped before they ever reach SQL.
        for task in graphs.drain(1..) { task.abort(); }
        gate.release();
        for task in held { assert!(task.await.unwrap().is_ok()); }
        let graph = tokio::time::timeout(Duration::from_secs(2), graphs.remove(0)).await.unwrap().unwrap();
        assert_eq!(graph.0, StatusCode::OK);
        assert!(graph.1["nodes"].is_array());
        assert_eq!(state.viewer_admission.available_permits(), 2);
    });
}
