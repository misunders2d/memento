//! One native service owner. Capture/health remain independent of model/index startup.
use crate::{AppState, Recall, RecallRequest, indexing::Indexer, recall_timings::RecallTimings, retrieval::Retrieval};
use memento_ann::{Projection, discovery::Discovery};
use memento_core::{Error, ModelSignature, Result, Store, Tier};
use memento_domain::{
    consolidation::DiscoveryProvider,
    jobs::{Handler, Worker},
    processing::Processing,
};
use memento_embedding::{EmbeddingModel, ModelConfig};
use memento_maintenance::{
    cold::ColdManager,
    cold_runtime::{ColdPolicy, ColdRuntime},
    drive::DriveColdStore,
    drive_auth::{self, DriveAuthorization},
    transport::NativeTransport,
};
use memento_owner::owner::{AdminServices, OwnerServer};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
#[derive(Clone)]
pub struct Config {
    pub data_dir: PathBuf,
    pub model: Option<ModelConfig>,
    pub bootstrap_assets: bool,
    pub text_reranker_dir: Option<PathBuf>,
    pub text_scorer_threads: usize,
    pub pdfium: Option<PathBuf>,
    pub allowed_hosts: Vec<String>,
    pub compact_executable: PathBuf,
    pub cold_policy: ColdPolicy,
    pub cold_max_importance: f64,
}
struct DeferredRecall(RwLock<Option<Arc<Retrieval>>>);
impl DeferredRecall {
    fn current(&self) -> Result<Arc<Retrieval>> {
        self.0
            .read()
            .map_err(|_| Error::Unavailable("Native retrieval is unavailable"))?
            .clone()
            .ok_or(Error::Unavailable("Native model and index are not ready"))
    }
}
impl Recall for DeferredRecall {
    fn recall(&self, request: &RecallRequest, deadline: Instant) -> Result<Vec<Value>> {
        self.current()?.recall(request, deadline)
    }
    fn recall_timed(
        &self,
        request: &RecallRequest,
        deadline: Instant,
        timings: &RecallTimings,
    ) -> Result<Vec<Value>> {
        self.current()?.recall_timed(request, deadline, timings)
    }
    fn ready(&self) -> bool {
        self.0
            .read()
            .is_ok_and(|r| r.as_ref().is_some_and(|v| v.ready()))
    }
}
struct DeferredDiscovery(Arc<RwLock<Option<Arc<Projection>>>>);
impl DiscoveryProvider for DeferredDiscovery {
    fn discover(&self, ids: &[String]) -> Result<Value> {
        match self
            .0
            .read()
            .map_err(|_| Error::Unavailable("Discovery state is unavailable"))?
            .clone()
        {
            Some(p) => Discovery::new(p).discover(ids),
            None => Ok(json!({"candidates":{},"pending":true})),
        }
    }
}
pub struct Services {
    pub cold: RwLock<Option<Arc<ColdManager>>>,
    cold_runtime: Mutex<Option<Arc<ColdRuntime>>>,
    install: Mutex<()>,
    pub drive_authorization: RwLock<Option<Arc<DriveAuthorization>>>,
    pub reauthorization: AtomicBool,
    pub startup_error: Mutex<Option<&'static str>>,
    pub index_error: AtomicBool,
    stop: Arc<AtomicBool>,
    projection: Arc<RwLock<Option<Arc<Projection>>>>,
    worker: Mutex<Option<Worker>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
}
impl Services {
    pub fn cold(&self) -> Option<Arc<ColdManager>> {
        self.cold.read().ok().and_then(|v| v.clone())
    }
    pub fn projection(&self) -> Option<Arc<Projection>> {
        self.projection.read().ok().and_then(|v| v.clone())
    }
    pub fn cold_status(&self) -> Result<Option<Value>> {
        self.cold_runtime
            .lock()
            .map_err(|_| Error::Unavailable("Cold runtime is unavailable"))?
            .as_ref()
            .map(|r| Ok(serde_json::to_value(r.status()?)?))
            .transpose()
    }
    /// Publish cancellation without joining workers or waiting for HTTP drain.
    pub fn request_stop(&self) {
        self.stop.store(true, Ordering::Release);
    }
    pub fn close(&self) {
        self.request_stop();
        if let Ok(auth) = self.drive_authorization.read() {
            if let Some(auth) = auth.as_ref() {
                auth.close();
            }
        }
        // Never hold the thread registry while joining: startup may register the cold worker.
        loop {
            let handles = self
                .threads
                .lock()
                .map(|mut t| std::mem::take(&mut *t))
                .unwrap_or_default();
            if handles.is_empty() {
                break;
            }
            for handle in handles {
                let _ = handle.join();
            }
        }
        if let Ok(mut worker) = self.worker.lock() {
            if let Some(worker) = worker.as_mut() {
                worker.close();
            }
            worker.take();
        }
        if let Ok(runtime) = self.cold_runtime.lock() {
            if let Some(runtime) = runtime.as_ref() {
                let _ = runtime.close();
            }
        }
    }
}
pub struct Application {
    pub state: AppState,
    owner: Option<OwnerServer>,
    pub services: Arc<Services>,
}
impl Application {
    pub fn start(config: Config) -> Result<Self> {
        let store = Store::open(&config.data_dir)?;
        let mut state = AppState::new(store.clone())?;
        let services = Arc::new(Services {
            cold: RwLock::new(None),
            cold_runtime: Mutex::new(None),
            install: Mutex::new(()),
            drive_authorization: RwLock::new(None),
            reauthorization: AtomicBool::new(false),
            startup_error: Mutex::new(None),
            index_error: AtomicBool::new(false),
            stop: Arc::new(AtomicBool::new(false)),
            projection: Arc::new(RwLock::new(None)),
            worker: Mutex::new(None),
            threads: Mutex::new(Vec::new()),
        });
        state.services = Some(services.clone());
        state.allowed_hosts = Arc::new(config.allowed_hosts.clone());
        let transport = Arc::new(NativeTransport::new());
        if config
            .data_dir
            .join("drive-oauth.json")
            .symlink_metadata()
            .is_ok()
        {
            match DriveColdStore::from_file(config.data_dir.clone(), transport.clone()) {
                Ok(backend) => {
                    let mut manager = ColdManager::new(store.clone(), backend)?;
                    Arc::get_mut(&mut manager)
                        .ok_or(Error::Unavailable("Cold configuration is unavailable"))?
                        .max_importance = config.cold_max_importance;
                    let runtime = ColdRuntime::new(manager.clone(), config.cold_policy.clone())?;
                    runtime.start()?;
                    *services
                        .cold
                        .write()
                        .map_err(|_| Error::Unavailable("Cold state is unavailable"))? =
                        Some(manager);
                    *services
                        .cold_runtime
                        .lock()
                        .map_err(|_| Error::Unavailable("Cold state is unavailable"))? =
                        Some(runtime);
                }
                Err(e) if drive_auth::requires_reauthorization(&e) => {
                    services.reauthorization.store(true, Ordering::Release)
                }
                Err(e) => return Err(e),
            }
        }
        let weak = Arc::downgrade(&services);
        let data = config.data_dir.clone();
        let policy = config.cold_policy.clone();
        let max_importance = config.cold_max_importance;
        let install_store = store.clone();
        let install = Arc::new(
            move |backend: Arc<DriveColdStore>,
                  grant: drive_auth::ConnectionGrant,
                  reconnect: bool|
                  -> Result<()> {
                let services = weak
                    .upgrade()
                    .ok_or(Error::Unavailable("Service is shutting down"))?;
                let _gate = services
                    .install
                    .lock()
                    .map_err(|_| Error::Unavailable("Drive installation is unavailable"))?;
                if services.stop.load(Ordering::Acquire) {
                    return Err(Error::Unavailable("Service is shutting down"));
                }
                if services.cold().is_some() && !reconnect {
                    return Err(Error::Conflict("Drive is already connected"));
                }
                let mut manager = ColdManager::new(install_store.clone(), backend)?;
                Arc::get_mut(&mut manager)
                    .ok_or(Error::Unavailable("Cold configuration is unavailable"))?
                    .max_importance = max_importance;
                let runtime = ColdRuntime::new(manager.clone(), policy.clone())?;
                // Complete fallible startup before publishing the replacement grant. A failed
                // publication closes the prepared worker and leaves the previous backend intact.
                runtime.start()?;
                if let Err(error) = grant.publish(&data) {
                    let _ = runtime.close();
                    return Err(error);
                }
                let previous = services
                    .cold_runtime
                    .lock()
                    .map_err(|_| Error::Unavailable("Cold state is unavailable"))?
                    .replace(runtime);
                *services
                    .cold
                    .write()
                    .map_err(|_| Error::Unavailable("Cold state is unavailable"))? = Some(manager);
                services.reauthorization.store(false, Ordering::Release);
                if let Some(previous) = previous {
                    let _ = previous.close();
                }
                Ok(())
            },
        );
        let auth = Arc::new(DriveAuthorization::new(
            config.data_dir.clone(),
            transport,
            install,
        ));
        *services
            .drive_authorization
            .write()
            .map_err(|_| Error::Unavailable("Drive state is unavailable"))? = Some(auth);
        let enabled = config.model.is_some();
        state.media_enabled = enabled;
        let deferred = Arc::new(DeferredRecall(RwLock::new(None)));
        if enabled {
            state.recall = Some(deferred.clone());
            state.processing = Processing::new(state.graph.clone())
                .with_discovery(Arc::new(DeferredDiscovery(services.projection.clone())));
        }
        let weak = Arc::downgrade(&services);
        let compact_store = store.clone();
        let helper = config.compact_executable.clone();
        let admin = AdminServices::new(
            state.graph.clone(),
            Arc::new(state.processing.clone()),
            Arc::new(move || weak.upgrade().and_then(|s| s.cold())),
            Arc::new(move || compact_store.compact_with_helper(&helper)),
        )?;
        let owner = Some(OwnerServer::start(admin)?);
        if let Some(model_config) = config.model {
            let background_services = services.clone();
            let graph = state.graph.clone();
            let queue = state.processing.queue.clone();
            let pdfium = config.pdfium;
            let thread = thread::Builder::new()
                .name("memento-startup".into())
                .spawn(move || {
                    let start = (|| -> Result<()> {
                        if config.bootstrap_assets {
                            crate::bootstrap::prepare(
                                &NativeTransport::new(),
                                &model_config.model_dir,
                                &background_services.stop,
                            )?;
                        }
                        let runtime_library = model_config.runtime_library.clone();
                        let model = EmbeddingModel::load(model_config).map_err(|_| {
                            Error::Unavailable("Pinned multimodal model failed to load")
                        })?;
                        if background_services.stop.load(Ordering::Acquire) {
                            return Ok(());
                        }
                        let text_reranker_root =
                            config.text_reranker_dir.clone().unwrap_or_else(|| {
                                config
                                    .data_dir
                                    .join("models")
                                    .join("text-reranker")
                                    .join(crate::text_reranker::MODEL_REVISION)
                            });
                        if config.text_reranker_dir.is_none() {
                            crate::text_reranker::prepare(
                                &NativeTransport::new(),
                                &text_reranker_root,
                                &background_services.stop,
                            )?;
                        }
                        let text_reranker = Arc::new(crate::text_reranker::TextReranker::load(
                            &text_reranker_root,
                            &runtime_library,
                            config.text_scorer_threads,
                        )?);
                        if background_services.stop.load(Ordering::Acquire) {
                            return Ok(());
                        }
                        crate::legacy_text::upgrade(&store, &model, &background_services.stop)?;
                        let projection = Projection::open(store.clone(), ModelSignature::pinned())?;
                        *background_services
                            .projection
                            .write()
                            .map_err(|_| Error::Unavailable("Search state is unavailable"))? =
                            Some(projection.clone());
                        // Initial HOT build gates background encoding only. Capture/health are already serving.
                        rebuild_until_ready(&projection, Tier::Hot, &background_services.stop)?;
                        if background_services.stop.load(Ordering::Acquire) {
                            return Ok(());
                        }
                        let retrieval = Arc::new(Retrieval::new(
                            store.clone(),
                            projection.clone(),
                            model.clone(),
                            text_reranker,
                        ));
                        *deferred
                            .0
                            .write()
                            .map_err(|_| Error::Unavailable("Search state is unavailable"))? =
                            Some(retrieval);
                        let indexer = Indexer::new(
                            store.clone(),
                            projection.clone(),
                            model.clone(),
                            background_services.stop.clone(),
                        );
                        let mut handlers = HashMap::<String, Handler>::new();
                        if let Some(pdfium) = pdfium {
                            let weak = Arc::downgrade(&background_services);
                            let local = store.blobs().clone();
                            let ingest =
                                memento_media::ingest::MediaIngest::new(graph, model, pdfium)
                                    .with_reader(Arc::new(move |sha| {
                                        match weak.upgrade().and_then(|s| s.cold()) {
                                            Some(cold) => cold.reader(sha),
                                            None => local.reader(sha),
                                        }
                                    }));
                            handlers.insert("media".into(), ingest.handler());
                        }
                        let maintain_store = store.clone();
                        let captured = memento_media::captured::CapturedMedia::new(queue.clone());
                        let status = Arc::downgrade(&background_services);
                        let maintain = Arc::new(move || {
                            let raw = maintain_store.compress_raw_step();
                            let indexed = indexer.run_once();
                            let attachments = captured.step();
                            if let Err(error) = &attachments {
                                eprintln!(
                                    "Captured media maintenance failed ({}); data retained",
                                    memento_domain::jobs::error_category(error)
                                );
                            }
                            if let Some(status) = status.upgrade() {
                                status
                                    .index_error
                                    .store(indexed.is_err(), Ordering::Release);
                            }
                            if let Err(error) = &raw {
                                eprintln!(
                                    "Raw storage compression failed ({}); data retained",
                                    memento_domain::jobs::error_category(error)
                                );
                            }
                            Ok(indexed? || raw.unwrap_or(false) || attachments.unwrap_or(false))
                        });
                        let mut worker = Worker::new(queue, handlers, Some(maintain));
                        worker.start()?;
                        *background_services.worker.lock().map_err(|_| {
                            Error::Unavailable("Background worker is unavailable")
                        })? = Some(worker);
                        let cold_projection = projection.clone();
                        let stop = background_services.stop.clone();
                        let cold_thread = thread::Builder::new()
                            .name("memento-archive".into())
                            .spawn(move || {
                                let _ = rebuild_until_ready(&cold_projection, Tier::Cold, &stop);
                                while !stop.load(Ordering::Acquire) {
                                    if cold_projection.synchronize(Tier::Cold).is_err() {
                                        let _ = rebuild_until_ready(
                                            &cold_projection,
                                            Tier::Cold,
                                            &stop,
                                        );
                                    }
                                    pause(&stop, Duration::from_millis(250));
                                }
                            })?;
                        background_services
                            .threads
                            .lock()
                            .map_err(|_| Error::Unavailable("Search lifecycle is unavailable"))?
                            .push(cold_thread);
                        while !background_services.stop.load(Ordering::Acquire) {
                            if projection.synchronize(Tier::Hot).is_err() {
                                rebuild_until_ready(
                                    &projection,
                                    Tier::Hot,
                                    &background_services.stop,
                                )?;
                            }
                            pause(&background_services.stop, Duration::from_millis(100));
                        }
                        Ok(())
                    })();
                    if let Err(error) = start {
                        if !background_services.stop.load(Ordering::Acquire) {
                            eprintln!(
                                "Native startup failed ({})",
                                memento_domain::jobs::error_category(&error)
                            );
                            if let Ok(mut status) = background_services.startup_error.lock() {
                                *status = Some(match error {
                                    Error::Unavailable(message) | Error::Validation(message) => {
                                        message
                                    }
                                    _ => "Native model or search startup failed",
                                });
                            }
                        }
                    }
                })?;
            services
                .threads
                .lock()
                .map_err(|_| Error::Unavailable("Service lifecycle is unavailable"))?
                .push(thread);
        } else {
            let maintenance_store = store.clone();
            let mut worker = Worker::new(
                state.processing.queue.clone(),
                HashMap::new(),
                Some(Arc::new(move || maintenance_store.compress_raw_step())),
            );
            worker.start()?;
            *services
                .worker
                .lock()
                .map_err(|_| Error::Unavailable("Background worker is unavailable"))? =
                Some(worker);
        }
        Ok(Self {
            state,
            owner,
            services,
        })
    }
    pub fn close(&mut self) {
        self.state.processing.queue.request_stop();
        self.services.close();
        if let Some(owner) = self.owner.take() {
            if owner.close().is_err() {
                eprintln!("Owner socket cleanup failed");
            }
        }
    }
}
impl Drop for Application {
    fn drop(&mut self) {
        self.close();
    }
}
fn pause(stop: &AtomicBool, duration: Duration) {
    let until = Instant::now() + duration;
    while !stop.load(Ordering::Acquire) && Instant::now() < until {
        thread::sleep(Duration::from_millis(20));
    }
}
fn rebuild_until_ready(projection: &Projection, tier: Tier, stop: &AtomicBool) -> Result<()> {
    while !stop.load(Ordering::Acquire) {
        match projection.synchronize(tier) {
            Ok(true) => return Ok(()),
            // A valid bounded catch-up has more events, not a damaged index.
            // Keep draining the existing shard and check stop before every step.
            Ok(false) => continue,
            Err(_) => {}
        }
        match projection.rebuild(tier, stop) {
            Ok(()) => return Ok(()),
            Err(_) if !stop.load(Ordering::Acquire) => pause(stop, Duration::from_secs(1)),
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(test)]
mod startup_tests {
    use super::*;
    use memento_core::{F32Blob, VectorKind, VectorWrite};
    use std::os::unix::fs::MetadataExt;
    #[test]
    fn healthy_large_dirty_cut_catches_up_without_replacing_the_hot_index() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data")).unwrap();
        let projection = Projection::open(store.clone(), ModelSignature::pinned()).unwrap();
        let mut vector = [0.; 768];
        vector[0] = 1.;
        store.write(|tx|{
            tx.sql().execute("INSERT INTO graph_commits VALUES('test','test','{}','{}','owner','Synthetic startup catch-up','2026-10-26T00:00:00Z')",[])?;
            tx.sql().execute("INSERT INTO node_versions VALUES('node',1,'test','knowledge','A stable synthetic remembered fact','active',NULL,NULL,NULL,0,'[\"personal\"]')",[])?;
            tx.put_vector(&VectorWrite{seq:0,target:"node".into(),kind:VectorKind::Node,version:1,current:true,tier:Tier::Hot,embedding:F32Blob::from_values(&vector)?})?;Ok(())
        }).unwrap();
        let stop = AtomicBool::new(false);
        projection.rebuild(Tier::Hot, &stop).unwrap();
        let hot = store.data_dir().join("search/hot");
        let inode = std::fs::metadata(&hot).unwrap().ino();
        let original: Value =
            serde_json::from_slice(&std::fs::read(hot.join("checkpoint.json")).unwrap()).unwrap();
        let before_bytes = store
            .read(|tx| {
                Ok(tx.sql().query_row(
                    "SELECT embedding FROM vector_blobs WHERE seq=0",
                    [],
                    |r| r.get::<_, Vec<u8>>(0),
                )?)
            })
            .unwrap();
        store
            .write(|tx| {
                for _ in 0..600 {
                    tx.touch_vector(0, Tier::Hot)?;
                }
                Ok(())
            })
            .unwrap();
        let cut = store
            .read(|tx| {
                assert_eq!(
                    tx.sql().query_row(
                        "SELECT count(*) FROM projection_events WHERE tier='hot'",
                        [],
                        |r| r.get::<_, i64>(0)
                    )?,
                    600
                );
                tx.fence(Tier::Hot)
            })
            .unwrap();
        rebuild_until_ready(&projection, Tier::Hot, &stop).unwrap();
        assert_eq!(std::fs::metadata(&hot).unwrap().ino(), inode);
        let ready: Value =
            serde_json::from_slice(&std::fs::read(hot.join("checkpoint.json")).unwrap()).unwrap();
        assert_eq!(ready["db_uuid"], original["db_uuid"]);
        assert_eq!(ready["signature"], original["signature"]);
        assert_eq!(ready["cursor"]["id"], cut.id);
        assert!(ready["pending"].is_null());
        assert_eq!(ready["building"], false);
        store
            .read(|tx| {
                assert_eq!(
                    tx.sql().query_row(
                        "SELECT count(*) FROM projection_events WHERE tier='hot'",
                        [],
                        |r| r.get::<_, i64>(0)
                    )?,
                    0
                );
                assert_eq!(
                    tx.sql().query_row(
                        "SELECT embedding FROM vector_blobs WHERE seq=0",
                        [],
                        |r| r.get::<_, Vec<u8>>(0)
                    )?,
                    before_bytes
                );
                Ok(())
            })
            .unwrap();
        let hits = projection
            .snapshot(true, false, |_, view| {
                view.search(
                    Tier::Hot,
                    &vector,
                    1,
                    "node",
                    memento_ann::Pool::Ordinary,
                    None,
                    None,
                )
            })
            .unwrap();
        assert_eq!(hits[0].target, "node");
    }
}
