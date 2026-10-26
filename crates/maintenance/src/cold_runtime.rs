//! One bounded cold worker. It never creates or retries owner backups.
use crate::cold::{ColdManager, Sweep};
use memento_core::{Error, Result};
use serde::Serialize;
use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
#[derive(Clone)]
pub struct ColdPolicy {
    pub after_days: u64,
    pub min_bytes: u64,
    pub cache_bytes: u64,
    pub cache_ttl_seconds: u64,
    pub batch_limit: usize,
}
impl Default for ColdPolicy {
    fn default() -> Self {
        Self {
            after_days: 90,
            min_bytes: 10 * 1024 * 1024,
            cache_bytes: 1024 * 1024 * 1024,
            cache_ttl_seconds: 3600,
            batch_limit: 10,
        }
    }
}
#[derive(Clone, Serialize)]
pub struct Status {
    pub last_error: Option<&'static str>,
    pub sweep_failure_count: u64,
}
struct State {
    status: Status,
    next: Instant,
}
pub struct ColdRuntime {
    pub manager: Arc<ColdManager>,
    policy: ColdPolicy,
    state: Mutex<State>,
    stopping: AtomicBool,
    wake: (Mutex<()>, Condvar),
    thread: Mutex<Option<JoinHandle<()>>>,
}
impl ColdRuntime {
    pub fn new(manager: Arc<ColdManager>, policy: ColdPolicy) -> Result<Arc<Self>> {
        if policy.after_days == 0
            || policy.min_bytes == 0
            || policy.batch_limit == 0
            || policy.batch_limit > 100
        {
            return Err(Error::Validation("Invalid cold retention schedule"));
        }
        Ok(Arc::new(Self {
            manager,
            policy,
            state: Mutex::new(State {
                status: Status {
                    last_error: None,
                    sweep_failure_count: 0,
                },
                next: Instant::now(),
            }),
            stopping: AtomicBool::new(false),
            wake: (Mutex::new(()), Condvar::new()),
            thread: Mutex::new(None),
        }))
    }
    pub fn status(&self) -> Result<Status> {
        Ok(self
            .state
            .lock()
            .map_err(|_| Error::Unavailable("Cold worker state is unavailable"))?
            .status
            .clone())
    }
    pub fn run_once(&self) -> Result<bool> {
        if self.stopping.load(Ordering::Acquire) {
            return Ok(false);
        }
        {
            let state = self
                .state
                .lock()
                .map_err(|_| Error::Unavailable("Cold worker state is unavailable"))?;
            if Instant::now() < state.next {
                return Ok(false);
            }
        }
        let outcome = (|| {
            let result = self.manager.sweep(
                self.policy.after_days,
                self.policy.min_bytes,
                self.policy.batch_limit,
            )?;
            self.manager
                .trim_cache(self.policy.cache_bytes, self.policy.cache_ttl_seconds)?;
            Ok::<Sweep, Error>(result)
        })();
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Unavailable("Cold worker state is unavailable"))?;
        match outcome {
            Ok(result) => {
                state.status.sweep_failure_count = result.failure_count;
                state.status.last_error = if result.failure_count > 0 {
                    Some("PendingArchives")
                } else {
                    None
                };
                state.next = Instant::now()
                    + if result.batch_full {
                        Duration::ZERO
                    } else {
                        Duration::from_secs(60)
                    };
                Ok(result.batch_full)
            }
            Err(_) => {
                state.status.last_error = Some("ColdMaintenanceFailed");
                state.next = Instant::now() + Duration::from_secs(60);
                Ok(false)
            }
        }
    }
    pub fn start(self: &Arc<Self>) -> Result<()> {
        let mut handle = self
            .thread
            .lock()
            .map_err(|_| Error::Unavailable("Cold worker state is unavailable"))?;
        if handle.is_some() || self.stopping.load(Ordering::Acquire) {
            return Err(Error::Conflict("Cold worker already started or stopped"));
        }
        let runtime = self.clone();
        *handle = Some(
            thread::Builder::new()
                .name("memento-cold".into())
                .spawn(move || {
                    // Per-thread niceness on Linux; does not change another task/session.
                    unsafe {
                        libc::setpriority(libc::PRIO_PROCESS, 0, 10);
                    }
                    while !runtime.stopping.load(Ordering::Acquire) {
                        if runtime.run_once().unwrap_or(false) {
                            continue;
                        }
                        let guard = runtime.wake.0.lock();
                        if let Ok(guard) = guard {
                            let _ = runtime.wake.1.wait_timeout_while(
                                guard,
                                Duration::from_secs(60),
                                |_| !runtime.stopping.load(Ordering::Acquire),
                            );
                        } else {
                            break;
                        }
                    }
                })?,
        );
        Ok(())
    }
    pub fn close(&self) -> Result<()> {
        self.stopping.store(true, Ordering::Release);
        self.wake.1.notify_all();
        if let Some(handle) = self
            .thread
            .lock()
            .map_err(|_| Error::Unavailable("Cold worker state is unavailable"))?
            .take()
        {
            handle
                .join()
                .map_err(|_| Error::Unavailable("Cold worker failed during shutdown"))?;
        }
        Ok(())
    }
}
