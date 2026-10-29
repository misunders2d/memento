//! Durable leased work and one-at-a-time background unit completion.
use crate::util::*;
use crate::{Graph, TxGraph, context};
use memento_core::{Error, Result, WriteTxn};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

pub fn timestamp() -> f64 {
    chrono::Utc::now().timestamp_micros() as f64 / 1e6
}
#[derive(Clone)]
pub struct Job {
    pub queue: Queue,
    pub id: String,
    pub kind: String,
    pub payload: Value,
    pub owner: String,
    pub attempts: i64,
}
impl Job {
    /// Checked only between indexing units; current unit retains its lease and
    /// may commit before the handler releases the unfinished job for restart.
    pub fn stopping(&self) -> bool {
        self.queue.shutdown.load(Ordering::Acquire)
    }
    pub fn check(&self) -> Result<()> {
        self.queue.graph.store.read(|tx| self.check_in(tx.sql()))
    }
    pub fn check_in(&self, db: &Connection) -> Result<()> {
        let row = one(
            db,
            "SELECT state,owner,lease_until FROM jobs WHERE id=?",
            [&self.id],
        )?;
        if row.as_ref().is_none_or(|row| {
            row["state"] != "leased"
                || row["owner"] != self.owner
                || row["lease_until"]
                    .as_f64()
                    .is_none_or(|until| until <= timestamp())
        }) {
            return Err(Error::Conflict("Work lease expired or was reassigned"));
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct Queue {
    pub graph: Graph,
    pub lease_seconds: u64,
    shutdown: Arc<AtomicBool>,
}
impl Queue {
    pub fn new(graph: Graph) -> Self {
        Self {
            graph,
            lease_seconds: 180,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
    /// Signal between-unit cancellation before a lifecycle owner joins other workers.
    /// The current leased unit may still commit, exactly as Worker::close permits.
    pub fn request_stop(&self) {
        self.shutdown.store(true, Ordering::Release);
    }
    pub fn with_lease(graph: Graph, lease_seconds: u64) -> Result<Self> {
        if lease_seconds == 0 {
            return Err(Error::Validation("Lease duration must be positive"));
        }
        Ok(Self {
            graph,
            lease_seconds,
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }
    pub fn insert(tx: &WriteTxn, key: &str, kind: &str, payload: &Value) -> Result<String> {
        let serialized = encode_json(payload)?;
        if let Some(prior) = one(
            tx.sql(),
            "SELECT id,kind,payload FROM jobs WHERE job_key=?",
            [key],
        )? {
            if prior["kind"] != kind || prior["payload"] != serialized {
                return Err(Error::Conflict(
                    "Job identifier already has different input",
                ));
            }
            return Ok(get_str(&prior, "id")?.into());
        }
        let job = id();
        tx.sql().execute("INSERT INTO jobs(id,job_key,kind,payload,state,available_at,attempts) VALUES (?,?,?,?,'pending',?,0)",params![job,key,kind,serialized,timestamp()])?;
        Ok(job)
    }
    pub fn submit_media(&self, command: &Value) -> Result<Value> {
        let ctx = context(
            get_str(command, "command_id")?.into(),
            get_str(command, "actor")?.into(),
            get_str(command, "why")?.into(),
        );
        let request = json!({"op":"queue_media","input":command});
        self.graph.store.command(&ctx, &request, |tx, _commit| {
            let mut resolved = TxGraph::resolve_evidence(tx, command)?;
            let labels = resolved["scope"]
                .as_array()
                .ok_or(Error::Validation("Invalid scope"))?
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .ok_or(Error::Validation("Invalid scope"))
                })
                .collect::<Result<Vec<_>>>()?;
            resolved["scope"] = json!(scope(tx.sql(), "media", &labels)?);
            resolved.as_object_mut().unwrap().remove("source");
            resolved.as_object_mut().unwrap().remove("conversation_id");
            let job = Self::insert(tx, &format!("media/{}", ctx.command_id), "media", &resolved)?;
            TxGraph::evidence(tx, &job, 1, &resolved["evidence"])?;
            Ok(json!({"job_id":job,"status":"accepted","status_url":format!("/jobs/{job}")}))
        })
    }
    pub fn get(&self, job: &str) -> Result<Option<Value>> {
        self.graph.store.read(|tx| {
            let mut row = one(
                tx.sql(),
                "SELECT id,kind,state,attempts,error,result FROM jobs WHERE id=?",
                [job],
            )?;
            if let Some(row) = &mut row {
                if let Some(result) = row["result"].as_str() {
                    row["result"] = decode_json(result)?;
                }
            }
            Ok(row)
        })
    }
    pub fn claim(&self, kinds: &[String], exclude: &[String]) -> Result<Option<Job>> {
        if kinds.is_empty() {
            return Ok(None);
        }
        let now = timestamp();
        let owner = id();
        let row=self.graph.store.write(|tx|{let row=one(tx.sql(),"SELECT * FROM jobs WHERE kind IN (SELECT value FROM json_each(?1)) AND ((state='pending' AND available_at<=?2) OR (state='leased' AND lease_until<=?2)) AND id NOT IN (SELECT value FROM json_each(?3)) AND NOT (kind='consolidate' AND EXISTS (SELECT 1 FROM jobs live WHERE live.kind='consolidate' AND live.state='leased' AND live.lease_until>?2)) ORDER BY available_at,id LIMIT 1",params![encode_json(&json!(kinds))?,now,encode_json(&json!(exclude))?])?;if let Some(row)=&row{tx.sql().execute("UPDATE jobs SET state='leased',owner=?,lease_until=?,attempts=attempts+1 WHERE id=?",params![owner,now+self.lease_seconds as f64,row["id"].as_str()])?;}Ok(row)})?;
        row.map(|row| {
            Ok(Job {
                queue: self.clone(),
                id: get_str(&row, "id")?.into(),
                kind: get_str(&row, "kind")?.into(),
                payload: decode_json(get_str(&row, "payload")?)?,
                owner,
                attempts: get_i64(&row, "attempts")? + 1,
            })
        })
        .transpose()
    }
    pub fn renew(&self, job: &Job) -> Result<bool> {
        let now = timestamp();
        self.graph.store.write(|tx|Ok(tx.sql().execute("UPDATE jobs SET lease_until=? WHERE id=? AND state='leased' AND owner=? AND lease_until>?",params![now+self.lease_seconds as f64,job.id,job.owner,now])?==1))
    }
    pub fn complete(&self, job: &Job, result: &Value) -> Result<()> {
        self.graph.store.write(|tx|{job.check_in(tx.sql())?;tx.sql().execute("UPDATE jobs SET state='done',result=?,error=NULL,owner=NULL,lease_until=NULL WHERE id=? AND owner=?",params![encode_json(result)?,job.id,job.owner])?;Ok(())})
    }
    /// Returns a lease its client never received: no failure, backoff or attempt.
    pub fn unclaim(&self, job: &Job) -> Result<()> {
        self.graph.store.write(|tx|{tx.sql().execute("UPDATE jobs SET state='pending',owner=NULL,lease_until=NULL,attempts=max(attempts-1,0) WHERE id=? AND state='leased' AND owner=?",params![job.id,job.owner])?;Ok(())})
    }
    pub fn fail(&self, job: &Job, failure: JobFailure) -> Result<()> {
        let delay = failure.retry_after.unwrap_or_else(|| {
            Duration::from_secs(
                (5u64.saturating_mul(1 << ((job.attempts - 1).clamp(0, 10) as u32))).min(3600),
            )
        });
        self.graph.store.write(|tx|{let retryable=if failure.category=="CapturedSourceUnavailable"&&job.kind=="media"&&job.payload["captured"]==true{active_captured_source(tx.sql(),get_str(&job.payload,"sha256")?)?}else{failure.retryable};tx.sql().execute("UPDATE jobs SET state=?,available_at=?,error=?,owner=NULL,lease_until=NULL WHERE id=? AND state='leased' AND owner=?",params![if retryable{"pending"}else{"failed"},timestamp()+delay.as_secs_f64(),failure.category,job.id,job.owner])?;Ok(())})
    }
}
pub fn active_captured_source(db: &Connection, sha: &str) -> Result<bool> {
    Ok(one(db,"SELECT 1 FROM captured_media_refs r JOIN episodes e ON e.id=r.episode_id WHERE r.sha256=? AND NOT EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id) AND NOT EXISTS (SELECT 1 FROM media_files m JOIN forgotten_targets f ON f.target=m.node_id WHERE m.sha256=r.sha256) LIMIT 1",[sha])?.is_some())
}

pub fn error_category(error: &Error) -> &'static str {
    match error {
        Error::Validation(_) => "ValueError",
        Error::Conflict(_) => "ConflictError",
        Error::Io(error) if error.kind() == std::io::ErrorKind::NotFound => "FileNotFoundError",
        Error::Io(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            "PermissionError"
        }
        Error::Io(_) => "OSError",
        _ => "RuntimeError",
    }
}

#[derive(Clone, Debug)]
pub struct JobFailure {
    pub category: &'static str,
    pub retryable: bool,
    pub retry_after: Option<Duration>,
}
impl From<Error> for JobFailure {
    fn from(error: Error) -> Self {
        let (category, retryable) = match error {
            Error::Validation(_) => ("ValueError", false),
            Error::Conflict(_) => ("ConflictError", false),
            Error::Io(ref error) if error.kind() == std::io::ErrorKind::NotFound => {
                ("FileNotFoundError", true)
            }
            Error::Io(ref error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                ("PermissionError", true)
            }
            Error::Io(_) => ("OSError", true),
            _ => ("RuntimeError", true),
        };
        Self {
            category,
            retryable,
            retry_after: None,
        }
    }
}
pub type Handler = Arc<dyn Fn(&Job) -> std::result::Result<Value, JobFailure> + Send + Sync>;
pub type Maintenance = Arc<dyn Fn() -> Result<bool> + Send + Sync>;
pub struct Worker {
    queue: Queue,
    handlers: HashMap<String, Handler>,
    maintenance: Option<Maintenance>,
    stop: Arc<AtomicBool>,
    wake: Arc<(Mutex<()>, Condvar)>,
    thread: Option<JoinHandle<()>>,
}
impl Worker {
    pub fn new(
        queue: Queue,
        handlers: HashMap<String, Handler>,
        maintenance: Option<Maintenance>,
    ) -> Self {
        let stop = Arc::clone(&queue.shutdown);
        Self {
            queue,
            handlers,
            maintenance,
            stop,
            wake: Arc::new((Mutex::new(()), Condvar::new())),
            thread: None,
        }
    }
    pub fn run_once(&self) -> Result<bool> {
        run_once(&self.queue, &self.handlers, self.maintenance.as_ref())
    }
    pub fn start(&mut self) -> Result<()> {
        if self.thread.is_some() {
            return Err(Error::Conflict("Job worker already started"));
        }
        let queue = self.queue.clone();
        let handlers = self.handlers.clone();
        let maintenance = self.maintenance.clone();
        let stop = Arc::clone(&self.stop);
        let wake = Arc::clone(&self.wake);
        self.thread = Some(thread::Builder::new().name("memento-jobs".into()).spawn(
            move || {
                while !stop.load(Ordering::Acquire) {
                    let worked = match run_once(&queue, &handlers, maintenance.as_ref()) {
                        Ok(worked) => worked,
                        Err(error) => {
                            eprintln!("Job dispatch failed ({})", error_category(&error));
                            false
                        }
                    };
                    if !worked {
                        let (lock, cond) = &*wake;
                        if let Ok(guard) = lock.lock() {
                            let _ = cond.wait_timeout_while(guard, Duration::from_secs(1), |_| {
                                !stop.load(Ordering::Acquire)
                            });
                        }
                    }
                }
            },
        )?);
        Ok(())
    }
    pub fn close(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.wake.1.notify_all();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.close();
    }
}
fn run_once(
    queue: &Queue,
    handlers: &HashMap<String, Handler>,
    maintenance: Option<&Maintenance>,
) -> Result<bool> {
    let maintained = if let Some(callback) = maintenance {
        match callback() {
            Ok(maintained) => maintained,
            Err(error) => {
                eprintln!(
                    "Background maintenance failed ({}); data retained",
                    error_category(&error)
                );
                false
            }
        }
    } else {
        false
    };
    let kinds = handlers.keys().cloned().collect::<Vec<_>>();
    let Some(job) = queue.claim(&kinds, &[])? else {
        return Ok(maintained);
    };
    let finished = Arc::new((Mutex::new(false), Condvar::new()));
    let pulse_finished = Arc::clone(&finished);
    let pulse_queue = queue.clone();
    let pulse_job = job.clone();
    let pulse = thread::Builder::new()
        .name("memento-lease".into())
        .spawn(move || {
            let (lock, cond) = &*pulse_finished;
            loop {
                let Ok(guard) = lock.lock() else {
                    break;
                };
                let Ok((done, _)) = cond.wait_timeout_while(
                    guard,
                    Duration::from_secs_f64(pulse_queue.lease_seconds as f64 / 3.0),
                    |done| !*done,
                ) else {
                    break;
                };
                if *done {
                    break;
                }
                drop(done);
                match pulse_queue.renew(&pulse_job) {
                    Ok(true) => (),
                    Ok(false) => break,
                    Err(error) => {
                        eprintln!("Lease renewal failed ({})", error_category(&error));
                        break;
                    }
                }
            }
        })?;
    let result =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handlers[&job.kind](&job)))
            .unwrap_or_else(|_| {
                Err(JobFailure {
                    category: "RuntimeError",
                    retryable: true,
                    retry_after: None,
                })
            });
    let completed = match result {
        Ok(result) => match queue.complete(&job, &result) {
            Err(Error::Conflict(_)) => Ok(()),
            other => other,
        },
        Err(error) => queue.fail(&job, error),
    };
    if let Ok(mut done) = finished.0.lock() {
        *done = true;
    }
    finished.1.notify_all();
    let _ = pulse.join();
    completed?;
    Ok(true)
}
