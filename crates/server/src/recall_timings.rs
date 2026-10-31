//! Per-request `/recall` stage clocks. Durations, counts and request flags only:
//! never query text, context, memory text or memory IDs. `Instant` reads only.
use crate::RecallRequest;
use serde_json::{Map, Value, json};
use std::{
    cell::Cell,
    time::{Duration, Instant},
};

macro_rules! keys {
    ($kind:ident, $names:ident, $($variant:ident = $name:literal),+ $(,)?) => {
        #[derive(Clone, Copy)]
        pub enum $kind { $($variant),+ }
        pub(crate) const $names: &[&str] = &[$($name),+];
    };
}
// Stages accumulate over both passes. `scorer` and `scorer_lock_wait` are also
// inside `validation` and `fallback`; pass walls contain their index/search stages.
keys!(Stage, STAGES,
    Queue = "queue",
    Embedding = "embedding",
    FirstPass = "first_pass",
    ArchivePass = "archive_pass",
    IndexColdLock = "index_cold_lock",
    IndexHotLock = "index_hot_lock",
    IndexOpen = "index_open",
    IndexColdSync = "index_cold_sync",
    IndexHotSync = "index_hot_sync",
    Period = "period",
    SearchNode = "search_node",
    SearchCandidate = "search_candidate",
    SearchSegment = "search_segment",
    SearchDeepRaw = "search_deep_raw",
    SearchParked = "search_parked",
    SearchCold = "search_cold",
    NameAnchors = "name_anchors",
    Lexical = "lexical",
    Hydration = "hydration",
    Validation = "validation",
    Fallback = "fallback",
    Finish = "finish",
    Rows = "rows",
    PartialAnchors = "partial_anchors",
    Scorer = "scorer",
    ScorerLockWait = "scorer_lock_wait",
);
keys!(Count, COUNTS,
    Passes = "passes",
    AnnHits = "ann_hits",
    LexicalHits = "lexical_hits",
    Discovered = "discovered",
    Named = "named",
    Hydrated = "hydrated",
    Admitted = "admitted",
    Validated = "validated",
    ColdHits = "cold_hits",
    Finished = "finished",
    PartialAnchors = "partial_anchors",
    ScorerCalls = "scorer_calls",
    ScoredPassages = "scored_passages",
    Returned = "returned",
);
/// One request's clocks; single-threaded, shared by `&` across the pipeline's closures.
pub struct RecallTimings {
    stages: [Cell<Duration>; STAGES.len()],
    counts: [Cell<u64>; COUNTS.len()],
    pass: Cell<&'static str>,
    caught_up: Cell<bool>,
}
impl Default for RecallTimings {
    fn default() -> Self {
        Self {
            stages: Default::default(),
            counts: Default::default(),
            pass: Cell::new("none"),
            caught_up: Cell::new(false),
        }
    }
}
impl RecallTimings {
    pub fn add(&self, stage: Stage, elapsed: Duration) {
        let cell = &self.stages[stage as usize];
        cell.set(cell.get() + elapsed);
    }
    /// Adds the time since `since` and returns now, for back-to-back stages.
    pub fn lap(&self, stage: Stage, since: Instant) -> Instant {
        let now = Instant::now();
        self.add(stage, now - since);
        now
    }
    pub fn count(&self, count: Count, n: usize) {
        let cell = &self.counts[count as usize];
        cell.set(cell.get() + n as u64);
    }
    /// Final cut, as in per-row `explain`: `hot` or `archive`; `none` without vector search.
    pub fn pass(&self, pass: &'static str) {
        self.pass.set(pass);
    }
    pub fn index(&self, wait: &memento_ann::SnapshotTiming) {
        self.add(Stage::IndexColdLock, wait.cold_lock);
        self.add(Stage::IndexHotLock, wait.hot_lock);
        self.add(Stage::IndexOpen, wait.open);
        self.add(Stage::IndexColdSync, wait.cold_sync);
        self.add(Stage::IndexHotSync, wait.hot_sync);
        if wait.caught_up {
            self.caught_up.set(true);
        }
    }
    /// The HTTP `explain` `timings` object. Request flags are mapped to fixed
    /// values, so unvalidated input never reaches the log.
    pub fn json(&self, request: &RecallRequest, total: Duration) -> Value {
        let us = |d: Duration| json!(u64::try_from(d.as_micros()).unwrap_or(u64::MAX));
        let stages: Map<String, Value> = STAGES
            .iter()
            .zip(&self.stages)
            .map(|(k, v)| ((*k).to_owned(), us(v.get())))
            .collect();
        let counts: Map<String, Value> = COUNTS
            .iter()
            .zip(&self.counts)
            .map(|(k, v)| ((*k).to_owned(), json!(v.get())))
            .collect();
        let effort = match request.effort.as_str() {
            e @ ("cheap" | "normal" | "deep") => e,
            _ => "invalid",
        };
        let scope = match request.scope.as_deref() {
            None => "all",
            Some(s @ ("personal" | "professional")) => s,
            Some(_) => "invalid",
        };
        json!({
            "effort": effort,
            "automatic": request.automatic,
            "limit": request.limit,
            "scope": scope,
            "pass": self.pass.get(),
            "index_caught_up": self.caught_up.get(),
            "total_us": us(total),
            "stages_us": stages,
            "counts": counts,
        })
    }
}
/// The single stderr line per HTTP `/recall`, beside the service's other JSON diagnostics.
pub(crate) fn log(timings: &Value, ok: bool) {
    let line = json!({"event":"recall_timing","ok":ok,"timings":timings}).to_string();
    #[cfg(test)]
    LOGGED.lock().unwrap().push(line.clone());
    eprintln!("{line}");
}
#[cfg(test)]
pub(crate) static LOGGED: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
