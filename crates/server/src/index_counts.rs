//! Global search-index counts, refreshed off the request path. The exact count
//! visits every index unit (minutes at millions of units), so `/status` serves
//! the latest completed count with its age instead of recounting per request.
use memento_core::Store;
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

/// A status request starts a new background count once the latest is this old.
pub const REFRESH_SECONDS: f64 = 300.;

#[derive(Default)]
pub struct IndexCounts {
    latest: Mutex<Option<(f64, Value)>>,
    refreshing: AtomicBool,
}
struct Done<'a>(&'a AtomicBool);
impl Drop for Done<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
impl IndexCounts {
    /// Latest completed count, if any; starts at most one background refresh
    /// when it is missing or stale. Never waits for the count.
    pub fn current(self: &Arc<Self>, store: &Arc<Store>, now: f64) -> Option<(f64, Value)> {
        let latest = self.latest();
        let stale = latest
            .as_ref()
            .is_none_or(|(at, _)| now - at >= REFRESH_SECONDS);
        if stale && !self.refreshing.swap(true, Ordering::AcqRel) {
            let (counts, store) = (self.clone(), store.clone());
            std::thread::spawn(move || {
                let _done = Done(&counts.refreshing);
                counts.refresh(&store);
            });
        }
        latest
    }
    /// One exact global count in its own read snapshot; a failure keeps the
    /// previous count.
    pub fn refresh(&self, store: &Store) {
        let counted = store.read(|tx| crate::indexing::status(tx.sql(), None, false, false, false));
        if let Ok(value) = counted {
            *self.latest.lock().unwrap_or_else(|e| e.into_inner()) =
                Some((memento_domain::jobs::timestamp(), value));
        }
    }
    fn latest(&self) -> Option<(f64, Value)> {
        self.latest.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

/// Status `indexing` object: live readiness plus the latest global count and
/// its age, or `counting` before the first count completes.
pub fn view(
    latest: Option<(f64, Value)>,
    error: bool,
    hot_ready: bool,
    cold_ready: bool,
    now: f64,
) -> Value {
    let mut value = match latest {
        Some((at, counts)) => {
            let mut counts = counts;
            crate::records::merge(
                &mut counts,
                json!({"counted_at":at,"age_seconds":(now - at).max(0.).round()}),
            );
            counts
        }
        None => json!({"counting":true}),
    };
    crate::records::merge(&mut value, json!({"scope":"global"}));
    crate::records::merge(
        &mut value,
        crate::indexing::live_fields(error, hot_ready, cold_ready),
    );
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serves_latest_global_count_with_age_and_counting_before_first() {
        let first = view(None, false, true, false, 10.);
        assert_eq!(first["counting"], true);
        assert_eq!(first["scope"], "global");
        assert_eq!(first["available"], true);
        assert_eq!(first["search"]["ready"], true);
        assert!(first.get("total").is_none());

        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data")).unwrap();
        store.capture(serde_json::from_value(json!({"source":"synthetic","event_id":"counts","conversation_id":"thread","kind":"user","text":"Index count fixture","metadata":{}})).unwrap()).unwrap();
        let counts = Arc::new(IndexCounts::default());
        counts.refresh(&store);
        let (at, _) = counts.latest().unwrap();
        let shown = view(counts.latest(), true, false, false, at + 42.);
        assert_eq!(shown["total"], 0);
        assert_eq!(shown["unprepared_episodes"], 1);
        assert_eq!(shown["age_seconds"], 42.);
        assert_eq!(shown["scope"], "global");
        assert_eq!(shown["error"], "NativeIndexingError");
        assert_eq!(shown["search"]["ready"], false);
        assert!(shown.get("counting").is_none());

        // A fresh count is served without starting another refresh.
        assert!(counts.current(&store, at + 1.).is_some());
        assert!(!counts.refreshing.load(Ordering::Acquire));
        // A stale count is still served while one refresh runs in the background.
        assert!(counts.current(&store, at + REFRESH_SECONDS).is_some());
    }
}
