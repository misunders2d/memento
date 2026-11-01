//! Per-request processing stage timings. One `/work` request runs on one
//! blocking thread, so stages accumulate thread-locally and the HTTP handler
//! takes them for its single `work_timing` log line. Names and microseconds only.
use std::{cell::RefCell, collections::BTreeMap, time::Instant};

thread_local! {
    static STAGES: RefCell<BTreeMap<&'static str, u128>> = const { RefCell::new(BTreeMap::new()) };
}

/// Adds the time since `since` to `name` and restarts `since`.
pub fn lap(name: &'static str, since: &mut Instant) {
    let elapsed = since.elapsed().as_micros();
    STAGES.with(|stages| *stages.borrow_mut().entry(name).or_default() += elapsed);
    *since = Instant::now();
}

/// Returns and clears this thread's accumulated stages.
pub fn take() -> BTreeMap<&'static str, u128> {
    STAGES.with(|stages| std::mem::take(&mut *stages.borrow_mut()))
}

#[cfg(test)]
mod tests {
    #[test]
    fn laps_accumulate_per_stage_and_take_clears() {
        let mut since = std::time::Instant::now();
        super::lap("a", &mut since);
        super::lap("a", &mut since);
        super::lap("b", &mut since);
        assert_eq!(super::take().into_keys().collect::<Vec<_>>(), ["a", "b"]);
        assert!(super::take().is_empty());
    }
}
