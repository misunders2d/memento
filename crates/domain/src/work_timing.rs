//! Per-request processing stage timings. One `/work` request runs on one
//! blocking thread, so stages accumulate thread-locally and the HTTP handler
//! takes them for its single `work_timing` log line. Names with microseconds or
//! cheap counts only.
use std::{cell::RefCell, collections::BTreeMap, time::Instant};

thread_local! {
    static STAGES: RefCell<BTreeMap<&'static str, u128>> = const { RefCell::new(BTreeMap::new()) };
    static COUNTS: RefCell<BTreeMap<&'static str, u64>> = const { RefCell::new(BTreeMap::new()) };
}

/// Adds the time since `since` to `name` and restarts `since`.
pub fn lap(name: &'static str, since: &mut Instant) {
    let elapsed = since.elapsed().as_micros();
    STAGES.with(|stages| *stages.borrow_mut().entry(name).or_default() += elapsed);
    *since = Instant::now();
}

/// Adds `n` to the count `name`.
pub fn count(name: &'static str, n: u64) {
    COUNTS.with(|counts| *counts.borrow_mut().entry(name).or_default() += n);
}

/// Returns and clears this thread's accumulated stages.
pub fn take() -> BTreeMap<&'static str, u128> {
    STAGES.with(|stages| std::mem::take(&mut *stages.borrow_mut()))
}

/// Returns and clears this thread's accumulated counts.
pub fn take_counts() -> BTreeMap<&'static str, u64> {
    COUNTS.with(|counts| std::mem::take(&mut *counts.borrow_mut()))
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
        super::count("n", 2);
        super::count("n", 3);
        assert_eq!(super::take_counts().into_iter().collect::<Vec<_>>(), [("n", 5)]);
        assert!(super::take_counts().is_empty());
    }
}
