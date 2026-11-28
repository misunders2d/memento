//! J4/N4: a deadline-bounded read interrupts runaway SQL and never affects the next read.
use memento_core::{Error, Store, store::READ_DEADLINE};
use std::time::{Duration, Instant};

#[test]
fn read_until_interrupts_slow_sql_at_the_deadline_and_the_next_read_is_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("data")).unwrap();
    let endless = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c) SELECT count(*) FROM c";
    let started = Instant::now();
    let result = store.read_until(started + Duration::from_millis(200), |tx| {
        Ok(tx.sql().query_row(endless, [], |r| r.get::<_, i64>(0))?)
    });
    let waited = started.elapsed();
    assert!(matches!(result, Err(Error::Unavailable(READ_DEADLINE))), "{result:?}");
    assert!(waited >= Duration::from_millis(200) && waited < Duration::from_secs(3), "{waited:?}");
    // A fresh read and a fresh deadline-bounded read both complete normally.
    let count = |tx: &memento_core::ReadTxn<'_>| {
        Ok(tx.sql().query_row("SELECT count(*) FROM runtime_state", [], |r| r.get::<_, i64>(0))?)
    };
    assert_eq!(store.read(count).unwrap(), 1);
    assert_eq!(store.read_until(Instant::now() + Duration::from_secs(5), count).unwrap(), 1);
    // An already expired deadline fails as a deadline, not as storage corruption.
    let expired = store.read_until(Instant::now() - Duration::from_millis(1), count);
    assert!(matches!(expired, Err(Error::Unavailable(READ_DEADLINE))), "{expired:?}");
}
