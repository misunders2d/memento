use super::*;
use memento_core::{VectorKind, VectorWrite};
use rusqlite::{Connection, StatementStatus};

const ORIGINAL: &str = "SELECT i.seq,i.target,i.kind,i.version,b.embedding,b.current,b.generation FROM vector_items i JOIN vector_blobs b ON b.seq=i.seq WHERE b.tier=?1 AND (?2 IS NULL OR i.seq>?2) ORDER BY i.seq LIMIT ?3";

fn signature(row: &ProjectionRow) -> (i64, String, String, i64, Vec<u8>, bool, i64) {
    (
        row.seq,
        row.target.clone(),
        row.kind.clone(),
        row.version,
        row.embedding.as_bytes().to_vec(),
        row.current,
        row.generation,
    )
}

#[test]
fn keyset_pages_preserve_signed_extremes_tiers_inactive_rows_and_f32_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("data")).unwrap();
    store
        .write(|tx| {
            for seq in [i64::MIN, i64::MIN + 1]
                .into_iter()
                .chain(-300..=300)
                .chain([i64::MAX - 1, i64::MAX])
            {
                let mut values = [0.; 768];
                values[0] = 1.;
                values[1] = (seq % 17) as f32;
                tx.put_vector(&VectorWrite {
                    seq,
                    target: format!("point-{seq}"),
                    kind: VectorKind::Node,
                    version: 1,
                    current: seq % 2 == 0,
                    tier: if seq % 3 == 0 { Tier::Cold } else { Tier::Hot },
                    embedding: F32Blob::from_values(&values)?,
                })?;
            }
            // The join must keep excluding metadata with no corresponding BLOB.
            tx.sql()
                .execute("DELETE FROM vector_blobs WHERE seq=1", [])?;
            Ok(())
        })
        .unwrap();
    store
        .read(|tx| {
            for tier in [Tier::Hot, Tier::Cold] {
                let mut original = tx.sql().prepare(ORIGINAL)?;
                let expected = original
                    .query_map(params![tier.as_str(), None::<i64>, i64::MAX], decode_row)?
                    .map(|row| row.map(|value| signature(&value.unwrap())))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                let mut actual = Vec::new();
                let mut after = None;
                loop {
                    let batch = rows(tx, tier, None, after)?;
                    assert!(batch.len() <= BATCH);
                    let Some(last) = batch.last() else { break };
                    after = Some(last.seq);
                    actual.extend(batch.iter().map(signature));
                }
                assert_eq!(actual, expected);
                assert!(actual.iter().any(|row| !row.5));
                assert!(rows(tx, tier, None, Some(i64::MAX))?.is_empty());
                let expected_after_min = original
                    .query_map(params![tier.as_str(), i64::MIN, BATCH], decode_row)?
                    .map(|row| row.map(|value| signature(&value.unwrap())))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                assert_eq!(
                    rows(tx, tier, None, Some(i64::MIN))?
                        .iter()
                        .map(signature)
                        .collect::<Vec<_>>(),
                    expected_after_min
                );
            }
            let hot = rows(tx, Tier::Hot, None, None)?;
            assert_eq!(hot[0].seq, i64::MIN);
            assert!(
                rows(
                    tx,
                    Tier::Hot,
                    Some(&BTreeSet::from([i64::MIN, 1, i64::MAX])),
                    None
                )?
                .iter()
                .map(|row| row.seq)
                .eq([i64::MIN, i64::MAX])
            );
            Ok(())
        })
        .unwrap();
}

fn measured_page(db: &Connection, sql: &str, after: Option<i64>) -> (Vec<i64>, i32) {
    let mut query = db.prepare(sql).unwrap();
    let ids = query
        .query_map(params!["hot", after, 16], |r| r.get(0))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    (ids, query.get_status(StatementStatus::VmStep))
}

#[test]
fn late_keyset_page_work_is_independent_of_already_visited_prefix() {
    let mut db = Connection::open_in_memory().unwrap();
    db.execute_batch("CREATE TABLE vector_items(seq INTEGER PRIMARY KEY,target TEXT NOT NULL,kind TEXT NOT NULL,version INTEGER NOT NULL); CREATE TABLE vector_blobs(seq INTEGER PRIMARY KEY,embedding BLOB NOT NULL,current INTEGER NOT NULL,generation INTEGER NOT NULL,tier TEXT NOT NULL);").unwrap();
    let mut bytes = vec![0; 3072];
    bytes[..4].copy_from_slice(&1f32.to_le_bytes());
    let tx = db.transaction().unwrap();
    {
        let mut item = tx
            .prepare("INSERT INTO vector_items VALUES(?1,'point','node',1)")
            .unwrap();
        let mut blob = tx
            .prepare("INSERT INTO vector_blobs VALUES(?1,?2,1,0,'hot')")
            .unwrap();
        for seq in 1..=64 {
            item.execute([seq]).unwrap();
            blob.execute(params![seq, bytes]).unwrap();
        }
    }
    tx.commit().unwrap();
    let (sql, lower) = rebuild_page(Some(0));
    let (expected, small_steps) = measured_page(&db, sql, Some(lower));
    let tx = db.transaction().unwrap();
    {
        let mut item = tx
            .prepare("INSERT INTO vector_items VALUES(?1,'prefix','node',1)")
            .unwrap();
        let mut blob = tx
            .prepare("INSERT INTO vector_blobs VALUES(?1,?2,0,0,'hot')")
            .unwrap();
        for seq in -4096..=-1 {
            item.execute([seq]).unwrap();
            blob.execute(params![seq, bytes]).unwrap();
        }
    }
    tx.commit().unwrap();
    let (actual, large_steps) = measured_page(&db, sql, Some(lower));
    let (original, old_steps) = measured_page(&db, ORIGINAL, Some(0));
    assert_eq!(actual, expected);
    assert_eq!(actual, original);
    assert_eq!(actual, (1..=16).collect::<Vec<i64>>());
    assert!(
        large_steps <= small_steps + 32,
        "prefix changed VM work: {small_steps} -> {large_steps}"
    );
    assert!(large_steps < 1000, "late page used {large_steps} VM steps");
    assert!(
        old_steps > large_steps * 10,
        "old scan {old_steps}, keyset {large_steps}"
    );
}
