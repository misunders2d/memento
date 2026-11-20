// Per-request passage-score reuse across the hot and archive cuts of one recall.
// Query vectors are synthetic; the fake scorer is deterministic by passage text.
fn fake_logit(text: &str) -> f32 {
    if text.contains("Rejected") { -10. } else { 2.5 }
}

#[test]
fn archive_cut_reuses_hot_cut_passage_scores_and_returns_the_same_rows() {
    let mut f = Fixture::new();
    // Hot rows above the cosine floor, so both cuts shortlist them for scoring.
    // The entity passes scoring but is not a meaningful answer; the facts are rejected.
    let person = f.add("Mara Quillon", "person", "2026-10-20T00:00:00Z", 0.5, 0.9, "personal");
    for (n, cosine) in [0.86, 0.84, 0.82].into_iter().enumerate() {
        f.add(&format!("Rejected hot passage {n}"), "knowledge", "2026-10-19T00:00:00Z", 0.5, cosine, "personal");
    }
    let raw = raw_point(&f, "archive-raw", ("synthetic", "user", "Fresh archive evidence kept only in raw history"), json!({}), 0.9, -1);
    for tier in [Tier::Hot, Tier::Cold] {
        f.projection.rebuild(tier, &AtomicBool::new(false)).unwrap();
    }
    let request = RecallRequest { query: "unseen paraphrase".into(), limit: 10, explain: true, ..Default::default() };
    let mut vector = [0.; 768];
    vector[0] = 1.;
    let deadline = Instant::now() + Duration::from_secs(10);
    let calls = std::cell::RefCell::new(Vec::<String>::new());
    let scorer = |documents: &[&str], _: Instant| {
        calls.borrow_mut().extend(documents.iter().map(|text| text.to_string()));
        Ok(documents.iter().map(|text| fake_logit(text)).collect())
    };
    // Reference: one standalone archive cut through the plain scorer.
    let reference = f.projection.snapshot_until(true, true, deadline, |tx, view| Retrieval::current_scored(tx, view, &request, &vector, true, deadline, Some(&scorer))).unwrap().0;
    let expected: HashSet<String> = calls.borrow_mut().drain(..).collect();
    assert_eq!(expected.len(), 5, "{expected:?}");
    let timings = RecallTimings::default();
    let rows = Retrieval::passes(&f.projection, &request, &vector, deadline, &scorer, &timings).unwrap();
    // Same rows, order, logits and explanations. Only age decay differs: each cut reads
    // its own clock, in the `decay` field and the third element of the explained sort key.
    let without_clock = |rows: &[Value]| -> Vec<Value> {
        rows.iter().cloned().map(|mut row| {
            assert!(row.as_object_mut().unwrap().remove("decay").is_some());
            assert!(row["explain"]["sort_key"][2].is_f64());
            row["explain"]["sort_key"][2] = Value::Null;
            row
        }).collect()
    };
    assert_eq!(without_clock(&rows), without_clock(&reference));
    let ids: Vec<&str> = rows.iter().map(key).collect();
    assert!(ids.contains(&person.as_str()) && ids.contains(&raw.chunk.as_str()), "{rows:?}");
    assert!(rows.iter().all(|row| row["explain"]["pass"] == "archive"), "{rows:?}");
    // Both cuts ran, yet each distinct passage went through the model exactly once.
    let calls = calls.borrow();
    let distinct: HashSet<String> = calls.iter().cloned().collect();
    assert_eq!(distinct, expected);
    assert_eq!(calls.len(), distinct.len(), "{calls:?}");
    let counts = &timings.json(&request, Duration::ZERO)["counts"];
    assert_eq!(counts["passes"], 2);
    assert_eq!(counts["scored_passages"], 5);
    // The archive cut took the four hot passages from the first cut.
    assert_eq!(counts["scorer_memo_hits"], 4);
}

#[test]
fn failed_expired_or_malformed_scores_are_never_stored_and_a_stored_score_respects_the_deadline() {
    let later = || Instant::now() + Duration::from_secs(10);
    let seen = std::cell::RefCell::new(Vec::<Vec<String>>::new());
    let record = |documents: &[&str]| seen.borrow_mut().push(documents.iter().map(|text| text.to_string()).collect());
    let good = |documents: &[&str], _: Instant| { record(documents); Ok(documents.iter().map(|text| text.len() as f32).collect()) };
    let expired = |documents: &[&str], _: Instant| { record(documents); Err(Error::Unavailable("Text passage scoring deadline expired")) };
    let nonfinite = |documents: &[&str], _: Instant| { record(documents); Ok(documents.iter().enumerate().map(|(n, _)| if n == 0 { 1. } else { f32::NAN }).collect()) };
    let short = |documents: &[&str], _: Instant| { record(documents); Ok(vec![1.; documents.len() - 1]) };
    let runs = || std::mem::take(&mut *seen.borrow_mut());
    let memo = ScoreMemo::default();
    let mut reused = 0;
    // A timed-out, failed or malformed batch stores none of its passages, not even the valid ones.
    assert!(matches!(memo.score(&["aa", "bbb"], later(), &expired, &mut reused), Err(Error::Unavailable("Text passage scoring deadline expired"))));
    for malformed in [&nonfinite as &TextScore<'_>, &short] {
        assert!(matches!(memo.score(&["aa", "bbb"], later(), malformed, &mut reused), Err(Error::Unavailable("Text passage scoring returned invalid logits"))));
    }
    assert_eq!((runs().len(), reused), (3, 0));
    assert_eq!(memo.score(&["aa", "bbb"], later(), &good, &mut reused).unwrap(), [2., 3.]);
    assert_eq!((runs(), reused), (vec![vec!["aa".to_owned(), "bbb".to_owned()]], 0));
    // Stored logits come back in input order; only distinct new passages run, once each.
    assert_eq!(memo.score(&["bbb", "dddd", "aa", "dddd"], later(), &good, &mut reused).unwrap(), [3., 4., 2., 4.]);
    assert_eq!((runs(), reused), (vec![vec!["dddd".to_owned()]], 3));
    // A failure beside stored passages stores nothing new and returns no partial result.
    assert!(memo.score(&["aa", "eeeee"], later(), &expired, &mut reused).is_err());
    assert_eq!(runs(), [["eeeee"]]);
    assert_eq!(memo.score(&["aa", "eeeee"], later(), &good, &mut reused).unwrap(), [2., 5.]);
    assert_eq!((runs(), reused), (vec![vec!["eeeee".to_owned()]], 1));
    // A fully stored batch makes no model run before the deadline.
    assert_eq!(memo.score(&["eeeee", "aa"], later(), &expired, &mut reused).unwrap(), [5., 2.]);
    assert_eq!((runs().len(), reused), (0, 2));
    // After the deadline it gets the scorer's own expiry, exactly as without the memo.
    assert!(matches!(memo.score(&["eeeee", "aa"], Instant::now(), &expired, &mut reused), Err(Error::Unavailable("Text passage scoring deadline expired"))));
    assert_eq!(runs(), [["eeeee", "aa"]]);
}
