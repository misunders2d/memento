// HTTP-only recall explanation: returned rows only, computed from facts the
// pipeline already produced, and never a change to selection or order.
fn explained(f: &Fixture, query: &str, automatic: bool, explain: bool) -> Vec<Value> {
    f.projection.rebuild(Tier::Hot, &AtomicBool::new(false)).unwrap();
    let score = |documents: &[&str], _: Instant| Ok(vec![1.; documents.len()]);
    let request = RecallRequest{query:query.into(),automatic,explain,limit:6,scope:Some("personal".into()),..Default::default()};
    let mut vector = [0.; 768];
    vector[0] = 1.;
    let deadline = Instant::now() + std::time::Duration::from_secs(5);
    f.projection.snapshot_until(true, false, deadline, |tx, view| Retrieval::current_scored(tx, view, &request, &vector, false, deadline, Some(&score))).unwrap().0
}
// Each request reads its own clock, so `decay` legitimately differs between two calls.
fn stripped(rows: &[Value]) -> Vec<Value> {
    rows.iter().cloned().map(|mut r| { r.as_object_mut().unwrap().remove("explain"); r.as_object_mut().unwrap().remove("decay"); r }).collect()
}

#[test]
fn explain_describes_returned_rows_only_and_changes_nothing_else() {
    let mut f = Fixture::new();
    let person = f.add("Mark Petrov", "person", "2020-01-01T00:00:00Z", 0.5, 0.1, "personal");
    let strong = f.add("The quartz calibration runs weekly.", "knowledge", "2026-10-20T00:00:00Z", 0.5, 0.9, "personal");
    let weaker = f.add("Quartz samples stay in the cold room.", "knowledge", "2026-10-19T00:00:00Z", 0.9, 0.8, "personal");
    f.add("Unrelated harbor schedule note.", "knowledge", "2026-10-18T00:00:00Z", 0.5, 0.2, "personal");
    for (query, automatic) in [("quartz", false), ("quartz", true), ("quartz with Mark Petrov", false), ("quartz with Mark Petrov", true)] {
        let off = explained(&f, query, automatic, false);
        let on = explained(&f, query, automatic, true);
        assert!(off.iter().all(|r| r.get("explain").is_none()));
        assert_eq!(stripped(&on), stripped(&off), "{query} {automatic}");
        assert!(on.iter().all(|r| r.as_object().unwrap().keys().all(|k| !k.starts_with('_'))));
        let mut ranked = Vec::new();
        for row in &on {
            let explain = &row["explain"];
            if explain["ordering"] == "positional" {
                assert!(explain["sort_key"].is_null());
                continue;
            }
            // Only per-row facts: no rejected IDs, counts, confidence or timings.
            let mut keys: Vec<&str> = explain.as_object().unwrap().keys().map(String::as_str).collect();
            keys.sort_unstable();
            assert_eq!(keys, ["ann", "lexical", "ordering", "pass", "passage_validation", "route", "sort_key"]);
            assert_eq!(explain["route"], "ranked");
            assert_eq!(explain["pass"], "hot");
            assert_eq!(explain["ann"]["cosine"], row["cosine"]);
            assert_eq!(explain["ann"]["floor"], if automatic { AUTO_FLOOR } else { TEXT_FLOOR });
            ranked.push(explain["sort_key"].as_array().unwrap().clone());
        }
        assert!(!ranked.is_empty());
        // The sort tuple reproduces the relative order of ranked rows only.
        for pair in ranked.windows(2) {
            let (a, b) = (&pair[0], &pair[1]);
            let ordering = (0..5)
                .map(|i| b[i].as_f64().unwrap().total_cmp(&a[i].as_f64().unwrap()))
                .find(|o| o.is_ne())
                .unwrap_or_else(|| a[5].as_str().cmp(&b[5].as_str()));
            assert_ne!(ordering, std::cmp::Ordering::Greater, "{a:?} {b:?}");
        }
        let first = on.iter().find(|r| r["id"] == strong).unwrap();
        assert_eq!(first["explain"]["sort_key"][5], strong);
        assert!(on.iter().any(|r| r["id"] == weaker));
        if query.contains("Mark") {
            let anchor = on.iter().find(|r| r["id"] == person).unwrap();
            assert_eq!(anchor["explain"]["route"], "anchor");
            assert_eq!(anchor["explain"]["ordering"], "positional");
        }
    }
    // Passage validation reports the actual decision: an all-term literal match is exempt,
    // a semantic-only match is scored.
    let on = explained(&f, "quartz", true, true);
    let exempt = &on.iter().find(|r| r["id"] == strong).unwrap()["explain"];
    assert_eq!(exempt["lexical"]["and_literal"], true);
    assert_eq!(exempt["passage_validation"], json!({"status":"not_run","reason":"route_exempt"}));
    let on = explained(&f, "mineral oscillator", true, true);
    let semantic = on.iter().find(|r| r["id"] == strong).unwrap();
    assert_eq!(semantic["match"], "semantic");
    let validated = &semantic["explain"]["passage_validation"];
    assert_eq!(validated["status"], "passed");
    assert_eq!(validated["logit"], 1.0);
    assert_eq!(validated["floor"], SEMANTIC_TEXT_LOGIT_FLOOR);
    // Without a scorer the validator is explicitly not run.
    let request = RecallRequest{query:"quartz".into(),automatic:true,explain:true,limit:6,scope:Some("personal".into()),..Default::default()};
    let mut vector = [0.; 768];
    vector[0] = 1.;
    let deadline = Instant::now() + std::time::Duration::from_secs(5);
    let rows = f.projection.snapshot_until(true, false, deadline, |tx, view| Retrieval::current(tx, view, &request, &vector, false, deadline)).unwrap().0;
    assert!(rows.iter().filter(|r| r["explain"]["ordering"] == "sort_key").all(|r| r["explain"]["passage_validation"] == json!({"status":"not_run","reason":"no_scorer"})));
}
