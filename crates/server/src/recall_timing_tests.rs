// HTTP `/recall` stage timings over the real two-cut ranking path.
// Query vectors are synthetic and the passage scorer always passes.
struct TimedRecall(Arc<Projection>);
impl crate::Recall for TimedRecall {
    fn recall(&self, request: &RecallRequest, deadline: Instant) -> Result<Vec<Value>> {
        self.recall_timed(request, deadline, &RecallTimings::default())
    }
    fn recall_timed(&self, request: &RecallRequest, deadline: Instant, timings: &RecallTimings) -> Result<Vec<Value>> {
        let mut vector = [0.; 768];
        vector[0] = 1.;
        let score = |documents: &[&str], _: Instant| Ok(vec![1.; documents.len()]);
        Retrieval::passes(&self.0, request, &vector, deadline, &score, timings)
    }
    fn ready(&self) -> bool { true }
}
fn timed_state(f: &Fixture) -> crate::AppState {
    for tier in [Tier::Hot, Tier::Cold] {
        f.projection.rebuild(tier, &AtomicBool::new(false)).unwrap();
    }
    let mut state = crate::AppState::new(f.store.clone()).unwrap();
    state.recall = Some(Arc::new(TimedRecall(f.projection.clone())));
    state
}
fn us(timings: &Value, stage: &str) -> u64 {
    timings["stages_us"][stage].as_u64().unwrap()
}

#[test]
fn explain_reports_stage_timings_and_the_log_line_has_no_request_text() {
    use crate::recall_timings::{COUNTS, LOGGED, STAGES};
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
    let (query, context) = ("Zyxwquartzsentinel", "Qvbnprivatecontextsentinel");
    let mut f = Fixture::new();
    f.add("The quartz calibration runs weekly.", "knowledge", "2026-10-20T00:00:00Z", 0.5, 0.9, "personal");
    let state = timed_state(&f);
    let path = format!("/recall?q={query}&context={context}&effort=cheap&limit=6&automatic=true&scope=personal");
    let plain = runtime.block_on(call(&state, "GET", &path, None));
    assert!(plain.get("timings").is_none());
    let body = runtime.block_on(call(&state, "GET", &format!("{path}&explain=true"), None));
    let timings = &body["timings"];
    let mut keys: Vec<&str> = timings.as_object().unwrap().keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(keys, ["automatic", "counts", "cuts", "effort", "failure", "failure_tier", "index_caught_up", "limit", "pass", "scope", "stages_us", "total_us"]);
    assert_eq!((&timings["failure"], &timings["failure_tier"]), (&Value::Null, &Value::Null));
    assert_eq!(timings["cuts"].as_array().unwrap().len(), 1);
    assert_eq!((&timings["cuts"][0]["pass"], &timings["cuts"][0]["ok"]), (&json!("hot"), &json!(true)));
    for (group, expected) in [("stages_us", STAGES), ("counts", COUNTS)] {
        let mut names: Vec<&str> = timings[group].as_object().unwrap().keys().map(String::as_str).collect();
        let mut expected = expected.to_vec();
        names.sort_unstable();
        expected.sort_unstable();
        assert_eq!(names, expected, "{group}");
    }
    assert_eq!((&timings["effort"], &timings["automatic"], &timings["limit"], &timings["scope"]), (&json!("cheap"), &json!(true), &json!(6), &json!("personal")));
    assert_eq!(timings["pass"], "hot");
    let counts = &timings["counts"];
    assert_eq!(counts["passes"], 1);
    assert_eq!((&counts["query_chars"], &counts["context_chars"]), (&json!(query.len()), &json!(context.len())));
    assert_eq!(counts["returned"], body["memories"].as_array().unwrap().len());
    assert_eq!(stripped(body["memories"].as_array().unwrap()), stripped(plain["memories"].as_array().unwrap()));
    assert!(counts["ann_hits"].as_u64().unwrap() >= 1 && counts["returned"].as_u64().unwrap() >= 1);
    assert!(counts["scorer_calls"].as_u64().unwrap() >= 1 && counts["scored_passages"].as_u64().unwrap() >= 1);
    // Measured stages are disjoint intervals inside their pass wall; the scorer overlaps validation.
    let outside = ["queue", "embedding", "embedding_lane_wait", "embedding_tokenize", "embedding_inference", "first_pass", "archive_pass", "scorer", "scorer_lock_wait"];
    let inside: u64 = STAGES.iter().filter(|s| !outside.contains(*s)).map(|s| us(timings, s)).sum();
    assert!(inside <= us(timings, "first_pass"), "{timings}");
    assert!(us(timings, "scorer") <= us(timings, "validation") + us(timings, "fallback"), "{timings}");
    assert!(us(timings, "queue") + us(timings, "first_pass") <= timings["total_us"].as_u64().unwrap(), "{timings}");
    // An unscoped automatic request without a relevant memory takes the archive cut.
    let mut empty = Fixture::new();
    empty.add("Unrelated harbor schedule note.", "knowledge", "2026-10-18T00:00:00Z", 0.5, 0.2, "personal");
    let archive = runtime.block_on(call(&timed_state(&empty), "GET", &format!("/recall?q={query}&automatic=true&explain=true"), None));
    let timings = &archive["timings"];
    assert_eq!((&timings["pass"], &timings["counts"]["passes"], &timings["scope"]), (&json!("archive"), &json!(2), &json!("all")));
    assert!(us(timings, "archive_pass") <= timings["total_us"].as_u64().unwrap());
    let passes: Vec<&Value> = timings["cuts"].as_array().unwrap().iter().map(|c| &c["pass"]).collect();
    assert_eq!(passes, [&json!("hot"), &json!("archive")]);
    // A failed archive cut (Cold never built) is classified by fixed class and tier only.
    let mut down = Fixture::new();
    down.add("Unrelated harbor schedule note.", "knowledge", "2026-10-18T00:00:00Z", 0.5, 0.2, "personal");
    down.projection.rebuild(Tier::Hot, &AtomicBool::new(false)).unwrap();
    let mut cold_down = crate::AppState::new(down.store.clone()).unwrap();
    cold_down.recall = Some(Arc::new(TimedRecall(down.projection.clone())));
    let (status, failed) = runtime.block_on(call_status(&cold_down, "GET", &format!("/recall?q={query}&automatic=true&explain=true"), None));
    assert_eq!(status, axum::http::StatusCode::SERVICE_UNAVAILABLE, "{failed}");
    let line = LOGGED.lock().unwrap().iter().rev()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|line| line["ok"] == false).expect("failed recall is logged");
    assert_eq!((&line["timings"]["failure"], &line["timings"]["failure_tier"]), (&json!("index_unavailable"), &json!("cold")), "{failed} {line}");
    assert_eq!(line["timings"]["cuts"][1]["ok"], false);
    // The log line is exactly the response object; no request text reaches it.
    let logged = LOGGED.lock().unwrap().clone();
    for expected in [&body["timings"], &archive["timings"]] {
        assert!(logged.iter().any(|line| serde_json::from_str::<Value>(line).unwrap() == json!({"event":"recall_timing","ok":true,"timings":expected})));
    }
    let ids: Vec<&str> = body["memories"].as_array().unwrap().iter().map(|r| r["id"].as_str().unwrap()).collect();
    assert!(!ids.is_empty());
    for line in &logged {
        assert!(!line.contains(query) && !line.contains(context) && !line.contains("quartz") && !line.contains("harbor"), "{line}");
        assert!(ids.iter().all(|id| !line.contains(id)), "{line}");
    }
}
