// Stored-memory period recall over synthetic sources and synthetic vectors.
// Vector values only control relevance order; dates come from original evidence.
const WEEK_START: &str = "2026-10-16T00:00:00Z";
const WEEK_END: &str = "2026-10-30T00:00:00Z";
fn dated_node(f: &mut Fixture, text: &str, dates: &[Option<&str>], cosine: f32, scope: &str) -> String {
    let mut evidence = Vec::new();
    for date in dates {
        f.seq += 1;
        let event = format!("period-{}", f.seq);
        let episode = f.store.capture(serde_json::from_value(json!({"source":"synthetic:period","event_id":event,"conversation_id":event,"kind":"user","text":text,"occurred_at":date,"metadata":{}})).unwrap()).unwrap();
        evidence.push(json!({"episode_id":episode.id,"quote":text}));
    }
    f.seq += 1;
    let node = f.graph.remember(serde_json::from_value(json!({"command_id":format!("period-node-{}",f.seq),"actor":"owner","why":"Exact synthetic period evidence","type":"knowledge","text":text,"scope":[scope],"evidence":evidence})).unwrap()).unwrap();
    let id = key(&node).to_owned();
    let mut vector = [0f32; 768];
    vector[0] = cosine;
    vector[1] = (1. - cosine * cosine).sqrt();
    let seq = f.seq;
    f.store.write(|tx| tx.put_vector(&VectorWrite{seq,target:id.clone(),kind:VectorKind::Node,version:1,embedding:F32Blob::from_values(&vector)?,current:true,tier:Tier::Hot})).unwrap();
    id
}
fn dated_candidate(f: &mut Fixture, id: &str, text: &str, date: &str, cosine: f32) {
    f.seq += 1;
    let event = format!("period-candidate-{}", f.seq);
    let episode = f.store.capture(serde_json::from_value(json!({"source":"synthetic:period","event_id":event,"conversation_id":event,"kind":"user","text":text,"occurred_at":date,"metadata":{}})).unwrap()).unwrap();
    let context = memento_core::CommandContext { command_id: event.clone(), actor: "fixture".into(), why: "Synthetic extraction".into() };
    let mut vector = [0f32; 768];
    vector[0] = cosine;
    vector[1] = (1. - cosine * cosine).sqrt();
    let seq = 5000 + f.seq;
    f.store.command(&context, &json!({"op":"fixture_candidate"}), |tx, commit| {
        let run = format!("run-{id}");
        tx.sql().execute("INSERT INTO extraction_runs VALUES(?,?,?,'test','test','[]','{}','2026-10-30T00:00:00Z')", params![run, commit, episode.id])?;
        tx.sql().execute("INSERT INTO candidates VALUES(?,?,'memory','knowledge',?,'Exact source',NULL,NULL,1,NULL,NULL,'[\"personal\"]')", params![id, run, text])?;
        tx.sql().execute("INSERT INTO graph_evidence VALUES(?,1,?,0,?,?)", params![id, episode.id, text.chars().count() as i64, text])?;
        tx.touch_target(id)?;
        tx.put_vector(&VectorWrite{seq,target:id.into(),kind:VectorKind::Candidate,version:1,embedding:F32Blob::from_values(&vector)?,current:true,tier:Tier::Hot})?;
        Ok(json!({"id":id}))
    }).unwrap();
}
fn period_request(query: &str, limit: usize, since: Option<&str>, until: Option<&str>, scope: Option<&str>) -> RecallRequest {
    RecallRequest {
        query: query.into(),
        limit,
        scope: scope.map(str::to_owned),
        memory_since: since.map(str::to_owned),
        memory_until: until.map(str::to_owned),
        ..Default::default()
    }
}
/// Semantic + period through the same snapshot path recall uses.
fn semantic(f: &Fixture, request: &RecallRequest) -> Vec<Value> {
    f.projection.rebuild(Tier::Hot, &AtomicBool::new(false)).unwrap();
    let mut vector = [0f32; 768];
    vector[0] = 1.;
    let deadline = Instant::now() + std::time::Duration::from_secs(5);
    f.projection
        .snapshot_until(true, false, deadline, |tx, view| Retrieval::current(tx, view, request, &vector, false, deadline))
        .unwrap()
        .0
}
/// Period-only listing; no vector, model or ANN index.
fn listed(f: &Fixture, request: &RecallRequest) -> Vec<Value> {
    let period = Period::of(request).unwrap().unwrap();
    f.store.read(|tx| period::list(tx, request, &period, Utc::now())).unwrap()
}
fn ids(rows: &[Value]) -> Vec<String> {
    rows.iter().map(|row| key(row).to_owned()).collect()
}

#[test]
fn period_eligibility_precedes_the_top_n_cut_for_semantic_recall() {
    let mut f = Fixture::new();
    let mut closer = Vec::new();
    for n in 0..25 {
        closer.push(dated_node(&mut f, &format!("Archived orbital note number {n} about telescope storage"), &[Some("2026-09-01T09:00:00Z")], 0.97, "personal"));
    }
    for n in 0..5 {
        closer.push(dated_node(&mut f, &format!("Later orbital note number {n} about telescope storage"), &[Some("2026-10-30T09:00:00Z")], 0.98, "personal"));
    }
    let inside = dated_node(&mut f, "Recent telescope storage decision for the observatory", &[Some("2026-10-20T10:00:00+02:00")], 0.8, "personal");
    // Control: a post-hoc date filter over the ordinary top-n would see only distractors.
    let unfiltered = semantic(&f, &period_request("unrelated probe", 1, None, None, Some("personal")));
    assert!(closer.contains(&key(&unfiltered[0]).to_owned()));
    let request = period_request("unrelated probe", 1, Some(WEEK_START), Some(WEEK_END), Some("personal"));
    let rows = semantic(&f, &request);
    assert_eq!(ids(&rows), [inside.clone()]);
    assert_eq!(rows[0]["source_occurred_at"], "2026-10-20T08:00:00+00:00");
    assert_eq!(rows[0]["tier"], "durable");
    assert_eq!(rows[0]["evidence"][0]["quote_excerpt"], "Recent telescope storage decision for the observatory");
    assert!((number(&rows[0], "cosine", 0.) - 0.8).abs() < 1e-6);
    // Unscoped and period-only paths agree on the same window.
    assert_eq!(ids(&semantic(&f, &period_request("unrelated probe", 1, Some(WEEK_START), Some(WEEK_END), None))), [inside.clone()]);
    assert_eq!(ids(&listed(&f, &request)), [inside]);
}

#[test]
fn period_lexical_admission_is_restricted_before_its_bm25_limit() {
    let mut f = Fixture::new();
    for n in 0..12 {
        dated_node(&mut f, &format!("zephyr zephyr {n}"), &[Some("2026-01-01T00:00:00Z")], 0.1, "personal");
    }
    let inside = dated_node(&mut f, "The zephyr ticket was finally reconciled in the warehouse audit this week", &[Some("2026-10-21T00:00:00Z")], 0.1, "personal");
    let unfiltered = semantic(&f, &period_request("zephyr", 1, None, None, Some("personal")));
    assert!(!ids(&unfiltered).contains(&inside));
    let rows = semantic(&f, &period_request("zephyr", 1, Some(WEEK_START), Some(WEEK_END), Some("personal")));
    assert_eq!(ids(&rows), [inside]);
    assert_eq!(rows[0]["match"], "keyword");
}

#[test]
fn period_boundaries_are_half_open_timezone_normalized_and_microsecond_exact() {
    let mut f = Fixture::new();
    let start = dated_node(&mut f, "Exactly at the start of the reporting window", &[Some("2026-10-16T02:00:00+02:00")], 0.9, "personal");
    let before = dated_node(&mut f, "One microsecond before the reporting window", &[Some("2026-10-15T23:59:59.999999Z")], 0.9, "personal");
    let last = dated_node(&mut f, "One microsecond before the reporting window closes", &[Some("2026-10-29T23:59:59.999999Z")], 0.9, "personal");
    let end = dated_node(&mut f, "Exactly at the end of the reporting window", &[Some("2026-10-30T00:00:00Z")], 0.9, "personal");
    for (since, until) in [
        (WEEK_START, WEEK_END),
        ("2026-10-16T03:00:00+03:00", "2026-10-29T19:00:00-05:00"),
        ("2026-10-16 00:00:00+00:00", "2026-10-30 00:00:00+00:00"),
    ] {
        let request = period_request("", 10, Some(since), Some(until), None);
        assert_eq!(ids(&listed(&f, &request)), [last.clone(), start.clone()], "{since} {until}");
        let mut found = ids(&semantic(&f, &period_request("reporting probe", 10, Some(since), Some(until), None)));
        found.sort();
        let mut expected = vec![start.clone(), last.clone()];
        expected.sort();
        assert_eq!(found, expected, "{since} {until}");
    }
    // Single bounds are open on the other side.
    assert_eq!(ids(&listed(&f, &period_request("", 10, Some(WEEK_END), None, None))), [end.clone()]);
    assert_eq!(ids(&listed(&f, &period_request("", 10, None, Some(WEEK_START), None))), [before.clone()]);
    assert_eq!(ids(&listed(&f, &period_request("", 2, Some("2000-01-01T00:00:00Z"), None, None))), [end, last]);
}

#[test]
fn period_excludes_rounded_boundary_neighbours_before_search_limits() {
    let mut f = Fixture::new();
    for n in 0..24 {
        dated_node(&mut f, &format!("Boundary boundary noise {n}"), &[Some("2026-10-15T23:59:59.999999Z")], 0.99, "personal");
        dated_node(&mut f, &format!("Boundary boundary future {n}"), &[Some(WEEK_END)], 0.99, "personal");
    }
    let inside = dated_node(&mut f, "Boundary review completed inside the reporting window", &[Some("2026-10-21T00:00:00Z")], 0.8, "personal");
    let request = period_request("unrelated semantic probe", 1, Some(WEEK_START), Some(WEEK_END), None);
    let period = Period::of(&request).unwrap().unwrap();
    let admitted = f.store.read(|tx| period::eligible(tx, &period, None)).unwrap();
    assert_eq!(serde_json::from_str::<Vec<String>>(&admitted.ids_json).unwrap(), [inside.clone()]);
    assert_eq!(admitted.points.len(), 1);
    assert_eq!(ids(&semantic(&f, &request)), [inside.clone()]);
    // The same exact eligible IDs fence lexical admission before its bm25 cut.
    assert_eq!(ids(&semantic(&f, &period_request("boundary", 1, Some(WEEK_START), Some(WEEK_END), None))), [inside]);
}

#[test]
fn period_uses_latest_original_source_time_not_capture_import_or_citation() {
    let mut f = Fixture::new();
    // Captured today, but the original conversation happened in January 2025.
    let imported = dated_node(&mut f, "Imported January planning note about the supplier", &[Some("2025-01-05T12:00:00Z")], 0.9, "personal");
    let unknown = dated_node(&mut f, "Undated statement about the supplier contract", &[None], 0.9, "personal");
    let mixed = dated_node(&mut f, "Supplier contract mentioned in January and again in October", &[Some("2025-01-06T12:00:00Z"), None, Some("2026-10-22T12:00:00Z")], 0.9, "personal");
    // Citation renewal changes ranking only; it never moves the source date.
    f.store.write(|tx| { tx.sql().execute("INSERT INTO node_stats(node_id,last_cited_at) VALUES(?,?) ON CONFLICT(node_id) DO UPDATE SET last_cited_at=excluded.last_cited_at", params![imported, "2026-10-25T00:00:00+00:00"])?; Ok(()) }).unwrap();
    let recent = period_request("", 10, Some(WEEK_START), Some(WEEK_END), None);
    let rows = listed(&f, &recent);
    assert_eq!(ids(&rows), [mixed.clone()]);
    assert_eq!(rows[0]["source_occurred_at"], "2026-10-22T12:00:00+00:00");
    assert_eq!(ids(&semantic(&f, &period_request("supplier probe", 10, Some(WEEK_START), Some(WEEK_END), None))), [mixed.clone()]);
    // The latest known source decides: the mixed memory is not a January memory.
    let january = period_request("", 10, Some("2025-01-01T00:00:00Z"), Some("2025-02-01T00:00:00Z"), None);
    assert_eq!(ids(&listed(&f, &january)), [imported.clone()]);
    // Unknown source dates never satisfy an explicit range, even an open one.
    let everything = ids(&listed(&f, &period_request("", 10, Some("1970-01-01T00:00:00Z"), None, None)));
    assert!(!everything.contains(&unknown));
    assert_eq!(everything, [mixed, imported]);
    // Unfiltered recall still returns the undated memory.
    assert!(ids(&semantic(&f, &period_request("supplier probe", 10, None, None, None))).contains(&unknown));
}

#[test]
fn period_keeps_scope_forgetting_supersession_and_candidate_guards() {
    let mut f = Fixture::new();
    let personal = dated_node(&mut f, "Personal errand recorded during the window", &[Some("2026-10-20T00:00:00Z")], 0.9, "personal");
    let professional = dated_node(&mut f, "Professional review recorded during the window", &[Some("2026-10-21T00:00:00Z")], 0.9, "professional");
    let forgotten = dated_node(&mut f, "Forgotten detail recorded during the window", &[Some("2026-10-22T00:00:00Z")], 0.95, "personal");
    f.store.write(|tx| { tx.sql().execute("INSERT INTO forgotten_targets SELECT ?1,commit_id,'node' FROM node_versions WHERE id=?1", [&forgotten])?; Ok(()) }).unwrap();
    // Version 1 evidence is inside the window; the corrected current version is not.
    let revised = dated_node(&mut f, "Shipment cadence was weekly in the window", &[Some("2026-10-23T00:00:00Z")], 0.95, "personal");
    let text = "Shipment cadence is monthly.";
    let source = f.store.capture(serde_json::from_value(json!({"source":"synthetic:period","event_id":"cadence","conversation_id":"cadence","kind":"user","text":text,"occurred_at":"2026-11-02T00:00:00Z","metadata":{}})).unwrap()).unwrap();
    let corrected = f.graph.supersede(serde_json::from_value(json!({"command_id":"cadence","actor":"owner","why":"Corrected cadence","old":revised,"kind":"correction","new":{"type":"knowledge","text":text,"scope":["personal"]},"evidence":[{"episode_id":source.id,"quote":text}]})).unwrap()).unwrap();
    assert_eq!(corrected["version"], 2);
    current_vector(&f.store, &revised, 2, 7000);
    dated_candidate(&mut f, "period-pending", "Pending proposal captured inside the window", "2026-10-24T00:00:00Z", 0.85);
    dated_candidate(&mut f, "period-rejected", "Rejected proposal captured inside the window", "2026-10-25T00:00:00Z", 0.85);
    f.store.write(|tx| { tx.sql().execute("INSERT INTO candidate_decisions VALUES('decision-rejected','period-rejected',(SELECT commit_id FROM extraction_runs WHERE id='run-period-rejected'),'reject',NULL,NULL)", [])?; Ok(()) }).unwrap();
    for effort in ["cheap", "normal", "deep"] {
        let mut request = period_request("", 10, Some(WEEK_START), Some(WEEK_END), None);
        request.effort = effort.into();
        let rows = listed(&f, &request);
        assert_eq!(ids(&rows), ["period-pending".to_owned(), professional.clone(), personal.clone()], "{effort}");
        assert_eq!((rows[0]["tier"].as_str(), rows[0]["confirmed"].as_bool()), (Some("candidate"), Some(false)));
        assert!(rows.iter().all(|row| row["type"] != "raw_episode" && row["tier"] != "raw"));
        let mut request = period_request("window probe", 10, Some(WEEK_START), Some(WEEK_END), None);
        request.effort = effort.into();
        let mut found = ids(&semantic(&f, &request));
        found.sort();
        let mut expected = vec!["period-pending".to_owned(), professional.clone(), personal.clone()];
        expected.sort();
        assert_eq!(found, expected, "{effort}");
    }
    let scoped = period_request("", 10, Some(WEEK_START), Some(WEEK_END), Some("personal"));
    assert_eq!(ids(&listed(&f, &scoped)), ["period-pending".to_owned(), personal.clone()]);
    assert_eq!(ids(&semantic(&f, &period_request("window probe", 10, Some(WEEK_START), Some(WEEK_END), Some("professional")))), [professional]);
    // The corrected current version is found by its own source period.
    let november = listed(&f, &period_request("", 10, Some("2026-11-01T00:00:00Z"), None, None));
    assert_eq!((ids(&november), november[0]["version"].as_i64(), november[0]["text"].as_str()), (vec![revised], Some(2), Some(text)));
}

#[test]
fn period_validation_rejects_naive_malformed_and_empty_or_reversed_ranges() {
    for (since, until, message) in [
        (Some("2026-10-16T00:00:00"), None, "Memory period bounds must be aware ISO timestamps"),
        (Some("last two weeks"), None, "Memory period bounds must be aware ISO timestamps"),
        (None, Some(""), "Memory period bounds must be aware ISO timestamps"),
        (Some(WEEK_END), Some(WEEK_START), "Memory period start must be before its end"),
        (Some("2026-10-16T02:00:00+02:00"), Some(WEEK_START), "Memory period start must be before its end"),
    ] {
        let error = Period::of(&period_request("", 10, since, until, None)).unwrap_err();
        assert_eq!(error.to_string(), message);
    }
    assert!(Period::of(&period_request("", 10, None, None, None)).unwrap().is_none());
}

#[test]
fn period_prefilter_uses_existing_activity_and_evidence_indexes() {
    let f = Fixture::new();
    for (since, until) in [(Some(WEEK_START), Some(WEEK_END)), (Some(WEEK_START), None), (None, Some(WEEK_END))] {
    let period = Period::of(&period_request("", 10, since, until, None)).unwrap().unwrap();
    let plan = f.store.read(|tx| {
        let sql = format!("EXPLAIN QUERY PLAN {} SELECT id FROM in_range", period.sql());
        let mut statement = tx.sql().prepare(&sql)?;
        let (since, until) = period.sql_bounds();
        let rows = statement.query_map(params![since, until, Option::<String>::None], |r| r.get::<_, String>(3))?.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows.join("\n"))
    }).unwrap();
    println!("{plan}");
    assert!(plan.contains("episodes_activity"), "{plan}");
    assert!(plan.contains("search_evidence_episode"), "{plan}");
    // No full episode, node or candidate scan: the window drives every lookup.
    for table in ["e", "episodes", "n", "node_versions", "c", "candidates", "g", "graph_evidence"] {
        assert!(plan.lines().all(|line| !(line == format!("SCAN {table}") || line.starts_with(&format!("SCAN {table} ")))), "{table}: {plan}");
    }
    }
}

#[test]
#[ignore = "synthetic scale timing; run explicitly"]
fn period_prefilter_synthetic_scale_timing() {
    let f = Fixture::new();
    let (episodes, nodes, candidates) = (200_000usize, 30_000usize, 20_000usize);
    let start = chrono::DateTime::parse_from_rfc3339("2025-09-25T00:00:00Z").unwrap().with_timezone(&Utc);
    let context = memento_core::CommandContext { command_id: "scale".into(), actor: "fixture".into(), why: "Synthetic scale".into() };
    let mut state = 12345u64;
    let mut next = move |n: usize| { state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); ((state >> 33) as usize) % n };
    let begun = Instant::now();
    f.store.command(&context, &json!({"op":"fixture_scale"}), |tx, commit| {
        let db = tx.sql();
        let mut episode = db.prepare("INSERT INTO episodes(id,source,event_id,conversation_id,kind,text,occurred_at,metadata,captured_at) VALUES(?1,'synthetic:scale',?1,'scale','user','Synthetic scale text',?2,'{}','2026-10-30T00:00:00Z')")?;
        for n in 0..episodes {
            let at = start + chrono::Duration::seconds((n as i64) * 172);
            let occurred = (n % 20 != 0).then(|| at.to_rfc3339_opts(chrono::SecondsFormat::Secs, false));
            episode.execute(params![format!("e{n}"), occurred])?;
        }
        let mut node = db.prepare("INSERT INTO node_versions(id,version,commit_id,type,text,state,pinned,scope) VALUES(?1,1,?2,'knowledge','Synthetic scale memory','active',0,'[\"personal\"]')")?;
        let mut evidence = db.prepare("INSERT OR IGNORE INTO graph_evidence VALUES(?1,1,?2,0,20,'Synthetic scale text')")?;
        let mut item = db.prepare("INSERT INTO vector_items(target,kind,version) VALUES(?1,?2,1)")?;
        for n in 0..nodes {
            let id = format!("n{n}");
            node.execute(params![id, commit])?;
            for _ in 0..=next(3) { evidence.execute(params![id, format!("e{}", next(episodes))])?; }
            item.execute(params![id, "node"])?;
        }
        db.execute("INSERT INTO extraction_runs VALUES('scale-run',?1,'e1','test','test','[]','{}','2026-10-30T00:00:00Z')", [commit])?;
        let mut candidate = db.prepare("INSERT INTO candidates VALUES(?1,'scale-run','memory','knowledge','Synthetic scale candidate','Exact',NULL,NULL,1,NULL,NULL,'[\"personal\"]')")?;
        for n in 0..candidates {
            let id = format!("c{n}");
            candidate.execute([&id])?;
            evidence.execute(params![id, format!("e{}", next(episodes))])?;
            item.execute(params![id, "candidate"])?;
        }
        Ok(json!({}))
    }).unwrap();
    println!("populated {episodes} episodes, {nodes} nodes, {candidates} candidates in {:?}", begun.elapsed());
    let end = start + chrono::Duration::seconds((episodes as i64) * 172);
    for (label, since, until) in [
        ("14 days", Some(end - chrono::Duration::days(14)), None),
        ("90 days", Some(end - chrono::Duration::days(90)), None),
        ("closed 14 days, mid-history", Some(start + chrono::Duration::days(100)), Some(start + chrono::Duration::days(114))),
        ("all known", Some(start - chrono::Duration::days(1)), None),
        ("until-only first 14 days", None, Some(start + chrono::Duration::days(14))),
    ] {
        let text = |at: Option<DateTime<Utc>>| at.map(|at| at.to_rfc3339());
        let request = period_request("", 10, text(since).as_deref(), text(until).as_deref(), None);
        let period = Period::of(&request).unwrap().unwrap();
        let plan = f.store.read(|tx| {
            let mut statement = tx.sql().prepare(&format!("EXPLAIN QUERY PLAN {} SELECT id FROM in_range", period.sql()))?;
            let (since, until) = period.sql_bounds();
            let rows = statement.query_map(params![since, until, Option::<String>::None], |r| r.get::<_, String>(3))?.collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows.into_iter().filter(|line| line.starts_with("SCAN") || line.contains("episodes_activity")).collect::<Vec<_>>().join(" | "))
        }).unwrap();
        println!("{label} plan: {plan}");
        let timed = Instant::now();
        let eligible = f.store.read(|tx| period::eligible(tx, &period, None)).unwrap();
        let eligible_time = timed.elapsed();
        let timed = Instant::now();
        let listed = listed(&f, &request);
        println!("{label}: eligible ids={} points={} in {eligible_time:?}; period-only list={} in {:?}", serde_json::from_str::<Vec<String>>(&eligible.ids_json).unwrap().len(), eligible.points.len(), listed.len(), timed.elapsed());
    }
}
