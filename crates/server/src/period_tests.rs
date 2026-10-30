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

/// Pager page fetches (cache hits plus misses) on this read connection since
/// the previous call; overflow pages walked to reach a later column count too.
fn page_fetches(tx: &ReadTxn<'_>) -> i64 {
    use rusqlite::ffi;
    let handle = unsafe { tx.sql().handle() };
    [ffi::SQLITE_DBSTATUS_CACHE_HIT, ffi::SQLITE_DBSTATUS_CACHE_MISS]
        .into_iter()
        .map(|op| {
            let (mut current, mut high) = (0, 0);
            assert_eq!(unsafe { ffi::sqlite3_db_status(handle, op, &mut current, &mut high, 1) }, ffi::SQLITE_OK);
            i64::from(current)
        })
        .sum()
}
/// Native-shaped raw history: large unextracted records inside the window,
/// some without an original date. None of them is memory evidence.
fn raw_history(f: &mut Fixture, count: usize, bytes: usize) {
    let text = "x".repeat(bytes);
    let first = f.seq;
    f.seq += count as i64;
    let context = memento_core::CommandContext { command_id: format!("raw-{first}"), actor: "fixture".into(), why: "Synthetic raw history".into() };
    f.store.command(&context, &json!({"op":"fixture_raw"}), |tx, _| {
        let mut insert = tx.sql().prepare("INSERT INTO episodes(id,source,event_id,conversation_id,kind,text,occurred_at,metadata,captured_at) VALUES(?1,'synthetic:native',?1,'raw','native_record',?2,?3,'{\"extraction_mode\":\"skip\"}',?4)")?;
        for n in 0..count {
            let at = format!("2026-10-{:02}T{:02}:00:00Z", 17 + n % 12, n % 24);
            insert.execute(params![format!("raw-{}", first as usize + n), text, (n % 5 != 0).then_some(&at), at])?;
        }
        Ok(json!({}))
    }).unwrap();
}
fn plan(f: &Fixture, period: &Period) -> Vec<(i64, i64, String)> {
    f.store.read(|tx| {
        let mut statement = tx.sql().prepare(&format!("EXPLAIN QUERY PLAN {} SELECT id FROM in_range", period.sql()))?;
        let (since, until) = period.sql_bounds();
        Ok(statement.query_map(params![since, until, Option::<String>::None], |r| Ok((r.get(0)?, r.get(1)?, r.get(3)?)))?.collect::<rusqlite::Result<Vec<_>>>()?)
    }).unwrap()
}
/// Plan lines below the node whose detail is exactly `root`.
fn subtree(plan: &[(i64, i64, String)], root: &str) -> Vec<String> {
    let top = plan.iter().find(|(_, _, detail)| detail == root).unwrap_or_else(|| panic!("{root}: {plan:?}")).0;
    let mut ids = std::collections::HashSet::from([top]);
    let mut lines = Vec::new();
    for (id, parent, detail) in plan {
        if ids.contains(parent) {
            ids.insert(*id);
            lines.push(detail.clone());
        }
    }
    lines
}

#[test]
fn period_lookup_starts_from_current_memories_through_covering_indexes() {
    let f = Fixture::new();
    for (since, until) in [(Some(WEEK_START), Some(WEEK_END)), (Some(WEEK_START), None), (None, Some(WEEK_END))] {
        let lines = plan(&f, &Period::of(&period_request("", 10, since, until, None)).unwrap().unwrap());
        let text = lines.iter().map(|(_, _, detail)| detail.as_str()).collect::<Vec<_>>().join("\n");
        println!("{since:?} {until:?}\n{text}");
        // The window contributes sequence numbers only, from the expression index.
        assert!(subtree(&lines, "MATERIALIZE win").iter().any(|line| line.starts_with("SEARCH e USING COVERING INDEX episodes_activity")), "{text}");
        // Current identities and their evidence are probed without reading an
        // episode, node or candidate row (so no large raw text is walked).
        for root in ["MATERIALIZE touched", "MATERIALIZE media"] {
            for line in subtree(&lines, root) {
                let table = line.split_whitespace().nth(1).unwrap_or("");
                if (line.starts_with("SEARCH ") || line.starts_with("SCAN ")) && ["e", "g", "node_versions", "candidates"].contains(&table) {
                    assert!(line.contains("COVERING INDEX"), "{root}: {line}\n{text}");
                    // Window membership filters the evidence lookup; it must not
                    // become a per-window-row `rowid` seek inside every probe.
                    assert!(!line.contains("rowid="), "{root}: {line}\n{text}");
                }
            }
        }
        // Never a full raw-history or evidence-table scan.
        for table in ["e", "episodes", "g", "graph_evidence"] {
            assert!(lines.iter().all(|(_, _, line)| !(line == &format!("SCAN {table}") || line.starts_with(&format!("SCAN {table} ")))), "{table}: {text}");
        }
    }
}

#[test]
fn period_lookup_cost_follows_current_memory_evidence_not_raw_history() {
    let mut f = Fixture::new();
    let inside = dated_node(&mut f, "Inventory decision recorded inside the window", &[Some("2026-10-20T00:00:00Z")], 0.9, "personal");
    dated_candidate(&mut f, "period-cost-candidate", "Pending carrier proposal recorded inside the window", "2026-10-21T00:00:00Z", 0.9);
    // Eleven historical versions cite sources inside the window; the current
    // version cites only an older source, so the memory is outside the window.
    let historic = dated_node(&mut f, "Warehouse lease note version 1", &[Some("2026-10-18T00:00:00Z")], 0.9, "personal");
    for version in 2..=12 {
        let text = format!("Warehouse lease note version {version}");
        let date = if version == 12 { "2025-03-01T00:00:00Z" } else { "2026-10-19T00:00:00Z" };
        let source = f.store.capture(serde_json::from_value(json!({"source":"synthetic:period","event_id":format!("lease-{version}"),"conversation_id":"lease","kind":"user","text":text,"occurred_at":date,"metadata":{}})).unwrap()).unwrap();
        f.graph.supersede(serde_json::from_value(json!({"command_id":format!("lease-{version}"),"actor":"owner","why":"Refined lease note","old":historic,"kind":"correction","new":{"type":"knowledge","text":text,"scope":["personal"]},"evidence":[{"episode_id":source.id,"quote":text}]})).unwrap()).unwrap();
    }
    let request = period_request("", 10, Some(WEEK_START), Some(WEEK_END), None);
    let period = Period::of(&request).unwrap().unwrap();
    let measure = |f: &Fixture| f.store.read(|tx| {
        page_fetches(tx);
        let eligible = period::eligible(tx, &period, None)?;
        let eligible_pages = page_fetches(tx);
        let listed = period::list(tx, &request, &period, Utc::now())?;
        Ok((eligible, eligible_pages, listed, page_fetches(tx)))
    }).unwrap();
    let raw = 600;
    // 32 KiB records span several overflow pages; reaching `occurred_at`
    // after `text` in every raw row would cost thousands of page fetches.
    raw_history(&mut f, raw, 32 * 1024);
    let (eligible, eligible_pages, listed, list_pages) = measure(&f);
    raw_history(&mut f, raw, 32 * 1024);
    let (_, more_eligible_pages, more_listed, more_list_pages) = measure(&f);
    println!("raw={raw}: eligible pages {eligible_pages}, list pages {list_pages}; raw={}: eligible pages {more_eligible_pages}, list pages {more_list_pages}", raw * 2);
    let mut admitted = serde_json::from_str::<Vec<String>>(&eligible.ids_json).unwrap();
    admitted.sort();
    let mut expected = vec!["period-cost-candidate".to_owned(), inside.clone()];
    expected.sort();
    assert_eq!(admitted, expected);
    assert_eq!(ids(&listed), ["period-cost-candidate".to_owned(), inside.clone()]);
    assert_eq!(ids(&more_listed), ids(&listed));
    // Fewer page fetches than unrelated raw records, and doubling them adds
    // only window index pages, not a per-record row read.
    assert!(eligible_pages < raw as i64 && list_pages < raw as i64, "{eligible_pages} {list_pages}");
    assert!(more_eligible_pages - eligible_pages < raw as i64 / 20 && more_list_pages - list_pages < raw as i64 / 20, "{eligible_pages}->{more_eligible_pages} {list_pages}->{more_list_pages}");
}

#[test]
#[ignore = "synthetic scale timing; run explicitly"]
fn period_prefilter_synthetic_scale_timing() {
    let f = Fixture::new();
    // Native-shaped: dated message sources for memories spread over a year,
    // plus a recent month of large unextracted native records (some undated).
    let (messages, raw, raw_bytes, nodes, candidates) = (120_000usize, 30_000usize, 16 * 1024, 30_000usize, 20_000usize);
    let start = chrono::DateTime::parse_from_rfc3339("2025-09-25T00:00:00Z").unwrap().with_timezone(&Utc);
    let end = start + chrono::Duration::days(400);
    let context = memento_core::CommandContext { command_id: "scale".into(), actor: "fixture".into(), why: "Synthetic scale".into() };
    let mut state = 12345u64;
    let mut next = move |n: usize| { state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); ((state >> 33) as usize) % n };
    let begun = Instant::now();
    f.store.command(&context, &json!({"op":"fixture_scale"}), |tx, commit| {
        let db = tx.sql();
        let text = "x".repeat(raw_bytes);
        let mut native = db.prepare("INSERT INTO episodes(id,source,event_id,conversation_id,kind,text,occurred_at,metadata,captured_at) VALUES(?1,'synthetic:native',?1,'raw','native_record',?2,?3,'{\"extraction_mode\":\"skip\"}',?4)")?;
        let mut message = db.prepare("INSERT INTO episodes(id,source,event_id,conversation_id,kind,text,occurred_at,metadata,captured_at) VALUES(?1,'synthetic:scale',?1,'scale','user','Synthetic scale text',?2,'{}','2026-10-30T00:00:00Z')")?;
        let raw_start = end - chrono::Duration::days(30);
        for n in 0..messages.max(raw) {
            if n < messages {
                let at = start + chrono::Duration::seconds((n as i64) * 400 * 86400 / messages as i64);
                let occurred = (n % 20 != 0).then(|| at.to_rfc3339_opts(chrono::SecondsFormat::Secs, false));
                message.execute(params![format!("e{n}"), occurred])?;
            }
            if n < raw {
                let at = (raw_start + chrono::Duration::seconds((n as i64) * 30 * 86400 / raw as i64)).to_rfc3339_opts(chrono::SecondsFormat::Secs, false);
                native.execute(params![format!("r{n}"), text, (n % 10 != 0).then_some(&at), at])?;
            }
        }
        let mut node = db.prepare("INSERT INTO node_versions(id,version,commit_id,type,text,state,pinned,scope) VALUES(?1,?2,?3,'knowledge','Synthetic scale memory','active',0,'[\"personal\"]')")?;
        let mut evidence = db.prepare("INSERT INTO graph_evidence VALUES(?1,?2,?3,?4,?4+20,'Synthetic scale text')")?;
        let mut item = db.prepare("INSERT INTO vector_items(target,kind,version) VALUES(?1,?2,?3)")?;
        for n in 0..nodes {
            let id = format!("n{n}");
            // Historical versions cite their own sources; only the last is current.
            let versions = 1 + next(3) as i64;
            for version in 1..=versions {
                node.execute(params![id, version, commit])?;
                for span in 0..=next(3) { evidence.execute(params![id, version, format!("e{}", next(messages)), span])?; }
            }
            item.execute(params![id, "node", versions])?;
        }
        db.execute("INSERT INTO extraction_runs VALUES('scale-run',?1,'e1','test','test','[]','{}','2026-10-30T00:00:00Z')", [commit])?;
        let mut candidate = db.prepare("INSERT INTO candidates VALUES(?1,'scale-run','memory','knowledge','Synthetic scale candidate','Exact',NULL,NULL,1,NULL,NULL,'[\"personal\"]')")?;
        for n in 0..candidates {
            let id = format!("c{n}");
            candidate.execute([&id])?;
            evidence.execute(params![id, 1, format!("e{}", next(messages)), 0])?;
            item.execute(params![id, "candidate", 1])?;
        }
        Ok(json!({}))
    }).unwrap();
    println!("populated {messages} dated messages, {raw} raw native records of {raw_bytes} bytes, {nodes} nodes with history, {candidates} candidates in {:?}", begun.elapsed());
    for (label, since, until) in [
        ("14 days", Some(end - chrono::Duration::days(14)), None),
        ("90 days", Some(end - chrono::Duration::days(90)), None),
        ("closed 14 days, mid-history", Some(start + chrono::Duration::days(100)), Some(start + chrono::Duration::days(114))),
        ("all known", Some(start - chrono::Duration::days(1)), None),
        ("until-only first 14 days", None, Some(start + chrono::Duration::days(14))),
    ] {
        let text = |at: Option<DateTime<Utc>>| at.map(|at| at.to_rfc3339());
        let mut request = period_request("", 10, text(since).as_deref(), text(until).as_deref(), None);
        request.effort = "cheap".into();
        let period = Period::of(&request).unwrap().unwrap();
        let (eligible, eligible_pages, eligible_time, listed, list_pages, list_time) = f.store.read(|tx| {
            page_fetches(tx);
            let timed = Instant::now();
            let eligible = period::eligible(tx, &period, None)?;
            let (eligible_pages, eligible_time) = (page_fetches(tx), timed.elapsed());
            let timed = Instant::now();
            let listed = period::list(tx, &request, &period, Utc::now())?;
            Ok((eligible, eligible_pages, eligible_time, listed, page_fetches(tx), timed.elapsed()))
        }).unwrap();
        println!("{label}: eligible ids={} points={} pages={eligible_pages} in {eligible_time:?}; period-only list={} pages={list_pages} in {list_time:?}", serde_json::from_str::<Vec<String>>(&eligible.ids_json).unwrap().len(), eligible.points.len(), listed.len());
    }
}
