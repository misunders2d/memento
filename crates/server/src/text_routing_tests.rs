#[test]
fn semantic_text_filter_preserves_literal_entity_behavior_media_and_public_scores() {
    let f = Fixture::new();
    let lesson = "validated-fixture-lesson".to_owned();
    let text = "Use distinct fixture identifiers for independent operations.";
    let source = f.store.capture(serde_json::from_value(json!({"source":"synthetic","event_id":"behavior-provenance","conversation_id":"behavior-provenance","kind":"user","text":text,"metadata":{}})).unwrap()).unwrap();
    // Append the first version and its exact evidence through the command store;
    // never retrofit behavior by updating immutable history created by remember.
    let context = memento_core::CommandContext {
        command_id: "behavior-provenance".into(),
        actor: "fixture".into(),
        why: "Exact synthetic behavior provenance".into(),
    };
    // Store::command supplies the actor/why/command envelope, exactly as the
    // consolidation writer does; the supplied request is the bare command.
    let request = json!({"op":"consolidate","output":{"actions":[{"candidate_id":"lesson","operation":"remember","type":"experience","behavior":{"authority":"explicit_user","applicability":"Independent persisted fixture operations","conduct":text,"exceptions":[],"rationale":"Exact fixture user instruction"}}]}});
    f.store.command(&context,&request,|tx,commit| {
        tx.touch_target(&lesson)?;
        tx.sql().execute("INSERT INTO node_versions(id,version,commit_id,type,text,state,pinned,scope) VALUES(?,1,?,'experience',?,'active',0,'[\"personal\"]')",params![lesson,commit,text])?;
        tx.sql().execute("INSERT INTO graph_evidence(target,version,episode_id,start,end,quote) VALUES(?,1,?,0,?,?)",params![lesson,source.id,text.chars().count() as i64,text])?;
        Ok(json!({"results":{"lesson":{"id":lesson}}}))
    }).unwrap();
    f.store
        .read(|tx| {
            let stored = behavior(tx.sql(), &lesson)?
                .expect("Fixture must have persisted behavior before routing");
            assert_eq!(stored["authority"], "explicit_user");
            assert_eq!(stored["conduct"], text);
            Ok(())
        })
        .unwrap();
    let mut rows = BTreeMap::new();
    for (id, route, extra) in [
        ("semantic-reject", "semantic", json!({})),
        ("semantic-keep", "semantic", json!({})),
        ("literal", "keyword", json!({})),
        ("hybrid", "hybrid", json!({})),
        (
            "exact-entity",
            "semantic",
            json!({"category":"entity_context"}),
        ),
        (
            "partial-entity",
            "partial_entity_name",
            json!({"category":"entity_context"}),
        ),
        (
            "suggestion",
            "semantic",
            json!({"category":"behavioral_suggestion"}),
        ),
        (
            "behavior",
            "semantic",
            json!({"behavior":{"authority":"explicit_user"}}),
        ),
        (
            "unvalidated-experience",
            "semantic",
            json!({"type":"experience","tier":"candidate"}),
        ),
        (
            "image",
            "semantic",
            json!({"type":"media","modality":"image"}),
        ),
        (
            "audio",
            "semantic",
            json!({"type":"media","modality":"audio"}),
        ),
    ] {
        let mut row =
            json!({"id":id,"type":"knowledge","text":id,"match":route,"score":0.8,"cosine":0.8});
        records::merge(&mut row, extra);
        rows.insert(id.to_owned(), row);
    }
    rows.insert(lesson.clone(),json!({"id":lesson,"type":"experience","text":"Validated behavior before finish","match":"semantic","score":0.8,"cosine":0.8}));
    let before = rows.clone();
    let calls = std::cell::RefCell::new(Vec::<String>::new());
    let score = |documents: &[&str], _: Instant| {
        assert!(documents.len() <= MAX_CANDIDATES);
        calls
            .borrow_mut()
            .extend(documents.iter().map(|s| s.to_string()));
        Ok(documents
            .iter()
            .map(|s| if *s == "semantic-keep" { 1. } else { -9. })
            .collect())
    };
    f.store
        .read(|tx| {
            filter_text_passages(
                tx.sql(),
                &mut rows,
                10,
                Instant::now() + std::time::Duration::from_secs(5),
                Some(&score),
                &HashSet::new(),
            )
        })
        .unwrap();
    assert!(!rows.contains_key("semantic-reject"));
    for (id, row) in &rows {
        assert_eq!(row, &before[id]);
    }
    let mut called = calls.into_inner();
    called.sort();
    assert_eq!(
        called,
        vec!["semantic-keep", "semantic-reject", "unvalidated-experience"]
    );
}

#[test]
fn semantic_text_or_only_discovery_is_scored_without_changing_true_and_literal_output() {
    let mut f = Fixture::new();
    // Exactly one of the two distinct query terms, despite a higher dense score.
    let unrelated_text = "cedar trousers shipment concerns delivery paperwork";
    let unrelated = f.add(
        unrelated_text,
        "knowledge",
        "2026-10-26T00:00:00Z",
        0.5,
        0.99,
        "personal",
    );
    let supported = f.add(
        "cedar oxygen storage is the literal supported fixture",
        "knowledge",
        "2026-10-26T00:00:00Z",
        0.5,
        0.9,
        "personal",
    );
    f.projection
        .rebuild(Tier::Hot, &AtomicBool::new(false))
        .unwrap();
    let mut vector = [0.; 768];
    vector[0] = 1.;
    for automatic in [false, true] {
        let request = RecallRequest {
            query: "cedar oxygen".into(),
            automatic,
            scope: Some("personal".into()),
            ..Default::default()
        };
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        let baseline = f
            .projection
            .snapshot_until(true, false, deadline, |tx, view| {
                Retrieval::current(tx, view, &request, &vector, false, deadline)
            })
            .unwrap()
            .0;
        assert_eq!(baseline.len(), 2);
        let original_negative = baseline.iter().find(|r| r["id"] == unrelated).unwrap();
        let original_literal = baseline.iter().find(|r| r["id"] == supported).unwrap();
        assert_eq!(original_negative["match"], "hybrid");
        assert_eq!(original_literal["match"], "hybrid");
        let called = std::cell::RefCell::new(Vec::new());
        let score = |documents: &[&str], _: Instant| {
            called
                .borrow_mut()
                .extend(documents.iter().map(|s| s.to_string()));
            // A true AND literal must survive even a scorer rejecting every input.
            Ok(vec![-9.; documents.len()])
        };
        let rows = f
            .projection
            .snapshot_until(true, false, deadline, |tx, view| {
                Retrieval::current_scored(
                    tx,
                    view,
                    &request,
                    &vector,
                    false,
                    deadline,
                    Some(&score),
                )
            })
            .unwrap()
            .0;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], supported);
        assert_eq!(called.into_inner(), vec![unrelated_text.to_owned()]);
        for field in [
            "id", "version", "type", "text", "cosine", "score", "match", "category", "scope",
            "evidence",
        ] {
            assert_eq!(
                rows[0][field], original_literal[field],
                "literal field {field}"
            );
        }
    }
}

#[test]
fn semantic_text_rejection_precedes_raw_fallback_without_changing_literal_or_scoped_paths() {
    let mut f = Fixture::new();
    let curated = f.add(
        "Irrelevant curated phantom record",
        "knowledge",
        "2026-10-26T00:00:00Z",
        0.5,
        0.98,
        "personal",
    );
    let raw = raw_point(
        &f,
        "text-filter-raw",
        (
            "synthetic",
            "user",
            "Supported brine density source evidence",
        ),
        json!({}),
        0.9,
        -1,
    );
    f.projection
        .rebuild(Tier::Hot, &AtomicBool::new(false))
        .unwrap();
    f.projection
        .rebuild(Tier::Cold, &AtomicBool::new(false))
        .unwrap();
    let score = |documents: &[&str], _: Instant| {
        Ok(documents
            .iter()
            .map(|s| if s.contains("phantom") { -9. } else { 1. })
            .collect())
    };
    let mut vector = [0.; 768];
    vector[0] = 1.;
    for automatic in [false, true] {
        let mut request = RecallRequest {
            query: "unseen paraphrase".into(),
            automatic,
            ..Default::default()
        };
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        let first = f
            .projection
            .snapshot_until(true, false, deadline, |tx, view| {
                Retrieval::current_scored(
                    tx,
                    view,
                    &request,
                    &vector,
                    false,
                    deadline,
                    Some(&score),
                )
            })
            .unwrap();
        assert!(first.0.is_empty());
        assert!(first.1);
        let rows = f
            .projection
            .snapshot_until(true, true, deadline, |tx, view| {
                Retrieval::current_scored(tx, view, &request, &vector, true, deadline, Some(&score))
            })
            .unwrap()
            .0;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], raw.chunk);
        assert_eq!(rows[0]["episode_id"], raw.episode);
        assert!((number(&rows[0], "cosine", 0.) - 0.9).abs() < 1e-6);
        assert!(rows.iter().all(|r| r["id"] != curated));
        request.scope = Some("personal".into());
        let scoped = f
            .projection
            .snapshot_until(true, true, deadline, |tx, view| {
                Retrieval::current_scored(tx, view, &request, &vector, true, deadline, Some(&score))
            })
            .unwrap();
        assert!(scoped.0.is_empty());
        assert!(!scoped.1);
    }
    for deep in [false, true] {
        let request = RecallRequest {
            query: "brine".into(),
            effort: if deep { "deep" } else { "normal" }.into(),
            ..Default::default()
        };
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        let reject = |documents: &[&str], _: Instant| Ok(vec![-9.; documents.len()]);
        let rows = f
            .projection
            .snapshot_until(true, true, deadline, |tx, view| {
                Retrieval::current_scored(
                    tx,
                    view,
                    &request,
                    &vector,
                    true,
                    deadline,
                    Some(&reject),
                )
            })
            .unwrap()
            .0;
        assert!(rows.iter().any(|r| r["episode_id"] == raw.episode
            && ["keyword", "hybrid"].contains(&r["match"].as_str().unwrap_or(""))));
    }
    let request = RecallRequest {
        query: "unseen paraphrase".into(),
        effort: "deep".into(),
        ..Default::default()
    };
    let deadline = Instant::now() + std::time::Duration::from_secs(5);
    let reject = |documents: &[&str], _: Instant| Ok(vec![-9.; documents.len()]);
    let rows = f
        .projection
        .snapshot_until(true, true, deadline, |tx, view| {
            Retrieval::current_scored(tx, view, &request, &vector, true, deadline, Some(&reject))
        })
        .unwrap()
        .0;
    assert!(rows.is_empty());
}

#[test]
fn semantic_text_empty_passages_do_not_poison_valid_scoring_or_typed_bypasses() {
    let f = Fixture::new();
    let mut rows = BTreeMap::new();
    for (id, text) in [
        ("empty", json!("")),
        ("whitespace", json!(" \n\t")),
        ("null", Value::Null),
        ("nonstring", json!(7)),
    ] {
        rows.insert(
            id.to_owned(),
            json!({"id":id,"type":"knowledge","text":text,"match":"semantic","score":1.,"cosine":1.}),
        );
    }
    rows.insert(
        "missing".into(),
        json!({"id":"missing","type":"knowledge","match":"semantic","score":1.,"cosine":1.}),
    );
    let valid = "  Valid semantic passage text stays unchanged.  ";
    rows.insert(
        "valid".into(),
        json!({"id":"valid","type":"knowledge","text":valid,"match":"semantic","score":0.8,"cosine":0.8}),
    );
    for (id, extra) in [
        ("literal", json!({"match":"keyword","text":""})),
        ("full-and", json!({"match":"hybrid","text":" \n\t"})),
        ("entity", json!({"category":"entity_context","text":null})),
        (
            "behavior",
            json!({"behavior":{"authority":"explicit_user"},"text":7}),
        ),
        (
            "image",
            json!({"type":"media","modality":"image","text":""}),
        ),
        (
            "audio",
            json!({"type":"media","modality":"audio","text":null}),
        ),
    ] {
        let mut row = json!({"id":id,"type":"knowledge","match":"semantic","score":0.8,"cosine":0.8});
        records::merge(&mut row, extra);
        rows.insert(id.to_owned(), row);
    }
    let before = rows.clone();
    let calls = std::cell::Cell::new(0);
    let score = |documents: &[&str], _: Instant| {
        calls.set(calls.get() + 1);
        if documents.iter().any(|text| text.trim().is_empty()) {
            return Err(Error::Validation(
                "Text passage scoring requires nonempty text",
            ));
        }
        assert_eq!(documents, &[valid]);
        Ok(vec![1.])
    };
    f.store
        .read(|tx| {
            filter_text_passages(
                tx.sql(),
                &mut rows,
                10,
                Instant::now() + std::time::Duration::from_secs(5),
                Some(&score),
                &HashSet::new(),
            )
        })
        .expect("Empty stored passages must not turn valid recall into a scoring error");
    assert_eq!(calls.get(), 1);
    assert_eq!(rows.len(), 7);
    for id in ["empty", "whitespace", "null", "nonstring", "missing"] {
        assert!(!rows.contains_key(id));
    }
    for (id, row) in &rows {
        assert_eq!(row, &before[id]);
    }
}

#[test]
fn semantic_text_all_empty_passages_are_rejected_without_scoring() {
    let f = Fixture::new();
    let mut rows = BTreeMap::from([
        (
            "empty".into(),
            json!({"id":"empty","type":"knowledge","text":"","match":"semantic"}),
        ),
        (
            "whitespace".into(),
            json!({"id":"whitespace","type":"knowledge","text":" \n\t","match":"semantic"}),
        ),
        (
            "missing".into(),
            json!({"id":"missing","type":"knowledge","match":"semantic"}),
        ),
    ]);
    let calls = std::cell::Cell::new(0);
    let score = |_: &[&str], _: Instant| {
        calls.set(calls.get() + 1);
        Err(Error::Validation(
            "Text passage scoring requires nonempty text",
        ))
    };
    f.store
        .read(|tx| {
            filter_text_passages(
                tx.sql(),
                &mut rows,
                10,
                Instant::now() + std::time::Duration::from_secs(5),
                Some(&score),
                &HashSet::new(),
            )
        })
        .unwrap();
    assert!(rows.is_empty());
    assert_eq!(calls.get(), 0);
}

#[test]
fn semantic_text_batch_failure_is_atomic_and_effective_query_matches_encoder_context() {
    let f = Fixture::new();
    let mut rows = BTreeMap::new();
    for n in 0..11 {
        let id = format!("semantic-{n:02}");
        rows.insert(
            id.clone(),
            json!({"id":id,"type":"knowledge","text":id,"match":"semantic","score":1.,"cosine":1.}),
        );
    }
    rows.insert(
        "empty".into(),
        json!({"id":"empty","type":"knowledge","text":"","match":"semantic"}),
    );
    rows.insert(
        "whitespace".into(),
        json!({"id":"whitespace","type":"knowledge","text":" \n\t","match":"semantic"}),
    );
    let before = rows.clone();
    let calls = std::cell::Cell::new(0);
    let score = |documents: &[&str], _: Instant| {
        calls.set(calls.get() + 1);
        if calls.get() == 2 {
            Err(Error::Unavailable("Fixture scorer failed"))
        } else {
            Ok(vec![-9.; documents.len()])
        }
    };
    assert!(
        f.store
            .read(|tx| filter_text_passages(
                tx.sql(),
                &mut rows,
                11,
                Instant::now() + std::time::Duration::from_secs(5),
                Some(&score),
                &HashSet::new()
            ))
            .is_err()
    );
    assert_eq!(calls.get(), 2);
    assert_eq!(rows, before);
    let mut request = RecallRequest {
        query: "What about it?".into(),
        context: "  Existing salinity procedure  ".into(),
        automatic: true,
        ..Default::default()
    };
    assert_eq!(
        effective_query(&request),
        "What about it?\nRecent conversation:\nExisting salinity procedure"
    );
    request.automatic = false;
    assert_eq!(effective_query(&request), request.query);
}

#[test]
fn semantic_text_deadline_after_scoring_preserves_pending_empty_rejections() {
    let f = Fixture::new();
    let mut rows = BTreeMap::from([
        (
            "empty".into(),
            json!({"id":"empty","type":"knowledge","text":"","match":"semantic"}),
        ),
        (
            "whitespace".into(),
            json!({"id":"whitespace","type":"knowledge","text":" \n\t","match":"semantic"}),
        ),
        (
            "valid".into(),
            json!({"id":"valid","type":"knowledge","text":"Valid passage","match":"semantic"}),
        ),
    ]);
    let before = rows.clone();
    let calls = std::cell::Cell::new(0);
    f.store
        .read(|tx| {
            let deadline = Instant::now() + std::time::Duration::from_millis(100);
            let score = |documents: &[&str], deadline: Instant| {
                calls.set(calls.get() + 1);
                assert_eq!(documents, &["Valid passage"]);
                std::thread::sleep(
                    deadline.saturating_duration_since(Instant::now())
                        + std::time::Duration::from_millis(1),
                );
                Ok(vec![-9.])
            };
            let result = filter_text_passages(
                tx.sql(),
                &mut rows,
                10,
                deadline,
                Some(&score),
                &HashSet::new(),
            );
            assert!(matches!(
                result,
                Err(Error::Unavailable("Semantic search deadline expired"))
            ));
            Ok(())
        })
        .unwrap();
    assert_eq!(calls.get(), 1);
    assert_eq!(rows, before);
}

#[test]
fn semantic_text_scoring_holds_read_snapshot_without_holding_capture_writer() {
    let mut f = Fixture::new();
    f.add(
        "Stable curated reader fixture",
        "knowledge",
        "2026-10-26T00:00:00Z",
        0.5,
        0.9,
        "personal",
    );
    f.projection
        .rebuild(Tier::Hot, &AtomicBool::new(false))
        .unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (captured_tx, captured_rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let projection = f.projection.clone();
        let reader = scope.spawn(move || {
            let request = RecallRequest {
                query: "unseen paraphrase".into(),
                scope: Some("personal".into()),
                ..Default::default()
            };
            let mut vector = [0.; 768];
            vector[0] = 1.;
            let deadline = Instant::now() + std::time::Duration::from_secs(5);
            projection
                .snapshot_until(true, false, deadline, |tx, view| {
                    let score = |documents: &[&str], _: Instant| {
                        entered_tx.send(()).unwrap();
                        release_rx
                            .recv_timeout(std::time::Duration::from_secs(3))
                            .unwrap();
                        let fresh: i64 = tx.sql().query_row(
                            "SELECT count(*) FROM episodes WHERE event_id='during-text-score'",
                            [],
                            |r| r.get(0),
                        )?;
                        assert_eq!(fresh, 0);
                        Ok(vec![1.; documents.len()])
                    };
                    Retrieval::current_scored(
                        tx,
                        view,
                        &request,
                        &vector,
                        false,
                        deadline,
                        Some(&score),
                    )
                })
                .unwrap()
                .0
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let store = f.store.clone();
        let writer=scope.spawn(move|| {
            let captured=store.capture(serde_json::from_value(json!({"source":"synthetic","event_id":"during-text-score","conversation_id":"capture","kind":"user","text":"Capture commits while the semantic scorer holds a reader","metadata":{}})).unwrap()).unwrap();
            captured_tx.send(captured.id).unwrap();
        });
        let capture = captured_rx.recv_timeout(std::time::Duration::from_secs(1));
        release_tx.send(()).unwrap();
        let capture =
            capture.expect("Capture must commit before releasing the scored read snapshot");
        writer.join().unwrap();
        assert_eq!(reader.join().unwrap().len(), 1);
        assert!(f.store.episode(&capture).unwrap().is_some());
    });
}
