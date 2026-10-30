// Coverage-first automatic recall: complete short facts travel beside the unchanged
// summary; long bodies are explicitly withheld under the default disclosure policy.
const SYNTHETIC_CREDENTIAL: &str = "MEMENTO_SYNTHETIC_CREDENTIAL_Q7Q7Q7Q7Q7Q7Q7Q7Q7Q7Q7Q7";
fn long_body(lead: &str, tail: &str) -> String {
    let mut body = format!("{lead} ");
    while body.chars().count() < 250 {
        body.push_str("Synthetic filler clause. ");
    }
    body + tail
}
fn summary(body: &str) -> String {
    body.chars().take(240).collect()
}
fn hot_vector(f: &Fixture, id: &str, kind: VectorKind, cosine: f32, seq: i64) {
    let mut vector = [0f32; 768];
    vector[0] = cosine;
    vector[1] = (1. - cosine * cosine).sqrt();
    f.store
        .write(|tx| {
            tx.put_vector(&VectorWrite{seq,target:id.into(),kind,version:1,embedding:F32Blob::from_values(&vector)?,current:true,tier:Tier::Hot})?;
            Ok(())
        })
        .unwrap();
}
fn row<'a>(rows: &'a [Value], id: &str) -> &'a Value {
    rows.iter().find(|r| r["id"] == id).unwrap_or_else(|| panic!("{id} missing from {rows:?}"))
}
#[test]
fn automatic_full_text_withholds_long_facts_and_keeps_short_facts_complete() {
    let mut f = Fixture::new();
    let body = long_body("Synthetic quartz deployment rule.", "Only when the owner approves in writing.");
    let long = f.add(&body, "knowledge", "2026-10-20T00:00:00Z", 0.5, 0.9, "personal");
    let brief = f.add("Quartz fixture short assertion stays whole.", "knowledge", "2026-10-20T00:00:00Z", 0.5, 0.8, "personal");
    let person = f.add("Mark Petrov", "person", "2020-01-01T00:00:00Z", 0.5, 0.1, "personal");
    f.connect(&long, &person);
    let explicit = f.recall("quartz", 6, false, Some("personal"));
    let fact = row(&explicit, &long);
    assert_eq!(fact["text"], summary(&body));
    assert_eq!(fact["text_truncated"], true);
    assert!(fact.get("full_text").is_none());
    assert_eq!(row(&explicit, &brief)["text_truncated"], false);
    let automatic = f.recall("quartz", 6, true, Some("personal"));
    let fact = row(&automatic, &long);
    let text = fact["text"].as_str().unwrap();
    assert!(text.starts_with(&format!("Source 2026-10-20: {}", summary(&body))));
    assert!(text.contains(" (Connected: Mark Petrov [person; related_to "), "{text}");
    assert_eq!(fact["text_truncated"], true);
    assert!(fact.get("full_text").is_none());
    let short = row(&automatic, &brief);
    assert_eq!(short["text_truncated"], false);
    assert_eq!(short["full_text"], short["text"]);
    assert!(automatic.iter().all(|r| r.get("_full_render").is_none() && r.get("_full_text").is_none()));
}

#[test]
fn automatic_full_text_withholds_long_bodies_even_when_framing_fits() {
    let mut f = Fixture::new();
    // "Source 2026-10-20: " is 19 bytes, so 1481 body bytes render to exactly 1500.
    let fits = format!("Quartz boundary fact. {}", "x".repeat(1481 - 22));
    let over = format!("Quartz overflow fact. {}", "y".repeat(1482 - 22));
    let fits_id = f.add(&fits, "knowledge", "2026-10-20T00:00:00Z", 0.5, 0.9, "personal");
    let over_id = f.add(&over, "knowledge", "2026-10-20T00:00:00Z", 0.5, 0.89, "personal");
    let automatic = f.recall("quartz", 6, true, Some("personal"));
    assert_eq!(row(&automatic, &fits_id)["text_truncated"], true);
    assert!(row(&automatic, &fits_id).get("full_text").is_none());
    let withheld = row(&automatic, &over_id);
    assert_eq!(withheld["text_truncated"], true);
    assert!(withheld.get("full_text").is_none());
}

#[test]
fn synthetic_credential_tail_stays_behind_existing_guards() {
    let mut f = Fixture::new();
    let body = long_body("Synthetic quartz integration note.", &format!("Fixture key {SYNTHETIC_CREDENTIAL}."));
    // An approved new custom type is registered sensitive (owner review gate).
    let episode = f.store.capture(serde_json::from_value(json!({"source":"synthetic","event_id":"private","conversation_id":"private","kind":"user","text":body,"occurred_at":"2026-10-20T00:00:00Z","metadata":{}})).unwrap()).unwrap();
    let proposal = f.graph.remember(serde_json::from_value(json!({"command_id":"private","actor":"owner","why":"Exact synthetic evidence","type":"synthetic_private","text":body,"scope":["personal"],"evidence":[{"episode_id":episode.id,"quote":body}]})).unwrap()).unwrap();
    f.graph.decide_review(serde_json::from_value(json!({"command_id":"private-ok","actor":"owner","why":"Explicit synthetic approval","proposal_id":proposal["id"],"decision":"approve"})).unwrap()).unwrap();
    let private = f.store.read(|tx| Ok(tx.sql().query_row("SELECT id FROM node_versions WHERE type='synthetic_private'", [], |r| r.get::<_, String>(0))?)).unwrap();
    hot_vector(&f, &private, VectorKind::Node, 0.9, 900);
    // Distinct wording so automatic deduplication keeps both rows.
    let ordinary = long_body("Synthetic quartz deployment memo.", &format!("Fixture key {SYNTHETIC_CREDENTIAL}."));
    let personal = f.add(&ordinary, "knowledge", "2026-10-20T00:00:00Z", 0.5, 0.88, "personal");
    for (automatic, scope) in [(false, None), (false, Some("personal")), (true, Some("personal"))] {
        let rows = f.recall("quartz", 6, automatic, scope);
        assert_eq!(row(&rows, &private)["text_truncated"], true);
        if automatic {
            // Sensitive types keep only the legacy summary; the new bridge then omits them.
            assert!(row(&rows, &private).get("full_text").is_none());
            rows.iter().filter(|r| r["id"] != personal).for_each(|r| assert!(!r.to_string().contains(SYNTHETIC_CREDENTIAL)));
        } else {
            assert!(!serde_json::to_string(&rows).unwrap().contains(SYNTHETIC_CREDENTIAL));
        }
    }
    // Scope exclusion is unchanged: neither rendering reaches another scope.
    let other = f.recall("quartz", 6, true, Some("professional"));
    assert!(!serde_json::to_string(&other).unwrap().contains(SYNTHETIC_CREDENTIAL));
    // Ordinary types do not bypass the default tail policy either. This does not
    // claim to detect secrets in content; it preserves the existing disclosure bound.
    let rows = f.recall("quartz", 6, true, Some("personal"));
    assert!(row(&rows, &personal).get("full_text").is_none());
    assert!(!serde_json::to_string(&rows).unwrap().contains(SYNTHETIC_CREDENTIAL));
}

#[test]
fn parked_candidate_full_text_withholds_the_long_body_and_keeps_unverified_framing() {
    let f = Fixture::new();
    let body = long_body("Synthetic quartz historical candidate.", "Only for the archived staging fixture.");
    let episode = f.store.capture(serde_json::from_value(json!({"source":"synthetic","event_id":"parked","conversation_id":"parked","kind":"user","text":body,"occurred_at":"2026-10-20T00:00:00Z","metadata":{}})).unwrap()).unwrap();
    let id = "c_parked_full_text_fixture";
    let context = memento_core::CommandContext {command_id:"parked".into(),actor:"fixture".into(),why:"Exact parked candidate evidence".into()};
    f.store.command(&context, &json!({"op":"fixture_parked"}), |tx, commit| {
        tx.touch_target(id)?;
        tx.sql().execute("INSERT INTO extraction_runs VALUES('run-parked',?,?,'fixture','fixture','[]','{}','2026-10-20T00:00:00Z')", params![commit, episode.id])?;
        tx.sql().execute("INSERT INTO candidates VALUES(?,'run-parked','memory','knowledge',?,'Synthetic extraction reason',NULL,NULL,0.9,NULL,NULL,'[\"personal\"]')", params![id, body])?;
        tx.sql().execute("INSERT INTO graph_evidence VALUES(?,1,?,0,?,?)", params![id, episode.id, body.chars().count() as i64, body])?;
        tx.sql().execute("INSERT INTO historical_parked_candidates(candidate_id) VALUES(?)", [id])?;
        Ok(json!({"id":id}))
    }).unwrap();
    hot_vector(&f, id, VectorKind::Candidate, 0.9, 901);
    f.projection.rebuild(Tier::Hot, &AtomicBool::new(false)).unwrap();
    f.projection.rebuild(Tier::Cold, &AtomicBool::new(false)).unwrap();
    let score = |documents: &[&str], _: Instant| Ok(vec![1.; documents.len()]);
    let request = RecallRequest{query:"quartz".into(),automatic:true,effort:"cheap".into(),limit:6,..Default::default()};
    let mut vector = [0.; 768];
    vector[0] = 1.;
    let deadline = Instant::now() + std::time::Duration::from_secs(5);
    let rows = f.projection.snapshot_until(true, true, deadline, |tx, view| Retrieval::current_scored(tx, view, &request, &vector, true, deadline, Some(&score))).unwrap().0;
    let parked = row(&rows, id);
    assert!(parked["text"].as_str().unwrap().contains("Unverified historical candidate (source 2026-10-20): "));
    assert_eq!(parked["text_truncated"], true);
    assert!(parked.get("full_text").is_none());
    assert!(parked.get("_full_render").is_none());
}

#[test]
fn automatic_media_pointer_is_complete_and_never_marked_truncated() {
    let f = Fixture::new();
    let id = "media-full-text";
    let text = long_body("Synthetic quartz attachment description.", "Final caption clause.");
    let source = f.store.capture(serde_json::from_value(json!({"source":"synthetic:media","event_id":id,"conversation_id":id,"kind":"user","text":text,"occurred_at":"2020-01-01T00:00:00Z","metadata":{}})).unwrap()).unwrap();
    let context = memento_core::CommandContext {command_id:id.into(),actor:"fixture".into(),why:"Exact attachment evidence".into()};
    let sha = "b".repeat(64);
    let segment = format!("{id}-segment");
    f.store.command(&context, &json!({"op":"fixture_media"}), |tx, commit| {
        tx.touch_target(id)?;
        tx.sql().execute("INSERT INTO node_versions(id,version,commit_id,type,text,state,pinned,scope) VALUES(?,1,?,'media',?,'active',0,'[\"personal\"]')", params![id, commit, text])?;
        tx.sql().execute("INSERT INTO media_files VALUES(?,?,'synthetic.png','image',?)", params![id, sha, commit])?;
        tx.sql().execute("INSERT INTO graph_evidence VALUES(?,1,?,0,?,?)", params![id, source.id, text.chars().count() as i64, text])?;
        tx.sql().execute("INSERT INTO segments(id,node_id,modality,text,artifact_sha256) VALUES(?,?,'image','',?)", params![segment, id, sha])?;
        Ok(json!({"id":id}))
    }).unwrap();
    hot_vector(&f, &segment, VectorKind::Segment, 0.9, 902);
    let automatic = f.recall("quartz", 6, true, None);
    let media = row(&automatic, id);
    assert_eq!(media["category"], "media_reference");
    assert!(media["text"].as_str().unwrap().contains("attachment \"synthetic.png\""));
    assert!(media.get("text_truncated").is_none() && media.get("full_text").is_none());
    let explicit = f.recall("quartz", 6, false, None);
    assert_eq!(row(&explicit, id)["text_truncated"], true);
}
