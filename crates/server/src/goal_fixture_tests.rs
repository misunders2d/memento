// Goal fixture (debate Phase 0): synthetic-only cases over the real two-cut automatic
// recall path (`Retrieval::passes`). Query vectors are synthetic, so semantic quality
// of the real encoder is NOT measured here; latencies are synthetic-scale, never
// production numbers. Each case prints one `GOAL_FIXTURE {json}` line (run with
// `--nocapture`). Hard asserts cover only outcomes that already hold; recorded gaps
// become asserts in the commit that closes them.
struct GoalCase {
    case: &'static str,
    entity: Option<String>,
    connected: Vec<String>,
    forbidden: Vec<String>,
}
fn gf_commit(f: &Fixture) {
    f.store
        .write(|tx| {
            tx.sql().execute(
                "INSERT INTO graph_commits SELECT 'gf-c','gf-c','{}','{}','owner','Goal fixture','2026-01-01T00:00:00Z' WHERE NOT EXISTS(SELECT 1 FROM graph_commits WHERE id='gf-c')",
                [],
            )?;
            Ok(())
        })
        .unwrap();
}
// A raw node version in the fixture commit; merged/refined rows point at `superseded`.
fn gf_node(f: &Fixture, id: &str, kind: &str, text: &str, state: &str, superseded: Option<&str>, scope: &str) {
    gf_commit(f);
    f.store
        .write(|tx| {
            tx.sql().execute(
                "INSERT INTO node_versions VALUES(?1,1,'gf-c',?2,?3,?4,NULL,NULL,?5,0,?6)",
                params![id, kind, text, state, superseded, format!("[\"{scope}\"]")],
            )?;
            Ok(())
        })
        .unwrap();
}
fn gf_merge(f: &Fixture, source: &str, target: &str, alias: Option<&str>) {
    f.store
        .write(|tx| {
            tx.sql().execute("INSERT INTO entity_merges VALUES(?,?,'gf-c')", params![source, target])?;
            if let Some(alias) = alias {
                tx.sql().execute("INSERT INTO alias_fts(id,text) VALUES(?,?)", params![source, alias])?;
            }
            Ok(())
        })
        .unwrap();
}
// A pending candidate from one synthetic extraction run; `parked` adds it to the historical pool.
fn gf_candidate(
    f: &mut Fixture,
    id: &str,
    kind: &str,
    memory_type: &str,
    text: &str,
    ends: (Option<&str>, Option<&str>),
    scope: &str,
    parked: bool,
) {
    gf_commit(f);
    f.seq += 1;
    let episode = f
        .store
        .capture(
            serde_json::from_value(json!({
                "source":"synthetic",
                "event_id":format!("gf-run-{id}"),
                "conversation_id":"goal-fixture",
                "kind":"user",
                "text":text,
                "occurred_at":"2020-06-01T00:00:00Z",
                "metadata":{}
            }))
            .unwrap(),
        )
        .unwrap();
    f.store
        .write(|tx| {
            let db = tx.sql();
            db.execute(
                "INSERT INTO extraction_runs VALUES(?1,'gf-c',?2,'v1','fixture','[]','{}','2020-06-01T00:00:00Z')",
                params![format!("run-{id}"), episode.id],
            )?;
            db.execute(
                "INSERT INTO candidates VALUES(?1,?2,?3,?4,?5,'Synthetic fixture candidate',?6,?7,0.8,NULL,NULL,?8)",
                params![id, format!("run-{id}"), kind, memory_type, text, ends.0, ends.1, format!("[\"{scope}\"]")],
            )?;
            if parked {
                db.execute("INSERT INTO historical_parked_candidates VALUES(?,NULL)", [id])?;
            }
            Ok(())
        })
        .unwrap();
}
fn gf_rebuild(f: &Fixture, cold: bool) {
    f.projection.rebuild(Tier::Hot, &AtomicBool::new(false)).unwrap();
    if cold {
        f.projection.rebuild(Tier::Cold, &AtomicBool::new(false)).unwrap();
    }
}
fn gf_filler(n: usize) -> String {
    (0..n).map(|i| format!("filler{i:03}")).collect::<Vec<_>>().join(" ")
}
// One automatic request as the bridge sends it (cheap, limit 6) over both cuts.
fn gf_recall(
    f: &Fixture,
    query: &str,
    context: &str,
    scope: Option<&str>,
    budget: std::time::Duration,
) -> (Result<Vec<Value>>, Value) {
    let mut vector = [0f32; 768];
    vector[0] = 1.;
    let request = RecallRequest {
        query: query.into(),
        context: context.into(),
        effort: "cheap".into(),
        limit: 6,
        automatic: true,
        scope: scope.map(str::to_owned),
        ..Default::default()
    };
    let score = |documents: &[&str], _: Instant| Ok(vec![1.; documents.len()]);
    let timings = RecallTimings::default();
    let started = Instant::now();
    let rows = Retrieval::passes(&f.projection, &request, &vector, started + budget, &score, &timings);
    let value = timings.json_outcome(&request, started.elapsed(), rows.as_ref().err());
    (rows, value)
}
fn gf_connections(row: &Value) -> Vec<String> {
    row["connections"]
        .as_array()
        .map(|c| c.iter().filter_map(|c| c["id"].as_str().map(str::to_owned)).collect())
        .unwrap_or_default()
}
fn gf_parked(row: &Value) -> bool {
    row["historical"] == true || row["text"].as_str().is_some_and(|t| t.starts_with("Unverified historical"))
}
// Scores one case; prints and returns its report object.
fn gf_report(spec: &GoalCase, rows: &Result<Vec<Value>>, timings: &Value) -> Value {
    let empty = Vec::new();
    let delivered = rows.as_ref().unwrap_or(&empty);
    let ids: Vec<&str> = delivered.iter().filter_map(|r| r["id"].as_str()).collect();
    let entity_row = spec.entity.as_ref().and_then(|e| {
        delivered.iter().find(|r| r["id"] == e.as_str() && r["category"] == "entity_context")
    });
    let reachable: HashSet<String> = delivered
        .iter()
        .flat_map(|r| {
            let mut v = gf_connections(r);
            v.extend(r["id"].as_str().map(str::to_owned));
            v
        })
        .collect();
    let connected = spec.connected.iter().filter(|c| reachable.contains(*c)).count();
    let stage = |s: &str| timings["stages_us"][s].clone();
    let report = json!({
        "case": spec.case,
        "ok": rows.is_ok(),
        "failure": timings["failure"],
        "failure_tier": timings["failure_tier"],
        "pass": timings["pass"],
        "entity_expected": spec.entity.is_some(),
        "entity_hit": entity_row.is_some(),
        "connected_expected": spec.connected.len(),
        "connected_hit": connected,
        "forbidden_present": spec.forbidden.iter().filter(|x| ids.contains(&x.as_str())).count(),
        "parked_delivered": delivered.iter().filter(|r| gf_parked(r)).count(),
        "parked_labelled": delivered.iter().filter(|r| gf_parked(r) && r["text"].as_str().is_some_and(|t| t.starts_with("Unverified historical"))).count(),
        "returned": delivered.iter().map(|r| json!({
            "category": r["category"], "tier": r["tier"], "historical": r["historical"].as_bool().unwrap_or(false),
            "connections": gf_connections(r).len(), "same_name_count": r["same_name_count"],
        })).collect::<Vec<_>>(),
        "total_us": timings["total_us"],
        "stages_us": {
            "first_pass": stage("first_pass"), "archive_pass": stage("archive_pass"),
            "name_anchors": stage("name_anchors"), "lexical": stage("lexical"),
            "validation": stage("validation"), "fallback": stage("fallback"),
            "partial_anchors": stage("partial_anchors"),
        },
        "cuts": timings["cuts"],
    });
    println!("GOAL_FIXTURE {report}");
    report
}
// Person `name` with one connected fact; returns (person, fact).
fn gf_person_with_fact(f: &mut Fixture, name: &str, fact: &str, scope: &str) -> (String, String) {
    let person = f.add(name, "person", "2026-01-01T00:00:00Z", 0.5, 0.1, scope);
    let fact = f.add(fact, "knowledge", "2026-01-02T00:00:00Z", 0.5, 0.2, scope);
    f.connect(&person, &fact);
    (person, fact)
}
const SECOND: std::time::Duration = std::time::Duration::from_secs(1);
fn gf_cases() -> Vec<Value> {
    let mut out = Vec::new();
    // named: entity + connected fact, Hot answer, Cold available.
    {
        let mut f = Fixture::new();
        let (p, fact) = gf_person_with_fact(&mut f, "Orla Venn", "Orla Venn maintains the kiln rota", "personal");
        gf_rebuild(&f, true);
        let (rows, t) = gf_recall(&f, "What should I tell Orla Venn tomorrow?", "", None, 5 * SECOND);
        out.push(gf_report(&GoalCase { case: "named", entity: Some(p), connected: vec![fact], forbidden: vec![] }, &rows, &t));
    }
    // semantic_only: unnamed prompt, one strongly similar fact.
    {
        let mut f = Fixture::new();
        let fact = f.add("Greenhouse sensors report humidity hourly", "knowledge", "2026-01-01T00:00:00Z", 0.5, 0.9, "personal");
        gf_rebuild(&f, true);
        let (rows, t) = gf_recall(&f, "how often do the plant monitors check moisture", "", None, 5 * SECOND);
        out.push(gf_report(&GoalCase { case: "semantic_only", entity: None, connected: vec![fact], forbidden: vec![] }, &rows, &t));
    }
    // long prompts: the name after 64 distinct tokens (tail) and in the middle.
    for (case, query) in [
        ("long_tail_name_after_token64", format!("{} please ask Brannoch Quill", gf_filler(80))),
        ("long_middle_name_after_token64", format!("{} Brannoch Quill {}", gf_filler(70), gf_filler(10).replace("filler", "tail"))),
        ("long_name_before_token64", format!("ask Brannoch Quill {}", gf_filler(80))),
    ] {
        let mut f = Fixture::new();
        let (p, fact) = gf_person_with_fact(&mut f, "Brannoch Quill", "Brannoch Quill owns the ferry ledger", "personal");
        gf_rebuild(&f, true);
        let (rows, t) = gf_recall(&f, &query, "", None, 5 * SECOND);
        out.push(gf_report(&GoalCase { case, entity: Some(p), connected: vec![fact], forbidden: vec![] }, &rows, &t));
    }
    // follow-up: the antecedent names the entity, the current message does not.
    {
        let mut f = Fixture::new();
        let (p, fact) = gf_person_with_fact(&mut f, "Orla Venn", "Orla Venn maintains the kiln rota", "personal");
        gf_rebuild(&f, true);
        let (rows, t) = gf_recall(&f, "what about it?", "We were discussing Orla Venn and the kiln.", None, 5 * SECOND);
        out.push(gf_report(&GoalCase { case: "follow_up_antecedent_name", entity: Some(p), connected: vec![fact], forbidden: vec![] }, &rows, &t));
    }
    // Cyrillic short name.
    {
        let mut f = Fixture::new();
        let (p, fact) = gf_person_with_fact(&mut f, "Ия Лис", "Ия Лис ведёт календарь поставок", "personal");
        gf_rebuild(&f, true);
        let (rows, t) = gf_recall(&f, "Напомни, что сказала Ия Лис", "", None, 5 * SECOND);
        out.push(gf_report(&GoalCase { case: "cyrillic_short_name", entity: Some(p), connected: vec![fact], forbidden: vec![] }, &rows, &t));
    }
    // homonyms: two distinct people spelled alike stay distinct and labelled.
    {
        let mut f = Fixture::new();
        let (a, fa) = gf_person_with_fact(&mut f, "Sam Rook", "Sam Rook repairs harbour cranes", "personal");
        let (b, fb) = gf_person_with_fact(&mut f, "Sam Rook", "Sam Rook teaches violin", "personal");
        gf_rebuild(&f, true);
        let (rows, t) = gf_recall(&f, "Sam Rook called again", "", None, 5 * SECOND);
        let mut report = gf_report(&GoalCase { case: "homonyms", entity: Some(a.clone()), connected: vec![fa, fb], forbidden: vec![] }, &rows, &t);
        let rows = rows.unwrap_or_default();
        report["both_distinct"] = json!([&a, &b].iter().all(|id| rows.iter().any(|r| r["id"] == id.as_str())));
        report["same_name_labelled"] = json!(rows.iter().filter(|r| r["same_name_count"] == 2).count());
        println!("GOAL_FIXTURE {report}");
        out.push(report);
    }
    // alias + two-hop merge chain: alias on A, A merged into B, B refined into C.
    {
        let mut f = Fixture::new();
        let (c, fact) = gf_person_with_fact(&mut f, "Kestrel Works", "Kestrel Works builds tidal pumps", "personal");
        gf_node(&f, "gf-a", "organization", "Kestrel Robotics", "merged", Some("gf-b"), "personal");
        gf_node(&f, "gf-b", "organization", "Kestrel Labs", "refined", Some(&c), "personal");
        gf_merge(&f, "gf-a", "gf-b", Some("Kestrel Robotics"));
        gf_rebuild(&f, true);
        let (rows, t) = gf_recall(&f, "any news from Kestrel Robotics", "", None, 5 * SECOND);
        out.push(gf_report(&GoalCase { case: "alias_multi_hop_merge", entity: Some(c), connected: vec![fact], forbidden: vec!["gf-a".into(), "gf-b".into()] }, &rows, &t));
    }
    // J11: parked entity candidates reached through exact name anchors.
    {
        let mut f = Fixture::new();
        gf_candidate(&mut f, "gf-park-1", "entity", "person", "Tamsin Hollow", (None, None), "personal", true);
        gf_rebuild(&f, true);
        let (rows, t) = gf_recall(&f, "did Tamsin Hollow reply", "", None, 5 * SECOND);
        out.push(gf_report(&GoalCase { case: "parked_one", entity: Some("gf-park-1".into()), connected: vec![], forbidden: vec![] }, &rows, &t));
    }
    {
        let mut f = Fixture::new();
        for (id, name) in [("gf-park-1", "Tamsin Hollow"), ("gf-park-2", "Ivo Marsh"), ("gf-park-3", "Pell Arden")] {
            gf_candidate(&mut f, id, "entity", "person", name, (None, None), "personal", true);
        }
        gf_rebuild(&f, true);
        let (rows, t) = gf_recall(&f, "Tamsin Hollow, Ivo Marsh and Pell Arden met", "", None, 5 * SECOND);
        out.push(gf_report(&GoalCase { case: "parked_multiple", entity: None, connected: vec![], forbidden: vec![] }, &rows, &t));
    }
    {
        let mut f = Fixture::new();
        gf_candidate(&mut f, "gf-park-1", "entity", "person", "Tamsin Hollow", (None, None), "personal", true);
        gf_candidate(&mut f, "gf-park-2", "entity", "project", "Lantern Survey", (None, None), "personal", true);
        gf_candidate(&mut f, "gf-park-rel", "relation", "works_on", "Tamsin Hollow works on Lantern Survey", (Some("gf-park-1"), Some("gf-park-2")), "personal", true);
        gf_rebuild(&f, true);
        let (rows, t) = gf_recall(&f, "what is Tamsin Hollow doing", "", None, 5 * SECOND);
        out.push(gf_report(&GoalCase { case: "parked_connection", entity: Some("gf-park-1".into()), connected: vec!["gf-park-2".into()], forbidden: vec![] }, &rows, &t));
    }
    {
        let mut f = Fixture::new();
        let (p, fact) = gf_person_with_fact(&mut f, "Tamsin Hollow", "Tamsin Hollow leads the archive move", "personal");
        gf_candidate(&mut f, "gf-park-1", "entity", "person", "Tamsin Hollow", (None, None), "personal", true);
        gf_rebuild(&f, true);
        let (rows, t) = gf_recall(&f, "did Tamsin Hollow reply", "", None, 5 * SECOND);
        out.push(gf_report(&GoalCase { case: "parked_vs_current_fact", entity: Some(p), connected: vec![fact], forbidden: vec![] }, &rows, &t));
    }
    // B01: mixed-scope facts. Unscoped (today's bridge) vs scoped requests.
    for (case, scope) in [("mixed_scope_unscoped", None), ("mixed_scope_personal", Some("personal"))] {
        let mut f = Fixture::new();
        let personal = f.add("Weekend hiking gear list", "knowledge", "2026-01-01T00:00:00Z", 0.5, 0.9, "personal");
        let professional = f.add("Quarterly supplier gear audit", "knowledge", "2026-01-01T00:00:00Z", 0.5, 0.9, "professional");
        gf_rebuild(&f, true);
        let (rows, t) = gf_recall(&f, "gear list", "", scope, 5 * SECOND);
        let forbidden = if scope.is_some() { vec![professional.clone()] } else { vec![] };
        let mut report = gf_report(&GoalCase { case, entity: None, connected: vec![personal.clone()], forbidden }, &rows, &t);
        report["other_scope_present"] = json!(rows.as_ref().map_or(false, |r| r.iter().any(|r| r["id"] == professional.as_str())));
        println!("GOAL_FIXTURE {report}");
        out.push(report);
    }
    // Cold down: labels-only Hot (named entity, no meaningful fact) forces the archive cut.
    {
        let mut f = Fixture::new();
        let p = f.add("Odile Brack", "person", "2026-01-01T00:00:00Z", 0.5, 0.1, "personal");
        gf_rebuild(&f, false);
        let (rows, t) = gf_recall(&f, "has Odile Brack written", "", None, 5 * SECOND);
        out.push(gf_report(&GoalCase { case: "cold_down_hot_labels_only", entity: Some(p), connected: vec![], forbidden: vec![] }, &rows, &t));
    }
    // Cold down with a useful Hot fact: no archive cut needed.
    {
        let mut f = Fixture::new();
        let fact = f.add("Greenhouse sensors report humidity hourly", "knowledge", "2026-01-01T00:00:00Z", 0.5, 0.9, "personal");
        gf_rebuild(&f, false);
        let (rows, t) = gf_recall(&f, "plant moisture checks", "", None, 5 * SECOND);
        out.push(gf_report(&GoalCase { case: "cold_down_hot_useful", entity: None, connected: vec![fact], forbidden: vec![] }, &rows, &t));
    }
    // Cold down with nothing relevant in Hot: must never read as healthy-empty.
    {
        let mut f = Fixture::new();
        f.add("Unrelated harbor schedule note", "knowledge", "2026-01-01T00:00:00Z", 0.5, 0.2, "personal");
        gf_rebuild(&f, false);
        let (rows, t) = gf_recall(&f, "zyxw unmatched request", "", None, 5 * SECOND);
        out.push(gf_report(&GoalCase { case: "cold_down_hot_empty", entity: None, connected: vec![], forbidden: vec![] }, &rows, &t));
    }
    // Cold stall: another reader holds Cold past the request deadline.
    {
        let mut f = Fixture::new();
        let p = f.add("Odile Brack", "person", "2026-01-01T00:00:00Z", 0.5, 0.1, "personal");
        gf_rebuild(&f, true);
        let (held, release) = (std::sync::mpsc::channel::<()>(), std::sync::mpsc::channel::<()>());
        let projection = f.projection.clone();
        let holder = std::thread::spawn(move || {
            projection
                .snapshot_until(false, true, Instant::now() + 10 * SECOND, |_, _| {
                    held.0.send(()).unwrap();
                    let _ = release.1.recv_timeout(10 * SECOND);
                    Ok(())
                })
                .unwrap();
        });
        held.1.recv().unwrap();
        let (rows, t) = gf_recall(&f, "has Odile Brack written", "", None, SECOND);
        release.0.send(()).unwrap();
        holder.join().unwrap();
        out.push(gf_report(&GoalCase { case: "cold_stall_hot_labels_only", entity: Some(p), connected: vec![], forbidden: vec![] }, &rows, &t));
    }
    // Hot contention: another reader holds Hot past the request deadline.
    {
        let mut f = Fixture::new();
        let (p, fact) = gf_person_with_fact(&mut f, "Orla Venn", "Orla Venn maintains the kiln rota", "personal");
        gf_rebuild(&f, true);
        let (held, release) = (std::sync::mpsc::channel::<()>(), std::sync::mpsc::channel::<()>());
        let projection = f.projection.clone();
        let holder = std::thread::spawn(move || {
            projection
                .snapshot_until(true, false, Instant::now() + 10 * SECOND, |_, _| {
                    held.0.send(()).unwrap();
                    let _ = release.1.recv_timeout(10 * SECOND);
                    Ok(())
                })
                .unwrap();
        });
        held.1.recv().unwrap();
        let (rows, t) = gf_recall(&f, "What should I tell Orla Venn tomorrow?", "", None, SECOND);
        release.0.send(()).unwrap();
        holder.join().unwrap();
        out.push(gf_report(&GoalCase { case: "hot_contention", entity: Some(p), connected: vec![fact], forbidden: vec![] }, &rows, &t));
    }
    // Projection lag beyond the 512-event foreground admission.
    {
        let mut f = Fixture::new();
        let (p, fact) = gf_person_with_fact(&mut f, "Orla Venn", "Orla Venn maintains the kiln rota", "personal");
        gf_rebuild(&f, true);
        f.store
            .write(|tx| {
                let seq: i64 = tx.sql().query_row("SELECT max(seq) FROM vector_blobs", [], |r| r.get(0))?;
                for _ in 0..600 {
                    tx.touch_vector(seq, Tier::Hot)?;
                }
                Ok(())
            })
            .unwrap();
        let (rows, t) = gf_recall(&f, "What should I tell Orla Venn tomorrow?", "", None, 5 * SECOND);
        out.push(gf_report(&GoalCase { case: "lag_over_512", entity: Some(p), connected: vec![fact], forbidden: vec![] }, &rows, &t));
    }
    out
}
fn gf_case<'a>(reports: &'a [Value], case: &str) -> &'a Value {
    reports.iter().rev().find(|r| r["case"] == case).unwrap_or_else(|| panic!("missing case {case}"))
}
#[test]
fn goal_fixture_report() {
    let reports = gf_cases();
    // Outcomes that already hold on the unchanged code.
    for case in ["named", "long_name_before_token64", "cyrillic_short_name", "alias_multi_hop_merge"] {
        let r = gf_case(&reports, case);
        assert_eq!((r["ok"].as_bool(), r["entity_hit"].as_bool(), r["connected_hit"].as_u64()), (Some(true), Some(true), Some(1)), "{r}");
    }
    let r = gf_case(&reports, "alias_multi_hop_merge");
    assert_eq!(r["forbidden_present"], 0, "{r}");
    let r = gf_case(&reports, "semantic_only");
    assert_eq!((r["ok"].as_bool(), r["connected_hit"].as_u64()), (Some(true), Some(1)), "{r}");
    let r = gf_case(&reports, "homonyms");
    assert_eq!((r["both_distinct"].as_bool(), r["same_name_labelled"].as_u64()), (Some(true), Some(2)), "{r}");
    let r = gf_case(&reports, "mixed_scope_personal");
    assert_eq!((r["ok"].as_bool(), r["forbidden_present"].as_u64(), r["connected_hit"].as_u64()), (Some(true), Some(0), Some(1)), "{r}");
    let r = gf_case(&reports, "cold_down_hot_useful");
    assert_eq!((r["ok"].as_bool(), r["connected_hit"].as_u64()), (Some(true), Some(1)), "{r}");
    // Never healthy-empty when the archive tier needed for completeness is down.
    let r = gf_case(&reports, "cold_down_hot_empty");
    assert_eq!(r["ok"], false, "{r}");
}
