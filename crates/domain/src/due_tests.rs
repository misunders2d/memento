//! Automatic loop due dates from explicit source dates; no model calls.
use super::*;

const ASK: &str = "'first steps' should be added as a project under the video production initiative. I'll create a google drive folder for videos specifically and we'll be uploading assets there (tomorrow)";
const UPLOAD: &str = "we'll be uploading assets there (tomorrow)";

fn claim_in(f: &Fixture, zone: &str, kind: &str) -> Value {
    let claimed = f.processing.claim(Claim {
        agent: "fixture".into(),
        timezone_name: zone.into(),
        compaction: Some(Compaction { source: "contract".into(), conversation_id: "thread".into() }),
        exclude: vec![],
        consolidate_now: true,
        kinds: Some(vec![kind.into()]),
        consolidation_start: None,
        consolidation_end: None,
        continuous: false,
        batch_size: 16,
    });
    claimed.unwrap()["work"].as_array().unwrap().iter().find(|work| work["kind"] == kind).unwrap().clone()
}
fn due(at: &str, quote: &str) -> Value {
    json!({"at":at,"quote":quote,"overdue":false})
}
/// One loop draft citing `quote`, stored with `text` (an exact identity when reused).
fn dated(work: &Value, quote: &str, text: &str, open_loop: bool, due: Value) -> Value {
    let windows = work["input"]["windows"].as_array().unwrap().iter().map(|window| {
        let mut draft = draft(quote, "knowledge", open_loop);
        draft["text"] = json!(text);
        draft["due"] = due.clone();
        let candidates = if window["text"].as_str().unwrap().contains(quote) { vec![draft] } else { vec![] };
        json!({"id":window["id"],"candidates":candidates})
    });
    json!({"windows":windows.collect::<Vec<_>>()})
}
fn marker(f: &Fixture, id: &str) -> Value {
    f.graph.store.read(|tx| loops::current(tx.sql(), id)).unwrap().unwrap_or(Value::Null)
}
fn newest_candidate(f: &Fixture) -> String {
    f.stored("SELECT id FROM candidates ORDER BY rowid DESC LIMIT 1")
}
fn listed(f: &Fixture, before: &str) -> Value {
    f.graph.loop_command(serde_json::from_value(json!({"action":"list","due_before":before,"responsibility":"unknown"})).unwrap()).unwrap()
}
/// The owner's example promoted to a loop node dated by its explicit "tomorrow".
fn promoted(f: &Fixture, zone: &str) -> String {
    f.capture("ask", "thread", ASK, "user", "contract", json!({}), "2026-11-01T10:00:00Z");
    let work = claim_in(f, zone, "extract");
    f.complete(&work, dated(&work, UPLOAD, UPLOAD, true, due("2026-11-02", "tomorrow"))).unwrap();
    let work = claim_in(f, zone, "consolidate");
    let candidate = &work["input"]["candidates"][0];
    let mut keep = action(candidate, "remember");
    keep["open_loop"] = json!(true);
    let result = f.complete(&work, json!({"actions":[keep]})).unwrap();
    result["results"][candidate["id"].as_str().unwrap()]["id"].as_str().unwrap().to_owned()
}

#[test]
fn explicit_tomorrow_resolves_in_claim_timezone_survives_promotion_and_lists_by_due_before() {
    let f = Fixture::new();
    f.capture("ask", "thread", ASK, "user", "contract", json!({}), "2026-11-01T10:00:00Z");
    let work = claim_in(&f, "Europe/Berlin", "extract");
    assert_eq!(work["input"]["timezone"], "Europe/Berlin");
    assert_eq!(work["input"]["windows"][0]["local_time"], "2026-11-01T11:00:00+01:00 Sunday");
    f.complete(&work, dated(&work, UPLOAD, UPLOAD, true, due("2026-11-02", "tomorrow"))).unwrap();
    // End of 2026-11-02 in Berlin (CET), stored UTC-normalized like a manual reminder.
    let candidate = newest_candidate(&f);
    assert_eq!(marker(&f, &candidate)["due_at"], "2026-11-02T22:59:59+00:00");

    let work = claim_in(&f, "Europe/Berlin", "consolidate");
    let supplied = &work["input"]["candidates"][0];
    assert_eq!(supplied["evidence"][0]["local_time"], "2026-11-01T11:00:00+01:00 Sunday");
    assert_eq!(supplied["loop"]["due_at"], "2026-11-02T22:59:59+00:00");
    let mut keep = action(supplied, "remember");
    keep["open_loop"] = json!(true);
    let result = f.complete(&work, json!({"actions":[keep]})).unwrap();
    let target = result["results"][candidate.as_str()]["id"].as_str().unwrap().to_owned();
    assert_eq!(marker(&f, &target)["due_at"], "2026-11-02T22:59:59+00:00");

    // "Today" is due_before the next local midnight; overdue is due_before now.
    assert_eq!(listed(&f, "2026-11-02T00:00:00+01:00")["total"], 0);
    let today = listed(&f, "2026-11-03T00:00:00+01:00");
    assert_eq!((today["total"].clone(), today["loops"][0]["id"].clone()), (json!(1), json!(target)));
    assert_eq!(today["loops"][0]["due_at"], "2026-11-02T22:59:59+00:00");
    assert_eq!(listed(&f, "2026-11-02T18:00:00+01:00")["total"], 0);
    assert_eq!(listed(&f, "2026-11-03T09:00:00+01:00")["total"], 1);
}

#[test]
fn invented_absent_or_vague_due_dates_are_refused_with_retry_feedback() {
    let f = Fixture::new();
    f.capture("ask", "thread", ASK, "user", "contract", json!({}), "2026-11-01T10:00:00Z");
    let work = claim_in(&f, "UTC", "extract");
    for (due, open_loop, code) in [
        // An invented date: its expression is not in the supporting quote.
        (due("2026-11-06", "by Friday"), true, "candidate_due_quote_invalid"),
        (due("soon", "tomorrow"), true, "candidate_due_invalid"),
        (due("2026-11-02T09:00:00", "tomorrow"), true, "candidate_due_invalid"),
        (due("2026-11-02", "tomorrow"), false, "candidate_due_invalid"),
        (due("2026-10-31", "tomorrow"), true, "candidate_due_before_source"),
    ] {
        let error = f.complete(&work, dated(&work, UPLOAD, UPLOAD, open_loop, due)).unwrap_err();
        assert_eq!(error.to_string(), code);
        let context: Value = serde_json::from_str(&f.stored::<String>("SELECT context FROM jobs WHERE kind='extract'")).unwrap();
        let feedback = &context["previous_rejection"];
        assert_eq!((feedback["code"].as_str(), feedback["references"][0]["key"].as_str()), (Some(code), Some("fact")));
    }
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM candidates"), 0);
    // Vague timing ("soon") has no due date: an undated loop is valid.
    f.complete(&work, dated(&work, UPLOAD, UPLOAD, true, Value::Null)).unwrap();
    assert_eq!(marker(&f, &newest_candidate(&f))["due_at"], Value::Null);
}

#[test]
fn newer_explicit_date_updates_automatic_due_but_manual_choices_win() {
    for manual in [None, Some("remind"), Some("later")] {
        let f = Fixture::new();
        let target = promoted(&f, "UTC");
        assert_eq!(marker(&f, &target)["due_at"], "2026-11-02T23:59:59+00:00");
        match manual {
            Some("remind") => f.graph.loop_command(serde_json::from_value(json!({"action":"remind","id":target,"command_id":"manual-due","actor":"owner","why":"Owner date","due_at":"2026-11-20T09:00:00Z"})).unwrap()).unwrap(),
            Some(_) => f.graph.loop_command(serde_json::from_value(json!({"action":"later","id":target,"command_id":"manual-later","actor":"owner","why":"Not now","days":7})).unwrap()).unwrap(),
            None => Value::Null,
        };
        let expected = marker(&f, &target)["due_at"].clone();
        let restate = |event: &str, line: &str, date: &str, when: &str, at: &str| {
            f.capture(event, "thread", line, "user", "contract", json!({}), date);
            let work = claim_in(&f, "UTC", "extract");
            f.complete(&work, dated(&work, line, UPLOAD, true, due(at, when))).unwrap();
            let work = claim_in(&f, "UTC", "consolidate");
            let candidate = &work["input"]["candidates"][0];
            assert!(work["input"]["existing_nodes"].as_array().unwrap().iter().any(|node| node["id"] == json!(target)));
            let mut same = action(candidate, "duplicate");
            same["target"] = json!(target);
            same["open_loop"] = json!(true);
            f.complete(&work, json!({"actions":[same]})).unwrap();
        };
        // Captured later but dated before the pending source: never newer.
        restate("old", "Assets could land on Thursday instead.", "2026-10-31T08:00:00Z", "Thursday", "2026-11-05");
        assert_eq!(marker(&f, &target)["due_at"], expected);
        restate("move", "Asset upload moves to Wednesday.", "2026-11-02T08:00:00Z", "Wednesday", "2026-11-04");
        let now = marker(&f, &target);
        if manual.is_none() {
            assert_eq!(now["due_at"], "2026-11-04T23:59:59+00:00");
        } else {
            assert_eq!(now["due_at"], expected, "{manual:?}");
        }
        assert_eq!(now["state"], "open");
    }
}

#[test]
fn consolidation_dates_a_loop_only_from_an_original_premise() {
    let f = Fixture::new();
    f.capture("ask", "thread", ASK, "user", "contract", json!({}), "2026-11-01T10:00:00Z");
    let work = claim_in(&f, "America/New_York", "extract");
    f.complete(&work, dated(&work, UPLOAD, UPLOAD, true, Value::Null)).unwrap();
    let work = claim_in(&f, "America/New_York", "consolidate");
    let candidate = &work["input"]["candidates"][0];
    assert_eq!(candidate["evidence"][0]["local_time"], "2026-11-01T05:00:00-05:00 Sunday");
    for (open_loop, due, code) in [
        (json!(true), due("2026-11-06", "by Friday"), "consolidation_due_quote_invalid"),
        (Value::Null, due("2026-11-02", "tomorrow"), "consolidation_due_invalid"),
        (json!(true), due("2026-10-30", "tomorrow"), "consolidation_due_before_source"),
    ] {
        let mut keep = action(candidate, "remember");
        keep["open_loop"] = open_loop;
        keep["due"] = due;
        assert_eq!(f.complete(&work, json!({"actions":[keep]})).unwrap_err().to_string(), code);
    }
    let mut keep = action(candidate, "remember");
    keep["open_loop"] = json!(true);
    keep["due"] = due("2026-11-02", "tomorrow");
    let result = f.complete(&work, json!({"actions":[keep]})).unwrap();
    let target = result["results"][candidate["id"].as_str().unwrap()]["id"].as_str().unwrap();
    // End of 2026-11-02 in New York, the day after DST ended (EST).
    assert_eq!(marker(&f, target)["due_at"], "2026-11-03T04:59:59+00:00");
}

#[test]
fn automatic_due_date_does_not_protect_a_declined_marker() {
    let f = Fixture::new();
    f.capture("ask", "thread", ASK, "user", "contract", json!({}), "2026-11-01T10:00:00Z");
    let work = claim_in(&f, "UTC", "extract");
    f.complete(&work, dated(&work, UPLOAD, UPLOAD, true, due("2026-11-02", "tomorrow"))).unwrap();
    let work = claim_in(&f, "UTC", "consolidate");
    let candidate = &work["input"]["candidates"][0];
    let result = f.complete(&work, json!({"actions":[action(candidate, "remember")]})).unwrap();
    let target = result["results"][candidate["id"].as_str().unwrap()]["id"].as_str().unwrap();
    assert_eq!(marker(&f, candidate["id"].as_str().unwrap())["state"], "closed");
    assert_eq!(marker(&f, target), Value::Null);
}

#[test]
fn pre_due_extraction_lease_is_refused_and_rederived() {
    let f = Fixture::new();
    f.seed();
    let work = claim_in(&f, "UTC", "extract");
    f.graph.store.write(|tx| { tx.sql().execute("UPDATE jobs SET context=json_set(context,'$.policy_version','extract-v2') WHERE kind='extract' AND state='leased'", [])?; Ok(()) }).unwrap();
    let output = json!({"windows":[{"id":work["input"]["windows"][0]["id"],"candidates":[draft(FACT,"knowledge",false)]}]});
    assert_eq!(f.complete(&work, output.clone()).unwrap_err().to_string(), "Extraction policy changed; derive a fresh plan");
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM candidates"), 0);
    f.processing.release(serde_json::from_value(json!({"id":work["id"],"owner":work["owner"],"error":"service_rejected"})).unwrap()).unwrap();
    f.graph.store.write(|tx| { tx.sql().execute("UPDATE jobs SET available_at=0 WHERE kind='extract'", [])?; Ok(()) }).unwrap();
    let retry = claim_in(&f, "UTC", "extract");
    assert_eq!(retry["id"], work["id"]);
    f.complete(&retry, output).unwrap();
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM candidates"), 1);
}
