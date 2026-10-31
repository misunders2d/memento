//! Behavioral lessons see and link the existing entities they name; no model calls.
use super::*;

const LESSON: &str = "For stockq lookups, always use --ledger archive.";

fn approve(f: &Fixture, key: &str, result: Value) -> Value {
    if result["status"] == "pending_review" {
        f.graph.decide_review(serde_json::from_value(json!({"command_id":key,"actor":"owner","why":"Explicit synthetic owner approval","proposal_id":result["id"],"decision":"approve"})).unwrap()).unwrap()
    } else {
        result
    }
}

fn existing(f: &Fixture, key: &str, typ: &str, text: &str, episode: &str, quote: &str) -> String {
    let result = f.graph.remember(serde_json::from_value(json!({"command_id":key,"actor":"owner","why":"Original synthetic assertion","type":typ,"text":text,"scope":["professional"],"evidence":[{"episode_id":episode,"quote":quote}]})).unwrap()).unwrap();
    approve(f, &format!("approve-{key}"), result)["id"].as_str().unwrap().to_owned()
}

fn tool(f: &Fixture, key: &str, name: &str, line: &str) -> (String, String) {
    let episode = f.capture(key, "thread", line, "user", "contract", json!({}), "2026-10-18T11:00:00Z");
    (existing(f, key, "tool", name, &episode, name), episode)
}

fn remember_lesson(f: &Fixture, work: &Value, conduct: &str) -> String {
    let candidate = &work["input"]["candidates"][0];
    let mut lesson = action(candidate, "remember");
    lesson["behavior"] = json!({"applicability":"Inventory lookups","conduct":conduct,"rationale":"The owner directly stated this lookup practice.","exceptions":[],"authority":"explicit_user"});
    let result = f.complete(work, json!({"actions":[lesson]})).unwrap();
    result["results"][candidate["id"].as_str().unwrap()]["id"].as_str().unwrap().to_owned()
}

fn ids(value: &Value) -> Vec<String> {
    value.as_array().unwrap().iter().map(|id| id.as_str().unwrap().to_owned()).collect()
}

fn supplied(work: &Value) -> Vec<String> {
    work["input"]["existing_nodes"].as_array().unwrap().iter().map(|n| n["id"].as_str().unwrap().to_owned()).collect()
}

#[test]
fn new_lesson_sees_named_tools_then_links_through_seeded_maintenance() {
    let f = Fixture::new();
    // Same spelling, separate identities: both are supplied, neither is merged.
    let (stockq, tool_episode) = tool(&f, "stockq", "Stockq", "Stockq is the inventory lookup CLI.");
    let (namesake, _) = tool(&f, "stockq-band", "Stockq", "Stockq is also a band name.");
    let distractor = existing(&f, "ledger-service", "project", "Ledger Service", &tool_episode, "inventory lookup CLI");
    let episode = f.capture("lesson", "thread", LESSON, "user", "contract", json!({}), "2026-10-18T12:00:00Z");
    f.extract("contract", "thread", LESSON, "experience", false);
    let work = f.consolidate_work();
    let candidate = &work["input"]["candidates"][0];
    assert_eq!(candidate["type"], "experience");
    // A new lesson cannot link in its own plan, so it spends no ceiling on named entities.
    assert!(candidate.get("named_entities").is_none());
    // The validator offers a new candidate no link to an existing entity: one remember.
    let lesson = remember_lesson(&f, &work, "For stockq lookups, use --ledger archive.");
    let groups = f.graph.store.read(|tx| rows(tx.sql(), "SELECT seed_ids,frontier,origin,state FROM repair_groups", [])).unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0]["origin"], "lesson-links");
    assert_eq!(groups[0]["seed_ids"], json!([&lesson]).to_string());
    assert_eq!(groups[0]["frontier"], groups[0]["seed_ids"]);
    // The existing repair-group job makes the lesson a maintenance target with its tools.
    let work = f.consolidate_work();
    assert!(work["input"]["candidates"].as_array().unwrap().is_empty());
    assert_eq!(work["input"]["maintenance_ids"], json!([&lesson]));
    let nodes = work["input"]["existing_nodes"].as_array().unwrap();
    let target = nodes.iter().find(|n| n["id"] == lesson.as_str()).unwrap();
    assert_eq!(target["behavior"]["authority"], "explicit_user");
    // Same spelling, separate identities: both are supplied, neither is merged.
    let named = ids(&target["named_entities"]);
    assert_eq!(named.iter().cloned().collect::<std::collections::BTreeSet<_>>(), std::collections::BTreeSet::from([stockq.clone(), namesake.clone()]));
    assert!(!named.contains(&distractor), "a partial name is never a match");
    assert!(named.iter().all(|id| supplied(&work).contains(id)));
    let repair = json!({"operation":"link","target":lesson,"dst":stockq,"relation":"mentions","evidence":[{"episode_id":episode,"quote":LESSON,"occurrence":null},{"episode_id":tool_episode,"quote":"Stockq","occurrence":null}],"why":"The lesson's conduct is about the stockq inventory CLI"});
    let result = f.complete(&work, json!({"actions":[],"repairs":[repair]})).unwrap();
    let edge = f.graph.history(result["repair_results"][&lesson]["id"].as_str().unwrap()).unwrap();
    assert_eq!(edge[0]["src"], lesson.as_str());
    assert_eq!(edge[0]["dst"], stockq.as_str());
    assert_eq!(edge[0]["rel"], "mentions");
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM edge_versions"), 1);
    // Repair jobs carry no candidate actions, so they never seed another group.
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM repair_groups"), 1);
    assert_eq!(f.stored::<String>("SELECT state FROM repair_groups"), "done");
}

#[test]
fn lesson_maintenance_target_matches_conduct_within_the_context_ceiling() {
    let f = Fixture::new();
    let names = ["Zorvex", "Quillon", "Brakka", "Tamsin", "Vorlok", "Hextar"];
    let roster = f.capture("roster", "thread", &names.join(" "), "user", "contract", json!({}), "2026-10-18T10:00:00Z");
    let mut tools = names.iter().map(|name| existing(&f, &format!("tool-{name}"), "tool", name, &roster, name)).collect::<Vec<_>>();
    tools.sort();
    // Sixty lexically similar experiences exceed the ceiling on their own.
    let lines = (0..60).map(|i| format!("Inventory lookup incident {i} used the archive ledger.")).collect::<Vec<_>>();
    let history = f.capture("incidents", "thread", &lines.join(" "), "user", "contract", json!({}), "2026-10-18T10:30:00Z");
    for (i, line) in lines.iter().enumerate() {
        existing(&f, &format!("incident-{i}"), "experience", line, &history, line);
    }
    let text = "Inventory lookups should use the archive ledger.";
    f.capture("lesson", "thread", text, "user", "contract", json!({}), "2026-10-18T12:00:00Z");
    f.extract("contract", "thread", text, "experience", false);
    let work = f.consolidate_work();
    // New candidates get no named entities; only maintenance targets do.
    assert!(work["input"]["candidates"][0].get("named_entities").is_none());
    assert!(supplied(&work).len() <= 48);
    let lesson = remember_lesson(&f, &work, &format!("For {} lookups, use --ledger archive.", names.join(", ")));
    // The explicit backfill path reuses the same pending group for the same seeds.
    let seeded = f.stored::<String>("SELECT id FROM repair_groups WHERE origin='lesson-links'");
    let backfill = f.processing.maintain(processing::Maintenance { command_id:"backfill-lesson-links".into(),actor:"owner".into(),why:"Link existing lessons to named entities".into(),target_ids:vec![lesson.clone()],origin:Some("lesson-links".into()) }).unwrap();
    assert_eq!(backfill["repair_group_id"], seeded.as_str());
    let work = f.consolidate_work();
    assert_eq!(work["input"]["maintenance_ids"], json!([&lesson]));
    let shown = supplied(&work);
    assert!(shown.len() <= 48, "context ceiling");
    let target = work["input"]["existing_nodes"].as_array().unwrap().iter().find(|n| n["id"] == lesson.as_str()).unwrap();
    // Bounded per lesson, deterministic (equal rarity orders by id), and all supplied.
    assert_eq!(ids(&target["named_entities"]), tools[..3].to_vec());
    assert!(tools[..3].iter().all(|id| shown.contains(id)));
    assert!(tools[3..].iter().all(|id| !shown.contains(id)));
    // Comparison targets still fill the remaining ceiling.
    let incidents = work["input"]["existing_nodes"].as_array().unwrap().iter().filter(|n| n["type"] == "experience" && n["id"] != lesson.as_str()).count();
    assert!(incidents >= 40, "{incidents} comparison targets");
}
