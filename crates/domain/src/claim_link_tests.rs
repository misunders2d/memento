//! Claim/entity link capability and the existing authority fences; no model calls.
use super::*;

const CLAIM: &str = "Orion expedition preserves signed archives.";
const SUBJECT: &str = "Orion expedition";

fn extraction(f: &Fixture) -> (Value, Value, String) {
    let episode = f.capture("claim-link", "thread", CLAIM, "user", "contract", json!({}), "2026-10-18T12:00:00Z");
    let work = f.claim("contract", "thread", Some(vec!["extract".into()]))["work"][0].clone();
    let mut memory = draft(CLAIM, "knowledge", false);
    memory["key"] = json!("claim");
    memory["scope"] = json!(["personal"]);
    let mut entity = draft(SUBJECT, "project", false);
    entity["key"] = json!("subject");
    entity["scope"] = json!(["personal"]);
    let mut relation = draft(CLAIM, "knowledge", false);
    relation["key"] = json!("subject-link");
    relation["kind"] = json!("relation");
    relation["type"] = json!("mentions");
    relation["src_key"] = json!("claim");
    relation["dst_key"] = json!("subject");
    relation["scope"] = json!(["personal"]);
    let output = json!({"windows":[{"id":work["input"]["windows"][0]["id"],"candidates":[memory,entity,relation]}]});
    (work, output, episode)
}

#[test]
fn memory_entity_relation_extracts_and_promotes_in_one_consolidation_commit() {
    let f = Fixture::new();
    let (work, output, episode) = extraction(&f);
    assert_eq!(f.complete(&work, output).unwrap()["candidate_count"], 3);
    let work = f.consolidate_work();
    let candidates = work["input"]["candidates"].as_array().unwrap();
    let claim = candidates.iter().find(|c| c["kind"] == "memory").unwrap();
    let entity = candidates.iter().find(|c| c["kind"] == "entity").unwrap();
    let relation = candidates.iter().find(|c| c["kind"] == "relation").unwrap();
    assert_eq!(relation["src"], claim["id"]);
    assert_eq!(relation["dst"], entity["id"]);
    let mut link = action(relation, "link");
    link["type"] = Value::Null;
    link["text"] = Value::Null;
    link["scope"] = Value::Null;
    link["src"] = claim["id"].clone();
    link["dst"] = entity["id"].clone();
    link["relation"] = json!("mentions");
    // Deliberately submit the dependency before its remembers.
    let result = f.complete(&work, json!({"actions":[link,action(claim,"remember"),action(entity,"remember")]})).unwrap();
    let results = &result["results"];
    let fact_id = results[claim["id"].as_str().unwrap()]["id"].as_str().unwrap();
    let entity_id = results[entity["id"].as_str().unwrap()]["id"].as_str().unwrap();
    let edge_id = results[relation["id"].as_str().unwrap()]["id"].as_str().unwrap();
    let edge = f.graph.history(edge_id).unwrap();
    assert_eq!(edge[0]["src"], fact_id);
    assert_eq!(edge[0]["dst"], entity_id);
    assert_eq!(edge[0]["rel"], "mentions");
    assert_eq!(edge[0]["evidence"][0]["episode_id"], episode);
    assert_eq!(edge[0]["evidence"][0]["quote"], CLAIM);
    for id in [fact_id, entity_id] {
        assert_eq!(f.graph.history(id).unwrap()[0]["scope"], json!(["personal"]));
    }
    assert_eq!(f.stored::<i64>("SELECT count(DISTINCT commit_id) FROM (SELECT commit_id FROM node_versions UNION ALL SELECT commit_id FROM edge_versions)"), 1);
}

#[test]
fn relation_candidate_cannot_be_an_extraction_endpoint_and_batch_rolls_back() {
    let f = Fixture::new();
    let (work, mut output, _) = extraction(&f);
    output["windows"][0]["candidates"][2]["dst_key"] = json!("subject-link");
    let error = f.complete(&work, output).unwrap_err();
    assert_eq!(error.to_string(), "candidate_relation_endpoint_missing");
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM candidates"), 0);
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM extraction_runs"), 0);
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM jobs WHERE state='leased'"), 1);
}

fn approve(f: &Fixture, key: &str, result: Value) -> Value {
    if result["status"] == "pending_review" {
        f.graph.decide_review(serde_json::from_value(json!({"command_id":key,"actor":"owner","why":"Explicit synthetic owner approval","proposal_id":result["id"],"decision":"approve"})).unwrap()).unwrap()
    } else {
        result
    }
}

fn existing_pair(f: &Fixture, sensitive: bool) -> (String, String, String) {
    let episode = f.capture("existing-claim-link", "thread", CLAIM, "user", "contract", json!({}), "2026-10-18T12:00:00Z");
    let remember = |key: &str, typ: &str, text: &str| {
        let result = f.graph.remember(serde_json::from_value(json!({"command_id":key,"actor":"owner","why":"Original synthetic assertion","type":typ,"text":text,"scope":["personal"],"evidence":[{"episode_id":episode,"quote":CLAIM}]})).unwrap()).unwrap();
        approve(f, &format!("approve-{key}"), result)["id"].as_str().unwrap().to_owned()
    };
    let claim = remember("existing-claim", if sensitive {"private_claim_kind"} else {"knowledge"}, CLAIM);
    let entity = remember("existing-subject", "project", SUBJECT);
    (episode, claim, entity)
}

fn maintained_work(f: &Fixture, claim: &str, entity: &str) -> Value {
    f.processing.maintain(processing::Maintenance { command_id:"inspect-claim-links".into(),actor:"owner".into(),why:"Inspect existing subject link".into(),target_ids:vec![claim.into(),entity.into()],origin:None }).unwrap();
    let work = f.consolidate_work();
    assert!(work["input"]["candidates"].as_array().unwrap().is_empty());
    assert!(work["input"]["existing_nodes"].as_array().unwrap().iter().any(|n| n["id"] == claim));
    assert!(work["input"]["existing_nodes"].as_array().unwrap().iter().any(|n| n["id"] == entity));
    work
}

fn repair(episode: &str, claim: &str, entity: &str) -> Value {
    json!({"operation":"link","target":claim,"dst":entity,"relation":"mentions","evidence":[{"episode_id":episode,"quote":CLAIM,"occurrence":null}],"why":"Original claim explicitly identifies its subject"})
}

fn pin(f: &Fixture, target: &str) {
    // The fixture owner appends a real pin version; no immutable-row mutation.
    f.graph.store.command(&context("owner-pin".into(),"owner".into(),"Manual synthetic protection".into()), &json!({"op":"pin","node_id":target,"pinned":true}), |tx,commit| curation::pin(tx,commit,target,true)).unwrap();
}

#[test]
fn existing_only_link_preserves_scope_and_original_evidence() {
    let f = Fixture::new();
    let (episode, claim, entity) = existing_pair(&f, false);
    let work = maintained_work(&f, &claim, &entity);
    let result = f.complete(&work, json!({"actions":[],"repairs":[repair(&episode,&claim,&entity)]})).unwrap();
    let id = result["repair_results"][&claim]["id"].as_str().unwrap();
    let history = f.graph.history(id).unwrap();
    assert_eq!(history[0]["src"], claim);
    assert_eq!(history[0]["dst"], entity);
    assert_eq!(history[0]["evidence"][0]["episode_id"], episode);
    assert_eq!(history[0]["evidence"][0]["quote"], CLAIM);
    assert_eq!(f.graph.history(&claim).unwrap()[0]["scope"], json!(["personal"]));
    assert_eq!(f.graph.history(&entity).unwrap()[0]["text"], SUBJECT);
    assert_eq!(f.stored::<i64>("SELECT count(*) FROM candidates"), 0);
}

#[test]
fn existing_link_rejects_unsupplied_evidence_scope_forgetting_and_stale_versions_atomically() {
    for defect in ["unknown", "relation-endpoint", "empty-evidence", "invented-quote", "scope", "forgotten", "stale-version", "missing-dst", "supersedes"] {
        let f = Fixture::new();
        let (episode, claim, entity) = existing_pair(&f, false);
        let edge = if defect == "relation-endpoint" {
            Some(f.graph.link(serde_json::from_value(json!({"command_id":"existing-edge","actor":"owner","why":"Synthetic original relation","a":claim,"rel":"related_to","b":entity,"evidence":[{"episode_id":episode,"quote":CLAIM}]})).unwrap()).unwrap()["id"].clone())
        } else { None };
        let work = maintained_work(&f, &claim, &entity);
        let mut proposed = repair(&episode, &claim, &entity);
        match defect {
            "unknown" => proposed["dst"] = json!("not-supplied"),
            "relation-endpoint" => proposed["dst"] = edge.unwrap(),
            "empty-evidence" => proposed["evidence"] = json!([]),
            "invented-quote" => proposed["evidence"][0]["quote"] = json!("Not in the original source"),
            "scope" => proposed["scope"] = json!(["professional"]),
            "forgotten" => {
                let preview = f.graph.forget_preview(&entity).unwrap();
                f.graph.forget(serde_json::from_value(json!({"command_id":"forget-subject","actor":"owner","why":"Explicit synthetic forgetting","target":entity,"token":preview["token"]})).unwrap()).unwrap();
            }
            "stale-version" => pin(&f, &entity),
            "missing-dst" => { proposed.as_object_mut().unwrap().remove("dst"); }
            "supersedes" => proposed["relation"] = json!("supersedes"),
            _ => unreachable!(),
        }
        let commits = f.stored::<i64>("SELECT count(*) FROM graph_commits");
        assert!(f.complete(&work, json!({"actions":[],"repairs":[proposed]})).is_err(), "{defect}");
        assert_eq!(f.stored::<i64>("SELECT count(*) FROM graph_commits"), commits, "{defect}");
        assert_eq!(f.stored::<i64>("SELECT count(*) FROM edge_versions WHERE rel='mentions'"), 0, "{defect}");
        assert_eq!(f.graph.store.read(|tx| tx.original_text(&episode)).unwrap().unwrap(), CLAIM);
    }
}

#[test]
fn link_and_duplicate_evidence_preserve_pin_and_sensitive_review_paths() {
    for duplicate in [false, true] {
        for protection in ["none", "pin", "sensitive"] {
            let f = Fixture::new();
            let (episode, claim, entity) = existing_pair(&f, protection == "sensitive");
            let edge = if duplicate {
                let result = f.graph.link(serde_json::from_value(json!({"command_id":"existing-mentions","actor":"owner","why":"Original exact subject relation","a":claim,"rel":"mentions","b":entity,"evidence":[{"episode_id":episode,"quote":SUBJECT}]})).unwrap()).unwrap();
                Some(approve(&f,"approve-existing-mentions",result)["id"].as_str().unwrap().to_owned())
            } else { None };
            if protection == "pin" { pin(&f, &entity); }
            let work = maintained_work(&f, &claim, &entity);
            let result = f.complete(&work, json!({"actions":[],"repairs":[repair(&episode,&claim,&entity)]})).unwrap();
            let result = &result["repair_results"][&claim];
            if protection != "none" {
                assert_eq!(result["status"], "pending_review");
                assert_eq!(f.graph.pending_reviews().unwrap().len(), 1);
                assert_eq!(f.stored::<i64>("SELECT count(*) FROM edge_versions WHERE rel='mentions'"), if duplicate {1} else {0});
            } else if let Some(edge) = edge {
                assert_eq!(result["id"], edge);
                let history = f.graph.history(&edge).unwrap();
                assert_eq!(history.len(), 2);
                assert_eq!(history.last().unwrap()["evidence"].as_array().unwrap().len(), 2);
                assert_eq!(f.stored::<i64>("SELECT count(DISTINCT id) FROM edge_versions WHERE rel='mentions'"), 1);
            } else {
                assert_eq!(f.graph.history(result["id"].as_str().unwrap()).unwrap()[0]["rel"], "mentions");
            }
        }
    }
}

#[test]
fn existing_link_follows_only_same_plan_refinement_not_correction_or_world_change() {
    for kind in ["refinement", "correction", "change"] {
        let f = Fixture::new();
        let (episode, claim, entity) = existing_pair(&f, false);
        let work = maintained_work(&f, &claim, &entity);
        let replacement = json!({"operation":"supersede","target":entity,"type":"project","text":SUBJECT,"scope":["personal"],"change_kind":kind,"valid_at":if kind=="change" {json!("2026-10-18T12:00:00Z")} else {Value::Null},"evidence":[{"episode_id":episode,"quote":CLAIM,"occurrence":null}],"why":"Synthetic same-plan lifecycle control"});
        let result = f.complete(&work, json!({"actions":[],"repairs":[replacement,repair(&episode,&claim,&entity)]}));
        if kind == "refinement" {
            let result = result.unwrap();
            let replacement = &result["repair_results"][&entity]["superseded_by"];
            let edge = result["repair_results"][&claim]["id"].as_str().unwrap();
            assert_eq!(f.graph.history(edge).unwrap()[0]["dst"], *replacement);
        } else {
            assert!(result.is_err());
            assert_eq!(f.graph.history(&entity).unwrap().len(), 1);
            assert_eq!(f.stored::<i64>("SELECT count(*) FROM edge_versions"), 0);
        }
    }
}
