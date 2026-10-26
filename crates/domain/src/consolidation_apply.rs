use super::{
    Consolidator, VERSION, continuation, ineligible, native_trace, strings, unique, word_terms,
};
use crate::util::*;
use crate::{
    TxGraph, behavior_ground_truth, context, curation,
    extraction::source_span,
    importance,
    jobs::{Job, timestamp},
    loops,
    pipeline_schema::{self, WorkResult, reject},
};
use memento_core::{Error, Result, WriteTxn};
use rusqlite::{Connection, params};
use serde_json::{Map, Value, json};
use std::collections::{BTreeSet, HashMap};

fn keyed(value: &Value) -> Result<Map<String, Value>> {
    let mut result = Map::new();
    for item in value
        .as_array()
        .ok_or(Error::Validation("Invalid prepared context"))?
    {
        result.insert(get_str(item, "id")?.into(), item.clone());
    }
    Ok(result)
}
fn reject_item(
    code: &'static str,
    item: &Value,
    source: Option<&Value>,
) -> crate::pipeline_schema::WorkError {
    let mut reference = json!({"operation":item["operation"]});
    if item.get("candidate_id").is_some() {
        reference["candidate_id"] = item["candidate_id"].clone();
    } else {
        reference["target"] = item["target"].clone();
    }
    if let Some(source) = source {
        for key in ["episode_id", "quote", "occurrence"] {
            reference[key] = source[key].clone();
        }
    }
    reject(code, vec![reference])
}
fn span(episodes: &Map<String, Value>, source: &Value) -> Result<(usize, usize)> {
    source_span(
        get_str(
            episodes
                .get(get_str(source, "episode_id")?)
                .ok_or(Error::Validation("consolidation_evidence_not_supplied"))?,
            "text",
        )?,
        get_str(source, "quote")?,
        source["occurrence"].as_u64().map(|value| value as usize),
    )
}
fn technical_grounding(
    action: &Value,
    candidate: &Value,
    episodes: &Map<String, Value>,
    traces: &[Value],
) -> Result<bool> {
    if action["behavior"]["authority"] != "inferred" {
        return Ok(false);
    }
    let evidence = action["evidence"].as_array().unwrap();
    for source in evidence {
        let episode = &episodes[get_str(source, "episode_id")?];
        let (start, end) = span(episodes, source)?;
        if native_trace(episode).is_none()
            && !behavior_ground_truth(episode, Some(start), Some(end))
        {
            return Ok(false);
        }
    }
    let cited = evidence
        .iter()
        .map(|source| source["episode_id"].as_str().unwrap().to_owned())
        .collect::<BTreeSet<_>>();
    let selected = if action["technical_evidence"].is_null() {
        cited.clone()
    } else {
        strings(&action["technical_evidence"])?
            .into_iter()
            .collect()
    };
    let original = candidate["evidence"]
        .as_array()
        .unwrap()
        .iter()
        .map(|source| source["episode_id"].as_str().unwrap().to_owned())
        .filter(|id| cited.contains(id))
        .collect::<BTreeSet<_>>();
    for anchor in original {
        let episode = &episodes[&anchor];
        if native_trace(episode).is_none() {
            continue;
        }
        if cited.iter().any(|id| {
            episodes[id]["source"] != episode["source"]
                || episodes[id]["conversation_id"] != episode["conversation_id"]
        }) {
            continue;
        }
        let mut trace = Map::new();
        for item in traces {
            if item["episode_id"]
                .as_str()
                .is_some_and(|id| selected.contains(id))
                && item["native_trace"].as_str().is_some()
                && item["source"] == episode["source"]
                && item["conversation_id"] == episode["conversation_id"]
            {
                trace.insert(get_str(item, "episode_id")?.into(), item.clone());
            }
        }
        if !action["technical_evidence"].is_null()
            && trace.keys().cloned().collect::<BTreeSet<_>>() != selected
        {
            continue;
        }
        let attempts = trace
            .values()
            .filter(|item| item["native_trace"] == "attempt")
            .filter_map(|item| item["sequence"].as_i64())
            .max();
        let results = trace
            .values()
            .filter(|item| item["native_trace"] == "result")
            .filter_map(|item| item["sequence"].as_i64())
            .max();
        if let (Some(attempt), Some(result)) = (attempts, results) {
            if attempt < result {
                return Ok(true);
            }
        }
    }
    Ok(false)
}
struct Prepared {
    selected: Map<String, Value>,
    nodes: Map<String, Value>,
    edges: Map<String, Value>,
    episodes: Map<String, Value>,
    actions: Vec<Value>,
    repairs: Vec<Value>,
    continue_with: Vec<String>,
    resolutions: Map<String, Value>,
}
fn validate(
    db: &Connection,
    dir: &std::path::Path,
    context: &Value,
    output: &Value,
) -> WorkResult<Prepared> {
    if !pipeline_schema::validate(&pipeline_schema::CONSOLIDATION, output) {
        return Err(reject("consolidation_schema_invalid", vec![]));
    }
    let selected = keyed(&context["candidates"])?;
    let nodes = keyed(&context["existing_nodes"])?;
    let edges = keyed(&context["existing_relations"])?;
    let windows = context["raw_windows"]
        .as_array()
        .ok_or(Error::Validation("Invalid prepared windows"))?;
    let mut spans: HashMap<String, Vec<(usize, usize)>> = HashMap::new();
    let mut traces = windows.clone();
    for item in nodes.values().chain(edges.values()) {
        for reference in item["evidence"].as_array().unwrap() {
            spans
                .entry(get_str(reference, "episode_id")?.into())
                .or_default()
                .push((
                    reference["start"].as_u64().unwrap() as usize,
                    reference["end"].as_u64().unwrap() as usize,
                ));
            if reference["native_trace"].as_str().is_some() {
                traces.push(reference.clone());
            }
        }
    }
    let mut supplied = windows
        .iter()
        .map(|window| window["episode_id"].as_str().unwrap().to_owned())
        .collect::<BTreeSet<_>>();
    supplied.extend(spans.keys().cloned());
    let mut actions = output["actions"].as_array().unwrap().clone();
    let mut repairs = output
        .get("repairs")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut cited = BTreeSet::new();
    for item in actions.iter().chain(&repairs) {
        for source in item["evidence"].as_array().unwrap() {
            cited.insert(get_str(source, "episode_id")?.to_owned());
        }
    }
    if !cited.is_subset(&supplied) {
        return Err(reject("consolidation_evidence_not_supplied", vec![]));
    }
    let mut episodes = Map::new();
    for id in cited {
        let mut episode = one(db, "SELECT * FROM episodes WHERE id=?", [&id])?
            .ok_or(Error::Validation("extraction_source_missing"))?;
        episode["text"] = json!(memento_core::raw::read_original(db, dir, &id)?.ok_or(
            Error::Unavailable("Original raw source artifact is unavailable or corrupt")
        )?);
        episode["metadata"] = decode_json(get_str(&episode, "metadata")?)?;
        episodes.insert(id, episode);
    }
    if actions.len() != selected.len()
        || actions
            .iter()
            .map(|item| item["candidate_id"].as_str().unwrap().to_owned())
            .collect::<BTreeSet<_>>()
            != selected.keys().cloned().collect()
    {
        return Err(reject("consolidation_candidate_coverage_invalid", vec![]));
    }
    let mut allowed = selected
        .keys()
        .chain(nodes.keys())
        .chain(edges.keys())
        .chain(context["resolved_candidates"].as_object().unwrap().keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    for candidate in selected.values() {
        for key in ["src", "dst"] {
            if let Some(id) = candidate[key].as_str() {
                allowed.insert(id.into());
            }
        }
    }
    let authorized = |source: &Value, start: usize, end: usize| {
        let id = source["episode_id"].as_str().unwrap();
        windows.iter().any(|window| {
            window["episode_id"] == id
                && window["start"].as_u64().unwrap() as usize <= start
                && end <= window["end"].as_u64().unwrap() as usize
        }) || spans.get(id).is_some_and(|spans| {
            spans
                .iter()
                .any(|(left, right)| *left <= start && end <= *right)
        })
    };
    for action in &mut actions {
        for key in ["target", "src", "dst"] {
            if action[key].as_str().is_some_and(|id| !allowed.contains(id)) {
                return Err(reject_item(
                    "consolidation_reference_not_supplied",
                    action,
                    None,
                ));
            }
        }
        let closed = action
            .get("closed_targets")
            .map(strings)
            .transpose()?
            .unwrap_or_default();
        if closed.iter().collect::<BTreeSet<_>>().len() != closed.len()
            || closed
                .iter()
                .any(|id| context["loop_snapshots"].get(id).is_none())
        {
            return Err(reject_item(
                "consolidation_loop_target_not_supplied",
                action,
                None,
            ));
        }
        if !closed.is_empty() && action["open_loop"] == true {
            return Err(reject_item(
                "consolidation_loop_state_contradictory",
                action,
                None,
            ));
        }
        action["closed_targets"] = json!(closed);
        for source in action["evidence"].as_array().unwrap() {
            let (start, end) = span(&episodes, source).map_err(|error| match error {
                Error::Validation(code) => reject_item(code, action, Some(source)),
                other => other.into(),
            })?;
            if !authorized(source, start, end) {
                return Err(reject_item(
                    "consolidation_evidence_outside_context",
                    action,
                    Some(source),
                ));
            }
        }
        let candidate = &selected[get_str(action, "candidate_id")?];
        let original = candidate["evidence"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|reference| reference["episode_id"].as_str())
            .collect::<BTreeSet<_>>();
        if !matches!(action["operation"].as_str(), Some("reject" | "defer"))
            || !action["closed_targets"].as_array().unwrap().is_empty()
        {
            if !action["evidence"].as_array().unwrap().iter().any(|source| {
                source["episode_id"]
                    .as_str()
                    .is_some_and(|id| original.contains(id))
            }) {
                return Err(reject_item(
                    "consolidation_missing_candidate_source",
                    action,
                    None,
                ));
            }
        }
        validate_behavior(action, candidate, &episodes, &traces).map_err(|error| match error {
            Error::Validation(code) => reject_item(code, action, None),
            other => other.into(),
        })?;
        for field in ["valid_from", "valid_to", "valid_at"] {
            action[field] = json!(time(action[field].as_str())?);
        }
        if let (Some(left), Some(right)) =
            (action["valid_from"].as_str(), action["valid_to"].as_str())
        {
            if parse_time(left)? >= parse_time(right)? {
                return Err(reject_item(
                    "consolidation_world_interval_invalid",
                    action,
                    None,
                ));
            }
        }
    }
    let continue_with = output
        .get("continue_with")
        .map(strings)
        .transpose()?
        .unwrap_or_default();
    if continue_with.iter().collect::<BTreeSet<_>>().len() != continue_with.len()
        || continue_with.iter().any(|id| !nodes.contains_key(id))
    {
        return Err(reject("consolidation_reference_not_supplied", vec![]));
    }
    let mut repaired = BTreeSet::new();
    for repair in &mut repairs {
        let target = get_str(repair, "target")?.to_owned();
        let absorbed = repair
            .get("absorbed")
            .map(strings)
            .transpose()?
            .unwrap_or_default();
        let mut affected = vec![target.clone()];
        affected.extend(absorbed.clone());
        if affected.iter().collect::<BTreeSet<_>>().len() != affected.len()
            || affected
                .iter()
                .any(|id| repaired.contains(id) || !nodes.contains_key(id))
        {
            return Err(reject("consolidation_reference_not_supplied", vec![]));
        }
        repaired.extend(affected.clone());
        if (repair["operation"] == "absorb") != !absorbed.is_empty() {
            return Err(reject("consolidation_reference_not_supplied", vec![]));
        }
        repair["absorbed"] = json!(absorbed);
        if repair["operation"] == "absorb" {
            if let Some(code) = curation::absorb_check(db, &target, &absorbed)? {
                return Err(reject_item(code, repair, None));
            }
        } else if repair["operation"] == "retire" {
            if let Some(code) = retire_check(db, &target, &repair["evidence"])? {
                return Err(reject_item(code, repair, None));
            }
        }
        for source in repair["evidence"].as_array().unwrap() {
            let (start, end) = span(&episodes, source)?;
            if !authorized(source, start, end) {
                return Err(reject("consolidation_evidence_outside_context", vec![]));
            }
        }
        if repair["operation"] != "close_loop" {
            for target in &affected {
                let mut grounded = false;
                for source in repair["evidence"].as_array().unwrap() {
                    let (start, end) = span(&episodes, source)?;
                    if nodes[target]["evidence"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|reference| {
                            reference["episode_id"] == source["episode_id"]
                                && reference["start"].as_u64().unwrap() as usize <= start
                                && end <= reference["end"].as_u64().unwrap() as usize
                        })
                    {
                        grounded = true;
                        break;
                    }
                }
                if !grounded {
                    return Err(reject("consolidation_missing_candidate_source", vec![]));
                }
            }
        }
        for field in ["valid_from", "valid_to", "valid_at"] {
            repair[field] = json!(time(repair[field].as_str())?);
        }
        if repair["operation"] == "supersede" {
            if repair["type"].as_str().is_none_or(str::is_empty)
                || repair["text"].as_str().is_none_or(str::is_empty)
                || repair["change_kind"].is_null()
            {
                return Err(reject("consolidation_supersede_invalid", vec![]));
            }
            if repair["change_kind"] == "change" && repair["valid_at"].is_null() {
                return Err(reject("consolidation_change_time_required", vec![]));
            }
        } else if [
            "type",
            "text",
            "scope",
            "change_kind",
            "valid_at",
            "valid_from",
            "valid_to",
            "behavior",
        ]
        .iter()
        .any(|field| !repair[*field].is_null())
        {
            return Err(reject("consolidation_schema_invalid", vec![]));
        }
        validate_behavior(repair, &nodes[&target], &episodes, &traces)?;
    }
    Ok(Prepared {
        selected,
        nodes,
        edges,
        episodes,
        actions,
        repairs,
        continue_with,
        resolutions: context["resolved_candidates"].as_object().unwrap().clone(),
    })
}
fn validate_behavior(
    action: &Value,
    candidate: &Value,
    episodes: &Map<String, Value>,
    traces: &[Value],
) -> Result<()> {
    let evidence = action["evidence"].as_array().unwrap();
    if !action["technical_evidence"].is_null() {
        let selected = strings(&action["technical_evidence"])?;
        if action["behavior"]["authority"] != "inferred"
            || selected.iter().collect::<BTreeSet<_>>().len() != selected.len()
            || selected
                .iter()
                .any(|id| !evidence.iter().any(|source| source["episode_id"] == *id))
        {
            return Err(Error::Validation(
                "consolidation_technical_evidence_invalid",
            ));
        }
    }
    if !action["behavior"].is_null() {
        if !matches!(action["operation"].as_str(), Some("remember" | "supersede"))
            || action["type"] != "experience"
        {
            return Err(Error::Validation(
                "consolidation_behavior_requires_experience",
            ));
        }
        if action["behavior"]["exceptions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| {
                value
                    .as_str()
                    .is_none_or(|value| value.trim().is_empty() || value.chars().count() > 1200)
            })
        {
            return Err(Error::Validation(
                "consolidation_behavior_exception_invalid",
            ));
        }
        let original = candidate["evidence"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|reference| reference["episode_id"].as_str())
            .collect::<BTreeSet<_>>();
        let mut grounded = false;
        for source in evidence {
            let id = get_str(source, "episode_id")?;
            if original.contains(id) {
                let (start, end) = span(episodes, source)?;
                if behavior_ground_truth(&episodes[id], Some(start), Some(end)) {
                    grounded = true;
                    break;
                }
            }
        }
        if !grounded && !technical_grounding(action, candidate, episodes, traces)? {
            return Err(Error::Validation(
                "consolidation_behavior_requires_user_ground_truth",
            ));
        }
    }
    Ok(())
}
fn retire_check(db: &Connection, target: &str, evidence: &Value) -> Result<Option<&'static str>> {
    let n = match node(db, target) {
        Ok(n) => n,
        Err(Error::Validation(_)) | Err(Error::NotFound(_)) => {
            return Ok(Some("consolidation_retire_target_invalid"));
        }
        Err(error) => return Err(error),
    };
    if n["state"] != "active" {
        return Ok(Some("consolidation_retire_target_invalid"));
    }
    if !memory_type(db, get_str(&n, "type")?)? {
        return Ok(Some("consolidation_retire_type_invalid"));
    }
    let refs = rows(
        db,
        "SELECT episode_id,quote FROM graph_evidence WHERE target=? AND version=?",
        params![target, n["version"].as_i64()],
    )?;
    if evidence.as_array().is_none_or(|evidence| {
        evidence.is_empty()
            || evidence.iter().any(|source| {
                source["quote"].as_str().is_none_or(|quote| {
                    quote.is_empty()
                        || !refs.iter().any(|reference| {
                            reference["episode_id"] == source["episode_id"]
                                && reference["quote"]
                                    .as_str()
                                    .is_some_and(|text| text.contains(quote))
                        })
                })
            })
    }) {
        return Ok(Some("consolidation_retire_evidence_invalid"));
    }
    Ok(None)
}

pub(super) fn apply(
    consolidator: &Consolidator,
    job: &Job,
    context_value: &Value,
    output: &Value,
    agent: &str,
    model: Option<&str>,
    usage: Option<&Value>,
) -> WorkResult<Value> {
    if context_value["loop_policy_version"] != VERSION {
        return Err(
            Error::Conflict("Consolidation loop policy changed; derive a fresh plan").into(),
        );
    }
    let ids = context_value["candidates"]
        .as_array()
        .ok_or(Error::Validation("Invalid prepared context"))?
        .iter()
        .map(|candidate| candidate["id"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    if !consolidator
        .graph
        .store
        .read(|tx| ineligible(tx.sql(), &ids))?
        .is_empty()
    {
        return Err(
            Error::Conflict("Consolidation eligibility changed; derive a fresh plan").into(),
        );
    }
    let prepared = consolidator
        .graph
        .store
        .read(|tx| Ok(validate(tx.sql(), tx.data_dir(), context_value, output)))??;
    for repair in &prepared.repairs {
        if repair["operation"] == "retire"
            && context_value["_retirement"]["ready"]
                .get(get_str(repair, "target")?)
                .is_none()
        {
            return Err(reject_item(
                "consolidation_retire_context_incomplete",
                repair,
                None,
            ));
        }
    }
    let request = json!({"op":"consolidate","job_id":job.id,"version":VERSION,"agent":agent,"reported_model":model,"usage":usage,"output":output});
    let mut reasons = vec![];
    for action in &prepared.actions {
        reasons.push(format!(
            "{}: {}",
            get_str(action, "candidate_id")?,
            get_str(action, "why")?
        ));
    }
    for repair in &prepared.repairs {
        reasons.push(format!(
            "{}: {}",
            get_str(repair, "target")?,
            get_str(repair, "why")?
        ));
    }
    let why = if reasons.is_empty() {
        "Inspected supplied memory context".into()
    } else {
        reasons.join("\n")
    };
    let ctx = context(
        format!("consolidate/{}", job.id),
        format!("consolidator/{agent}"),
        why,
    );
    Ok(consolidator.graph.store.command(&ctx,&request,|tx,commit|{
        job.check_in(tx.sql())?;check_continuity(tx.sql(),context_value)?;for repair in &prepared.repairs{if repair["operation"]=="retire"{check_retirement(tx.sql(),context_value,get_str(repair,"target")?)?;}}
        if !ineligible(tx.sql(),&ids)?.is_empty(){return Err(Error::Conflict("Consolidation eligibility changed; derive a fresh plan"));}for(target,frozen)in &prepared.nodes{if node(tx.sql(),target)?["version"]!=frozen["version"]{return Err(Error::Conflict("Consolidation graph context changed; derive a fresh plan"));}}for(target,frozen)in &prepared.edges{if one(tx.sql(),"SELECT version FROM edge_versions WHERE id=? ORDER BY version DESC LIMIT 1",[target])?.is_none_or(|current|current["version"]!=frozen["version"]){return Err(Error::Conflict("Consolidation relation context changed; derive a fresh plan"));}}
        let loop_snapshots=context_value["loop_snapshots"].as_object().unwrap();if !prepared.repairs.is_empty()||prepared.actions.iter().any(|action|!action["closed_targets"].as_array().unwrap().is_empty()){for(target,frozen)in loop_snapshots{let version=prepared.nodes.get(target).or_else(||prepared.edges.get(target)).and_then(|value|value["version"].as_i64()).unwrap_or(1);if loops::snapshot(tx.sql(),target,version)?!=*frozen{return Err(Error::Conflict("Consolidation loop state changed; derive a fresh plan"));}}}
        let mut closures=vec![];let mut closure_sources:HashMap<String,Vec<Option<String>>>=HashMap::new();for action in &prepared.actions{let candidate_id=get_str(action,"candidate_id")?;let original=prepared.selected[candidate_id]["evidence"].as_array().unwrap().iter().map(|reference|reference["episode_id"].as_str().unwrap()).collect::<BTreeSet<_>>();let sources=action["evidence"].as_array().unwrap().iter().filter(|source|original.contains(source["episode_id"].as_str().unwrap())).map(|source|prepared.episodes[source["episode_id"].as_str().unwrap()]["occurred_at"].as_str().map(str::to_owned)).collect::<Vec<_>>();for target in action["closed_targets"].as_array().unwrap(){let target=target.as_str().unwrap();let(at,status)=loops::closure_time(&loop_snapshots[target],&sources)?;closures.push(json!({"candidate_id":candidate_id,"target":target,"source_occurred_at":at,"status":status}));}closure_sources.insert(candidate_id.into(),sources);}
        let mut results=Map::new();let mut destinations:Map<String,Value>=Map::new();let mut resolutions=prepared.resolutions.clone();let mut actions=prepared.actions.clone();actions.sort_by_key(|action|(action["operation"]=="link",!matches!(action["operation"].as_str(),Some("remember"|"reject"|"defer"))));
        for action in &actions{let candidate_id=get_str(action,"candidate_id")?;let candidate=&prepared.selected[candidate_id];if one(tx.sql(),&format!("SELECT 1 FROM candidates c WHERE c.id=? AND {ACTIVE_CANDIDATE}"),[candidate_id])?.is_none(){continue;}let decision=one(tx.sql(),"SELECT id FROM candidate_decisions WHERE candidate_id=? ORDER BY rowid DESC LIMIT 1",[candidate_id])?;if decision.map(|decision|decision["id"].clone()).unwrap_or(Value::Null)!=candidate["decision_id"]{return Err(Error::Conflict("Candidate decision changed while consolidation was running"));}
            let(mut result,state)=apply_action(tx,commit,action,candidate,&resolutions,&prepared.nodes,&destinations)?;let target=result["superseded_by"].as_str().or_else(||result["merged_into"].as_str()).or_else(||result["id"].as_str()).map(str::to_owned);
            if state=="promote"{if let Some(target)=&target{let marker=loops::current(tx.sql(),candidate_id)?;if !(action["open_loop"]==false&&loops::decline_extracted(tx,commit,candidate_id)?){loops::transfer(tx,commit,candidate_id,target)?;}importance::transfer_usage(tx,candidate_id,target)?;loops::mark(tx,commit,target,action["open_loop"].as_bool(),Some(candidate_id),marker.as_ref())?;}}else if state=="review"{loops::mark(tx,commit,candidate_id,action["open_loop"].as_bool(),None,None)?;}
            tx.sql().execute("INSERT INTO candidate_decisions VALUES (?,?,?,?,?,?)",params![id(),candidate_id,commit,state,target,if state=="defer"{Some(timestamp()+3600.0)}else{None}])?;if ["promote","reject"].contains(&state){tx.sql().execute("UPDATE vector_blobs SET current=0 WHERE seq IN (SELECT seq FROM vector_items WHERE kind='candidate' AND target=?)",[candidate_id])?;}tx.touch_target(candidate_id)?;
            if state=="promote"{if let Some(target)=&target{resolutions.insert(candidate_id.into(),json!(target));if action["operation"]=="supersede"{if let Some(previous)=action["target"].as_str(){let previous=resolutions.get(previous).and_then(Value::as_str).unwrap_or(previous);destinations.insert(previous.into(),json!(target));}}}}
            result["decision"]=json!(state);results.insert(candidate_id.into(),result);
        }
        let mut repair_results=Map::new();for repair in &prepared.repairs{let original=get_str(repair,"target")?;let target=follow_refinements(tx.sql(),original,&destinations)?;let evidence=&repair["evidence"];let result=match get_str(repair,"operation")?{
            "absorb"=>{let mut absorbed=vec![];for source in strings(&repair["absorbed"])?{absorbed.push(follow_refinements(tx.sql(),&source,&destinations)?);}let result=curation::absorb(tx,commit,&target,&absorbed,Some(evidence),false,None)?;if result["status"]=="absorbed"{for source in absorbed{if node(tx.sql(),&source)?["state"]=="merged"{destinations.insert(source,json!(target));}}}result},
            "supersede"=>{let result=TxGraph::supersede(tx,commit,&json!({"op":"supersede","old":target,"new":{"type":repair["type"],"text":repair["text"],"scope":repair.get("scope").filter(|scope|!scope.is_null()).unwrap_or(&prepared.nodes[original]["scope"])},"kind":repair["change_kind"],"valid_at":repair["valid_at"],"evidence":evidence}),false)?;if let Some(new)=result["superseded_by"].as_str(){destinations.insert(target.clone(),json!(new));}result},
            "decline_loop"=>loops::decline_automatic(tx,commit,&target,&loop_snapshots[original])?,
            "retire"=>curation::retire(tx,commit,&target,evidence)?,
            "close_loop"=>{let sources=evidence.as_array().unwrap().iter().map(|source|prepared.episodes[source["episode_id"].as_str().unwrap()]["occurred_at"].as_str().map(str::to_owned)).collect::<Vec<_>>();let(at,mut status)=loops::closure_time(&loop_snapshots[original],&sources)?;if prepared.nodes[original]["pinned"].as_i64().unwrap_or(0)!=0{status=Some("protected_loop_choice");}if status.is_none(){let previous=loops::current(tx.sql(),&target)?.ok_or(Error::Conflict("Consolidation loop state changed; derive a fresh plan"))?;loops::append(tx,commit,&target,"closed",previous["due_at"].as_str(),None)?;status=Some("closed");}closures.push(json!({"target":target,"status":status,"source_occurred_at":at}));json!({"id":target,"status":status,"source_occurred_at":at})},
            _=>return Err(Error::Validation("consolidation_schema_invalid")),
        };repair_results.insert(original.into(),result);}
        closures.sort_by(|a,b|b["source_occurred_at"].as_str().unwrap_or("").cmp(a["source_occurred_at"].as_str().unwrap_or("")));for closure in &mut closures{let original=get_str(closure,"target")?.to_owned();let mut target=resolutions.get(&original).and_then(Value::as_str).unwrap_or(&original).to_owned();let mut seen=BTreeSet::new();while let Some(next)=destinations.get(&target).and_then(Value::as_str){if !seen.insert(target.clone()){return Err(Error::Conflict("Consolidation identity cycle"));}target=next.into();}closure["target"]=json!(target);if !closure["status"].is_null(){continue;}let candidate_id=get_str(closure,"candidate_id")?.to_owned();if target!=original&&loop_snapshots.contains_key(&target){let(at,status)=loops::closure_time(&loop_snapshots[&target],&closure_sources[&candidate_id])?;closure["source_occurred_at"]=json!(at);closure["status"]=json!(status);if status.is_some(){continue;}}
            if !matches!(results.get(&candidate_id).and_then(|result|result["decision"].as_str()),Some("promote"|"reject")){closure["status"]=json!("decision_pending");continue;}let marker=loops::current(tx.sql(),&target)?;if marker.as_ref().is_none_or(|marker|marker["state"]!="open"){closure["status"]=json!("not_open");continue;}loops::append(tx,commit,&target,"closed",marker.as_ref().unwrap()["due_at"].as_str(),None)?;closure["status"]=json!("closed");
        }
        if !job.payload["night"].is_null(){tx.sql().execute("UPDATE consolidation_nights SET cursor=max(cursor,?) WHERE id=?",params![job.payload["next_cursor"].as_i64(),job.payload["night"].as_str()])?;}
        let mut context=context_value.clone();let redirected=apply_continuity(tx,commit,&mut context)?;let mut progressed=!redirected.is_empty();for(target,n)in &prepared.nodes{progressed|=node(tx.sql(),target)?["version"]!=n["version"];}if !progressed{for(target,frozen)in loop_snapshots{let current=one(tx.sql(),"SELECT id FROM loop_events WHERE target=? ORDER BY rowid DESC LIMIT 1",[target])?.map(|row|row["id"].clone()).unwrap_or(Value::Null);if current!=frozen["event_id"]{progressed=true;break;}}}
        continue_work(tx,commit,job,&context,&prepared.continue_with,&prepared.nodes,&destinations,progressed)?;
        Ok(json!({"candidate_count":results.len(),"results":results,"reported_model":model,"usage":usage,"loop_closures":closures,"repair_results":repair_results,"redirected_relations":redirected}))
    })?)
}

fn resolve(
    db: &Connection,
    target: Option<&str>,
    resolutions: &Map<String, Value>,
    destinations: &Map<String, Value>,
    follow: bool,
) -> Result<Option<String>> {
    let Some(target) = target else {
        return Ok(None);
    };
    let mut target = if target.starts_with("c_") {
        let Some(target) = resolutions.get(target).and_then(Value::as_str) else {
            return Ok(None);
        };
        target.to_owned()
    } else {
        target.to_owned()
    };
    let mut seen = BTreeSet::new();
    while let Some(next) = destinations.get(&target).and_then(Value::as_str) {
        if !follow
            || !matches!(
                node(db, &target)?["state"].as_str(),
                Some("merged" | "refined")
            )
        {
            return Ok(None);
        }
        if !seen.insert(target.clone()) {
            return Err(Error::Conflict("Consolidation identity cycle"));
        }
        target = next.into();
    }
    Ok(Some(target))
}
fn follow_refinements(
    db: &Connection,
    target: &str,
    destinations: &Map<String, Value>,
) -> Result<String> {
    let mut target = target.to_owned();
    let mut seen = BTreeSet::new();
    while let Some(next) = destinations.get(&target).and_then(Value::as_str) {
        if node(db, &target)?["state"] != "refined" {
            break;
        }
        if !seen.insert(target.clone()) {
            return Err(Error::Conflict("Consolidation identity cycle"));
        }
        target = next.into();
    }
    Ok(target)
}
fn apply_action(
    tx: &WriteTxn,
    commit: &str,
    action: &Value,
    candidate: &Value,
    resolutions: &Map<String, Value>,
    supplied: &Map<String, Value>,
    destinations: &Map<String, Value>,
) -> Result<(Value, &'static str)> {
    let op = get_str(action, "operation")?;
    let evidence = &action["evidence"];
    let labels = action
        .get("scope")
        .filter(|value| !value.is_null())
        .unwrap_or(&candidate["scope"]);
    let result = match op {
        "reject" => return Ok((json!({"status":"reject"}), "reject")),
        "defer" => return Ok((json!({"status":"defer"}), "defer")),
        "remember" => {
            let kind = action["type"]
                .as_str()
                .filter(|value| !value.is_empty())
                .ok_or(Error::Validation("consolidation_remember_invalid"))?;
            let text = action["text"]
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .ok_or(Error::Validation("consolidation_remember_invalid"))?;
            if !known_type(tx.sql(), kind)? {
                curation::propose(
                    tx,
                    commit,
                    &json!({"op":"register_type","name":kind,"category":if candidate["kind"]=="entity"{"entity"}else{"memory"},"sensitive":true,"evidence":evidence}),
                )?
            } else {
                let mut request = json!({"op":"remember","type":kind,"text":text,"scope":labels,"valid_from":action["valid_from"],"valid_to":action["valid_to"],"evidence":evidence});
                let collisions = crate::exact_identities(tx.sql(), kind, text)?
                    .into_iter()
                    .filter(|item| item["commit_id"] != commit)
                    .collect::<Vec<_>>();
                let exact = collisions
                    .iter()
                    .map(|item| item["id"].as_str().unwrap())
                    .collect::<Vec<_>>();
                let unchecked = exact.iter().any(|target| !supplied.contains_key(*target));
                if !unchecked {
                    TxGraph::remember(tx, commit, &request, false)?
                } else if candidate["identity"].is_object()
                    && candidate["identity"]["exact_total"].as_u64()
                        == candidate["identity"]["exact"]
                            .as_array()
                            .map(|items| items.len() as u64)
                    && kind == candidate["type"]
                    && identity(text) == identity(get_str(candidate, "text")?)
                {
                    return Err(Error::Conflict(
                        "Consolidation identity context changed; derive a fresh plan",
                    ));
                } else {
                    let scope_values = labels
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|value| value.as_str().unwrap().to_owned())
                        .collect::<Vec<_>>();
                    request["scope"] = json!(scope(tx.sql(), kind, &scope_values)?);
                    request["identity"] = json!({"status":"exact_matches_not_supplied","exact_total":exact.len(),"supplied":exact.iter().filter(|target|supplied.contains_key(**target)).collect::<Vec<_>>()});
                    let request = TxGraph::resolve_evidence(tx, &request)?;
                    let bases = collisions
                        .iter()
                        .map(|item| {
                            (
                                item["id"].as_str().unwrap().to_owned(),
                                item["version"].clone(),
                            )
                        })
                        .collect::<Map<_, _>>();
                    TxGraph::propose(tx, commit, &request, &json!(bases))?
                }
            }
        }
        "supersede" => {
            let target = resolve(
                tx.sql(),
                action["target"].as_str(),
                resolutions,
                destinations,
                false,
            )?;
            if action["target"].as_str().is_some() && target.is_none() {
                return Ok((json!({"status":"dependency_pending_review"}), "defer"));
            }
            let target = target.ok_or(Error::Validation("consolidation_supersede_invalid"))?;
            if action["type"].as_str().is_none_or(str::is_empty)
                || action["text"].as_str().is_none_or(str::is_empty)
                || action["change_kind"].is_null()
            {
                return Err(Error::Validation("consolidation_supersede_invalid"));
            }
            if action["change_kind"] == "change" && action["valid_at"].is_null() {
                return Err(Error::Validation("consolidation_change_time_required"));
            }
            TxGraph::supersede(
                tx,
                commit,
                &json!({"op":"supersede","old":target,"new":{"type":action["type"],"text":action["text"],"scope":labels},"kind":action["change_kind"],"valid_at":action["valid_at"],"evidence":evidence}),
                false,
            )?
        }
        "duplicate" => {
            if action["target"].is_null() {
                return Err(Error::Validation("consolidation_duplicate_target_required"));
            }
            let Some(target) = resolve(
                tx.sql(),
                action["target"].as_str(),
                resolutions,
                destinations,
                false,
            )?
            else {
                return Ok((json!({"status":"dependency_pending_review"}), "defer"));
            };
            curation::attach(tx, commit, &target, evidence, false, None)?
        }
        "link" | "merge" => {
            if action["src"].is_null() || action["dst"].is_null() {
                return Err(Error::Validation(if op == "link" {
                    "consolidation_relation_endpoints_required"
                } else {
                    "consolidation_merge_endpoints_required"
                }));
            }
            let a = resolve(
                tx.sql(),
                action["src"].as_str(),
                resolutions,
                destinations,
                true,
            )?;
            let b = resolve(
                tx.sql(),
                action["dst"].as_str(),
                resolutions,
                destinations,
                true,
            )?;
            let (Some(a), Some(b)) = (a, b) else {
                return Ok((json!({"status":"dependency_pending_review"}), "defer"));
            };
            if op == "merge" {
                curation::propose(
                    tx,
                    commit,
                    &json!({"op":"merge","a":a,"b":b,"evidence":evidence}),
                )?
            } else {
                let rel = action["relation"]
                    .as_str()
                    .ok_or(Error::Validation("consolidation_relation_invalid"))?;
                if !RELATIONS.contains(&rel) {
                    return Err(Error::Validation("consolidation_relation_invalid"));
                }
                let existing = one(
                    tx.sql(),
                    "SELECT e.id FROM edge_versions e WHERE e.src=? AND e.rel=? AND e.dst=? AND e.state='active' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id) LIMIT 1",
                    params![a, rel, b],
                )?;
                if let Some(existing) = existing {
                    curation::attach(tx, commit, get_str(&existing, "id")?, evidence, false, None)?
                } else {
                    TxGraph::link(
                        tx,
                        commit,
                        &json!({"op":"link","a":a,"rel":rel,"b":b,"evidence":evidence}),
                        false,
                    )?
                }
            }
        }
        "register_type" => {
            if action["type"].is_null()
                || action["category"].is_null()
                || action["sensitive"].is_null()
            {
                return Err(Error::Validation("consolidation_type_definition_invalid"));
            }
            curation::propose(
                tx,
                commit,
                &json!({"op":"register_type","name":action["type"],"category":action["category"],"sensitive":action["sensitive"],"evidence":evidence}),
            )?
        }
        _ => return Err(Error::Validation("consolidation_schema_invalid")),
    };
    let state = if result["status"] == "pending_review" {
        "review"
    } else {
        "promote"
    };
    Ok((result, state))
}

fn check_continuity(db: &Connection, context: &Value) -> Result<()> {
    let frozen = &context["_continuity"];
    for (target, version) in frozen["nodes"].as_object().into_iter().flatten() {
        if node(db, target)?["version"] != *version {
            return Err(Error::Conflict(
                "Consolidation continuity node changed; derive a fresh plan",
            ));
        }
    }
    for (target, version) in frozen["edges"].as_object().into_iter().flatten() {
        if one(
            db,
            "SELECT version FROM edge_versions WHERE id=? ORDER BY version DESC LIMIT 1",
            [target],
        )?
        .is_none_or(|row| row["version"] != *version)
        {
            return Err(Error::Conflict(
                "Consolidation continuity relation changed; derive a fresh plan",
            ));
        }
    }
    for (target, previous) in frozen["loops"].as_object().into_iter().flatten() {
        let version = frozen["nodes"]
            .get(target)
            .or_else(|| frozen["edges"].get(target))
            .and_then(Value::as_i64)
            .ok_or(Error::Conflict(
                "Consolidation continuity node changed; derive a fresh plan",
            ))?;
        if loops::snapshot(db, target, version)? != *previous {
            return Err(Error::Conflict(
                "Consolidation continuity loop changed; derive a fresh plan",
            ));
        }
    }
    for collision in frozen["collisions"].as_array().into_iter().flatten() {
        let current=rows(db,"SELECT e.id,e.version FROM edge_versions e WHERE e.src=? AND e.rel=? AND e.dst=? AND e.state='active' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id)",params![collision["src"].as_str(),collision["relation"].as_str(),collision["dst"].as_str()])?.into_iter().map(|row|(row["id"].as_str().unwrap().to_owned(),row["version"].clone())).collect::<Map<_,_>>();
        if json!(current) != collision["versions"] {
            return Err(Error::Conflict(
                "Consolidation continuity collision changed; derive a fresh plan",
            ));
        }
    }
    Ok(())
}
fn check_retirement(db: &Connection, context: &Value, target: &str) -> Result<()> {
    let frozen = context["_retirement"]["ready"]
        .get(target)
        .ok_or(Error::Validation("consolidation_retire_context_incomplete"))?;
    let current=rows(db,"SELECT e.id,e.version FROM edge_versions e WHERE (e.src=? OR e.dst=?) AND e.rel<>'supersedes' AND e.state='active' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id)",params![target,target])?.into_iter().map(|row|(row["id"].as_str().unwrap().to_owned(),row["version"].clone())).collect::<Map<_,_>>();
    if json!(current) != frozen["versions"] {
        return Err(Error::Conflict(
            "Consolidation retirement relation changed; derive a fresh plan",
        ));
    }
    for (edge, previous) in frozen["loops"].as_object().unwrap() {
        if loops::snapshot(db, edge, current[edge].as_i64().unwrap())? != *previous {
            return Err(Error::Conflict(
                "Consolidation retirement loop changed; derive a fresh plan",
            ));
        }
    }
    Ok(())
}
fn apply_continuity(tx: &WriteTxn, commit: &str, context: &mut Value) -> Result<Vec<Value>> {
    let frozen = context["_continuity"].clone();
    let collision_keys = frozen["collisions"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|item| {
            (
                item["src"].as_str().unwrap().to_owned(),
                item["relation"].as_str().unwrap().to_owned(),
                item["dst"].as_str().unwrap().to_owned(),
            )
        })
        .collect::<BTreeSet<_>>();
    let mut changed = vec![];
    for movement in frozen["moves"].as_array().into_iter().flatten() {
        let old = get_str(movement, "old")?;
        if let Some(chain) = continuation(tx.sql(), old)? {
            let destination = chain.last().unwrap();
            let mut selected = vec![];
            for edge in rows(
                tx.sql(),
                "SELECT e.* FROM edge_versions e WHERE e.id IN (SELECT value FROM json_each(?)) AND (e.src=? OR e.dst=?) AND e.state='active' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id)",
                params![encode_json(&movement["edge_ids"])?, old, old],
            )? {
                let src = if edge["src"] == old {
                    destination.as_str()
                } else {
                    get_str(&edge, "src")?
                };
                let dst = if edge["dst"] == old {
                    destination.as_str()
                } else {
                    get_str(&edge, "dst")?
                };
                if src != dst
                    && !collision_keys.contains(&(
                        src.into(),
                        get_str(&edge, "rel")?.into(),
                        dst.into(),
                    ))
                {
                    let pending = &mut context["_continuity"]["pending"];
                    if !pending.as_array().unwrap().contains(&json!(destination)) {
                        pending.as_array_mut().unwrap().push(json!(destination));
                    }
                } else {
                    selected.push(get_str(&edge, "id")?.into());
                }
            }
            changed.extend(curation::redirect_edges(
                tx,
                commit,
                old,
                destination,
                Some(&selected),
            )?);
        }
    }
    Ok(changed)
}
fn continue_work(
    tx: &WriteTxn,
    commit: &str,
    job: &Job,
    context: &Value,
    continue_with: &[String],
    nodes: &Map<String, Value>,
    _destinations: &Map<String, Value>,
    progressed: bool,
) -> Result<()> {
    let group = if let Some(id) = job.payload["repair_group_id"].as_str() {
        one(tx.sql(), "SELECT * FROM repair_groups WHERE id=?", [id])?
    } else {
        None
    };
    if group
        .as_ref()
        .is_some_and(|group| group["active_job"] != job.id)
    {
        return Err(Error::Conflict(
            "Maintenance group changed; derive a fresh plan",
        ));
    }
    let mut reviewed = group
        .as_ref()
        .map(|group| decode_json(get_str(group, "reviewed_ids")?).and_then(|ids| strings(&ids)))
        .transpose()?
        .unwrap_or_default()
        .into_iter()
        .collect::<BTreeSet<_>>();
    reviewed.extend(nodes.keys().cloned());
    let mut frontier = group
        .as_ref()
        .map(|group| decode_json(get_str(group, "frontier")?).and_then(|ids| strings(&ids)))
        .transpose()?
        .unwrap_or_default()
        .into_iter()
        .filter(|id| !reviewed.contains(id))
        .collect::<Vec<_>>();
    frontier.extend(
        context["_continuity"]
            .get("pending")
            .map(strings)
            .transpose()?
            .unwrap_or_default(),
    );
    let mut anchors = vec![];
    for target in continue_with {
        let mut remaining = context["_unchecked"]
            .get(target)
            .map(strings)
            .transpose()?
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !reviewed.contains(id))
            .collect::<Vec<_>>();
        if context["_retirement"]["pending"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|id| id == target)
        {
            remaining.push(target.clone());
        }
        if progressed {
            let seed = &nodes[target];
            let head = word_terms(get_str(seed, "text")?, 1, 1)
                .into_iter()
                .next()
                .unwrap_or_else(|| seed["text"].as_str().unwrap().into());
            let canonical_seed = curation::canonical(tx.sql(), target)?;
            for (other, n) in nodes {
                let other_head = word_terms(get_str(n, "text")?, 1, 1)
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| n["text"].as_str().unwrap().into());
                let current = curation::canonical(tx.sql(), other)?;
                if n["type"] == seed["type"]
                    && other_head == head
                    && current["state"] == "active"
                    && current["id"] != canonical_seed["id"]
                {
                    let id = get_str(&current, "id")?.to_owned();
                    remaining.push(id.clone());
                    reviewed.remove(&id);
                }
            }
        }
        if !remaining.is_empty() {
            anchors.push(target.clone());
            frontier.extend(remaining);
        }
    }
    anchors.extend(frontier);
    let mut frontier = unique(anchors);
    for id in &frontier {
        reviewed.remove(id);
    }
    if let Some(group) = &group {
        let mut survivors = Map::new();
        for seed in strings(&decode_json(get_str(group, "seed_ids")?)?)? {
            let n = curation::canonical(tx.sql(), &seed)?;
            if n["state"] == "active" {
                survivors.insert(get_str(&n, "id")?.into(), n);
            }
        }
        let mut merged = BTreeSet::new();
        for row in rows(
            tx.sql(),
            "SELECT DISTINCT superseded_by FROM node_versions WHERE commit_id=? AND state='merged'",
            [commit],
        )? {
            let n = curation::canonical(tx.sql(), get_str(&row, "superseded_by")?)?;
            merged.insert(get_str(&n, "id")?.to_owned());
        }
        let supplied = nodes
            .keys()
            .map(|id| {
                curation::canonical(tx.sql(), id).and_then(|n| Ok(get_str(&n, "id")?.to_owned()))
            })
            .collect::<Result<BTreeSet<_>>>()?;
        let mut comparison = vec![];
        for target in merged.iter().filter(|id| survivors.contains_key(*id)) {
            let missing = survivors
                .iter()
                .filter(|(id, n)| {
                    *id != target
                        && !supplied.contains(*id)
                        && n["type"] == survivors[target]["type"]
                })
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                comparison.push(target.clone());
                comparison.extend(missing);
                comparison = unique(comparison);
            }
        }
        if !comparison.is_empty() {
            for id in &comparison {
                reviewed.remove(id);
            }
            reviewed.extend(merged.iter().filter(|id| comparison.contains(id)).cloned());
            comparison.extend(
                frontier
                    .into_iter()
                    .filter(|id| !comparison.contains(id))
                    .collect::<Vec<_>>(),
            );
            frontier = comparison;
        }
    }
    let mut resolved = vec![];
    for target in frontier {
        let n = curation::canonical(tx.sql(), &target)?;
        if n["state"] == "active" {
            let id = get_str(&n, "id")?.to_owned();
            if !resolved.contains(&id) {
                resolved.push(id);
            }
        }
    }
    let stamp = now();
    if let Some(group) = group {
        let generation = get_i64(&group, "generation")? + 1;
        let pending = !context["_continuity"]["pending"]
            .as_array()
            .unwrap()
            .is_empty()
            || !context["_retirement"]["pending"]
                .as_array()
                .unwrap()
                .is_empty();
        let available = if !resolved.is_empty() && !progressed && pending {
            timestamp() + 900.0
        } else {
            0.0
        };
        tx.sql().execute("UPDATE repair_groups SET frontier=?,reviewed_ids=?,state=?,generation=?,active_job=NULL,available_at=?,updated_at=? WHERE id=?",params![encode_json(&json!(resolved))?,encode_json(&json!(reviewed.into_iter().collect::<Vec<_>>()))?,if resolved.is_empty(){"done"}else{"pending"},generation,available,stamp,group["id"].as_str()])?;
    } else {
        if resolved.is_empty() {
            return Ok(());
        }
        let mut seeds = continue_with.to_vec();
        seeds.extend(
            context["_continuity"]
                .get("pending")
                .map(strings)
                .transpose()?
                .unwrap_or_default(),
        );
        seeds.sort();
        seeds.dedup();
        let serialized = encode_json(&json!(seeds))?;
        if one(
            tx.sql(),
            "SELECT id FROM repair_groups WHERE state='pending' AND seed_ids=? LIMIT 1",
            [&serialized],
        )?
        .is_some()
        {
            return Ok(());
        }
        let id = format!("r_{}", &crate::util::id()[..24]);
        tx.sql().execute("INSERT INTO repair_groups(id,seed_ids,frontier,reviewed_ids,state,created_at,updated_at) VALUES (?,?,?,?,'pending',?,?)",params![id,serialized,encode_json(&json!(resolved))?,encode_json(&json!(reviewed.into_iter().collect::<Vec<_>>()))?,stamp,stamp])?;
    }
    Ok(())
}
