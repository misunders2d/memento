//! Reactive loop state and original-source chronology, independent of assertions.
use crate::util::*;
use crate::{Graph, behavior, context, dto};
use chrono::{Datelike, Duration, Utc};
use memento_core::{Error, Result, WriteTxn};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::collections::HashMap;

pub fn valid_responsibility(value: &str) -> bool {
    matches!(value, "owner" | "other" | "unknown")
}

/// Only indexed loop events and their immutable commits are inspected. Incoming
/// identity/promotion branches preserve role choices through refinement/transfer.
pub fn responsibilities(db: &Connection, targets: &[String]) -> Result<HashMap<String, Value>> {
    let selected = rows(db, "WITH RECURSIVE ancestry(root,id) AS (
        SELECT value,value FROM json_each(?1) UNION
        SELECT a.root,n.id FROM ancestry a JOIN node_versions n ON n.superseded_by=a.id
          WHERE EXISTS(SELECT 1 FROM loop_events l WHERE l.target=n.id) UNION
        SELECT a.root,d.candidate_id FROM ancestry a JOIN candidate_decisions d ON d.target_id=a.id
          AND d.decision='promote' WHERE EXISTS(SELECT 1 FROM loop_events l WHERE l.target=d.candidate_id))
        SELECT a.root,l.target,c.recorded_at,r.value AS attribution FROM ancestry a
        CROSS JOIN loop_events l INDEXED BY loop_target ON l.target=a.id JOIN graph_commits c ON c.id=l.commit_id
        JOIN json_each(c.result,'$.loop_responsibilities') r ON r.key=l.target
        WHERE NOT EXISTS(SELECT 1 FROM json_each(r.value,'$.evidence') ref
          JOIN episodes e ON e.id=json_extract(ref.value,'$.episode_id')
          JOIN forgotten_sources f ON f.source=e.source AND f.event_id=e.event_id)
        ORDER BY json_extract(r.value,'$.manual') DESC,c.recorded_at DESC,l.rowid DESC",
        [encode_json(&json!(targets))?])?;
    let mut result = HashMap::new();
    for row in selected {
        let value = decode_json(get_str(&row, "attribution")?)?;
        if value["responsibility"]
            .as_str()
            .is_some_and(valid_responsibility)
        {
            result
                .entry(get_str(&row, "root")?.to_owned())
                .or_insert(value);
        }
    }
    Ok(result)
}

pub fn responsibility(db: &Connection, target: &str) -> Result<String> {
    Ok(responsibilities(db, &[target.into()])?
        .get(target)
        .and_then(|value| value["responsibility"].as_str())
        .unwrap_or("unknown")
        .into())
}

/// Validate source quotes without rewriting assertion evidence. Native user
/// provenance is necessary for owner attribution, never semantic ownership proof.
pub fn grounded_sources(
    tx: &WriteTxn,
    target: &str,
    sources: &Value,
    original: bool,
    allow_instruction: bool,
) -> Result<Value> {
    let evidence = sources
        .as_array()
        .filter(|sources| !sources.is_empty())
        .ok_or(Error::Validation(
            "Loop responsibility requires exact source evidence",
        ))?;
    let mut grounded_owner = false;
    let mut references = vec![];
    let mut source_times = vec![];
    for source in evidence {
        let quote = get_str(source, "quote")?;
        if quote.is_empty() {
            return Err(Error::Validation(
                "Evidence quote must be a nonempty string",
            ));
        }
        let episode_id = get_str(source, "episode_id")?;
        let mut episode = one(tx.sql(), "SELECT * FROM episodes WHERE id=?", [episode_id])?
            .ok_or(Error::Validation("Unknown source episode"))?;
        if original
            && one(
                tx.sql(),
                "SELECT 1 FROM graph_evidence WHERE target=? AND episode_id=? LIMIT 1",
                params![target, episode_id],
            )?
            .is_none()
        {
            return Err(Error::Validation(
                "Loop responsibility requires an original target source",
            ));
        }
        if one(
            tx.sql(),
            "SELECT 1 FROM forgotten_sources WHERE source=? AND event_id=?",
            params![episode["source"].as_str(), episode["event_id"].as_str()],
        )?
        .is_some()
        {
            return Err(Error::Conflict(
                "This evidence was excluded from active memory by a human decision",
            ));
        }
        let metadata = decode_json(get_str(&episode, "metadata")?)?;
        let text = tx
            .original_text(episode_id)?
            .ok_or(Error::Validation("Unknown source episode"))?;
        let instruction = episode["kind"] == "developer"
            || memento_core::raw::legacy_native_context(
                get_str(&episode, "source")?,
                get_str(&episode, "kind")?,
                Some(&text),
                Some(&metadata),
            );
        if !episode["text_runs"].is_null()
            || matches!(
                episode["kind"].as_str(),
                Some("native_record" | "native_hook" | "native_live_record")
            )
            || (instruction && !allow_instruction)
            || ((metadata["evidence_eligible"] == false || metadata["extraction_mode"] == "skip")
                && !(instruction && allow_instruction))
        {
            return Err(Error::Validation(
                "Loop responsibility requires eligible original evidence",
            ));
        }
        let (start, end) = match (source["start"].as_u64(), source["end"].as_u64()) {
            (Some(start), Some(end)) if substring(&text, start as usize, end as usize) == quote => {
                (start as usize, end as usize)
            }
            _ => crate::extraction::source_span(
                &text,
                quote,
                source["occurrence"].as_u64().map(|n| n as usize),
            )?,
        };
        if original && one(tx.sql(),"SELECT 1 FROM graph_evidence WHERE target=? AND episode_id=? AND start<=? AND end>=? LIMIT 1",params![target,episode_id,start as i64,end as i64])?.is_none(){return Err(Error::Validation("Loop responsibility requires an original target source"));}
        episode["text"] = json!(text);
        grounded_owner |= episode["source"]
            .as_str()
            .is_some_and(|source| source.starts_with("native:"))
            && crate::behavior_ground_truth(&episode, Some(start), Some(end));
        references.push(json!({"episode_id":episode_id,"start":start,"end":end}));
        source_times.push(episode["occurred_at"].clone());
    }
    Ok(json!({"evidence":references,"source_times":source_times,"owner_grounded":grounded_owner}))
}

/// Role-only operation preserves lifecycle/dates and all explicit role choices.
pub fn assign(
    tx: &WriteTxn,
    commit: &str,
    target_id: &str,
    value: &str,
    sources: &Value,
    manual: bool,
) -> Result<Value> {
    if !valid_responsibility(value) {
        return Err(Error::Validation("Invalid loop responsibility"));
    }
    let active = target(tx.sql(), target_id)?.ok_or(Error::Validation(
        "Responsibility requires an active visible target",
    ))?;
    if one(
        tx.sql(),
        "SELECT 1 FROM forgotten_targets WHERE target=?",
        [target_id],
    )?
    .is_some()
    {
        return Err(Error::Validation(
            "Responsibility requires an active visible target",
        ));
    }
    let previous = current(tx.sql(), target_id)?.ok_or(Error::Validation(
        "Responsibility requires an existing loop",
    ))?;
    let grounded = grounded_sources(tx, target_id, sources, !manual, false)?;
    if value == "owner" && grounded["owner_grounded"] != true {
        return Err(Error::Validation(
            "Owner responsibility requires original native user evidence",
        ));
    }
    let attribution = responsibilities(tx.sql(), &[target_id.into()])?;
    if !manual
        && (attribution
            .get(target_id)
            .is_some_and(|entry| entry["manual"] == true)
            || protected(tx.sql(), &active)?)
    {
        return Ok(json!({"id":target_id,"status":"protected_loop_responsibility"}));
    }
    let mut result = append(
        tx,
        commit,
        target_id,
        get_str(&previous, "state")?,
        previous["due_at"].as_str(),
        previous["deferred_until"].as_str(),
    )?;
    result["responsibility"] = json!(value);
    result["loop_responsibilities"] =
        json!({target_id:{"responsibility":value,"manual":manual,"evidence":grounded["evidence"]}});
    Ok(result)
}

pub fn current(db: &Connection, target: &str) -> Result<Option<Value>> {
    let mut value = one(
        db,
        "SELECT target AS id,state,due_at,deferred_until FROM loop_events WHERE target=? ORDER BY rowid DESC LIMIT 1",
        [target],
    )?;
    if let Some(value) = value.as_mut() {
        value["responsibility"] = json!(responsibility(db, target)?);
    }
    Ok(value)
}
pub fn events(db: &Connection, target: &str) -> Result<Vec<Value>> {
    let mut values = rows(
        db,
        "SELECT l.state,l.due_at,l.deferred_until,l.commit_id,c.actor,c.why,c.recorded_at,r.value AS assignment,json_extract(c.result,'$.evidence') AS evidence FROM loop_events l JOIN graph_commits c ON c.id=l.commit_id LEFT JOIN json_each(c.result,'$.loop_responsibilities') r ON r.key=l.target WHERE l.target=? ORDER BY l.rowid",
        [target],
    )?;
    for value in &mut values {
        for field in ["assignment", "evidence"] {
            if let Some(raw) = value[field].as_str() {
                value[field] = decode_json(raw)?;
            }
        }
    }
    Ok(values)
}

pub fn append(
    tx: &WriteTxn,
    commit: &str,
    target: &str,
    state: &str,
    due: Option<&str>,
    deferred: Option<&str>,
) -> Result<Value> {
    tx.sql().execute(
        "INSERT INTO loop_events VALUES (?,?,?,?,?,?)",
        params![id(), target, commit, state, due, deferred],
    )?;
    tx.touch_target(target)?;
    Ok(json!({"id":target,"state":state,"due_at":due,"deferred_until":deferred}))
}
pub fn transfer(tx: &WriteTxn, commit: &str, source: &str, target: &str) -> Result<()> {
    if source == target {
        return Ok(());
    }
    let Some(previous) = current(tx.sql(), source)? else {
        return Ok(());
    };
    if current(tx.sql(), target)?.is_none() {
        append(
            tx,
            commit,
            target,
            get_str(&previous, "state")?,
            previous["due_at"].as_str(),
            previous["deferred_until"].as_str(),
        )?;
    }
    if previous["state"] == "open" {
        append(
            tx,
            commit,
            source,
            "closed",
            previous["due_at"].as_str(),
            None,
        )?;
    }
    Ok(())
}
pub fn lineage(db: &Connection, target: &str) -> Result<Vec<String>> {
    let mut pending = vec![target.to_owned()];
    let mut seen = std::collections::HashSet::new();
    let mut result = vec![];
    while let Some(target) = pending.pop() {
        if !seen.insert(target.clone()) {
            continue;
        }
        let incoming = rows(
            db,
            "SELECT DISTINCT n.id FROM node_versions n WHERE n.superseded_by=? AND EXISTS (SELECT 1 FROM loop_events l WHERE l.target=n.id) UNION SELECT d.candidate_id AS id FROM candidate_decisions d WHERE d.target_id=? AND d.decision='promote' AND EXISTS (SELECT 1 FROM loop_events l WHERE l.target=d.candidate_id)",
            params![target, target],
        )?;
        for row in incoming {
            pending.push(get_str(&row, "id")?.into());
        }
        result.push(target);
    }
    Ok(result)
}
pub fn snapshot(db: &Connection, target: &str, version: i64) -> Result<Value> {
    let marker = one(
        db,
        "SELECT id,state,due_at,deferred_until FROM loop_events WHERE target=? ORDER BY rowid DESC LIMIT 1",
        [target],
    )?;
    let mut manual_dates = vec![];
    for branch in lineage(db, target)? {
        if let Some(row) = one(
            db,
            "SELECT c.recorded_at FROM loop_events l JOIN graph_commits c ON c.id=l.commit_id WHERE l.target=? AND json_extract(c.request,'$.command.op')='loop' AND coalesce(json_extract(c.request,'$.command.action'),'') NOT IN ('assign','decline') AND NOT (json_extract(c.request,'$.command.action')='close' AND coalesce(json_array_length(json_extract(c.request,'$.command.evidence')),0)>0) ORDER BY l.rowid DESC LIMIT 1",
            [&branch],
        )? {
            manual_dates.push(get_str(&row, "recorded_at")?.to_owned());
        }
    }
    let source_dates = rows(
        db,
        "SELECT DISTINCT e.occurred_at FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id WHERE g.target=? AND g.version=?",
        params![target, version],
    )?;
    let pending_date =
        if source_dates.is_empty() || source_dates.iter().any(|row| row["occurred_at"].is_null()) {
            None
        } else {
            latest(
                source_dates
                    .iter()
                    .map(|row| row["occurred_at"].as_str().unwrap()),
            )?
        };
    Ok(
        json!({"event_id":marker.as_ref().map(|row|row["id"].clone()),"state":marker.as_ref().map(|row|row["state"].clone()),
        "pending_source_at":pending_date,"manual_choice_at":latest(manual_dates.iter().map(String::as_str))?}),
    )
}
fn latest<'a>(values: impl Iterator<Item = &'a str>) -> Result<Option<String>> {
    let mut best = None;
    for value in values {
        let parsed = parse_time(value)?;
        if best.as_ref().is_none_or(|(at, _)| parsed > *at) {
            best = Some((parsed, value.to_owned()));
        }
    }
    Ok(best.map(|(_, value)| value))
}
pub fn closure_time(
    snapshot: &Value,
    sources: &[Option<String>],
) -> Result<(Option<String>, Option<&'static str>)> {
    if snapshot["state"] != "open" {
        return Ok((None, Some("not_open")));
    }
    let Some(pending) = snapshot["pending_source_at"].as_str() else {
        return Ok((None, Some("chronology_unknown")));
    };
    if sources.is_empty() || sources.iter().any(Option::is_none) {
        return Ok((None, Some("chronology_unknown")));
    }
    let mut boundary = parse_time(pending)?;
    if let Some(manual) = snapshot["manual_choice_at"].as_str() {
        boundary = boundary.max(parse_time(manual)?);
    }
    let dates = sources
        .iter()
        .map(|value| parse_time(value.as_deref().unwrap()))
        .collect::<Result<Vec<_>>>()?;
    if dates.iter().any(|value| *value <= boundary) {
        return Ok((None, Some("not_newer")));
    }
    Ok((dates.into_iter().max().map(format_time), None))
}
fn closed_at(db: &Connection, target: &str) -> Result<Option<String>> {
    let mut boundaries = vec![];
    for branch in lineage(db, target)? {
        for row in rows(
            db,
            "SELECT l.commit_id,l.state,c.request,c.result,c.recorded_at FROM loop_events l LEFT JOIN graph_commits c ON c.id=l.commit_id WHERE l.target=? ORDER BY l.rowid DESC",
            [&branch],
        )? {
            if row["state"] != "closed" {
                break;
            }
            let request = row["request"]
                .as_str()
                .map(decode_json)
                .transpose()?
                .unwrap_or(json!({}));
            if request["command"]["op"] == "loop" && request["command"]["action"] == "close" {
                let grounded = request["command"]["evidence"]
                    .as_array()
                    .is_some_and(|items| !items.is_empty());
                let result = row["result"]
                    .as_str()
                    .map(decode_json)
                    .transpose()?
                    .unwrap_or(json!({}));
                if let Some(at) = if grounded {
                    result["source_occurred_at"].as_str()
                } else {
                    row["recorded_at"].as_str()
                } {
                    boundaries.push(at.to_owned());
                }
                break;
            }
            if request["command"]["op"] == "consolidate" {
                let result = row["result"]
                    .as_str()
                    .map(decode_json)
                    .transpose()?
                    .unwrap_or(json!({}));
                if let Some(closure) = result["loop_closures"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .find(|item| item["target"] == branch && item["status"] == "closed")
                {
                    if let Some(at) = closure["source_occurred_at"].as_str() {
                        boundaries.push(at.to_owned());
                    }
                    break;
                }
            }
        }
    }
    latest(boundaries.iter().map(String::as_str))
}
pub fn mark(
    tx: &WriteTxn,
    commit: &str,
    target: &str,
    open_loop: Option<bool>,
    evidence_target: Option<&str>,
    evidence_loop: Option<&Value>,
) -> Result<()> {
    if open_loop != Some(true) {
        return Ok(());
    }
    let Some(previous) = current(tx.sql(), target)? else {
        append(tx, commit, target, "open", None, None)?;
        return Ok(());
    };
    if previous["state"] != "closed"
        || evidence_target.is_none()
        || evidence_loop.is_some_and(|value| value["state"] == "closed")
    {
        return Ok(());
    }
    let Some(closed) = closed_at(tx.sql(), target)? else {
        return Ok(());
    };
    let boundary = parse_time(&closed)?;
    let sources = rows(
        tx.sql(),
        "SELECT e.occurred_at FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id WHERE g.target=? AND e.occurred_at IS NOT NULL",
        [evidence_target.unwrap()],
    )?;
    for row in sources {
        if parse_time(get_str(&row, "occurred_at")?)? > boundary {
            let due = evidence_loop
                .and_then(|value| value["due_at"].as_str())
                .or_else(|| previous["due_at"].as_str());
            append(
                tx,
                commit,
                target,
                "open",
                due,
                evidence_loop.and_then(|value| value["deferred_until"].as_str()),
            )?;
            break;
        }
    }
    Ok(())
}
pub fn decline_extracted(tx: &WriteTxn, commit: &str, target: &str) -> Result<bool> {
    let Some(marker) = current(tx.sql(), target)? else {
        return Ok(false);
    };
    if marker["state"] != "open"
        || !marker["due_at"].is_null()
        || !marker["deferred_until"].is_null()
    {
        return Ok(false);
    }
    let records = rows(
        tx.sql(),
        "SELECT c.request FROM loop_events l LEFT JOIN graph_commits c ON c.id=l.commit_id WHERE l.target=?",
        [target],
    )?;
    if records.is_empty() {
        return Ok(false);
    }
    for record in records {
        if record["request"]
            .as_str()
            .map(decode_json)
            .transpose()?
            .is_none_or(|value| value["command"]["op"] != "extract")
        {
            return Ok(false);
        }
    }
    append(tx, commit, target, "closed", None, None)?;
    Ok(true)
}
pub fn decline_automatic(
    tx: &WriteTxn,
    commit: &str,
    target: &str,
    expected: &Value,
) -> Result<Value> {
    let n = self::target(tx.sql(), target)?.ok_or(Error::Validation(
        "Loop markers require an active memory or unconfirmed candidate",
    ))?;
    let live = snapshot(tx.sql(), target, get_i64(&n, "version")?)?;
    if live != *expected {
        return Err(Error::Conflict(
            "Consolidation loop state changed; derive a fresh plan",
        ));
    }
    let marker = current(tx.sql(), target)?;
    if (n["state"] != "active" && n["tier"] != "candidate")
        || protected(tx.sql(), &n)?
        || !live["manual_choice_at"].is_null()
        || marker.as_ref().is_none_or(|value| {
            value["state"] != "open"
                || !value["due_at"].is_null()
                || !value["deferred_until"].is_null()
        })
    {
        return Ok(json!({"id":target,"status":"protected_loop_choice"}));
    }
    append(tx, commit, target, "closed", None, None)?;
    Ok(json!({"id":target,"status":"declined_not_obligation"}))
}

pub fn protected(db: &Connection, target: &Value) -> Result<bool> {
    if target["pinned"].as_i64().unwrap_or(0) != 0 {
        return Ok(true);
    }
    for endpoint in ["src", "dst"] {
        if let Some(id) = target[endpoint].as_str() {
            if self::endpoint(db, id)?.is_none_or(|node| node["pinned"].as_i64().unwrap_or(0) != 0)
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub fn endpoint(db: &Connection, target: &str) -> Result<Option<Value>> {
    let mut current = target.to_owned();
    let mut seen = std::collections::HashSet::new();
    for _ in 0..64 {
        if !seen.insert(current.clone())
            || one(
                db,
                "SELECT 1 FROM forgotten_targets WHERE target=?",
                [&current],
            )?
            .is_some()
        {
            return Ok(None);
        }
        if let Some(row) = one(
            db,
            "SELECT * FROM node_versions WHERE id=? ORDER BY version DESC LIMIT 1",
            [&current],
        )? {
            if row["state"] == "active" {
                return Ok(Some(row));
            }
            if matches!(row["state"].as_str(), Some("merged" | "refined")) {
                if let Some(next) = row["superseded_by"].as_str() {
                    current = next.into();
                    continue;
                }
            }
            return Ok(None);
        }
        if let Some(row) = one(
            db,
            &format!("SELECT c.id FROM candidates c WHERE c.id=? AND {ACTIVE_CANDIDATE}"),
            [&current],
        )? {
            return Ok(Some(row));
        }
        if let Some(row) = one(
            db,
            "SELECT decision,target_id FROM candidate_decisions WHERE candidate_id=? ORDER BY rowid DESC LIMIT 1",
            [&current],
        )? {
            if row["decision"] == "promote" {
                if let Some(next) = row["target_id"].as_str() {
                    current = next.into();
                    continue;
                }
            }
        }
        return Ok(None);
    }
    Ok(None)
}

/// Filter lightweight loop rows; shared endpoints resolve once per response.
/// No original source text, assertion payload or evidence hydration is needed.
pub fn visible_loop_targets(db: &Connection, values: Vec<Value>) -> Result<Vec<Value>> {
    let mut checked = HashMap::<String, bool>::new();
    let mut result = vec![];
    for row in values {
        let relation = row["candidate_kind"] == "relation"
            || !row["loop_src"].is_null()
            || !row["loop_dst"].is_null();
        let mut visible = true;
        if relation {
            for key in ["loop_src", "loop_dst"] {
                let Some(id) = row[key].as_str() else {
                    visible = false;
                    break;
                };
                let allowed = if let Some(value) = checked.get(id) {
                    *value
                } else {
                    let value = endpoint(db, id)?.is_some();
                    checked.insert(id.into(), value);
                    value
                };
                if !allowed {
                    visible = false;
                    break;
                }
            }
        }
        if visible {
            result.push(row);
        }
    }
    Ok(result)
}

pub fn target(db: &Connection, id: &str) -> Result<Option<Value>> {
    let Some(mut n) = one(
        db,
        "SELECT n.id,n.version,n.type,substr(n.text,1,240) AS text,n.state,n.valid_from,n.valid_to,n.pinned,n.superseded_by,n.commit_id,n.scope,(SELECT c.recorded_at FROM node_versions v JOIN graph_commits c ON c.id=v.commit_id WHERE v.id=n.id ORDER BY v.version LIMIT 1) AS created_at,coalesce((SELECT importance FROM node_stats WHERE node_id=n.id),0.5) AS importance,coalesce((SELECT decay FROM node_stats WHERE node_id=n.id),1) AS decay,coalesce((SELECT archived FROM node_stats WHERE node_id=n.id),0) AS archived,coalesce((SELECT access_count FROM node_stats WHERE node_id=n.id),0) AS access_count,(SELECT last_cited_at FROM node_stats WHERE node_id=n.id) AS last_cited_at FROM node_versions n WHERE n.id=? AND n.state='active' AND n.version=(SELECT max(v.version) FROM node_versions v WHERE v.id=n.id)",
        [id],
    )?
    else {
        let candidate = one(
            db,
            &format!(
                "SELECT c.id,c.type,c.kind,c.src,c.dst,substr(c.text,1,240) AS text,c.scope,c.valid_from,c.valid_to FROM candidates c WHERE c.id=? AND {ACTIVE_CANDIDATE}"
            ),
            [id],
        )?;
        let mut value = if let Some(mut value) = candidate {
            if value["kind"] == "relation" {
                for key in ["src", "dst"] {
                    let Some(id) = value[key].as_str() else {
                        return Ok(None);
                    };
                    if endpoint(db, id)?.is_none() {
                        return Ok(None);
                    }
                }
            }
            value["scope"] = decode_json(get_str(&value, "scope")?)?;
            value["version"] = json!(1);
            value["tier"] = json!("candidate");
            value["confirmed"] = json!(false);
            value
        } else {
            let Some(mut edge) = one(
                db,
                "SELECT e.* FROM edge_versions e WHERE e.id=? AND e.state='active' AND e.version=(SELECT max(v.version) FROM edge_versions v WHERE v.id=e.id)",
                [id],
            )?
            else {
                return Ok(None);
            };
            let a = node(db, get_str(&edge, "src")?)?;
            let b = node(db, get_str(&edge, "dst")?)?;
            if a["state"] != "active" || b["state"] != "active" {
                return Ok(None);
            }
            for key in ["src", "dst"] {
                if endpoint(db, get_str(&edge, key)?)?.is_none() {
                    return Ok(None);
                }
            }
            let mut labels = decode_json(get_str(&a, "scope")?)?
                .as_array()
                .unwrap()
                .clone();
            for label in decode_json(get_str(&b, "scope")?)?.as_array().unwrap() {
                if !labels.contains(label) {
                    labels.push(label.clone());
                }
            }
            labels.sort_by(|a, b| a.as_str().cmp(&b.as_str()));
            edge["type"] = json!("relation");
            edge["text"] = json!(format!(
                "{} {} {}",
                excerpt(get_str(&a, "text")?, 100),
                get_str(&edge, "rel")?,
                excerpt(get_str(&b, "text")?, 100)
            ));
            edge["scope"] = json!(labels);
            edge["tier"] = json!("durable");
            edge["confirmed"] = json!(true);
            edge
        };
        value["evidence"] = Value::Array(rows(
            db,
            "SELECT episode_id,start,end,substr(quote,1,240) AS quote_excerpt FROM graph_evidence WHERE target=? AND version=? ORDER BY episode_id,start LIMIT 5",
            params![id, value["version"].as_i64()],
        )?);
        return Ok(Some(value));
    };
    n["scope"] = decode_json(get_str(&n, "scope")?)?;
    n["tier"] = json!("durable");
    n["confirmed"] = json!(true);
    n["evidence"] = Value::Array(rows(
        db,
        "SELECT g.episode_id,g.start,g.end,CASE WHEN EXISTS (SELECT 1 FROM episodes e JOIN forgotten_sources f ON f.source=e.source AND f.event_id=e.event_id WHERE e.id=g.episode_id) THEN NULL ELSE substr(g.quote,1,240) END AS quote_excerpt FROM graph_evidence g WHERE g.target=? AND g.version=? ORDER BY g.episode_id,g.start LIMIT 5",
        params![id, n["version"].as_i64()],
    )?);
    let change=one(db,"SELECT c.recorded_at,substr(c.actor,1,128) AS actor,substr(c.why,1,240) AS why,(SELECT count(*) FROM node_versions WHERE id=?) AS change_count FROM node_versions n JOIN graph_commits c ON c.id=n.commit_id WHERE n.id=? AND n.version=?",params![id,id,n["version"].as_i64()])?.ok_or(Error::Validation("Missing assertion commit"))?;
    n["change_count"] = change["change_count"].clone();
    n["last_change"] =
        json!({"recorded_at":change["recorded_at"],"actor":change["actor"],"why":change["why"]});
    n["loop"] = current(db, id)?.unwrap_or(Value::Null);
    if let Some(b) = behavior(db, id)? {
        n["authority"] = b["authority"].clone();
        n["behavior"] = b;
        n["category"] = json!("behavioral_suggestion");
    }
    Ok(Some(n))
}

pub fn command(graph: &Graph, command: dto::Loop) -> Result<Value> {
    if ![
        "list", "open", "close", "later", "remind", "assign", "decline",
    ]
    .contains(&command.action.as_str())
    {
        return Err(Error::Validation("Unknown loop action"));
    }
    if !matches!(
        command.scope.as_deref(),
        None | Some("personal" | "professional")
    ) {
        return Err(Error::Validation("Invalid scope"));
    }
    if !command.days.is_finite() || command.days <= 0.0 {
        return Err(Error::Validation("days must be a positive finite number"));
    }
    if !(1..=100).contains(&command.limit) {
        return Err(Error::Validation("Loop list limit must be 1..100"));
    }
    optional(command.id.as_deref(), 64, "Invalid loop identifier")?;
    optional(
        command.command_id.as_deref(),
        512,
        "Invalid command identifier",
    )?;
    optional(command.actor.as_deref(), 256, "Invalid actor")?;
    if command.why.as_deref().is_some_and(str::is_empty) {
        return Err(Error::Validation("Invalid loop reason"));
    }
    if command.due_at.is_some() && command.action != "remind" {
        return Err(Error::Validation("due_at is only supported by remind"));
    }
    if command.due_before.is_some() && command.action != "list" {
        return Err(Error::Validation("due_before is only supported by list"));
    }
    if command
        .responsibility
        .as_deref()
        .is_some_and(|value| !valid_responsibility(value))
    {
        return Err(Error::Validation("Invalid loop responsibility"));
    }
    if command.responsibility.is_some() && !matches!(command.action.as_str(), "list" | "assign") {
        return Err(Error::Validation(
            "responsibility is only supported by assign or list",
        ));
    }
    if !command.evidence.is_empty()
        && !matches!(command.action.as_str(), "assign" | "close" | "decline")
    {
        return Err(Error::Validation(
            "Loop evidence is only supported by assign, close or decline",
        ));
    }
    if command.evidence.len() > 8 {
        return Err(Error::Validation("Loop evidence limit is eight"));
    }
    let cutoff = time(command.due_before.as_deref())?;
    if command.action == "list" {
        return graph.store.read(|tx|{
            let db=tx.sql();let now=now();
            let sql=format!("WITH eligible AS (SELECT l.target AS id,l.state,l.due_at,l.deferred_until FROM loop_events l WHERE l.rowid=(SELECT max(x.rowid) FROM loop_events x WHERE x.target=l.target) AND l.state='open' AND NOT EXISTS (SELECT 1 FROM forgotten_targets f WHERE f.target=l.target) AND (l.deferred_until IS NULL OR l.deferred_until<=?1) AND (?2 IS NULL OR l.due_at<?2) AND EXISTS (SELECT 1 FROM node_versions n WHERE n.id=l.target AND n.state='active' AND n.version=(SELECT max(v.version) FROM node_versions v WHERE v.id=n.id) AND (?3 IS NULL OR EXISTS (SELECT 1 FROM json_each(n.scope) WHERE value=?3)) UNION ALL SELECT 1 FROM candidates c WHERE c.id=l.target AND {ACTIVE_CANDIDATE} AND (?3 IS NULL OR EXISTS (SELECT 1 FROM json_each(c.scope) WHERE value=?3)) UNION ALL SELECT 1 FROM edge_versions e JOIN node_versions a ON a.id=e.src JOIN node_versions b ON b.id=e.dst WHERE e.id=l.target AND e.state='active' AND a.state='active' AND b.state='active' AND e.version=(SELECT max(v.version) FROM edge_versions v WHERE v.id=e.id) AND a.version=(SELECT max(v.version) FROM node_versions v WHERE v.id=a.id) AND b.version=(SELECT max(v.version) FROM node_versions v WHERE v.id=b.id) AND (?3 IS NULL OR EXISTS (SELECT 1 FROM json_each(a.scope) WHERE value=?3) OR EXISTS (SELECT 1 FROM json_each(b.scope) WHERE value=?3)))) SELECT eligible.*,c.kind AS candidate_kind,coalesce(c.src,e.src) AS loop_src,coalesce(c.dst,e.dst) AS loop_dst,(SELECT max(julianday(e.occurred_at)) FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id WHERE g.target=eligible.id) AS newest_evidence FROM eligible LEFT JOIN candidates c ON c.id=eligible.id LEFT JOIN edge_versions e ON e.id=eligible.id AND e.version=(SELECT max(v.version) FROM edge_versions v WHERE v.id=e.id) ORDER BY due_at IS NULL,due_at,newest_evidence DESC,eligible.id");
            let selected=visible_loop_targets(db,rows(db,&sql,params![now,cutoff,command.scope])?)?;
            let ids=selected.iter().map(|row|row["id"].as_str().unwrap().to_owned()).collect::<Vec<_>>();
            let roles=responsibilities(db,&ids)?;
            let matches=selected.iter().filter(|row|command.responsibility.as_deref().is_none_or(|wanted|roles.get(row["id"].as_str().unwrap()).and_then(|role|role["responsibility"].as_str()).unwrap_or("unknown")==wanted)).collect::<Vec<_>>();
            let total=json!(matches.len());let mut values=vec![];
            for row in matches.into_iter().take(command.limit) {if let Some(mut value)=target(db,get_str(row,"id")?)? {for key in ["id","state","due_at","deferred_until"] {value[key]=row[key].clone();}value["responsibility"]=json!(roles.get(row["id"].as_str().unwrap()).and_then(|role|role["responsibility"].as_str()).unwrap_or("unknown"));values.push(value);}}
            Ok(json!({"loops":values,"total":total}))
        });
    }
    let target_id = command
        .id
        .as_deref()
        .ok_or(Error::Validation("A loop mutation requires id"))?;
    let due = time(command.due_at.as_deref())?;
    if command.action == "remind" && due.is_none() {
        return Err(Error::Validation("remind requires an aware due_at"));
    }
    let mut request = json!({"op":"loop","action":command.action,"id":target_id});
    if command.action == "later" {
        request["days"] = json!(command.days);
    }
    if command.action == "remind" {
        request["due_at"] = json!(due);
    }
    if command.action == "assign" {
        let value = command
            .responsibility
            .as_deref()
            .ok_or(Error::Validation("assign requires responsibility"))?;
        request["responsibility"] = json!(value);
        request["evidence"] = serde_json::to_value(&command.evidence)?;
    }
    if !command.evidence.is_empty() || command.action == "decline" {
        request["evidence"] = serde_json::to_value(&command.evidence)?;
    }
    let context = context(
        command.command_id.clone().unwrap_or_else(id),
        command.actor.clone().unwrap_or("harness/loop".into()),
        command
            .why
            .clone()
            .unwrap_or("Loop state adjustment".into()),
    );
    graph.store.command(&context, &request, |tx, commit| {
        let active=target(tx.sql(), target_id)?;
        if active.is_none() || one(tx.sql(),"SELECT 1 FROM forgotten_targets WHERE target=?",[target_id])?.is_some() {
            return Err(Error::Validation(
                "Loop markers require an active memory or unconfirmed candidate",
            ));
        }
        let marker = current(tx.sql(), target_id)?;
        if command.action=="assign" {
            return assign(tx,commit,target_id,command.responsibility.as_deref().unwrap(),&request["evidence"],true);
        }
        if command.action=="decline" || (command.action=="close" && !command.evidence.is_empty()) {
            let grounded=grounded_sources(tx,target_id,&request["evidence"],command.action=="decline",command.action=="decline")?;
            let frozen=snapshot(tx.sql(),target_id,active.as_ref().unwrap()["version"].as_i64().unwrap())?;
            let mut result=if command.action=="decline" {decline_automatic(tx,commit,target_id,&frozen)?} else {
                let times=grounded["source_times"].as_array().unwrap().iter().map(|at|at.as_str().map(str::to_owned)).collect::<Vec<_>>();
                let(at,mut status)=closure_time(&frozen,&times)?;
                if protected(tx.sql(),active.as_ref().unwrap())? {status=Some("protected_loop_choice");}
                let mut result=json!({"id":target_id,"status":status.unwrap_or("closed"),"source_occurred_at":at});
                if status.is_none(){let previous=marker.as_ref().unwrap();let value=append(tx,commit,target_id,"closed",previous["due_at"].as_str(),None)?;for(key,value)in value.as_object().unwrap(){result[key]=value.clone();}}
                result
            };
            result["evidence"]=grounded["evidence"].clone();return Ok(result);
        }
        let deadline = marker.as_ref().and_then(|value| value["due_at"].as_str());
        if command.action == "close" {
            return append(tx, commit, target_id, "closed", deadline, None);
        }
        if command.action == "later" {
            let micros = command.days * 86400.0 * 1000000.0;
            if micros > i64::MAX as f64 {
                return Err(Error::Validation("Deferral is outside the timestamp range"));
            }
            let deferred = Utc::now()
                .checked_add_signed(Duration::microseconds(micros.round() as i64))
                .ok_or(Error::Validation("Deferral is outside the timestamp range"))?;
            if !(1..=9999).contains(&deferred.year()) {
                return Err(Error::Validation("Deferral is outside the timestamp range"));
            }
            return append(
                tx,
                commit,
                target_id,
                "open",
                deadline,
                Some(&format_time(deferred)),
            );
        }
        append(
            tx,
            commit,
            target_id,
            "open",
            if command.action == "remind" {
                due.as_deref()
            } else {
                deadline
            },
            None,
        )
    })
}

#[cfg(test)]
mod responsibility_tests {
    use super::*;
    use memento_core::{CommandContext, Store};
    use serde::de::DeserializeOwned;
    use std::{fs, os::unix::fs::PermissionsExt, sync::Arc};

    fn parse<T: DeserializeOwned>(value: Value) -> T {
        serde_json::from_value(value).unwrap()
    }
    struct F {
        _dir: tempfile::TempDir,
        store: Arc<Store>,
        graph: Graph,
        episode: String,
        owner: String,
        other: String,
    }
    impl F {
        fn new() -> Self {
            let dir = tempfile::Builder::new()
                .permissions(fs::Permissions::from_mode(0o700))
                .tempdir()
                .unwrap();
            let store = Store::open(dir.path()).unwrap();
            let episode=store.capture(parse(json!({"source":"native:fixture:pi","event_id":"one","conversation_id":"thread","kind":"user","text":"I need to renew my passport. Lena owes me signed approval.","occurred_at":"2026-01-01T00:00:00Z"}))).unwrap().id;
            let graph = Graph::new(store.clone());
            let mut ids = vec![];
            for (key, quote, scope) in [
                ("owner", "I need to renew my passport.", "professional"),
                ("other", "Lena owes me signed approval.", "personal"),
            ] {
                let result=graph.remember(parse(json!({"command_id":key,"actor":"fixture","why":"Original obligation evidence","type":"knowledge","text":quote,"scope":[scope],"evidence":[{"episode_id":episode,"quote":quote}]}))).unwrap();
                let id = result["id"].as_str().unwrap().to_owned();
                graph
                    .loop_command(parse(
                        json!({"action":"open","id":id,"command_id":format!("open-{key}")}),
                    ))
                    .unwrap();
                ids.push(id);
            }
            Self {
                _dir: dir,
                store,
                graph,
                episode,
                owner: ids[0].clone(),
                other: ids[1].clone(),
            }
        }
        fn tag(&self, target: &str, role: &str, episode: &str, quote: &str, key: &str) -> Value {
            self.graph.loop_command(parse(json!({"action":"assign","id":target,"responsibility":role,"command_id":key,"actor":"owner","why":"Explicit grounded role","evidence":[{"episode_id":episode,"quote":quote}]}))).unwrap()
        }
    }

    #[test]
    fn legacy_unknown_owner_and_other_wait_are_independent_of_scope() {
        let f = F::new();
        let unknown = f
            .graph
            .loop_command(parse(json!({"action":"list","responsibility":"unknown"})))
            .unwrap();
        assert_eq!(unknown["total"], 2);
        f.tag(
            &f.owner,
            "owner",
            &f.episode,
            "I need to renew my passport.",
            "tag-owner",
        );
        f.tag(
            &f.other,
            "other",
            &f.episode,
            "Lena owes me signed approval.",
            "tag-other",
        );
        let own = f
            .graph
            .loop_command(parse(
                json!({"action":"list","responsibility":"owner","limit":1}),
            ))
            .unwrap();
        assert_eq!(own["total"], 1);
        assert_eq!(own["loops"][0]["id"], f.owner);
        assert_eq!(own["loops"][0]["scope"], json!(["professional"]));
        let waiting = f
            .graph
            .loop_command(parse(json!({"action":"list","responsibility":"other"})))
            .unwrap();
        assert_eq!(waiting["total"], 1);
        assert_eq!(waiting["loops"][0]["state"], "open");
        assert_eq!(waiting["loops"][0]["id"], f.other);
    }

    #[test]
    fn fresh_manual_clarification_preserves_dates_choice_and_identity_continuity() {
        let f = F::new();
        f.graph.loop_command(parse(json!({"action":"remind","id":f.owner,"command_id":"due","due_at":"2027-01-01T00:00:00Z"}))).unwrap();
        f.graph
            .loop_command(parse(
                json!({"action":"later","id":f.owner,"command_id":"later","days":7}),
            ))
            .unwrap();
        let before = f
            .store
            .read(|tx| current(tx.sql(), &f.owner))
            .unwrap()
            .unwrap();
        let choice =
            f.store.read(|tx| snapshot(tx.sql(), &f.owner, 1)).unwrap()["manual_choice_at"].clone();
        let clarification=f.store.capture(parse(json!({"source":"native:fixture:pi","event_id":"clarification","conversation_id":"thread","kind":"user","text":"That passport obligation is mine."}))).unwrap().id;
        f.tag(
            &f.owner,
            "owner",
            &clarification,
            "That passport obligation is mine.",
            "clarify",
        );
        let after = f
            .store
            .read(|tx| current(tx.sql(), &f.owner))
            .unwrap()
            .unwrap();
        for key in ["state", "due_at", "deferred_until"] {
            assert_eq!(before[key], after[key]);
        }
        assert_eq!(
            f.store.read(|tx| snapshot(tx.sql(), &f.owner, 1)).unwrap()["manual_choice_at"],
            choice
        );
        let refined=f.graph.supersede(parse(json!({"command_id":"refine","actor":"owner","why":"Clearer same obligation","old":f.owner,"kind":"refinement","new":{"type":"knowledge","text":"Passport renewal remains outstanding.","scope":["professional"]},"evidence":[{"episode_id":f.episode,"quote":"I need to renew my passport."}]}))).unwrap()["superseded_by"].as_str().unwrap().to_owned();
        let result = f
            .store
            .command(
                &CommandContext {
                    command_id: "auto-relabel".into(),
                    actor: "consolidator/fixture".into(),
                    why: "Synthetic conflicting role".into(),
                },
                &json!({"op":"consolidate"}),
                |tx, commit| {
                    assign(
                        tx,
                        commit,
                        &refined,
                        "other",
                        &json!([{"episode_id":f.episode,"quote":"I need to renew my passport."}]),
                        false,
                    )
                },
            )
            .unwrap();
        assert_eq!(result["status"], "protected_loop_responsibility");
        f.graph
            .loop_command(parse(
                json!({"action":"close","id":refined,"command_id":"close"}),
            ))
            .unwrap();
        let closed = f
            .store
            .read(|tx| current(tx.sql(), &refined))
            .unwrap()
            .unwrap();
        assert_eq!(closed["state"], "closed");
        assert_eq!(closed["responsibility"], "owner");
    }

    #[test]
    fn owner_requires_native_user_evidence_and_forgotten_roles_are_hidden() {
        let f = F::new();
        let assistant=f.store.capture(parse(json!({"source":"native:fixture:pi","event_id":"assistant","conversation_id":"thread","kind":"assistant","text":"The passport is the owner task."}))).unwrap().id;
        let result=f.graph.loop_command(parse(json!({"action":"assign","id":f.owner,"responsibility":"owner","evidence":[{"episode_id":assistant,"quote":"The passport is the owner task."}]})));
        assert!(result.is_err());
        f.tag(
            &f.owner,
            "owner",
            &f.episode,
            "I need to renew my passport.",
            "assigned",
        );
        f.store
            .write(|tx| {
                tx.sql().execute(
                    "INSERT INTO forgotten_sources VALUES('native:fixture:pi','one',?,'fixture')",
                    [&f.owner],
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(
            f.store
                .read(|tx| responsibility(tx.sql(), &f.owner))
                .unwrap(),
            "unknown"
        );
    }

    #[test]
    fn grounded_recovery_supports_every_loop_target_and_source_time_reopening() {
        for kind in ["node", "candidate", "edge"] {
            let f = F::new();
            let pending=f.store.capture(parse(json!({"source":"native:fixture:pi","event_id":"pending","conversation_id":"thread","kind":"user","text":"I still owe the signed report.","occurred_at":"2026-01-01T00:00:00Z"}))).unwrap().id;
            let node=f.graph.remember(parse(json!({"command_id":"report","actor":"fixture","why":"Original pending evidence","type":"knowledge","text":"I still owe the signed report.","scope":["professional"],"evidence":[{"episode_id":pending,"quote":"I still owe the signed report."}]}))).unwrap()["id"].as_str().unwrap().to_owned();
            let target = if kind == "edge" {
                f.graph.link(parse(json!({"command_id":"report-edge","actor":"fixture","why":"Report connection","a":node,"rel":"related_to","b":f.other,"evidence":[{"episode_id":pending,"quote":"I still owe the signed report."}]}))).unwrap()["id"].as_str().unwrap().to_owned()
            } else if kind == "candidate" {
                f.store.command(&CommandContext{command_id:"candidate".into(),actor:"extractor/fixture".into(),why:"Synthetic pending candidate".into()},&json!({"op":"extract"}),|tx,commit|{
                    tx.sql().execute("INSERT INTO extraction_runs VALUES('report-run',?,?,'fixture','fixture','[]','{}','2026-01-01T00:00:00Z')",params![commit,pending])?;
                    tx.sql().execute("INSERT INTO candidates VALUES('report-candidate','report-run','memory','knowledge','I still owe the signed report.','Fixture',NULL,NULL,0.9,NULL,NULL,'[\"professional\"]')",[])?;
                    tx.sql().execute("INSERT INTO graph_evidence VALUES('report-candidate',1,?,0,30,'I still owe the signed report.')",[&pending])?;
                    Ok(json!({}))
                }).unwrap();
                "report-candidate".to_owned()
            } else {
                node
            };
            f.store
                .command(
                    &CommandContext {
                        command_id: "automatic".into(),
                        actor: "extractor/fixture".into(),
                        why: "Synthetic automatic marker".into(),
                    },
                    &json!({"op":"extract"}),
                    |tx, commit| {
                        mark(tx, commit, &target, Some(true), None, None)?;
                        Ok(json!({}))
                    },
                )
                .unwrap();
            f.tag(
                &target,
                "owner",
                &pending,
                "I still owe the signed report.",
                "role-only",
            );
            assert!(
                f.store.read(|tx| snapshot(tx.sql(), &target, 1)).unwrap()["manual_choice_at"]
                    .is_null()
            );
            let completion=f.store.capture(parse(json!({"source":"native:fixture:pi","event_id":"complete","conversation_id":"thread","kind":"user","text":"I sent the signed report.","occurred_at":"2026-01-02T00:00:00Z"}))).unwrap().id;
            let closed=f.graph.loop_command(parse(json!({"action":"close","id":target,"command_id":"grounded-close","evidence":[{"episode_id":completion,"quote":"I sent the signed report."}]}))).unwrap();
            assert_eq!(closed["status"], "closed");
            let renewed=f.store.capture(parse(json!({"source":"native:fixture:pi","event_id":"renewed","conversation_id":"thread","kind":"user","text":"The signed report is pending again.","occurred_at":"2026-01-03T00:00:00Z"}))).unwrap().id;
            let fresh=f.graph.remember(parse(json!({"command_id":"renewed-report","actor":"fixture","why":"Later original evidence","type":"knowledge","text":"The signed report is pending again.","scope":["professional"],"evidence":[{"episode_id":renewed,"quote":"The signed report is pending again."}]}))).unwrap()["id"].as_str().unwrap().to_owned();
            f.store
                .command(
                    &CommandContext {
                        command_id: "reopen".into(),
                        actor: "consolidator/fixture".into(),
                        why: "Later source reopening".into(),
                    },
                    &json!({"op":"consolidate"}),
                    |tx, commit| {
                        mark(tx, commit, &target, Some(true), Some(&fresh), None)?;
                        Ok(json!({}))
                    },
                )
                .unwrap();
            assert_eq!(
                f.store
                    .read(|tx| current(tx.sql(), &target))
                    .unwrap()
                    .unwrap()["state"],
                "open"
            );
            assert!(
                f.store.read(|tx| snapshot(tx.sql(), &target, 1)).unwrap()["manual_choice_at"]
                    .is_null()
            );
        }
    }

    #[test]
    fn empty_quote_occurrence_zero_cannot_assign_close_or_decline() {
        let f = F::new();
        for action in ["assign", "close", "decline"] {
            let mut command = json!({"action":action,"id":f.owner,"command_id":format!("empty-{action}"),"evidence":[{"episode_id":f.episode,"quote":"","occurrence":0}]});
            if action == "assign" {
                command["responsibility"] = json!("unknown");
            }
            assert!(f.graph.loop_command(parse(command)).is_err());
        }
        assert_eq!(
            f.store
                .read(|tx| current(tx.sql(), &f.owner))
                .unwrap()
                .unwrap()["state"],
            "open"
        );
    }
}
