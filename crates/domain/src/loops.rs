//! Reactive loop state and original-source chronology, independent of assertions.
use crate::util::*;
use crate::{Graph, behavior, context, dto};
use chrono::{Datelike, Duration, Utc};
use memento_core::{Error, Result, WriteTxn};
use rusqlite::{Connection, params};
use serde_json::{Value, json};

pub fn current(db: &Connection, target: &str) -> Result<Option<Value>> {
    one(
        db,
        "SELECT target AS id,state,due_at,deferred_until FROM loop_events WHERE target=? ORDER BY rowid DESC LIMIT 1",
        [target],
    )
}
pub fn events(db: &Connection, target: &str) -> Result<Vec<Value>> {
    rows(
        db,
        "SELECT l.state,l.due_at,l.deferred_until,l.commit_id,c.actor,c.why,c.recorded_at FROM loop_events l JOIN graph_commits c ON c.id=l.commit_id WHERE l.target=? ORDER BY l.rowid",
        [target],
    )
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
            "SELECT c.recorded_at FROM loop_events l JOIN graph_commits c ON c.id=l.commit_id WHERE l.target=? AND json_extract(c.request,'$.command.op')='loop' ORDER BY l.rowid DESC LIMIT 1",
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
                if let Some(at) = row["recorded_at"].as_str() {
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
    let n = node(tx.sql(), target)?;
    let live = snapshot(tx.sql(), target, get_i64(&n, "version")?)?;
    if live != *expected {
        return Err(Error::Conflict(
            "Consolidation loop state changed; derive a fresh plan",
        ));
    }
    let marker = current(tx.sql(), target)?;
    if n["state"] != "active"
        || n["pinned"].as_i64().unwrap_or(0) != 0
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
                "SELECT c.id,c.type,substr(c.text,1,240) AS text,c.scope,c.valid_from,c.valid_to FROM candidates c WHERE c.id=? AND {ACTIVE_CANDIDATE}"
            ),
            [id],
        )?;
        let mut value = if let Some(mut value) = candidate {
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
    if !["list", "open", "close", "later", "remind"].contains(&command.action.as_str()) {
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
    let cutoff = time(command.due_before.as_deref())?;
    if command.action == "list" {
        return graph.store.read(|tx|{
            let db=tx.sql();let now=now();
            let sql=format!("WITH eligible AS (SELECT l.target AS id,l.state,l.due_at,l.deferred_until FROM loop_events l WHERE l.rowid=(SELECT max(x.rowid) FROM loop_events x WHERE x.target=l.target) AND l.state='open' AND (l.deferred_until IS NULL OR l.deferred_until<=?1) AND (?2 IS NULL OR l.due_at<?2) AND EXISTS (SELECT 1 FROM node_versions n WHERE n.id=l.target AND n.state='active' AND n.version=(SELECT max(v.version) FROM node_versions v WHERE v.id=n.id) AND (?3 IS NULL OR EXISTS (SELECT 1 FROM json_each(n.scope) WHERE value=?3)) UNION ALL SELECT 1 FROM candidates c WHERE c.id=l.target AND {ACTIVE_CANDIDATE} AND (?3 IS NULL OR EXISTS (SELECT 1 FROM json_each(c.scope) WHERE value=?3)) UNION ALL SELECT 1 FROM edge_versions e JOIN node_versions a ON a.id=e.src JOIN node_versions b ON b.id=e.dst WHERE e.id=l.target AND e.state='active' AND a.state='active' AND b.state='active' AND e.version=(SELECT max(v.version) FROM edge_versions v WHERE v.id=e.id) AND a.version=(SELECT max(v.version) FROM node_versions v WHERE v.id=a.id) AND b.version=(SELECT max(v.version) FROM node_versions v WHERE v.id=b.id) AND (?3 IS NULL OR EXISTS (SELECT 1 FROM json_each(a.scope) WHERE value=?3) OR EXISTS (SELECT 1 FROM json_each(b.scope) WHERE value=?3)))) SELECT eligible.*,count(*) OVER () AS total,(SELECT max(julianday(e.occurred_at)) FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id WHERE g.target=eligible.id) AS newest_evidence FROM eligible ORDER BY due_at IS NULL,due_at,newest_evidence DESC,id LIMIT ?4");
            let selected=rows(db,&sql,params![now,cutoff,command.scope,command.limit as i64])?;
            let total=selected.first().map(|row|row["total"].clone()).unwrap_or(json!(0));let mut values=vec![];
            for row in selected {if let Some(mut value)=target(db,get_str(&row,"id")?)? {for key in ["id","state","due_at","deferred_until"] {value[key]=row[key].clone();}values.push(value);}}
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
    let context = context(
        command.command_id.clone().unwrap_or_else(id),
        command.actor.clone().unwrap_or("harness/loop".into()),
        command
            .why
            .clone()
            .unwrap_or("Loop state adjustment".into()),
    );
    graph.store.command(&context, &request, |tx, commit| {
        if target(tx.sql(), target_id)?.is_none() {
            return Err(Error::Validation(
                "Loop markers require an active memory or unconfirmed candidate",
            ));
        }
        let marker = current(tx.sql(), target_id)?;
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
