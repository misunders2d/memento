//! Append-only graph authority over the single native SQLite writer.
//! Agent APIs cannot approve protected changes; owner transports call explicit
//! review/merge/forget methods after their independent OS admission boundary.
pub mod authority;
pub mod consolidation;
pub mod curation;
pub mod demand;
mod diff;
pub mod dto;
pub mod extraction;
pub mod forgetting;
pub mod importance;
pub mod jobs;
pub mod loops;
pub mod pipeline_schema;
pub mod processing;
/// Frozen source-runtime word and casefold data, shared with retrieval.
pub mod unicode;
mod util;
pub use authority::behavior_ground_truth;
pub use demand::{demand_extract, source_text_ranges};
#[cfg(test)]
mod pipeline_tests;
#[cfg(test)]
mod tests;

use dto::{Evidence, Link, Remember, Supersede};
use memento_core::{CommandContext, Error, Result, Store, WriteTxn};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::sync::Arc;
use util::*;

#[derive(Clone)]
pub struct Graph {
    pub store: Arc<Store>,
}

impl Graph {
    pub fn new(store: Arc<Store>) -> Self {
        Self { store }
    }

    pub fn remember(&self, command: Remember) -> Result<Value> {
        validate_common(
            &command.command_id,
            &command.actor,
            &command.why,
            &command.evidence,
            command.source.as_deref(),
            command.conversation_id.as_deref(),
        )?;
        if !valid_type(&command.kind) || command.text.trim().is_empty() {
            return Err(Error::Validation("Invalid node type or empty assertion"));
        }
        let valid_from = time(command.valid_from.as_deref())?;
        let valid_to = time(command.valid_to.as_deref())?;
        if let (Some(left), Some(right)) = (&valid_from, &valid_to) {
            if parse_time(left)? >= parse_time(right)? {
                return Err(Error::Validation("valid_from must precede valid_to"));
            }
        }
        let labels = self
            .store
            .read(|tx| scope(tx.sql(), &command.kind, &command.scope))?;
        let request = json!({"op":"remember","type":command.kind,"text":command.text,
            "evidence":command.evidence,"scope":labels,"source":command.source,
            "conversation_id":command.conversation_id,"valid_from":valid_from,"valid_to":valid_to});
        let context = context(command.command_id, command.actor, command.why);
        self.store.command(&context, &request, |tx, commit| {
            TxGraph::remember(tx, commit, &request, false)
        })
    }

    pub fn link(&self, command: Link) -> Result<Value> {
        validate_common(
            &command.command_id,
            &command.actor,
            &command.why,
            &command.evidence,
            command.source.as_deref(),
            command.conversation_id.as_deref(),
        )?;
        required(&command.a, 64, "Invalid relation endpoint")?;
        required(&command.b, 64, "Invalid relation endpoint")?;
        if !valid_relation(&command.rel) {
            return Err(Error::Validation("Invalid directional relation"));
        }
        let request = json!({"op":"link","a":command.a,"rel":command.rel,"b":command.b,
            "evidence":command.evidence,"source":command.source,"conversation_id":command.conversation_id});
        let context = context(command.command_id, command.actor, command.why);
        self.store.command(&context, &request, |tx, commit| {
            TxGraph::link(tx, commit, &request, false)
        })
    }

    pub fn supersede(&self, command: Supersede) -> Result<Value> {
        validate_common(
            &command.command_id,
            &command.actor,
            &command.why,
            &command.evidence,
            command.source.as_deref(),
            command.conversation_id.as_deref(),
        )?;
        required(&command.old, 64, "Invalid supersession target")?;
        if !["correction", "change", "refinement"].contains(&command.kind.as_str()) {
            return Err(Error::Validation(
                "Supersession kind must be correction, change or refinement",
            ));
        }
        let valid_at = time(command.valid_at.as_deref())?;
        if command.kind == "change" && valid_at.is_none() {
            return Err(Error::Validation(
                "A world-time change requires an aware valid_at",
            ));
        }
        if !valid_type(&command.new.kind) || command.new.text.trim().is_empty() {
            return Err(Error::Validation("Invalid replacement assertion"));
        }
        let labels = self
            .store
            .read(|tx| scope(tx.sql(), &command.new.kind, &command.new.scope))?;
        let request = json!({"op":"supersede","old":command.old,
            "new":{"type":command.new.kind,"text":command.new.text,"scope":labels},
            "evidence":command.evidence,"kind":command.kind,"valid_at":valid_at,
            "source":command.source,"conversation_id":command.conversation_id});
        let context = context(command.command_id, command.actor, command.why);
        self.store.command(&context, &request, |tx, commit| {
            TxGraph::supersede(tx, commit, &request, false)
        })
    }

    pub fn history(&self, target: &str) -> Result<Vec<Value>> {
        self.store
            .read(|tx| history(tx.sql(), tx.data_dir(), target))
    }

    pub fn loop_command(&self, command: dto::Loop) -> Result<Value> {
        loops::command(self, command)
    }
    pub fn cite(&self, command: dto::Citation) -> Result<Value> {
        importance::cite(self, command)
    }
    pub fn pending_reviews(&self) -> Result<Vec<Value>> {
        self.store.read(|tx| curation::pending_reviews(tx.sql()))
    }
    pub fn decide_review(&self, command: dto::Review) -> Result<Value> {
        curation::decide_review(self, command)
    }
    pub fn forget_preview(&self, target: &str) -> Result<Value> {
        self.store.read(|tx| forgetting::preview(tx.sql(), target))
    }
    pub fn forget(&self, command: dto::Forget) -> Result<Value> {
        forgetting::forget(self, command)
    }
    pub fn merge_inventory(&self) -> Result<Value> {
        self.store.read(|tx| curation::inventory(tx.sql()))
    }
    pub fn merge_preview(
        &self,
        cluster: &str,
        into: Option<&str>,
        exclude: &[String],
        include: &[String],
    ) -> Result<Value> {
        self.store
            .read(|tx| curation::merge_plan(tx.sql(), cluster, into, exclude, include))
    }
    pub fn merge(&self, command: dto::Merge) -> Result<Value> {
        curation::merge(self, command)
    }

    pub fn recall_keywords(
        &self,
        query: &str,
        limit: usize,
        scope: Option<&str>,
    ) -> Result<Vec<Value>> {
        self.store
            .read(|tx| recall_keywords(tx.sql(), query, limit, scope))
    }
    pub fn exact_identities(&self, kind: &str, text: &str) -> Result<Vec<Value>> {
        self.store.read(|tx| exact_identities(tx.sql(), kind, text))
    }
}

pub fn context(command_id: String, actor: String, why: String) -> CommandContext {
    CommandContext {
        command_id,
        actor,
        why,
    }
}

pub fn validate_common(
    command_id: &str,
    actor: &str,
    why: &str,
    evidence: &[Evidence],
    source: Option<&str>,
    conversation: Option<&str>,
) -> Result<()> {
    required(command_id, 512, "command_id, actor and why are required")?;
    required(actor, 256, "command_id, actor and why are required")?;
    if why.trim().is_empty() {
        return Err(Error::Validation("command_id, actor and why are required"));
    }
    if evidence.is_empty() || evidence.len() > 100 {
        return Err(Error::Validation(
            "An assertion requires exact source evidence",
        ));
    }
    optional(source, 256, "Invalid source")?;
    optional(conversation, 512, "Invalid conversation_id")?;
    for item in evidence {
        optional(
            item.episode_id.as_deref(),
            64,
            "Invalid evidence episode identifier",
        )?;
        if item.quote.is_empty() {
            return Err(Error::Validation(
                "Evidence quote must be a nonempty string",
            ));
        }
    }
    Ok(())
}

/// Stateless transactional operations shared by commands and processing.
/// Caller holds one writer transaction; no operation performs network/model I/O.
pub struct TxGraph;

impl TxGraph {
    pub fn resolve_evidence(tx: &WriteTxn, request: &Value) -> Result<Value> {
        let mut resolved = request.clone();
        let list = request["evidence"].as_array().ok_or(Error::Validation(
            "An assertion requires exact source evidence",
        ))?;
        let mut sources = Vec::new();
        for source in list {
            if source
                .get("episode_id")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty())
            {
                sources.push(source.clone());
                continue;
            }
            let quote = get_str(source, "quote")?;
            if quote.is_empty() {
                return Err(Error::Validation(
                    "Evidence quote must be a nonempty string",
                ));
            }
            let origin = request["source"]
                .as_str()
                .filter(|value| !value.is_empty())
                .ok_or(Error::Validation(
                    "Quote-only evidence requires source and conversation_id",
                ))?;
            let conversation = request["conversation_id"]
                .as_str()
                .filter(|value| !value.is_empty())
                .ok_or(Error::Validation(
                    "Quote-only evidence requires source and conversation_id",
                ))?;
            let row = one(tx.sql(), "SELECT id FROM episodes WHERE source=? AND conversation_id=? AND instr(text,?)>0 AND coalesce(json_extract(metadata,'$.extraction_mode'),'message')!='skip' AND coalesce(json_extract(metadata,'$.evidence_eligible'),1)!=0 AND kind NOT IN ('native_record','native_hook','native_live_record') AND NOT EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=episodes.source AND f.event_id=episodes.event_id) ORDER BY seq DESC LIMIT 1", params![origin, conversation, quote])?
                .ok_or(Error::Validation("Evidence quote was not found in the current conversation"))?;
            let mut source = source.clone();
            source["episode_id"] = row["id"].clone();
            sources.push(source);
        }
        resolved["evidence"] = Value::Array(sources);
        Ok(resolved)
    }

    pub fn evidence(tx: &WriteTxn, target: &str, version: i64, sources: &Value) -> Result<()> {
        let list = sources
            .as_array()
            .filter(|values| !values.is_empty())
            .ok_or(Error::Validation(
                "An assertion requires exact source evidence",
            ))?;
        let mut seen = std::collections::HashSet::new();
        for source in list {
            let episode_id = get_str(source, "episode_id")?;
            let row = one(
                tx.sql(),
                "SELECT id,source,event_id,text_runs FROM episodes WHERE id=?",
                [episode_id],
            )?
            .ok_or(Error::Validation("Unknown source episode"))?;
            if one(
                tx.sql(),
                "SELECT 1 FROM forgotten_sources WHERE source=? AND event_id=? LIMIT 1",
                params![row["source"].as_str(), row["event_id"].as_str()],
            )?
            .is_some()
            {
                return Err(Error::Conflict(
                    "This evidence was excluded from active memory by a human decision",
                ));
            }
            if !row["text_runs"].is_null() {
                return Err(Error::Validation(
                    "Offloaded raw source is not assertion evidence; cite the message projection",
                ));
            }
            let text = tx
                .original_text(episode_id)?
                .ok_or(Error::Validation("Unknown source episode"))?;
            let quote = get_str(source, "quote")?;
            if quote.is_empty() {
                return Err(Error::Validation(
                    "Evidence quote must be a nonempty string",
                ));
            }
            // Search at every UTF-8 character boundary to retain Python's
            // overlapping occurrences, then store Unicode scalar offsets.
            let matches = text
                .char_indices()
                .filter_map(|(byte, _)| text[byte..].starts_with(quote).then_some(byte))
                .collect::<Vec<_>>();
            let occurrence = source.get("occurrence").filter(|value| !value.is_null());
            if matches.is_empty() || occurrence.is_none() && matches.len() != 1 {
                return Err(Error::Validation(
                    "Evidence must match exactly and unambiguously",
                ));
            }
            let index = occurrence
                .map_or(Some(0), Value::as_u64)
                .ok_or(Error::Validation("Invalid evidence occurrence"))?
                as usize;
            let byte = *matches
                .get(index)
                .ok_or(Error::Validation("Invalid evidence occurrence"))?;
            let start = text[..byte].chars().count();
            let end = start + quote.chars().count();
            if !seen.insert((episode_id.to_owned(), start, end)) {
                continue;
            }
            tx.sql().execute("INSERT INTO graph_evidence SELECT ?,?,?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM graph_evidence WHERE target=? AND version=? AND episode_id=? AND start=? AND end=?)",
                params![target,version,episode_id,start as i64,end as i64,quote,target,version,episode_id,start as i64,end as i64])?;
        }
        tx.touch_target(target)?;
        Ok(())
    }

    pub fn propose(tx: &WriteTxn, commit: &str, request: &Value, bases: &Value) -> Result<Value> {
        let proposal = format!("r_{}", &id()[..16]);
        Self::evidence(tx, &proposal, 1, &request["evidence"])?;
        tx.sql().execute(
            "INSERT INTO review_proposals VALUES (?,?,?,?)",
            params![proposal, commit, encode_json(request)?, encode_json(bases)?],
        )?;
        Ok(json!({"id":proposal,"status":"pending_review"}))
    }

    pub fn insert_node(tx: &WriteTxn, commit: &str, request: &Value) -> Result<Value> {
        let target = short_id();
        tx.sql().execute(
            "INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            params![
                target,
                1,
                commit,
                get_str(request, "type")?,
                get_str(request, "text")?,
                "active",
                request["valid_from"].as_str(),
                request["valid_to"].as_str(),
                Option::<&str>::None,
                i64::from(request["pinned"].as_bool().unwrap_or(false)),
                encode_json(&request["scope"])?
            ],
        )?;
        tx.sql()
            .execute("INSERT INTO node_stats(node_id) VALUES (?)", [&target])?;
        Self::evidence(tx, &target, 1, &request["evidence"])?;
        tx.touch_target(&target)?;
        Ok(json!({"id":target,"version":1}))
    }

    pub fn remember(tx: &WriteTxn, commit: &str, request: &Value, approved: bool) -> Result<Value> {
        let mut request = Self::resolve_evidence(tx, request)?;
        let kind = get_str(&request, "type")?;
        let labels: Vec<String> = serde_json::from_value(request["scope"].clone())
            .map_err(|_| Error::Validation("Invalid scope"))?;
        request["scope"] = json!(scope(tx.sql(), kind, &labels)?);
        let kind = get_str(&request, "type")?;
        let known = known_type(tx.sql(), kind)?;
        if !approved && (!known || sensitive(tx.sql(), kind)?) {
            return Self::propose(tx, commit, &request, &json!({}));
        }
        if !known {
            tx.sql().execute(
                "INSERT INTO memory_types VALUES (?,?,?,?)",
                params![kind, "memory", 1, commit],
            )?;
            tx.touch_type(kind)?;
        }
        Self::insert_node(tx, commit, &request)
    }

    pub fn link(tx: &WriteTxn, commit: &str, request: &Value, approved: bool) -> Result<Value> {
        let request = Self::resolve_evidence(tx, request)?;
        let a = node(tx.sql(), get_str(&request, "a")?)?;
        let b = node(tx.sql(), get_str(&request, "b")?)?;
        if a["state"] != "active" || b["state"] != "active" {
            return Err(Error::Validation("Relations require active endpoints"));
        }
        if !approved
            && ([&a, &b]
                .iter()
                .any(|node| node["pinned"].as_i64().unwrap_or(0) != 0)
                || sensitive(tx.sql(), get_str(&a, "type")?)?
                || sensitive(tx.sql(), get_str(&b, "type")?)?)
        {
            return Self::propose(
                tx,
                commit,
                &request,
                &json!({get_str(&a,"id")?:a["version"],get_str(&b,"id")?:b["version"]}),
            );
        }
        let target = short_id();
        tx.sql().execute(
            "INSERT INTO edge_versions VALUES (?,?,?,?,?,?,?)",
            params![
                target,
                1,
                commit,
                get_str(&request, "a")?,
                get_str(&request, "rel")?,
                get_str(&request, "b")?,
                "active"
            ],
        )?;
        Self::evidence(tx, &target, 1, &request["evidence"])?;
        tx.touch_target(get_str(&a, "id")?)?;
        tx.touch_target(get_str(&b, "id")?)?;
        Ok(json!({"id":target,"version":1}))
    }

    pub fn supersede(
        tx: &WriteTxn,
        commit: &str,
        request: &Value,
        approved: bool,
    ) -> Result<Value> {
        let mut request = Self::resolve_evidence(tx, request)?;
        let labels: Vec<String> = serde_json::from_value(request["new"]["scope"].clone())
            .map_err(|_| Error::Validation("Invalid scope"))?;
        request["new"]["scope"] =
            json!(scope(tx.sql(), get_str(&request["new"], "type")?, &labels)?);
        let old = get_str(&request, "old")?;
        let new = &request["new"];
        let kind = get_str(&request, "kind")?;
        let previous = node(tx.sql(), old)?;
        if previous["state"] != "active" {
            return Err(Error::Validation("Supersession requires an active node"));
        }
        let new_type = get_str(new, "type")?;
        let labels: Vec<String> = serde_json::from_value(new["scope"].clone())
            .map_err(|_| Error::Validation("Invalid scope"))?;
        scope(tx.sql(), new_type, &labels)?;
        if !approved
            && (previous["pinned"].as_i64().unwrap_or(0) != 0
                || sensitive(tx.sql(), get_str(&previous, "type")?)?
                || sensitive(tx.sql(), new_type)?
                || !known_type(tx.sql(), new_type)?)
        {
            return Self::propose(tx, commit, &request, &json!({old:previous["version"]}));
        }
        if !known_type(tx.sql(), new_type)? {
            tx.sql().execute(
                "INSERT INTO memory_types VALUES (?,?,?,?)",
                params![new_type, "memory", 1, commit],
            )?;
            tx.touch_type(new_type)?;
        }
        let valid_at = request["valid_at"].as_str();
        if kind == "change" {
            let instant = parse_time(valid_at.ok_or(Error::Validation(
                "A world-time change requires an aware valid_at",
            ))?)?;
            if let Some(start) = previous["valid_from"].as_str() {
                if instant <= parse_time(start)? {
                    return Err(Error::Validation(
                        "Change must follow the previous valid_from",
                    ));
                }
            }
        }
        let (from, to) = if kind == "change" {
            (request["valid_at"].clone(), Value::Null)
        } else if kind == "refinement" {
            (previous["valid_from"].clone(), previous["valid_to"].clone())
        } else {
            (Value::Null, Value::Null)
        };
        let replacement = Self::insert_node(
            tx,
            commit,
            &json!({"type":new_type,"text":new["text"],"scope":new["scope"],
            "valid_from":from,"valid_to":to,"evidence":request["evidence"],"pinned":previous["pinned"].as_i64().unwrap_or(0) != 0}),
        )?;
        let replacement_id = get_str(&replacement, "id")?;
        let version = get_i64(&previous, "version")? + 1;
        let state = match kind {
            "correction" => "corrected",
            "change" => "superseded",
            "refinement" => "refined",
            _ => return Err(Error::Validation("Invalid supersession kind")),
        };
        tx.sql().execute(
            "INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            params![
                old,
                version,
                commit,
                get_str(&previous, "type")?,
                get_str(&previous, "text")?,
                state,
                previous["valid_from"].as_str(),
                if kind == "change" {
                    valid_at
                } else {
                    previous["valid_to"].as_str()
                },
                replacement_id,
                get_i64(&previous, "pinned")?,
                get_str(&previous, "scope")?
            ],
        )?;
        Self::evidence(tx, old, version, &request["evidence"])?;
        curation::invalidate_vectors(tx, old)?;
        let edge = short_id();
        tx.sql().execute(
            "INSERT INTO edge_versions VALUES (?,?,?,?,?,?,?)",
            params![edge, 1, commit, replacement_id, "supersedes", old, "active"],
        )?;
        Self::evidence(tx, &edge, 1, &request["evidence"])?;
        if kind == "refinement" {
            let old_scope = decode_json(get_str(&previous, "scope")?)?;
            if labels.iter().all(|label| {
                old_scope
                    .as_array()
                    .is_some_and(|labels| labels.iter().any(|old| old == label))
            }) {
                curation::union_evidence(
                    tx,
                    replacement_id,
                    1,
                    &[(old.to_owned(), get_i64(&previous, "version")?)],
                )?;
            }
            curation::redirect_edges(tx, commit, old, replacement_id, None)?;
        }
        loops::transfer(tx, commit, old, replacement_id)?;
        importance::transfer_usage(tx, old, replacement_id)?;
        tx.touch_target(old)?;
        tx.touch_target(replacement_id)?;
        Ok(json!({"id":old,"version":version,"superseded_by":replacement_id,"kind":kind}))
    }
}

pub fn behavior(db: &Connection, target: &str) -> Result<Option<Value>> {
    let record = one(
        db,
        "SELECT g.request,g.result FROM node_versions n JOIN graph_commits g ON g.id=n.commit_id WHERE n.id=? AND n.version=1",
        [target],
    )?;
    let Some(record) = record else {
        return Ok(None);
    };
    let request = decode_json(get_str(&record, "request")?)?;
    let result = decode_json(get_str(&record, "result")?)?;
    let command = &request["command"];
    if command["op"] != "consolidate" {
        return Ok(None);
    }
    for action in command["output"]["actions"]
        .as_array()
        .into_iter()
        .flatten()
    {
        let outcome = &result["results"][action["candidate_id"].as_str().unwrap_or("")];
        let id = outcome["superseded_by"]
            .as_str()
            .or_else(|| outcome["id"].as_str());
        if id == Some(target)
            && ["remember", "supersede"].contains(&action["operation"].as_str().unwrap_or(""))
            && action["type"] == "experience"
            && action["behavior"].is_object()
        {
            return Ok(Some(action["behavior"].clone()));
        }
    }
    for repair in command["output"]["repairs"]
        .as_array()
        .into_iter()
        .flatten()
    {
        let outcome = &result["repair_results"][repair["target"].as_str().unwrap_or("")];
        if outcome["superseded_by"] == target
            && repair["operation"] == "supersede"
            && repair["type"] == "experience"
            && repair["behavior"].is_object()
        {
            return Ok(Some(repair["behavior"].clone()));
        }
    }
    Ok(None)
}

pub fn history(db: &Connection, data_dir: &std::path::Path, target: &str) -> Result<Vec<Value>> {
    let chunk = one(
        db,
        "SELECT episode_id,start,end FROM episode_chunks WHERE id=?",
        [target],
    )?;
    let episode_id = chunk
        .as_ref()
        .and_then(|row| row["episode_id"].as_str())
        .unwrap_or(target);
    if let Some(mut raw) = one(db, "SELECT * FROM episodes WHERE id=?", [episode_id])? {
        let text = memento_core::raw::read_original(db, data_dir, episode_id)?.ok_or(
            Error::Unavailable("Original raw source artifact is unavailable or corrupt"),
        )?;
        let blocked = one(
            db,
            "SELECT 1 FROM forgotten_sources WHERE source=? AND event_id=? LIMIT 1",
            params![raw["source"].as_str(), raw["event_id"].as_str()],
        )?
        .is_some();
        let parked=one(db,"SELECT 1 FROM historical_parking WHERE source=? AND conversation_id=? AND ?>=0 AND ?<=parked_through",params![raw["source"].as_str(),raw["conversation_id"].as_str(),raw["seq"].as_i64(),raw["seq"].as_i64()])?.is_some();
        let start = chunk
            .as_ref()
            .and_then(|row| row["start"].as_u64())
            .unwrap_or(0) as usize;
        let end = chunk
            .as_ref()
            .and_then(|row| row["end"].as_u64())
            .map(|value| value as usize)
            .unwrap_or(text.chars().count());
        let exact = substring(&text, start, end);
        let source = raw["source"].clone();
        let captured = raw["captured_at"].clone();
        raw.as_object_mut().expect("SQL row").remove("seq");
        raw.as_object_mut().unwrap().remove("text_runs");
        raw.as_object_mut().unwrap().remove("text_sha256");
        raw.as_object_mut().unwrap().remove("raw_frame");
        raw["metadata"] = decode_json(get_str(&raw, "metadata")?)?;
        for (key,value) in json!({"id":target,"episode_id":episode_id,"chunk_id":chunk.as_ref().map(|_|target),"version":1,"text":exact,
            "type":"raw_episode","state":"raw","confirmed":false,"historical":parked,"actor":source,"why":"Immutable captured evidence",
            "recorded_at":captured,"recorded_until":null,"valid_from":null,"valid_to":null,"forgotten":blocked,
            "evidence":[{"episode_id":episode_id,"start":start,"end":end,"quote":substring(&text,start,end)}]}).as_object().unwrap() { raw[key]=value.clone(); }
        return Ok(vec![raw]);
    }
    let mut values = rows(
        db,
        "WITH RECURSIVE lineage(id) AS (SELECT id FROM node_versions WHERE id=? UNION SELECT CASE WHEN e.src=l.id THEN e.dst ELSE e.src END FROM edge_versions e JOIN lineage l ON e.src=l.id OR e.dst=l.id WHERE e.rel='supersedes') SELECT n.*,c.actor,c.why,c.recorded_at,lead(c.recorded_at) OVER (PARTITION BY n.id ORDER BY n.version) AS recorded_until FROM lineage l JOIN node_versions n ON n.id=l.id JOIN graph_commits c ON c.id=n.commit_id ORDER BY (SELECT v.rowid FROM node_versions v WHERE v.id=n.id ORDER BY v.version LIMIT 1),n.version",
        [target],
    )?;
    if values.is_empty() {
        values = rows(
            db,
            "SELECT e.*,c.actor,c.why,c.recorded_at,lead(c.recorded_at) OVER (ORDER BY e.version) AS recorded_until FROM edge_versions e JOIN graph_commits c ON c.id=e.commit_id WHERE e.id=? ORDER BY e.version",
            [target],
        )?;
    }
    if values.is_empty() {
        values = rows(
            db,
            "SELECT p.*,1 AS version,'candidate' AS state,r.version AS extractor_version,r.requested_model,r.reported_models,r.usage,r.commit_id,c.actor,c.why AS extraction_why,c.recorded_at,NULL AS recorded_until FROM candidates p JOIN extraction_runs r ON r.id=p.run_id JOIN graph_commits c ON c.id=r.commit_id WHERE p.id=?",
            [target],
        )?;
    }
    let mut previous = String::new();
    for value in &mut values {
        if let Some(labels) = value["scope"].as_str() {
            value["scope"] = decode_json(labels)?;
        }
        let id = get_str(value, "id")?.to_owned();
        let version = get_i64(value, "version")?;
        if value["type"] == "experience" {
            if let Some(behavior) = behavior(db, &id)? {
                value["authority"] = behavior["authority"].clone();
                value["behavior"] = behavior;
                value["category"] = json!("behavioral_suggestion");
            }
        }
        value["forgotten"] =
            json!(one(db, "SELECT 1 FROM forgotten_targets WHERE target=?", [&id])?.is_some());
        value["evidence"] = Value::Array(rows(
            db,
            "SELECT episode_id,start,end,quote FROM graph_evidence WHERE target=? AND version=?",
            params![id, version],
        )?);
        let text = value["text"].as_str().unwrap_or("").to_owned();
        value["diff"] = Value::Array(diff::word_diff(&previous, &text));
        previous = text;
        value["loop"] = loops::current(db, &id)?.unwrap_or(Value::Null);
        if version == 1 {
            value["loop_events"] = Value::Array(loops::events(db, &id)?);
        }
        if value["state"] == "candidate" {
            value["reported_models"] = decode_json(get_str(value, "reported_models")?)?;
            value["usage"] = decode_json(get_str(value, "usage")?)?;
            value["decisions"] = Value::Array(rows(
                db,
                "SELECT d.decision,d.target_id,d.next_review_at,c.actor,c.why,c.recorded_at FROM candidate_decisions d JOIN graph_commits c ON c.id=d.commit_id WHERE d.candidate_id=? ORDER BY c.recorded_at,d.rowid",
                [&id],
            )?);
            value["entity_matches"] = Value::Array(rows(
                db,
                "SELECT * FROM candidate_entity_matches WHERE candidate_id=? ORDER BY target,version",
                [&id],
            )?);
        }
    }
    Ok(values)
}

pub fn recall_keywords(
    db: &Connection,
    query: &str,
    limit: usize,
    scope: Option<&str>,
) -> Result<Vec<Value>> {
    if !(1..=100).contains(&limit)
        || query.contains('\0')
        || !matches!(scope, None | Some("personal" | "professional"))
    {
        return Err(Error::Validation("Invalid recall limit, query or scope"));
    }
    let literal = query
        .split_whitespace()
        .map(|token| format!("\"{}\"", token.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" ");
    if literal.is_empty() {
        return Ok(vec![]);
    }
    let columns = "n.id,n.version,n.type,substr(n.text,1,240) AS text,n.state,n.valid_from,n.valid_to,n.pinned,n.superseded_by,n.commit_id,n.scope,(SELECT c.recorded_at FROM node_versions v JOIN graph_commits c ON c.id=v.commit_id WHERE v.id=n.id ORDER BY v.version LIMIT 1) AS created_at,coalesce((SELECT importance FROM node_stats WHERE node_id=n.id),0.5) AS importance,coalesce((SELECT decay FROM node_stats WHERE node_id=n.id),1) AS decay,coalesce((SELECT archived FROM node_stats WHERE node_id=n.id),0) AS archived,coalesce((SELECT access_count FROM node_stats WHERE node_id=n.id),0) AS access_count,(SELECT last_cited_at FROM node_stats WHERE node_id=n.id) AS last_cited_at";
    let visible = "n.state='active' AND n.version=(SELECT max(v.version) FROM node_versions v WHERE v.id=n.id) AND NOT EXISTS (SELECT 1 FROM forgotten_targets f WHERE f.target=n.id) AND (NOT EXISTS (SELECT 1 FROM captured_media cm WHERE cm.node_id=n.id) OR EXISTS (SELECT 1 FROM captured_media cm JOIN captured_media_refs r ON r.sha256=cm.sha256 JOIN episodes e ON e.id=r.episode_id WHERE cm.node_id=n.id AND NOT EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id) AND NOT EXISTS (SELECT 1 FROM media_files m JOIN forgotten_targets f ON f.target=m.node_id WHERE m.sha256=cm.sha256))) AND (?2 IS NULL OR EXISTS (SELECT 1 FROM json_each(n.scope) WHERE value=?2))";
    let mut hits = rows(
        db,
        &format!(
            "SELECT {columns},bm25(node_fts) AS keyword_rank FROM node_fts JOIN node_versions n ON n.id=node_fts.id AND n.version=node_fts.version WHERE node_fts MATCH ?1 AND {visible} ORDER BY keyword_rank,n.id LIMIT ?3"
        ),
        params![literal, scope, limit as i64],
    )?;
    let aliases = rows(
        db,
        &format!(
            "WITH RECURSIVE canonical(source,target,depth) AS (SELECT source,target,0 FROM entity_merges UNION ALL SELECT c.source,m.superseded_by,c.depth+1 FROM canonical c JOIN node_versions m ON m.id=c.target AND m.state IN ('merged','refined') AND m.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=m.id) WHERE m.superseded_by IS NOT NULL AND c.depth<64) SELECT {columns},bm25(alias_fts) AS keyword_rank FROM alias_fts JOIN canonical a ON a.source=alias_fts.id JOIN node_versions n ON n.id=a.target WHERE alias_fts MATCH ?1 AND {visible} AND NOT EXISTS (SELECT 1 FROM entity_merges WHERE source=a.target) ORDER BY keyword_rank,n.id LIMIT ?3"
        ),
        params![literal, scope, limit as i64],
    )?;
    for row in aliases {
        if !hits.iter().any(|hit| hit["id"] == row["id"]) {
            hits.push(row);
        }
    }
    for row in &mut hits {
        row["scope"] = decode_json(get_str(row, "scope")?)?;
    }
    hits.sort_by(|a, b| {
        a["keyword_rank"]
            .as_f64()
            .unwrap_or(0.0)
            .total_cmp(&b["keyword_rank"].as_f64().unwrap_or(0.0))
            .then_with(|| a["id"].as_str().cmp(&b["id"].as_str()))
    });
    hits.truncate(limit);
    Ok(hits)
}

pub fn exact_identities(db: &Connection, kind: &str, text: &str) -> Result<Vec<Value>> {
    let key = identity(text);
    let contains_word = key
        .chars()
        .any(|letter| crate::unicode::is_word(letter) && letter != '_');
    let active = "n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)";
    let (records, aliases) = if contains_word {
        let phrase = format!("\"{}\"", key.replace('"', "\"\""));
        (
            rows(
                db,
                &format!(
                    "SELECT n.id,n.version,n.text,n.commit_id FROM node_fts JOIN node_versions n ON n.id=node_fts.id AND n.version=node_fts.version WHERE node_fts MATCH ? AND n.type=? AND {active}"
                ),
                params![phrase, kind],
            )?,
            rows(
                db,
                "SELECT id,text FROM alias_fts WHERE alias_fts MATCH ?",
                [phrase],
            )?,
        )
    } else {
        (
            rows(
                db,
                &format!(
                    "SELECT n.id,n.version,n.text,n.commit_id FROM node_versions n WHERE n.type=? AND {active}"
                ),
                [kind],
            )?,
            rows(db, "SELECT id,text FROM alias_fts", [])?,
        )
    };
    let mut matches = std::collections::BTreeMap::new();
    for record in records {
        if identity(get_str(&record, "text")?) == key {
            matches.insert(get_str(&record,"id")?.to_owned(),json!({"id":record["id"],"version":record["version"],"commit_id":record["commit_id"]}));
        }
    }
    for alias in aliases {
        if identity(get_str(&alias, "text")?) != key {
            continue;
        }
        let n = curation::canonical(db, get_str(&alias, "id")?)?;
        if n["type"] == kind && n["state"] == "active" {
            matches
                .entry(get_str(&n, "id")?.to_owned())
                .or_insert(json!({"id":n["id"],"version":n["version"],"commit_id":n["commit_id"]}));
        }
    }
    Ok(matches.into_values().collect())
}
