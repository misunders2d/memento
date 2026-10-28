//! Bounded source-backed stewardship with one prepared graph authority cut.
use crate::util::*;
use crate::{Graph, jobs::Job, pipeline_schema::WorkResult};
use memento_core::{Error, Result};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::sync::Arc;
#[path = "consolidation_apply.rs"]
mod apply;
#[path = "consolidation_context.rs"]
mod context_builder;
pub const VERSION: &str = "consolidate-v8";
pub const ELIGIBLE: &str = "EXISTS (SELECT 1 FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id WHERE g.target=c.id AND g.version=1 AND NOT (CASE WHEN e.kind='developer' THEN legacy_native_context(e.source,e.kind,NULL,NULL) WHEN e.kind='user' AND e.source GLOB 'native:*:codex' THEN legacy_native_context(e.source,e.kind,e.text,e.metadata) ELSE 0 END))";
pub const UNPARKED: &str = "NOT EXISTS (SELECT 1 FROM historical_parked_candidates h WHERE h.candidate_id=c.id AND h.released_at IS NULL)";
pub const AWAITING: &str = "(d.next_review_at>?3 OR (d.decision='review' AND NOT EXISTS (SELECT 1 FROM review_proposals p WHERE p.id=d.target_id AND NOT EXISTS (SELECT 1 FROM review_decisions r WHERE r.proposal_id=p.id) AND ((json_extract(p.command,'$.identity.status')='exact_matches_not_supplied' AND EXISTS (SELECT 1 FROM json_each(p.base_versions) j WHERE j.value<>(SELECT max(n.version) FROM node_versions n WHERE n.id=j.key))) OR (json_extract(p.command,'$.op')='remember' AND json_extract(p.command,'$.type')='person' AND json_type(p.command,'$.identity') IS NULL)))))";

/// Advisory ANN lookup runs outside every writer transaction. Authority failures
/// propagate; the provider itself labels an unavailable derived search pending.
pub trait DiscoveryProvider: Send + Sync {
    fn discover(&self, candidate_ids: &[String]) -> Result<Value>;
}
#[derive(Clone)]
pub struct Consolidator {
    pub graph: Graph,
    pub discovery: Option<Arc<dyn DiscoveryProvider>>,
    pub search_configured: bool,
}
impl Consolidator {
    pub fn new(graph: Graph) -> Self {
        Self {
            graph,
            discovery: None,
            search_configured: false,
        }
    }
    pub fn with_discovery(mut self, provider: Arc<dyn DiscoveryProvider>) -> Self {
        self.discovery = Some(provider);
        self.search_configured = true;
        self
    }
    pub fn prepare_discovery(&self, job: &Job) -> Result<Option<Value>> {
        if let Some(provider) = &self.discovery {
            let ids = strings(&job.payload["candidate_ids"])?;
            return Ok(Some(provider.discover(&ids)?));
        }
        Ok(self
            .search_configured
            .then(|| json!({"candidates":{},"pending":true})))
    }
    pub fn pending(
        &self,
        limit: usize,
        after: i64,
        through: i64,
        unreserved: bool,
    ) -> Result<Vec<Value>> {
        self.graph
            .store
            .read(|tx| pending_in(tx.sql(), limit, after, through, unreserved))
    }
    pub fn prepare(&self, job: &Job, discovery: Option<&Value>) -> Result<Value> {
        self.graph.store.read(|tx| {
            context_builder::prepare(
                tx.sql(),
                tx.data_dir(),
                job,
                discovery,
                self.search_configured,
            )
        })
    }
    pub fn model_input(context: &Value) -> Value {
        Value::Object(
            context
                .as_object()
                .into_iter()
                .flatten()
                .filter(|(key, _)| !key.starts_with('_'))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        )
    }
    pub fn apply(
        &self,
        job: &Job,
        context: &Value,
        output: &Value,
        agent: &str,
        model: Option<&str>,
        usage: Option<&Value>,
    ) -> WorkResult<Value> {
        apply::apply(self, job, context, output, agent, model, usage)
    }
}
pub fn pending_in(
    db: &Connection,
    limit: usize,
    after: i64,
    through: i64,
    unreserved: bool,
) -> Result<Vec<Value>> {
    rows(
        db,
        &format!(
            "SELECT c.*,c.rowid AS position,r.recorded_at AS extracted_at,(SELECT d.id FROM candidate_decisions d WHERE d.candidate_id=c.id ORDER BY d.rowid DESC LIMIT 1) AS decision_id FROM candidates c JOIN extraction_runs r ON r.id=c.run_id WHERE {ACTIVE_CANDIDATE} AND {ELIGIBLE} AND {UNPARKED} AND c.rowid>?1 AND c.rowid<=?2 AND (?5=0 OR NOT EXISTS (SELECT 1 FROM jobs j,json_each(j.payload,'$.candidate_ids') held WHERE j.kind='consolidate' AND j.state IN ('pending','leased') AND held.value=c.id)) AND NOT EXISTS (SELECT 1 FROM candidate_decisions d WHERE d.candidate_id=c.id AND d.rowid=(SELECT max(x.rowid) FROM candidate_decisions x WHERE x.candidate_id=c.id) AND {AWAITING}) ORDER BY c.rowid LIMIT ?4"
        ),
        params![
            after,
            through,
            crate::jobs::timestamp(),
            limit as i64,
            i64::from(unreserved)
        ],
    )
}
pub fn ineligible(db: &Connection, ids: &[String]) -> Result<Vec<String>> {
    if ids.is_empty() {
        return Ok(vec![]);
    }
    Ok(rows(db,&format!("SELECT c.id FROM candidates c WHERE c.id IN (SELECT value FROM json_each(?)) AND NOT ({ELIGIBLE} AND {UNPARKED})"),[encode_json(&json!(ids))?])?.into_iter().map(|row|row["id"].as_str().unwrap().to_owned()).collect())
}
pub(super) fn strings(value: &Value) -> Result<Vec<String>> {
    value
        .as_array()
        .ok_or(Error::Validation("Invalid identifier list"))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or(Error::Validation("Invalid identifier"))
        })
        .collect()
}
pub(super) fn unique(values: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    values
        .into_iter()
        .filter(|value| seen.insert(value.clone()))
        .collect()
}
pub fn native_trace(episode: &Value) -> Option<&'static str> {
    let source = episode["source"].as_str()?;
    let parts = source.split(':').collect::<Vec<_>>();
    if parts.len() != 3
        || parts[0] != "native"
        || parts[1].is_empty()
        || !["pi", "omp", "claude", "codex", "agy"].contains(&parts[2])
    {
        return None;
    }
    let metadata = &episode["metadata"];
    if metadata["native_id"].as_str().is_none_or(str::is_empty)
        || metadata.get("native_context_kinds").is_some()
        || metadata.get("content_item_kinds").is_some()
    {
        return None;
    }
    if episode["kind"] == "tool"
        && metadata["extraction_mode"] == "tool_output"
        && metadata["evidence_eligible"] != false
    {
        return Some("result");
    }
    if episode["kind"] == "assistant" && metadata["extraction_mode"] == "message" {
        return Some(if metadata["evidence_eligible"] == false {
            "attempt"
        } else {
            "assistant"
        });
    }
    None
}
pub(super) fn word_terms(text: &str, minimum: usize, limit: usize) -> Vec<String> {
    let folded = crate::unicode::casefold(text);
    let mut words = vec![];
    let mut current = String::new();
    for letter in folded.chars().chain(std::iter::once(' ')) {
        if crate::unicode::is_word(letter) {
            current.push(letter);
        } else if !current.is_empty() {
            if current.chars().count() >= minimum && !words.contains(&current) {
                words.push(current.clone());
                if words.len() == limit {
                    break;
                }
            }
            current.clear();
        }
    }
    words
}
pub(super) fn continuation(db: &Connection, target: &str) -> Result<Option<Vec<String>>> {
    let mut chain = vec![target.to_owned()];
    for _ in 0..64 {
        let n = node(db, chain.last().unwrap())?;
        if n["state"] == "active" {
            return Ok(Some(chain));
        }
        let Some(next) = n["superseded_by"].as_str() else {
            return Ok(None);
        };
        let proven = if n["state"] == "refined" {
            one(db,"SELECT 1 FROM edge_versions WHERE src=? AND rel='supersedes' AND dst=? AND version=1",params![next,n["id"].as_str()])?.is_some()
        } else if n["state"] == "merged" {
            one(
                db,
                "SELECT 1 FROM entity_merges WHERE source=? AND target=?",
                params![n["id"].as_str(), next],
            )?
            .is_some()
        } else {
            return Ok(None);
        };
        if !proven || chain.iter().any(|id| id == next) {
            return Ok(None);
        }
        chain.push(next.into());
    }
    Ok(None)
}
