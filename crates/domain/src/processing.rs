//! Existing host-runner scheduling, grounding and leased completion protocol.
use crate::util::*;
use crate::{
    Graph,
    consolidation::{self, Consolidator, DiscoveryProvider},
    context,
    extraction::Extractor,
    importance,
    jobs::{Job, Queue, timestamp},
    pipeline_schema, source_text_ranges,
};
use chrono::{DateTime, Duration, Timelike, Utc};
use chrono_tz::Tz;
use memento_core::{Error, Result, WriteTxn};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;

pub const PARKED_ERROR: &str = "historical_parked";
const PROGRESS: &str = "FROM processing_progress p LEFT JOIN historical_parking h ON h.source=p.source AND h.conversation_id=p.conversation_id";
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Compaction {
    pub source: String,
    pub conversation_id: String,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Claim {
    pub agent: String,
    #[serde(rename = "timezone")]
    pub timezone_name: String,
    #[serde(default)]
    pub compaction: Option<Compaction>,
    #[serde(default)]
    pub exclude: Vec<String>,
    #[serde(default)]
    pub consolidate_now: bool,
    #[serde(default)]
    pub kinds: Option<Vec<String>>,
    #[serde(default)]
    pub consolidation_start: Option<String>,
    #[serde(default)]
    pub consolidation_end: Option<String>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Lease {
    pub id: String,
    pub owner: String,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Complete {
    pub id: String,
    pub owner: String,
    pub agent: String,
    pub output: Value,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub usage: Option<Value>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Release {
    pub id: String,
    pub owner: String,
    pub error: String,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Maintenance {
    pub command_id: String,
    pub actor: String,
    pub why: String,
    pub target_ids: Vec<String>,
    #[serde(default)]
    pub origin: Option<String>,
}

#[derive(Clone)]
pub struct Processing {
    pub graph: Graph,
    pub queue: Queue,
    pub extractor: Extractor,
    pub consolidator: Consolidator,
}
impl Processing {
    pub fn new(graph: Graph) -> Self {
        Self {
            queue: Queue::new(graph.clone()),
            extractor: Extractor::new(graph.clone()),
            consolidator: Consolidator::new(graph.clone()),
            graph,
        }
    }
    pub fn with_discovery(mut self, provider: Arc<dyn DiscoveryProvider>) -> Self {
        self.consolidator = self.consolidator.with_discovery(provider);
        self
    }
    pub fn compact(&self, command: Compaction) -> Result<Value> {
        required(&command.source, 256, "Invalid source")?;
        required(&command.conversation_id, 512, "Invalid conversation_id")?;
        self.graph.store.write(|tx|{tx.sql().execute("INSERT INTO processing_progress(source,conversation_id,compact_through) VALUES (?,?,coalesce((SELECT max(seq) FROM episodes WHERE source=? AND conversation_id=?),0)) ON CONFLICT(source,conversation_id) DO UPDATE SET compact_through=max(compact_through,excluded.compact_through)",params![command.source,command.conversation_id,command.source,command.conversation_id])?;Ok(json!({"status":"noted"}))})
    }
    pub fn history_status(&self) -> Result<Value> {
        self.graph.store.read(|tx| historical_status(tx.sql()))
    }
    pub fn park_history(&self) -> Result<Value> {
        self.park_history_at(Utc::now())
    }
    pub fn park_history_at(&self, instant: DateTime<Utc>) -> Result<Value> {
        self.graph.store.write(|tx|{if one(tx.sql(),"SELECT 1 FROM historical_activation",[])?.is_none(){tx.sql().execute("INSERT INTO historical_activation(singleton,activated_at) VALUES (1,?)",[format_time(instant)])?;tx.sql().execute("INSERT INTO historical_parking(source,conversation_id,parked_through,live_seq,live_offset) SELECT p.source,p.conversation_id,p.latest_seq,p.latest_seq,0 FROM processing_progress p WHERE p.latest_seq>p.seq",[])?;tx.sql().execute("UPDATE jobs SET state='failed',error='historical_parked',owner=NULL,lease_until=NULL WHERE kind='extract' AND state IN ('pending','leased') AND json_extract(payload,'$.mode') IS NULL AND json_extract(payload,'$.cursor') IS NULL AND EXISTS (SELECT 1 FROM historical_parking h WHERE h.source=json_extract(jobs.payload,'$.source') AND h.conversation_id=json_extract(jobs.payload,'$.conversation_id') AND json_extract(jobs.payload,'$.next_seq')<=h.parked_through)",[])?;tx.sql().execute("UPDATE jobs SET state='failed',error='historical_parked',owner=NULL,lease_until=NULL WHERE kind='consolidate' AND state IN ('pending','leased')",[])?;tx.sql().execute(&format!("INSERT INTO historical_parked_candidates(candidate_id) SELECT c.id FROM candidates c WHERE {ACTIVE_CANDIDATE}"),[])?;for row in rows(tx.sql(),"SELECT source,conversation_id FROM historical_parking",[])?{tx.touch_conversation(get_str(&row,"source")?,get_str(&row,"conversation_id")?)?;}for row in rows(tx.sql(),"SELECT candidate_id FROM historical_parked_candidates WHERE released_at IS NULL",[])?{tx.touch_target(get_str(&row,"candidate_id")?)?;}
            // Native header5 is never lowered to the older Python parkingheader3.
        }historical_status(tx.sql())})
    }
    pub fn maintain(&self, command: Maintenance) -> Result<Value> {
        if command.target_ids.is_empty()
            || command.target_ids.len() > 48
            || command
                .target_ids
                .iter()
                .any(|id| id.is_empty() || id.chars().count() > 64)
            || command
                .target_ids
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != command.target_ids.len()
        {
            return Err(Error::Validation(
                "target_ids must be 1..48 distinct existing identifiers",
            ));
        }
        required(&command.command_id, 512, "Invalid command identifier")?;
        required(&command.actor, 256, "Invalid actor")?;
        if command.why.trim().is_empty() {
            return Err(Error::Validation("A maintenance reason is required"));
        }
        optional(command.origin.as_deref(), 256, "Invalid maintenance origin")?;
        let mut targets = command.target_ids;
        targets.sort();
        let request = json!({"op":"maintenance","target_ids":targets,"origin":command.origin});
        let ctx = context(command.command_id, command.actor, command.why);
        self.graph.store.command(&ctx,&request,|tx,_commit|{for target in &targets{if !visible_active(tx.sql(),target)?{return Err(Error::Validation("Maintenance requires active visible existing nodes"));}}let serialized=encode_json(&json!(targets))?;if let Some(existing)=one(tx.sql(),"SELECT id,state FROM repair_groups WHERE seed_ids=? AND state='pending' LIMIT 1",[&serialized])?{return Ok(json!({"repair_group_id":existing["id"],"status":existing["state"],"target_ids":targets}));}let group=format!("r_{}",&id()[..24]);let stamp=now();tx.sql().execute("INSERT INTO repair_groups(id,seed_ids,frontier,reviewed_ids,state,origin,created_at,updated_at) VALUES (?,?,?,'[]','pending',?,?,?)",params![group,serialized,serialized,command.origin,stamp,stamp])?;Ok(json!({"repair_group_id":group,"status":"pending","target_ids":targets}))})
    }
    pub fn claim(&self, command: Claim) -> Result<Value> {
        self.claim_at(command, Utc::now())
    }
    pub fn claim_at(&self, command: Claim, instant: DateTime<Utc>) -> Result<Value> {
        required(&command.agent, 64, "Invalid processing agent")?;
        required(&command.timezone_name, 128, "Invalid timezone")?;
        let zone: Tz = command
            .timezone_name
            .parse()
            .map_err(|_| Error::Validation("timezone must be an installed IANA timezone name"))?;
        let window = night_window(
            command.consolidation_start.as_deref(),
            command.consolidation_end.as_deref(),
        )?;
        if let Some(kinds) = &command.kinds {
            if kinds.is_empty()
                || kinds.len() > 2
                || kinds
                    .iter()
                    .any(|kind| kind != "extract" && kind != "consolidate")
                || kinds
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    != kinds.len()
            {
                return Err(Error::Validation(
                    "kinds must contain distinct extract/consolidate values",
                ));
            }
        }
        if let Some(compaction) = command.compaction.clone() {
            self.compact(compaction)?;
        }
        let schedule = || {
            self.graph.store.write(|tx| {
                enqueue_extract(tx, instant)?;
                enqueue_night(
                    tx,
                    instant,
                    zone,
                    &command.timezone_name,
                    window,
                    command.consolidate_now,
                    &command.exclude,
                )
            })
        };
        let night_due = schedule()?;
        let requested = command
            .kinds
            .clone()
            .unwrap_or(vec!["extract".into(), "consolidate".into()]);
        let mut work = vec![];
        for kind in if night_due {
            vec!["extract", "consolidate"]
        } else {
            vec!["extract"]
        } {
            if !requested.iter().any(|requested| requested == kind) {
                continue;
            }
            loop {
                let Some(job) = self.queue.claim(&[kind.into()], &command.exclude)? else {
                    break;
                };
                let discovery = if kind == "consolidate" {
                    self.consolidator.prepare_discovery(&job)?
                } else {
                    None
                };
                let prior = self.graph.store.read(|tx| {
                    one(
                        tx.sql(),
                        "SELECT result FROM graph_commits WHERE command_id=?",
                        [format!("{kind}/{}", job.id)],
                    )
                })?;
                if let Some(prior) = prior {
                    self.queue
                        .complete(&job, &decode_json(get_str(&prior, "result")?)?)?;
                    schedule()?;
                    continue;
                }
                let previous = self
                    .graph
                    .store
                    .read(|tx| one(tx.sql(), "SELECT context FROM jobs WHERE id=?", [&job.id]))?;
                let rejection = previous
                    .and_then(|row| row["context"].as_str().map(str::to_owned))
                    .map(|context| decode_json(&context))
                    .transpose()?
                    .and_then(|context| context.get("previous_rejection").cloned());
                let mut prepared = if kind == "extract" {
                    self.extractor.prepare(&job)?
                } else {
                    self.consolidator.prepare(&job, discovery.as_ref())?
                };
                if let Some(rejection) = rejection {
                    prepared["previous_rejection"] = rejection;
                }
                self.graph.store.write(|tx| {
                    job.check_in(tx.sql())?;
                    tx.sql().execute(
                        "UPDATE jobs SET context=? WHERE id=? AND owner=?",
                        params![encode_json(&prepared)?, job.id, job.owner],
                    )?;
                    Ok(())
                })?;
                let input = if kind == "extract" {
                    Extractor::model_input(&prepared)
                } else {
                    Consolidator::model_input(&prepared)
                };
                let output = if kind == "extract"
                    && prepared["windows"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .all(|window| window["text"].as_str().is_none_or(str::is_empty))
                {
                    Some(
                        json!({"windows":input["windows"].as_array().unwrap().iter().map(|window|json!({"id":window["id"],"candidates":[]})).collect::<Vec<_>>()}),
                    )
                } else if kind == "consolidate"
                    && prepared["candidates"].as_array().unwrap().is_empty()
                    && prepared["maintenance_ids"].as_array().unwrap().is_empty()
                {
                    Some(json!({"actions":[]}))
                } else {
                    None
                };
                if let Some(output) = output {
                    let result = if kind == "extract" {
                        self.extractor
                            .apply(&job, &prepared, &output, &command.agent, None, None)
                    } else {
                        self.consolidator.apply(
                            &job,
                            &prepared,
                            &output,
                            &command.agent,
                            None,
                            None,
                        )
                    }
                    .map_err(|error| error.error)?;
                    self.queue.complete(&job, &result)?;
                    schedule()?;
                    continue;
                }
                let instruction = if kind == "extract" {
                    format!(
                        "{}\nReturn windows with exactly the supplied ids and candidates for each. Entity keys are local to each window.",
                        pipeline_schema::EXTRACTION_INSTRUCTION
                    )
                } else {
                    pipeline_schema::CONSOLIDATION_INSTRUCTION.into()
                };
                let schema = if kind == "extract" {
                    pipeline_schema::EXTRACTION.clone()
                } else {
                    pipeline_schema::CONSOLIDATION.clone()
                };
                work.push(json!({"id":job.id,"owner":job.owner,"kind":kind,"instruction":instruction,"input":input,"schema":schema}));
                break;
            }
        }
        let backlog=self.graph.store.read(|tx|Ok(json!({"pending_extract_conversations":pending_conversations(tx.sql(),None)?,"pending_extract_jobs":tx.sql().query_row("SELECT count(*) FROM jobs WHERE state='pending' AND kind='extract'",[],|row|row.get::<_,i64>(0))?,"leased_extract_jobs":tx.sql().query_row("SELECT count(*) FROM jobs WHERE state='leased' AND kind='extract'",[],|row|row.get::<_,i64>(0))?})))?;
        Ok(json!({"lease_seconds":self.queue.lease_seconds,"work":work,"backlog":backlog}))
    }
    fn leased(&self, id: &str, owner: &str) -> Result<(Job, Value)> {
        required(id, 128, "Invalid work identifier")?;
        required(owner, 128, "Invalid lease owner")?;
        self.graph.store.read(|tx| {
            let row = one(tx.sql(), "SELECT * FROM jobs WHERE id=?", [id])?
                .ok_or(Error::Validation("Unknown model work"))?;
            let kind = get_str(&row, "kind")?;
            if !["extract", "consolidate"].contains(&kind) {
                return Err(Error::Validation("Unknown model work"));
            }
            let job = Job {
                queue: self.queue.clone(),
                id: id.into(),
                kind: kind.into(),
                payload: decode_json(get_str(&row, "payload")?)?,
                owner: owner.into(),
                attempts: get_i64(&row, "attempts")?,
            };
            job.check_in(tx.sql())?;
            let context = decode_json(get_str(&row, "context")?)?;
            Ok((job, context))
        })
    }
    pub fn renew(&self, command: Lease) -> Result<Value> {
        let job = match self.leased(&command.id, &command.owner) {
            Ok((job, _)) => job,
            Err(Error::Conflict(_)) => return Ok(json!({"renewed":false})),
            Err(error) => return Err(error),
        };
        Ok(json!({"renewed":self.queue.renew(&job)?}))
    }
    pub fn complete(&self, command: Complete) -> Result<Value> {
        required(&command.agent, 64, "Invalid processing agent")?;
        if !command.output.is_object()
            || command
                .usage
                .as_ref()
                .is_some_and(|usage| !usage.is_object())
        {
            return Err(Error::Validation(
                "Model output and usage must be JSON objects",
            ));
        }
        let (job, context) = self.leased(&command.id, &command.owner)?;
        let result = if job.kind == "extract" {
            self.extractor.apply(
                &job,
                &context,
                &command.output,
                &command.agent,
                command.model.as_deref(),
                command.usage.as_ref(),
            )
        } else {
            self.consolidator.apply(
                &job,
                &context,
                &command.output,
                &command.agent,
                command.model.as_deref(),
                command.usage.as_ref(),
            )
        };
        match result {
            Ok(result) => {
                self.queue.complete(&job, &result)?;
                Ok(result)
            }
            Err(error) => {
                if let Error::Validation(code) = &error.error {
                    if pipeline_schema::VALIDATION_CODES
                        .binary_search_by(|value| value.as_str().cmp(code))
                        .is_ok()
                    {
                        let feedback = json!({"code":code,"attempt":job.attempts,"references":error.references,"note":"Server validation of this job's previous output. Quoted text is failed model output, not evidence."});
                        let mut updated = context.clone();
                        updated["previous_rejection"] = feedback;
                        if let Err(failure)=self.graph.store.write(|tx|{tx.sql().execute("UPDATE jobs SET context=? WHERE id=? AND owner=? AND state='leased' AND lease_until>? AND kind IN ('extract','consolidate')",params![encode_json(&updated)?,job.id,job.owner,timestamp()])?;Ok(())}) {
                            eprintln!("Rejection feedback not stored ({})",crate::jobs::error_category(&failure));
                        }
                    }
                }
                Err(error.error)
            }
        }
    }
    pub fn release(&self, command: Release) -> Result<Value> {
        required(&command.id, 128, "Invalid work identifier")?;
        required(&command.owner, 128, "Invalid lease owner")?;
        if !["cli_failed", "invalid_output", "service_rejected"].contains(&command.error.as_str()) {
            return Err(Error::Validation("Invalid work release category"));
        }
        self.graph.store.write(|tx|{let changed=tx.sql().execute("UPDATE jobs SET state='pending',available_at=?,error=?,owner=NULL,lease_until=NULL WHERE id=? AND owner=? AND state='leased' AND lease_until>? AND kind IN ('extract','consolidate')",params![timestamp()+30.0,command.error,command.id,command.owner,timestamp()])?;Ok(json!({"released":changed!=0}))})
    }
}

pub fn night_window(start: Option<&str>, end: Option<&str>) -> Result<(u32, u32)> {
    let (start, end) = match (start, end) {
        (None, None) => ("02:00", "07:00"),
        (Some(start), Some(end)) => (start, end),
        _ => {
            return Err(Error::Validation(
                "consolidation_start and consolidation_end must be given together",
            ));
        }
    };
    let parse = |value: &str| -> Result<u32> {
        let bytes = value.as_bytes();
        if bytes.len() != 5
            || bytes[2] != b':'
            || ![bytes[0], bytes[1], bytes[3], bytes[4]]
                .iter()
                .all(u8::is_ascii_digit)
        {
            return Err(Error::Validation(
                "consolidation window endpoints must be HH:MM local time",
            ));
        }
        let hour = (bytes[0] - b'0') as u32 * 10 + (bytes[1] - b'0') as u32;
        let minute = (bytes[3] - b'0') as u32 * 10 + (bytes[4] - b'0') as u32;
        if hour > 23 || minute > 59 {
            return Err(Error::Validation(
                "consolidation window endpoints must be HH:MM local time",
            ));
        }
        Ok(hour * 60 + minute)
    };
    let start = parse(start)?;
    let end = parse(end)?;
    if start == end {
        return Err(Error::Validation(
            "consolidation window endpoints must differ",
        ));
    }
    Ok((start, end))
}
pub fn pending_conversations(db: &Connection, source: Option<&str>) -> Result<i64> {
    Ok(db.query_row(&format!("SELECT count(*) {PROGRESS} WHERE p.latest_seq>p.seq AND p.latest_seq>coalesce(h.live_seq,p.seq) AND (?1 IS NULL OR p.source=?1)"),[source],|row|row.get(0))?)
}
pub fn historical_status(db: &Connection) -> Result<Value> {
    let activation = one(db, "SELECT activated_at FROM historical_activation", [])?;
    let parking=one(db,"SELECT count(*) AS parked_conversations,coalesce(sum(p.latest_seq>h.live_seq),0) AS live_pending_conversations FROM historical_parking h JOIN processing_progress p ON p.source=h.source AND p.conversation_id=h.conversation_id",[])?.unwrap();
    let candidates=one(db,"SELECT coalesce(sum(released_at IS NULL),0) AS parked_candidates,coalesce(sum(released_at IS NOT NULL),0) AS released_candidates FROM historical_parked_candidates",[])?.unwrap();
    Ok(
        json!({"active":activation.is_some(),"activated_at":activation.as_ref().map(|row|row["activated_at"].clone()),"header":db.query_row("PRAGMA user_version",[],|row|row.get::<_,i64>(0))?,"parked_conversations":parking["parked_conversations"],"live_pending_conversations":parking["live_pending_conversations"],"parked_candidates":candidates["parked_candidates"],"released_candidates":candidates["released_candidates"]}),
    )
}
fn visible_active(db: &Connection, target: &str) -> Result<bool> {
    Ok(one(db,"SELECT 1 FROM node_versions n WHERE n.id=? AND n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) AND (NOT EXISTS (SELECT 1 FROM captured_media cm WHERE cm.node_id=n.id) OR EXISTS (SELECT 1 FROM captured_media cm JOIN captured_media_refs r ON r.sha256=cm.sha256 JOIN episodes e ON e.id=r.episode_id WHERE cm.node_id=n.id AND NOT EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id) AND NOT EXISTS (SELECT 1 FROM media_files m JOIN forgotten_targets f ON f.target=m.node_id WHERE m.sha256=cm.sha256)))",[target])?.is_some())
}
fn ascii_json(value: &Value) -> Result<String> {
    let canonical = encode_json(value)?;
    let mut out = String::new();
    for letter in canonical.chars() {
        if letter.is_ascii() {
            out.push(letter);
        } else {
            let mut buffer = [0; 2];
            for code in letter.encode_utf16(&mut buffer) {
                out.push_str(&format!("\\u{code:04x}"));
            }
        }
    }
    Ok(out)
}
fn enqueue_extract(tx: &WriteTxn, instant: DateTime<Utc>) -> Result<()> {
    let last = one(
        tx.sql(),
        "SELECT json_extract(payload,'$.order') AS admission_order FROM jobs WHERE kind='extract' ORDER BY rowid DESC LIMIT 1",
        [],
    )?;
    let order = if last
        .as_ref()
        .is_none_or(|row| row["admission_order"] == "oldest")
    {
        "newest"
    } else {
        "oldest"
    };
    let ordering = if order == "newest" {
        "p.latest_seq DESC"
    } else {
        "(SELECT e.seq FROM episodes e WHERE e.source=p.source AND e.conversation_id=p.conversation_id AND e.seq>coalesce(h.live_seq,p.seq) ORDER BY e.seq LIMIT 1)"
    };
    let groups = rows(
        tx.sql(),
        &format!(
            "SELECT p.source,p.conversation_id,p.latest_seq,p.latest_at,p.compact_through,coalesce(h.live_seq,p.seq) AS cursor,coalesce(h.live_offset,p.offset) AS offset,(h.source IS NOT NULL) AS live {PROGRESS} WHERE p.latest_seq>p.seq AND p.latest_seq>coalesce(h.live_seq,p.seq) ORDER BY {ordering}"
        ),
        [],
    )?;
    for group in groups {
        let source = get_str(&group, "source")?;
        let conversation = get_str(&group, "conversation_id")?;
        if one(tx.sql(),"SELECT 1 FROM jobs WHERE kind='extract' AND state IN ('pending','leased') AND json_extract(payload,'$.source')=? AND json_extract(payload,'$.conversation_id')=? AND json_extract(payload,'$.mode') IS NULL LIMIT 1",params![source,conversation])?.is_some(){continue;}
        let idle = (instant - parse_time(get_str(&group, "latest_at")?)?).num_seconds() >= 1800;
        let through = if idle {
            get_i64(&group, "latest_seq")?
        } else {
            get_i64(&group, "compact_through")?
        };
        let cursor = get_i64(&group, "cursor")?;
        if through < cursor || through == 0 {
            continue;
        }
        let selected = rows(
            tx.sql(),
            "SELECT id,seq,text,kind,metadata FROM episodes e WHERE source=? AND conversation_id=? AND seq<=? AND seq>? AND coalesce(json_extract(metadata,'$.extraction_mode'),'message')!='skip' AND NOT legacy_native_context(e.source,e.kind,e.text,e.metadata) AND NOT EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id) ORDER BY seq LIMIT 65",
            params![source, conversation, through, cursor],
        )?;
        let offset = get_i64(&group, "offset")? as usize;
        let partial = if offset != 0 {
            one(tx.sql(),"SELECT seq FROM episodes WHERE source=? AND conversation_id=? AND seq>? ORDER BY seq LIMIT 1",params![source,conversation,cursor])?.and_then(|row|row["seq"].as_i64())
        } else {
            None
        };
        let (mut windows, mut characters) = (vec![], 0);
        let (mut next_seq, mut next_offset) = (cursor, 0);
        let mut stopped = false;
        for row in selected {
            let row_seq = get_i64(&row, "seq")?;
            let offset = if partial == Some(row_seq) { offset } else { 0 };
            let metadata = decode_json(get_str(&row, "metadata")?)?;
            let tool = metadata["extraction_mode"] == "tool_output" || row["kind"] == "tool";
            for (left, right) in source_text_ranges(get_str(&row, "text")?) {
                if right < offset || right == offset && right != 0 {
                    continue;
                }
                let mut start = left.max(offset);
                loop {
                    let end = if tool {
                        right
                    } else {
                        right.min(start + 12000)
                    };
                    let projected = if tool {
                        (end - start).min(2000)
                    } else {
                        end - start
                    };
                    if windows.len() == 64 || characters + projected > 48000 {
                        next_seq=tx.sql().query_row("SELECT coalesce(max(seq),0) FROM episodes WHERE source=? AND conversation_id=? AND seq<?",params![source,conversation,row_seq],|row|row.get(0))?;
                        next_offset = start;
                        stopped = true;
                        break;
                    }
                    let episode = get_str(&row, "id")?;
                    windows.push(json!({"id":format!("{episode}:{start}"),"episode_id":episode,"start":start,"end":end}));
                    characters += projected;
                    if end == right {
                        break;
                    }
                    start = left.max(end.saturating_sub(1000));
                }
                if stopped {
                    break;
                }
            }
            if stopped {
                break;
            }
            next_seq = row_seq;
            next_offset = 0;
        }
        if !stopped {
            next_seq = through;
            next_offset = 0;
        }
        let live = group["live"].as_i64().unwrap_or(0) != 0;
        if windows.is_empty() {
            tx.sql().execute(if live{"UPDATE historical_parking SET live_seq=?,live_offset=? WHERE source=? AND conversation_id=?"}else{"UPDATE processing_progress SET seq=?,offset=? WHERE source=? AND conversation_id=?"},params![next_seq,next_offset as i64,source,conversation])?;
            continue;
        }
        let mut payload = json!({"version":crate::extraction::VERSION,"source":source,"conversation_id":conversation,"windows":windows,"next_seq":next_seq,"next_offset":next_offset});
        if live {
            payload["cursor"] = json!("live");
        }
        let key = format!(
            "extract/{}",
            memento_core::raw::sha256(ascii_json(&payload)?.as_bytes())
        );
        if one(tx.sql(), "SELECT 1 FROM jobs WHERE job_key=?", [&key])?.is_some() {
            return Ok(());
        }
        payload["order"] = json!(order);
        Queue::insert(tx, &key, "extract", &payload)?;
        return Ok(());
    }
    Ok(())
}
fn enqueue_repair(tx: &WriteTxn) -> Result<bool> {
    for group in rows(
        tx.sql(),
        "SELECT * FROM repair_groups WHERE state='pending' ORDER BY created_at,id",
        [],
    )? {
        let group_id = get_str(&group, "id")?;
        if let Some(active) = group["active_job"].as_str() {
            if one(tx.sql(), "SELECT state FROM jobs WHERE id=?", [active])?
                .is_some_and(|job| matches!(job["state"].as_str(), Some("pending" | "leased")))
            {
                continue;
            }
            tx.sql().execute("UPDATE repair_groups SET active_job=NULL,generation=generation+1,available_at=?,updated_at=? WHERE id=?",params![timestamp()+900.0,now(),group_id])?;
            continue;
        }
        if group["available_at"].as_f64().unwrap_or(0.0) > timestamp() {
            continue;
        }
        let frontier = decode_json(get_str(&group, "frontier")?)?;
        let frontier = consolidation::strings(&frontier)?;
        if frontier.is_empty() {
            tx.sql().execute(
                "UPDATE repair_groups SET state='done' WHERE id=?",
                [group_id],
            )?;
            continue;
        }
        let reviewed = decode_json(get_str(&group, "reviewed_ids")?)?;
        let reviewed = consolidation::strings(&reviewed)?;
        let limit = if reviewed.contains(&frontier[0]) {
            48
        } else {
            8
        };
        let payload = json!({"version":consolidation::VERSION,"night":null,"candidate_ids":[],"next_cursor":0,"repair_group_id":group_id,"maintenance_ids":frontier.into_iter().take(limit).collect::<Vec<_>>(),"reviewed_ids":reviewed});
        let job = Queue::insert(
            tx,
            &format!(
                "consolidate/repair/{group_id}/{}",
                get_i64(&group, "generation")?
            ),
            "consolidate",
            &payload,
        )?;
        tx.sql().execute(
            "UPDATE repair_groups SET active_job=? WHERE id=?",
            params![job, group_id],
        )?;
        return Ok(true);
    }
    Ok(false)
}
fn enqueue_night(
    tx: &WriteTxn,
    instant: DateTime<Utc>,
    zone: Tz,
    zone_name: &str,
    window: (u32, u32),
    force: bool,
    exclude: &[String],
) -> Result<bool> {
    let local = instant.with_timezone(&zone);
    let minute = local.hour() * 60 + local.minute();
    let (start, end) = window;
    let (within, anchor) = if start < end {
        (start <= minute && minute < end, local.date_naive())
    } else {
        (
            minute >= start || minute < end,
            if minute >= start {
                local.date_naive()
            } else {
                local.date_naive() - Duration::days(1)
            },
        )
    };
    let night = format!("{zone_name}/{anchor}");
    tx.sql().execute("UPDATE jobs SET state='failed',error='night_window_closed',owner=NULL,lease_until=NULL WHERE kind='consolidate' AND (state='pending' OR (state='leased' AND lease_until<=?)) AND json_extract(payload,'$.night') IS NOT NULL AND (json_extract(payload,'$.night')<>? OR ?=0)",params![instant.timestamp_micros() as f64/1e6,night,i64::from(within)])?;
    if !within && !force {
        return Ok(false);
    }
    if one(
        tx.sql(),
        "SELECT 1 FROM jobs WHERE kind='consolidate' AND state='leased' AND lease_until>? LIMIT 1",
        [timestamp()],
    )?
    .is_some()
    {
        return Ok(true);
    }
    if one(tx.sql(),"SELECT 1 FROM jobs WHERE kind='consolidate' AND state IN ('pending','leased') AND id NOT IN (SELECT value FROM json_each(?)) LIMIT 1",[encode_json(&json!(exclude))?])?.is_some(){return Ok(true);}
    let reserved = one(
        tx.sql(),
        "SELECT 1 FROM jobs WHERE kind='consolidate' AND state IN ('pending','leased') LIMIT 1",
        [],
    )?
    .is_some();
    let frozen = one(
        tx.sql(),
        "SELECT * FROM consolidation_nights WHERE id=?",
        [&night],
    )?;
    let previous = one(
        tx.sql(),
        "SELECT payload FROM jobs WHERE kind='consolidate' ORDER BY rowid DESC LIMIT 1",
        [],
    )?;
    let prefer = previous
        .map(|row| decode_json(row["payload"].as_str().unwrap()))
        .transpose()?
        .is_none_or(|payload| payload["repair_group_id"].as_str().is_none());
    if prefer && enqueue_repair(tx)? {
        return Ok(true);
    }
    let (night, cursor, through) = if force {
        (
            None,
            0,
            tx.sql()
                .query_row("SELECT coalesce(max(rowid),0) FROM candidates", [], |row| {
                    row.get::<_, i64>(0)
                })?,
        )
    } else if let Some(frozen) = frozen {
        if !frozen["finished_at"].is_null() {
            return enqueue_repair(tx);
        }
        (
            Some(night),
            get_i64(&frozen, "cursor")?,
            get_i64(&frozen, "through_rowid")?,
        )
    } else {
        let through =
            tx.sql()
                .query_row("SELECT coalesce(max(rowid),0) FROM candidates", [], |row| {
                    row.get::<_, i64>(0)
                })?;
        if through == 0 {
            return enqueue_repair(tx);
        }
        tx.sql().execute(
            "INSERT INTO consolidation_nights(id,through_rowid) VALUES (?,?)",
            params![night, through],
        )?;
        (Some(night), 0, through)
    };
    let candidates = consolidation::pending_in(tx.sql(), 16, cursor, through, true)?;
    if candidates.is_empty() {
        if enqueue_repair(tx)? {
            return Ok(true);
        }
        if reserved {
            return Ok(true);
        }
        if let Some(night) = night {
            tx.sql().execute(
                "UPDATE consolidation_nights SET finished_at=? WHERE id=?",
                params![format_time(instant), night],
            )?;
        }
        importance::refresh(tx, Some(instant))?;
        return Ok(false);
    }
    let next = candidates
        .iter()
        .filter_map(|candidate| candidate["position"].as_i64())
        .max()
        .unwrap();
    let payload = json!({"version":consolidation::VERSION,"night":night,"candidate_ids":candidates.iter().map(|candidate|candidate["id"].clone()).collect::<Vec<_>>(),"next_cursor":next});
    let key = if force {
        format!("consolidate/manual/{}", id())
    } else {
        format!(
            "consolidate/{}",
            memento_core::raw::sha256(ascii_json(&payload)?.as_bytes())
        )
    };
    Queue::insert(tx, &key, "consolidate", &payload)?;
    Ok(true)
}
