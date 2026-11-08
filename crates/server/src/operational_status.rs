//! Observational counts only. Caller supplies live indexing/Drive observations;
//! no scheduling, model/ANN work, transfer, health writer probe or mutation here.
use crate::query_rows::{ACTIVE_CANDIDATE, one, rows};
use memento_core::{Error, Result, Store};
use memento_domain::{
    consolidation::{AWAITING, ELIGIBLE},
    processing::{EXTRACTION_INPUT, admissible_through, historical_status, pending_conversations},
};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Instant};
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct StatusQuery {
    pub source: Option<String>,
}
#[derive(Clone, Serialize)]
pub struct DriveStatus {
    pub connected: bool,
    pub reauthorization_required: bool,
    pub archive_failure_count: Option<u64>,
    pub archive_error: Option<String>,
}
impl Default for DriveStatus {
    fn default() -> Self {
        Self {
            connected: false,
            reauthorization_required: false,
            archive_failure_count: None,
            archive_error: None,
        }
    }
}
pub struct RuntimeStatus {
    pub indexing: Value,
    pub drive: DriveStatus,
}
impl RuntimeStatus {
    pub fn disabled() -> Self {
        Self {
            indexing: json!({"available":false,"reason":"embedding_runtime_disabled"}),
            drive: DriveStatus::default(),
        }
    }
}
fn conversation_count(db: &Connection, source: Option<&str>) -> Result<i64> {
    let sources = if source.is_some() {
        "VALUES (?1)"
    } else {
        "SELECT min(source) FROM episodes UNION ALL SELECT (SELECT min(e.source) FROM episodes e WHERE e.source>s.source) FROM sources s WHERE s.source IS NOT NULL"
    };
    let sql = format!(
        "WITH RECURSIVE sources(source) AS ({sources}),conversations(source,conversation_id) AS (SELECT s.source,(SELECT min(e.conversation_id) FROM episodes e WHERE e.source=s.source) FROM sources s WHERE s.source IS NOT NULL UNION ALL SELECT c.source,(SELECT min(e.conversation_id) FROM episodes e WHERE e.source=c.source AND e.conversation_id>c.conversation_id) FROM conversations c WHERE c.conversation_id IS NOT NULL) SELECT count(*) FROM conversations WHERE conversation_id IS NOT NULL"
    );
    Ok(if source.is_some() {
        db.query_row(&sql, [source], |r| r.get(0))?
    } else {
        db.query_row(&sql, [], |r| r.get(0))?
    })
}
// Source filters probe from each job/file's own rows; CROSS JOIN pins that
// order so the planner never scans every episode of the source per row.
const CONSOLIDATE_ORIGIN: &str = "EXISTS(SELECT 1 FROM json_each(j.payload,'$.candidate_ids') c CROSS JOIN graph_evidence g CROSS JOIN episodes e WHERE g.target=c.value AND e.id=g.episode_id AND e.source=?2)";
const CAPTURED_MEDIA: &str = "SELECT cm.state,j.state AS job_state,count(*) AS count FROM captured_media cm LEFT JOIN jobs j ON j.id=cm.job_id WHERE (?1 IS NULL OR EXISTS(SELECT 1 FROM captured_media_refs r CROSS JOIN episodes e WHERE r.sha256=cm.sha256 AND e.id=r.episode_id AND e.source=?1)) GROUP BY cm.state,j.state";
fn jobs(db: &Connection, kind: &str, source: Option<&str>) -> Result<Value> {
    let origin = match kind {
        "extract" => "json_extract(j.payload,'$.source')=?2",
        "consolidate" => CONSOLIDATE_ORIGIN,
        "media" => {
            "EXISTS(SELECT 1 FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id WHERE g.target=j.id AND e.source=?2)"
        }
        _ => return Err(Error::Validation("Invalid status work kind")),
    };
    let records = rows(
        db,
        &format!(
            "SELECT state,error,count(*) AS count FROM jobs j WHERE kind=?1 AND (?2 IS NULL OR {origin}) GROUP BY state,error"
        ),
        params![kind, source],
    )?;
    let mut result = json!({"available":true,"pending_jobs":0,"leased_jobs":0,"completed_jobs":0,"failed_jobs":0,"retrying_jobs":0,"parked_jobs":0,"rebatched_jobs":0});
    let mut errors = BTreeMap::<(String, String), i64>::new();
    for row in records {
        let state = row["state"]
            .as_str()
            .ok_or(Error::Unavailable("Invalid stored job state"))?;
        let count = row["count"].as_i64().unwrap();
        if state == "failed" && row["error"] == "historical_parked" {
            result["parked_jobs"] = json!(result["parked_jobs"].as_i64().unwrap() + count);
            continue;
        }
        if state == "failed" && row["error"] == "rebatch_requested" {
            result["rebatched_jobs"] = json!(result["rebatched_jobs"].as_i64().unwrap() + count);
            continue;
        }
        let field = match state {
            "pending" => "pending_jobs",
            "leased" => "leased_jobs",
            "done" => "completed_jobs",
            "failed" => "failed_jobs",
            _ => return Err(Error::Unavailable("Invalid stored job state")),
        };
        result[field] = json!(result[field].as_i64().unwrap() + count);
        if state == "pending" && !row["error"].is_null() {
            result["retrying_jobs"] = json!(result["retrying_jobs"].as_i64().unwrap() + count);
        }
        if ["pending", "failed"].contains(&state) && !row["error"].is_null() {
            let error = row["error"].as_str().unwrap_or("");
            let safe = if [
                "cli_failed",
                "invalid_output",
                "service_rejected",
                "night_window_closed",
                "ValueError",
                "ConflictError",
                "OSError",
                "FileNotFoundError",
                "PermissionError",
                "TimeoutError",
                "InterruptedError",
                "RuntimeError",
                "CapturedSourceUnavailable",
                "historical_parked",
            ]
            .contains(&error)
            {
                error
            } else {
                "unknown_error"
            };
            *errors.entry((state.into(), safe.into())).or_default() += count;
        }
    }
    result["errors"]=json!(errors.into_iter().map(|((state,category),count)|json!({"state":state,"category":category,"count":count})).collect::<Vec<_>>());
    Ok(result)
}
fn captured_media_status(db: &Connection, source: Option<&str>) -> Result<Value> {
    let records = rows(
        db,
        CAPTURED_MEDIA,
        [source],
    )?;
    let mut counts = json!({"waiting":0,"ready":0,"queued":0,"indexed":0,"failed":0,"unsupported":0,"forgotten":0});
    for row in records {
        let mut state = row["state"]
            .as_str()
            .ok_or(Error::Unavailable("Invalid captured media state"))?;
        if row["job_state"] == "failed" {
            state = "failed";
        } else if state == "indexed" && row["job_state"] != "done" {
            state = "queued";
        }
        let count = counts
            .get(state)
            .and_then(Value::as_i64)
            .ok_or(Error::Unavailable("Invalid captured media state"))?;
        counts[state] = json!(count + row["count"].as_i64().unwrap());
    }
    let progress=one(db,"SELECT seq,(SELECT coalesce(max(seq),0) FROM episodes) AS latest_seq FROM captured_media_progress WHERE singleton=1",[])?.ok_or(Error::Unavailable("Captured media progress is unavailable"))?;
    counts["discovery"] =
        json!({"scope":"global","through_seq":progress["seq"],"latest_seq":progress["latest_seq"]});
    Ok(counts)
}
fn parked_conversations(db: &Connection, source: Option<&str>) -> Result<i64> {
    Ok(db.query_row(
        "SELECT count(*) FROM historical_parking WHERE (?1 IS NULL OR source=?1)",
        [source],
        |r| r.get(0),
    )?)
}
fn candidate_counts(
    db: &Connection,
    source: Option<&str>,
    now: f64,
) -> Result<serde_json::Map<String, Value>> {
    let awaiting = AWAITING.replace("?3", "?1");
    let pending=one(db,&format!("WITH candidate_status AS MATERIALIZED (SELECT ({ELIGIBLE}) AS eligible,NOT EXISTS(SELECT 1 FROM candidate_decisions d WHERE d.candidate_id=c.id AND d.rowid=(SELECT max(x.rowid) FROM candidate_decisions x WHERE x.candidate_id=c.id) AND {awaiting}) AS ready,EXISTS(SELECT 1 FROM historical_parked_candidates h WHERE h.candidate_id=c.id AND h.released_at IS NULL) AS parked FROM candidates c WHERE {ACTIVE_CANDIDATE} AND (?2 IS NULL OR EXISTS(SELECT 1 FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id WHERE g.target=c.id AND e.source=?2))) SELECT coalesce(sum(eligible AND NOT parked),0) AS pending_candidates,coalesce(sum(eligible AND ready AND NOT parked),0) AS ready_candidates,coalesce(sum(NOT eligible),0) AS excluded_legacy_candidates,coalesce(sum(eligible AND parked),0) AS parked_candidates FROM candidate_status"),params![now,source])?.unwrap();
    Ok(pending.as_object().unwrap().clone())
}
fn repair_groups(db: &Connection) -> Result<Value> {
    let mut repair=one(db,"SELECT coalesce(sum(state='pending'),0) AS pending,coalesce(sum(state='done'),0) AS done FROM repair_groups",[])?.unwrap();
    repair["scope"] = json!("global");
    Ok(repair)
}
pub fn operational_status(
    db: &Connection,
    source: Option<&str>,
    runtime: &RuntimeStatus,
    now: f64,
) -> Result<Value> {
    operational_status_timed(db, source, runtime, now, &mut Vec::new())
}
/// Same counts; appends per-section elapsed microseconds for the timing log.
pub fn operational_status_timed(
    db: &Connection,
    source: Option<&str>,
    runtime: &RuntimeStatus,
    now: f64,
    stages: &mut Vec<(&'static str, u128)>,
) -> Result<Value> {
    let mut last = Instant::now();
    let mut lap = |name: &'static str| {
        stages.push((name, last.elapsed().as_micros()));
        last = Instant::now();
    };
    if !runtime.indexing.is_object() || !runtime.indexing["available"].is_boolean() {
        return Err(Error::Unavailable("Live indexing status is unavailable"));
    }
    let source_where = if source.is_some() {
        " WHERE source=?1"
    } else {
        ""
    };
    let source_and = if source.is_some() {
        " AND source=?1"
    } else {
        ""
    };
    let episodes: i64 = db.query_row(
        &format!("SELECT count(*) FROM episodes{source_where}"),
        rusqlite::params_from_iter(source),
        |r| r.get(0),
    )?;
    lap("episodes");
    let captured = json!({"episodes":episodes,"conversations":conversation_count(db,source)?});
    lap("conversations");
    let raw_index = if source.is_some() {
        "episodes_status_cover_v1"
    } else {
        "episodes_status_raw_v1"
    };
    let mut raw=one(db,&format!("SELECT coalesce(sum(text_runs IS NULL),0) AS inline_raw_episodes,coalesce(sum(CASE (text_runs IS NULL) WHEN 1 THEN length(CAST(text AS BLOB)) ELSE 0 END),0) AS inline_raw_bytes,coalesce(sum(NOT(text_runs IS NULL)),0) AS offloaded_raw_episodes FROM episodes INDEXED BY {raw_index} WHERE kind IN ('native_record','native_live_record','native_hook') AND CAST(coalesce(json_extract(metadata,'$.extraction_mode'),'message') AS TEXT)='skip'{source_and}"),rusqlite::params_from_iter(source))?.unwrap();
    lap("raw_inline");
    let compressed=one(db,&format!("SELECT coalesce(sum(record_count),0) AS records,coalesce(sum(raw_size),0) AS original,coalesce(sum(length(data)),0) AS stored FROM raw_frames INDEXED BY raw_frames_status_cover_v1{source_where}"),rusqlite::params_from_iter(source))?.unwrap();
    raw["inline_raw_episodes"] = json!(
        raw["inline_raw_episodes"].as_i64().unwrap() - compressed["records"].as_i64().unwrap()
    );
    raw["compressed_raw_episodes"] = compressed["records"].clone();
    raw["compressed_original_bytes"] = compressed["original"].clone();
    raw["compressed_raw_bytes"] = compressed["stored"].clone();
    let objects=one(db,"SELECT count(*) AS objects,coalesce(sum(size),0) AS bytes,coalesce(sum(remote_id IS NULL),0) AS local_objects,coalesce(sum(CASE WHEN remote_id IS NULL THEN size ELSE 0 END),0) AS local_bytes,coalesce(sum(remote_id IS NOT NULL),0) AS remote_objects,coalesce(sum(CASE WHEN remote_id IS NOT NULL THEN size ELSE 0 END),0) AS remote_bytes FROM blobs",[])?.unwrap();
    let artifacts = json!({"scope":"global","registered":{"objects":objects["objects"],"bytes":objects["bytes"]},"local_only":{"objects":objects["local_objects"],"bytes":objects["local_bytes"]},"verified_remote":{"objects":objects["remote_objects"],"bytes":objects["remote_bytes"]}});
    lap("raw_frames_and_artifacts");
    let mut extraction = jobs(db, "extract", source)?;
    lap("extraction_jobs");
    extraction["pending_conversations"] = json!(pending_conversations(db, source)?);
    lap("pending_conversations");
    extraction["parked_conversations"] = json!(parked_conversations(db, source)?);
    lap("parked_conversations");
    let mut progress = rows(
        db,
        "SELECT p.source,p.conversation_id,p.seq AS cursor,p.offset,p.compact_through,p.latest_seq,h.parked_through,h.live_seq AS live_cursor,h.live_offset FROM processing_progress p LEFT JOIN historical_parking h ON h.source=p.source AND h.conversation_id=p.conversation_id WHERE (?1 IS NULL OR p.source=?1) ORDER BY p.source,p.conversation_id",
        [source],
    )?;
    for row in &mut progress {
        if row["parked_through"].is_null() {
            let object = row.as_object_mut().unwrap();
            for field in ["parked_through", "live_cursor", "live_offset"] {
                object.remove(field);
            }
        }
    }
    extraction["progress"] = json!(progress);
    lap("extraction_progress");
    let mut consolidation = jobs(db, "consolidate", source)?;
    lap("consolidation_jobs");
    consolidation
        .as_object_mut()
        .unwrap()
        .extend(candidate_counts(db, source, now)?);
    lap("candidates");
    consolidation["repair_groups"] = repair_groups(db)?;
    lap("repair_groups");
    let mut historical = historical_status(db)?;
    lap("historical");
    historical["scope"] = json!("global");
    let mut media = jobs(db, "media", source)?;
    lap("media_jobs");
    media["captured_attachments"] = captured_media_status(db, source)?;
    lap("captured_media");
    Ok(
        json!({"source":source,"captured":captured,"raw_storage":raw,"artifact_storage":artifacts,"indexing":runtime.indexing,"extraction":extraction,"consolidation":consolidation,"historical":historical,"media":media,"drive":runtime.drive}),
    )
}

/// Pending conversations whose remaining extraction input is counted exactly per request.
pub const COUNTED_CONVERSATIONS: usize = 64;
/// Matching episode rows counted per request across those conversations.
pub const COUNTED_RECORDS: i64 = 50_000;
const MESSAGE: &str = "e.kind IN ('user','assistant')";
/// Compact global processing summary for the viewer dashboard: the same job,
/// candidate and historical counts as status, without per-conversation progress.
/// Record counts use the extraction input predicate and admission rule, are
/// bounded, and report `partial` instead of guessing past the bound.
pub fn processing_summary(
    db: &Connection,
    indexing: &Value,
    now: f64,
    instant: chrono::DateTime<chrono::Utc>,
) -> Result<Value> {
    bounded_summary(db, indexing, now, instant, COUNTED_CONVERSATIONS, COUNTED_RECORDS)
}
fn bounded_summary(
    db: &Connection,
    indexing: &Value,
    now: f64,
    instant: chrono::DateTime<chrono::Utc>,
    conversation_limit: usize,
    record_limit: i64,
) -> Result<Value> {
    let mut extraction = jobs(db, "extract", None)?;
    let pending = pending_conversations(db, None)?;
    extraction["pending_conversations"] = json!(pending);
    extraction["parked_conversations"] = json!(parked_conversations(db, None)?);
    let progress = "FROM processing_progress p LEFT JOIN historical_parking h ON h.source=p.source AND h.conversation_id=p.conversation_id";
    let mut conversations = one(db, &format!("SELECT count(*) AS tracked,coalesce(sum(h.source IS NULL AND p.latest_seq<=p.seq),0) AS up_to_date,coalesce(sum(h.source IS NOT NULL AND p.latest_seq<=h.live_seq),0) AS parked_live_up_to_date {progress}"), [])?.unwrap();
    conversations["in_live_job"] = json!(db.query_row(
        "SELECT count(*) FROM (SELECT DISTINCT json_extract(payload,'$.source'),json_extract(payload,'$.conversation_id') FROM jobs WHERE kind='extract' AND state IN ('pending','leased') AND json_extract(payload,'$.mode') IS NULL AND coalesce(json_extract(payload,'$.cursor'),'')!='history')",
        [],
        |r| r.get::<_, i64>(0)
    )?);
    extraction["conversations"] = conversations;
    // Remaining live input after each cursor, newest conversations first like admission.
    let groups = rows(db, &format!("SELECT p.source,p.conversation_id,p.latest_seq,p.latest_at,p.compact_through,coalesce(h.live_seq,p.seq) AS cursor {progress} WHERE p.latest_seq>p.seq AND p.latest_seq>coalesce(h.live_seq,p.seq) ORDER BY p.latest_seq DESC LIMIT ?1"), [conversation_limit as i64])?;
    let count = format!("SELECT coalesce(sum(e.seq<=?4 AND {MESSAGE}),0) AS admissible_messages,coalesce(sum(e.seq<=?4 AND NOT ({MESSAGE})),0) AS admissible_other,coalesce(sum(e.seq>?4 AND {MESSAGE}),0) AS waiting_messages,coalesce(sum(e.seq>?4 AND NOT ({MESSAGE})),0) AS waiting_other,count(*) AS rows FROM (SELECT e.seq,e.kind FROM episodes e WHERE e.source=?1 AND e.conversation_id=?2 AND e.seq>?3 AND e.seq<=?5 AND {EXTRACTION_INPUT} LIMIT ?6) e");
    let fields = ["admissible_messages", "admissible_other", "waiting_messages", "waiting_other"];
    let mut totals = json!({"admissible_messages":0,"admissible_other":0,"waiting_messages":0,"waiting_other":0});
    let (mut budget, mut counted, mut partial) = (record_limit, 0, groups.len() as i64 != pending);
    for group in &groups {
        if budget <= 0 {
            partial = true;
            break;
        }
        let cursor = group["cursor"].as_i64().unwrap_or(0);
        let latest = group["latest_seq"].as_i64().unwrap_or(0);
        let Ok(through) = admissible_through(group, instant) else {
            partial = true;
            continue;
        };
        let boundary = if through == 0 { cursor } else { through.clamp(cursor, latest) };
        let row = one(db, &count, params![group["source"].as_str(), group["conversation_id"].as_str(), cursor, boundary, latest, budget + 1])?.unwrap();
        let used = row["rows"].as_i64().unwrap_or(0);
        if used > budget {
            partial = true;
            break;
        }
        budget -= used;
        counted += 1;
        for field in fields {
            totals[field] = json!(totals[field].as_i64().unwrap() + row[field].as_i64().unwrap());
        }
    }
    totals["counted_conversations"] = json!(counted);
    totals["partial"] = json!(partial);
    extraction["remaining"] = totals;
    // Distinct episodes named by queued and leased extraction jobs.
    let empty = json!({"jobs":0,"messages":0,"other":0});
    let mut batched = json!({"pending_live":empty,"leased_live":empty,"pending_demand":empty,"leased_demand":empty,"pending_history":empty,"leased_history":empty});
    for row in rows(db, &format!("SELECT j.state,CASE WHEN json_extract(j.payload,'$.cursor')='history' THEN 'history' WHEN json_extract(j.payload,'$.mode') IS NULL THEN 'live' ELSE 'demand' END AS mode,count(DISTINCT j.id) AS jobs,count(DISTINCT CASE WHEN {MESSAGE} THEN e.id END) AS messages,count(DISTINCT CASE WHEN NOT ({MESSAGE}) THEN e.id END) AS other FROM jobs j CROSS JOIN json_each(j.payload,'$.windows') w JOIN episodes e ON e.id=json_extract(w.value,'$.episode_id') WHERE j.kind='extract' AND j.state IN ('pending','leased') GROUP BY 1,2"), [])? {
        let key = format!("{}_{}", row["state"].as_str().unwrap_or(""), row["mode"].as_str().unwrap_or(""));
        if batched.get(&key).is_some() {
            batched[key] = json!({"jobs":row["jobs"],"messages":row["messages"],"other":row["other"]});
        }
    }
    extraction["batched"] = batched;
    let mut consolidation = jobs(db, "consolidate", None)?;
    consolidation
        .as_object_mut()
        .unwrap()
        .extend(candidate_counts(db, None, now)?);
    consolidation["repair_groups"] = repair_groups(db)?;
    Ok(json!({"generated_at":now,"indexing":indexing,"extraction":extraction,"consolidation":consolidation,"historical":historical_status(db)?}))
}
#[cfg(test)]
#[path = "processing_summary_tests.rs"]
mod processing_summary_tests;
pub fn status(
    store: &Arc<Store>,
    query: &StatusQuery,
    runtime: &RuntimeStatus,
    now: f64,
) -> Result<Value> {
    store.read(|tx| operational_status(tx.sql(), query.source.as_deref(), runtime, now))
}
#[cfg(test)]
mod plan_tests {
    use super::*;
    // Source-filtered counts must probe each job/file's own rows, never range
    // over every episode of the source once per row.
    #[test]
    fn source_filters_probe_rows_not_source_episodes() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data")).unwrap();
        store
            .read(|tx| {
                let consolidate = format!("SELECT state,error,count(*) AS count FROM jobs j WHERE kind=?1 AND (?2 IS NULL OR {CONSOLIDATE_ORIGIN}) GROUP BY state,error");
                for (sql, params) in [(consolidate.as_str(), 2), (CAPTURED_MEDIA, 1)] {
                    let mut plan = tx.sql().prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
                    let values: Vec<Option<&str>> = vec![Some("consolidate"), Some("source")];
                    let details = plan
                        .query_map(rusqlite::params_from_iter(&values[2 - params..]), |r| r.get::<_, String>(3))?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    assert!(details.iter().any(|d| d.contains("episodes") && d.contains("(id=?)")), "{details:?}");
                    assert!(!details.iter().any(|d| d.contains("(source=?)")), "{details:?}");
                }
                Ok(())
            })
            .unwrap();
    }
}
