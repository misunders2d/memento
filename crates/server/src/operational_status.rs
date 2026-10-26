//! Observational counts only. Caller supplies live indexing/Drive observations;
//! no scheduling, model/ANN work, transfer, health writer probe or mutation here.
use crate::query_rows::{ACTIVE_CANDIDATE, one, rows};
use memento_core::{Error, Result, Store};
use memento_domain::{
    consolidation::{AWAITING, ELIGIBLE},
    processing::{historical_status, pending_conversations},
};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
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
fn jobs(db: &Connection, kind: &str, source: Option<&str>) -> Result<Value> {
    let origin = match kind {
        "extract" => "json_extract(j.payload,'$.source')=?2",
        "consolidate" => {
            "EXISTS(SELECT 1 FROM json_each(j.payload,'$.candidate_ids') c JOIN graph_evidence g ON g.target=c.value JOIN episodes e ON e.id=g.episode_id WHERE e.source=?2)"
        }
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
    let mut result = json!({"available":true,"pending_jobs":0,"leased_jobs":0,"completed_jobs":0,"failed_jobs":0,"retrying_jobs":0,"parked_jobs":0});
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
        "SELECT cm.state,j.state AS job_state,count(*) AS count FROM captured_media cm LEFT JOIN jobs j ON j.id=cm.job_id WHERE (?1 IS NULL OR EXISTS(SELECT 1 FROM captured_media_refs r JOIN episodes e ON e.id=r.episode_id WHERE r.sha256=cm.sha256 AND e.source=?1)) GROUP BY cm.state,j.state",
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
pub fn operational_status(
    db: &Connection,
    source: Option<&str>,
    runtime: &RuntimeStatus,
    now: f64,
) -> Result<Value> {
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
    let captured = json!({"episodes":episodes,"conversations":conversation_count(db,source)?});
    let raw_index = if source.is_some() {
        "episodes_status_cover_v1"
    } else {
        "episodes_status_raw_v1"
    };
    let mut raw=one(db,&format!("SELECT coalesce(sum(text_runs IS NULL),0) AS inline_raw_episodes,coalesce(sum(CASE (text_runs IS NULL) WHEN 1 THEN length(CAST(text AS BLOB)) ELSE 0 END),0) AS inline_raw_bytes,coalesce(sum(NOT(text_runs IS NULL)),0) AS offloaded_raw_episodes FROM episodes INDEXED BY {raw_index} WHERE kind IN ('native_record','native_live_record','native_hook') AND CAST(coalesce(json_extract(metadata,'$.extraction_mode'),'message') AS TEXT)='skip'{source_and}"),rusqlite::params_from_iter(source))?.unwrap();
    let compressed=one(db,&format!("SELECT coalesce(sum(record_count),0) AS records,coalesce(sum(raw_size),0) AS original,coalesce(sum(length(data)),0) AS stored FROM raw_frames INDEXED BY raw_frames_status_cover_v1{source_where}"),rusqlite::params_from_iter(source))?.unwrap();
    raw["inline_raw_episodes"] = json!(
        raw["inline_raw_episodes"].as_i64().unwrap() - compressed["records"].as_i64().unwrap()
    );
    raw["compressed_raw_episodes"] = compressed["records"].clone();
    raw["compressed_original_bytes"] = compressed["original"].clone();
    raw["compressed_raw_bytes"] = compressed["stored"].clone();
    let objects=one(db,"SELECT count(*) AS objects,coalesce(sum(size),0) AS bytes,coalesce(sum(remote_id IS NULL),0) AS local_objects,coalesce(sum(CASE WHEN remote_id IS NULL THEN size ELSE 0 END),0) AS local_bytes,coalesce(sum(remote_id IS NOT NULL),0) AS remote_objects,coalesce(sum(CASE WHEN remote_id IS NOT NULL THEN size ELSE 0 END),0) AS remote_bytes FROM blobs",[])?.unwrap();
    let artifacts = json!({"scope":"global","registered":{"objects":objects["objects"],"bytes":objects["bytes"]},"local_only":{"objects":objects["local_objects"],"bytes":objects["local_bytes"]},"verified_remote":{"objects":objects["remote_objects"],"bytes":objects["remote_bytes"]}});
    let mut extraction = jobs(db, "extract", source)?;
    extraction["pending_conversations"] = json!(pending_conversations(db, source)?);
    extraction["parked_conversations"] = json!(db.query_row(
        "SELECT count(*) FROM historical_parking WHERE (?1 IS NULL OR source=?1)",
        [source],
        |r| r.get::<_, i64>(0)
    )?);
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
    let mut consolidation = jobs(db, "consolidate", source)?;
    let awaiting = AWAITING.replace("?3", "?1");
    let pending=one(db,&format!("WITH candidate_status AS MATERIALIZED (SELECT ({ELIGIBLE}) AS eligible,NOT EXISTS(SELECT 1 FROM candidate_decisions d WHERE d.candidate_id=c.id AND d.rowid=(SELECT max(x.rowid) FROM candidate_decisions x WHERE x.candidate_id=c.id) AND {awaiting}) AS ready,EXISTS(SELECT 1 FROM historical_parked_candidates h WHERE h.candidate_id=c.id AND h.released_at IS NULL) AS parked FROM candidates c WHERE {ACTIVE_CANDIDATE} AND (?2 IS NULL OR EXISTS(SELECT 1 FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id WHERE g.target=c.id AND e.source=?2))) SELECT coalesce(sum(eligible AND NOT parked),0) AS pending_candidates,coalesce(sum(eligible AND ready AND NOT parked),0) AS ready_candidates,coalesce(sum(NOT eligible),0) AS excluded_legacy_candidates,coalesce(sum(eligible AND parked),0) AS parked_candidates FROM candidate_status"),params![now,source])?.unwrap();
    consolidation
        .as_object_mut()
        .unwrap()
        .extend(pending.as_object().unwrap().clone());
    let mut repair=one(db,"SELECT coalesce(sum(state='pending'),0) AS pending,coalesce(sum(state='done'),0) AS done FROM repair_groups",[])?.unwrap();
    repair["scope"] = json!("global");
    consolidation["repair_groups"] = repair;
    let mut historical = historical_status(db)?;
    historical["scope"] = json!("global");
    let mut media = jobs(db, "media", source)?;
    media["captured_attachments"] = captured_media_status(db, source)?;
    Ok(
        json!({"source":source,"captured":captured,"raw_storage":raw,"artifact_storage":artifacts,"indexing":runtime.indexing,"extraction":extraction,"consolidation":consolidation,"historical":historical,"media":media,"drive":runtime.drive}),
    )
}
pub fn status(
    store: &Arc<Store>,
    query: &StatusQuery,
    runtime: &RuntimeStatus,
    now: f64,
) -> Result<Value> {
    store.read(|tx| operational_status(tx.sql(), query.source.as_deref(), runtime, now))
}
