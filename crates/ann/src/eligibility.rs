//! Query-independent eligibility, computed from the pinned authority snapshot.
use crate::ProjectionRow;
use memento_core::{Result, raw::legacy_native_context};
use regex::Regex;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::sync::LazyLock;
pub const ACTIVE_CANDIDATE: &str = "NOT EXISTS (SELECT 1 FROM forgotten_targets f WHERE f.target=c.id) AND NOT EXISTS (SELECT 1 FROM candidate_decisions d WHERE d.candidate_id=c.id AND d.rowid=(SELECT max(x.rowid) FROM candidate_decisions x WHERE x.candidate_id=c.id) AND d.decision IN ('promote','reject')) AND NOT EXISTS (SELECT 1 FROM extraction_runs r JOIN episodes e ON e.id=r.episode_id JOIN forgotten_sources f ON f.source=e.source AND f.event_id=e.event_id WHERE r.id=c.run_id)";
pub const UNPARKED: &str = "NOT EXISTS (SELECT 1 FROM historical_parked_candidates h WHERE h.candidate_id=c.id AND h.released_at IS NULL)";
macro_rules! raw_fallback {
    ($parking:literal) => {
        concat!($parking, "e.kind NOT IN ('developer','native_record','native_live_record','native_hook') AND coalesce(json_extract(e.metadata,'$.extraction_mode'),'message')<>'skip' AND coalesce(json_extract(e.metadata,'$.evidence_eligible'),1)<>0 AND NOT EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id)")
    };
}
pub const RAW_FALLBACK: &str = raw_fallback!("");
pub const HISTORICAL: &str = raw_fallback!("EXISTS (SELECT 1 FROM historical_parking h WHERE h.source=e.source AND h.conversation_id=e.conversation_id AND e.seq<=h.parked_through) AND ");
pub fn visible_sql(node: &str) -> String {
    format!(
        "(NOT EXISTS (SELECT 1 FROM captured_media cm WHERE cm.node_id={node}) OR EXISTS (SELECT 1 FROM captured_media cm JOIN captured_media_refs r ON r.sha256=cm.sha256 JOIN episodes e ON e.id=r.episode_id WHERE cm.node_id={node} AND NOT EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id) AND NOT EXISTS (SELECT 1 FROM media_files m JOIN forgotten_targets f ON f.target=m.node_id WHERE m.sha256=cm.sha256)))"
    )
}
pub fn automatic_content(text: &str) -> bool {
    static WORDS: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[^\W_]+").unwrap());
    static PATH: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^\S+(?:[/\\]\S+|\.[A-Za-z0-9]{1,8})$").unwrap());
    static FILE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^(?:~?/|[A-Za-z]:[/\\]|\.{1,2}/).+\.[A-Za-z0-9]{1,8}$").unwrap()
    });
    static REL: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(
            r"(?i)^.{1,120}\s+(?:is\s+)?(?:related to|associated with|mentions)\s+.{1,120}[.!]?$",
        )
        .unwrap()
    });
    let text = text.trim().trim_matches(['`', '"', '\'']);
    WORDS.find_iter(text).take(3).count() >= 3
        && !(!text.contains('\n') && PATH.is_match(text))
        && !FILE.is_match(text)
        && !REL.is_match(text)
}
pub fn legacy_only(db: &Connection, id: &str, version: i64) -> Result<bool> {
    let mut query=db.prepare("SELECT e.source,e.kind,e.text,e.metadata FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id WHERE g.target=? AND g.version=?")?;
    let rows = query.query_map(params![id, version], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, Option<String>>(2)?,
            r.get::<_, String>(3)?,
        ))
    })?;
    let mut found = false;
    for row in rows {
        let (source, kind, text, metadata) = row?;
        let metadata: Value = serde_json::from_str(&metadata)?;
        if !legacy_native_context(&source, &kind, text.as_deref(), Some(&metadata)) {
            return Ok(false);
        }
        found = true;
    }
    Ok(found)
}
struct Record {
    id: String,
    version: i64,
    kind: String,
    text: String,
    scope: String,
    candidate_kind: Option<String>,
    parked: bool,
}
pub fn payload(db: &Connection, row: &ProjectionRow) -> Result<Value> {
    let mut flags = json!({"kind":row.kind,"type":"","current":row.current,"ordinary":false,"automatic":false,"historical":false,"raw_fallback":false,"parked_candidate":false,"entity":false,"personal":false,"professional":false,"target":row.target,"version":row.version,"generation":row.generation});
    let record=match row.kind.as_str(){
        "node"=>db.query_row(&format!("SELECT n.id,n.version,n.type,substr(n.text,1,240),n.scope FROM node_versions n WHERE n.id=? AND n.version=? AND n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) AND NOT EXISTS (SELECT 1 FROM forgotten_targets f WHERE f.target=n.id) AND {}",visible_sql("n.id")),params![row.target,row.version],|r|Ok(Record{id:r.get(0)?,version:r.get(1)?,kind:r.get(2)?,text:r.get(3)?,scope:r.get(4)?,candidate_kind:None,parked:false})).optional()?,
        "candidate"=>db.query_row(&format!("SELECT c.id,c.type,substr(c.text,1,240),c.scope,c.kind,EXISTS(SELECT 1 FROM historical_parked_candidates h WHERE h.candidate_id=c.id AND h.released_at IS NULL) FROM candidates c JOIN extraction_runs r ON r.id=c.run_id WHERE c.id=? AND {ACTIVE_CANDIDATE}"),[&row.target],|r|Ok(Record{id:r.get(0)?,version:1,kind:r.get(1)?,text:r.get(2)?,scope:r.get(3)?,candidate_kind:r.get(4)?,parked:r.get(5)?})).optional()?,
        "segment"=>db.query_row(&format!("SELECT n.id,n.version,n.type,substr(n.text,1,240),n.scope FROM segments s JOIN media_files f ON f.node_id=s.node_id JOIN node_versions n ON n.id=s.node_id WHERE s.id=? AND n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) AND NOT EXISTS(SELECT 1 FROM forgotten_targets f WHERE f.target=n.id) AND {}",visible_sql("n.id")),[&row.target],|r|Ok(Record{id:r.get(0)?,version:r.get(1)?,kind:r.get(2)?,text:r.get(3)?,scope:r.get(4)?,candidate_kind:None,parked:false})).optional()?,
        "episode"=>{let episode=db.query_row(&format!("SELECT e.source,e.kind,e.text,e.metadata,coalesce(json_extract(e.metadata,'$.extraction_mode'),'message')<>'skip' AND NOT EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id),({HISTORICAL}),({RAW_FALLBACK}) FROM episode_chunks x JOIN episodes e ON e.id=x.episode_id WHERE x.id=?"),[&row.target],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,Option<String>>(2)?,r.get::<_,String>(3)?,r.get::<_,bool>(4)?,r.get::<_,bool>(5)?,r.get::<_,bool>(6)?))).optional()?;
            if let Some((source,kind,text,metadata,ordinary,historical,fallback))=episode{let metadata:Value=serde_json::from_str(&metadata)?;let legacy=legacy_native_context(&source,&kind,text.as_deref(),Some(&metadata));flags["type"]=json!("raw_episode");flags["ordinary"]=json!(ordinary);flags["historical"]=json!(historical&&!legacy);flags["raw_fallback"]=json!(fallback&&!legacy);}None},
        _=>None
    };
    if let Some(record) = record {
        flags["type"] = json!(record.kind);
        let scope: Vec<String> = serde_json::from_str(&record.scope)?;
        flags["personal"] = json!(scope.iter().any(|v| v == "personal"));
        flags["professional"] = json!(scope.iter().any(|v| v == "professional"));
        let entity = [
            "person",
            "organization",
            "project",
            "concept",
            "place",
            "tool",
        ]
        .contains(&record.kind.as_str())
            || db.query_row(
                "SELECT EXISTS(SELECT 1 FROM memory_types WHERE name=? AND category='entity')",
                [&record.kind],
                |r| r.get::<_, bool>(0),
            )?;
        let legacy = legacy_only(db, &record.id, record.version)?;
        let content = row.kind == "segment" || automatic_content(&record.text);
        flags["ordinary"] = json!(row.kind != "node" || record.kind != "media");
        flags["entity"] = json!(row.kind == "node");
        let automatic = match row.kind.as_str() {
            "candidate" => {
                let memory = record.candidate_kind.as_deref() == Some("memory")
                    && ![
                        "person",
                        "organization",
                        "project",
                        "concept",
                        "place",
                        "tool",
                    ]
                    .contains(&record.kind.as_str());
                flags["parked_candidate"] = json!(memory && record.parked && content && !legacy);
                memory
                    && !entity
                    && record.kind != "experience"
                    && !record.parked
                    && content
                    && !legacy
            }
            "segment" => content && !legacy,
            _ => {
                !entity
                    && record.kind != "media"
                    && content
                    && !legacy
                    && (record.kind != "experience"
                        || memento_domain::behavior(db, &record.id)?.is_some())
            }
        };
        flags["automatic"] = json!(automatic);
    }
    Ok(flags)
}
