//! Snapshot-bound recall hydration. Source authority stays in original SQLite rows.
use memento_ann::eligibility::{ACTIVE_CANDIDATE, visible_sql};
use memento_core::{CharSpan, Error, ReadTxn, Result, raw};
use memento_domain::{behavior, loops};
use rusqlite::{Connection, Params, params, types::ValueRef};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
pub const NODE_SUMMARY: &str = "n.id,n.version,n.type,substr(n.text,1,240) AS text,n.state,n.valid_from,n.valid_to,n.pinned,n.superseded_by,n.commit_id,n.scope,(SELECT c.recorded_at FROM node_versions v JOIN graph_commits c ON c.id=v.commit_id WHERE v.id=n.id ORDER BY v.version LIMIT 1) AS created_at,coalesce((SELECT importance FROM node_stats WHERE node_id=n.id),0.5) AS importance,coalesce((SELECT decay FROM node_stats WHERE node_id=n.id),1) AS decay,coalesce((SELECT archived FROM node_stats WHERE node_id=n.id),0) AS archived,coalesce((SELECT access_count FROM node_stats WHERE node_id=n.id),0) AS access_count,(SELECT last_cited_at FROM node_stats WHERE node_id=n.id) AS last_cited_at";
pub fn rows<P: Params>(db: &Connection, sql: &str, params: P) -> Result<Vec<Value>> {
    let mut statement = db.prepare(sql)?;
    let names = statement
        .column_names()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let mut cursor = statement.query(params)?;
    let mut result = Vec::new();
    while let Some(row) = cursor.next()? {
        let mut value = Map::new();
        for (i, name) in names.iter().enumerate() {
            let scalar = match row.get_ref(i)? {
                ValueRef::Null => Value::Null,
                ValueRef::Integer(v) => json!(v),
                ValueRef::Real(v) if v.is_finite() => json!(v),
                ValueRef::Text(v) => Value::String(
                    std::str::from_utf8(v)
                        .map_err(|_| Error::Unavailable("Stored text is not valid Unicode"))?
                        .into(),
                ),
                _ => {
                    return Err(Error::Unavailable(
                        "Stored recall field has an invalid type",
                    ));
                }
            };
            value.insert(name.clone(), scalar);
        }
        result.push(Value::Object(value));
    }
    Ok(result)
}
pub fn one<P: Params>(db: &Connection, sql: &str, params: P) -> Result<Option<Value>> {
    Ok(rows(db, sql, params)?.into_iter().next())
}
pub fn merge(record: &mut Value, fields: Value) {
    if let Some(fields) = fields.as_object() {
        for (name, value) in fields {
            record[name] = value.clone();
        }
    }
}
pub fn scope(record: &mut Value) -> Result<()> {
    if let Some(value) = record["scope"].as_str() {
        let value = serde_json::from_str::<Value>(value)?;
        record["scope"] = value;
    }
    Ok(())
}
pub fn node(db: &Connection, id: &str, version: i64) -> Result<Option<Value>> {
    one(
        db,
        &format!(
            "SELECT {NODE_SUMMARY} FROM node_versions n WHERE n.id=? AND n.version=? AND n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) AND NOT EXISTS(SELECT 1 FROM forgotten_targets f WHERE f.target=n.id) AND {}",
            visible_sql("n.id")
        ),
        params![id, version],
    )
}
pub fn candidate(db: &Connection, id: &str) -> Result<Option<Value>> {
    let row = one(
        db,
        &format!(
            "SELECT c.id,c.type,substr(c.text,1,240) AS text,c.kind AS candidate_kind,c.src,c.dst,c.confidence,c.valid_from,c.valid_to,c.scope,coalesce((SELECT importance FROM node_stats WHERE node_id=c.id),0.5) AS importance,coalesce((SELECT decay FROM node_stats WHERE node_id=c.id),1) AS decay,coalesce((SELECT archived FROM node_stats WHERE node_id=c.id),0) AS archived,substr(c.why,1,240) AS why,r.episode_id,r.recorded_at AS created_at,EXISTS(SELECT 1 FROM historical_parked_candidates h WHERE h.candidate_id=c.id AND h.released_at IS NULL) AS historical FROM candidates c JOIN extraction_runs r ON r.id=c.run_id WHERE c.id=? AND {ACTIVE_CANDIDATE}"
        ),
        [id],
    )?;
    let Some(mut row) = row else { return Ok(None) };
    scope(&mut row)?;
    row["historical"] = json!(row["historical"] == 1);
    merge(
        &mut row,
        json!({"version":1,"tier":"candidate","state":"candidate","confirmed":false,"pinned":0}),
    );
    Ok(Some(row))
}
pub fn segment(db: &Connection, id: &str) -> Result<Option<Value>> {
    one(
        db,
        &format!(
            "SELECT {NODE_SUMMARY},s.id AS segment_id,s.modality,s.start_s,s.end_s,s.page,s.char_start,s.char_end,substr(s.text,1,240) AS match_text,'/blobs/'||s.source_sha256 AS source_text_url,'/blobs/'||f.sha256 AS blob_url,'/blobs/'||s.artifact_sha256 AS artifact_url,f.sha256,f.filename,f.kind AS media_kind FROM segments s JOIN media_files f ON f.node_id=s.node_id JOIN node_versions n ON n.id=s.node_id WHERE s.id=? AND n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) AND NOT EXISTS(SELECT 1 FROM forgotten_targets f WHERE f.target=n.id) AND {}",
            visible_sql("n.id")
        ),
        [id],
    )
}
pub fn candidate_details(db: &Connection, id: &str) -> Result<Value> {
    let evidence = rows(
        db,
        "SELECT episode_id,start,end,substr(quote,1,240) AS quote_excerpt FROM graph_evidence WHERE target=? AND version=1 ORDER BY episode_id,start LIMIT 5",
        [id],
    )?;
    let run = one(
        db,
        "SELECT g.recorded_at,g.actor,substr(g.why,1,240) AS why FROM candidates c JOIN extraction_runs r ON r.id=c.run_id JOIN graph_commits g ON g.id=r.commit_id WHERE c.id=?",
        [id],
    )?;
    let matches = rows(
        db,
        "SELECT m.target,m.cosine,m.name_match,n.type,substr(n.text,1,160) AS one_line FROM candidate_entity_matches m JOIN node_versions n ON n.id=m.target AND n.version=m.version WHERE m.candidate_id=? AND n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) AND NOT EXISTS(SELECT 1 FROM forgotten_targets f WHERE f.target=n.id) LIMIT 5",
        [id],
    )?;
    Ok(
        json!({"evidence":evidence,"change_count":1,"last_change":run,"entity_matches":matches,"loop":loops::current(db,id)?}),
    )
}
pub fn details(db: &Connection, id: &str, version: i64) -> Result<Value> {
    let change=one(db,"SELECT c.recorded_at,substr(c.actor,1,128) AS actor,substr(c.why,1,240) AS why,(SELECT count(*) FROM node_versions WHERE id=?1) AS change_count FROM node_versions n JOIN graph_commits c ON c.id=n.commit_id WHERE n.id=?1 AND n.version=?2",params![id,version])?.ok_or(Error::Unavailable("Recall version is unavailable"))?;
    let evidence = rows(
        db,
        "SELECT g.episode_id,g.start,g.end,CASE WHEN EXISTS(SELECT 1 FROM episodes e JOIN forgotten_sources f ON f.source=e.source AND f.event_id=e.event_id WHERE e.id=g.episode_id) THEN NULL ELSE substr(g.quote,1,240) END AS quote_excerpt FROM graph_evidence g WHERE g.target=? AND g.version=? ORDER BY g.episode_id,g.start LIMIT 5",
        params![id, version],
    )?;
    let mut result = json!({"evidence":evidence,"change_count":change["change_count"],"last_change":{"recorded_at":change["recorded_at"],"actor":change["actor"],"why":change["why"]},"loop":loops::current(db,id)?});
    if let Some(behavior) = behavior(db, id)? {
        result["authority"] = behavior["authority"].clone();
        result["behavior"] = behavior;
        result["category"] = json!("behavioral_suggestion");
    }
    Ok(result)
}
pub fn entity_type(db: &Connection, kind: &str) -> Result<bool> {
    if [
        "person",
        "organization",
        "project",
        "concept",
        "place",
        "tool",
    ]
    .contains(&kind)
    {
        return Ok(true);
    }
    Ok(db.query_row(
        "SELECT EXISTS(SELECT 1 FROM memory_types WHERE name=? AND category='entity')",
        [kind],
        |r| r.get(0),
    )?)
}
pub fn meaningful(db: &Connection, record: &Value) -> Result<bool> {
    if record["tier"] == "raw"
        || record["tier"] == "reference"
        || record["historical"] == true
        || record["type"] == "media"
        || entity_type(db, record["type"].as_str().unwrap_or(""))?
    {
        return Ok(false);
    }
    Ok(record["tier"] != "candidate" || record["candidate_kind"] == "memory")
}
pub fn episode(
    tx: &ReadTxn<'_>,
    id: &str,
    start: usize,
    end: Option<usize>,
    chunk: Option<&str>,
    historical: bool,
    focus: Option<usize>,
) -> Result<Option<Value>> {
    let Some(episode) = tx.episode(id)? else {
        return Ok(None);
    };
    let source = episode["source"].as_str().unwrap();
    let event = episode["event_id"].as_str().unwrap();
    if tx.sql().query_row(
        "SELECT EXISTS(SELECT 1 FROM forgotten_sources WHERE source=? AND event_id=?)",
        params![source, event],
        |r| r.get::<_, bool>(0),
    )? {
        return Ok(None);
    }
    let text = episode["text"].as_str().unwrap();
    if historical
        && raw::legacy_native_context(
            source,
            episode["kind"].as_str().unwrap(),
            Some(text),
            Some(&episode["metadata"]),
        )
    {
        return Ok(None);
    }
    let end = end.unwrap_or_else(|| text.chars().count());
    if end < start || end > text.chars().count() {
        return Err(Error::Unavailable(
            "Raw passage span is outside original evidence",
        ));
    }
    let left = focus
        .map(|focus| start.max(focus.saturating_sub(80).min(end.saturating_sub(240))))
        .unwrap_or(start);
    let quote = CharSpan {
        start: left,
        end: end.min(left + 240),
    }
    .slice(text)?;
    let evidence = CharSpan {
        start,
        end: end.min(start + 240),
    }
    .slice(text)?;
    Ok(Some(
        json!({"id":chunk.unwrap_or(id),"episode_id":id,"type":"raw_episode","tier":"raw","version":1,"state":"raw","confirmed":false,"historical":historical,"text":quote,"source":source,"source_kind":episode["kind"],"conversation_id":episode["conversation_id"],"occurred_at":episode["occurred_at"],"pinned":0,"created_at":episode["captured_at"],"valid_from":null,"valid_to":null,"chunk_id":chunk,"evidence":[{"episode_id":id,"start":start,"end":end,"quote_excerpt":evidence}],"source_url":format!("/episodes/{id}"),"change_count":1,"last_change":{"recorded_at":episode["captured_at"],"actor":source,"why":"Immutable captured evidence"}}),
    ))
}
pub fn matched_passage(
    tx: &ReadTxn<'_>,
    id: &str,
    query: &str,
    historical: bool,
) -> Result<Option<Value>> {
    // Whole-episode FTS is discovery only: the focused excerpt can contain just
    // one query term. Semantic recall validates this passage with its scorer;
    // literal raw-evidence endpoints keep their existing source-text behavior.
    let Some(source) = tx.original_text(id)? else {
        return Ok(None);
    };
    let mut hits = Vec::new();
    for term in query.split_whitespace() {
        let pattern = regex::RegexBuilder::new(&regex::escape(term))
            .case_insensitive(true)
            .build()
            .map_err(|_| Error::Validation("Invalid literal query"))?;
        if let Some(found) = pattern.find(&source) {
            hits.push(source[..found.start()].chars().count());
        }
    }
    let position = hits.into_iter().min().unwrap_or(0);
    let chunk = one(
        tx.sql(),
        "SELECT id,start,end FROM episode_chunks WHERE episode_id=? AND start<=? AND end>? ORDER BY start LIMIT 1",
        params![id, position, position],
    )?;
    if let Some(chunk) = chunk {
        return episode(
            tx,
            id,
            chunk["start"].as_u64().unwrap() as usize,
            Some(chunk["end"].as_u64().unwrap() as usize),
            chunk["id"].as_str(),
            historical,
            Some(position),
        );
    }
    let start = position.saturating_sub(200);
    episode(
        tx,
        id,
        start,
        Some(source.chars().count().min(start + 1000)),
        None,
        historical,
        Some(position),
    )
}
pub fn related(db: &Connection, id: &str, hops: usize, scope: Option<&str>) -> Result<Vec<Value>> {
    if ![1, 2].contains(&hops) {
        return Err(Error::Validation(
            "Related traversal is limited to one or two hops",
        ));
    }
    // Expand only reached origins. Eligibility precedes each origin's eight-edge
    // limit, including duplicate endpoints/self-edges just as in the old walk.
    // Every caller supplies its pinned read transaction, so all hops share a cut.
    let sql = format!(
        "SELECT n.id,n.type,substr(n.text,1,160) AS one_line,n.valid_from,n.valid_to,n.scope,coalesce((SELECT importance FROM node_stats WHERE node_id=n.id),0.5) AS importance,a.rel AS relation,a.direction,a.id AS edge_id FROM (SELECT e.id,e.dst AS target,e.rel,'out' AS direction FROM edge_versions e WHERE e.src=?2 AND e.state='active' AND e.version=(SELECT max(v.version) FROM edge_versions v WHERE v.id=e.id) UNION ALL SELECT e.id,e.src AS target,e.rel,'in' AS direction FROM edge_versions e WHERE e.dst=?2 AND e.state='active' AND e.version=(SELECT max(v.version) FROM edge_versions v WHERE v.id=e.id)) a JOIN node_versions n ON n.id=a.target WHERE n.state='active' AND n.version=(SELECT max(v.version) FROM node_versions v WHERE v.id=n.id) AND NOT EXISTS(SELECT 1 FROM forgotten_targets f WHERE f.target=n.id) AND {} AND (?1 IS NULL OR EXISTS(SELECT 1 FROM json_each(n.scope) WHERE value=?1)) ORDER BY a.id LIMIT 8",
        visible_sql("n.id")
    );
    let mut frontier = BTreeSet::from([id.to_owned()]);
    let mut expanded = BTreeSet::new();
    let mut reached = BTreeMap::<String, Value>::new();
    for depth in 1..=hops {
        let mut next = BTreeSet::new();
        for origin in frontier {
            if !expanded.insert(origin.clone()) {
                continue;
            }
            for mut item in rows(db, &sql, params![scope, origin])? {
                let target = item["id"].as_str().unwrap().to_owned();
                next.insert(target.clone());
                if target == id {
                    continue;
                }
                item["hops"] = json!(depth);
                item["via"] = json!(origin);
                let replace = reached.get(&target).is_none_or(|old| {
                    (depth, item["edge_id"].as_str().unwrap(), origin.as_str())
                        < (
                            old["hops"].as_u64().unwrap() as usize,
                            old["edge_id"].as_str().unwrap(),
                            old["via"].as_str().unwrap(),
                        )
                });
                if replace {
                    reached.insert(target, item);
                }
            }
        }
        frontier = next;
    }
    let mut result = reached.into_values().collect::<Vec<_>>();
    result.sort_by(|a, b| {
        a["hops"]
            .as_u64()
            .cmp(&b["hops"].as_u64())
            .then_with(|| {
                b["importance"]
                    .as_f64()
                    .unwrap()
                    .partial_cmp(&a["importance"].as_f64().unwrap())
                    .unwrap()
            })
            .then_with(|| a["id"].as_str().cmp(&b["id"].as_str()))
    });
    result.truncate(16);
    for item in &mut result {
        self::scope(item)?;
        item["loop"] = json!(loops::current(db, item["id"].as_str().unwrap())?);
    }
    Ok(result)
}

#[cfg(test)]
#[path = "related_tests.rs"]
mod related_tests;

pub fn media_sources(db: &Connection, record: &mut Value) -> Result<bool> {
    if record["type"] != "media" {
        return Ok(true);
    }
    let id = record["id"].as_str().unwrap_or("").to_owned();
    let Some(media) = one(
        db,
        "SELECT sha256 FROM captured_media WHERE node_id=?",
        [&id],
    )?
    else {
        return Ok(true);
    };
    let sha = media["sha256"].as_str().unwrap();
    let hidden:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM forgotten_targets WHERE target=?1) OR EXISTS(SELECT 1 FROM node_versions WHERE id=?1 AND version=(SELECT max(version) FROM node_versions WHERE id=?1) AND state<>'active') OR EXISTS(SELECT 1 FROM media_files m JOIN forgotten_targets f ON f.target=m.node_id WHERE m.sha256=?2)",params![id,sha],|r|r.get(0))?;
    if hidden {
        return Ok(false);
    }
    let select = "SELECT r.episode_id,e.source,e.conversation_id,e.occurred_at,e.captured_at,r.start,r.end,r.quote FROM captured_media_refs r JOIN episodes e ON e.id=r.episode_id WHERE r.sha256=? AND NOT EXISTS(SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id)";
    let references = rows(db, &format!("{select} ORDER BY e.seq DESC LIMIT 5"), [sha])?;
    if references.is_empty() {
        return Ok(false);
    }
    let newest = one(
        db,
        &format!(
            "{select} AND e.occurred_at IS NOT NULL ORDER BY julianday(e.occurred_at) DESC LIMIT 1"
        ),
        [sha],
    )?;
    let first = newest.as_ref().unwrap_or(&references[0]);
    merge(
        record,
        details(db, &id, record["version"].as_i64().unwrap())?,
    );
    let evidence=references.iter().map(|r|json!({"episode_id":r["episode_id"],"start":r["start"],"end":r["end"],"quote_excerpt":r["quote"].as_str().unwrap_or("").chars().take(240).collect::<String>()})).collect::<Vec<_>>();
    merge(
        record,
        json!({"category":"media_reference","tier":"reference","confirmed":false,"references":references,"evidence":evidence,"source":first["source"],"conversation_id":first["conversation_id"],"source_url":format!("/episodes/{}",first["episode_id"].as_str().unwrap()),"_source_occurred_at":newest.as_ref().map(|r|r["occurred_at"].clone()).unwrap_or(Value::Null)}),
    );
    Ok(true)
}
pub fn media_pointer(db: &Connection, record: &mut Value) -> Result<()> {
    let mut text = format!(
        "{} attachment {}; original {}.",
        record["media_kind"]
            .as_str()
            .map(|s| {
                let mut chars = s.chars();
                chars
                    .next()
                    .map(|c| c.to_uppercase().collect::<String>() + &chars.as_str().to_lowercase())
                    .unwrap_or_default()
            })
            .unwrap_or_default(),
        serde_json::to_string(&record["filename"])?,
        record["blob_url"].as_str().unwrap_or("")
    );
    let source = if record["source"].is_string() {
        record["source"].as_str().map(str::to_owned)
    } else {
        one(db,"SELECT e.source FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id WHERE g.target=? AND g.version=? AND NOT EXISTS(SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id) ORDER BY julianday(e.occurred_at) DESC,e.id LIMIT 1",params![record["id"].as_str(),record["version"].as_i64()])?.and_then(|r|r["source"].as_str().map(str::to_owned))
    };
    if let Some(source) = source {
        text.push_str(&format!(" From {}.", serde_json::to_string(&source)?));
    }
    record["text"] = json!(text);
    record["category"] = json!("media_reference");
    Ok(())
}
