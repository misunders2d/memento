//! Exact stored full-name/reviewed-alias context. No partial-token identity inference.
use crate::records::{self, merge, one, rows};
use memento_ann::eligibility::{ACTIVE_CANDIDATE, legacy_only, visible_sql};
use memento_core::{Error, Result};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashSet};
fn tokens(text: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut word = String::new();
    for c in text.chars().chain(std::iter::once(' ')) {
        if c != '_' && memento_domain::unicode::is_word(c) {
            word.push(c)
        } else if !word.is_empty() {
            result.push(std::mem::take(&mut word).to_lowercase());
        }
    }
    result
}
fn contained(name: &str, request: &[String]) -> bool {
    let name = tokens(name);
    !name.is_empty() && request.windows(name.len()).any(|part| part == name)
}
/// Existing tokenizer, shared by full-name anchors and grounded one-hop subject admission.
pub fn contains_name(name: &str, text: &str) -> bool {
    contained(name, &tokens(text))
}
pub fn connection_name_matches(db: &Connection, item: &Value, query: &str) -> Result<bool> {
    let request = tokens(query);
    let overlaps = |name: &str| tokens(name).iter().any(|token| request.contains(token));
    if overlaps(
        item["name"]
            .as_str()
            .unwrap_or(item["text"].as_str().unwrap_or("")),
    ) {
        return Ok(true);
    }
    // The same reviewed merge lineage as T1; aliases never create or merge identities here.
    let mut aliases = db.prepare("WITH RECURSIVE canonical(source,target,depth) AS (SELECT source,target,0 FROM entity_merges UNION ALL SELECT c.source,m.superseded_by,c.depth+1 FROM canonical c JOIN node_versions m ON m.id=c.target AND m.state IN ('merged','refined') AND m.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=m.id) WHERE m.superseded_by IS NOT NULL AND c.depth<64) SELECT a.text FROM alias_fts a JOIN canonical c ON c.source=a.id WHERE c.target=?")?;
    for alias in aliases.query_map([item["id"].as_str().unwrap()], |row| {
        row.get::<_, String>(0)
    })? {
        if overlaps(&alias?) {
            return Ok(true);
        }
    }
    Ok(false)
}
fn identity(text: &str) -> String {
    text.to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}
pub fn anchors(
    db: &Connection,
    query: &str,
    limit: usize,
    scope: Option<&str>,
) -> Result<Vec<Value>> {
    let budget = 2.min(limit);
    if budget == 0 {
        return Ok(Vec::new());
    }
    let request = tokens(query);
    let mut seen = HashSet::new();
    let terms = request
        .iter()
        .filter(|t| seen.insert((*t).clone()))
        .take(64)
        .collect::<Vec<_>>();
    if terms.is_empty() {
        return Ok(Vec::new());
    }
    let matched = terms
        .into_iter()
        .map(|s| format!("\"{}\"", s.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" OR ");
    let entity = "(n.type IN ('person','organization','project','concept','place','tool') OR EXISTS(SELECT 1 FROM memory_types t WHERE t.name=n.type AND t.category='entity'))";
    let visible = format!(
        "n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) AND {entity} AND {} AND (?2 IS NULL OR EXISTS(SELECT 1 FROM json_each(n.scope) WHERE value=?2)) AND NOT EXISTS(SELECT 1 FROM entity_merges m WHERE m.source=n.id) AND NOT EXISTS(SELECT 1 FROM forgotten_targets f WHERE f.target=n.id)",
        visible_sql("n.id")
    );
    let count = "(SELECT count(*) FROM graph_evidence g WHERE g.target=n.id AND g.version=n.version) AS evidence_count";
    let mut found = BTreeMap::new();
    for row in rows(
        db,
        &format!(
            "SELECT {},{count},NULL AS alias,'durable' AS tier FROM node_fts JOIN node_versions n ON n.id=node_fts.id AND n.version=node_fts.version WHERE node_fts MATCH ?1 AND {visible} ORDER BY bm25(node_fts),n.id LIMIT 64",
            records::NODE_SUMMARY
        ),
        params![matched, scope],
    )? {
        if contained(row["text"].as_str().unwrap_or(""), &request) {
            found
                .entry(row["id"].as_str().unwrap().to_owned())
                .or_insert(row);
        }
    }
    let aliases = format!(
        "WITH RECURSIVE canonical(source,target,depth) AS (SELECT source,target,0 FROM entity_merges UNION ALL SELECT c.source,m.superseded_by,c.depth+1 FROM canonical c JOIN node_versions m ON m.id=c.target AND m.state IN ('merged','refined') AND m.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=m.id) WHERE m.superseded_by IS NOT NULL AND c.depth<64) SELECT {},{count},alias_fts.text AS alias,'durable' AS tier FROM alias_fts JOIN canonical a ON a.source=alias_fts.id JOIN node_versions n ON n.id=a.target WHERE alias_fts MATCH ?1 AND {visible} ORDER BY bm25(alias_fts),n.id LIMIT 64",
        records::NODE_SUMMARY
    );
    for row in rows(db, &aliases, params![matched, scope])? {
        if contained(row["alias"].as_str().unwrap_or(""), &request) {
            found
                .entry(row["id"].as_str().unwrap().into())
                .or_insert(row);
        }
    }
    for row in rows(
        db,
        &format!(
            "SELECT c.id,c.type,c.text,c.scope,'candidate' AS tier,(SELECT max(julianday(e.occurred_at)) FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id WHERE g.target=c.id AND g.version=1 AND e.occurred_at IS NOT NULL) AS source_at FROM candidate_fts JOIN candidates c ON c.id=candidate_fts.id WHERE candidate_fts MATCH ?1 AND c.kind='entity' AND {ACTIVE_CANDIDATE} AND (?2 IS NULL OR EXISTS(SELECT 1 FROM json_each(c.scope) WHERE value=?2)) ORDER BY bm25(candidate_fts),c.id LIMIT 64"
        ),
        params![matched, scope],
    )? {
        if contained(row["text"].as_str().unwrap_or(""), &request) {
            found
                .entry(row["id"].as_str().unwrap().into())
                .or_insert(row);
        }
    }
    let mut spelled: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    for row in found.values() {
        spelled
            .entry((
                row["type"].as_str().unwrap().into(),
                identity(row["text"].as_str().unwrap()),
            ))
            .or_default()
            .push(row["id"].as_str().unwrap().into());
    }
    let mut ordered = found.into_values().collect::<Vec<_>>();
    ordered.sort_by(|a, b| {
        let durable_a = a["tier"] == "durable";
        let durable_b = b["tier"] == "durable";
        durable_b
            .cmp(&durable_a)
            .then_with(|| {
                if durable_a {
                    b["evidence_count"]
                        .as_i64()
                        .unwrap_or(0)
                        .cmp(&a["evidence_count"].as_i64().unwrap_or(0))
                } else {
                    b["source_at"]
                        .as_f64()
                        .unwrap_or(f64::NEG_INFINITY)
                        .total_cmp(&a["source_at"].as_f64().unwrap_or(f64::NEG_INFINITY))
                }
            })
            .then_with(|| a["id"].as_str().cmp(&b["id"].as_str()))
    });
    let mut result = Vec::new();
    for item in ordered.into_iter().take(budget * 2) {
        let same = &spelled[&(
            item["type"].as_str().unwrap().into(),
            identity(item["text"].as_str().unwrap()),
        )];
        let mut anchor = if item["tier"] == "durable" {
            durable(db, &item, scope)?
        } else {
            pending(db, &item, scope)?
        };
        if let Some(ref mut anchor) = anchor {
            if same.len() > 1 {
                anchor["same_name_count"] = json!(same.len());
                anchor["same_name_ids"] = json!(
                    same.iter()
                        .filter(|id| Some(id.as_str()) != anchor["id"].as_str())
                        .collect::<Vec<_>>()
                );
                anchor["text"] = json!(format!(
                    "One of {} separate entries spelled alike; not merged. {}",
                    same.len(),
                    anchor["text"].as_str().unwrap()
                ));
            }
        }
        if let Some(anchor) = anchor {
            result.push(anchor);
        }
        if result.len() == budget {
            break;
        }
    }
    Ok(result)
}
fn durable(db: &Connection, item: &Value, scope: Option<&str>) -> Result<Option<Value>> {
    let id = item["id"].as_str().unwrap();
    let version = item["version"].as_i64().unwrap();
    if legacy_only(db, id, version)? {
        return Ok(None);
    }
    let mut record = item.clone();
    for key in ["evidence_count", "alias", "tier"] {
        record.as_object_mut().unwrap().remove(key);
    }
    records::scope(&mut record)?;
    merge(&mut record, json!({"tier":"durable","confirmed":true}));
    merge(&mut record, records::details(db, id, version)?);
    let connections=records::related(db,id,1,scope)?.into_iter().take(5).map(|row|json!({"id":row["id"],"type":row["type"],"relation":row["relation"],"direction":row["direction"],"relation_id":row["edge_id"],"basis":"edge","text":row["one_line"]})).collect();
    Ok(Some(compose(
        record,
        item["text"].as_str().unwrap(),
        connections,
        item["alias"].as_str(),
    )?))
}
fn endpoint(db: &Connection, id: &str, scope: Option<&str>) -> Result<Option<Value>> {
    if let Some(mut row) = one(
        db,
        &format!(
            "SELECT c.id,c.type,substr(c.text,1,160) AS text FROM candidates c WHERE c.id=?1 AND {ACTIVE_CANDIDATE} AND (?2 IS NULL OR EXISTS(SELECT 1 FROM json_each(c.scope) WHERE value=?2))"
        ),
        params![id, scope],
    )? {
        row["tier"] = json!("candidate");
        return Ok(Some(row));
    }
    let Some(promoted) = one(
        db,
        "SELECT d.target_id FROM candidate_decisions d WHERE d.candidate_id=? AND d.rowid=(SELECT max(x.rowid) FROM candidate_decisions x WHERE x.candidate_id=?) AND d.decision='promote' AND d.target_id IS NOT NULL",
        [id, id],
    )?
    else {
        return Ok(None);
    };
    let sql = format!(
        "WITH RECURSIVE canonical(id,depth) AS (SELECT ?1,0 UNION ALL SELECT m.superseded_by,c.depth+1 FROM canonical c JOIN node_versions m ON m.id=c.id AND m.state IN ('merged','refined') AND m.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=m.id) WHERE m.superseded_by IS NOT NULL AND c.depth<64) SELECT n.id,n.type,substr(n.text,1,160) AS text FROM node_versions n WHERE n.id=(SELECT id FROM canonical ORDER BY depth DESC LIMIT 1) AND n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) AND {} AND NOT EXISTS(SELECT 1 FROM entity_merges e WHERE e.source=n.id) AND NOT EXISTS(SELECT 1 FROM forgotten_targets f WHERE f.target=n.id) AND (?2 IS NULL OR EXISTS(SELECT 1 FROM json_each(n.scope) WHERE value=?2))",
        visible_sql("n.id")
    );
    let mut row = one(db, &sql, params![promoted["target_id"].as_str(), scope])?;
    if let Some(ref mut row) = row {
        row["tier"] = json!("durable");
    }
    Ok(row)
}
fn pending(db: &Connection, item: &Value, scope: Option<&str>) -> Result<Option<Value>> {
    let id = item["id"].as_str().unwrap();
    let Some(mut record) = records::candidate(db, id)? else {
        return Ok(None);
    };
    if legacy_only(db, id, 1)? {
        return Ok(None);
    }
    merge(&mut record, records::candidate_details(db, id)?);
    let mut connections = Vec::new();
    for row in rows(
        db,
        &format!(
            "SELECT c.id,c.type AS relation,c.src,c.dst FROM candidates c WHERE c.kind='relation' AND (c.src=?1 OR c.dst=?1) AND {ACTIVE_CANDIDATE} AND (?2 IS NULL OR EXISTS(SELECT 1 FROM json_each(c.scope) WHERE value=?2)) ORDER BY c.id LIMIT 32"
        ),
        params![id, scope],
    )? {
        let out = row["src"] == id;
        let other = if out {
            row["dst"].as_str()
        } else {
            row["src"].as_str()
        }
        .ok_or(Error::Unavailable(
            "Pending relation endpoint is unavailable",
        ))?;
        if let Some(mut endpoint) = endpoint(db, other, scope)? {
            merge(
                &mut endpoint,
                json!({"relation":row["relation"],"direction":if out{"out"}else{"in"},"relation_id":row["id"],"basis":"pending_relation"}),
            );
            connections.push(endpoint);
        }
        if connections.len() == 5 {
            break;
        }
    }
    let hints = format!(
        "SELECT m.target,n.type,substr(n.text,1,160) AS text FROM candidate_entity_matches m JOIN node_versions n ON n.id=m.target WHERE m.candidate_id=?1 AND m.name_match=1 AND n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) AND {} AND NOT EXISTS(SELECT 1 FROM entity_merges e WHERE e.source=n.id) AND NOT EXISTS(SELECT 1 FROM forgotten_targets f WHERE f.target=n.id) AND (?2 IS NULL OR EXISTS(SELECT 1 FROM json_each(n.scope) WHERE value=?2)) ORDER BY m.target LIMIT 2",
        visible_sql("n.id")
    );
    for row in rows(db, &hints, params![id, scope])? {
        if connections.len() == 5 {
            break;
        }
        connections.push(json!({"id":row["target"],"type":row["type"],"text":row["text"],"relation":"possibly_same_as","direction":"out","relation_id":null,"basis":"candidate_entity_match"}));
    }
    Ok(Some(compose(
        record,
        item["text"].as_str().unwrap(),
        connections,
        None,
    )?))
}
fn compose(
    mut record: Value,
    name: &str,
    connections: Vec<Value>,
    alias: Option<&str>,
) -> Result<Value> {
    let clean = |s: &str| s.split_whitespace().collect::<Vec<_>>().join(" ");
    let labels = record["scope"]
        .as_array()
        .ok_or(Error::Unavailable("Entity scope is unavailable"))?
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join("/");
    let mut head = format!(
        "{} [{}; {}]",
        clean(name),
        record["type"].as_str().unwrap(),
        labels
    );
    if let Some(alias) = alias {
        head.push_str(&format!(
            " (matched alias {})",
            serde_json::to_string(&clean(alias))?
        ));
    }
    let mut lines = Vec::new();
    for item in &connections {
        let reference = match item["basis"].as_str() {
            Some("edge") => format!("edge {}", item["relation_id"].as_str().unwrap()),
            Some("pending_relation") => {
                format!("pending {}", item["relation_id"].as_str().unwrap())
            }
            _ => format!("memory {}", item["id"].as_str().unwrap()),
        };
        let other = format!(
            "{} [{}; {}]",
            clean(item["text"].as_str().unwrap())
                .chars()
                .take(60)
                .collect::<String>(),
            item["type"].as_str().unwrap(),
            reference
        );
        lines.push(if item["basis"] == "candidate_entity_match" {
            format!("possibly the same as durable {other} (unverified name match)")
        } else if item["direction"] == "out" {
            format!("{} {other}", item["relation"].as_str().unwrap())
        } else {
            format!("{other} {} this", item["relation"].as_str().unwrap())
        });
    }
    let body = if lines.is_empty() {
        "No recorded connections.".into()
    } else {
        format!("Connections: {}.", lines.join("; "))
    };
    merge(
        &mut record,
        json!({"text":format!("{head} {body}"),"category":"entity_context","match":"entity_name","connections":connections,"cosine":null,"score":1.0,"related":[]}),
    );
    record["duplicate_ids"] = json!([]);
    Ok(record)
}

/// Recorded-connection context uses the same bounded renderer as exact name anchors.
pub fn from_connection(
    db: &Connection,
    item: &Value,
    scope: Option<&str>,
) -> Result<Option<Value>> {
    durable(db, item, scope)
}
