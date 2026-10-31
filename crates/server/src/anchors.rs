//! Literal name context; partial-name relevance never establishes identity.
use crate::records::{self, merge, one, rows};
use memento_ann::eligibility::{ACTIVE_CANDIDATE, legacy_only, visible_sql};
use memento_core::{Error, Result};
use memento_domain::unicode::{name_contained as contained, name_tokens as tokens};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashSet};
/// Existing tokenizer, shared by full-name anchors and grounded one-hop subject admission.
pub fn contains_name(name: &str, text: &str) -> bool {
    contained(name, &tokens(text))
}
pub fn connection_name_matches(db: &Connection, item: &Value, query: &str) -> Result<bool> {
    let request = tokens(query);
    let overlaps = |name: &str| {
        tokens(name_head(name))
            .iter()
            .any(|token| request.contains(token))
    };
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
    let found = exact(db, &request, scope)?;
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
/// Whole-name rows by id; the first index to find an id keeps it. Each index
/// ranks its top 64 by bm25 then id, and only kept rows load summary columns.
fn exact(
    db: &Connection,
    request: &[String],
    scope: Option<&str>,
) -> Result<BTreeMap<String, Value>> {
    let mut found = BTreeMap::new();
    let mut seen = HashSet::new();
    let terms = request
        .iter()
        .filter(|t| seen.insert((*t).clone()))
        .take(64)
        .collect::<Vec<_>>();
    if terms.is_empty() {
        return Ok(found);
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
    let node = format!(
        "SELECT {},{count},?3 AS alias,'durable' AS tier FROM node_versions n WHERE n.id=?1 AND n.version=?2",
        records::NODE_SUMMARY
    );
    let missing = || Error::Unavailable("Entity name row is unavailable");
    for row in rows(
        db,
        &format!(
            "SELECT n.id,n.version,substr(n.text,1,240) AS text FROM node_fts JOIN node_versions n ON n.id=node_fts.id AND n.version=node_fts.version WHERE node_fts MATCH ?1 AND {visible} ORDER BY bm25(node_fts),n.id LIMIT 64"
        ),
        params![matched, scope],
    )? {
        let id = row["id"].as_str().unwrap();
        if contained(row["text"].as_str().unwrap_or(""), request) && !found.contains_key(id) {
            let full = one(db, &node, params![id, row["version"].as_i64(), None::<&str>])?;
            found.insert(id.to_owned(), full.ok_or_else(missing)?);
        }
    }
    let aliases = format!(
        "WITH RECURSIVE canonical(source,target,depth) AS (SELECT source,target,0 FROM entity_merges UNION ALL SELECT c.source,m.superseded_by,c.depth+1 FROM canonical c JOIN node_versions m ON m.id=c.target AND m.state IN ('merged','refined') AND m.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=m.id) WHERE m.superseded_by IS NOT NULL AND c.depth<64) SELECT n.id,n.version,alias_fts.text AS alias FROM alias_fts JOIN canonical a ON a.source=alias_fts.id JOIN node_versions n ON n.id=a.target WHERE alias_fts MATCH ?1 AND {visible} ORDER BY bm25(alias_fts),n.id LIMIT 64"
    );
    for row in rows(db, &aliases, params![matched, scope])? {
        let (id, alias) = (row["id"].as_str().unwrap(), row["alias"].as_str());
        if contained(alias.unwrap_or(""), request) && !found.contains_key(id) {
            let full = one(db, &node, params![id, row["version"].as_i64(), alias])?;
            found.insert(id.to_owned(), full.ok_or_else(missing)?);
        }
    }
    let candidate = "SELECT c.id,c.type,c.text,c.scope,'candidate' AS tier,(SELECT max(julianday(e.occurred_at)) FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id WHERE g.target=c.id AND g.version=1 AND e.occurred_at IS NOT NULL) AS source_at FROM candidates c WHERE c.id=?1";
    for row in rows(
        db,
        &format!(
            "SELECT c.id,c.text FROM candidate_fts JOIN candidates c ON c.id=candidate_fts.id WHERE candidate_fts MATCH ?1 AND c.kind='entity' AND {ACTIVE_CANDIDATE} AND (?2 IS NULL OR EXISTS(SELECT 1 FROM json_each(c.scope) WHERE value=?2)) ORDER BY bm25(candidate_fts),c.id LIMIT 64"
        ),
        params![matched, scope],
    )? {
        let id = row["id"].as_str().unwrap();
        if contained(row["text"].as_str().unwrap_or(""), request) && !found.contains_key(id) {
            let full = one(db, candidate, [id])?;
            found.insert(id.to_owned(), full.ok_or_else(missing)?);
        }
    }
    Ok(found)
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
    // Up to two behavioral lessons claim connection slots ahead of the arbitrary
    // eight-edge related() window; related() itself is unchanged for other callers.
    let mut picked = lessons(db, id, scope)?;
    for row in records::related(db, id, 1, scope)? {
        if picked.len() == 5 {
            break;
        }
        if !picked.iter().any(|lesson| lesson["id"] == row["id"]) {
            picked.push(row);
        }
    }
    let connections=picked.into_iter().map(|row|json!({"id":row["id"],"type":row["type"],"relation":row["relation"],"direction":row["direction"],"relation_id":row["edge_id"],"basis":"edge","text":row["one_line"]})).collect();
    Ok(Some(compose(
        record,
        item["text"].as_str().unwrap(),
        connections,
        item["alias"].as_str(),
    )?))
}
/// At most two linked behavior-bearing experience neighbors, by importance then id.
/// Bounded: twelve keyed adjacency rows, each with one behavior lookup.
fn lessons(db: &Connection, id: &str, scope: Option<&str>) -> Result<Vec<Value>> {
    let sql = format!(
        "SELECT n.id,n.version,n.type,substr(n.text,1,160) AS one_line,a.rel AS relation,a.direction,a.id AS edge_id FROM (SELECT e.id,e.dst AS target,e.rel,'out' AS direction FROM edge_versions e WHERE e.src=?2 AND e.state='active' AND e.version=(SELECT max(v.version) FROM edge_versions v WHERE v.id=e.id) UNION ALL SELECT e.id,e.src AS target,e.rel,'in' AS direction FROM edge_versions e WHERE e.dst=?2 AND e.state='active' AND e.version=(SELECT max(v.version) FROM edge_versions v WHERE v.id=e.id)) a JOIN node_versions n ON n.id=a.target WHERE n.type='experience' AND n.id<>?2 AND n.state='active' AND n.version=(SELECT max(v.version) FROM node_versions v WHERE v.id=n.id) AND NOT EXISTS(SELECT 1 FROM forgotten_targets f WHERE f.target=n.id) AND {} AND (?1 IS NULL OR EXISTS(SELECT 1 FROM json_each(n.scope) WHERE value=?1)) ORDER BY coalesce((SELECT importance FROM node_stats WHERE node_id=n.id),0.5) DESC,n.id,a.id LIMIT 12",
        visible_sql("n.id")
    );
    let mut result: Vec<Value> = Vec::new();
    for row in rows(db, &sql, params![scope, id])? {
        let lesson = row["id"].as_str().unwrap();
        if result.iter().any(|item| item["id"] == lesson)
            || legacy_only(db, lesson, row["version"].as_i64().unwrap())?
            || memento_domain::behavior(db, lesson)?.is_none()
        {
            continue;
        }
        result.push(row);
        if result.len() == 2 {
            break;
        }
    }
    Ok(result)
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
            "SELECT c.id,c.type AS relation,c.src,c.dst FROM candidates c WHERE c.kind='relation' AND c.id IN (SELECT id FROM candidates WHERE kind='relation' AND src=?1 UNION SELECT id FROM candidates WHERE kind='relation' AND dst=?1) AND {ACTIVE_CANDIDATE} AND (?2 IS NULL OR EXISTS(SELECT 1 FROM json_each(c.scope) WHERE value=?2)) ORDER BY c.id LIMIT 32"
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

/// Read-only discovery name head. Gloss text is neither a name token nor an alias.
/// Stored text and identity matching remain unchanged.
fn name_head(text: &str) -> &str {
    text.split(['—', '–', ':', '\n', '(', ';', ','])
        .next()
        .unwrap_or("")
        .split(" - ")
        .next()
        .unwrap_or("")
        .trim()
}
fn quoted(token: &str) -> String {
    format!("\"{}\"", token.replace('"', "\"\""))
}
struct Partial {
    item: Value,
    rare: Vec<String>,
    cosine: f64,
    pure_name: bool,
}
fn partials(
    db: &Connection,
    query: &str,
    vector: &[f32; 768],
    scope: Option<&str>,
    floor: f64,
) -> Result<Vec<Partial>> {
    use rusqlite::OptionalExtension;
    let query_tokens = tokens(query);
    let request = query_tokens
        .iter()
        .take(64)
        .cloned()
        .collect::<HashSet<_>>();
    if request.is_empty() {
        return Ok(Vec::new());
    }
    let mut terms = request.iter().map(|s| quoted(s)).collect::<Vec<_>>();
    terms.sort();
    let matched = terms.join(" OR ");
    let active = format!(
        "n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) AND {} AND NOT EXISTS(SELECT 1 FROM forgotten_targets f WHERE f.target=n.id) AND (?2 IS NULL OR EXISTS(SELECT 1 FROM json_each(n.scope) WHERE value=?2))",
        visible_sql("n.id")
    );
    let entity = "(n.type IN ('person','organization','project','concept','place','tool') OR EXISTS(SELECT 1 FROM memory_types t WHERE t.name=n.type AND t.category='entity'))";
    let eligible = format!(
        "{active} AND {entity} AND NOT EXISTS(SELECT 1 FROM entity_merges m WHERE m.source=n.id)"
    );
    let count = "(SELECT count(*) FROM graph_evidence g WHERE g.target=n.id AND g.version=n.version) AS evidence_count";
    let mut found = BTreeMap::<String, (Value, HashSet<String>, bool)>::new();
    let mut collect = |row: Value, name: &str| {
        let name = tokens(name_head(name));
        if name.len() < 2 {
            return;
        }
        // Intent guard uses the entire query, not the bounded FTS discovery prefix.
        // One stored/reviewed name variant must account for every literal token.
        let pure_name = query_tokens.iter().all(|token| name.contains(token));
        let overlap = name
            .into_iter()
            .filter(|t| request.contains(t))
            .collect::<HashSet<_>>();
        if overlap.is_empty() {
            return;
        }
        let id = row["id"].as_str().unwrap().to_owned();
        let entry = found
            .entry(id)
            .or_insert_with(|| (row, HashSet::new(), false));
        entry.1.extend(overlap);
        entry.2 |= pure_name;
    };
    for row in rows(
        db,
        &format!(
            "SELECT {},n.text AS name,{count},NULL AS alias,'durable' AS tier FROM node_fts JOIN node_versions n ON n.id=node_fts.id AND n.version=node_fts.version WHERE node_fts MATCH ?1 AND {eligible} ORDER BY bm25(node_fts),n.id LIMIT 64",
            records::NODE_SUMMARY
        ),
        params![matched, scope],
    )? {
        let name = row["name"].as_str().unwrap().to_owned();
        collect(row, &name);
    }
    let aliases = format!(
        "WITH RECURSIVE canonical(source,target,depth) AS (SELECT source,target,0 FROM entity_merges UNION ALL SELECT c.source,m.superseded_by,c.depth+1 FROM canonical c JOIN node_versions m ON m.id=c.target AND m.state IN ('merged','refined') AND m.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=m.id) WHERE m.superseded_by IS NOT NULL AND c.depth<64) SELECT {},n.text AS name,{count},alias_fts.text AS alias,'durable' AS tier FROM alias_fts JOIN canonical a ON a.source=alias_fts.id JOIN node_versions n ON n.id=a.target WHERE alias_fts MATCH ?1 AND {eligible} ORDER BY bm25(alias_fts),n.id LIMIT 64",
        records::NODE_SUMMARY
    );
    for row in rows(db, &aliases, params![matched, scope])? {
        let alias = row["alias"].as_str().unwrap().to_owned();
        collect(row, &alias);
    }
    for row in rows(
        db,
        &format!(
            "SELECT c.id,1 AS version,c.type,c.text,c.text AS name,c.scope,'candidate' AS tier,(SELECT max(julianday(e.occurred_at)) FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id WHERE g.target=c.id AND g.version=1 AND e.occurred_at IS NOT NULL) AS source_at FROM candidate_fts JOIN candidates c ON c.id=candidate_fts.id WHERE candidate_fts MATCH ?1 AND c.kind='entity' AND {ACTIVE_CANDIDATE} AND (?2 IS NULL OR EXISTS(SELECT 1 FROM json_each(c.scope) WHERE value=?2)) ORDER BY bm25(candidate_fts),c.id LIMIT 64"
        ),
        params![matched, scope],
    )? {
        let name = row["name"].as_str().unwrap().to_owned();
        collect(row, &name);
    }
    if found.is_empty() {
        return Ok(Vec::new());
    }
    let active_count: i64 = db.query_row(
        &format!("SELECT count(*) FROM node_versions n WHERE {active}"),
        params![Option::<i64>::None, scope],
        |r| r.get(0),
    )?;
    let threshold = 16.max(active_count / 100);
    let mut rarity = BTreeMap::new();
    let query_norm = vector
        .iter()
        .map(|v| (*v as f64).powi(2))
        .sum::<f64>()
        .sqrt();
    if !query_norm.is_finite() || query_norm <= 0. {
        return Ok(Vec::new());
    }
    let mut result = Vec::new();
    for (_, (mut item, overlap, pure_name)) in found {
        let id = item["id"].as_str().unwrap();
        let version = item["version"].as_i64().unwrap();
        if legacy_only(db, id, version)? {
            continue;
        }
        let mut rare = Vec::new();
        for token in &overlap {
            if !rarity.contains_key(token) {
                let df:i64=db.query_row(&format!("SELECT count(*) FROM (SELECT n.id FROM node_fts JOIN node_versions n ON n.id=node_fts.id AND n.version=node_fts.version WHERE node_fts MATCH ?1 AND {active} LIMIT ?3)"),params![quoted(token),scope,threshold+1],|r|r.get(0))?;
                rarity.insert(token.clone(), df <= threshold);
            }
            if rarity[token] {
                rare.push(token.clone());
            }
        }
        if rare.is_empty() {
            continue;
        }
        // Keyed current-vector reads only; no whole-vector scan and no new ANN mask.
        let kind = if item["tier"] == "durable" {
            "node"
        } else {
            "candidate"
        };
        let bytes:Option<Vec<u8>>=db.query_row("SELECT b.embedding FROM vector_items i JOIN vector_blobs b ON b.seq=i.seq WHERE i.target=? AND i.kind=? AND i.version=? AND b.current=1 ORDER BY i.seq LIMIT 1",params![id,kind,version],|r|r.get(0)).optional()?;
        let Some(bytes) = bytes else { continue };
        let values = memento_core::F32Blob::from_bytes(bytes)?.values();
        let norm = values
            .iter()
            .map(|v| (*v as f64).powi(2))
            .sum::<f64>()
            .sqrt()
            * query_norm;
        let cosine = values
            .iter()
            .zip(vector)
            .map(|(a, b)| *a as f64 * *b as f64)
            .sum::<f64>()
            / norm;
        if !norm.is_finite() || norm <= 0. || !cosine.is_finite() || cosine < floor {
            continue;
        }
        item.as_object_mut().unwrap().remove("name");
        result.push(Partial {
            item,
            rare,
            cosine: cosine.clamp(-1., 1.) as f32 as f64,
            pure_name,
        });
    }
    result.sort_by(|a, b| {
        (b.item["tier"] == "durable")
            .cmp(&(a.item["tier"] == "durable"))
            .then_with(|| b.cosine.total_cmp(&a.cosine))
            .then_with(|| {
                b.item["evidence_count"]
                    .as_i64()
                    .unwrap_or(0)
                    .cmp(&a.item["evidence_count"].as_i64().unwrap_or(0))
            })
            .then_with(|| {
                b.item["source_at"]
                    .as_f64()
                    .unwrap_or(f64::NEG_INFINITY)
                    .total_cmp(&a.item["source_at"].as_f64().unwrap_or(f64::NEG_INFINITY))
            })
            .then_with(|| a.item["id"].as_str().cmp(&b.item["id"].as_str()))
    });
    Ok(result)
}
/// Add relevance-qualified partial-name contexts after the actually retained facts/T2.
/// No identity decision, inferred edge or basis_memory_id is made by this path.
pub fn add_partials(
    db: &Connection,
    query: &str,
    vector: &[f32; 768],
    scope: Option<&str>,
    floor: f64,
    facts: &[Value],
    output: &mut Vec<Value>,
    limit: usize,
) -> Result<()> {
    let budget = 2.min(limit);
    let contexts = |rows: &[Value]| {
        rows.iter()
            .filter(|r| r["category"] == "entity_context")
            .count()
    };
    if budget == 0 || contexts(output) >= budget {
        return Ok(());
    }
    let candidates = partials(db, query, vector, scope, floor)?;
    let mut spelled = BTreeMap::<(String, String), Vec<String>>::new();
    for candidate in &candidates {
        spelled
            .entry((
                candidate.item["type"].as_str().unwrap().into(),
                identity(candidate.item["text"].as_str().unwrap()),
            ))
            .or_default()
            .push(candidate.item["id"].as_str().unwrap().into());
    }
    for candidate in candidates {
        if contexts(output) >= budget {
            break;
        }
        let id = candidate.item["id"].as_str().unwrap();
        if output
            .iter()
            .any(|r| r["id"] == id && r["category"] == "entity_context")
        {
            continue;
        }
        let retained = facts
            .iter()
            .filter(|f| output.iter().any(|r| r["id"] == f["id"]))
            .collect::<Vec<_>>();
        let shares = |fact: &Value, terms: &[String]| {
            // finish() reads this field from the same snapshot's immutable assertion.
            // Rendered text can include Source/Connected wrappers and is not corroboration.
            let Some(assertion) = fact["_full_text"].as_str() else {
                return false;
            };
            let text = tokens(assertion);
            terms.iter().any(|t| text.contains(t))
        };
        let corroborator = retained
            .iter()
            .filter(|f| shares(f, &candidate.rare))
            .min_by_key(|f| output.iter().position(|r| r["id"] == f["id"]).unwrap());
        // A higher entity cosine does not establish partial-name intent: e.g. a
        // modal/verb homograph can narrowly beat an unrelated retained fact.
        if corroborator.is_none() && !candidate.pure_name {
            continue;
        }
        let mut proposed = output
            .iter()
            .filter(|r| r["id"] != id)
            .cloned()
            .collect::<Vec<_>>();
        let first_fact = proposed
            .iter()
            .position(|r| retained.iter().any(|f| f["id"] == r["id"]));
        let after = if let Some(fact) = corroborator {
            proposed.iter().position(|r| r["id"] == fact["id"])
        } else {
            first_fact
        };
        let mut index = after.map(|i| i + 1).unwrap_or_else(|| {
            proposed
                .iter()
                .take_while(|r| r["category"] == "entity_context")
                .count()
        });
        if let Some(fact_index) = after {
            let basis = proposed[fact_index]["id"].clone();
            while index < proposed.len()
                && (proposed[index]["basis_memory_id"] == basis
                    || proposed[index]["match"] == "partial_entity_name")
            {
                index += 1;
            }
        }
        if corroborator.is_none() {
            while index < proposed.len()
                && proposed[index]["cosine"]
                    .as_f64()
                    .is_some_and(|c| c >= candidate.cosine)
            {
                index += 1;
            }
        }
        if index >= limit {
            continue;
        }
        let rendered = if candidate.item["tier"] == "durable" {
            durable(db, &candidate.item, scope)?
        } else {
            pending(db, &candidate.item, scope)?
        };
        let Some(mut anchor) = rendered else { continue };
        let same = &spelled[&(
            candidate.item["type"].as_str().unwrap().into(),
            identity(candidate.item["text"].as_str().unwrap()),
        )];
        if same.len() > 1 {
            anchor["same_name_count"] = json!(same.len());
            anchor["same_name_ids"] = json!(
                same.iter()
                    .filter(|other| other.as_str() != id)
                    .collect::<Vec<_>>()
            );
            anchor["text"] = json!(format!(
                "One of {} separate entries spelled alike; not merged. {}",
                same.len(),
                anchor["text"].as_str().unwrap()
            ));
        }
        anchor["match"] = json!("partial_entity_name");
        anchor["cosine"] = json!(candidate.cosine);
        anchor["score"] = json!(candidate.cosine);
        proposed.insert(index, anchor);
        proposed.truncate(limit);
        *output = proposed;
    }
    Ok(())
}

#[cfg(test)]
mod lesson_slot_tests {
    use super::*;
    use memento_core::{CommandContext, Store};

    #[test]
    fn linked_behavior_lesson_claims_a_connection_slot_on_a_hub_entity() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data")).unwrap();
        let text = "For Hubtool deploys, run the dry-run first.";
        let source = store.capture(serde_json::from_value(json!({"source":"synthetic","event_id":"lesson","conversation_id":"lesson","kind":"user","text":text,"metadata":{}})).unwrap()).unwrap();
        store.write(|tx| {
            let db = tx.sql();
            db.execute("INSERT INTO graph_commits VALUES('manual','manual','{}','{}','owner','Explicit manual relation fixture','2026-10-26T00:00:00Z')", [])?;
            db.execute("INSERT INTO node_versions VALUES('hub',1,'manual','tool','Hubtool','active',NULL,NULL,NULL,0,'[\"professional\"]')", [])?;
            // Ten peer edges whose ids sort before the lesson edge fill related()'s eight-edge window.
            for i in 0..10 {
                let peer = format!("peer{i:02}");
                db.execute("INSERT INTO node_versions VALUES(?1,1,'manual','person',?1,'active',NULL,NULL,NULL,0,'[\"professional\"]')", [&peer])?;
                db.execute("INSERT INTO edge_versions VALUES(?,1,'manual','hub','related_to',?,'active')", params![format!("e{i:02}"), peer])?;
            }
            Ok(())
        }).unwrap();
        // Behavior comes only from the consolidation commit that produced the version.
        let context = CommandContext { command_id: "lesson".into(), actor: "fixture".into(), why: "Synthetic behavior provenance".into() };
        let request = json!({"op":"consolidate","output":{"actions":[{"candidate_id":"lesson","operation":"remember","type":"experience","behavior":{"authority":"explicit_user","applicability":"Hubtool deploys","conduct":text,"exceptions":[],"rationale":"Exact fixture user instruction"}}]}});
        store.command(&context, &request, |tx, commit| {
            tx.touch_target("lesson-node")?;
            tx.sql().execute("INSERT INTO node_versions VALUES('lesson-node',1,?,'experience',?,'active',NULL,NULL,NULL,0,'[\"professional\"]')", params![commit, text])?;
            tx.sql().execute("INSERT INTO graph_evidence(target,version,episode_id,start,end,quote) VALUES('lesson-node',1,?,0,?,?)", params![source.id, text.chars().count() as i64, text])?;
            tx.sql().execute("INSERT INTO edge_versions VALUES('z-lesson',1,?,'lesson-node','mentions','hub','active')", [commit])?;
            Ok(json!({"results":{"lesson":{"id":"lesson-node"}}}))
        }).unwrap();
        store.read(|tx| {
            let db = tx.sql();
            assert!(memento_domain::behavior(db, "lesson-node")?.is_some());
            // related() keeps its arbitrary first-eight-edges semantics for other callers.
            let related = records::related(db, "hub", 1, None)?;
            assert_eq!(related.len(), 8);
            assert!(!related.iter().any(|row| row["id"] == "lesson-node"));
            let found = anchors(db, "How do Hubtool deploys work?", 2, None)?;
            let anchor = found.iter().find(|anchor| anchor["id"] == "hub").unwrap();
            let connections = anchor["connections"].as_array().unwrap();
            assert_eq!(connections.len(), 5);
            assert_eq!(connections[0]["id"], "lesson-node");
            assert_eq!(connections[0]["relation"], "mentions");
            assert_eq!(connections[0]["direction"], "in");
            assert_eq!(connections[0]["relation_id"], "z-lesson");
            assert!(connections[1..].iter().all(|row| row["type"] == "person"));
            assert!(anchor["text"].as_str().unwrap().contains("[experience; edge z-lesson] mentions this"));
            Ok(())
        }).unwrap();
    }
}

#[cfg(test)]
mod exact_name_equivalence_tests {
    use super::*;
    use memento_core::Store;
    use std::time::{Duration, Instant};

    // Verbatim pre-change anchors(), renamed; rendering helpers are unchanged.
    fn anchors_before(
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

    // The same pre-change discovery, stopped before rendering.
    fn found_before(db: &Connection, query: &str, scope: Option<&str>) -> Result<BTreeMap<String, Value>> {
        let request = tokens(query);
        let mut seen = HashSet::new();
        let terms = request
            .iter()
            .filter(|t| seen.insert((*t).clone()))
            .take(64)
            .collect::<Vec<_>>();
        if terms.is_empty() {
            return Ok(BTreeMap::new());
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
        Ok(found)
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self, n: usize) -> usize {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (self.0 >> 33) as usize % n
        }
    }
    // Limit groups use names absent from the random vocabulary.
    const NAMES: [&str; 9] = ["Nova", "Kim", "Lee", "Park", "Delta", "Echo", "River", "Stone", "Mila"];
    const TYPES: [&str; 8] = ["person", "organization", "project", "tool", "concept", "custom_ent", "custom_mem", "fact"];
    const SCOPES: [&str; 3] = ["[\"personal\"]", "[\"professional\"]", "[\"personal\",\"professional\"]"];

    /// Random entities plus three groups over the 64-row limit of each index:
    /// 90 same-spelled "Acme" nodes, 80 merged sources aliased "Orion", 80 "Zeta" candidates.
    fn fixture(store: &Store, extra: usize) {
        let episodes = (0..3)
            .map(|i| {
                store
                    .capture(serde_json::from_value(json!({"source":"synthetic","event_id":format!("anchor-{i}"),"conversation_id":"anchor","kind":"user","text":format!("Anchor fixture {i}"),"occurred_at":format!("2026-0{}-01T00:00:00Z", i + 1),"metadata":{}})).unwrap())
                    .unwrap()
                    .id
            })
            .collect::<Vec<_>>();
        store.write(|tx| {
            let db = tx.sql();
            let mut rng = Rng(7);
            for (i, at) in ["2026-01-01", "2026-02-01", "2026-03-01"].iter().enumerate() {
                db.execute("INSERT INTO graph_commits VALUES(?1,?1,'{}','{}','owner','Name fixture',?2)", params![format!("c{i}"), format!("{at}T00:00:00Z")])?;
            }
            db.execute("INSERT INTO memory_types VALUES('custom_ent','entity',0,'c0')", [])?;
            db.execute("INSERT INTO memory_types VALUES('custom_mem','memory',0,'c0')", [])?;
            let node = |id: &str, version: i64, kind: &str, text: &str, state: &str, superseded: Option<&str>, scope: &str, commit: usize| {
                db.execute("INSERT INTO node_versions VALUES(?1,?2,?3,?4,?5,?6,NULL,NULL,?7,?8,?9)", params![id, version, format!("c{commit}"), kind, text, state, superseded, (version % 2) as i64, scope])
            };
            let name = |rng: &mut Rng| {
                (0..1 + rng.next(3)).map(|_| NAMES[rng.next(NAMES.len())]).collect::<Vec<_>>().join(" ")
            };
            let mut ids = Vec::new();
            for i in 0..400 + extra {
                let id = format!("n{i:05}");
                let mut text = name(&mut rng);
                if rng.next(5) == 0 {
                    text.push_str(" — the deploy tool for recall memory work");
                }
                let kind = TYPES[rng.next(TYPES.len())];
                let scope = SCOPES[rng.next(3)];
                if rng.next(6) == 0 {
                    node(&id, 1, kind, &name(&mut rng), "active", None, scope, 0)?;
                    node(&id, 2, kind, &text, if rng.next(8) == 0 { "retracted" } else { "active" }, None, scope, 1)?;
                } else {
                    node(&id, 1, kind, &text, "active", None, scope, rng.next(3))?;
                }
                for k in 0..rng.next(5) {
                    db.execute("INSERT INTO graph_evidence SELECT id,max(version),?2,?3,?3,'q' FROM node_versions WHERE id=?1", params![id, format!("ep{k}"), k as i64])?;
                }
                if rng.next(2) == 0 {
                    db.execute("INSERT INTO node_stats(node_id,importance,decay,archived,access_count,last_cited_at) VALUES(?,?,?,?,?,?)", params![id, rng.next(10) as f64 / 10., rng.next(10) as f64 / 10., rng.next(2) as i64, rng.next(9) as i64, "2026-10-01T00:00:00Z"])?;
                }
                if rng.next(20) == 0 {
                    db.execute("INSERT INTO forgotten_targets VALUES(?,'c0','node')", [&id])?;
                }
                ids.push(id);
            }
            // Random merge chains source -> target (-> next) with random-name aliases.
            for i in 0..40 {
                let source = format!("m{i:03}");
                let target = ids[rng.next(ids.len())].clone();
                node(&source, 1, "person", &name(&mut rng), "active", None, SCOPES[rng.next(3)], 0)?;
                node(&source, 2, "person", &name(&mut rng), "merged", Some(target.as_str()), SCOPES[rng.next(3)], 1)?;
                db.execute("INSERT INTO entity_merges VALUES(?,?,'c1')", params![source, target])?;
                for _ in 0..1 + rng.next(2) {
                    db.execute("INSERT INTO alias_fts(id,text) VALUES(?,?)", params![source, name(&mut rng)])?;
                }
            }
            // Two-hop chain: m-a -> m-b (merged into n00001) carries one alias.
            node("m-a", 1, "person", "Echo Stone", "merged", Some("m-b"), SCOPES[0], 0)?;
            node("m-b", 1, "person", "Echo River", "refined", Some("n00001"), SCOPES[0], 0)?;
            db.execute("INSERT INTO entity_merges VALUES('m-a','m-b','c1')", [])?;
            db.execute("INSERT INTO alias_fts(id,text) VALUES('m-a','Echo Stone Mila')", [])?;
            // Same target, two aliases tied on bm25.
            node("t-tie", 1, "person", "Tie target", "active", None, SCOPES[2], 0)?;
            for (source, alias) in [("s-tie1", "Mila Stone"), ("s-tie2", "Stone Mila")] {
                node(source, 1, "person", alias, "merged", Some("t-tie"), SCOPES[2], 0)?;
                db.execute("INSERT INTO entity_merges VALUES(?,'t-tie','c1')", [source])?;
                db.execute("INSERT INTO alias_fts(id,text) VALUES(?,?)", params![source, alias])?;
            }
            node("lp1", 1, "person", "Lee Park", "active", None, SCOPES[0], 0)?;
            node("lp2", 1, "person", "Lee  park", "active", None, SCOPES[1], 2)?;
            for i in 0..90 {
                node(&format!("acme{i:03}"), 1, "organization", "Acme", "active", None, SCOPES[i % 3], i % 3)?;
                for k in 0..i % 4 {
                    db.execute("INSERT INTO graph_evidence VALUES(?,1,?,0,1,'q')", params![format!("acme{i:03}"), format!("ep{k}")])?;
                }
            }
            for i in 0..80 {
                let (source, target) = (format!("src{i:03}"), format!("tgt{i:03}"));
                node(&target, 1, "person", &format!("Target person {i}"), "active", None, SCOPES[i % 3], 0)?;
                node(&source, 1, "person", "Old Orion", "active", None, SCOPES[i % 3], 0)?;
                node(&source, 2, "person", "Old Orion", "merged", Some(target.as_str()), SCOPES[i % 3], 1)?;
                db.execute("INSERT INTO entity_merges VALUES(?,?,'c1')", params![source, target])?;
                db.execute("INSERT INTO alias_fts(id,text) VALUES(?,'Orion')", [&source])?;
                if i % 10 == 0 {
                    db.execute("INSERT INTO alias_fts(id,text) VALUES(?,'Orion Belt')", [&source])?;
                }
            }
            db.execute("INSERT INTO extraction_runs VALUES('run0','c0',?,'fixture','fixture','[]','{}','2026-01-01T00:00:00Z')", [&episodes[0]])?;
            let candidate = |id: &str, kind: &str, kind_type: &str, text: &str, src: Option<&str>, dst: Option<&str>, scope: &str| {
                db.execute("INSERT INTO candidates VALUES(?1,'run0',?2,?3,?4,'Fixture',?5,?6,0.8,NULL,NULL,?7)", params![id, kind, kind_type, text, src, dst, scope])
            };
            for i in 0..80 {
                let id = format!("zeta{i:03}");
                candidate(&id, "entity", "person", "Zeta", None, None, SCOPES[i % 3])?;
                if i % 4 == 0 {
                    db.execute("INSERT INTO graph_evidence VALUES(?,1,?,0,1,'q')", params![id, episodes[i % 3]])?;
                }
            }
            for i in 0..60 {
                let id = format!("cand{i:03}");
                candidate(&id, if i % 5 == 0 { "memory" } else { "entity" }, "person", &name(&mut rng), None, None, SCOPES[rng.next(3)])?;
                if rng.next(3) == 0 {
                    db.execute("INSERT INTO graph_evidence VALUES(?,1,?,0,1,'q')", params![id, episodes[rng.next(3)]])?;
                }
                if rng.next(4) == 0 {
                    db.execute("INSERT INTO candidate_entity_matches VALUES(?,?,1,0.9,1,'fixture','Fixture','2026-01-01T00:00:00Z')", params![id, ids[rng.next(ids.len())]])?;
                }
            }
            for i in 0..20 {
                let (src, dst) = (format!("cand{:03}", rng.next(60)), format!("cand{:03}", rng.next(60)));
                candidate(&format!("rel{i:03}"), "relation", "knows", "relation", Some(src.as_str()), Some(dst.as_str()), SCOPES[rng.next(3)])?;
            }
            for (i, (candidate_id, decision)) in [("zeta000", "promote"), ("zeta001", "reject"), ("zeta002", "defer"), ("cand001", "promote"), ("cand002", "reject")].iter().enumerate() {
                db.execute("INSERT INTO candidate_decisions VALUES(?,?,'c0',?,?,NULL)", params![format!("d{i}"), candidate_id, decision, (*decision == "promote").then_some("n00002")])?;
            }
            for i in 0..300 {
                db.execute("INSERT INTO edge_versions VALUES(?,1,'c0',?,'related_to',?,'active')", params![format!("e{i:03}"), ids[rng.next(ids.len())], ids[rng.next(ids.len())]])?;
            }
            Ok(())
        })
        .unwrap();
    }
    fn queries() -> Vec<String> {
        let mut result = [
            "Acme", "tell me about ACME", "Orion", "Orion Belt and Orion", "Zeta", "zeta acme orion",
            "Lee Park", "lee park and Lee Park", "Mila Stone Mila", "Echo Stone Mila", "What did Lee Park and Kim say about Nova River?",
            "", "the", "\"Acme\" OR Zeta", "deploy tool for recall memory work",
        ]
        .map(String::from)
        .to_vec();
        let mut rng = Rng(11);
        let filler = ["about", "the", "and", "what", "Acme", "Orion", "Zeta"];
        for _ in 0..40 {
            let words = (0..2 + rng.next(7))
                .map(|_| if rng.next(3) == 0 { filler[rng.next(filler.len())] } else { NAMES[rng.next(NAMES.len())] })
                .collect::<Vec<_>>();
            result.push(words.join(" "));
        }
        result
    }

    #[test]
    fn top_64_then_hydrate_matches_previous_name_anchors() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data")).unwrap();
        fixture(&store, 0);
        store
            .read(|tx| {
                let db = tx.sql();
                let mut compared = 0;
                for query in queries() {
                    for scope in [None, Some("personal"), Some("professional"), Some("absent")] {
                        let before = found_before(db, &query, scope)?;
                        let after = exact(db, &tokens(&query), scope)?;
                        assert_eq!(serde_json::to_string(&before)?, serde_json::to_string(&after)?, "{query:?} {scope:?}");
                        for limit in [0, 1, 2, 6] {
                            let before = anchors_before(db, &query, limit, scope)?;
                            let after = anchors(db, &query, limit, scope)?;
                            assert_eq!(serde_json::to_string(&before)?, serde_json::to_string(&after)?, "{query:?} {scope:?} {limit}");
                            compared += usize::from(!after.is_empty());
                        }
                    }
                }
                assert!(compared > 20, "{compared}");
                // Each index really hits its 64-row limit with more passing rows beyond it.
                let tier = |found: &BTreeMap<String, Value>, f: &dyn Fn(&Value) -> bool| found.values().filter(|r| f(r)).count();
                let acme = exact(db, &tokens("Acme"), None)?;
                assert_eq!(tier(&acme, &|r| r["id"].as_str().unwrap().starts_with("acme")), 64);
                let orion = exact(db, &tokens("Orion"), None)?;
                assert_eq!(tier(&orion, &|r| r["alias"] == "Orion"), 64);
                let zeta = exact(db, &tokens("Zeta"), None)?;
                assert_eq!(tier(&zeta, &|r| r["tier"] == "candidate"), 64);
                let named = anchors(db, "Acme", 2, None)?;
                assert_eq!(named[0]["same_name_count"], 64);
                assert!(exact(db, &tokens("Mila Stone Mila"), None)?["t-tie"]["alias"].is_string());
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn hydrating_only_kept_rows_is_faster_on_a_broad_match() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("data")).unwrap();
        fixture(&store, 6000);
        store
            .read(|tx| {
                let db = tx.sql();
                let query = "How do I deploy the recall memory tool for work with Lee Park?";
                let time = |f: &dyn Fn() -> Result<Vec<Value>>| -> Result<(Duration, String)> {
                    let mut best = Duration::MAX;
                    let mut out = String::new();
                    for _ in 0..3 {
                        let at = Instant::now();
                        out = serde_json::to_string(&f()?)?;
                        best = best.min(at.elapsed());
                    }
                    Ok((best, out))
                };
                let (old, before) = time(&|| anchors_before(db, query, 6, None))?;
                let (new, after) = time(&|| anchors(db, query, 6, None))?;
                assert_eq!(before, after);
                eprintln!("synthetic name anchors, broad match: previous={old:?} top-64-then-hydrate={new:?}");
                Ok(())
            })
            .unwrap();
    }
}
