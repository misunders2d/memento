//! Existing read-only viewer DTOs. Caller owns snapshot/HTTP Host/security headers.
use crate::query_rows::{self, ACTIVE_CANDIDATE, one, rows};
use memento_core::{Error, Result};
use rusqlite::{Connection, params};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap, HashSet};
pub const MAX_NODES: usize = 50_000;
pub const CSP: &str = "default-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'";
pub const STATIC_FILES: &[&str] = &[
    "app.js",
    "app.css",
    "layout-worker.js",
    "vendor/graphology.umd.min.js",
    "vendor/sigma.min.js",
    "vendor/forceatlas2.min.js",
];
const CURRENT_NODE: &str = "n.state='active' AND n.version=(SELECT max(v.version) FROM node_versions v WHERE v.id=n.id) AND NOT EXISTS (SELECT 1 FROM forgotten_targets f WHERE f.target=n.id)";
const CURRENT_EDGE: &str = "e.state='active' AND e.version=(SELECT max(v.version) FROM edge_versions v WHERE v.id=e.id) AND NOT EXISTS(SELECT 1 FROM forgotten_targets f WHERE f.target=e.id)";
const OPEN_LOOP: &str =
    "l.rowid=(SELECT max(x.rowid) FROM loop_events x WHERE x.target=l.target) AND l.state='open'";
fn default_view() -> String {
    "memories".into()
}
fn fifty() -> u32 {
    50
}
fn hundred() -> u32 {
    100
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphQuery {
    #[serde(default = "default_view")]
    pub view: String,
    pub scope: Option<String>,
    #[serde(default)]
    pub all: bool,
    pub id: Option<String>,
}
impl Default for GraphQuery {
    fn default() -> Self {
        Self {
            view: default_view(),
            scope: None,
            all: false,
            id: None,
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchQuery {
    pub q: String,
    pub scope: Option<String>,
    #[serde(default = "hundred")]
    pub limit: u32,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageQuery {
    #[serde(default)]
    pub offset: u64,
    #[serde(default = "fifty")]
    pub limit: u32,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationQuery {
    pub source: String,
    pub conversation_id: String,
    #[serde(default)]
    pub after: u64,
    #[serde(default = "fifty")]
    pub limit: u32,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidatesQuery {
    pub q: Option<String>,
    #[serde(default)]
    pub offset: u64,
    #[serde(default = "fifty")]
    pub limit: u32,
}
fn scope(value: Option<&str>) -> Result<()> {
    if value.is_some_and(|s| !["personal", "professional"].contains(&s)) {
        return Err(Error::Validation("Invalid viewer scope"));
    }
    Ok(())
}
fn page(offset: u64, limit: u32, max: u32) -> Result<()> {
    if offset > i64::MAX as u64 || limit == 0 || limit > max {
        return Err(Error::Validation("Invalid viewer page bounds"));
    }
    Ok(())
}
fn whitespace(c: char) -> bool {
    c.is_whitespace() || ('\u{001c}'..='\u{001f}').contains(&c)
}
fn label(text: &str) -> String {
    text.split(whitespace)
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(80)
        .collect()
}
fn within(item: &Value, scope: Option<&str>) -> bool {
    scope.is_none_or(|scope| {
        item["scope"]
            .as_array()
            .is_some_and(|s| s.iter().any(|v| v == scope))
    })
}
fn loop_value(row: &Value, now: &str) -> Value {
    json!({"due_at":row["due_at"],"deferred":row["deferred_until"].as_str().is_some_and(|v|!v.is_empty()&&v>now),"overdue":row["due_at"].as_str().is_some_and(|v|!v.is_empty()&&v<now)})
}
fn behavior_map(db: &Connection, target: Option<&str>) -> Result<HashMap<String, Value>> {
    let owners = rows(
        db,
        "SELECT n.id,n.commit_id FROM node_versions n WHERE n.version=1 AND n.type='experience' AND (?1 IS NULL OR n.id=?1)",
        [target],
    )?;
    let ids = owners
        .iter()
        .map(|n| n["commit_id"].clone())
        .collect::<Vec<_>>();
    let mut parsed = HashMap::<String, HashMap<String, Value>>::new();
    for record in rows(
        db,
        "SELECT id,request,result FROM graph_commits WHERE id IN (SELECT value FROM json_each(?))",
        [serde_json::to_string(&ids)?],
    )? {
        let request: Value = serde_json::from_str(
            record["request"]
                .as_str()
                .ok_or(Error::Unavailable("Invalid behavior authority record"))?,
        )?;
        let result: Value = serde_json::from_str(
            record["result"]
                .as_str()
                .ok_or(Error::Unavailable("Invalid behavior authority result"))?,
        )?;
        let command = &request["command"];
        let mut found = HashMap::new();
        if command["op"] == "consolidate" {
            for action in command["output"]["actions"]
                .as_array()
                .into_iter()
                .flatten()
            {
                let outcome = &result["results"][action["candidate_id"].as_str().unwrap_or("")];
                let id = outcome["superseded_by"]
                    .as_str()
                    .or_else(|| outcome["id"].as_str());
                if let Some(id) = id {
                    if ["remember", "supersede"]
                        .contains(&action["operation"].as_str().unwrap_or(""))
                        && action["type"] == "experience"
                        && action["behavior"]
                            .as_object()
                            .is_some_and(|v| !v.is_empty())
                    {
                        found
                            .entry(id.into())
                            .or_insert_with(|| action["behavior"].clone());
                    }
                }
            }
            for repair in command["output"]["repairs"]
                .as_array()
                .into_iter()
                .flatten()
            {
                let outcome = &result["repair_results"][repair["target"].as_str().unwrap_or("")];
                if let Some(id) = outcome["superseded_by"].as_str() {
                    if repair["operation"] == "supersede"
                        && repair["type"] == "experience"
                        && repair["behavior"]
                            .as_object()
                            .is_some_and(|v| !v.is_empty())
                    {
                        found
                            .entry(id.into())
                            .or_insert_with(|| repair["behavior"].clone());
                    }
                }
            }
        }
        parsed.insert(record["id"].as_str().unwrap().into(), found);
    }
    let mut result = HashMap::new();
    for owner in owners {
        let id = owner["id"].as_str().unwrap();
        if let Some(value) = parsed
            .get(owner["commit_id"].as_str().unwrap())
            .and_then(|m| m.get(id))
        {
            result.insert(id.into(), value.clone());
        }
    }
    Ok(result)
}
pub fn graph(db: &Connection, q: &GraphQuery, now: &str) -> Result<Value> {
    scope(q.scope.as_deref())?;
    if !["memories", "behaviors", "loops", "around"].contains(&q.view.as_str()) {
        return Err(Error::Validation("Invalid graph view"));
    }
    let mut nodes = HashMap::new();
    let mut order = Vec::new();
    for row in rows(
        db,
        &format!(
            "SELECT n.id,n.type,substr(n.text,1,240) AS text,n.scope,n.pinned,c.recorded_at,coalesce(s.importance,0.5)*coalesce(s.decay,1) AS weight FROM node_versions n JOIN graph_commits c ON c.id=n.commit_id LEFT JOIN node_stats s ON s.node_id=n.id WHERE {CURRENT_NODE}"
        ),
        [],
    )? {
        let id = row["id"].as_str().unwrap().to_owned();
        let weight = if row["pinned"].as_i64().unwrap_or(0) != 0 {
            1.0
        } else {
            format!(
                "{:.4}",
                row["weight"]
                    .as_f64()
                    .ok_or(Error::Unavailable("Invalid viewer weight"))?
            )
            .parse::<f64>()
            .map_err(|_| Error::Unavailable("Invalid viewer weight"))?
        };
        nodes.insert(id.clone(),json!({"id":id,"type":row["type"],"label":label(row["text"].as_str().unwrap()),"scope":query_rows::array(&row["scope"] )?,"weight":weight,"at":row["recorded_at"],"behavior":null,"loop":null}));
        order.push(id);
    }
    let mut edges = rows(
        db,
        &format!("SELECT e.id,e.src,e.rel,e.dst FROM edge_versions e WHERE {CURRENT_EDGE}"),
        [],
    )?
    .into_iter()
    .filter(|r| {
        nodes.contains_key(r["src"].as_str().unwrap())
            && nodes.contains_key(r["dst"].as_str().unwrap())
    })
    .collect::<Vec<_>>();
    for edge in &mut edges {
        edge["loop"] = Value::Null;
    }
    for (id, behavior) in behavior_map(db, None)? {
        if let Some(node) = nodes.get_mut(&id) {
            node["behavior"] = behavior["authority"].clone();
        }
    }
    for row in rows(
        db,
        &format!("SELECT l.target,l.due_at,l.deferred_until FROM loop_events l WHERE {OPEN_LOOP}"),
        [],
    )? {
        let target = row["target"].as_str().unwrap();
        if let Some(node) = nodes.get_mut(target) {
            node["loop"] = loop_value(&row, now);
        } else if let Some(edge) = edges.iter_mut().find(|e| e["id"] == target) {
            edge["loop"] = loop_value(&row, now);
        }
    }
    let candidates=rows(db,&format!("SELECT c.id,substr(c.text,1,240) AS text,c.scope,l.due_at,l.deferred_until FROM loop_events l JOIN candidates c ON c.id=l.target WHERE {OPEN_LOOP} AND {ACTIVE_CANDIDATE} ORDER BY l.due_at IS NULL,l.due_at,c.rowid DESC"),[])?.into_iter().map(|row|Ok(json!({"id":row["id"],"label":label(row["text"].as_str().unwrap()),"scope":query_rows::array(&row["scope"] )?,"loop":loop_value(&row,now)}))).collect::<Result<Vec<_>>>()?.into_iter().filter(|r|within(r,q.scope.as_deref())).collect::<Vec<_>>();
    let mut looped = nodes
        .values()
        .filter(|n| within(n, q.scope.as_deref()) && !n["loop"].is_null())
        .map(|n| n["loop"].clone())
        .collect::<Vec<_>>();
    looped.extend(
        edges
            .iter()
            .filter(|e| {
                !e["loop"].is_null()
                    && (within(&nodes[e["src"].as_str().unwrap()], q.scope.as_deref())
                        || within(&nodes[e["dst"].as_str().unwrap()], q.scope.as_deref()))
            })
            .map(|e| e["loop"].clone()),
    );
    looped.extend(candidates.iter().map(|c| c["loop"].clone()));
    let loops = json!({"open":looped.iter().filter(|l|l["deferred"]!=true).count(),"deferred":looped.iter().filter(|l|l["deferred"]==true).count(),"candidates":{"total":candidates.len(),"items":candidates.iter().take(100).cloned().collect::<Vec<_>>()}});
    nodes.retain(|_, n| within(n, q.scope.as_deref()));
    order.retain(|id| nodes.contains_key(id));
    let mut focus = HashSet::new();
    match q.view.as_str() {
        "behaviors" => focus.extend(
            nodes
                .iter()
                .filter(|(_, n)| !n["behavior"].is_null())
                .map(|(id, _)| id.clone()),
        ),
        "loops" => {
            focus.extend(
                nodes
                    .iter()
                    .filter(|(_, n)| !n["loop"].is_null())
                    .map(|(id, _)| id.clone()),
            );
            for edge in &edges {
                if !edge["loop"].is_null() {
                    for endpoint in ["src", "dst"] {
                        let id = edge[endpoint].as_str().unwrap();
                        if nodes.contains_key(id) {
                            focus.insert(id.to_owned());
                        }
                    }
                }
            }
        }
        "around" => {
            let id =
                q.id.as_ref()
                    .filter(|id| nodes.contains_key(*id))
                    .ok_or(Error::NotFound("Memory not found in this scope"))?;
            focus.insert(id.clone());
        }
        _ => (),
    }
    let chosen = if q.all {
        nodes.keys().cloned().collect::<HashSet<_>>()
    } else if q.view == "memories" {
        let mut ranked = order.clone();
        ranked.sort_by(|a, b| {
            nodes[b]["weight"]
                .as_f64()
                .unwrap()
                .total_cmp(&nodes[a]["weight"].as_f64().unwrap())
                .then(a.cmp(b))
        });
        let mut recent = order;
        recent.sort_by(|a, b| nodes[b]["at"].as_str().cmp(&nodes[a]["at"].as_str()));
        ranked
            .into_iter()
            .take(200)
            .chain(recent.into_iter().take(100))
            .collect()
    } else {
        let mut chosen = focus.clone();
        for edge in &edges {
            let a = edge["src"].as_str().unwrap();
            let b = edge["dst"].as_str().unwrap();
            if focus.contains(a) && nodes.contains_key(b) {
                chosen.insert(b.into());
            }
            if focus.contains(b) && nodes.contains_key(a) {
                chosen.insert(a.into());
            }
        }
        chosen
    };
    let mut result = json!({"view":q.view,"total_nodes":nodes.len(),"total_edges":edges.iter().filter(|e|nodes.contains_key(e["src"].as_str().unwrap())&&nodes.contains_key(e["dst"].as_str().unwrap())).count(),"loops":loops,"too_large":chosen.len()>MAX_NODES,"nodes":[],"edges":[]});
    if chosen.len() > MAX_NODES {
        return Ok(result);
    }
    let sorted = chosen.iter().cloned().collect::<BTreeSet<_>>();
    result["nodes"] = json!(
        sorted
            .into_iter()
            .map(|id| {
                let mut node = nodes.remove(&id).unwrap();
                node["focus"] = json!(focus.contains(&id));
                node
            })
            .collect::<Vec<_>>()
    );
    result["edges"] = json!(
        edges
            .into_iter()
            .filter(|e| chosen.contains(e["src"].as_str().unwrap())
                && chosen.contains(e["dst"].as_str().unwrap()))
            .collect::<Vec<_>>()
    );
    Ok(result)
}
pub fn node(db: &Connection, id: &str, now: &str) -> Result<Value> {
    let row = one(
        db,
        &format!(
            "SELECT n.id,n.type,n.text,n.scope FROM node_versions n WHERE n.id=? AND {CURRENT_NODE}"
        ),
        [id],
    )?
    .ok_or(Error::NotFound("Memory not found"))?;
    let event = one(
        db,
        &format!(
            "SELECT l.due_at,l.deferred_until FROM loop_events l WHERE l.target=? AND {OPEN_LOOP}"
        ),
        [id],
    )?;
    let links = rows(
        db,
        &format!(
            "SELECT e.rel,e.src,e.dst,n.id,n.type,substr(n.text,1,240) AS text FROM edge_versions e JOIN node_versions n ON n.id=CASE WHEN e.src=?1 THEN e.dst ELSE e.src END WHERE (e.src=?1 OR e.dst=?1) AND {CURRENT_EDGE} AND {CURRENT_NODE} LIMIT 200"
        ),
        [id],
    )?;
    let behavior = if row["type"] == "experience" {
        behavior_map(db, Some(id))?.remove(id)
    } else {
        None
    };
    Ok(
        json!({"id":row["id"],"type":row["type"],"text":row["text"],"scope":query_rows::array(&row["scope"] )?,"behavior":behavior,"loop":event.map(|l|loop_value(&l,now)),"links":links.into_iter().map(|l|json!({"rel":l["rel"],"outgoing":l["src"]==id,"id":l["id"],"type":l["type"],"label":label(l["text"].as_str().unwrap())})).collect::<Vec<_>>()}),
    )
}
pub fn literal_match(q: &str) -> Option<String> {
    let query = q
        .split(whitespace)
        .filter(|s| !s.is_empty())
        .map(|s| format!("\"{}\"", s.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" ");
    if query.is_empty() { None } else { Some(query) }
}
pub fn search(db: &Connection, q: &SearchQuery) -> Result<Value> {
    scope(q.scope.as_deref())?;
    page(0, q.limit, 200)?;
    if q.q.is_empty() || q.q.chars().count() > 512 {
        return Err(Error::Validation("Invalid viewer search query"));
    }
    let Some(query) = literal_match(&q.q) else {
        return Ok(json!({"results":[]}));
    };
    let result = rows(
        db,
        &format!(
            "SELECT n.id,n.type,substr(n.text,1,240) AS text,n.scope FROM node_fts JOIN node_versions n ON n.id=node_fts.id AND n.version=node_fts.version WHERE node_fts MATCH ?1 AND {CURRENT_NODE} AND (?2 IS NULL OR EXISTS(SELECT 1 FROM json_each(n.scope) WHERE value=?2)) ORDER BY bm25(node_fts),n.id LIMIT ?3"
        ),
        params![query, q.scope, q.limit],
    )?;
    Ok(
        json!({"results":result.into_iter().map(|r|json!({"id":r["id"],"type":r["type"],"label":label(r["text"].as_str().unwrap())})).collect::<Vec<_>>()}),
    )
}
pub fn conversations(db: &Connection, q: &PageQuery) -> Result<Value> {
    page(q.offset, q.limit, 100)?;
    let total: i64 = db.query_row(
        "SELECT count(*) FROM processing_progress WHERE latest_seq>seq",
        [],
        |r| r.get(0),
    )?;
    let mut items = rows(
        db,
        "SELECT p.source,p.conversation_id,p.seq AS cursor,p.latest_at,h.parked_through,h.live_seq AS live_cursor,(h.source IS NOT NULL) AS parked FROM processing_progress p LEFT JOIN historical_parking h ON h.source=p.source AND h.conversation_id=p.conversation_id WHERE p.latest_seq>p.seq ORDER BY p.latest_at DESC,p.source,p.conversation_id LIMIT ? OFFSET ?",
        params![q.limit, q.offset],
    )?;
    for row in &mut items {
        row["parked"] = json!(row["parked"].as_i64().unwrap() != 0);
    }
    Ok(json!({"total":total,"items":items}))
}
pub fn conversation<F: FnMut(&Value) -> Result<Option<String>>>(
    db: &Connection,
    q: &ConversationQuery,
    mut original: F,
) -> Result<Value> {
    page(q.after, q.limit, 100)?;
    for s in [&q.source, &q.conversation_id] {
        if s.is_empty() || s.chars().count() > 256 {
            return Err(Error::Validation("Invalid conversation identity"));
        }
    }
    let records = rows(
        db,
        "SELECT e.seq,e.id,e.source,e.conversation_id,e.kind,e.occurred_at,e.captured_at,e.text,e.text_runs,e.text_sha256,e.raw_frame FROM episodes e WHERE e.source=? AND e.conversation_id=? AND e.seq>? AND NOT EXISTS(SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id) ORDER BY e.seq LIMIT ?",
        params![q.source, q.conversation_id, q.after, q.limit],
    )?;
    let mut items = Vec::new();
    for row in records {
        let text = match original(&row) {
            Ok(v) => v,
            Err(Error::Unavailable(_)) => None,
            Err(error) => return Err(error),
        };
        let truncated = text.as_ref().is_some_and(|t| t.chars().count() > 4000);
        items.push(json!({"seq":row["seq"],"id":row["id"],"kind":row["kind"],"at":if row["occurred_at"].is_null(){row["captured_at"].clone()}else{row["occurred_at"].clone()},"text":text.map(|t|t.chars().take(4000).collect::<String>()),"truncated":truncated}));
    }
    Ok(json!({"items":items}))
}
pub fn candidates(db: &Connection, q: &CandidatesQuery) -> Result<Value> {
    page(q.offset, q.limit, 100)?;
    if q.q.as_ref().is_some_and(|s| s.chars().count() > 512) {
        return Err(Error::Validation("Invalid candidate query"));
    }
    let query = q.q.as_deref().and_then(literal_match);
    let predicate = format!(
        "{ACTIVE_CANDIDATE} AND (?1 IS NULL OR c.id IN (SELECT id FROM candidate_fts WHERE candidate_fts MATCH ?1))"
    );
    let total: i64 = db.query_row(
        &format!("SELECT count(*) FROM candidates c WHERE {predicate}"),
        [&query],
        |r| r.get(0),
    )?;
    let mut items = rows(
        db,
        &format!(
            "SELECT c.id,c.kind,c.type,substr(c.text,1,400) AS text,c.confidence,c.scope,g.recorded_at,(SELECT d.decision FROM candidate_decisions d WHERE d.candidate_id=c.id ORDER BY d.rowid DESC LIMIT 1) AS decision,(SELECT l.state FROM loop_events l WHERE l.target=c.id ORDER BY l.rowid DESC LIMIT 1) AS loop FROM candidates c JOIN extraction_runs r ON r.id=c.run_id JOIN graph_commits g ON g.id=r.commit_id WHERE {predicate} ORDER BY c.rowid DESC LIMIT ?2 OFFSET ?3"
        ),
        params![query, q.limit, q.offset],
    )?;
    for row in &mut items {
        row["scope"] = query_rows::array(&row["scope"])?;
        row["loop"] = json!(row["loop"] == "open");
    }
    Ok(json!({"total":total,"items":items}))
}
pub fn host_allowed(host: &str, allowed: &[String]) -> bool {
    let Some((hostname, _)) = crate::host_parts(host) else {
        return false;
    };
    ["localhost", "127.0.0.1", "::1"].contains(&hostname.as_str())
        || allowed
            .iter()
            .any(|a| a.eq_ignore_ascii_case(host) || a.eq_ignore_ascii_case(&hostname))
}
