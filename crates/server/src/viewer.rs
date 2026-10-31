//! Existing read-only viewer DTOs. Caller owns snapshot/HTTP Host/security headers.
use crate::query_rows::{self, ACTIVE_CANDIDATE, one, rows};
use memento_core::{Error, Result};
use memento_domain::loops;
use rusqlite::{Connection, functions::FunctionFlags, params};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
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
pub(crate) const CURRENT_NODE: &str = "n.state='active' AND n.version=(SELECT max(v.version) FROM node_versions v WHERE v.id=n.id) AND NOT EXISTS (SELECT 1 FROM forgotten_targets f WHERE f.target=n.id)";
pub(crate) const CURRENT_EDGE: &str = "e.state='active' AND e.version=(SELECT max(v.version) FROM edge_versions v WHERE v.id=e.id) AND NOT EXISTS(SELECT 1 FROM forgotten_targets f WHERE f.target=e.id)";
pub(crate) const OPEN_LOOP: &str =
    "l.rowid=(SELECT max(x.rowid) FROM loop_events x WHERE x.target=l.target) AND l.state='open'";
/// The node set the viewer shows: current versions together with their commit.
const NODES: &str = "node_versions n JOIN graph_commits c ON c.id=n.commit_id";
const IN_SCOPE: &str = "(?1 IS NULL OR EXISTS(SELECT 1 FROM json_each(n.scope) WHERE value=?1))";
const IN_IDS: &str = "n.id IN (SELECT value FROM json_each(?2))";
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
    pub responsibility: Option<String>,
}
impl Default for GraphQuery {
    fn default() -> Self {
        Self {
            view: default_view(),
            scope: None,
            all: false,
            id: None,
            responsibility: None,
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
pub(crate) fn scope(value: Option<&str>) -> Result<()> {
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
pub(crate) fn label(text: &str) -> String {
    text.split(whitespace)
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(80)
        .collect()
}
pub(crate) fn within(item: &Value, scope: Option<&str>) -> bool {
    scope.is_none_or(|scope| {
        item["scope"]
            .as_array()
            .is_some_and(|s| s.iter().any(|v| v == scope))
    })
}
pub(crate) fn loop_value(row: &Value, now: &str) -> Value {
    json!({"due_at":row["due_at"],"deferred":row["deferred_until"].as_str().is_some_and(|v|!v.is_empty()&&v>now),"overdue":row["due_at"].as_str().is_some_and(|v|!v.is_empty()&&v<now),"responsibility":row["responsibility"].as_str().unwrap_or("unknown")})
}
pub(crate) fn behavior_map(db: &Connection, targets: &[String]) -> Result<HashMap<String, Value>> {
    // A behavior comes from the content commit (creation or same-ID
    // revision) that produced the current version, as in domain::behavior;
    // recovered legacy content resolves through the replacement it came from.
    let owners = rows(
        db,
        "WITH cur AS (SELECT c.id,c.version FROM node_versions c WHERE c.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=c.id) AND c.type='experience' AND c.id IN (SELECT value FROM json_each(?1))), own AS (SELECT cur.id,coalesce(r.source_id,cur.id) AS source,coalesce(r.source_version,cur.version) AS at FROM cur LEFT JOIN node_revisions r ON r.id=cur.id AND r.version=(SELECT max(y.version) FROM node_revisions y WHERE y.id=cur.id AND y.version<=cur.version)) SELECT own.id,own.source,d.version,d.commit_id FROM own JOIN node_versions d ON d.id=own.source AND d.version=coalesce((SELECT max(r.version) FROM node_revisions r WHERE r.id=own.source AND r.version<=own.at),1)",
        [serde_json::to_string(targets)?],
    )?;
    let ids = owners
        .iter()
        .map(|n| n["commit_id"].clone())
        .collect::<Vec<_>>();
    let mut parsed = HashMap::<String, HashMap<(String, i64), Value>>::new();
    // Legacy supersedes produced a replacement at version 1; same-ID
    // revisions produce the reported version of the revised memory.
    let produced = |outcome: &Value, operation: &str| -> Option<(String, i64)> {
        if let Some(id) = outcome["superseded_by"].as_str() {
            return Some((id.into(), 1));
        }
        let id = outcome["id"].as_str()?.to_owned();
        if operation == "remember" {
            return Some((id, 1));
        }
        outcome["version"]
            .as_i64()
            .filter(|version| *version > 1)
            .map(|version| (id, version))
    };
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
                if let Some(id) = produced(outcome, action["operation"].as_str().unwrap_or("")) {
                    if ["remember", "supersede"]
                        .contains(&action["operation"].as_str().unwrap_or(""))
                        && action["type"] == "experience"
                        && action["behavior"]
                            .as_object()
                            .is_some_and(|v| !v.is_empty())
                    {
                        found
                            .entry(id)
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
                if let Some(id) = produced(outcome, "supersede") {
                    if repair["operation"] == "supersede"
                        && repair["type"] == "experience"
                        && repair["behavior"]
                            .as_object()
                            .is_some_and(|v| !v.is_empty())
                    {
                        found
                            .entry(id)
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
        let key = (
            owner["source"].as_str().unwrap().to_owned(),
            owner["version"].as_i64().unwrap_or(1),
        );
        if let Some(value) = parsed
            .get(owner["commit_id"].as_str().unwrap())
            .and_then(|m| m.get(&key))
        {
            result.insert(id.into(), value.clone());
        }
    }
    Ok(result)
}
/// Rank with exactly the rounded weight the viewer returns.
fn weight(pinned: i64, raw: f64) -> Result<f64> {
    if pinned != 0 {
        return Ok(1.0);
    }
    if !raw.is_finite() {
        return Err(Error::Unavailable("Invalid viewer weight"));
    }
    format!("{raw:.4}")
        .parse::<f64>()
        .map_err(|_| Error::Unavailable("Invalid viewer weight"))
}
fn select_ids<P: rusqlite::Params>(db: &Connection, sql: &str, params: P) -> Result<Vec<String>> {
    Ok(rows(db, sql, params)?
        .into_iter()
        .map(|r| r["id"].as_str().unwrap().to_owned())
        .collect())
}
/// Current viewer nodes among `targets`, each mapped to whether it is inside the scope.
fn current(db: &Connection, scope: Option<&str>, targets: &[String]) -> Result<HashMap<String, bool>> {
    Ok(rows(
        db,
        &format!("SELECT n.id,{IN_SCOPE} AS inside FROM {NODES} WHERE {IN_IDS} AND {CURRENT_NODE}"),
        params![scope, serde_json::to_string(targets)?],
    )?
    .into_iter()
    .map(|r| (r["id"].as_str().unwrap().to_owned(), r["inside"] == 1))
    .collect())
}
fn role<'a>(roles: &'a HashMap<String, Value>, target: &str) -> &'a str {
    roles
        .get(target)
        .and_then(|value| value["responsibility"].as_str())
        .unwrap_or("unknown")
}
pub fn graph(db: &Connection, q: &GraphQuery, now: &str) -> Result<Value> {
    graph_capped(db, q, now, MAX_NODES)
}
/// Each view selects its nodes in SQL; labels, behaviors and loop roles are
/// resolved only for what is returned. Header counts stay whole-graph.
pub(crate) fn graph_capped(db: &Connection, q: &GraphQuery, now: &str, cap: usize) -> Result<Value> {
    scope(q.scope.as_deref())?;
    if q.responsibility.as_deref().is_some_and(|value|!loops::valid_responsibility(value)) {
        return Err(Error::Validation("Invalid loop responsibility"));
    }
    if !["memories", "behaviors", "loops", "around"].contains(&q.view.as_str()) {
        return Err(Error::Validation("Invalid graph view"));
    }
    let inside = q.scope.as_deref();
    let wanted = q.responsibility.as_deref();
    let keep = |roles: &HashMap<String, Value>, target: &str| wanted.is_none_or(|w| w == role(roles, target));
    let total_nodes: usize = db.query_row(
        &format!("SELECT count(*) FROM {NODES} WHERE {CURRENT_NODE} AND {IN_SCOPE}"),
        [inside],
        |r| r.get(0),
    )?;
    let total_edges: usize = db.query_row(
        &format!("SELECT count(*) FROM edge_versions e WHERE {CURRENT_EDGE} AND EXISTS(SELECT 1 FROM {NODES} WHERE n.id=e.src AND {CURRENT_NODE} AND {IN_SCOPE}) AND EXISTS(SELECT 1 FROM {NODES} WHERE n.id=e.dst AND {CURRENT_NODE} AND {IN_SCOPE})"),
        [inside],
        |r| r.get(0),
    )?;
    let markers = rows(db,&format!("SELECT l.target,l.due_at,l.deferred_until FROM loop_events l WHERE {OPEN_LOOP}"),[])?
        .into_iter()
        .map(|row| (row["target"].as_str().unwrap().to_owned(), row))
        .collect::<HashMap<_, _>>();
    let targets = markers.keys().cloned().collect::<Vec<_>>();
    // A responsibility filter needs every open loop's role; otherwise only returned loops need one.
    let mut roles = if wanted.is_some() { loops::responsibilities(db, &targets)? } else { HashMap::new() };
    let loop_nodes = current(db, inside, &targets)?;
    let loop_edges = rows(db, &format!("SELECT e.id,e.src,e.dst FROM edge_versions e WHERE e.id IN (SELECT value FROM json_each(?1)) AND {CURRENT_EDGE}"), [serde_json::to_string(&targets)?])?
        .into_iter()
        .filter(|e| !loop_nodes.contains_key(e["id"].as_str().unwrap()))
        .collect::<Vec<_>>();
    let ends = current(db, inside, &loop_edges.iter().flat_map(|e| [e["src"].as_str().unwrap().to_owned(), e["dst"].as_str().unwrap().to_owned()]).collect::<Vec<_>>())?;
    let loop_edges = loop_edges
        .into_iter()
        .filter(|e| ends.contains_key(e["src"].as_str().unwrap()) && ends.contains_key(e["dst"].as_str().unwrap()) && keep(&roles, e["id"].as_str().unwrap()))
        .collect::<Vec<_>>();
    let deferred = |target: &str| loop_value(&markers[target], now)["deferred"] == true;
    let mut looped = loop_nodes
        .iter()
        .filter(|(id, scoped)| **scoped && keep(&roles, id))
        .map(|(id, _)| deferred(id))
        .collect::<Vec<_>>();
    looped.extend(
        loop_edges
            .iter()
            .filter(|e| ends[e["src"].as_str().unwrap()] || ends[e["dst"].as_str().unwrap()])
            .map(|e| deferred(e["id"].as_str().unwrap())),
    );
    let mut candidates = Vec::new();
    for row in loops::visible_loop_targets(db,rows(db,&format!("SELECT c.id,c.kind AS candidate_kind,c.src AS loop_src,c.dst AS loop_dst,substr(c.text,1,240) AS text,c.scope,l.due_at,l.deferred_until FROM loop_events l JOIN candidates c ON c.id=l.target WHERE {OPEN_LOOP} AND {ACTIVE_CANDIDATE} ORDER BY l.due_at IS NULL,l.due_at,c.rowid DESC"),[])?)? {
        let item = json!({"id":row["id"],"label":label(row["text"].as_str().unwrap()),"scope":query_rows::array(&row["scope"])?,"loop":null});
        if within(&item, inside) && keep(&roles, row["id"].as_str().unwrap()) {
            looped.push(loop_value(&row, now)["deferred"] == true);
            candidates.push((row, item));
        }
    }
    let full_candidate_loops = q.all && q.view == "loops";
    let candidate_overflow = full_candidate_loops && candidates.len() > cap;
    let candidate_limit = if candidate_overflow {
        0
    } else if full_candidate_loops {
        cap
    } else {
        100
    };
    let candidate_total = candidates.len();
    candidates.truncate(candidate_limit);
    if wanted.is_none() && !candidates.is_empty() {
        roles.extend(loops::responsibilities(db, &candidates.iter().map(|(row, _)| row["id"].as_str().unwrap().to_owned()).collect::<Vec<_>>())?);
    }
    let items = candidates
        .into_iter()
        .map(|(mut row, mut item)| {
            row["responsibility"] = json!(role(&roles, row["id"].as_str().unwrap()));
            item["loop"] = loop_value(&row, now);
            item
        })
        .collect::<Vec<_>>();
    let loop_counts = json!({"open":looped.iter().filter(|d|!**d).count(),"deferred":looped.iter().filter(|d|**d).count(),"candidates":{"total":candidate_total,"items":items}});
    let mut result = json!({"view":q.view,"total_nodes":total_nodes,"total_edges":total_edges,"loops":loop_counts,"too_large":false,"nodes":[],"edges":[]});
    let mut focus = HashSet::new();
    if q.view == "around" {
        let id = q.id.clone().unwrap_or_default();
        if current(db, inside, std::slice::from_ref(&id))?.get(&id) != Some(&true) {
            return Err(Error::NotFound("Memory not found in this scope"));
        }
        focus.insert(id);
    }
    let all = q.all && !(q.view == "loops" && q.responsibility.is_some());
    // "Show all" is refused from the count alone, before any node is read.
    if (all && total_nodes > cap) || candidate_overflow {
        result["too_large"] = json!(true);
        return Ok(result);
    }
    let mut behaviors = None;
    match q.view.as_str() {
        "behaviors" => {
            let experiences = select_ids(db, &format!("SELECT n.id FROM {NODES} WHERE n.type='experience' AND {CURRENT_NODE} AND {IN_SCOPE}"), [inside])?;
            let map = behavior_map(db, &experiences)?;
            focus.extend(map.iter().filter(|(_, b)| !b["authority"].is_null()).map(|(id, _)| id.clone()));
            behaviors = Some(map);
        }
        "loops" => {
            focus.extend(loop_nodes.iter().filter(|(id, scoped)| **scoped && keep(&roles, id)).map(|(id, _)| id.clone()));
            for edge in &loop_edges {
                for endpoint in ["src", "dst"] {
                    let id = edge[endpoint].as_str().unwrap();
                    if ends[id] {
                        focus.insert(id.to_owned());
                    }
                }
            }
        }
        _ => (),
    }
    let chosen = if all {
        None
    } else if q.view == "memories" {
        db.create_scalar_function("viewer_weight", 2, FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC, |ctx| {
            weight(ctx.get(0)?, ctx.get(1)?).map_err(|e| rusqlite::Error::UserFunctionError(Box::new(e)))
        })?;
        Some(select_ids(db, &format!("SELECT id FROM (SELECT n.id FROM {NODES} LEFT JOIN node_stats s ON s.node_id=n.id WHERE {CURRENT_NODE} AND {IN_SCOPE} ORDER BY viewer_weight(n.pinned,coalesce(s.importance,0.5)*coalesce(s.decay,1)) DESC,n.id LIMIT 200) UNION SELECT id FROM (SELECT n.id FROM {NODES} WHERE {CURRENT_NODE} AND {IN_SCOPE} ORDER BY c.recorded_at DESC,n.rowid LIMIT 100)"), [inside])?)
    } else {
        let others = select_ids(db, &format!("SELECT e.dst AS id FROM edge_versions e WHERE e.src IN (SELECT value FROM json_each(?1)) AND {CURRENT_EDGE} UNION SELECT e.src FROM edge_versions e WHERE e.dst IN (SELECT value FROM json_each(?1)) AND {CURRENT_EDGE}"), [serde_json::to_string(&focus)?])?;
        let mut chosen = focus.iter().cloned().collect::<Vec<_>>();
        chosen.extend(current(db, inside, &others)?.into_iter().filter(|(id, scoped)| *scoped && !focus.contains(id)).map(|(id, _)| id));
        Some(chosen)
    };
    if chosen.as_ref().is_some_and(|c| c.len() > cap) {
        result["too_large"] = json!(true);
        return Ok(result);
    }
    let select = format!("SELECT n.id,n.type,substr(n.text,1,240) AS text,n.scope,n.pinned,c.recorded_at,coalesce(s.importance,0.5)*coalesce(s.decay,1) AS weight FROM {NODES} LEFT JOIN node_stats s ON s.node_id=n.id WHERE {CURRENT_NODE} AND {IN_SCOPE}");
    let found = match &chosen {
        None => rows(db, &select, [inside])?,
        Some(chosen) => rows(db, &format!("{select} AND {IN_IDS}"), params![inside, serde_json::to_string(chosen)?])?,
    };
    let mut nodes = Vec::new();
    for row in found {
        let weight = weight(
            row["pinned"].as_i64().unwrap_or(0),
            row["weight"].as_f64().ok_or(Error::Unavailable("Invalid viewer weight"))?,
        )?;
        nodes.push(json!({"id":row["id"],"type":row["type"],"label":label(row["text"].as_str().unwrap()),"scope":query_rows::array(&row["scope"] )?,"weight":weight,"at":row["recorded_at"],"behavior":null,"loop":null}));
    }
    nodes.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    let returned = nodes.iter().map(|n| n["id"].as_str().unwrap().to_owned()).collect::<HashSet<_>>();
    let mut edges = match chosen {
        None => rows(db, &format!("SELECT e.id,e.src,e.rel,e.dst FROM edge_versions e WHERE {CURRENT_EDGE} ORDER BY e.rowid"), [])?
            .into_iter()
            .filter(|e| returned.contains(e["src"].as_str().unwrap()) && returned.contains(e["dst"].as_str().unwrap()))
            .collect::<Vec<_>>(),
        Some(_) => rows(db, &format!("SELECT e.id,e.src,e.rel,e.dst FROM edge_versions e WHERE e.src IN (SELECT value FROM json_each(?1)) AND e.dst IN (SELECT value FROM json_each(?1)) AND {CURRENT_EDGE} ORDER BY e.rowid"), [serde_json::to_string(&returned)?])?,
    };
    let behaviors = match behaviors {
        Some(map) => map,
        None => behavior_map(db, &nodes.iter().filter(|n| n["type"] == "experience").map(|n| n["id"].as_str().unwrap().to_owned()).collect::<Vec<_>>())?,
    };
    let marked = nodes
        .iter()
        .chain(&edges)
        .map(|item| item["id"].as_str().unwrap().to_owned())
        .filter(|id| markers.contains_key(id))
        .collect::<Vec<_>>();
    if wanted.is_none() && !marked.is_empty() {
        roles.extend(loops::responsibilities(db, &marked)?);
    }
    let loop_of = |id: &str| {
        markers.get(id).filter(|_| keep(&roles, id)).map(|row| {
            let mut row = row.clone();
            row["responsibility"] = json!(role(&roles, id));
            loop_value(&row, now)
        })
    };
    for node in &mut nodes {
        let id = node["id"].as_str().unwrap().to_owned();
        if let Some(behavior) = behaviors.get(&id) {
            node["behavior"] = behavior["authority"].clone();
        }
        if let Some(value) = loop_of(&id) {
            node["loop"] = value;
        }
        node["focus"] = json!(focus.contains(&id));
    }
    for edge in &mut edges {
        let id = edge["id"].as_str().unwrap().to_owned();
        // A loop on an ID that is also a current node belongs to the node.
        edge["loop"] = loop_of(&id).filter(|_| !loop_nodes.contains_key(&id)).unwrap_or(Value::Null);
    }
    result["nodes"] = json!(nodes);
    result["edges"] = json!(edges);
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
    let mut event = one(
        db,
        &format!(
            "SELECT l.due_at,l.deferred_until FROM loop_events l WHERE l.target=? AND {OPEN_LOOP}"
        ),
        [id],
    )?;
    if let Some(event)=event.as_mut(){event["responsibility"]=json!(loops::responsibility(db,id)?);}
    let links = rows(
        db,
        &format!(
            "SELECT e.rel,e.src,e.dst,n.id,n.type,substr(n.text,1,240) AS text FROM edge_versions e JOIN node_versions n ON n.id=CASE WHEN e.src=?1 THEN e.dst ELSE e.src END WHERE (e.src=?1 OR e.dst=?1) AND {CURRENT_EDGE} AND {CURRENT_NODE} LIMIT 200"
        ),
        [id],
    )?;
    let behavior = if row["type"] == "experience" {
        behavior_map(db, &[id.to_owned()])?.remove(id)
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
