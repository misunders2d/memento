//! Human-gated active-memory exclusion; source bytes and backups are retained.
use crate::util::*;
use crate::{Graph, context, curation, dto};
use memento_core::{Error, Result, WriteTxn};
use rusqlite::{Connection, params};
use serde_json::{Value, json};

/// Active nodes/edges outside `?1` that cite a source event of `?1` and whose whole
/// current evidence lies in that event set or in already forgotten source events.
const ORPHANED: &str = "WITH t(id) AS (SELECT value FROM json_each(?1)),
fresh(source,event_id) AS (SELECT DISTINCT e.source,e.event_id FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id WHERE g.target IN (SELECT id FROM t)),
gone(source,event_id) AS (SELECT source,event_id FROM fresh UNION SELECT source,event_id FROM forgotten_sources),
touched(id) AS (SELECT DISTINCT g.target FROM fresh JOIN episodes e ON e.source=fresh.source AND e.event_id=fresh.event_id JOIN graph_evidence g ON g.episode_id=e.id WHERE g.target NOT IN (SELECT id FROM t)),
cur(id,version,kind) AS (SELECT n.id,n.version,'node' FROM node_versions n JOIN touched ON touched.id=n.id WHERE n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)
 UNION ALL SELECT e.id,e.version,'edge' FROM edge_versions e JOIN touched ON touched.id=e.id WHERE e.state='active' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id))
SELECT c.id,c.version,c.kind FROM cur c WHERE EXISTS (SELECT 1 FROM graph_evidence g WHERE g.target=c.id AND g.version=c.version)
 AND NOT EXISTS (SELECT 1 FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id WHERE g.target=c.id AND g.version=c.version AND NOT EXISTS (SELECT 1 FROM gone WHERE gone.source=e.source AND gone.event_id=e.event_id))
 ORDER BY c.id";
const INCIDENT: &str = "SELECT e.id,e.version FROM edge_versions e WHERE (e.src IN (SELECT value FROM json_each(?1)) OR e.dst IN (SELECT value FROM json_each(?1))) AND e.state='active' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id) ORDER BY e.id";

pub fn preview(db: &Connection, target: &str) -> Result<Value> {
    let existing = one(
        db,
        "SELECT 1 FROM forgotten_targets WHERE target=?",
        [target],
    )?
    .is_some();
    let n = one(
        db,
        "SELECT * FROM node_versions WHERE id=? ORDER BY version DESC LIMIT 1",
        [target],
    )?;
    let edge = one(
        db,
        "SELECT * FROM edge_versions WHERE id=? ORDER BY version DESC LIMIT 1",
        [target],
    )?;
    let candidate = one(db, "SELECT * FROM candidates WHERE id=?", [target])?;
    let (kind, label, mut nodes, mut edges, mut targets) = if let Some(n) = n {
        if n["state"] != "active" && !existing {
            return Err(Error::Validation(
                "Choose the active memory identifier; this node is no longer active",
            ));
        }
        let family = rows(
            db,
            "WITH RECURSIVE family(id) AS (SELECT ? UNION SELECT n.id FROM node_versions n JOIN family f ON n.superseded_by=f.id WHERE n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)) SELECT n.id,n.version FROM node_versions n JOIN family f ON f.id=n.id WHERE n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) ORDER BY n.id",
            [target],
        )?;
        let mut nodes = json!({});
        let mut ids = vec![];
        for row in family {
            let id = get_str(&row, "id")?;
            nodes[id] = row["version"].clone();
            ids.push(id.to_owned());
        }
        let mut edges = json!({});
        let mut edge_ids = vec![];
        for row in rows(
            db,
            "SELECT e.id,e.version FROM edge_versions e WHERE (e.src IN (SELECT value FROM json_each(?1)) OR e.dst IN (SELECT value FROM json_each(?1))) AND e.state='active' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id) ORDER BY e.id",
            [encode_json(&json!(ids))?],
        )? {
            let id = get_str(&row, "id")?;
            edges[id] = row["version"].clone();
            edge_ids.push(id.to_owned());
        }
        ids.extend(edge_ids);
        (
            "node",
            excerpt(get_str(&n, "text")?, 240),
            nodes,
            edges,
            ids,
        )
    } else if let Some(edge) = edge {
        if edge["state"] != "active" && !existing {
            return Err(Error::Validation("Relation is no longer active"));
        }
        (
            "edge",
            get_str(&edge, "rel")?.into(),
            json!({}),
            json!({target:edge["version"]}),
            vec![target.into()],
        )
    } else if let Some(candidate) = candidate {
        if one(db,"SELECT target_id FROM candidate_decisions WHERE candidate_id=? AND decision='promote' AND rowid=(SELECT max(rowid) FROM candidate_decisions WHERE candidate_id=?)",params![target,target])?.is_some(){return Err(Error::Validation("Candidate was promoted; choose its active target"));}
        (
            "candidate",
            excerpt(get_str(&candidate, "text")?, 240),
            json!({}),
            json!({}),
            vec![target.into()],
        )
    } else {
        return Err(Error::Validation("Unknown active-memory identifier"));
    };
    // Owner decision 2026-10-31: forgetting a source event also forgets other active
    // memories whose entire current evidence lies in forgotten source events.
    let mut dependents = vec![];
    while !existing {
        let found = rows(db, ORPHANED, [encode_json(&json!(targets))?])?;
        if found.is_empty() {
            break;
        }
        let mut orphan_nodes = vec![];
        for row in found {
            let id = get_str(&row, "id")?.to_owned();
            if row["kind"] == "node" {
                nodes[&id] = row["version"].clone();
                orphan_nodes.push(id.clone());
            } else {
                edges[&id] = row["version"].clone();
            }
            dependents.push(id.clone());
            targets.push(id);
        }
        for row in rows(db, INCIDENT, [encode_json(&json!(orphan_nodes))?])? {
            let id = get_str(&row, "id")?.to_owned();
            if edges.get(&id).is_none() {
                edges[&id] = row["version"].clone();
                targets.push(id);
            }
        }
    }
    let sources = rows(
        db,
        "SELECT DISTINCT e.source,e.event_id FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id WHERE g.target IN (SELECT value FROM json_each(?)) ORDER BY e.source,e.event_id",
        [encode_json(&json!(targets))?],
    )?;
    let mut record = json!({"target":target,"kind":kind,"label":label,"nodes":nodes,"edges":edges,"targets":targets,"dependents":dependents,"sources":sources,"already_forgotten":existing,"policy":"active_memory_only","raw_history":"retained","backups":"retained"});
    record["token"] = json!(hash_json(&record)?);
    Ok(record)
}
pub fn exclude(tx: &WriteTxn, commit: &str, record: &Value) -> Result<()> {
    let mut kinds = std::collections::BTreeMap::new();
    for id in record["nodes"].as_object().unwrap().keys() {
        kinds.insert(id.as_str(), "node");
    }
    for id in record["edges"].as_object().unwrap().keys() {
        kinds.insert(id.as_str(), "edge");
    }
    kinds.insert(get_str(record, "target")?, get_str(record, "kind")?);
    for target in record["targets"].as_array().unwrap() {
        let target = target.as_str().unwrap();
        tx.sql().execute("INSERT INTO forgotten_targets SELECT ?,?,? WHERE NOT EXISTS (SELECT 1 FROM forgotten_targets WHERE target=?)",params![target,commit,kinds[target],target])?;
        tx.touch_forgotten_target(target)?;
        if kinds[target] == "candidate" {
            tx.sql().execute("UPDATE vector_blobs SET current=0 WHERE seq IN (SELECT seq FROM vector_items WHERE kind='candidate' AND target=?)",[target])?;
            tx.touch_target(target)?;
        }
    }
    for source in record["sources"].as_array().unwrap() {
        let origin = get_str(source, "source")?;
        let event = get_str(source, "event_id")?;
        tx.sql().execute("INSERT INTO forgotten_sources SELECT ?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM forgotten_sources WHERE source=? AND event_id=? AND target=?)",params![origin,event,record["target"].as_str(),commit,origin,event,record["target"].as_str()])?;
        // Existing authority's source-wide semantic exclusion includes raw chunks,
        // candidates and every assertion/segment supported by that source event.
        tx.sql().execute("UPDATE vector_blobs SET current=0 WHERE seq IN (SELECT i.seq FROM vector_items i JOIN episode_chunks x ON x.id=i.target JOIN episodes e ON e.id=x.episode_id WHERE i.kind='episode' AND e.source=?1 AND e.event_id=?2 UNION SELECT i.seq FROM vector_items i JOIN candidates c ON c.id=i.target JOIN extraction_runs r ON r.id=c.run_id JOIN episodes e ON e.id=r.episode_id WHERE i.kind='candidate' AND e.source=?1 AND e.event_id=?2)",params![origin,event])?;
        tx.touch_source(origin, event)?;
    }
    for target in record["nodes"].as_object().unwrap().keys() {
        let n = node(tx.sql(), target)?;
        let version = get_i64(&n, "version")?;
        if n["state"] == "active" {
            tx.sql().execute(
                "INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                params![
                    target,
                    version + 1,
                    commit,
                    n["type"].as_str(),
                    n["text"].as_str(),
                    "forgotten",
                    n["valid_from"].as_str(),
                    n["valid_to"].as_str(),
                    n["superseded_by"].as_str(),
                    n["pinned"].as_i64(),
                    n["scope"].as_str()
                ],
            )?;
            tx.sql().execute("INSERT INTO graph_evidence SELECT target,?,episode_id,start,end,quote FROM graph_evidence WHERE target=? AND version=?",params![version+1,target,version])?;
        }
        curation::invalidate_vectors(tx, target)?;
        tx.touch_target(target)?;
    }
    for target in record["edges"].as_object().unwrap().keys() {
        if let Some(edge) = one(
            tx.sql(),
            "SELECT * FROM edge_versions WHERE id=? ORDER BY version DESC LIMIT 1",
            [target],
        )? {
            if edge["state"] == "active" {
                let version = get_i64(&edge, "version")?;
                tx.sql().execute(
                    "INSERT INTO edge_versions VALUES (?,?,?,?,?,?,?)",
                    params![
                        target,
                        version + 1,
                        commit,
                        edge["src"].as_str(),
                        edge["rel"].as_str(),
                        edge["dst"].as_str(),
                        "forgotten"
                    ],
                )?;
                tx.sql().execute("INSERT INTO graph_evidence SELECT target,?,episode_id,start,end,quote FROM graph_evidence WHERE target=? AND version=?",params![version+1,target,version])?;
                tx.touch_target(target)?;
            }
        }
    }
    Ok(())
}
pub fn forget(graph: &Graph, command: dto::Forget) -> Result<Value> {
    let request = json!({"op":"forget_active","target":command.target});
    let context = context(command.command_id, command.actor, command.why);
    graph.store.command(&context,&request,|tx,commit|{let record=preview(tx.sql(),&command.target)?;if record["token"]!=command.token{return Err(Error::Conflict("Forget preview is stale; inspect and confirm the current scope"));}if record["already_forgotten"]==true{return Ok(json!({"id":command.target,"status":"already_forgotten","policy":"active_memory_only"}));}exclude(tx,commit,&record)?;Ok(json!({"id":command.target,"status":"forgotten","policy":"active_memory_only","raw_history":"retained","backups":"retained"}))})
}
