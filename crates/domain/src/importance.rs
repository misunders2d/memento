//! Committed usage and ranking-only recency; no age-based eligibility changes.
use crate::util::*;
use crate::{Graph, context, dto};
use chrono::Utc;
use memento_core::{Error, Result, WriteTxn};
use rusqlite::{Connection, params};
use serde_json::{Value, json};

pub fn query_key(query: &str) -> String {
    let folded = crate::unicode::casefold(query);
    let mut words = std::collections::BTreeSet::new();
    let mut word = String::new();
    for letter in folded.chars().chain(std::iter::once(' ')) {
        if crate::unicode::is_word(letter) {
            word.push(letter);
        } else if !word.is_empty() {
            words.insert(std::mem::take(&mut word));
        }
    }
    memento_core::raw::sha256(words.into_iter().collect::<Vec<_>>().join(" ").as_bytes())
}
pub fn transfer_usage(tx: &WriteTxn, source: &str, target: &str) -> Result<()> {
    if source == target {
        return Ok(());
    }
    tx.sql().execute("INSERT INTO node_stats(node_id,importance,decay,archived,access_count,last_cited_at) SELECT ?,importance,decay,archived,access_count,last_cited_at FROM node_stats WHERE node_id=? ON CONFLICT(node_id) DO UPDATE SET importance=max(node_stats.importance,excluded.importance),decay=max(node_stats.decay,excluded.decay),archived=min(node_stats.archived,excluded.archived),access_count=node_stats.access_count+excluded.access_count,last_cited_at=CASE WHEN node_stats.last_cited_at IS NULL OR julianday(excluded.last_cited_at)>julianday(node_stats.last_cited_at) THEN excluded.last_cited_at ELSE node_stats.last_cited_at END",params![target,source])?;
    tx.sql().execute("INSERT INTO query_feedback(query_key,target,shown,cited) SELECT query_key,?,shown,cited FROM query_feedback WHERE target=? ON CONFLICT(query_key,target) DO UPDATE SET shown=query_feedback.shown+excluded.shown,cited=query_feedback.cited+excluded.cited",params![target,source])?;
    Ok(())
}
pub fn cite(graph: &Graph, command: dto::Citation) -> Result<Value> {
    required(&command.event_id, 512, "Invalid citation event identifier")?;
    if command
        .query
        .as_deref()
        .is_some_and(|value| value.chars().count() > 4096)
    {
        return Err(Error::Validation("Citation query too long"));
    }
    let mut ids = command.ids;
    ids.sort();
    ids.dedup();
    let mut shown = command.shown;
    shown.sort();
    shown.dedup();
    let request = json!({"op":"cite","ids":ids,"query":command.query,"shown":shown});
    let context = context(
        format!("cite/{}", command.event_id),
        "harness/citation".into(),
        "Memory citations in committed assistant output".into(),
    );
    graph.store.command(&context,&request,|tx,_commit|{
        let mut accepted=vec![];let mut demand_jobs=vec![];let mut demand_skipped=vec![];let now=now();
        for target in &ids {
            let n=one(tx.sql(),"SELECT state FROM node_versions WHERE id=? ORDER BY version DESC LIMIT 1",[target])?;
            let candidate=one(tx.sql(),&format!("SELECT c.id,c.src,c.dst FROM candidates c WHERE c.id=? AND {ACTIVE_CANDIDATE}"),[target])?;
            let chunk=one(tx.sql(),"SELECT episode_id,start,end FROM episode_chunks WHERE id=?",[target])?;
            let episode_id=chunk.as_ref().and_then(|value|value["episode_id"].as_str()).unwrap_or(target);
            let episode=one(tx.sql(),"SELECT e.* FROM episodes e WHERE e.id=? AND NOT EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id)",[episode_id])?;
            if n.as_ref().is_none_or(|value|value["state"]!="active")&&candidate.is_none()&&episode.is_none(){continue;}
            tx.sql().execute("INSERT INTO node_stats(node_id,access_count,last_cited_at) VALUES (?,1,?) ON CONFLICT(node_id) DO UPDATE SET access_count=access_count+1,last_cited_at=excluded.last_cited_at,archived=0,decay=1,importance=min(1,importance+0.05)",params![target,now])?;
            accepted.push(target.clone());
            if let Some(candidate)=candidate {
                for name in ["id","src","dst"] {
                    if let Some(id)=candidate[name].as_str(){tx.sql().execute("UPDATE historical_parked_candidates SET released_at=? WHERE released_at IS NULL AND candidate_id=?",params![now,id])?;tx.touch_target(id)?;}
                }
            }
            if let Some(episode)=episode {
                // The processing module installs this already-existing demand
                // admission callback. Refuse to acknowledge raw citation without
                // it rather than silently dropping demand activation semantics.
                let (job,skipped)=crate::demand_extract(tx,&episode,chunk.as_ref().and_then(|value|value["start"].as_i64()),chunk.as_ref().and_then(|value|value["end"].as_i64()))?;
                if skipped{demand_skipped.push(target.clone());}else if let Some(job)=job{if !demand_jobs.contains(&job){demand_jobs.push(job);}}
            }
        }
        if let Some(query)=command.query.as_deref().filter(|value|!value.is_empty()) {
            let key=query_key(query);let mut targets=shown.clone();targets.extend(accepted.iter().cloned());targets.sort();targets.dedup();
            for target in targets {tx.sql().execute("INSERT INTO query_feedback(query_key,target,shown,cited) VALUES (?,?,?,?) ON CONFLICT(query_key,target) DO UPDATE SET shown=shown+excluded.shown,cited=cited+excluded.cited",params![key,target,i64::from(shown.contains(&target)),i64::from(accepted.contains(&target))])?;}
        }
        let unknown=ids.iter().filter(|target|!accepted.contains(target)).cloned().collect::<Vec<_>>();
        Ok(json!({"cited":accepted,"unknown_or_inactive":unknown,"demand_jobs":demand_jobs,"demand_skipped":demand_skipped}))
    })
}

pub fn query_recency(
    db: &Connection,
    record: &Value,
    authority: Option<&str>,
    source_override: Option<Option<&str>>,
) -> Result<Value> {
    let target = get_str(record, "id")?;
    let source = if let Some(value) = source_override {
        value.map(str::to_owned)
    } else if record["tier"] == "raw" {
        one(
            db,
            "SELECT occurred_at FROM episodes WHERE id=?",
            [record["episode_id"].as_str().unwrap_or(target)],
        )?
        .and_then(|value| value["occurred_at"].as_str().map(str::to_owned))
    } else {
        one(db,"SELECT e.occurred_at FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id WHERE g.target=? AND g.version=? AND e.occurred_at IS NOT NULL AND NOT EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id) ORDER BY julianday(e.occurred_at) DESC,e.id LIMIT 1",params![target,record["version"].as_i64()])?.and_then(|value|value["occurred_at"].as_str().map(str::to_owned))
    };
    let cited = one(
        db,
        "SELECT last_cited_at FROM node_stats WHERE node_id=?",
        [target],
    )?
    .and_then(|value| value["last_cited_at"].as_str().map(str::to_owned));
    let source_at = source.as_deref().and_then(|value| parse_time(value).ok());
    let cited_at = cited.as_deref().and_then(|value| parse_time(value).ok());
    let anchor = source_at.into_iter().chain(cited_at).max();
    let age = anchor
        .map(|at| (Utc::now() - at).num_microseconds().unwrap_or(0) as f64 / 86400.0 / 1000000.0)
        .unwrap_or(0.0)
        .max(0.0);
    let decay = if record["pinned"].as_i64().unwrap_or(0) != 0
        || record["pinned"] == true
        || authority == Some("explicit_user")
    {
        1.0
    } else {
        (-age / 90.0).exp2()
    };
    Ok(
        json!({"source_occurred_at":source_at.map(format_time),"ranking_anchor":anchor.map(format_time),"decay":decay}),
    )
}

pub fn refresh(tx: &WriteTxn, instant: Option<chrono::DateTime<Utc>>) -> Result<usize> {
    let instant = instant.unwrap_or_else(Utc::now);
    let records = rows(
        tx.sql(),
        "WITH current_nodes AS (SELECT n.* FROM node_versions n WHERE n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)),current_edges AS (SELECT e.* FROM edge_versions e WHERE e.state='active' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id)),neighbors AS (SELECT src AS id,dst AS neighbor FROM current_edges UNION SELECT dst,src FROM current_edges),degrees AS (SELECT n.id,n.type,count(DISTINCT l.neighbor) AS degree FROM current_nodes n LEFT JOIN neighbors l ON l.id=n.id GROUP BY n.id),norms AS (SELECT type,avg(degree) AS mean_degree FROM degrees GROUP BY type) SELECT n.id,n.pinned,s.access_count,s.last_cited_at,d.degree,t.mean_degree,(SELECT c.recorded_at FROM node_versions v JOIN graph_commits c ON c.id=v.commit_id WHERE v.id=n.id ORDER BY v.version LIMIT 1) AS created_at,(SELECT count(DISTINCT e.episode_id) FROM graph_evidence e WHERE e.target=n.id AND e.version=n.version) AS sources FROM current_nodes n JOIN node_stats s ON s.node_id=n.id JOIN degrees d ON d.id=n.id JOIN norms t ON t.type=n.type",
        [],
    )?;
    for row in &records {
        let mut base = row["created_at"]
            .as_str()
            .map(parse_time)
            .transpose()?
            .unwrap_or(instant);
        if let Some(cited) = row["last_cited_at"].as_str() {
            base = base.max(parse_time(cited)?);
        }
        let age = (instant - base).num_microseconds().unwrap_or(0) as f64 / 86400.0 / 1e6;
        let degree = row["degree"].as_f64().unwrap_or(0.0);
        let mean = row["mean_degree"].as_f64().unwrap_or(0.0);
        let cited = row["access_count"].as_f64().unwrap_or(0.0);
        let sources = row["sources"].as_f64().unwrap_or(0.0);
        let pinned = row["pinned"].as_i64().unwrap_or(0) != 0;
        let importance = if pinned {
            1.0
        } else {
            (0.30
                + 0.25 * degree / (degree + mean + 1.0)
                + 0.30 * cited / (cited + 3.0)
                + 0.15 * sources / (sources + 3.0))
                .min(0.95)
        };
        let decay = if pinned {
            1.0
        } else {
            (-age.max(0.0) / 90.0).exp2()
        };
        tx.sql().execute(
            "UPDATE node_stats SET importance=?,decay=?,archived=0 WHERE node_id=?",
            params![importance, decay, row["id"].as_str()],
        )?;
    }
    Ok(records.len())
}
