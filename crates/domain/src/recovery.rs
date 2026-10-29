//! Owner recovery that folds legacy replacement-ID revision chains back into
//! the original memory ID as its later content versions. Legacy markers,
//! replacement rows, evidence and raw history stay; an exact preview token
//! fences every member, relation, loop marker and pending review.
use crate::util::*;
use crate::{Graph, context, curation, dto, importance, loops};
use memento_core::{Error, Result, WriteTxn};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};

fn legacy_kind(state: &str) -> Option<&'static str> {
    match state {
        "refined" => Some("refinement"),
        "corrected" => Some("correction"),
        "superseded" => Some("change"),
        _ => None,
    }
}

fn content(db: &Connection, id: &str, version: i64) -> Result<Option<Value>> {
    one(
        db,
        "SELECT * FROM node_versions WHERE id=? AND version=? AND state='active'",
        params![id, version],
    )
}

/// One legacy chain from its root, or the reason it cannot be recovered safely.
fn walk(
    db: &Connection,
    root: &str,
    next: &HashMap<String, Value>,
    incoming: &HashMap<String, usize>,
) -> Result<(Vec<String>, Vec<Value>, Option<&'static str>)> {
    let mut members = vec![root.to_owned()];
    let mut hops = vec![];
    let mut current = root.to_owned();
    while let Some(link) = next.get(&current) {
        let to = get_str(link, "superseded_by")?.to_owned();
        if members.len() > 64 {
            return Ok((members, hops, Some("too_deep")));
        }
        if members.contains(&to) {
            return Ok((members, hops, Some("cycle")));
        }
        if incoming.get(&to).copied().unwrap_or(0) > 1 {
            members.push(to);
            return Ok((members, hops, Some("branch")));
        }
        // A replacement is proven only when the same commit created it and its
        // still-active supersedes edge.
        let commit = get_str(link, "commit_id")?;
        let edge = one(db, "SELECT e.id,e.src,e.dst,l.version,l.state FROM edge_versions e JOIN edge_versions l ON l.id=e.id AND l.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id) WHERE e.src=? AND e.rel='supersedes' AND e.dst=? AND e.version=1 AND e.commit_id=?", params![to, current, commit])?;
        let created = one(
            db,
            "SELECT 1 FROM node_versions WHERE id=? AND version=1 AND commit_id=?",
            params![to, commit],
        )?;
        let Some(edge) = edge.filter(|edge| edge["state"] == "active" && created.is_some()) else {
            members.push(to);
            return Ok((members, hops, Some("unproven_lineage")));
        };
        let kind = legacy_kind(get_str(link, "state")?).unwrap();
        let version = match next.get(&to) {
            Some(after) => get_i64(after, "version")? - 1,
            None => {
                let end = node(db, &to)?;
                if end["state"] != "active" {
                    members.push(to);
                    return Ok((members, hops, Some("end_not_active")));
                }
                one(db, "SELECT min(version) AS version FROM node_revisions WHERE id=?", [&to])?
                    .and_then(|row| row["version"].as_i64())
                    .map_or(get_i64(&end, "version")?, |first| first - 1)
            }
        };
        let Some(row) = content(db, &to, version)? else {
            members.push(to);
            return Ok((members, hops, Some("unproven_lineage")));
        };
        hops.push(json!({"kind":kind,"valid_at":if kind=="change" {row["valid_from"].clone()} else {Value::Null},
            "source":{"id":to,"version":version},"supersedes":{"id":edge["id"],"src":edge["src"],"dst":edge["dst"],"version":edge["version"]}}));
        members.push(to.clone());
        current = to;
    }
    // Same-ID revisions the live end gained since cutover continue in order.
    let end = node(db, &current)?;
    let revisions = rows(
        db,
        "SELECT version,kind,valid_at,source_id FROM node_revisions WHERE id=? ORDER BY version",
        [&current],
    )?;
    for (index, revision) in revisions.iter().enumerate() {
        if !revision["source_id"].is_null() {
            return Ok((members, hops, Some("recovered_member")));
        }
        let version = revisions
            .get(index + 1)
            .map_or(Ok(get_i64(&end, "version")?), |after| {
                get_i64(after, "version").map(|v| v - 1)
            })?;
        if content(db, &current, version)?.is_none() {
            return Ok((members, hops, Some("unproven_lineage")));
        }
        hops.push(json!({"kind":revision["kind"],"valid_at":revision["valid_at"],"source":{"id":current,"version":version},"supersedes":null}));
    }
    let forgotten = one(
        db,
        "SELECT 1 FROM forgotten_targets WHERE target IN (SELECT value FROM json_each(?)) LIMIT 1",
        [encode_json(&json!(members))?],
    )?;
    Ok((
        members,
        hops,
        forgotten.is_some().then_some("forgotten"),
    ))
}

/// The fenced chain, or why it must stay untouched for a distinct review.
type Blocked = Option<(&'static str, Vec<String>)>;

fn fence(db: &Connection, root: &str, members: &[String], hops: Vec<Value>) -> Result<(Value, Blocked)> {
    let ids = encode_json(&json!(members))?;
    let end = members.last().unwrap();
    let mut summaries = vec![];
    let mut marks = json!({});
    // Every member state and recovered content version keeps the same
    // pin/sensitive gates as an ordinary protected identity change.
    let mut rows_seen = vec![];
    for id in members {
        let n = node(db, id)?;
        summaries.push(json!({"id":id,"version":n["version"],"state":n["state"],"type":n["type"],"text":excerpt(get_str(&n,"text")?,160)}));
        // Event identity and manual/source chronology, not only display state.
        let mut mark = loops::snapshot(db, id, get_i64(&n, "version")?)?;
        if let Some(live) = loops::current(db, id)? {
            for key in ["due_at", "deferred_until", "responsibility"] {
                mark[key] = live[key].clone();
            }
        }
        marks[id] = mark;
        rows_seen.push(n);
    }
    for hop in &hops {
        let source = get_str(&hop["source"], "id")?;
        rows_seen.extend(content(db, source, get_i64(&hop["source"], "version")?)?);
    }
    let mut reasons = vec![];
    if rows_seen.iter().any(|n| n["pinned"].as_i64().unwrap_or(0) != 0) {
        reasons.push("pinned");
    }
    let mut sensitive_type = false;
    for n in &rows_seen {
        sensitive_type |= sensitive(db, get_str(n, "type")?)?;
    }
    if sensitive_type {
        reasons.push("sensitive");
    }
    // Different types or scopes may be different referents or audiences.
    let mut blocked = None;
    for (key, reason) in [("type", "type_changed"), ("scope", "scope_changed")] {
        if blocked.is_none() && rows_seen.iter().any(|n| n[key] != rows_seen[0][key]) {
            blocked = Some((reason, vec![]));
        }
    }
    // Relations still on any retired member up to the last correction or
    // change (legacy orphans included) describe superseded content; recovery
    // must not make them current without an explicit decision.
    let left = hops
        .iter()
        .rposition(|hop| hop["kind"] != "refinement")
        .map_or(&members[..0], |last| &members[..(last + 1).min(members.len() - 1)]);
    let mut obsolete = vec![];
    let mut relations = json!({});
    let mut reattached = vec![];
    for edge in rows(
        db,
        "SELECT e.id,e.version,e.src,e.dst FROM edge_versions e WHERE (e.src IN (SELECT value FROM json_each(?1)) OR e.dst IN (SELECT value FROM json_each(?1))) AND e.state='active' AND e.rel<>'supersedes' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id) ORDER BY e.id",
        [&ids],
    )? {
        let edge_id = get_str(&edge, "id")?;
        relations[edge_id] = json!({"version":edge["version"],"loop":loops::snapshot(db,edge_id,get_i64(&edge,"version")?)?});
        if [&edge["src"], &edge["dst"]]
            .iter()
            .any(|side| side.as_str().is_some_and(|id| left.iter().any(|m| m == id)))
        {
            obsolete.push(edge_id.to_owned());
        }
        // Relations left on a retired member become current again on the root.
        if [&edge["src"], &edge["dst"]]
            .iter()
            .any(|side| side.as_str().is_some_and(|id| id != end && members.iter().any(|m| m == id)))
        {
            reattached.push(edge_id.to_owned());
        }
    }
    let reviews = rows(db, "SELECT DISTINCT p.id FROM review_proposals p,json_each(p.base_versions) j WHERE j.key IN (SELECT value FROM json_each(?)) AND NOT EXISTS (SELECT 1 FROM review_decisions d WHERE d.proposal_id=p.id) ORDER BY p.id", [&ids])?
        .into_iter()
        .map(|row| row["id"].clone())
        .collect::<Vec<_>>();
    if blocked.is_none() && !obsolete.is_empty() {
        blocked = Some(("relations_need_review", obsolete));
    }
    Ok((json!({"root":root,"members":summaries,"hops":hops,"relations":relations,
        "reattached_relations":reattached,"loops":marks,"pending_reviews":reviews,"protected_reasons":reasons}), blocked))
}

/// Read-only dry run: the next `limit` recoverable chains plus every ambiguous
/// one. Protected chains are only reported unless the owner names each root.
pub fn plan(db: &Connection, limit: usize, exclude: &[String], protected: &[String]) -> Result<Value> {
    if !(1..=256).contains(&limit) {
        return Err(Error::Validation("Recovery limit must be between 1 and 256"));
    }
    if protected.iter().any(|id| exclude.contains(id)) {
        return Err(Error::Validation(
            "A chain cannot be both excluded and selected as protected",
        ));
    }
    let links = rows(db, "SELECT n.id,n.version,n.state,n.superseded_by,n.commit_id FROM node_versions n WHERE n.state IN ('refined','corrected','superseded') AND n.superseded_by IS NOT NULL AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) ORDER BY n.id", [])?;
    let mut next = HashMap::new();
    let mut incoming = HashMap::<String, usize>::new();
    for link in &links {
        next.insert(get_str(link, "id")?.to_owned(), link.clone());
        *incoming
            .entry(get_str(link, "superseded_by")?.to_owned())
            .or_default() += 1;
    }
    let mut seen = HashSet::new();
    let (mut chains, mut ambiguous, mut excluded, mut held) = (vec![], vec![], vec![], vec![]);
    let mut selected = 0;
    let mut remaining = 0;
    for link in &links {
        let root = get_str(link, "id")?;
        if incoming.contains_key(root) {
            continue;
        }
        let (mut members, hops, reason) = walk(db, root, &next, &incoming)?;
        if reason.is_some() {
            // Report the whole unrecoverable chain once, under its real reason.
            while let Some(link) = members.last().and_then(|id| next.get(id)) {
                let to = get_str(link, "superseded_by")?.to_owned();
                if members.len() > 64 || members.contains(&to) {
                    break;
                }
                members.push(to);
            }
        }
        seen.extend(members.iter().cloned());
        if let Some(reason) = reason {
            ambiguous.push(json!({"root":root,"members":members,"reason":reason}));
        } else if exclude.iter().any(|id| id == root) {
            excluded.push(json!(root));
        } else {
            let (chain, blocked) = fence(db, root, &members, hops)?;
            if let Some((reason, relations)) = blocked {
                let mut entry = json!({"root":root,"members":members,"reason":reason});
                if !relations.is_empty() {
                    entry["relations"] = json!(relations);
                }
                ambiguous.push(entry);
                continue;
            }
            let guarded = !chain["protected_reasons"].as_array().unwrap().is_empty();
            if guarded && protected.iter().any(|id| id == root) {
                selected += 1;
            } else if guarded {
                held.push(json!({"root":root,"members":members,"reasons":chain["protected_reasons"]}));
                continue;
            }
            if chains.len() < limit {
                chains.push(chain);
            } else {
                remaining += 1;
            }
        }
    }
    if selected != protected.len() {
        return Err(Error::Validation(
            "Protected selections must name protected recoverable chains",
        ));
    }
    for link in &links {
        let id = get_str(link, "id")?;
        if !seen.contains(id) {
            ambiguous.push(json!({"root":id,"members":[id],"reason":"cycle"}));
        }
    }
    let ambiguous_count = ambiguous.len();
    ambiguous.truncate(limit);
    let protected_count = held.len();
    held.truncate(limit);
    let mut plan = json!({"policy":"owner_legacy_revision_recovery","raw_history":"retained","limit":limit,
        "chains":chains,"remaining_ready":remaining,"excluded":excluded,"protected":held,"protected_count":protected_count,
        "ambiguous":ambiguous,"ambiguous_count":ambiguous_count});
    plan["token"] = json!(hash_json(&plan)?);
    Ok(plan)
}

fn apply_chain(tx: &WriteTxn, commit: &str, chain: &Value) -> Result<Value> {
    let root = get_str(chain, "root")?;
    let members = chain["members"].as_array().unwrap();
    let mut version = get_i64(&members[0], "version")?;
    for hop in chain["hops"].as_array().unwrap() {
        let source = get_str(&hop["source"], "id")?;
        let source_version = get_i64(&hop["source"], "version")?;
        let row = content(tx.sql(), source, source_version)?
            .ok_or(Error::Conflict("Recovery content changed; preview again"))?;
        version += 1;
        tx.sql().execute(
            "INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            params![
                root,
                version,
                commit,
                row["type"].as_str(),
                row["text"].as_str(),
                "active",
                row["valid_from"].as_str(),
                row["valid_to"].as_str(),
                Option::<&str>::None,
                row["pinned"].as_i64(),
                row["scope"].as_str()
            ],
        )?;
        tx.sql().execute(
            "INSERT INTO node_revisions(id,version,kind,valid_at,commit_id,source_id,source_version) VALUES (?,?,?,?,?,?,?)",
            params![root, version, hop["kind"].as_str(), hop["valid_at"].as_str(), commit, source, source_version],
        )?;
        curation::union_evidence(tx, root, version, &[(source.into(), source_version)])?;
        // The replacement no longer supersedes its predecessor; its row stays.
        if let Some(edge) = hop["supersedes"].as_object() {
            let edge_id = edge["id"].as_str().unwrap();
            let edge_version = edge["version"].as_i64().unwrap();
            tx.sql().execute(
                "INSERT INTO edge_versions VALUES (?,?,?,?,?,?,?)",
                params![
                    edge_id,
                    edge_version + 1,
                    commit,
                    edge["src"].as_str(),
                    "supersedes",
                    edge["dst"].as_str(),
                    "inactive"
                ],
            )?;
            curation::copy_evidence(tx, edge_id, edge_version, edge_version + 1)?;
        }
    }
    // The live end's marker is current; the root's legacy marker was handed on.
    let end = get_str(members.last().unwrap(), "id")?;
    if let Some(live) = loops::current(tx.sql(), end)? {
        let mine = loops::current(tx.sql(), root)?;
        let marker = |value: &Value| {
            (
                value["state"].clone(),
                value["due_at"].clone(),
                value["deferred_until"].clone(),
            )
        };
        if mine.as_ref().map(marker) != Some(marker(&live)) {
            loops::append(
                tx,
                commit,
                root,
                get_str(&live, "state")?,
                live["due_at"].as_str(),
                live["deferred_until"].as_str(),
            )?;
        }
        if live["state"] == "open" {
            loops::append(tx, commit, end, "closed", live["due_at"].as_str(), None)?;
        }
    }
    let mut merged = vec![];
    for member in &members[1..] {
        let id = get_str(member, "id")?;
        let n = node(tx.sql(), id)?;
        let current = get_i64(&n, "version")?;
        tx.sql().execute(
            "INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            params![
                id,
                current + 1,
                commit,
                n["type"].as_str(),
                n["text"].as_str(),
                "merged",
                n["valid_from"].as_str(),
                n["valid_to"].as_str(),
                root,
                n["pinned"].as_i64(),
                n["scope"].as_str()
            ],
        )?;
        curation::copy_evidence(tx, id, current, current + 1)?;
        tx.sql().execute(
            "INSERT INTO entity_merges VALUES (?,?,?)",
            params![id, root, commit],
        )?;
        curation::redirect_edges(tx, commit, id, root, None)?;
        importance::transfer_usage(tx, id, root)?;
        curation::invalidate_vectors(tx, id)?;
        merged.push(json!(id));
    }
    curation::invalidate_vectors(tx, root)?;
    let ids = members
        .iter()
        .map(|member| get_str(member, "id").map(String::from))
        .collect::<Result<Vec<_>>>()?;
    let requeued = curation::requeue_stale(tx, commit, &ids, None)?;
    Ok(json!({"id":root,"version":version,"merged":merged,"requeued_candidates":requeued}))
}

/// Applies exactly the confirmed preview; any change after it refuses the plan.
pub fn recover(graph: &Graph, command: dto::RecoverRevisions) -> Result<Value> {
    let mut exclude = command.exclude;
    exclude.sort();
    exclude.dedup();
    let mut protected = command.protected;
    protected.sort();
    protected.dedup();
    let request = json!({"op":"recover_revisions","limit":command.limit,"exclude":exclude,"protected":protected,"token":command.token});
    let ctx = context(command.command_id, command.actor, command.why);
    graph.store.command(&ctx, &request, |tx, commit| {
        let plan = plan(tx.sql(), command.limit, &exclude, &protected)?;
        if plan["token"] != command.token {
            return Err(Error::Conflict(
                "Recovery plan is stale; preview and confirm the current plan",
            ));
        }
        let chains = plan["chains"].as_array().unwrap();
        if chains.is_empty() {
            return Err(Error::Validation("No recoverable legacy revision chains"));
        }
        let mut recovered = vec![];
        for chain in chains {
            recovered.push(apply_chain(tx, commit, chain)?);
        }
        Ok(json!({"recovered":recovered,"remaining_ready":plan["remaining_ready"],"ambiguous_count":plan["ambiguous_count"]}))
    })
}

/// The retired replacement-ID supersede write path, so tests can build the
/// legacy chains that recovery folds back.
#[cfg(test)]
pub(crate) fn legacy_supersede(graph: &Graph, command: Value) -> Result<Value> {
    use crate::TxGraph;
    let ctx = context(
        get_str(&command, "command_id")?.into(),
        "legacy".into(),
        "Legacy replacement fixture".into(),
    );
    graph.store.command(&ctx, &command, |tx, commit| {
        let request = &command;
        let old = get_str(request, "old")?;
        let new = &request["new"];
        let kind = get_str(request, "kind")?;
        let previous = node(tx.sql(), old)?;
        let valid_at = request["valid_at"].as_str();
        let (from, to) = match kind {
            "change" => (request["valid_at"].clone(), Value::Null),
            "refinement" => (previous["valid_from"].clone(), previous["valid_to"].clone()),
            _ => (Value::Null, Value::Null),
        };
        let replacement = TxGraph::insert_node(
            tx,
            commit,
            &json!({"type":new["type"],"text":new["text"],"scope":new["scope"],
            "valid_from":from,"valid_to":to,"evidence":request["evidence"],"pinned":previous["pinned"].as_i64().unwrap_or(0) != 0}),
        )?;
        let replacement_id = get_str(&replacement, "id")?;
        let version = get_i64(&previous, "version")? + 1;
        let state = match kind {
            "correction" => "corrected",
            "change" => "superseded",
            _ => "refined",
        };
        tx.sql().execute(
            "INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            params![
                old,
                version,
                commit,
                get_str(&previous, "type")?,
                get_str(&previous, "text")?,
                state,
                previous["valid_from"].as_str(),
                if kind == "change" {
                    valid_at
                } else {
                    previous["valid_to"].as_str()
                },
                replacement_id,
                get_i64(&previous, "pinned")?,
                get_str(&previous, "scope")?
            ],
        )?;
        TxGraph::evidence(tx, old, version, &request["evidence"])?;
        curation::invalidate_vectors(tx, old)?;
        let edge = short_id();
        tx.sql().execute(
            "INSERT INTO edge_versions VALUES (?,?,?,?,?,?,?)",
            params![edge, 1, commit, replacement_id, "supersedes", old, "active"],
        )?;
        TxGraph::evidence(tx, &edge, 1, &request["evidence"])?;
        if kind == "refinement" {
            curation::union_evidence(
                tx,
                replacement_id,
                1,
                &[(old.to_owned(), get_i64(&previous, "version")?)],
            )?;
            curation::redirect_edges(tx, commit, old, replacement_id, None)?;
        }
        loops::transfer(tx, commit, old, replacement_id)?;
        importance::transfer_usage(tx, old, replacement_id)?;
        Ok(json!({"id":old,"version":version,"superseded_by":replacement_id,"kind":kind}))
    })
}
