//! Protected review, identity continuity, retirement and exact owner merge plans.
use crate::util::*;
use crate::{Graph, TxGraph, behavior, context, dto, importance, loops};
use memento_core::{Error, Result, WriteTxn};
use rusqlite::{Connection, params};
use serde_json::{Value, json};

pub fn invalidate_vectors(tx: &WriteTxn, target: &str) -> Result<()> {
    tx.sql().execute("UPDATE vector_blobs SET current=0 WHERE seq IN (SELECT seq FROM vector_items WHERE (kind='node' AND target=?) OR (kind='segment' AND target IN (SELECT id FROM segments WHERE node_id=?)))",params![target,target])?;
    tx.touch_target(target)?;
    Ok(())
}
pub fn copy_vector(tx: &WriteTxn, target: &str, old: i64, new: i64) -> Result<()> {
    tx.sql().execute("UPDATE vector_blobs SET current=0 WHERE seq IN (SELECT seq FROM vector_items WHERE kind='node' AND target=?)",[target])?;
    let previous = one(
        tx.sql(),
        "SELECT i.seq FROM vector_items i JOIN vector_blobs v ON v.seq=i.seq WHERE i.target=? AND i.kind='node' AND i.version=?",
        params![target, old],
    )?;
    if let Some(previous) = previous {
        let seq = get_i64(&previous, "seq")?;
        tx.sql().execute(
            "UPDATE vector_items SET version=? WHERE seq=?",
            params![new, seq],
        )?;
        tx.sql()
            .execute("UPDATE vector_blobs SET current=1 WHERE seq=?", [seq])?;
    }
    tx.touch_target(target)?;
    Ok(())
}
pub fn union_evidence(
    tx: &WriteTxn,
    target: &str,
    version: i64,
    sources: &[(String, i64)],
) -> Result<()> {
    for (source, source_version) in sources {
        tx.sql().execute("INSERT INTO graph_evidence SELECT ?,?,g.episode_id,g.start,g.end,g.quote FROM graph_evidence g WHERE g.target=? AND g.version=? AND NOT EXISTS (SELECT 1 FROM graph_evidence x WHERE x.target=? AND x.version=? AND x.episode_id=g.episode_id AND x.start=g.start AND x.end=g.end) AND (g.target=? OR NOT EXISTS (SELECT 1 FROM episodes e JOIN forgotten_sources f ON f.source=e.source AND f.event_id=e.event_id WHERE e.id=g.episode_id))",params![target,version,source,source_version,target,version,target])?;
    }
    tx.touch_target(target)?;
    Ok(())
}
fn copy_evidence(tx: &WriteTxn, target: &str, old: i64, new: i64) -> Result<()> {
    tx.sql().execute("INSERT INTO graph_evidence SELECT target,?,episode_id,start,end,quote FROM graph_evidence WHERE target=? AND version=?",params![new,target,old])?;
    tx.touch_target(target)?;
    Ok(())
}
pub fn pin(tx: &WriteTxn, commit: &str, target: &str, pinned: bool) -> Result<Value> {
    let n = node(tx.sql(), target)?;
    let old = get_i64(&n, "version")?;
    if n["state"] != "active" {
        return Err(Error::Validation("Pin requires an active node"));
    }
    if (n["pinned"].as_i64().unwrap_or(0) != 0) == pinned {
        return Ok(json!({"id":target,"version":old,"pinned":pinned}));
    }
    tx.sql().execute(
        "INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        params![
            target,
            old + 1,
            commit,
            n["type"].as_str(),
            n["text"].as_str(),
            n["state"].as_str(),
            n["valid_from"].as_str(),
            n["valid_to"].as_str(),
            n["superseded_by"].as_str(),
            i64::from(pinned),
            n["scope"].as_str()
        ],
    )?;
    copy_evidence(tx, target, old, old + 1)?;
    copy_vector(tx, target, old, old + 1)?;
    Ok(json!({"id":target,"version":old+1,"pinned":pinned}))
}
pub fn attach(
    tx: &WriteTxn,
    commit: &str,
    target: &str,
    evidence: &Value,
    approved: bool,
    expected: Option<i64>,
) -> Result<Value> {
    let n = one(
        tx.sql(),
        "SELECT * FROM node_versions WHERE id=? ORDER BY version DESC LIMIT 1",
        [target],
    )?;
    let row = match &n {
        Some(n) => n.clone(),
        None => one(
            tx.sql(),
            "SELECT * FROM edge_versions WHERE id=? ORDER BY version DESC LIMIT 1",
            [target],
        )?
        .ok_or(Error::Conflict(
            "Corroboration requires an active assertion",
        ))?,
    };
    let old = get_i64(&row, "version")?;
    if row["state"] != "active" {
        return Err(Error::Conflict(
            "Corroboration requires an active assertion",
        ));
    }
    if expected.is_some_and(|version| version != old) {
        return Err(Error::Conflict("Corroboration proposal is stale"));
    }
    let mut bases = json!({});
    let endpoints = if let Some(n) = &n {
        bases[target] = n["version"].clone();
        vec![n.clone()]
    } else {
        let a = node(tx.sql(), get_str(&row, "src")?)?;
        let b = node(tx.sql(), get_str(&row, "dst")?)?;
        bases[get_str(&a, "id")?] = a["version"].clone();
        bases[get_str(&b, "id")?] = b["version"].clone();
        vec![a, b]
    };
    let mut protected = false;
    for endpoint in endpoints {
        if endpoint["state"] != "active" {
            return Err(Error::Conflict(
                "Corroboration endpoints are no longer active",
            ));
        }
        protected |= endpoint["pinned"].as_i64().unwrap_or(0) != 0
            || sensitive(tx.sql(), get_str(&endpoint, "type")?)?;
    }
    if !approved && protected {
        return TxGraph::propose(
            tx,
            commit,
            &json!({"op":"attach","target":target,"evidence":evidence,"expected_version":old}),
            &bases,
        );
    }
    if n.is_some() {
        tx.sql().execute(
            "INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            params![
                target,
                old + 1,
                commit,
                row["type"].as_str(),
                row["text"].as_str(),
                row["state"].as_str(),
                row["valid_from"].as_str(),
                row["valid_to"].as_str(),
                row["superseded_by"].as_str(),
                row["pinned"].as_i64(),
                row["scope"].as_str()
            ],
        )?;
        copy_vector(tx, target, old, old + 1)?;
    } else {
        tx.sql().execute(
            "INSERT INTO edge_versions VALUES (?,?,?,?,?,?,?)",
            params![
                target,
                old + 1,
                commit,
                row["src"].as_str(),
                row["rel"].as_str(),
                row["dst"].as_str(),
                row["state"].as_str()
            ],
        )?;
    }
    copy_evidence(tx, target, old, old + 1)?;
    TxGraph::evidence(tx, target, old + 1, evidence)?;
    Ok(json!({"id":target,"version":old+1}))
}
fn validate_type(request: &Value) -> Result<()> {
    if !request["name"].as_str().is_some_and(valid_type)
        || !matches!(request["category"].as_str(), Some("memory" | "entity"))
        || !request["sensitive"].is_boolean()
    {
        return Err(Error::Validation("Invalid reviewed type definition"));
    }
    Ok(())
}
pub fn propose(tx: &WriteTxn, commit: &str, request: &Value) -> Result<Value> {
    let mut bases = json!({});
    match get_str(request, "op")? {
        "register_type" => {
            validate_type(request)?;
            if known_type(tx.sql(), get_str(request, "name")?)? {
                return Err(Error::Conflict("Type already registered"));
            }
        }
        "pin" => {
            if !request["pinned"].is_boolean() {
                return Err(Error::Validation("Pin state must be boolean"));
            }
            let n = node(tx.sql(), get_str(request, "node_id")?)?;
            if n["state"] != "active" {
                return Err(Error::Validation("Only active nodes can be pinned"));
            }
            bases[get_str(&n, "id")?] = n["version"].clone();
        }
        "merge" => {
            let a = node(tx.sql(), get_str(request, "a")?)?;
            let b = node(tx.sql(), get_str(request, "b")?)?;
            if a["id"] == b["id"]
                || a["type"] != b["type"]
                || a["state"] != "active"
                || b["state"] != "active"
                || !entity_type(tx.sql(), get_str(&a, "type")?)?
            {
                return Err(Error::Validation(
                    "Merge requires distinct active entities of the same type",
                ));
            }
            bases[get_str(&a, "id")?] = a["version"].clone();
            bases[get_str(&b, "id")?] = b["version"].clone();
        }
        _ => return Err(Error::Validation("Unknown protected operation")),
    }
    TxGraph::propose(tx, commit, request, &bases)
}
pub fn apply_review(
    tx: &WriteTxn,
    commit: &str,
    request: &Value,
    proposal: Option<&str>,
) -> Result<Value> {
    match get_str(request, "op")? {
        "register_type" => {
            validate_type(request)?;
            let name = get_str(request, "name")?;
            if known_type(tx.sql(), name)? {
                return Err(Error::Conflict("Type was registered after this proposal"));
            }
            tx.sql().execute(
                "INSERT INTO memory_types VALUES (?,?,?,?)",
                params![
                    name,
                    request["category"].as_str(),
                    i64::from(request["sensitive"].as_bool().unwrap()),
                    commit
                ],
            )?;
            tx.touch_type(name)?;
            Ok(json!({"type":name,"status":"registered"}))
        }
        "pin" => pin(
            tx,
            commit,
            get_str(request, "node_id")?,
            request["pinned"]
                .as_bool()
                .ok_or(Error::Validation("Pin state must be boolean"))?,
        ),
        "attach" => attach(
            tx,
            commit,
            get_str(request, "target")?,
            &request["evidence"],
            true,
            request["expected_version"].as_i64(),
        ),
        "absorb" => {
            let members = request["absorbed"]
                .as_array()
                .ok_or(Error::Validation("Invalid absorbed members"))?
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .ok_or(Error::Validation("Invalid absorbed member"))
                })
                .collect::<Result<Vec<_>>>()?;
            absorb(
                tx,
                commit,
                get_str(request, "survivor")?,
                &members,
                Some(&request["evidence"]),
                true,
                proposal,
            )
        }
        "merge" => absorb_one(
            tx,
            commit,
            get_str(request, "a")?,
            get_str(request, "b")?,
            true,
            proposal,
        ),
        _ => Err(Error::Validation("Unknown protected operation")),
    }
}
fn choice(db: &Connection, target: &str) -> Result<Option<Value>> {
    Ok(loops::current(db, target)?.map(|marker| {
        if marker["state"] == "closed" {
            json!(["closed"])
        } else {
            json!([marker["state"], marker["due_at"], marker["deferred_until"]])
        }
    }))
}
pub fn redirect_edges(
    tx: &WriteTxn,
    commit: &str,
    old: &str,
    new: &str,
    edge_ids: Option<&[String]>,
) -> Result<Vec<Value>> {
    if old == new {
        return Ok(vec![]);
    }
    let selected = rows(
        tx.sql(),
        "SELECT e.* FROM edge_versions e WHERE (e.src=? OR e.dst=?) AND e.state='active' AND e.rel<>'supersedes' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id) ORDER BY e.id",
        params![old, old],
    )?;
    let mut moved = vec![];
    for edge in selected {
        let edge_id = get_str(&edge, "id")?;
        if edge_ids.is_some_and(|ids| !ids.iter().any(|id| id == edge_id)) {
            continue;
        }
        let src = if edge["src"] == old {
            new
        } else {
            get_str(&edge, "src")?
        };
        let dst = if edge["dst"] == old {
            new
        } else {
            get_str(&edge, "dst")?
        };
        let version = get_i64(&edge, "version")?;
        let mine = choice(tx.sql(), edge_id)?;
        let mine_manual = loops::snapshot(tx.sql(), edge_id, version)?["manual_choice_at"].clone();
        let mut same = None;
        if src != dst {
            for row in rows(
                tx.sql(),
                "SELECT e.* FROM edge_versions e WHERE e.src=? AND e.rel=? AND e.dst=? AND e.id<>? AND e.state='active' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id) ORDER BY e.id",
                params![src, edge["rel"].as_str(), dst, edge_id],
            )? {
                let theirs = choice(tx.sql(), get_str(&row, "id")?)?;
                let manual = loops::snapshot(
                    tx.sql(),
                    get_str(&row, "id")?,
                    get_i64(&row, "version")?,
                )?["manual_choice_at"]
                    .clone();
                if mine.is_none()
                    || mine == theirs && mine_manual == manual
                    || theirs.is_none() && mine_manual.is_null()
                {
                    same = Some(row);
                    break;
                }
            }
        }
        let state = if src == dst || same.is_some() {
            "inactive"
        } else {
            "active"
        };
        tx.sql().execute(
            "INSERT INTO edge_versions VALUES (?,?,?,?,?,?,?)",
            params![
                edge_id,
                version + 1,
                commit,
                src,
                edge["rel"].as_str(),
                dst,
                state
            ],
        )?;
        copy_evidence(tx, edge_id, version, version + 1)?;
        if let Some(same) = &same {
            let target = get_str(same, "id")?;
            let v = get_i64(same, "version")?;
            tx.sql().execute(
                "INSERT INTO edge_versions VALUES (?,?,?,?,?,?,?)",
                params![
                    target,
                    v + 1,
                    commit,
                    same["src"].as_str(),
                    same["rel"].as_str(),
                    same["dst"].as_str(),
                    "active"
                ],
            )?;
            union_evidence(
                tx,
                target,
                v + 1,
                &[(target.into(), v), (edge_id.into(), version)],
            )?;
            loops::transfer(tx, commit, edge_id, target)?;
        }
        tx.touch_target(edge_id)?;
        tx.touch_target(src)?;
        tx.touch_target(dst)?;
        moved.push(json!({"id":edge_id,"version":version+1,"src":src,"dst":dst,"state":state,"folded_into":same.as_ref().map(|row|row["id"].clone())}));
    }
    Ok(moved)
}
fn requeue_stale(
    tx: &WriteTxn,
    commit: &str,
    targets: &[String],
    exclude: Option<&str>,
) -> Result<Vec<String>> {
    let proposals = rows(
        tx.sql(),
        "SELECT DISTINCT p.id,p.base_versions FROM review_proposals p,json_each(p.base_versions) j WHERE j.key IN (SELECT value FROM json_each(?)) AND p.id IS NOT ? AND NOT EXISTS (SELECT 1 FROM review_decisions d WHERE d.proposal_id=p.id) ORDER BY p.id",
        params![encode_json(&json!(targets))?, exclude],
    )?;
    let mut requeued = vec![];
    for proposal in proposals {
        let bases = decode_json(get_str(&proposal, "base_versions")?)?;
        let mut stale = false;
        for (target, version) in bases
            .as_object()
            .ok_or(Error::Validation("Invalid proposal bases"))?
        {
            stale |= node(tx.sql(), target)?["version"] != *version;
        }
        if !stale {
            continue;
        }
        for row in rows(
            tx.sql(),
            "SELECT d.candidate_id FROM candidate_decisions d WHERE d.decision='review' AND d.target_id=? AND d.rowid=(SELECT max(x.rowid) FROM candidate_decisions x WHERE x.candidate_id=d.candidate_id) ORDER BY d.candidate_id",
            [get_str(&proposal, "id")?],
        )? {
            let target = get_str(&row, "candidate_id")?;
            tx.sql().execute(
                "INSERT INTO candidate_decisions VALUES (?,?,?,?,?,?)",
                params![
                    id(),
                    target,
                    commit,
                    "defer",
                    Option::<&str>::None,
                    Option::<f64>::None
                ],
            )?;
            tx.touch_target(target)?;
            requeued.push(target.into());
        }
    }
    Ok(requeued)
}
pub fn absorb_one(
    tx: &WriteTxn,
    commit: &str,
    source: &str,
    target: &str,
    entity: bool,
    proposal: Option<&str>,
) -> Result<Value> {
    let a = node(tx.sql(), source)?;
    let b = node(tx.sql(), target)?;
    if a["state"] != "active"
        || b["state"] != "active"
        || source == target
        || a["type"] != b["type"]
    {
        return Err(Error::Conflict("Entity merge is no longer applicable"));
    }
    let av = get_i64(&a, "version")?;
    let bv = get_i64(&b, "version")?;
    tx.sql().execute(
        "INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        params![
            source,
            av + 1,
            commit,
            a["type"].as_str(),
            a["text"].as_str(),
            "merged",
            a["valid_from"].as_str(),
            a["valid_to"].as_str(),
            target,
            a["pinned"].as_i64(),
            a["scope"].as_str()
        ],
    )?;
    copy_evidence(tx, source, av, av + 1)?;
    tx.sql().execute(
        "INSERT INTO entity_merges VALUES (?,?,?)",
        params![source, target, commit],
    )?;
    if entity {
        tx.sql().execute(
            "INSERT INTO alias_fts(id,text) VALUES (?,?)",
            params![source, a["text"].as_str()],
        )?;
    }
    let mut labels = decode_json(get_str(&a, "scope")?)?
        .as_array()
        .unwrap()
        .clone();
    for label in decode_json(get_str(&b, "scope")?)?.as_array().unwrap() {
        if !labels.contains(label) {
            labels.push(label.clone());
        }
    }
    labels.sort_by(|a, b| a.as_str().cmp(&b.as_str()));
    tx.sql().execute(
        "INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        params![
            target,
            bv + 1,
            commit,
            b["type"].as_str(),
            b["text"].as_str(),
            b["state"].as_str(),
            b["valid_from"].as_str(),
            b["valid_to"].as_str(),
            b["superseded_by"].as_str(),
            b["pinned"].as_i64(),
            encode_json(&json!(labels))?
        ],
    )?;
    union_evidence(
        tx,
        target,
        bv + 1,
        &[(target.into(), bv), (source.into(), av)],
    )?;
    copy_vector(tx, target, bv, bv + 1)?;
    redirect_edges(tx, commit, source, target, None)?;
    invalidate_vectors(tx, source)?;
    if a["pinned"].as_i64().unwrap_or(0) != 0 && b["pinned"].as_i64().unwrap_or(0) == 0 {
        pin(tx, commit, target, true)?;
    }
    importance::transfer_usage(tx, source, target)?;
    loops::transfer(tx, commit, source, target)?;
    let requeued = requeue_stale(tx, commit, &[source.into(), target.into()], proposal)?;
    tx.touch_target(source)?;
    tx.touch_target(target)?;
    Ok(
        json!({"id":source,"version":av+1,"merged_into":target,"survivor_version":node(tx.sql(),target)?["version"],"scope":labels,"requeued_candidates":requeued}),
    )
}
pub fn canonical(db: &Connection, target: &str) -> Result<Value> {
    let mut current = target.to_owned();
    for _ in 0..64 {
        let n = node(db, &current)?;
        if !matches!(n["state"].as_str(), Some("merged" | "refined"))
            || n["superseded_by"].is_null()
        {
            return Ok(n);
        }
        current = get_str(&n, "superseded_by")?.into();
    }
    Err(Error::Validation("Entity merge chain is too deep"))
}

pub fn absorb_check(
    db: &Connection,
    survivor: &str,
    absorbed: &[String],
) -> Result<Option<&'static str>> {
    if absorbed.is_empty()
        || absorbed.len() > 8
        || absorbed.iter().any(|id| id == survivor)
        || absorbed
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len()
            != absorbed.len()
    {
        return Ok(Some("consolidation_absorb_member_invalid"));
    }
    let mut nodes = vec![];
    for id in std::iter::once(survivor).chain(absorbed.iter().map(String::as_str)) {
        match node(db, id) {
            Ok(n) if n["state"] == "active" => nodes.push(n),
            Ok(_) | Err(Error::Validation(_)) | Err(Error::NotFound(_)) => {
                return Ok(Some("consolidation_absorb_member_invalid"));
            }
            Err(error) => return Err(error),
        }
    }
    if nodes.iter().any(|n| n["type"] != nodes[0]["type"]) {
        return Ok(Some("consolidation_absorb_type_mismatch"));
    }
    if !entity_type(db, get_str(&nodes[0], "type")?)? {
        if nodes.iter().any(|n| n["scope"] != nodes[0]["scope"]) {
            return Ok(Some("consolidation_absorb_scope_mismatch"));
        }
        if nodes.iter().any(|n| {
            n["valid_from"] != nodes[0]["valid_from"] || n["valid_to"] != nodes[0]["valid_to"]
        }) {
            return Ok(Some("consolidation_absorb_validity_mismatch"));
        }
        let b = behavior(db, survivor)?;
        for target in absorbed {
            if let Some(other) = behavior(db, target)? {
                if b.as_ref() != Some(&other) {
                    return Ok(Some("consolidation_absorb_behavior_mismatch"));
                }
            }
        }
    }
    let mut choices = vec![];
    for n in &nodes {
        if let Some(value) = choice(db, get_str(n, "id")?)? {
            if !choices.contains(&value) {
                choices.push(value);
            }
        }
    }
    if choices.len() > 1 {
        return Ok(Some("consolidation_absorb_loop_conflict"));
    }
    let mut ids = absorbed.to_vec();
    ids.push(survivor.into());
    let mut groups: std::collections::BTreeMap<(String, String, String), Vec<Value>> =
        std::collections::BTreeMap::new();
    for edge in rows(
        db,
        "SELECT e.* FROM edge_versions e WHERE (e.src IN (SELECT value FROM json_each(?1)) OR e.dst IN (SELECT value FROM json_each(?1))) AND e.state='active' AND e.rel<>'supersedes' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id)",
        [encode_json(&json!(ids))?],
    )? {
        let a = get_str(&edge, "src")?;
        let b = get_str(&edge, "dst")?;
        let src = if absorbed.iter().any(|id| id == a) {
            survivor
        } else {
            a
        };
        let dst = if absorbed.iter().any(|id| id == b) {
            survivor
        } else {
            b
        };
        let marker = choice(db, get_str(&edge, "id")?)?;
        if src == dst && (absorbed.iter().any(|id| id == a || id == b)) {
            if marker.as_ref().is_some_and(|value| value[0] == "open") {
                return Ok(Some("consolidation_absorb_loop_conflict"));
            }
            continue;
        }
        if let Some(value) = marker {
            let group = groups
                .entry((src.into(), get_str(&edge, "rel")?.into(), dst.into()))
                .or_default();
            if !group.contains(&value) {
                group.push(value);
            }
        }
    }
    if groups.values().any(|values| values.len() > 1) {
        return Ok(Some("consolidation_absorb_loop_conflict"));
    }
    Ok(None)
}
pub fn absorb(
    tx: &WriteTxn,
    commit: &str,
    survivor: &str,
    absorbed: &[String],
    evidence: Option<&Value>,
    approved: bool,
    proposal: Option<&str>,
) -> Result<Value> {
    if let Some(code) = absorb_check(tx.sql(), survivor, absorbed)? {
        return Err(Error::Conflict(code));
    }
    let mut bases = json!({});
    let mut protected = false;
    let first = node(tx.sql(), survivor)?;
    for id in std::iter::once(survivor).chain(absorbed.iter().map(String::as_str)) {
        let n = node(tx.sql(), id)?;
        bases[id] = n["version"].clone();
        protected |= n["pinned"].as_i64().unwrap_or(0) != 0
            || sensitive(tx.sql(), get_str(&n, "type")?)?
            || n["scope"] != first["scope"];
    }
    if protected && !approved {
        let evidence = evidence
            .filter(|value| value.as_array().is_some_and(|items| !items.is_empty()))
            .ok_or(Error::Validation(
                "A protected absorb requires evidence for human review",
            ))?;
        return TxGraph::propose(
            tx,
            commit,
            &json!({"op":"absorb","survivor":survivor,"absorbed":absorbed,"evidence":evidence}),
            &bases,
        );
    }
    let entity = entity_type(tx.sql(), get_str(&first, "type")?)?;
    let mut merged = vec![];
    let mut requeued = std::collections::BTreeSet::new();
    for source in absorbed {
        let result = absorb_one(tx, commit, source, survivor, entity, proposal)?;
        for id in result["requeued_candidates"]
            .as_array()
            .into_iter()
            .flatten()
        {
            requeued.insert(id.as_str().unwrap_or("").to_owned());
        }
        merged.push(result);
    }
    let last = merged
        .last()
        .ok_or(Error::Validation("No absorbed members"))?;
    if let Some(evidence) = evidence {
        TxGraph::evidence(tx, survivor, get_i64(last, "survivor_version")?, evidence)?;
    }
    Ok(
        json!({"id":survivor,"version":last["survivor_version"],"absorbed":absorbed,"scope":last["scope"],"status":"absorbed","requeued_candidates":requeued.into_iter().collect::<Vec<_>>()}),
    )
}
pub fn retire(tx: &WriteTxn, commit: &str, target: &str, evidence: &Value) -> Result<Value> {
    let n = node(tx.sql(), target).map_err(|error| match error {
        Error::Validation(_) | Error::NotFound(_) => {
            Error::Conflict("consolidation_retire_target_invalid")
        }
        other => other,
    })?;
    let v = get_i64(&n, "version")?;
    if n["state"] != "active" {
        return Err(Error::Conflict("consolidation_retire_target_invalid"));
    }
    if !memory_type(tx.sql(), get_str(&n, "type")?)? {
        return Err(Error::Conflict("consolidation_retire_type_invalid"));
    }
    let originals = rows(
        tx.sql(),
        "SELECT episode_id,quote FROM graph_evidence WHERE target=? AND version=?",
        params![target, v],
    )?;
    if evidence.as_array().is_none_or(|refs| {
        refs.is_empty()
            || refs.iter().any(|reference| {
                reference["quote"].as_str().is_none_or(|quote| {
                    quote.is_empty()
                        || !originals.iter().any(|original| {
                            original["episode_id"] == reference["episode_id"]
                                && original["quote"]
                                    .as_str()
                                    .is_some_and(|text| text.contains(quote))
                        })
                })
            })
    }) {
        return Err(Error::Conflict("consolidation_retire_evidence_invalid"));
    }
    let edges = rows(
        tx.sql(),
        "SELECT e.* FROM edge_versions e WHERE (e.src=? OR e.dst=?) AND e.state='active' AND e.rel<>'supersedes' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id) ORDER BY e.id",
        params![target, target],
    )?;
    let mut protection = if n["pinned"].as_i64().unwrap_or(0) != 0 {
        Some("protected_pin")
    } else if sensitive(tx.sql(), get_str(&n, "type")?)? {
        Some("protected_sensitive")
    } else if behavior(tx.sql(), target)?.is_some_and(|value| value["authority"] == "explicit_user")
    {
        Some("protected_explicit_user")
    } else {
        None
    };
    if protection.is_none() {
        for (id, version) in std::iter::once((target, v)).chain(edges.iter().map(|edge| {
            (
                edge["id"].as_str().unwrap(),
                edge["version"].as_i64().unwrap(),
            )
        })) {
            let marker = loops::current(tx.sql(), id)?;
            if !loops::snapshot(tx.sql(), id, version)?["manual_choice_at"].is_null()
                || marker.as_ref().is_some_and(|value| {
                    !value["due_at"].is_null() || !value["deferred_until"].is_null()
                })
            {
                protection = Some("protected_loop_choice");
                break;
            }
        }
    }
    if let Some(status) = protection {
        return Ok(json!({"id":target,"version":v,"status":status}));
    }
    tx.sql().execute(
        "INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        params![
            target,
            v + 1,
            commit,
            n["type"].as_str(),
            n["text"].as_str(),
            "retired",
            n["valid_from"].as_str(),
            n["valid_to"].as_str(),
            Option::<&str>::None,
            n["pinned"].as_i64(),
            n["scope"].as_str()
        ],
    )?;
    copy_evidence(tx, target, v, v + 1)?;
    invalidate_vectors(tx, target)?;
    let mut relations = vec![];
    let mut declined = vec![];
    let mut targets = vec![target.to_owned()];
    for edge in edges {
        let id = get_str(&edge, "id")?;
        let version = get_i64(&edge, "version")?;
        tx.sql().execute(
            "INSERT INTO edge_versions VALUES (?,?,?,?,?,?,?)",
            params![
                id,
                version + 1,
                commit,
                edge["src"].as_str(),
                edge["rel"].as_str(),
                edge["dst"].as_str(),
                "inactive"
            ],
        )?;
        copy_evidence(tx, id, version, version + 1)?;
        tx.touch_target(id)?;
        relations.push(json!({"id":id,"version":version+1}));
        targets.push(id.into());
    }
    for id in targets {
        if loops::current(tx.sql(), &id)?.is_some_and(|marker| marker["state"] == "open") {
            loops::append(tx, commit, &id, "closed", None, None)?;
            declined.push(id);
        }
    }
    Ok(
        json!({"id":target,"version":v+1,"status":"retired","relations":relations,"declined_loops":declined}),
    )
}

const LEGACY_REVIEW: &str = "(json_extract(p.command,'$.op')='remember' AND json_extract(p.command,'$.type')='person' AND json_type(p.command,'$.identity') IS NULL)";
pub fn pending_reviews(db: &Connection) -> Result<Vec<Value>> {
    let mut records = rows(
        db,
        &format!(
            "SELECT p.*,c.actor,c.why,c.recorded_at,({LEGACY_REVIEW} AND EXISTS (SELECT 1 FROM candidate_decisions d WHERE d.decision='review' AND d.target_id=p.id)) AS legacy FROM review_proposals p JOIN graph_commits c ON c.id=p.commit_id WHERE NOT EXISTS (SELECT 1 FROM review_decisions d WHERE d.proposal_id=p.id) ORDER BY c.recorded_at,p.id"
        ),
        [],
    )?;
    for record in &mut records {
        let mut stale = record["legacy"].as_i64().unwrap_or(0) != 0;
        let bases = decode_json(get_str(record, "base_versions")?)?;
        for (id, version) in bases.as_object().unwrap() {
            stale |= node(db, id)?["version"] != *version;
        }
        record.as_object_mut().unwrap().remove("legacy");
        record["stale"] = json!(stale);
    }
    Ok(records)
}
pub fn decide_review(graph: &Graph, command: dto::Review) -> Result<Value> {
    if !["approve", "reject"].contains(&command.decision.as_str()) {
        return Err(Error::Validation("Decision must be approve or reject"));
    }
    let request =
        json!({"op":"review","proposal_id":command.proposal_id,"decision":command.decision});
    let ctx = context(command.command_id, command.actor, command.why);
    graph.store.command(&ctx,&request,|tx,commit|{
        let proposal=one(tx.sql(),"SELECT * FROM review_proposals WHERE id=?",[&command.proposal_id])?.ok_or(Error::Validation("Unknown proposal"))?;
        if one(tx.sql(),"SELECT 1 FROM review_decisions WHERE proposal_id=?",[&command.proposal_id])?.is_some(){return Err(Error::Validation("Proposal already decided"));}
        let linked=one(tx.sql(),"SELECT 1 FROM candidate_decisions WHERE decision='review' AND target_id=? LIMIT 1",[&command.proposal_id])?.is_some();
        let legacy=linked&&one(tx.sql(),&format!("SELECT 1 FROM review_proposals p WHERE p.id=? AND {LEGACY_REVIEW}"),[&command.proposal_id])?.is_some();let mut result=json!({"id":command.proposal_id,"status":"rejected"});
        if command.decision=="approve"{
            if legacy{return Err(Error::Conflict("Legacy person proposal is stale; its candidate is consolidated again"));}
            if one(tx.sql(),"SELECT 1 FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id JOIN forgotten_sources f ON f.source=e.source AND f.event_id=e.event_id WHERE g.target=? LIMIT 1",[&command.proposal_id])?.is_some(){return Err(Error::Conflict("Proposal evidence was excluded by a human forget decision"));}
            for (id,version) in decode_json(get_str(&proposal,"base_versions")?)?.as_object().unwrap(){if node(tx.sql(),id)?["version"]!=*version{return Err(Error::Conflict("Proposal is stale; submit a new proposal"));}}
            let request=decode_json(get_str(&proposal,"command")?)?;result=match get_str(&request,"op")?{"remember"=>TxGraph::remember(tx,commit,&request,true)?,"supersede"=>TxGraph::supersede(tx,commit,&request,true)?,"link"=>TxGraph::link(tx,commit,&request,true)?,_=>apply_review(tx,commit,&request,Some(&command.proposal_id))?};
        }
        tx.sql().execute("INSERT INTO review_decisions VALUES (?,?,?)",params![command.proposal_id,commit,command.decision])?;
        let candidates=rows(tx.sql(),"SELECT d.candidate_id FROM candidate_decisions d WHERE d.decision='review' AND d.target_id=? AND d.rowid=(SELECT max(x.rowid) FROM candidate_decisions x WHERE x.candidate_id=d.candidate_id)",[&command.proposal_id])?;
        let target=if command.decision=="reject"{None}else{result["superseded_by"].as_str().or_else(||result["merged_into"].as_str()).or_else(||result["id"].as_str())};
        for candidate in candidates{let id=get_str(&candidate,"candidate_id")?;let state=if command.decision=="reject"{if legacy{"defer"}else{"reject"}}else if target.is_some(){"promote"}else{"defer"};
            tx.sql().execute("INSERT INTO candidate_decisions VALUES (?,?,?,?,?,?)",params![crate::util::id(),id,commit,state,target,Option::<f64>::None])?;
            if ["promote","reject"].contains(&state){tx.sql().execute("UPDATE vector_blobs SET current=0 WHERE seq IN (SELECT seq FROM vector_items WHERE kind='candidate' AND target=?)",[id])?;}tx.touch_target(id)?;
            if state=="promote"{if let Some(target)=target{let marker=loops::current(tx.sql(),id)?;loops::transfer(tx,commit,id,target)?;importance::transfer_usage(tx,id,target)?;loops::mark(tx,commit,target,Some(marker.as_ref().is_some_and(|value|value["state"]=="open")),Some(id),marker.as_ref())?;}}
        }Ok(result)
    })
}

fn active_entities(db: &Connection) -> Result<Vec<Value>> {
    let mut records = rows(
        db,
        "SELECT n.id,n.version,n.type,n.text,n.scope,n.pinned,(SELECT count(*) FROM graph_evidence g WHERE g.target=n.id AND g.version=n.version) AS evidence,(SELECT count(*) FROM edge_versions e WHERE (e.src=n.id OR e.dst=n.id) AND e.state='active' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id)) AS relations,(SELECT c.recorded_at FROM node_versions v JOIN graph_commits c ON c.id=v.commit_id WHERE v.id=n.id AND v.version=1) AS created_at FROM node_versions n LEFT JOIN memory_types t ON t.name=n.type WHERE n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) AND (n.type IN ('person','organization','project','concept','place','tool') OR t.category='entity') ORDER BY n.id",
        [],
    )?;
    for record in &mut records {
        record["scope"] = decode_json(get_str(record, "scope")?)?;
        record["pinned"] = json!(record["pinned"].as_i64().unwrap_or(0) != 0);
    }
    Ok(records)
}
fn order(members: &mut [Value]) {
    members.sort_by(|a, b| {
        b["pinned"]
            .as_bool()
            .cmp(&a["pinned"].as_bool())
            .then_with(|| b["evidence"].as_i64().cmp(&a["evidence"].as_i64()))
            .then_with(|| {
                a["created_at"]
                    .as_str()
                    .unwrap_or("")
                    .cmp(b["created_at"].as_str().unwrap_or(""))
            })
            .then_with(|| a["id"].as_str().cmp(&b["id"].as_str()))
    });
}
pub fn inventory(db: &Connection) -> Result<Value> {
    let mut groups: std::collections::BTreeMap<(String, String), Vec<Value>> =
        std::collections::BTreeMap::new();
    for n in active_entities(db)? {
        groups
            .entry((get_str(&n, "type")?.into(), identity(get_str(&n, "text")?)))
            .or_default()
            .push(n);
    }
    let mut clusters = vec![];
    for ((kind, name), mut members) in groups {
        if members.len() < 2 {
            continue;
        }
        order(&mut members);
        let hash = hash_json(&json!([kind, name]))?;
        clusters.push(json!({"cluster":&hash[..12],"type":kind,"name":name,"members":members}));
    }
    Ok(json!({"clusters":clusters,"policy":"suspicion_only"}))
}
fn summary(db: &Connection, member: &Value) -> Result<Value> {
    let target = get_str(member, "id")?;
    let version = get_i64(member, "version")?;
    let sources = rows(
        db,
        "SELECT g.episode_id,e.source,e.conversation_id,e.occurred_at,CASE WHEN EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id) THEN NULL ELSE substr(g.quote,1,160) END AS quote_excerpt FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id WHERE g.target=? AND g.version=? ORDER BY julianday(e.occurred_at) DESC,g.episode_id,g.start LIMIT 3",
        params![target, version],
    )?;
    let mut result = json!({});
    for key in [
        "id",
        "version",
        "text",
        "scope",
        "pinned",
        "evidence",
        "relations",
    ] {
        result[key] = member[key].clone();
    }
    result["sources"] = json!(sources);
    result["loop"] = loops::snapshot(db, target, version)?;
    Ok(result)
}
pub fn merge_plan(
    db: &Connection,
    cluster: &str,
    into: Option<&str>,
    exclude: &[String],
    include: &[String],
) -> Result<Value> {
    let inventory = inventory(db)?;
    let found = inventory["clusters"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["cluster"] == cluster)
        .ok_or(Error::Validation(
            "Unknown or already resolved duplicate cluster",
        ))?;
    let original = found["members"].as_array().unwrap();
    let mut included = include.to_vec();
    included.sort();
    included.dedup();
    if included.iter().any(|id| exclude.contains(id)) {
        return Err(Error::Validation(
            "An identity cannot be both included and excluded",
        ));
    }
    if exclude
        .iter()
        .any(|id| !original.iter().any(|n| n["id"] == *id))
    {
        return Err(Error::Validation(
            "Exclusions must be members of this cluster",
        ));
    }
    let variants = included
        .into_iter()
        .filter(|id| !original.iter().any(|n| n["id"] == *id))
        .collect::<Vec<_>>();
    let entities = active_entities(db)?;
    let mut members = original
        .iter()
        .filter(|n| !exclude.iter().any(|id| n["id"] == *id))
        .cloned()
        .collect::<Vec<_>>();
    for id in &variants {
        let n = entities
            .iter()
            .find(|n| n["id"] == *id && n["type"] == found["type"])
            .ok_or(Error::Validation(
                "Included identities must be active entities of the cluster type",
            ))?;
        members.push(n.clone());
    }
    order(&mut members);
    if members.len() < 2 {
        return Err(Error::Validation(
            "A merge plan needs a survivor and at least one absorbed member",
        ));
    }
    if members.len() > 33 {
        return Err(Error::Validation("A merge plan absorbs at most 32 members"));
    }
    let survivor = if let Some(into) = into {
        members
            .iter()
            .find(|n| n["id"] == into)
            .ok_or(Error::Validation(
                "Survivor must belong to the selected members",
            ))?
    } else {
        &members[0]
    };
    let mut absorbed = vec![];
    let mut excluded = vec![];
    let ids = members.iter().map(|n| n["id"].clone()).collect::<Vec<_>>();
    for member in &members {
        if member["id"] != survivor["id"] {
            absorbed.push(summary(db, member)?);
        }
    }
    for member in original {
        if exclude.iter().any(|id| member["id"] == *id) {
            excluded.push(summary(db, member)?);
        }
    }
    let mut relations = json!({});
    for edge in rows(
        db,
        "SELECT e.id,e.version FROM edge_versions e WHERE (e.src IN (SELECT value FROM json_each(?1)) OR e.dst IN (SELECT value FROM json_each(?1))) AND e.state='active' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id) ORDER BY e.id",
        [encode_json(&json!(ids))?],
    )? {
        relations[get_str(&edge, "id")?] = json!({"version":edge["version"],"loop":loops::snapshot(db,get_str(&edge,"id")?,get_i64(&edge,"version")?)?});
    }
    let reviews=rows(db,"SELECT DISTINCT p.id FROM review_proposals p,json_each(p.base_versions) j WHERE j.key IN (SELECT value FROM json_each(?)) AND NOT EXISTS (SELECT 1 FROM review_decisions d WHERE d.proposal_id=p.id) ORDER BY p.id",[encode_json(&json!(ids))?])?.into_iter().map(|n|n["id"].clone()).collect::<Vec<_>>();
    let mut labels = std::collections::BTreeSet::new();
    for member in &members {
        for label in member["scope"].as_array().unwrap() {
            labels.insert(label.as_str().unwrap().to_owned());
        }
    }
    let mut plan = json!({"cluster":cluster,"type":found["type"],"name":found["name"],"survivor":summary(db,survivor)?,"absorbed":absorbed,"excluded":excluded,"resulting_scope":labels.into_iter().collect::<Vec<_>>(),"relations":relations,"pending_reviews":reviews,"policy":"owner_selected_exact_plan","raw_history":"retained"});
    if !variants.is_empty() {
        plan["included"] = json!(variants);
    }
    plan["token"] = json!(hash_json(&plan)?);
    Ok(plan)
}
pub fn merge(graph: &Graph, command: dto::Merge) -> Result<Value> {
    let mut exclude = command.exclude;
    exclude.sort();
    exclude.dedup();
    let mut include = command.include;
    include.sort();
    include.dedup();
    let mut request = json!({"op":"merge_cluster","cluster":command.cluster,"into":command.into,"exclude":exclude,"token":command.token});
    if !include.is_empty() {
        request["include"] = json!(include);
    }
    let ctx = context(command.command_id, command.actor, command.why);
    graph.store.command(&ctx,&request,|tx,commit|{let plan=merge_plan(tx.sql(),&command.cluster,Some(&command.into),&exclude,&include)?;if plan["token"]!=command.token{return Err(Error::Conflict("Merge plan is stale; preview and confirm the current plan"));}let mut merged=vec![];let mut requeued=std::collections::BTreeSet::new();let mut last=None;for member in plan["absorbed"].as_array().unwrap(){let result=absorb_one(tx,commit,get_str(member,"id")?,get_str(&plan["survivor"],"id")?,true,None)?;merged.push(member["id"].clone());for id in result["requeued_candidates"].as_array().unwrap(){requeued.insert(id.as_str().unwrap().to_owned());}last=Some(result);}let last=last.ok_or(Error::Validation("No selected merge members"))?;Ok(json!({"cluster":command.cluster,"survivor":plan["survivor"]["id"],"survivor_version":last["survivor_version"],"merged":merged,"scope":last["scope"],"requeued_candidates":requeued.into_iter().collect::<Vec<_>>()}))})
}
