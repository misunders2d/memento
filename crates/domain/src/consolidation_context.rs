use super::{ELIGIBLE, UNPARKED, VERSION, continuation, native_trace, strings, unique, word_terms};
use crate::util::*;
use crate::{
    behavior, behavior_ground_truth, curation, exact_identities, jobs::Job, loops, recall_keywords,
    unicode::{name_contained, name_tokens},
};
use memento_core::{Error, Result};
use rusqlite::{Connection, params};
use serde_json::{Map, Value, json};
use std::{
    collections::{BTreeSet, HashMap},
    path::Path,
};

pub(super) const VISIBLE: &str = "(NOT EXISTS (SELECT 1 FROM captured_media cm WHERE cm.node_id=n.id) OR EXISTS (SELECT 1 FROM captured_media cm JOIN captured_media_refs r ON r.sha256=cm.sha256 JOIN episodes e ON e.id=r.episode_id WHERE cm.node_id=n.id AND NOT EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id) AND NOT EXISTS (SELECT 1 FROM media_files m JOIN forgotten_targets f ON f.target=m.node_id WHERE m.sha256=cm.sha256)))";
/// Communicated follow-up for a loop-flagged candidate: eligible user/assistant message
/// projections after its newest source, excluding tool calls/output, hooks, instruction
/// envelopes, forgotten sources and assistant records holding only private reasoning blocks.
const LATER_MESSAGES: &str = "SELECT e.id FROM episodes e WHERE e.source=?1 AND e.conversation_id=?2 AND e.seq>?3 AND e.kind IN ('user','assistant') AND coalesce(json_extract(e.metadata,'$.extraction_mode'),'message')='message' AND coalesce(json_extract(e.metadata,'$.evidence_eligible'),1)<>0 AND NOT legacy_native_context(e.source,e.kind,e.text,e.metadata) AND NOT (e.kind='assistant' AND json_valid(e.text) AND json_type(e.text)='array' AND NOT EXISTS (SELECT 1 FROM json_each(e.text) j WHERE json_extract(e.text,'$['||j.key||'].type')='text')) AND NOT EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id) ORDER BY e.seq LIMIT ?4";
// Measured on a real Pi install/enroll request: its completion report was the fifth such
// message (~13 minutes later) and its statement sat ~1.1k characters from the record's end.
const LATER_MESSAGES_PER_CANDIDATE: i64 = 8;
const LATER_MESSAGE_CHARS: usize = 1500;
const LATER_CANDIDATE_CHARS: usize = 8000;
const LATER_BATCH_CHARS: usize = 24000;
type WindowKey = (String, usize, usize);
struct Builder<'a> {
    db: &'a Connection,
    dir: &'a Path,
    episodes: HashMap<String, Value>,
    windows: Vec<(WindowKey, Value)>,
    nodes: Map<String, Value>,
    edges: Map<String, Value>,
    budget: BTreeSet<String>,
    continuity: Value,
    retirement: Value,
}
impl<'a> Builder<'a> {
    fn new(db: &'a Connection, dir: &'a Path) -> Self {
        Self {
            db,
            dir,
            episodes: HashMap::new(),
            windows: vec![],
            nodes: Map::new(),
            edges: Map::new(),
            budget: BTreeSet::new(),
            continuity: json!({"checked":[],"nodes":{},"edges":{},"loops":{},"collisions":[],"moves":[],"pending":[]}),
            retirement: json!({"checked":[],"ready":{},"pending":[]}),
        }
    }
    fn episode(&mut self, id: &str) -> Result<Value> {
        if let Some(episode) = self.episodes.get(id) {
            return Ok(episode.clone());
        }
        let mut episode = one(self.db, "SELECT * FROM episodes WHERE id=?", [id])?
            .ok_or(Error::Validation("extraction_source_missing"))?;
        episode["text"] = json!(
            memento_core::raw::read_original(self.db, self.dir, id)?.ok_or(Error::Unavailable(
                "Original raw source artifact is unavailable or corrupt"
            ))?
        );
        episode["metadata"] = decode_json(get_str(&episode, "metadata")?)?;
        self.episodes.insert(id.into(), episode.clone());
        Ok(episode)
    }
    fn window(&mut self, key: WindowKey, value: Value) {
        if let Some((_, current)) = self
            .windows
            .iter_mut()
            .find(|(existing, _)| existing == &key)
        {
            *current = value;
        } else {
            self.windows.push((key, value));
        }
    }
    fn evidence(&mut self, target: &str, version: i64, add_window: bool) -> Result<Vec<Value>> {
        let mut refs = vec![];
        for row in rows(
            self.db,
            // Bounded founding and latest original evidence (four each), chronological;
            // undated sources sort last so a dated decisive correction is never displaced.
            "WITH ranked AS (SELECT g.*,row_number() OVER (ORDER BY julianday(e.occurred_at) IS NULL,julianday(e.occurred_at),g.episode_id,g.start) AS early,row_number() OVER (ORDER BY julianday(e.occurred_at) IS NULL,julianday(e.occurred_at) DESC,g.episode_id DESC,g.start DESC) AS late FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id WHERE g.target=? AND g.version=?) SELECT * FROM ranked WHERE early<=4 OR late<=4 ORDER BY early",
            params![target, version],
        )? {
            let episode_id = get_str(&row, "episode_id")?;
            let episode = self.episode(episode_id)?;
            let raw = get_str(&episode, "text")?;
            let quote = get_str(&row, "quote")?;
            let start = get_i64(&row, "start")? as usize;
            let end = get_i64(&row, "end")? as usize;
            if substring(raw, start, end) != quote {
                return Err(Error::Conflict(
                    "Stored evidence no longer matches its immutable source",
                ));
            }
            let mut occurrence = None;
            let mut count = 0;
            for (offset, (byte, _)) in raw.char_indices().enumerate() {
                if raw[byte..].starts_with(quote) {
                    if offset == start {
                        occurrence = Some(count);
                        break;
                    }
                    count += 1;
                }
            }
            let occurrence = occurrence.ok_or(Error::Conflict(
                "Stored evidence no longer matches its immutable source",
            ))?;
            if add_window {
                let left = start.saturating_sub(400);
                let right = (end + 400).min(raw.chars().count());
                self.window((episode_id.into(),left,right),json!({"episode_id":episode_id,"start":left,"end":right,"kind":episode["kind"],"text":substring(raw,left,right),"occurred_at":episode["occurred_at"],"source":episode["source"],"event_id":episode["event_id"],"conversation_id":episode["conversation_id"],"native_id":episode["metadata"]["native_id"],"behavior_ground_truth":behavior_ground_truth(&episode,None,None)}));
            }
            let mut reference = json!({"episode_id":episode_id,"quote":quote,"occurrence":occurrence,"start":start,"end":end,"source":episode["source"],"kind":episode["kind"],"occurred_at":episode["occurred_at"],"conversation_id":episode["conversation_id"],"event_id":episode["event_id"],"native_id":episode["metadata"]["native_id"],"behavior_ground_truth":behavior_ground_truth(&episode,Some(start),Some(end))});
            if matches!(native_trace(&episode), Some("attempt" | "result")) {
                reference["sequence"] = episode["seq"].clone();
                reference["native_trace"] = json!(native_trace(&episode));
            }
            refs.push(reference);
        }
        Ok(refs)
    }
    fn add_node(&mut self, target: &str) -> Result<bool> {
        if self.nodes.contains_key(target) {
            return Ok(true);
        }
        let mut n = node(self.db, target)?;
        if n["state"] != "active"
            || one(
                self.db,
                &format!(
                    "SELECT 1 FROM node_versions n WHERE n.id=? AND n.version=? AND {VISIBLE}"
                ),
                params![target, n["version"].as_i64()],
            )?
            .is_none()
        {
            return Ok(false);
        }
        let version = get_i64(&n, "version")?;
        let identity = format!("n:{target}:{version}");
        if !self.budget.contains(&identity) && self.budget.len() >= 48 {
            return Ok(false);
        }
        self.budget.insert(identity);
        n["scope"] = decode_json(get_str(&n, "scope")?)?;
        n["evidence"] = json!(self.evidence(target, version, false)?);
        n["loop"] = loops::current(self.db, target)?.unwrap_or(Value::Null);
        if let Some(b) = behavior(self.db, target)? {
            n["behavior"] = b;
        }
        self.nodes.insert(target.into(), n);
        Ok(true)
    }
    fn allocate(&mut self, queues: &[(String, Vec<String>)], rounds: Option<usize>) -> Result<()> {
        let mut positions = vec![0; queues.len()];
        let mut remaining = rounds.unwrap_or(usize::MAX);
        while remaining > 0 {
            let mut progressed = false;
            for (index, (_, queue)) in queues.iter().enumerate() {
                while positions[index] < queue.len() {
                    let target = &queue[positions[index]];
                    positions[index] += 1;
                    if !self.nodes.contains_key(target) {
                        self.add_node(target)?;
                        progressed = true;
                        break;
                    }
                }
            }
            if !progressed {
                break;
            }
            remaining -= 1;
        }
        Ok(())
    }
    fn continuity(&mut self) -> Result<()> {
        let targets = self.nodes.keys().cloned().collect::<Vec<_>>();
        for target in targets {
            if self.continuity["checked"]
                .as_array()
                .unwrap()
                .contains(&json!(target))
            {
                continue;
            }
            self.continuity["checked"]
                .as_array_mut()
                .unwrap()
                .push(json!(target));
            let ancestors = rows(
                self.db,
                "WITH RECURSIVE ancestors(id,depth) AS (SELECT ?,0 UNION ALL SELECT n.id,a.depth+1 FROM ancestors a JOIN node_versions n ON n.superseded_by=a.id WHERE a.depth<64 AND n.state IN ('refined','merged') AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)) SELECT DISTINCT a.id FROM ancestors a WHERE a.depth>0 AND EXISTS (SELECT 1 FROM edge_versions e WHERE (e.src=a.id OR e.dst=a.id) AND e.state='active' AND e.rel<>'supersedes' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id)) ORDER BY a.id LIMIT 49",
                [&target],
            )?;
            if ancestors.len() > 48 {
                push_unique(&mut self.continuity["pending"], json!(target));
            }
            for ancestor in ancestors.iter().take(48) {
                let old = get_str(ancestor, "id")?;
                let Some(chain) = continuation(self.db, old)? else {
                    continue;
                };
                if chain.last() != Some(&target) {
                    continue;
                }
                let mut chain_nodes = Map::new();
                for id in &chain {
                    chain_nodes.insert(id.clone(), node(self.db, id)?["version"].clone());
                }
                let edges = rows(
                    self.db,
                    "SELECT e.* FROM edge_versions e WHERE (e.src=? OR e.dst=?) AND e.state='active' AND e.rel<>'supersedes' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id) ORDER BY e.id LIMIT 49",
                    params![old, old],
                )?;
                if edges.len() > 48 {
                    push_unique(&mut self.continuity["pending"], json!(target));
                }
                let mut selected = vec![];
                for edge in edges {
                    let src = if edge["src"] == old {
                        target.as_str()
                    } else {
                        get_str(&edge, "src")?
                    };
                    let dst = if edge["dst"] == old {
                        target.as_str()
                    } else {
                        get_str(&edge, "dst")?
                    };
                    let collisions = if src == dst {
                        vec![]
                    } else {
                        rows(
                            self.db,
                            "SELECT e.* FROM edge_versions e WHERE e.src=? AND e.rel=? AND e.dst=? AND e.id<>? AND e.state='active' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id) ORDER BY e.id LIMIT 49",
                            params![src, edge["rel"].as_str(), dst, edge["id"].as_str()],
                        )?
                    };
                    let mut versions = chain_nodes.clone();
                    for id in [get_str(&edge, "src")?, get_str(&edge, "dst")?, src, dst] {
                        versions.insert(id.into(), node(self.db, id)?["version"].clone());
                    }
                    let mut required = self.budget.clone();
                    for (id, version) in &versions {
                        required.insert(format!("n:{id}:{}", version.as_i64().unwrap()));
                    }
                    for row in std::iter::once(&edge).chain(collisions.iter()) {
                        required.insert(format!(
                            "e:{}:{}",
                            get_str(row, "id")?,
                            get_i64(row, "version")?
                        ));
                    }
                    if collisions.len() > 48 || required.len() > 48 {
                        push_unique(&mut self.continuity["pending"], json!(target));
                        continue;
                    }
                    self.budget = required;
                    for (id, version) in versions {
                        self.continuity["nodes"][&id] = version.clone();
                        self.continuity["loops"][&id] =
                            loops::snapshot(self.db, &id, version.as_i64().unwrap())?;
                    }
                    for row in std::iter::once(&edge).chain(collisions.iter()) {
                        let id = get_str(row, "id")?;
                        self.continuity["edges"][id] = row["version"].clone();
                        self.continuity["loops"][id] =
                            loops::snapshot(self.db, id, get_i64(row, "version")?)?;
                    }
                    if src != dst {
                        let versions = collisions
                            .iter()
                            .map(|row| {
                                (
                                    row["id"].as_str().unwrap().to_owned(),
                                    row["version"].clone(),
                                )
                            })
                            .collect::<Map<_, _>>();
                        self.continuity["collisions"].as_array_mut().unwrap().push(
                            json!({"src":src,"relation":edge["rel"],"dst":dst,"versions":versions}),
                        );
                    }
                    selected.push(edge["id"].clone());
                }
                if !selected.is_empty() {
                    self.continuity["moves"]
                        .as_array_mut()
                        .unwrap()
                        .push(json!({"old":old,"target":target,"chain":chain,"edge_ids":selected}));
                }
            }
        }
        Ok(())
    }
    fn retirement(&mut self) -> Result<()> {
        let nodes = self.nodes.clone();
        for (target, n) in nodes {
            if self.retirement["checked"]
                .as_array()
                .unwrap()
                .contains(&json!(target))
            {
                continue;
            }
            self.retirement["checked"]
                .as_array_mut()
                .unwrap()
                .push(json!(target));
            if !memory_type(self.db, get_str(&n, "type")?)? {
                continue;
            }
            let edges = rows(
                self.db,
                "SELECT e.id,e.version FROM edge_versions e WHERE (e.src=? OR e.dst=?) AND e.rel<>'supersedes' AND e.state='active' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id) ORDER BY e.id LIMIT 49",
                params![target, target],
            )?;
            let mut required = self.budget.clone();
            for edge in &edges {
                required.insert(format!(
                    "e:{}:{}",
                    get_str(edge, "id")?,
                    get_i64(edge, "version")?
                ));
            }
            if edges.len() > 48 || required.len() > 48 {
                push_unique(&mut self.retirement["pending"], json!(target));
                continue;
            }
            self.budget = required;
            let mut versions = Map::new();
            let mut snapshots = Map::new();
            for edge in edges {
                let id = get_str(&edge, "id")?;
                versions.insert(id.into(), edge["version"].clone());
                snapshots.insert(
                    id.into(),
                    loops::snapshot(self.db, id, get_i64(&edge, "version")?)?,
                );
            }
            self.retirement["ready"][&target] = json!({"versions":versions,"loops":snapshots});
        }
        Ok(())
    }
    fn technical_context(&mut self, candidates: &[Value]) -> Result<()> {
        for (_, window) in &mut self.windows {
            let episode = &self.episodes[get_str(window, "episode_id")?];
            window["sequence"] = episode["seq"].clone();
            window["native_trace"] = json!(native_trace(episode));
        }
        let mut nearby: HashMap<String, Vec<Value>> = HashMap::new();
        let mut projected: HashMap<String, Option<(Value, &'static str)>> = HashMap::new();
        let (mut added_rows, mut added_chars) = (0, 0);
        for candidate in candidates {
            if candidate["type"] != "experience" || added_rows >= 32 || added_chars >= 16000 {
                continue;
            }
            let mut neighbors: HashMap<String, (i64, Value, &'static str)> = HashMap::new();
            for reference in candidate["evidence"].as_array().unwrap() {
                let episode = self.episode(get_str(reference, "episode_id")?)?;
                if native_trace(&episode).is_none() && !behavior_ground_truth(&episode, None, None)
                {
                    continue;
                }
                let position = get_i64(&episode, "seq")?;
                let key = get_str(&episode, "id")?.to_owned();
                if !nearby.contains_key(&key) {
                    let mut records = vec![];
                    for (comparison, order) in [("<", "DESC"), (">", "ASC")] {
                        records.extend(rows(self.db,&format!("SELECT id,seq,source,event_id,conversation_id,kind,occurred_at,CASE WHEN kind='assistant' THEN substr(text,-2000) WHEN kind='tool' THEN substr(text,1,2000) END AS text,CASE WHEN kind='assistant' THEN max(0,length(text)-2000) ELSE 0 END AS start,CASE WHEN kind IN ('assistant','tool') THEN metadata END AS metadata FROM episodes WHERE source=? AND conversation_id=? AND seq{comparison}? ORDER BY seq {order} LIMIT 32"),params![episode["source"].as_str(),episode["conversation_id"].as_str(),position])?);
                    }
                    nearby.insert(key.clone(), records);
                }
                for row in &nearby[&key] {
                    if row["metadata"].is_null() || row["text"].as_str().is_none_or(str::is_empty) {
                        continue;
                    }
                    let id = get_str(row, "id")?.to_owned();
                    if !projected.contains_key(&id) {
                        let mut item = row.clone();
                        item["metadata"] = decode_json(get_str(&item, "metadata")?)?;
                        let marker = native_trace(&item);
                        let blocked = if matches!(marker, Some("attempt" | "result")) {
                            one(
                                self.db,
                                "SELECT 1 FROM forgotten_sources WHERE source=? AND event_id=?",
                                params![item["source"].as_str(), item["event_id"].as_str()],
                            )?
                            .is_some()
                        } else {
                            true
                        };
                        projected.insert(
                            id.clone(),
                            if blocked {
                                None
                            } else {
                                Some((item, marker.unwrap()))
                            },
                        );
                    }
                    if let Some((item, marker)) = &projected[&id] {
                        let distance = (get_i64(item, "seq")? - position).abs();
                        if neighbors
                            .get(&id)
                            .is_none_or(|(prior, _, _)| distance < *prior)
                        {
                            neighbors.insert(id, (distance, item.clone(), *marker));
                        }
                    }
                }
            }
            let mut neighbors = neighbors.into_values().collect::<Vec<_>>();
            neighbors.sort_by_key(|(distance, episode, _)| {
                (*distance, episode["seq"].as_i64().unwrap())
            });
            let (mut count, mut chars) = (0, 0);
            for (_, episode, marker) in neighbors {
                if count >= 12 || chars >= 8000 || added_rows >= 32 || added_chars >= 16000 {
                    break;
                }
                let id = get_str(&episode, "id")?;
                let start = get_i64(&episode, "start")? as usize;
                let text = get_str(&episode, "text")?;
                let len = text.chars().count();
                if self.windows.iter().any(|(_, window)| {
                    window["episode_id"] == id
                        && window["start"].as_u64().unwrap() as usize <= start
                        && window["end"].as_u64().unwrap() as usize >= start + len
                }) {
                    continue;
                }
                let limit = (8000 - chars).min(16000 - added_chars);
                let left = if marker == "attempt" {
                    len.saturating_sub(limit)
                } else {
                    0
                };
                let exact = substring(
                    text,
                    left,
                    if marker == "attempt" {
                        len
                    } else {
                        limit.min(len)
                    },
                );
                let length = exact.chars().count();
                let start = start + left;
                self.window((id.into(),start,start+length),json!({"episode_id":id,"start":start,"end":start+length,"kind":episode["kind"],"text":exact,"occurred_at":episode["occurred_at"],"source":episode["source"],"event_id":episode["event_id"],"conversation_id":episode["conversation_id"],"native_id":episode["metadata"]["native_id"],"behavior_ground_truth":false,"sequence":episode["seq"],"native_trace":marker}));
                count += 1;
                chars += length;
                added_rows += 1;
                added_chars += length;
            }
        }
        Ok(())
    }
    /// Lifecycle-only windows (`later_context`) for candidates whose loop is open, plus the
    /// private anchor apply uses to recognise a later completion. Apply never stores these
    /// spans as premises, so they cannot freshen the pending obligation's source date.
    fn later_context(&mut self, candidates: &[Value]) -> Result<Value> {
        let mut anchors = Map::new();
        let mut added = 0;
        for candidate in candidates {
            if candidate["loop"]["state"] != "open" {
                continue;
            }
            let mut anchor: Option<Value> = None;
            for reference in candidate["evidence"].as_array().unwrap() {
                let episode = self.episode(get_str(reference, "episode_id")?)?;
                if anchor
                    .as_ref()
                    .is_none_or(|current| current["seq"].as_i64() < episode["seq"].as_i64())
                {
                    anchor = Some(episode);
                }
            }
            let Some(anchor) = anchor else { continue };
            anchors.insert(
                get_str(candidate, "id")?.into(),
                json!({"source":anchor["source"],"conversation_id":anchor["conversation_id"],"after_seq":anchor["seq"]}),
            );
            let mut chars = 0;
            for row in rows(
                self.db,
                LATER_MESSAGES,
                params![anchor["source"].as_str(), anchor["conversation_id"].as_str(), anchor["seq"].as_i64(), LATER_MESSAGES_PER_CANDIDATE],
            )? {
                let episode = self.episode(get_str(&row, "id")?)?;
                let id = get_str(&episode, "id")?;
                let raw = get_str(&episode, "text")?;
                let len = raw.chars().count();
                // An assistant record ends with its reply; a user message starts with it.
                let (start, end) = if episode["kind"] == "assistant" {
                    (len.saturating_sub(LATER_MESSAGE_CHARS), len)
                } else {
                    (0, len.min(LATER_MESSAGE_CHARS))
                };
                if self.windows.iter().any(|(_, window)| {
                    window["episode_id"] == id
                        && window["start"].as_u64().unwrap() as usize <= start
                        && window["end"].as_u64().unwrap() as usize >= end
                }) {
                    continue;
                }
                if chars + end - start > LATER_CANDIDATE_CHARS || added + end - start > LATER_BATCH_CHARS {
                    break;
                }
                chars += end - start;
                added += end - start;
                self.window((id.into(),start,end),json!({"episode_id":id,"start":start,"end":end,"kind":episode["kind"],"text":substring(raw,start,end),"occurred_at":episode["occurred_at"],"source":episode["source"],"event_id":episode["event_id"],"conversation_id":episode["conversation_id"],"native_id":episode["metadata"]["native_id"],"behavior_ground_truth":false,"sequence":episode["seq"],"native_trace":null,"later_context":true}));
            }
        }
        Ok(Value::Object(anchors))
    }
}
fn push_unique(values: &mut Value, value: Value) {
    if !values.as_array().unwrap().contains(&value) {
        values.as_array_mut().unwrap().push(value);
    }
}
fn neighbors(db: &Connection, target: &str, limit: usize) -> Result<Vec<String>> {
    Ok(rows(db,"SELECT n.id FROM edge_versions e JOIN node_versions n ON n.id=CASE WHEN e.src=?1 THEN e.dst ELSE e.src END WHERE (e.src=?1 OR e.dst=?1) AND e.rel<>'supersedes' AND e.state='active' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id) AND n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) ORDER BY n.id LIMIT ?2",params![target,limit as i64])?.into_iter().map(|row|row["id"].as_str().unwrap().into()).collect())
}
fn lexical(
    db: &Connection,
    text: &str,
    kind: &str,
    minimum: usize,
    limit: usize,
    reviewed: &[String],
    head: bool,
) -> Result<Vec<String>> {
    let terms = word_terms(text, minimum, 32);
    if terms.is_empty() {
        return Ok(vec![]);
    }
    let query = if head {
        format!("\"{}\"", terms[0])
    } else {
        terms
            .iter()
            .map(|term| format!("\"{term}\""))
            .collect::<Vec<_>>()
            .join(" OR ")
    };
    Ok(rows(db,"SELECT n.id FROM node_fts JOIN node_versions n ON n.id=node_fts.id AND n.version=node_fts.version WHERE node_fts MATCH ? AND n.type=? AND n.state='active' AND n.id NOT IN (SELECT value FROM json_each(?)) AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) ORDER BY bm25(node_fts),n.id LIMIT ?",params![query,kind,encode_json(&json!(reviewed))?,limit as i64])?.into_iter().map(|row|row["id"].as_str().unwrap().into()).collect())
}
/// Named entities supplied per experience lesson (candidate or maintenance target).
pub(super) const LESSON_SUBJECTS: usize = 3;
const ENTITY: &str = "(n.type IN ('person','organization','project','concept','place','tool') OR EXISTS(SELECT 1 FROM memory_types t WHERE t.name=n.type AND t.category='entity'))";
/// Lesson fields that state what it is about: text, then behavior applicability/conduct.
pub(super) fn lesson_texts<'v>(text: &'v Value, behavior: &'v Value) -> Vec<&'v str> {
    [text, &behavior["applicability"], &behavior["conduct"]]
        .into_iter()
        .filter_map(Value::as_str)
        .collect()
}
/// Active visible entity nodes whose whole stored name is token-window contained in one
/// field, exactly as entity anchors match a request. FTS (rarest terms first) is discovery
/// only; same-spelled entities stay separate identities and nothing is inferred from names.
pub(super) fn named_entities(db: &Connection, texts: &[&str], limit: usize) -> Result<Vec<String>> {
    let fields = texts.iter().map(|text| name_tokens(text)).collect::<Vec<_>>();
    let mut seen = BTreeSet::new();
    let terms = fields
        .iter()
        .flatten()
        .filter(|token| seen.insert(token.as_str()))
        .take(128)
        .map(|token| format!("\"{}\"", token.replace('"', "\"\"")))
        .collect::<Vec<_>>();
    let mut result = vec![];
    if terms.is_empty() || limit == 0 {
        return Ok(result);
    }
    for row in rows(db,&format!("SELECT n.id,n.text FROM node_fts JOIN node_versions n ON n.id=node_fts.id AND n.version=node_fts.version WHERE node_fts MATCH ? AND {ENTITY} AND n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) AND NOT EXISTS (SELECT 1 FROM entity_merges m WHERE m.source=n.id) AND NOT EXISTS (SELECT 1 FROM forgotten_targets f WHERE f.target=n.id) AND {VISIBLE} ORDER BY bm25(node_fts),n.id LIMIT 128"),[terms.join(" OR ")])? {
        let text = get_str(&row, "text")?;
        if fields.iter().any(|tokens| name_contained(text, tokens)) {
            result.push(get_str(&row, "id")?.to_owned());
            if result.len() == limit {
                break;
            }
        }
    }
    Ok(result)
}
fn hinted(db: &Connection, hint: Option<&Value>, kind: &str) -> Result<Vec<String>> {
    let mut result = vec![];
    for row in hint
        .and_then(|hint| hint[kind].as_array())
        .into_iter()
        .flatten()
    {
        if one(db,"SELECT 1 FROM node_versions n WHERE n.id=? AND n.version=? AND n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)",params![row["target"].as_str(),row["version"].as_i64()])?.is_some(){result.push(get_str(row,"target")?.into());}
    }
    Ok(result)
}

pub(super) fn prepare(
    db: &Connection,
    dir: &Path,
    job: &Job,
    discovery: Option<&Value>,
    configured: bool,
) -> Result<Value> {
    let ids = strings(&job.payload["candidate_ids"])?;
    let maintenance = job
        .payload
        .get("maintenance_ids")
        .map(strings)
        .transpose()?
        .unwrap_or_default();
    let reviewed = job
        .payload
        .get("reviewed_ids")
        .map(strings)
        .transpose()?
        .unwrap_or_default();
    let mut builder = Builder::new(db, dir);
    // Narrowing's terminal singleton (in-memory clone only): its at most three validated top
    // targets precede promoted endpoints and continuity inside the same ceiling.
    for target in job.payload.get("_priority").map(strings).transpose()?.unwrap_or_default() {
        builder.add_node(&target)?;
    }
    let mut candidates = rows(
        db,
        &format!(
            "SELECT c.*,c.rowid AS position,(SELECT d.id FROM candidate_decisions d WHERE d.candidate_id=c.id ORDER BY d.rowid DESC LIMIT 1) AS decision_id FROM candidates c WHERE c.id IN (SELECT value FROM json_each(?)) AND {ACTIVE_CANDIDATE} AND {ELIGIBLE} AND {UNPARKED} ORDER BY c.rowid"
        ),
        [encode_json(&json!(ids))?],
    )?;
    let mut resolutions = Map::new();
    for candidate in &mut candidates {
        candidate["scope"] = decode_json(get_str(candidate, "scope")?)?;
        let id = get_str(candidate, "id")?.to_owned();
        candidate["loop"] = loops::current(db, &id)?.unwrap_or(Value::Null);
        candidate["evidence"] = json!(builder.evidence(&id, 1, true)?);
        for key in ["src", "dst"] {
            if let Some(endpoint) = candidate[key].as_str() {
                if let Some(decision) = one(
                    db,
                    "SELECT decision,target_id FROM candidate_decisions WHERE candidate_id=? ORDER BY rowid DESC LIMIT 1",
                    [endpoint],
                )? {
                    if decision["decision"] == "promote" {
                        let target =
                            curation::canonical(db, get_str(&decision, "target_id")?)?["id"]
                                .as_str()
                                .unwrap()
                                .to_owned();
                        resolutions.insert(endpoint.into(), json!(target));
                        builder.add_node(&target)?;
                    }
                }
            }
        }
    }
    for target in &maintenance {
        let n = curation::canonical(db, target)?;
        if n["state"] == "active" {
            builder.add_node(get_str(&n, "id")?)?;
        }
    }
    builder.continuity()?;
    builder.retirement()?;
    let mut identities: Vec<(String, Vec<String>, Vec<String>)> = vec![];
    let mut relevant: Vec<(String, Vec<String>)> = vec![];
    let mut optional: Vec<(String, Vec<String>)> = vec![];
    for candidate in &candidates {
        let id = get_str(candidate, "id")?;
        let mut queue = vec![];
        let vector=db.query_row("SELECT v.embedding FROM vector_items i JOIN vector_blobs v ON v.seq=i.seq WHERE i.target=? AND i.kind='candidate' AND v.current=1",[id],|row|row.get::<_,Vec<u8>>(0)).optional()?;
        let hint = discovery
            .and_then(|value| value["candidates"].get(id))
            .filter(|hint| {
                hint["source"]
                    == json!([
                        candidate["kind"],
                        candidate["type"],
                        candidate["text"],
                        candidate["scope"]
                    ])
                    && vector
                        .as_ref()
                        .is_some_and(|vector| hint["vector"] == memento_core::raw::sha256(vector))
            });
        if candidate["kind"] != "relation" {
            let exact =
                exact_identities(db, get_str(candidate, "type")?, get_str(candidate, "text")?)?
                    .into_iter()
                    .map(|row| row["id"].as_str().unwrap().to_owned())
                    .collect::<Vec<_>>();
            let lex = lexical(
                db,
                get_str(candidate, "text")?,
                get_str(candidate, "type")?,
                3,
                8,
                &[],
                false,
            )?;
            let heads = lexical(
                db,
                get_str(candidate, "text")?,
                get_str(candidate, "type")?,
                3,
                8,
                &[],
                true,
            )?;
            let semantic = hinted(db, hint, "typed")?;
            relevant.push((
                id.into(),
                // Top target per channel; longer exact lists keep their existing review gate.
                unique(exact.first().into_iter().chain(lex.first()).chain(semantic.first()).cloned()),
            ));
            let mut similar = heads;
            for index in 0..lex.len().max(semantic.len()) {
                for value in [lex.get(index), semantic.get(index)].into_iter().flatten() {
                    if !exact.contains(value) {
                        similar.push(value.clone());
                    }
                }
            }
            let similar = unique(similar)
                .into_iter()
                .filter(|id| !exact.contains(id))
                .collect::<Vec<_>>();
            queue.extend(exact.clone());
            queue.extend(similar.clone());
            identities.push((id.into(), exact, similar));
        }
        queue.extend(hinted(db, hint, "broad")?);
        queue.extend(
            recall_keywords(db, &excerpt(get_str(candidate, "text")?, 512), 8, None)?
                .into_iter()
                .map(|row| row["id"].as_str().unwrap().to_owned()),
        );
        optional.push((id.into(), unique(queue)));
    }
    // Only existing lessons (maintenance targets) get named entities: a new lesson cannot
    // link in its own plan, so supplying them for candidates would only spend the ceiling.
    let mut mentioned: Vec<(String, Vec<String>)> = vec![];
    let mut subjects = vec![];
    for target in &maintenance {
        let n = curation::canonical(db, target)?;
        let id = get_str(&n, "id")?;
        if !builder.nodes.contains_key(id) {
            continue;
        }
        let exact = exact_identities(db, get_str(&n, "type")?, get_str(&n, "text")?)?
            .into_iter()
            .map(|row| row["id"].as_str().unwrap().to_owned());
        let mut queue = exact.collect::<Vec<_>>();
        queue.extend(neighbors(db, id, 96)?);
        queue.extend(lexical(
            db,
            get_str(&n, "text")?,
            get_str(&n, "type")?,
            2,
            96,
            &reviewed,
            false,
        )?);
        // An experience target's named entities; listing them as subjects too keeps an
        // unfitted one in the existing unchecked/continue_with bookkeeping.
        if n["type"] == "experience" {
            let named = named_entities(db, &lesson_texts(&n["text"], &builder.nodes[id]["behavior"]), LESSON_SUBJECTS)?;
            queue.extend(named.iter().cloned());
            mentioned.push((id.into(), named));
        }
        subjects.push((
            id.into(),
            unique(queue)
                .into_iter()
                .filter(|value| value != id && !reviewed.contains(value))
                .collect(),
        ));
    }
    let mut comparison: Vec<(String, Vec<String>)> = vec![];
    for (key, exact, similar) in &identities {
        let mut queue = unique(exact.iter().chain(similar).cloned());
        for target in queue.clone().iter().take(8) {
            queue.extend(neighbors(db, target, 8)?);
        }
        let queue = unique(queue);
        let original = optional.iter_mut().find(|(id, _)| id == key).unwrap();
        original.1 = unique(queue.iter().chain(&original.1).cloned());
        let candidate = candidates
            .iter()
            .find(|candidate| candidate["id"] == *key)
            .unwrap();
        let head = word_terms(get_str(candidate, "text")?, 1, 1)
            .into_iter()
            .next()
            .unwrap_or_else(|| candidate["text"].as_str().unwrap().into());
        let subject = format!("{}\0{head}", get_str(candidate, "type")?);
        if let Some((_, prior)) = comparison.iter_mut().find(|(key, _)| key == &subject) {
            *prior = unique(prior.iter().chain(&queue).cloned());
        } else {
            comparison.push((subject, queue));
        }
    }
    builder.allocate(&comparison, Some(2))?;
    builder.continuity()?;
    builder.retirement()?;
    builder.allocate(&comparison, Some(6))?;
    // After reserved comparison slots, at most LESSON_SUBJECTS per lesson.
    builder.allocate(&mentioned, None)?;
    builder.allocate(&subjects, None)?;
    let exposures = exposures(&mut builder, &candidates)?;
    builder.allocate(&optional, None)?;
    builder.continuity()?;
    builder.retirement()?;
    // Bounded continuation: in cursor-free admission, a candidate whose top exact,
    // lexical or semantic target did not fit the ceiling stays pending for a later batch
    // instead of being decided without them. At least one candidate always proceeds.
    let mut deferred = vec![];
    if job.payload["night"].is_null() {
        let mut uncovered = BTreeSet::new();
        for (id, targets) in &relevant {
            for target in targets {
                if !builder.nodes.contains_key(target) && one(db,&format!("SELECT 1 FROM node_versions n WHERE n.id=? AND n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) AND {VISIBLE}"),[target])?.is_some() {
                    uncovered.insert(id.clone());
                }
            }
        }
        // A relation waits with a deferred endpoint candidate instead of a dependency defer.
        for candidate in &candidates {
            if ["src", "dst"].iter().any(|key| candidate[*key].as_str().is_some_and(|id| uncovered.contains(id))) {
                uncovered.insert(get_str(candidate, "id")?.to_owned());
            }
        }
        if uncovered.len() < candidates.len() {
            candidates.retain(|candidate| !uncovered.contains(candidate["id"].as_str().unwrap()));
            optional.retain(|(id, _)| !uncovered.contains(id));
            deferred = uncovered.into_iter().collect();
        } else if job.payload["_priority"].is_null() && candidates.iter().any(|candidate| candidate["kind"] != "relation") {
            // None fits beside its targets: reprepare the first half of the non-relation
            // candidates plus relations whose batch endpoints stay; the rest stays pending.
            // A lone kept candidate's top targets are seeded first in the in-memory clone.
            // Each level keeps fewer non-relation candidates or seeds once, so this terminates.
            let batch = uncovered; // here: every candidate in this context
            let entities = candidates.iter().filter(|candidate| candidate["kind"] != "relation").collect::<Vec<_>>();
            let entities = &entities[..(entities.len() / 2).max(1)];
            let mut kept = entities.iter().map(|candidate| get_str(candidate, "id").map(str::to_owned)).collect::<Result<BTreeSet<_>>>()?;
            for candidate in candidates.iter().filter(|candidate| candidate["kind"] == "relation") {
                if ["src", "dst"].iter().all(|key| candidate[*key].as_str().is_none_or(|id| !batch.contains(id) || kept.contains(id))) {
                    kept.insert(get_str(candidate, "id")?.to_owned());
                }
            }
            let mut narrowed = job.clone();
            narrowed.payload["candidate_ids"] = json!(kept);
            if let [only] = entities {
                narrowed.payload["_priority"] = json!(relevant.iter().find(|(id, _)| only["id"] == *id).map(|(_, targets)| targets.clone()).unwrap_or_default());
            }
            let mut context = prepare(db, dir, &narrowed, discovery, configured)?;
            let rest = batch.into_iter().filter(|id| !kept.contains(id));
            context["_deferred"] = json!(unique(strings(&context["_deferred"])?.into_iter().chain(rest)));
            return Ok(context);
        }
    }
    for candidate in &mut candidates {
        if let Some((_, exact, similar)) =
            identities.iter().find(|(id, _, _)| candidate["id"] == *id)
        {
            candidate["identity"] = json!({"exact":exact.iter().filter(|id|builder.nodes.contains_key(*id)).collect::<Vec<_>>(),"exact_total":exact.len(),"similar":similar.iter().filter(|id|builder.nodes.contains_key(*id)).collect::<Vec<_>>()});
        }
    }
    for (lesson, named) in &mentioned {
        let supplied = named.iter().filter(|id| builder.nodes.contains_key(*id)).cloned().collect::<Vec<_>>();
        if supplied.is_empty() {
            continue;
        }
        if let Some(n) = builder.nodes.get_mut(lesson) {
            n["named_entities"] = json!(supplied);
        }
    }
    let node_ids = builder.nodes.keys().cloned().collect::<Vec<_>>();
    if !node_ids.is_empty() {
        for mut edge in rows(
            db,
            "SELECT e.* FROM edge_versions e WHERE e.state='active' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id) AND e.src IN (SELECT value FROM json_each(?1)) AND e.dst IN (SELECT value FROM json_each(?1)) LIMIT 64",
            [encode_json(&json!(node_ids))?],
        )? {
            let id = get_str(&edge, "id")?.to_owned();
            edge["evidence"] = json!(builder.evidence(&id, get_i64(&edge, "version")?, false)?);
            edge["loop"] = loops::current(db, &id)?.unwrap_or(Value::Null);
            builder.edges.insert(id, edge);
        }
    }
    let mut snapshots = Map::new();
    for item in candidates
        .iter()
        .chain(builder.nodes.values())
        .chain(builder.edges.values())
    {
        let id = get_str(item, "id")?;
        snapshots.insert(
            id.into(),
            loops::snapshot(db, id, item["version"].as_i64().unwrap_or(1))?,
        );
    }
    builder.technical_context(&candidates)?;
    let later = builder.later_context(&candidates)?;
    let mut unchecked: Map<String, Value> = Map::new();
    for (key, queue) in &subjects {
        let missing = queue
            .iter()
            .filter(|target| !builder.nodes.contains_key(*target) && !reviewed.contains(target))
            .cloned()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            unchecked.insert(key.clone(), json!(missing));
        }
    }
    for (_, queue) in &optional {
        let missing = queue
            .iter()
            .filter(|target| !builder.nodes.contains_key(*target) && !reviewed.contains(target))
            .cloned()
            .collect::<Vec<_>>();
        if missing.is_empty() {
            continue;
        }
        for target in queue {
            if builder.nodes.contains_key(target) {
                let mut values = unchecked
                    .get(target)
                    .map(strings)
                    .transpose()?
                    .unwrap_or_default();
                values.extend(missing.clone());
                unchecked.insert(target.clone(), json!(unique(values)));
            }
        }
    }
    let mut windows = coalesce_windows(builder
        .windows
        .into_iter()
        .map(|(_, value)| value)
        .collect::<Vec<_>>());
    windows.sort_by_key(|window| {
        (
            window["sequence"].as_i64().unwrap_or(0),
            window["start"].as_i64().unwrap_or(0),
        )
    });
    let mut maintenance_ids = vec![];
    for target in maintenance {
        let n = curation::canonical(db, &target)?;
        let id = get_str(&n, "id")?.to_owned();
        if builder.nodes.contains_key(&id) && !maintenance_ids.contains(&id) {
            maintenance_ids.push(id);
        }
    }
    let pending = !unchecked.is_empty()
        || !builder.continuity["pending"].as_array().unwrap().is_empty()
        || discovery.map_or(configured, |value| value["pending"] == true);
    let ready = builder.retirement["ready"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    Ok(
        json!({"loop_policy_version":VERSION,"candidates":candidates,"existing_nodes":builder.nodes.into_values().collect::<Vec<_>>(),"existing_relations":builder.edges.into_values().collect::<Vec<_>>(),"raw_windows":windows,"resolved_candidates":resolutions,"loop_snapshots":snapshots,"behavior_exposures":exposures,"maintenance_ids":maintenance_ids,"context_truncated":pending,"retirement_ready":ready,"_unchecked":unchecked,"_deferred":deferred,"_continuity":builder.continuity,"_retirement":builder.retirement,"_later_context":later}),
    )
}

/// Exact union of overlapping source spans with identical provenance/authority.
/// This removes repeated context, never adds a gap or combines native trace kinds.
fn coalesce_windows(mut windows: Vec<Value>) -> Vec<Value> {
    windows.sort_by_key(|w| (w["episode_id"].as_str().unwrap_or("").to_owned(), w["start"].as_u64().unwrap_or(0)));
    let metadata = |w: &Value| { let mut fields = w.as_object().unwrap().clone(); for key in ["start","end","text"] { fields.remove(key); } fields };
    let mut merged: Vec<Value> = vec![];
    for window in windows {
        if let Some(previous) = merged.last_mut() {
            let end = previous["end"].as_u64().unwrap();
            let start = window["start"].as_u64().unwrap();
            if start <= end && metadata(previous) == metadata(&window) {
                let next_end = window["end"].as_u64().unwrap();
                if next_end > end {
                    let mut text = previous["text"].as_str().unwrap().to_owned();
                    text.push_str(&substring(window["text"].as_str().unwrap(), (end-start) as usize, (next_end-start) as usize));
                    previous["text"] = json!(text);
                    previous["end"] = json!(next_end);
                }
                continue;
            }
        }
        merged.push(window);
    }
    merged
}

#[cfg(test)]
mod overlap_tests {
    use super::*;
    #[test]
    fn exact_unicode_union_preserves_gaps_and_authority() {
        let w = |start,end,text,authority| json!({"episode_id":"e","start":start,"end":end,"text":text,"behavior_ground_truth":authority});
        let result = coalesce_windows(vec![w(0,3,"a🧠б",true),w(2,5,"бcd",true),w(6,7,"x",true),w(1,2,"🧠",false)]);
        assert_eq!(result.len(),4); // Different authority interrupts merging; never broaden it.
        let result = coalesce_windows(vec![w(0,3,"a🧠б",true),w(2,5,"бcd",true),w(6,7,"x",true)]);
        assert_eq!(result,vec![w(0,5,"a🧠бcd",true),w(6,7,"x",true)]);
    }

    #[test]
    fn existing_evidence_keeps_founding_and_latest_original_sources() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::Builder::new().permissions(std::fs::Permissions::from_mode(0o700)).tempdir().unwrap();
        let store = memento_core::Store::open(dir.path()).unwrap();
        // Nine supports: the decisive correction is the ninth by id (dropped by the old
        // id-order LIMIT 8) and the founding source has a middle id.
        let days = ["2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05", "2026-01-06", "2026-01-01", "2026-01-07", "2026-01-08", "2026-01-09"];
        store.write(|tx| {
            for (index, day) in days.iter().enumerate() {
                let text = if index == 8 { "Correction: the launch moved to Friday." } else { "The launch is on Monday." };
                tx.sql().execute("INSERT INTO episodes(id,source,event_id,conversation_id,kind,text,occurred_at,metadata,captured_at) VALUES (?,?,?,?,?,?,?,?,?)",
                    params![format!("e{index}"), "fixture", format!("event{index}"), "thread", "user", text, format!("{day}T12:00:00Z"), "{}", "2026-02-01T00:00:00Z"])?;
                tx.sql().execute("INSERT INTO graph_evidence(target,version,episode_id,start,end,quote) VALUES ('n1',1,?,0,?,?)",
                    params![format!("e{index}"), text.chars().count() as i64, text])?;
            }
            Ok(())
        }).unwrap();
        let ids = store.read(|tx| Builder::new(tx.sql(), dir.path()).evidence("n1", 1, false)).unwrap()
            .iter().map(|reference| reference["episode_id"].as_str().unwrap().to_owned()).collect::<Vec<_>>();
        assert_eq!(ids, ["e5", "e0", "e1", "e2", "e4", "e6", "e7", "e8"]);
    }
}

use rusqlite::OptionalExtension;
fn offers(db: &Connection, episode: &Value) -> Result<Vec<Value>> {
    let Some(before) = episode["occurred_at"]
        .as_str()
        .and_then(|value| time(Some(value)).ok().flatten())
    else {
        return Ok(vec![]);
    };
    let mut records = vec![];
    for (comparison, order, limit) in [("<", "DESC", 1024), (">", "ASC", 64)] {
        records.extend(rows(db,&format!("SELECT id,kind,CASE WHEN kind='native_hook' THEN metadata END AS metadata,CASE WHEN kind='native_hook' THEN occurred_at END AS occurred_at FROM episodes WHERE source=? AND conversation_id=? AND seq{comparison}? ORDER BY seq {order} LIMIT {limit}"),params![episode["source"].as_str(),episode["conversation_id"].as_str(),episode["seq"].as_i64()])?);
    }
    let mut result = vec![];
    for row in records {
        if row["kind"] != "native_hook" {
            continue;
        }
        let Some(when) = row["occurred_at"]
            .as_str()
            .and_then(|value| time(Some(value)).ok().flatten())
        else {
            continue;
        };
        if when >= before {
            continue;
        }
        let metadata = decode_json(get_str(&row, "metadata")?)?;
        let Some(pairs) = metadata["exposure"].as_array() else {
            continue;
        };
        if metadata["extraction_mode"] != "skip"
            || metadata["evidence_eligible"] != false
            || pairs.is_empty()
            || pairs.len() > 2
        {
            continue;
        }
        let mut seen = BTreeSet::new();
        let mut valid = true;
        for pair in pairs {
            if pair.as_array().is_none_or(|pair| pair.len() != 2)
                || pair[0]
                    .as_str()
                    .is_none_or(|id| id.is_empty() || id.chars().count() > 64)
                || pair[1].as_i64().is_none_or(|version| version < 1)
                || !seen.insert(encode_json(pair)?)
            {
                valid = false;
                break;
            }
        }
        if valid {
            result.push(json!({"episode_id":row["id"],"occurred_at":when,"behaviors":pairs}));
        }
    }
    Ok(result)
}
fn exposed_node(db: &Connection, target: &str, version: i64) -> Result<Option<(Value, String)>> {
    let Some(shown) = one(
        db,
        "SELECT id,version,type,text FROM node_versions WHERE id=? AND version=?",
        params![target, version],
    )?
    else {
        return Ok(None);
    };
    let Some(b) = (if shown["type"] == "experience" {
        behavior(db, target)?
    } else {
        None
    }) else {
        return Ok(None);
    };
    let mut target = target.to_owned();
    let mut seen = BTreeSet::new();
    while seen.len() < 48 && seen.insert(target.clone()) {
        if one(
            db,
            "SELECT 1 FROM forgotten_targets WHERE target=?",
            [&target],
        )?
        .is_some()
        {
            return Ok(None);
        }
        let Some(n) = one(
            db,
            "SELECT * FROM node_versions WHERE id=? ORDER BY version DESC LIMIT 1",
            [&target],
        )?
        else {
            break;
        };
        if n["state"] == "active" {
            return Ok(Some((
                json!({"id":shown["id"],"version":version,"text":shown["text"],"behavior":b}),
                target,
            )));
        }
        if !matches!(
            n["state"].as_str(),
            Some("corrected" | "superseded" | "refined")
        ) {
            break;
        }
        let Some(next) = n["superseded_by"].as_str() else {
            break;
        };
        target = next.into();
    }
    Ok(None)
}
fn exposures(builder: &mut Builder<'_>, candidates: &[Value]) -> Result<Vec<Value>> {
    let mut cache: HashMap<String, Vec<Value>> = HashMap::new();
    let mut exposures: Vec<((String, String, i64), Value)> = vec![];
    for candidate in candidates {
        if candidate["type"] != "experience" {
            continue;
        }
        let mut times = vec![];
        let mut valid = true;
        for reference in candidate["evidence"].as_array().unwrap() {
            let episode = builder.episode(get_str(reference, "episode_id")?)?;
            if let Some(time) = episode["occurred_at"]
                .as_str()
                .and_then(|value| crate::util::time(Some(value)).ok().flatten())
            {
                times.push(time);
            } else {
                valid = false;
                break;
            }
        }
        if !valid || times.is_empty() {
            continue;
        }
        let before = times.iter().min().unwrap();
        let mut selected: Map<String, Value> = Map::new();
        for reference in candidate["evidence"].as_array().unwrap() {
            let id = get_str(reference, "episode_id")?;
            if !cache.contains_key(id) {
                let episode = builder.episode(id)?;
                cache.insert(id.into(), offers(builder.db, &episode)?);
            }
            for offer in &cache[id] {
                if offer["occurred_at"].as_str().unwrap() < before {
                    selected.insert(get_str(offer, "episode_id")?.into(), offer.clone());
                }
            }
        }
        let mut selected = selected.into_values().collect::<Vec<_>>();
        selected.sort_by(|a, b| {
            b["occurred_at"]
                .as_str()
                .cmp(&a["occurred_at"].as_str())
                .then_with(|| b["episode_id"].as_str().cmp(&a["episode_id"].as_str()))
        });
        for offer in selected.into_iter().take(2) {
            for pair in offer["behaviors"].as_array().unwrap() {
                let target = pair[0].as_str().unwrap();
                let version = pair[1].as_i64().unwrap();
                let key = (
                    get_str(&offer, "episode_id")?.to_owned(),
                    target.to_owned(),
                    version,
                );
                if !exposures.iter().any(|(prior, _)| *prior == key) {
                    let Some((shown, current_target)) = exposed_node(builder.db, target, version)?
                    else {
                        continue;
                    };
                    let identity = format!("n:{target}:{version}");
                    if !builder.budget.contains(&identity) && builder.budget.len() >= 48 {
                        continue;
                    }
                    builder.budget.insert(identity);
                    let current_target = if builder.add_node(&current_target)? {
                        Some(current_target)
                    } else {
                        None
                    };
                    exposures.push((key.clone(),json!({"receipt_episode_id":offer["episode_id"],"shown_at":offer["occurred_at"],"shown":shown,"current_target":current_target,"candidate_ids":[]})));
                }
                let exposure = &mut exposures
                    .iter_mut()
                    .find(|(prior, _)| *prior == key)
                    .unwrap()
                    .1;
                push_unique(&mut exposure["candidate_ids"], candidate["id"].clone());
            }
        }
    }
    Ok(exposures.into_iter().map(|(_, value)| value).collect())
}
