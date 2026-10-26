//! Relevance-first native recall. All index/payload/hydration reads share one authority cut.
use crate::{Recall, RecallRequest, anchors, records};
use chrono::{DateTime, Utc};
use memento_ann::{
    Pool, Projection, SearchHit, SearchView,
    eligibility::{self, ACTIVE_CANDIDATE, HISTORICAL, visible_sql},
};
use memento_core::{Error, F32Blob, ReadTxn, Result, Store, Tier};
use memento_domain::{behavior, importance, unicode};
use memento_embedding::EmbeddingModel;
use rusqlite::{OptionalExtension, params};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::Instant,
};
const TEXT_FLOOR: f64 = 0.685;
const AUTO_FLOOR: f64 = 0.66;
fn number(v: &Value, key: &str, default: f64) -> f64 {
    v[key].as_f64().unwrap_or(default)
}
fn key(v: &Value) -> &str {
    v["id"].as_str().unwrap_or("")
}
fn floor(v: &Value, automatic: bool) -> f64 {
    if v["type"] != "media" {
        return if automatic { AUTO_FLOOR } else { TEXT_FLOOR };
    }
    match v["modality"].as_str() {
        Some("image") => 0.67,
        Some("audio") => 0.645,
        Some("text") => TEXT_FLOOR,
        _ => 1.0,
    }
}
fn check(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        Err(Error::Unavailable("Semantic search deadline expired"))
    } else {
        Ok(())
    }
}
fn literal(query: &str, or: bool) -> String {
    query
        .split_whitespace()
        .map(|t| format!("\"{}\"", t.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(if or { " OR " } else { " " })
}
fn order(a: &Value, b: &Value) -> std::cmp::Ordering {
    number(b, "score", 0.)
        .total_cmp(&number(a, "score", 0.))
        .then_with(|| number(b, "decay", 1.).total_cmp(&number(a, "decay", 1.)))
        .then_with(|| number(b, "importance", 0.5).total_cmp(&number(a, "importance", 0.5)))
        .then_with(|| number(b, "_feedback", 0.).total_cmp(&number(a, "_feedback", 0.)))
        .then_with(|| key(a).cmp(key(b)))
}
fn ranked(db: &rusqlite::Connection, record: &mut Value, at: DateTime<Utc>) -> Result<()> {
    let authority = record["authority"].as_str().map(str::to_owned);
    let overridden = record
        .as_object_mut()
        .unwrap()
        .remove("_source_occurred_at");
    let fields = importance::query_recency_at(
        db,
        record,
        authority.as_deref(),
        overridden.as_ref().map(|v| v.as_str()),
        at,
    )?;
    records::merge(record, fields);
    Ok(())
}
pub struct Retrieval {
    pub store: Arc<Store>,
    pub projection: Arc<Projection>,
    pub model: EmbeddingModel,
}
impl Retrieval {
    pub fn new(store: Arc<Store>, projection: Arc<Projection>, model: EmbeddingModel) -> Self {
        Self {
            store,
            projection,
            model,
        }
    }
    /// Model calls happen before taking the search lock or SQLite snapshot.
    fn encode(&self, request: &RecallRequest) -> Result<[f32; 768]> {
        let text = if request.automatic {
            automatic_query(&request.query, &request.context)
        } else {
            request.query.clone()
        };
        self.model
            .encode_query(&text)
            .map_err(|_| Error::Unavailable("Native query encoding failed"))
    }
    pub fn recall_vector(
        &self,
        request: &RecallRequest,
        vector: &[f32; 768],
        deadline: Instant,
    ) -> Result<Vec<Value>> {
        let deep = request.effort == "deep" && !request.automatic && request.scope.is_none();
        let (mut results, need_archive) =
            self.projection
                .snapshot_until(true, deep, deadline, |tx, view| {
                    Self::current(tx, view, request, vector, deep, deadline)
                })?;
        if need_archive && !deep {
            // A new cut, acquired cold->hot, prevents mixing a future index with an old snapshot.
            // Hot answers never wait for archive construction unless the existing raw fallback is required.
            let value = self
                .projection
                .snapshot_until(true, true, deadline, |tx, view| {
                    Self::current(tx, view, request, vector, true, deadline)
                })?;
            results = value.0;
        }
        Ok(results)
    }
    fn current(
        tx: &ReadTxn<'_>,
        view: &SearchView<'_>,
        request: &RecallRequest,
        vector: &[f32; 768],
        archive: bool,
        deadline: Instant,
    ) -> Result<(Vec<Value>, bool)> {
        let db = tx.sql();
        let scope = request.scope.as_deref();
        let at = Utc::now();
        let limit = if request.automatic {
            request.limit.min(6)
        } else {
            request.limit
        };
        let named = anchors::anchors(db, &request.query, limit, scope)?;
        let mut matches = BTreeMap::<String, Value>::new();
        let mut keyword_targets = HashSet::new();
        let n = if request.automatic {
            (limit * 20).clamp(24, 120)
        } else {
            (limit * 20).min(1000)
        };
        let pool = if request.automatic {
            Pool::Automatic
        } else {
            Pool::Ordinary
        };
        let mut hits = Vec::new();
        for kind in ["node", "candidate", "segment"] {
            hits.extend(view.search(Tier::Hot, vector, n, kind, pool, scope, None)?);
        }
        if !request.automatic {
            // Existing all-term literal admission stays unchanged. Semantic-only OR discovery
            // below requires native cosine admission; a shared name token is not an identity anchor.
            Self::keywords(
                tx,
                &literal(&request.query, false),
                limit,
                scope,
                &mut matches,
                &mut keyword_targets,
            )?;
        }
        let mut discoveries = Vec::new();
        Self::discover(
            tx,
            &literal(&request.query, true),
            limit,
            scope,
            vector,
            request.automatic,
            &mut discoveries,
        )?;
        for hit in &discoveries {
            if let Some(record) = hydrate(tx, hit, false)? {
                if hit.cosine as f64 >= floor(&record, request.automatic) {
                    keyword_targets.insert(key(&record).to_owned());
                }
            }
        }
        hits.extend(discoveries);
        hits.sort_by(|a, b| {
            b.cosine
                .total_cmp(&a.cosine)
                .then_with(|| a.target.cmp(&b.target))
        });
        for hit in hits {
            check(deadline)?;
            let Some(mut record) = hydrate(tx, &hit, false)? else {
                continue;
            };
            let id = key(&record).to_owned();
            let keyword = keyword_targets.contains(&id);
            let cosine = hit.cosine as f64;
            if cosine < floor(&record, request.automatic) && !keyword {
                continue;
            }
            if request.automatic && !automatic_allowed(db, &record)? {
                continue;
            }
            let keyword_semantic = keyword && cosine >= floor(&record, false);
            records::merge(
                &mut record,
                json!({"cosine":cosine,"score":cosine+if keyword {0.25}else{0.0},"match":if keyword_semantic{"hybrid"}else if keyword {"keyword"}else{"semantic"}}),
            );
            let replace = matches.get(&id).is_none_or(|old| {
                old["cosine"].is_null() || number(&record, "score", 0.) > number(old, "score", 0.)
            });
            if replace {
                matches.insert(id, record);
            }
        }
        let meaningful = matches.values().try_fold(false, |yes, v| {
            Ok::<_, Error>(yes || records::meaningful(db, v)?)
        })?;
        let need_archive = scope.is_none()
            && !meaningful
            && (request.effort != "deep" || request.automatic)
            && matches.len() + named.len() < limit;
        if request.effort == "deep" && !request.automatic && scope.is_none() {
            for tier in [Tier::Hot, Tier::Cold] {
                for hit in view.search(tier, vector, n, "episode", Pool::Ordinary, None, None)? {
                    if hit.cosine as f64 >= TEXT_FLOOR {
                        if let Some(mut row) = hydrate(tx, &hit, false)? {
                            records::merge(
                                &mut row,
                                json!({"cosine":hit.cosine,"score":hit.cosine,"match":"semantic"}),
                            );
                            matches.entry(key(&row).to_owned()).or_insert(row);
                        }
                    }
                }
            }
            for episode in
                tx.search_episodes(Some(&request.query), (limit * 4).min(100), None, None, None)?
            {
                if let Some(mut row) =
                    records::matched_passage(tx, key(&episode), &request.query, false)?
                {
                    let old = matches.get(key(&row));
                    let cosine = old.and_then(|v| v["cosine"].as_f64());
                    records::merge(
                        &mut row,
                        json!({"score":cosine.unwrap_or(TEXT_FLOOR)+0.25,"cosine":cosine,"match":if cosine.is_some(){"hybrid"}else{"keyword"}}),
                    );
                    matches.insert(key(&row).into(), row);
                }
            }
        } else if need_archive && archive {
            let mut fallback = Self::historical(
                tx,
                view,
                &request.query,
                vector,
                if request.automatic { 1 } else { limit },
                !request.automatic,
                at,
            )?;
            if request.automatic {
                let mut parked = Vec::new();
                for hit in view.search(
                    Tier::Hot,
                    vector,
                    4,
                    "candidate",
                    Pool::ParkedCandidate,
                    None,
                    None,
                )? {
                    if (hit.cosine as f64) < AUTO_FLOOR {
                        continue;
                    }
                    if let Some(mut row) = records::candidate(db, &hit.target)? {
                        if !automatic_allowed(db, &row)? {
                            continue;
                        }
                        finish(db, &mut row, at)?;
                        records::merge(
                            &mut row,
                            json!({"category":"memory","cosine":hit.cosine,"score":hit.cosine,"match":"semantic","related":[],"duplicate_ids":[]}),
                        );
                        row["text"] = json!(format!(
                            "Unverified historical candidate{}: {}",
                            source_label(&row),
                            row["text"].as_str().unwrap_or("")
                        ));
                        parked.push(row);
                    }
                }
                parked.sort_by(order);
                if let Some(row) = parked.into_iter().next() {
                    fallback.push(row);
                }
            }
            for row in fallback {
                matches.entry(key(&row).into()).or_insert(row);
            }
        }
        let mut records = Vec::new();
        for (_, mut row) in matches {
            if !records::media_sources(db, &mut row)? {
                continue;
            }
            finish(db, &mut row, at)?;
            if request.automatic && row["type"] == "media" {
                records::media_pointer(db, &mut row)?;
            }
            if !request.automatic {
                let feedback: Option<(i64, i64)> = db
                    .query_row(
                        "SELECT shown,cited FROM query_feedback WHERE query_key=? AND target=?",
                        params![importance::query_key(&request.query), key(&row)],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .optional()?;
                let f = feedback
                    .map(|(shown, cited)| {
                        ((2 * cited - shown) as f64 / (shown.max(1) as f64)).clamp(-1., 1.)
                    })
                    .unwrap_or(0.);
                row["_feedback"] = json!(if row.get("behavior").is_some_and(|v| !v.is_null()) {
                    f.max(0.)
                } else {
                    f
                });
            }
            records.push(row);
        }
        records.sort_by(order);
        let mut facts = Vec::new();
        // Exact full-name/alias T1 anchors keep their existing priority and budget.
        let mut emitted_ids = named
            .iter()
            .map(|v| key(v).to_owned())
            .collect::<HashSet<_>>();
        let mut contexts = named.len();
        let mut output = named;
        let mut grouped = BTreeMap::<String, usize>::new();
        let (mut suggestions, mut media) = (0, 0);
        for mut row in records {
            if output.len() >= limit {
                break;
            }
            if emitted_ids.contains(key(&row)) {
                continue;
            }
            if request.automatic {
                let signature = format!(
                    "{}\0{}\0{}\0{}",
                    row["category"],
                    row["authority"],
                    row["scope"],
                    if row["type"] == "media" {
                        row["sha256"].as_str().unwrap_or("").to_owned()
                    } else {
                        unicode::casefold(
                            row["_full_text"]
                                .as_str()
                                .unwrap_or(row["text"].as_str().unwrap_or("")),
                        )
                        .split_whitespace()
                        .collect::<Vec<_>>()
                        .join(" ")
                        .trim_end_matches(['.', '!'])
                        .to_owned()
                    }
                );
                if let Some(index) = grouped.get(&signature) {
                    let previous = &mut output[*index];
                    previous["duplicate_ids"]
                        .as_array_mut()
                        .unwrap()
                        .push(row["id"].clone());
                    for evidence in row["evidence"].as_array().into_iter().flatten() {
                        if !previous["evidence"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .any(|r| {
                                r["episode_id"] == evidence["episode_id"]
                                    && r["start"] == evidence["start"]
                                    && r["end"] == evidence["end"]
                            })
                        {
                            previous["evidence"]
                                .as_array_mut()
                                .unwrap()
                                .push(evidence.clone());
                        }
                    }
                    continue;
                }
                if row["category"] == "behavioral_suggestion" {
                    if suggestions == 2 {
                        continue;
                    }
                    suggestions += 1;
                }
                if row["category"] == "media_reference" {
                    if media == 1 {
                        continue;
                    }
                    media += 1;
                }
                grouped.insert(signature, output.len());
                row["duplicate_ids"] = json!([]);
                if let Some(date) = row["source_occurred_at"].as_str() {
                    let prefix = format!("Source {}: ", &date[..10.min(date.len())]);
                    row["text"] = json!(format!("{prefix}{}", row["text"].as_str().unwrap_or("")));
                    if row["behavior"].is_object() {
                        row["behavior"]["applicability"] = json!(format!(
                            "{prefix}{}",
                            row["behavior"]["applicability"].as_str().unwrap_or("")
                        ));
                    }
                }
            } else if request.effort != "cheap" && row["tier"] == "durable" {
                row["related"] = json!(records::related(
                    db,
                    key(&row),
                    if request.effort == "deep" { 2 } else { 1 },
                    scope
                )?);
            }
            let mut connections = Vec::new();
            if row["tier"] == "durable" && records::meaningful(db, &row)? {
                for endpoint in records::related(db, key(&row), 1, scope)? {
                    if !records::entity_type(db, endpoint["type"].as_str().unwrap_or(""))? {
                        continue;
                    }
                    let Some(base) = records::one(
                        db,
                        &format!(
                            "SELECT {},n.text AS name FROM node_versions n WHERE n.id=? AND n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)",
                            records::NODE_SUMMARY
                        ),
                        [key(&endpoint)],
                    )?
                    else {
                        continue;
                    };
                    if eligibility::legacy_only(db, key(&base), base["version"].as_i64().unwrap())?
                    {
                        continue;
                    }
                    connections.push((endpoint, base));
                }
            }
            if request.automatic {
                // Role-only requests retain recorded names without inventing standalone subjects.
                // Keep related=[] and never alter the stored assertion or behavioral applicability.
                let mut provenance = Vec::new();
                for (endpoint, base) in &connections {
                    let name = base["name"].as_str().unwrap();
                    if anchors::contains_name(name, row["text"].as_str().unwrap_or("")) {
                        continue;
                    }
                    let clean = name
                        .split_whitespace()
                        .collect::<Vec<_>>()
                        .join(" ")
                        .chars()
                        .take(160)
                        .collect::<String>();
                    provenance.push(format!(
                        "{clean} [{}; {} {}; edge {}]",
                        base["type"].as_str().unwrap(),
                        endpoint["relation"].as_str().unwrap(),
                        endpoint["direction"].as_str().unwrap(),
                        endpoint["edge_id"].as_str().unwrap()
                    ));
                    if provenance.len() == 2 {
                        break;
                    }
                }
                if !provenance.is_empty() {
                    row["text"] = json!(format!(
                        "{} (Connected: {})",
                        row["text"].as_str().unwrap_or(""),
                        provenance.join("; ")
                    ));
                }
            }
            if records::meaningful(db, &row)? {
                facts.push(row.clone());
            }
            row.as_object_mut().unwrap().remove("_full_text");
            row.as_object_mut().unwrap().remove("_feedback");
            emitted_ids.insert(key(&row).to_owned());
            let basis = row.clone();
            // Retain the admitted fact before spending any shared slot on its connections.
            output.push(row);
            for (_, base) in connections {
                if contexts == 2 || output.len() == limit {
                    break;
                }
                if emitted_ids.contains(key(&base))
                    || !anchors::connection_name_matches(db, &base, &request.query)?
                {
                    continue;
                }
                let mut rendered = base.clone();
                rendered.as_object_mut().unwrap().remove("name");
                if let Some(mut anchor) = anchors::from_connection(db, &rendered, scope)? {
                    anchor["match"] = json!("recorded_connection");
                    anchor["basis_memory_id"] = basis["id"].clone();
                    anchor["score"] = basis["score"].clone();
                    anchor["cosine"] = basis["cosine"].clone();
                    emitted_ids.insert(key(&anchor).to_owned());
                    contexts += 1;
                    output.push(anchor);
                }
            }
        }
        anchors::add_partials(
            db,
            &request.query,
            vector,
            scope,
            TEXT_FLOOR,
            &facts,
            &mut output,
            limit,
        )?;
        Ok((output, need_archive && !archive))
    }
    fn keywords(
        tx: &ReadTxn<'_>,
        query: &str,
        limit: usize,
        scope: Option<&str>,
        found: &mut BTreeMap<String, Value>,
        targets: &mut HashSet<String>,
    ) -> Result<()> {
        for (kind, id, version) in keyword_ids(tx, query, (limit * 4).min(100), scope)? {
            let hit = SearchHit {
                seq: 0,
                target: id,
                kind,
                version,
                cosine: 0.,
            };
            if let Some(mut row) = hydrate(tx, &hit, false)? {
                records::merge(&mut row, json!({"score":TEXT_FLOOR+0.25,"match":"keyword"}));
                targets.insert(key(&row).into());
                found.entry(key(&row).into()).or_insert(row);
            }
        }
        Ok(())
    }
    fn discover(
        tx: &ReadTxn<'_>,
        query: &str,
        limit: usize,
        scope: Option<&str>,
        vector: &[f32; 768],
        automatic: bool,
        hits: &mut Vec<SearchHit>,
    ) -> Result<()> {
        for (kind, id, version) in keyword_ids(tx, query, (limit * 4).min(100), scope)? {
            let mut statement=tx.sql().prepare("SELECT i.seq,b.embedding,b.generation FROM vector_items i JOIN vector_blobs b ON b.seq=i.seq WHERE i.target=? AND i.kind=? AND i.version=? AND b.current=1")?;
            for row in statement.query_map(params![id, kind, version], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })? {
                let (seq, bytes, generation) = row?;
                let blob = F32Blob::from_bytes(bytes)?;
                let flags = eligibility::payload(
                    tx.sql(),
                    &memento_ann::ProjectionRow {
                        seq,
                        target: id.clone(),
                        kind: kind.clone(),
                        version,
                        embedding: blob.clone(),
                        current: true,
                        generation,
                    },
                )?;
                if flags[if automatic { "automatic" } else { "ordinary" }] != true {
                    continue;
                }
                let values = blob.values();
                let dot = values
                    .iter()
                    .zip(vector)
                    .map(|(a, b)| *a as f64 * *b as f64)
                    .sum::<f64>();
                let norm = values
                    .iter()
                    .map(|v| (*v as f64).powi(2))
                    .sum::<f64>()
                    .sqrt()
                    * vector
                        .iter()
                        .map(|v| (*v as f64).powi(2))
                        .sum::<f64>()
                        .sqrt();
                if norm > 0. {
                    hits.push(SearchHit {
                        seq,
                        target: id.clone(),
                        kind: kind.clone(),
                        version,
                        cosine: (dot / norm).clamp(-1., 1.) as f32,
                    });
                }
            }
        }
        Ok(())
    }
    fn historical(
        tx: &ReadTxn<'_>,
        view: &SearchView<'_>,
        query: &str,
        vector: &[f32; 768],
        limit: usize,
        keywords: bool,
        at: DateTime<Utc>,
    ) -> Result<Vec<Value>> {
        let mut found = BTreeMap::new();
        for hit in view.search(
            Tier::Cold,
            vector,
            (limit * 4).max(8),
            "episode",
            Pool::Historical,
            None,
            None,
        )? {
            if (hit.cosine as f64) < TEXT_FLOOR {
                continue;
            }
            if let Some(mut row) = hydrate(tx, &hit, true)? {
                records::merge(
                    &mut row,
                    json!({"cosine":hit.cosine,"score":hit.cosine,"match":"semantic"}),
                );
                found.entry(key(&row).to_owned()).or_insert(row);
            }
        }
        if keywords {
            for row in tx.search_episodes(Some(query), (limit * 4).min(100), None, None, None)? {
                let historical: bool = tx.sql().query_row(
                    &format!(
                        "SELECT EXISTS(SELECT 1 FROM episodes e WHERE e.id=? AND {HISTORICAL})"
                    ),
                    [key(&row)],
                    |r| r.get(0),
                )?;
                if !historical {
                    continue;
                }
                if let Some(mut passage) = records::matched_passage(tx, key(&row), query, true)? {
                    let cosine = found.get(key(&passage)).and_then(|v| v["cosine"].as_f64());
                    records::merge(
                        &mut passage,
                        json!({"score":cosine.unwrap_or(TEXT_FLOOR)+0.25,"cosine":cosine,"match":if cosine.is_some(){"hybrid"}else{"keyword"}}),
                    );
                    found.insert(key(&passage).into(), passage);
                }
            }
        }
        let mut result = Vec::new();
        for (_, mut row) in found {
            ranked(tx.sql(), &mut row, at)?;
            row["related"] = json!([]);
            if !keywords {
                if !eligibility::automatic_content(row["text"].as_str().unwrap_or("")) {
                    continue;
                }
                let date = source_label(&row);
                row["text"] = json!(format!(
                    "Unverified historical raw passage{date}: {}",
                    row["text"]
                        .as_str()
                        .unwrap_or("")
                        .split_whitespace()
                        .collect::<Vec<_>>()
                        .join(" ")
                ));
                row["category"] = json!("historical_raw");
                row["importance"] = json!(0.5);
                row["archived"] = json!(0);
            }
            result.push(row);
        }
        result.sort_by(order);
        result.truncate(limit);
        Ok(result)
    }
}
impl Recall for Retrieval {
    fn recall(&self, request: &RecallRequest, deadline: Instant) -> Result<Vec<Value>> {
        let vector = self.encode(request)?;
        check(deadline)?;
        self.recall_vector(request, &vector, deadline)
    }
    fn ready(&self) -> bool {
        self.projection.ready(Tier::Hot)
    }
}
fn source_label(row: &Value) -> String {
    row["source_occurred_at"]
        .as_str()
        .map(|s| format!(" (source {})", &s[..s.len().min(10)]))
        .unwrap_or_default()
}
fn automatic_query(query: &str, context: &str) -> String {
    static FOLLOW: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"(?i)^(?:and\b|what about\b|how about\b|continue\b|go on\b|а\b|как насчет\b|что насчет\b)|^(?:what|how|why|when|where|which)\b.{0,100}\b(?:it|that|this|those|them|same)\b|^(?:does|do|can|could|would|should|is|are|will)\s+(?:it|that|this|those|they|them)\b|^(?:please )?(?:do|use|apply|repeat) (?:it|that|this|the same)\b").unwrap()
    });
    if !context.trim().is_empty()
        && (query.split_whitespace().count() <= 2 || FOLLOW.is_match(query))
    {
        format!("{query}\nRecent conversation:\n{}", context.trim())
            .chars()
            .take(4096)
            .collect()
    } else {
        query.into()
    }
}
fn automatic_allowed(db: &rusqlite::Connection, row: &Value) -> Result<bool> {
    if !eligibility::automatic_content(row["text"].as_str().unwrap_or("")) && row["type"] != "media"
    {
        return Ok(false);
    }
    if eligibility::legacy_only(db, key(row), row["version"].as_i64().unwrap_or(1))? {
        return Ok(false);
    }
    if row["type"] == "experience"
        && row["tier"] != "candidate"
        && behavior(db, key(row))?.is_none()
    {
        return Ok(false);
    }
    Ok(true)
}
fn finish(db: &rusqlite::Connection, row: &mut Value, at: DateTime<Utc>) -> Result<()> {
    records::scope(row)?;
    if row["tier"] == "candidate" {
        records::merge(row, records::candidate_details(db, key(row))?);
    } else if row["tier"] != "raw" && row["tier"] != "reference" {
        records::merge(
            row,
            records::details(db, key(row), row["version"].as_i64().unwrap())?,
        );
        row["tier"] = json!("durable");
        row["confirmed"] = json!(true);
    }
    if row["type"] == "media" && row["blob_url"].is_null() {
        if let Some(v) = records::one(
            db,
            "SELECT sha256 FROM media_files WHERE node_id=?",
            [key(row)],
        )? {
            row["blob_url"] = json!(format!("/blobs/{}", v["sha256"].as_str().unwrap()));
        }
    }
    if row["category"].is_null() {
        row["category"] = json!("memory");
    }
    if row["related"].is_null() {
        row["related"] = json!([]);
    }
    ranked(db, row, at)?;
    let table = if row["tier"] == "candidate" {
        "candidates"
    } else {
        "node_versions"
    };
    if row["tier"] != "raw" {
        let sql = if table == "candidates" {
            "SELECT text FROM candidates WHERE id=?1"
        } else {
            "SELECT text FROM node_versions WHERE id=?1 AND version=?2"
        };
        row["_full_text"] = if table == "candidates" {
            db.query_row(sql, [key(row)], |r| r.get::<_, String>(0))
                .optional()?
        } else {
            db.query_row(sql, params![key(row), row["version"].as_i64()], |r| {
                r.get::<_, String>(0)
            })
            .optional()?
        }
        .map(Value::String)
        .unwrap_or(Value::Null);
    }
    Ok(())
}
fn hydrate(tx: &ReadTxn<'_>, hit: &SearchHit, historical: bool) -> Result<Option<Value>> {
    match hit.kind.as_str() {
        "node" => records::node(tx.sql(), &hit.target, hit.version),
        "candidate" => records::candidate(tx.sql(), &hit.target),
        "segment" => records::segment(tx.sql(), &hit.target),
        "episode" => {
            let Some(chunk) = records::one(
                tx.sql(),
                "SELECT episode_id,start,end FROM episode_chunks WHERE id=?",
                [&hit.target],
            )?
            else {
                return Ok(None);
            };
            records::episode(
                tx,
                chunk["episode_id"].as_str().unwrap(),
                chunk["start"].as_u64().unwrap() as usize,
                Some(chunk["end"].as_u64().unwrap() as usize),
                Some(&hit.target),
                historical,
                None,
            )
        }
        _ => Ok(None),
    }
}
fn keyword_ids(
    tx: &ReadTxn<'_>,
    query: &str,
    limit: usize,
    scope: Option<&str>,
) -> Result<Vec<(String, String, i64)>> {
    if query.is_empty() {
        return Ok(Vec::new());
    }
    let db = tx.sql();
    let mut out = Vec::new();
    for row in records::rows(
        db,
        &format!(
            "SELECT n.id,n.version FROM node_fts JOIN node_versions n ON n.id=node_fts.id AND n.version=node_fts.version WHERE node_fts MATCH ?1 AND n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) AND NOT EXISTS(SELECT 1 FROM forgotten_targets f WHERE f.target=n.id) AND {} AND (?2 IS NULL OR EXISTS(SELECT 1 FROM json_each(n.scope) WHERE value=?2)) ORDER BY bm25(node_fts),n.id LIMIT ?3",
            visible_sql("n.id")
        ),
        params![query, scope, limit],
    )? {
        out.push((
            "node".into(),
            key(&row).into(),
            row["version"].as_i64().unwrap(),
        ));
    }
    for row in records::rows(
        db,
        &format!(
            "SELECT c.id FROM candidate_fts JOIN candidates c ON c.id=candidate_fts.id WHERE candidate_fts MATCH ?1 AND {ACTIVE_CANDIDATE} AND (?2 IS NULL OR EXISTS(SELECT 1 FROM json_each(c.scope) WHERE value=?2)) ORDER BY bm25(candidate_fts),c.id LIMIT ?3"
        ),
        params![query, scope, limit],
    )? {
        out.push(("candidate".into(), key(&row).into(), 1));
    }
    for row in records::rows(
        db,
        &format!(
            "SELECT s.id,n.version FROM segment_fts JOIN segments s ON s.id=segment_fts.id JOIN media_files f ON f.node_id=s.node_id JOIN node_versions n ON n.id=s.node_id WHERE segment_fts MATCH ?1 AND n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) AND NOT EXISTS(SELECT 1 FROM forgotten_targets f WHERE f.target=n.id) AND {} AND (?2 IS NULL OR EXISTS(SELECT 1 FROM json_each(n.scope) WHERE value=?2)) ORDER BY bm25(segment_fts),s.id LIMIT ?3",
            visible_sql("n.id")
        ),
        params![query, scope, limit],
    )? {
        out.push(("segment".into(), key(&row).into(), 1));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use memento_core::{ModelSignature, VectorKind, VectorWrite};
    use memento_domain::Graph;
    use std::sync::atomic::AtomicBool;
    struct Fixture {
        _dir: tempfile::TempDir,
        store: Arc<Store>,
        graph: Graph,
        projection: Arc<Projection>,
        seq: i64,
    }
    impl Fixture {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(dir.path().join("data")).unwrap();
            let graph = Graph::new(store.clone());
            let projection = Projection::open(store.clone(), ModelSignature::pinned()).unwrap();
            Self {
                _dir: dir,
                store,
                graph,
                projection,
                seq: 0,
            }
        }
        fn add(
            &mut self,
            text: &str,
            kind: &str,
            date: &str,
            importance: f64,
            cosine: f32,
            scope: &str,
        ) -> String {
            self.seq += 1;
            let event = format!("s{}", self.seq);
            let episode=self.store.capture(serde_json::from_value(json!({"source":"synthetic","event_id":event,"conversation_id":event,"kind":"user","text":text,"occurred_at":date,"metadata":{}})).unwrap()).unwrap();
            let node=self.graph.remember(serde_json::from_value(json!({"command_id":event,"actor":"owner","why":"Exact synthetic evidence","type":kind,"text":text,"scope":[scope],"evidence":[{"episode_id":episode.id,"quote":text}]})).unwrap()).unwrap();
            let id = key(&node).to_owned();
            let mut vector = [0f32; 768];
            vector[0] = cosine;
            vector[1] = (1. - cosine * cosine).sqrt();
            let seq = self.seq;
            self.store.write(|tx|{tx.sql().execute("INSERT INTO node_stats(node_id,importance) VALUES(?,?) ON CONFLICT(node_id) DO UPDATE SET importance=excluded.importance",params![id,importance])?;tx.put_vector(&VectorWrite{seq,target:id.clone(),kind:VectorKind::Node,version:1,embedding:F32Blob::from_values(&vector)?,current:true,tier:Tier::Hot})?;Ok(())}).unwrap();
            id
        }
        fn connect(&self, a: &str, b: &str) {
            let id = format!("edge-{a}-{b}");
            let text = "Recorded source directly connects the two fixture records";
            let source=self.store.capture(serde_json::from_value(json!({"source":"synthetic","event_id":id,"conversation_id":"edges","kind":"user","text":text,"metadata":{}})).unwrap()).unwrap();
            self.graph.link(serde_json::from_value(json!({"command_id":id,"actor":"owner","why":"Exact recorded fixture connection","a":a,"b":b,"rel":"related_to","evidence":[{"episode_id":source.id,"quote":text}]})).unwrap()).unwrap();
        }
        fn recall(
            &self,
            query: &str,
            limit: usize,
            automatic: bool,
            scope: Option<&str>,
        ) -> Vec<Value> {
            self.projection
                .rebuild(Tier::Hot, &AtomicBool::new(false))
                .unwrap();
            let mut vector = [0f32; 768];
            vector[0] = 1.;
            let r = RecallRequest {
                query: query.into(),
                limit,
                automatic,
                scope: scope.map(str::to_owned),
                ..Default::default()
            };
            self.projection
                .snapshot_until(
                    true,
                    false,
                    Instant::now() + std::time::Duration::from_secs(5),
                    |tx, view| {
                        Retrieval::current(
                            tx,
                            view,
                            &r,
                            &vector,
                            false,
                            Instant::now() + std::time::Duration::from_secs(5),
                        )
                    },
                )
                .unwrap()
                .0
        }
    }
    #[test]
    fn stronger_query_relevance_beats_fresh_importance_and_opposed_feedback_in_normal_and_automatic()
     {
        for reversed in [false, true] {
            let mut f = Fixture::new();
            let (old, new) = if reversed {
                ("2026-10-26T00:00:00Z", "2020-01-01T00:00:00Z")
            } else {
                ("2020-01-01T00:00:00Z", "2026-10-26T00:00:00Z")
            };
            let strong = f.add(
                "Durable source fact has precise applicability",
                "knowledge",
                old,
                0.1,
                0.95,
                "personal",
            );
            let weak = f.add(
                "A broadly associated semantic hub fact",
                "knowledge",
                new,
                1.,
                0.75,
                "personal",
            );
            f.store
                .write(|tx| {
                    for (id, cited) in [(&strong, 0), (&weak, 10)] {
                        tx.sql().execute(
                            "INSERT INTO query_feedback VALUES(?,?,10,?)",
                            params![importance::query_key("unseen retrieval request"), id, cited],
                        )?;
                    }
                    Ok(())
                })
                .unwrap();
            for automatic in [false, true] {
                let result = f.recall("unseen retrieval request", 1, automatic, Some("personal"));
                assert_eq!(result[0]["id"], strong);
                assert!((number(&result[0], "score", 0.) - 0.95).abs() < 1e-6);
                assert_eq!(
                    result[0]["source_occurred_at"],
                    if reversed {
                        "2026-10-26T00:00:00+00:00"
                    } else {
                        "2020-01-01T00:00:00+00:00"
                    }
                );
            }
        }
    }
    #[test]
    fn exact_relevance_ties_use_age_then_importance_and_preserve_citation_metadata() {
        let mut f = Fixture::new();
        let old = f.add(
            "Earlier comparable assertion source",
            "knowledge",
            "2020-01-01T00:00:00Z",
            1.,
            0.8,
            "personal",
        );
        let recent = f.add(
            "Recent comparable assertion source",
            "knowledge",
            "2026-10-25T00:00:00Z",
            0.1,
            0.8,
            "personal",
        );
        assert_eq!(
            f.recall("unseen request", 1, false, Some("personal"))[0]["id"],
            recent
        );
        f.store.write(|tx|{tx.sql().execute("UPDATE node_stats SET last_cited_at='2026-10-26T00:00:00+00:00' WHERE node_id IN (?,?)",params![old,recent])?;Ok(())}).unwrap();
        let row = f.recall("unseen request", 1, false, Some("personal"));
        assert_eq!(row[0]["id"], old);
        assert_eq!(row[0]["ranking_anchor"], "2026-10-26T00:00:00+00:00");
        assert_eq!(row[0]["source_occurred_at"], "2020-01-01T00:00:00+00:00");
        assert_eq!(row[0]["evidence"].as_array().unwrap().len(), 1);
    }
    #[test]
    fn or_discovery_is_semantically_admitted_and_does_not_anchor_partial_names() {
        let mut f = Fixture::new();
        let will = f.add(
            "Will Smith",
            "person",
            "2026-10-25T00:00:00Z",
            1.,
            0.2,
            "personal",
        );
        let weather = f.add(
            "Tomorrow heavy rain is forecast for the garden",
            "knowledge",
            "2020-01-01T00:00:00Z",
            0.1,
            0.8,
            "personal",
        );
        for automatic in [false, true] {
            let rows = f.recall("Will it rain tomorrow", 3, automatic, Some("personal"));
            assert!(rows.iter().any(|r| r["id"] == weather));
            assert!(rows.iter().all(|r| r["id"] != will));
        }
        let mark = f.add(
            "Mark Petrov",
            "person",
            "2026-10-25T00:00:00Z",
            1.,
            0.2,
            "personal",
        );
        assert!(
            f.recall("Mark checklist done", 3, false, Some("personal"))
                .iter()
                .all(|r| r["id"] != mark)
        );
    }
    #[test]
    fn exact_same_names_remain_separate_and_scope_filtered() {
        let mut f = Fixture::new();
        let one = f.add(
            "Leonid Demchenko",
            "person",
            "2026-10-25T00:00:00Z",
            0.5,
            0.8,
            "personal",
        );
        let two = f.add(
            "Leonid Demchenko",
            "person",
            "2026-10-25T00:00:00Z",
            0.5,
            0.8,
            "personal",
        );
        let secret = f.add(
            "Leonid Demchenko",
            "person",
            "2026-10-25T00:00:00Z",
            1.,
            0.9,
            "professional",
        );
        let rows = f.recall("Who is Leonid Demchenko?", 2, true, Some("personal"));
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|r| r["id"] == one));
        assert!(rows.iter().any(|r| r["id"] == two));
        assert!(
            rows.iter()
                .all(|r| r["id"] != secret && r["same_name_count"] == 2)
        );
    }
    #[test]
    fn admitted_fact_lifts_only_recorded_scope_eligible_entity_connections() {
        let mut f = Fixture::new();
        let person = f.add(
            "Nadia Orlov",
            "person",
            "2020-01-01T00:00:00Z",
            0.1,
            0.2,
            "personal",
        );
        let fact = f.add(
            "The logistics pickup is scheduled tomorrow",
            "knowledge",
            "2020-01-01T00:00:00Z",
            0.1,
            0.9,
            "personal",
        );
        let text = "Nadia coordinates the pickup";
        let episode=f.store.capture(serde_json::from_value(json!({"source":"synthetic","event_id":"edge","conversation_id":"edge","kind":"user","text":text,"metadata":{}})).unwrap()).unwrap();
        f.graph.link(serde_json::from_value(json!({"command_id":"edge","actor":"owner","why":"Recorded exact relation","a":fact,"b":person,"rel":"related_to","evidence":[{"episode_id":episode.id,"quote":text}]})).unwrap()).unwrap();
        let rows = f.recall("unseen pickup request", 2, true, Some("personal"));
        assert_eq!(rows[0]["id"], fact);
        assert!(rows.iter().all(|r| r["category"] != "entity_context"));
        assert_eq!(rows[0]["related"], json!([]));
        let text = rows[0]["text"].as_str().unwrap();
        assert!(text.starts_with("Source 2020-01-01: "));
        assert!(text.contains("Connected: Nadia Orlov [person; related_to out; edge "));
        let normal = f.recall("Who manages logistics?", 2, false, Some("personal"));
        assert_eq!(normal[0]["id"], fact);
        assert!(normal.iter().all(|r| r["category"] != "entity_context"));
        assert!(
            normal[0]["related"]
                .as_array()
                .unwrap()
                .iter()
                .any(|r| r["id"] == person)
        );
        let named = f.recall("Does Nadia handle pickup?", 3, true, Some("personal"));
        assert_eq!(named[0]["id"], fact);
        assert_eq!(named[1]["id"], person);
        assert_eq!(named[1]["match"], "recorded_connection");
        assert_eq!(named[1]["basis_memory_id"], fact);
    }
    #[test]
    fn t2_context_never_preempts_stronger_edgeless_fact_or_its_own_basis() {
        let mut f = Fixture::new();
        let strong = f.add(
            "Stored oxygen procedure remains specifically applicable",
            "knowledge",
            "2020-01-01T00:00:00Z",
            0.1,
            0.95,
            "personal",
        );
        let weak = f.add(
            "Native helper has some broadly related steps",
            "knowledge",
            "2026-10-25T00:00:00Z",
            1.0,
            0.67,
            "personal",
        );
        let tool = f.add(
            "Pi command tool",
            "tool",
            "2026-10-25T00:00:00Z",
            1.0,
            0.2,
            "personal",
        );
        let quote = "Pi is related to this helper";
        let source=f.store.capture(serde_json::from_value(json!({"source":"synthetic","event_id":"weak-edge","conversation_id":"thread","kind":"user","text":quote,"metadata":{}})).unwrap()).unwrap();
        f.graph.link(serde_json::from_value(json!({"command_id":"weak-edge","actor":"owner","why":"Exact synthetic connection","a":weak,"b":tool,"rel":"related_to","evidence":[{"episode_id":source.id,"quote":quote}]})).unwrap()).unwrap();
        for automatic in [false, true] {
            let rows = f.recall(
                "unseen Pi retention request",
                1,
                automatic,
                Some("personal"),
            );
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0]["id"], strong);
            assert!((number(&rows[0], "score", 0.) - 0.95).abs() < 1e-6);
        }
        let rows = f.recall("unseen Pi retention request", 2, true, Some("personal"));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], strong);
        assert_eq!(rows[1]["id"], weak);
        let rows = f.recall("unseen Pi retention request", 3, true, Some("personal"));
        assert_eq!(rows[0]["id"], strong);
        assert_eq!(rows[1]["id"], weak);
        assert_eq!(rows[2]["id"], tool);
        assert_eq!(rows[2]["basis_memory_id"], weak);
        assert!(
            rows[1]["text"]
                .as_str()
                .unwrap()
                .contains("Pi command tool [tool; related_to out; edge ")
        );
    }
    #[test]
    fn t2_named_ambiguity_keeps_separate_scopes_and_unnamed_roles_never_anchor() {
        let mut f = Fixture::new();
        let personal = f.add(
            "Leonid Mironov",
            "person",
            "2020-01-01T00:00:00Z",
            0.5,
            0.2,
            "personal",
        );
        let professional = f.add(
            "Leonid Sokolov",
            "person",
            "2020-01-01T00:00:00Z",
            0.5,
            0.2,
            "professional",
        );
        let other = f.add(
            "Margarita Orlov",
            "person",
            "2020-01-01T00:00:00Z",
            0.5,
            0.2,
            "personal",
        );
        let mark = f.add(
            "Mark Petrov",
            "person",
            "2020-01-01T00:00:00Z",
            0.5,
            0.2,
            "professional",
        );
        let family = f.add(
            "Family members arranged tomorrow's pickup",
            "knowledge",
            "2020-01-01T00:00:00Z",
            0.5,
            0.90,
            "personal",
        );
        let logistics = f.add(
            "Warehouse schedule includes the evening delivery",
            "knowledge",
            "2020-01-01T00:00:00Z",
            0.5,
            0.91,
            "professional",
        );
        f.connect(&family, &personal);
        f.connect(&family, &other);
        f.connect(&logistics, &professional);
        for automatic in [false, true] {
            let rows = f.recall("Which Leonid should reply?", 6, automatic, None);
            let named = rows
                .iter()
                .filter(|r| r["category"] == "entity_context")
                .collect::<Vec<_>>();
            assert_eq!(named.len(), 2);
            assert!(named.iter().any(|r| r["id"] == personal));
            assert!(named.iter().any(|r| r["id"] == professional));
            assert!(named.iter().all(|r| r["id"] != other && r["id"] != mark));
            for anchor in named {
                let fact = rows
                    .iter()
                    .position(|r| r["id"] == anchor["basis_memory_id"])
                    .unwrap();
                let entity = rows.iter().position(|r| r["id"] == anchor["id"]).unwrap();
                assert!(fact < entity);
            }
            let scoped = f.recall("Which Leonid should reply?", 6, automatic, Some("personal"));
            assert!(
                scoped
                    .iter()
                    .any(|r| r["id"] == personal && r["category"] == "entity_context")
            );
            assert!(
                scoped
                    .iter()
                    .all(|r| r["id"] != professional && r["id"] != logistics)
            );
            for query in ["Mark checklist done", "Who manages logistics?"] {
                let rows = f.recall(query, 6, automatic, None);
                assert!(rows.iter().any(|r| r["id"] == logistics));
                assert!(rows.iter().all(|r| r["category"] != "entity_context"));
                let fact = rows.iter().find(|r| r["id"] == logistics).unwrap();
                if automatic {
                    assert_eq!(fact["related"], json!([]));
                    assert!(
                        fact["text"]
                            .as_str()
                            .unwrap()
                            .contains("Leonid Sokolov [person; related_to out; edge ")
                    );
                } else {
                    assert!(
                        fact["related"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|r| r["id"] == professional)
                    );
                }
            }
        }
    }
    #[test]
    fn t3_real_unlinked_bio_shape_keeps_bio_then_canonical_and_recorded_scoped_edges() {
        let mut f = Fixture::new();
        let entity = f.add(
            "Leonid North",
            "person",
            "2020-01-01T00:00:00Z",
            0.5,
            0.7948922,
            "personal",
        );
        let bio = f.add(
            "Leonid is the family member with a long retained biography",
            "knowledge",
            "2020-01-01T00:00:00Z",
            0.5,
            0.77928543,
            "personal",
        );
        let relative = f.add(
            "Family Relative",
            "person",
            "2020-01-01T00:00:00Z",
            0.5,
            0.2,
            "personal",
        );
        let coworker = f.add(
            "Professional Colleague",
            "person",
            "2020-01-01T00:00:00Z",
            0.5,
            0.2,
            "professional",
        );
        f.connect(&entity, &relative);
        f.connect(&entity, &coworker);
        for i in 0..6 {
            f.add(
                &format!("Other retained family detail number {i}"),
                "knowledge",
                "2020-01-01T00:00:00Z",
                0.5,
                0.69,
                "personal",
            );
        }
        for automatic in [true, false] {
            let rows = f.recall("Who is Leonid?", 6, automatic, Some("personal"));
            assert_eq!(rows[0]["id"], bio);
            assert_eq!(rows[1]["id"], entity);
            assert_eq!(rows[1]["match"], "partial_entity_name");
            assert!(rows[1].get("basis_memory_id").is_none());
            let edges = rows[1]["connections"].as_array().unwrap();
            assert_eq!(edges.len(), 1);
            assert_eq!(edges[0]["id"], relative);
            assert!(edges.iter().all(|e| e["id"] != bio && e["id"] != coworker));
            assert!(rows.len() <= 6);
        }
    }
    #[test]
    fn t3_gloss_and_full_name_terms_never_turn_unnamed_roles_into_partial_names() {
        let mut f = Fixture::new();
        let person = f.add(
            "Leonid North — manages logistics",
            "person",
            "2020-01-01T00:00:00Z",
            0.5,
            0.85,
            "personal",
        );
        let fact = f.add(
            "A manager handles logistics and all evening deliveries",
            "knowledge",
            "2020-01-01T00:00:00Z",
            0.5,
            0.9,
            "personal",
        );
        for q in ["Who manages logistics?", "Who handles evening deliveries?"] {
            let rows = f.recall(q, 6, true, Some("personal"));
            assert_eq!(rows[0]["id"], fact);
            assert!(rows.iter().all(|r| r["id"] != person));
        }
        let rows = f.recall("Who is Leonid?", 6, true, Some("personal"));
        assert!(rows.iter().all(|r| r["id"] != person)); // entity lacks corroboration and loses cosine dominance
        f.connect(&fact, &person);
        let rows = f.recall("Who manages logistics?", 6, true, Some("personal"));
        assert!(rows.iter().all(|r| r["category"] != "entity_context"));
        assert!(
            rows[0]["text"]
                .as_str()
                .unwrap()
                .contains("Connected: Leonid North")
        );
    }
    #[test]
    fn t3_two_alex_entries_stay_separate_and_strong_edgeless_fact_keeps_limit_one() {
        let mut f = Fixture::new();
        let one = f.add(
            "Alex North",
            "person",
            "2020-01-01T00:00:00Z",
            0.5,
            0.82,
            "personal",
        );
        let two = f.add(
            "Alex South",
            "person",
            "2020-01-01T00:00:00Z",
            0.5,
            0.81,
            "personal",
        );
        let fact = f.add(
            "Alex is mentioned in a retained family source",
            "knowledge",
            "2020-01-01T00:00:00Z",
            0.5,
            0.90,
            "personal",
        );
        let rows = f.recall("Which Alex should reply?", 6, true, Some("personal"));
        assert_eq!(rows[0]["id"], fact);
        assert_eq!(rows[1]["id"], one);
        assert_eq!(rows[2]["id"], two);
        assert!(
            rows[1..3]
                .iter()
                .all(|r| r["connections"] == json!([]) && r.get("basis_memory_id").is_none())
        );
        let strong = f.add(
            "Alex has a precise oxygen procedure applicable to this request",
            "knowledge",
            "2020-01-01T00:00:00Z",
            0.1,
            0.95,
            "personal",
        );
        for automatic in [false, true] {
            assert_eq!(
                f.recall("Which Alex should reply?", 1, automatic, Some("personal"))[0]["id"],
                strong
            );
        }
    }
    #[test]
    fn t3_mixed_query_needs_corroboration_and_pure_name_keeps_normal_floor() {
        let mut f = Fixture::new();
        let strong = f.add(
            "Nadia North",
            "person",
            "2020-01-01T00:00:00Z",
            0.5,
            0.8,
            "personal",
        );
        let weak = f.add(
            "Nadia South",
            "person",
            "2020-01-01T00:00:00Z",
            1.,
            0.684,
            "personal",
        );
        let zero = f.add(
            "Someone Else",
            "person",
            "2020-01-01T00:00:00Z",
            1.,
            0.99,
            "personal",
        );
        let fact = f.add(
            "An unrelated retained statement about ordinary work",
            "knowledge",
            "2020-01-01T00:00:00Z",
            0.5,
            0.75,
            "personal",
        );
        let rows = f.recall("What did Nadia do?", 6, true, Some("personal"));
        assert_eq!(rows[0]["id"], fact);
        assert!(rows.iter().all(|r| r["category"] != "entity_context"));
        let rows = f.recall("Nadia", 6, true, Some("personal"));
        assert_eq!(rows[0]["id"], fact);
        assert_eq!(rows[1]["id"], strong);
        assert!(rows.iter().all(|r| r["id"] != weak && r["id"] != zero));
        let tie = f.add(
            "Nadia West",
            "person",
            "2020-01-01T00:00:00Z",
            1.,
            0.75,
            "personal",
        );
        let rows = f.recall("What did Nadia do?", 6, true, Some("personal"));
        assert!(rows.iter().all(|r| r["id"] != tie));
        assert!(
            f.recall("Nadia", 6, true, Some("personal"))
                .iter()
                .any(|r| r["id"] == tie)
        );
    }
    #[test]
    fn t3_df_is_current_active_visible_and_scoped_not_global_or_version_counted() {
        let mut f = Fixture::new();
        let entity = f.add(
            "Leonid North",
            "person",
            "2020-01-01T00:00:00Z",
            0.5,
            0.8,
            "personal",
        );
        f.add(
            "Leonid is present in a retained biography",
            "knowledge",
            "2020-01-01T00:00:00Z",
            0.5,
            0.9,
            "personal",
        );
        for i in 0..17 {
            f.add(
                &format!("Leonid appears in professional source {i}"),
                "knowledge",
                "2020-01-01T00:00:00Z",
                0.5,
                0.7,
                "professional",
            );
            let forgotten = f.add(
                &format!("Leonid appears in excluded personal source {i}"),
                "knowledge",
                "2020-01-01T00:00:00Z",
                0.5,
                0.7,
                "personal",
            );
            f.store.write(|tx|{tx.sql().execute("INSERT INTO forgotten_targets SELECT ?1,commit_id,'node' FROM node_versions WHERE id=?1",[&forgotten])?;Ok(())}).unwrap();
        }
        assert!(
            f.recall("Who is Leonid?", 6, true, Some("personal"))
                .iter()
                .any(|r| r["id"] == entity && r["match"] == "partial_entity_name")
        );
        assert!(
            f.recall("Who is Leonid?", 6, true, None)
                .iter()
                .all(|r| r["category"] != "entity_context")
        );
        for i in 0..17 {
            f.add(
                &format!("Leonid appears in another active personal source {i}"),
                "knowledge",
                "2020-01-01T00:00:00Z",
                0.5,
                0.7,
                "personal",
            );
        }
        assert!(
            f.recall("Who is Leonid?", 6, true, Some("personal"))
                .iter()
                .all(|r| r["category"] != "entity_context")
        );
    }
    #[test]
    fn t3_reviewed_alias_uses_current_pinned_canonical_vector_not_stale_names() {
        let mut f = Fixture::new();
        let entity = f.add(
            "Leonid North",
            "person",
            "2020-01-01T00:00:00Z",
            0.5,
            0.8,
            "personal",
        );
        f.add(
            "Leonid appears in an independently retained biography",
            "knowledge",
            "2020-01-01T00:00:00Z",
            0.5,
            0.9,
            "personal",
        );
        f.store.write(|tx|{
            tx.sql().execute("INSERT INTO node_versions SELECT id,2,commit_id,type,'Leo North','active',valid_from,valid_to,superseded_by,1,scope FROM node_versions WHERE id=? AND version=1",[&entity])?;
            tx.sql().execute("INSERT INTO graph_evidence SELECT target,2,episode_id,start,end,quote FROM graph_evidence WHERE target=? AND version=1",[&entity])?;
            tx.sql().execute("UPDATE vector_blobs SET current=0 WHERE seq IN(SELECT seq FROM vector_items WHERE target=?)",[&entity])?;
            let mut v=[0.;768];v[0]=0.8;v[1]=0.6;
            tx.put_vector(&VectorWrite{seq:1000,target:entity.clone(),kind:VectorKind::Node,version:2,embedding:F32Blob::from_values(&v)?,current:true,tier:Tier::Hot})?;Ok(())
        }).unwrap();
        assert!(
            f.recall("Who is Leonid?", 6, true, Some("personal"))
                .iter()
                .all(|r| r["id"] != entity)
        );
        f.store.write(|tx|{
            tx.sql().execute("INSERT INTO entity_merges SELECT 'reviewed-source',id,commit_id FROM node_versions WHERE id=? AND version=2",[&entity])?;
            tx.sql().execute("INSERT INTO alias_fts VALUES('reviewed-source','Leonid North — old reviewed description')",[])?;Ok(())
        }).unwrap();
        let rows = f.recall("Who is Leonid?", 6, true, Some("personal"));
        let anchor = rows.iter().find(|r| r["id"] == entity).unwrap();
        assert_eq!(anchor["version"], 2);
        assert_eq!(anchor["pinned"], 1);
        assert_eq!(anchor["match"], "partial_entity_name");
        f.store
            .write(|tx| {
                tx.sql()
                    .execute("UPDATE vector_blobs SET current=0 WHERE seq=1000", [])?;
                Ok(())
            })
            .unwrap();
        assert!(
            f.recall("Who is Leonid?", 6, true, Some("personal"))
                .iter()
                .all(|r| r["id"] != entity)
        );
    }
    #[test]
    fn t3_preserves_t1_t2_shared_context_budget() {
        let mut f = Fixture::new();
        let t1 = f.add(
            "Uma",
            "person",
            "2020-01-01T00:00:00Z",
            0.5,
            0.1,
            "personal",
        );
        let t2 = f.add(
            "Nadia North",
            "person",
            "2020-01-01T00:00:00Z",
            0.5,
            0.1,
            "personal",
        );
        let t3 = f.add(
            "Leonid South",
            "person",
            "2020-01-01T00:00:00Z",
            0.5,
            0.8,
            "personal",
        );
        let fact = f.add(
            "Leonid has a documented relevant family biography",
            "knowledge",
            "2020-01-01T00:00:00Z",
            0.5,
            0.9,
            "personal",
        );
        f.connect(&fact, &t2);
        let rows = f.recall("Uma Nadia Leonid", 6, true, Some("personal"));
        assert_eq!(rows[0]["id"], t1);
        assert_eq!(rows[1]["id"], fact);
        assert_eq!(rows[2]["id"], t2);
        assert_eq!(rows[2]["match"], "recorded_connection");
        assert!(rows.iter().all(|r| r["id"] != t3));
        assert_eq!(
            rows.iter()
                .filter(|r| r["category"] == "entity_context")
                .count(),
            2
        );
    }

    #[test]
    fn t3_candidate_separate_identity_scope_decision_and_source_forgetting_are_fenced() {
        let mut f = Fixture::new();
        let fact = f.add(
            "Alex occurs in this retained family biography",
            "knowledge",
            "2020-01-01T00:00:00Z",
            0.5,
            0.9,
            "personal",
        );
        let seed = f.add(
            "Alex North",
            "person",
            "2020-01-01T00:00:00Z",
            0.5,
            0.8,
            "personal",
        );
        let episode=f.store.capture(serde_json::from_value(json!({"source":"synthetic","event_id":"pending-alex","conversation_id":"pending-alex","kind":"user","text":"Alex South","metadata":{}})).unwrap()).unwrap();
        f.store.write(|tx|{
            tx.sql().execute("INSERT INTO extraction_runs SELECT 'pending-run',commit_id,?2,'test','test','[]','{}','2026-10-25' FROM node_versions WHERE id=?1 AND version=1",params![seed,episode.id])?;
            tx.sql().execute("INSERT INTO candidates VALUES('pending-alex','pending-run','entity','person','Alex South','Exact source',NULL,NULL,1,NULL,NULL,'[\"personal\"]')",[])?;
            tx.sql().execute("INSERT INTO graph_evidence VALUES('pending-alex',1,?1,0,10,'Alex South')",[&episode.id])?;
            let mut v=[0.;768];v[0]=0.81;v[1]=(1f32-v[0]*v[0]).sqrt();
            tx.put_vector(&VectorWrite{seq:2000,target:"pending-alex".into(),kind:VectorKind::Candidate,version:1,embedding:F32Blob::from_values(&v)?,current:true,tier:Tier::Hot})?;Ok(())
        }).unwrap();
        let rows = f.recall("Which Alex?", 6, true, Some("personal"));
        assert_eq!(rows[0]["id"], fact);
        assert_eq!(rows[1]["id"], seed); // durable tie class precedes candidate even with slightly lower cosine
        let pending = rows.iter().find(|r| r["id"] == "pending-alex").unwrap();
        assert_eq!(pending["tier"], "candidate");
        assert_eq!(pending["match"], "partial_entity_name");
        assert_eq!(pending["confirmed"], false);
        assert!(
            f.recall("Which Alex?", 6, true, Some("professional"))
                .iter()
                .all(|r| r["id"] != "pending-alex")
        );
        f.store.write(|tx|{tx.sql().execute("INSERT INTO forgotten_sources SELECT 'synthetic','pending-alex','pending-alex',commit_id FROM node_versions WHERE id=? AND version=1",[&seed])?;Ok(())}).unwrap();
        assert!(
            f.recall("Which Alex?", 6, true, Some("personal"))
                .iter()
                .all(|r| r["id"] != "pending-alex")
        );
        f.store.write(|tx|{
            tx.sql().execute("INSERT INTO extraction_runs SELECT 'reject-run',r.commit_id,g.episode_id,r.version,r.requested_model,r.reported_models,r.usage,r.recorded_at FROM extraction_runs r JOIN graph_evidence g ON g.target=?1 WHERE r.id='pending-run' LIMIT 1",[&seed])?;
            tx.sql().execute("INSERT INTO candidates SELECT 'pending-rejected','reject-run',kind,type,text,why,src,dst,confidence,valid_from,valid_to,scope FROM candidates WHERE id='pending-alex'",[])?;
            let mut v=[0.;768];v[0]=0.81;v[1]=(1f32-v[0]*v[0]).sqrt();
            tx.put_vector(&VectorWrite{seq:2001,target:"pending-rejected".into(),kind:VectorKind::Candidate,version:1,embedding:F32Blob::from_values(&v)?,current:true,tier:Tier::Hot})?;Ok(())
        }).unwrap();
        assert!(
            f.recall("Which Alex?", 6, true, Some("personal"))
                .iter()
                .any(|r| r["id"] == "pending-rejected")
        );
        f.store.write(|tx|{tx.sql().execute("INSERT INTO candidate_decisions SELECT 'reject-pending','pending-rejected',commit_id,'reject',NULL,NULL FROM node_versions WHERE id=? AND version=1",[&seed])?;Ok(())}).unwrap();
        assert!(
            f.recall("Which Alex?", 6, true, Some("personal"))
                .iter()
                .all(|r| r["id"] != "pending-rejected")
        );
    }

    #[test]
    fn t3_legacy_only_forgotten_nonfinite_and_missing_current_vectors_do_not_anchor() {
        let mut f = Fixture::new();
        let fact = f.add(
            "Leonid is described by the retained source biography",
            "knowledge",
            "2020-01-01T00:00:00Z",
            0.5,
            0.9,
            "personal",
        );
        let episode=f.store.capture(serde_json::from_value(json!({"source":"native:fixture:codex","event_id":"legacy-only","conversation_id":"legacy-only","kind":"developer","text":"Leonid Legacy","metadata":{}})).unwrap()).unwrap();
        // Synthetic historical graph supported only by developer instruction evidence.
        f.store.write(|tx|{
            tx.sql().execute("INSERT INTO node_versions SELECT 't3-legacy',1,commit_id,'person','Leonid Legacy','active',NULL,NULL,NULL,0,'[\"personal\"]' FROM node_versions WHERE id=? AND version=1",[&fact])?;
            tx.sql().execute("INSERT INTO graph_evidence VALUES('t3-legacy',1,?,0,13,'Leonid Legacy')",[&episode.id])?;
            let mut v=[0.;768];v[0]=0.8;v[1]=0.6;
            tx.put_vector(&VectorWrite{seq:3000,target:"t3-legacy".into(),kind:VectorKind::Node,version:1,embedding:F32Blob::from_values(&v)?,current:true,tier:Tier::Hot})?;Ok(())
        }).unwrap();
        for (name, mode) in [
            ("Leonid Forgotten", "forgotten"),
            ("Leonid Nan", "nan"),
            ("Leonid Missing", "missing"),
        ] {
            let id = f.add(name, "person", "2020-01-01T00:00:00Z", 0.5, 0.8, "personal");
            f.store.write(|tx|{
                match mode {
                    "forgotten"=>{tx.sql().execute("INSERT INTO forgotten_targets SELECT id,commit_id,'node' FROM node_versions WHERE id=? AND version=1",[&id])?;},
                    "nan"=>{let v=[f32::NAN;768];tx.sql().execute("UPDATE vector_blobs SET embedding=?1 WHERE seq IN(SELECT seq FROM vector_items WHERE target=?2)",params![F32Blob::from_values(&v)?.as_bytes(),id])?;},
                    _=>{tx.sql().execute("UPDATE vector_blobs SET current=0 WHERE seq IN(SELECT seq FROM vector_items WHERE target=?)",[&id])?;}
                } Ok(())
            }).unwrap();
        }
        // Reject before any ANN engine handles the deliberately corrupt fixture bytes.
        f.store
            .read(|tx| {
                let mut v = [0.; 768];
                v[0] = 1.;
                let mut output = Vec::new();
                anchors::add_partials(
                    tx.sql(),
                    "Who is Leonid?",
                    &v,
                    Some("personal"),
                    TEXT_FLOOR,
                    &[],
                    &mut output,
                    6,
                )?;
                assert!(output.is_empty());
                Ok(())
            })
            .unwrap();
    }
    #[test]
    fn t3_partial_same_spelling_reports_ambiguity_without_merging() {
        let mut f = Fixture::new();
        let one = f.add(
            "Alex North",
            "person",
            "2020-01-01T00:00:00Z",
            0.5,
            0.82,
            "personal",
        );
        let two = f.add(
            "Alex North",
            "person",
            "2020-01-01T00:00:00Z",
            0.5,
            0.81,
            "personal",
        );
        f.add(
            "Alex appears in this retained biography",
            "knowledge",
            "2020-01-01T00:00:00Z",
            0.5,
            0.9,
            "personal",
        );
        let rows = f.recall("Who is Alex?", 6, true, Some("personal"));
        let entities = rows
            .iter()
            .filter(|r| r["category"] == "entity_context")
            .collect::<Vec<_>>();
        assert_eq!(entities.len(), 2);
        assert!(entities.iter().any(|r| r["id"] == one));
        assert!(entities.iter().any(|r| r["id"] == two));
        assert!(entities.iter().all(|r| r["same_name_count"] == 2
            && r["same_name_ids"].as_array().unwrap().len() == 1
            && r["text"].as_str().unwrap().contains("not merged")));
    }
    #[test]
    fn t3_actual_mark_cosine_advantage_does_not_imply_mixed_query_name_intent() {
        let mut f = Fixture::new();
        let mark = f.add(
            "Mark Petrov",
            "person",
            "2020-01-01T00:00:00Z",
            0.5,
            0.7044715,
            "professional",
        );
        let finance = f.add(
            "Finance Team",
            "organization",
            "2020-01-01T00:00:00Z",
            0.5,
            0.2,
            "professional",
        );
        let logistics = f.add(
            "The logistics checklist is done for tomorrow's delivery",
            "knowledge",
            "2020-01-01T00:00:00Z",
            0.5,
            0.6990622,
            "professional",
        );
        f.connect(&mark, &finance);
        let rows = f.recall("Mark checklist done", 6, true, Some("professional"));
        assert_eq!(rows[0]["id"], logistics);
        assert!(
            rows.iter().all(|r| r["category"] != "entity_context"
                && r["id"] != mark
                && r["id"] != finance)
        );
        // Literal pure-name intent can admit context after the retained unrelated fact.
        let rows = f.recall("Mark", 6, true, Some("professional"));
        assert_eq!(rows[0]["id"], logistics);
        assert_eq!(rows[1]["id"], mark);
        assert_eq!(rows[1]["match"], "partial_entity_name");
        assert_eq!(rows[1]["connections"][0]["id"], finance);
    }
    #[test]
    fn t3_pure_partial_name_without_fact_passes_but_every_mixed_query_token_is_checked() {
        let mut f = Fixture::new();
        let mark = f.add(
            "Mark Petrov — manages logistics",
            "person",
            "2020-01-01T00:00:00Z",
            0.5,
            0.8,
            "personal",
        );
        for query in ["Mark", "MARK!", "Mark Mark"] {
            let rows = f.recall(query, 6, true, Some("personal"));
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0]["id"], mark);
            assert_eq!(rows[0]["match"], "partial_entity_name");
            assert!(rows[0].get("basis_memory_id").is_none());
        }
        for query in [
            "Who is Mark?".to_owned(),
            "Mark manages logistics".into(),
            format!("{}done", "Mark ".repeat(65)),
        ] {
            let rows = f.recall(&query, 6, true, Some("personal"));
            assert!(rows.iter().all(|r| r["category"] != "entity_context"));
        }
    }
    #[test]
    fn t3_pure_reviewed_alias_uses_one_literal_name_variant_without_gloss_tokens() {
        let mut f = Fixture::new();
        let canonical = f.add(
            "Marcus North",
            "person",
            "2020-01-01T00:00:00Z",
            0.5,
            0.8,
            "personal",
        );
        f.store.write(|tx|{
            tx.sql().execute("INSERT INTO entity_merges SELECT 'reviewed-source',id,commit_id FROM node_versions WHERE id=? AND version=1",[&canonical])?;
            tx.sql().execute("INSERT INTO alias_fts VALUES('reviewed-source','Mark Petrov — checklist done')",[])?;Ok(())
        }).unwrap();
        let rows = f.recall("Mark", 6, true, Some("personal"));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], canonical);
        assert_eq!(rows[0]["match"], "partial_entity_name");
        for query in ["Mark status", "Mark checklist done", "Marcus Mark"] {
            assert!(
                f.recall(query, 6, true, Some("personal"))
                    .iter()
                    .all(|r| r["category"] != "entity_context")
            );
        }
    }
    #[test]
    fn t3_display_connected_or_archive_wrappers_are_never_name_corroboration() {
        let mut f = Fixture::new();
        f.add(
            "Mark Petrov",
            "person",
            "2020-01-01T00:00:00Z",
            0.5,
            0.8,
            "personal",
        );
        let fact = f.add(
            "The logistics checklist has an unrelated stored assertion",
            "knowledge",
            "2020-01-01T00:00:00Z",
            0.5,
            0.7,
            "personal",
        );
        f.store.read(|tx|{
            let mut vector=[0.;768];vector[0]=1.;
            let assertion:String=tx.sql().query_row("SELECT text FROM node_versions WHERE id=? AND version=1",[&fact],|r|r.get(0))?;
            for original in [json!(assertion),Value::Null] {
                for display in ["Source 2020-01-01: Logistics (Connected: Mark Petrov)","Unverified historical source Mark: Logistics"] {
                    let fact=json!({"id":fact,"type":"knowledge","tier":"durable","category":"memory","text":display,"_full_text":original,"cosine":0.7});
                    let mut output=vec![fact.clone()];
                    anchors::add_partials(tx.sql(),"Mark checklist done",&vector,Some("personal"),TEXT_FLOOR,&[fact],&mut output,6)?;
                    assert_eq!(output.len(),1);assert!(output.iter().all(|r|r["category"]!="entity_context"));
                }
            }
            Ok(())
        }).unwrap();
    }
}
