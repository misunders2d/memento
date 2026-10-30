//! Stored-memory period filter (`memory_since` inclusive, `memory_until` exclusive).
//! The axis is the latest known original source time of each current eligible
//! memory: the same value recall reports as `source_occurred_at`. Capture,
//! import, extraction, promotion and citation times never qualify, and unknown
//! source dates never match a requested period. Raw `since`/`source` evidence
//! filtering is a separate contract.
use crate::{RecallRequest, records, retrieval};
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use memento_ann::eligibility::{ACTIVE_CANDIDATE, visible_sql};
use memento_core::{Error, ReadTxn, Result, utc_time};
use rusqlite::params;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, Debug)]
pub struct Period {
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
}
fn bound(value: Option<&str>) -> Result<Option<DateTime<Utc>>> {
    const INVALID: Error = Error::Validation("Memory period bounds must be aware ISO timestamps");
    value
        .map(|value| {
            let normalized = utc_time(value).map_err(|_| INVALID)?;
            Ok(DateTime::parse_from_rfc3339(&normalized)
                .map_err(|_| INVALID)?
                .with_timezone(&Utc))
        })
        .transpose()
}
impl Period {
    pub fn of(request: &RecallRequest) -> Result<Option<Self>> {
        let (since, until) = (
            bound(request.memory_since.as_deref())?,
            bound(request.memory_until.as_deref())?,
        );
        if since.is_none() && until.is_none() {
            return Ok(None);
        }
        if since
            .zip(until)
            .is_some_and(|(since, until)| since >= until)
        {
            return Err(Error::Validation(
                "Memory period start must be before its end",
            ));
        }
        Ok(Some(Self { since, until }))
    }
    /// Exact half-open check on the reported source time; unknown never matches.
    pub fn contains(&self, source_occurred_at: &Value) -> bool {
        let Some(at) = source_occurred_at
            .as_str()
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        else {
            return false;
        };
        let at = at.with_timezone(&Utc);
        self.since.is_none_or(|since| at >= since) && self.until.is_none_or(|until| at < until)
    }
    /// SQLite julianday compares at millisecond resolution. Widen each bound by
    /// one millisecond so the indexed prefilter is a superset; `contains` decides.
    pub(crate) fn sql_bounds(&self) -> (Option<String>, Option<String>) {
        let text = |at: DateTime<Utc>| at.to_rfc3339_opts(SecondsFormat::Millis, true);
        (
            self.since.map(|at| text(at - Duration::milliseconds(1))),
            self.until.map(|at| text(at + Duration::milliseconds(1))),
        )
    }
    /// `in_range(id, kind, version, latest)` over current eligible memories.
    /// Work starts from current memory identities, never from raw history:
    /// `episodes.occurred_at` is stored after the potentially large `text`
    /// column, so reading it walks that row's overflow pages. `win` therefore
    /// takes only window sequence numbers from the `episodes_activity`
    /// expression index, and `touched` probes each current version's evidence
    /// through covering primary/unique-key lookups without reading episode rows.
    /// The unary `+` keeps window membership a filter on that `id` lookup:
    /// as an index constraint SQLite would seek once per window row per probe.
    /// Both are supersets (a known date equals the indexed coalesce). Only
    /// touched memories read their evidence dates for the exact latest known
    /// original source. Captured media use their newest active reference,
    /// matching recall's reported media source time.
    pub(crate) fn sql(&self) -> String {
        let activity = "julianday(coalesce(e.occurred_at,e.captured_at))";
        let mut window = Vec::new();
        let mut latest = Vec::new();
        if self.since.is_some() {
            window.push(format!("{activity}>=julianday(?1)"));
            latest.push("d.latest>=julianday(?1)");
        }
        if self.until.is_some() {
            window.push(format!("{activity}<julianday(?2)"));
            latest.push("d.latest<julianday(?2)");
        }
        let window = window.join(" AND ");
        let latest = latest.join(" AND ");
        let known = "e.occurred_at IS NOT NULL AND NOT EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id)";
        let node = format!(
            "n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) AND NOT EXISTS (SELECT 1 FROM forgotten_targets f WHERE f.target=n.id) AND {} AND (?3 IS NULL OR EXISTS (SELECT 1 FROM json_each(n.scope) WHERE value=?3))",
            visible_sql("n.id")
        );
        format!(
            "WITH win AS MATERIALIZED (SELECT e.seq FROM episodes e WHERE {window}),\
             touched AS MATERIALIZED (SELECT p.target,p.kind,p.version FROM (\
             SELECT id AS target,'node' AS kind,max(version) AS version FROM node_versions GROUP BY id \
             UNION ALL SELECT id,'candidate',1 FROM candidates) p \
             WHERE EXISTS (SELECT 1 FROM graph_evidence g CROSS JOIN episodes e ON e.id=g.episode_id WHERE g.target=p.target AND g.version=p.version AND +e.seq IN (SELECT seq FROM win))),\
             dated AS MATERIALIZED (SELECT t.target,t.kind,t.version,(SELECT max(julianday(e.occurred_at)) FROM graph_evidence g CROSS JOIN episodes e ON e.id=g.episode_id WHERE g.target=t.target AND g.version=t.version AND {known}) AS latest FROM touched t),\
             media AS MATERIALIZED (SELECT cm.node_id FROM captured_media cm WHERE cm.node_id IS NOT NULL AND EXISTS (SELECT 1 FROM captured_media_refs r CROSS JOIN episodes e ON e.id=r.episode_id WHERE r.sha256=cm.sha256 AND +e.seq IN (SELECT seq FROM win)) GROUP BY cm.node_id),\
             media_dated AS MATERIALIZED (SELECT m.node_id AS target,(SELECT max(julianday(e.occurred_at)) FROM captured_media cm JOIN captured_media_refs r ON r.sha256=cm.sha256 JOIN episodes e ON e.id=r.episode_id WHERE cm.node_id=m.node_id AND {known}) AS latest FROM media m),\
             in_range AS (\
             SELECT n.id,'node' AS kind,n.version,d.latest FROM dated d CROSS JOIN node_versions n ON n.id=d.target AND n.version=d.version WHERE d.kind='node' AND NOT EXISTS (SELECT 1 FROM captured_media cm WHERE cm.node_id=n.id) AND {latest} AND {node} \
             UNION ALL SELECT n.id,'node',n.version,d.latest FROM media_dated d CROSS JOIN node_versions n ON n.id=d.target WHERE {latest} AND {node} \
             UNION ALL SELECT c.id,'candidate',1,d.latest FROM dated d CROSS JOIN candidates c ON c.id=d.target WHERE d.kind='candidate' AND {latest} AND {ACTIVE_CANDIDATE} AND (?3 IS NULL OR EXISTS (SELECT 1 FROM json_each(c.scope) WHERE value=?3)))"
        )
    }
    fn rows(&self, tx: &ReadTxn<'_>, select: &str, scope: Option<&str>) -> Result<Vec<Value>> {
        let (since, until) = self.sql_bounds();
        records::rows(
            tx.sql(),
            &format!("{} {select}", self.sql()),
            params![since, until, scope],
        )
    }
}
/// Memory IDs eligible by date plus their current search points. Restricting
/// the existing exact ANN search to these points keeps a strong in-period
/// match from being starved by closer out-of-period neighbours.
pub struct Eligible {
    pub ids_json: String,
    pub points: Vec<u64>,
}
pub fn eligible(tx: &ReadTxn<'_>, period: &Period, scope: Option<&str>) -> Result<Eligible> {
    let mut ids = HashSet::new();
    let mut points = Vec::new();
    let mut checked = HashMap::new();
    // Only rows near a widened bound need exact source hydration. Keep those
    // false positives out of both ANN and lexical admission before their limits,
    // rather than discarding them after they have crowded out valid matches.
    let boundary = "((?1 IS NOT NULL AND r.latest<=julianday(?1,'+0.003 seconds')) OR (?2 IS NOT NULL AND r.latest>=julianday(?2,'-0.003 seconds')))";
    for row in period.rows(
        tx,
        &format!("SELECT r.id,r.kind,r.version,i.seq,{boundary} AS boundary FROM in_range r LEFT JOIN vector_items i ON i.target=r.id AND i.kind=r.kind \
         UNION ALL SELECT r.id,r.kind,r.version,i.seq,{boundary} AS boundary FROM in_range r JOIN segments s ON s.node_id=r.id JOIN vector_items i ON i.target=s.id AND i.kind='segment'"),
        scope,
    )? {
        let id = row["id"].as_str().unwrap_or("");
        if row["boundary"].as_i64() == Some(1) {
            if !checked.contains_key(id) {
                let record = if row["kind"] == "candidate" {
                    records::candidate(tx.sql(), id)?
                } else {
                    records::node(tx.sql(), id, row["version"].as_i64().unwrap_or(0))?
                };
                let mut within = false;
                if let Some(mut record) = record {
                    if records::media_sources(tx.sql(), &mut record)? {
                        retrieval::finish(tx.sql(), &mut record, Utc::now())?;
                        within = period.contains(&record["source_occurred_at"]);
                    }
                }
                checked.insert(id.to_owned(), within);
            }
            if !checked[id] {
                continue;
            }
        }
        if let Some(seq) = row["seq"].as_u64() {
            points.push(seq);
        }
        ids.insert(id.to_owned());
    }
    points.sort_unstable();
    points.dedup();
    let ids_json = serde_json::to_string(&ids)?;
    Ok(Eligible { ids_json, points })
}
/// Period-only recall: newest in-period stored memories first, no query vector.
pub fn list(
    tx: &ReadTxn<'_>,
    request: &RecallRequest,
    period: &Period,
    at: DateTime<Utc>,
) -> Result<Vec<Value>> {
    let db = tx.sql();
    let scope = request.scope.as_deref();
    let mut output = Vec::new();
    for item in period.rows(
        tx,
        "SELECT id,kind,version FROM in_range ORDER BY latest DESC,id",
        scope,
    )? {
        if output.len() == request.limit {
            break;
        }
        let id = item["id"].as_str().unwrap_or("");
        let row = if item["kind"] == "candidate" {
            records::candidate(db, id)?
        } else {
            records::node(db, id, item["version"].as_i64().unwrap_or(0))?
        };
        let Some(mut row) = row else { continue };
        if !records::media_sources(db, &mut row)? {
            continue;
        }
        retrieval::finish(db, &mut row, at)?;
        if !period.contains(&row["source_occurred_at"]) {
            continue;
        }
        if request.effort != "cheap" && row["tier"] == "durable" {
            row["related"] = json!(records::related(
                db,
                id,
                if request.effort == "deep" { 2 } else { 1 },
                scope
            )?);
        }
        let fields = row.as_object_mut().unwrap();
        fields.remove("_full_text");
        records::merge(&mut row, json!({"match":"period","score":0.0}));
        output.push(row);
    }
    Ok(output)
}
