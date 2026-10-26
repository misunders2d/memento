use chrono::{DateTime, Datelike, SecondsFormat, Timelike, Utc};
use memento_core::{Error, Result};
use rusqlite::{Connection, Params, Row, types::ValueRef};
use serde_json::{Map, Value};

pub const ENTITIES: &[&str] = &[
    "person",
    "organization",
    "project",
    "concept",
    "place",
    "tool",
];
pub const MEMORIES: &[&str] = &["experience", "knowledge", "decision"];
pub const TYPES: &[&str] = &[
    "experience",
    "knowledge",
    "decision",
    "media",
    "person",
    "organization",
    "project",
    "concept",
    "place",
    "tool",
];
pub const RELATIONS: &[&str] = &[
    "belongs_to",
    "follows_up_on",
    "works_in",
    "decided_by",
    "mentions",
    "part_of",
    "caused_by",
    "owned_by",
    "related_to",
];
pub const ACTIVE_CANDIDATE: &str = "NOT EXISTS (SELECT 1 FROM forgotten_targets f WHERE f.target=c.id) AND NOT EXISTS (SELECT 1 FROM candidate_decisions d WHERE d.candidate_id=c.id AND d.rowid=(SELECT max(x.rowid) FROM candidate_decisions x WHERE x.candidate_id=c.id) AND d.decision IN ('promote','reject')) AND NOT EXISTS (SELECT 1 FROM extraction_runs r JOIN episodes e ON e.id=r.episode_id JOIN forgotten_sources f ON f.source=e.source AND f.event_id=e.event_id WHERE r.id=c.run_id)";

pub fn id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}
pub fn short_id() -> String {
    id()[..12].into()
}
pub fn now() -> String {
    format_time(Utc::now())
}
pub fn format_time(value: DateTime<Utc>) -> String {
    let value = value
        .with_nanosecond(value.nanosecond() / 1000 * 1000)
        .expect("valid truncated timestamp");
    value.to_rfc3339_opts(
        if value.timestamp_subsec_micros() == 0 {
            SecondsFormat::Secs
        } else {
            SecondsFormat::Micros
        },
        false,
    )
}
pub fn parse_time(value: &str) -> Result<DateTime<Utc>> {
    let parsed = DateTime::parse_from_rfc3339(value)
        .or_else(|_| DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f%:z"))
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| Error::Validation("World time must be an aware ISO timestamp"))?;
    if !(1..=9999).contains(&parsed.year()) || parsed.nanosecond() >= 1_000_000_000 {
        return Err(Error::Validation(
            "World time must be an aware ISO timestamp",
        ));
    }
    Ok(parsed)
}
pub fn time(value: Option<&str>) -> Result<Option<String>> {
    value
        .map(|value| parse_time(value).map(format_time))
        .transpose()
}
pub fn required(value: &str, maximum: usize, message: &'static str) -> Result<()> {
    if value.is_empty() || value.chars().count() > maximum {
        return Err(Error::Validation(message));
    }
    Ok(())
}
pub fn optional(value: Option<&str>, maximum: usize, message: &'static str) -> Result<()> {
    if let Some(value) = value {
        required(value, maximum, message)?;
    }
    Ok(())
}
pub fn valid_type(value: &str) -> bool {
    value.len() <= 64
        && value.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}
pub fn identity(value: &str) -> String {
    value
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}
pub fn substring(value: &str, start: usize, end: usize) -> String {
    value
        .chars()
        .skip(start)
        .take(end.saturating_sub(start))
        .collect()
}
pub fn excerpt(value: &str, length: usize) -> String {
    value.chars().take(length).collect()
}
pub fn get_str<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or(Error::Validation("Missing required command field"))
}
pub fn get_i64(value: &Value, key: &str) -> Result<i64> {
    value
        .get(key)
        .and_then(Value::as_i64)
        .ok_or(Error::Validation("Missing integer command field"))
}
pub fn decode_json(value: &str) -> Result<Value> {
    serde_json::from_str(value).map_err(|_| Error::Validation("Invalid stored JSON"))
}
pub fn encode_json(value: &Value) -> Result<String> {
    // Kernel owns Python-compatible sorted/spaced canonical serialization.
    memento_core::canonical_json(value)
}
pub fn hash_json(value: &Value) -> Result<String> {
    Ok(memento_core::raw::sha256(encode_json(value)?.as_bytes()))
}
pub fn row_value(row: &Row<'_>) -> rusqlite::Result<Value> {
    let mut value = Map::new();
    for (index, name) in row.as_ref().column_names().iter().enumerate() {
        let entry = match row.get_ref(index)? {
            ValueRef::Null => Value::Null,
            ValueRef::Integer(number) => Value::from(number),
            ValueRef::Real(number) => serde_json::Number::from_f64(number)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            ValueRef::Text(text) => Value::String(String::from_utf8_lossy(text).into_owned()),
            ValueRef::Blob(_) => {
                return Err(rusqlite::Error::InvalidColumnType(
                    index,
                    (*name).into(),
                    rusqlite::types::Type::Blob,
                ));
            }
        };
        value.insert((*name).into(), entry);
    }
    Ok(Value::Object(value))
}
pub fn rows<P: Params>(db: &Connection, sql: &str, parameters: P) -> Result<Vec<Value>> {
    let mut statement = db.prepare(sql)?;
    let values = statement
        .query_map(parameters, row_value)?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(values)
}
pub fn one<P: Params>(db: &Connection, sql: &str, parameters: P) -> Result<Option<Value>> {
    Ok(rows(db, sql, parameters)?.into_iter().next())
}
pub fn node(db: &Connection, target: &str) -> Result<Value> {
    one(
        db,
        "SELECT * FROM node_versions WHERE id=? ORDER BY version DESC LIMIT 1",
        [target],
    )?
    .ok_or(Error::Validation("Unknown node"))
}
pub fn known_type(db: &Connection, kind: &str) -> Result<bool> {
    Ok(TYPES.contains(&kind)
        || one(db, "SELECT 1 FROM memory_types WHERE name=?", [kind])?.is_some())
}
pub fn sensitive(db: &Connection, kind: &str) -> Result<bool> {
    Ok(one(
        db,
        "SELECT sensitive FROM memory_types WHERE name=?",
        [kind],
    )?
    .is_some_and(|value| value["sensitive"].as_i64().unwrap_or(0) != 0))
}
pub fn entity_type(db: &Connection, kind: &str) -> Result<bool> {
    Ok(ENTITIES.contains(&kind)
        || one(
            db,
            "SELECT 1 FROM memory_types WHERE name=? AND category='entity'",
            [kind],
        )?
        .is_some())
}
pub fn memory_type(db: &Connection, kind: &str) -> Result<bool> {
    Ok(MEMORIES.contains(&kind)
        || one(
            db,
            "SELECT 1 FROM memory_types WHERE name=? AND category='memory'",
            [kind],
        )?
        .is_some())
}
pub fn scope(db: &Connection, kind: &str, labels: &[String]) -> Result<Vec<String>> {
    if labels.is_empty()
        || labels.len() > 2
        || labels
            .iter()
            .any(|label| label != "personal" && label != "professional")
        || labels.len() == 2 && labels[0] == labels[1]
    {
        return Err(Error::Validation(
            "scope requires distinct personal/professional labels",
        ));
    }
    if labels.len() == 2 && !entity_type(db, kind)? {
        return Err(Error::Validation("Only an entity may have both scopes"));
    }
    let mut labels = labels.to_vec();
    labels.sort();
    Ok(labels)
}
