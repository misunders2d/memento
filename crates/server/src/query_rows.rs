//! Small local read-only SQL→JSON adapter; no service lifecycle dependency.
use memento_core::{Error, Result};
use rusqlite::{Connection, Params, types::ValueRef};
use serde_json::{Map, Value, json};
pub fn rows<P: Params>(db: &Connection, sql: &str, params: P) -> Result<Vec<Value>> {
    let mut q = db.prepare(sql)?;
    let names = q
        .column_names()
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let mut cursor = q.query(params)?;
    let mut rows = Vec::new();
    while let Some(row) = cursor.next()? {
        let mut record = Map::new();
        for (i, name) in names.iter().enumerate() {
            let value = match row.get_ref(i)? {
                ValueRef::Null => Value::Null,
                ValueRef::Integer(v) => json!(v),
                ValueRef::Real(v) if v.is_finite() => json!(v),
                ValueRef::Text(v) => Value::String(
                    std::str::from_utf8(v)
                        .map_err(|_| Error::Unavailable("Stored viewer text is invalid Unicode"))?
                        .into(),
                ),
                _ => {
                    return Err(Error::Unavailable(
                        "Stored viewer field has an invalid type",
                    ));
                }
            };
            record.insert(name.clone(), value);
        }
        rows.push(Value::Object(record));
    }
    Ok(rows)
}
pub fn one<P: Params>(db: &Connection, sql: &str, params: P) -> Result<Option<Value>> {
    Ok(rows(db, sql, params)?.into_iter().next())
}
pub fn array(text: &Value) -> Result<Value> {
    let value: Value = serde_json::from_str(
        text.as_str()
            .ok_or(Error::Unavailable("Stored scope is invalid"))?,
    )?;
    if !value.is_array() {
        return Err(Error::Unavailable("Stored scope is invalid"));
    }
    Ok(value)
}
pub const ACTIVE_CANDIDATE: &str = "NOT EXISTS (SELECT 1 FROM forgotten_targets f WHERE f.target=c.id) AND NOT EXISTS (SELECT 1 FROM candidate_decisions d WHERE d.candidate_id=c.id AND d.rowid=(SELECT max(x.rowid) FROM candidate_decisions x WHERE x.candidate_id=c.id) AND d.decision IN ('promote','reject')) AND NOT EXISTS (SELECT 1 FROM extraction_runs r JOIN episodes e ON e.id=r.episode_id JOIN forgotten_sources f ON f.source=e.source AND f.event_id=e.event_id WHERE r.id=c.run_id)";
