//! Whole-prompt exact-name lookup (J2). A process-local phrase index over the
//! append-only name sources (entity node versions, merge aliases, entity
//! candidates), caught up by rowid on each read cut. It only proposes targets:
//! visibility, current version, merges, forgetting and scope are decided live by
//! the caller's SQL on its own cut, so a stale phrase is never exposed.
use memento_core::Result;
use memento_domain::unicode::name_tokens;
use rusqlite::Connection;
use std::{
    collections::{HashMap, HashSet},
    sync::{LazyLock, Mutex},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Kind {
    Node,
    Alias,
    Candidate,
}
const SOURCES: [(Kind, &str); 3] = [
    // Every version of every entity node: the caller keeps only current names.
    (
        Kind::Node,
        "SELECT rowid,text FROM node_versions n WHERE rowid>?1 AND rowid<=?2 AND (n.type IN \
        ('person','organization','project','concept','place','tool') OR EXISTS(SELECT 1 FROM memory_types t \
        WHERE t.name=n.type AND t.category='entity'))",
    ),
    (Kind::Alias, "SELECT rowid,text FROM alias_fts WHERE rowid>?1 AND rowid<=?2"),
    (
        Kind::Candidate,
        "SELECT rowid,text FROM candidates WHERE rowid>?1 AND rowid<=?2 AND kind='entity'",
    ),
];
const TABLES: [&str; 3] = ["node_versions", "alias_fts", "candidates"];

#[derive(Default, Debug, PartialEq)]
pub struct NameIndex {
    // rowid watermarks per source, then memory_types' max rowid (entity typing).
    marks: [i64; 4],
    // First token -> (whole token sequence -> source rows that spell it).
    by_first: HashMap<String, HashMap<Vec<String>, Vec<(Kind, i64)>>>,
}
impl NameIndex {
    fn catch_up(&mut self, db: &Connection) -> Result<()> {
        let max = |table: &str| -> Result<i64> {
            Ok(db.query_row(&format!("SELECT coalesce(max(rowid),0) FROM {table}"), [], |r| r.get(0))?)
        };
        let tops = [max(TABLES[0])?, max(TABLES[1])?, max(TABLES[2])?, max("memory_types")?];
        // A reader on an older cut than the index needs nothing: the index is a superset
        // and live SQL drops rows that cut cannot see. A newer entity typing rebuilds;
        // append-only sources otherwise only extend.
        if tops[3] > self.marks[3] {
            *self = Self::default();
            self.marks[3] = tops[3];
        }
        for (i, (kind, sql)) in SOURCES.iter().enumerate() {
            if tops[i] <= self.marks[i] {
                continue;
            }
            let mut statement = db.prepare_cached(sql)?;
            let mut found = statement.query(rusqlite::params![self.marks[i], tops[i]])?;
            while let Some(row) = found.next()? {
                let (rowid, text): (i64, String) = (row.get(0)?, row.get(1)?);
                let tokens = name_tokens(&text);
                let Some(first) = tokens.first().cloned() else {
                    continue;
                };
                self.by_first
                    .entry(first)
                    .or_default()
                    .entry(tokens)
                    .or_default()
                    .push((*kind, rowid));
            }
            self.marks[i] = tops[i];
        }
        Ok(())
    }
    /// Source rows whose whole name appears as a contiguous token window anywhere in
    /// `prompt`; no token cap, no ranking before containment.
    fn lookup(&self, prompt: &[String]) -> Vec<(Kind, i64)> {
        let mut found = HashSet::new();
        for (start, token) in prompt.iter().enumerate() {
            let Some(names) = self.by_first.get(token) else {
                continue;
            };
            for (name, rows) in names {
                if prompt[start..].starts_with(name) {
                    found.extend(rows.iter().copied());
                }
            }
        }
        let mut found = found.into_iter().collect::<Vec<_>>();
        found.sort();
        found
    }
}

static INDEXES: LazyLock<Mutex<HashMap<String, NameIndex>>> = LazyLock::new(Default::default);

/// Rows (by source and rowid) spelling a whole name contained in `prompt`, read at
/// the caller's cut. Rows newer than that cut may be proposed; the caller's live
/// SQL simply does not find them.
pub fn lookup(db: &Connection, prompt: &[String]) -> Result<Vec<(Kind, i64)>> {
    let uuid: String = db.query_row("SELECT db_uuid FROM runtime_state WHERE singleton=1", [], |r| r.get(0))?;
    let mut indexes = INDEXES
        .lock()
        .map_err(|_| memento_core::Error::Unavailable("Name index requires recovery"))?;
    // One live database per process: another database replaces the index, never accumulates.
    if !indexes.contains_key(&uuid) {
        indexes.clear();
    }
    let index = indexes.entry(uuid).or_default();
    index.catch_up(db)?;
    Ok(index.lookup(prompt))
}

#[cfg(test)]
pub fn rebuilt(db: &Connection) -> Result<NameIndex> {
    let mut index = NameIndex::default();
    index.catch_up(db)?;
    Ok(index)
}
#[cfg(test)]
pub fn incremental(db: &Connection, index: &mut NameIndex) -> Result<()> {
    index.catch_up(db)
}
#[cfg(test)]
pub fn normalized(index: &NameIndex) -> Vec<(Vec<String>, Vec<(Kind, i64)>)> {
    let mut out = index
        .by_first
        .values()
        .flat_map(|names| names.iter().map(|(name, rows)| {
            let mut rows = rows.clone();
            rows.sort();
            (name.clone(), rows)
        }))
        .collect::<Vec<_>>();
    out.sort();
    out
}
