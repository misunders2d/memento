//! Bounded discovery/admission for captured attachments. Parsing and reference
//! authority stay in core; ordinary media jobs perform decoding outside writers.
use crate::stage;
use memento_core::{
    Result, canonical_json,
    capture::{EpisodeInput, captured_references, record_references},
};
use memento_domain::{TxGraph, jobs::Queue};
use rusqlite::params;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const BATCH: i64 = 64;
const MAX_PROJECTED_CHARS: i64 = 1024 * 1024;
const MAX_AUTOMATIC_JOBS: i64 = 2;

#[derive(Clone)]
pub struct CapturedMedia {
    queue: Queue,
}
impl CapturedMedia {
    pub fn new(queue: Queue) -> Self {
        Self { queue }
    }

    /// Backfill imported episodes without reading large native-record bodies.
    /// Snapshot parsing precedes a short idempotent reference/cursor transaction.
    pub fn discover(&self) -> Result<bool> {
        let (last, projected) = self.queue.graph.store.read(|tx| {
            let cursor: i64 = tx.sql().query_row("SELECT seq FROM captured_media_progress WHERE singleton=1", [], |r| r.get(0))?;
            let mut statement = tx.sql().prepare("SELECT seq,id,source,kind,CASE WHEN kind IN ('user','assistant','tool') THEN substr(text,1,?2) END,CASE WHEN kind IN ('user','assistant','tool') THEN metadata END FROM episodes WHERE seq>?1 ORDER BY seq LIMIT ?3")?;
            let mut rows = statement.query(params![cursor, MAX_PROJECTED_CHARS + 1, BATCH])?;
            let mut last = None;
            let mut projected = Vec::new();
            while let Some(row) = rows.next()? {
                last = Some(row.get::<_, i64>(0)?);
                let Some(metadata) = row.get::<_, Option<String>>(5)? else { continue };
                // Only source/kind/text/metadata are consumed by the sole parser.
                let input: EpisodeInput = serde_json::from_value(json!({
                    "source":row.get::<_,String>(2)?, "kind":row.get::<_,String>(3)?,
                    "text":row.get::<_,String>(4)?, "metadata":serde_json::from_str::<Value>(&metadata)?,
                    "event_id":"", "conversation_id":""
                }))?;
                projected.push((row.get::<_,String>(1)?, captured_references(&input)?));
            }
            Ok((last, projected))
        })?;
        let Some(last) = last else { return Ok(false) };
        self.queue.graph.store.write(|tx| {
            for (episode, references) in &projected {
                record_references(tx, episode, references)?;
            }
            tx.sql().execute(
                "UPDATE captured_media_progress SET seq=max(seq,?) WHERE singleton=1",
                [last],
            )?;
            Ok(())
        })?;
        Ok(true)
    }

    /// Admit at most two outstanding automatic jobs, fenced by the same writer
    /// that checks source/target forgetting and the pinned representation identity.
    pub fn admit(&self) -> Result<bool> {
        self.queue.graph.store.write(|tx| {
            let mut outstanding: i64 = tx.sql().query_row("SELECT count(*) FROM captured_media cm JOIN jobs j ON j.id=cm.job_id WHERE j.state IN ('pending','leased')", [], |r| r.get(0))?;
            if outstanding >= MAX_AUTOMATIC_JOBS { return Ok(false) }
            let rows = {
                let mut statement = tx.sql().prepare("SELECT sha256,kind,filename FROM captured_media WHERE state='ready' ORDER BY rowid LIMIT ?")?;
                statement.query_map([BATCH], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?)))?.collect::<std::result::Result<Vec<_>,_>>()?
            };
            let mut admitted = false;
            for (sha, kind, filename) in rows {
                if outstanding >= MAX_AUTOMATIC_JOBS { break }
                let forgotten: bool = tx.sql().query_row("SELECT EXISTS(SELECT 1 FROM media_files m JOIN forgotten_targets f ON f.target=m.node_id WHERE m.sha256=?)", [&sha], |r| r.get(0))?;
                if forgotten {
                    tx.sql().execute("UPDATE captured_media SET state='forgotten',error='forgotten_target' WHERE sha256=?", [&sha])?;
                    continue;
                }
                let source = match stage::active_source(tx.sql(), &sha) {
                    Ok(source) => source,
                    Err(memento_core::Error::Conflict("Captured media no longer has an active source")) => {
                        tx.sql().execute("UPDATE captured_media SET state='forgotten',error='forgotten_source' WHERE sha256=?", [&sha])?;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let identity = stage::representation_db(tx.sql(), &kind, &filename)?;
                let payload = json!({"actor":"memento:captured-media","why":"Index a captured attachment reference","sha256":sha,"kind":kind,"filename":filename,"evidence":[],"scope":[],"captured":true});
                let job = Queue::insert(tx, &job_key(&sha, &identity)?, "media", &payload)?;
                // The current active occurrence is checked again by each ingest
                // unit/final publication; admission records exact quote evidence.
                TxGraph::evidence(tx, &job, 1, &json!([source]))?;
                tx.sql().execute("UPDATE captured_media SET state='queued',job_id=?,error=NULL WHERE sha256=?", params![job, sha])?;
                admitted = true;
                outstanding += 1;
            }
            Ok(admitted)
        })
    }

    pub fn step(&self) -> Result<bool> {
        let discovered = self.discover()?;
        Ok(self.admit()? || discovered)
    }
}

fn job_key(sha: &str, identity: &str) -> Result<String> {
    // Python json.dumps([sha, identity], sort_keys=True) uses default spaces and
    // ensure_ascii=True, including surrogate pairs for non-BMP suffixes.
    let encoded = canonical_json(&json!([sha, identity]))?;
    let mut ascii = String::with_capacity(encoded.len());
    for ch in encoded.chars() {
        if ch.is_ascii() {
            ascii.push(ch);
        } else {
            for unit in ch.encode_utf16(&mut [0; 2]) {
                use std::fmt::Write;
                write!(ascii, "\\u{unit:04x}").unwrap();
            }
        }
    }
    Ok(format!(
        "captured-media/{:x}",
        Sha256::digest(ascii.as_bytes())
    ))
}

#[test]
fn captured_key_preserves_python_ascii_escaping_and_spaces() {
    // Independently frozen with stdlib hashlib/json.dumps, including one
    // non-BMP suffix character that requires a JSON UTF-16 surrogate pair.
    assert_eq!(
        job_key(&"a".repeat(64), "native .Ω😀").unwrap(),
        "captured-media/c6eefa8fe3dd187f1f826a516f5d0679824634dc77326802e000101b2645fc5d"
    );
}
