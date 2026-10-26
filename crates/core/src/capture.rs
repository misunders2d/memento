//! Immutable capture and literal raw evidence search. No model or HTTP under writer.
use crate::{
    Error, ReadTxn, Result, Store, WriteTxn, blobs::BlobInfo, canonical::canonical_compact_json,
    now_utc, raw, utc_time,
};
use regex::Regex;
use rusqlite::{OptionalExtension, params, types::Value as SqlValue};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::{io::Cursor, sync::LazyLock};
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EpisodeInput {
    pub source: String,
    pub event_id: String,
    pub conversation_id: String,
    pub kind: String,
    pub text: String,
    #[serde(default)]
    pub occurred_at: Option<String>,
    #[serde(default)]
    pub metadata: Map<String, Value>,
}
pub struct CaptureResult {
    pub id: String,
    pub created: bool,
}
fn required(text: &str, max: usize) -> Result<()> {
    if text.is_empty() || text.chars().count() > max {
        return Err(Error::Validation("Invalid capture identity field"));
    }
    Ok(())
}
fn metadata(value: &Map<String, Value>) -> Result<String> {
    canonical_compact_json(&Value::Object(value.clone()))
}
impl EpisodeInput {
    pub fn normalize(mut self) -> Result<Self> {
        required(&self.source, 256)?;
        required(&self.event_id, 512)?;
        required(&self.conversation_id, 512)?;
        required(&self.kind, 128)?;
        self.occurred_at = self
            .occurred_at
            .as_deref()
            .map(|value| {
                use chrono::{Datelike, SecondsFormat, Timelike};
                let value = if value.as_bytes().get(10) == Some(&b' ') {
                    format!("{}T{}", &value[..10], &value[11..])
                } else {
                    value.to_owned()
                };
                let time = chrono::DateTime::parse_from_rfc3339(&value).map_err(|_| {
                    Error::Validation("Capture time must be an aware ISO timestamp")
                })?;
                if !(1..=9999).contains(&time.year()) || time.nanosecond() >= 1_000_000_000 {
                    return Err(Error::Validation(
                        "Capture time must be an aware ISO timestamp",
                    ));
                }
                let time = time
                    .with_nanosecond(time.nanosecond() / 1000 * 1000)
                    .unwrap();
                Ok(time.to_rfc3339_opts(
                    if time.nanosecond() == 0 {
                        SecondsFormat::Secs
                    } else {
                        SecondsFormat::Micros
                    },
                    false,
                ))
            })
            .transpose()?;
        metadata(&self.metadata)?;
        Ok(self)
    }
}
struct Prior {
    id: String,
    source: String,
    event_id: String,
    conversation: String,
    kind: String,
    text: String,
    occurred: Option<String>,
    metadata: String,
    sha: Option<String>,
}
fn prior(
    tx: &WriteTxn<'_, '_>,
    input: &EpisodeInput,
    encoded: &str,
) -> Result<Option<CaptureResult>> {
    let old=tx.sql().query_row("SELECT id,source,event_id,conversation_id,kind,text,occurred_at,metadata,text_sha256 FROM episodes WHERE source=? AND event_id=?",params![input.source,input.event_id],|r|Ok(Prior{id:r.get(0)?,source:r.get(1)?,event_id:r.get(2)?,conversation:r.get(3)?,kind:r.get(4)?,text:r.get(5)?,occurred:r.get(6)?,metadata:r.get(7)?,sha:r.get(8)?})).optional()?;
    match old {
        None => Ok(None),
        Some(old) => {
            let same_text = old
                .sha
                .as_ref()
                .map(|sha| sha == &raw::sha256(input.text.as_bytes()))
                .unwrap_or(old.text == input.text);
            if !same_text
                || old.source != input.source
                || old.event_id != input.event_id
                || old.conversation != input.conversation_id
                || old.kind != input.kind
                || old.occurred != input.occurred_at
                || old.metadata != encoded
            {
                return Err(Error::Conflict(
                    "Event identifier already contains different evidence",
                ));
            }
            Ok(Some(CaptureResult {
                id: old.id,
                created: false,
            }))
        }
    }
}
fn elide(
    store: &Store,
    input: &EpisodeInput,
) -> Result<(String, Option<String>, Option<String>, Vec<BlobInfo>)> {
    if !["native_record", "native_live_record", "native_hook"].contains(&input.kind.as_str())
        || input
            .metadata
            .get("extraction_mode")
            .and_then(Value::as_str)
            != Some("skip")
    {
        return Ok((input.text.clone(), None, None, Vec::new()));
    }
    static OPAQUE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"([A-Za-z0-9+/]+)={0,2}").unwrap());
    let mut skeleton = String::new();
    let mut refs = Vec::new();
    let mut artifacts = Vec::new();
    let mut cursor = 0;
    for capture in OPAQUE.captures_iter(&input.text) {
        let run = capture.get(1).unwrap();
        if run.len() < 65536 {
            continue;
        }
        let entire = capture.get(0).unwrap();
        let info = store
            .blobs()
            .put(&mut Cursor::new(entire.as_str().as_bytes()), 1024 * 1024)?;
        skeleton.push_str(&input.text[cursor..entire.start()]);
        refs.push((
            input.text[..entire.start()].chars().count(),
            info.sha256.clone(),
            entire.len(),
        ));
        artifacts.push(info);
        cursor = entire.end();
    }
    if refs.is_empty() {
        return Ok((input.text.clone(), None, None, artifacts));
    }
    skeleton.push_str(&input.text[cursor..]);
    Ok((
        skeleton,
        Some(serde_json::to_string(&refs)?),
        Some(raw::sha256(input.text.as_bytes())),
        artifacts,
    ))
}
impl Store {
    pub fn capture(&self, input: EpisodeInput) -> Result<CaptureResult> {
        let input = input.normalize()?;
        let encoded = metadata(&input.metadata)?;
        // Reject an existing changed identity before producing any physical artifacts.
        if let Some(result) = self.write(|tx| prior(tx, &input, &encoded))? {
            return Ok(result);
        }
        let (text, runs, sha, artifacts) = elide(self, &input)?;
        let references = captured_references(&input)?;
        self.write(|tx|{if let Some(result)=prior(tx,&input,&encoded)?{return Ok(result)}for artifact in artifacts{tx.register_blob(&artifact,"application/x-memento-raw-run")?;}
            let id=uuid::Uuid::new_v4().simple().to_string();let captured=now_utc();tx.sql().execute("INSERT INTO episodes(id,source,event_id,conversation_id,kind,text,occurred_at,metadata,captured_at,text_runs,text_sha256) VALUES(?,?,?,?,?,?,?,?,?,?,?)",params![id,input.source,input.event_id,input.conversation_id,input.kind,text,input.occurred_at,encoded,captured,runs,sha])?;let seq=tx.sql().last_insert_rowid();
            record_references(tx,&id,&references)?;
            tx.sql().execute("INSERT INTO processing_progress(source,conversation_id,latest_seq,latest_at) VALUES(?,?,?,?) ON CONFLICT(source,conversation_id) DO UPDATE SET latest_seq=excluded.latest_seq,latest_at=excluded.latest_at",params![input.source,input.conversation_id,seq,captured])?;Ok(CaptureResult{id,created:true})
        })
    }
    pub fn episode(&self, id: &str) -> Result<Option<Value>> {
        self.read(|tx| tx.episode(id))
    }
    pub fn search_episodes(
        &self,
        query: Option<&str>,
        limit: usize,
        conversation: Option<&str>,
        source: Option<&str>,
        since: Option<&str>,
    ) -> Result<Vec<Value>> {
        self.read(|tx| tx.search_episodes(query, limit, conversation, source, since))
    }
}
impl ReadTxn<'_> {
    pub fn episode(&self, id: &str) -> Result<Option<Value>> {
        let row=self.sql().query_row("SELECT id,source,event_id,conversation_id,kind,text,occurred_at,metadata,captured_at FROM episodes WHERE id=?",[id],|r|Ok(json!({"id":r.get::<_,String>(0)?,"source":r.get::<_,String>(1)?,"event_id":r.get::<_,String>(2)?,"conversation_id":r.get::<_,String>(3)?,"kind":r.get::<_,String>(4)?,"text":r.get::<_,String>(5)?,"occurred_at":r.get::<_,Option<String>>(6)?,"metadata":r.get::<_,String>(7)?,"captured_at":r.get::<_,String>(8)?}))).optional()?;
        let Some(mut value) = row else {
            return Ok(None);
        };
        value["text"] = json!(
            self.original_text(id)?
                .ok_or(Error::NotFound("Episode not found"))?
        );
        value["metadata"] = serde_json::from_str(value["metadata"].as_str().unwrap())?;
        Ok(Some(value))
    }
    pub fn search_episodes(
        &self,
        query: Option<&str>,
        limit: usize,
        conversation: Option<&str>,
        source: Option<&str>,
        since: Option<&str>,
    ) -> Result<Vec<Value>> {
        if !(1..=100).contains(&limit) {
            return Err(Error::Validation("Search limit must be between 1 and 100"));
        }
        let query = query.unwrap_or("");
        if query.chars().count() > 4096 || query.contains('\0') {
            return Err(Error::Validation(
                "Search text must be at most 4096 characters without NUL",
            ));
        }
        let since = since.map(utc_time).transpose()?;
        let mut filters = String::from(
            " AND NOT EXISTS(SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id)",
        );
        let mut values = Vec::new();
        if let Some(conversation) = conversation {
            filters.push_str(" AND e.conversation_id=?");
            values.push(SqlValue::Text(conversation.into()));
        }
        if let Some(source) = source {
            filters.push_str(" AND e.source=?");
            values.push(SqlValue::Text(source.into()));
        }
        if let Some(since) = since {
            filters.push_str(" AND julianday(coalesce(e.occurred_at,e.captured_at))>=julianday(?)");
            values.push(SqlValue::Text(since));
        }
        let columns =
            "e.id,e.source,e.event_id,e.conversation_id,e.kind,e.occurred_at,e.captured_at";
        let terms = query.split_whitespace().collect::<Vec<_>>();
        let sql;
        if !terms.is_empty() {
            let matched = terms
                .into_iter()
                .map(|t| format!("\"{}\"", t.replace('"', "\"\"")))
                .collect::<Vec<_>>()
                .join(" ");
            values.insert(0, SqlValue::Text(matched.clone()));
            values.insert(0, SqlValue::Text(matched));
            values.push(SqlValue::Integer(limit as i64));
            sql = format!(
                "SELECT {columns},substr(snippet(episodes_fts,0,'','','…',32),1,512),NULL FROM episodes_fts JOIN episodes e ON e.seq=episodes_fts.rowid WHERE episodes_fts MATCH ? AND episodes_fts.rowid IN(SELECT episodes_fts.rowid FROM episodes_fts JOIN episodes e ON e.seq=episodes_fts.rowid WHERE episodes_fts MATCH ?{filters} ORDER BY bm25(episodes_fts),e.id LIMIT ?) ORDER BY bm25(episodes_fts),e.id"
            );
        } else {
            values.push(SqlValue::Integer(limit as i64));
            sql = format!(
                "SELECT {columns},substr(e.text,1,512),e.raw_frame FROM episodes e WHERE 1=1{filters} ORDER BY julianday(coalesce(e.occurred_at,e.captured_at)) DESC,e.seq DESC LIMIT ?"
            );
        }
        let mut statement = self.sql().prepare(&sql)?;
        let mut result = Vec::new();
        for row in statement.query_map(rusqlite::params_from_iter(values),|r|Ok((json!({"id":r.get::<_,String>(0)?,"source":r.get::<_,String>(1)?,"event_id":r.get::<_,String>(2)?,"conversation_id":r.get::<_,String>(3)?,"kind":r.get::<_,String>(4)?,"occurred_at":r.get::<_,Option<String>>(5)?,"captured_at":r.get::<_,String>(6)?,"excerpt":r.get::<_,String>(7)?}),r.get::<_,Option<String>>(8)?)))?{let (mut value,frame)=row?;if frame.is_some(){let id=value["id"].as_str().unwrap();let text=self.original_text(id)?.ok_or(Error::NotFound("Episode not found"))?;value["excerpt"]=json!(text.chars().take(512).collect::<String>());}result.push(value);}
        Ok(result)
    }
}
#[derive(Clone)]
pub struct Reference {
    sha: String,
    kind: Option<String>,
    filename: String,
    start: usize,
    end: usize,
    quote: String,
}
pub fn captured_references(input: &EpisodeInput) -> Result<Vec<Reference>> {
    static NATIVE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"\Anative:[^:]+:[^:]+\z").unwrap());
    if !NATIVE.is_match(&input.source)
        || !["user", "assistant", "tool"].contains(&input.kind.as_str())
    {
        return Ok(Vec::new());
    }
    let context = |key: &str| {
        input
            .metadata
            .get(key)
            .and_then(Value::as_array)
            .is_some_and(|v| {
                v.iter().any(|v| {
                    v.as_str().is_some_and(|s| {
                        [
                            "hooks.additional_context",
                            "agents_md.instructions",
                            "environments.environment_context",
                        ]
                        .contains(&s)
                    })
                })
            })
    };
    if input
        .metadata
        .get("extraction_mode")
        .and_then(Value::as_str)
        == Some("skip")
        || context("native_context_kinds")
        || context("content_item_kinds")
        || raw::legacy_native_context(
            &input.source,
            &input.kind,
            Some(&input.text),
            Some(&Value::Object(input.metadata.clone())),
        )
    {
        return Ok(Vec::new());
    }
    let mut candidates = Vec::new();
    fn walk(value: &Value, depth: usize, count: &mut usize, found: &mut Vec<Value>) {
        if *count >= 4096 {
            return;
        }
        *count += 1;
        match value {
            Value::Object(map) => {
                let typ = map
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_lowercase();
                if map.contains_key("artifact_status")
                    && [
                        "image",
                        "input_image",
                        "local_image",
                        "image_url",
                        "audio",
                        "input_audio",
                        "video",
                        "document",
                        "file",
                    ]
                    .contains(&typ.as_str())
                {
                    found.push(value.clone())
                } else if depth < 16 {
                    for child in map.values().rev() {
                        walk(child, depth + 1, count, found);
                    }
                }
            }
            Value::Array(values) if depth < 16 => {
                for child in values.iter() {
                    walk(child, depth + 1, count, found)
                }
            }
            _ => {}
        }
    }
    walk(
        input.metadata.get("artifacts").unwrap_or(&Value::Null),
        0,
        &mut 0,
        &mut candidates,
    );
    if input.text.chars().count() <= 1024 * 1024 && input.text.starts_with(['[', '{']) {
        if let Ok(value) = serde_json::from_str(&input.text) {
            walk(&value, 0, &mut 0, &mut candidates)
        }
    }
    let mut seen = std::collections::HashSet::new();
    let mut result = Vec::new();
    for stub in candidates.into_iter().take(128) {
        let Some(sha) = stub["sha256"].as_str() else {
            continue;
        };
        if sha.len() != 64
            || !sha
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            || !seen.insert(sha.to_owned())
        {
            continue;
        }
        let typ = stub["type"].as_str().unwrap().to_lowercase();
        let mime = stub["mime"]
            .as_str()
            .unwrap_or("application/octet-stream")
            .to_lowercase();
        let mut kind = match typ.as_str() {
            "image" | "input_image" | "local_image" | "image_url" => "image",
            "audio" | "input_audio" => "audio",
            "video" => "video",
            _ => "document",
        };
        if ["document", "file"].contains(&typ.as_str()) {
            if let Some(media) = mime
                .split('/')
                .next()
                .filter(|v| ["image", "audio", "video"].contains(v))
            {
                kind = media;
            }
        }
        let filename = stub["filename"]
            .as_str()
            .filter(|s| !s.is_empty())
            .and_then(|name| std::path::Path::new(name).file_name())
            .and_then(|s| s.to_str())
            .map(|s| s.chars().take(256).collect())
            .unwrap_or_else(|| {
                format!(
                    "{sha}{}",
                    match mime.as_str() {
                        "image/png" => ".png",
                        "image/jpeg" => ".jpg",
                        "audio/wav" => ".wav",
                        "video/mp4" => ".mp4",
                        "application/pdf" => ".pdf",
                        "text/plain" => ".txt",
                        // Canonical Python3.14 mimetypes suffixes for the existing
                        // supported document decoders. Keep unknown MIME unchanged.
                        "text/html" => ".html",
                        "text/markdown" => ".md",
                        "text/rtf" => ".rtf",
                        "text/csv" => ".csv",
                        "application/json" => ".json",
                        "text/xml" => ".xml",
                        "application/xml" => ".xsl",
                        "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => ".docx",
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => ".xlsx",
                        "application/vnd.openxmlformats-officedocument.presentationml.presentation" => ".pptx",
                        "application/vnd.oasis.opendocument.text" => ".odt",
                        "application/vnd.oasis.opendocument.spreadsheet" => ".ods",
                        "application/vnd.oasis.opendocument.presentation" => ".odp",
                        _ => "",
                    }
                )
            });
        let (start, quote) = if let Some(offset) = input.text.find(sha) {
            (input.text[..offset].chars().count(), sha.into())
        } else {
            (0, input.text.chars().take(256).collect::<String>())
        };
        if quote.is_empty() {
            continue;
        }
        result.push(Reference {
            sha: sha.into(),
            kind: if mime == "application/x-memento-raw-run" {
                None
            } else {
                Some(kind.into())
            },
            filename,
            start,
            end: start + quote.chars().count(),
            quote,
        });
    }
    Ok(result)
}
pub fn record_references(tx: &WriteTxn<'_, '_>, episode: &str, refs: &[Reference]) -> Result<()> {
    for reference in refs {
        let exists = tx.sql().query_row(
            "SELECT EXISTS(SELECT 1 FROM blobs WHERE sha256=?)",
            [&reference.sha],
            |r| r.get::<_, bool>(0),
        )?;
        let state = if reference.kind.is_none() {
            "unsupported"
        } else if exists {
            "ready"
        } else {
            "waiting"
        };
        tx.sql().execute("INSERT OR IGNORE INTO captured_media(sha256,kind,filename,state,error) VALUES(?,?,?,?,?)",params![reference.sha,reference.kind,reference.filename,state,if reference.kind.is_none(){Some("unsupported_type")}else{None}])?;
        let inserted=tx.sql().execute("INSERT OR IGNORE INTO captured_media_refs(sha256,episode_id,start,end,quote) VALUES(?,?,?,?,?)",params![reference.sha,episode,reference.start,reference.end,reference.quote])?;
        if exists {
            tx.sql().execute(
                "UPDATE captured_media SET state='ready' WHERE sha256=? AND state='waiting'",
                [&reference.sha],
            )?;
        }
        let active:bool=tx.sql().query_row("SELECT NOT EXISTS(SELECT 1 FROM media_files m JOIN forgotten_targets f ON f.target=m.node_id WHERE m.sha256=?1) AND EXISTS(SELECT 1 FROM captured_media_refs r JOIN episodes e ON e.id=r.episode_id WHERE r.sha256=?1 AND NOT EXISTS(SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id))",[&reference.sha],|r|r.get(0))?;
        if active {
            tx.sql().execute("UPDATE captured_media SET state=?,error=NULL WHERE sha256=? AND state='forgotten' AND error='forgotten_source' AND job_id IS NULL",params![if exists{"ready"}else{"waiting"},reference.sha])?;
            if inserted > 0 {
                tx.sql().execute("UPDATE jobs SET state='pending',available_at=?,error=NULL WHERE id=(SELECT job_id FROM captured_media WHERE sha256=?) AND state='failed' AND error='CapturedSourceUnavailable'",params![chrono::Utc::now().timestamp_millis() as f64/1000.,reference.sha])?;
            }
        }
        tx.touch_media(&reference.sha)?;
    }
    Ok(())
}
