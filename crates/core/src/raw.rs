//! Local original-source hydration; never performs network/model work under a transaction.
use crate::{CharSpan, Error, Result};
use flate2::{Decompress, FlushDecompress, Status};
use regex::Regex;
use rusqlite::{Connection, OptionalExtension, functions::FunctionFlags};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    io::Read,
    path::Path,
    sync::{Arc, LazyLock, Mutex},
};
static NATIVE_SOURCE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\Anative:[^:]+:codex\z").unwrap());
static LEGACY_OPENING: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\A# AGENTS\.md instructions(?: for [^\r\n]+)?\r?\n[ \t\r\n]*<INSTRUCTIONS>")
        .unwrap()
});
pub(crate) type FramePermits = Arc<Mutex<HashMap<i64, (String, String)>>>;
pub fn sha256(data: &[u8]) -> String {
    Sha256::digest(data)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn unavailable() -> Error {
    Error::Unavailable("Original raw source artifact is unavailable or corrupt")
}
fn blob(data_dir: &Path, sha: &str, length: usize) -> Result<Vec<u8>> {
    if sha.len() != 64
        || !sha
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(unavailable());
    }
    let path = data_dir.join("blobs").join(&sha[..2]).join(&sha[2..]);
    let file = std::fs::File::open(path).map_err(|_| unavailable())?;
    let mut value = Vec::new();
    file.take(length.checked_add(1).ok_or_else(unavailable)? as u64)
        .read_to_end(&mut value)
        .map_err(|_| unavailable())?;
    if value.len() != length || sha256(&value) != sha {
        return Err(unavailable());
    }
    Ok(value)
}
fn decode_frame(data: &[u8], size: usize, sha: &str) -> Result<Vec<u8>> {
    if size > 262144 || data.len() > 263168 {
        return Err(unavailable());
    }
    let mut out = Vec::with_capacity(size + 1);
    let mut decoder = Decompress::new(true);
    let status = decoder
        .decompress_vec(data, &mut out, FlushDecompress::Finish)
        .map_err(|_| unavailable())?;
    if status != Status::StreamEnd
        || decoder.total_in() != data.len() as u64
        || out.len() != size
        || sha256(&out) != sha
    {
        return Err(unavailable());
    }
    Ok(out)
}
/// Validate every physical frame, including retained frames no longer referenced by a row.
/// Offline restore/import uses this before publication; bounded one-frame memory.
pub fn validate_frames(db: &Connection) -> Result<()> {
    let mut query = db.prepare("SELECT raw_size,sha256,data FROM raw_frames ORDER BY id")?;
    let mut rows = query.query([])?;
    while let Some(row) = rows.next()? {
        let size: usize = row.get(0)?;
        let sha: String = row.get(1)?;
        let data: Vec<u8> = row.get(2)?;
        decode_frame(&data, size, &sha)?;
    }
    Ok(())
}
pub fn read_original(db: &Connection, data_dir: &Path, id: &str) -> Result<Option<String>> {
    let row=db.query_row("SELECT text,text_runs,raw_frame,text_sha256,source,conversation_id FROM episodes WHERE id=?",[id],|r|Ok((r.get::<_,String>(0)?,r.get::<_,Option<String>>(1)?,r.get::<_,Option<String>>(2)?,r.get::<_,Option<String>>(3)?,r.get::<_,String>(4)?,r.get::<_,String>(5)?))).optional()?;
    let Some((text, runs, frames, checksum, source, conversation)) = row else {
        return Ok(None);
    };
    let mut bytes = Vec::new();
    if let Some(frames) = frames {
        if frames.len() > 1024 * 1024 {
            return Err(unavailable());
        }
        let refs: Vec<[i64; 3]> = serde_json::from_str(&frames).map_err(|_| unavailable())?;
        if refs.is_empty() {
            return Err(unavailable());
        }
        for [identity, offset, length] in refs {
            if identity <= 0
                || offset < 0
                || length < 0
                || offset.checked_add(length).is_none_or(|v| v > 262144)
            {
                return Err(unavailable());
            }
            let frame = db
                .query_row(
                    "SELECT source,conversation_id,raw_size,sha256,data FROM raw_frames WHERE id=?",
                    [identity],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, usize>(2)?,
                            r.get::<_, String>(3)?,
                            r.get::<_, Vec<u8>>(4)?,
                        ))
                    },
                )
                .optional()?
                .ok_or_else(unavailable)?;
            if frame.0 != source || frame.1 != conversation {
                return Err(unavailable());
            }
            let decoded = decode_frame(&frame.4, frame.2, &frame.3)?;
            bytes.extend_from_slice(
                decoded
                    .get(offset as usize..(offset + length) as usize)
                    .ok_or_else(unavailable)?,
            );
        }
    } else if let Some(runs) = runs {
        let refs: Vec<(usize, String, usize)> =
            serde_json::from_str(&runs).map_err(|_| unavailable())?;
        let mut cursor = 0usize;
        let mut original = 0usize;
        for (offset, sha, length) in refs {
            let count = offset.checked_sub(original).ok_or_else(unavailable)?;
            let end = cursor.checked_add(count).ok_or_else(unavailable)?;
            bytes.extend_from_slice(CharSpan { start: cursor, end }.slice(&text)?.as_bytes());
            let payload = blob(data_dir, &sha, length)?;
            if !payload.is_ascii() {
                return Err(unavailable());
            }
            bytes.extend_from_slice(&payload);
            cursor = end;
            original = offset.checked_add(length).ok_or_else(unavailable)?;
        }
        bytes.extend_from_slice(
            CharSpan {
                start: cursor,
                end: text.chars().count(),
            }
            .slice(&text)?
            .as_bytes(),
        );
    } else {
        return Ok(Some(text));
    }
    if checksum.as_deref() != Some(sha256(&bytes).as_str()) {
        return Err(unavailable());
    }
    Ok(Some(String::from_utf8(bytes).map_err(|_| unavailable())?))
}
pub fn legacy_native_context(
    source: &str,
    kind: &str,
    text: Option<&str>,
    metadata: Option<&Value>,
) -> bool {
    if !matches!(kind, "user" | "developer") || !NATIVE_SOURCE.is_match(source) {
        return false;
    }
    if kind == "developer" {
        return true;
    }
    let (Some(text), Some(Value::Object(fields))) = (text, metadata) else {
        return false;
    };
    if fields.contains_key("native_context_kinds")
        || fields.contains_key("content_item_kinds")
        || fields
            .get("extraction_mode")
            .is_some_and(|mode| mode.as_str() != Some("message"))
    {
        return false;
    }
    if text.starts_with("<codex_internal_context source=\"goal\">") {
        return text.trim_end().ends_with("</codex_internal_context>")
            && text.matches("<codex_internal_context").count() == 1
            && text.matches("</codex_internal_context>").count() == 1;
    }
    let Some(found) = LEGACY_OPENING.find(text) else {
        return false;
    };
    if text.matches("<INSTRUCTIONS>").count() != 1 || text.matches("</INSTRUCTIONS>").count() != 1 {
        return false;
    }
    let Some((_, suffix)) = text[found.end()..].split_once("</INSTRUCTIONS>") else {
        return false;
    };
    let suffix = suffix.trim();
    suffix.is_empty()
        || (suffix.starts_with("<environment_context>")
            && suffix.ends_with("</environment_context>")
            && suffix.matches("<environment_context>").count() == 1
            && suffix.matches("</environment_context>").count() == 1)
}
pub(crate) fn register(db: &Connection, permits: FramePermits) -> Result<()> {
    db.create_scalar_function(
        "raw_frame_write",
        3,
        FunctionFlags::SQLITE_UTF8,
        move |ctx| {
            let seq = ctx.get::<i64>(0)?;
            let refs = ctx.get::<Option<String>>(1)?;
            let sha = ctx.get::<Option<String>>(2)?;
            Ok(match (refs, sha) {
                (Some(refs), Some(sha)) => permits
                    .lock()
                    .map(|p| p.get(&seq) == Some(&(refs, sha)))
                    .unwrap_or(false),
                _ => false,
            })
        },
    )?;
    db.create_scalar_function(
        "legacy_native_context",
        4,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let source = ctx.get::<Option<String>>(0)?;
            let kind = ctx.get::<Option<String>>(1)?;
            let text = ctx.get::<Option<String>>(2)?;
            let metadata = ctx
                .get::<Option<String>>(3)?
                .and_then(|v| serde_json::from_str::<Value>(&v).ok());
            Ok(match (source, kind) {
                (Some(source), Some(kind)) => {
                    legacy_native_context(&source, &kind, text.as_deref(), metadata.as_ref())
                }
                _ => false,
            })
        },
    )?;
    Ok(())
}
