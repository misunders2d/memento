//! Existing parked-raw citation admission, deterministic and transactional.
use crate::util::*;
use memento_core::{Error, Result, WriteTxn};
use rusqlite::params;
use serde_json::{Value, json};

pub fn legacy_native_context(source: &str, kind: &str, text: &str, metadata: &Value) -> bool {
    memento_core::raw::legacy_native_context(source, kind, Some(text), Some(metadata))
}

pub fn source_text_ranges(text: &str) -> Vec<(usize, usize)> {
    let full = vec![(0, text.chars().count())];
    if !text.trim_start().starts_with(['[', '{']) {
        return full;
    }
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return full;
    };
    let mut opaque = std::collections::HashSet::new();
    fn visit(value: &Value, media: bool, opaque: &mut std::collections::HashSet<String>) {
        match value {
            Value::Array(items) => {
                for item in items {
                    visit(item, media, opaque)
                }
            }
            Value::Object(map) => {
                let media = media
                    || map.get("type").and_then(Value::as_str).is_some_and(|kind| {
                        [
                            "image",
                            "image_url",
                            "input_image",
                            "audio",
                            "input_audio",
                            "file",
                            "document",
                        ]
                        .contains(&kind)
                    });
                for (key, child) in map {
                    if media
                        && [
                            "data",
                            "base64",
                            "b64_json",
                            "file_data",
                            "url",
                            "image_url",
                        ]
                        .contains(&key.as_str())
                        && child.as_str().is_some_and(|text| {
                            text.chars().count() > 256
                                && (text.starts_with("data:")
                                    || !["url", "image_url"].contains(&key.as_str()))
                        })
                    {
                        opaque.insert(child.as_str().unwrap().into());
                    } else {
                        visit(child, media, opaque)
                    }
                }
            }
            _ => {}
        }
    }
    visit(&value, false, &mut opaque);
    if opaque.is_empty() {
        return full;
    }
    let bytes = text.as_bytes();
    let mut cursor = 0;
    let mut start = 0;
    let mut ranges = vec![];
    while cursor < bytes.len() {
        if bytes[cursor] != b'"' {
            cursor += 1;
            continue;
        }
        let left = cursor;
        cursor += 1;
        while cursor < bytes.len() {
            if bytes[cursor] == b'\\' {
                cursor += 2;
                continue;
            }
            if bytes[cursor] == b'"' {
                cursor += 1;
                break;
            }
            cursor += 1;
        }
        let Some(slice) = text.get(left..cursor) else {
            return full;
        };
        if serde_json::from_str::<String>(slice)
            .ok()
            .is_some_and(|decoded| opaque.contains(&decoded))
        {
            if start < left {
                ranges.push((text[..start].chars().count(), text[..left].chars().count()));
            }
            start = cursor;
        }
    }
    if start < text.len() {
        ranges.push((text[..start].chars().count(), text.chars().count()));
    }
    ranges
}

/// `(job_id, skipped)`; None job is the existing skipped/no-demand outcome.
pub fn demand_extract(
    tx: &WriteTxn,
    episode: &Value,
    start: Option<i64>,
    end: Option<i64>,
) -> Result<(Option<String>, bool)> {
    let source = get_str(episode, "source")?;
    let conversation = get_str(episode, "conversation_id")?;
    let id = get_str(episode, "id")?;
    let row = one(
        tx.sql(),
        "SELECT p.seq AS cursor,h.parked_through FROM historical_parking h JOIN processing_progress p ON p.source=h.source AND p.conversation_id=h.conversation_id WHERE h.source=? AND h.conversation_id=?",
        params![source, conversation],
    )?;
    let Some(row) = row else {
        return Ok((None, true));
    };
    let seq = get_i64(episode, "seq")?;
    if !(get_i64(&row, "cursor")? < seq && seq <= get_i64(&row, "parked_through")?) {
        return Ok((None, true));
    }
    let metadata = if let Some(text) = episode["metadata"].as_str() {
        decode_json(text)?
    } else {
        episode["metadata"].clone()
    };
    let kind = get_str(episode, "kind")?;
    if metadata["extraction_mode"] == "skip"
        || [
            "developer",
            "native_record",
            "native_live_record",
            "native_hook",
        ]
        .contains(&kind)
    {
        return Ok((None, true));
    }
    let text = tx.original_text(id)?.ok_or(Error::Unavailable(
        "Original raw source artifact is unavailable or corrupt",
    ))?;
    if legacy_native_context(source, kind, &text, &metadata) {
        return Ok((None, true));
    }
    let (start, end) = if let (Some(start), Some(end)) = (start, end) {
        (start.max(0) as usize, end.max(0) as usize)
    } else {
        if text.chars().count() > 12000 {
            return Ok((None, true));
        }
        (0, text.chars().count())
    };
    let tool = metadata["extraction_mode"] == "tool_output" || kind == "tool";
    let mut windows = vec![];
    for (left, right) in source_text_ranges(&text) {
        let left = left.max(start);
        let right = right.min(end);
        if left >= right {
            continue;
        }
        let mut at = left;
        loop {
            let stop = if tool { right } else { right.min(at + 12000) };
            windows.push(json!({"id":format!("{id}:{at}"),"episode_id":id,"start":at,"end":stop}));
            if stop >= right || windows.len() == 64 {
                break;
            }
            at = left.max(stop.saturating_sub(1000));
        }
    }
    if windows.is_empty() {
        return Ok((None, true));
    }
    let payload = json!({"version":"extract-v2","mode":"demand","source":source,"conversation_id":conversation,"episode_id":id,"start":start,"end":end,"windows":windows});
    // Legacy job key uses Python ensure_ascii=True, unlike graph commands.
    let canonical = encode_json(&payload)?;
    let mut ascii = String::new();
    for letter in canonical.chars() {
        if letter.is_ascii() {
            ascii.push(letter);
        } else {
            let mut buffer = [0; 2];
            for code in letter.encode_utf16(&mut buffer) {
                ascii.push_str(&format!("\\u{code:04x}"));
            }
        }
    }
    let key = format!(
        "extract/demand/{}",
        memento_core::raw::sha256(ascii.as_bytes())
    );
    if let Some(prior) = one(
        tx.sql(),
        "SELECT id,kind,payload FROM jobs WHERE job_key=?",
        [&key],
    )? {
        if prior["kind"] != "extract" || decode_json(get_str(&prior, "payload")?)? != payload {
            return Err(Error::Conflict(
                "Job identifier already has different input",
            ));
        }
        return Ok((Some(get_str(&prior, "id")?.into()), false));
    }
    let job = crate::util::id();
    let available = chrono::Utc::now().timestamp_micros() as f64 / 1e6;
    tx.sql().execute("INSERT INTO jobs(id,job_key,kind,payload,state,available_at,attempts) VALUES (?,?,?,?,'pending',?,0)",params![job,key,"extract",encode_json(&payload)?,available])?;
    Ok((Some(job), false))
}
