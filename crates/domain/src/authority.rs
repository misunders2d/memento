//! Source-role/span authority. Instructions in evidence never authorize changes.
use crate::demand::legacy_native_context;
use crate::util::substring;
use regex::Regex;
use serde_json::Value;
use std::sync::LazyLock;

pub fn behavior_ground_truth(episode: &Value, start: Option<usize>, end: Option<usize>) -> bool {
    let metadata = match &episode["metadata"] {
        Value::String(value) => serde_json::from_str(value).unwrap_or(Value::Null),
        other => other.clone(),
    };
    let text = episode["text"].as_str().unwrap_or("");
    let source = episode["source"].as_str().unwrap_or("");
    let kind = episode["kind"].as_str().unwrap_or("");
    if legacy_native_context(source, kind, text, &metadata) {
        return false;
    }
    static PASTED: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?i)<(/?)pasted_content\b[^>]*>").unwrap());
    static PEER: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?i)\[\s*(?:HERDR\s+)?AGENT MESSAGE\s*\]|Message Type:\s*(?:NEW_TASK|MESSAGE|FINAL_ANSWER)|<(?:system-reminder|collaboration[^>]*|tool_result)\b").unwrap()
    });
    static FENCE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?m)^\s*(?:```|~~~)").unwrap());
    let mut ranges = vec![];
    let mut depth = 0usize;
    let mut opened = 0;
    for tag in PASTED.captures_iter(text) {
        let full = tag.get(0).unwrap();
        if tag.get(1).unwrap().as_str().is_empty() {
            if depth == 0 {
                opened = text[..full.start()].chars().count();
            }
            depth += 1;
        } else if depth > 0 {
            depth -= 1;
            if depth == 0 {
                ranges.push((opened, text[..full.end()].chars().count()));
            }
        }
    }
    let length = text.chars().count();
    if depth > 0 {
        ranges.push((opened, length));
    }
    let mut own = String::new();
    let mut cursor = 0;
    for (left, right) in &ranges {
        own.push_str(&substring(text, cursor, *left));
        cursor = *right;
    }
    own.push_str(&substring(text, cursor, length));
    if kind != "user"
        || metadata["evidence_eligible"] == false
        || metadata
            .get("extraction_mode")
            .is_some_and(|value| value != "message")
        || own.trim().is_empty()
        || PEER.is_match(&own)
    {
        return false;
    }
    let Some(start) = start else {
        return true;
    };
    let end = end.unwrap_or(length);
    if start > end
        || end > length
        || ranges
            .iter()
            .any(|(left, right)| start < *right && end > *left)
    {
        return false;
    }
    let mut prefix = substring(text, 0, start);
    for (left, right) in ranges.iter().rev() {
        if *left < start {
            prefix = format!(
                "{}{}",
                substring(&prefix, 0, *left),
                substring(&prefix, (*right).min(start), prefix.chars().count())
            );
        }
    }
    if FENCE.find_iter(&prefix).count() % 2 != 0 {
        return false;
    }
    let original_prefix = substring(text, 0, start);
    let line = original_prefix.rsplit('\n').next().unwrap_or("");
    if line.trim_start().starts_with('>') {
        return false;
    }
    let quote = substring(text, start, end);
    let quote = quote.trim();
    !["```", "~~~", ">"]
        .iter()
        .any(|prefix| quote.starts_with(prefix))
}
