use crate::{Error, Result};
use std::path::Path;
use tokenizers::{PostProcessor, Tokenizer, TruncationDirection};

pub const QUERY_PREFIX: &str = "task: search result | query: ";
pub const MAX_SEQUENCE_LENGTH: usize = 8192;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextChunk {
    /// Zero-based Unicode character offsets, never UTF-8 byte offsets.
    pub start: usize,
    pub end: usize,
    pub text: String,
}

#[derive(Debug, Clone)]
pub struct TextInput {
    pub input_ids: Vec<i64>,
    pub attention_mask: Vec<i64>,
}

pub(crate) struct TextProcessor {
    tokenizer: Tokenizer,
    special_tokens: usize,
}

impl TextProcessor {
    pub fn load(path: &Path) -> Result<Self> {
        let mut tokenizer =
            Tokenizer::from_file(path).map_err(|e| Error::Tokenizer(e.to_string()))?;
        // HF split_special_tokens=True: user spellings stay literal text.
        tokenizer.set_encode_special_tokens(true);
        tokenizer.with_padding(None);
        tokenizer
            .with_truncation(None)
            .map_err(|e| Error::Tokenizer(e.to_string()))?;
        let special_tokens = tokenizer
            .get_post_processor()
            .map(|p| p.added_tokens(false))
            .unwrap_or(0);
        if special_tokens != 2 {
            return Err(Error::InvalidAssets(
                "pinned BOS/EOS template changed".into(),
            ));
        }
        Ok(Self {
            tokenizer,
            special_tokens,
        })
    }

    pub fn encode(&self, text: &str) -> Result<TextInput> {
        // Truncate the body before the immutable BOS/EOS postprocessor, as the
        // tokenizer's built-in truncation does. This shares one vocabulary
        // between short encodes and untruncated character-span chunking without
        // mutable per-call settings, a global lock or a deep BPE clone.
        let mut body = self
            .tokenizer
            .encode(text, false)
            .map_err(|e| Error::Tokenizer(e.to_string()))?;
        body.truncate(
            MAX_SEQUENCE_LENGTH - self.special_tokens,
            0,
            TruncationDirection::Right,
        );
        let encoded = self
            .tokenizer
            .post_process(body, None, true)
            .map_err(|e| Error::Tokenizer(e.to_string()))?;
        Ok(TextInput {
            input_ids: encoded.get_ids().iter().map(|x| *x as i64).collect(),
            attention_mask: encoded
                .get_attention_mask()
                .iter()
                .map(|x| *x as i64)
                .collect(),
        })
    }

    pub fn chunks(&self, text: &str, size: usize, overlap: usize) -> Result<Vec<TextChunk>> {
        if size == 0 || overlap >= size {
            return Err(Error::InvalidInput(
                "chunk size must exceed its nonnegative overlap",
            ));
        }
        if text.chars().all(python_whitespace) {
            return Ok(Vec::new());
        }
        let encoded = self
            .tokenizer
            .encode_char_offsets(text, false)
            .map_err(|e| Error::Tokenizer(e.to_string()))?;
        let offsets = encoded.get_offsets();
        let boundaries: Vec<usize> = text
            .char_indices()
            .map(|(i, _)| i)
            .chain(std::iter::once(text.len()))
            .collect();
        let length = boundaries.len() - 1;
        if offsets.is_empty() {
            return Ok(vec![TextChunk {
                start: 0,
                end: length,
                text: text.into(),
            }]);
        }
        let mut result = Vec::new();
        let mut start = 0;
        while start < offsets.len() {
            let mut end = (start + size).min(offsets.len());
            while end < offsets.len() && end > start && offsets[end].0 < offsets[end - 1].1 {
                end -= 1;
            }
            if end == start {
                return Err(Error::InvalidInput(
                    "one Unicode character exceeds the passage token budget",
                ));
            }
            let mut left = if start > 0 { offsets[start].0 } else { 0 };
            let mut right = if end < offsets.len() {
                offsets[end].0
            } else {
                length
            };
            if left > right || right > length {
                return Err(Error::Tokenizer("invalid character offsets".into()));
            }
            while left < right
                && text[boundaries[left]..boundaries[right]]
                    .chars()
                    .next()
                    .is_some_and(python_whitespace)
            {
                left += 1;
            }
            while right > left
                && text[boundaries[left]..boundaries[right]]
                    .chars()
                    .next_back()
                    .is_some_and(python_whitespace)
            {
                right -= 1;
            }
            if left < right {
                result.push(TextChunk {
                    start: left,
                    end: right,
                    text: text[boundaries[left]..boundaries[right]].into(),
                });
            }
            if end == offsets.len() {
                break;
            }
            let mut next = (start + 1).max(end.saturating_sub(overlap));
            while next < end && offsets[next].0 < offsets[next - 1].1 {
                next += 1;
            }
            start = next;
        }
        Ok(result)
    }
}

fn python_whitespace(c: char) -> bool {
    c.is_whitespace() || ('\u{001c}'..='\u{001f}').contains(&c)
}

pub fn document_prompt(text: &str, title: Option<&str>) -> String {
    format!(
        "title: {} | text: {}",
        title.filter(|s| !s.is_empty()).unwrap_or("none"),
        text
    )
}

pub(crate) fn media_ids(begin: i64, placeholder: i64, end: i64, tokens: usize) -> TextInput {
    let mut input_ids = Vec::with_capacity(tokens + 4);
    input_ids.extend([2, begin]);
    input_ids.extend(std::iter::repeat_n(placeholder, tokens));
    input_ids.extend([end, 1]);
    TextInput {
        attention_mask: vec![1; input_ids.len()],
        input_ids,
    }
}
