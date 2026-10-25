use crate::{Error, Result};
use chrono::{DateTime, Datelike, SecondsFormat, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
pub const FORMAT_VERSION: i64 = 5;
pub const EMBED_DIM: usize = 768;
pub const EMBED_BYTES: usize = EMBED_DIM * 4;
pub const MODEL_ID: &str = "google/embeddinggemma-2";
pub const MODEL_REVISION: &str = "914f7f89142e33e77833254d9c9b90c3cef7303b";
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    Personal,
    Professional,
}
impl Scope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Personal => "personal",
            Self::Professional => "professional",
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Vec<Scope>", into = "Vec<Scope>")]
pub struct ScopeSet(Vec<Scope>);
impl ScopeSet {
    pub fn new(mut values: Vec<Scope>) -> Result<Self> {
        if values.is_empty() || values.len() > 2 {
            return Err(Error::Validation(
                "scope requires distinct personal/professional labels",
            ));
        }
        values.sort();
        if values.windows(2).any(|v| v[0] == v[1]) {
            return Err(Error::Validation(
                "scope requires distinct personal/professional labels",
            ));
        }
        Ok(Self(values))
    }
    pub fn as_slice(&self) -> &[Scope] {
        &self.0
    }
    pub fn contains(&self, scope: Scope) -> bool {
        self.0.contains(&scope)
    }
}
impl TryFrom<Vec<Scope>> for ScopeSet {
    type Error = Error;
    fn try_from(v: Vec<Scope>) -> Result<Self> {
        Self::new(v)
    }
}
impl From<ScopeSet> for Vec<Scope> {
    fn from(v: ScopeSet) -> Self {
        v.0
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Hot,
    Cold,
}
impl Tier {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hot => "hot",
            Self::Cold => "cold",
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VectorKind {
    Node,
    Candidate,
    Episode,
    Segment,
}
impl VectorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Candidate => "candidate",
            Self::Episode => "episode",
            Self::Segment => "segment",
        }
    }
}
#[derive(Clone, Eq, PartialEq)]
pub struct F32Blob(Vec<u8>);
impl F32Blob {
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        if bytes.len() != EMBED_BYTES {
            return Err(Error::Validation(
                "Embedding blob must contain exactly 768 Float32 values",
            ));
        }
        Ok(Self(bytes))
    }
    pub fn from_values(values: &[f32]) -> Result<Self> {
        if values.len() != EMBED_DIM {
            return Err(Error::Validation("Embedding dimensions differ"));
        }
        Self::from_bytes(values.iter().flat_map(|v| v.to_le_bytes()).collect())
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
    pub fn values(&self) -> Vec<f32> {
        self.0
            .chunks_exact(4)
            .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
            .collect()
    }
}
impl fmt::Debug for F32Blob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("F32Blob")
            .field("bytes", &self.0.len())
            .finish()
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ModelSignature {
    pub model_id: String,
    pub revision: String,
    pub dimensions: usize,
    pub text_encoding: i64,
}
impl ModelSignature {
    pub fn pinned() -> Self {
        Self {
            model_id: MODEL_ID.into(),
            revision: MODEL_REVISION.into(),
            dimensions: EMBED_DIM,
            text_encoding: 1,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Evidence {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub episode_id: Option<String>,
    pub quote: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub occurrence: Option<usize>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CharSpan {
    pub start: usize,
    pub end: usize,
}
impl CharSpan {
    pub fn slice(self, text: &str) -> Result<String> {
        if self.start > self.end || self.end > text.chars().count() {
            return Err(Error::Validation("Evidence span is outside source"));
        }
        Ok(text
            .chars()
            .skip(self.start)
            .take(self.end - self.start)
            .collect())
    }
}
pub fn source_span(text: &str, quote: &str, occurrence: Option<usize>) -> Result<CharSpan> {
    if quote.is_empty() {
        return Err(Error::Validation(
            "Evidence quote must be a nonempty string",
        ));
    }
    let found = text
        .char_indices()
        .enumerate()
        .filter_map(|(character, (byte, _))| text[byte..].starts_with(quote).then_some(character))
        .collect::<Vec<_>>();
    let which = match occurrence {
        Some(n) => n,
        None if found.len() == 1 => 0,
        None => {
            return Err(Error::Validation(
                "Evidence must match exactly and unambiguously",
            ));
        }
    };
    let start = *found
        .get(which)
        .ok_or(Error::Validation("Invalid evidence occurrence"))?;
    Ok(CharSpan {
        start,
        end: start + quote.chars().count(),
    })
}
pub fn utc_time(value: &str) -> Result<String> {
    let input = if value.as_bytes().get(10) == Some(&b' ') {
        format!("{}T{}", &value[..10], &value[11..])
    } else {
        value.into()
    };
    let time = DateTime::parse_from_rfc3339(&input)
        .map_err(|_| Error::Validation("World time must be an aware ISO timestamp"))?;
    if !(1..=9999).contains(&time.year()) || time.nanosecond() >= 1_000_000_000 {
        return Err(Error::Validation(
            "World time must be an aware ISO timestamp",
        ));
    }
    let time = time
        .with_nanosecond(time.nanosecond() / 1000 * 1000)
        .unwrap()
        .with_timezone(&Utc);
    Ok(time.to_rfc3339_opts(
        if time.nanosecond() == 0 {
            SecondsFormat::Secs
        } else {
            SecondsFormat::Micros
        },
        false,
    ))
}
pub fn now_utc() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Micros, false)
}
#[derive(Clone, Debug)]
pub struct CommandContext {
    pub command_id: String,
    pub actor: String,
    pub why: String,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventFence {
    pub id: i64,
    pub nonce: String,
    pub generation: i64,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceCut {
    pub db_uuid: String,
    pub generation: i64,
}
#[derive(Clone, Debug)]
pub struct DirtyEvent {
    pub fence: EventFence,
    pub vector_seq: i64,
    pub tier: Tier,
}
#[derive(Clone, Debug)]
pub struct VectorWrite {
    pub seq: i64,
    pub target: String,
    pub kind: VectorKind,
    pub version: i64,
    pub current: bool,
    pub tier: Tier,
    pub embedding: F32Blob,
}
