//! Strict reference-only media command; original bytes arrive through /blobs.
use memento_core::{Error, Result};
use memento_domain::{dto::Evidence, validate_common};
use serde::{Deserialize, Serialize};
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MediaInput {
    pub command_id: String,
    pub actor: String,
    pub why: String,
    pub evidence: Vec<Evidence>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub conversation_id: Option<String>,
    pub sha256: String,
    pub kind: String,
    pub filename: String,
    pub scope: Vec<String>,
}
impl MediaInput {
    pub fn checked(self) -> Result<serde_json::Value> {
        validate_common(
            &self.command_id,
            &self.actor,
            &self.why,
            &self.evidence,
            self.source.as_deref(),
            self.conversation_id.as_deref(),
        )?;
        if self.sha256.len() != 64
            || !self
                .sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(Error::Validation("Expected a lowercase SHA-256 digest"));
        }
        if !["image", "audio", "video", "document"].contains(&self.kind.as_str())
            || self.filename.is_empty()
            || self.filename.chars().count() > 256
            || self.scope.len() != 1
            || !["personal", "professional"].contains(&self.scope[0].as_str())
        {
            return Err(Error::Validation("Invalid media kind, filename or scope"));
        }
        Ok(serde_json::to_value(self)?)
    }
}
