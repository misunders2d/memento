//! Strict agent wire contracts. MCP's omission sentinels belong to its adapter.
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Evidence {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub episode_id: Option<String>,
    pub quote: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub occurrence: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Remember {
    pub command_id: String,
    pub actor: String,
    pub why: String,
    pub evidence: Vec<Evidence>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub conversation_id: Option<String>,
    #[serde(rename = "type")]
    pub kind: String,
    pub text: String,
    pub scope: Vec<String>,
    #[serde(default)]
    pub valid_from: Option<String>,
    #[serde(default)]
    pub valid_to: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Link {
    pub command_id: String,
    pub actor: String,
    pub why: String,
    pub evidence: Vec<Evidence>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub conversation_id: Option<String>,
    pub a: String,
    pub rel: String,
    pub b: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Replacement {
    #[serde(rename = "type")]
    pub kind: String,
    pub text: String,
    pub scope: Vec<String>,
}

fn correction() -> String {
    "correction".into()
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Supersede {
    pub command_id: String,
    pub actor: String,
    pub why: String,
    pub evidence: Vec<Evidence>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub conversation_id: Option<String>,
    pub old: String,
    pub new: Replacement,
    #[serde(default = "correction")]
    pub kind: String,
    #[serde(default)]
    pub valid_at: Option<String>,
}

fn seven() -> f64 {
    7.0
}
fn twenty() -> usize {
    20
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Loop {
    pub action: String,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default = "seven")]
    pub days: f64,
    #[serde(default)]
    pub due_at: Option<String>,
    #[serde(default)]
    pub due_before: Option<String>,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default = "twenty")]
    pub limit: usize,
    #[serde(default)]
    pub command_id: Option<String>,
    #[serde(default)]
    pub actor: Option<String>,
    #[serde(default)]
    pub why: Option<String>,
    #[serde(default)]
    pub responsibility: Option<String>,
    #[serde(default)]
    pub evidence: Vec<Evidence>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Citation {
    pub event_id: String,
    pub ids: Vec<String>,
    #[serde(default)]
    pub query: Option<String>,
    #[serde(default)]
    pub shown: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Review {
    pub command_id: String,
    pub actor: String,
    pub why: String,
    pub proposal_id: String,
    pub decision: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Forget {
    pub command_id: String,
    pub actor: String,
    pub why: String,
    pub target: String,
    pub token: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Merge {
    pub command_id: String,
    pub actor: String,
    pub why: String,
    pub cluster: String,
    pub into: String,
    #[serde(default)]
    pub exclude: Vec<String>,
    pub token: String,
    #[serde(default)]
    pub include: Vec<String>,
}
