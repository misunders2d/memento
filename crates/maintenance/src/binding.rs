use crate::fs_safe;
use memento_core::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PublicBinding {
    pub client_id: String,
    pub account: String,
    pub folder_id: String,
}
pub fn object_id(value: &str) -> Result<&str> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
    {
        return Err(Error::Validation("Invalid Drive object id"));
    }
    Ok(value)
}
impl PublicBinding {
    pub fn validate(&self) -> Result<()> {
        for (text, max) in [(&self.client_id, 4096), (&self.account, 320)] {
            if text.is_empty()
                || text != text.trim()
                || text.chars().count() > max
                || text.chars().any(|c| (c as u32) < 32)
            {
                return Err(Error::Validation("Invalid public Drive binding"));
            }
        }
        object_id(&self.folder_id)?;
        Ok(())
    }
    pub fn matches(&self, other: &Self) -> bool {
        self.client_id == other.client_id
            && self.folder_id == other.folder_id
            && memento_domain::unicode::casefold(&self.account)
                == memento_domain::unicode::casefold(&other.account)
    }
}
pub fn read_binding(data: &Path) -> Result<Option<PublicBinding>> {
    let path = data.join("drive-binding.json");
    if !path.try_exists()? && std::fs::symlink_metadata(&path).is_err() {
        return Ok(None);
    }
    let value: PublicBinding = serde_json::from_slice(&fs_safe::read_private(&path, 65536)?)?;
    value.validate()?;
    Ok(Some(value))
}
pub fn check_binding(data: &Path, actual: &PublicBinding) -> Result<Option<PublicBinding>> {
    actual.validate()?;
    let expected = read_binding(data)?;
    if expected
        .as_ref()
        .is_some_and(|expected| !expected.matches(actual))
    {
        return Err(Error::Conflict(
            "Drive binding differs from the restored instance",
        ));
    }
    Ok(expected)
}
/// Export only the public identity, never serialized credentials. Bound bootstrap
/// and installed grant must agree when both are explicitly present.
pub fn export_binding(data: &Path) -> Result<PublicBinding> {
    if ["drive-client.json", "drive-oauth.json"]
        .iter()
        .any(|name| std::fs::symlink_metadata(data.join(name)).is_ok())
    {
        // Full original-client comparison includes the private Desktop secret;
        // the only value returned or packed remains the public binding.
        return Ok(crate::drive_auth::load_bindings(data)?.public);
    }
    read_binding(data)?.ok_or(Error::Unavailable(
        "Remote originals require the original public Drive binding",
    ))
}
