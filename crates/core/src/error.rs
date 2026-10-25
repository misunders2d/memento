use std::{error::Error as StdError, fmt};
pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug)]
pub enum Error {
    Validation(&'static str),
    Conflict(&'static str),
    NotFound(&'static str),
    Unavailable(&'static str),
    UnsupportedHeader(i64),
    Storage(rusqlite::Error),
    Io(std::io::Error),
    Json(serde_json::Error),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Validation(s) | Self::Conflict(s) | Self::NotFound(s) | Self::Unavailable(s) => {
                f.write_str(s)
            }
            Self::UnsupportedHeader(v) => write!(f, "Unsupported database schema version: {v}"),
            Self::Storage(_) | Self::Io(_) => f.write_str("Evidence storage is unavailable"),
            Self::Json(_) => f.write_str("Invalid JSON"),
        }
    }
}
impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Storage(e) => Some(e),
            Self::Io(e) => Some(e),
            Self::Json(e) => Some(e),
            _ => None,
        }
    }
}
impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        Self::Storage(e)
    }
}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}
