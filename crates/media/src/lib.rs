//! Bounded native artifact decoders and resumable media ingestion.
pub mod av;
pub mod captured;
pub mod documents;
mod error;
mod image_formats;
pub mod images;
pub mod ingest;
pub mod pdf;
mod stage;
pub mod video;
pub use error::{Error, Result};
#[cfg(test)]
mod stage_tests;

#[cfg(test)]
mod ingest_tests;

#[cfg(test)]
mod captured_tests;
