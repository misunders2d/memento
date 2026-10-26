//! Native authority kernel. Domain/HTTP/model integration is intentionally separate.
pub mod blobs;
pub mod canonical;
pub mod capture;
pub mod compact;
pub mod compression;
pub use compact::compact_worker;
pub mod error;
pub mod invalidation;
pub mod raw;
pub mod store;
pub mod types;
pub use canonical::canonical_json;
pub use error::{Error, Result};
pub use store::{ReadTxn, Store, WriteTxn};
pub use types::*;
