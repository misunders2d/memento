//! Native authority kernel. Domain/HTTP/model integration is intentionally separate.
pub mod canonical;
pub mod error;
pub mod invalidation;
pub mod raw;
pub mod store;
pub mod types;
pub use canonical::canonical_json;
pub use error::{Error, Result};
pub use store::{ReadTxn, Store, WriteTxn};
pub use types::*;
