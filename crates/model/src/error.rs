#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid embedding input: {0}")]
    InvalidInput(&'static str),
    #[error("invalid pinned model assets: {0}")]
    InvalidAssets(String),
    #[error("embedding output contains nonfinite values or a nonfinite/zero norm")]
    InvalidVector,
    #[error("tokenizer failed: {0}")]
    Tokenizer(String),
    #[error("embedding lane lock was poisoned")]
    PoisonedLane,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Ort(#[from] ort::Error),
    #[error("ORT runtime library failed to load: {0}")]
    RuntimeLoad(#[from] ort::LoadDynamicError),
    #[error(transparent)]
    Image(#[from] ::image::ImageError),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<ort::Error<ort::session::builder::SessionBuilder>> for Error {
    fn from(error: ort::Error<ort::session::builder::SessionBuilder>) -> Self {
        // Drop the recoverable builder while its mapping owner is still alive.
        Self::Ort(error.into())
    }
}
