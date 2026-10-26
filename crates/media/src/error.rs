#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("media indexing stopped between units")]
    Stopped,
    #[error("artifact cannot be decoded: {0}")]
    Decode(&'static str),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Zip(#[from] zip::result::ZipError),
    #[error(transparent)]
    Xml(#[from] quick_xml::Error),
    #[error(transparent)]
    Model(#[from] memento_embedding::Error),
    #[error(transparent)]
    Core(#[from] memento_core::Error),
    #[error(transparent)]
    Av(#[from] ffmpeg_next::Error),
    #[error(transparent)]
    Pdf(#[from] pdfium_render::prelude::PdfiumError),
    #[error(transparent)]
    Image(#[from] image::ImageError),
}
pub type Result<T> = std::result::Result<T, Error>;
