//! Native, pinned FP32 EmbeddingGemma2 with one foreground and one background lane.
//! No network access, Python execution, quantization or per-call model loading.
mod audio;
mod error;
mod image;
mod runtime;
mod text;
pub mod validation;

pub use audio::{AudioInput, AUDIO_MAX_SAMPLES, AUDIO_SAMPLE_RATE};
pub use error::{Error, Result};
pub use image::ImageInput;
pub use runtime::{BackgroundLane, EmbeddingModel, ForegroundLane, ModelConfig};
use std::path::Path;
pub use text::{document_prompt, TextChunk, TextInput, MAX_SEQUENCE_LENGTH, QUERY_PREFIX};

pub const DIMENSIONS: usize = 768;
pub const MODEL_ID: &str = "google/embeddinggemma-2";
pub const MODEL_REVISION: &str = "914f7f89142e33e77833254d9c9b90c3cef7303b";
pub const EXPORT_REVISION: &str = "daa72c51243991dfcaf9f9137d2c573d8f7790c0";
pub const TOKENIZER_SHA256: &str =
    "4d777ef5bdc1aa36227abdfb77c3e49e7b9c892d16e1b6bda41c393504828be4";

/// Immutable frontend; methods produce one bounded item. Decode/ingest policy
/// (EXIF, source artifacts, PDF/video/HEIF extraction) remains the caller's job.
pub struct Preprocessor {
    text: text::TextProcessor,
    audio: audio::AudioProcessor,
}

impl Preprocessor {
    pub fn load(tokenizer_path: &Path) -> Result<Self> {
        runtime::verify_path(tokenizer_path, TOKENIZER_SHA256)?;
        Ok(Self {
            text: text::TextProcessor::load(tokenizer_path)?,
            audio: audio::AudioProcessor::new(),
        })
    }
    pub fn query(&self, text: &str) -> Result<TextInput> {
        self.text.encode(&format!("{QUERY_PREFIX}{text}"))
    }
    pub fn document(&self, text: &str, title: Option<&str>) -> Result<TextInput> {
        self.text.encode(&document_prompt(text, title))
    }
    pub fn prompted_text(&self, text: &str) -> Result<TextInput> {
        self.text.encode(text)
    }
    pub fn chunks(&self, text: &str, size: usize, overlap: usize) -> Result<Vec<TextChunk>> {
        self.text.chunks(text, size, overlap)
    }
    pub fn chunk_text(&self, text: &str) -> Result<Vec<TextChunk>> {
        self.chunks(text, 128, 16)
    }
    pub fn image(&self, path: &Path, budget: u32) -> Result<ImageInput> {
        let image = image::decode_rgb(path)?;
        self.rgb(
            image.width() as usize,
            image.height() as usize,
            image.as_raw(),
            budget,
        )
    }
    pub fn rgb(&self, width: usize, height: usize, rgb: &[u8], budget: u32) -> Result<ImageInput> {
        image::preprocess_rgb(width, height, rgb, budget)
    }
    pub fn audio(&self, samples: &[f32], sample_rate: u32) -> Result<AudioInput> {
        self.audio.preprocess(samples, sample_rate)
    }
}

#[cfg(test)]
mod ownership_types {
    use super::*;
    fn assert_send_sync<T: Send + Sync>() {}
    #[test]
    fn lane_owners_are_thread_safe() {
        assert_send_sync::<EmbeddingModel>();
        assert_send_sync::<ForegroundLane>();
        assert_send_sync::<BackgroundLane>();
        assert_send_sync::<Preprocessor>();
    }
}
