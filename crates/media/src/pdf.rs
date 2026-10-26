//! Page text/render selection through the same native PDFium implementation.
use crate::Error;
use crate::Result;
use image::RgbImage;
use pdfium_render::prelude::*;
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
pub const MAX_PAGES: usize = 5000;
const PDFIUM_SHA256: &str = "fd4de9f91f183476d04b1bc3fd17dacd95aba349c55d2c376512d95e082b441e";
struct Owner {
    path: PathBuf,
    pdfium: Arc<Pdfium>,
}
static OWNER: Mutex<Option<Owner>> = Mutex::new(None);
fn library(path: &Path) -> Result<Arc<Pdfium>> {
    let canonical = path.canonicalize()?;
    let mut owner = OWNER
        .lock()
        .map_err(|_| Error::Decode("PDFium owner lock poisoned"))?;
    if let Some(current) = owner.as_ref() {
        if current.path != canonical {
            return Err(Error::Decode("PDFium library path changed"));
        }
        return Ok(current.pdfium.clone());
    }
    let mut file = File::open(&canonical)?;
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    if format!("{:x}", hash.finalize()) != PDFIUM_SHA256 {
        return Err(Error::Decode("PDFium library checksum differs"));
    }
    // pdfium-render0.9.4 promotes bindings to a process-wide OnceCell. Bind once,
    // retain its owner, and clone only the safe handle for each document. Its
    // thread_safe feature serializes native calls; this init lock is released
    // before callbacks/model inference so foreground work cannot wait on it.
    let pdfium = Arc::new(Pdfium::new(Pdfium::bind_to_library(&canonical)?));
    *owner = Some(Owner {
        path: canonical,
        pdfium: pdfium.clone(),
    });
    Ok(pdfium)
}

pub struct PdfPage {
    pub number: usize,
    pub text: String,
    pub image: Option<RgbImage>,
}
pub struct PdfSummary {
    pub total: usize,
    pub emitted: usize,
}
pub fn pages(
    path: &Path,
    library: &Path,
    stop: &dyn Fn() -> bool,
    emit: &mut dyn FnMut(PdfPage) -> Result<bool>,
) -> Result<PdfSummary> {
    let pdfium = self::library(library)?;
    let document = pdfium.load_pdf_from_file(path, None)?;
    let total = document.pages().len() as usize;
    let mut emitted = 0;
    for index in 0..total.min(MAX_PAGES) {
        if stop() {
            break;
        }
        let page = document.pages().get(index as i32)?;
        let text = page.text()?.all().replace("\r\n", "\n").replace('\r', "\n");
        let text = text
            .trim_matches(|c: char| c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c))
            .to_owned();
        let image = if text.chars().count() >= 200 {
            None
        } else {
            let width = page.width().value as f64;
            let height = page.height().value as f64;
            let scale = 1400.0 / width.max(height).max(1.0);
            let config = PdfRenderConfig::new()
                .set_target_size(
                    (width * scale).ceil() as i32,
                    (height * scale).ceil() as i32,
                )
                .render_form_data(false);
            match page.render_with_config(&config) {
                Ok(bitmap) => Some(bitmap.as_image()?.into_rgb8()),
                Err(_) => continue,
            }
        };
        emitted += 1;
        if !emit(PdfPage {
            number: index + 1,
            text,
            image,
        })? {
            break;
        }
    }
    Ok(PdfSummary { total, emitted })
}
