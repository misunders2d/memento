//! Decode/infer outside the writer; stage and publish lease-fenced units.
use crate::{
    Error, Result, av, documents, images, pdf,
    stage::{self, Metadata, Unit},
    video,
};
use memento_core::blobs::{BlobInfo, BlobReader};
use memento_domain::{
    Graph,
    jobs::{Handler, Job, JobFailure},
};
use memento_embedding::EmbeddingModel;
use serde_json::Value;
use std::{
    io::Cursor,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
pub type Reader = Arc<dyn Fn(&str) -> memento_core::Result<BlobReader> + Send + Sync>;
// Private seam for bounded pipeline-control tests. Public construction always
// receives the real pinned EmbeddingModel; no runtime backend selector exists.
pub(crate) trait Encoder: Send + Sync {
    fn chunk_text(&self, text: &str) -> Result<Vec<memento_embedding::TextChunk>>;
    fn encode_document(&self, text: &str, title: Option<&str>) -> Result<[f32; 768]>;
    fn encode_rgb(&self, w: usize, h: usize, rgb: &[u8], budget: u32) -> Result<[f32; 768]>;
    fn encode_audio(&self, samples: &[f32], rate: u32) -> Result<[f32; 768]>;
}
impl Encoder for EmbeddingModel {
    fn chunk_text(&self, text: &str) -> Result<Vec<memento_embedding::TextChunk>> {
        Ok(EmbeddingModel::chunk_text(self, text)?)
    }
    fn encode_document(&self, text: &str, title: Option<&str>) -> Result<[f32; 768]> {
        Ok(EmbeddingModel::encode_document(self, text, title)?)
    }
    fn encode_rgb(&self, w: usize, h: usize, rgb: &[u8], budget: u32) -> Result<[f32; 768]> {
        Ok(EmbeddingModel::encode_rgb(self, w, h, rgb, budget)?)
    }
    fn encode_audio(&self, samples: &[f32], rate: u32) -> Result<[f32; 768]> {
        Ok(EmbeddingModel::encode_audio(self, samples, rate)?)
    }
}
#[derive(Clone)]
pub struct MediaIngest {
    graph: Graph,
    model: Arc<dyn Encoder>,
    pdfium: PathBuf,
    reader: Reader,
}
impl MediaIngest {
    pub fn new(graph: Graph, model: EmbeddingModel, pdfium: PathBuf) -> Self {
        let blobs = graph.store.blobs().clone();
        Self {
            graph,
            model: Arc::new(model),
            pdfium,
            reader: Arc::new(move |sha| blobs.reader(sha)),
        }
    }
    #[cfg(test)]
    pub(crate) fn for_test(graph: Graph, model: Arc<dyn Encoder>) -> Self {
        let blobs = graph.store.blobs().clone();
        Self {
            graph,
            model,
            pdfium: PathBuf::new(),
            reader: Arc::new(move |sha| blobs.reader(sha)),
        }
    }
    /// Wire ColdManager::reader here; all rehydration stays outside transactions.
    pub fn with_reader(mut self, reader: Reader) -> Self {
        self.reader = reader;
        self
    }
    pub fn handler(&self) -> Handler {
        let this = self.clone();
        Arc::new(move |job| this.handle(job))
    }
    pub fn handle(&self, job: &Job) -> std::result::Result<Value, JobFailure> {
        self.run(job).map_err(|error|{
   if matches!(error,Error::Stopped){return JobFailure{category:"InterruptedError",retryable:true,retry_after:Some(Duration::ZERO)}}
   if matches!(&error,Error::Core(memento_core::Error::Conflict(message)) if *message=="Work lease expired or was reassigned"){
    return JobFailure{category:"ConflictError",retryable:true,retry_after:Some(Duration::ZERO)}
   }
   if job.payload["captured"]==true{
    let unavailable=self.graph.store.read(|tx|{let sha=stage::string(&job.payload,"sha256")?;let target:bool=tx.sql().query_row("SELECT EXISTS(SELECT 1 FROM media_files m JOIN forgotten_targets f ON f.target=m.node_id WHERE m.sha256=?)",[sha],|r|r.get(0))?;Ok(!target&& !memento_domain::jobs::active_captured_source(tx.sql(),sha)?)}).unwrap_or(false);
    if unavailable{return JobFailure{category:"CapturedSourceUnavailable",retryable:false,retry_after:None}}
   }
   match error{Error::Core(e)=>e.into(),Error::Io(e)=>memento_core::Error::Io(e).into(),Error::Decode(_)|Error::Zip(_)|Error::Xml(_)|Error::Json(_)|Error::Image(_)|Error::Av(_)|Error::Pdf(_)=>JobFailure{category:"ValueError",retryable:false,retry_after:None},_=>JobFailure{category:"RuntimeError",retryable:true,retry_after:None}}
  })
    }
    fn save(&self, bytes: &[u8], mime: &str) -> Result<(BlobInfo, String)> {
        Ok((
            self.graph
                .store
                .blobs()
                .put(&mut Cursor::new(bytes), bytes.len() as u64)?,
            mime.into(),
        ))
    }
    fn visit(
        &self,
        job: &Job,
        identity: &str,
        cursor: usize,
        ordinal: &mut usize,
        make: impl FnOnce() -> Result<Unit>,
    ) -> Result<bool> {
        if job.stopping() {
            return Err(Error::Stopped);
        }
        if *ordinal >= cursor {
            job.check()?;
            let unit = make()?;
            stage::persist(&self.graph.store, job, identity, *ordinal, unit)?;
        }
        *ordinal += 1;
        Ok(true)
    }
    fn image_unit(&self, rgb: &image::RgbImage, mut metadata: Metadata) -> Result<Unit> {
        let artifact = self.save(&images::jpeg(rgb)?, "image/jpeg")?;
        metadata.artifact_sha256 = artifact.0.sha256.clone();
        let vector = self.model.encode_rgb(
            rgb.width() as usize,
            rgb.height() as usize,
            rgb.as_raw(),
            70,
        )?;
        Ok(Unit {
            metadata,
            vector,
            artifacts: vec![artifact],
        })
    }
    fn text(
        &self,
        text: &str,
        source_artifact: &str,
        page: Option<usize>,
        job: &Job,
        identity: &str,
        cursor: usize,
        ordinal: &mut usize,
    ) -> Result<()> {
        let source = self.save(text.as_bytes(), "text/plain")?;
        for chunk in self.model.chunk_text(text)? {
            self.visit(job, identity, cursor, ordinal, || {
                let mut metadata = meta("text", source_artifact);
                metadata.text = chunk.text.clone();
                metadata.page = page;
                metadata.char_start = Some(chunk.start);
                metadata.char_end = Some(chunk.end);
                metadata.source_sha256 = Some(source.0.sha256.clone());
                let vector = self.model.encode_document(&chunk.text, None)?;
                Ok(Unit {
                    metadata,
                    vector,
                    artifacts: vec![source.clone()],
                })
            })?;
        }
        Ok(())
    }
    fn audio(
        &self,
        path: &Path,
        job: &Job,
        identity: &str,
        cursor: usize,
        ordinal: &mut usize,
    ) -> Result<()> {
        av::audio_windows(path, &|| job.stopping(), &mut |window| {
            self.visit(job, identity, cursor, ordinal, || {
                let vector = self.model.encode_audio(&window.samples, 16000)?;
                let artifact = self.save(&wav(&window.samples), "audio/wav")?;
                let mut metadata = meta("audio", &artifact.0.sha256);
                metadata.start_s = Some(window.start_s);
                metadata.end_s = Some(window.end_s);
                Ok(Unit {
                    metadata,
                    vector,
                    artifacts: vec![artifact],
                })
            })
        })
    }
    fn reuse(&self, job: &Job, identity: &str, cursor: usize, ordinal: &mut usize) -> Result<bool> {
        let candidate=self.graph.store.read(|tx|{
   use rusqlite::OptionalExtension;
   Ok(tx.sql().query_row("SELECT f.node_id FROM media_files f JOIN media_representations r ON r.node_id=f.node_id JOIN node_versions n ON n.id=f.node_id WHERE f.sha256=?1 AND r.identity=?2 AND n.state='active' AND n.version=(SELECT max(v.version) FROM node_versions v WHERE v.id=n.id) AND NOT EXISTS(SELECT 1 FROM forgotten_targets x WHERE x.target=n.id) AND (NOT EXISTS(SELECT 1 FROM captured_media cm WHERE cm.node_id=n.id) OR EXISTS(SELECT 1 FROM captured_media cm JOIN captured_media_refs cr ON cr.sha256=cm.sha256 JOIN episodes e ON e.id=cr.episode_id WHERE cm.node_id=n.id AND NOT EXISTS(SELECT 1 FROM forgotten_sources fs WHERE fs.source=e.source AND fs.event_id=e.event_id) AND NOT EXISTS(SELECT 1 FROM media_files mf JOIN forgotten_targets ft ON ft.target=mf.node_id WHERE mf.sha256=cm.sha256))) AND EXISTS(SELECT 1 FROM segments s WHERE s.node_id=n.id) AND NOT EXISTS(SELECT 1 FROM segments s LEFT JOIN vector_items i ON i.target=s.id AND i.kind='segment' AND i.version=1 LEFT JOIN vector_blobs b ON b.seq=i.seq AND b.current=1 WHERE s.node_id=n.id AND b.seq IS NULL) ORDER BY f.rowid LIMIT 1",rusqlite::params![stage::string(&job.payload,"sha256")?,identity],|r|r.get::<_,String>(0)).optional()?)
  })?;
        let Some(node) = candidate else {
            return Ok(false);
        };
        let expected: usize = self.graph.store.read(|tx| {
            Ok(tx.sql().query_row(
                "SELECT count(*) FROM segments WHERE node_id=?",
                [&node],
                |r| r.get::<_, i64>(0),
            )? as usize)
        })?;
        let mut last = 0i64;
        loop {
            if job.stopping() {
                return Err(Error::Stopped);
            }
            let next=self.graph.store.read(|tx|{
    use rusqlite::OptionalExtension;
    Ok(tx.sql().query_row("SELECT s.rowid,s.modality,s.text,s.artifact_sha256,s.start_s,s.end_s,s.page,s.char_start,s.char_end,s.source_sha256,b.embedding FROM segments s JOIN vector_items i ON i.target=s.id AND i.kind='segment' AND i.version=1 JOIN vector_blobs b ON b.seq=i.seq AND b.current=1 WHERE s.node_id=? AND s.rowid>? ORDER BY s.rowid LIMIT 1",rusqlite::params![node,last],|r|Ok((r.get::<_,i64>(0)?,Metadata{modality:r.get(1)?,text:r.get(2)?,artifact_sha256:r.get(3)?,start_s:r.get(4)?,end_s:r.get(5)?,page:r.get::<_,Option<i64>>(6)?.map(|n|n as usize),char_start:r.get::<_,Option<i64>>(7)?.map(|n|n as usize),char_end:r.get::<_,Option<i64>>(8)?.map(|n|n as usize),source_sha256:r.get(9)?},r.get::<_,Vec<u8>>(10)?))).optional()?)
   })?;
            let Some((rowid, metadata, bytes)) = next else {
                break;
            };
            last = rowid;
            if *ordinal < cursor {
                *ordinal += 1;
                continue;
            }
            let _artifact = (self.reader)(&metadata.artifact_sha256)?;
            let _source = metadata
                .source_sha256
                .as_ref()
                .map(|sha| (self.reader)(sha))
                .transpose()?;
            self.visit(job, identity, cursor, ordinal, || {
                let values = memento_core::F32Blob::from_bytes(bytes)?.values();
                let vector: [f32; 768] = values
                    .try_into()
                    .map_err(|_| Error::Decode("reused vector dimensions differ"))?;
                Ok(Unit {
                    metadata,
                    vector,
                    artifacts: vec![],
                })
            })?;
        }
        if *ordinal != expected {
            return Err(memento_core::Error::Conflict(
                "Reusable representation changed during copy",
            )
            .into());
        }
        Ok(true)
    }
    fn run(&self, job: &Job) -> Result<Value> {
        if job.kind != "media" {
            return Err(Error::Decode("expected media job"));
        }
        if let Some(prior) = stage::previous(&self.graph.store, job)? {
            return Ok(prior);
        }
        let kind = stage::string(&job.payload, "kind")?;
        let filename = stage::string(&job.payload, "filename")?;
        let sha = stage::string(&job.payload, "sha256")?;
        let identity = stage::representation(&self.graph.store, kind, filename)?;
        let cursor = stage::cursor(&self.graph.store, job, &identity)?;
        let original = (self.reader)(sha)?;
        let info = BlobInfo {
            sha256: sha.into(),
            size: std::fs::metadata(original.path())?.len(),
            tier: "hot".into(),
        };
        self.graph.store.write(|tx| {
            job.check_in(tx.sql())?;
            stage::check_source(tx.sql(), job)?;
            tx.register_blob(&info, "application/octet-stream")?;
            Ok(())
        })?;
        let mut ordinal = 0;
        if !self.reuse(job, &identity, cursor, &mut ordinal)? {
            match kind {
                "image" => {
                    self.visit(job, &identity, cursor, &mut ordinal, || {
                        let image = images::prepare(original.path(), filename, 1024, 1)?;
                        let vector = self.model.encode_rgb(
                            image.rgb.width() as usize,
                            image.rgb.height() as usize,
                            image.rgb.as_raw(),
                            70,
                        )?;
                        Ok(Unit {
                            metadata: meta("image", sha),
                            vector,
                            artifacts: vec![],
                        })
                    })?;
                }
                "audio" => self.audio(original.path(), job, &identity, cursor, &mut ordinal)?,
                "video" => {
                    video::keyframes(original.path(), &|| job.stopping(), &mut |frame| {
                        self.visit(job, &identity, cursor, &mut ordinal, || {
                            let mut metadata = meta("image", "");
                            metadata.start_s = Some(frame.start_s);
                            metadata.end_s = Some(frame.end_s);
                            self.image_unit(&frame.rgb, metadata)
                        })
                    })?;
                    if !job.stopping() {
                        self.audio(original.path(), job, &identity, cursor, &mut ordinal)?;
                    }
                }
                "document" => {
                    let extension = Path::new(filename)
                        .extension()
                        .and_then(|x| x.to_str())
                        .unwrap_or("");
                    if extension.eq_ignore_ascii_case("pdf") {
                        pdf::pages(
                            original.path(),
                            &self.pdfium,
                            &|| job.stopping(),
                            &mut |page| {
                                if let Some(rgb) = page.image {
                                    self.visit(job, &identity, cursor, &mut ordinal, || {
                                        let rgb = images::jpeg_backed(&rgb)?;
                                        let mut metadata = meta("image", "");
                                        metadata.text = page.text;
                                        metadata.page = Some(page.number);
                                        self.image_unit(&rgb, metadata)
                                    })?;
                                } else {
                                    self.text(
                                        &page.text,
                                        sha,
                                        Some(page.number),
                                        job,
                                        &identity,
                                        cursor,
                                        &mut ordinal,
                                    )?;
                                }
                                Ok(true)
                            },
                        )?;
                    } else {
                        self.text(
                            &documents::document_text(original.path(), extension)?,
                            sha,
                            None,
                            job,
                            &identity,
                            cursor,
                            &mut ordinal,
                        )?;
                    }
                }
                _ => return Err(Error::Decode("unsupported media kind")),
            }
        }
        if job.stopping() {
            return Err(Error::Stopped);
        }
        if ordinal < cursor {
            return Err(Error::Decode("decoder produced fewer units than staged"));
        }
        // Pending/leased stage refs are eviction-fenced by ColdManager. Preflight
        // one file at a time (bounded FD count) before the final writer transaction.
        for sha in stage::artifact_refs(&self.graph.store, job)? {
            drop((self.reader)(&sha)?);
        }
        stage::finish(&self.graph.store, job, &identity, ordinal)
    }
}
fn meta(modality: &str, artifact: &str) -> Metadata {
    Metadata {
        modality: modality.into(),
        text: String::new(),
        artifact_sha256: artifact.into(),
        start_s: None,
        end_s: None,
        page: None,
        char_start: None,
        char_end: None,
        source_sha256: None,
    }
}
fn wav(samples: &[f32]) -> Vec<u8> {
    let n = (samples.len() * 2) as u32;
    let mut bytes = Vec::with_capacity(n as usize + 44);
    bytes.extend_from_slice(b"RIFF");
    bytes.extend_from_slice(&(n + 36).to_le_bytes());
    bytes.extend_from_slice(b"WAVEfmt ");
    bytes.extend_from_slice(&16u32.to_le_bytes());
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(&16000u32.to_le_bytes());
    bytes.extend_from_slice(&32000u32.to_le_bytes());
    bytes.extend_from_slice(&2u16.to_le_bytes());
    bytes.extend_from_slice(&16u16.to_le_bytes());
    bytes.extend_from_slice(b"data");
    bytes.extend_from_slice(&n.to_le_bytes());
    for x in samples {
        bytes.extend_from_slice(&((x.clamp(-1.0, 1.0) * 32767.0) as i16).to_le_bytes());
    }
    bytes
}
