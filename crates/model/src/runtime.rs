use crate::{
    validation, AudioInput, Error, ImageInput, Preprocessor, Result, TextChunk, TextInput,
    DIMENSIONS, MODEL_REVISION,
};
use memmap2::{Mmap, MmapOptions};
use ort::{
    session::{builder::GraphOptimizationLevel, Session},
    value::Tensor,
};
use sha2::{Digest, Sha256};
use std::{
    borrow::Cow,
    ffi::{c_char, CStr},
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

const GRAPH_FILES: [(&str, &str, &str, u64); 3] = [
    (
        "model",
        "bc47de15f81208a5c99e5ab10f746d5e33b51ea228b7dc0bef9c133a94f1c1c3",
        "9fd452bfc6916e5a92f080f8dada35598ca4512b21c65897d4c5974d5f9630a2",
        1084170240,
    ),
    (
        "vision_encoder",
        "a3fb44f8b5b152cfee6afe16e2cd69e774b7e1d3b822d1db1190ddf7b08b575e",
        "ca8d28c99feb8129618f141978a27adae25090e0dadb68a30c235e191c01e37e",
        671026176,
    ),
    (
        "audio_encoder",
        "e0a62590c49949fe0b1410f5dc52c92a185abb4c4e69e2c50a4e316103fdbc09",
        "6943b910d1d1e9b5dffd4ccad63f61aa308ad10b70ed128bb61886b119da1e3a",
        1172750848,
    ),
];
const RUNTIME_SHA256: &str = "469849977cb60744ea160927e1a3a0643fd4c55c0aff743251c6d293402e334e";
static ACTIVE_RUNTIME: Mutex<Option<PathBuf>> = Mutex::new(None);

#[derive(Clone, Debug)]
pub struct ModelConfig {
    /// Immutable export root containing onnx/*.onnx and *.onnx_data.
    pub model_dir: PathBuf,
    pub runtime_library: PathBuf,
    pub revision: String,
    /// Defaults to model_dir/tokenizer.json; useful for existing pinned caches.
    pub tokenizer_path: Option<PathBuf>,
    /// Defaults to model_dir/onnx; supports separately mounted small graphs.
    pub graph_dir: Option<PathBuf>,
}

impl ModelConfig {
    pub fn new(model_dir: impl Into<PathBuf>, runtime_library: impl Into<PathBuf>) -> Self {
        Self {
            model_dir: model_dir.into(),
            runtime_library: runtime_library.into(),
            revision: MODEL_REVISION.into(),
            tokenizer_path: None,
            graph_dir: None,
        }
    }
}

pub(crate) fn verify_path(path: &Path, expected: &str) -> Result<()> {
    let mut file = File::open(path)?;
    verify_reader(&mut file, expected)
}
fn verify_reader(file: &mut File, expected: &str) -> Result<()> {
    let mut hash = Sha256::new();
    let mut buffer = vec![0_u8; 4 * 1024 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    if format!("{:x}", hash.finalize()) != expected {
        return Err(Error::InvalidAssets("asset checksum mismatch".into()));
    }
    Ok(())
}

struct Mapping {
    map: Mmap,
    _file: File,
}
struct Assets {
    mappings: Vec<Mapping>,
    graph_dir: PathBuf,
}

impl Assets {
    fn load(config: &ModelConfig) -> Result<Self> {
        if config.revision != MODEL_REVISION {
            return Err(Error::InvalidAssets("unsupported model revision".into()));
        }
        let graph_dir = config
            .graph_dir
            .clone()
            .unwrap_or_else(|| config.model_dir.join("onnx"));
        Self::load_files(&config.model_dir, graph_dir, &GRAPH_FILES)
    }
    fn load_files(
        model_dir: &Path,
        graph_dir: PathBuf,
        files: &[(&str, &str, &str, u64); 3],
    ) -> Result<Self> {
        // One auxiliary worker plus this caller bound verification to two jobs.
        // Both results are joined before errors escape, so partial maps/files drop.
        let mappings = std::thread::scope(|scope| -> Result<Vec<Mapping>> {
            let first = std::thread::Builder::new().spawn_scoped(scope, || {
                files[..2]
                    .iter()
                    .map(|spec| Self::mapping(model_dir, &graph_dir, *spec))
                    .collect::<Result<Vec<_>>>()
            })?;
            let last = Self::mapping(model_dir, &graph_dir, files[2]);
            let mut mappings = first
                .join()
                .map_err(|_| Error::InvalidAssets("asset verification worker failed".into()))??;
            mappings.push(last?);
            Ok(mappings)
        })?;
        Ok(Self {
            mappings,
            graph_dir,
        })
    }
    fn mapping(
        model_dir: &Path,
        graph_dir: &Path,
        (name, graph_sha, weight_sha, bytes): (&str, &str, &str, u64),
    ) -> Result<Mapping> {
        verify_path(&graph_dir.join(format!("{name}.onnx")), graph_sha)?;
        let mut file = File::open(model_dir.join("onnx").join(format!("{name}.onnx_data")))?;
        if file.metadata()?.len() != bytes {
            return Err(Error::InvalidAssets("weight file size mismatch".into()));
        }
        verify_reader(&mut file, weight_sha)?;
        // SAFETY: caller supplies immutable pinned assets. The readonly map
        // and its file owner are retained until every SessionSet is dropped.
        let map = unsafe { MmapOptions::new().map(&file)? };
        Ok(Mapping { map, _file: file })
    }
}

#[repr(C)]
struct ApiBase {
    get_api: unsafe extern "C" fn(u32) -> *const std::ffi::c_void,
    get_version_string: unsafe extern "C" fn() -> *const c_char,
}

fn initialize_runtime(path: &Path) -> Result<()> {
    let canonical = path.canonicalize()?;
    let mut active = ACTIVE_RUNTIME.lock().map_err(|_| Error::PoisonedLane)?;
    if let Some(current) = active.as_ref() {
        if *current != canonical {
            return Err(Error::InvalidAssets(
                "another runtime library is already active".into(),
            ));
        }
        return Ok(());
    }
    verify_path(&canonical, RUNTIME_SHA256)?;
    // Verify actual DLL version before asking the Rust binding to initialize it.
    unsafe {
        let library = libloading::Library::new(&canonical)
            .map_err(|e| Error::InvalidAssets(e.to_string()))?;
        let get_base: libloading::Symbol<unsafe extern "C" fn() -> *const ApiBase> = library
            .get(b"OrtGetApiBase\0")
            .map_err(|e| Error::InvalidAssets(e.to_string()))?;
        let base = get_base();
        if base.is_null() {
            return Err(Error::InvalidAssets("missing ORT API".into()));
        }
        let version = CStr::from_ptr(((*base).get_version_string)())
            .to_str()
            .map_err(|_| Error::InvalidAssets("invalid runtime version".into()))?;
        if version != "1.31.0" {
            return Err(Error::InvalidAssets("ORT1.31.0 is required".into()));
        }
    }
    if !ort::init_from(&canonical)?
        .with_name("memento-embedding")
        .commit()
    {
        return Err(Error::InvalidAssets(
            "ORT was initialized outside this pinned model owner".into(),
        ));
    }
    *active = Some(canonical);
    Ok(())
}

// Rust drops fields in declaration order. All sessions are destroyed before
// their last Arc<Assets>, including on load failure. No ORT values are exposed
// by this module, so no borrowed-weight tensor can outlive its owning SessionSet.
struct SessionSet {
    text: Session,
    vision: Session,
    audio: Session,
    _assets: Arc<Assets>,
}

impl SessionSet {
    fn load(assets: Arc<Assets>) -> Result<Self> {
        let text = Self::session(&assets, 0)?;
        let vision = Self::session(&assets, 1)?;
        let audio = Self::session(&assets, 2)?;
        Ok(Self {
            text,
            vision,
            audio,
            _assets: assets,
        })
    }
    fn session(assets: &Assets, index: usize) -> Result<Session> {
        let name = GRAPH_FILES[index].0;
        // SAFETY: this lifetime extension is private to the SessionSet owner.
        // Assets are immutable and Arc-held after all three sessions; session
        // drop precedes unmapping. The temporary builder never escapes. On any
        // error its caller still owns Assets while partial sessions are dropped.
        let buffer: &'static [u8] =
            unsafe { std::mem::transmute::<&[u8], &'static [u8]>(&assets.mappings[index].map[..]) };
        Ok(Session::builder()?
            .with_intra_threads(1)?
            .with_inter_threads(1)?
            .with_execution_providers([ort::ep::CPU::default()
                .with_arena_allocator(false)
                .build()])?
            .with_optimization_level(GraphOptimizationLevel::Disable)?
            .with_memory_pattern(false)?
            .with_prepacking(false)?
            .with_config_entry(
                "session.use_external_initializer_file_buffers_directly",
                "1",
            )?
            .with_external_initializer_file_in_memory(
                format!("{name}.onnx_data"),
                Cow::Borrowed(buffer),
            )?
            .commit_from_file(assets.graph_dir.join(format!("{name}.onnx")))?)
    }
    fn text(&mut self, input: TextInput) -> Result<[f32; DIMENSIONS]> {
        self.run(input, empty_features()?, empty_features()?)
    }
    fn image(&mut self, image: ImageInput) -> Result<[f32; DIMENSIONS]> {
        let ids = image.text_input();
        let outputs = self.vision.run(ort::inputs![
            "pixel_values"=>Tensor::from_array(([1,image.max_patches,768],image.pixels))?,
            "pixel_position_ids"=>Tensor::from_array(([1,image.max_patches,2],image.position_ids))?
        ])?;
        let (shape, features) = outputs["image_features"].try_extract_tensor::<f32>()?;
        if shape.as_ref() != [image.soft_tokens as i64, 512] {
            return Err(Error::InvalidVector);
        }
        let features = Tensor::from_array(([image.soft_tokens, 512], features.to_vec()))?;
        drop(outputs);
        self.run(ids, features, empty_features()?)
    }
    fn audio(&mut self, audio: AudioInput) -> Result<[f32; DIMENSIONS]> {
        let ids = audio.text_input();
        let outputs = self.audio.run(ort::inputs![
            "input_features"=>Tensor::from_array(([1,audio.frames,128],audio.features))?,
            "input_features_mask"=>Tensor::from_array(([1,audio.frames],audio.frame_mask))?
        ])?;
        let (shape, features) = outputs["audio_features"].try_extract_tensor::<f32>()?;
        if shape.as_ref() != [audio.soft_tokens as i64, 512] {
            return Err(Error::InvalidVector);
        }
        let features = Tensor::from_array(([audio.soft_tokens, 512], features.to_vec()))?;
        drop(outputs);
        self.run(ids, empty_features()?, features)
    }
    fn run(
        &mut self,
        input: TextInput,
        image: Tensor<f32>,
        audio: Tensor<f32>,
    ) -> Result<[f32; DIMENSIONS]> {
        if input.input_ids.len() != input.attention_mask.len() {
            return Err(Error::InvalidInput("token/mask lengths differ"));
        }
        let count = input.input_ids.len();
        let outputs = self.text.run(ort::inputs![
            "input_ids"=>Tensor::from_array(([1,count],input.input_ids))?,
            "attention_mask"=>Tensor::from_array(([1,count],input.attention_mask))?,
            "image_features"=>image,"video_features"=>empty_features()?,"audio_features"=>audio
        ])?;
        let (_, values) = outputs["sentence_embedding"].try_extract_tensor::<f32>()?;
        validation::normalize(values)
    }
}

fn empty_features() -> Result<Tensor<f32>> {
    Ok(Tensor::from_array(([0, 512], Vec::<f32>::new()))?)
}
struct Core {
    preprocessor: Preprocessor,
    foreground: Mutex<SessionSet>,
    background: Mutex<SessionSet>,
}

#[derive(Clone)]
pub struct EmbeddingModel {
    core: Arc<Core>,
}
#[derive(Clone)]
pub struct ForegroundLane {
    core: Arc<Core>,
}
#[derive(Clone)]
pub struct BackgroundLane {
    core: Arc<Core>,
}

impl EmbeddingModel {
    pub fn load(config: ModelConfig) -> Result<Self> {
        initialize_runtime(&config.runtime_library)?;
        let tokenizer = config
            .tokenizer_path
            .clone()
            .unwrap_or_else(|| config.model_dir.join("tokenizer.json"));
        let preprocessor = Preprocessor::load(&tokenizer)?;
        let assets = Arc::new(Assets::load(&config)?);
        let foreground = Mutex::new(SessionSet::load(assets.clone())?);
        let background = Mutex::new(SessionSet::load(assets)?);
        Ok(Self {
            core: Arc::new(Core {
                preprocessor,
                foreground,
                background,
            }),
        })
    }
    pub fn foreground(&self) -> ForegroundLane {
        ForegroundLane {
            core: self.core.clone(),
        }
    }
    pub fn background(&self) -> BackgroundLane {
        BackgroundLane {
            core: self.core.clone(),
        }
    }
    pub fn encode_query(&self, text: &str) -> Result<[f32; DIMENSIONS]> {
        self.foreground().encode_query(text)
    }
    pub fn encode_document(&self, text: &str, title: Option<&str>) -> Result<[f32; DIMENSIONS]> {
        self.background().encode_document(text, title)
    }
    pub fn encode_image(&self, path: &Path, budget: u32) -> Result<[f32; DIMENSIONS]> {
        self.background().encode_image(path, budget)
    }
    pub fn encode_rgb(
        &self,
        width: usize,
        height: usize,
        rgb: &[u8],
        budget: u32,
    ) -> Result<[f32; DIMENSIONS]> {
        self.background().encode_rgb(width, height, rgb, budget)
    }
    pub fn encode_audio(&self, samples: &[f32], sample_rate: u32) -> Result<[f32; DIMENSIONS]> {
        self.background().encode_audio(samples, sample_rate)
    }
    pub fn chunks(&self, text: &str, size: usize, overlap: usize) -> Result<Vec<TextChunk>> {
        self.core.preprocessor.chunks(text, size, overlap)
    }
    pub fn chunk_text(&self, text: &str) -> Result<Vec<TextChunk>> {
        self.core.preprocessor.chunk_text(text)
    }
}

impl ForegroundLane {
    pub fn encode_query(&self, text: &str) -> Result<[f32; DIMENSIONS]> {
        let mut lane = self
            .core
            .foreground
            .lock()
            .map_err(|_| Error::PoisonedLane)?;
        lane.text(self.core.preprocessor.query(text)?)
    }
}
impl BackgroundLane {
    pub fn encode_document(&self, text: &str, title: Option<&str>) -> Result<[f32; DIMENSIONS]> {
        let mut lane = self
            .core
            .background
            .lock()
            .map_err(|_| Error::PoisonedLane)?;
        lane.text(self.core.preprocessor.document(text, title)?)
    }
    pub fn encode_image(&self, path: &Path, budget: u32) -> Result<[f32; DIMENSIONS]> {
        let mut lane = self
            .core
            .background
            .lock()
            .map_err(|_| Error::PoisonedLane)?;
        lane.image(self.core.preprocessor.image(path, budget)?)
    }
    pub fn encode_rgb(
        &self,
        width: usize,
        height: usize,
        rgb: &[u8],
        budget: u32,
    ) -> Result<[f32; DIMENSIONS]> {
        let mut lane = self
            .core
            .background
            .lock()
            .map_err(|_| Error::PoisonedLane)?;
        lane.image(self.core.preprocessor.rgb(width, height, rgb, budget)?)
    }
    pub fn encode_audio(&self, samples: &[f32], sample_rate: u32) -> Result<[f32; DIMENSIONS]> {
        let mut lane = self
            .core
            .background
            .lock()
            .map_err(|_| Error::PoisonedLane)?;
        lane.audio(self.core.preprocessor.audio(samples, sample_rate)?)
    }
}

#[cfg(test)]
#[path = "asset_tests.rs"]
mod asset_tests;
