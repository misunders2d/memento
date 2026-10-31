//! Optional published text-passage scorer, separate from multimodal embeddings.
//! Returns raw Identity-activation logits in input order. No cutoff or routing.
use crate::bootstrap::{self, Asset};
use memento_core::{Error, Result};
use memento_maintenance::transport::Transport;
use ort::{
    session::{RunOptions, Session, builder::GraphOptimizationLevel},
    value::Tensor,
};
use std::{
    path::Path,
    sync::{
        Arc, Mutex, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokenizers::{
    PostProcessor, Tokenizer, TruncationDirection, TruncationParams, TruncationStrategy,
};

pub const MODEL_ID: &str = "cross-encoder/mmarco-mMiniLMv2-L12-H384-v1";
pub const MODEL_REVISION: &str = "1427fd652930e4ba29e8149678df786c240d8825";
pub const MAX_CANDIDATES: usize = 10;
const MAX_SEQUENCE_LENGTH: usize = 512;
const ASSETS: &[Asset<'static>] = &[
    Asset {
        path: "onnx/model.onnx",
        url: "https://huggingface.co/cross-encoder/mmarco-mMiniLMv2-L12-H384-v1/resolve/1427fd652930e4ba29e8149678df786c240d8825/onnx/model.onnx",
        bytes: 470883696,
        sha: "3e9a03ed1e966f7c5288dd4230e3d6a9bf5e3a170a06f1f4241c5bca12c6487c",
    },
    Asset {
        path: "tokenizer.json",
        url: "https://huggingface.co/cross-encoder/mmarco-mMiniLMv2-L12-H384-v1/resolve/1427fd652930e4ba29e8149678df786c240d8825/tokenizer.json",
        bytes: 17082660,
        sha: "62c24cdc13d4c9952d63718d6c9fa4c287974249e16b7ade6d5a85e7bbb75626",
    },
    Asset {
        path: "config.json",
        url: "https://huggingface.co/cross-encoder/mmarco-mMiniLMv2-L12-H384-v1/resolve/1427fd652930e4ba29e8149678df786c240d8825/config.json",
        bytes: 891,
        sha: "cc2cfe51aa3fd759d21d21acf5dfd6994aa67a3c9210636d22e143699d336c77",
    },
    Asset {
        path: "tokenizer_config.json",
        url: "https://huggingface.co/cross-encoder/mmarco-mMiniLMv2-L12-H384-v1/resolve/1427fd652930e4ba29e8149678df786c240d8825/tokenizer_config.json",
        bytes: 435,
        sha: "e7fbfbfa6347b4e414c1cee50d142e2c2f9a895dad68b068ae83a8b564c3837e",
    },
    Asset {
        path: "special_tokens_map.json",
        url: "https://huggingface.co/cross-encoder/mmarco-mMiniLMv2-L12-H384-v1/resolve/1427fd652930e4ba29e8149678df786c240d8825/special_tokens_map.json",
        bytes: 239,
        sha: "378eb3bf733eb16e65792d7e3fda5b8a4631387ca04d2015199c4d4f22ae554d",
    },
    Asset {
        path: "README.md",
        url: "https://huggingface.co/cross-encoder/mmarco-mMiniLMv2-L12-H384-v1/resolve/1427fd652930e4ba29e8149678df786c240d8825/README.md",
        bytes: 2278,
        sha: "474736a65d6393a060119a8dc304563af67af4d8d86ccfee4a05dd0df107fc11",
    },
];

/// Prepare only an instance-owned default cache. Explicit external caches are
/// verified by `load` and are never downloaded into or repaired there.
pub fn prepare(transport: &dyn Transport, root: &Path, stop: &AtomicBool) -> Result<()> {
    if std::fs::symlink_metadata(root).is_ok_and(|m| m.file_type().is_symlink()) {
        return Err(Error::Validation("Model cache cannot be a symbolic link"));
    }
    for asset in ASSETS {
        bootstrap::ensure(transport, root, asset, stop)?;
    }
    Ok(())
}

fn unavailable() -> Error {
    Error::Unavailable("Text passage scorer is unavailable")
}
fn check(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        Err(Error::Unavailable("Text passage scoring deadline expired"))
    } else {
        Ok(())
    }
}
fn validate(query: &str, documents: &[&str], deadline: Instant) -> Result<()> {
    check(deadline)?;
    if query.trim().is_empty() || documents.iter().any(|s| s.trim().is_empty()) {
        return Err(Error::Validation(
            "Text passage scoring requires nonempty text",
        ));
    }
    if documents.len() > MAX_CANDIDATES {
        return Err(Error::Validation(
            "Text passage shortlist exceeds ten records",
        ));
    }
    Ok(())
}
fn logits(shape: &[i64], values: &[f32]) -> Result<f32> {
    if shape != [1, 1] || values.len() != 1 || !values[0].is_finite() {
        Err(unavailable())
    } else {
        // Publisher config specifies Identity, not sigmoid/probability.
        Ok(values[0])
    }
}

/// One CPU session (1..=8 intra-op threads, default 1); scores serialize
/// independently of EmbeddingGemma lanes.
pub struct TextReranker {
    tokenizer: Tokenizer,
    session: Mutex<Session>,
}
impl TextReranker {
    /// Load only pinned local FP32 assets. Performs no network/authority access.
    pub fn load(root: &Path, runtime_library: &Path, threads: usize) -> Result<Self> {
        if !(1..=8).contains(&threads) {
            return Err(Error::Validation("MEMENTO_TEXT_SCORER_THREADS must be 1..8"));
        }
        for asset in ASSETS {
            if !bootstrap::verify(&root.join(asset.path), asset)? {
                return Err(Error::Unavailable(
                    "Pinned text passage model asset is missing",
                ));
            }
        }
        memento_embedding::initialize_runtime(runtime_library).map_err(|_| unavailable())?;
        let mut tokenizer =
            Tokenizer::from_file(root.join("tokenizer.json")).map_err(|_| unavailable())?;
        tokenizer.with_padding(None);
        tokenizer
            .with_truncation(Some(TruncationParams {
                max_length: MAX_SEQUENCE_LENGTH,
                direction: TruncationDirection::Right,
                strategy: TruncationStrategy::LongestFirst,
                stride: 0,
            }))
            .map_err(|_| unavailable())?;
        if tokenizer.get_post_processor().map(|p| p.added_tokens(true)) != Some(4) {
            return Err(unavailable());
        }
        let session = Session::builder()
            .map_err(|_| unavailable())?
            .with_intra_threads(threads)
            .map_err(|_| unavailable())?
            .with_inter_threads(1)
            .map_err(|_| unavailable())?
            .with_execution_providers([ort::ep::CPU::default().with_arena_allocator(false).build()])
            .map_err(|_| unavailable())?
            .with_optimization_level(GraphOptimizationLevel::Disable)
            .map_err(|_| unavailable())?
            .with_memory_pattern(false)
            .map_err(|_| unavailable())?
            .with_prepacking(false)
            .map_err(|_| unavailable())?
            .commit_from_file(root.join("onnx/model.onnx"))
            .map_err(|_| unavailable())?;
        let mut inputs: Vec<_> = session.inputs().iter().map(|v| v.name()).collect();
        inputs.sort_unstable();
        if inputs != ["attention_mask", "input_ids"]
            || session.outputs().len() != 1
            || session.outputs()[0].name() != "logits"
        {
            return Err(unavailable());
        }
        Ok(Self {
            tokenizer,
            session: Mutex::new(session),
        })
    }

    /// Raw logits in the supplied order; no sorting, cutoff, category decisions,
    /// or media handling. Caller owns typed routing and the remaining budget.
    /// Checks surround tokenization; ORT inference is terminated at the deadline.
    /// Expiry/failure discards every partial score. A later call gets fresh options.
    /// `lock_wait` receives the time spent waiting for the shared session.
    pub fn score(
        &self,
        query: &str,
        documents: &[&str],
        deadline: Instant,
        lock_wait: &mut Duration,
    ) -> Result<Vec<f32>> {
        validate(query, documents, deadline)?;
        if documents.is_empty() {
            return Ok(Vec::new());
        }
        let waiting = Instant::now();
        let mut session = loop {
            // Kept current so an expiry while waiting still reports the wait.
            *lock_wait = waiting.elapsed();
            check(deadline)?;
            match self.session.try_lock() {
                Ok(session) => break session,
                Err(TryLockError::Poisoned(_)) => return Err(unavailable()),
                Err(TryLockError::WouldBlock) => std::thread::sleep(
                    deadline
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_millis(1)),
                ),
            }
        };
        let options = Arc::new(RunOptions::new().map_err(|_| unavailable())?);
        let finished = AtomicBool::new(false);
        std::thread::scope(|scope| {
            let alarm = scope.spawn(|| {
                while !finished.load(Ordering::Acquire) {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        let _ = options.terminate();
                        break;
                    }
                    std::thread::park_timeout(remaining);
                }
            });
            let result = (|| {
                let mut scores = Vec::with_capacity(documents.len());
                for document in documents {
                    check(deadline)?;
                    // Publisher Transformers example: unmodified query/document pair,
                    // add_special_tokens=true, longest_first truncation, batch size one.
                    let encoded = self
                        .tokenizer
                        .encode((query, *document), true)
                        .map_err(|_| unavailable())?;
                    check(deadline)?;
                    let n = encoded.len();
                    let ids: Vec<i64> = encoded.get_ids().iter().map(|v| i64::from(*v)).collect();
                    let mask: Vec<i64> = encoded
                        .get_attention_mask()
                        .iter()
                        .map(|v| i64::from(*v))
                        .collect();
                    let outputs = session.run_with_options(ort::inputs![
                        "input_ids" => Tensor::from_array(([1, n], ids)).map_err(|_| unavailable())?,
                        "attention_mask" => Tensor::from_array(([1, n], mask)).map_err(|_| unavailable())?
                    ], &*options).map_err(|_| unavailable())?;
                    let (shape, values) = outputs["logits"]
                        .try_extract_tensor::<f32>()
                        .map_err(|_| unavailable())?;
                    scores.push(logits(shape.as_ref(), values)?);
                }
                check(deadline)?;
                Ok(scores)
            })();
            finished.store(true, Ordering::Release);
            alarm.thread().unpark();
            if alarm.join().is_err() {
                return Err(unavailable());
            }
            check(deadline)?;
            result
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_expiry_oversized_shortlists_and_empty_text() {
        let future = Instant::now() + Duration::from_secs(1);
        assert!(validate("query", &["text"], Instant::now()).is_err());
        assert!(validate("query", &["text"; 11], future).is_err());
        assert!(validate("query", &["  "], future).is_err());
        assert!(validate(" ", &["text"], future).is_err());
        assert!(validate("query", &[], future).is_ok());
    }
    #[test]
    fn preserves_identity_logits_and_rejects_wrong_shape_or_nonfinite_output() {
        assert_eq!(logits(&[1, 1], &[-6.4651623]).unwrap(), -6.4651623);
        assert_eq!(logits(&[1, 1], &[5.21]).unwrap(), 5.21);
        assert!(logits(&[1], &[1.]).is_err());
        assert!(logits(&[1, 1], &[f32::NAN]).is_err());
        assert!(logits(&[1, 1], &[f32::INFINITY]).is_err());
        assert!(logits(&[1, 1], &[]).is_err());
    }
    #[test]
    fn incomplete_explicit_cache_fails_without_preparing_or_writing_assets() {
        let dir = tempfile::tempdir().unwrap();
        assert!(TextReranker::load(dir.path(), Path::new("missing-runtime"), 1).is_err());
        for threads in [0, 9] {
            assert!(matches!(
                TextReranker::load(dir.path(), Path::new("missing-runtime"), threads),
                Err(Error::Validation(_))
            ));
        }
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }
    #[test]
    #[ignore = "requires explicitly supplied pinned local model assets and ORT; no download"]
    fn pinned_model_raw_order_termination_and_recovery() {
        let root =
            std::env::var_os("MEMENTO_TEXT_RERANKER_TEST_ASSETS").expect("explicit test assets");
        let runtime = std::env::var_os("MEMENTO_ORT_LIBRARY").expect("explicit pinned runtime");
        let scorer = TextReranker::load(Path::new(&root), Path::new(&runtime), 1).unwrap();
        let query = "How many people live in Berlin?";
        let documents = [
            "Berlin had a population of 3,520,031 registered inhabitants in an area of 891.82 square kilometers.",
            "New York City is famous for the Metropolitan Museum of Art.",
        ];
        let before = scorer
            .score(query, &documents, Instant::now() + Duration::from_secs(30), &mut Duration::default())
            .unwrap();
        assert_eq!(before.len(), 2);
        assert!(before[0] > before[1]);
        // Exercise real ORT termination without relying on a machine-speed race.
        let encoded = scorer
            .tokenizer
            .encode((query, documents[0]), true)
            .unwrap();
        let n = encoded.len();
        let ids: Vec<i64> = encoded.get_ids().iter().map(|v| i64::from(*v)).collect();
        let mask: Vec<i64> = encoded
            .get_attention_mask()
            .iter()
            .map(|v| i64::from(*v))
            .collect();
        let options = RunOptions::new().unwrap();
        options.terminate().unwrap();
        {
            let mut session = scorer.session.lock().unwrap();
            assert!(
                session
                    .run_with_options(
                        ort::inputs![
                            "input_ids" => Tensor::from_array(([1, n], ids)).unwrap(),
                            "attention_mask" => Tensor::from_array(([1, n], mask)).unwrap()
                        ],
                        &options
                    )
                    .is_err()
            );
        }
        assert!(scorer.score(query, &documents, Instant::now(), &mut Duration::default()).is_err());
        let after = scorer
            .score(query, &documents, Instant::now() + Duration::from_secs(30), &mut Duration::default())
            .unwrap();
        assert_eq!(before, after);
    }
    #[test]
    #[ignore = "requires explicitly supplied pinned local model assets and ORT; no download"]
    fn pinned_model_threads_keep_scores() {
        let root =
            std::env::var_os("MEMENTO_TEXT_RERANKER_TEST_ASSETS").expect("explicit test assets");
        let runtime = std::env::var_os("MEMENTO_ORT_LIBRARY").expect("explicit pinned runtime");
        let long = "Memento keeps raw history and consolidates durable facts overnight. ".repeat(60);
        let queries = ["How many people live in Berlin?", "which loops does the owner still need to close", long.as_str()];
        let documents: Vec<String> = (0..10)
            .map(|i| format!("Passage {i}: {}", "Berlin population registry and open loop evidence notes. ".repeat(1 + i * 4)))
            .collect();
        let documents: Vec<&str> = documents.iter().map(String::as_str).collect();
        let deadline = || Instant::now() + Duration::from_secs(120);
        let one = TextReranker::load(Path::new(&root), Path::new(&runtime), 1).unwrap();
        for threads in [2, 4] {
            let many = TextReranker::load(Path::new(&root), Path::new(&runtime), threads).unwrap();
            for query in queries {
                let started = Instant::now();
                let a = one.score(query, &documents, deadline(), &mut Duration::default()).unwrap();
                let single = started.elapsed();
                let started = Instant::now();
                let b = many.score(query, &documents, deadline(), &mut Duration::default()).unwrap();
                let parallel = started.elapsed();
                let diff = a.iter().zip(&b).map(|(x, y)| (x - y).abs()).fold(0f32, f32::max);
                let order = |v: &[f32]| {
                    let mut i: Vec<usize> = (0..v.len()).collect();
                    i.sort_by(|x, y| v[*y].total_cmp(&v[*x]));
                    i
                };
                eprintln!("threads={threads} query_chars={} max_diff={diff:e} one={single:?} many={parallel:?}", query.len());
                assert!(diff < 1e-4);
                assert_eq!(order(&a), order(&b));
            }
        }
    }
}
