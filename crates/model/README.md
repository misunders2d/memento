# memento-embedding

Native CPU EmbeddingGemma2 for the pinned Memento vector space. The public result is always `[f32; 768]`. Text control-token spellings remain literal; query/document prefixes, Unicode character chunk spans, RGB patch positions and16kHz log-mel preprocessing follow the existing implementation.

```rust
use memento_embedding::{EmbeddingModel, ModelConfig};
let model = EmbeddingModel::load(ModelConfig::new(model_dir, ort_library))?;
let query = model.encode_query("Find the document about cushions.")?;
let document = model.encode_document("Exact passage text", None)?;
```

`model_dir` contains the pinned exported `onnx/{model,vision_encoder,audio_encoder}.onnx` graphs and matching `.onnx_data` files, plus the original `tokenizer.json`. `ModelConfig::tokenizer_path` and `graph_dir` support separately mounted existing caches. Source revision914f7f89142e33e77833254d9c9b90c3cef7303b, export revisiondaa72c51243991dfcaf9f9137d2c573d8f7790c0, runtimeORT1.31.0 and every asset checksum are pinned. Loading verifies all weights once and does no network access or conversion. Assets must remain immutable for the model lifetime. Keep the runtime library installed for the process lifetime.

The owned model is cheap to clone. `foreground()` exposes query encoding; `background()` exposes document/image/audio encoding. Background admission includes preprocessing, so only one background unit runs at once. Each lane has three one-thread sessions; all six sessions share the same three read-only weight mappings. There is no global inference lock. The private lifetime extension required by the binding is guarded by an `Arc` owner and session-before-map drop order; neither sessions nor borrowed ORT values escape the module. No per-call or process-lifetime `Box::leak` is used.

`encode_image(path,budget)` decodes common RGB image formats. `encode_rgb(width,height,bytes,budget)` accepts canonical decoder output for PDF pages, video frames and other image codecs. Ingest owns EXIF orientation, transparency-on-white, original artifacts, page/time provenance and codec-specific normalization. Budgets are70,140,280,560,1120. Audio accepts one finite mono float32 slice at16000Hz, at most480000 samples; the native source's too-short analysis-window failure is retained. There is no implicit resampling or caption substitute.

`chunks(text,size,overlap)` returns exact Unicode character offsets and slices. `chunk_text(text)` uses128 tokens with16-token overlap. Byte-fallback tokens sharing a Unicode character stay together; an impossible token budget returns an error. One immutable tokenizer supplies encodes and untruncated chunks; truncation reserves the pinned BOS/EOS template rather than cloning the vocabulary or mutating per-call options.

The model is FP32. DSP follows the reference's F32 Hann/frame multiply, F64 FFT/mel calculations and final F32 feature tensors. Every model vector is checked for finite elements and a finite nonzero norm. The quality helper rejects nonfinite dot products/norms/cosines before clamping; NaN cannot become a passing zero error.

Private qualification data, model weights and build outputs are excluded from this crate. The consuming workspace owns its lockfile and optimization profile. Use optimized builds for DSP workloads; release mode also optimizes FFT/tokenizer/codec dependencies. Development-only timings are not paired application performance evidence.

Resampling numerics derive from PyTorch2.14.1 CPU antialiased uint8 bicubic (itself attributing Keys filtering to Pillow). Full upstream copyright/license notices are included in `licenses/`. Runtime ORT and dependencies retain their own upstream licenses. Model weights are not included.
