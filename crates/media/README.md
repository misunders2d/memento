# memento-media

Native image/audio/video/PDF and document ingestion into the Memento authority. The public constructor requires the pinned full `memento_embedding::EmbeddingModel`; there is no runtime model substitution or Python decoder.

```rust
let ingest = MediaIngest::new(graph, model, pdfium_library_path)
    .with_reader(Arc::new(move |sha| cold_manager.reader(sha)));
let media_handler = ingest.handler();
```

`captured::CapturedMedia::new(queue).step()` backfills at most64 imported episodes and admits at most2 outstanding automatic media jobs. It uses core's exact attachment-reference parser and the same representation identity, provenance and forgetting guards as ordinary ingestion. Run it through existing maintenance after model readiness.

`handler()` implements the existing domain jobs handler. Codec work and one background model unit run outside the writer. Each complete unit is persisted in the core `media_stage_units` derived table with exact3072-byte F32 data and artifact/page/time/Unicode-character metadata. A final lease/source/model-fenced transaction publishes the media node, segments, vectors, representation marker and dirty events, then deletes only that job's staging rows and clears its context. An interrupted unit is retried; completed staged units resume without another encoder call. A crash after final graph commit reuses the existing command result. Failed decoding preserves the original bytes and reports failure.

Use the core DDL for `media_stage_units`; this crate does not create authority tables. Pending/leased job payload and staged artifact/source SHA references must participate in normal eviction protection. `ColdManager::reader` performs verified rehydration outside transactions. Original reader leases remain live; previews are preflighted one at a time to keep file descriptors bounded. Portable restore preserves both stage rows and job context, or must discard both atomically before restarting from the verified original.

New ingests use decoder representation2, binding the pinned model, decoder pipeline, kind and document suffix. Only an active, complete representation with that identity can supply reused units. Existing older representations, exact vectors, original artifacts and segments remain unchanged and searchable; changing implementation does not schedule corpus re-embedding.

Images apply native JPEG draft decoding, EXIF orientation, transparency on white and source-compatible Lanczos thumbnails; HEIF uses libheif's container transforms and the pinned source plugin's EXIF-reset behavior. AVIF uses pinned libavif1.4.2 with source-compatible color/alpha conversion and container orientation. JPEG2000 uses native FFmpeg; 16-bit grayscale shares PNG/TIFF's source-compatible clamp. Format dispatch reads bytes so captured filenames need no suffix. Preview JPEGs use quality90 and4:2:0. Raw camera formats remain unsupported as in the source. PDFium uses one process-owned, checksum-pinned library handle, with native calls protected by the binding's thread-safe adapter; repeated documents reuse it. PDFium selects text at200 characters or renders the page at1400px; pages are one-based, capped5000. Documents retain source UTF8 projections and exact tokenizer character spans; Office/OpenDocument ZIP members are streamed under36MB budgets, projections bounded by the existing9M-character policy. HTML, RTF, notebooks and source plain-text encodings are supported.

Audio decodes to mono16kHz float32, aligns container timestamps, pads gaps, trims leading samples and clips after the source's stereo downmix. Thirty-second windows hop25s, retain tails with at least2s new audio, reject RMS<=0.003 and count silence toward2160 windows. Video samples shots every3s or duration/1200, deduplicates the source8x8 hash, preserves display rotation, and retains actual frame-to-next-shot time ranges. Artifacts are real JPEG/WAV/original blobs, with no caption substitution.

Native dependencies are FFmpeg9 APIs, libheif>=1.17, libavif1.4.2, TurboJPEG3 and a compatible PDFium library. Build/runtime packaging must verify actual symbols, ABI, transitive libraries and licenses. A host library cannot be copied into an older glibc image merely because its SONAME matches. Production should use an optimized build for image/audio processing. The private encoder seam exists for pipeline-control tests; public construction always uses the real model.

Thumbnail numerics derive from Pillow12.3.0; its copyright/license notice is included in `licenses/Pillow`. Native dependencies keep their own licenses; no native libraries or model weights are bundled in this source crate.
