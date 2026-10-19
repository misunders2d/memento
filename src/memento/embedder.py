"""EmbeddingGemma 2 on CPU (float32): query, text, image and audio vectors in one vector space.

One vector per natural unit, never mixed (DigUp). Prompts (model card): a query gets "task: search result | query: ",
a document "title: {title or 'none'} | text: {text}" (built by the caller), media get no prompt.
"""

from __future__ import annotations

import os
from threading import RLock
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from PIL import Image
    from sentence_transformers import SentenceTransformer

EMBED_DIM = 768
AUDIO_SR = 16000
AUDIO_MAX_SAMPLES = 30 * AUDIO_SR  # the feature extractor silently cuts one audio item to its first 30 s
TEXT_BATCH = 1  # Bound CPU activations under the service's 2-CPU/5-GiB budget.
IMAGE_BATCH = 4
AUDIO_BATCH = 2


def load_model(model_id: str, revision: str, *, text_only: bool) -> SentenceTransformer:
    import torch
    # One CPU thread in each of the bounded foreground/background lanes avoids
    # oversubscribing the production two-CPU quota during automatic recall.
    torch.set_num_threads(int(os.environ.get('OMP_NUM_THREADS', '1')))
    from sentence_transformers import SentenceTransformer

    class _CPUModel(SentenceTransformer):
        def __init__(self, *args, **kwargs):
            self.tokenization_lock = RLock()
            super().__init__(*args, **kwargs)

        def preprocess(self, *args, **kwargs):
            # Fast-tokenizer padding/truncation settings are mutable. Serialize
            # preprocessing, not the read-only forward pass shared by queries
            # and one background encoder.
            with self.tokenization_lock:
                return super().preprocess(*args, **kwargs)

    # dtype=float32 is MANDATORY: the checkpoint's config default is bfloat16 (loaded silently if omitted, and
    # its vectors drift from an fp32 index); the model card forbids float16.
    # Text-only query vectors are bit-identical to full-model query vectors (verified).
    config_kwargs = {"vision_config": None, "audio_config": None} if text_only else None
    return _CPUModel(model_id, revision=revision, device="cpu",
                     model_kwargs={"dtype": torch.float32}, config_kwargs=config_kwargs).eval()


def _unit_rows(v: np.ndarray) -> np.ndarray:
    v = np.asarray(v, dtype=np.float32).reshape(-1, EMBED_DIM)
    n = np.linalg.norm(v, axis=1, keepdims=True)
    return v / np.where(n > 0, n, 1.0)


def _empty() -> np.ndarray:
    return np.zeros((0, EMBED_DIM), np.float32)


QUERY_PROMPT = "task: search result | query: "  # the model's "SearchQuery" prompt (identical vectors, verified)


def _as_text(texts: list[str]) -> list[dict]:
    """Text as {"text": ...}: sentence-transformers reads a BARE string that looks like a URL or a media file path as
    an image/audio/video and fetches or opens it (verified: a query "https://example.invalid/x.jpg" made a network
    call). Queries come from users, so they are always passed this way."""
    return [{"text": t} for t in texts]


def embed_query(model: SentenceTransformer, text: str) -> np.ndarray:
    """(768,) float32, L2-normalized."""
    return _unit_rows(model.encode(_as_text([QUERY_PROMPT + text]), convert_to_numpy=True))[0]


def embed_texts(model: SentenceTransformer, texts: list[str]) -> np.ndarray:
    """(n, 768). The texts are already prompted ("title: ... | text: ..."); nothing is added here."""
    if not texts:
        return _empty()
    return _unit_rows(model.encode(_as_text(list(texts)), batch_size=TEXT_BATCH, convert_to_numpy=True))


def embed_images(model: SentenceTransformer, images: list[Image.Image], budget: int) -> np.ndarray:
    """(n, 768): one vector per image at `budget` vision soft tokens (70/140/280/560/1120)."""
    if not images:
        return _empty()
    return _unit_rows(model.encode([{"image": img} for img in images], batch_size=IMAGE_BATCH,
                                   processing_kwargs={"image": {"max_soft_tokens": budget}},
                                   convert_to_numpy=True))


def embed_audio(model: SentenceTransformer, waves: list[np.ndarray]) -> np.ndarray:
    """(n, 768): one vector per window. Each wave must already be mono 16 kHz, at most 30 s.

    The processor never checks or resamples the rate (anything is read as 16 kHz), and it silently drops audio past
    30 s of one item, so a longer wave is refused here instead.
    """
    if not waves:
        return _empty()
    items = []
    for w in waves:
        a = np.ascontiguousarray(w, dtype=np.float32).reshape(-1)
        if not 0 < len(a) <= AUDIO_MAX_SAMPLES:
            raise ValueError(f"an audio window must hold 1..{AUDIO_MAX_SAMPLES} samples (30 s at 16 kHz)")
        items.append({"audio": a})
    return _unit_rows(model.encode(items, batch_size=AUDIO_BATCH, convert_to_numpy=True))


def prefetch(model_id: str, revision: str) -> None:
    """Download the embedding model into the HF cache (needs network)."""
    from huggingface_hub import snapshot_download

    snapshot_download(model_id, revision=revision)
