"""Video/audio reading with PyAV (bundled FFmpeg; no ffmpeg CLI).

Reads local files only (the indexer downloads each file to tmp/ first). Indexing units (ports of DigUp
`MediaExtractors.swift`): `keyframes` (one frame per shot) and `audio_windows` (30 s windows every 25 s).
"""
from __future__ import annotations

import io
import math
from pathlib import Path

import av
import numpy as np
from PIL import Image

SAMPLE_RATE = 16_000


def _open(src: str | Path):
    return av.open(str(src))


def _t0(c) -> float:
    """Container start time in seconds; window times are relative to it (like ffmpeg -ss)."""
    return (c.start_time or 0) / av.time_base


def probe_duration(src: str | Path) -> float:
    with _open(src) as c:
        if c.duration is not None:
            return c.duration / av.time_base
        for s in c.streams:
            if s.duration is not None and s.time_base is not None:
                return float(s.duration * s.time_base)
    raise ValueError("no duration in container")


def _duration(c) -> float:
    if c.duration is not None:
        return c.duration / av.time_base
    for s in c.streams:
        if s.duration is not None and s.time_base is not None:
            return float(s.duration * s.time_base)
    return math.nan


def _jpeg_backed(img: Image.Image) -> Image.Image:
    """The image as an in-memory JPEG (q90, like DigUp's temporary frame files), decoded lazily on first use:
    ~100 KB per frame instead of ~2 MB, so 1200 frames of a long video don't hold gigabytes."""
    buf = io.BytesIO()
    img.convert("RGB").save(buf, format="JPEG", quality=90)
    buf.seek(0)
    return Image.open(buf)


def average_hash(img: Image.Image) -> int:
    """8×8 grayscale average hash: bit i (row-major) is set where pixel i is brighter than the integer mean."""
    pixels = img.convert("L").resize((8, 8), Image.Resampling.BOX).tobytes()
    mean = sum(pixels) // len(pixels)
    return sum(1 << i for i, p in enumerate(pixels) if p > mean)


def keyframes(path, every: float = 3.0, max_frames: int = 1200,
              max_px: int = 1024) -> list[tuple[float, float, Image.Image]]:
    """Samples a frame every `every` seconds and keeps one per shot: a frame that looks like the previous kept one
    (8×8 average hash within 4 bits) only extends it. Each kept frame stands for the stretch until the next kept
    frame (the last one until the end). Frames fit in `max_px`, display rotation applied.
    Port of DigUp `VideoExtractor.keyframes`; images are JPEG-backed (see `_jpeg_backed`)."""
    frames: list[list] = []  # [start, end, image]
    with _open(path) as c:
        duration = _duration(c)
        if not c.streams.video or not math.isfinite(duration) or duration <= 0:
            return []
        vs = c.streams.video[0]
        vs.thread_type = "AUTO"
        base = _t0(c)
        step = max(every, duration / max_frames)
        tol = step / 2  # DigUp's requestedTimeTolerance before/after
        last_hash = None
        t = min(0.5, duration / 2)
        while t < duration:
            target, t = base + t, t + step
            try:
                c.seek(int(target / vs.time_base), stream=vs, backward=True)  # keyframe <= target
                frame = None
                for f in c.decode(vs):
                    if f.time is not None and f.time >= target - tol:
                        frame = f if f.time <= target + tol else None
                        break
            except av.error.FFmpegError:  # an unreadable stretch fails one sample, like a failed generator result
                continue
            if frame is None:
                continue
            scale = min(1.0, max_px / max(frame.width, frame.height))
            img = frame.to_image(width=max(1, round(frame.width * scale)),
                                 height=max(1, round(frame.height * scale)), interpolation="AREA")
            if frame.rotation:  # display matrix: CCW angle; PIL rotate() is CCW too
                img = img.rotate(frame.rotation, expand=True)
            h = average_hash(img)
            if last_hash is not None and (h ^ last_hash).bit_count() <= 4:
                continue
            last_hash = h
            frames.append([max(0.0, frame.time - base), duration, _jpeg_backed(img)])
    for i in range(len(frames) - 1):
        frames[i][1] = frames[i + 1][0]
    return [(s, e, img) for s, e, img in frames]


def _decoded(c, stream):
    """The stream's frames, packet by packet: a packet the decoder rejects (a damaged stretch of the file) is
    skipped instead of failing the whole file."""
    for packet in c.demux(stream):
        try:
            yield from packet.decode()
        except av.error.InvalidDataError:
            continue


def _silence(n: int):
    """`n` samples of silence, a second at a time: a bogus timestamp far ahead must not allocate it all at once
    (the window cap in iter_audio_windows counts silent windows and stops the read)."""
    while n > 0:
        yield np.zeros(min(n, SAMPLE_RATE), np.float32)
        n -= SAMPLE_RATE


def _mono_16k(c, a):
    """Decoded audio as mono 16 kHz float32 chunks (downmix 0.5*(L+R), clipped), aligned to the container start
    a late first sample is padded with silence, an early one trimmed. A gap after skipped damaged packets is filled
    with silence too, so later audio keeps its time."""
    t0 = _t0(c)
    stereo = a.codec_context.layout.nb_channels > 1
    rs = av.AudioResampler(format="fltp", layout="stereo" if stereo else "mono", rate=SAMPLE_RATE)
    trim = None  # samples still to drop (negative lead)
    out = 0      # samples yielded so far

    def mono(frames):
        for r in frames:
            pcm = r.to_ndarray()
            yield np.clip(0.5 * (pcm[0] + pcm[1]) if stereo else pcm[0], -1.0, 1.0).astype(np.float32)

    for f in _decoded(c, a):
        if f.time is None:
            continue
        at = round((f.time - t0) * SAMPLE_RATE)
        if trim is None:
            if at > 0:
                yield from _silence(at)
                out += at
            trim = max(0, -at)
        elif at - out > SAMPLE_RATE // 10:   # damaged packets were skipped before this frame
            yield from _silence(at - out)
            out = at
        for chunk in mono(rs.resample(f)):
            if trim:
                cut = min(trim, len(chunk))
                chunk, trim = chunk[cut:], trim - cut
            out += len(chunk)
            yield chunk
    for chunk in mono(rs.resample(None)):  # flush
        if trim:
            cut = min(trim, len(chunk))
            chunk, trim = chunk[cut:], trim - cut
        yield chunk


# Audio windows read per file at most (about 15 hours at a 25 s hop): video frames have such a cap too (1,200), and a
# tiny, very long compressed file must not run the indexer for ever.
MAX_AUDIO_WINDOWS = 2160


def audio_windows(path, window: float = 30.0, hop: float = 25.0, min_tail: float = 2.0,
                  rms_gate: float = 0.003) -> list[tuple[float, float, np.ndarray]]:
    """`iter_audio_windows` as a list (small files and tests; the indexer streams them)."""
    return list(iter_audio_windows(path, window, hop, min_tail, rms_gate))


def iter_audio_windows(path, window: float = 30.0, hop: float = 25.0, min_tail: float = 2.0,
                       rms_gate: float = 0.003, max_windows: int = MAX_AUDIO_WINDOWS):
    """`window`-second pieces of mono 16 kHz audio `hop` seconds apart, skipping silent ones (RMS <= `rms_gate`,
    ≈ −50 dBFS). One model input ignores anything past 30 s, so windows stay ≤ 30 s. The tail is kept when the new
    audio beyond the previous window's overlap (or the whole file, if shorter than a window) is ≥ `min_tail` s.
    Streams the decode and yields window by window; stops after `max_windows` windows (silent ones included). Port of DigUp `AudioExtractor.windows`."""
    win, hop_n = int(window * SAMPLE_RATE), int(hop * SAMPLE_RATE)
    offset = 0  # sample index of buf[0] in the file
    decoded = 0

    def emit(buf: np.ndarray, count: int):
        piece = buf[:count]
        if piece.size and float(np.sqrt(np.mean(np.square(piece, dtype=np.float64)))) > rms_gate:
            return (offset / SAMPLE_RATE, (offset + count) / SAMPLE_RATE, piece.copy())
        return None

    pending: list[np.ndarray] = []
    have = 0
    with _open(path) as c:
        if not c.streams.audio:
            return
        for chunk in _mono_16k(c, c.streams.audio[0]):
            pending.append(chunk)
            have += len(chunk)
            if have < win:
                continue
            buf = np.concatenate(pending)
            while len(buf) >= win:
                if (piece := emit(buf, win)) is not None:
                    yield piece
                decoded += 1  # silent windows count too: hours of silence must not be decoded for ever
                if decoded >= max_windows:
                    return
                buf = buf[hop_n:]
                offset += hop_n
            pending, have = [buf], len(buf)
    buf = np.concatenate(pending) if pending else np.zeros(0, np.float32)
    covered = 0 if offset == 0 else win - hop_n
    if len(buf) - covered >= int(min_tail * SAMPLE_RATE) and (piece := emit(buf, len(buf))) is not None:
        yield piece
