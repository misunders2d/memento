"""Exact Unicode passage spans shared by raw-episode and document indexing."""

from typing import NamedTuple


class TextChunk(NamedTuple):
    start: int
    end: int
    text: str


def chunks(text: str, tokenizer, size: int = 128, overlap: int = 16) -> list[TextChunk]:
    """Bound inference tokens while retaining exact, overlapping Unicode spans."""
    if size <= 0 or not 0 <= overlap < size:
        raise ValueError('Chunk size must exceed its nonnegative overlap')
    if not text.strip():
        return []
    offsets = tokenizer(text, add_special_tokens=False, return_offsets_mapping=True,
                        truncation=False)['offset_mapping']
    if not offsets:
        return [TextChunk(0, len(text), text)]
    out = []
    start = 0
    while start < len(offsets):
        end = min(start + size, len(offsets))
        # Byte-fallback tokens can share one Unicode character: never split it.
        while end < len(offsets) and end > start and offsets[end][0] < offsets[end - 1][1]:
            end -= 1
        if end == start:
            raise ValueError('A Unicode character exceeds the passage token budget')
        left = offsets[start][0] if start else 0
        right = offsets[end][0] if end < len(offsets) else len(text)
        while left < right and text[left].isspace():
            left += 1
        while right > left and text[right - 1].isspace():
            right -= 1
        if left < right:
            out.append(TextChunk(left, right, text[left:right]))
        if end == len(offsets):
            break
        nxt = max(start + 1, end - overlap)
        while nxt < end and offsets[nxt][0] < offsets[nxt - 1][1]:
            nxt += 1
        start = nxt
    return out
