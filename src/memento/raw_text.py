"""Lossless physical elision of long opaque runs from skipped native records."""

import hashlib
import io
import json
import re
import zlib
import codecs

MIME = 'application/x-memento-raw-run'
KINDS = ('native_record', 'native_live_record', 'native_hook')
FRAME_BYTES = 256 * 1024
OPAQUE = re.compile(r'([A-Za-z0-9+/]+)={0,2}')
_SHA = re.compile(r'[0-9a-f]{64}')


class RawTextUnavailable(RuntimeError):
    def __init__(self):
        super().__init__('Original raw source artifact is unavailable or corrupt')


def frame_refs(value):
    if not isinstance(value, str) or len(value) > 1024 * 1024:
        raise ValueError('Invalid raw frame references')
    result = json.loads(value)
    if not isinstance(result, list) or not result:
        raise ValueError('Invalid raw frame references')
    for item in result:
        if (not isinstance(item, list) or len(item) != 3
                or any(type(number) is not int for number in item)
                or item[0] <= 0 or item[1] < 0 or item[2] < 0
                or item[1] + item[2] > FRAME_BYTES):
            raise ValueError('Invalid raw frame references')
    return result


def decode_frame(payload, size, sha256):
    """Never allocate according to an unchecked compressed stream's size."""
    if (type(size) is not int or not 0 <= size <= FRAME_BYTES
            or not isinstance(sha256, str) or not _SHA.fullmatch(sha256)
            or not isinstance(payload, bytes) or len(payload) > FRAME_BYTES + 1024):
        raise ValueError('Invalid raw frame')
    decoder = zlib.decompressobj()
    result = decoder.decompress(payload, size + 1)
    if (len(result) != size or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail
            or hashlib.sha256(result).hexdigest() != sha256):
        raise ValueError('Invalid raw frame')
    return result


def frame_parts(references, full_sha, source, conversation, reader):
    try:
        if not isinstance(full_sha, str) or not _SHA.fullmatch(full_sha):
            raise ValueError('Invalid original raw checksum')
        digest = hashlib.sha256()
        for identity, offset, length in frame_refs(references):
            frame = reader(identity)
            if frame is None or frame['source'] != source or frame['conversation_id'] != conversation:
                raise ValueError('Raw frame belongs to another conversation')
            decoded = frame['decoded']
            if offset + length > len(decoded):
                raise ValueError('Raw frame slice is outside its payload')
            part = decoded[offset:offset + length]
            digest.update(part)
            yield part
        if digest.hexdigest() != full_sha:
            raise ValueError('Invalid reconstructed raw text')
    except Exception:
        raise RawTextUnavailable() from None


def hydrate_frames(references, full_sha, source, conversation, reader):
    try:
        return b''.join(frame_parts(references, full_sha, source, conversation, reader)).decode('utf-8')
    except Exception:
        raise RawTextUnavailable() from None


def validate_frames(db):
    """Validate private archive frames with one decoded-frame cache, never all raw text."""
    columns = {row[1] for row in db.execute('PRAGMA table_info(episodes)')}
    if 'raw_frame' not in columns:
        return  # Pre-frame archives remain readable without a schema upgrade.
    if not db.execute("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='raw_frames'").fetchone():
        raise ValueError('Raw frame table is missing')
    frames = {}
    for identity, source, conversation, size, count, sha, packed_size in db.execute(
            'SELECT id,source,conversation_id,raw_size,record_count,sha256,length(data) FROM raw_frames'):
        if (type(size) is not int or not 0 <= size <= FRAME_BYTES or type(count) is not int or count < 0
                or not isinstance(sha, str) or not _SHA.fullmatch(sha)
                or type(packed_size) is not int or not 0 < packed_size <= FRAME_BYTES + 1024):
            raise ValueError('Invalid raw frame metadata')
        frames[identity] = dict(source=source, conversation_id=conversation, raw_size=size,
                                record_count=count, sha256=sha, used=0, count=0, referenced=False)
    cached = (None, None)
    def read(identity):
        nonlocal cached
        if cached[0] != identity:
            frame = frames[identity]
            payload = db.execute('SELECT data FROM raw_frames WHERE id=?', (identity,)).fetchone()[0]
            cached = (identity, dict(frame, decoded=decode_frame(payload, frame['raw_size'], frame['sha256'])))
        return cached[1]
    try:
        for source, conversation, kind, metadata, text, refs, full_sha, opaque in db.execute('''SELECT
                source,conversation_id,kind,metadata,text,raw_frame,text_sha256,text_runs
                FROM episodes WHERE raw_frame IS NOT NULL ORDER BY seq'''):
            if (kind not in KINDS or json.loads(metadata).get('extraction_mode') != 'skip'
                    or text != '' or opaque is not None):
                raise ValueError('Invalid compressed raw source')
            references = frame_refs(refs)
            for identity, offset, length in references:
                frame = frames[identity]
                if offset != frame['used']:
                    raise ValueError('Raw frame slices overlap or leave a gap')
                frame['used'] += length
                frame['referenced'] = True
            frames[references[0][0]]['count'] += 1
            decoder = codecs.getincrementaldecoder('utf-8')()
            for part in frame_parts(refs, full_sha, source, conversation, read):
                decoder.decode(part)
            decoder.decode(b'', final=True)
        if any(not f['referenced'] or f['used'] != f['raw_size'] or f['count'] != f['record_count']
               for f in frames.values()):
            raise ValueError('Raw frame coverage differs from its records')
    except (ValueError, KeyError, RawTextUnavailable, zlib.error):
        raise ValueError('Invalid compressed raw source coverage or checksum') from None


def runs(value):
    result = json.loads(value)
    if not isinstance(result, list) or not result:
        raise ValueError('Invalid raw text runs')
    end = 0
    for item in result:
        if (not isinstance(item, list) or len(item) != 3 or type(item[0]) is not int
                or not isinstance(item[1], str) or not _SHA.fullmatch(item[1])
                or type(item[2]) is not int or item[0] < end or item[2] < 65536):
            raise ValueError('Invalid raw text runs')
        end = item[0] + item[2]
    return result


def elide(text, blobs):
    skeleton, references, artifacts = [], [], []
    cursor = 0
    for match in OPAQUE.finditer(text):
        if match.end(1) - match.start(1) < 65536:
            continue
        info = blobs.put(io.BytesIO(match.group().encode('ascii')))
        skeleton.append(text[cursor:match.start()])
        references.append([match.start(), info['sha256'], match.end() - match.start()])
        artifacts.append(info)
        cursor = match.end()
    if not references:
        return text, None, None, []
    skeleton.append(text[cursor:])
    return (''.join(skeleton), json.dumps(references, separators=(',', ':')),
            hashlib.sha256(text.encode('utf-8')).hexdigest(), artifacts)


def hydrate(skeleton, references, full_sha, reader):
    try:
        pieces, cursor, original = [], 0, 0
        for offset, sha256, length in runs(references):
            count = offset - original
            if cursor + count > len(skeleton):
                raise ValueError('Invalid raw text skeleton')
            pieces.append(skeleton[cursor:cursor + count])
            with reader(sha256) as source:
                payload = source.read(length + 1)
            if len(payload) != length or hashlib.sha256(payload).hexdigest() != sha256:
                raise ValueError('Invalid raw text artifact')
            pieces.append(payload.decode('ascii'))
            cursor += count
            original = offset + length
        pieces.append(skeleton[cursor:])
        text = ''.join(pieces)
        if hashlib.sha256(text.encode('utf-8')).hexdigest() != full_sha:
            raise ValueError('Invalid reconstructed raw text')
        return text
    except Exception:
        raise RawTextUnavailable() from None
