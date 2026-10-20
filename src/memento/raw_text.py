"""Lossless physical elision of long opaque runs from skipped native records."""

import hashlib
import io
import json
import re

MIME = 'application/x-memento-raw-run'
KINDS = ('native_record', 'native_live_record', 'native_hook')
OPAQUE = re.compile(r'[A-Za-z0-9+/]{65536,}={0,2}')
_SHA = re.compile(r'[0-9a-f]{64}')


class RawTextUnavailable(RuntimeError):
    def __init__(self):
        super().__init__('Original raw source artifact is unavailable or corrupt')


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
