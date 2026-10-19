import hashlib
from io import BytesIO

import pytest

from memento.blobs import BlobStore


def test_content_addressed_artifact_and_corruption_repair(tmp_path):
    blobs = BlobStore(tmp_path)
    content = b'image\x00original\r\n'
    info = blobs.put(BytesIO(content))
    assert info['sha256'] == hashlib.sha256(content).hexdigest()
    assert blobs.open(info['sha256']).read() == content
    blobs.path(info['sha256']).write_bytes(b'corrupted')
    with pytest.raises(OSError, match='Corrupt blob'):
        blobs.open(info['sha256'])
    assert blobs.put(BytesIO(content)) == info
    assert blobs.open(info['sha256']).read() == content


@pytest.mark.parametrize('value', ['../escape', 'a'*63, 'A'*64, '/etc/passwd'])
def test_blob_path_rejects_non_digest(tmp_path, value):
    with pytest.raises(ValueError, match='SHA-256'):
        BlobStore(tmp_path).path(value)
