"""Consumed media images release decoded pixels while originals and vectors survive."""

import hashlib
import io
from threading import RLock
from types import SimpleNamespace

import numpy as np
from PIL import Image
import pytest

from memento.extract import Page, PdfRead, _jpeg_backed
from memento.graph import Graph
from memento.ingest import MediaIngest
from memento.store import Store


@pytest.mark.parametrize('kind', ['document', 'video'])
@pytest.mark.parametrize('fail_at', [None, 2])
def test_consumed_pdf_pages_and_video_frames_release_pixels(kind, fail_at, tmp_path, monkeypatch):
    store = Store(tmp_path)
    images, expected_derivatives = [], []
    try:
        for index in range(6):
            with Image.new('RGB', (64, 64), (index * 30, 90, 120)) as original:
                image = _jpeg_backed(original)
            images.append(image)
            # Derive the expected bytes from a separate image so the retained
            # input objects remain undecoded until the real ingest helper runs.
            with Image.open(io.BytesIO(image.fp.getvalue())) as expected:
                content = io.BytesIO()
                expected.convert('RGB').save(content, 'JPEG', quality=90)
                expected_derivatives.append(content.getvalue())
        assert all(image._im is None for image in images)
        if kind == 'document':
            monkeypatch.setattr('memento.ingest.pdf_pages', lambda path: PdfRead(
                [Page(index + 1, '', image) for index, image in enumerate(images)], len(images), None))
        else:
            monkeypatch.setattr('memento.ingest.keyframes', lambda path:
                [(float(index), float(index + 1), image) for index, image in enumerate(images)])
            monkeypatch.setattr(MediaIngest, '_audio', lambda self, path: iter(()))
        calls, vectors = [], []
        vector = np.zeros(768, np.float32)
        vector[0] = 1

        def encode(model, batch, budget):
            assert budget == 70 and batch == [images[len(calls)]]
            for consumed in images[:len(calls)]:
                with pytest.raises(ValueError, match='closed'):
                    consumed.getpixel((0, 0))
            calls.append(batch[0].getpixel((0, 0)))
            if len(calls) - 1 == fail_at:
                raise ValueError('Synthetic encoder failure')
            return vector[None, :].copy()

        monkeypatch.setattr('memento.ingest.embed_images', encode)
        graph = Graph(store)
        retrieval = SimpleNamespace(graph=graph, _inference=RLock(), model=object(),
                                    _put=lambda *args: vectors.append(args[-1]))
        ingest = MediaIngest(retrieval, store.artifacts)
        original_bytes = b'Exact immutable original media\x00\xff\r\n'
        info = store.blobs.put(io.BytesIO(original_bytes))
        store.artifacts.register(info)
        source, _ = store.capture(dict(source='fixture', event_id='save', conversation_id='current',
            kind='user', text='Save this original media.', occurred_at=None, metadata={}))
        arguments = dict(command_id='save-media', actor='fixture', why='Explicit original save',
            sha256=info['sha256'], kind=kind, filename='original.pdf' if kind == 'document' else 'original.mp4',
            scope=['personal'], evidence=[dict(episode_id=source['id'], quote='Save this original media.')])
        if fail_at is None:
            result = ingest.ingest(**arguments)
            assert result['blob_url'] == '/blobs/' + info['sha256']
            assert len(result['segments']) == len(images)
            assert len(vectors) == len(images) and all(np.array_equal(item, vector) for item in vectors)
        else:
            with pytest.raises(ValueError, match='Synthetic encoder failure'):
                ingest.ingest(**arguments)
            assert vectors == []
            assert all(image._im is None for image in images[fail_at + 1:])
        assert store.blobs.open(info['sha256']).read() == original_bytes
        for image in images[:len(calls)]:
            with pytest.raises(ValueError, match='closed'):
                image.getpixel((0, 0))
        for content in expected_derivatives[:len(calls)]:
            sha256 = hashlib.sha256(content).hexdigest()
            assert store.blobs.open(sha256).read() == content
    finally:
        for image in images:
            image.close()
        store.close()
