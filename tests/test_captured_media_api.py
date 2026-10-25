"""Real service wiring indexes a captured native attachment without an explicit media command."""

import hashlib
import io
import json
import re
import time

import numpy as np
import pytest
from fastapi.testclient import TestClient
from PIL import Image

from memento.api import create_app
from memento.embedder import EMBED_DIM


class Model:
    """Only model loading and encoding are replaced; decoding and commits stay real."""

    @staticmethod
    def tokenizer(text, **kwargs):
        return {'offset_mapping': [match.span() for match in re.finditer(r'\S+', text)]}

    def encode(self, inputs, **kwargs):
        return np.ones((len(inputs), EMBED_DIM), np.float32)


def png():
    content = io.BytesIO()
    Image.new('RGB', (4, 3), (200, 40, 10)).save(content, 'PNG')
    return content.getvalue()


@pytest.mark.parametrize('blob_first', [True, False])
def test_captured_attachment_indexes_through_real_worker_in_either_order(tmp_path, monkeypatch, blob_first):
    assert_indexes(tmp_path, monkeypatch, blob_first)


def test_failing_raw_compression_does_not_starve_captured_media(tmp_path, monkeypatch, caplog):
    def failing(self):
        raise RuntimeError('private raw detail')
    monkeypatch.setattr('memento.store.Store.compress_raw_step', failing)
    assert_indexes(tmp_path, monkeypatch, True)
    assert 'Raw storage compression failed (RuntimeError)' in caplog.text
    assert 'private raw detail' not in caplog.text


def assert_indexes(tmp_path, monkeypatch, blob_first):
    monkeypatch.setenv('MEMENTO_EMBEDDINGS', '1')
    monkeypatch.setattr('memento.retrieval.load_model', lambda *args, **kwargs: Model())
    original = png()
    sha = hashlib.sha256(original).hexdigest()
    stub = dict(type='image', mime='image/png', sha256=sha, artifact_status='queued', blob_url='/blobs/' + sha)
    episode = dict(source='native:fixture:pi', event_id='attachment', conversation_id='chat', kind='user',
        text=json.dumps([stub]), occurred_at='2026-10-23T10:00:00+00:00',
        metadata=dict(extraction_mode='message', native_id='native-record', artifacts=[stub]))
    with TestClient(create_app(tmp_path)) as client:
        def upload():
            response = client.post('/blobs', content=original, headers={'Content-Type': 'image/png'})
            assert response.status_code == 201 and response.json()['sha256'] == sha

        def attachments():
            return client.get('/status').json()['media']['captured_attachments']

        if blob_first:
            upload()
        captured = client.post('/episodes', json=episode)
        assert captured.status_code == 201
        episode_id = captured.json()['id']
        if not blob_first:
            waiting = attachments()
            assert waiting['waiting'] == 1 and waiting['queued'] == waiting['indexed'] == 0
            upload()
        deadline = time.monotonic() + 15
        while attachments()['indexed'] != 1:
            assert time.monotonic() < deadline, attachments()
            time.sleep(0.05)
        state = client.app.state
        with state.store._mutex:
            db = state.store._db
            row = db.execute('SELECT job_id,node_id FROM captured_media WHERE sha256=?', (sha,)).fetchone()
            refs = [r[0] for r in db.execute('SELECT episode_id FROM captured_media_refs WHERE sha256=?', (sha,))]
            vectors = db.execute('''SELECT s.modality,s.artifact_sha256,v.embedding FROM segments s
                JOIN vector_items i ON i.target=s.id AND i.kind='segment'
                JOIN memory_vectors v ON v.rowid=i.seq AND i.current=1 WHERE s.node_id=?''',
                (row['node_id'],)).fetchall()
            assert db.execute('SELECT sha256 FROM media_files WHERE node_id=?', (row['node_id'],)).fetchone()[0] == sha
            assert db.execute('SELECT 1 FROM media_representations WHERE node_id=?', (row['node_id'],)).fetchone()
            assert db.execute('SELECT count(*) FROM jobs').fetchone()[0] == 1
            assert db.execute("""SELECT count(*) FROM graph_commits
                WHERE json_extract(request,'$.command.op')<>'media_ingest'""").fetchone()[0] == 0
        assert refs == [episode_id]
        assert [(v['modality'], v['artifact_sha256']) for v in vectors] == [('image', sha)]
        assert np.isclose(np.linalg.norm(np.frombuffer(vectors[0]['embedding'], np.float32)), 1)
        job = client.get(f"/jobs/{row['job_id']}").json()
        assert job['state'] == 'done' and job['error'] is None and job['result']['id'] == row['node_id']
        version = client.get(f"/history/{row['node_id']}").json()['versions'][0]
        assert version['scope'] == [] and version['evidence'][0]['episode_id'] == episode_id
        assert version['evidence'][0]['quote'] == sha
        assert client.get(f'/blobs/{sha}').content == original
        assert client.get(f'/episodes/{episode_id}').json()['metadata'] == episode['metadata']
