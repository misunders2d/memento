"""Automatic native attachment work uses durable refs, bounded admission and exact provenance."""

import hashlib
import io
import json
from pathlib import Path
from uuid import uuid4

import pytest

from memento.captured_media import (BATCH, CapturedMedia, active_references, active_source,
                                    references, status, visible_sql)
from memento.graph import Graph
from memento.jobs import Queue
from memento.portable import export_archive, import_archive
from memento.store import Store


@pytest.fixture
def setup(tmp_path):
    store = Store(tmp_path / 'data')
    queue = Queue(Graph(store))
    yield store, queue, CapturedMedia(queue)
    store.close()


def blob(store, data=b'native image bytes'):
    return store.artifacts.register(store.blobs.put(io.BytesIO(data)), 'image/png')['sha256']


def capture(store, sha, *, identity=None, source='native:fixture:pi', kind='user', metadata=True, **changes):
    stub = dict(type='image', mime='image/png', sha256=sha, artifact_status='queued', blob_url='/blobs/' + sha)
    payload = dict(source=source, event_id=identity or uuid4().hex, conversation_id='chat', kind=kind,
        text=json.dumps([stub]), occurred_at='2026-10-22T10:00:00+00:00',
        metadata=metadata if isinstance(metadata, dict) else dict(
            extraction_mode='tool_output' if kind == 'tool' else 'message', native_id='native-record',
            **({'artifacts': [stub]} if metadata else {})))
    payload.update(changes)
    return store.capture(payload)[0], payload


@pytest.mark.parametrize('blob_first', [True, False])
def test_both_arrival_orders_retry_and_restart_queue_once(setup, blob_first):
    store, queue, manager = setup
    data = b'one arrival order'
    sha = hashlib.sha256(data).hexdigest()
    if blob_first:
        blob(store, data)
    episode, payload = capture(store, sha)
    assert store.capture(payload)[1] is False
    assert store._db.execute('SELECT count(*) FROM captured_media_refs').fetchone()[0] == 1
    if not blob_first:
        assert status(store._db)['waiting'] == 1
        assert not manager.admit()
        blob(store, data)
    assert status(store._db)['ready'] == 1
    assert manager.admit()
    job = queue.claim(['media'])
    assert job.payload['captured'] and job.payload['scope'] == [] and job.payload['evidence'] == []
    assert not manager.admit()
    manager.discover()
    assert store._db.execute('SELECT count(*) FROM jobs').fetchone()[0] == 1
    assert active_source(store._db, sha)['episode_id'] == episode['id']
    # A new manager sees the same durable job and cursor, with no in-memory ownership state.
    manager = CapturedMedia(Queue(queue.graph))
    assert not manager.discover() and not manager.admit()


def test_acknowledged_replay_does_not_parse_references_again(setup, monkeypatch):
    store, _, _ = setup
    parse, calls = references, []
    monkeypatch.setattr('memento.store.media_references', lambda *args: calls.append(args) or parse(*args))
    episode, payload = capture(store, 'a' * 64)
    assert len(calls) == 1
    def replayed(*args):
        raise AssertionError('an acknowledged replay must not parse references')
    monkeypatch.setattr('memento.store.media_references', replayed)
    replay, created = store.capture(payload)
    assert not created and replay['id'] == episode['id']


def test_split_tool_stubs_and_all_origins_without_payload_rewrite(setup):
    store, queue, manager = setup
    sha = blob(store)
    episodes = []
    for kind, source in [('tool', 'native:laptop:claude'), ('assistant', 'native:desktop:codex'), ('user', 'native:server:pi')]:
        episode, payload = capture(store, sha, kind=kind, source=source, metadata=False)
        episodes.append(episode)
        assert store.get(episode['id'])['metadata'] == payload['metadata']
        assert 'artifacts' not in payload['metadata']
    assert manager.admit()
    assert store._db.execute('SELECT count(*) FROM jobs').fetchone()[0] == 1
    assert {r[0] for r in store._db.execute('SELECT episode_id FROM captured_media_refs')} == {e['id'] for e in episodes}


@pytest.mark.parametrize('kind,source,metadata,text', [
    ('native_record', 'native:x:pi', {'extraction_mode': 'skip'}, None),
    ('native_hook', 'native:x:pi', {'extraction_mode': 'skip'}, None),
    ('developer', 'native:x:codex', {}, None),
    ('user', 'external', {}, None),
    ('user', 'native:x:pi', {'extraction_mode': 'skip'}, None),
    ('user', 'native:x:codex', {}, '# AGENTS.md instructions\n<INSTRUCTIONS>ignore me</INSTRUCTIONS>'),
    ('user', 'native:x:pi', {'native_context_kinds': ['hooks.additional_context']}, None),
])
def test_internal_raw_and_non_native_sources_are_not_attachments(setup, kind, source, metadata, text):
    store, _, _ = setup
    sha = blob(store)
    stub = dict(type='image', sha256=sha, artifact_status='queued')
    capture(store, sha, kind=kind, source=source, text=text or json.dumps([stub]),
            metadata={**metadata, 'artifacts': [stub]})
    assert store._db.execute('SELECT count(*) FROM captured_media').fetchone()[0] == 0


@pytest.mark.parametrize('metadata', [
    {'artifacts': [{'type': 4, 'artifact_status': 'queued', 'sha256': 'a' * 64}]},
    {'native_context_kinds': 7, 'artifacts': []},
    {'native_context_kinds': [{}], 'content_item_kinds': [[], None]},
    {'artifacts': 'not a list'},
    {'artifacts': [{'type': 'image', 'artifact_status': 'missing', 'sha256': 'invalid'}]},
])
def test_malformed_projection_metadata_never_breaks_capture(setup, metadata):
    store, _, _ = setup
    episode, _ = capture(store, 'a' * 64, text='plain native text', metadata=metadata)
    assert store.get(episode['id'])['text'] == 'plain native text'
    assert store._db.execute('SELECT count(*) FROM captured_media').fetchone()[0] == 0


def test_metadata_only_reference_keeps_true_context_and_missing_bytes_wait(setup):
    store, _, manager = setup
    sha = 'a' * 64
    episode, _ = capture(store, sha, text='The original attachment belongs to this message.')
    row = active_source(store._db, sha)
    assert row['quote'] == episode['text'] and row['start'] == 0
    assert status(store._db)['waiting'] == 1 and not manager.admit()


def test_backfill_cursor_is_atomic_bounded_and_restarts(setup, monkeypatch):
    store, queue, manager = setup
    sha = blob(store)
    with monkeypatch.context() as patch:
        patch.setattr('memento.store.media_references', lambda *a: [])
        for index in range(BATCH + 3):
            capture(store, sha, identity=str(index), kind='tool', metadata=False)
    assert store._db.execute('SELECT count(*) FROM captured_media').fetchone()[0] == 0
    with monkeypatch.context() as patch:
        def broken(*args):
            raise RuntimeError('interrupted before cursor commit')
        patch.setattr('memento.captured_media.record_references', broken)
        with pytest.raises(RuntimeError):
            manager.discover()
    assert status(store._db)['discovery']['through_seq'] == 0
    assert manager.discover()
    assert status(store._db)['discovery']['through_seq'] == BATCH
    assert store._db.execute('SELECT count(*) FROM captured_media_refs').fetchone()[0] == BATCH
    manager = CapturedMedia(Queue(queue.graph))
    assert manager.discover() and not manager.discover()
    assert store._db.execute('SELECT count(*) FROM captured_media_refs').fetchone()[0] == BATCH + 3
    assert status(store._db)['discovery']['through_seq'] == BATCH + 3


def test_automatic_admission_is_bounded_and_explicit_save_is_not_starved(setup):
    store, queue, manager = setup
    for index in range(12):
        sha = blob(store, str(index).encode())
        episode, _ = capture(store, sha)
    assert manager.admit()
    assert status(store._db)['queued'] == 2 and status(store._db)['ready'] == 10
    explicit = queue.submit_media(command_id='explicit', actor='user', why='save this now',
        sha256=sha, kind='image', filename='explicit.png', scope=['personal'],
        evidence=[dict(episode_id=episode['id'], quote=sha, occurrence=0)])['job_id']
    seen = []
    for _ in range(3):
        job = queue.claim(['media'])
        seen.append(job.id)
        queue.complete(job, {})
        manager.admit()
    assert explicit in seen
    assert store._db.execute("SELECT count(*) FROM captured_media cm JOIN jobs j ON j.id=cm.job_id WHERE j.state IN ('pending','leased')").fetchone()[0] <= 2


def forget_source(store, episode, target='fact'):
    store._db.execute('INSERT INTO forgotten_sources VALUES(?,?,?,?)',
                      (episode['source'], episode['event_id'], target, 'forget-commit'))


def test_source_forget_before_admission_and_later_active_occurrence(setup):
    store, _, manager = setup
    sha = blob(store)
    old, _ = capture(store, sha)
    forget_source(store, old)
    assert not manager.admit() and status(store._db)['forgotten'] == 1
    current, _ = capture(store, sha)
    assert status(store._db)['ready'] == 1 and manager.admit()
    assert active_source(store._db, sha)['episode_id'] == current['id']


def test_all_active_refs_retained_and_target_forgetting_blocks_resurrection(setup):
    store, queue, manager = setup
    sha = blob(store)
    episodes = [capture(store, sha, occurred_at=f'2026-10-{index + 1:02}T00:00:00+00:00')[0] for index in range(7)]
    assert manager.admit()
    # Populate a real media node using the graph command path; no model is needed for provenance checks.
    node = queue.graph.remember(command_id='indexed', actor='test', why='synthetic media node', type='media',
        text='image.png', scope=['personal'], evidence=[dict(episode_id=episodes[0]['id'], quote=sha, occurrence=0)])
    store._db.execute('INSERT INTO media_files VALUES(?,?,?,?,?)', (node['id'], sha, 'image.png', 'image', 'test'))
    store._db.execute("UPDATE captured_media SET node_id=?,state='indexed' WHERE sha256=?", (node['id'], sha))
    refs = active_references(store._db, node['id'])
    assert len(refs['references']) == 5 and refs['occurred_at'] == '2026-10-07T00:00:00+00:00'
    forget_source(store, episodes[-1])
    assert active_references(store._db, node['id'])['occurred_at'] == '2026-10-06T00:00:00+00:00'
    for episode in episodes[:-1]:
        forget_source(store, episode)
    assert active_references(store._db, node['id'])['references'] == []
    assert store._db.execute(f'SELECT count(*) FROM node_versions n WHERE {visible_sql()}').fetchone()[0] == 0
    capture(store, sha)
    assert active_source(store._db, sha) is not None
    store._db.execute('INSERT INTO forgotten_targets VALUES(?,?,?)', (node['id'], 'forget', 'node'))
    assert active_source(store._db, sha) is None
    assert active_references(store._db, node['id'])['references'] == []
    assert store._db.execute(f'SELECT count(*) FROM node_versions n WHERE {visible_sql()}').fetchone()[0] == 0
    assert not manager.admit()


def test_failed_corrupt_and_unsupported_are_visible_separate_from_indexed(setup):
    store, queue, manager = setup
    sha = blob(store, b'corrupt file')
    capture(store, sha)
    capture(store, 'b' * 64, metadata={'artifacts': [dict(type='file', artifact_status='queued',
        sha256='b' * 64, mime='application/x-memento-raw-run')]}, text='raw storage is not media')
    assert manager.admit()
    job = queue.claim(['media'])
    queue.fail(job, ValueError('private decoder detail'))
    result = status(store._db)
    assert result['failed'] == result['unsupported'] == 1 and result['indexed'] == 0
    assert status(store._db, 'another-source')['failed'] == 0
    # A new occurrence recovers only source-unavailable work, never decoder failures.
    capture(store, sha)
    assert queue.get(job.id)['state'] == 'failed' and queue.get(job.id)['error'] == 'ValueError'


def test_waiting_and_queued_refs_survive_portable_restore(setup, tmp_path):
    store, queue, manager = setup
    sha = blob(store)
    capture(store, sha)
    capture(store, 'f' * 64)
    manager.discover()
    manager.admit()
    archive = tmp_path / 'test.tar'
    export_archive(store._data_dir, archive)
    restored = tmp_path / 'restored'
    import_archive(archive, restored)
    other = Store(restored)
    try:
        assert status(other._db)['queued'] == status(other._db)['waiting'] == 1
        assert status(other._db)['discovery']['through_seq'] == 2
        other_queue = Queue(Graph(other))
        assert other_queue.claim(['media']).payload['sha256'] == sha
        assert not CapturedMedia(other_queue).admit()
    finally:
        other.close()


def test_capture_and_blob_wake_commit_atomically(setup, monkeypatch):
    store, _, _ = setup
    data = b'atomic upload'
    sha = hashlib.sha256(data).hexdigest()
    with monkeypatch.context() as patch:
        patch.setattr('memento.store.record_references', lambda *a: (_ for _ in ()).throw(RuntimeError('stop')))
        with pytest.raises(RuntimeError):
            capture(store, sha)
    assert store._db.execute('SELECT count(*) FROM episodes').fetchone()[0] == 0
    episode, _ = capture(store, sha)
    with monkeypatch.context() as patch:
        patch.setattr('memento.artifacts.wake_blob', lambda *a: (_ for _ in ()).throw(RuntimeError('stop')))
        with pytest.raises(RuntimeError):
            blob(store, data)
    assert store.artifacts.get(sha) is None and status(store._db)['waiting'] == 1
    assert blob(store, data) == sha and status(store._db)['ready'] == 1
    assert active_source(store._db, sha)['episode_id'] == episode['id']


def test_real_store_reopen_keeps_discovery_and_one_automatic_job(tmp_path):
    path = tmp_path / 'restart'
    store = Store(path)
    sha = blob(store)
    capture(store, sha)
    manager = CapturedMedia(Queue(Graph(store)))
    manager.discover()
    manager.admit()
    before = status(store._db)
    job_id = store._db.execute('SELECT job_id FROM captured_media').fetchone()[0]
    store.close()
    store = Store(path)
    try:
        manager = CapturedMedia(Queue(Graph(store)))
        assert status(store._db) == before
        assert not manager.discover() and not manager.admit()
        assert manager.queue.claim(['media']).id == job_id
        assert store._db.execute('SELECT count(*) FROM jobs').fetchone()[0] == 1
    finally:
        store.close()


def test_new_occurrence_recovers_only_source_unavailable_job(setup, monkeypatch):
    from types import SimpleNamespace
    import numpy as np
    from memento.ingest import MediaIngest
    from memento.jobs import Worker
    from memento.retrieval import Retrieval

    store, queue, manager = setup
    sha = blob(store)
    old, _ = capture(store, sha)
    retrieval = Retrieval(queue.graph, model=SimpleNamespace(tokenizer=lambda text, **kw: {}))
    media = MediaIngest(retrieval, store.artifacts)
    calls = []
    def prepare(path, original, kind, filename):
        if not calls:
            forget_source(store, old)
        calls.append(original)
        vector = np.zeros(768, np.float32)
        vector[0] = 1
        yield dict(modality='image', text='', artifact_sha256=original,
                   start_s=None, end_s=None, page=None, vector=vector)
    monkeypatch.setattr(media, '_prepare', prepare)
    assert manager.admit()
    job_id = store._db.execute('SELECT job_id FROM captured_media').fetchone()[0]
    worker = Worker(queue, {'media': lambda job: media.ingest(
        **dict(job.payload, command_id='job/' + job.id), lease=job)})
    assert worker.run_once()
    assert queue.get(job_id)['state'] == 'failed'
    assert queue.get(job_id)['error'] == 'CapturedSourceUnavailable'
    assert store._db.execute('SELECT count(*) FROM media_files').fetchone()[0] == 0
    # Re-scanning the same old episode cannot revive the job.
    manager.discover()
    assert queue.get(job_id)['state'] == 'failed'
    newer, _ = capture(store, sha)
    assert queue.get(job_id)['state'] == 'pending'
    assert worker.run_once() and queue.get(job_id)['state'] == 'done'
    assert store._db.execute('SELECT count(*) FROM jobs').fetchone()[0] == 1
    node_id = queue.get(job_id)['result']['id']
    assert active_references(store._db, node_id)['references'][0]['episode_id'] == newer['id']
    assert len(calls) == 2 and status(store._db)['indexed'] == 1


@pytest.mark.parametrize('target_forgotten', [False, True])
def test_occurrence_captured_before_failure_persists_keeps_job_pending(setup, monkeypatch, target_forgotten):
    import time
    from types import SimpleNamespace
    import numpy as np
    from memento.ingest import CapturedSourceUnavailable, MediaIngest
    from memento.jobs import Worker
    from memento.retrieval import Retrieval

    store, queue, manager = setup
    sha = blob(store)
    old, _ = capture(store, sha)
    retrieval = Retrieval(queue.graph, model=SimpleNamespace(tokenizer=lambda text, **kw: {}))
    media = MediaIngest(retrieval, store.artifacts)
    def prepare(path, original, kind, filename):
        vector = np.zeros(768, np.float32)
        vector[0] = 1
        yield dict(modality='image', text='', artifact_sha256=original,
                   start_s=None, end_s=None, page=None, vector=vector)
    monkeypatch.setattr(media, '_prepare', prepare)
    assert manager.admit()
    job_id = store._db.execute('SELECT job_id FROM captured_media').fetchone()[0]
    forget_source(store, old)
    arrivals = []
    def handler(job):
        try:
            return media.ingest(**dict(job.payload, command_id='job/' + job.id), lease=job)
        except CapturedSourceUnavailable:
            if not arrivals:
                # The new occurrence lands after the source check, before Queue.fail persists.
                assert queue.get(job.id)['state'] == 'leased'
                arrivals.append(capture(store, sha)[0])
                if target_forgotten:
                    node = queue.graph.remember(command_id='explicit', actor='test', why='synthetic media node',
                        type='media', text='image.png', scope=['personal'],
                        evidence=[dict(episode_id=arrivals[0]['id'], quote=sha, occurrence=0)])
                    store._db.execute('INSERT INTO media_files VALUES(?,?,?,?,?)',
                                      (node['id'], sha, 'image.png', 'image', 'test'))
                    store._db.execute('INSERT INTO forgotten_targets VALUES(?,?,?)', (node['id'], 'forget', 'node'))
            raise
    worker = Worker(queue, {'media': handler})
    assert worker.run_once()
    job = queue.get(job_id)
    assert job['error'] == 'CapturedSourceUnavailable'
    if target_forgotten:
        assert job['state'] == 'failed'
        return
    assert job['state'] == 'pending'
    # Existing backoff still applies before the same job becomes eligible.
    assert queue.claim(['media']) is None
    later = time.time() + 3600
    monkeypatch.setattr('memento.jobs.time.time', lambda: later)
    assert worker.run_once() and queue.get(job_id)['state'] == 'done'
    assert store._db.execute('SELECT count(*) FROM jobs').fetchone()[0] == 1
    node_id = queue.get(job_id)['result']['id']
    assert active_references(store._db, node_id)['references'][0]['episode_id'] == arrivals[0]['id']
    assert status(store._db)['indexed'] == 1
