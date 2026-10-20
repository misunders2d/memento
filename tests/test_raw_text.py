"""Isolated lossless opaque-run storage; no model, provider or live service."""

import base64
from contextlib import contextmanager
import hashlib
import json
import re
import sqlite3
import subprocess
import sys

import pytest
from fastapi.testclient import TestClient

from memento.api import create_app
from memento.cold_manager import ColdManager
from memento.graph import Graph
from memento.jobs import Queue
from memento.portable import export_archive, import_archive
from memento.raw_text import MIME, RawTextUnavailable, runs
from memento.store import ConflictError, Store
from test_cold import DirectoryColdFixture


OPAQUE = base64.b64encode(bytes(range(256)) * 1024).decode('ascii')


def episode(text, event='one', **changes):
    return dict(source='native:fixture:codex', event_id=event, conversation_id='current',
                kind='native_record', text=text, occurred_at=None,
                metadata={'extraction_mode': 'skip', 'native_id': event}) | changes


@pytest.fixture
def storage(tmp_path):
    data = tmp_path / 'data'
    store = Store(data)
    manager = ColdManager(store.artifacts, DirectoryColdFixture(tmp_path / 'remote'), data)
    try:
        yield store, Graph(store), manager
    finally:
        store.close()


def test_subthreshold_raw_runs_capture_and_retry_complete_within_subprocess_deadline(tmp_path):
    text = json.dumps({'type': 'native_record', 'content': ['A' * (60 * 1024)] * 4})
    script = '''
import json
from pathlib import Path
import sys
from memento.store import Store
payload = json.load(sys.stdin)
store = Store(Path(sys.argv[1]))
try:
    captured, created = store.capture(payload)
    assert created and store.get(captured['id'])['text'] == payload['text']
    assert store.capture(payload) == (captured, False)
    row = store._db.execute('SELECT text,text_runs,text_sha256 FROM episodes').fetchone()
    assert tuple(row) == (payload['text'], None, None)
    assert store._db.execute('SELECT count(*) FROM blobs').fetchone()[0] == 0
    print(json.dumps({'bytes': len(payload['text'].encode()), 'inline': True, 'lossless_retry': True}))
finally:
    store.close()
'''
    result = subprocess.run([sys.executable, '-c', script, str(tmp_path / 'store')],
                            input=json.dumps(episode(text)), text=True, capture_output=True,
                            check=True, timeout=5)
    assert json.loads(result.stdout) == {'bytes': 245814, 'inline': True, 'lossless_retry': True}


@pytest.mark.parametrize('alphabet_length,padding', [
    (65535, ''), (65535, '='), (65535, '=='), (65536, ''), (65536, '='),
    (65536, '=='), (65536, '==='), (65537, '==')])
def test_raw_run_threshold_excludes_padding_and_preserves_exact_unicode_offsets(storage, alphabet_length, padding):
    store, _, _ = storage
    prefix, suffix = 'Ω😀\r\n[', ']\r\noriginal end'
    alphabet = 'A' * (alphabet_length - 2) + '+/'
    text = prefix + alphabet + padding + suffix
    payload = episode(text)
    captured, _ = store.capture(payload)
    row = store._db.execute('SELECT text,text_runs,text_sha256 FROM episodes').fetchone()
    if alphabet_length < 65536:
        assert tuple(row) == (text, None, None)
        assert store._db.execute('SELECT count(*) FROM blobs').fetchone()[0] == 0
    else:
        offloaded = alphabet + padding[:2]
        sha = hashlib.sha256(offloaded.encode('ascii')).hexdigest()
        assert runs(row['text_runs']) == [[len(prefix), sha, len(offloaded)]]
        assert row['text'] == prefix + padding[2:] + suffix
        assert row['text_sha256'] == hashlib.sha256(text.encode('utf-8')).hexdigest()
        assert store.blobs.path(sha).read_bytes() == offloaded.encode('ascii')
    assert store.get(captured['id'])['text'] == text
    assert store.capture(payload) == (captured, False)


def test_two_envelopes_share_exact_run_and_reconstruct_original_unicode(storage):
    store, graph, manager = storage
    originals = [json.dumps(dict(id=key, note='Ω\r\n😀', image=OPAQUE), ensure_ascii=False)
                 for key in ('first-envelope', 'different-second-envelope')]
    ids = [store.capture(episode(text, str(index)))[0]['id'] for index, text in enumerate(originals)]
    rows = store._db.execute('SELECT text,text_runs,text_sha256 FROM episodes').fetchall()
    assert all(len(row['text']) < 128 and OPAQUE not in row['text'] for row in rows)
    assert runs(rows[0]['text_runs'])[0][1] == runs(rows[1]['text_runs'])[0][1]
    assert store._db.execute('SELECT count(*) FROM blobs WHERE mime=?', (MIME,)).fetchone()[0] == 1
    assert [store.get(key)['text'] for key in ids] == originals
    assert graph.history(ids[0])[0]['evidence'][0]['quote'] == originals[0]
    assert all('text_runs' not in store.get(key) and 'text_sha256' not in store.get(key) for key in ids)
    # Surrounding envelope differences do not prevent reuse of this whole run.
    assert sum(len(text.encode()) for text in originals) > 2 * len(OPAQUE)
    assert store._db.execute('SELECT sum(size) FROM blobs').fetchone()[0] == len(OPAQUE)


def test_real_native_chunk_boundaries_preserve_text_without_promising_dedup(storage, tmp_path):
    from memento.bridge import Spool
    store, _, _ = storage
    spool = Spool(tmp_path / 'runner.json', {'client_uid': 'raw-fixture'})
    large_payload = base64.b64encode(bytes(range(256)) * 5120).decode('ascii')
    originals = [json.dumps(dict(id=key, payload=large_payload))
                 for key in ('short', 'a-different-longer-envelope')]
    try:
        for index, text in enumerate(originals):
            spool.episode('codex', str(index), 'unchanged-native-id', 'native_record', text,
                          metadata={'extraction_mode': 'skip', 'native_id': 'unchanged-native-id'})
        ids = {'0': [], '1': []}
        for (body,) in spool.db.execute("SELECT body FROM outbox WHERE endpoint='/episodes' ORDER BY id"):
            payload = json.loads(body)
            captured, _ = store.capture(payload)
            ids[payload['conversation_id']].append(captured['id'])
        for index, text in enumerate(originals):
            assert len(ids[str(index)]) == 2
            assert ''.join(store.get(identity)['text'] for identity in ids[str(index)]) == text
        # Different offsets at the 1MiB boundary produce different maximal runs.
        # Whole-run equality, rather than the underlying image, defines reuse.
        references = sum(len(runs(row[0])) for row in store._db.execute('SELECT text_runs FROM episodes'))
        assert references == 4
        assert store._db.execute('SELECT count(*) FROM blobs').fetchone()[0] == references
    finally:
        spool.close()


def test_only_skipped_native_raw_kinds_are_offloaded(storage):
    store, _, _ = storage
    for index, changes in enumerate((dict(kind='user', metadata={}), dict(kind='native_record', metadata={}),
                                     dict(kind='tool', metadata={'extraction_mode': 'skip'}))):
        record, _ = store.capture(episode(OPAQUE, str(index), **changes))
        row = store._db.execute('SELECT text,text_runs FROM episodes WHERE id=?', (record['id'],)).fetchone()
        assert row['text'] == OPAQUE and row['text_runs'] is None
    assert store._db.execute('SELECT count(*) FROM blobs').fetchone()[0] == 0


def test_offloaded_and_legacy_retries_keep_identity_without_drive_or_re_elision(storage, monkeypatch):
    import memento.raw_text as physical
    store, _, manager = storage
    payload = episode('prefix Ω ' + OPAQUE + ' suffix')
    captured, _ = store.capture(payload)
    manager.sweep()
    monkeypatch.setattr(manager.backend, 'download', lambda *args: pytest.fail('Retry must not fetch Drive'))
    monkeypatch.setattr(physical, 'OPAQUE', re.compile(r'(?!)'))
    assert store.capture(payload) == (captured, False)
    with pytest.raises(ConflictError):
        store.capture(payload | {'text': payload['text'] + ' changed'})
    legacy = episode(OPAQUE, 'legacy')
    old, _ = store.capture(legacy)
    monkeypatch.undo()
    assert store.capture(legacy) == (old, False)
    row = store._db.execute('SELECT text_runs FROM episodes WHERE id=?', (old['id'],)).fetchone()
    assert row['text_runs'] is None


def test_blob_registration_and_episode_reference_roll_back_together(storage):
    store, _, _ = storage
    store._db.execute("CREATE TRIGGER fail_raw BEFORE INSERT ON episodes BEGIN SELECT RAISE(ABORT,'fixture failure'); END")
    with pytest.raises(sqlite3.IntegrityError, match='fixture failure'):
        store.capture(episode(OPAQUE))
    assert store._db.execute('SELECT count(*) FROM episodes').fetchone()[0] == 0
    assert store._db.execute('SELECT count(*) FROM blobs').fetchone()[0] == 0
    assert store.blobs.path(hashlib.sha256(OPAQUE.encode()).hexdigest()).read_bytes() == OPAQUE.encode()
    store._db.execute('DROP TRIGGER fail_raw')
    record, created = store.capture(episode(OPAQUE))
    assert created and store.get(record['id'])['text'] == OPAQUE


def test_hydration_is_reader_fenced_outside_writer_and_corruption_is_explicit(storage):
    store, graph, manager = storage
    record, _ = store.capture(episode('Ω ' + OPAQUE + ' end'))
    manager.sweep()
    reader = manager.reader

    @contextmanager
    def checked(sha256):
        assert not store._mutex._is_owned()
        with reader(sha256) as source:
            assert manager.trim_cache(max_bytes=0, ttl_seconds=0)['evicted'] == []
            yield source

    store.artifact_reader = checked
    assert store.get(record['id'])['text'] == 'Ω ' + OPAQUE + ' end'
    assert graph.history(record['id'])[0]['text'] == 'Ω ' + OPAQUE + ' end'
    assert len(manager.trim_cache(max_bytes=0, ttl_seconds=0)['evicted']) == 1
    sha = store._db.execute('SELECT sha256 FROM blobs').fetchone()[0]
    (manager.backend.root / sha).write_bytes(b'corrupt remote original')
    with pytest.raises(RawTextUnavailable, match='unavailable or corrupt'):
        store.get(record['id'])
    assert manager._readers == {}


@pytest.mark.parametrize('operation', ['remember', 'job', 'review'])
def test_offloaded_raw_cannot_enter_assertions_jobs_or_reviews(storage, operation):
    store, graph, manager = storage
    quote = 'A meaningful user statement.'
    raw, _ = store.capture(episode(quote + OPAQUE))
    manager.sweep()
    store.artifact_reader = lambda *args: pytest.fail('Evidence admission must never hydrate opaque raw')
    evidence = [dict(episode_id=raw['id'], quote=quote)]
    with pytest.raises(ValueError, match='cite the message projection'):
        if operation == 'job':
            sha256 = store._db.execute('SELECT sha256 FROM blobs').fetchone()[0]
            Queue(graph).submit_media(command_id='refused', actor='fixture', why='Explicit save',
                sha256=sha256, kind='image', filename='original.png', scope=['personal'], evidence=evidence)
        else:
            graph.remember(command_id='refused', actor='fixture', why='Explicit save',
                type='person' if operation == 'review' else 'knowledge', text=quote,
                scope=['personal'], evidence=evidence)
    for table in ('node_versions', 'jobs', 'review_proposals', 'graph_evidence', 'graph_commits'):
        assert store._db.execute(f'SELECT count(*) FROM {table}').fetchone()[0] == 0


def test_projected_message_quotes_remember_link_supersede_preserve_original_offsets(storage):
    store, graph, manager = storage
    quote = 'Ω says exact term.'
    text = quote + '\r\n' + OPAQUE + '\r\n' + quote
    store.capture(episode(text))
    manager.sweep()
    text = quote + '\r\nA concrete user message about the project.\r\n' + quote
    projected, _ = store.capture(episode(text, 'message-one', kind='user',
        metadata={'extraction_mode': 'message', 'native_id': 'one'}))
    store.artifact_reader = lambda *args: pytest.fail('Projected message evidence must not fetch Drive')
    evidence = [dict(episode_id=projected['id'], quote=quote, occurrence=1)]
    first_args = dict(command_id='first', actor='fixture', why='Explicit projected message evidence',
        type='knowledge', text='Original assertion.', scope=['personal'], evidence=evidence)
    first = graph.remember(**first_args)
    second = graph.remember(**dict(first_args, command_id='second', text='Related assertion.'))
    relation = graph.link(command_id='relation', actor='fixture', why='Related original evidence',
        a=first['id'], rel='related_to', b=second['id'], evidence=evidence)
    replacement = graph.supersede(command_id='correct', actor='fixture', why='Explicit projected correction',
        old=first['id'], new=dict(type='knowledge', text='Corrected assertion.', scope=['personal']), evidence=evidence)
    for target in (first['id'], second['id'], relation['id'], replacement['superseded_by']):
        for row in graph.history(target):
            for source in row['evidence']:
                assert source['start'] == text.rfind(quote) and source['end'] == len(text)
                assert text[source['start']:source['end']] == source['quote'] == quote
    assert all(OPAQUE not in row['request'] for row in store._db.execute('SELECT request FROM graph_commits'))
    manager.trim_cache(max_bytes=0, ttl_seconds=0)
    manager.backend.download = lambda *args: pytest.fail('Committed retry must not rehydrate evidence')
    assert graph.remember(**first_args) == first


def test_legacy_inline_raw_quote_evidence_remains_supported(storage, monkeypatch):
    import memento.raw_text as physical
    store, graph, _ = storage
    quote = 'Legacy exact statement.'
    text = OPAQUE + '\r\n' + quote
    monkeypatch.setattr(physical, 'OPAQUE', re.compile(r'(?!)'))
    raw, _ = store.capture(episode(text))
    store.artifact_reader = lambda *args: pytest.fail('Legacy inline evidence does not fetch artifacts')
    saved = graph.remember(command_id='legacy-save', actor='fixture', why='Existing inline evidence',
        type='knowledge', text=quote, scope=['personal'], evidence=[dict(episode_id=raw['id'], quote=quote)])
    source = graph.history(saved['id'])[0]['evidence'][0]
    assert source['start'] == text.index(quote) and source['end'] == len(text)
    assert source['quote'] == quote


def test_complete_portable_roundtrip_includes_evicted_raw_runs(storage, tmp_path):
    store, graph, manager = storage
    text = 'raw Ω\r\n' + OPAQUE
    record, _ = store.capture(episode(text))
    assert len(manager.sweep()['promoted']) == 1
    archive = tmp_path / 'complete.tar'
    result = export_archive(manager.data_dir, archive, cold=manager)
    assert result['blob_count'] == 1
    restored = tmp_path / 'restored'
    import_archive(archive, restored)
    recovered = Store(restored)
    try:
        assert recovered.get(record['id'])['text'] == text
        assert Graph(recovered).history(record['id'])[0]['text'] == text
    finally:
        recovered.close()
    store._db.execute('DELETE FROM blobs')
    with pytest.raises(ValueError, match='Raw source references'):
        export_archive(manager.data_dir, tmp_path / 'invalid.tar', cold=manager)


def test_old_portable_database_without_physical_columns_restores(tmp_path):
    data = tmp_path / 'old'
    store = Store(data)
    record, _ = store.capture(episode('Legacy inline raw.'))
    for column in ('text_runs', 'text_sha256'):
        store._db.execute('ALTER TABLE episodes DROP COLUMN ' + column)
    store.close()
    archive = tmp_path / 'old.tar'
    export_archive(data, archive)
    destination = tmp_path / 'restored-old'
    import_archive(archive, destination)
    recovered = Store(destination)
    try:
        assert recovered.get(record['id'])['text'] == 'Legacy inline raw.'
        assert {'text_runs', 'text_sha256'} <= {row[1] for row in recovered._db.execute('PRAGMA table_info(episodes)')}
    finally:
        recovered.close()


def test_http_raw_retrieval_and_status_keep_wire_unchanged_and_fail_explicitly(tmp_path, monkeypatch):
    monkeypatch.setenv('MEMENTO_EMBEDDINGS', '0')
    with TestClient(create_app(tmp_path / 'service')) as client:
        payload = episode('prefix Ω ' + OPAQUE + ' suffix')
        created = client.post('/episodes', json=payload)
        assert created.status_code == 201
        identity = created.json()['id']
        assert client.post('/episodes', json=payload).json() == {'id': identity}
        response = client.get('/episodes/' + identity)
        assert response.json()['text'] == payload['text']
        assert response.json()['metadata'] == payload['metadata']
        status = client.get('/status').json()['raw_storage']
        assert status == dict(inline_raw_episodes=0, inline_raw_bytes=0, offloaded_raw_episodes=1)
        sha = client.app.state.store._db.execute('SELECT sha256 FROM blobs').fetchone()[0]
        client.app.state.blobs.path(sha).write_bytes(b'corrupted')
        for path in ('/episodes/' + identity, '/history/' + identity):
            failure = client.get(path)
            assert failure.status_code == 503
            assert failure.json()['detail'] == 'Original raw source artifact is unavailable or corrupt'
