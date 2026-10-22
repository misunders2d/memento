"""Isolated lossless opaque-run storage; no model, provider or live service."""

import base64
from contextlib import contextmanager
import hashlib
import json
import re
import sqlite3
import subprocess
import sys
import zlib
import time

import pytest
from fastapi.testclient import TestClient

from memento.api import create_app
from memento.cold_manager import ColdManager
from memento.graph import Graph
from memento.jobs import Queue
from memento.portable import export_archive, import_archive
from memento.raw_text import FRAME_BYTES, MIME, RawTextUnavailable, decode_frame, runs
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


def test_portable_roundtrip_retains_evicted_raw_references_until_original_backend_reconnects(storage, tmp_path):
    store, graph, manager = storage
    text = 'raw Ω\r\n' + OPAQUE
    record, _ = store.capture(episode(text))
    assert len(manager.sweep()['promoted']) == 1
    from memento.drive_connect import _publish
    _publish(manager.data_dir / 'drive-binding.json', dict(client_id='fixture-client',
        account='owner@example.test', folder_id='fixture_folder'))
    archive = tmp_path / 'complete.tar'
    result = export_archive(manager.data_dir, archive, cold=manager)
    assert result['blob_count'] == 1 and result['remote_reference_count'] == 1 and not result['self_contained']
    restored = tmp_path / 'restored'
    import_archive(archive, restored)
    recovered = Store(restored)
    try:
        with pytest.raises(RawTextUnavailable):
            recovered.get(record['id'])
        ColdManager(recovered.artifacts, manager.backend, restored)
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
    store._db.execute('DROP INDEX episodes_status_cover_v1')
    store._db.execute('DROP TRIGGER episodes_no_update')
    for column in ('text_runs', 'text_sha256', 'raw_frame'):
        store._db.execute('ALTER TABLE episodes DROP COLUMN ' + column)
    store._db.execute('DROP TABLE raw_frames')
    store._db.execute("""CREATE TRIGGER episodes_no_update BEFORE UPDATE ON episodes
        BEGIN SELECT RAISE(ABORT,'episodes are immutable'); END""")
    store.close()
    archive = tmp_path / 'old.tar'
    export_archive(data, archive)
    destination = tmp_path / 'restored-old'
    import_archive(archive, destination)
    recovered = Store(destination)
    try:
        assert recovered.get(record['id'])['text'] == 'Legacy inline raw.'
        assert {'text_runs', 'text_sha256'} <= {row[1] for row in recovered._db.execute('PRAGMA table_info(episodes)')}
        assert recovered._db.execute("SELECT 1 FROM sqlite_schema WHERE name='episodes_status_cover_v1'").fetchone()
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
        assert status == dict(inline_raw_episodes=0, inline_raw_bytes=0, offloaded_raw_episodes=1,
                             compressed_raw_episodes=0, compressed_original_bytes=0, compressed_raw_bytes=0)
        sha = client.app.state.store._db.execute('SELECT sha256 FROM blobs').fetchone()[0]
        client.app.state.blobs.path(sha).write_bytes(b'corrupted')
        for path in ('/episodes/' + identity, '/history/' + identity):
            failure = client.get(path)
            assert failure.status_code == 503
            assert failure.json()['detail'] == 'Original raw source artifact is unavailable or corrupt'


def test_local_frames_preserve_raw_history_replay_quotes_and_activity(storage, tmp_path):
    store, graph, manager = storage
    quote = 'Ω exact repeated statement.'
    payloads = [episode(quote + '\r\n' + ('literal context ' * 1000) + '\r\n' + quote, str(i))
                for i in range(3)]
    payloads += [episode('Other conversation raw.', 'other', conversation_id='other'),
                 episode('Searchable projection token.', 'projection', kind='user',
                         metadata={'extraction_mode': 'message'})]
    captured = [store.capture(payload)[0] for payload in payloads]
    opaque, _ = store.capture(episode(OPAQUE, 'opaque'))
    command = dict(command_id='before-frame', actor='fixture', why='Exact legacy raw evidence',
        type='knowledge', text=quote, scope=['personal'],
        evidence=[dict(episode_id=captured[0]['id'], quote=quote, occurrence=1)])
    before = graph.remember(**command)
    manager.backend.download = lambda *a: pytest.fail('Local frames do not fetch Drive')
    for _ in range(20):
        if not store.compress_raw_step():
            break
    else:
        pytest.fail('Finite raw conversion did not finish')
    assert store._db.execute('SELECT count(*) FROM raw_frames').fetchone()[0] == 2
    assert [store.get(row['id']) for row in captured] == captured
    assert graph.history(captured[0]['id'])[0]['text'] == payloads[0]['text']
    assert graph.remember(**command) == before
    after = graph.remember(**dict(command, command_id='after-frame'))
    expected = dict(episode_id=captured[0]['id'], start=payloads[0]['text'].rfind(quote),
                    end=len(payloads[0]['text']), quote=quote)
    assert graph.history(after['id'])[0]['evidence'] == [expected]
    for payload, row in zip(payloads, captured):
        assert store.capture(payload) == (row, False)
    with pytest.raises(ConflictError):
        store.capture(payloads[0] | {'text': 'Different raw bytes'})
    assert store._db.execute('SELECT raw_frame FROM episodes WHERE id=?', (opaque['id'],)).fetchone()[0] is None
    activity = {row['id']: row['excerpt'] for row in store.search(limit=20)}
    assert all(activity[row['id']] == row['text'][:512] for row in captured)
    assert [row['id'] for row in store.search('Searchable projection')] == [captured[-1]['id']]
    assert store.search('literal context') == []  # Skipped raw was never in FTS.
    for statement in ("UPDATE episodes SET text='changed' WHERE raw_frame IS NOT NULL",
                      "UPDATE episodes SET metadata='{}' WHERE raw_frame IS NOT NULL"):
        with pytest.raises(sqlite3.IntegrityError, match='immutable'):
            store._db.execute(statement)
    archive = tmp_path / 'frames.tar'
    export_archive(store._data_dir, archive, cold=manager)
    restored = tmp_path / 'restored'
    import_archive(archive, restored)
    recovered = Store(restored)
    try:
        assert [recovered.get(row['id']) for row in captured] == captured
        assert recovered.capture(payloads[0]) == (captured[0], False)
        assert Graph(recovered).history(after['id']) == graph.history(after['id'])
    finally:
        recovered.close()


def test_large_legacy_raw_spans_bounded_frames_with_exact_utf8_and_source_counts(storage, tmp_path, monkeypatch):
    store, graph, _ = storage
    import memento.store as storage_module
    encode = storage_module.zlib.compress
    sizes = []
    def outside_writer(payload, level):
        assert not store._mutex._is_owned()
        sizes.append(len(payload))
        return encode(payload, level)
    monkeypatch.setattr(storage_module.zlib, 'compress', outside_writer)
    text = ('Ω\r\nlegacy context ' * (FRAME_BYTES // 8)) + 'exact ending'
    payload = episode(text)
    original, _ = store.capture(payload)
    assert store.compress_raw_step()
    frames = store._db.execute('SELECT raw_size,record_count,length(data) FROM raw_frames').fetchall()
    assert len(frames) > 1 and all(row[0] <= FRAME_BYTES for row in frames)
    assert sizes and max(sizes) <= FRAME_BYTES
    assert sum(row[0] for row in frames) == len(text.encode())
    assert sum(row[1] for row in frames) == 1
    assert store.get(original['id']) == original
    assert store.capture(payload) == (original, False)
    saved = graph.remember(command_id='large-quote', actor='fixture', why='Legacy exact quote',
        type='knowledge', text='exact ending', scope=['personal'],
        evidence=[dict(episode_id=original['id'], quote='exact ending')])
    assert graph.history(saved['id'])[0]['evidence'][0]['start'] == len(text) - len('exact ending')
    from memento.status import operational_status
    from types import SimpleNamespace
    from memento.processing import Processing
    state = SimpleNamespace(store=store, processing=Processing(graph), retrieval=None,
                            cold_runtime=None, cold=None, drive_reauthorization_required=False)
    status = operational_status(state, payload['source'])['raw_storage']
    assert status['inline_raw_episodes'] == 0 and status['inline_raw_bytes'] == 0
    assert status['compressed_raw_episodes'] == 1
    assert status['compressed_original_bytes'] == len(text.encode())
    assert status['compressed_raw_bytes'] == sum(row[2] for row in frames)
    other = operational_status(state, 'unrelated')['raw_storage']
    assert all(value == 0 for value in other.values())
    archive = tmp_path / 'large-frame.tar'
    export_archive(store._data_dir, archive)
    destination = tmp_path / 'large-frame-restored'
    import_archive(archive, destination)
    restored = Store(destination)
    try:
        assert restored.get(original['id']) == original
    finally:
        restored.close()


def test_frame_status_cover_preserves_source_counts_without_frame_table_reads(storage, monkeypatch):
    from types import SimpleNamespace
    from memento.status import operational_status

    store, _, _ = storage
    captured = [store.capture(episode('Ω raw source.\n' * (FRAME_BYTES // 8),
        source=source))[0] for source in ('native:fixture:codex', '')]
    for _ in range(4):
        if not store.compress_raw_step():
            break
    else:
        pytest.fail('Synthetic raw frames did not finish compressing')
    frames = store._db.execute('SELECT source,record_count,raw_size,length(data) FROM raw_frames').fetchall()
    assert any(row[1] == 0 for row in frames), 'Continuation frames must not count as extra records'
    assert store._db.execute("SELECT 1 FROM sqlite_schema WHERE name='raw_frames_status_cover_v1'").fetchone()
    assert not store._db.execute("SELECT 1 FROM sqlite_schema WHERE name='raw_frames_source'").fetchone()
    state = SimpleNamespace(store=store, retrieval=None, cold_runtime=None, cold=None,
                            drive_reauthorization_required=False)
    snapshot, statements = store.read_snapshot, []

    @contextmanager
    def observed_snapshot():
        with snapshot() as reader:
            reader.set_trace_callback(lambda sql: statements.append(sql) if 'sum(record_count)' in sql else None)
            yield reader

    monkeypatch.setattr(store, 'read_snapshot', observed_snapshot)
    for source in (None, 'native:fixture:codex', 'absent', ''):
        selected = [row for row in frames if source is None or row[0] == source]
        assert operational_status(state, source)['raw_storage'] == dict(
            inline_raw_episodes=0, inline_raw_bytes=0, offloaded_raw_episodes=0,
            compressed_raw_episodes=sum(row[1] for row in selected),
            compressed_original_bytes=sum(row[2] for row in selected),
            compressed_raw_bytes=sum(row[3] for row in selected))
    assert [store.get(row['id']) for row in captured] == captured
    if sqlite3.sqlite_version_info >= (3, 46, 1):
        with snapshot() as reader:
            page = reader.execute("SELECT rootpage FROM sqlite_schema WHERE name='raw_frames'").fetchone()[0]
            assert len(statements) == 4
            for sql in statements:
                program = list(reader.execute('EXPLAIN ' + sql))
                cursors = {op['p1'] for op in program if op['opcode'] == 'OpenRead' and op['p2'] == page}
                assert not any(op['opcode'] == 'Column' and op['p1'] in cursors for op in program), sql


def test_frame_status_cover_upgrade_is_atomic_and_preserves_physical_data(tmp_path, monkeypatch, caplog):
    import memento.store as store_module

    data = tmp_path / 'instance'
    store = Store(data)
    original, _ = store.capture(episode('Exact frame source Ω. ' * 100))
    assert store.compress_raw_step()
    frames = [tuple(row) for row in store._db.execute('SELECT * FROM raw_frames ORDER BY id')]
    store._db.execute('DROP INDEX IF EXISTS raw_frames_status_cover_v1')
    store._db.execute('CREATE INDEX IF NOT EXISTS raw_frames_source ON raw_frames(source)')
    store.close()
    upgrade = store_module.upgrade_status_index

    def interrupted(db):
        upgrade(db)
        raise RuntimeError('Synthetic interruption after raw frame index build')

    with monkeypatch.context() as patch:
        patch.setattr(store_module, 'upgrade_status_index', interrupted)
        with pytest.raises(RuntimeError, match='Synthetic interruption'):
            Store(data)
    with sqlite3.connect(data / 'memory.sqlite') as db:
        assert db.execute("SELECT 1 FROM sqlite_schema WHERE name='raw_frames_source'").fetchone()
        assert not db.execute("SELECT 1 FROM sqlite_schema WHERE name='raw_frames_status_cover_v1'").fetchone()
        assert db.execute('SELECT * FROM raw_frames ORDER BY id').fetchall() == frames
    caplog.clear()
    reopened = Store(data)
    try:
        assert reopened.get(original['id']) == original
        assert [tuple(row) for row in reopened._db.execute('SELECT * FROM raw_frames ORDER BY id')] == frames
        assert reopened._db.execute('PRAGMA user_version').fetchone()[0] == 1
        assert not reopened._db.execute("SELECT 1 FROM sqlite_schema WHERE name='raw_frames_source'").fetchone()
        page = reopened._db.execute(
            "SELECT rootpage FROM sqlite_schema WHERE name='raw_frames_status_cover_v1'").fetchone()[0]
        version = reopened._db.execute('PRAGMA schema_version').fetchone()[0]
        messages = [record.message for record in caplog.records if record.name == 'memento.schema']
        assert len(messages) == 2 and messages[0] == 'Building raw frame status index'
        assert messages[1].startswith('Raw frame status index build completed in ')
    finally:
        reopened.close()
    caplog.clear()
    reopened = Store(data)
    try:
        assert reopened.get(original['id']) == original
        assert reopened._db.execute('PRAGMA schema_version').fetchone()[0] == version
        assert reopened._db.execute(
            "SELECT rootpage FROM sqlite_schema WHERE name='raw_frames_status_cover_v1'").fetchone()[0] == page
        assert not [record for record in caplog.records if record.name == 'memento.schema']
    finally:
        reopened.close()


@pytest.mark.parametrize('failure', ['publish', 'corrupt-frame', 'oversized-payload', 'wrong-owner', 'wrong-offset'])
def test_frame_failures_never_publish_partial_conversion_or_hide_corruption(storage, failure, monkeypatch):
    store, _, _ = storage
    original, _ = store.capture(episode('Exact raw Ω source. ' * 100))
    if failure == 'publish':
        store._db.execute("""CREATE TRIGGER fail_frame_reference BEFORE UPDATE ON episodes
            WHEN NEW.raw_frame IS NOT NULL BEGIN SELECT RAISE(ABORT,'fixture failure'); END""")
        with pytest.raises(sqlite3.IntegrityError, match='fixture failure'):
            store.compress_raw_step()
        assert store.get(original['id']) == original
        assert store._db.execute('SELECT count(*) FROM raw_frames').fetchone()[0] == 0
        store._db.execute('DROP TRIGGER fail_frame_reference')
    assert store.compress_raw_step()
    if failure == 'publish':
        assert store.get(original['id']) == original
        return
    if failure in ('corrupt-frame', 'oversized-payload', 'wrong-owner'):
        store._db.execute('DROP TRIGGER raw_frames_no_update')
        if failure == 'corrupt-frame':
            store._db.execute("UPDATE raw_frames SET data=x'00010203'")
        elif failure == 'oversized-payload':
            store._db.execute('UPDATE raw_frames SET data=zeroblob(?)', (FRAME_BYTES + 1025,))
        else:
            store._db.execute("UPDATE raw_frames SET conversation_id='wrong'")
    else:
        store._db.execute('DROP TRIGGER episodes_no_update')
        refs = json.loads(store._db.execute('SELECT raw_frame FROM episodes').fetchone()[0])
        refs[0][1] += 1
        store._db.execute('UPDATE episodes SET raw_frame=?', (json.dumps(refs),))
    with pytest.raises(RawTextUnavailable):
        store.get(original['id'])
    with pytest.raises((ValueError, RawTextUnavailable)):
        export_archive(store._data_dir, store._data_dir / 'refused.tar')
    assert not (store._data_dir / 'refused.tar').exists()


@pytest.mark.parametrize('damage', ['trailing', 'oversized', 'truncated', 'checksum'])
def test_local_frame_decoder_rejects_bombs_and_invalid_streams(damage):
    payload = b'exact bytes' * 100
    encoded = zlib.compress(payload, 1)
    size, sha = len(payload), hashlib.sha256(payload).hexdigest()
    if damage == 'trailing':
        encoded += b'extraneous'
    elif damage == 'oversized':
        encoded, size = zlib.compress(b'x' * (FRAME_BYTES + 1)), FRAME_BYTES
    elif damage == 'truncated':
        encoded = encoded[:-1]
    else:
        sha = '0' * 64
    with pytest.raises((ValueError, zlib.error)):
        decode_frame(encoded, size, sha)


def test_background_frames_work_without_embeddings_or_drive_and_keep_http_reads(tmp_path, monkeypatch):
    monkeypatch.setenv('MEMENTO_EMBEDDINGS', '0')
    payload = episode('Local raw Ω\r\n' * 200)
    with TestClient(create_app(tmp_path / 'service')) as client:
        identity = client.post('/episodes', json=payload).json()['id']
        store = client.app.state.store
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            with store._mutex:
                framed = store._db.execute('SELECT raw_frame FROM episodes WHERE id=?', (identity,)).fetchone()[0]
            if framed is not None:
                break
            time.sleep(0.01)
        assert framed is not None
        assert client.get('/episodes/' + identity).json()['text'] == payload['text']
        assert client.get('/history/' + identity).json()['versions'][0]['text'] == payload['text']
        assert client.get('/episodes/search').json()['episodes'][0]['excerpt'] == payload['text'][:512]
        assert client.post('/episodes', json=payload).json() == {'id': identity}
        counters = client.get('/status', params={'source': payload['source']}).json()['raw_storage']
        assert counters['compressed_raw_episodes'] == 1 and counters['inline_raw_episodes'] == 0


def test_idle_raw_maintenance_does_not_create_temporary_files(storage, monkeypatch):
    store, _, _ = storage
    store.capture(episode('Genuine projection.', kind='user', metadata={'extraction_mode': 'message'}))
    monkeypatch.setattr('memento.store.tempfile.TemporaryFile',
                        lambda *a, **kw: pytest.fail('Idle scan must not create a staging file'))
    assert store.compress_raw_step()
    assert not store.compress_raw_step()
