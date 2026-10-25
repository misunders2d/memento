"""Real pinned-child projection, transactional fences and recovery (synthetic data)."""

from concurrent.futures import ThreadPoolExecutor
import fcntl
import os
from pathlib import Path
import shutil
import sqlite3
from threading import Event
import time
from types import SimpleNamespace

import pytest
import httpx
from fastapi.testclient import TestClient

np = pytest.importorskip('numpy')

from memento.ann import Projection
from memento.api import create_app
from memento.curation import pin, retire
from memento.consolidation import Consolidator
from memento.forgetting import apply as forget_apply, preview as forget_preview
from memento.graph import Graph
from memento.processing import Processing
from memento.qdrant import Qdrant, SearchUnavailable
from memento.retrieval import EMBED_DIM, MODEL_ID, MODEL_REVISION, Retrieval
from memento.store import Store
from test_retrieval import assertion, candidate, index


def make_retrieval(path, monkeypatch, *, binary=None):
    monkeypatch.delenv('MEMENTO_QDRANT_BINARY', raising=False)
    query = np.zeros(EMBED_DIM, np.float32)
    query[0] = 1
    monkeypatch.setattr('memento.retrieval.embed_query', lambda *_: query)
    model = SimpleNamespace(tokenizer=lambda text, **kw: {'offset_mapping': [(i, i + 1) for i in range(len(text))]})
    store = Store(path)
    retrieval = Retrieval(Graph(store), model=model)
    if binary:
        try:
            retrieval._projection = Projection(store, EMBED_DIM,
                dict(model_id=MODEL_ID, revision=MODEL_REVISION, dimensions=EMBED_DIM, projection=1),
                retrieval._projection_payloads, transport=Qdrant(binary, path / 'search', EMBED_DIM))
        except BaseException:
            retrieval.close()
            store.close()
            raise
    return retrieval, query


@pytest.fixture
def real(tmp_path, monkeypatch):
    binary = os.environ.get('MEMENTO_TEST_QDRANT_BINARY')
    if not binary:
        pytest.skip('Set MEMENTO_TEST_QDRANT_BINARY to the pinned real binary')
    retrieval = None
    try:
        retrieval, query = make_retrieval(tmp_path, monkeypatch, binary=binary)
        yield retrieval, query, binary
    finally:
        if retrieval is not None:
            retrieval.close()
            retrieval.store.close()
        # Qdrant preallocates substantial WAL space even for tiny corpora.
        # Retain the small SQLite/config/log fixtures, never its heavy cache.
        for directory in (tmp_path / 'search', tmp_path / 'restored/search'):
            lock = directory / 'child.lock'
            if not lock.is_file():
                continue
            with lock.open('a') as lease:
                fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
                for name in ('storage', 'snapshots'):
                    heavy = directory / name
                    if heavy.is_dir() and not heavy.is_symlink():
                        shutil.rmtree(heavy)


def sync(retrieval):
    projection = retrieval._projection
    with projection._lock, retrieval.store.read_snapshot() as db:
        state, cut = projection._cut(db)
        projection._sync(db, state, cut, time.monotonic() + 20, rebuild=True)


def forget(retrieval, target):
    request = forget_preview(retrieval.graph, target)
    return retrieval.graph._command('forget-' + target, 'owner', 'Synthetic exclusion', dict(target=target),
        lambda commit: forget_apply(retrieval.graph, commit, target, request['token']))


def set_pin(retrieval, target, value, key):
    return retrieval.graph._command(key, 'owner', 'Synthetic pin transition', dict(target=target, pinned=value),
        lambda commit: pin(retrieval.graph, commit, target, value))


def test_empty_new_projection_becomes_ready_without_inventing_results(real):
    r, _, _ = real
    sync(r)
    assert r.recall('unmatchedword') == []
    assert r._projection.available is True


def test_search_outage_is_http_503_while_capture_and_local_health_continue(tmp_path, monkeypatch):
    monkeypatch.setenv('MEMENTO_EMBEDDINGS', '0')
    def unavailable(*args, **kwargs):
        raise SearchUnavailable('Search synchronization is pending')
    with TestClient(create_app(tmp_path)) as client:
        client.app.state.retrieval = SimpleNamespace(_projection=SimpleNamespace(available=False),
                                                     health=lambda: None, recall=unavailable)
        health = client.get('/health')
        assert health.status_code == 200 and health.json()['search_ready'] is False
        recalled = client.get('/recall', params={'q': 'synthetic'})
        assert recalled.status_code == 503 and recalled.json() == {'detail': 'Search synchronization is pending'}
        captured = client.post('/episodes', json=dict(source='synthetic', event_id='new',
            conversation_id='new', kind='user', text='Capture remains available during search indexing.'))
        assert captured.status_code == 201


def test_real_prefilters_precede_topk_and_forgetting_is_visible(real):
    r, _, _ = real
    wanted, _ = assertion(r, 'The Alder record describes the requested family evidence.', scope=['professional'])
    index(r, wanted, .8)
    # More wrong-scope rows than either requested ANN pool: a global top-K
    # followed by filtering would lose the wanted row entirely.
    for number in range(30):
        wrong, _ = assertion(r, f'The unrelated personal record has detail {number}.', scope=['personal'])
        index(r, wrong, .99)
    sync(r)
    for automatic in (False, True):
        [hit] = r.recall('unmatchedword', scope='professional', limit=1, automatic=automatic)
        assert hit['id'] == wanted and hit['score'] == pytest.approx(.8, abs=2e-6)
    forget(r, wanted)
    assert r.recall('unmatchedword', scope='professional', limit=1) == []
    assert r.recall('unmatchedword', scope='professional', limit=1, automatic=True) == []
    assert r.store._db.execute('SELECT count(*) FROM search_outbox').fetchone()[0] == 0


def test_snapshot_cut_and_same_target_acknowledgment_race(real, monkeypatch):
    r, vector, _ = real
    target, _ = assertion(r, 'The Alder record has stable original source evidence.')
    index(r, target, .9)
    sync(r)
    set_pin(r, target, True, 'before-cut')
    projection = r._projection
    original = projection.transport.commit_checkpoint
    changed = False

    def commit(marker, **kwargs):
        nonlocal changed
        assert not r.store._db.in_transaction, 'Network work entered the graph writer transaction'
        original(marker, **kwargs)
        if marker['pending'] is None and not changed:
            changed = True
            set_pin(r, target, False, 'after-cut-same-target')

    monkeypatch.setattr(projection.transport, 'commit_checkpoint', commit)
    with projection.snapshot() as db:
        rows = projection.search(db, vector, 1, kind='node', pool='ordinary')
        assert rows[0]['target'] == target and rows[0]['version'] == 2
        assert db.execute('SELECT max(version) FROM node_versions WHERE id=?', (target,)).fetchone()[0] == 2
        assert r.store._db.execute('SELECT max(version) FROM node_versions WHERE id=?', (target,)).fetchone()[0] == 3
    assert r.store._db.execute('SELECT count(*) FROM search_outbox').fetchone()[0] > 0
    with projection.snapshot() as db:
        assert projection.search(db, vector, 1, kind='node', pool='ordinary')[0]['version'] == 3
    assert r.store._db.execute('SELECT count(*) FROM search_outbox').fetchone()[0] == 0


def test_delayed_sync_does_not_block_capture_or_health(real, monkeypatch):
    r, _, _ = real
    target, _ = assertion(r, 'The Alder record has independently captured evidence.')
    index(r, target, .9)
    sync(r)
    set_pin(r, target, True, 'delay-cut')
    entered, resume = Event(), Event()
    original = r._projection.transport.upsert

    def delayed(points, **kwargs):
        if points and isinstance(points[0]['id'], int):
            assert not r.store._db.in_transaction
            entered.set()
            assert resume.wait(1)
        return original(points, **kwargs)

    monkeypatch.setattr(r._projection.transport, 'upsert', delayed)
    with ThreadPoolExecutor(max_workers=1) as pool:
        request = pool.submit(r.recall, 'unmatchedword')
        assert entered.wait(1)
        try:
            r.store.capture(dict(source='independent', event_id='new', conversation_id='new', kind='user',
                text='Independent synthetic capture while network synchronization is blocked.', occurred_at=None, metadata={}))
            r.store.health()
        finally:
            resume.set()
        assert request.result(timeout=3)[0]['id'] == target


def test_two_overlapping_healthy_queries_share_existing_budget_and_leave_capture_free(real, monkeypatch):
    r, _, _ = real
    target, _ = assertion(r, 'The Alder record remains relevant for both connected agents.')
    index(r, target, .9)
    sync(r)
    entered = Event()
    original = r._projection.transport.search
    def slow(vector, limit, equals, **kwargs):
        if equals['kind'] == 'node':
            entered.set()
            time.sleep(.2)
        return original(vector, limit, equals, **kwargs)
    monkeypatch.setattr(r._projection.transport, 'search', slow)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(r.recall, 'unmatchedword', effort='cheap')
        assert entered.wait(1)
        second = pool.submit(r.recall, 'unmatchedword', effort='cheap')
        r.store.capture(dict(source='concurrent', event_id='captured', conversation_id='concurrent', kind='user',
            text='Independent capture while two healthy searches overlap.', occurred_at=None, metadata={}))
        r.store.health()
        assert not first.done(), 'Capture should complete while the first search is deliberately delayed'
        assert first.result(timeout=3)[0]['id'] == target
        assert second.result(timeout=3)[0]['id'] == target


def test_initial_build_pauses_encoder_and_does_not_reset_after_long_healthy_progress(real, monkeypatch):
    import memento.ann as ann_module
    import memento.qdrant as qdrant_module
    r, vector, _ = real
    indexed, _ = assertion(r, 'The indexed Alder source is already authoritative.')
    index(r, indexed, .9)
    pending, _ = assertion(r, 'The pending Alder source waits for background encoding.')
    entered, resume, encoded = Event(), Event(), Event()
    clock_offset = [0]
    resets = []
    original_upsert = r._projection.transport.upsert
    original_reset = r._projection.transport.reset
    def reset(**kwargs):
        resets.append(True)
        return original_reset(**kwargs)
    def paused(points, **kwargs):
        result = original_upsert(points, **kwargs)
        if points and isinstance(points[0]['id'], int) and not entered.is_set():
            entered.set()
            assert resume.wait(4)
            clock_offset[0] = 7200  # Healthy full build can exceed one hour.
        return result
    def encode(model, texts):
        assert r._projection.available
        encoded.set()
        return np.stack([vector] * len(texts))
    monkeypatch.setattr(r._projection.transport, 'upsert', paused)
    monkeypatch.setattr(r._projection.transport, 'reset', reset)
    monkeypatch.setattr('memento.retrieval.embed_texts', encode)
    real_clock = time.monotonic
    clock = SimpleNamespace(monotonic=lambda: real_clock() + clock_offset[0])
    # Replace only these modules' clock references, not httpx/threading clocks
    # or the actual per-RPC socket deadlines.
    with monkeypatch.context() as elapsed:
        elapsed.setattr(ann_module, 'time', clock)
        elapsed.setattr(qdrant_module, 'time', clock)
        r.start()
        assert entered.wait(2)
        try:
            assert not encoded.wait(.2)
            assert r.store._db.execute('SELECT 1 FROM vector_items WHERE target=?', (pending,)).fetchone() is None
            r.store.capture(dict(source='during-build', event_id='capture', conversation_id='during-build', kind='user',
                text='Capture stays available while the initial derived index builds.', occurred_at=None, metadata={}))
            r.store.health()
        finally:
            resume.set()
        assert encoded.wait(8)
        assert len(resets) == 1, 'Elapsed healthy progress must not restart the full build'
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline and r.store._db.execute(
            'SELECT 1 FROM vector_items WHERE target=?', (pending,)).fetchone() is None:
        time.sleep(.02)
    assert r.store._db.execute('SELECT 1 FROM vector_items WHERE target=?', (pending,)).fetchone()


@pytest.mark.parametrize('first_restart_fails', [False, True])
def test_existing_synchronizer_reopens_killed_child_without_service_restart(real, monkeypatch, first_restart_fails):
    r, _, _ = real
    target, _ = assertion(r, 'The Alder source survives an unexpected search child exit.')
    index(r, target, .9)
    sync(r)
    history = r.graph.history(target)
    projection = r._projection
    child = projection.transport.process
    entered, resume = Event(), Event()
    attempts = []
    original = projection.transport.reopen
    def controlled():
        attempts.append(time.monotonic())
        assert not r.store._db.in_transaction
        if len(attempts) == 1:
            entered.set()
            assert resume.wait(4)
            if first_restart_fails:
                raise SearchUnavailable('Synthetic transient restart failure')
        return original()
    monkeypatch.setattr(projection.transport, 'reopen', controlled)
    projection.start()
    child.kill()
    child.wait(timeout=3)
    assert entered.wait(2)
    try:
        with pytest.raises(SearchUnavailable):
            r.recall('unmatchedword')
        assert projection.available is False
        r.store.capture(dict(source='recovery', event_id='capture', conversation_id='recovery', kind='user',
            text='Independent capture remains possible during child recovery.', occurred_at=None, metadata={}))
        r.store.health()
    finally:
        resume.set()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            hits = r.recall('unmatchedword')
            if hits and hits[0]['id'] == target:
                break
        except SearchUnavailable:
            pass
        time.sleep(.05)
    else:
        pytest.fail('Background child recovery did not restore the same source fact')
    assert projection.transport.process.pid != child.pid and projection.available
    assert r.graph.history(target) == history
    if first_restart_fails:
        assert len(attempts) >= 2 and attempts[1] - attempts[0] >= 1


@pytest.mark.parametrize('failure', ['after_points', 'after_checkpoint'])
def test_uncertain_acknowledgment_replays_or_acknowledges_exact_fence(real, monkeypatch, failure):
    r, _, _ = real
    target, _ = assertion(r, 'The Alder record is deliberately excluded by its owner.')
    index(r, target, .9)
    sync(r)
    forget(r, target)
    projection = r._projection
    if failure == 'after_points':
        original = projection.transport.client.request
        def interrupted(method, path, **kwargs):
            response = original(method, path, **kwargs)
            points = (kwargs.get('json') or {}).get('points', [])
            if points and isinstance(points[0], dict) and isinstance(points[0]['id'], int):
                assert 'ordering=strong' in path and 'wait=true' in path
                raise httpx.ReadTimeout('synthetic-private-transport-marker')
            return response
        monkeypatch.setattr(projection.transport.client, 'request', interrupted)
    else:
        original = projection._ack
        def interrupted(cut):
            raise SearchUnavailable('Synthetic crash before local acknowledgment')
        monkeypatch.setattr(projection, '_ack', interrupted)
    with pytest.raises(SearchUnavailable):
        r.recall('unmatchedword')
    assert r.store._db.execute('SELECT count(*) FROM search_outbox').fetchone()[0] > 0
    if failure == 'after_points':
        monkeypatch.setattr(projection.transport.client, 'request', original)
    else:
        monkeypatch.setattr(projection, '_ack', original)
    monkeypatch.setattr(projection.transport, 'reset', lambda **kw: pytest.fail('A valid retained fence should replay, not rebuild'))
    assert r.recall('unmatchedword') == []
    assert r.store._db.execute('SELECT count(*) FROM search_outbox').fetchone()[0] == 0


@pytest.mark.parametrize('partial_remote', [False, True])
def test_restored_sqlite_rebuilds_absent_or_partially_newer_index(real, tmp_path, monkeypatch, partial_remote):
    r, _, binary = real
    target, _ = assertion(r, 'The Alder archive keeps the original family evidence.')
    index(r, target, .9)
    sync(r)
    history = r.graph.history(target)
    embeddings = [tuple(row) for row in r.store._db.execute('SELECT rowid,embedding FROM memory_vectors')]
    restored = tmp_path / 'restored'
    restored.mkdir(mode=0o700)
    with sqlite3.connect(restored / 'memory.sqlite') as destination:
        r.store._db.backup(destination)
    if partial_remote:
        forget(r, target)
        original = r._projection.transport.upsert
        def interrupted(points, **kwargs):
            original(points, **kwargs)
            if points and isinstance(points[0]['id'], int):
                raise SearchUnavailable('Synthetic crash after partial remote mutation')
        monkeypatch.setattr(r._projection.transport, 'upsert', interrupted)
        with pytest.raises(SearchUnavailable):
            r.recall('unmatchedword')
        r.close()
        shutil.copytree(tmp_path / 'search', restored / 'search')
    recovered, _ = make_retrieval(restored, monkeypatch, binary=binary)
    try:
        with pytest.raises(SearchUnavailable, match='rebuild'):
            recovered.recall('unmatchedword')
        sync(recovered)
        assert recovered.recall('unmatchedword')[0]['id'] == target
        assert recovered.graph.history(target) == history
        assert [tuple(row) for row in recovered.store._db.execute('SELECT rowid,embedding FROM memory_vectors')] == embeddings
    finally:
        recovered.close()
        recovered.store.close()


def test_child_exit_is_an_outage_and_never_uses_sqlite_fallback(real, monkeypatch):
    r, _, _ = real
    target, _ = assertion(r, 'The Alder record remains authoritative during a search outage.')
    index(r, target, .9)
    sync(r)
    child = r._projection.transport.process
    child.terminate()
    child.wait(timeout=3)
    with pytest.raises(SearchUnavailable, match='unavailable'):
        r.recall('unmatchedword')
    assert r.graph.history(target)[0]['state'] == 'active'
    assert r._projection.available is False


def test_corrupt_derived_storage_reopens_empty_then_rebuilds_from_unchanged_sqlite(real, tmp_path, monkeypatch, caplog):
    r, _, binary = real
    target, _ = assertion(r, 'The Alder archive preserves the authoritative original record.')
    index(r, target, .9)
    sync(r)
    history = r.graph.history(target)
    original = [tuple(row) for row in r.store._db.execute('SELECT rowid,embedding FROM memory_vectors')]
    r.close()
    r.store.close()
    config = tmp_path / 'search/storage/collections/memento_vectors/config.json'
    assert config.is_file()
    config.write_text('{invalid synthetic derived collection metadata')
    recovered, _ = make_retrieval(tmp_path, monkeypatch, binary=binary)
    try:
        with pytest.raises(SearchUnavailable, match='rebuild'):
            recovered.recall('unmatchedword')
        sync(recovered)
        assert recovered.recall('unmatchedword')[0]['id'] == target
        assert recovered.graph.history(target) == history
        assert [tuple(row) for row in recovered.store._db.execute('SELECT rowid,embedding FROM memory_vectors')] == original
        assert 'rebuilding the disposable index from SQLite' in caplog.text
        assert not list((tmp_path / 'search').glob('unreadable-*'))
    finally:
        recovered.close()
        recovered.store.close()


def test_zero_and_signed_sqlite_vector_ids_are_disjoint_from_uuid_checkpoint(real):
    r, vector, _ = real
    targets = []
    for seq in (0, -1):
        target, _ = assertion(r, 'The Alder record uses stable vector identity ' + str(seq) + '.')
        r.store._db.execute('BEGIN IMMEDIATE')
        r.store._db.execute("INSERT INTO vector_items(seq,target,kind,version) VALUES (?,?,'node',1)", (seq, target))
        r.store._db.execute('INSERT INTO memory_vectors(rowid,embedding,current) VALUES (?,?,1)', (seq, vector.tobytes()))
        r.store._db.execute('COMMIT')
        targets.append(target)
    sync(r)
    assert {item['id'] for item in r.recall('unmatchedword', limit=10)} == set(targets)
    assert r._projection.transport.checkpoint(deadline=time.monotonic()+2)['pending'] is None


def test_real_historical_candidate_and_media_paths_keep_original_evidence(real):
    r, _, _ = real
    episode, _ = r.store.capture(dict(source='archive', event_id='old', conversation_id='archive', kind='user',
        text='The Alder arrival was recorded in the original archive.', occurred_at='2024-01-01T00:00:00Z', metadata={}))
    r.store._db.execute('INSERT INTO episode_chunks VALUES (?,?,0,?)', ('archive-chunk', episode['id'], len(episode['text'])))
    index(r, 'archive-chunk', .90, kind='episode')
    parked, original_episode = candidate(r, 'The Alder archive preserves its original family evidence.')
    index(r, parked, .95, kind='candidate')
    Processing(r.graph).park_history()
    sync(r)
    deep = r.recall('unmatchedword', effort='deep')
    raw = next(item for item in deep if item['id'] == 'archive-chunk')
    assert raw['episode_id'] == episode['id'] and raw['evidence'][0]['start'] == 0
    assert raw['evidence'][0]['end'] == len(episode['text'])
    offered = r.recall('unmatchedword', automatic=True)
    assert [item['id'] for item in offered] == [parked, 'archive-chunk']
    assert all(item['historical'] and item['confirmed'] is False for item in offered)
    assert offered[0]['evidence'][0]['episode_id'] == original_episode['id']
    assert offered[1]['category'] == 'historical_raw'
    live, _ = candidate(r, 'The active Alder reference keeps its original source evidence.')
    index(r, live, .85, kind='candidate')
    media, _ = assertion(r, 'The source document depicts the Alder desk.', type='media')
    r.store._db.execute('INSERT INTO media_files VALUES (?,?,?,?,?)', (media, 'a' * 64, 'fixture.txt', 'text', 'fixture'))
    r.store._db.execute('''INSERT INTO segments(id,node_id,modality,text,artifact_sha256,source_sha256)
        VALUES ('media-page',?,'text','The Alder desk is depicted in the source.',?,?)''', (media, 'b' * 64, 'c' * 64))
    index(r, 'media-page', .99, kind='segment')
    sync(r)
    automatic = r.recall('unmatchedword', automatic=True)
    assert [item['id'] for item in automatic] == [media, live]
    assert automatic[0]['category'] == 'media_reference' and automatic[0]['blob_url'] == '/blobs/' + 'a' * 64
    assert automatic[1]['tier'] == 'candidate' and automatic[1]['confirmed'] is False


def test_entity_association_search_precedes_writer_and_stale_hint_is_omitted(real, monkeypatch):
    r, vector, _ = real
    person, _ = assertion(r, 'Rowan Vale', type='person')
    index(r, person, .9)
    pending, _ = candidate(r, 'Rowan Vale', type='person', kind='entity')
    sync(r)
    monkeypatch.setattr('memento.retrieval.embed_texts', lambda model, texts: np.stack([vector] * len(texts)))
    original = r._projection.search
    called = False
    def concurrent_change(db, query, limit, **kwargs):
        nonlocal called
        assert not r.store._db.in_transaction
        rows = original(db, query, limit, **kwargs)
        if kwargs['pool'] == 'entity':
            called = True
            set_pin(r, person, True, 'stale-hint')
        return rows
    monkeypatch.setattr(r._projection, 'search', concurrent_change)
    r.index_pending(limit=16)
    assert called
    assert r.store._db.execute('SELECT 1 FROM vector_items WHERE target=?', (pending,)).fetchone()
    assert r.store._db.execute('SELECT count(*) FROM candidate_entity_matches WHERE candidate_id=?', (pending,)).fetchone()[0] == 0


def test_configured_consolidation_prepares_ann_hints_before_capture_mutex(real, monkeypatch):
    r, _, _ = real
    existing, _ = assertion(r, 'The Alder archive retains source evidence for every record.')
    index(r, existing, .9)
    pending, _ = candidate(r, 'The Cedar handbook preserves original proof for each entry.')
    index(r, pending, .9, kind='candidate')
    sync(r)
    calls = []
    original = r._projection.transport.request
    def observed(method, path, body=None, **kwargs):
        assert not r.store._db.in_transaction
        # A separate thread must be able to take the actual capture mutex, not
        # merely see autocommit on the writer connection.
        with ThreadPoolExecutor(max_workers=1) as pool:
            def probe():
                with r.store._mutex:
                    r.store.health()
            pool.submit(probe).result(timeout=.5)
        calls.append(path)
        return original(method, path, body, **kwargs)
    monkeypatch.setattr(r._projection.transport, 'request', observed)
    statements = []
    r.store._db.set_trace_callback(statements.append)
    try:
        work = Processing(r.graph).claim(agent='fixture', timezone_name='UTC', consolidate_now=True, kinds=['consolidate'])
    finally:
        r.store._db.set_trace_callback(None)
    assert any('/points/query' in path for path in calls)
    assert not any('embedding MATCH' in sql for sql in statements)
    [task] = work['work']
    assert task['kind'] == 'consolidate' and pending in {item['id'] for item in task['input']['candidates']}
    assert existing in {item['id'] for item in task['input']['existing_nodes']}


@pytest.mark.parametrize('operation', ['candidate_terminal', 'parking', 'release', 'custom_type',
    'source_forget', 'media_hide', 'media_reappear', 'delete_vector', 'correction', 'retire', 'merge'])
def test_mutation_invalidation_covers_every_changed_payload(tmp_path, monkeypatch, operation):
    from memento.curation import apply_review
    from memento.importance import cite
    r, _ = make_retrieval(tmp_path, monkeypatch)
    try:
        db = r.store._db
        def define(commit):
            db.execute("INSERT INTO memory_types VALUES ('alder_kind','memory',0,?)", (commit,))
            return {}
        if operation == 'custom_type':
            # Legacy unknown-type rows can precede their first registry entry.
            # Type definitions themselves are immutable; never update one.
            episode, _ = r.store.capture(dict(source='legacy-type', event_id='legacy', conversation_id='legacy',
                kind='user', text='The Alder archive retains its original evidence.', occurred_at=None, metadata={}))
            evidence = [dict(episode_id=episode['id'], quote=episode['text'])]
            request = r.graph._resolve_evidence(dict(type='alder_kind', text=episode['text'],
                scope=['professional'], evidence=evidence))
            node = r.graph._command('legacy-type', 'fixture', 'Legacy unknown-type fixture', {},
                                    lambda commit: r.graph._insert_node(commit, request))['id']
        else:
            r.graph._command('type', 'owner', 'Synthetic custom type', {}, define)
            node, evidence = assertion(r, 'The Alder archive retains its original evidence.', type='alder_kind')
        index(r, node, .9)
        pending, _ = candidate(r, 'The Cedar handbook preserves each original source.', type='alder_kind')
        index(r, pending, .8, kind='candidate')
        episode = evidence[0]['episode_id']
        db.execute('INSERT INTO episode_chunks VALUES (?,?,0,?)', ('raw-chunk', episode, len(evidence[0]['quote'])))
        index(r, 'raw-chunk', .85, kind='episode')
        media, _ = assertion(r, 'The archived document has its original source evidence.', type='media')
        db.execute('INSERT INTO media_files VALUES (?,?,?,?,?)', (media, 'a' * 64, 'fixture.txt', 'text', 'fixture'))
        db.execute('''INSERT INTO segments(id,node_id,modality,text,artifact_sha256,source_sha256)
            VALUES ('page',?,'text','The Alder page preserves its source. ',?,?)''', (media, 'b' * 64, 'c' * 64))
        index(r, 'page', .88, kind='segment')
        if operation in ('media_hide', 'media_reappear'):
            db.execute("INSERT INTO captured_media(sha256,kind,filename,state,node_id) VALUES (?,'text','fixture.txt','indexed',?)",
                       ('a' * 64, media))
            if operation == 'media_hide':
                db.execute('INSERT INTO captured_media_refs VALUES (?,?,0,1,?)', ('a' * 64, episode, 'T'))
        if operation == 'release':
            db.execute('INSERT INTO historical_parked_candidates VALUES (?,NULL)', (pending,))
        if operation == 'merge':
            first, _ = assertion(r, 'Rowan Vale', type='person', scope=['personal'])
            survivor, _ = assertion(r, 'Rowan', type='person', scope=['professional'])
            index(r, first, .95)
            index(r, survivor, .94)
        def payloads():
            return r._projection_payloads(db, db.execute('''SELECT i.*,v.current FROM vector_items i
                JOIN memory_vectors v ON v.rowid=i.seq''').fetchall())
        before = payloads()
        fence = db.execute('SELECT max(id) FROM search_outbox').fetchone()[0]
        if operation == 'candidate_terminal':
            def reject(commit):
                db.execute("INSERT INTO candidate_decisions VALUES ('terminal',?,?,'reject',NULL,NULL)", (pending, commit))
                return {}
            r.graph._command('reject', 'owner', 'Synthetic terminal decision', {}, reject)
        elif operation == 'parking':
            Processing(r.graph).park_history()
        elif operation == 'release':
            cite(r.graph, event_id='release', ids=[pending])
        elif operation == 'custom_type':
            def register(commit):
                db.execute("INSERT INTO memory_types VALUES ('alder_kind','entity',0,?)", (commit,))
                return {}
            r.graph._command('register-entity', 'owner', 'Register the previously unknown entity type', {}, register)
        elif operation == 'source_forget':
            forget(r, node)  # Existing owner forget also excludes its original source.
        elif operation == 'media_hide':
            db.execute('DELETE FROM captured_media_refs WHERE sha256=?', ('a' * 64,))
        elif operation == 'media_reappear':
            db.execute('INSERT INTO captured_media_refs VALUES (?,?,0,1,?)', ('a' * 64, episode, 'T'))
        elif operation == 'delete_vector':
            db.execute('DELETE FROM vector_items WHERE target=?', (node,))
        elif operation == 'correction':
            r.graph.supersede(command_id='correct', actor='owner', why='Synthetic supported refinement',
                old=node, new=dict(type='alder_kind', text='The Alder archive keeps source evidence.', scope=['professional']),
                evidence=evidence, kind='refinement')
        elif operation == 'retire':
            r.graph._command('retire', 'owner', 'Synthetic retirement', {}, lambda commit: retire(r.graph, commit, node, evidence))
        else:
            r.graph._command('merge', 'owner', 'Synthetic reviewed merge', {}, lambda commit:
                apply_review(r.graph, commit, dict(op='merge', a=first, b=survivor)))
        after = payloads()
        changed = {seq for seq in before if before[seq] != after.get(seq)}
        dirty = {row[0] for row in db.execute('SELECT seq FROM search_outbox WHERE id>?', (fence,))}
        assert changed, 'Fixture must actually change semantic eligibility'
        assert changed <= dirty, 'A changed point was not invalidated in the same transaction'
    finally:
        r.close()
        r.store.close()


def test_outbox_rollback_and_legacy_writer_need_only_builtin_sql(tmp_path, monkeypatch):
    r, vector = make_retrieval(tmp_path, monkeypatch)
    try:
        target, _ = assertion(r, 'The Alder record has immutable source evidence.')
        index(r, target, .9)
        before = [tuple(row) for row in r.store._db.execute('SELECT * FROM search_outbox')]
        r.store._db.execute('BEGIN IMMEDIATE')
        r.store._db.execute('UPDATE vector_items SET version=2 WHERE target=?', (target,))
        r.store._db.execute('ROLLBACK')
        assert [tuple(row) for row in r.store._db.execute('SELECT * FROM search_outbox')] == before
        with sqlite3.connect(tmp_path / 'memory.sqlite') as old_writer:
            # No registered Python UDF and no Qdrant connection in this writer.
            old_writer.execute('UPDATE vector_items SET version=2 WHERE target=?', (target,))
        later = list(r.store._db.execute('SELECT * FROM search_outbox WHERE id>?', (before[-1][0],)))
        assert later and all(row['seq'] == r.store._db.execute(
            'SELECT seq FROM vector_items WHERE target=?', (target,)).fetchone()[0] for row in later)
        assert r.store._db.execute('PRAGMA user_version').fetchone()[0] == 2
        with pytest.raises(sqlite3.IntegrityError, match='immutable'):
            r.store._db.execute("UPDATE search_outbox SET nonce='changed'")
    finally:
        r.close()
        r.store.close()


@pytest.mark.parametrize('previous', ['missing', 'unguarded'])
def test_parking_trigger_upgrade_is_atomic_and_idempotent(tmp_path, monkeypatch, previous):
    import memento.store as store_module
    name = 'search_historical_parking_update'
    store = Store(tmp_path)
    original, _ = store.capture(dict(source='startup', event_id='one', conversation_id='startup', kind='user',
        text='Exact authoritative source survives a derived trigger upgrade.', occurred_at=None, metadata={}))
    expected = store._db.execute('SELECT sql FROM sqlite_schema WHERE name=?', (name,)).fetchone()[0]
    store._db.execute(f'DROP TRIGGER {name}')
    old = None
    if previous == 'unguarded':
        old = expected[:expected.index('WHEN')] + expected[expected.index('BEGIN'):]
        store._db.execute(old)
    version = store._db.execute('PRAGMA schema_version').fetchone()[0]
    events = [tuple(row) for row in store._db.execute('SELECT * FROM search_outbox')]
    store.close()
    upgrade = store_module.upgrade_search
    def interrupted(db):
        upgrade(db)
        raise RuntimeError('Synthetic interruption after derived trigger upgrade')
    with monkeypatch.context() as patch:
        patch.setattr(store_module, 'upgrade_search', interrupted)
        with pytest.raises(RuntimeError, match='Synthetic interruption'):
            Store(tmp_path)
    with sqlite3.connect(tmp_path / 'memory.sqlite') as db:
        assert db.execute('PRAGMA schema_version').fetchone()[0] == version
        row = db.execute('SELECT sql FROM sqlite_schema WHERE name=?', (name,)).fetchone()
        assert (row[0] if row else None) == old
        assert db.execute('SELECT * FROM search_outbox').fetchall() == events
    store = Store(tmp_path)
    try:
        assert store.get(original['id']) == original
        assert store._db.execute('SELECT sql FROM sqlite_schema WHERE name=?', (name,)).fetchone()[0] == expected
        assert [tuple(row) for row in store._db.execute('SELECT * FROM search_outbox')] == events
        version = store._db.execute('PRAGMA schema_version').fetchone()[0]
    finally:
        store.close()
    store = Store(tmp_path)
    try:
        assert store.get(original['id']) == original
        assert store._db.execute('PRAGMA schema_version').fetchone()[0] == version
        assert store._db.execute('SELECT sql FROM sqlite_schema WHERE name=?', (name,)).fetchone()[0] == expected
    finally:
        store.close()


def test_live_extraction_cursor_does_not_invalidate_historical_vectors(tmp_path, monkeypatch):
    r, _ = make_retrieval(tmp_path, monkeypatch)
    try:
        def capture(event):
            return r.store.capture(dict(source='cursor-fixture', event_id=event, conversation_id='c', kind='user',
                text='The Alder historical record is ' + event + '.', occurred_at=None, metadata={}))[0]
        first = capture('first')
        r.store._db.execute('INSERT INTO episode_chunks VALUES (?,?,0,?)', ('first-chunk', first['id'], len(first['text'])))
        index(r, 'first-chunk', .9, kind='episode')
        Processing(r.graph).park_history()
        second = capture('second')
        r.store._db.execute('INSERT INTO episode_chunks VALUES (?,?,0,?)', ('second-chunk', second['id'], len(second['text'])))
        index(r, 'second-chunk', .8, kind='episode')
        db = r.store._db
        seq = db.execute('SELECT seq FROM episodes WHERE id=?', (second['id'],)).fetchone()[0]
        fence = db.execute('SELECT max(id) FROM search_outbox').fetchone()[0]
        db.execute("UPDATE historical_parking SET live_seq=?,live_offset=100 WHERE source='cursor-fixture' AND conversation_id='c'", (seq,))
        assert db.execute('SELECT count(*) FROM search_outbox WHERE id>?', (fence,)).fetchone()[0] == 0
        rows = db.execute('''SELECT i.*,v.current FROM vector_items i JOIN memory_vectors v ON v.rowid=i.seq''').fetchall()
        before = r._projection_payloads(db, rows)
        db.execute("UPDATE historical_parking SET parked_through=? WHERE source='cursor-fixture' AND conversation_id='c'", (seq,))
        after = r._projection_payloads(db, rows)
        changed = {item['seq'] for item in rows if before[item['seq']] != after[item['seq']]}
        assert changed and changed <= {row[0] for row in db.execute('SELECT seq FROM search_outbox WHERE id>?', (fence,))}
    finally:
        r.close()
        r.store.close()
