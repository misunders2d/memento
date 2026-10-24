"""Health never queues behind the application mutex, yet still reports real writer contention.

Store.health probes BEGIN IMMEDIATE/ROLLBACK on its own short-lived connection
with a bounded busy wait; Retrieval.health reads failure eligibility from one
WAL snapshot. Neither mutates anything.
"""

import sqlite3
import time
from contextlib import contextmanager
from threading import Event, Thread
from types import SimpleNamespace
from uuid import uuid4

import pytest

from memento.curation import attach
from memento.graph import Graph
from memento.store import Store


@pytest.fixture
def store(tmp_path):
    instance = Store(tmp_path)
    try:
        yield instance
    finally:
        instance.close()


def _hold_mutex(store):
    """Another thread owns the application mutex without any SQLite transaction."""
    held, release = Event(), Event()

    def hold():
        with store._mutex:
            held.set()
            release.wait(10)

    thread = Thread(target=hold)
    thread.start()
    assert held.wait(5)
    return release, thread


def _capture(store, text):
    episode, _ = store.capture(dict(source='test', event_id=uuid4().hex, conversation_id='c', kind='user',
                                    text=text, occurred_at=None, metadata={}))
    return episode


def test_health_completes_while_another_thread_holds_the_mutex_without_a_transaction(store):
    release, thread = _hold_mutex(store)
    try:
        started = time.monotonic()
        store.health()
        assert time.monotonic() - started < 2, 'health waited on the application mutex'
    finally:
        release.set()
        thread.join(5)


def test_real_writer_transaction_fails_health_within_the_bounded_wait(store):
    other = sqlite3.connect(store._data_dir / 'memory.sqlite', isolation_level=None)
    try:
        other.execute('BEGIN IMMEDIATE')
        started = time.monotonic()
        with pytest.raises(sqlite3.OperationalError):
            store.health()
        elapsed = time.monotonic() - started
        assert 0.1 <= elapsed < 2, elapsed
        other.execute('ROLLBACK')
    finally:
        other.close()
    store.health()


def test_closed_store_and_missing_database_are_unhealthy(tmp_path):
    store = Store(tmp_path)
    store.close()
    with pytest.raises(RuntimeError, match='closed'):
        store.health()
    reopened = Store(tmp_path)
    try:
        reopened.health()
        (tmp_path / 'memory.sqlite').unlink()
        with pytest.raises(sqlite3.OperationalError):
            reopened.health()
        assert not (tmp_path / 'memory.sqlite').exists(), 'the health probe created a database file'
    finally:
        reopened.close()


def test_health_probe_writes_and_commits_nothing(store):
    _capture(store, 'Evidence captured before the probe.')
    wal = store._data_dir / 'memory.sqlite-wal'

    def state():
        return (store._db.execute('PRAGMA data_version').fetchone()[0],
                store._db.execute('PRAGMA schema_version').fetchone()[0],
                store._db.execute('SELECT count(*) FROM episodes').fetchone()[0],
                wal.stat().st_size if wal.exists() else 0)

    before = state()
    for _ in range(3):
        store.health()
    assert state() == before
    assert not store._db.in_transaction
    # Ordinary capture still works on the application writer afterwards.
    assert store.get(_capture(store, 'Captured after the probe.')['id'])['text'] == 'Captured after the probe.'


@pytest.fixture
def retrieval(tmp_path):
    pytest.importorskip('numpy')
    from memento.retrieval import Retrieval

    store = Store(tmp_path)
    tokenizer = lambda text, **kwargs: {'offset_mapping': [(i, i + 1) for i in range(len(text))]}
    result = Retrieval(Graph(store), model=SimpleNamespace(tokenizer=tokenizer))
    try:
        yield result
    finally:
        store.close()


def _assertion(r, text):
    episode = _capture(r.store, text)
    evidence = [dict(episode_id=episode['id'], quote=text)]
    node = r.graph.remember(command_id=uuid4().hex, actor='user', why='explicit assertion',
                            type='knowledge', text=text, scope=['professional'], evidence=evidence)
    return node['id'], evidence


def _fail(r, target, version):
    r.store._db.execute('''INSERT INTO index_failures VALUES (?,?,1,'ValueError',0,0)
        ON CONFLICT(target,version) DO UPDATE SET resolved=0''', (target, version))


def _attach(graph, target, evidence):
    request = dict(op='attach', target=target, evidence=evidence)
    return graph._command(uuid4().hex, 'model-fixture', 'explicit synthetic corroboration', request,
                          lambda commit: attach(graph, commit, target, evidence))


def test_index_error_and_eligible_unresolved_failure_still_fail_retrieval_health(retrieval):
    r = retrieval
    r.health()
    node, _ = _assertion(r, 'The failed unit still awaits an embedding.')
    _fail(r, node, 1)
    with pytest.raises(RuntimeError, match='embeddings failed'):
        r.health()
    r.index_error = ValueError('synthetic')
    with pytest.raises(RuntimeError, match='indexing failed'):
        r.health()


def test_obsolete_failures_are_ignored_and_current_failures_still_count(retrieval):
    r = retrieval
    node, evidence = _assertion(r, 'The failed unit still awaits an embedding.')
    _fail(r, node, 1)
    with pytest.raises(RuntimeError):
        r.health()
    # Corroboration makes version 2 current: the version 1 failure is obsolete,
    # and a merely pending current version is not a failure.
    assert _attach(r.graph, node, evidence)['version'] == 2
    r.health()
    _fail(r, node, 2)
    with pytest.raises(RuntimeError):
        r.health()
    r.graph.supersede(command_id='correct', actor='fixture', why='explicit corrected statement', old=node,
                      new=dict(type='knowledge', text='The corrected unit awaits an embedding.', scope=['professional']),
                      evidence=evidence)
    r.health()
    assert r.store._db.execute('SELECT count(*) FROM index_failures WHERE resolved=0').fetchone()[0] == 2


def test_retrieval_health_reads_one_snapshot_without_the_writer_mutex(retrieval, monkeypatch):
    r = retrieval
    node, _ = _assertion(r, 'The failed unit still awaits an embedding.')
    _fail(r, node, 1)
    snapshots = []
    original = r.store.read_snapshot

    @contextmanager
    def counted():
        with original() as db:
            snapshots.append(db)
            yield db

    monkeypatch.setattr(r.store, 'read_snapshot', counted)
    writer_statements = []
    r.store._db.set_trace_callback(writer_statements.append)
    release, thread = _hold_mutex(r.store)
    try:
        started = time.monotonic()
        with pytest.raises(RuntimeError, match='embeddings failed'):
            r.health()
        assert time.monotonic() - started < 2, 'retrieval health waited on the application mutex'
    finally:
        release.set()
        thread.join(5)
        r.store._db.set_trace_callback(None)
    assert len(snapshots) == 1
    assert not [sql for sql in writer_statements if 'index_failures' in sql]
    assert r.store._db.execute('SELECT count(*) FROM index_failures WHERE resolved=0').fetchone()[0] == 1
