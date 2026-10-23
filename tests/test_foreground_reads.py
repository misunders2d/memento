"""Foreground recall and raw search read one WAL snapshot and never hold the capture writer mutex."""

from contextlib import contextmanager
from threading import Event, Thread

import pytest

np = pytest.importorskip('numpy')

from memento.store import Store
from test_retrieval import assertion, index, retrieval  # noqa: F401  (fixture reuse)


def _pause_at(statement, reached, release):
    """Block the thread executing the first statement that contains the marker."""
    def trace(sql):
        if statement in sql and not reached.is_set():
            reached.set()
            release.wait(5)
    return trace


def _trace_every_connection(monkeypatch, store, trace):
    original = store.read_snapshot

    @contextmanager
    def traced():
        with original() as db:
            db.set_trace_callback(trace)
            yield db

    monkeypatch.setattr(store, 'read_snapshot', traced)
    store._db.set_trace_callback(trace)


@pytest.mark.parametrize('path', ['automatic', 'deep', 'raw_search'])
def test_foreground_reads_leave_the_writer_free_for_health_and_capture(retrieval, monkeypatch, path):
    r = retrieval
    node, evidence = assertion(r, 'The synthetic user works in the Europe/Berlin timezone.')
    index(r, node, .9)
    expected = evidence[0]['episode_id'] if path == 'raw_search' else node
    reached, release = Event(), Event()
    marker = 'memory_vectors v JOIN vector_items i' if path == 'automatic' else 'episodes_fts MATCH'
    _trace_every_connection(monkeypatch, r.store, _pause_at(marker, reached, release))
    outcome = {}

    def read():
        if path == 'automatic':
            outcome['result'] = r.recall('Which timezone does the user work in?', automatic=True)
        elif path == 'deep':
            outcome['result'] = r.recall('timezone', effort='deep')
        else:
            outcome['result'] = r.store.search('timezone')

    reader = Thread(target=read)
    reader.start()
    try:
        assert reached.wait(5), 'the read never reached its traced statement'
        finished = Event()

        def foreground():
            r.store.health()
            r.store.capture(dict(source='test', event_id='during-read', conversation_id='c', kind='user',
                                 text='captured while a read was in progress', occurred_at=None, metadata={}))
            finished.set()

        writer = Thread(target=foreground)
        writer.start()
        blocked = not finished.wait(2)
    finally:
        release.set()
        reader.join(5)
    writer.join(5)
    assert not blocked, 'health/capture waited on the writer mutex while a foreground read ran'
    assert not reader.is_alive() and 'result' in outcome
    assert any(item['id'] == expected for item in outcome['result'])
    # The paused read used one snapshot: the capture committed meanwhile is absent from it.
    assert r.store.get(r.store.search('progress')[0]['id'])['text'].startswith('captured while')


def test_raw_search_builds_snippets_only_for_returned_rows(tmp_path):
    store = Store(tmp_path)
    try:
        for number in range(40):
            store.capture(dict(source='probe', event_id=f'e{number}', conversation_id='c', kind='user',
                               text=f'memento keeps exact evidence number {number}. ' * 50,
                               occurred_at=None, metadata={}))
        fetches = []
        with store.read_snapshot() as db:
            # FTS5 reads each document body through this content-table statement.
            db.set_trace_callback(lambda sql: fetches.append(sql) if "FROM 'main'.'episodes' T" in sql else None)
            bounded = store.search('memento', 5, db=db)
            db.set_trace_callback(None)
            complete = store.search('memento', 40, db=db)
        assert len(bounded) == 5 and len(complete) == 40
        assert len(fetches) <= 5, 'snippets were built for rows that were never returned'
        # Ranking still covers every match; only snippet construction is bounded.
        assert [row['id'] for row in bounded] == [row['id'] for row in complete[:5]]
        assert all(row['excerpt'] for row in bounded)
    finally:
        store.close()
