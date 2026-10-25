"""Feedback breaks relevance ties; behavioral lessons alone ignore negative signal."""

import math
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

np = pytest.importorskip('numpy')

from memento.graph import Graph
from memento.importance import cite, query_key
from memento.processing import Processing
from memento.retrieval import Retrieval
from memento.store import Store
from test_behavioral_learning import action, complete, extract


def _assertion(graph, text, *, type):
    key = uuid4().hex
    episode, _ = graph.store.capture(dict(source='feedback-fixture', event_id=key, conversation_id='facts',
                                          kind='user', text=text, occurred_at=None, metadata={}))
    return graph.remember(command_id=key, actor='user', why='explicit assertion', type=type, text=text,
                          scope=['professional'], evidence=[dict(episode_id=episode['id'], quote=text)])['id']


def _index(retrieval, target, cosine):
    vector = np.zeros(768, np.float32)
    vector[0], vector[1] = cosine, math.sqrt(1 - cosine * cosine)
    with retrieval.store._mutex:
        retrieval.store._db.execute('BEGIN IMMEDIATE')
        try:
            retrieval._put(target, 'node', 1, vector)
            retrieval.store._db.execute('COMMIT')
        except BaseException:
            retrieval.store._db.execute('ROLLBACK')
            raise


def _feedback(store, query, target, *, shown, cited):
    with store._mutex:
        store._db.execute('INSERT INTO query_feedback(query_key,target,shown,cited) VALUES (?,?,?,?)',
                          (query_key(query), target, shown, cited))


def test_negative_feedback_is_neutral_only_for_behavioral_lessons(tmp_path, monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 10, 25, 12, tzinfo=timezone.utc)
    monkeypatch.setattr('memento.importance.datetime', Clock)
    monkeypatch.setattr('memento.retrieval.datetime', Clock)
    store = Store(tmp_path)
    try:
        graph = Graph(store)
        processing = Processing(graph)
        incident = ('I had to edit three settings by hand for Project Cedar and it failed twice. '
                    'Using the shipped installer removed those errors and saved the repeated setup work.')
        episode, task = extract(processing, incident)
        lesson = complete(processing, task, action(task, episode))['id']
        assert graph._behavior(lesson) is not None
        fact = _assertion(graph, 'The Cedar release ships with a cobalt checklist.', type='knowledge')
        incident_only = _assertion(graph, 'The Cedar deploy failed twice before the installer fixed it.', type='experience')
        assert graph._behavior(incident_only) is None
        # Neutral peers on both sides of the stable-ID tie expose both reward
        # and penalty, independently of the generated target IDs.
        peers = []
        for digit in ('0', 'f'):
            with monkeypatch.context() as fixed:
                fixed.setattr('memento.graph.uuid4', lambda: SimpleNamespace(hex=digit * 32))
                peers.append(_assertion(graph, 'The neutral comparator has label ' + digit + '.', type='knowledge'))
        low, high = peers
        targets = (lesson, fact, incident_only)
        assert all(low < target < high for target in targets)
        stronger = _assertion(graph, 'The requested record has more relevant source details.', type='knowledge')
        tied = (*targets, *peers)
        all_targets = (*tied, stronger)
        # Unknown original dates have neutral age; equal importance leaves
        # feedback, then stable IDs, as the only tie-breakers.
        for target in all_targets:
            store._db.execute('UPDATE node_stats SET importance=.5 WHERE node_id=?', (target,))

        q = np.zeros(768, np.float32)
        q[0] = 1
        monkeypatch.setattr('memento.retrieval.embed_query', lambda model, text: q)
        tokenizer = lambda text, **kwargs: {'offset_mapping': [(i, i + 1) for i in range(len(text))]}
        retrieval = Retrieval(graph, model=SimpleNamespace(tokenizer=tokenizer))
        for target in tied:
            _index(retrieval, target, .9)
        _index(retrieval, stronger, .91)

        queries = dict(neutral='neutral cedar question', negative='negative cedar question',
                       positive='positive cedar question', opposed='opposed cedar question')
        for target in targets:
            _feedback(store, queries['negative'], target, shown=4, cited=0)
            _feedback(store, queries['positive'], target, shown=4, cited=4)
        _feedback(store, queries['opposed'], stronger, shown=4, cited=0)
        for target in tied:
            _feedback(store, queries['opposed'], target, shown=4, cited=4)
        feedback = [tuple(row) for row in store._db.execute('SELECT * FROM query_feedback ORDER BY query_key,target')]
        histories = {target: graph.history(target) for target in all_targets}
        hits = {name: retrieval.recall(query, effort='cheap', limit=10) for name, query in queries.items()}
        orders = {name: [item['id'] for item in rows] for name, rows in hits.items()}
        for name, rows in hits.items():
            assert set(orders[name]) == set(all_targets)
            assert orders[name][0] == stronger, 'Opposing feedback cannot overturn stronger relevance'
            for item in rows:
                assert item['score'] == pytest.approx(.91 if item['id'] == stronger else .9)
                assert item['decay'] == 1 and item['importance'] == .5
        assert orders['neutral'] == [stronger, *sorted(tied)]
        # Behavioral negative feedback remains neutral; ordinary facts and
        # incidents fall below the otherwise equal high-ID neutral peer.
        assert [target for target in orders['negative'] if target in (lesson, high)] == [lesson, high]
        assert all(orders['negative'].index(target) > orders['negative'].index(high)
                   for target in (fact, incident_only))
        # Positive feedback moves every target ahead of the equal low-ID peer.
        assert all(orders['positive'].index(target) < orders['positive'].index(low) for target in targets)
        assert [tuple(row) for row in store._db.execute(
            'SELECT * FROM query_feedback ORDER BY query_key,target')] == feedback

        before = dict(store._db.execute('SELECT access_count,importance FROM node_stats WHERE node_id=?', (lesson,)).fetchone())
        receipt = cite(graph, event_id='committed-feedback', ids=[lesson], query=queries['neutral'], shown=all_targets)
        assert cite(graph, event_id='committed-feedback', ids=[lesson], query=queries['neutral'], shown=all_targets) == receipt
        stats = dict(store._db.execute('SELECT access_count,importance,last_cited_at FROM node_stats WHERE node_id=?', (lesson,)).fetchone())
        assert stats == dict(access_count=before['access_count'] + 1, importance=before['importance'] + .05,
                             last_cited_at=Clock.now().isoformat())
        recorded = {row['target']: (row['shown'], row['cited']) for row in store._db.execute(
            'SELECT target,shown,cited FROM query_feedback WHERE query_key=?', (query_key(queries['neutral']),))}
        assert recorded == {target: (1, int(target == lesson)) for target in all_targets}
        assert [tuple(row) for row in store._db.execute(
            'SELECT * FROM query_feedback WHERE query_key<>? ORDER BY query_key,target',
            (query_key(queries['neutral']),))] == feedback
        renewed = retrieval.recall(queries['neutral'], effort='cheap', limit=10)
        assert [item['id'] for item in renewed[:2]] == [stronger, lesson]
        assert {item['id']: item['score'] for item in renewed} == {
            item['id']: item['score'] for item in hits['neutral']}
        assert {target: graph.history(target) for target in all_targets} == histories
    finally:
        store.close()
