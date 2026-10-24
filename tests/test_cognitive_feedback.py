"""Explicit-query feedback: behavioral lessons ignore negative signal; everything else keeps it."""

import math
from types import SimpleNamespace
from uuid import uuid4

import pytest

np = pytest.importorskip('numpy')

from memento.graph import Graph
from memento.importance import query_key
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

        q = np.zeros(768, np.float32)
        q[0] = 1
        monkeypatch.setattr('memento.retrieval.embed_query', lambda model, text: q)
        tokenizer = lambda text, **kwargs: {'offset_mapping': [(i, i + 1) for i in range(len(text))]}
        retrieval = Retrieval(graph, model=SimpleNamespace(tokenizer=tokenizer))
        for target in (lesson, fact, incident_only):
            _index(retrieval, target, .9)

        queries = dict(neutral='neutral cedar question', negative='negative cedar question',
                       positive='positive cedar question')
        for target in (lesson, fact, incident_only):
            _feedback(store, queries['negative'], target, shown=4, cited=0)
            _feedback(store, queries['positive'], target, shown=4, cited=4)
        scores = {name: {item['id']: item['score'] for item in retrieval.recall(query, effort='cheap', limit=10)}
                  for name, query in queries.items()}
        for name in queries:
            assert set(scores[name]) == {lesson, fact, incident_only}

        # Behavioral lesson: negative signal clamps to neutral, reward still applies.
        assert scores['negative'][lesson] == pytest.approx(scores['neutral'][lesson])
        assert scores['positive'][lesson] == pytest.approx(scores['neutral'][lesson] * 1.1)
        # Ordinary fact and non-behavioral incident keep the full signal in both directions.
        for target in (fact, incident_only):
            assert scores['negative'][target] == pytest.approx(scores['neutral'][target] * 0.9)
            assert scores['positive'][target] == pytest.approx(scores['neutral'][target] * 1.1)
    finally:
        store.close()
