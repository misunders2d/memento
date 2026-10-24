"""Rejected model output leaves bounded private feedback for that job's next attempt only."""

import io
import json
import sqlite3
import time
import urllib.error
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from memento import runner
from memento.api import create_app
from memento.graph import Graph
from memento.jobs import LostLease
from memento.processing import Processing
from memento.store import Store
from test_behavioral_learning import action, extract

ARGS = dict(agent='fixture', timezone_name='UTC')
NOW = datetime.now(timezone.utc).replace(hour=12)


def stored(store, job_id):
    row = store._db.execute('SELECT context FROM jobs WHERE id=?', (job_id,)).fetchone()
    return json.loads(row[0]).get('previous_rejection')


def reclaim(store, processing, **kwargs):
    store._db.execute('UPDATE jobs SET available_at=0')
    return processing.claim(**ARGS, now=NOW, **kwargs)['work']


def draft(quote, text='The approval policy requires explicit operator consent.'):
    return dict(key='policy', kind='memory', type='knowledge', text=text, quote=quote,
        why='Exact durable source assertion', occurrence=None, src_key=None, dst_key=None,
        valid_from=None, valid_to=None, scope=['professional'], open_loop=False, confidence=.8)


def test_consolidation_rejection_feedback_is_bounded_private_served_and_replaced(tmp_path):
    store = Store(tmp_path)
    try:
        processing = Processing(Graph(store))
        episode, task = extract(processing, 'Manual setup failed repeatedly; using the installer fixed setup.')
        bad = action(task, episode, quote='Unobserved ' + 'x' * 400)
        with pytest.raises(ValueError) as failure:
            processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[bad]))
        assert str(failure.value) == 'candidate_quote_not_in_source'
        feedback = stored(store, task['id'])
        assert feedback['code'] == 'candidate_quote_not_in_source' and feedback['attempt'] == 1
        [reference] = feedback['references']
        assert reference['candidate_id'] == bad['candidate_id'] and reference['operation'] == 'remember'
        assert reference['episode_id'] == episode['id'] and len(reference['quote']) == 300
        assert 'failed model output' in feedback['note']
        # Nothing committed, and the lease is untouched until the runner's normal release.
        assert store._db.execute('SELECT count(*) FROM node_versions').fetchone()[0] == 0
        assert not store._db.execute('SELECT 1 FROM graph_commits WHERE command_id=?', ('consolidate/' + task['id'],)).fetchone()
        assert tuple(store._db.execute('SELECT state,owner FROM jobs WHERE id=?', (task['id'],)).fetchone()) == ('leased', task['owner'])
        processing.release(id=task['id'], owner=task['owner'], error='service_rejected')
        # A restarted service serves the feedback beside the fresh context under the same job identity.
        processing = Processing(Graph(store))
        [again] = reclaim(store, processing, consolidate_now=True)
        assert again['id'] == task['id'] and again['owner'] != task['owner']
        assert again['input']['previous_rejection'] == feedback and stored(store, task['id']) == feedback
        assert again['input']['candidates'][0]['id'] == bad['candidate_id']
        # Another failure replaces the feedback instead of accumulating history.
        with pytest.raises(ValueError, match='consolidation_schema_invalid'):
            processing.complete(id=again['id'], owner=again['owner'], agent='fixture', output=dict(actions='nonsense'))
        replaced = stored(store, task['id'])
        assert (replaced['code'], replaced['attempt'], replaced['references']) == ('consolidation_schema_invalid', 2, [])
        processing.release(id=again['id'], owner=again['owner'], error='invalid_output')
        [third] = reclaim(store, processing, consolidate_now=True)
        assert third['input']['previous_rejection'] == replaced
        result = processing.complete(id=third['id'], owner=third['owner'], agent='fixture',
                                     output=dict(actions=[action(third, episode)]))
        assert result['results'][bad['candidate_id']]['decision'] == 'promote'
        assert store._db.execute('SELECT state FROM jobs WHERE id=?', (task['id'],)).fetchone()[0] == 'done'
    finally:
        store.close()


def test_lost_or_superseded_lease_never_writes_feedback(tmp_path):
    store = Store(tmp_path)
    try:
        processing = Processing(Graph(store))
        episode, task = extract(processing, 'Manual setup failed repeatedly; using the installer fixed setup.')
        bad = action(task, episode, quote='Unobserved quote')
        store._db.execute('UPDATE jobs SET lease_until=? WHERE id=?', (time.time() - 1, task['id']))
        with pytest.raises(LostLease):
            processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[bad]))
        assert stored(store, task['id']) is None
        [fresh] = processing.claim(**ARGS, now=NOW, consolidate_now=True)['work']
        assert fresh['id'] == task['id'] and fresh['owner'] != task['owner']
        # The old owner's late rejection cannot touch the new owner's context.
        with pytest.raises(LostLease):
            processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[bad]))
        assert stored(store, task['id']) is None
        with pytest.raises(ValueError, match='candidate_quote_not_in_source'):
            processing.complete(id=fresh['id'], owner=fresh['owner'], agent='fixture', output=dict(actions=[bad]))
        assert stored(store, task['id'])['attempt'] == 2
    finally:
        store.close()


def test_feedback_storage_failure_keeps_the_original_rejection_and_lease(tmp_path, caplog):
    store = Store(tmp_path)
    try:
        processing = Processing(Graph(store))
        episode, task = extract(processing, 'Manual setup failed repeatedly; using the installer fixed setup.')
        real = store._db

        class Broken:
            def __getattr__(self, name):
                return getattr(real, name)

            def execute(self, sql, *args):
                if 'UPDATE jobs SET context' in sql:
                    raise sqlite3.OperationalError('disk I/O error')
                return real.execute(sql, *args)

        store._db = Broken()
        try:
            with pytest.raises(ValueError, match='candidate_quote_not_in_source'):
                processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
                                    output=dict(actions=[action(task, episode, quote='Unobserved quote')]))
        finally:
            store._db = real
        assert stored(store, task['id']) is None
        assert 'Rejection feedback not stored (OperationalError)' in caplog.text
        assert store._db.execute('SELECT state FROM jobs WHERE id=?', (task['id'],)).fetchone()[0] == 'leased'
        assert not store._db.execute('SELECT 1 FROM node_versions').fetchone()
    finally:
        store.close()


def test_extraction_feedback_names_the_window_handle_inside_the_compact_view(tmp_path):
    store = Store(tmp_path)
    try:
        processing = Processing(Graph(store))
        source = 'native:synthetic-device:claude'
        episode = store.capture(dict(source=source, event_id='e1', conversation_id='c', kind='user',
            text='The approval policy requires explicit operator consent.', occurred_at=None, metadata={}))[0]
        [work] = processing.claim(**ARGS, now=NOW, compaction=dict(source=source, conversation_id='c'))['work']
        with pytest.raises(ValueError, match='candidate_quote_not_in_source'):
            processing.complete(id=work['id'], owner=work['owner'], agent='fixture',
                                output=dict(windows=[dict(id='w0', candidates=[draft('Invented consent quote')])]))
        feedback = stored(store, work['id'])
        assert feedback['references'] == [dict(window_id='w0', key='policy', quote='Invented consent quote', occurrence=None)]
        assert store._db.execute('SELECT count(*) FROM candidates').fetchone()[0] == 0
        processing.release(id=work['id'], owner=work['owner'], error='service_rejected')
        [again] = reclaim(store, processing)
        assert again['id'] == work['id'] and set(again['input']) == {'windows', 'previous_rejection'}
        assert again['input']['previous_rejection'] == feedback
        assert again['input']['windows'][0]['id'] == 'w0' and 'episode_id' not in again['input']['windows'][0]
        result = processing.complete(id=again['id'], owner=again['owner'], agent='fixture',
                                     output=dict(windows=[dict(id='w0', candidates=[draft(episode['text'])])]))
        assert result['candidate_count'] == 1
    finally:
        store.close()


def test_rejection_response_and_runner_receipt_carry_only_the_fixed_code(tmp_path, monkeypatch):
    monkeypatch.setenv('MEMENTO_EMBEDDINGS', '0')
    with TestClient(create_app(tmp_path)) as client:
        source = 'native:fixture:claude'
        assert client.post('/episodes', json=dict(source=source, event_id='e1', conversation_id='c', kind='user',
            text='Durable fact text.', metadata={})).status_code in (200, 201)
        [work] = client.post('/work/claim', json=dict(agent='fixture', timezone='UTC',
            compaction=dict(source=source, conversation_id='c'))).json()['work']
        response = client.post('/work/complete', json=dict(id=work['id'], owner=work['owner'], agent='fixture',
            output=dict(windows=[dict(id='w0', candidates=[draft('Secret invented quote')])])))
        assert response.status_code == 422 and response.json() == dict(detail='candidate_quote_not_in_source')
        assert 'Secret invented quote' not in response.text
        error = urllib.error.HTTPError('http://service/work/complete', 422, 'Unprocessable', {}, io.BytesIO(response.content))
        failure = runner.RunnerError('service_rejected')
        failure.__cause__ = error
        receipt = runner.work_failure_receipt(work['id'], 'extract', 'service_rejected', failure)
        assert receipt == dict(id=work['id'], kind='extract', error='service_rejected', http_status=422,
                               detail_code='candidate_quote_not_in_source')
        # The private job context alone keeps the bounded offending quote.
        assert stored(client.app.state.store, work['id'])['references'][0]['quote'] == 'Secret invented quote'


def test_unknown_value_errors_are_never_recorded_as_feedback(tmp_path, monkeypatch):
    store = Store(tmp_path)
    try:
        processing = Processing(Graph(store))
        episode, task = extract(processing, 'Manual setup failed repeatedly; using the installer fixed setup.')
        monkeypatch.setattr(processing.consolidator, 'apply',
                            lambda *args, **kwargs: (_ for _ in ()).throw(ValueError('plausible_unknown_code')))
        with pytest.raises(ValueError, match='^plausible_unknown_code$'):
            processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
                                output=dict(actions=[action(task, episode)]))
        assert stored(store, task['id']) is None
    finally:
        store.close()


def test_work_rejected_carries_only_known_codes_and_bounded_reference_fields():
    from memento.extraction import VALIDATION_CODES, WorkRejected
    with pytest.raises(TypeError):
        WorkRejected('arbitrary_text_from_anywhere')
    rejected = WorkRejected('candidate_quote_not_in_source', [
        dict(window_id='w0', key='k', quote='q' * 400, occurrence=None, traceback='secret', body='secret')] * 5)
    assert str(rejected) == 'candidate_quote_not_in_source' and len(rejected.references) == 3
    assert rejected.references[0] == dict(window_id='w0', key='k', quote='q' * 300, occurrence=None)
    assert 'consolidation_technical_evidence_invalid' in VALIDATION_CODES


def test_feedback_survives_closing_and_reopening_the_store(tmp_path):
    store = Store(tmp_path)
    processing = Processing(Graph(store))
    episode, task = extract(processing, 'Manual setup failed repeatedly; using the installer fixed setup.')
    with pytest.raises(ValueError, match='candidate_quote_not_in_source'):
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
                            output=dict(actions=[action(task, episode, quote='Unobserved quote')]))
    feedback = stored(store, task['id'])
    processing.release(id=task['id'], owner=task['owner'], error='service_rejected')
    store.close()
    store = Store(tmp_path)
    try:
        processing = Processing(Graph(store))
        [again] = reclaim(store, processing, consolidate_now=True)
        assert again['id'] == task['id'] and again['input']['previous_rejection'] == feedback
    finally:
        store.close()


def test_fixed_graph_rule_rejections_also_leave_feedback_without_changing_the_response(tmp_path, monkeypatch):
    monkeypatch.setenv('MEMENTO_EMBEDDINGS', '0')
    with TestClient(create_app(tmp_path)) as client:
        store, processing = client.app.state.store, client.app.state.processing
        episode, task = extract(processing, 'Manual setup failed repeatedly; using the installer fixed setup.')
        both_scopes = dict(action(task, episode, behavior=None), scope=['personal', 'professional'])
        response = client.post('/work/complete', json=dict(id=task['id'], owner=task['owner'], agent='fixture',
                                                           output=dict(actions=[both_scopes])))
        assert response.status_code == 422 and response.json() == dict(detail='Only an entity may have both scopes')
        feedback = stored(store, task['id'])
        assert feedback['code'] == 'Only an entity may have both scopes' and feedback['references'] == []
        assert not store._db.execute('SELECT 1 FROM node_versions').fetchone()
        processing.release(id=task['id'], owner=task['owner'], error='service_rejected')
        [again] = reclaim(store, processing, consolidate_now=True)
        assert again['input']['previous_rejection'] == feedback
