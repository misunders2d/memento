"""Isolated real bridge/capture/processing plumbing; scripted output is not model judgment."""

import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import patch

from fastapi.testclient import TestClient

from memento import bridge
from memento.api import create_app
from memento.graph import Graph
from memento.processing import Processing
from memento.store import Store


LESSONS = [
    dict(text='When connecting a native agent, use its supported installer.',
         quote='Using the supported installer avoided the repeated setup errors when connecting an agent.',
         behavior=dict(applicability='Connecting a native agent.', conduct='Use its supported installer.',
                       rationale='Avoid repeated setup errors.', exceptions=[], authority='inferred')),
    dict(text='When connecting a native agent, use its supported installer and keep human permission prompts explicit.',
         quote='Keep native permission prompts explicit; never bypass the human approval step.',
         behavior=dict(applicability='Connecting a native agent.',
                       conduct='Use its supported installer and keep human permission prompts explicit.',
                       rationale='Avoid setup errors while retaining human approval.',
                       exceptions=['Wait for human approval when the native client requires it.'], authority='inferred')),
]
CASES = [
    dict(role='relevant', text='For future agent setups, require a captured test event before declaring the connection ready. '
         'The installer finished here, but capture was still disconnected.'),
    dict(role='unrelated', text='A separate correction: our release meeting is Thursday, not Friday.'),
    dict(role='temporary', text='For this sandbox demonstration only, copy the files by hand. '
         'Keep my general preference for the supported installer unchanged.'),
]
INVARIANTS = [
    'Relevant lasting feedback refines the CURRENT successor, preserving human approval and adding the captured-test-event readiness check.',
    'The unrelated meeting correction does not revise the installer lesson.',
    'The temporary sandbox exception does not erase or reverse the general installer preference.',
    'Only genuine source quotes authorize changes; the offered historical definition is advisory and is not resurrected.',
]
SOURCE, CONVERSATION = 'native:exposure-integration:pi', 'feedback'


class OfferClock(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 1, 1, 0, 1, tzinfo=timezone.utc)


@contextmanager
def prepared_case(directory):
    """No model, server thread, installed integration or real endpoint is used."""
    store = Store(directory / 'service')
    graph = Graph(store)
    processing = Processing(graph)
    app = create_app(directory / 'service')
    app.state.store, app.state.graph, app.state.processing = store, graph, processing
    # No lifespan: use real HTTP validation/routes without starting workers/admin/model.
    client = TestClient(app)
    try:
        def post(endpoint, value):
            response = client.post(endpoint, json=value)
            assert response.status_code in (200, 201), response.text
            return response.json()

        def capture(event, text, minute, conversation='premises'):
            return post('/episodes', dict(source=SOURCE, event_id=event, conversation_id=conversation,
                kind='user', text=text, occurred_at=f'2026-01-01T00:{minute:02}:00Z', metadata={}))['id']

        premises = [capture('premise-1', LESSONS[0]['quote'], 0)]
        original = graph._command('seed-original', 'fixture', 'Exact causal source',
            dict(op='consolidate', output=dict(actions=[dict(candidate_id='seed', operation='remember',
                type='experience', behavior=LESSONS[0]['behavior'])])),
            lambda commit: dict(results={'seed': graph._apply_remember(commit,
                dict(type='experience', text=LESSONS[0]['text'], scope=['professional'],
                     evidence=[dict(episode_id=premises[0], quote=LESSONS[0]['quote'])]))}))['results']['seed']['id']
        config = dict(uid=os.getuid(), client_uid='exposure-integration', preset='pi', agent='pi',
                      url='http://127.0.0.1:1', timezone='UTC')
        config_path = directory / 'runner.json'
        bridge.private_write(config_path, bridge.encode(config))
        spool = bridge.Spool(config_path, config)
        memory = dict(id=original, version=1, text=LESSONS[0]['text'], category='behavioral_suggestion',
                      tier='node', behavior=LESSONS[0]['behavior'])
        with patch.object(bridge, 'http', return_value=dict(memories=[memory])), \
                patch.object(bridge.datetime, 'datetime', OfferClock):
            offers = [bridge.native(spool, dict(harness='pi', session_id=CONVERSATION,
                       action='recall', query='Connect the agent.'), recall=bridge.passive) for _ in range(2)]
        assert offers[0] == offers[1] and original in offers[0]['shown']
        spool.commit()
        queued = [bytes(row[0]) for row in spool.db.execute('SELECT body FROM outbox ORDER BY id')]
        assert len(queued) == 2
        assert len({json.loads(raw)['event_id'] for raw in queued}) == 2
        spool.close()
        # The historical offered version is replaced before correction/upload.
        premises.append(capture('premise-2', LESSONS[1]['quote'], 2))
        successor = graph._command('seed-successor', 'fixture', 'Preserve native approval',
            dict(op='consolidate', output=dict(actions=[dict(candidate_id='seed', operation='supersede',
                type='experience', behavior=LESSONS[1]['behavior'])])),
            lambda commit: dict(results={'seed': graph._apply_supersede(commit,
                dict(old=original, new=dict(type='experience', text=LESSONS[1]['text'], scope=['professional']),
                     kind='refinement', valid_at=None, evidence=[dict(episode_id=episode, quote=lesson['quote'])
                     for episode, lesson in zip(premises, LESSONS)]))}))['results']['seed']['superseded_by']
        feedback = {case['role']: capture(case['role'], case['text'], 3 + index, CONVERSATION)
                    for index, case in enumerate(CASES)}
        attempts = []

        def upload(_config, endpoint, body, **_kwargs):
            assert endpoint == '/episodes'
            attempts.append(bridge.encode(body))
            result = post(endpoint, body)
            if len(attempts) == 1:
                raise TimeoutError('synthetic lost acknowledgment')
            return result

        with patch.object(bridge, 'http', side_effect=upload):
            assert bridge.upload(config_path, config)['pending'] == 2
            assert bridge.upload(config_path, config) == dict(pending=0, conflicts=0)
        assert attempts == [queued[0], queued[0], queued[1]]
        receipts = [store._db.execute('SELECT id FROM episodes WHERE source=? AND event_id=?',
                    (SOURCE, json.loads(raw)['event_id'])).fetchone()[0] for raw in queued]
        for _ in range(8):
            if not store.compress_raw_step():
                break
        else:
            raise AssertionError('Tiny compression fixture did not drain')
        assert all(store._db.execute('SELECT raw_frame FROM episodes WHERE id=?', (receipt,)).fetchone()[0]
                   is not None for receipt in receipts)
        assert all(post('/episodes', json.loads(raw))['id'] == receipt for raw, receipt in zip(queued, receipts))
        task = post('/work/claim', dict(agent='fixture', timezone='UTC',
                    compaction=dict(source=SOURCE, conversation_id=CONVERSATION)))['work'][0]
        assert task['kind'] == 'extract'
        context = json.loads(store._db.execute('SELECT context FROM jobs WHERE id=?', (task['id'],)).fetchone()[0])
        assert {window['episode_id']: window['text'] for window in context['windows']} == {
            feedback[case['role']]: case['text'] for case in CASES}
        extracted = post('/work/complete', dict(id=task['id'], owner=task['owner'], agent='fixture',
            output=dict(windows=[dict(id=window['id'], candidates=[dict(key='feedback', kind='memory',
                type='experience', text=window['text'], quote=window['text'], why='Original user feedback',
                occurrence=None, src_key=None, dst_key=None, valid_from=None, valid_to=None,
                scope=['professional'], open_loop=False, confidence=.8)]) for window in task['input']['windows']])))
        get = store.get

        def no_receipt_hydration(episode):
            assert episode not in receipts, 'Receipt context must use metadata, not hydrate frames'
            return get(episode)

        with patch.object(store, 'get', side_effect=no_receipt_hydration):
            task = post('/work/claim', dict(agent='fixture', timezone='UTC', consolidate_now=True))['work'][0]
        assert task['kind'] == 'consolidate'
        candidates = {next(role for role, episode in feedback.items()
                           if episode == candidate['evidence'][0]['episode_id']): candidate['id']
                      for candidate in task['input']['candidates']}
        yield dict(store=store, graph=graph, client=client, processing=processing, task=task,
                   original=original, successor=successor, premises=premises, feedback=feedback,
                   candidates=candidates, receipts=receipts, offers=offers, extracted=extracted)
    finally:
        client.close()
        store.close()


def scripted_plan(case):
    """A known valid plan checks transport/application, not semantic model performance."""
    common = dict(type='decision', scope=['professional'], open_loop=False, target=None,
                  src=None, dst=None, relation=None, change_kind=None, valid_at=None,
                  valid_from=None, valid_to=None, category=None, sensitive=None, behavior=None)
    actions = [dict(common, candidate_id=case['candidates'][item['role']], operation='remember',
        text=item['text'], why='Preserve genuine task feedback', evidence=[dict(
            episode_id=case['feedback'][item['role']], quote=item['text'], occurrence=None)]) for item in CASES]
    actions[0].update(operation='supersede', target=case['successor'], type='experience', change_kind='refinement',
        text=LESSONS[1]['text'] + ' Require a captured test event before declaring setup ready.',
        behavior=dict(LESSONS[1]['behavior'], conduct=LESSONS[1]['behavior']['conduct'] +
                      ' Require a captured test event before declaring setup ready.'),
        evidence=[dict(episode_id=episode, quote=lesson['quote'], occurrence=None)
                  for episode, lesson in zip(case['premises'], LESSONS)] + actions[0]['evidence'])
    return dict(actions=actions)


def test_fresh_offers_survive_replay_frames_and_refine_current_successor(tmp_path):
    with prepared_case(tmp_path) as case:
        context = case['task']['input']
        assert len(context['behavior_exposures']) == 2
        assert all(item['shown'] == dict(id=case['original'], version=1, text=LESSONS[0]['text'],
                                        behavior=LESSONS[0]['behavior']) for item in context['behavior_exposures'])
        assert all(item['current_target'] == case['successor'] for item in context['behavior_exposures'])
        assert {node['id'] for node in context['existing_nodes']} == {case['successor']}
        assert not set(case['receipts']) & {window['episode_id'] for window in context['raw_windows']}
        assert case['store']._db.execute('SELECT coalesce(sum(access_count),0) FROM node_stats').fetchone()[0] == 0
        assert not case['store']._db.execute('SELECT 1 FROM query_feedback').fetchone()
        plan = scripted_plan(case)
        response = case['client'].post('/work/complete', json=dict(id=case['task']['id'], owner=case['task']['owner'],
            agent='fixture', output=plan))
        assert response.status_code == 200, response.text
        replacement = response.json()['results'][case['candidates']['relevant']]['superseded_by']
        history = case['client'].get('/history/' + replacement).json()['versions']
        assert case['graph']._node(case['successor'])['state'] == 'refined'
        assert {evidence['episode_id'] for version in history for evidence in version['evidence']} == {
            *case['premises'], case['feedback']['relevant']}
        assert not case['store']._db.execute('SELECT 1 FROM graph_evidence WHERE episode_id IN (?,?)',
                                             case['receipts']).fetchone()
