import pytest

from datetime import datetime, timezone
from memento.graph import Graph
from memento.store import ConflictError, Store
from memento.processing import Processing


def setup(path):
    store = Store(path)
    graph = Graph(store)
    payload = dict(source='stable-harness', event_id='immutable-event-1', conversation_id='c',
                   kind='user', text='Orion has a red cover.', occurred_at=None, metadata={})
    episode, _ = store.capture(payload)
    evidence = [dict(episode_id=episode['id'], quote=payload['text'])]
    node = graph.remember(command_id='memory', actor='fixture', why='explicit saved assertion',
                          type='knowledge', text=payload['text'], scope=['professional'], evidence=evidence)
    return store, graph, payload, episode, evidence, node


def forget(graph, node):
    preview = graph._forget_preview(node['id'])
    return graph._forget(command_id='forget', actor='human-fixture', why='explicit active-memory exclusion',
                          target=node['id'], token=preview['token'])


def test_forget_hides_active_memory_but_keeps_exact_raw_and_reasoned_history(tmp_path):
    store, graph, payload, episode, evidence, node = setup(tmp_path)
    try:
        result = forget(graph, node)
        assert result['status'] == 'forgotten'
        assert graph.recall_keywords('Orion') == []
        assert store.search('Orion') == []
        assert store.get(episode['id'])['text'] == payload['text']
        history = graph.history(node['id'])
        assert [item['state'] for item in history] == ['active', 'forgotten']
        assert history[-1]['text'] == payload['text']
        assert history[-1]['why'] == 'explicit active-memory exclusion'
        assert history[-1]['forgotten']
        with pytest.raises(ConflictError):
            graph.remember(command_id='resurrect', actor='agent', why='same evidence',
                type='knowledge', text=payload['text'], scope=['professional'], evidence=evidence)
    finally:
        store.close()


def test_preexisting_extraction_work_does_not_export_forgotten_source(tmp_path):
    store, graph, payload, episode, evidence, node = setup(tmp_path)
    try:
        forget(graph, node)
        processing = Processing(graph)
        result = processing.claim(agent='fixture', timezone_name='UTC',
            compaction=dict(source=payload['source'], conversation_id=payload['conversation_id']),
            now=datetime.now(timezone.utc).replace(hour=12))
        assert result['work'] == []
        assert store._db.execute('SELECT count(*) FROM candidates').fetchone()[0] == 0
        assert store.get(episode['id'])['text'] == payload['text']
    finally:
        store.close()




def test_human_forget_after_claim_fences_derivation_and_later_windows(tmp_path):
    store, graph, payload, episode, evidence, node = setup(tmp_path)
    try:
        processing = Processing(graph)
        work = processing.claim(agent='fixture', timezone_name='UTC',
            compaction=dict(source=payload['source'], conversation_id=payload['conversation_id']),
            now=datetime.now(timezone.utc).replace(hour=12))['work'][0]
        forget(graph, node)
        output = dict(windows=[dict(id=work['input']['windows'][0]['id'], candidates=[
            dict(key='claim', kind='memory', type='knowledge', text=payload['text'], quote=payload['text'],
                 why='Explicit source assertion', occurrence=None, src_key=None, dst_key=None,
                 valid_from=None, valid_to=None, scope=['professional'], open_loop=False, confidence=.8)])])
        result = processing.complete(id=work['id'], owner=work['owner'], output=output, agent='fixture')
        assert result['candidate_count'] == 0
        assert store._db.execute('SELECT count(*) FROM candidates').fetchone()[0] == 0
        assert processing.claim(agent='fixture', timezone_name='UTC',
            now=datetime.now(timezone.utc).replace(hour=12))['work'] == []
        assert graph.recall_keywords('Orion') == []
    finally:
        store.close()
