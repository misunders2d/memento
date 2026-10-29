"""Explicit duplicate reconciliation through real graph, HTTP and MCP boundaries."""
import asyncio
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from fastmcp import Client

from memento.api import create_app
from memento.graph import Graph
from memento.mcp_server import create_mcp
from memento.store import ConflictError, Store


@pytest.fixture
def world(tmp_path):
    store = Store(tmp_path)
    graph = Graph(store)
    episode, _ = store.capture(dict(source='test', event_id='one', conversation_id='thread', kind='user',
        text='Vega Incorporated is Vega. Vega is the same organization. Orion uses Vega. Unrelated text.',
        occurred_at=None, metadata={}))
    evidence = [dict(episode_id=episode['id'], quote=quote)
                for quote in ('Vega Incorporated is Vega.', 'Vega is the same organization.')]

    def seed(key, scope=('professional',)):
        return graph.remember(command_id=key, actor='fixture', why='Original identity evidence',
            type='organization', text=key, scope=list(scope), evidence=[evidence[0 if key == 'Vega' else 1]])['id']

    survivor, duplicate = seed('Vega'), seed('Vega Incorporated')
    command = dict(command_id='reconcile', actor='agent', why='Owner requested proven same-organization merge',
        survivor=survivor, absorbed=[duplicate], expected_versions={survivor: 1, duplicate: 1}, evidence=evidence)
    yield store, graph, seed, command, episode
    store.close()


def test_reconcile_preserves_relations_loops_history_and_identical_retry(world):
    store, graph, seed, command, episode = world
    other = seed('Orion')
    edge = graph.link(command_id='edge', actor='fixture', why='Original relation', a=other,
        rel='uses', b=command['absorbed'][0], evidence=[dict(episode_id=episode['id'], quote='Orion uses Vega.')])
    graph.loop(action='open', id=command['absorbed'][0])
    before = graph.history(command['absorbed'][0])
    result = graph.reconcile(**command)
    assert result['status'] == 'absorbed'
    assert graph.reconcile(**command) == result
    survivor = graph.history(command['survivor'])[-1]
    absorbed = graph.history(command['absorbed'][0])
    immutable = lambda rows: [{k: v for k, v in row.items() if k not in ('recorded_until', 'loop', 'loop_events')}
                              for row in rows]
    assert immutable(absorbed[:len(before)]) == immutable(before)
    assert absorbed[0]['loop_events'][:len(before[0]['loop_events'])] == before[0]['loop_events']
    assert absorbed[-1]['state'] == 'merged' and absorbed[-1]['superseded_by'] == command['survivor']
    assert survivor['version'] > 1
    assert graph.history(edge['id'])[-1]['dst'] == command['survivor']
    assert graph.loop(action='list')['loops'][0]['id'] == command['survivor']
    assert len(store.search('Vega Incorporated is Vega.')) == 1
    with pytest.raises(ConflictError, match='different command'):
        graph.reconcile(**dict(command, why='Changed reason'))


@pytest.mark.parametrize('failure', ['stale', 'missing_member', 'wrong_quote', 'duplicate', 'extra_version'])
def test_reconcile_rejects_atomically(world, failure):
    store, graph, _, command, episode = world
    survivor, duplicate = command['survivor'], command['absorbed'][0]
    if failure == 'stale':
        command['expected_versions'][survivor] = 2
    elif failure == 'missing_member':
        command['evidence'] = command['evidence'][:1]
    elif failure == 'wrong_quote':
        command['evidence'][0]['quote'] = 'Invented quote'
    elif failure == 'duplicate':
        command['absorbed'] = [duplicate, duplicate]
    else:
        command['expected_versions']['unknown'] = 1
    with pytest.raises((ValueError, ConflictError)):
        graph.reconcile(**command)
    assert graph.history(survivor)[-1]['version'] == graph.history(duplicate)[-1]['version'] == 1
    assert not graph.pending_reviews()
    assert not store._db.execute("SELECT 1 FROM graph_commits WHERE command_id='reconcile'").fetchone()


def test_differently_scoped_members_propose_review_without_approval(world):
    _, graph, seed, command, _ = world
    personal = seed('Vega personal', ('personal',))
    command.update(absorbed=[personal], expected_versions={command['survivor']: 1, personal: 1})
    result = graph.reconcile(**command)
    assert result['status'] == 'pending_review'
    assert graph.reconcile(**command) == result
    assert len(graph.pending_reviews()) == 1
    assert graph.history(personal)[-1]['state'] == 'active'


def test_real_mcp_reconcile_tool(world):
    store, graph, _, command, _ = world

    async def scenario():
        async with Client(create_mcp(SimpleNamespace(store=store, graph=graph, retrieval=None))) as client:
            result = (await client.call_tool('reconcile', {'command': command})).data
            assert result['status'] == 'absorbed'
            assert (await client.call_tool('reconcile', {'command': command})).data == result
    asyncio.run(scenario())


def test_http_reconcile_contract_and_quote_only_provenance(tmp_path, monkeypatch):
    monkeypatch.setenv('MEMENTO_EMBEDDINGS', '0')
    with TestClient(create_app(tmp_path)) as client:
        episode = client.post('/episodes', json=dict(source='test', event_id='one', conversation_id='thread',
            kind='user', text='Vega Incorporated is Vega.', occurred_at=None, metadata={})).json()
        ids = []
        for name in ('Vega', 'Vega Incorporated'):
            response = client.post('/remember', json=dict(command_id=name, actor='fixture', why='Original evidence',
                type='organization', text=name, scope=['professional'],
                evidence=[dict(episode_id=episode['id'], quote='Vega Incorporated is Vega.')]))
            assert response.status_code == 200
            ids.append(response.json()['id'])
        command = dict(command_id='merge', actor='agent', why='Owner requested same identity',
            survivor=ids[0], absorbed=ids[1:], expected_versions=dict.fromkeys(ids, 1),
            evidence=[dict(quote='Vega Incorporated is Vega.')], source='test', conversation_id='thread')
        assert client.post('/reconcile', json=dict(command, approved=True)).status_code == 422
        result = client.post('/reconcile', json=command)
        assert result.status_code == 200 and result.json()['status'] == 'absorbed'
        assert client.post('/reconcile', json=command).json() == result.json()
