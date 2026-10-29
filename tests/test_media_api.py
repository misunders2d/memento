"""Cheap real HTTP/MCP/queue/provenance tests; model inference is a separate acceptance check."""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from fastmcp import Client

from memento.api import create_app
from memento.ingest import MediaIngest
from memento.jobs import Worker
from memento.mcp_server import create_mcp


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv('MEMENTO_EMBEDDINGS', '0')
    with TestClient(create_app(tmp_path)) as client:
        yield client


def media_command(client):
    original = b'original image\x00\xff\r\n' * 400
    uploaded = client.post('/blobs', content=original, headers={'Content-Type': 'image/png'})
    assert uploaded.status_code == 201
    episode = client.post('/episodes', json=dict(source='native:fixture:pi', event_id='image-request',
        conversation_id='current', kind='user', text='Please save this original image.', metadata={})).json()
    command = dict(command_id='image-save', actor='native:fixture:pi', why='Explicit media save',
        sha256=uploaded.json()['sha256'], filename='original.png', kind='image', scope=['personal'],
        source='native:fixture:pi', conversation_id='current', evidence=[dict(quote='save this original image')])
    return original, episode['id'], command


def enable_prepared_media(client, monkeypatch):
    state = client.app.state
    retrieval = SimpleNamespace(graph=state.graph, _put=lambda *args: None)
    state.media = MediaIngest(retrieval, state.artifacts)
    monkeypatch.setattr(state.media, '_prepare', lambda path, sha256, kind, filename: iter([
        dict(modality='image', text='', artifact_sha256=sha256, start_s=None,
             end_s=None, page=None, vector=[1.0])]))
    return state


def test_media_http_accepts_resolved_provenance_and_returns_exact_original(client, monkeypatch):
    original, episode, command = media_command(client)
    assert client.post('/media', json=command).status_code == 503
    state = enable_prepared_media(client, monkeypatch)
    accepted = client.post('/media', json=command)
    assert accepted.status_code == 202
    accepted = accepted.json()
    assert client.post('/remember', json=command).json() == accepted
    assert client.get(accepted['status_url']).json()['state'] == 'pending'
    worker = Worker(state.jobs, {'media': lambda job: state.media.ingest(
        **dict(job.payload, command_id='job/' + job.id), lease=job)})
    assert worker.run_once()
    job = client.get(accepted['status_url']).json()
    assert job['state'] == 'done' and job['error'] is None
    history = client.get('/history/' + job['result']['id']).json()['versions']
    assert history[0]['evidence'][0]['episode_id'] == episode
    assert history[0]['evidence'][0]['quote'] == 'save this original image'
    downloaded = client.get(job['result']['blob_url'])
    assert downloaded.content == original
    assert downloaded.headers['content-type'] == 'image/png'
    assert state.store.get(episode)['text'] == 'Please save this original image.'
    assert client.post('/media', json=dict(command, filename='changed.png')).status_code == 409


def test_mcp_tools_accept_media_reference_without_bytes(client, monkeypatch):
    _, _, command = media_command(client)
    state = enable_prepared_media(client, monkeypatch)

    async def scenario():
        async with Client(create_mcp(state)) as tools:
            assert {tool.name for tool in await tools.list_tools()} == {
                'recall', 'remember', 'link', 'supersede', 'reconcile', 'history', 'loop', 'work'}
            accepted = (await tools.call_tool('remember', {'command': command})).data
            assert accepted['status'] == 'accepted'
            assert client.get(accepted['status_url']).json()['state'] == 'pending'

    asyncio.run(scenario())


@pytest.mark.parametrize('error_name,category', [
    ('ValueError', 'ValueError'),
    ('private provider text: synthetic-secret', 'unknown_error'),
])
def test_status_is_read_only_source_scoped_and_distinguishes_disabled_indexing(
        client, monkeypatch, error_name, category):
    _, _, command = media_command(client)
    state = enable_prepared_media(client, monkeypatch)
    accepted = client.post('/media', json=command).json()
    error_type = type(error_name, (ValueError,), {})
    state.jobs.fail(state.jobs.claim(['media']), error_type('private bytes must not appear'))
    before = state.store._db.total_changes
    status = client.get('/status', params={'source': 'native:fixture:pi'}).json()
    assert status['captured'] == dict(episodes=1, conversations=1)
    assert status['indexing'] == dict(available=False, reason='embedding_runtime_disabled')
    assert status['media']['failed_jobs'] == 1 and status['media']['pending_jobs'] == 0
    assert status['media']['errors'] == [dict(state='failed', category=category, count=1)]
    assert 'private' not in str(status)
    assert status['extraction']['errors'] == status['consolidation']['errors'] == []
    assert status['extraction']['pending_conversations'] == 1
    other = client.get('/status', params={'source': 'other'}).json()
    assert other['media']['failed_jobs'] == 0 and other['media']['errors'] == []
    assert state.store._db.total_changes == before
    assert client.get(accepted['status_url']).json()['error'] == error_name


def test_http_automatic_recall_forwards_bounded_context_without_changing_raw_search(client):
    calls = []
    client.app.state.retrieval = SimpleNamespace(recall=lambda *args, **kwargs: calls.append((args, kwargs)) or [])
    assert client.get('/recall', params=dict(q='how should I respond', automatic='true',
                                           context='current request')).status_code == 200
    assert calls == [(('how should I respond', 'normal', 10),
                      dict(scope=None, automatic=True, context='current request'))]
    assert client.get('/recall', params=dict(q='raw', source='native:fixture:pi', automatic='true')).status_code == 200
    assert len(calls) == 1
    assert client.get('/recall', params=dict(q='query', context='x' * 4097)).status_code == 422


def test_status_reports_candidates_waiting_for_night_even_without_consolidation_jobs(client, monkeypatch):
    clock = [datetime.now(timezone.utc).timestamp()]
    monkeypatch.setattr('memento.jobs.time.time', lambda: clock[0])
    client.post('/episodes', json=dict(source='native:fixture:pi', event_id='durable-fact',
        conversation_id='current', kind='user', text='The project uses blue folders.', metadata={}))
    processing = client.app.state.processing
    work = processing.claim(agent='fixture', timezone_name='UTC',
        compaction=dict(source='native:fixture:pi', conversation_id='current'),
        now=datetime.now(timezone.utc).replace(hour=12))['work'][0]
    assert client.post('/work/release', json=dict(
        id=work['id'], owner=work['owner'], error='invalid_output')).json() == dict(released=True)
    status = client.get('/status', params={'source': 'native:fixture:pi'}).json()
    assert status['extraction']['retrying_jobs'] == 1
    assert status['extraction']['errors'] == [dict(state='pending', category='invalid_output', count=1)]
    assert client.get('/status', params={'source': 'other'}).json()['extraction']['errors'] == []
    clock[0] += 31
    work = processing.claim(agent='fixture', timezone_name='UTC',
        now=datetime.now(timezone.utc).replace(hour=12))['work'][0]
    assert client.get('/status').json()['extraction']['errors'] == []  # The retry is now leased.
    candidate = dict(key='fact', kind='memory', type='knowledge', text='The project uses blue folders.',
        quote='The project uses blue folders.', why='Explicit durable user statement', occurrence=None,
        src_key=None, dst_key=None, valid_from=None, valid_to=None, scope=['professional'],
        open_loop=False, confidence=.9)
    processing.complete(id=work['id'], owner=work['owner'], agent='fixture',
        output=dict(windows=[dict(id=work['input']['windows'][0]['id'], candidates=[candidate])]))
    status = client.get('/status', params={'source': 'native:fixture:pi'}).json()
    assert status['extraction']['completed_jobs'] == 1
    assert status['extraction']['errors'] == [] and status['extraction']['retrying_jobs'] == 0
    assert status['consolidation']['pending_jobs'] == 0
    assert status['consolidation']['pending_candidates'] == 1
    assert status['consolidation']['ready_candidates'] == 1
    assert client.get('/status', params={'source': 'other'}).json()['consolidation']['pending_candidates'] == 0


def test_status_exposes_global_verified_storage_progress_without_claiming_cache_bytes(client, tmp_path):
    from memento.cold_manager import ColdManager
    from test_cold import DirectoryColdFixture

    payloads = (b'local-only original', b'verified remote original')
    infos = [client.post('/blobs', content=content).json() for content in payloads]
    before = client.get('/status', params={'source': 'unrelated-origin'}).json()['artifact_storage']
    assert before == dict(scope='global', registered=dict(objects=2, bytes=sum(map(len, payloads))),
                          local_only=dict(objects=2, bytes=sum(map(len, payloads))),
                          verified_remote=dict(objects=0, bytes=0))
    state = client.app.state
    manager = ColdManager(state.artifacts, DirectoryColdFixture(tmp_path / 'remote'), tmp_path)
    state.cold = manager
    manager.promote(infos[1]['sha256'])
    changes = state.store._db.total_changes
    progress = client.get('/status', params={'source': 'unrelated-origin'}).json()['artifact_storage']
    assert progress == dict(scope='global', registered=dict(objects=2, bytes=sum(map(len, payloads))),
                            local_only=dict(objects=1, bytes=len(payloads[0])),
                            verified_remote=dict(objects=1, bytes=len(payloads[1])))
    assert state.store._db.total_changes == changes
    assert client.get('/blobs/' + infos[1]['sha256']).content == payloads[1]
    assert client.get('/status').json()['artifact_storage'] == progress
