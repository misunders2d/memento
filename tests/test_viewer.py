import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from memento.api import create_app
from memento.graph import Graph
from memento.processing import Processing
from memento.store import Store
from test_behavioral_learning import BEHAVIOR, action, complete, extract

XSS = '<img src=x onerror=alert(1)> Ирина prefers tea'
LOCAL = {'host': 'localhost:8765'}
UI = Path(__file__).parents[1] / 'src' / 'memento' / 'ui'


def seed(tmp_path):
    store = Store(tmp_path)
    graph = Graph(store)
    processing = Processing(graph)
    incident = ('I had to edit three settings by hand for Project Cedar and it failed twice. '
                'Using the shipped installer removed those errors and saved the repeated setup work.')
    lesson_episode, task = extract(processing, incident)
    lesson = complete(processing, task, action(task, lesson_episode))['id']
    episode, _ = store.capture(dict(source='seed', event_id='1', conversation_id='seed', kind='user',
        text=f'{XSS}. Atlas launch. Cedar budget. Forgotten thing.', occurred_at=None, metadata={}))

    def remember(key, text, quote, scope):
        return graph.remember(command_id=key, actor='user', why='explicit', type='knowledge', text=text,
            scope=scope, evidence=[dict(episode_id=episode['id'], quote=quote)])['id']

    ids = dict(tea=remember('tea', XSS, XSS, ['personal']),
               atlas=remember('atlas', 'Atlas launch is planned', 'Atlas launch', ['professional']),
               cedar=remember('cedar', 'Cedar budget review', 'Cedar budget', ['personal']),
               gone=remember('gone', 'Forgotten thing', 'Forgotten thing', ['personal']))
    ids['edge'] = graph.link(command_id='link', actor='user', why='related', a=ids['tea'], rel='related_to',
        b=ids['atlas'], evidence=[dict(episode_id=episode['id'], quote='Atlas launch')])['id']
    graph.loop(action='open', id=ids['cedar'], command_id='open-cedar')
    graph.loop(action='remind', id=ids['cedar'], due_at='2000-01-01T00:00:00+00:00', command_id='due-cedar')
    graph.loop(action='open', id=ids['atlas'], command_id='open-atlas')
    graph.loop(action='later', id=ids['atlas'], days=3, command_id='defer-atlas')
    ids['lesson'] = lesson
    pending, _ = store.capture(dict(source='native:abc:claude', event_id='p1', conversation_id='pending-chat',
        kind='user', text='Remind me to renew the Zephyr passport', occurred_at=None, metadata={}))
    task = processing.claim(agent='fixture', timezone_name='UTC', now=datetime.now(timezone.utc),
        compaction=dict(source='native:abc:claude', conversation_id='pending-chat'))['work'][0]
    processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(windows=[dict(
        id=task['input']['windows'][0]['id'], candidates=[dict(key='zephyr', kind='memory', type='knowledge',
        text='Renew the Zephyr passport', quote='renew the Zephyr passport', why='Requested reminder',
        occurrence=None, src_key=None, dst_key=None, valid_from=None, valid_to=None, scope=['personal'],
        open_loop=True, confidence=.9)])]))
    store.capture(dict(source='native:abc:claude', event_id='p2', conversation_id='pending-chat',
        kind='user', text='Also the Zephyr visa', occurred_at=None, metadata={}))
    expected = {scope: graph.loop(action='list', scope=scope, limit=100)['total']
                for scope in (None, 'personal', 'professional')}
    store._db.execute("INSERT INTO forgotten_targets(target,commit_id,kind) VALUES (?,'test','node')", (ids['gone'],))
    store.close()
    return ids, expected


@pytest.fixture
def seeded(tmp_path, monkeypatch):
    monkeypatch.setenv('MEMENTO_ALLOWED_HOSTS', 'zen.example.ts.net')
    ids, expected = seed(tmp_path)
    with TestClient(create_app(tmp_path), headers=LOCAL) as client:
        yield client, ids, expected, tmp_path


def graph_view(client, **params):
    response = client.get('/ui/api/graph', params=params)
    assert response.status_code == 200, response.text
    return response.json()


def test_graph_views_scope_and_loop_counts_match_loop_list(seeded):
    client, ids, expected, _ = seeded
    data = graph_view(client)
    nodes = {node['id']: node for node in data['nodes']}
    assert set(nodes) == {ids['tea'], ids['atlas'], ids['cedar'], ids['lesson']}
    assert nodes[ids['tea']]['label'] == XSS
    assert nodes[ids['lesson']]['behavior'] == BEHAVIOR['authority']
    assert nodes[ids['cedar']]['loop']['overdue'] and not nodes[ids['cedar']]['loop']['deferred']
    assert nodes[ids['atlas']]['loop']['deferred']
    assert [edge['id'] for edge in data['edges']] == [ids['edge']]
    for scope in (None, 'personal', 'professional'):
        loops = graph_view(client, scope=scope or '')['loops'] if scope else data['loops']
        assert loops['open'] == expected[scope]
    assert data['loops']['deferred'] == 1
    assert [item['label'] for item in data['loops']['candidates']['items']] == ['Renew the Zephyr passport']
    personal = graph_view(client, scope='personal')
    assert {node['id'] for node in personal['nodes']} == {ids['tea'], ids['cedar']}
    assert personal['edges'] == []
    behaviors = graph_view(client, view='behaviors')
    assert [node['id'] for node in behaviors['nodes'] if node['focus']] == [ids['lesson']]
    loops = graph_view(client, view='loops')
    assert {node['id'] for node in loops['nodes'] if node['focus']} == {ids['cedar'], ids['atlas']}
    around = graph_view(client, view='around', id=ids['tea'])
    assert {node['id'] for node in around['nodes']} == {ids['tea'], ids['atlas']}
    assert client.get('/ui/api/graph', params=dict(view='around', id=ids['gone'])).status_code == 404
    assert len(graph_view(client, all='true')['nodes']) == 4


def test_details_search_and_unprocessed_lists(seeded):
    client, ids, _, _ = seeded
    lesson = client.get(f'/ui/api/node/{ids["lesson"]}').json()
    assert lesson['behavior'] == BEHAVIOR
    tea = client.get(f'/ui/api/node/{ids["tea"]}').json()
    assert tea['text'] == XSS and tea['links'][0] | {'id': None} == dict(rel='related_to', outgoing=True,
        id=None, type='knowledge', label='Atlas launch is planned')
    assert client.get(f'/ui/api/node/{ids["gone"]}').status_code == 404
    found = client.get('/ui/api/search', params=dict(q='Atlas')).json()['results']
    assert [item['id'] for item in found] == [ids['atlas']]
    assert client.get('/ui/api/search', params=dict(q='Forgotten')).json()['results'] == []
    assert client.get('/ui/api/search', params=dict(q='Atlas', scope='personal')).json()['results'] == []
    assert client.get('/ui/api/search', params=dict(q='" OR NEAR(')).status_code == 200
    conversations = client.get('/ui/api/conversations').json()
    pending = next(item for item in conversations['items'] if item['conversation_id'] == 'pending-chat')
    episodes = client.get('/ui/api/conversation', params=dict(source=pending['source'],
        conversation_id='pending-chat', after=pending['cursor'])).json()['items']
    assert [item['text'] for item in episodes] == ['Also the Zephyr visa']
    candidates = client.get('/ui/api/candidates').json()
    zephyr = next(item for item in candidates['items'] if item['text'] == 'Renew the Zephyr passport')
    assert zephyr['loop'] and zephyr['scope'] == ['personal']
    assert client.get('/ui/api/candidates', params=dict(q='Zephyr')).json()['total'] == 1


def test_complete_loop_view_includes_full_candidate_cohort(seeded, monkeypatch):
    client, _, _, _ = seeded
    db = client.app.state.store._db
    candidate = db.execute('SELECT * FROM candidates LIMIT 1').fetchone()
    commit = db.execute('SELECT commit_id FROM extraction_runs WHERE id=?', (candidate['run_id'],)).fetchone()[0]
    for index in range(103):
        id = f'loop-candidate-{index}'
        scope = '["professional"]' if index == 101 else '["personal"]'
        db.execute('''INSERT INTO candidates SELECT ?,run_id,kind,type,text,why,src,dst,confidence,
            valid_from,valid_to,? FROM candidates WHERE id=?''', (id, scope, candidate['id']))
        db.execute('INSERT INTO loop_events VALUES (?,?,?,\'open\',NULL,NULL)', (f'loop-{index}', id, commit))
    db.execute("INSERT INTO forgotten_targets VALUES('loop-candidate-102',?,'candidate')", (commit,))
    bounded = graph_view(client, view='loops')['loops']['candidates']
    assert bounded['total'] == 103 and len(bounded['items']) == 100
    complete = graph_view(client, view='loops', all='true')
    assert not complete['too_large']
    assert len(complete['loops']['candidates']['items']) == complete['loops']['candidates']['total'] == 103
    personal = graph_view(client, view='loops', all='true', scope='personal')['loops']['candidates']
    assert len(personal['items']) == personal['total'] == 102
    assert not {'loop-candidate-101', 'loop-candidate-102'} & {item['id'] for item in personal['items']}
    assert len(graph_view(client, all='true')['loops']['candidates']['items']) == 100
    monkeypatch.setattr('memento.viewer.MAX_NODES', 10)
    overflow = graph_view(client, view='loops', all='true')
    assert overflow['too_large'] and overflow['nodes'] == [] and overflow['edges'] == []
    assert overflow['loops']['candidates'] == dict(total=103, items=[])


def test_viewing_writes_nothing(seeded):
    client, ids, _, data_dir = seeded

    def fingerprint():
        db = sqlite3.connect(f'file:{data_dir / "memory.sqlite"}?mode=ro', uri=True)
        try:
            return [db.execute(f'SELECT count(*),{column} FROM {table}').fetchone() for table, column in (
                ('graph_commits', 'max(rowid)'), ('node_stats', 'sum(access_count)'),
                ('query_feedback', 'sum(shown+cited)'), ('loop_events', 'max(rowid)'),
                ('candidate_decisions', 'max(rowid)'), ('node_versions', 'max(rowid)'))]
        finally:
            db.close()

    before = fingerprint()
    for view in ('memories', 'behaviors', 'loops'):
        graph_view(client, view=view, all='true')
    client.get(f'/ui/api/node/{ids["tea"]}')
    client.get('/ui/api/search', params=dict(q='Atlas'))
    client.get('/ui/api/conversations')
    client.get('/ui/api/candidates', params=dict(q='Zephyr'))
    assert fingerprint() == before


def test_host_check_headers_and_static_allowlist(seeded):
    client, _, _, _ = seeded
    page = client.get('/ui/')
    assert page.status_code == 200 and "default-src 'self'" in page.headers['content-security-policy']
    assert page.headers['x-content-type-options'] == 'nosniff'
    assert client.get('/ui', follow_redirects=False).headers['location'] == '/ui/'
    for name in ('app.js', 'app.css', 'layout-worker.js', 'vendor/sigma.min.js'):
        response = client.get(f'/ui/static/{name}')
        assert response.status_code == 200 and 'content-security-policy' in response.headers
    assert client.get('/ui/static/../viewer.py').status_code == 404
    assert client.get('/ui/static/index.html').status_code == 404
    api = client.get('/ui/api/graph', headers={'accept-encoding': 'gzip'})
    assert api.headers['cache-control'] == 'no-store' and api.json()['nodes']
    for host, status in (('evil.example', 403), ('127.0.0.1:9', 200), ('[::1]:8765', 200),
                         ('zen.example.ts.net', 200), ('zen.example.ts.net.evil.example', 403)):
        assert client.get('/ui/api/graph', headers={'host': host}).status_code == status, host
    assert client.get('/ui/', headers={'host': 'evil.example'}).status_code == 403


def test_page_never_renders_memory_text_as_html():
    script = (UI / 'app.js').read_text()
    for sink in ('innerHTML', 'outerHTML', 'insertAdjacentHTML', 'document.write', 'eval(', 'new Function'):
        assert sink not in script
    assert 'http' not in (UI / 'index.html').read_text().replace('http-equiv', '')
