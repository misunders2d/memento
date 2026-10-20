"""Real tool-boundary behavior; no model, network service, or mocked graph."""
import asyncio
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from fastmcp import Client
from fastmcp.exceptions import ToolError

from memento.api import create_app
from memento.graph import Graph
from memento.mcp_server import create_mcp
from memento.store import Store


@pytest.fixture
def tool_state(tmp_path):
    store = Store(tmp_path)
    episode, _ = store.capture(dict(source='test', event_id='one', conversation_id='c',
        kind='user', text='Original technical fact. Refined technical fact with detail.',
        occurred_at=None, metadata={}))
    graph = Graph(store)
    memory = graph.remember(command_id='seed', actor='user', why='Exact source',
        type='knowledge', text='Original technical fact.', scope=['professional'],
        evidence=[dict(episode_id=episode['id'], quote='Original technical fact.')])
    graph.loop(action='open', id=memory['id'])
    yield SimpleNamespace(store=store, graph=graph, retrieval=None), episode['id'], memory['id']
    store.close()


@pytest.mark.parametrize('placeholder', ['personal', 'all'])
def test_recorded_omp_optional_dates_list_real_markers(tool_state, placeholder):
    state, _, memory = tool_state

    async def scenario():
        async with Client(create_mcp(state)) as client:
            # The recorded OMP tool_execution_start arguments, not a repaired prompt.
            result = (await client.call_tool('loop', dict(action='list', id=placeholder,
                days=7, due_at='', due_before='', scope='professional',
                why='Read-only listing requested before answering from recalled evidence.'))).data
            assert [row['id'] for row in result['loops']] == [memory]
            assert result['total'] == 1
            assert result['loops'][0]['state'] == 'open'
            assert result['loops'][0]['due_at'] is None
            assert len(state.graph.history(memory)) == 1

    asyncio.run(scenario())


def test_mcp_empty_activity_filters_mean_omitted_not_invalid_date(tool_state):
    state, episode, _ = tool_state

    async def scenario():
        async with Client(create_mcp(state)) as client:
            result = (await client.call_tool('recall', dict(query='', since='', source='', scope=''))).data
            assert [row['id'] for row in result['memories']] == [episode]
            assert result['memories'][0]['type'] == 'raw_episode'
            assert result['memories'][0]['confirmed'] is False

    asyncio.run(scenario())


def test_mcp_empty_effort_uses_default_and_invalid_nonempty_effort_rejects(tool_state):
    state, episode, _ = tool_state

    async def scenario():
        async with Client(create_mcp(state)) as client:
            omitted = (await client.call_tool('recall', {'query': ''})).data
            assert [row['id'] for row in omitted['memories']] == [episode]
            for effort in ('', None):
                result = (await client.call_tool('recall', dict(query='', effort=effort))).data
                assert result == omitted
            with pytest.raises(ToolError, match='Invalid effort'):
                await client.call_tool('recall', dict(query='', effort='unsupported'))

    asyncio.run(scenario())


def test_mcp_empty_optional_command_dates_and_episode_preserve_exact_evidence(tool_state):
    state, episode, _ = tool_state

    async def scenario():
        async with Client(create_mcp(state)) as client:
            command = dict(command_id='save', actor='user', why='Explicit quoted save',
                type='knowledge', text='Original technical fact.', scope=['professional'],
                source='test', conversation_id='c', valid_from='', valid_to='',
                evidence=[dict(episode_id='', quote='Original technical fact.')])
            saved = (await client.call_tool('remember', {'command': command})).data
            original = state.graph.history(saved['id'])[0]
            assert original['valid_from'] is None and original['valid_to'] is None
            assert original['evidence'][0]['episode_id'] == episode
            assert original['evidence'][0]['quote'] == 'Original technical fact.'
            refined = (await client.call_tool('supersede', {'command': dict(
                command_id='refine', actor='user', why='Supported extra detail',
                old=saved['id'], new=dict(type='knowledge',
                    text='Refined technical fact with detail.', scope=['professional']),
                kind='refinement', valid_at='', source='test', conversation_id='c',
                evidence=[dict(episode_id='', quote='Refined technical fact with detail.')])})).data
            history = state.graph.history(refined['superseded_by'])
            assert history == state.graph.history(saved['id'])
            assert [row['state'] for row in history] == ['active', 'refined', 'active']
            assert history[-1]['evidence'][0]['quote'] == 'Refined technical fact with detail.'

    asyncio.run(scenario())


@pytest.mark.parametrize('invalid', ['not-a-date', '2099-01-01T09:00:00'])
def test_mcp_still_rejects_nonempty_invalid_or_naive_dates(tool_state, invalid):
    state, _, memory = tool_state

    async def scenario():
        async with Client(create_mcp(state)) as client:
            with pytest.raises(ToolError):
                await client.call_tool('loop', dict(action='remind', id=memory, due_at=invalid))
            marker = state.graph.history(memory)[0]['loop']
            assert marker['due_at'] is None and marker['state'] == 'open'

    asyncio.run(scenario())


@pytest.mark.parametrize('required', ['text', 'why', 'quote'])
def test_mcp_empty_required_assertion_fields_still_reject_without_mutation(tool_state, required):
    state, episode, memory = tool_state
    command = dict(command_id='bad-save', actor='user', why='Explicit save', type='knowledge',
        text='Original technical fact.', scope=['professional'],
        evidence=[dict(episode_id=episode, quote='Original technical fact.')])
    if required == 'quote':
        command['evidence'][0]['quote'] = ''
    else:
        command[required] = ''

    async def scenario():
        async with Client(create_mcp(state)) as client:
            with pytest.raises(ToolError):
                await client.call_tool('remember', {'command': command})
            assert state.graph.history(memory)[0]['state'] == 'active'
            assert state.store._db.execute('SELECT count(*) FROM node_versions').fetchone()[0] == 1

    asyncio.run(scenario())


def test_empty_world_change_time_does_not_invent_a_timestamp(tool_state):
    state, episode, memory = tool_state

    async def scenario():
        async with Client(create_mcp(state)) as client:
            with pytest.raises(ToolError):
                await client.call_tool('supersede', {'command': dict(command_id='change', actor='user',
                    why='Changed circumstances', old=memory,
                    new=dict(type='knowledge', text='Refined technical fact with detail.', scope=['professional']),
                    kind='change', valid_at='', evidence=[dict(episode_id=episode,
                        quote='Refined technical fact with detail.')])})
            assert len(state.graph.history(memory)) == 1
            assert state.graph.history(memory)[0]['valid_to'] is None

    asyncio.run(scenario())


def test_mcp_redirect_preserves_private_https_behind_tls_proxy(tmp_path, monkeypatch):
    from urllib.parse import urljoin

    monkeypatch.setenv('MEMENTO_EMBEDDINGS', '0')
    with TestClient(create_app(tmp_path), base_url='http://backend', follow_redirects=False) as client:
        response = client.post('/mcp?transport=streamable', headers={
            'Host': 'memory.example', 'X-Forwarded-Proto': 'https'})
    assert response.status_code == 307
    assert urljoin('https://memory.example/mcp', response.headers['location']) == (
        'https://memory.example/mcp/?transport=streamable')


def test_http_does_not_accept_native_empty_date_sentinels(tmp_path, monkeypatch):
    monkeypatch.setenv('MEMENTO_EMBEDDINGS', '0')
    with TestClient(create_app(tmp_path)) as client:
        episode = client.post('/episodes', json=dict(source='test', event_id='one',
            conversation_id='c', kind='user', text='Exact HTTP evidence.')).json()
        result = client.post('/remember', json=dict(command_id='strict-http', actor='user',
            why='Explicit save', type='knowledge', text='Exact HTTP evidence.', scope=['professional'],
            valid_from='', evidence=[dict(episode_id=episode['id'], quote='Exact HTTP evidence.')]))
        assert result.status_code == 422


def test_client_bundle_stamp_matches_recall_bridge_and_old_updater_contract(tmp_path, monkeypatch):
    from memento import api, runner
    monkeypatch.setenv('MEMENTO_EMBEDDINGS', '0')
    app = create_app(tmp_path)
    with TestClient(app) as client:
        response = client.get('/client-release')
        revision = response.json()['revision']
        # The existing updater accepts the additive metadata and validates the
        # actual served bytes, carrying the stamp through an old updater too.
        files = runner.release_files(response.content)
        served = client.get('/bridge')
        assert served.content == files['bridge.py']
        namespace = {'__name__': 'isolated_stamped_bridge'}
        exec(compile(files['bridge.py'], 'isolated_stamped_bridge.py', 'exec'), namespace)
        assert namespace['CLIENT_REVISION'] == revision
        assert namespace['CLIENT_REVISION'] is not None
        assert client.get('/recall').json() == {'memories': [], 'client_revision': revision}
        assert served.headers['cache-control'] == 'no-store'
        # Requests reuse the built bundle, without another read/hash pass.
        monkeypatch.setattr(api, 'client_release_assets', lambda: pytest.fail('bundle rebuilt during request'))
        assert client.get('/client-release').content == response.content
        assert client.get('/bridge').content == served.content
        assert client.get('/recall').json()['client_revision'] == revision


def test_client_bundle_identity_tracks_each_original_asset_only(tmp_path, monkeypatch):
    from pathlib import Path
    from memento import api, runner
    source = Path(api.__file__).parent
    for name in runner.RELEASE_FILES:
        (tmp_path / name).write_bytes((source / name).read_bytes())
    monkeypatch.setattr(api, '__file__', str(tmp_path / 'api.py'))
    revision, bridge_bytes, release = api.client_release_assets()
    assert api.client_release_assets() == (revision, bridge_bytes, release)
    (tmp_path / 'api.py').write_text('Server-only changes are not client assets.')
    assert api.client_release_assets()[0] == revision
    for name in runner.RELEASE_FILES:
        path = tmp_path / name
        original = path.read_bytes()
        path.write_bytes(original + b'\n')
        assert api.client_release_assets()[0] != revision
        path.write_bytes(original)
    assert api.client_release_assets()[0] == revision


def test_official_omp_validator_accepts_recorded_omissions_and_rejects_invalid_data(tmp_path):
    import json
    import os
    from pathlib import Path
    import shutil
    import subprocess

    executable = shutil.which('omp')
    if executable is None:
        pytest.skip('Native OMP validator regression requires the actual OMP CLI')
    source = Path(__file__).resolve().parents[1] / 'src/memento/native_omp.ts'
    observer = tmp_path / 'validator.ts'
    output = tmp_path / 'validation.json'
    observer.write_text('import actual from ' + json.dumps(source.as_uri()) + ';\n' + '''
import { writeFileSync } from "node:fs";
export default async function(api) {
  const definitions = new Map();
  const wrapped = new Proxy(api, { get(target, key) {
    if (key === "on") return () => {};
    if (key === "registerTool") return tool => { definitions.set(tool.name, tool); };
    const value = Reflect.get(target, key);
    return typeof value === "function" ? value.bind(target) : value;
  }});
  actual(wrapped);
  const loop = {action:"list", id:"personal", days:7, due_at:"", due_before:"", scope:"personal"};
  const command = {why:"Explicit synthetic save", type:"knowledge", text:"Exact source fact",
    scope:["professional"], evidence:[{quote:"Exact source fact"}]};
  const cases = [
    ["recorded_personal", "memento_loop", loop],
    ["recorded_all", "memento_loop", {...loop, id:"all"}],
    ["optional_filters", "memento_recall", {query:"", effort:"", limit:3, scope:"", since:"", source:""}],
    ["optional_save_dates", "memento_remember", {command:{...command, valid_from:"", valid_to:"",
      evidence:[{episode_id:"", quote:"Exact source fact"}]}}],
    ["media_reference", "memento_remember", {command:{why:command.why, evidence:command.evidence,
      sha256:"a".repeat(64), kind:"image", filename:"original.webp", scope:["personal"]}}],
    ["media_binary_rejected", "memento_remember", {command:{why:command.why, evidence:command.evidence,
      sha256:"a".repeat(64), kind:"image", filename:"original.webp", scope:["personal"], data:"opaque-base64"}}],
    ["media_without_evidence", "memento_remember", {command:{why:command.why,
      sha256:"a".repeat(64), kind:"image", filename:"original.webp", scope:["personal"]}}],
    ["optional_change_date", "memento_supersede", {command:{why:command.why, old:"memory",
      new:{type:"knowledge", text:command.text, scope:command.scope}, evidence:command.evidence,
      kind:"refinement", valid_at:""}}],
    ["valid_date", "memento_loop", {...loop, due_at:"2099-01-01T09:00:00+00:00"}],
    ["invalid_date", "memento_loop", {...loop, due_at:"not-a-date"}],
    ["invalid_scope", "memento_loop", {...loop, scope:"unscoped"}],
    ["required_text", "memento_remember", {command:{...command, text:""}}],
    ["required_quote", "memento_remember", {command:{...command, evidence:[{quote:""}]}}],
  ];
  const results = {};
  for (const [name, tool, input] of cases) {
    results[name] = (await definitions.get(tool).parameters.safeParse(input)).success;
  }
  writeFileSync(process.env.MEMENTO_VALIDATION_OUTPUT, JSON.stringify(results));
}
''')
    home = tmp_path / 'home'
    home.mkdir()
    # RPC startup needs a selected provider, but this test never sends a prompt.
    # A dummy key and closed loopback endpoint avoid real auth or generation.
    environment = {'PATH': os.environ['PATH'], 'LANG': 'C.UTF-8',
        'HOME': str(home), 'XDG_CONFIG_HOME': str(home / '.config'),
        'XDG_DATA_HOME': str(home / '.local/share'), 'XDG_CACHE_HOME': str(home / '.cache'),
        'PI_CODING_AGENT_DIR': str(home / 'agent'), 'MEMENTO_VALIDATION_OUTPUT': str(output),
        'OPENAI_API_KEY': 'synthetic-never-used', 'OPENAI_BASE_URL': 'http://127.0.0.1:1/v1'}
    result = subprocess.run([executable, '--mode', 'rpc', '--no-tools', '--no-lsp', '--no-rules',
        '--no-title', '--no-extensions', '--no-skills', '--no-session', '--extension', str(observer)],
        input='{"id":"schema-regression","type":"get_state"}\n', text=True,
        capture_output=True, cwd=tmp_path, env=environment, timeout=45)
    assert result.returncode == 0, result.stderr
    assert json.loads(output.read_text()) == {
        'recorded_personal': True, 'recorded_all': True, 'optional_filters': True,
        'media_reference': True, 'media_binary_rejected': False, 'media_without_evidence': False,
        'optional_save_dates': True, 'optional_change_date': True, 'valid_date': True,
        'invalid_date': False, 'invalid_scope': False, 'required_text': False, 'required_quote': False}
