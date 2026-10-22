"""Antigravity (agy) native bridge; all paths, transcripts and configuration are synthetic."""
import json

from memento import bridge
from test_bridge import pending, spool_at

REQUEST = '<USER_REQUEST>\nMy synthetic badge number is 4417.\n</USER_REQUEST>\n<ADDITIONAL_METADATA>\nLocal time.\n</ADDITIONAL_METADATA>'


def transcript(tmp_path, steps, conversation='c0nv'):
    path = tmp_path / 'brain' / conversation / '.system_generated' / 'logs' / 'transcript_full.jsonl'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b''.join(bridge.encode(step) + b'\n' for step in steps))
    return path


def step(index, typ, source='MODEL', **fields):
    return {'step_index': index, 'source': source, 'type': typ, 'status': 'DONE',
            'created_at': '2026-10-22T10:42:49Z', **fields}


STEPS = [
    step(0, 'USER_INPUT', 'USER_EXPLICIT', content=REQUEST),
    step(1, 'SYSTEM_MESSAGE', 'SYSTEM_SDK', content='Memento recalled context [memory:x]'),
    step(2, 'PLANNER_RESPONSE', content='Noted [memory:abc].', tool_calls=[{'name': 'run_command', 'args': {'CommandLine': 'ls'}}]),
    step(3, 'GENERIC', content='file.txt'),
    step(4, 'CHECKPOINT', 'SYSTEM', content='summary'),
]


def messages(spool):
    return [part for part in pending(spool) if part['kind'] not in ('native_record', 'native_hook')]


def test_transcript_projects_request_response_and_tool_output_once(tmp_path):
    spool = spool_at(tmp_path, preset='agy')
    path = transcript(tmp_path, STEPS)
    spool.journal(path, 'agy', 'c0nv')
    projected = messages(spool)
    assert [(part['kind'], part['text']) for part in projected] == [
        ('user', 'My synthetic badge number is 4417.'),
        ('assistant', 'Noted [memory:abc].'),
        ('assistant', bridge.encode({'type': 'function_call', 'tool_calls': STEPS[2]['tool_calls'],
                                     'timestamp': STEPS[2]['created_at']}).decode()),
        ('tool', 'file.txt')]
    assert projected[0]['metadata']['extraction_mode'] == 'message'
    assert projected[0]['occurred_at'] == '2026-10-22T10:42:49Z'
    assert projected[2]['metadata']['evidence_eligible'] is False
    assert projected[3]['metadata']['extraction_mode'] == 'tool_output'
    assert all('recalled' not in part['text'] for part in projected)
    assert pending(spool, '/usage')[0]['ids'] == ['abc']
    assert len(pending(spool, '/work/compaction')) == 1
    raw = [part for part in pending(spool) if part['kind'] == 'native_record']
    assert len(raw) == len(STEPS) and raw[0]['metadata']['native_id'] == 'step:0'
    count = len(pending(spool))
    spool.reconcile(str(path.resolve()), force=True)
    assert len(pending(spool)) == count
    spool.close()


def test_hooks_inject_provenance_and_turn_recall(tmp_path, monkeypatch):
    spool = spool_at(tmp_path, preset='agy')
    queries = []

    def recalled(config, endpoint, body=None, timeout=1):
        queries.append(bridge.urllib.parse.parse_qs(bridge.urllib.parse.urlsplit(endpoint).query)['q'][0])
        return {'memories': [{'id': 'm1', 'text': 'Badge is 4417.'}]}

    monkeypatch.setattr(bridge, 'http', recalled)
    path = transcript(tmp_path, STEPS[:1])
    common = {'conversationId': 'c0nv', 'transcriptPath': str(path), 'workspacePaths': ['/w']}
    start = bridge.agy_hook(spool, 'SessionStart', common)
    text = start['injectSteps'][0]['systemMessage']['systemMessage']
    assert '"source":"native:test-device:agy"' in text and '"conversation_id":"c0nv"' in text
    first = bridge.agy_hook(spool, 'PreInvocation', {**common, 'invocationNum': 0, 'initialNumSteps': 1})
    context = first['injectSteps'][0]['systemMessage']['systemMessage']
    assert '[memory:m1]' in context and len(context.encode()) <= bridge.CONTEXT_BYTES
    assert queries == ['My synthetic badge number is 4417.']
    hooks = len([part for part in pending(spool) if part['kind'] == 'native_hook'])
    assert bridge.agy_hook(spool, 'PreInvocation', {**common, 'invocationNum': 1}) == {}
    assert queries == ['My synthetic badge number is 4417.']
    assert len([part for part in pending(spool) if part['kind'] == 'native_hook']) == hooks
    assert bridge.agy_hook(spool, 'Stop', {**common, 'terminationReason': 'NO_TOOL_CALL'}) == {}
    spool.close()


def test_memento_writes_need_provenance_and_never_auto_approve(tmp_path, monkeypatch):
    spool = spool_at(tmp_path, preset='agy')
    monkeypatch.setattr(bridge, 'http', lambda *args, **kwargs: {'id': 'captured'})
    command = {'evidence': [{'quote': 'badge'}]}
    data = {'conversationId': 'c0nv', 'toolCall': {'name': 'call_mcp_tool', 'args': {
        'ServerName': 'memento', 'ToolName': 'remember', 'Arguments': {'command': command}}}}
    assert bridge.agy_hook(spool, 'PreToolUse', data)['decision'] == 'deny'
    command.update(source='native:test-device:agy', conversation_id='c0nv')
    # agy denies an empty object; "ask" keeps its normal permission policy.
    assert bridge.agy_hook(spool, 'PreToolUse', data) == {'decision': 'ask'}
    data['toolCall']['args']['Arguments'] = json.dumps({'command': command})
    assert bridge.agy_hook(spool, 'PreToolUse', data) == {'decision': 'ask'}
    data['toolCall']['args'].update(ServerName='other', Arguments={})
    assert bridge.agy_hook(spool, 'PreToolUse', data) == {'decision': 'ask'}
    spool.close()


def test_main_always_prints_a_json_object(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv('MEMENTO_PROCESSING', '1')
    assert bridge.main(['agy', '--event', 'Stop', '--config', str(tmp_path / 'missing.json')]) == 0
    assert json.loads(capsys.readouterr().out) == {}
    monkeypatch.delenv('MEMENTO_PROCESSING')
    monkeypatch.setattr('sys.stdin', __import__('io').StringIO('not json'))
    config = tmp_path / 'runner.json'
    config.write_bytes(bridge.encode({'uid': 1000, 'client_uid': 'd', 'url': 'http://127.0.0.1:1', 'preset': 'agy'}))
    bridge.main(['agy', '--event', 'Stop', '--config', str(config)])
    assert json.loads(capsys.readouterr().out) == {}


def test_installer_owns_one_named_hook_and_mcp_entry(tmp_path):
    config_path = tmp_path / 'runner.json'
    config_path.write_bytes(bridge.encode({'uid': 'u', 'url': 'http://127.0.0.1:1/', 'preset': 'agy'}))
    root = tmp_path / 'native'
    root.mkdir()
    other = {'PreInvocation': [{'type': 'command', 'command': 'other-hook', 'timeout': 10}]}
    (root / 'hooks.json').write_bytes(bridge.encode({'other': other}))
    (root / 'mcp_config.json').write_bytes(bridge.encode({'mcpServers': {'x': {'serverUrl': 'http://x/mcp'}}}))
    result = bridge.install(config_path, root)
    assert result['status'] == 'installed'
    bridge.install(config_path, root)
    hooks = json.loads((root / 'hooks.json').read_bytes())
    assert list(hooks) == ['other', 'memento'] and hooks['other'] == other
    assert set(hooks['memento']) == {'SessionStart', 'PreInvocation', 'PreToolUse', 'Stop'}
    assert hooks['memento']['PreInvocation'][0]['command'].endswith('--event PreInvocation')
    assert hooks['memento']['PreToolUse'][0]['matcher'] == '^call_mcp_tool$'
    servers = json.loads((root / 'mcp_config.json').read_bytes())['mcpServers']
    assert servers == {'x': {'serverUrl': 'http://x/mcp'}, 'memento': {'serverUrl': 'http://127.0.0.1:1/mcp/'}}
    assert (root / 'hooks.json').stat().st_mode & 0o777 == 0o600


def test_history_import_selects_full_transcripts_and_path_conversation(tmp_path, monkeypatch):
    spool = spool_at(tmp_path, preset='agy')
    path = transcript(tmp_path, STEPS)
    (path.parent / 'transcript.jsonl').write_bytes(bridge.encode(step(0, 'USER_INPUT', content='truncated')) + b'\n')
    chunk = path.parent / 'chunks' / 'transcript_full' / '00000000.jsonl'
    chunk.parent.mkdir(parents=True)
    chunk.write_bytes(path.read_bytes())
    result = bridge.import_history(spool, 'agy', [tmp_path / 'brain'])
    assert result['files'] == 1 and result['conversations'] == ['c0nv']
    assert [part['text'] for part in messages(spool) if part['kind'] == 'user'] == ['My synthetic badge number is 4417.']
    spool.close()
