"""Native bridge edge cases; all paths/configuration are synthetic and temporary."""
import json
import tomllib
import pytest

from memento import bridge


def spool_at(tmp_path, **overrides):
    config = {'uid': 1000, 'client_uid': 'test-device', 'url': 'http://127.0.0.1:1', 'preset': 'claude',
              'timezone': 'UTC', **overrides}
    path = tmp_path / 'runner.json'
    path.write_bytes(bridge.encode(config))
    return bridge.Spool(path, config)


def pending(spool, endpoint='/episodes'):
    return [json.loads(raw) for (raw,) in spool.db.execute(
        'SELECT body FROM outbox WHERE endpoint=? ORDER BY id', (endpoint,))]


def test_exact_unicode_chunks_survive_restart(tmp_path):
    spool = spool_at(tmp_path)
    text = ('𝄞é中文\n"\\' * 120000)
    spool.episode('claude', 'session', 'entry', 'assistant', text)
    spool.commit()
    parts = pending(spool)
    assert len(parts) > 1
    assert all(len(bridge.encode(part)) <= bridge.MAX_BODY for part in parts)
    assert ''.join(part['text'] for part in parts) == text
    identities = [part['event_id'] for part in parts]
    spool.close()
    spool = bridge.Spool(tmp_path / 'runner.json', json.loads((tmp_path / 'runner.json').read_bytes()))
    spool.episode('claude', 'session', 'entry', 'assistant', text)
    assert [part['event_id'] for part in pending(spool)] == identities
    spool.close()


def test_only_complete_lines_and_unknown_records_retained(tmp_path):
    spool = spool_at(tmp_path)
    journal = tmp_path / 'journal.jsonl'
    unknown = {'type': 'new_native_record', 'opaque': {'payload': 'whole value'}}
    message = {'type': 'assistant', 'uuid': 'a', 'message': {'role': 'assistant', 'content': 'answer [memory:abc]'}}
    first = bridge.encode(unknown) + b'\n'
    second = bridge.encode(message)
    journal.write_bytes(first + second[:30])
    spool.journal(journal, 'claude', 's')
    assert [part['text'] for part in pending(spool)] == [first.decode()]
    assert not pending(spool, '/usage')
    with journal.open('ab') as stream:
        stream.write(second[30:] + b'\n')
    spool.reconcile(str(journal.resolve()))
    assert pending(spool, '/usage')[0]['ids'] == ['abc']
    count = len(pending(spool))
    spool.reconcile(str(journal.resolve()))
    assert len(pending(spool)) == count
    spool.close()


def test_codex_event_projection_never_counts_twice(tmp_path):
    spool = spool_at(tmp_path)
    spool.record('codex', 's', {'type': 'event_msg', 'payload': {'type': 'agent_message', 'message': 'answer [memory:x]'}}, identity='1')
    spool.record('codex', 's', {'type': 'response_item', 'payload': {
        'type': 'message', 'role': 'assistant', 'content': [{'type': 'output_text', 'text': 'answer [memory:x]'}]}}, identity='2')
    assert len([part for part in pending(spool) if part['kind'] == 'assistant']) == 1
    assert len(pending(spool, '/usage')) == 1
    spool.close()


def test_native_roles_and_channel_text_preserved(tmp_path):
    spool = spool_at(tmp_path)
    text = 'Channel framing from Someone:\\nfull original body'
    spool.record('pi', 's', {'id': 'u', 'type': 'message', 'message': {'role': 'user', 'content': text}})
    spool.record('pi', 's', {'id': 'c', 'type': 'custom_message', 'customType': 'custom',
                           'content': 'entire channel message'})
    messages = [part for part in pending(spool) if part['metadata']['extraction_mode'] == 'message']
    assert [(part['kind'], part['text']) for part in messages] == [
        ('user', text), ('custom', 'entire channel message')]
    spool.close()


def test_live_full_content_is_raw_until_committed(tmp_path):
    spool = spool_at(tmp_path)
    live = {'entry_id': 'live:1', 'committed': False, 'message': {'role': 'assistant', 'content': 'full [memory:m]'}}
    bridge.native(spool, {'harness': 'omp', 'session_id': 's', 'action': 'capture', 'live': [live]})
    assert not pending(spool, '/usage')
    assert all(part['metadata']['extraction_mode'] == 'skip' for part in pending(spool))
    entry = {'id': 'committed', 'type': 'message', 'message': {'role': 'assistant', 'content': 'truncated'}}
    live.update(entry_id='committed', committed=True)
    bridge.native(spool, {'harness': 'omp', 'session_id': 's', 'action': 'capture', 'entries': [entry], 'live': [live]})
    assert [part['text'] for part in pending(spool) if part['kind'] == 'assistant'] == ['full [memory:m]']
    assert pending(spool, '/usage')[0]['ids'] == ['m']
    spool.close()


def test_tool_output_stays_full_and_calls_are_not_summarized(tmp_path):
    spool = spool_at(tmp_path)
    block = {'type': 'tool_use', 'id': 't', 'name': 'shell', 'input': {'command': 'bad\\ncommand'}}
    spool.record('claude', 's', {'type': 'assistant', 'uuid': 'call', 'message': {'role': 'assistant', 'content': [block]}})
    full_output = 'entire tool output\\n' * 1000
    spool.record('claude', 's', {'type': 'user', 'uuid': 'result', 'message': {'role': 'user', 'content': [
        {'type': 'tool_result', 'tool_use_id': 't', 'is_error': True, 'content': full_output}]}})
    call = next(part for part in pending(spool) if part['kind'] == 'assistant')
    assert call['text'] == bridge.encode([block]).decode('utf-8')
    output = next(part for part in pending(spool) if part['kind'] == 'tool')
    assert output['text'] == full_output
    assert output['metadata']['extraction_mode'] == 'tool_output'
    spool.close()


def test_native_installer_preserves_hooks_mcp_and_codex_trust(tmp_path):
    config_path = tmp_path / 'runner.json'
    config_path.write_bytes(bridge.encode({'uid': 'u', 'url': 'http://127.0.0.1:1', 'preset': 'codex'}))
    root = tmp_path / 'native'
    root.mkdir()
    existing = {'hooks': {'Stop': [{'hooks': [{'type': 'command', 'command': 'existing-hook'}]}]}, 'unrelated': 4}
    (root / 'hooks.json').write_bytes(bridge.encode(existing))
    original = '[features]\nhooks = true\n\n[mcp_servers.other]\nurl = "http://other/mcp"\n\n[hook_trust]\nnever_touch = "value"\n'
    (root / 'config.toml').write_text(original)
    result = bridge.install(config_path, root)
    assert result['status'] == 'pending-native-trust'
    client_uid = json.loads(config_path.read_bytes())['client_uid']
    assert len(client_uid) == 32
    hooks = json.loads((root / 'hooks.json').read_bytes())
    assert hooks['unrelated'] == 4
    assert hooks['hooks']['Stop'][0]['hooks'][0]['command'] == 'existing-hook'
    assert original in (root / 'config.toml').read_text()
    bridge.install(config_path, root)
    assert json.loads(config_path.read_bytes())['client_uid'] == client_uid
    parsed = tomllib.loads((root / 'config.toml').read_text())
    assert parsed['mcp_servers']['memento']['url'] == 'http://127.0.0.1:1/mcp'
    assert parsed['mcp_servers']['other']['url'] == 'http://other/mcp'
    assert parsed['hook_trust']['never_touch'] == 'value'
    again = json.loads((root / 'hooks.json').read_bytes())
    assert len(again['hooks']['Stop']) == 2
    assert (root / 'hooks.json').stat().st_mode & 0o777 == 0o600
    assert hooks['hooks']['SessionStart'][-1]['hooks'][0]['additionalContextLimit'] == 1500
    assert 'additionalContextLimit' not in hooks['hooks']['PreCompact'][-1]['hooks'][0]
    assert hooks['hooks']['SessionEnd'][-1]['hooks'][0]['timeout'] == 3


def test_processing_guard_before_stdin_and_spool(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv('MEMENTO_PROCESSING', '1')
    assert bridge.main(['native', '--config', str(tmp_path / 'missing.json')]) == 0
    assert json.loads(capsys.readouterr().out) == {'context': '', 'shown': []}
    assert not list(tmp_path.iterdir())


def test_passive_recall_is_bounded_and_never_usage(tmp_path, monkeypatch):
    spool = spool_at(tmp_path)
    monkeypatch.setattr(bridge, 'http', lambda *args, **kwargs: {
        'memories': [{'id': 'abc', 'text': '🧠' * 10000}]})
    result = bridge.passive(spool, 'session', 'query')
    assert len(result['context'].encode('utf-8')) <= bridge.CONTEXT_BYTES
    assert result['shown'] == ['abc']
    assert not pending(spool, '/usage')
    spool.close()


def test_tool_fragment_cap_applies_once_per_original_output(tmp_path):
    spool = spool_at(tmp_path)
    text = '🧠' * 400000
    spool.episode('pi', 's', 'tool-output', 'tool', text,
                  metadata={'extraction_mode': 'tool_output'})
    parts = pending(spool)
    assert len(parts) > 1
    assert ''.join(part['text'] for part in parts) == text
    assert parts[0]['metadata']['extraction_mode'] == 'tool_output'
    assert all(part['metadata']['extraction_mode'] == 'skip' for part in parts[1:])
    assert all(len(bridge.encode(part)) <= bridge.MAX_BODY for part in parts)
    spool.close()


def test_offline_cache_does_not_leak_previous_request(tmp_path, monkeypatch):
    spool = spool_at(tmp_path)
    monkeypatch.setattr(bridge, 'http', lambda *args, **kwargs: {
        'memories': [{'id': 'abc', 'text': 'evidence for this query'}]})
    first = bridge.passive(spool, 'session', 'original query')

    def offline(*args, **kwargs):
        raise OSError('offline')

    monkeypatch.setattr(bridge, 'http', offline)
    assert bridge.passive(spool, 'session', 'original query') == first
    assert bridge.passive(spool, 'session', 'different query') == {'context': '', 'shown': []}
    spool.close()


def test_native_transport_failure_is_not_success(tmp_path, monkeypatch, capsys):
    spool = spool_at(tmp_path)
    config = spool.config_path
    spool.close()
    monkeypatch.setattr(bridge.json, 'load', lambda _stream: {
        'harness': 'pi', 'session_id': 's', 'action': 'tool', 'tool': 'recall',
        'arguments': {'query': 'synthetic', 'scope': 'personal'}})

    def offline(*args, **kwargs):
        raise OSError('offline')

    monkeypatch.setattr(bridge, 'http', offline)
    assert bridge.main(['native', '--config', config]) == 1
    assert json.loads(capsys.readouterr().out)['error'] == 'bridge_failed'


@pytest.mark.parametrize('harness,header', [
    ('pi', {'type': 'session', 'id': 'history-session'}),
    ('omp', {'type': 'session', 'id': 'history-session'}),
    ('claude', {'type': 'system', 'sessionId': 'history-session'}),
    ('codex', {'type': 'session_meta', 'payload': {'id': 'history-session'}}),
])
def test_explicit_history_import_is_idempotent_and_native_identified(tmp_path, harness, header):
    spool = spool_at(tmp_path)
    history = tmp_path / 'explicit-histories'
    history.mkdir()
    message = {'id': 'message', 'type': 'message',
               'message': {'role': 'user', 'content': 'exact full history 猫 🧠'}}
    if harness == 'claude':
        message = {'uuid': 'message', 'type': 'user', 'sessionId': 'history-session',
                   'message': {'role': 'user', 'content': 'exact full history 猫 🧠'}}
    if harness == 'codex':
        message = {'type': 'response_item', 'ordinal': 2,
                   'payload': {'type': 'message', 'role': 'user',
                               'content': [{'type': 'input_text', 'text': 'exact full history 猫 🧠'}]}}
    path = history / 'session.jsonl'
    path.write_bytes(bridge.encode(header) + b'\n' + bridge.encode(message) + b'\n')
    first = bridge.import_history(spool, harness, [history, path])
    assert first == {'status': 'queued', 'source': 'native:test-device:' + harness,
                     'files': 1, 'records_added': 2}
    assert bridge.import_history(spool, harness, [history])['records_added'] == 0
    episodes = pending(spool)
    assert all(part['conversation_id'] == 'history-session' for part in episodes)
    assert any(part['kind'] == 'user' and part['text'] == 'exact full history 猫 🧠' for part in episodes)
    spool.close()


def test_headerless_import_replays_from_zero_with_native_identity(tmp_path, monkeypatch):
    spool = spool_at(tmp_path)
    path = tmp_path / 'session.jsonl'
    path.write_bytes(bridge.encode({'id': 'u', 'role': 'user', 'content': 'native full text'}) + b'\n')
    spool.journal(path, 'pi', 'native-session')
    offsets = []
    reconcile = spool.reconcile

    def replay(path):
        offsets.append(spool.db.execute('SELECT offset FROM journals WHERE path=?', (path,)).fetchone()[0])
        reconcile(path)

    monkeypatch.setattr(spool, 'reconcile', replay)
    assert bridge.import_history(spool, 'pi', [path])['records_added'] == 0
    assert spool.db.execute('SELECT conversation FROM journals').fetchone()[0] == 'native-session'
    assert offsets == [0]
    spool.close()


def test_client_identity_separates_devices_with_same_os_uid(tmp_path):
    assert bridge.source({'uid': 1000, 'client_uid': 'device-one'}, 'pi') != bridge.source(
        {'uid': 1000, 'client_uid': 'device-two'}, 'pi')


def test_claude_provider_request_blobs_are_raw_only(tmp_path):
    spool = spool_at(tmp_path)
    snapshot = {'type': 'api-request-blob', 'sessionId': 's', 'hash': 'snapshot',
                'message': {'role': 'assistant', 'content': [
                    {'type': 'text', 'text': 'replayed context [memory:replayed]'}]}}
    spool.record('claude', 's', snapshot)
    assert not pending(spool, '/usage')
    assert all(part['metadata']['extraction_mode'] == 'skip' for part in pending(spool))
    assert pending(spool)[0]['text'] == bridge.encode(snapshot).decode('utf-8')
    spool.record('claude', 's', {'type': 'assistant', 'uuid': 'committed',
                               'message': snapshot['message']})
    assert len(pending(spool, '/usage')) == 1
    assert [part['kind'] for part in pending(spool)].count('assistant') == 1
    spool.close()


def test_quote_only_native_save_flushes_only_current_conversation(tmp_path, monkeypatch):
    spool = spool_at(tmp_path)
    spool.episode('pi', 'other', 'other', 'user', 'other conversation')
    calls = []

    def accepted(config, endpoint, body=None, timeout=1):
        calls.append((endpoint, body))
        return {'id': 'saved'}

    monkeypatch.setattr(bridge, 'http', accepted)
    bridge.native(spool, {'harness': 'pi', 'session_id': 'current', 'action': 'tool',
        'entries': [{'id': 'user', 'type': 'message', 'message': {'role': 'user', 'content': 'exact current quote'}}],
        'tool': 'remember', 'arguments': {'command': {
            'type': 'knowledge', 'text': 'saved fact', 'scope': ['personal'],
            'evidence': [{'quote': 'exact current quote'}], 'source': 'wrong', 'conversation_id': 'wrong'}}})
    assert [endpoint for endpoint, _ in calls] == ['/episodes', '/episodes', '/remember']
    assert all(body['conversation_id'] == 'current' for _, body in calls)
    assert calls[-1][1]['source'] == 'native:test-device:pi'
    assert calls[-1][1]['evidence'] == [{'quote': 'exact current quote'}]
    assert [body['conversation_id'] for body in pending(spool)] == ['other']
    spool.close()


def test_quote_only_native_save_fails_closed_when_capture_cannot_upload(tmp_path, monkeypatch):
    spool = spool_at(tmp_path)
    spool.episode('pi', 's', 'user', 'user', 'quote')
    calls = []

    def offline(config, endpoint, body=None, timeout=1):
        calls.append(endpoint)
        raise OSError('offline')

    monkeypatch.setattr(bridge, 'http', offline)
    with pytest.raises(OSError):
        bridge.native(spool, {'harness': 'pi', 'session_id': 's', 'action': 'tool',
            'tool': 'remember', 'arguments': {'command': {'evidence': [{'quote': 'quote'}]}}})
    assert calls == ['/episodes']
    assert len(pending(spool)) == 1
    spool.close()


def test_mcp_provenance_hooks_never_auto_approve(tmp_path, monkeypatch):
    spool = spool_at(tmp_path)
    monkeypatch.setattr(bridge, 'http', lambda *args, **kwargs: {'id': 'captured'})
    data = {'hook_event_name': 'PreToolUse', 'session_id': 's', 'tool_use_id': 'call',
            'tool_name': 'mcp__memento__remember', 'tool_input': {'command': {'evidence': [{'quote': 'quote'}]}}}
    result = bridge.hook(spool, 'claude', data)['hookSpecificOutput']
    assert 'permissionDecision' not in result
    assert result['updatedInput']['command']['source'] == 'native:test-device:claude'
    assert result['updatedInput']['command']['conversation_id'] == 's'
    assert result['updatedInput']['command']['command_id'] == 'claude:s:call:remember'
    assert bridge.hook(spool, 'codex', data)['hookSpecificOutput']['permissionDecision'] == 'deny'
    data['tool_input']['command'].update(source='native:test-device:codex', conversation_id='s')
    assert bridge.hook(spool, 'codex', data) == {}
    spool.close()


def test_passive_query_includes_actual_workspace(tmp_path, monkeypatch):
    spool = spool_at(tmp_path)
    endpoints = []

    def recalled(config, endpoint, body=None, timeout=1):
        endpoints.append((endpoint, timeout))
        return {'memories': []}

    monkeypatch.setattr(bridge, 'http', recalled)
    bridge.passive(spool, 's', 'user query', '/projects/example')
    query = bridge.urllib.parse.parse_qs(bridge.urllib.parse.urlsplit(endpoints[0][0]).query)['q'][0]
    assert query == 'Working directory: /projects/example\nDirectory/repo name: example\nuser query'
    assert endpoints[0][1] == 1
    spool.close()


def test_codex_hook_context_and_tool_call_json_are_not_quote_evidence(tmp_path):
    spool = spool_at(tmp_path)
    spool.record('codex', 's', {'type': 'response_item', 'payload': {
        'type': 'message', 'role': 'developer', 'content': [{'type': 'input_text', 'text': 'recalled context'}],
        'internal_chat_message_metadata_passthrough': {'content_item_kinds': ['hooks.additional_context']}}}, identity='context')
    spool.record('pi', 's', {'type': 'message', 'id': 'call', 'message': {
        'role': 'assistant', 'content': [{'type': 'toolCall', 'id': 'tool', 'name': 'memento_remember', 'arguments': {'quote': 'echo'}}]}})
    projected = [body for body in pending(spool) if body['kind'] != 'native_record']
    assert all(body['metadata']['evidence_eligible'] is False for body in projected)
    assert next(body for body in projected if body['kind'] == 'developer')['metadata']['extraction_mode'] == 'skip'
    assert next(body for body in projected if body['kind'] == 'assistant')['metadata']['extraction_mode'] == 'message'
    spool.close()


@pytest.mark.parametrize('harness', ['pi', 'omp', 'claude', 'codex'])
def test_live_journal_then_import_preserves_conversation_and_events(tmp_path, harness):
    spool = spool_at(tmp_path)
    if harness == 'codex':
        header = {'type': 'session_meta', 'payload': {'id': 'different-header-session'}}
        message = {'type': 'response_item', 'ordinal': 2, 'payload': {
            'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'full native 猫 🧠'}]}}
    elif harness == 'claude':
        header = {'type': 'system', 'sessionId': 'different-header-session'}
        message = {'type': 'user', 'uuid': 'message', 'sessionId': 'different-header-session',
                   'message': {'role': 'user', 'content': 'full native 猫 🧠'}}
    else:
        header = {'type': 'session', 'id': 'different-header-session'}
        message = {'type': 'message', 'id': 'message', 'message': {'role': 'user', 'content': 'full native 猫 🧠'}}
    path = tmp_path / 'native.jsonl'
    path.write_bytes(bridge.encode(header) + b'\n' + bridge.encode(message) + b'\n')
    spool.journal(path, harness, 'live-session')
    original = pending(spool)
    spool.journal(path, harness, 'later-native-alias')
    assert spool.db.execute('SELECT harness,conversation FROM journals').fetchone() == (harness, 'live-session')
    for _ in range(2):
        assert bridge.import_history(spool, harness, [path])['records_added'] == 0
        assert pending(spool) == original
    assert all(part['conversation_id'] == 'live-session' for part in original)
    assert any(part['kind'] == 'user' and part['text'] == 'full native 猫 🧠' for part in original)
    spool.close()


@pytest.mark.parametrize('background,expected_pending', [(False, 0), (True, 1)])
def test_upload_drain_and_background_bound_preserve_pending_work(tmp_path, monkeypatch, capsys,
                                                              background, expected_pending):
    spool = spool_at(tmp_path)
    for index in range(3):
        spool.episode('pi', 's', str(index), 'user', 'full message ' + str(index))
    spool.commit()
    spool.close()
    elapsed = [0]
    monkeypatch.setattr(bridge.time, 'monotonic', lambda: elapsed[0])

    def accepted(*args, **kwargs):
        elapsed[0] += 11
        return {}

    monkeypatch.setattr(bridge, 'http', accepted)
    status = bridge.main(['upload', '--config', str(tmp_path / 'runner.json'),
                          *(['--background'] if background else [])])
    spool = spool_at(tmp_path)
    assert len(pending(spool)) == expected_pending
    assert status == bool(expected_pending)
    output = capsys.readouterr().out
    if background:
        assert output == ''
    else:
        assert json.loads(output) == {'pending': 0, 'conflicts': 0}
    spool.close()


@pytest.mark.parametrize('conflict', [False, True])
def test_explicit_upload_reports_retained_transport_errors_and_conflicts(tmp_path, monkeypatch,
                                                                       capsys, conflict):
    spool = spool_at(tmp_path)
    spool.episode('pi', 's', 'u', 'user', 'retained full message')
    spool.commit()
    spool.close()

    def rejected(*args, **kwargs):
        if conflict:
            raise bridge.urllib.error.HTTPError('http://localhost/episodes', 409, 'conflict', {}, None)
        raise OSError('offline')

    monkeypatch.setattr(bridge, 'http', rejected)
    assert bridge.main(['upload', '--config', str(tmp_path / 'runner.json')]) == 1
    assert json.loads(capsys.readouterr().out) == {
        'pending': 0 if conflict else 1, 'conflicts': 1 if conflict else 0}
    spool = spool_at(tmp_path)
    assert pending(spool)[0]['text'] == 'retained full message'
    assert spool.db.execute('SELECT state FROM outbox').fetchone()[0] == ('conflict' if conflict else 'pending')
    spool.close()
