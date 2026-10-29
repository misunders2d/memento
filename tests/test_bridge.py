"""Native bridge edge cases; all paths/configuration are synthetic and temporary."""
import json
import base64
import builtins
import io
import http.server
from http.client import BadStatusLine
import threading
import time
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


def test_fresh_local_ready_claim_does_not_load_http_or_tls(tmp_path):
    import subprocess
    import sys

    result = subprocess.run([sys.executable, '-c', '''
import sys
from memento import bridge
config = dict(uid=1000, client_uid='local-claim', url='http://127.0.0.1:1')
spool = bridge.Spool(sys.argv[1], config)
try:
    assert bridge.native(spool, dict(harness='pi', session_id='local', action='recall-ready', turn='unowned')) == {'pending': False}
    assert 'http.client' not in sys.modules
    assert 'urllib.request' not in sys.modules
    assert 'ssl' not in sys.modules
finally:
    spool.close()
''', str(tmp_path / 'runner.json')], capture_output=True, text=True, timeout=3)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize('status', [302, 307])
def test_http_redirect_never_resends_captured_evidence(status):
    calls = []
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            calls.append(self.path)
            self.send_response(status if self.path == '/episodes' else 200)
            self.send_header('Location', '/redirected')
            self.end_headers()
        do_GET = do_POST
        def log_message(self, *_args):
            pass
    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with pytest.raises(bridge.urllib.error.HTTPError) as rejected:
            bridge.http(dict(url=f'http://127.0.0.1:{server.server_port}'), '/episodes',
                        {'text': 'Synthetic evidence must stay at its configured endpoint.'})
        assert rejected.value.code == status
        assert calls == ['/episodes']
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


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
    assert parsed['mcp_servers']['other']['url'] == 'http://other/mcp'
    assert parsed['hook_trust']['never_touch'] == 'value'
    again = json.loads((root / 'hooks.json').read_bytes())
    assert len(again['hooks']['Stop']) == 2
    assert (root / 'hooks.json').stat().st_mode & 0o777 == 0o600
    assert hooks['hooks']['SessionStart'][-1]['hooks'][0]['additionalContextLimit'] == 1500
    assert 'additionalContextLimit' not in hooks['hooks']['PreCompact'][-1]['hooks'][0]
    assert hooks['hooks']['SessionEnd'][-1]['hooks'][0]['timeout'] == 3
    prompt = hooks['hooks']['UserPromptSubmit']
    assert prompt[-2]['hooks'][0] == {'type': 'command', 'command': prompt[-1]['hooks'][0]['command'].removesuffix(' --late-recall'),
                                     'timeout': 5, 'additionalContextLimit': 1500}
    assert prompt[-1]['hooks'][0] == {'type': 'command', 'command': prompt[-2]['hooks'][0]['command'] + ' --late-recall',
        'async': True, 'timeout': 16, 'additionalContextLimit': 1500}
    assert again == hooks


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


def test_healthy_empty_recall_is_not_reported_as_unavailable(tmp_path, monkeypatch):
    spool = spool_at(tmp_path)
    monkeypatch.setattr(bridge, 'http', lambda *args, **kwargs: {'memories': []})
    assert bridge.passive(spool, 'session', 'query') == {'context': '', 'shown': []}
    spool.close()


def test_passive_slow_healthy_response_and_stalled_request_budget(tmp_path):
    release = threading.Event()
    calls = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            calls.append(self.path)
            if len(calls) > 1:
                release.wait(10)
                return
            time.sleep(1.1)
            body = bridge.encode({'memories': [{'id': 'slow-fact', 'text': 'Synthetic recalled fact.'}]})
            try:
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except OSError:
                pass

        def log_message(self, *_args):
            pass

    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .05}, daemon=True)
    thread.start()
    spool = spool_at(tmp_path, url=f'http://127.0.0.1:{server.server_port}')
    try:
        healthy = bridge.passive(spool, 'session', 'synthetic query')
        assert 'error' not in healthy and healthy['shown'] == ['slow-fact']
        started = time.monotonic()
        assert bridge.passive(spool, 'session', 'synthetic query') == {**healthy, 'error': 'recall_unavailable'}
        assert 2.5 <= time.monotonic() - started < 4.5
        assert len(calls) == 2  # No retry after the stalled request.
    finally:
        release.set()
        spool.close()
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.mark.parametrize('installed,offered', [
    (None, 'b' * 64), ('unknown', 'b' * 64), ('a' * 64, 'a' * 64),
    ('a' * 64, None), ('a' * 64, 123), ('a' * 64, 'B' * 64),
    ('a' * 64, 'b' * 63), ('a' * 64, 'b' * 65), ('a' * 64, 'b' * 64 + '\n'),
])
def test_client_notice_skips_matching_unknown_or_malformed_revisions(tmp_path, monkeypatch, installed, offered):
    spool = spool_at(tmp_path)
    monkeypatch.setattr(bridge, 'CLIENT_REVISION', installed)
    monkeypatch.setattr(bridge, 'http', lambda *_args, **_kwargs: {'memories': [], 'client_revision': offered})
    assert bridge.passive(spool, 'session', 'ordinary request') == {'context': '', 'shown': []}
    assert spool.db.execute('SELECT count(*) FROM recall_cache WHERE conversation=?',
                            (bridge._UPDATE_NOTICE_KEY,)).fetchone()[0] == 0
    spool.close()


def test_client_notice_is_local_once_per_offered_revision_and_never_memory_cache(tmp_path, monkeypatch):
    spool = spool_at(tmp_path)
    monkeypatch.setattr(bridge, 'CLIENT_REVISION', 'a' * 64)
    response = {'memories': [{'id': 'fact', 'text': 'A useful supported synthetic fact.'}],
                'client_revision': 'b' * 64, 'update_command': 'arbitrary-server-command'}
    calls = []
    def recall(*args, **kwargs):
        calls.append(args[1])
        return response
    monkeypatch.setattr(bridge, 'http', recall)
    first = bridge.passive(spool, 'first-session', 'ordinary request')
    notice = first['update_notice']
    assert 'memento agent update claude --config ' in notice and spool.config_path in notice
    assert '--native-config-dir' in notice and 'When asked' in notice
    assert 'arbitrary-server-command' not in notice and 'newer' not in notice
    assert notice not in first['context'] and first['shown'] == ['fact']
    assert spool.db.execute('SELECT context FROM recall_cache WHERE conversation=?',
                            ('first-session',)).fetchone()[0] == first['context']
    spool.commit()
    config_path, config = spool.config_path, spool.config
    spool.close()
    spool = bridge.Spool(config_path, config)
    assert 'update_notice' not in bridge.passive(spool, 'second-session', 'other request')
    response['client_revision'] = 'c' * 64
    assert 'update_notice' in bridge.passive(spool, 'second-session', 'another request')
    assert 'update_notice' not in bridge.passive(spool, 'third-session', 'third request')
    assert len(calls) == 4 and all(path.startswith('/recall?') for path in calls)
    assert spool.db.execute('SELECT count(*) FROM recall_cache WHERE conversation=?',
                            (bridge._UPDATE_NOTICE_KEY,)).fetchone()[0] == 1
    spool.close()


@pytest.mark.parametrize('failure', ['outage', 'error_body', 'malformed_body'])
def test_failed_recall_does_not_consume_or_replay_update_notice(tmp_path, monkeypatch, failure):
    spool = spool_at(tmp_path)
    monkeypatch.setattr(bridge, 'CLIENT_REVISION', 'a' * 64)
    response = {'memories': [{'id': 'fact', 'text': 'Supported cached evidence.'}], 'client_revision': 'b' * 64}
    def recall(*_args, **_kwargs):
        if failure == 'outage':
            raise OSError('unavailable')
        return {**response, 'error': 'unavailable'} if failure == 'error_body' else []
    monkeypatch.setattr(bridge, 'http', recall)
    failed = bridge.passive(spool, 's', 'ordinary request')
    assert failed['error'] == 'recall_unavailable' and 'update_notice' not in failed
    monkeypatch.setattr(bridge, 'http', lambda *_args, **_kwargs: response)
    success = bridge.passive(spool, 's', 'ordinary request')
    assert success['update_notice'] and success['shown'] == ['fact']
    def outage(*_args, **_kwargs):
        raise OSError('unavailable')
    monkeypatch.setattr(bridge, 'http', outage)
    cached = bridge.passive(spool, 's', 'ordinary request')
    assert cached == {'context': success['context'], 'shown': ['fact'], 'error': 'recall_unavailable'}
    spool.close()


def test_client_notice_claim_is_atomic_across_native_sessions(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    spool = spool_at(tmp_path)
    path, config = spool.config_path, spool.config
    spool.close()
    monkeypatch.setattr(bridge, 'CLIENT_REVISION', 'a' * 64)
    monkeypatch.setattr(bridge, 'http', lambda *_args, **_kwargs: {'memories': [], 'client_revision': 'b' * 64})
    def recall(index):
        local = bridge.Spool(path, config)
        try:
            result = bridge.passive(local, 'session-' + str(index), 'ordinary request')
            local.commit()
            return result
        finally:
            local.close()
    with ThreadPoolExecutor(max_workers=4) as workers:
        results = list(workers.map(recall, range(4)))
    assert sum('update_notice' in result for result in results) == 1
    assert all(result['context'] == '' and result['shown'] == [] for result in results)


def test_client_notice_that_cannot_fit_is_not_consumed(tmp_path, monkeypatch):
    spool = spool_at(tmp_path)
    monkeypatch.setattr(bridge, 'CLIENT_REVISION', 'a' * 64)
    monkeypatch.setattr(bridge, 'http', lambda *_args, **_kwargs: {'memories': [], 'client_revision': 'b' * 64})
    assert 'update_notice' not in bridge.passive(spool, 's', 'ordinary request', budget=1, notice_in_context=True)
    assert bridge.passive(spool, 's', 'ordinary request', notice_in_context=True)['update_notice']
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
    assert bridge.passive(spool, 'session', 'original query') == {**first, 'error': 'recall_unavailable'}
    assert bridge.passive(spool, 'session', 'different query') == {
        'context': '', 'shown': [], 'error': 'recall_unavailable'}
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
    assert {key: first[key] for key in ('status', 'source', 'files', 'records_added')} == {
        'status': 'queued', 'source': 'native:test-device:' + harness, 'files': 1, 'records_added': 2}
    assert first['coverage'] == {'unsupported_records': 0, 'attachments': 0,
                                  'missing_attachments': 0, 'unsupported_attachments': 0, 'inline_payload_bytes': 0}
    assert first['pending'] == len(pending(spool)) + 1 and first['conflicts'] == 0
    assert first['conversations'] == ['history-session']
    assert bridge.import_history(spool, harness, [history])['records_added'] == 0
    episodes = pending(spool)
    assert all(part['conversation_id'] == 'history-session' for part in episodes)
    assert any(part['kind'] == 'user' and part['text'] == 'exact full history 猫 🧠' for part in episodes)
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
    assert [endpoint for endpoint, _ in calls] == ['/episodes', '/remember']
    assert all(body['conversation_id'] == 'current' for _, body in calls)
    assert calls[-1][1]['source'] == 'native:test-device:pi'
    assert calls[-1][1]['evidence'] == [{'quote': 'exact current quote'}]
    assert [(body['conversation_id'], body['kind']) for body in pending(spool)] == [
        ('other', 'user'), ('current', 'native_record')]
    spool.close()


def test_quote_only_native_save_fails_closed_when_capture_cannot_upload(tmp_path, monkeypatch):
    spool = spool_at(tmp_path)
    spool.record('pi', 's', {'id': 'user', 'type': 'message', 'message': {'role': 'user', 'content': 'quote'}})
    calls = []

    def offline(config, endpoint, body=None, timeout=1):
        calls.append(endpoint)
        raise OSError('offline')

    monkeypatch.setattr(bridge, 'http', offline)
    with pytest.raises(OSError):
        bridge.native(spool, {'harness': 'pi', 'session_id': 's', 'action': 'tool',
            'tool': 'remember', 'arguments': {'command': {'evidence': [{'quote': 'quote'}]}}})
    assert calls == ['/episodes']
    assert len(pending(spool)) == 2
    spool.close()


def test_quote_flush_ignores_large_backlog_and_uses_latest_eligible_projection(tmp_path, monkeypatch):
    spool = spool_at(tmp_path)
    unrelated = bridge.encode({'source': 'other-device', 'conversation_id': 'other', 'text': 'unrelated'})
    spool.db.executemany('INSERT INTO outbox(key,endpoint,body) VALUES(?,?,?)',
                        ((f'backlog:{index}', '/episodes', unrelated) for index in range(100000)))
    quote = 'Exact 猫 quote\nwith "punctuation".'
    for identity, role in (('old', 'user'), ('new', 'assistant')):
        spool.record('pi', 'current', {'id': identity, 'type': 'message',
                                     'message': {'role': role, 'content': quote}})
    newest = json.loads(spool.db.execute('SELECT body FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0])
    # A raw revision is newer than the matching assistant projection, but does
    # not emit a second message. Record insertion order must not select "old".
    spool.record('pi', 'current', {'id': 'old', 'type': 'message',
                                 'message': {'role': 'user', 'content': quote + ' revised native snapshot'}})
    spool.record('pi', 'current', {'id': 'echo', 'type': 'message', 'message': {
        'role': 'assistant', 'content': [{'type': 'toolCall', 'name': 'remember', 'arguments': {'quote': quote}}]}})
    spool.commit()
    before = spool.db.execute('SELECT count(*) FROM outbox').fetchone()[0]
    work = 0

    def bounded_work():
        nonlocal work
        work += 1
        return work > 2000

    uploaded = []

    def accepted(_config, endpoint, body, **_kwargs):
        assert endpoint == '/episodes' and not spool.db.in_transaction
        # No selection cursor/read transaction may prevent a concurrent writer.
        with bridge.sqlite3.connect(spool.path, timeout=.1) as other:
            other.execute('UPDATE outbox SET state=state WHERE id=1')
        uploaded.append(body)
        return {'id': 'actual-ack-id'}

    monkeypatch.setattr(bridge, 'http', accepted)
    spool.db.set_progress_handler(bounded_work, 100)
    result = bridge.flush_conversation(spool, 'pi', 'current', [quote])
    spool.db.set_progress_handler(None, 0)
    assert uploaded == [newest]
    assert result == {'status': 'uploaded', 'evidence': [{
        'quote_index': 0, 'event_id': newest['event_id'], 'episode_id': 'actual-ack-id'}], 'unmatched_quotes': []}
    assert spool.db.execute('SELECT count(*) FROM outbox').fetchone()[0] == before - 1
    assert work <= 2000
    spool.close()


def test_quote_flush_finds_all_message_chunks_and_codex_mcp_result_keys(tmp_path, monkeypatch):
    spool = spool_at(tmp_path)
    monkeypatch.setattr(bridge, 'MAX_BODY', 1024)
    spool.record('codex', 'current', {'type': 'response_item', 'payload': {
        'type': 'message', 'role': 'assistant',
        'content': [{'type': 'output_text', 'text': 'First exact quote.' + 'abcdefghij' * 500 + 'Last exact quote.'}]}},
        identity='chunked')
    chunks = [body for body in pending(spool) if body['kind'] == 'assistant']
    assert len(chunks) > 2
    spool.record('codex', 'current', {'type': 'event_msg', 'payload': {
        'type': 'item_completed', 'item': {'type': 'McpToolCall', 'id': 'mcp-item',
            'server': 'example', 'tool': 'lookup', 'arguments': {},
            'result': {'content': [{'type': 'text', 'text': 'Actual MCP result evidence.'}]}}}}, identity='mcp')
    result_body = next(body for body in pending(spool) if body['kind'] == 'tool')
    # Exact full chunks distinguish the later fragment from the first fragment.
    quotes = [chunks[0]['text'], chunks[-1]['text'], result_body['text']]
    uploaded = []
    monkeypatch.setattr(bridge, 'http', lambda _config, _endpoint, body, **_kwargs:
                        uploaded.append(body) or {'id': body['event_id']})
    result = bridge.flush_conversation(spool, 'codex', 'current', quotes)
    assert result['status'] == 'uploaded' and not result['unmatched_quotes']
    assert len(result['evidence']) == 3
    assert result['evidence'][-1]['event_id'] == result_body['event_id']
    assert any(body['event_id'] == chunks[-1]['event_id'] for body in uploaded)
    assert all(body['kind'] in ('assistant', 'tool') and body['metadata'].get('evidence_eligible', True)
               for body in uploaded)
    spool.close()


def test_quote_flush_does_not_claim_unmatched_or_embedded_tool_output_is_uploaded(tmp_path, monkeypatch):
    spool = spool_at(tmp_path)
    spool.record('claude', 'current', {'type': 'user', 'uuid': 'result', 'message': {'role': 'user', 'content': [
        {'type': 'tool_result', 'tool_use_id': 't', 'content': 'Embedded tool evidence.'}]}})
    spool.record('codex', 'current', {'type': 'response_item', 'payload': {
        'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'Injected context.'}],
        'internal_chat_message_metadata_passthrough': {'content_item_kinds': ['agents_md.instructions']}}},
        identity='injected')
    monkeypatch.setattr(bridge, 'http', lambda *_args, **_kwargs: pytest.fail('No eligible pending message matched'))
    for harness, quote in (('claude', 'Embedded tool evidence.'), ('codex', 'Injected context.')):
        assert bridge.flush_conversation(spool, harness, 'current', [quote]) == {
            'status': 'not_found', 'evidence': [], 'unmatched_quotes': [0]}
    assert len(pending(spool)) == 4
    spool.close()


@pytest.mark.parametrize('harness', ['pi', 'omp', 'claude', 'codex'])
@pytest.mark.parametrize('tool', ['remember', 'link', 'supersede', 'reconcile'])
def test_mutation_callers_flush_only_unresolved_quotes_without_permission_changes(tmp_path, monkeypatch, harness, tool):
    spool = spool_at(tmp_path)
    flushed = []
    monkeypatch.setattr(bridge, 'flush_conversation', lambda _spool, _harness, _conversation, quotes:
                        flushed.append(quotes))

    def accepted(_config, endpoint, body, **_kwargs):
        assert endpoint == '/' + tool and not spool.db.in_transaction
        return {'id': 'saved'}

    monkeypatch.setattr(bridge, 'http', accepted)
    command = {'source': bridge.source(spool.config, harness), 'conversation_id': 'current',
               'evidence': [{'episode_id': 'already-remote', 'quote': 'Remote quote.'}, {'quote': 'Pending quote.'}]}
    for evidence in (command['evidence'], command['evidence'][:1]):
        command['evidence'] = evidence
        flushed.clear()
        if harness in ('pi', 'omp'):
            result = bridge.native(spool, {'harness': harness, 'session_id': 'current', 'action': 'tool',
                'entries': [{'id': 'u', 'role': 'user', 'content': 'Unrelated pending capture.'}],
                'tool': tool, 'arguments': {'command': command}})
            assert result == {'id': 'saved'}
        else:
            result = bridge.hook(spool, harness, {'hook_event_name': 'PreToolUse', 'session_id': 'current',
                'tool_name': 'mcp__memento__' + tool, 'tool_use_id': 'call', 'tool_input': {'command': command}})
            if harness == 'codex':
                assert result == {}
            else:
                assert 'permissionDecision' not in result['hookSpecificOutput']
                assert result['hookSpecificOutput']['updatedInput']['command']['evidence'] == evidence
        assert flushed == ([['Pending quote.']] if len(evidence) == 2 else [])
    spool.close()


@pytest.mark.parametrize('failure', ['conflict', 'timeout'])
def test_quote_flush_failure_retains_exact_projection_and_does_not_save(tmp_path, monkeypatch, failure):
    spool = spool_at(tmp_path)
    spool.record('pi', 'current', {'id': 'u', 'role': 'user', 'content': 'Exact pending evidence.'})
    before = spool.db.execute('SELECT id,body FROM outbox ORDER BY id').fetchall()
    calls = []

    def failed(_config, endpoint, _body, **_kwargs):
        calls.append(endpoint)
        if failure == 'conflict':
            raise bridge.urllib.error.HTTPError('http://private.invalid', 409, 'private', {}, None)
        raise TimeoutError('private transport details')

    monkeypatch.setattr(bridge, 'http', failed)
    with pytest.raises((bridge.urllib.error.HTTPError, TimeoutError)):
        bridge.native(spool, {'harness': 'pi', 'session_id': 'current', 'action': 'tool',
            'tool': 'remember', 'arguments': {'evidence': [{'quote': 'Exact pending evidence.'}]}})
    assert calls == ['/episodes']
    assert spool.db.execute('SELECT id,body FROM outbox ORDER BY id').fetchall() == before
    assert spool.db.execute('SELECT state FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0] == (
        'conflict' if failure == 'conflict' else 'pending')
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


@pytest.mark.parametrize('query,followup', [
    ('does it introduce unnecessary overhead?', True),
    ('what about other laptop?', True),
    ('Where should I have lunch?', False),
])
def test_passive_preserves_human_query_and_native_followup_boundary_with_cwd(tmp_path, monkeypatch, query, followup):
    from memento.retrieval import _automatic_query

    spool = spool_at(tmp_path)
    endpoints = []

    def recalled(config, endpoint, body=None, timeout=1):
        endpoints.append((endpoint, timeout))
        return {'memories': []}

    monkeypatch.setattr(bridge, 'http', recalled)
    antecedent = 'How should I connect a second native client?'
    bridge.passive(spool, 's', antecedent, '/projects/example')
    bridge.passive(spool, 's', query, '/projects/example')
    requests = [bridge.urllib.parse.parse_qs(bridge.urllib.parse.urlsplit(endpoint).query)
                for endpoint, _ in endpoints]
    assert requests[0]['q'] == [antecedent]
    assert requests[1]['q'] == [query] and requests[1]['context'] == [antecedent]
    effective = _automatic_query(requests[1]['q'][0], requests[1]['context'][0])
    if followup:
        assert antecedent in effective and query in effective
    else:
        assert effective == query
    assert '/projects/example' not in effective and 'Directory/repo name' not in effective
    assert spool.db.execute('SELECT query FROM recall_cache WHERE conversation=?', ('s',)).fetchone()[0] == query
    assert all(timeout == 3 for _, timeout in endpoints)
    spool.close()


@pytest.mark.parametrize('marker', ['hooks.additional_context', 'agents_md.instructions',
                                    'environments.environment_context'])
def test_codex_hook_context_and_tool_call_json_are_not_quote_evidence(tmp_path, marker):
    spool = spool_at(tmp_path)
    spool.record('codex', 's', {'type': 'response_item', 'payload': {
        'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'injected context'}],
        'internal_chat_message_metadata_passthrough': {'content_item_kinds': [marker]}}}, identity='context')
    spool.record('pi', 's', {'type': 'message', 'id': 'call', 'message': {
        'role': 'assistant', 'content': [{'type': 'toolCall', 'id': 'tool', 'name': 'memento_remember', 'arguments': {'quote': 'echo'}}]}})
    projected = [body for body in pending(spool) if body['kind'] != 'native_record']
    assert all(body['metadata']['evidence_eligible'] is False for body in projected)
    context = next(body for body in projected if body['kind'] == 'user')
    assert context['metadata']['extraction_mode'] == 'skip'
    assert context['metadata']['native_context_kinds'] == [marker]
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


def test_large_legacy_outbox_upload_selects_in_bounded_work_and_preserves_priority(tmp_path, monkeypatch):
    spool = spool_at(tmp_path)
    config = spool.config
    config_path = tmp_path / 'runner.json'
    original = b'original artifact bytes'
    spool.db.execute('DROP INDEX outbox_pending_order')
    spool.db.execute('INSERT INTO outbox(key,endpoint,body,content_type) VALUES(?,?,?,?)',
                     ('oldest-blob', '/blobs', original, 'application/octet-stream'))
    spool.db.execute("INSERT INTO outbox(key,endpoint,body,state) VALUES(?,?,?,'conflict')",
                     ('collision', '/episodes', bridge.encode({'text': 'Retained collision.'})))
    spool.db.executemany('INSERT INTO outbox(key,endpoint,body) VALUES(?,?,?)',
                         ((f'text{i}', '/episodes', bridge.encode({'text': f'Queued text {i}.'}))
                          for i in range(3, 100001)))
    spool.commit()
    spool.close()

    real_spool = bridge.Spool
    selection_work = []

    def open_spool(*args, **kwargs):
        result = real_spool(*args, **kwargs)

        def progress():
            selection_work[-1] += 100
            return selection_work[-1] > 1000

        def trace(sql):
            if sql.startswith('SELECT id,endpoint,body,content_type FROM outbox'):
                selection_work.append(0)
                result.db.set_progress_handler(progress, 100)
            else:
                result.db.set_progress_handler(None, 0)

        result.db.set_trace_callback(trace)
        return result

    attempted = []

    def transport(_config, endpoint, body, **_kwargs):
        assert endpoint == '/episodes'
        attempted.append(body['text'])
        if len(attempted) == 2:
            raise OSError('synthetic transport outage')
        return {}

    monkeypatch.setattr(bridge, 'Spool', open_spool)
    monkeypatch.setattr(bridge, 'http', transport)
    result = bridge.upload(config_path, config)
    assert {key: result[key] for key in ('pending', 'conflicts')} == {'pending': 99998, 'conflicts': 1}
    assert result['error']['category'] == 'network'
    assert attempted == ['Queued text 3.', 'Queued text 4.']
    assert len(selection_work) == 2 and max(selection_work) <= 1000
    spool = real_spool(config_path, config)
    try:
        assert spool.db.execute('SELECT body FROM outbox WHERE key=?', ('oldest-blob',)).fetchone() == (original,)
        assert spool.db.execute('SELECT state FROM outbox WHERE key=?', ('collision',)).fetchone() == ('conflict',)
        assert spool.db.execute('SELECT count(*) FROM outbox WHERE key=?', ('text3',)).fetchone()[0] == 0
        assert spool.db.execute('SELECT count(*) FROM outbox WHERE key=?', ('text4',)).fetchone()[0] == 1
    finally:
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


@pytest.mark.parametrize('failure,category,http_status', [
    ('network', 'network', None), ('http_protocol', 'network', None), ('timeout', 'timeout', None),
    ('wrapped_timeout', 'timeout', None), ('http', 'http', 503),
    ('conflict', 'http', 409), ('validation', 'validation', None), ('lock', 'lock', None)])
def test_explicit_upload_reports_retained_transport_errors_and_conflicts(tmp_path, monkeypatch,
                                                                       capsys, failure, category, http_status):
    spool = spool_at(tmp_path)
    private = 'private-transcript-credential-response-marker'
    text = 'retained full message ' + private
    spool.episode('pi', 's', 'u', 'user', text)
    spool.commit()
    spool.close()

    def rejected(*args, **kwargs):
        if http_status:
            raise bridge.urllib.error.HTTPError('https://' + private + '/episodes', http_status,
                private, {'Authorization': private}, io.BytesIO(private.encode()))
        if failure == 'lock':
            return {}
        if failure == 'wrapped_timeout':
            raise bridge.urllib.error.URLError(TimeoutError(private))
        if failure == 'http_protocol':
            raise BadStatusLine(private)
        raise {'network': OSError, 'timeout': TimeoutError, 'validation': ValueError}[failure](private)

    monkeypatch.setattr(bridge, 'http', rejected)
    blocker = bridge.sqlite3.connect(tmp_path / 'runner.bridge.sqlite3') if failure == 'lock' else None
    try:
        if blocker is not None:
            blocker.execute('BEGIN IMMEDIATE')
        assert bridge.main(['upload', '--config', str(tmp_path / 'runner.json')]) == 1
    finally:
        if blocker is not None:
            blocker.rollback()
            blocker.close()
    output = capsys.readouterr()
    result = json.loads(output.out)
    conflict = failure == 'conflict'
    assert {key: result[key] for key in ('pending', 'conflicts')} == {
        'pending': 0 if conflict else 1, 'conflicts': 1 if conflict else 0}
    assert result['error']['category'] == category
    assert result['error'].get('http_status') == http_status
    if failure != 'lock':
        assert result['error']['outbox_id'] == 1
    assert 'retry the same upload' in result['error']['action']
    assert private not in output.out + output.err
    spool = spool_at(tmp_path)
    assert pending(spool)[0]['text'] == text
    assert spool.db.execute('SELECT state FROM outbox').fetchone()[0] == ('conflict' if conflict else 'pending')
    spool.close()
    if not conflict:
        monkeypatch.setattr(bridge, 'http', lambda *args, **kwargs: {})
        assert bridge.main(['upload', '--config', str(tmp_path / 'runner.json')]) == 0
        assert json.loads(capsys.readouterr().out) == {'pending': 0, 'conflicts': 0}


def test_claude_workflow_journal_is_distinct_from_its_agents(tmp_path):
    spool = spool_at(tmp_path)
    workflow = tmp_path / 'workflows' / 'workflow'
    workflow.mkdir(parents=True)
    journal = workflow / 'journal.jsonl'
    events = [{'type': 'started', 'agentId': 'first-agent'},
              {'type': 'started', 'agentId': 'second-agent'}]
    journal.write_bytes(b''.join(bridge.encode(event) + b'\n' for event in events))
    agent = tmp_path / 'agent.jsonl'
    agent.write_bytes(bridge.encode({
        'type': 'assistant', 'uuid': 'answer', 'agentId': 'first-agent',
        'message': {'role': 'assistant', 'content': 'The first agent completed its task.'},
    }) + b'\n')
    bridge.import_history(spool, 'claude', [tmp_path])
    captured = pending(spool)
    agent_conversation = next(part['conversation_id'] for part in captured
                              if part['kind'] == 'assistant')
    workflow_conversations = {part['conversation_id'] for part in captured
                              if part['kind'] == 'native_record'
                              and json.loads(part['text']).get('type') == 'started'}
    assert agent_conversation == 'first-agent'
    assert agent_conversation not in workflow_conversations
    assert len(workflow_conversations) == 1
    spool.close()


def test_history_replay_backfills_missing_projection_without_repeating_raw(tmp_path):
    spool = spool_at(tmp_path)
    header = {'type': 'session_meta', 'ordinal': 1, 'payload': {'id': 'codex-session'}}
    record = {'type': 'response_item', 'ordinal': 2, 'payload': {
        'type': 'message', 'role': 'user',
        'content': [{'type': 'input_text', 'text': 'Keep the original searchable user text.'}]}}
    raw = bridge.encode(record).decode('utf-8') + '\n'
    path = tmp_path / 'rollout.jsonl'
    path.write_bytes(bridge.encode(header) + b'\n' + raw.encode('utf-8'))
    # Persist the raw-only state left by an older projection implementation.
    spool.episode('codex', 'codex-session', 'raw:2:' + bridge.digest(raw),
                  'native_record', raw, metadata={'extraction_mode': 'skip', 'native_id': '2'})
    spool.db.execute('INSERT INTO records VALUES(?,?,?,?)',
                     (bridge.source(spool.config, 'codex'), 'codex-session', '2', bridge.digest(raw)))
    spool.commit()
    spool.close()
    spool = spool_at(tmp_path)
    for _ in range(2):
        bridge.import_history(spool, 'codex', [path])
        captured = pending(spool)
        assert [part['text'] for part in captured if part['kind'] == 'user'] == [
            'Keep the original searchable user text.']
        assert [part['text'] for part in captured
                if part['kind'] == 'native_record' and part['text'] == raw] == [raw]
        assert all(part['conversation_id'] == 'codex-session' for part in captured)
    spool.close()


@pytest.mark.parametrize('canonical_first', [True, False])
def test_codex_canonical_user_notifications_remain_raw_without_suppressing_repeated_provider_turns(tmp_path, canonical_first):
    spool = spool_at(tmp_path)
    for number in range(2):
        identifier = 'user-' + str(number)
        item = {'type': 'UserMessage', 'id': 'canonical-' + identifier, 'client_id': 'native-client',
                'content': [{'type': 'text', 'text': 'Repeat this exact request.', 'text_elements': []}]}
        canonical = {'ordinal': number * 2, 'type': 'event_msg', 'payload': {
            'type': 'item_completed', 'thread_id': 's', 'turn_id': 'turn-' + str(number), 'item': item}}
        provider = {'ordinal': number * 2 + 1, 'type': 'response_item', 'payload': {
            'id': identifier, 'type': 'message', 'role': 'user',
            'content': [{'type': 'input_text', 'text': 'Repeat this exact request.'}]}}
        records = (canonical, provider) if canonical_first else (provider, canonical)
        for record in records:
            spool.record('codex', 's', record, identity=str(record['ordinal']))
    users = [part for part in pending(spool) if part['kind'] == 'user']
    assert [part['text'] for part in users] == ['Repeat this exact request.'] * 2
    assert len([part for part in pending(spool) if part['kind'] == 'native_record']) == 4
    spool.close()


def test_codex_canonical_assistant_and_mcp_result_are_projected_once(tmp_path):
    spool = spool_at(tmp_path)
    items = [
        {'type': 'AgentMessage', 'id': 'answer', 'content': [{'type': 'Text', 'text': 'answer [memory:m]'}],
         'phase': 'final_answer'},
        {'type': 'McpToolCall', 'id': 'call', 'server': 'example', 'tool': 'read', 'arguments': {'id': 'a'},
         'status': 'completed', 'result': {'content': [{'type': 'text', 'text': 'complete tool output'}],
                                         'isError': False}, 'duration': {'secs': 1, 'nanos': 0}}]
    for item in items:
        spool.record('codex', 's', {'type': 'event_msg', 'payload': {
            'type': 'item_completed', 'turn_id': 'turn', 'item': item}}, identity=item['id'])
    spool.record('codex', 's', {'type': 'response_item', 'payload': {
        'id': 'answer', 'type': 'message', 'role': 'assistant',
        'content': [{'type': 'output_text', 'text': 'answer [memory:m]'}]}}, identity='provider-answer')
    assert any(part['kind'] == 'assistant' and part['text'] == 'answer [memory:m]' for part in pending(spool))
    result = next(part for part in pending(spool) if part['kind'] == 'tool')
    assert result['text'] == 'complete tool output'
    assert result['metadata']['extraction_mode'] == 'tool_output'
    assert len(pending(spool, '/usage')) == 1
    assert len([part for part in pending(spool) if part['kind'] == 'assistant' and part['text'] == 'answer [memory:m]']) == 1
    spool.close()


def test_codex_assistant_realtime_transcript_is_exact_and_replays_once(tmp_path):
    spool = spool_at(tmp_path)
    timestamp = '2026-10-20T12:34:56.789Z'
    text = '  Spoken answer: café.\nKeep this exact ending.  '
    payload = {'id': 'spoken-answer', 'realtime_session_id': 'voice-session',
               'type': 'transcript_segment', 'role': 'assistant', 'text': text}
    raw_only = [
        {**payload, 'id': 'spoken-user', 'role': 'user'},
        {**payload, 'id': 'empty', 'text': ''},
        {**payload, 'id': 'invalid-text', 'text': ['not a transcript']},
        {key: value for key, value in payload.items() if key != 'text'},
        {**payload, 'id': ''}, {**payload, 'id': 7},
        {key: value for key, value in payload.items() if key != 'id'},
        {**payload, 'id': 'other-role', 'role': 'tool'},
        {'id': 'started', 'type': 'realtime_session_started', 'realtime_session_id': 'voice-session'},
        {'id': 'closed', 'type': 'realtime_session_closed', 'outcome': 'completed'},
        {**payload, 'id': 'other-type', 'type': 'unknown_realtime_item'}, None,
    ]
    records = [{'timestamp': timestamp, 'ordinal': index, 'type': 'realtime_item', 'payload': item}
               for index, item in enumerate([payload, payload, *raw_only], 1)]
    lines = [json.dumps(record, ensure_ascii=False) + '\n' for record in records]
    journal = tmp_path / 'voice.jsonl'
    journal.write_text(''.join(lines), encoding='utf-8')
    spool.journal(journal, 'codex', 'conversation')
    captured = pending(spool)
    messages = [part for part in captured if part['kind'] != 'native_record']
    assert len(messages) == 1
    assert (messages[0]['source'], messages[0]['conversation_id'], messages[0]['kind'],
            messages[0]['occurred_at'], messages[0]['text']) == (
                'native:test-device:codex', 'conversation', 'assistant', timestamp, text)
    assert messages[0]['metadata']['native_id'] == '1'
    assert messages[0]['metadata']['extraction_mode'] == 'message'
    assert [part['text'] for part in captured if part['kind'] == 'native_record'] == lines
    spool.reconcile(str(journal.resolve()), force=True)
    assert pending(spool) == captured
    spool.close()


@pytest.mark.parametrize('style', ['pi', 'claude', 'codex'])
def test_inline_artifact_projection_keeps_bytes_out_of_text_and_queues_exact_original(tmp_path, style):
    spool = spool_at(tmp_path)
    data = b'\x00opaque-image-bytes\xff' * 1000
    encoded = base64.b64encode(data).decode()
    block = {'type': 'image', 'data': encoded, 'mimeType': 'image/webp'}
    if style == 'claude':
        block = {'type': 'image', 'source': {'type': 'base64', 'data': encoded, 'media_type': 'image/webp'}}
    if style == 'codex':
        block = {'type': 'input_image', 'image_url': 'data:image/webp;base64,' + encoded}
    record = {'type': 'message', 'id': 'image', 'message': {'role': 'user',
              'content': [{'type': 'text', 'text': 'Remember this image.'}, block]}}
    spool.record('pi', 's', record)
    raw = next(part for part in pending(spool) if part['kind'] == 'native_record')
    user = next(part for part in pending(spool) if part['kind'] == 'user')
    assert json.loads(raw['text']) == record
    assert encoded not in user['text'] and 'Remember this image.' in user['text']
    artifact = user['metadata']['artifacts'][0]
    assert artifact['sha256'] == bridge.digest(data) and artifact['size'] == len(data)
    queued = spool.db.execute("SELECT body,content_type FROM outbox WHERE endpoint='/blobs'").fetchall()
    assert queued == [(data, 'image/webp')]
    spool.close()


def test_explicit_omp_blob_root_recovers_original_without_outside_discovery(tmp_path):
    spool = spool_at(tmp_path)
    sessions = tmp_path / 'sessions'
    blobs = tmp_path / 'blobs'
    sessions.mkdir()
    blobs.mkdir()
    data = b'exact OMP webp original'
    sha = bridge.digest(data)
    path = sessions / 'native.jsonl'
    path.write_bytes(bridge.encode({'type': 'session', 'id': 's'}) + b'\n' + bridge.encode({
        'type': 'message', 'id': 'u', 'message': {'role': 'user', 'content': [
            {'type': 'image', 'data': 'blob:sha256:' + sha, 'mimeType': 'image/webp'}]}}) + b'\n')
    missing = bridge.import_history(spool, 'omp', [sessions])
    assert missing['coverage']['missing_attachments'] == 1
    assert not spool.db.execute("SELECT 1 FROM outbox WHERE endpoint='/blobs'").fetchone()
    (blobs / (sha + '.webp')).write_bytes(data)
    recovered = bridge.import_history(spool, 'omp', [sessions], artifact_roots=[blobs])
    assert recovered['records_added'] == 0 and recovered['coverage']['attachments'] == 1
    assert spool.db.execute("SELECT body FROM outbox WHERE endpoint='/blobs'").fetchone()[0] == data
    assert len([part for part in pending(spool) if part['kind'] == 'user']) == 1
    spool.close()


def test_hundred_registered_histories_do_not_reopen_during_current_hook(tmp_path, monkeypatch):
    spool = spool_at(tmp_path)
    paths = []
    for index in range(100):
        path = tmp_path / (str(index) + '.jsonl')
        path.write_bytes(bridge.encode({'type': 'user', 'uuid': str(index),
            'message': {'role': 'user', 'content': 'old history'}}) + b'\n')
        spool.journal(path, 'claude', str(index))
        paths.append(path)
    opened = []
    original_open = builtins.open

    def counted(path, *args, **kwargs):
        if str(path).endswith('.jsonl'):
            opened.append(str(path))
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, 'open', counted)
    bridge.hook(spool, 'claude', {'session_id': '0', 'hook_event_name': 'Stop',
                                 'transcript_path': str(paths[0])})
    assert opened == []
    with original_open(paths[0], 'ab') as stream:
        stream.write(bridge.encode({'type': 'assistant', 'uuid': 'new',
                     'message': {'role': 'assistant', 'content': 'new answer'}}) + b'\n')
    bridge.hook(spool, 'claude', {'session_id': '0', 'hook_event_name': 'Stop',
                                 'transcript_path': str(paths[0])})
    assert opened == [str(paths[0])]
    spool.commit()
    spool.close()
    monkeypatch.setattr(bridge, 'http', lambda *args, **kwargs: {})
    monkeypatch.setattr(bridge.time, 'sleep', lambda *_: None)
    bridge.upload(tmp_path / 'runner.json', bridge.read_config(tmp_path / 'runner.json'),
                  background=True, paths=[str(paths[0])])
    assert opened == [str(paths[0])]


@pytest.mark.parametrize('event', ['SessionStart', 'UserPromptSubmit', 'Stop'])
def test_codex_ordinary_hook_defers_unread_transcript_until_detached_upload(tmp_path, monkeypatch, capsys, event):
    spool = spool_at(tmp_path, preset='codex')
    config_path, config = spool.config_path, spool.config
    spool.close()
    path = tmp_path / 'current.jsonl'
    raw = bridge.encode({'type': 'response_item', 'payload': {'type': 'message',
        'role': 'user', 'content': 'Exact unread source. ' + '猫' * 10000}}) + b'\n'
    tail = bridge.encode({'type': 'event_msg', 'payload': {'opaque': 'Exact next record.'}}) + b'\n'
    path.write_bytes(raw + tail[:20])
    reads_allowed = False
    original_open = builtins.open

    def guarded_open(value, *args, **kwargs):
        if str(value) == str(path):
            assert reads_allowed, 'ordinary Codex hook must not drain the transcript before returning'
        return original_open(value, *args, **kwargs)

    monkeypatch.setattr(builtins, 'open', guarded_open)
    monkeypatch.setattr(bridge, '_automatic', lambda *_args, **_kwargs:
                        {'context': 'Immediate recall [memory:synthetic].', 'shown': ['synthetic']})
    kicked = []
    monkeypatch.setattr(bridge, 'kick', lambda config, paths=(): kicked.append((config, paths)))
    data = {'session_id': 'current', 'hook_event_name': event, 'transcript_path': str(path),
            'prompt': 'Current synthetic query.', 'turn_id': 'current-turn'}
    monkeypatch.setattr(bridge.sys, 'stdin', io.StringIO(json.dumps(data)))
    started = time.monotonic()
    assert bridge.main(['codex', '--config', config_path]) == 0
    assert time.monotonic() - started < 1
    output = capsys.readouterr().out
    if event == 'UserPromptSubmit':
        assert 'Immediate recall [memory:synthetic].' in json.loads(output)['hookSpecificOutput']['additionalContext']
    assert kicked == [(config_path, [str(path)])]
    spool = bridge.Spool(config_path, config)
    assert spool.db.execute('SELECT conversation,offset FROM journals').fetchone() == ('current', 0)
    assert [body['kind'] for body in pending(spool)] == ['native_hook']
    spool.close()

    reads_allowed = True
    uploaded = []
    monkeypatch.setattr(bridge, 'http', lambda _config, endpoint, body, **_kwargs:
                        uploaded.append((endpoint, body)) or {})
    monkeypatch.setattr(bridge.time, 'sleep', lambda *_: None)
    assert bridge.upload(config_path, config, background=True, paths=[str(path)]) == {'pending': 0, 'conflicts': 0}
    assert [body['text'] for endpoint, body in uploaded
            if endpoint == '/episodes' and body['kind'] == 'native_record'] == [raw.decode()]
    assert [body['text'] for endpoint, body in uploaded
            if endpoint == '/episodes' and body['kind'] == 'user'] == [json.loads(raw)['payload']['content']]
    with original_open(path, 'ab') as stream:
        stream.write(tail[20:])
    bridge.upload(config_path, config, background=True, paths=[str(path)])
    assert [body['text'] for endpoint, body in uploaded
            if endpoint == '/episodes' and body['kind'] == 'native_record'] == [raw.decode(), tail.decode()]
    spool = bridge.Spool(config_path, config)
    assert spool.db.execute('SELECT offset FROM journals').fetchone()[0] == path.stat().st_size
    assert spool.db.execute('SELECT COUNT(*) FROM records').fetchone()[0] == 2
    spool.close()


def test_codex_busy_hook_returns_promptly_and_retries_registered_transcript(tmp_path, monkeypatch):
    spool = spool_at(tmp_path, preset='codex')
    config_path, config = spool.config_path, spool.config
    path = tmp_path / 'current.jsonl'
    path.write_bytes(b'')
    spool.journal(path, 'codex', 'current')
    spool.commit()
    spool.close()
    raw = bridge.encode({'type': 'response_item', 'payload': {'type': 'message',
        'role': 'user', 'content': 'Source retained while the writer is busy.'}}) + b'\n'
    path.write_bytes(raw)
    kicked = []
    monkeypatch.setattr(bridge, 'kick', lambda config, paths=(): kicked.append((config, paths)))
    monkeypatch.setattr(bridge.sys, 'stdin', io.StringIO(json.dumps({
        'session_id': 'current', 'hook_event_name': 'Stop', 'transcript_path': str(path)})))
    blocker = bridge.sqlite3.connect(tmp_path / 'runner.bridge.sqlite3')
    try:
        blocker.execute('BEGIN IMMEDIATE')
        started = time.monotonic()
        assert bridge.main(['codex', '--config', config_path]) == 0
        assert time.monotonic() - started < 1
        assert kicked == [(config_path, [str(path)])]
    finally:
        blocker.rollback()
        blocker.close()
    uploaded = []
    monkeypatch.setattr(bridge, 'http', lambda _config, endpoint, body, **_kwargs:
                        uploaded.append((endpoint, body)) or {})
    monkeypatch.setattr(bridge.time, 'sleep', lambda *_: None)
    bridge.upload(config_path, config, background=True, paths=kicked[0][1])
    assert [body['text'] for endpoint, body in uploaded if body.get('kind') == 'native_record'] == [raw.decode()]


@pytest.mark.parametrize('event', ['PreCompact', 'PostCompact', 'SessionStart', 'PreToolUse'])
def test_codex_evidence_and_compaction_hooks_reconcile_before_the_boundary(tmp_path, monkeypatch, event):
    spool = spool_at(tmp_path, preset='codex')
    path = tmp_path / 'current.jsonl'
    quote = 'Exact evidence preceding the boundary.'
    raw = bridge.encode({'type': 'response_item', 'payload': {'type': 'message',
        'role': 'user', 'content': quote}}) + b'\n'
    path.write_bytes(raw)
    uploaded = []
    monkeypatch.setattr(bridge, 'http', lambda _config, endpoint, body, **_kwargs:
                        uploaded.append((endpoint, body)) or {'id': 'acknowledged'})
    result = bridge.hook(spool, 'codex', {'session_id': 'current', 'hook_event_name': event,
        'source': 'compact', 'transcript_path': str(path), 'tool_name': 'mcp__memento__remember',
        'tool_input': {'command': {'source': bridge.source(spool.config, 'codex'),
            'conversation_id': 'current', 'evidence': [{'quote': quote}]}}},
        recall=lambda *_args, **_kwargs: {'context': '', 'shown': []})
    assert spool.db.execute('SELECT offset FROM journals').fetchone()[0] == len(raw)
    if event == 'PreToolUse':
        assert result == {}  # Preserve native approval policy.
        assert [(endpoint, body['text']) for endpoint, body in uploaded] == [('/episodes', quote)]
    else:
        queued = [(endpoint, json.loads(body)) for endpoint, body in
                  spool.db.execute('SELECT endpoint,body FROM outbox ORDER BY id')]
        assert queued[-1][0] == '/work/compaction'
        assert any(body.get('text') == quote for _, body in queued[:-1])
    spool.close()


def test_journal_rotation_with_same_header_and_size_preserves_new_record(tmp_path):
    spool = spool_at(tmp_path)
    path = tmp_path / 'journal.jsonl'
    header = bridge.encode({'type': 'session', 'id': 's'}) + b'\n'
    path.write_bytes(header + bridge.encode({'id': 'a', 'role': 'user', 'content': 'first'}) + b'\n')
    spool.journal(path, 'pi', 's')
    replacement = tmp_path / 'replacement'
    replacement.write_bytes(header + bridge.encode({'id': 'b', 'role': 'user', 'content': 'later'}) + b'\n')
    replacement.replace(path)
    spool.reconcile(str(path))
    assert [part['text'] for part in pending(spool) if part['kind'] == 'user'] == ['first', 'later']
    spool.close()


def test_passive_categories_followup_and_peer_anchor(tmp_path, monkeypatch):
    spool = spool_at(tmp_path)
    requests = []

    def recalled(_config, endpoint, **_kwargs):
        requests.append(bridge.urllib.parse.parse_qs(bridge.urllib.parse.urlsplit(endpoint).query))
        return {'memories': [{'id': 'f', 'text': 'A fact', 'category': 'memory'},
                             {'id': 'b', 'text': 'Use a supported check.', 'category': 'behavioral_suggestion',
                              'authority': 'inferred', 'behavior': {
                                  'applicability': 'When checking a candidate.', 'conduct': 'Use a supported check.',
                                  'exceptions': [], 'rationale': 'Keep evidence faithful.', 'authority': 'inferred'}}]}

    monkeypatch.setattr(bridge, 'http', recalled)
    first = bridge.passive(spool, 's', 'Which checks apply?')
    assert '[fact]' in first['context'] and '[behavioral suggestion;' in first['context']
    assert '[inferred]' in first['context']
    assert bridge.passive(spool, 's', '[HERDR AGENT MESSAGE] random peer result') == first
    assert len(requests) == 1
    bridge.passive(spool, 's', 'And for this project?')
    assert requests[-1]['automatic'] == ['true']
    assert requests[-1]['context'] == ['Which checks apply?']
    spool.close()


def test_legacy_binary_projection_is_never_rewritten_or_duplicated_during_artifact_recovery(tmp_path):
    spool = spool_at(tmp_path)
    data = b'legacy opaque image'
    record = {'type': 'message', 'id': 'u', 'message': {'role': 'user', 'content': [
        {'type': 'image', 'data': base64.b64encode(data).decode(), 'mimeType': 'image/webp'}]}}
    legacy_text = bridge.encode(record['message']['content']).decode()
    spool.episode('omp', 's', 'message:u', 'user', legacy_text,
                  metadata={'native_id': 'u', 'extraction_mode': 'message'})
    spool.db.execute('INSERT INTO projections VALUES(?,?,?)', (bridge.source(spool.config, 'omp'), 's', 'u'))
    path = tmp_path / 'history.jsonl'
    path.write_bytes(bridge.encode({'type': 'session', 'id': 's'}) + b'\n' + bridge.encode(record) + b'\n')
    original = [part for part in pending(spool) if part['kind'] == 'user']
    result = bridge.import_history(spool, 'omp', [path])
    assert [part for part in pending(spool) if part['kind'] == 'user'] == original
    assert original[0]['text'] == legacy_text
    assert result['coverage']['inline_payload_bytes'] == len(data)
    assert spool.db.execute("SELECT body FROM outbox WHERE endpoint='/blobs'").fetchone()[0] == data
    spool.close()


def test_shipped_quote_flush_reconciles_and_uploads_only_selected_conversation(tmp_path, monkeypatch, capsys):
    spool = spool_at(tmp_path)
    for conversation in ('selected', 'other'):
        path = tmp_path / (conversation + '.jsonl')
        path.write_bytes(bridge.encode({'id': 'u', 'role': 'user', 'content': 'exact source'}) + b'\n')
        spool.journal(path, 'omp', conversation)
    config = spool.config_path
    spool.commit()
    spool.close()
    uploaded = []
    monkeypatch.setattr(bridge, 'http', lambda _config, endpoint, body, **_kwargs: uploaded.append((endpoint, body)) or {})
    assert bridge.main(['flush-conversation', '--config', config, '--agent', 'omp',
                        '--conversation-id', 'selected']) == 0
    assert json.loads(capsys.readouterr().out) == {'status': 'uploaded'}
    assert uploaded and all(body['conversation_id'] == 'selected' for _, body in uploaded)
    spool = spool_at(tmp_path)
    assert pending(spool) and all(part['conversation_id'] == 'other' for part in pending(spool))
    spool.close()


def test_shipped_flush_repeatable_quotes_reports_only_actual_acknowledgements(tmp_path, monkeypatch, capsys):
    spool = spool_at(tmp_path)
    path = tmp_path / 'current.jsonl'
    path.write_bytes(bridge.encode({'id': 'first', 'role': 'user', 'content': 'First exact request.'}) + b'\n')
    spool.journal(path, 'omp', 'current')
    config = spool.config_path
    spool.commit()
    spool.close()
    with path.open('ab') as stream:
        stream.write(bridge.encode({'id': 'second', 'role': 'assistant', 'content': 'Second exact statement.'}) + b'\n')
    uploaded = []
    monkeypatch.setattr(bridge, 'http', lambda _config, endpoint, body, **_kwargs:
                        uploaded.append((endpoint, body)) or {'id': 'ack-' + str(len(uploaded))})
    args = ['flush-conversation', '--config', config, '--agent', 'omp', '--conversation-id', 'current',
            '--quote', 'First exact request.', '--quote', 'Second exact statement.', '--quote', 'Absent locally.']
    assert bridge.main(args) == 0
    assert json.loads(capsys.readouterr().out) == {
        'status': 'partial', 'evidence': [
            {'quote_index': index, 'event_id': body['event_id'], 'episode_id': 'ack-' + str(index + 1)}
            for index, (_, body) in enumerate(uploaded)], 'unmatched_quotes': [2]}
    assert [body['kind'] for _, body in uploaded] == ['user', 'assistant']
    assert bridge.main(args) == 0
    assert json.loads(capsys.readouterr().out) == {'status': 'not_found', 'evidence': [], 'unmatched_quotes': [0, 1, 2]}
    spool = spool_at(tmp_path)
    assert [body['kind'] for body in pending(spool)] == ['native_record', 'native_record']
    spool.close()


def test_slow_http_capture_is_retained_by_background_then_explicit_retry_acknowledges_same_event(tmp_path):
    from memento.store import Store

    store = Store(tmp_path / 'service')
    accepted = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            assert self.path == '/episodes'
            payload = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            episode, created = store.capture(payload)
            accepted.append((episode['id'], created))
            time.sleep(2.1)
            try:
                body = bridge.encode({'id': episode['id']})
                self.send_response(201 if created else 200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except OSError:
                pass  # The background caller has already timed out.

        def log_message(self, *_args):
            pass

    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .05}, daemon=True)
    thread.start()
    try:
        spool = spool_at(tmp_path, url=f'http://127.0.0.1:{server.server_port}')
        config, config_path = spool.config, spool.config_path
        spool.episode('omp', 'conversation', 'stable-event', 'user', 'Synthetic slow capture. ' * 6000)
        spool.commit()
        original = pending(spool)
        spool.close()
        result = bridge.upload(config_path, config, background=True)
        assert result['pending'] == 1 and result['conflicts'] == 0
        assert result['error']['category'] == 'timeout'
        spool = bridge.Spool(config_path, config)
        assert pending(spool) == original
        spool.close()
        started = time.monotonic()
        assert bridge.upload(config_path, config) == {'pending': 0, 'conflicts': 0}
        assert time.monotonic() - started >= 2
        assert len(accepted) == 2 and accepted[0][0] == accepted[1][0]
        assert [created for _, created in accepted] == [True, False]
        assert store._db.execute('SELECT count(*) FROM episodes').fetchone()[0] == 1
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
        store.close()


@pytest.mark.parametrize('background', [False, True])
def test_slow_blob_retains_original_while_later_text_uploads_and_retry_resumes(tmp_path, monkeypatch, background):
    spool = spool_at(tmp_path)
    config = spool.config
    config_path = spool.config_path
    original = b'opaque-original' * (8 * bridge.MAX_BODY // 15)
    spool.db.execute('INSERT INTO outbox(key,endpoint,body,content_type) VALUES(?,?,?,?)',
                     ('blob:oldest', '/blobs', original, 'image/webp'))
    spool.episode('omp', 's', 'queued-after-blob', 'user', 'text queued after the old blob')
    spool.commit()
    spool.close()
    elapsed = [0.0]
    attempts = []
    uploaded = []
    monkeypatch.setattr(bridge.time, 'monotonic', lambda: elapsed[0])
    monkeypatch.setattr(bridge.time, 'sleep', lambda seconds: elapsed.__setitem__(0, elapsed[0] + seconds))

    def slow(_config, endpoint, body, *, timeout, **_kwargs):
        if endpoint == '/blobs':
            attempts.append(timeout)
            assert timeout > 2
            if background:
                assert timeout <= 10
            elapsed[0] += timeout
            concurrent = bridge.Spool(config_path, config)
            concurrent.episode('omp', 's', 'during-blob', 'user', 'text captured during slow upload')
            concurrent.commit()
            concurrent.close()
            raise TimeoutError('synthetic slow artifact')
        uploaded.append(body['text'])
        return {}

    monkeypatch.setattr(bridge, 'http', slow)
    result = bridge.upload(config_path, config, background=background)
    assert {key: result[key] for key in ('pending', 'conflicts')} == {'pending': 1, 'conflicts': 0}
    assert result['error']['category'] == 'timeout'
    assert len(attempts) == 1
    assert uploaded == ['text queued after the old blob', 'text captured during slow upload']
    spool = spool_at(tmp_path)
    assert spool.db.execute('SELECT body FROM outbox').fetchall() == [(original,)]
    spool.close()
    retried = []
    monkeypatch.setattr(bridge, 'http', lambda _config, endpoint, body, **_kwargs: retried.append(body) or {})
    assert bridge.upload(config_path, config) == {'pending': 0, 'conflicts': 0}
    assert retried == [original]
