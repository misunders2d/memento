import json
import base64
import hashlib
import http.server
import io
import os
import sys
import threading
import time
from pathlib import Path

import pytest

from memento import bridge, runner


def native_config(path, agent='pi', mode='client'):
    value = {'uid': os.getuid(), 'client_uid': 'device-' + agent, 'agent': agent, 'preset': agent,
             'command': [agent], 'mode': mode, 'timezone': 'Etc/UTC',
             'url': 'http://127.0.0.1:1', 'deadline_seconds': 600}
    runner.private_write(path, runner.encode(value))
    return value


def test_status_uses_real_bridge_subprocess_without_adding_processing_guard(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv('MEMENTO_PROCESSING', raising=False)
    config = tmp_path / 'runner.json'
    value = native_config(config, 'omp')
    spool = bridge.Spool(config, value)
    spool.episode('omp', 's', 'u', 'user', 'still queued')
    spool.commit()
    spool.close()

    class Service:
        def __init__(self, _url):
            pass

        def get(self, endpoint):
            source = runner.urllib.parse.parse_qs(runner.urllib.parse.urlsplit(endpoint).query)['source'][0]
            return {'source': source, 'media': {'pending_jobs': int(source.startswith('artifact:'))}}

    monkeypatch.setattr(runner, 'Service', Service)
    assert runner.main(['status', '--config', str(config)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result['local']['pending'] == 1
    assert result['service']['media']['pending_jobs'] == 0
    assert result['artifacts_service']['media']['pending_jobs'] == 1
    assert 'MEMENTO_PROCESSING' not in os.environ


def test_quote_save_flushes_real_bridge_subprocess_before_media_submit(tmp_path, monkeypatch, capsys, media_http):
    monkeypatch.delenv('MEMENTO_PROCESSING', raising=False)
    url, state = media_http
    config = tmp_path / 'runner.json'
    value = {**native_config(config, 'omp'), 'url': url}
    runner.private_write(config, runner.encode(value))
    spool = bridge.Spool(config, value)
    spool.record('omp', 'conversation', {'id': 'u', 'type': 'message',
                                       'message': {'role': 'user', 'content': 'Exact native request.'}})
    spool.commit()
    spool.close()
    file = tmp_path / 'original.webp'
    file.write_bytes(b'original opaque bytes')
    assert runner.main(['media', 'save', str(file), '--config', str(config), '--scope', 'personal',
                        '--quote', 'Exact native request.', '--conversation-id', 'conversation']) == 0
    assert [endpoint for endpoint, _ in state['posts']] == ['/episodes', '/media']
    assert state['posts'][0][1]['kind'] == 'user'
    assert state['posts'][0][1]['text'] == 'Exact native request.'
    assert state['posts'][1][1]['evidence'] == [{'quote': 'Exact native request.'}]
    assert json.loads(capsys.readouterr().out)['job_id'] == 'job'
    spool = bridge.Spool(config, value)
    assert spool.status()['pending'] == 1
    assert json.loads(spool.db.execute('SELECT body FROM outbox').fetchone()[0])['kind'] == 'native_record'
    spool.close()


@pytest.mark.parametrize('arguments', [
    ['setup', '--agent', 'pi'], ['status'], ['upload'], ['media', 'status', 'job'],
    ['export', '--data-dir', '/private/data', '--output', '/private/archive'],
    ['restore', '--archive', '/private/archive', '--destination', '/private/empty'],
    ['--config', '/private/config', 'status'],
])
def test_packaged_frontdoor_routes_supported_operations(arguments, monkeypatch):
    from memento import __main__
    calls = []
    monkeypatch.setattr(runner, 'main', lambda argv: calls.append(argv) or 23)
    assert __main__.main(arguments) == 23
    assert calls == [arguments]


def test_status_selects_harness_config_and_reports_stages_without_claiming_completion(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path))
    host = runner.default_config()
    original = native_config(host, mode='server')
    path = host.parent / 'codex' / 'runner.json'
    native_config(path, agent='codex')
    observed = []
    service_status = {'captured': {'episodes': 7}, 'indexing': {'available': False},
                      'extraction': {'pending_conversations': 2}, 'consolidation': {'pending_jobs': 1}}

    class Service:
        def __init__(self, _url):
            pass

        def get(self, endpoint):
            observed.append(endpoint)
            return service_status

    monkeypatch.setattr(runner, 'Service', Service)
    monkeypatch.setattr(runner, 'bridge_command', lambda config, operation, **kw: {
        'pending': 3, 'conflicts': 0, 'captured_records': 9})
    assert runner.main(['status', '--agent', 'codex']) == 0
    result = json.loads(capsys.readouterr().out)
    assert result['config'] == str(path) and result['mode'] == 'client'
    assert result['local']['pending'] == 3 and result['service'] == service_status
    assert result['artifacts_service'] == service_status
    assert observed == ['/status?source=native%3Adevice-codex%3Acodex',
                        '/status?source=artifact%3Adevice-codex%3Acodex']
    assert runner.load_config(host) == original


def test_explicit_config_ignores_unrelated_invalid_default(tmp_path, monkeypatch):
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'xdg'))
    host = runner.default_config()
    host.parent.mkdir(parents=True)
    host.write_text('invalid unrelated configuration')
    path = tmp_path / 'private' / 'runner.json'
    expected = native_config(path, 'claude')
    args = type('Args', (), {'config': path, 'agent': 'claude'})()
    assert runner.selected_config(args) == (path, expected)


def test_frontdoor_import_reuses_shipped_bridge_and_explicit_artifact_roots(tmp_path, monkeypatch):
    path = tmp_path / 'runner.json'
    native_config(path, 'omp')
    observed = []
    monkeypatch.setattr(runner.subprocess, 'call', lambda argv: observed.append(argv) or 0)
    history = tmp_path / 'sessions'
    blobs = tmp_path / 'blobs'
    assert runner.main(['import-history', str(history), '--agent', 'omp', '--config', str(path),
                        '--artifact-root', str(blobs)]) == 0
    assert observed == [[sys.executable, str(Path(bridge.__file__)), 'import-history', '--config', str(path),
                         '--agent', 'omp', '--artifact-root', str(blobs), str(history)]]


@pytest.fixture
def media_http():
    state = {'posts': [], 'bytes': None}

    class Handler(http.server.BaseHTTPRequestHandler):
        def respond(self, value, content_type='application/json'):
            body = value if isinstance(value, bytes) else runner.encode(value)
            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            body = self.rfile.read(int(self.headers['Content-Length']))
            if self.path == '/blobs':
                state['bytes'] = body
                state['mime'] = self.headers['Content-Type']
                self.respond({'sha256': hashlib.sha256(body).hexdigest(), 'size': len(body)})
            else:
                value = json.loads(body)
                state['posts'].append((self.path, value))
                self.respond({'id': 'artifact-episode'} if self.path == '/episodes' else {
                    'job_id': 'job', 'status': 'accepted', 'status_url': '/jobs/job'})

        def do_GET(self):
            if self.path.startswith('/blobs/'):
                self.respond(state['bytes'], state['mime'])
            else:
                self.respond({'id': 'job', 'kind': 'media', 'state': 'done',
                              'result': {'id': 'memory', 'blob_url': '/blobs/' + hashlib.sha256(state['bytes']).hexdigest()}})

        def log_message(self, *_args):
            pass

    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield 'http://127.0.0.1:' + str(server.server_port), state
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_media_frontdoor_streams_original_and_uses_honest_artifact_provenance(tmp_path, monkeypatch, capsys, media_http):
    url, state = media_http
    config = tmp_path / 'runner.json'
    value = native_config(config, 'omp')
    runner.private_write(config, runner.encode({**value, 'url': url}))
    file = tmp_path / 'photo.webp'
    file.write_bytes(b'\x00original photo\xff' * 1000)
    arguments = ['--config', str(config)]
    assert runner.main(arguments + ['media', 'save', str(file), '--scope', 'personal']) == 0
    result = json.loads(capsys.readouterr().out)
    assert state['bytes'] == file.read_bytes() and state['mime'] == 'image/webp'
    endpoint, origin = state['posts'][0]
    assert endpoint == '/episodes' and origin['kind'] == 'artifact'
    assert origin['source'].startswith('artifact:') and origin['metadata']['extraction_mode'] == 'skip'
    command = state['posts'][1][1]
    assert command['evidence'] == [{'episode_id': 'artifact-episode', 'quote': 'Artifact filename: photo.webp'}]
    assert result['indexing'] == 'done' and result['job_id'] == 'job'
    output = tmp_path / 'retrieved.webp'
    assert runner.main(arguments + ['media', 'get', result['sha256'], '--output', str(output)]) == 0
    assert output.read_bytes() == file.read_bytes() and output.stat().st_mode & 0o077 == 0
    assert runner.main(arguments + ['media', 'get', result['sha256'], '--output', str(output)]) == 1
    capsys.readouterr()
    assert runner.main(arguments + ['media', 'status', 'job']) == 0
    assert json.loads(capsys.readouterr().out)['state'] == 'done'


def test_owner_frontdoor_preserves_interactive_export_gate(tmp_path):
    output = tmp_path / 'archive.tar'
    assert runner.main(['export', '--data-dir', str(tmp_path), '--output', str(output)]) != 0
    assert not output.exists()


def test_media_quote_flushes_capture_and_distinct_provenance_is_not_a_retry(tmp_path, monkeypatch, capsys, media_http):
    url, state = media_http
    config = tmp_path / 'runner.json'
    value = native_config(config, 'omp')
    runner.private_write(config, runner.encode({**value, 'url': url}))
    file = tmp_path / 'photo.webp'
    file.write_bytes(b'original bytes')
    flushed = []
    monkeypatch.setattr(runner, 'bridge_command', lambda *args, **kwargs: flushed.append((args, kwargs)) or {
        'status': 'uploaded'})
    identities = []
    for quote, conversation in (('Exact first request.', 'one'), ('Exact first request.', 'one'),
                                ('Exact second request.', 'one'), ('Exact first request.', 'two')):
        assert runner.main(['--config', str(config), 'media', 'save', str(file), '--scope', 'personal',
                            '--quote', quote, '--conversation-id', conversation]) == 0
        identities.append(json.loads(capsys.readouterr().out)['command_id'])
    assert identities[0] == identities[1]
    assert len(set(identities)) == 3
    assert len(flushed) == 4 and all(call[0][1] == 'flush-conversation' for call in flushed)
    assert [call[0][2] for call in flushed] == [
        ['--agent', 'omp', '--conversation-id', conversation, '--quote', quote]
        for quote, conversation in (('Exact first request.', 'one'), ('Exact first request.', 'one'),
                                    ('Exact second request.', 'one'), ('Exact first request.', 'two'))]
    assert all(endpoint == '/media' for endpoint, _ in state['posts'])
    assert state['posts'][0][1]['source'] == 'native:device-omp:omp'
    assert state['posts'][0][1]['evidence'] == [{'quote': 'Exact first request.'}]


def assistant_events(text='{"facts":[]}', **changes):
    message = dict(role='assistant', stopReason='stop', model='native-default',
                   usage={'input': 12, 'output': 3}, content=[{'type': 'text', 'text': text}])
    message.update(changes)
    return runner.encode({'type': 'message_end', 'message': message}) + b'\n'


def test_native_result_and_metadata_are_parsed_without_executing_text():
    output, model, usage = runner.parse_result(assistant_events(), 'pi')
    assert output == {'facts': []}
    assert model == 'native-default'
    assert usage == {'input': 12, 'output': 3}
    value = {'subtype': 'success', 'is_error': False, 'result': '{"facts":[]}',
             'modelUsage': {'claude-native': {}}, 'usage': {'output_tokens': 3}}
    assert runner.parse_result(runner.encode(value), 'claude') == (
        {'facts': []}, 'claude-native', {'output_tokens': 3})


def test_agy_server_processing_stays_refused_before_native_execution(monkeypatch):
    def forbidden(_command):
        pytest.fail('unqualified AGY production setup must not invoke the native CLI')
    monkeypatch.setattr(runner.shutil, 'which', forbidden)
    with pytest.raises(runner.RunnerError, match='AGY tools-zero isolation unverified'):
        runner.check_cli(['agy'], 'agy')


@pytest.mark.parametrize('raw', [
    assistant_events('private evidence is not JSON'),
    assistant_events(stopReason='length'),
    assistant_events(content=[{'type': 'toolCall', 'name': 'bash'}]),
    assistant_events('{"bad":NaN}'),
    b'{"type":"tool_execution_start"}\n' + assistant_events(),
])
def test_invalid_native_output_has_only_fixed_error(raw):
    with pytest.raises(runner.RunnerError, match='^invalid_output$'):
        runner.parse_result(raw, 'omp')


def test_custom_arguments_cannot_override_locked_native_flags():
    with pytest.raises(runner.RunnerError):
        runner.check_cli(['pi', '--tools', 'bash'], 'pi')


def test_missing_safety_capability_is_fatal(monkeypatch):
    monkeypatch.setattr(runner.shutil, 'which', lambda _: '/native/pi')
    monkeypatch.setattr(runner, 'run_process', lambda *a, **k: b'--print --no-tools')
    with pytest.raises(runner.RunnerError):
        runner.check_cli(['pi'], 'pi')


def test_codex_cannot_substitute_readonly_sandbox_for_tools_disabled(tmp_path):
    with pytest.raises(runner.RunnerError):
        runner.preset_flags('codex', tmp_path)


def test_claude_natively_validates_consolidation_but_not_extraction(monkeypatch):
    from pydantic import ValidationError
    from memento.consolidation import Plan
    from memento.extraction import BatchDrafts
    schema = Plan.model_json_schema()
    action = dict(candidate_id='c', operation='reject', type=None, text=None, scope=None, open_loop=None,
                  target=None, src=None, dst=None, relation=None, change_kind=None, valid_at=None,
                  valid_from=None, valid_to=None, category=None, sensitive=None, why='unsupported',
                  evidence=[{'episode_id': 'e', 'quote': 'q', 'occurrence': 0, 'occurred_at': '2026-10-23T00:00:00Z'}])
    # Observed native output copied a window field into evidence; the server still rejects it.
    assert schema['$defs']['Source']['additionalProperties'] is False
    with pytest.raises(ValidationError):
        Plan.model_validate({'actions': [action]})
    plan = {'actions': [{**action, 'evidence': []}]}
    # Claude's strict validator rejects the extraction schema's discriminator keyword.
    extract = {'id': 'extract', 'owner': 'lease', 'kind': 'extract', 'instruction': 'private',
               'input': {'text': 'secret evidence'}, 'schema': BatchDrafts.model_json_schema()}
    item = {'id': 'work', 'owner': 'lease', 'kind': 'consolidate', 'instruction': 'private',
            'input': {'text': 'secret evidence'}, 'schema': schema}
    observed, completed = [], []

    class Service:
        def __init__(self, *args, **kwargs):
            pass

        def post(self, path, value):
            if path == '/work/complete':
                completed.append(value)
            return {'renewed': True, 'released': True}

    def generate(argv, payload, **_kwargs):
        observed.append((argv, runner.decode(payload)))
        if '--json-schema' in argv:
            return runner.encode({'subtype': 'success', 'is_error': False, 'result': '',
                                  'structured_output': plan, 'modelUsage': {'claude-native': {}}})
        return runner.encode({'subtype': 'success', 'is_error': False, 'result': '{"windows":[]}',
                              'modelUsage': {'claude-native': {}}})

    monkeypatch.setattr(runner, 'Service', Service)
    monkeypatch.setattr(runner, 'run_process', generate)
    config = {'preset': 'claude', 'agent': 'claude', 'url': 'http://127.0.0.1:8765', 'deadline_seconds': 2}
    assert runner.process_work(config, Service(), [extract, item], 60, ['/native/claude']) == (0, ['/native/claude'])
    # Extraction and consolidation may finish in either order.
    (original, _), = [entry for entry in observed if '--json-schema' not in entry[0]]
    (argv, payload), = [entry for entry in observed if '--json-schema' in entry[0]]
    assert argv[argv.index('--json-schema') + 1] == runner.encode(schema).decode('utf-8')
    for flags in (original, argv):
        assert flags[flags.index('--tools') + 1] == '' and '--safe-mode' in flags and '--permission-prompts' in flags
        assert 'secret evidence' not in ' '.join(flags)
    assert payload['input'] == item['input']
    outputs = {value['id']: value for value in completed}
    assert outputs['extract']['output'] == {'windows': []} and outputs['work']['output'] == plan
    assert outputs['work']['model'] == 'claude-native'


def test_claude_without_native_schema_validation_is_fatal(monkeypatch, tmp_path):
    flags = [flag for flag in runner.preset_flags('claude', tmp_path) if flag.startswith('--')]
    assert '--json-schema' not in flags
    monkeypatch.setattr(runner.shutil, 'which', lambda _: '/native/claude')
    monkeypatch.setattr(runner, 'run_process', lambda *a, **k: ' '.join(flags).encode())
    with pytest.raises(runner.RunnerError):
        runner.check_cli(['claude'], 'claude')
    monkeypatch.setattr(runner, 'run_process', lambda *a, **k: ' '.join(flags + ['--json-schema']).encode())
    assert runner.check_cli(['claude'], 'claude') == ['/native/claude']


def test_subprocess_deadline_output_limit_and_no_stderr_disclosure():
    with pytest.raises(runner.RunnerError, match='^cli_failed$'):
        runner.run_process([sys.executable, '-c', 'import time; time.sleep(60)'], timeout=.1)
    with pytest.raises(runner.RunnerError, match='^cli_failed$'):
        runner.run_process([sys.executable, '-c', 'print("x" * 20000)'], limit=100)
    with pytest.raises(runner.RunnerError, match='^cli_failed$'):
        runner.run_process([sys.executable, '-c', 'import sys; sys.stderr.write("secret"); sys.exit(1)'])


def test_subprocess_payload_is_stdin_data_not_argv(tmp_path):
    payload = b'$(touch SHOULD_NOT_EXIST); @secret --tools bash\n'
    result = runner.run_process([sys.executable, '-c',
                                 'import sys; sys.stdout.buffer.write(sys.stdin.buffer.read())'],
                                payload, cwd=tmp_path)
    assert result == payload
    assert not (tmp_path / 'SHOULD_NOT_EXIST').exists()


def test_worker_subprocesses_are_excluded_from_native_capture(monkeypatch):
    monkeypatch.delenv('MEMENTO_PROCESSING', raising=False)
    assert runner.run_process([sys.executable, '-c',
                               'import os; print(os.environ["MEMENTO_PROCESSING"])']).splitlines() == [b'1']
    assert 'MEMENTO_PROCESSING' not in os.environ
    monkeypatch.setenv('MEMENTO_PROCESSING', '1')
    assert runner.run_process([sys.executable, '-c',
                               'import os; print(os.environ["MEMENTO_PROCESSING"])'],
                              processing=False).splitlines() == [b'1']


def test_heartbeat_renews_all_claimed_work_and_marks_lost():
    calls = []
    renewed = threading.Event()

    class Service:
        def post(self, path, value):
            calls.append((path, value))
            if len(calls) >= 2:
                renewed.set()
            return {'renewed': value['id'] != 'two'}

    leases = runner.Leases(Service(), [{'id': 'one', 'owner': 'a'}, {'id': 'two', 'owner': 'b'}], .08)
    leases.thread.start()
    try:
        assert renewed.wait(2)
    finally:
        leases.stop.set()
        leases.thread.join()
    assert {value['id'] for _, value in calls} == {'one', 'two'}
    assert leases.is_lost('two')
    assert not leases.is_lost('one')


@pytest.mark.parametrize('failure,status,body,diagnostic', [
    ('http', 422, runner.encode({'detail': 'consolidation_candidate_coverage_invalid', 'private': 'secret'}),
     {'http_status': 422, 'detail_code': 'consolidation_candidate_coverage_invalid'}),
    ('http', 422, runner.encode({'detail': 'candidate_quote_ambiguous'}).ljust(1024, b' '),
     {'http_status': 422, 'detail_code': 'candidate_quote_ambiguous'}),
    pytest.param('http', 422, runner.encode({'detail': 'candidate_window_coverage_invalid'}),
        {'http_status': 422, 'detail_code': 'candidate_window_coverage_invalid'}, id='extraction-window-coverage'),
    pytest.param('http', 422, runner.encode({'detail': 'World time must be an aware ISO timestamp'}),
        {'http_status': 422, 'detail_code': 'World time must be an aware ISO timestamp'}, id='extraction-world-time'),
    pytest.param('http', 422, runner.encode({'detail': 'consolidation_loop_target_not_supplied'}),
        {'http_status': 422, 'detail_code': 'consolidation_loop_target_not_supplied'}, id='consolidation-loop-target'),
    pytest.param('http', 422, runner.encode({'detail': 'consolidation_loop_state_contradictory'}),
        {'http_status': 422, 'detail_code': 'consolidation_loop_state_contradictory'}, id='consolidation-loop-state'),
    pytest.param('http', 422, runner.encode({'detail': 'Invalid reviewed type definition'}),
        {'http_status': 422, 'detail_code': 'Invalid reviewed type definition'}, id='consolidation-reviewed-type'),
    pytest.param('http', 422, runner.encode({'detail': 'Merge requires distinct active entities of the same type'}),
        {'http_status': 422, 'detail_code': 'Merge requires distinct active entities of the same type'}, id='consolidation-merge-type'),
    pytest.param('http', 422, runner.encode({'detail': 'Relations require active endpoints'}),
        {'http_status': 422, 'detail_code': 'Relations require active endpoints'}, id='consolidation-active-endpoints'),
    pytest.param('http', 422, runner.encode({'detail': 'Supersession requires an active node'}),
        {'http_status': 422, 'detail_code': 'Supersession requires an active node'}, id='consolidation-active-node'),
    pytest.param('http', 422, runner.encode({'detail': 'Change must follow the previous valid_from'}),
        {'http_status': 422, 'detail_code': 'Change must follow the previous valid_from'}, id='consolidation-change-time'),
    pytest.param('http', 422, runner.encode({'detail': 'Loop responsibility requires eligible original evidence'}),
        {'http_status': 422, 'detail_code': 'Loop responsibility requires eligible original evidence'}, id='loop-role-source'),
    pytest.param('http', 422, runner.encode({'detail': 'Owner responsibility requires original native user evidence'}),
        {'http_status': 422, 'detail_code': 'Owner responsibility requires original native user evidence'}, id='loop-owner-source'),
    pytest.param('http', 422, runner.encode({'detail': 'Missing assertion commit'}),
        {'http_status': 422, 'detail_code': 'Missing assertion commit'}, id='loop-commit-order'),
    pytest.param('http', 422, runner.encode({'detail': 'Unknown node: secret-node'}),
        {'http_status': 422}, id='dynamic-node-suppressed'),
    ('http', 422, runner.encode({'detail': 'secret private evidence'}), {'http_status': 422}),
    ('http', 422, runner.encode({'detail': 'consolidation_schema_invalid_secret'}), {'http_status': 422}),
    ('http', 409, runner.encode({'detail': 'secret conflict'}), {'http_status': 409}),
    ('timeout', None, b'', {'cause': 'timeout'}),
    ('wrapped_timeout', None, b'', {'cause': 'timeout'}),
    ('transport', None, b'', {'cause': 'transport_error'}),
    ('invalid_response', None, b'secret invalid JSON', {'cause': 'invalid_response'}),
    ('http', 422, b'{"detail": secret malformed JSON', {'http_status': 422}),
    ('http', 422, runner.encode({'detail': [{'msg': 'secret'}]}), {'http_status': 422}),
    ('http', 422, runner.encode({'detail': 'consolidation_schema_invalid'}).ljust(1025, b' '),
     {'http_status': 422}),
    ('body_read_failure', 422, b'', {'http_status': 422}),
    ('broken_stderr', 422, runner.encode({'detail': 'consolidation_schema_invalid'}), None),
])
def test_drain_excludes_failures_without_retrying_or_disclosing_text(
        monkeypatch, capsys, failure, status, body, diagnostic):
    calls = []
    reads, leases = [], set()
    backlog = {'pending_extract_conversations': 1, 'pending_extract_jobs': 1, 'leased_extract_jobs': 0}
    item = {'id': 'work', 'owner': 'lease', 'kind': 'extract', 'instruction': 'private',
            'input': {'text': 'secret'}, 'schema': {'type': 'object'}}

    class ErrorBody(io.BytesIO):
        def read(self, size=-1):
            reads.append(size)
            if failure == 'body_read_failure':
                raise TimeoutError('secret body timeout')
            return super().read(size)

    error_body = ErrorBody(body)

    class Opener:
        def open(self, *_args, **_kwargs):
            if failure == 'timeout':
                raise TimeoutError('secret timeout')
            if failure == 'wrapped_timeout':
                raise runner.urllib.error.URLError(TimeoutError('secret wrapped timeout'))
            if failure == 'transport':
                raise runner.urllib.error.URLError('secret transport')
            if failure == 'invalid_response':
                return io.BytesIO(body)
            raise runner.urllib.error.HTTPError('https://private.invalid/secret', status,
                'secret status text', {'X-Private': 'secret'}, error_body)

    class Service(runner.Service):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.opener = Opener()

        def post(self, path, value):
            calls.append((path, value))
            if path == '/work/claim':
                work = [] if item['id'] in value['exclude'] else [item]
                leases.update(entry['id'] for entry in work)
                return {'lease_seconds': 60, 'work': work, 'backlog': backlog}
            if path == '/work/complete':
                return super().post(path, value)
            if path == '/work/release':
                leases.remove(value['id'])
            return {'released': True, 'renewed': True}

    monkeypatch.setattr(runner, 'Service', Service)
    monkeypatch.setattr(runner, 'check_cli', lambda *a: ['/native/pi'])
    monkeypatch.setattr(runner, 'run_process', lambda *a, **k: assistant_events())
    config = {'preset': 'pi', 'agent': 'pi', 'command': ['pi'], 'timezone': 'Etc/UTC',
              'url': 'http://127.0.0.1:8765', 'deadline_seconds': 2, 'mode': 'server'}
    compaction = {'source': 'pi', 'conversation_id': 'conversation'}
    with monkeypatch.context() as logging_patch:
        if failure == 'broken_stderr':
            class BrokenStderr:
                def write(self, _value):
                    raise BrokenPipeError('secret broken stderr')
            logging_patch.setattr(runner.sys, 'stderr', BrokenStderr())
        assert runner.tick(config, compaction) == 1
    assert sum(path == '/work/claim' for path, _ in calls) == 2
    assert [value['exclude'] for path, value in calls if path == '/work/claim'] == [[], ['work']]
    assert calls[0][1]['compaction'] == compaction
    release = next(value for path, value in calls if path == '/work/release')
    assert release == {'id': 'work', 'owner': 'lease', 'error': 'service_rejected'}
    assert not leases
    captured = capsys.readouterr()
    assert [json.loads(line) for line in captured.out.splitlines()] == [{'backlog': backlog}] * 2
    receipts = [json.loads(line) for line in captured.err.splitlines()]
    assert receipts == ([] if diagnostic is None else [
        {'id': 'work', 'kind': 'extract', 'error': 'service_rejected', **diagnostic}])
    assert 'secret' not in captured.out + captured.err + json.dumps(calls)
    if status is not None:
        assert reads == [1025]
        assert error_body.closed
    else:
        assert not reads


def test_drain_retries_same_failed_id_after_healthy_work_passes_retry_interval(monkeypatch):
    clock = [100.0]
    claims, completed, releases = [], [], []
    pending = {'A', 'B'}
    active, attempts = {}, {'A': 0, 'B': 0}

    class Service:
        def __init__(self, *args, **kwargs):
            pass

        def post(self, path, value):
            if path == '/work/claim':
                claims.append(value['exclude'])
                ident = next((ident for ident in sorted(pending) if ident not in value['exclude']), None)
                if ident is None:
                    return {'lease_seconds': 60, 'work': []}
                attempts[ident] += 1
                owner = f'{ident}-{attempts[ident]}'
                active[ident] = owner
                item = {'id': ident, 'owner': owner,
                        'kind': 'consolidate' if ident == 'A' else 'extract',
                        'instruction': 'fixture', 'input': {'id': ident}, 'schema': {'type': 'object'}}
                return {'lease_seconds': 60, 'work': [item]}
            if path == '/work/complete':
                ident = value['id']
                assert active[ident] == value['owner']
                if ident == 'A' and attempts[ident] == 1:
                    raise runner.RunnerError('service_rejected')
                completed.append(ident)
                pending.remove(ident)
                active.pop(ident)
                return {}
            if path == '/work/release':
                releases.append(value)
                assert active.pop(value['id']) == value['owner']
                return {'released': True}
            assert path == '/work/renew'
            return {'renewed': active.get(value['id']) == value['owner']}

    def generate(_argv, payload, **_kwargs):
        ident = runner.decode(payload)['input']['id']
        if ident == 'B' or attempts[ident] == 1:
            # Even a slow failed A must be excluded after its batch finishes.
            clock[0] += runner.RETRY_SECONDS + 1
        return assistant_events()

    monkeypatch.setattr(runner, 'Service', Service)
    monkeypatch.setattr(runner, 'check_cli', lambda *a: ['/native/pi'])
    monkeypatch.setattr(runner, 'run_process', generate)
    monkeypatch.setattr(runner.time, 'monotonic', lambda: clock[0])
    config = {'preset': 'pi', 'agent': 'pi', 'command': ['pi'], 'timezone': 'Etc/UTC',
              'url': 'http://127.0.0.1:8765', 'deadline_seconds': 2, 'mode': 'server'}

    assert runner.tick(config) == 1
    assert claims == [[], ['A'], ['B'], ['A', 'B']]
    assert completed == ['B', 'A']
    assert attempts == {'A': 2, 'B': 1}
    assert releases == [{'id': 'A', 'owner': 'A-1', 'error': 'service_rejected'}]
    assert not active and not pending


def test_only_claim_requests_get_the_longer_background_allowance(monkeypatch):
    posts, created, lock = [], [], threading.Lock()
    items = overlap_items()
    claimed = []

    class Service(runner.Service):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            created.append(self.timeout)

        def post(self, path, value):
            with lock:
                posts.append((self.timeout, path))
            if path == '/work/claim':
                work = [] if claimed else list(items)
                claimed.extend(item['id'] for item in work)
                return {'lease_seconds': 60, 'work': work}
            return {'renewed': True, 'released': True}

    monkeypatch.setattr(runner, 'Service', Service)
    monkeypatch.setattr(runner, 'check_cli', lambda *a: ['/native/pi'])
    monkeypatch.setattr(runner, 'run_process', lambda *a, **k: assistant_events())
    assert runner.tick(OVERLAP_CONFIG) == 0
    by_path = {}
    for timeout, path in posts:
        by_path.setdefault(path, set()).add(timeout)
    assert runner.CLAIM_SECONDS == 60 and by_path['/work/claim'] == {60}
    assert by_path['/work/complete'] == {15}
    assert '/work/release' not in by_path
    # Foreground, claim, one heartbeat per lane (min(15, 60 / (8 * 1 item))) and second-lane clients.
    assert sorted(created) == [7.5, 7.5, 15, 15, 60]


@pytest.mark.parametrize('failure,status,body,diagnostic', [
    ('timeout', None, b'', {'cause': 'timeout'}),
    ('wrapped_timeout', None, b'', {'cause': 'timeout'}),
    ('transport', None, b'', {'cause': 'transport_error'}),
    ('invalid_response', None, b'secret invalid JSON', {'cause': 'invalid_response'}),
    ('http', 422, runner.encode({'detail': 'timezone must be an installed IANA timezone name secret'}),
     {'http_status': 422}),
    ('http', 503, b'secret upstream page', {'http_status': 503}),
    ('http', 422, runner.encode({'detail': 'candidate_quote_ambiguous'}),
     {'http_status': 422, 'detail_code': 'candidate_quote_ambiguous'}),
    ('broken_stderr', 503, b'', None),
])
def test_failed_claim_reports_safe_cause_without_changing_exit(
        tmp_path, monkeypatch, capsys, failure, status, body, diagnostic):
    opened = []

    class Opener:
        def open(self, request, *, timeout):
            opened.append((request.full_url, timeout))
            if failure == 'timeout':
                raise TimeoutError('secret timeout')
            if failure == 'wrapped_timeout':
                raise runner.urllib.error.URLError(TimeoutError('secret wrapped timeout'))
            if failure == 'transport':
                raise runner.urllib.error.URLError('secret transport')
            if failure == 'invalid_response':
                return io.BytesIO(body)
            raise runner.urllib.error.HTTPError('https://private.invalid/secret', status,
                'secret status text', {'X-Private': 'secret'}, io.BytesIO(body))

    class Service(runner.Service):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.opener = Opener()

    def forbidden(*args, **kwargs):
        pytest.fail('a failed claim must not inspect or execute a model CLI')

    monkeypatch.setattr(runner, 'Service', Service)
    monkeypatch.setattr(runner, 'check_cli', forbidden)
    monkeypatch.setattr(runner, 'run_process', forbidden)
    config = tmp_path / 'runner.json'
    native_config(config, 'pi', mode='server')
    with monkeypatch.context() as logging_patch:
        if failure == 'broken_stderr':
            class BrokenStderr:
                def write(self, _value):
                    raise BrokenPipeError('secret broken stderr')
            logging_patch.setattr(runner.sys, 'stderr', BrokenStderr())
            with pytest.raises(runner.RunnerError, match='^service_rejected$'):
                runner.tick(runner.load_config(config))
        else:
            assert runner.main(['--config', str(config), 'tick']) == 1
    # Exactly one claim attempt, with the claim allowance; no retry loop was added.
    assert opened == [('http://127.0.0.1:1/work/claim', runner.CLAIM_SECONDS)]
    captured = capsys.readouterr()
    assert captured.out == ''
    if diagnostic is None:
        assert captured.err == ''
    else:
        receipt, exit_line = captured.err.splitlines()
        assert json.loads(receipt) == {'stage': 'claim', 'error': 'service_rejected', **diagnostic}
        assert exit_line == 'memento-runner: service_rejected'
    assert 'secret' not in captured.out + captured.err


def test_claim_receipt_never_echoes_exception_text(monkeypatch, capsys):
    class Service:
        def __init__(self, *args, **kwargs):
            pass

        def post(self, path, value):
            assert path == '/work/claim'
            raise runner.RunnerError('SENTINEL arbitrary runner text')

    monkeypatch.setattr(runner, 'Service', Service)
    monkeypatch.setattr(runner, 'check_cli', lambda *a: pytest.fail('no model inspection after a failed claim'))
    with pytest.raises(runner.RunnerError, match='^SENTINEL arbitrary runner text$'):
        runner.tick(OVERLAP_CONFIG)
    captured = capsys.readouterr()
    assert json.loads(captured.err) == {'stage': 'claim', 'error': 'service_rejected', 'cause': 'unknown'}
    assert 'SENTINEL' not in captured.err and captured.out == ''


OVERLAP_CONFIG = {'preset': 'pi', 'agent': 'pi', 'command': ['pi'], 'timezone': 'Etc/UTC',
                  'url': 'http://127.0.0.1:8765', 'deadline_seconds': 2, 'mode': 'server'}


def overlap_items(extra_extracts=0):
    items = [{'id': 'extract-1', 'owner': 'lease-e', 'kind': 'extract', 'instruction': 'fixture',
              'input': {'id': 'extract-1'}, 'schema': {'type': 'object'}},
             {'id': 'consolidate-1', 'owner': 'lease-c', 'kind': 'consolidate', 'instruction': 'fixture',
              'input': {'id': 'consolidate-1'}, 'schema': {'type': 'object'}}]
    for number in range(extra_extracts):
        items.append({'id': f'extract-{number + 2}', 'owner': f'lease-e{number + 2}', 'kind': 'extract',
                      'instruction': 'fixture', 'input': {'id': f'extract-{number + 2}'}, 'schema': {'type': 'object'}})
    return items


def recording_service(monkeypatch, lost=()):
    """Every Service the runner builds shares one thread-safe call log."""
    calls, lock = [], threading.Lock()

    class Service:
        def __init__(self, *args, **kwargs):
            pass

        def post(self, path, value):
            with lock:
                calls.append((path, value))
            if path == '/work/renew':
                return {'renewed': value['id'] not in lost}
            return {'released': True, 'renewed': True}

    monkeypatch.setattr(runner, 'Service', Service)
    return Service(), calls


def posted(calls, path):
    return sorted(value['id'] for entry, value in calls if entry == path)


def test_extraction_and_consolidation_model_calls_overlap_with_isolated_executions(monkeypatch):
    service, calls = recording_service(monkeypatch)
    barrier = threading.Barrier(2)
    seen, lock = [], threading.Lock()

    def generate(argv, payload, **kwargs):
        with lock:
            seen.append((runner.decode(payload)['input']['id'], kwargs['cwd'], kwargs['cancelled']))
        barrier.wait(5)  # both calls are inside before either returns
        return assistant_events()

    monkeypatch.setattr(runner, 'run_process', generate)
    before = threading.active_count()
    assert runner.process_work(OVERLAP_CONFIG, service, overlap_items(), 60, ['/native/pi']) == (0, ['/native/pi'])
    assert sorted(ident for ident, _, _ in seen) == ['consolidate-1', 'extract-1']
    assert len({cwd for _, cwd, _ in seen}) == 2 and len({id(check) for _, _, check in seen}) == 2
    assert posted(calls, '/work/complete') == ['consolidate-1', 'extract-1']
    assert posted(calls, '/work/release') == []
    assert threading.active_count() == before


def test_same_kind_work_never_overlaps_and_at_most_two_executions_run(monkeypatch):
    service, calls = recording_service(monkeypatch)
    started = threading.Barrier(2)
    active, peak, lock = {}, [0], threading.Lock()

    def generate(argv, payload, **kwargs):
        ident = runner.decode(payload)['input']['id']
        kind = ident.split('-')[0]
        with lock:
            assert kind not in active, 'same-kind work overlapped'
            active[kind] = True
            peak[0] = max(peak[0], len(active))
        try:
            if ident in ('extract-1', 'consolidate-1'):
                # The first extraction and the consolidation meet; later extractions wait their turn.
                started.wait(5)
            return assistant_events()
        finally:
            with lock:
                del active[kind]

    monkeypatch.setattr(runner, 'run_process', generate)
    assert runner.process_work(OVERLAP_CONFIG, service, overlap_items(extra_extracts=2), 60, ['/native/pi']) == (0, ['/native/pi'])
    assert peak[0] == 2
    assert posted(calls, '/work/complete') == ['consolidate-1', 'extract-1', 'extract-2', 'extract-3']


@pytest.mark.parametrize('failure', ['cli_failed', 'lease_lost'])
def test_overlapped_failure_or_lease_loss_affects_only_its_own_item(monkeypatch, capsys, failure):
    service, calls = recording_service(monkeypatch, lost={'consolidate-1'} if failure == 'lease_lost' else ())
    barrier = threading.Barrier(2)

    def generate(argv, payload, *, cancelled, **kwargs):
        ident = runner.decode(payload)['input']['id']
        barrier.wait(5)
        if ident == 'consolidate-1':
            if failure == 'cli_failed':
                raise runner.RunnerError('cli_failed')
            # run_process polls cancelled(); the heartbeat must mark only this item lost.
            limit = time.monotonic() + 5
            while not cancelled():
                assert time.monotonic() < limit, 'lease loss never cancelled its own item'
                time.sleep(.01)
            raise runner.RunnerError('cli_failed')
        return assistant_events()

    monkeypatch.setattr(runner, 'run_process', generate)
    before = threading.active_count()
    assert runner.process_work(OVERLAP_CONFIG, service, overlap_items(), .4, ['/native/pi']) == (1, ['/native/pi'])
    assert posted(calls, '/work/complete') == ['extract-1']
    assert [value for path, value in calls if path == '/work/release'] == [
        {'id': 'consolidate-1', 'owner': 'lease-c', 'error': 'cli_failed'}]
    receipts = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    assert receipts == [{'id': 'consolidate-1', 'kind': 'consolidate', 'error': 'cli_failed'}]
    assert threading.active_count() == before


def test_unexpected_lane_error_surfaces_after_the_other_item_completes(monkeypatch):
    service, calls = recording_service(monkeypatch)
    barrier = threading.Barrier(2)

    def generate(argv, payload, **kwargs):
        barrier.wait(5)
        if runner.decode(payload)['input']['id'] == 'extract-1':
            raise RuntimeError('unexpected programming error')
        return assistant_events()

    monkeypatch.setattr(runner, 'run_process', generate)
    before = threading.active_count()
    with pytest.raises(RuntimeError, match='unexpected programming error'):
        runner.process_work(OVERLAP_CONFIG, service, overlap_items(), 60, ['/native/pi'])
    assert posted(calls, '/work/complete') == ['consolidate-1']
    assert threading.active_count() == before


def test_failed_lane_start_cancels_started_lane_stops_heartbeat_and_propagates(monkeypatch):
    service, calls = recording_service(monkeypatch)
    entered = threading.Event()

    class Thread(threading.Thread):
        def start(self):
            if self.name == 'memento-consolidate':
                # Fail only once the first lane is executing, so cancellation is tested mid-run.
                assert entered.wait(5), 'first lane never entered its model call'
                raise RuntimeError('cannot start new thread')
            super().start()

    def generate(argv, payload, *, cancelled, **kwargs):
        entered.set()
        limit = time.monotonic() + 5
        while not cancelled():
            assert time.monotonic() < limit, 'started lane was never cancelled'
            time.sleep(.01)
        raise runner.RunnerError('cli_failed')

    monkeypatch.setattr(runner.threading, 'Thread', Thread)
    monkeypatch.setattr(runner, 'run_process', generate)
    before = threading.active_count()
    with pytest.raises(RuntimeError, match='cannot start new thread'):
        runner.process_work(OVERLAP_CONFIG, service, overlap_items(), 60, ['/native/pi'])
    assert entered.is_set()
    assert posted(calls, '/work/complete') == []
    assert posted(calls, '/work/release') == ['extract-1']
    # The started lane and the heartbeat were both joined; nothing lingers.
    assert threading.active_count() == before


def test_tick_keeps_failure_exit_and_excludes_after_overlapped_batch(monkeypatch):
    claims, completed, releases = [], [], []
    pending = {'extract-1': 'extract', 'consolidate-1': 'consolidate'}

    class Service:
        def __init__(self, *args, **kwargs):
            pass

        def post(self, path, value):
            if path == '/work/claim':
                claims.append(value['exclude'])
                work = [{'id': ident, 'owner': 'lease-' + ident, 'kind': kind, 'instruction': 'fixture',
                         'input': {'id': ident}, 'schema': {'type': 'object'}}
                        for ident, kind in pending.items() if ident not in value['exclude']]
                return {'lease_seconds': 60, 'work': work}
            if path == '/work/complete':
                completed.append(value['id'])
                pending.pop(value['id'])
            if path == '/work/release':
                releases.append(value['id'])
                pending.pop(value['id'])
            return {'released': True, 'renewed': True}

    barrier = threading.Barrier(2)

    def generate(argv, payload, **kwargs):
        barrier.wait(5)
        if runner.decode(payload)['input']['id'] == 'extract-1':
            raise runner.RunnerError('invalid_output')
        return assistant_events()

    monkeypatch.setattr(runner, 'Service', Service)
    monkeypatch.setattr(runner, 'check_cli', lambda *a: ['/native/pi'])
    monkeypatch.setattr(runner, 'run_process', generate)
    assert runner.tick(OVERLAP_CONFIG) == 1
    assert completed == ['consolidate-1'] and releases == ['extract-1']
    # Whichever lane finished first claimed again while the other still ran: that claim
    # excluded both the finished ID (cooldown) and the running lease, and the final
    # claim after the batch excludes both as well. Neither ID is ever reissued.
    # If both lanes finish before the coordinator wakes, only the final claim follows.
    assert claims[0] == [] and 2 <= len(claims) <= 3
    assert all(claim == ['consolidate-1', 'extract-1'] for claim in claims[1:])


@pytest.mark.parametrize('consolidate_now', [False, True])
def test_tick_drains_new_batches_without_cap_and_checks_cli_once(monkeypatch, capsys, consolidate_now):
    claims, completed, checked = [], [], []
    items = [
        {'id': f'work-{number}', 'owner': f'lease-{number}', 'kind': 'extract',
         'instruction': 'extract', 'input': {}, 'schema': {'type': 'object'}}
        for number in range(7)
    ]

    class Service:
        def __init__(self, *args, **kwargs):
            pass

        def post(self, path, value):
            if path == '/work/claim':
                claims.append(value)
                work = next((item for item in items if item['id'] not in value['exclude']), None)
                return {'lease_seconds': 60, 'work': [work] if work else []}
            if path == '/work/complete':
                completed.append(value['id'])
            return {'renewed': True, 'released': True}

    def check(*args):
        checked.append(args)
        return ['/native/pi']

    monkeypatch.setattr(runner, 'Service', Service)
    monkeypatch.setattr(runner, 'check_cli', check)
    monkeypatch.setattr(runner, 'run_process', lambda *a, **k: assistant_events())
    config = {'mode': 'server', 'preset': 'pi', 'agent': 'pi', 'command': ['pi'],
              'timezone': 'Etc/UTC', 'url': 'http://127.0.0.1:8765', 'deadline_seconds': 2}
    nudge = {'source': 'pi', 'conversation_id': 'conversation'}
    assert runner.tick(config, nudge, consolidate_now=consolidate_now) == 0
    assert completed == [item['id'] for item in items]
    assert len(claims) == len(items) + 1
    assert claims[0]['compaction'] == nudge
    assert all(claim['compaction'] is None for claim in claims[1:])
    assert claims[-1]['exclude'] == sorted(completed)
    assert len(checked) == 1
    assert all(claim['consolidate_now'] is consolidate_now for claim in claims)
    assert capsys.readouterr().err == ''




def test_setup_temporary_locations_never_activate_timer(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, 'check_cli', lambda *a: ['/native/pi'])
    monkeypatch.setattr(runner, 'install_native', lambda *a: {})

    def forbidden(*a, **k):
        pytest.fail('setup --no-activate must not execute systemctl')

    monkeypatch.setattr(runner, 'run_process', forbidden)
    config = tmp_path / 'private' / 'runner.json'
    units = tmp_path / 'units'
    assert runner.main(['setup', '--agent', 'pi', '--timezone', 'Europe/Berlin',
                        '--config', str(config), '--unit-dir', str(units), '--no-activate']) == 0
    stored = runner.load_config(config)
    assert stored['timezone'] == 'Europe/Berlin'
    assert stored['uid'] == os.getuid()
    assert config.stat().st_mode & 0o077 == 0


def test_setup_consolidation_window_is_validated_persisted_and_preserved_on_reinstall(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, 'check_cli', lambda *a: ['/native/pi'])
    monkeypatch.setattr(runner, 'install_native', lambda *a: {})
    monkeypatch.setattr(runner, 'run_process', lambda *a, **k: pytest.fail('no systemctl with --no-activate'))
    config = tmp_path / 'private' / 'runner.json'
    base = ['setup', '--agent', 'pi', '--timezone', 'Europe/Kiev', '--config', str(config),
            '--unit-dir', str(tmp_path / 'units'), '--no-activate']
    # A legacy server config without the window loads and keeps the service default.
    assert runner.main(base) == 0
    assert not {'consolidation_start', 'consolidation_end'} & set(runner.load_config(config))
    assert runner.main(base + ['--consolidation-start', '01:00', '--consolidation-end', '08:00']) == 0
    stored = runner.load_config(config)
    assert (stored['consolidation_start'], stored['consolidation_end']) == ('01:00', '08:00')
    # Reinstall without the flags preserves the stored window along with the identity.
    assert runner.main(base) == 0
    assert runner.load_config(config) == stored
    for invalid in (['--consolidation-start', '1:00', '--consolidation-end', '08:00'],
                    ['--consolidation-start', '01:00', '--consolidation-end', '24:00'],
                    ['--consolidation-start', '02:00', '--consolidation-end', '02:00'],
                    ['--consolidation-start', '01:00', '--consolidation-end', '08:00\n'],
                    ['--consolidation-start', '٠١:00', '--consolidation-end', '08:00'],
                    ['--consolidation-start', '01:00']):
        assert runner.main(base + invalid) == 1
        assert runner.load_config(config) == stored
    # A client never carries a schedule, and a damaged half-window fails to load.
    client = tmp_path / 'client' / 'runner.json'
    assert runner.main(['agent', 'add', 'pi', '--config', str(client), '--url', 'https://private-service.example']) == 0
    assert not {'consolidation_start', 'consolidation_end'} & set(runner.load_config(client))
    runner.private_write(config, runner.encode({key: value for key, value in stored.items() if key != 'consolidation_end'}))
    with pytest.raises(runner.RunnerError):
        runner.load_config(config)


@pytest.mark.parametrize('window', [None, ('01:00', '08:00')])
def test_tick_forwards_only_a_configured_consolidation_window(monkeypatch, window):
    claims = []

    class Service:
        def __init__(self, *args, **kwargs):
            pass

        def post(self, path, value):
            assert path == '/work/claim'
            claims.append(value)
            return {'lease_seconds': 60, 'work': []}

    monkeypatch.setattr(runner, 'Service', Service)
    monkeypatch.setattr(runner, 'check_cli', lambda *a: ['/native/pi'])
    config = {'mode': 'server', 'preset': 'pi', 'agent': 'pi', 'command': ['pi'],
              'timezone': 'Europe/Kiev', 'url': 'http://127.0.0.1:8765', 'deadline_seconds': 2}
    if window:
        config.update(consolidation_start=window[0], consolidation_end=window[1])
    assert runner.tick(config) == 0
    [claim] = claims
    assert claim['timezone'] == 'Europe/Kiev'
    if window:
        assert (claim['consolidation_start'], claim['consolidation_end']) == window
    else:
        assert not {'consolidation_start', 'consolidation_end'} & set(claim)


def test_setup_persists_distinct_client_identity_across_same_uid_installs(tmp_path, monkeypatch):
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'config'))
    monkeypatch.setattr(runner, 'install_native', lambda *a: {})
    identities = []
    for name in ('first', 'second'):
        config = tmp_path / name / 'runner.json'
        args = ['agent', 'add', 'pi', '--config', str(config),
                '--url', 'https://private-service.example']
        assert runner.main(args) == 0
        stored = runner.load_config(config)
        assert stored['uid'] == os.getuid()
        identities.append(stored['client_uid'])
        assert runner.main(args) == 0
        assert runner.load_config(config)['client_uid'] == identities[-1]
    assert identities[0] != identities[1]


def test_agent_add_reuses_server_role_identity_and_timer(tmp_path, monkeypatch):
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'config'))
    monkeypatch.setattr(runner, 'check_cli', lambda *a: ['/native/pi'])
    monkeypatch.setattr(runner, 'install_native', lambda *a: {})
    config = runner.default_config()
    units = tmp_path / 'units'
    assert runner.main(['setup', '--agent', 'pi', '--timezone', 'Europe/Berlin',
                        '--config', str(config), '--unit-dir', str(units), '--no-activate']) == 0
    original = config.read_bytes()
    timer = (units / 'memento-runner.timer').read_bytes()
    service = (units / 'memento-runner.service').read_bytes()

    def forbidden(*args, **kwargs):
        pytest.fail('agent add must not inspect a model or change the processing timer')

    monkeypatch.setattr(runner, 'check_cli', forbidden)
    monkeypatch.setattr(runner, 'run_process', forbidden)
    assert runner.main(['agent', 'add', 'pi']) == 0
    assert config.read_bytes() == original
    assert (units / 'memento-runner.timer').read_bytes() == timer
    assert (units / 'memento-runner.service').read_bytes() == service

    assert runner.main(['agent', 'add', 'omp']) == 0
    client = runner.load_config(config.parent / 'omp' / 'runner.json')
    assert client['mode'] == 'client'
    assert client['url'] == runner.load_config(config)['url']
    assert client['client_uid'] != runner.load_config(config)['client_uid']
    assert config.read_bytes() == original
    assert runner.main(['agent', 'add', 'omp', '--config', str(config)]) == 1
    assert config.read_bytes() == original


@pytest.mark.parametrize('client', [False, True])
def test_failed_reinstall_retains_previous_connection_and_identity(tmp_path, monkeypatch, client):
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'config'))
    monkeypatch.setattr(runner, 'check_cli', lambda *a: ['/native/pi'])
    monkeypatch.setattr(runner, 'install_native', lambda *a: {})
    config = tmp_path / 'private' / 'runner.json'
    args = (['agent', 'add', 'pi', '--config', str(config)] if client else
            ['setup', '--agent', 'pi', '--timezone', 'Europe/Berlin', '--config', str(config),
             '--unit-dir', str(tmp_path / 'units'), '--no-activate'])
    assert runner.main(args + ['--url', 'https://original-private-service.example']) == 0
    original = config.read_bytes()

    def unavailable(*args):
        raise runner.RunnerError('native integration installation failed')

    monkeypatch.setattr(runner, 'install_native', unavailable)
    assert runner.main(args + ['--url', 'https://unreachable-private-service.example']) == 1
    assert config.read_bytes() == original
    assert config.stat().st_mode & 0o077 == 0


@pytest.mark.parametrize('agent', ['pi', 'omp', 'claude', 'codex'])
def test_laptop_setup_and_compaction_never_execute_or_claim(tmp_path, monkeypatch, agent):
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / '.config'))
    def forbidden(*args, **kwargs):
        pytest.fail('client mode must never execute or inspect a model CLI')

    monkeypatch.setattr(runner, 'check_cli', forbidden)
    monkeypatch.setattr(runner, 'run_process', forbidden)
    monkeypatch.setattr(runner, 'install_native', lambda *a: {})
    config = tmp_path / 'runner.json'
    units = tmp_path / '.config' / 'systemd' / 'user'
    assert runner.main(['agent', 'add', agent,
                        '--url', 'http://memento.owner-tailnet.ts.net',
                        '--config', str(config)]) == 0
    assert runner.load_config(config)['mode'] == 'client'
    assert not units.exists()
    calls = []

    class Service:
        def __init__(self, *args, **kwargs):
            pass

        def post(self, path, value):
            calls.append((path, value))
            return {'nudged': True}

    monkeypatch.setattr(runner, 'Service', Service)
    assert runner.main(['--config', str(config), 'compaction', '--source', agent,
                        '--conversation-id', 'conversation']) == 0
    assert calls == [('/work/compaction', {'source': agent, 'conversation_id': 'conversation'})]
    assert runner.main(['--config', str(config), 'tick']) == 1
    with pytest.raises(runner.RunnerError):
        runner.tick(runner.load_config(config))


def test_server_setup_requires_timezone_before_native_execution(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('missing server timezone must fail before native execution')

    monkeypatch.setattr(runner, 'check_cli', forbidden)
    config = tmp_path / 'runner.json'
    assert runner.main(['setup', '--agent', 'pi', '--config', str(config)]) == 1
    assert not config.exists()


def test_agent_add_without_connection_does_not_create_a_config(tmp_path, monkeypatch):
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path))
    assert runner.main(['agent', 'add', 'claude']) == 1
    assert not (tmp_path / 'memento').exists()






def test_http_evidence_cannot_be_redirected_and_tailnet_urls_are_accepted():
    assert runner.Service('http://memento.owner-tailnet.ts.net').base_url == 'http://memento.owner-tailnet.ts.net'
    assert runner.NoRedirect().redirect_request(None, None, 302, '', {}, 'https://elsewhere') is None


def test_systemd_values_do_not_expand_percent_or_environment():
    assert runner.unit_arg('/tmp/a %h $HOME "x"') == '"/tmp/a %%h $$HOME \\"x\\""'
    with pytest.raises(runner.RunnerError):
        runner.unit_arg('/tmp/a\nExecStart=bad')


@pytest.fixture
def client_release_http():
    from memento.api import client_release_assets
    _, _, release = client_release_assets()
    state = {'body': release.encode(), 'status': 200, 'extra_length': 0}

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.0'

        def do_GET(self):
            assert self.path == '/client-release'
            self.send_response(state['status'])
            self.send_header('Content-Length', str(len(state['body']) + state['extra_length']))
            self.end_headers()
            self.wfile.write(state['body'])

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield state, f'http://127.0.0.1:{server.server_port}'
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.fixture
def update_installation(tmp_path, monkeypatch, client_release_http):
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'config'))

    def forbidden(*args, **kwargs):
        pytest.fail('updates must not inspect models, install hooks, start sessions or change timers')

    monkeypatch.setattr(runner, 'run_process', forbidden)
    monkeypatch.setattr(runner, 'check_cli', forbidden)
    monkeypatch.setattr(runner, 'install_native', forbidden)

    def create(harness='pi', mode='client', alternate=False):
        config_path = runner.default_config()
        if alternate:
            host = {'uid': os.getuid(), 'client_uid': 'host-identity', 'agent': 'claude',
                    'preset': 'claude', 'command': ['claude'], 'mode': 'server', 'timezone': 'Etc/UTC',
                    'url': 'http://127.0.0.1:1', 'deadline_seconds': 600}
            runner.private_write(config_path, runner.encode(host))
            config_path = config_path.parent / harness / 'runner.json'
        config = {'uid': os.getuid(), 'client_uid': 'stable-device-identity', 'agent': harness,
                  'preset': harness, 'command': ['/existing/native'], 'mode': mode,
                  'timezone': 'Europe/Berlin', 'url': client_release_http[1], 'deadline_seconds': 321,
                  'unrelated': {'private': 'do not rewrite'}}
        runner.private_write(config_path, runner.encode(config) + b'\n')
        bridge_path = config_path.parent / 'bridge.py'
        runner.private_write(bridge_path, b'#!/usr/bin/env python3\n# previous bridge\n')
        root = tmp_path / 'native'
        root.mkdir()
        protected = [config_path]
        if alternate:
            protected.append(runner.default_config())
        targets = {bridge_path: 'bridge.py'}
        if harness in ('pi', 'omp'):
            extension = root / 'extensions/memento'
            extension.mkdir(parents=True)
            for filename, source in (('index.ts', 'native_' + harness + '.ts'),
                                     ('native_shared.ts', 'native_shared.ts')):
                target = extension / filename
                runner.private_write(target, b'// previous extension\n')
                targets[target] = source
            integration = extension / 'memento-config.json'
            runner.private_write(integration, runner.encode(
                {'config': str(config_path), 'bridge': str(bridge_path), 'python': sys.executable}))
            protected.append(integration)
        for filename in ('settings.json', 'hooks.json', 'config.toml', 'claude.json'):
            target = root / filename
            runner.private_write(target, b'private native login/model/hook/trust settings\n')
            protected.append(target)
        spool = bridge.Spool(config_path, config)
        spool.queue('/episodes', {'event_id': 'pending-event', 'text': 'durable raw capture'})
        spool.db.execute('INSERT INTO journals(path,harness,conversation,offset,prefix) VALUES(?,?,?,?,?)',
                         ('/native/history', harness, 'conversation', 123, 'cursor-prefix'))
        spool.commit()
        spool.close()
        protected.append(spool.path)
        units = tmp_path / 'units'
        for filename in ('memento-runner.service', 'memento-runner.timer'):
            target = units / filename
            runner.private_write(target, b'Existing ExecStart/timer must not change\n')
            protected.append(target)
        standalone = tmp_path / '.local/share/memento/runner.py'
        runner.private_write(standalone, b'#!/usr/bin/env python3\n# previous standalone runner\n')
        standalone.chmod(0o700)
        targets[standalone] = 'runner.py'
        command = tmp_path / '.local/bin/memento'
        command.parent.mkdir(parents=True)
        command.symlink_to(standalone)
        before = {path: (path.read_bytes(), path.stat().st_mode & 0o777) for path in protected}
        argv = ['agent', 'update', harness, '--native-config-dir', str(root)]
        return config_path, targets, before, argv, command

    return create


@pytest.mark.parametrize('harness', ['pi', 'omp', 'claude', 'codex'])
@pytest.mark.parametrize('mode', ['client', 'server'])
def test_agent_update_preserves_identity_outbox_settings_units_and_executable(
        update_installation, client_release_http, harness, mode):
    config, targets, protected, argv, command = update_installation(harness, mode)
    expected = runner.release_files(client_release_http[0]['body'])
    package_before = Path(runner.__file__).read_bytes()
    for _ in range(2):
        assert runner.main(argv) == 0
        for path, name in targets.items():
            assert path.read_bytes() == expected[name]
        assert {path: (path.read_bytes(), path.stat().st_mode & 0o777)
                for path in protected} == protected
        assert command.is_symlink()
        assert command.resolve().stat().st_mode & 0o777 == 0o700
        assert Path(runner.__file__).read_bytes() == package_before


def test_agent_update_uses_existing_harness_config_without_retargeting(update_installation):
    _, _, protected, argv, _ = update_installation('omp', alternate=True)
    assert runner.main(argv) == 0
    assert {path: (path.read_bytes(), path.stat().st_mode & 0o777)
            for path in protected} == protected


@pytest.mark.parametrize('failure', ['unavailable', 'truncated', 'hash', 'syntax', 'missing'])
def test_agent_update_rejects_failed_release_before_replacing_code(
        update_installation, client_release_http, failure):
    _, targets, protected, argv, _ = update_installation()
    old = {path: path.read_bytes() for path in targets}
    state = client_release_http[0]
    if failure == 'unavailable':
        state['status'] = 503
    elif failure == 'truncated':
        state['extra_length'] = 100
    else:
        bundle = runner.decode(state['body'])
        if failure == 'missing':
            del bundle['files']['native_omp.ts']
        elif failure == 'hash':
            bundle['files']['native_shared.ts']['sha256'] = '0' * 64
        else:
            invalid = b'def broken(:\n'
            bundle['files']['bridge.py'] = {'content': base64.b64encode(invalid).decode(),
                                           'sha256': hashlib.sha256(invalid).hexdigest()}
        state['body'] = runner.encode(bundle)
    assert runner.main(argv) == 1
    assert {path: path.read_bytes() for path in targets} == old
    assert {path: (path.read_bytes(), path.stat().st_mode & 0o777)
            for path in protected} == protected


@pytest.mark.parametrize('interruption', [OSError, KeyboardInterrupt])
def test_agent_update_interruption_retains_private_data_and_retry_completes(
        update_installation, client_release_http, monkeypatch, interruption):
    _, targets, protected, argv, command = update_installation()
    previous_runner = command.resolve().read_bytes()
    publish = runner.private_write
    calls = 0

    def interrupted(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise interruption()
        return publish(*args, **kwargs)

    monkeypatch.setattr(runner, 'private_write', interrupted)
    assert runner.main(argv) == (130 if interruption is KeyboardInterrupt else 1)
    assert command.resolve().read_bytes() == previous_runner
    assert {path: (path.read_bytes(), path.stat().st_mode & 0o777)
            for path in protected} == protected
    monkeypatch.setattr(runner, 'private_write', publish)
    assert runner.main(argv) == 0
    expected = runner.release_files(client_release_http[0]['body'])
    assert {path: path.read_bytes() for path in targets} == {
        path: expected[name] for path, name in targets.items()}
    assert {path: (path.read_bytes(), path.stat().st_mode & 0o777)
            for path in protected} == protected
    assert command.resolve().stat().st_mode & 0o777 == 0o700


class LaneServer:
    """A server fake that leases one job per kind per claim, honoring exclusions and kinds."""

    def __init__(self, pending, *, slow=(), complete_hook=None):
        self.pending = dict(pending)  # id -> kind
        self.claims, self.completed, self.releases, self.renewals = [], [], [], []
        self.leased = {}
        self.attempts = {ident: 0 for ident in pending}
        self.lock = threading.Lock()
        self.complete_hook = complete_hook

    def post(self, path, value):
        with self.lock:
            if path == '/work/claim':
                self.claims.append(value)
                kinds = value.get('kinds') or ['extract', 'consolidate']
                work = []
                for kind in kinds:
                    ident = next((ident for ident, pending_kind in self.pending.items()
                                  if pending_kind == kind and ident not in value['exclude']
                                  and ident not in self.leased), None)
                    if ident is not None:
                        self.attempts[ident] += 1
                        owner = f'{ident}-{self.attempts[ident]}'
                        self.leased[ident] = owner
                        work.append({'id': ident, 'owner': owner, 'kind': kind, 'instruction': 'fixture',
                                     'input': {'id': ident}, 'schema': {'type': 'object'}})
                return {'lease_seconds': 60, 'work': work}
            if path == '/work/complete':
                assert self.leased.pop(value['id']) == value['owner']
                self.pending.pop(value['id'])
                self.completed.append(value['id'])
                if self.complete_hook:
                    self.complete_hook(value['id'])
                return {}
            if path == '/work/release':
                assert self.leased.pop(value['id']) == value['owner']
                self.releases.append(value)
                return {'released': True}
            assert path == '/work/renew'
            self.renewals.append((time.monotonic(), value['id']))
            return {'renewed': self.leased.get(value['id']) == value['owner']}


def lane_server(monkeypatch, pending, **kwargs):
    server = LaneServer(pending, **kwargs)

    class Service:
        def __init__(self, *args, **kwargs):
            pass

        def post(self, path, value):
            return server.post(path, value)

    monkeypatch.setattr(runner, 'Service', Service)
    monkeypatch.setattr(runner, 'check_cli', lambda *a: ['/native/pi'])
    return server


def test_free_lane_replenishes_while_the_other_lane_is_still_running(monkeypatch):
    extracts_done = threading.Event()
    server = lane_server(monkeypatch, {'extract-1': 'extract', 'consolidate-1': 'consolidate',
                                       'extract-2': 'extract', 'extract-3': 'extract'},
                         complete_hook=lambda ident: extracts_done.set() if ident == 'extract-3' else None)
    active, peak, lock = {}, [0], threading.Lock()

    def generate(argv, payload, *, cancelled, **kwargs):
        ident = runner.decode(payload)['input']['id']
        kind = ident.split('-')[0]
        with lock:
            assert kind not in active, 'same-kind work overlapped'
            active[kind] = True
            peak[0] = max(peak[0], len(active))
        try:
            if ident == 'consolidate-1':
                # The slow lane finishes only after every extraction completed.
                assert extracts_done.wait(5), 'fast lane waited for the slow lane'
            return assistant_events()
        finally:
            with lock:
                del active[kind]

    monkeypatch.setattr(runner, 'run_process', generate)
    before = threading.active_count()
    assert runner.tick(OVERLAP_CONFIG) == 0
    assert server.completed == ['extract-1', 'extract-2', 'extract-3', 'consolidate-1']
    assert peak[0] == 2
    # First request keeps the old shape (no kinds); replenishment asks only for the free kind
    # while excluding the running lease; a drained kind is not asked again until a completion.
    assert 'kinds' not in server.claims[0] and server.claims[0]['exclude'] == []
    assert [claim.get('kinds') for claim in server.claims] == [None, ['extract'], ['extract'], ['extract'], None]
    assert server.claims[1]['exclude'] == ['consolidate-1', 'extract-1']
    assert server.claims[3]['exclude'] == ['consolidate-1', 'extract-1', 'extract-2', 'extract-3']
    assert server.claims[4]['exclude'] == ['consolidate-1', 'extract-1', 'extract-2', 'extract-3']
    assert set(server.attempts.values()) == {1}
    assert threading.active_count() == before


def test_failed_job_cooldown_starts_at_its_own_completion_not_the_batch(monkeypatch, capsys):
    release_seen = threading.Event()
    server = lane_server(monkeypatch, {'extract-1': 'extract', 'consolidate-1': 'consolidate'})
    original_post = server.post

    def post(path, value):
        result = original_post(path, value)
        if path == '/work/release':
            release_seen.set()
        return result
    server.post = post

    def generate(argv, payload, **kwargs):
        ident = runner.decode(payload)['input']['id']
        if ident == 'extract-1':
            raise runner.RunnerError('invalid_output')
        assert release_seen.wait(5), 'failed fast lane never released'
        # Give the coordinator time to claim again for the freed lane before finishing.
        deadline = time.monotonic() + 5
        while len(server.claims) < 2 and time.monotonic() < deadline:
            time.sleep(.01)
        return assistant_events()

    monkeypatch.setattr(runner, 'run_process', generate)
    assert runner.tick(OVERLAP_CONFIG) == 1
    assert server.completed == ['consolidate-1']
    assert server.releases == [{'id': 'extract-1', 'owner': 'extract-1-1', 'error': 'invalid_output'}]
    # The failed ID was excluded from the very next claim, made while the slow lane still ran.
    assert server.claims[1].get('kinds') == ['extract'] and 'extract-1' in server.claims[1]['exclude']
    assert server.attempts['extract-1'] == 1
    assert json.loads(capsys.readouterr().err) == {'id': 'extract-1', 'kind': 'extract', 'error': 'invalid_output'}


def test_running_lease_renews_while_the_coordinator_blocks_on_a_claim(monkeypatch):
    server = lane_server(monkeypatch, {'extract-1': 'extract', 'consolidate-1': 'consolidate'})
    blocking = {}
    original_post = server.post

    def post(path, value):
        if path == '/work/claim' and value.get('kinds') == ['extract']:
            blocking['start'] = time.monotonic()
            time.sleep(.5)  # the claim is slow while consolidate-1 is still running
            blocking['end'] = time.monotonic()
        result = original_post(path, value)
        if path == '/work/claim':
            result['lease_seconds'] = .4
        return result
    server.post = post

    def generate(argv, payload, **kwargs):
        if runner.decode(payload)['input']['id'] == 'consolidate-1':
            deadline = time.monotonic() + 5
            while 'end' not in blocking and time.monotonic() < deadline:
                time.sleep(.01)
        return assistant_events()

    monkeypatch.setattr(runner, 'run_process', generate)
    assert runner.tick(OVERLAP_CONFIG) == 0
    during = [ident for stamp, ident in server.renewals
              if ident == 'consolidate-1' and blocking['start'] < stamp < blocking['end']]
    assert during, 'no renewal reached the server while the claim blocked'
    assert server.completed == ['extract-1', 'consolidate-1']


@pytest.mark.parametrize('interruption', ['claim_error', 'keyboard_interrupt'])
def test_claim_failure_or_interrupt_cancels_running_work_and_joins_everything(monkeypatch, capsys, interruption):
    server = lane_server(monkeypatch, {'extract-1': 'extract', 'consolidate-1': 'consolidate'})
    original_post = server.post

    def post(path, value):
        if path == '/work/claim' and value.get('kinds') == ['extract']:
            if interruption == 'keyboard_interrupt':
                raise KeyboardInterrupt
            raise runner.RunnerError('service_rejected')
        return original_post(path, value)
    server.post = post
    saw_cancel = threading.Event()

    def generate(argv, payload, *, cancelled, **kwargs):
        if runner.decode(payload)['input']['id'] == 'consolidate-1':
            limit = time.monotonic() + 5
            while not cancelled():
                assert time.monotonic() < limit, 'running work was never cancelled'
                time.sleep(.01)
            saw_cancel.set()
            raise runner.RunnerError('cli_failed')
        return assistant_events()

    monkeypatch.setattr(runner, 'run_process', generate)
    before = threading.active_count()
    with pytest.raises(KeyboardInterrupt if interruption == 'keyboard_interrupt' else runner.RunnerError):
        runner.tick(OVERLAP_CONFIG)
    assert saw_cancel.is_set()
    assert server.completed == ['extract-1']
    assert server.releases == [{'id': 'consolidate-1', 'owner': 'consolidate-1-1', 'error': 'cli_failed'}]
    assert not server.leased and threading.active_count() == before
    receipts = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    expected = [{'id': 'consolidate-1', 'kind': 'consolidate', 'error': 'cli_failed'}]
    if interruption == 'claim_error':
        expected.insert(0, {'stage': 'claim', 'error': 'service_rejected', 'cause': 'unknown'})
    assert receipts == expected


@pytest.mark.parametrize('defect', ['malformed', 'duplicate', 'other_kind', 'running_id', 'excluded_id'])
def test_unexpected_claim_response_is_rejected_and_cancels_running_work(monkeypatch, defect):
    server = lane_server(monkeypatch, {'extract-1': 'extract', 'consolidate-1': 'consolidate', 'extract-2': 'extract'})
    original_post = server.post

    def post(path, value):
        result = original_post(path, value)
        if path == '/work/claim' and value.get('kinds') == ['extract']:
            item = dict(result['work'][0]) if result['work'] else None
            if defect == 'malformed':
                result['work'] = [dict(item, input='not an object')]
            elif defect == 'duplicate':
                result['work'] = [item, dict(item)]
            elif defect == 'other_kind':
                result['work'] = [dict(item, kind='consolidate')]
            elif defect == 'running_id':
                result['work'] = [dict(item, id='consolidate-1')]
            else:
                result['work'] = [dict(item, id='extract-1')]
        return result
    server.post = post

    def generate(argv, payload, *, cancelled, **kwargs):
        if runner.decode(payload)['input']['id'] == 'consolidate-1':
            limit = time.monotonic() + 5
            while not cancelled():
                assert time.monotonic() < limit, 'running work was never cancelled'
                time.sleep(.01)
            raise runner.RunnerError('cli_failed')
        return assistant_events()

    monkeypatch.setattr(runner, 'run_process', generate)
    before = threading.active_count()
    with pytest.raises(runner.RunnerError, match='^service_rejected$'):
        runner.tick(OVERLAP_CONFIG)
    assert server.completed == ['extract-1'] and 'extract-2' not in server.completed
    assert [release['id'] for release in server.releases] == ['consolidate-1']
    assert threading.active_count() == before


def test_cli_check_runs_once_after_heartbeats_start_and_keeps_both_leases_alive(monkeypatch):
    server = lane_server(monkeypatch, {'extract-1': 'extract', 'consolidate-1': 'consolidate'})
    original_post = server.post

    def post(path, value):
        result = original_post(path, value)
        if path == '/work/claim':
            result['lease_seconds'] = .4
        return result
    server.post = post
    checks, finished = [], {}

    def slow_check(command, preset):
        checks.append((command, preset, time.monotonic()))
        time.sleep(1.0)  # longer than two lease periods
        finished['at'] = time.monotonic()
        return ['/native/pi']

    def generate(argv, payload, **kwargs):
        assert 'at' in finished, 'native generation started before the safety check passed'
        return assistant_events()

    monkeypatch.setattr(runner, 'check_cli', slow_check)
    monkeypatch.setattr(runner, 'run_process', generate)
    assert runner.tick(OVERLAP_CONFIG) == 0
    assert len(checks) == 1 and checks[0][:2] == (['pi'], 'pi')
    assert sorted(server.completed) == ['consolidate-1', 'extract-1'] and not server.releases
    # Both leases were renewed while the single check was still blocking.
    renewed_during = {ident for stamp, ident in server.renewals if checks[0][2] < stamp < finished['at']}
    assert renewed_during == {'consolidate-1', 'extract-1'}
