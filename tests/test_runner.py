import json
import base64
import hashlib
import http.server
import os
import sys
import threading
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


def test_drain_excludes_failures_without_retrying_or_disclosing_text(monkeypatch):
    calls = []
    item = {'id': 'work', 'owner': 'lease', 'kind': 'extract', 'instruction': 'private',
            'input': {'text': 'secret'}, 'schema': {'type': 'object'}}

    class Service:
        def __init__(self, *args, **kwargs):
            pass

        def post(self, path, value):
            calls.append((path, value))
            if path == '/work/claim':
                return {'lease_seconds': 60, 'work': [] if item['id'] in value['exclude'] else [item]}
            if path == '/work/complete':
                raise runner.RunnerError('service_rejected')
            return {'released': True, 'renewed': True}

    monkeypatch.setattr(runner, 'Service', Service)
    monkeypatch.setattr(runner, 'check_cli', lambda *a: ['/native/pi'])
    monkeypatch.setattr(runner, 'run_process', lambda *a, **k: assistant_events())
    config = {'preset': 'pi', 'agent': 'pi', 'command': ['pi'], 'timezone': 'Etc/UTC',
              'url': 'http://127.0.0.1:8765', 'deadline_seconds': 2, 'mode': 'server'}
    compaction = {'source': 'pi', 'conversation_id': 'conversation'}
    assert runner.tick(config, compaction) == 1
    assert sum(path == '/work/claim' for path, _ in calls) == 2
    assert [value['exclude'] for path, value in calls if path == '/work/claim'] == [[], ['work']]
    assert calls[0][1]['compaction'] == compaction
    release = next(value for path, value in calls if path == '/work/release')
    assert release == {'id': 'work', 'owner': 'lease', 'error': 'service_rejected'}
    assert 'secret' not in json.dumps(calls)


@pytest.mark.parametrize('consolidate_now', [False, True])
def test_tick_drains_new_batches_without_cap_and_checks_cli_once(monkeypatch, consolidate_now):
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
