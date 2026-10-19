import json
import os
import sys
import threading

import pytest

from memento import runner


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
    with pytest.raises(runner.RunnerError, match='exactly one native executable'):
        runner.check_cli(['pi', '--tools', 'bash'], 'pi')


def test_missing_safety_capability_is_fatal(monkeypatch):
    monkeypatch.setattr(runner.shutil, 'which', lambda _: '/native/pi')
    monkeypatch.setattr(runner, 'run_process', lambda *a, **k: b'--print --no-tools')
    with pytest.raises(runner.RunnerError, match='required safety flags'):
        runner.check_cli(['pi'], 'pi')


def test_codex_cannot_substitute_readonly_sandbox_for_tools_disabled(tmp_path):
    with pytest.raises(runner.RunnerError, match='no verified all-tools-disabled'):
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


@pytest.mark.parametrize('flags, expected', [([], False), (['--consolidate-now'], True)])
def test_tick_cli_forwards_explicit_consolidation_override(monkeypatch, flags, expected):
    config = {'mode': 'server'}
    calls = []
    monkeypatch.setattr(runner, 'load_config', lambda path: config)

    def tick(value, compaction, *, consolidate_now):
        calls.append((value, compaction, consolidate_now))
        return 0

    monkeypatch.setattr(runner, 'tick', tick)
    assert runner.main(['tick', *flags]) == 0
    assert calls == [(config, None, expected)]


def test_setup_temporary_locations_never_activate_timer(tmp_path, monkeypatch, capsys):
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
    service = (units / 'memento-runner.service').read_text()
    assert '--config' in service and str(config) in service
    assert 'OnUnitInactiveSec=15min' in (units / 'memento-runner.timer').read_text()
    assert 'compaction' in capsys.readouterr().out


def test_setup_persists_distinct_client_identity_across_same_uid_installs(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, 'install_native', lambda *a: {})
    identities = []
    for name in ('first', 'second'):
        config = tmp_path / name / 'runner.json'
        args = ['setup', '--client', '--agent', 'pi', '--timezone', 'Etc/UTC',
                '--config', str(config)]
        assert runner.main(args) == 0
        stored = runner.load_config(config)
        assert stored['uid'] == os.getuid()
        identities.append(stored['client_uid'])
        assert runner.main(args) == 0
        assert runner.load_config(config)['client_uid'] == identities[-1]
    assert identities[0] != identities[1]


@pytest.mark.parametrize('agent', ['pi', 'omp', 'claude', 'codex'])
def test_laptop_setup_and_compaction_never_execute_or_claim(tmp_path, monkeypatch, agent):
    def forbidden(*args, **kwargs):
        pytest.fail('client mode must never execute or inspect a model CLI')

    monkeypatch.setattr(runner, 'check_cli', forbidden)
    monkeypatch.setattr(runner, 'run_process', forbidden)
    monkeypatch.setattr(runner, 'install_native', lambda *a: {})
    config = tmp_path / 'runner.json'
    units = tmp_path / 'units'
    assert runner.main(['setup', '--client', '--agent', agent, '--timezone', 'Etc/UTC',
                        '--url', 'http://memento.owner-tailnet.ts.net',
                        '--config', str(config), '--unit-dir', str(units)]) == 0
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
    with pytest.raises(runner.RunnerError, match='never claims model work'):
        runner.tick(runner.load_config(config))


def test_setup_requires_explicit_timezone_without_host_detection():
    with pytest.raises(SystemExit) as error:
        runner.main(['setup', '--client', '--agent', 'codex'])
    assert error.value.code == 2




def test_setup_downloads_shipped_bridge_and_passes_isolated_native_root(tmp_path, monkeypatch):
    downloaded = []
    executed = []
    source = b'#!/usr/bin/env python3'

    def download(service, path):
        downloaded.append((service.base_url, path))
        return source

    def execute(argv, **kwargs):
        executed.append((argv, kwargs))
        return b'{"status":"installed"}'

    monkeypatch.setattr(runner.Service, 'download', download)
    monkeypatch.setattr(runner, 'run_process', execute)
    config_path = tmp_path / 'runner.json'
    native_root = tmp_path / 'native'
    result = runner.install_native(config_path, {'preset': 'pi', 'url': 'http://127.0.0.1:8765'}, native_root)
    assert result == {'status': 'installed'}
    assert downloaded == [('http://127.0.0.1:8765', '/bridge')]
    assert (tmp_path / 'bridge.py').read_bytes() == source
    assert executed[0][0] == [
        sys.executable, str(tmp_path / 'bridge.py'), 'install', '--config', str(config_path),
        '--native-config-dir', str(native_root),
    ]


def test_http_evidence_cannot_be_redirected_and_tailnet_urls_are_accepted():
    assert runner.Service('http://memento.owner-tailnet.ts.net').base_url == 'http://memento.owner-tailnet.ts.net'
    assert runner.NoRedirect().redirect_request(None, None, 302, '', {}, 'https://elsewhere') is None


def test_systemd_values_do_not_expand_percent_or_environment():
    assert runner.unit_arg('/tmp/a %h $HOME "x"') == '"/tmp/a %%h $$HOME \\"x\\""'
    with pytest.raises(runner.RunnerError, match='control characters'):
        runner.unit_arg('/tmp/a\nExecStart=bad')
