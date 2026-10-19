import contextlib
import http.client
import http.server
import json
import os
import select
import signal
import subprocess
import sys
import socket
import threading
import time
import urllib.parse

import pytest

from memento import runner


class SyntheticDrive:
    """An actual HTTP boundary with single-use state and independent verification."""

    def __init__(self):
        self.begun = threading.Event()
        self.state = 'synthetic-state'
        self.code = 'synthetic-private-one-time-code'
        self.port = None
        self.bound_before_begin = False
        self.pending = False
        self.installed = False
        self.finish_calls = []
        self.verify = True
        self.result_status = 'connected'
        self.mutate_begin = lambda value: value
        self.redirect_begin = False
        self.followed_redirect = False
        self.begin_error = None
        self.finish_error = None

        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                owner.followed_redirect = True
                self.send_response(200)
                self.send_header('Content-Length', '2')
                self.end_headers()
                self.wfile.write(b'{}')

            def do_POST(self):
                value = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                status = 200
                if self.path == '/drive/connect/begin':
                    owner.port = value['callback_port']
                    # A second bind must fail already, even before the URL exists.
                    with socket.socket() as probe:
                        try:
                            probe.bind(('127.0.0.1', owner.port))
                        except OSError:
                            owner.bound_before_begin = True
                    owner.pending = True
                    fields = {'client_id': 'synthetic.apps.googleusercontent.com',
                              'response_type': 'code', 'scope': 'https://www.googleapis.com/auth/drive.file',
                              'state': owner.state, 'redirect_uri': f'http://127.0.0.1:{owner.port}/',
                              'code_challenge': 'synthetic-pkce', 'code_challenge_method': 'S256'}
                    result = owner.mutate_begin({
                        'authorization_url': 'https://accounts.google.com/o/oauth2/v2/auth?'
                                             + urllib.parse.urlencode(fields),
                        'state': owner.state, 'expires_in': 600})
                    owner.begun.set()
                elif self.path == '/drive/connect/finish':
                    owner.finish_calls.append(value)
                    valid = owner.pending and value.get('state') == owner.state
                    owner.pending = False
                    if not valid or value.get('error') or value.get('code') != owner.code or not owner.verify:
                        status, result = 400, {'detail': 'synthetic-private-verification-diagnostic'}
                    else:
                        owner.installed = owner.result_status == 'connected'
                        result = {'status': owner.result_status}
                else:
                    status, result = 404, {'detail': 'unexpected model-work endpoint'}
                if self.path == '/drive/connect/begin' and owner.begin_error is not None:
                    status, result = owner.begin_error
                elif self.path == '/drive/connect/finish' and owner.finish_error is not None:
                    status, result = owner.finish_error
                if owner.redirect_begin and self.path == '/drive/connect/begin':
                    self.send_response(303)
                    self.send_header('Location', owner.url + '/redirected')
                    self.send_header('Content-Length', '0')
                    self.end_headers()
                    return
                body = runner.encode(result)
                self.send_response(status)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.server = http.server.HTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}'

    def callback(self, pairs=None, *, headers=None, path='/', method='GET'):
        pairs = [('state', self.state), ('code', self.code)] if pairs is None else pairs
        target = path + '?' + urllib.parse.urlencode(pairs)
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=3)
        try:
            connection.request(method, target, headers=headers or {})
            response = connection.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            connection.close()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(3)


@pytest.fixture
def drive():
    boundary = SyntheticDrive()
    try:
        yield boundary
    finally:
        boundary.close()


@contextlib.contextmanager
def running_relay(drive, *, timeout=2):
    result = {}
    service = runner.Service(drive.url)

    def relay():
        try:
            runner.connect_drive(service, timeout=timeout)
            result['connected'] = True
        except runner.RunnerError as exc:
            result['error'] = str(exc)

    thread = threading.Thread(target=relay, daemon=True)
    thread.start()
    try:
        assert drive.begun.wait(2)
        yield result, thread
    finally:
        thread.join(4)
        assert not thread.is_alive(), 'relay did not close within its bounded lifetime'


def assert_closed(port):
    with socket.socket() as probe:
        probe.settimeout(.2)
        assert probe.connect_ex(('127.0.0.1', port)) != 0


def test_connected_requires_verified_install_and_bound_listener(drive, capsys):
    with running_relay(drive) as (result, thread):
        status, headers, body = drive.callback()
        thread.join(2)
        assert status == 200
        assert headers['Cache-Control'] == 'no-store'
        assert "default-src 'none'" in headers['Content-Security-Policy']
        assert headers['Referrer-Policy'] == 'no-referrer'
        assert b'<script' not in body and b'<form' not in body
    assert result == {'connected': True}
    assert drive.bound_before_begin and drive.installed
    assert drive.finish_calls == [{'state': drive.state, 'code': drive.code, 'error': None}]
    assert_closed(drive.port)
    output = capsys.readouterr()
    assert 'https://accounts.google.com/o/oauth2/v2/auth?' in output.out
    assert not output.err
    assert drive.code not in output.out + output.err + body.decode()


@pytest.mark.parametrize('hostile', [
    'wrong-state', 'duplicate-state', 'duplicate-code', 'code-and-error',
    'duplicate-other', 'empty-code', 'origin', 'host', 'path', 'post',
])
def test_hostile_callback_does_not_consume_authentic_flow(drive, hostile, capsys):
    pairs = [('state', drive.state), ('code', drive.code)]
    kwargs = {}
    if hostile == 'wrong-state':
        pairs[0] = ('state', 'hostile-state')
    elif hostile == 'duplicate-state':
        pairs.append(('state', drive.state))
    elif hostile == 'duplicate-code':
        pairs.append(('code', 'private-hostile-code'))
    elif hostile == 'code-and-error':
        pairs.append(('error', 'private-hostile-error'))
    elif hostile == 'duplicate-other':
        pairs.extend([('scope', 'one'), ('scope', 'two')])
    elif hostile == 'empty-code':
        pairs[1] = ('code', '')
    elif hostile == 'origin':
        kwargs['headers'] = {'Origin': 'https://hostile.invalid'}
    elif hostile == 'host':
        kwargs['headers'] = {'Host': 'hostile.invalid'}
    elif hostile == 'path':
        kwargs['path'] = '/hostile'
    elif hostile == 'post':
        kwargs['method'] = 'POST'
    with running_relay(drive) as (result, thread):
        status, headers, body = drive.callback(pairs, **kwargs)
        assert status >= 400
        assert headers['Cache-Control'] == 'no-store'
        assert drive.finish_calls == [] and drive.pending
        assert drive.code.encode() not in body
        assert drive.callback()[0] == 200
        thread.join(2)
    assert result == {'connected': True} and drive.installed
    output = capsys.readouterr()
    assert not output.err
    assert 'private-hostile-' not in output.out


@pytest.mark.parametrize('failure', ['provider-denial', 'verification', 'not-connected'])
def test_denial_and_server_failure_never_report_success(drive, failure, capsys):
    pairs = None
    if failure == 'provider-denial':
        pairs = [('state', drive.state), ('error', 'access_denied'),
                 ('error_description', 'private-provider-description')]
    elif failure == 'verification':
        drive.verify = False
    else:
        drive.result_status = 'pending'
    with running_relay(drive) as (result, thread):
        status, _, body = drive.callback(pairs)
        thread.join(2)
        assert status == 400
    expected = {'provider-denial': 'Drive connection cancelled',
                'verification': 'Drive authorization was rejected.',
                'not-connected': 'Drive connection failed'}[failure]
    assert result == {'error': expected}
    assert len(drive.finish_calls) == 1 and not drive.installed
    if failure == 'provider-denial':
        assert drive.finish_calls == [{'state': drive.state, 'code': None, 'error': 'access_denied'}]
    assert_closed(drive.port)
    output = capsys.readouterr()
    assert not output.err
    for secret in (drive.code, 'private-provider-description', 'synthetic-private-verification-diagnostic'):
        assert secret not in output.out + body.decode()


@pytest.mark.parametrize('change', [
    'host', 'scope', 'state', 'redirect', 'duplicate', 'fragment', 'secret',
])
def test_untrusted_begin_response_is_not_printed_or_opened(drive, change, capsys):
    def mutate(value):
        url = value['authorization_url']
        if change == 'host':
            url = url.replace('accounts.google.com', 'accounts.google.com.hostile.invalid')
        elif change == 'fragment':
            url += '#private-fragment'
        elif change == 'duplicate':
            url += '&state=' + drive.state
        elif change == 'secret':
            url += '&client_secret=private-client-secret'
        else:
            parsed = urllib.parse.urlsplit(url)
            fields = dict(urllib.parse.parse_qsl(parsed.query))
            key, wrong = {'scope': ('scope', 'https://www.googleapis.com/auth/drive'),
                          'state': ('state', 'wrong-state'),
                          'redirect': ('redirect_uri', 'http://127.0.0.1:1/') }[change]
            fields[key] = wrong
            url = urllib.parse.urlunsplit(parsed._replace(query=urllib.parse.urlencode(fields)))
        return dict(value, authorization_url=url)

    drive.mutate_begin = mutate
    with pytest.raises(runner.RunnerError):
        runner.connect_drive(runner.Service(drive.url), timeout=1)
    assert not drive.finish_calls
    assert_closed(drive.port)
    output = capsys.readouterr()
    assert output.out == output.err == ''


def test_new_google_authorization_parameters_do_not_break_connection(drive):
    def mutate(value):
        return {**value, 'authorization_url': value['authorization_url'] + '&enable_granular_consent=true'}

    drive.mutate_begin = mutate
    with running_relay(drive) as (result, thread):
        assert drive.callback()[0] == 200
        thread.join(2)
    assert result == {'connected': True} and drive.installed



@pytest.mark.parametrize('slow_request', [False, True])
def test_timeout_closes_listener_including_partial_local_request(drive, slow_request, capsys):
    started = time.monotonic()
    with running_relay(drive, timeout=.25) as (result, thread):
        if slow_request:
            with socket.create_connection(('127.0.0.1', drive.port), timeout=1) as client:
                client.sendall(b'GET /?code=private-partial-code HTTP/1.1\r\n')
                thread.join(2)
        else:
            thread.join(2)
    assert time.monotonic() - started < 2
    assert not drive.finish_calls
    assert_closed(drive.port)
    assert 'private-partial-code' not in ''.join(capsys.readouterr())


@pytest.mark.parametrize('url', ['http://service.example', 'http://127.0.0.1.hostile.invalid'])
def test_remote_plaintext_is_rejected_before_any_request(url):
    with pytest.raises(runner.RunnerError):
        runner.connect_drive(runner.Service(url), timeout=.1)


def test_service_transport_ignores_environment_proxy(drive, monkeypatch):
    monkeypatch.setenv('http_proxy', 'http://127.0.0.1:1')
    monkeypatch.setenv('HTTP_PROXY', 'http://127.0.0.1:1')
    monkeypatch.setenv('no_proxy', '')
    with running_relay(drive) as (result, thread):
        assert drive.callback()[0] == 200
        thread.join(2)
    assert result == {'connected': True}


def test_service_redirect_is_rejected_without_following(drive, capsys):
    drive.redirect_begin = True
    with pytest.raises(runner.RunnerError):
        runner.connect_drive(runner.Service(drive.url), timeout=1)
    assert not drive.followed_redirect
    assert not drive.finish_calls
    assert_closed(drive.port)
    assert capsys.readouterr().out == ''


def test_cancellation_closes_listener_with_fixed_safe_error(drive, monkeypatch, capsys):
    service = runner.Service(drive.url)
    original = service.post

    def interrupted(path, value, **kwargs):
        original(path, value, **kwargs)
        raise KeyboardInterrupt

    monkeypatch.setattr(service, 'post', interrupted)
    with pytest.raises(runner.RunnerError):
        runner.connect_drive(service, timeout=1)
    assert_closed(drive.port)
    assert capsys.readouterr().out == ''


def test_already_connected_is_success_without_another_consent_link(drive, capsys):
    drive.installed = True
    drive.begin_error = (409, {'detail': 'Drive is already connected'})
    assert runner.main(['drive-connect', '--url', drive.url]) == 0
    assert drive.installed and not drive.finish_calls
    assert_closed(drive.port)
    output = capsys.readouterr()
    assert 'https://accounts.google.com/' not in output.out




def test_terminal_cancellation_releases_pending_grant_for_reconnect(drive, tmp_path):
    process = subprocess.Popen(
        [sys.executable, runner.__file__, 'drive-connect', '--url', drive.url],
        cwd=tmp_path, env={**os.environ, 'HOME': str(tmp_path), 'XDG_CONFIG_HOME': str(tmp_path / 'config')},
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert select.select([process.stdout], [], [], 3)[0], 'helper did not print a consent link'
        assert process.stdout.readline().startswith('https://accounts.google.com/o/oauth2/v2/auth?')
        process.send_signal(signal.SIGINT)
        output, error = process.communicate(timeout=3)
        assert process.returncode == 1
        assert 'connected' not in output
        assert drive.finish_calls == [{'state': drive.state, 'error': 'access_denied'}]
        assert not drive.pending and not drive.installed
        assert_closed(drive.port)
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()




@pytest.mark.parametrize('mode', ['client', 'server', 'before-setup'])
def test_cli_connect_works_without_model_discovery_or_work(drive, mode, tmp_path, monkeypatch):
    config = tmp_path / 'runner.json'
    args = ['--config', str(config), 'drive-connect']
    if mode == 'before-setup':
        args += ['--url', drive.url]
    else:
        runner.private_write(config, runner.encode({
            'uid': os.getuid(), 'client_uid': 'synthetic-client', 'agent': 'pi', 'preset': 'pi',
            'mode': mode, 'timezone': 'Europe/Berlin', 'deadline_seconds': 600,
            'url': drive.url, 'command': ['missing-native-cli']}))

    def forbidden(*args, **kwargs):
        raise AssertionError('Drive connection must not discover a model or claim work')

    monkeypatch.setattr(runner, 'check_cli', forbidden)
    monkeypatch.setattr(runner, 'tick', forbidden)
    monkeypatch.setattr(runner, 'run_process', forbidden)
    outcome = {}
    thread = threading.Thread(target=lambda: outcome.update(status=runner.main(args)), daemon=True)
    thread.start()
    try:
        assert drive.begun.wait(2)
        assert drive.callback()[0] == 200
        thread.join(3)
        assert not thread.is_alive()
        assert outcome == {'status': 0} and drive.installed
    finally:
        thread.join(3)
    assert_closed(drive.port)
