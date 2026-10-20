import base64
import hashlib
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

from memento.cold import DriveColdStore
from memento.drive_auth import (DriveAuthorization, DriveAuthorizationError, configure_client,
                                requires_reauthorization)
from memento.drive_connect import SCOPES, _publish


ACCOUNT = 'owner@example.test'
FOLDER = 'original_folder'
AUTH_URI = 'https://accounts.google.com/o/oauth2/v2/auth'
TOKEN_URI = 'https://oauth2.googleapis.com/token'


def desktop():
    return dict(installed=dict(client_id='synthetic-original-client',
                               client_secret='synthetic-client-secret',
                               auth_uri='https://accounts.google.com/o/oauth2/auth',
                               token_uri=TOKEN_URI, redirect_uris=['http://localhost']),
                memento=dict(account=ACCOUNT, folder_id=FOLDER))


def previous_connection():
    return dict(type='authorized_user', token='synthetic-old-token',
                refresh_token='synthetic-old-refresh', client_id='synthetic-original-client',
                client_secret='synthetic-client-secret', token_uri=TOKEN_URI, scopes=SCOPES,
                memento=dict(account=ACCOUNT, folder_id=FOLDER))


def assert_error(status, operation, *args, **kwargs):
    with pytest.raises(DriveAuthorizationError) as caught:
        operation(*args, **kwargs)
    assert caught.value.status_code == status
    assert 'synthetic-' not in str(caught.value)
    assert ACCOUNT not in str(caught.value)
    return caught.value


@pytest.fixture
def oauth(tmp_path, monkeypatch):
    flow_module = pytest.importorskip('google_auth_oauthlib.flow')
    oauth_module = pytest.importorskip('requests_oauthlib')
    discovery = pytest.importorskip('googleapiclient.discovery')
    import requests

    state = dict(account=ACCOUNT, folder={}, requests=[], closed=0, session_closed=0,
                 flows=[], installed=[], now=100.0, created=0)
    _publish(tmp_path / 'drive-client.json', desktop())
    original_factory = flow_module.InstalledAppFlow.from_client_config

    def factory(config, scopes, **kwargs):
        flow = original_factory(config, scopes=scopes, **kwargs)
        state['flows'].append(flow)
        flow.oauth2session.close = lambda: state.update(session_closed=state['session_closed'] + 1)
        return flow

    def fetch_token(session, token_url, **kwargs):
        assert token_url == TOKEN_URI
        assert kwargs['code'] == 'synthetic-code'
        assert kwargs['client_secret'] == 'synthetic-client-secret'
        assert 43 <= len(kwargs['code_verifier']) <= 128
        assert kwargs['timeout'] == 30
        assert session.max_redirects == 0 and session.trust_env is False
        assert session.redirect_uri == 'http://127.0.0.1:18766/'
        state['requests'].append(kwargs)
        if state.get('fetch_error'):
            raise state['fetch_error']
        token = dict(access_token='synthetic-new-access', token_type='Bearer',
                     refresh_token=state.get('refresh_token', 'synthetic-new-refresh'),
                     expires_at=time.time() + 3600, scope=state.get('scopes', SCOPES))
        session.token = token
        return token

    class Request:
        def __init__(self, data):
            self.data = data

        def execute(self):
            if isinstance(self.data, Exception):
                raise self.data
            return self.data

    class Service:
        def about(self):
            return self

        def files(self):
            return self

        def get(self, **kwargs):
            if 'fileId' not in kwargs:
                return Request(dict(user=dict(emailAddress=state['account'])))
            assert kwargs['fileId'] == FOLDER
            if state.get('folder_error'):
                return Request(state['folder_error'])
            folder = dict(id=FOLDER, mimeType='application/vnd.google-apps.folder',
                          ownedByMe=True, trashed=False)
            folder.update(state['folder'])
            return Request(folder)

        def create(self, **kwargs):
            state['created'] += 1
            raise AssertionError('Authorization must never create a folder')

        def close(self):
            state['closed'] += 1

    def install(backend, config, reconnect):
        assert backend.folder_id == FOLDER
        state['installed'].append((backend, config))

    def no_network(*args, **kwargs):
        raise AssertionError('Unexpected real Google transport')

    monkeypatch.setattr(requests.Session, 'request', no_network)
    monkeypatch.setattr(flow_module.InstalledAppFlow, 'from_client_config', factory)
    monkeypatch.setattr(oauth_module.OAuth2Session, 'fetch_token', fetch_token)
    monkeypatch.setattr(discovery, 'build', lambda *args, **kwargs: Service())
    manager = DriveAuthorization(tmp_path, install, clock=lambda: state['now'])
    yield manager, state
    manager.close()
    for backend, _ in state['installed']:
        backend.close()


def test_success_uses_pkce_and_only_returns_public_flow_data(oauth):
    manager, state = oauth
    begun = manager.begin(18766)
    assert set(begun) == {'authorization_url', 'state', 'expires_in'}
    assert begun['expires_in'] == 600
    url = urlsplit(begun['authorization_url'])
    assert f'{url.scheme}://{url.netloc}{url.path}' == AUTH_URI
    query = parse_qs(url.query)
    assert query['redirect_uri'] == ['http://127.0.0.1:18766/']
    assert query['state'] == [begun['state']]
    assert query['scope'] == SCOPES
    assert query['response_type'] == ['code']
    assert query['access_type'] == ['offline'] and query['prompt'] == ['consent']
    assert query['include_granted_scopes'] == ['false']
    verifier = state['flows'][0].code_verifier
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip('=')
    assert query['code_challenge'] == [challenge]
    assert query['code_challenge_method'] == ['S256']
    assert 'synthetic-client-secret' not in json.dumps(begun)
    assert manager.finish(begun['state'], code='synthetic-code') == {'status': 'connected'}
    assert len(state['installed']) == 1 and state['created'] == 0
    backend, config = state['installed'][0]
    assert backend.folder_id == FOLDER
    assert config['memento'] == dict(account=ACCOUNT, folder_id=FOLDER)
    assert config['scopes'] == SCOPES and config['refresh_token'] == 'synthetic-new-refresh'
    assert state['closed'] == 0 and state['session_closed'] == 1
    assert_error(410, manager.finish, begun['state'], code='synthetic-code')


@pytest.mark.parametrize('change', ['account', 'wrong_id', 'not_folder', 'trashed', 'not_owned',
                                    'broader_scope', 'narrow_scope', 'no_refresh', 'missing_folder'])
def test_invalid_grants_cannot_install_or_touch_previous_output(tmp_path, oauth, change):
    manager, state = oauth
    _publish(tmp_path / 'drive-oauth.json', previous_connection())
    original = (tmp_path / 'drive-oauth.json').read_bytes()
    expected_status = 400
    if change == 'account':
        state['account'] = 'other@example.test'
    elif change == 'wrong_id':
        state['folder']['id'] = 'another_folder'
    elif change == 'not_folder':
        state['folder']['mimeType'] = 'text/plain'
    elif change == 'trashed':
        state['folder']['trashed'] = True
    elif change == 'not_owned':
        state['folder']['ownedByMe'] = False
    elif change == 'broader_scope':
        state['scopes'] = SCOPES + ['https://www.googleapis.com/auth/drive']
    elif change == 'narrow_scope':
        state['scopes'] = []
    elif change == 'no_refresh':
        state['refresh_token'] = None
    else:
        state['folder_error'] = RuntimeError('synthetic-private-http-error')
        expected_status = 502
    begun = manager.begin(18766)
    assert_error(expected_status, manager.finish, begun['state'], code='synthetic-code')
    assert not state['installed'] and state['created'] == 0
    assert (tmp_path / 'drive-oauth.json').read_bytes() == original
    assert state['session_closed'] == 1
    assert state['closed'] == (0 if change in ('broader_scope', 'narrow_scope', 'no_refresh') else 1)
    assert_error(410, manager.finish, begun['state'], code='synthetic-code')


def test_wrong_state_does_not_consume_pending_and_cancellation_does(oauth):
    manager, state = oauth
    begun = manager.begin(18766)
    assert_error(409, manager.begin, 18766)
    assert_error(400, manager.finish, 'substitution-state', code='synthetic-code')
    assert not state['requests'] and state['session_closed'] == 0
    assert_error(400, manager.finish, begun['state'], error='synthetic-provider-secret')
    assert not state['requests'] and not state['installed'] and state['session_closed'] == 1
    assert_error(410, manager.finish, begun['state'], code='synthetic-code')
    assert manager.begin(18766)['state'] != begun['state']


def test_deadline_is_exclusive_and_old_state_cannot_consume_replacement(oauth):
    manager, state = oauth
    first = manager.begin(18766)
    state['now'] += 600
    assert_error(410, manager.finish, first['state'], code='synthetic-code')
    assert not state['requests'] and state['session_closed'] == 1
    second = manager.begin(18766)
    assert second['state'] != first['state']
    assert_error(400, manager.finish, first['state'], code='synthetic-code')
    state['now'] += 600
    third = manager.begin(18766)
    assert third['state'] != second['state'] and state['session_closed'] == 2


@pytest.mark.parametrize('values', [dict(), dict(code='', error=None), dict(code='x', error='denied'),
                                  dict(code='x' * 4097), dict(error='x' * 257)])
def test_matching_malformed_finish_is_consumed(oauth, values):
    manager, state = oauth
    begun = manager.begin(18766)
    assert_error(400, manager.finish, begun['state'], **values)
    assert_error(410, manager.finish, begun['state'], code='synthetic-code')
    assert not state['requests'] and not state['installed']


def test_close_cancels_pending_and_disables_manager(oauth):
    manager, state = oauth
    begun = manager.begin(18766)
    manager.close()
    manager.close()
    assert state['session_closed'] == 1
    assert_error(503, manager.begin, 18766)
    assert_error(503, manager.finish, begun['state'], code='synthetic-code')


def test_mutex_allows_only_one_pending_flow(oauth):
    manager, state = oauth
    barrier = Barrier(2)

    def begin():
        barrier.wait(timeout=5)
        try:
            return manager.begin(18766)
        except DriveAuthorizationError as error:
            return error.status_code

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: begin(), range(2)))
    assert sum(isinstance(result, dict) for result in results) == 1
    assert 409 in results and len(state['flows']) == 1


def test_failed_install_is_not_success_and_closes_verified_backend(tmp_path, oauth):
    _, state = oauth

    def fail_install(backend, config, reconnect):
        raise RuntimeError('synthetic-new-refresh must not escape')

    manager = DriveAuthorization(tmp_path, fail_install)
    begun = manager.begin(18766)
    assert_error(502, manager.finish, begun['state'], code='synthetic-code')
    assert state['closed'] == 1 and not state['installed']
    assert not (tmp_path / 'drive-oauth.json').exists()


@pytest.mark.parametrize('change', ['account', 'folder_id', 'client_id', 'client_secret'])
def test_existing_binding_conflicts_are_rejected_before_consent(tmp_path, oauth, change):
    manager, state = oauth
    config = previous_connection()
    target = config['memento'] if change in ('account', 'folder_id') else config
    target[change] = 'different-original-binding'
    _publish(tmp_path / 'drive-oauth.json', config)
    assert_error(409, manager.begin, 18766)
    assert not state['flows'] and not state['requests'] and not state['installed']


def test_existing_private_connection_can_supply_original_client_and_folder(tmp_path, oauth):
    manager, state = oauth
    (tmp_path / 'drive-client.json').unlink()
    _publish(tmp_path / 'drive-oauth.json', previous_connection())
    begun = manager.begin(18766)
    assert manager.finish(begun['state'], code='synthetic-code') == {'status': 'connected'}
    assert state['created'] == 0 and state['installed'][0][1]['memento']['folder_id'] == FOLDER


@pytest.mark.parametrize('when', ['before_exchange', 'during_verification'])
def test_changed_binding_during_consent_cannot_install(tmp_path, oauth, monkeypatch, when):
    manager, state = oauth
    begun = manager.begin(18766)
    changed = desktop()
    changed['memento']['folder_id'] = 'operator_rebound_folder'
    if when == 'before_exchange':
        _publish(tmp_path / 'drive-client.json', changed)
    else:
        original_verify = DriveColdStore._verify_folder

        def verify(service, folder_id):
            original_verify(service, folder_id)
            _publish(tmp_path / 'drive-client.json', changed)

        monkeypatch.setattr(DriveColdStore, '_verify_folder', staticmethod(verify))
    assert_error(409, manager.finish, begun['state'], code='synthetic-code')
    assert not state['installed'] and state['closed'] == (when == 'during_verification')
    assert json.loads((tmp_path / 'drive-client.json').read_text()) == changed


@pytest.mark.parametrize('port', [True, False, 0, -1, 65536, 18766.0, '18766'])
def test_invalid_callback_port_is_rejected_without_configuration(tmp_path, port):
    manager = DriveAuthorization(tmp_path, lambda *_: pytest.fail('Unexpected install'))
    assert_error(400, manager.begin, port)


def test_missing_bootstrap_is_explicitly_unavailable(tmp_path):
    manager = DriveAuthorization(tmp_path, lambda *_: pytest.fail('Unexpected install'))
    assert_error(503, manager.begin, 18766)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize('change', ['public', 'symlink', 'directory', 'fifo', 'foreign_owner',
                                    'web', 'auth_host', 'auth_query', 'token_host',
                                    'missing_account', 'missing_folder', 'malformed'])
def test_bootstrap_is_strictly_private_and_pinned(tmp_path, monkeypatch, change):
    path = tmp_path / 'drive-client.json'
    config = desktop()
    if change == 'web':
        config['web'] = config.pop('installed')
    elif change == 'auth_host':
        config['installed']['auth_uri'] = 'https://accounts.google.com.evil.test/o/oauth2/v2/auth'
    elif change == 'auth_query':
        config['installed']['auth_uri'] = AUTH_URI + '?redirect_uri=https://evil.test'
    elif change == 'token_host':
        config['installed']['token_uri'] = 'https://evil.test/token'
    elif change == 'missing_account':
        config['memento'].pop('account')
    elif change == 'missing_folder':
        config['memento'].pop('folder_id')
    _publish(path, config)
    if change == 'public':
        path.chmod(0o644)
    elif change == 'symlink':
        target = tmp_path / 'original'
        path.rename(target)
        path.symlink_to(target)
    elif change in ('directory', 'fifo'):
        path.unlink()
        path.mkdir() if change == 'directory' else os.mkfifo(path, 0o600)
    elif change == 'foreign_owner':
        original_fstat = os.fstat

        def fstat(fd):
            value = original_fstat(fd)
            return SimpleNamespace(st_mode=value.st_mode, st_size=value.st_size, st_uid=987654321)

        monkeypatch.setattr(os, 'fstat', fstat)
    elif change == 'malformed':
        path.write_text('{synthetic-private-json')
    manager = DriveAuthorization(tmp_path, lambda *_: pytest.fail('Unexpected install'))
    assert_error(503, manager.begin, 18766)


def test_provider_errors_are_sanitized_and_consumed(oauth):
    manager, state = oauth
    from oauthlib.oauth2 import InvalidGrantError
    state['fetch_error'] = InvalidGrantError(description='synthetic-provider-secret')
    begun = manager.begin(18766)
    assert_error(400, manager.finish, begun['state'], code='synthetic-code')
    assert not state['installed'] and state['session_closed'] == 1
    assert_error(410, manager.finish, begun['state'], code='synthetic-code')


def test_operator_provisioning_is_private_and_preserves_original_bindings(tmp_path):
    source = tmp_path / 'original-desktop.json'
    config = desktop()
    config.pop('memento')
    _publish(source, config)
    data_dir = tmp_path / 'data'
    assert configure_client(source, ACCOUNT, FOLDER, data_dir) is None
    path = data_dir / 'drive-client.json'
    assert path.stat().st_mode & 0o777 == 0o600
    assert json.loads(path.read_text()) == desktop()
    _publish(data_dir / 'drive-oauth.json', previous_connection())
    original = path.read_bytes()
    assert_error(409, configure_client, source, 'other@example.test', FOLDER, data_dir)
    assert path.read_bytes() == original
    config['installed']['client_secret'] = 'synthetic-replaced-client-secret'
    _publish(source, config)
    assert_error(409, configure_client, source, ACCOUNT, FOLDER, data_dir)
    assert path.read_bytes() == original


def test_operator_provisioning_rejects_nonprivate_source_and_invalid_folder(tmp_path):
    source = tmp_path / 'original-desktop.json'
    _publish(source, desktop())
    data_dir = tmp_path / 'data'
    source.chmod(0o644)
    assert_error(400, configure_client, source, ACCOUNT, FOLDER, data_dir)
    source.chmod(0o600)
    assert_error(400, configure_client, source, ACCOUNT, 'folder/child', data_dir)
    assert not data_dir.exists()


@pytest.mark.parametrize('error_code,expected', [('invalid_grant', True), ('invalid_rapt', True),
                                                ('invalid_client', False), ('server_error', False)])
def test_only_typed_structured_definitive_refresh_rejection_requires_reauthorization(error_code, expected):
    exceptions = pytest.importorskip('google.auth.exceptions')
    error = exceptions.RefreshError('synthetic-private-description', {'error': error_code})
    assert requires_reauthorization(error) is expected
    assert not requires_reauthorization(exceptions.RefreshError('invalid_grant'))
    assert not requires_reauthorization(exceptions.RefreshError('private', {'error': error_code}, retryable=True))
    assert not requires_reauthorization(RuntimeError('invalid_grant', {'error': error_code}))


def test_stale_grant_startup_then_fresh_consent_activates_drive(tmp_path, oauth, monkeypatch):
    from fastapi.testclient import TestClient
    from google.auth.exceptions import RefreshError
    from memento.api import create_app

    _, state = oauth
    _publish(tmp_path / 'drive-oauth.json', previous_connection())

    def stale_connection(cls, path):
        raise RefreshError('synthetic-private-provider-description', {'error': 'invalid_grant'})

    monkeypatch.setattr(DriveColdStore, 'from_file', classmethod(stale_connection))
    monkeypatch.setenv('MEMENTO_EMBEDDINGS', '0')
    with TestClient(create_app(tmp_path)) as client:
        unhealthy = client.get('/health')
        assert unhealthy.status_code == 503
        assert 'reauthorization' in unhealthy.text.lower()
        assert client.app.state.cold is None
        begun = client.post('/drive/connect/begin', json={'callback_port': 18766})
        assert begun.status_code == 200
        finished = client.post('/drive/connect/finish', json={
            'state': begun.json()['state'], 'code': 'synthetic-code'})
        assert finished.status_code == 200 and finished.json() == {'status': 'connected'}
        assert client.app.state.cold is not None and client.app.state.cold_runtime is not None
        assert client.get('/health').status_code == 200
        assert client.post('/drive/connect/begin', json={'callback_port': 18766}).status_code == 409
        saved = json.loads((tmp_path / 'drive-oauth.json').read_text())
        assert saved['refresh_token'] == 'synthetic-new-refresh'
        assert saved['memento'] == dict(account=ACCOUNT, folder_id=FOLDER)
        assert 'synthetic-new-refresh' not in finished.text
    assert state['closed'] == 1 and state['created'] == 0


@pytest.mark.parametrize('outcome', ['denied', 'wrong_account', 'success', 'cleanup_error'])
def test_explicit_reconnect_preserves_working_grant_until_verified(tmp_path, oauth, monkeypatch, outcome):
    from fastapi.testclient import TestClient
    from googleapiclient.discovery import build
    from memento.api import create_app

    _, state = oauth
    path = tmp_path / 'drive-oauth.json'
    _publish(path, previous_connection())
    original = path.read_bytes()
    old_backend = DriveColdStore(FOLDER, ACCOUNT, service=build('drive', 'v3'))
    monkeypatch.setattr(DriveColdStore, 'from_file', classmethod(lambda cls, path: old_backend))
    monkeypatch.setenv('MEMENTO_EMBEDDINGS', '0')
    if outcome == 'cleanup_error':
        def failed_close():
            raise RuntimeError('synthetic-private-cleanup-diagnostic')
        monkeypatch.setattr(old_backend, 'close', failed_close)
    with TestClient(create_app(tmp_path)) as client:
        begun = client.post('/drive/connect/begin', json={'callback_port': 18766, 'reconnect': True})
        assert begun.status_code == 200
        assert client.get('/health').status_code == 200 and path.read_bytes() == original
        result = {'state': begun.json()['state']}
        if outcome == 'denied':
            result['error'] = 'access_denied'
        else:
            result['code'] = 'synthetic-code'
            if outcome == 'wrong_account':
                state['account'] = 'wrong@example.test'
        finished = client.post('/drive/connect/finish', json=result)
        assert client.get('/health').status_code == 200
        if outcome in ('denied', 'wrong_account'):
            assert finished.status_code == 400 and path.read_bytes() == original
        else:
            assert finished.status_code == 200 and finished.json() == {'status': 'connected'}
            saved = json.loads(path.read_text())
            assert saved['refresh_token'] == 'synthetic-new-refresh'
            assert saved['client_id'] == previous_connection()['client_id']
            assert saved['memento'] == previous_connection()['memento']
            assert path.stat().st_mode & 0o777 == 0o600
        assert state['created'] == 0
