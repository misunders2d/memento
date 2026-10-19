import json

import pytest

from memento.cold import DriveColdStore
from memento.drive_connect import SCOPES, _publish, connect, load_connection


ACCOUNT = 'owner@example.test'


def connection():
    return dict(type='authorized_user', token='synthetic-access', refresh_token='synthetic-refresh',
                client_id='synthetic-client', client_secret='synthetic-secret', scopes=SCOPES,
                memento=dict(account=ACCOUNT, folder_id='original_folder'))


def test_connection_is_private_atomic_and_has_one_account_folder(tmp_path):
    path = tmp_path / 'drive-oauth.json'
    _publish(path, connection())
    assert path.stat().st_mode & 0o777 == 0o600
    assert load_connection(path)['memento'] == dict(account=ACCOUNT, folder_id='original_folder')
    path.chmod(0o644)
    with pytest.raises(ValueError, match='private'):
        load_connection(path)
    path.chmod(0o600)
    linked = tmp_path / 'linked'
    linked.symlink_to(path)
    with pytest.raises(OSError):
        load_connection(linked)


def test_connection_rejects_missing_binding_and_broader_scope(tmp_path):
    path = tmp_path / 'drive-oauth.json'
    config = connection()
    config.pop('memento')
    _publish(path, config)
    with pytest.raises(ValueError, match='account'):
        load_connection(path)
    config = connection()
    config['scopes'] = SCOPES + ['https://www.googleapis.com/auth/drive']
    _publish(path, config)
    with pytest.raises(ValueError, match='only drive.file'):
        load_connection(path)


@pytest.fixture
def oauth(tmp_path, monkeypatch):
    flow_module = pytest.importorskip('google_auth_oauthlib.flow')
    discovery = pytest.importorskip('googleapiclient.discovery')
    state = dict(account=ACCOUNT, folder_exists=True, created=0, authorized=0, closed=0)
    class Request:
        def __init__(self, data):
            self.data = data
        def execute(self):
            return self.data
    class Service:
        def about(self):
            return self
        def files(self):
            return self
        def get(self, **kwargs):
            if 'fileId' not in kwargs:
                return Request(dict(user=dict(emailAddress=state['account'])))
            if not state['folder_exists']:
                raise RuntimeError('HTTP 404')
            return Request(dict(id=kwargs['fileId'], mimeType='application/vnd.google-apps.folder', ownedByMe=True))
        def create(self, **kwargs):
            state['created'] += 1
            assert kwargs['body']['mimeType'] == 'application/vnd.google-apps.folder'
            return Request(dict(id='new_folder'))
        def close(self):
            state['closed'] += 1
        def __enter__(self):
            return self
        def __exit__(self, *args):
            self.close()
    class Credentials:
        scopes = SCOPES
        granted_scopes = SCOPES
        refresh_token = 'synthetic-refresh'
        def to_json(self):
            config = connection()
            config.pop('memento')
            return json.dumps(config)
    class Flow:
        @classmethod
        def from_client_config(cls, config, *, scopes, autogenerate_code_verifier):
            assert scopes == SCOPES and autogenerate_code_verifier
            return cls()
        def run_local_server(self, **kwargs):
            state['authorized'] += 1
            assert kwargs['host'] == '127.0.0.1' and not kwargs['open_browser']
            assert kwargs['login_hint'] == ACCOUNT
            return Credentials()
    monkeypatch.setattr(flow_module, 'InstalledAppFlow', Flow)
    monkeypatch.setattr(discovery, 'build', lambda *args, **kwargs: Service())
    client = tmp_path / 'desktop-client.json'
    client.write_text(json.dumps(dict(installed=dict(client_id='synthetic-client'))))
    return client, state


def test_connect_creates_folder_once_and_reconnect_preserves_it(tmp_path, oauth):
    client, state = oauth
    path = tmp_path / 'drive-oauth.json'
    connect(client, ACCOUNT, path, no_browser=True)
    assert load_connection(path)['memento']['folder_id'] == 'new_folder'
    connect(client, ACCOUNT, path, no_browser=True)
    assert load_connection(path)['memento']['folder_id'] == 'new_folder'
    assert state['created'] == 1 and state['closed'] == 2


def test_wrong_granted_account_never_creates_folder_or_publishes(tmp_path, oauth):
    client, state = oauth
    state['account'] = 'other@example.test'
    path = tmp_path / 'drive-oauth.json'
    with pytest.raises(ValueError, match='differs'):
        connect(client, ACCOUNT, path, no_browser=True)
    assert not path.exists() and state['created'] == 0


def test_reconnect_does_not_change_account_or_replace_lost_folder(tmp_path, oauth):
    client, state = oauth
    path = tmp_path / 'drive-oauth.json'
    _publish(path, connection())
    original = path.read_bytes()
    with pytest.raises(ValueError, match='previously verified account'):
        connect(client, 'other@example.test', path, no_browser=True)
    assert state['authorized'] == 0
    state['folder_exists'] = False
    with pytest.raises(RuntimeError, match='HTTP 404'):
        connect(client, ACCOUNT, path, no_browser=True)
    assert state['created'] == 0 and path.read_bytes() == original


def test_from_file_verifies_bound_account_and_never_uses_another(tmp_path, oauth):
    _, state = oauth
    path = tmp_path / 'drive-oauth.json'
    _publish(path, connection())
    state['account'] = 'other@example.test'
    with pytest.raises(ValueError, match='differs'):
        DriveColdStore.from_file(path)
    assert state['closed'] == 1
