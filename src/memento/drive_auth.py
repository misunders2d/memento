"""Private, single-use authorization of the original account and Drive folder."""

import json
import os
import secrets
import stat
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from .cold import DriveColdStore
from .drive_connect import SCOPES, _publish, load_connection, check_binding

_AUTH_URI = 'https://accounts.google.com/o/oauth2/v2/auth'
_TOKEN_URI = 'https://oauth2.googleapis.com/token'
_AUTH_URIS = {_AUTH_URI, 'https://accounts.google.com/o/oauth2/auth'}
_TTL = 600
_MESSAGES = {
    400: 'Drive authorization was invalid or rejected.',
    409: 'Drive authorization is pending or conflicts with the original connection.',
    410: 'Drive authorization has expired or was already used.',
    503: 'Drive authorization is not configured or available.',
    502: 'Drive authorization could not be completed.',
}


class DriveAuthorizationError(Exception):
    def __init__(self, status_code: int, fixed_message: str | None = None):
        self.status_code = status_code
        message = _MESSAGES[status_code]
        if status_code == 409 and fixed_message == 'Drive is already connected':
            message = fixed_message
        super().__init__(message)


def requires_reauthorization(error: Exception) -> bool:
    """Recognize only a definitive structured Google refresh-token rejection."""
    try:
        from google.auth.exceptions import RefreshError
    except ImportError:
        return False
    if not isinstance(error, RefreshError) or error.retryable:
        return False
    return any(isinstance(value, dict)
               and value.get('error') in ('invalid_grant', 'invalid_rapt')
               for value in error.args[1:])


@dataclass(frozen=True, repr=False)
class _Binding:
    client: dict
    account: str
    folder_id: str

    def matches(self, other):
        return (self.client['client_id'] == other.client['client_id']
                and self.client['client_secret'] == other.client['client_secret']
                and self.account.casefold() == other.account.casefold()
                and self.folder_id == other.folder_id)


@dataclass(repr=False)
class _Pending:
    flow: object
    state: str
    deadline: float
    binding: _Binding = field(repr=False)
    reconnect: bool


def _read_private(path: Path) -> dict:
    # NONBLOCK lets fstat reject a FIFO without first waiting for its writer.
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'r', encoding='utf-8') as source:
        info = os.fstat(source.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077
                or info.st_uid not in (0, os.getuid()) or info.st_size > 65536):
            raise ValueError('Invalid private configuration')
        value = json.load(source)
    if not isinstance(value, dict):
        raise ValueError('Invalid private configuration')
    return value


def _client(config: dict) -> dict:
    installed = config.get('installed')
    if 'web' in config or not isinstance(installed, dict):
        raise ValueError('Invalid Desktop client')
    if (installed.get('auth_uri') not in _AUTH_URIS
            or installed.get('token_uri') != _TOKEN_URI):
        raise ValueError('Invalid Google endpoints')
    for key in ('client_id', 'client_secret'):
        value = installed.get(key)
        if not isinstance(value, str) or not value.strip() or value != value.strip():
            raise ValueError('Invalid Desktop client')
    return installed


def _binding(client: dict, metadata: dict) -> _Binding:
    if not isinstance(metadata, dict):
        raise ValueError('Missing connection binding')
    account = metadata.get('account')
    if (not isinstance(account, str) or not account.strip() or account != account.strip()
            or len(account) > 320 or any(ord(char) < 32 for char in account)):
        raise ValueError('Invalid connection account')
    folder_id = DriveColdStore._id(metadata.get('folder_id'))
    return _Binding(client, account, folder_id)


def _load_bindings(data_dir: Path) -> _Binding:
    bootstrap = data_dir / 'drive-client.json'
    connection = data_dir / 'drive-oauth.json'
    bindings = []
    try:
        if bootstrap.exists() or bootstrap.is_symlink():
            config = _read_private(bootstrap)
            bindings.append(_binding(_client(config), config.get('memento')))
        if connection.exists() or connection.is_symlink():
            # Reuse the private connection's credential/scope/binding validation.
            _read_private(connection)
            previous = load_connection(connection, data_dir=data_dir)
            client = dict(client_id=previous.get('client_id'),
                          client_secret=previous.get('client_secret'),
                          auth_uri=_AUTH_URI, token_uri=previous.get('token_uri', _TOKEN_URI))
            bindings.append(_binding(_client({'installed': client}), previous.get('memento')))
    except Exception:
        raise DriveAuthorizationError(503) from None
    if not bindings:
        raise DriveAuthorizationError(503)
    if any(not bindings[0].matches(other) for other in bindings[1:]):
        raise DriveAuthorizationError(409)
    try:
        check_binding(data_dir, bindings[0].client['client_id'], bindings[0].account, bindings[0].folder_id)
    except (ValueError, OSError):
        raise DriveAuthorizationError(409) from None
    return bindings[0]


def configure_client(client_json: Path, account: str, folder_id: str, data_dir: Path):
    """Provision an owner-supplied original Desktop client, without Google calls."""
    data_dir = Path(data_dir)
    try:
        supplied = _read_private(Path(client_json))
        installed = _client(supplied)
        binding = _binding(installed, dict(account=account, folder_id=folder_id))
        if 'memento' in supplied:
            declared = _binding(installed, supplied['memento'])
            if not binding.matches(declared):
                raise DriveAuthorizationError(409)
    except DriveAuthorizationError:
        raise
    except Exception:
        raise DriveAuthorizationError(400) from None
    try:
        check_binding(data_dir, binding.client['client_id'], binding.account, binding.folder_id)
    except (ValueError, OSError):
        raise DriveAuthorizationError(409) from None
    if any((data_dir / name).exists() or (data_dir / name).is_symlink()
           for name in ('drive-client.json', 'drive-oauth.json')):
        if not binding.matches(_load_bindings(data_dir)):
            raise DriveAuthorizationError(409)
    try:
        _publish(data_dir / 'drive-client.json', dict(installed=installed,
                 memento=dict(account=binding.account, folder_id=binding.folder_id)))
    except Exception:
        raise DriveAuthorizationError(503) from None


def _close_after_failure(resource):
    # Preserve the original sanitized failure if cleanup itself also fails.
    try:
        resource.close()
    except Exception:
        pass


class DriveAuthorization:
    def __init__(self, data_dir: Path, install: Callable[[DriveColdStore, dict, bool], None],
                 *, clock=time.monotonic):
        self._data_dir = Path(data_dir)
        self._install = install
        self._clock = clock
        self._lock = threading.Lock()
        self._pending = None
        self._closed = False

    def _discard(self):
        pending, self._pending = self._pending, None
        if pending is not None:
            try:
                pending.flow.oauth2session.close()
            except Exception:
                raise DriveAuthorizationError(502) from None

    def begin(self, callback_port: int, reconnect: bool = False) -> dict:
        with self._lock:
            if self._closed:
                raise DriveAuthorizationError(503)
            if (type(callback_port) is not int or not 1 <= callback_port <= 65535
                    or type(reconnect) is not bool):
                raise DriveAuthorizationError(400)
            if self._pending is not None:
                if self._clock() < self._pending.deadline:
                    raise DriveAuthorizationError(409)
                self._discard()
            binding = _load_bindings(self._data_dir)
            try:
                from google_auth_oauthlib.flow import InstalledAppFlow
            except ImportError:
                raise DriveAuthorizationError(503) from None
            flow = None
            try:
                # Only these validated fields reach the SDK, never arbitrary client options.
                client = dict(client_id=binding.client['client_id'],
                              client_secret=binding.client['client_secret'],
                              auth_uri=_AUTH_URI, token_uri=_TOKEN_URI)
                state = secrets.token_urlsafe(32)
                flow = InstalledAppFlow.from_client_config({'installed': client}, scopes=SCOPES,
                    state=state, autogenerate_code_verifier=True,
                    redirect_uri=f'http://127.0.0.1:{callback_port}/')
                # A redirect must never forward a code/client secret to another endpoint.
                flow.oauth2session.max_redirects = 0
                flow.oauth2session.trust_env = False
                url, returned_state = flow.authorization_url(state=state, access_type='offline',
                    prompt='consent', login_hint=binding.account, include_granted_scopes='false')
                if returned_state != state:
                    raise DriveAuthorizationError(502)
                self._pending = _Pending(flow, state, self._clock() + _TTL, binding, reconnect)
                return dict(authorization_url=url, state=state, expires_in=_TTL)
            except Exception:
                if flow is not None:
                    _close_after_failure(flow.oauth2session)
                raise DriveAuthorizationError(502) from None

    def finish(self, state: str, code: str | None = None, error: str | None = None) -> dict:
        with self._lock:
            if self._closed:
                raise DriveAuthorizationError(503)
            if not isinstance(state, str) or not 16 <= len(state) <= 512 or not state.isascii():
                raise DriveAuthorizationError(400)
            pending = self._pending
            if pending is None:
                raise DriveAuthorizationError(410)
            if self._clock() >= pending.deadline:
                self._discard()
                raise DriveAuthorizationError(410)
            if not secrets.compare_digest(state, pending.state):
                raise DriveAuthorizationError(400)
            # Only the genuine state may consume the one pending grant, including rejection.
            self._pending = None
            service = backend = None
            flow_closed = False
            try:
                if ((code is None) == (error is None)
                        or (code is not None and (not isinstance(code, str) or not code.strip()
                                                  or len(code) > 4096))
                        or (error is not None and (not isinstance(error, str) or not error.strip()
                                                   or len(error) > 256))):
                    raise DriveAuthorizationError(400)
                if error is not None:
                    raise DriveAuthorizationError(400)
                self._check_binding(pending.binding)
                from oauthlib.oauth2 import OAuth2Error
                from googleapiclient.discovery import build
                try:
                    pending.flow.fetch_token(code=code, timeout=30)
                except (OAuth2Error, Warning):
                    raise DriveAuthorizationError(400) from None
                credentials = pending.flow.credentials
                granted = credentials.granted_scopes
                if granted is None:
                    granted = credentials.scopes
                if isinstance(granted, str):
                    granted = granted.split()
                if (set(granted or []) != set(SCOPES) or not credentials.refresh_token
                        or not credentials.token):
                    raise DriveAuthorizationError(400)
                service = build('drive', 'v3', credentials=credentials, cache_discovery=False)
                try:
                    backend = DriveColdStore(pending.binding.folder_id, pending.binding.account,
                                             service=service)
                except ValueError:
                    raise DriveAuthorizationError(400) from None
                config = json.loads(credentials.to_json())
                config.update(type='authorized_user', scopes=list(SCOPES),
                              memento=dict(account=pending.binding.account,
                                           folder_id=pending.binding.folder_id))
                pending.flow.oauth2session.close()
                flow_closed = True
                # An operator may have replaced or removed a binding during consent/network I/O.
                self._check_binding(pending.binding)
                try:
                    self._install(backend, config, pending.reconnect)
                except DriveAuthorizationError:
                    raise
                except Exception:
                    raise DriveAuthorizationError(502) from None
                backend = service = None  # Ownership transfers only after install returns.
                return {'status': 'connected'}
            except DriveAuthorizationError:
                raise
            except ImportError:
                raise DriveAuthorizationError(503) from None
            except Exception:
                raise DriveAuthorizationError(502) from None
            finally:
                if backend is not None:
                    _close_after_failure(backend)
                elif service is not None:
                    _close_after_failure(service)
                if not flow_closed:
                    _close_after_failure(pending.flow.oauth2session)

    def _check_binding(self, expected):
        if not expected.matches(_load_bindings(self._data_dir)):
            raise DriveAuthorizationError(409)

    def close(self):
        with self._lock:
            self._closed = True
            self._discard()
