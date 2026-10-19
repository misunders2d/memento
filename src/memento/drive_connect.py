"""Memento-owned drive.file grant; laptop loopback authorization also works via SSH."""

import argparse
import json
import os
import stat
import sys
import tempfile
from pathlib import Path

SCOPES = ['https://www.googleapis.com/auth/drive.file']


def load_connection(path: Path) -> dict:
    """Read only the declared private connection, never ambient Google credentials."""
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'r', encoding='utf-8') as source:
        permissions = os.fstat(source.fileno())
        if (not stat.S_ISREG(permissions.st_mode) or permissions.st_mode & 0o077
                or permissions.st_uid not in (0, os.getuid())):
            raise ValueError('Drive connection must be an owner-private regular file (0600)')
        config = json.load(source)
    if not isinstance(config, dict) or config.get('type') != 'authorized_user':
        raise ValueError('Drive connection requires authorized-user credentials')
    if config.get('scopes') != SCOPES:
        raise ValueError('Drive connection must grant only drive.file')
    metadata = config.get('memento')
    if (not isinstance(metadata, dict) or not isinstance(metadata.get('account'), str)
            or not metadata['account'].strip() or not metadata.get('folder_id')):
        raise ValueError('Drive connection requires its verified account and dedicated folder')
    from .cold import DriveColdStore
    DriveColdStore._id(metadata['folder_id'])
    return config


def _publish(path: Path, config: dict):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, name = tempfile.mkstemp(prefix='.drive-oauth-', dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as target:
            os.fchmod(target.fileno(), 0o600)
            json.dump(config, target, ensure_ascii=True)
            target.write('\n')
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, path)
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        temporary.unlink(missing_ok=True)


def connect(client_json: Path, account: str, output: Path, *, no_browser=False, port=8766,
            folder_id: str | None = None):
    """Obtain a fresh explicit grant, retaining or explicitly recovering its folder."""
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    from .cold import DriveColdStore

    account = account.strip()
    if not account or not 1 <= port <= 65535:
        raise ValueError('An explicit account and valid loopback port are required')
    if folder_id is not None:
        folder_id = DriveColdStore._id(folder_id)
    previous = load_connection(output) if output.exists() or output.is_symlink() else None
    if previous and previous['memento']['account'].casefold() != account.casefold():
        raise ValueError('Reconnect must use the previously verified account')
    if previous and folder_id is not None and folder_id != previous['memento']['folder_id']:
        raise ValueError('Reconnect must use the previously verified folder')
    with client_json.open(encoding='utf-8') as source:
        client_config = json.load(source)
    if 'installed' not in client_config or 'web' in client_config:
        raise ValueError('Use a dedicated Google Desktop app OAuth client JSON')
    if previous and previous.get('client_id') != client_config['installed'].get('client_id'):
        raise ValueError('Reconnect must use the original OAuth client to retain folder access')
    flow = InstalledAppFlow.from_client_config(client_config, scopes=SCOPES,
                                               autogenerate_code_verifier=True)
    credentials = flow.run_local_server(host='127.0.0.1', port=port,
        open_browser=not no_browser, prompt='consent', login_hint=account,
        success_message='Authorization received. Return to memento for account and folder verification.')
    granted = credentials.granted_scopes if credentials.granted_scopes is not None else credentials.scopes
    if set(granted or []) != set(SCOPES) or not credentials.refresh_token:
        raise ValueError('A renewable drive.file-only grant is required')
    with build('drive', 'v3', credentials=credentials, cache_discovery=False) as service:
        DriveColdStore._verify_account(service, account)
        if previous:
            folder_id = previous['memento']['folder_id']
        if folder_id is None:
            folder = service.files().create(body={'name': 'Memento',
                'mimeType': 'application/vnd.google-apps.folder',
                'appProperties': {'memento': 'cold-storage'}}, fields='id').execute()
            folder_id = DriveColdStore._id(folder['id'])
        DriveColdStore._verify_folder(service, folder_id)
        config = json.loads(credentials.to_json())
        config.update(type='authorized_user', scopes=SCOPES,
                      memento={'account': account, 'folder_id': folder_id})
        _publish(output, config)
    return output


def main():
    parser = argparse.ArgumentParser(description='Connect memento to its own private Google Drive folder')
    subcommands = parser.add_subparsers(dest='command', required=True)
    command = subcommands.add_parser('connect', help='Authorize on a laptop, then copy the private output to the server')
    command.add_argument('--client-json', type=Path, required=True, help='Dedicated Desktop app OAuth client JSON')
    command.add_argument('--account', required=True, help='Exact Google account email; verified before saving')
    command.add_argument('--output', type=Path, required=True, help='Connection file; use DATA_DIR/drive-oauth.json')
    command.add_argument('--folder-id', help='Reuse an existing app-accessible Memento folder; never create a replacement')
    command.add_argument('--no-browser', action='store_true', help='Print the official loopback consent URL; use an SSH tunnel when remote')
    command.add_argument('--port', type=int, default=8766, help='Loopback callback port, also forwarded by an SSH tunnel (default: 8766)')
    configure = subcommands.add_parser('configure',
        help='Operator: provision the original client and existing folder for one-link authorization')
    configure.add_argument('--client-json', type=Path, required=True,
        help='Original private Desktop app OAuth client JSON')
    configure.add_argument('--account', required=True, help='Exact Google account to authorize')
    configure.add_argument('--folder-id', required=True, help='Existing app-accessible folder; never create another')
    configure.add_argument('--data-dir', type=Path, required=True, help='Service-owned private data directory')
    args = parser.parse_args()
    try:
        if args.command == 'configure':
            from .drive_auth import configure_client
            configure_client(args.client_json, args.account, args.folder_id, args.data_dir)
        else:
            connect(args.client_json, args.account, args.output, no_browser=args.no_browser,
                    port=args.port, folder_id=args.folder_id)
    except Exception as error:
        # OAuth exceptions can contain tokens or callback codes. Never echo them.
        print(f'Drive connection failed ({type(error).__name__}); connection setup did not complete.', file=sys.stderr)
        raise SystemExit(1) from None
    if args.command == 'configure':
        print('Original Desktop client and existing Drive folder configured for one-link authorization.')
    else:
        print('Drive account and dedicated folder verified; private connection saved.')


if __name__ == '__main__':
    main()
