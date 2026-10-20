"""Recover from an owner-exported file archive or a verified personal Drive backup."""

import argparse
import json
from pathlib import Path

from .cold import DriveColdStore
from .cold_manager import ColdManager


def main():
    parser = argparse.ArgumentParser(description='Restore a migration archive or personal Drive backup')
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--connection', type=Path,
                        help='Memento private drive-oauth.json (Drive restore mode only)')
    source.add_argument('--archive', type=Path, help='Owner archive; restore offline, then reconnect original Drive binding for remote files')
    parser.add_argument('--backup-id')
    parser.add_argument('--destination', type=Path)
    parser.add_argument('--allow-incomplete', action='store_true',
                        help='Explicitly recover metadata and available artifacts from a backup with recorded gaps')
    args = parser.parse_args()
    if args.archive:
        if not args.destination or args.backup_id or args.allow_incomplete:
            parser.error('--archive requires --destination and cannot use Drive backup options')
        from .portable import import_archive
        print(json.dumps(import_archive(args.archive, args.destination), ensure_ascii=True))
        return
    if bool(args.backup_id) != bool(args.destination):
        parser.error('--backup-id and --destination must be supplied together')
    if args.allow_incomplete and not args.backup_id:
        parser.error('--allow-incomplete requires --backup-id and --destination')
    backend = DriveColdStore.from_file(args.connection)
    try:
        if args.backup_id:
            result = ColdManager.restore_from(backend, args.backup_id, args.destination,
                                              allow_incomplete=args.allow_incomplete)
            print(json.dumps(result, ensure_ascii=True))
        else:
            print(json.dumps(backend.list_backups(), ensure_ascii=True))
    finally:
        backend.close()
