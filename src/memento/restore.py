"""Disaster recovery needs only memento's Drive grant, never the lost database."""

import argparse
import json
from pathlib import Path

from .cold import DriveColdStore
from .cold_manager import ColdManager


def main():
    parser = argparse.ArgumentParser(description='List or restore verified personal Drive backups')
    parser.add_argument('--connection', type=Path, required=True,
                        help='Memento private drive-oauth.json (also used by the API)')
    parser.add_argument('--backup-id')
    parser.add_argument('--destination', type=Path)
    parser.add_argument('--allow-incomplete', action='store_true',
                        help='Explicitly recover metadata and available artifacts from a backup with recorded gaps')
    args = parser.parse_args()
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
