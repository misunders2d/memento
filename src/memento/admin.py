"""Human review over a private Unix socket, never the agent HTTP/MCP surface."""

import argparse
import asyncio
import json
import os
import socket
import struct
import sys
from pathlib import Path
from uuid import uuid4

from .store import ConflictError


async def serve_admin(graph, data_dir: Path, services):
    connections = set()
    path = data_dir / 'admin.sock'
    if path.exists():
        if not path.is_socket():
            raise RuntimeError('Refusing to replace a non-socket admin path')
        path.unlink()  # the data directory's exclusive writer lock is already held

    async def handle(reader, writer):
        try:
            peer = writer.get_extra_info('socket')
            _, uid, _ = struct.unpack('3i', peer.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
            if uid not in (0, os.getuid()):
                raise PermissionError('Administrative identity required')
            raw = await asyncio.wait_for(reader.readline(), timeout=10)
            request = json.loads(raw)
            if request['operation'] == 'list':
                result = await asyncio.to_thread(graph.pending_reviews)
            elif request['operation'] == 'backup':
                if services.cold is None:
                    raise ValueError('Drive cold storage is not configured')
                result = await asyncio.to_thread(services.cold.backup)
            elif request['operation'] == 'export':
                from .portable import export_archive
                result = await asyncio.to_thread(export_archive, data_dir, Path(request['output']),
                                                 cold=services.cold)
            elif request['operation'] == 'compact':
                result = await asyncio.to_thread(graph.store.compact)
            elif request['operation'] == 'history_status':
                result = await asyncio.to_thread(services.processing.history_status)
            elif request['operation'] == 'history_park':
                result = await asyncio.to_thread(services.processing.park_history)
            elif request['operation'] in ('approve', 'reject'):
                result = await asyncio.to_thread(graph._decide_review,
                    command_id=request['command_id'], actor=f'human:uid:{uid}',
                    why=request['why'], proposal_id=request['proposal_id'],
                    decision=request['operation'])
            elif request['operation'] == 'inventory':
                result = await asyncio.to_thread(graph._merge_inventory)
            elif request['operation'] == 'merge_preview':
                result = await asyncio.to_thread(graph._merge_preview, request['cluster'],
                    request.get('into'), request.get('exclude', []), request.get('include', []))
            elif request['operation'] == 'merge':
                result = await asyncio.to_thread(graph._merge, command_id=request['command_id'],
                    actor=f'human:uid:{uid}', why=request['why'], cluster=request['cluster'],
                    into=request['into'], exclude=request['exclude'], token=request['token'],
                    include=request.get('include', []))
            elif request['operation'] == 'forget_preview':
                result = await asyncio.to_thread(graph._forget_preview, request['target'])
            elif request['operation'] == 'forget':
                result = await asyncio.to_thread(graph._forget, command_id=request['command_id'],
                    actor=f'human:uid:{uid}', why=request['why'], target=request['target'], token=request['token'])
            else:
                raise ValueError('Unknown administrative operation')
            response = {'result': result}
        except Exception as error:
            response = {'error': str(error)}
        try:
            writer.write(json.dumps(response, ensure_ascii=True).encode() + b'\n')
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
    async def tracked(reader, writer):
        task = asyncio.current_task()
        connections.add(task)
        try:
            await handle(reader, writer)
        finally:
            connections.discard(task)

    server = await asyncio.start_unix_server(tracked, path=path, limit=1024 * 1024)
    path.chmod(0o600)
    return server, path, connections


def _request(data_dir, request):
    with socket.socket(socket.AF_UNIX) as client:
        client.settimeout(30)
        client.connect(str(data_dir / 'admin.sock'))
        client.sendall(json.dumps(request).encode() + b'\n')
        if request['operation'] in ('backup', 'export', 'compact', 'history_park'):
            # Verified snapshots, downloads, uploads and whole-backlog parking may exceed decision timeouts.
            client.settimeout(None)
        with client.makefile('rb') as response:
            return json.loads(response.readline(4 * 1024 * 1024))


def main():
    parser = argparse.ArgumentParser(description='Human-only Memento review and portable export console')
    parser.add_argument('--data-dir', type=Path, required=True)
    commands = parser.add_subparsers(dest='operation', required=True)
    commands.add_parser('list')
    commands.add_parser('inventory', help='Read-only same-type entities spelled alike; suspicion, not identity')
    merge = commands.add_parser('merge', help='Preview and confirm merging one inventory cluster')
    merge.add_argument('cluster')
    merge.add_argument('--into', default=None, help='Survivor member (default: first listed)')
    merge.add_argument('--exclude', action='append', default=[], help='Member that stays a distinct identity')
    merge.add_argument('--include', action='append', default=[],
                       help='Active same-type spelling variant chosen to join this cluster')
    merge.add_argument('--why', required=True)
    merge.add_argument('--command-id', default=None)
    commands.add_parser('backup')
    commands.add_parser('compact', help='Reclaim free database pages; pauses service writes')
    history = commands.add_parser('history', help='One-time cutover: park the current unprocessed backlog as '
                                                  'searchable raw history, or read the parking state')
    history.add_argument('action', choices=['park', 'status'])
    export = commands.add_parser('export', help='Export database, Drive references and local-only payloads privately')
    export.add_argument('--output', type=Path, required=True, help='New file in an owner-private directory')
    for operation in ('approve', 'reject'):
        command = commands.add_parser(operation)
        command.add_argument('proposal_id')
        command.add_argument('--why', required=True)
        command.add_argument('--command-id', default=None)
    forget = commands.add_parser('forget')
    forget.add_argument('target')
    forget.add_argument('--why', required=True)
    forget.add_argument('--command-id', default=None)
    args = parser.parse_args()
    request = {'operation': args.operation}
    if args.operation == 'export':
        if not sys.stdin.isatty():
            parser.error('Plaintext archive export requires an interactive human terminal')
        output = args.output.absolute()
        expected = f'export private archive to {output}'
        if input(f'This plaintext archive contains private memory and Drive references; remote files stay on Drive. Type {expected!r} to confirm: ') != expected:
            parser.exit(1, 'Not confirmed; nothing changed.\n')
        request['output'] = str(output)
    elif args.operation == 'history':
        # An operational policy change on the owner socket, like compact: no review
        # confirmation. Park is a one-time cutover with no undo command.
        request['operation'] = 'history_' + args.action
    elif args.operation not in ('list', 'inventory', 'backup', 'compact'):
        if not sys.stdin.isatty():
            parser.error('Administrative decisions require an interactive human terminal')
        if args.operation == 'merge':
            preview = _request(args.data_dir, dict(operation='merge_preview', cluster=args.cluster,
                                                   into=args.into, exclude=args.exclude, include=args.include))
            if 'error' in preview:
                parser.exit(1, preview['error'] + '\n')
            plan = preview['result']
            print(json.dumps(plan, ensure_ascii=False, indent=2))
            expected = f"merge {len(plan['absorbed'])} into {plan['survivor']['id']}"
            # Pin the previewed survivor so a changed default cannot alter the confirmed plan.
            request.update(cluster=args.cluster, into=plan['survivor']['id'], exclude=args.exclude,
                           include=args.include, token=plan['token'])
        elif args.operation == 'forget':
            preview = _request(args.data_dir, dict(operation='forget_preview', target=args.target))
            if 'error' in preview:
                parser.exit(1, preview['error'] + '\n')
            print(json.dumps(preview['result'], ensure_ascii=False, indent=2))
            expected = f'forget {args.target} from active memory'
            request.update(target=args.target, token=preview['result']['token'])
        else:
            expected = f'{args.operation} {args.proposal_id}'
            request['proposal_id'] = args.proposal_id
        if input(f'Type {expected!r} to confirm (raw history and backups are retained): ') != expected:
            parser.exit(1, 'Not confirmed; nothing changed.\n')
        request.update(why=args.why, command_id=args.command_id or uuid4().hex)
    result = _request(args.data_dir, request)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if 'error' in result:
        raise SystemExit(1)
    if args.operation == 'compact' and not result['result']['complete']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
