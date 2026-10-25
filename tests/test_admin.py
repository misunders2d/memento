"""Owner-only physical maintenance on isolated SQLite data, without providers."""

import asyncio
import json
import os
import stat
import struct
import sys
from types import SimpleNamespace

import pytest

from memento import admin
from memento.graph import Graph
from memento.store import Store


def test_owner_socket_compact_reclaims_disk_and_preserves_raw_fts_graph_vectors(tmp_path, monkeypatch):
    store = Store(tmp_path)
    graph = Graph(store)
    try:
        payload = dict(source='fixture', event_id='raw', conversation_id='thread', kind='native_record',
            text='Ω exact source\r\n' * 50000, occurred_at=None, metadata={'extraction_mode': 'skip'})
        raw, _ = store.capture(payload)
        projected, _ = store.capture(dict(payload, event_id='projection', kind='user',
            text='Exact searchable source.', metadata={'extraction_mode': 'message'}))
        node = graph.remember(command_id='one', actor='fixture', why='Exact source', type='knowledge',
            text='Exact searchable source.', scope=['personal'],
            evidence=[dict(episode_id=projected['id'], quote='Exact searchable source.')])
        vector = struct.pack('<768f', 1.0, *([0.0] * 767))
        store._db.execute("INSERT INTO vector_items(seq,target,kind,version) VALUES(101,?,'node',1)", (node['id'],))
        store._db.execute("INSERT INTO memory_vectors(rowid,embedding,kind) VALUES(101,?,'node')", (vector,))
        while store.compress_raw_step():
            pass
        store._db.execute('CREATE TABLE discarded(payload BLOB)')
        store._db.execute('INSERT INTO discarded VALUES(zeroblob(2097152))')
        store._db.execute('DROP TABLE discarded')
        store._db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        before = graph.history(node['id'])
        run = __import__('subprocess').run
        def persistent_scratch(*args, **kwargs):
            scratch = kwargs['env']['SQLITE_TMPDIR']
            assert os.path.commonpath((scratch, str(tmp_path))) == str(tmp_path)
            assert os.path.isdir(scratch)
            return run(*args, **kwargs)
        monkeypatch.setattr('memento.store.subprocess.run', persistent_scratch)
        async def request():
            server, path, connections = await admin.serve_admin(graph, tmp_path, SimpleNamespace(cold=None))
            try:
                assert stat.S_IMODE(path.stat().st_mode) == 0o600
                return await asyncio.to_thread(admin._request, tmp_path, dict(operation='compact'))
            finally:
                server.close()
                await server.wait_closed()
                if connections:
                    await asyncio.gather(*tuple(connections))
                path.unlink(missing_ok=True)
        result = asyncio.run(request())['result']
        assert result['complete'] and result['checkpoint_busy'] == 0
        assert result['after']['database'] < result['before']['database']
        assert result['after']['wal'] == 0
        assert store.get(raw['id']) == raw and store.capture(payload) == (raw, False)
        assert graph.history(node['id']) == before
        assert store.search('Exact searchable')[0]['id'] == projected['id']
        assert [row[0] for row in store._db.execute(
            'SELECT rowid FROM memory_vectors WHERE embedding MATCH ? AND k=1', (vector,))] == [101]
        assert not list(tmp_path.glob('.memento-compact-*'))
    finally:
        store.close()


def test_owner_socket_merge_preview_carries_included_variants(tmp_path):
    store = Store(tmp_path)
    graph = Graph(store)
    try:
        text = 'Orion and Orion warehouse.'
        episode, _ = store.capture(dict(source='fixture', event_id='entities', conversation_id='thread',
            kind='user', text=text, occurred_at=None, metadata={}))
        ids = [graph.remember(command_id=f'entity-{index}', actor='fixture', why='Synthetic entity', type='project',
            text=name, scope=['professional'], evidence=[dict(episode_id=episode['id'], quote=text)])['id']
            for index, name in enumerate(('Orion', 'orion', 'Orion warehouse'))]
        cluster = graph._merge_inventory()['clusters'][0]['cluster']
        async def request():
            server, path, connections = await admin.serve_admin(graph, tmp_path, SimpleNamespace(cold=None))
            try:
                return await asyncio.to_thread(admin._request, tmp_path, dict(operation='merge_preview',
                    cluster=cluster, into=None, exclude=[], include=[ids[2]]))
            finally:
                server.close()
                await server.wait_closed()
                if connections:
                    await asyncio.gather(*tuple(connections))
                path.unlink(missing_ok=True)
        result = asyncio.run(request())['result']
        assert result['included'] == [ids[2]]
        assert result == graph._merge_preview(cluster, include=[ids[2]])
    finally:
        store.close()


@pytest.mark.parametrize('complete', [True, False])
def test_compact_cli_requires_owner_socket_but_no_decision_tty(tmp_path, monkeypatch, capsys, complete):
    requests = []
    monkeypatch.setattr(sys, 'argv', ['memento-admin', '--data-dir', str(tmp_path), 'compact'])
    monkeypatch.setattr(sys.stdin, 'isatty', lambda: False)
    monkeypatch.setattr('builtins.input', lambda *a: pytest.fail('Physical compact must not request a decision confirmation'))
    monkeypatch.setattr(admin, '_request', lambda path, request:
        requests.append((path, request)) or dict(result=dict(complete=complete, checkpoint_busy=int(not complete))))
    if complete:
        admin.main()
    else:
        with pytest.raises(SystemExit) as exit:
            admin.main()
        assert exit.value.code == 1
    assert requests == [(tmp_path, dict(operation='compact'))]
    assert json.loads(capsys.readouterr().out)['result']['complete'] == complete
