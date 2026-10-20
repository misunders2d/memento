import hashlib
import io
import json
import shutil
import sqlite3
import stat
import struct
import tarfile
from threading import RLock
from types import SimpleNamespace

import pytest

from memento.artifacts import Artifacts
from memento.blobs import BlobStore
from memento.graph import Graph
from memento.portable import export_archive, import_archive
from memento.store import Store


class ReadOnlyColdFiles:
    """A synthetic cold backend; exporting must never call upload or source fetch."""
    def __init__(self, root):
        self.root = root

    def download(self, object_id, destination, sha256):
        shutil.copyfile(self.root / object_id, destination)


def _rows(db, table):
    return sorted((tuple(row) for row in db.execute(f'SELECT * FROM "{table}"')), key=repr)


def _database_content(db):
    tables = [row[0] for row in db.execute(
        "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
    return {name: _rows(db, name) for name in tables}


@pytest.fixture
def portable(tmp_path):
    data = tmp_path / 'source'
    store = Store(data)
    graph = Graph(store)
    blobs = BlobStore(data / 'blobs')
    artifacts = Artifacts(store, blobs)
    remote = tmp_path / 'remote'
    remote.mkdir(mode=0o700)
    cold = SimpleNamespace(_mutex=RLock(), backend=ReadOnlyColdFiles(remote))
    contents = [b'hot original\x00\xff\r\n', b'cold original\x00' * 1000]
    infos = [artifacts.register(blobs.put(io.BytesIO(content))) for content in contents]
    sha = infos[1]['sha256']
    shutil.copyfile(blobs.path(sha), remote / 'old-object')
    blobs.path(sha).unlink()
    db = store._db
    db.execute("UPDATE blobs SET tier='cold',remote_id='old-object' WHERE sha256=?", (sha,))
    db.execute("INSERT INTO cold_transfers VALUES (?,?,'evicted','old-object','then')",
               (sha, json.dumps({'object_id': 'old-object', 'size': len(contents[1])})))
    db.execute("INSERT INTO cold_failures VALUES (?,2,'offline',123)", (sha,))
    db.execute("INSERT INTO blob_moves(sha256,phase,remote_id,at) VALUES (?,'evicted','old-object','then')", (sha,))
    db.execute("INSERT INTO backups VALUES ('old-backup','/old/host/backup.sqlite',?,1,'old-upload',?,'then')",
               ('f' * 64, json.dumps({'object_id': 'old-upload'})))
    db.execute("INSERT INTO backup_gaps VALUES ('old-backup',?,'missing','then')", ('e' * 64,))
    episode, _ = store.capture(dict(source='synthetic', event_id='one', conversation_id='thread',
        kind='user', text='Exact raw Ω\r\nKeep cold and hot evidence.', occurred_at=None,
        metadata={'source': 'original', 'nested': [1, True]}))
    evidence = [dict(episode_id=episode['id'], quote='Keep cold and hot evidence.')]
    node = graph.remember(command_id='memory', actor='human', why='exact evidence', type='knowledge',
                          text='Keep cold and hot evidence.', scope=['personal'], evidence=evidence)
    other = graph.remember(command_id='other', actor='human', why='related memory', type='knowledge',
                           text='Related evidence.', scope=['personal'], evidence=evidence)
    graph.link(command_id='link', actor='human', why='related', a=node['id'], b=other['id'],
               rel='related_to', evidence=evidence)
    graph.loop(action='open', id=node['id'], command_id='open')
    commit = db.execute('SELECT commit_id FROM node_versions WHERE id=?', (node['id'],)).fetchone()[0]
    db.execute('INSERT INTO media_files VALUES (?,?,?,?,?)', (node['id'], infos[0]['sha256'], 'original.bin', 'image', commit))
    db.execute('''INSERT INTO segments(id,node_id,modality,text,artifact_sha256,source_sha256)
                  VALUES ('segment',?,'text','exact extracted text',?,?)''',
               (node['id'], infos[1]['sha256'], infos[0]['sha256']))
    db.execute("INSERT INTO vector_model(singleton,model_id,revision,dimensions) VALUES (1,'synthetic','fixed',768)")
    seq = db.execute("INSERT INTO vector_items(target,kind,version) VALUES (?,'node',1)", (node['id'],)).lastrowid
    vector = struct.pack('<768f', 1.0, *([0.0] * 767))
    db.execute('INSERT INTO memory_vectors(rowid,embedding,current) VALUES (?,?,1)', (seq, vector))
    db.execute('''INSERT INTO jobs(id,job_key,kind,payload,state,available_at,attempts,owner,lease_until)
                  VALUES ('queued','media/queued','media',?,'leased',0,1,'old-owner',123)''',
               (json.dumps({'sha256': sha}),))
    (data / 'drive-oauth.json').write_text('DO NOT EXPORT DRIVE CREDENTIALS')
    (data / 'native-client.json').write_text('DO NOT EXPORT NATIVE IDENTITY')
    (data / 'models').mkdir()
    (data / 'models' / 'cache').write_text('DO NOT EXPORT MODEL CACHE')
    archive = tmp_path / 'portable.tar'
    try:
        yield SimpleNamespace(data=data, store=store, graph=graph, blobs=blobs, cold=cold,
                              remote=remote, infos=infos, contents=contents, archive=archive,
                              episode=episode, node=node, vector=vector)
    finally:
        store.close()


def test_mixed_hot_cold_roundtrip_is_complete_readonly_and_grant_independent(portable, tmp_path):
    p = portable
    before = _database_content(p.store._db)
    source_files = {name: hashlib.sha256((p.data / name).read_bytes()).hexdigest()
                    for name in ('memory.sqlite', 'memory.sqlite-wal')}
    result = export_archive(p.data, p.archive, cold=p.cold)
    assert result['complete'] and result['blob_count'] == 2
    assert stat.S_IMODE(p.archive.stat().st_mode) == 0o600
    assert _database_content(p.store._db) == before
    assert {name: hashlib.sha256((p.data / name).read_bytes()).hexdigest() for name in source_files} == source_files
    assert not p.blobs.path(p.infos[1]['sha256']).exists()
    with tarfile.open(p.archive) as archive:
        assert set(archive.getnames()) == {'manifest.json', 'memory.sqlite', *(
            f"blobs/{info['sha256'][:2]}/{info['sha256'][2:]}" for info in p.infos)}
    # Take the source and backend out of the restore path entirely.
    shutil.rmtree(p.remote)
    destination = tmp_path / 'new-instance'
    restored = import_archive(p.archive, destination)
    assert restored['complete']
    recovered = Store(destination)
    try:
        actual = _database_content(recovered._db)
        for table, rows in before.items():
            if table not in ('blobs', 'cold_transfers', 'cold_failures', 'backups'):
                assert actual[table] == rows, table
        for table in ('cold_transfers', 'cold_failures', 'backups'):
            assert actual[table] == []
        assert all(row[0] == 'hot' and row[1] is None for row in recovered._db.execute('SELECT tier,remote_id FROM blobs'))
        assert recovered.get(p.episode['id']) == p.episode
        assert Graph(recovered).history(p.node['id']) == p.graph.history(p.node['id'])
        for info, content in zip(p.infos, p.contents):
            path = BlobStore(destination / 'blobs').verify(info['sha256'])
            assert path.read_bytes() == content
            assert stat.S_IMODE(path.stat().st_mode) == 0o600
        hits = recovered._db.execute('''SELECT rowid FROM memory_vectors
            WHERE embedding MATCH ? AND k=1''', (p.vector,)).fetchall()
        assert [tuple(row) for row in hits] == [(1,)]
        assert stat.S_IMODE((destination / 'memory.sqlite').stat().st_mode) == 0o600
    finally:
        recovered.close()


@pytest.mark.parametrize('damage', ['missing-hot', 'corrupt-hot', 'missing-cold', 'corrupt-cold', 'no-backend'])
def test_export_refuses_any_payload_gap_without_publication(portable, tmp_path, damage):
    p = portable
    before = _database_content(p.store._db)
    if damage == 'missing-hot':
        p.blobs.path(p.infos[0]['sha256']).unlink()
    elif damage == 'corrupt-hot':
        p.blobs.path(p.infos[0]['sha256']).write_bytes(b'corrupt')
    elif damage == 'missing-cold':
        (p.remote / 'old-object').unlink()
    elif damage == 'corrupt-cold':
        (p.remote / 'old-object').write_bytes(b'corrupt')
    with pytest.raises((ValueError, OSError)):
        export_archive(p.data, p.archive, cold=None if damage == 'no-backend' else p.cold)
    assert not p.archive.exists()
    assert _database_content(p.store._db) == before
    assert not list(tmp_path.glob('.memento-export-*'))


def _rewrite(source, destination, mutation):
    with tarfile.open(source) as archive:
        members = [(member, archive.extractfile(member).read()) for member in archive]
    members = mutation(members)
    with tarfile.open(destination, 'w', format=tarfile.USTAR_FORMAT) as archive:
        for member, payload in members:
            archive.addfile(member, io.BytesIO(payload) if member.isreg() else None)


@pytest.mark.parametrize('damage', ['tamper', 'missing', 'extra', 'traversal', 'duplicate', 'link',
                                   'manifest-gap', 'unregistered-blob', 'broken-database', 'trailing-archive'])
@pytest.mark.parametrize('existing', [False, True])
def test_import_refuses_unsafe_or_incomplete_archives_without_partial_publication(portable, tmp_path, damage, existing):
    p = portable
    export_archive(p.data, p.archive, cold=p.cold)
    bad = tmp_path / 'bad.tar'

    def mutation(members):
        if damage == 'tamper':
            member, payload = members[-1]
            members[-1] = member, bytes([payload[0] ^ 1]) + payload[1:]
        elif damage == 'missing':
            members.pop()
        elif damage in ('extra', 'traversal'):
            extra = tarfile.TarInfo('unexpected' if damage == 'extra' else '../escaped')
            members.append((extra, b''))
        elif damage == 'duplicate':
            members.append(members[-1])
        elif damage == 'link':
            member, _ = members[-1]
            member.type, member.linkname, member.size = tarfile.SYMTYPE, '/etc/passwd', 0
            members[-1] = member, b''
        elif damage in ('manifest-gap', 'unregistered-blob', 'broken-database'):
            manifest = json.loads(members[0][1])
            if damage == 'manifest-gap':
                member, _ = members.pop()
                del manifest['files'][member.name]
            elif damage == 'unregistered-blob':
                digest = hashlib.sha256(b'extra unregistered').hexdigest()
                member = tarfile.TarInfo(f'blobs/{digest[:2]}/{digest[2:]}')
                member.size = len(b'extra unregistered')
                members.append((member, b'extra unregistered'))
                manifest['files'][member.name] = dict(sha256=digest, size=member.size)
            else:
                member, payload = members[1]
                payload = b'not sqlite' + payload[10:]
                members[1] = member, payload
                manifest['files']['memory.sqlite']['sha256'] = hashlib.sha256(payload).hexdigest()
            payload = json.dumps(manifest).encode()
            members[0][0].size = len(payload)
            members[0] = members[0][0], payload
        return members

    _rewrite(p.archive, bad, mutation)
    if damage == 'trailing-archive':
        with bad.open('ab') as target, p.archive.open('rb') as source:
            shutil.copyfileobj(source, target)
    original = bad.read_bytes()
    destination = tmp_path / 'restored'
    if existing:
        destination.mkdir(mode=0o700)
        inode = destination.stat().st_ino
    with pytest.raises((ValueError, OSError, sqlite3.Error, tarfile.TarError)):
        import_archive(bad, destination)
    assert bad.read_bytes() == original
    if existing:
        assert destination.stat().st_ino == inode and list(destination.iterdir()) == []
    else:
        assert not destination.exists()
    assert not (tmp_path / 'escaped').exists()
    assert not list(tmp_path.glob('.memento-import-*'))


def test_export_does_not_overwrite_and_restore_refuses_nonempty_or_symlink_targets(portable, tmp_path):
    p = portable
    export_archive(p.data, p.archive, cold=p.cold)
    original = p.archive.read_bytes()
    with pytest.raises(ValueError, match='already exist'):
        export_archive(p.data, p.archive, cold=p.cold)
    assert p.archive.read_bytes() == original
    destination = tmp_path / 'existing'
    destination.mkdir(mode=0o700)
    marker = destination / 'writer.lock'
    marker.write_text('existing/live instance')
    with pytest.raises(ValueError, match='empty'):
        import_archive(p.archive, destination)
    assert marker.read_text() == 'existing/live instance'
    link = tmp_path / 'link'
    link.symlink_to(destination, target_is_directory=True)
    with pytest.raises(ValueError, match='symlink'):
        import_archive(p.archive, link)
    with pytest.raises(ValueError, match='symlink'):
        import_archive(p.archive, link / 'child')
    public = tmp_path / 'public'
    public.mkdir(mode=0o755)
    with pytest.raises(ValueError, match='private'):
        export_archive(p.data, public / 'unsafe.tar', cold=p.cold)
