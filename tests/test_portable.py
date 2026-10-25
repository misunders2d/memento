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
from memento.drive_connect import SCOPES, _publish

DRIVE = dict(client_id='synthetic-client', account='owner@example.test', folder_id='original_folder')


class ReadOnlyColdFiles:
    """Export must never touch even an available remote backend."""
    def __init__(self, root):
        self.root = root

    def download(self, object_id, destination, sha256):
        pytest.fail('Migration export must not download Drive payloads')

    def upload(self, *args, **kwargs):
        pytest.fail('Migration export must not upload Drive payloads')


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
    db.execute("INSERT INTO memory_vectors(rowid,embedding,kind) VALUES (?,?,'node')", (seq, vector))
    db.execute('''INSERT INTO jobs(id,job_key,kind,payload,state,available_at,attempts,owner,lease_until)
                  VALUES ('queued','media/queued','media',?,'leased',0,1,'old-owner',123)''',
               (json.dumps({'sha256': sha}),))
    _publish(data / 'drive-oauth.json', dict(type='authorized_user', client_id=DRIVE['client_id'],
        client_secret='DO NOT EXPORT DRIVE CREDENTIALS', refresh_token='DO NOT EXPORT REFRESH TOKEN',
        scopes=SCOPES, memento=dict(account=DRIVE['account'], folder_id=DRIVE['folder_id'])))
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


def test_mixed_hot_cold_roundtrip_retains_drive_references_without_downloads(portable, tmp_path):
    p = portable
    before = _database_content(p.store._db)
    source_files = {name: hashlib.sha256((p.data / name).read_bytes()).hexdigest()
                    for name in ('memory.sqlite', 'memory.sqlite-wal')}
    result = export_archive(p.data, p.archive, cold=p.cold)
    assert result['complete'] and result['blob_count'] == 2
    assert result['version'] == 2 and not result['self_contained'] and result['remote_reference_count'] == 1
    assert stat.S_IMODE(p.archive.stat().st_mode) == 0o600
    assert _database_content(p.store._db) == before
    assert {name: hashlib.sha256((p.data / name).read_bytes()).hexdigest() for name in source_files} == source_files
    assert not p.blobs.path(p.infos[1]['sha256']).exists()
    with tarfile.open(p.archive) as archive:
        local = p.infos[0]['sha256']
        assert set(archive.getnames()) == {'manifest.json', 'memory.sqlite', f'blobs/{local[:2]}/{local[2:]}'}
        assert json.load(archive.extractfile('manifest.json'))['drive'] == DRIVE
    assert b'DO NOT EXPORT' not in p.archive.read_bytes()
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
        assert {row['sha256']: (row['tier'], row['remote_id']) for row in recovered._db.execute('SELECT * FROM blobs')} == {
            p.infos[0]['sha256']: ('hot', None), p.infos[1]['sha256']: ('cold', 'old-object')}
        assert recovered.get(p.episode['id']) == p.episode
        assert Graph(recovered).history(p.node['id']) == p.graph.history(p.node['id'])
        for info, content in zip(p.infos[:1], p.contents[:1]):
            path = BlobStore(destination / 'blobs').verify(info['sha256'])
            assert path.read_bytes() == content
            assert stat.S_IMODE(path.stat().st_mode) == 0o600
        assert not BlobStore(destination / 'blobs').path(p.infos[1]['sha256']).exists()
        assert json.loads((destination / 'drive-binding.json').read_text()) == DRIVE
        assert stat.S_IMODE((destination / 'drive-binding.json').stat().st_mode) == 0o600
        reexported = export_archive(destination, tmp_path / 'again.tar')
        assert reexported['drive'] == DRIVE and reexported['remote_reference_count'] == 1
        hits = recovered._db.execute('''SELECT rowid FROM memory_vectors
            WHERE embedding MATCH ? AND k=1''', (p.vector,)).fetchall()
        assert [tuple(row) for row in hits] == [(1,)]
        assert stat.S_IMODE((destination / 'memory.sqlite').stat().st_mode) == 0o600
    finally:
        recovered.close()


@pytest.mark.parametrize('has_index', [True, False])
def test_restore_uses_consumer_date_semantics_without_changing_source_rows(portable, tmp_path, monkeypatch, has_index):
    from memento import portable as module
    p = portable
    stamp = '2026-10-20T09:36:35.999859+00:00'
    native = sqlite3.connect(':memory:')
    try:
        consumer_value = native.execute('SELECT julianday(?)', (stamp,)).fetchone()[0]
        # Observed 3.46.1 and 3.54.0 values differ at this millisecond boundary.
        producer_value = (2461333.9004166666 if consumer_value != 2461333.9004166666
                          else 2461333.900416655)
        def producer_date(value):
            return producer_value if value == stamp else native.execute('SELECT julianday(?)', (value,)).fetchone()[0]
        p.store._db.create_function('julianday', 1, producer_date, deterministic=True)
        episode, _ = p.store.capture(dict(source='synthetic', event_id='boundary', conversation_id='thread',
            kind='user', text='Synthetic timestamp boundary.', occurred_at=stamp, metadata={}))
        p.store._db.execute('REINDEX episodes_activity')
        if not has_index:
            p.store._db.execute('DROP INDEX episodes_activity')
        before = _database_content(p.store._db)
        connect = module._connect
        def producer_connect(path, **kwargs):
            db = connect(path, **kwargs)
            db.create_function('julianday', 1, producer_date, deterministic=True)
            return db
        with monkeypatch.context() as patch:
            patch.setattr(module, '_connect', producer_connect)
            export_archive(p.data, p.archive, cold=p.cold)
        digest = hashlib.sha256(p.archive.read_bytes()).hexdigest()
        destination = tmp_path / 'different-sqlite-runtime'
        assert import_archive(p.archive, destination)['complete']
        db = connect(destination / 'memory.sqlite', readonly=True)
        try:
            assert db.execute('PRAGMA integrity_check').fetchall() == [('ok',)]
            actual = _database_content(db)
            for table, rows in before.items():
                if table not in ('blobs', 'cold_transfers', 'cold_failures', 'backups'):
                    assert actual[table] == rows, table
            assert bool(db.execute("SELECT 1 FROM sqlite_schema WHERE type='index' AND name='episodes_activity'").fetchone()) == has_index
            if has_index:
                values = db.execute('''SELECT id,julianday(coalesce(occurred_at,captured_at))
                    FROM episodes INDEXED BY episodes_activity
                    ORDER BY julianday(coalesce(occurred_at,captured_at)) DESC,seq DESC''').fetchall()
                assert dict(values)[episode['id']] == consumer_value != producer_value
        finally:
            db.close()
        assert _database_content(p.store._db) == before
        assert hashlib.sha256(p.archive.read_bytes()).hexdigest() == digest
    finally:
        native.close()


def test_large_database_header_reaches_strict_reader_without_large_payload_io(portable, tmp_path, monkeypatch):
    """Exercise real export headers and reader admission, not an 8 GiB roundtrip."""
    from memento import portable as module
    p = portable
    size = 2**33  # First size that cannot be represented in a USTAR size field.
    name = 'memory.sqlite'
    metadata = module._file_metadata
    monkeypatch.setattr(module, '_file_metadata', lambda path: dict(metadata(path), size=size))

    class StopAtPayload(Exception):
        pass

    headers, reads = [], []
    serialize = tarfile.TarInfo.tobuf
    copy = tarfile.copyfileobj
    def header(member, *args, **kwargs):
        value = serialize(member, *args, **kwargs)
        if member.name == name and member.size == size:
            headers.append(value)
        return value
    def bounded_copy(source, destination, length=None, **kwargs):
        if length == size:
            reads.append(len(source.read(1)))
            raise StopAtPayload
        return copy(source, destination, length, **kwargs)
    monkeypatch.setattr(tarfile.TarInfo, 'tobuf', header)
    monkeypatch.setattr(tarfile, 'copyfileobj', bounded_copy)
    with pytest.raises(StopAtPayload):
        export_archive(p.data, p.archive, cold=p.cold)
    assert reads == [1] and len(headers) == 1 and len(headers[0]) == tarfile.BLOCKSIZE
    parsed = tarfile.TarInfo.frombuf(headers[0], 'utf-8', 'strict')
    assert parsed.name == name and parsed.size == size and parsed.type == tarfile.REGTYPE
    assert headers[0][124] == 0x80 and not parsed.pax_headers and not parsed.linkname
    assert not p.archive.exists() and not list(tmp_path.glob('.memento-export-*'))

    files = {name: dict(size=size, sha256='0' * 64)}
    manifest = json.dumps(dict(format='memento-portable', version=2, files=files, drive=None)).encode()
    first = tarfile.TarInfo('manifest.json')
    first.size = len(manifest)
    probe = tmp_path / 'large-header.tar'
    probe.write_bytes(first.tobuf(format=tarfile.USTAR_FORMAT) + manifest +
        b'\0' * (-len(manifest) % tarfile.BLOCKSIZE) + headers[0] + b'x')
    assert probe.stat().st_size < 4096
    def admitted(source, destination, expected):
        assert destination.name == name
        # Header guards have passed; do not ask BufferedReader to prefetch a
        # deliberately absent giant body. Complete byte validation is separate.
        assert expected['size'] == size and source.tell() == 0
        raise StopAtPayload
    monkeypatch.setattr(module, '_copy_verified', admitted)
    destination = tmp_path / 'large-restored'
    with pytest.raises(StopAtPayload):
        import_archive(probe, destination)
    assert not destination.exists() and not list(tmp_path.glob('.memento-import-*'))


@pytest.mark.parametrize('damage', ['missing-hot', 'corrupt-hot'])
def test_export_refuses_any_payload_gap_without_publication(portable, tmp_path, damage):
    p = portable
    before = _database_content(p.store._db)
    if damage == 'missing-hot':
        p.blobs.path(p.infos[0]['sha256']).unlink()
    elif damage == 'corrupt-hot':
        p.blobs.path(p.infos[0]['sha256']).write_bytes(b'corrupt')
    with pytest.raises((ValueError, OSError)):
        export_archive(p.data, p.archive, cold=p.cold)
    assert not p.archive.exists()
    assert _database_content(p.store._db) == before
    assert not list(tmp_path.glob('.memento-export-*'))


def test_terabyte_remote_references_and_cached_remote_bytes_are_not_copied(portable, tmp_path):
    p = portable
    p.store._db.execute('UPDATE blobs SET size=? WHERE remote_id IS NOT NULL', (4 * 1024**4,))
    cached = p.store.artifacts.register(p.blobs.put(io.BytesIO(b'cached remote copy')))
    p.store._db.execute("UPDATE blobs SET remote_id='cached-object' WHERE sha256=?", (cached['sha256'],))
    local = p.store.artifacts.register(p.blobs.put(io.BytesIO(b'L' * 3072)))
    # Keep exactly one NULL-reference payload, including an unexpected cold tier.
    p.store._db.execute("UPDATE blobs SET remote_id='also-verified' WHERE sha256=?", (p.infos[0]['sha256'],))
    p.store._db.execute("UPDATE blobs SET tier='cold' WHERE sha256=?", (local['sha256'],))
    result = export_archive(p.data, p.archive, cold=p.cold)
    assert result['remote_reference_count'] == 3 and result['included_blob_count'] == 1
    with tarfile.open(p.archive) as archive:
        assert set(archive.getnames()) == {'manifest.json', 'memory.sqlite',
                                         f"blobs/{local['sha256'][:2]}/{local['sha256'][2:]}"}
    destination = tmp_path / 'restored-huge-refs'
    restored = import_archive(p.archive, destination)
    assert not restored['self_contained'] and restored['drive'] == DRIVE
    db = sqlite3.connect(destination / 'memory.sqlite')
    try:
        assert db.execute('SELECT size,remote_id,tier FROM blobs WHERE sha256=?', (p.infos[1]['sha256'],)).fetchone() == (
            4 * 1024**4, 'old-object', 'cold')
        assert db.execute('SELECT remote_id,tier FROM blobs WHERE sha256=?', (cached['sha256'],)).fetchone() == ('cached-object', 'cold')
        assert db.execute('SELECT remote_id,tier FROM blobs WHERE sha256=?', (local['sha256'],)).fetchone() == (None, 'hot')
    finally:
        db.close()
    assert BlobStore(destination / 'blobs').verify(local['sha256']).read_bytes() == b'L' * 3072
    assert not BlobStore(destination / 'blobs').path(cached['sha256']).exists()


@pytest.mark.parametrize('binding', ['missing', 'ambiguous'])
def test_remote_references_require_unambiguous_original_binding(portable, binding):
    p = portable
    if binding == 'missing':
        (p.data / 'drive-oauth.json').unlink()
    else:
        _publish(p.data / 'drive-binding.json', dict(DRIVE, folder_id='different_folder'))
    with pytest.raises(ValueError):
        export_archive(p.data, p.archive, cold=p.cold)
    assert not p.archive.exists()


def test_independent_v1_full_archive_remains_locally_restorable(portable, tmp_path):
    p = portable
    snapshot = tmp_path / 'legacy.sqlite'
    target = sqlite3.connect(snapshot)
    try:
        p.store._db.backup(target)
        target.execute('PRAGMA journal_mode=DELETE')
        target.execute('DROP INDEX episodes_status_cover_v1')
    finally:
        target.close()
    payloads = {'memory.sqlite': snapshot.read_bytes()}
    payloads.update({f"blobs/{info['sha256'][:2]}/{info['sha256'][2:]}": content
                     for info, content in zip(p.infos, p.contents)})
    manifest = dict(format='memento-portable', version=1, files={
        name: dict(sha256=hashlib.sha256(content).hexdigest(), size=len(content)) for name, content in payloads.items()})
    with tarfile.open(p.archive, 'w', format=tarfile.USTAR_FORMAT) as archive:
        for name, content in [('manifest.json', json.dumps(manifest).encode()), *payloads.items()]:
            member = tarfile.TarInfo(name)
            member.size = len(content)
            archive.addfile(member, io.BytesIO(content))
    destination = tmp_path / 'legacy-restored'
    result = import_archive(p.archive, destination)
    assert result['version'] == 1 and result['self_contained'] and result['remote_reference_count'] == 0
    assert not (destination / 'drive-binding.json').exists()
    db = sqlite3.connect(destination / 'memory.sqlite')
    try:
        assert db.execute("SELECT count(*) FROM blobs WHERE tier<>'hot' OR remote_id IS NOT NULL").fetchone()[0] == 0
    finally:
        db.close()
    for info, content in zip(p.infos, p.contents):
        assert BlobStore(destination / 'blobs').verify(info['sha256']).read_bytes() == content
    recovered = Store(destination)
    try:
        assert recovered.get(p.episode['id']) == p.episode
        assert recovered._db.execute("SELECT 1 FROM sqlite_schema WHERE name='episodes_status_cover_v1'").fetchone()
    finally:
        recovered.close()


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
