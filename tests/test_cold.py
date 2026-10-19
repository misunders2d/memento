import hashlib
import io
import json
import re
import shutil
from pathlib import Path
from threading import Event, Thread
from urllib.parse import parse_qs, urlsplit

import pytest

from memento.artifacts import Artifacts
from memento.blobs import BlobStore
from memento.cold import ColdObject, DriveColdStore, _hashes
from memento.cold_manager import ColdManager
from memento.graph import Graph
from memento.store import Store


class DirectoryColdFixture:
    """Test-only cold storage with real files, never a production fallback."""
    def __init__(self, root):
        self.root = root
        root.mkdir(mode=0o700, exist_ok=True)
        self.interrupt = False

    def upload(self, path, name, mime, checkpoint, save_checkpoint):
        sha, md5, size = _hashes(path)
        object_id = sha
        destination = self.root / object_id
        if not destination.exists():
            shutil.copyfile(path, destination)
        assert _hashes(destination) == (sha, md5, size)
        named = self.root / name
        if not named.exists():
            named.hardlink_to(destination)
        save_checkpoint(dict(object_id=object_id, sha256=sha, size=size))
        if self.interrupt:
            self.interrupt = False
            raise ConnectionError('Interrupted after cold copy, before reference flip')
        return ColdObject(object_id, size, sha, md5)

    def download(self, object_id, destination, sha256):
        destination = Path(destination)
        assert _hashes(self.root / object_id)[0] == sha256
        shutil.copyfile(self.root / object_id, destination)

    def list_backups(self):
        result = []
        for path in self.root.glob('memory-*.sqlite'):
            name = path.name[7:-7]
            gap_count = 0
            if name.startswith('incomplete-'):
                _, count, name = name.split('-', 2)
                gap_count = int(count)
            backup_id, digest = name.rsplit('-', 1)
            result.append(dict(id=backup_id, sha256=digest, remote_id=digest, size=path.stat().st_size,
                               gap_count=gap_count, complete=gap_count == 0))
        return result





@pytest.fixture
def cold(tmp_path):
    data = tmp_path / 'data'
    store = Store(data)
    graph = Graph(store)
    blobs = BlobStore(data / 'blobs')
    artifacts = Artifacts(store, blobs)
    backend = DirectoryColdFixture(tmp_path / 'remote')
    manager = ColdManager(artifacts, backend, data)
    yield graph, manager, backend
    store.close()


def test_interrupted_transfer_keeps_hot_copy_and_retry_commits_then_fetches(cold):
    graph, manager, backend = cold
    original = b'original artifact\x00' * 1024
    blob = manager.blobs.put(io.BytesIO(original))
    manager.artifacts.register(blob)
    backend.interrupt = True
    with pytest.raises(ConnectionError):
        manager.promote(blob['sha256'])
    assert manager.artifacts.get(blob['sha256'])['tier'] == 'hot'
    assert manager.blobs.open(blob['sha256']).read() == original
    assert (backend.root / blob['sha256']).read_bytes() == original
    result = manager.promote(blob['sha256'])
    assert result['tier'] == 'cold'
    assert not manager.blobs.path(blob['sha256']).exists()
    assert manager.fetch(blob['sha256']).read_bytes() == original


def test_backup_restore_preserves_graph_evidence_and_hot_artifacts(cold, tmp_path):
    graph, manager, backend = cold
    original = b'never lose hot media before its archive age'
    blob = manager.blobs.put(io.BytesIO(original))
    manager.artifacts.register(blob)
    episode, _ = graph.store.capture(dict(source='test', event_id='1', conversation_id='c',
        kind='user', text='an exact assertion', occurred_at=None, metadata={}))
    node = graph.remember(command_id='assertion', actor='user', why='explicit', type='knowledge',
        text='an exact assertion', scope=['personal'],
        evidence=[dict(episode_id=episode['id'], quote='an exact assertion')])
    backup = manager.backup()
    restored = tmp_path / 'restored'
    manager.restore(backup['id'], restored)
    recovered = Store(restored)
    try:
        assert recovered.get(episode['id'])['text'] == 'an exact assertion'
        assert Graph(recovered).history(node['id'])[0]['why'] == 'explicit'
        assert BlobStore(restored / 'blobs').open(blob['sha256']).read() == original
    finally:
        recovered.close()
    assert manager.artifacts.get(blob['sha256'])['tier'] == 'hot'


def test_hot_artifact_fetch_is_independent_of_blocked_backup_upload(cold, monkeypatch):
    graph, manager, backend = cold
    original = b'photo must remain available while Drive is stalled'
    blob = manager.blobs.put(io.BytesIO(original))
    manager.artifacts.register(blob)
    started, release, fetched = Event(), Event(), Event()
    upload = backend.upload
    errors, content = [], []
    def blocked_upload(*args, **kwargs):
        started.set()
        if not release.wait(5):
            raise TimeoutError('Fixture release deadline')
        return upload(*args, **kwargs)
    monkeypatch.setattr(backend, 'upload', blocked_upload)
    def backup():
        try:
            manager.backup()
        except Exception as error:
            errors.append(error)
    def fetch():
        try:
            content.append(manager.fetch(blob['sha256']).read_bytes())
        except Exception as error:
            errors.append(error)
        finally:
            fetched.set()
    copying = Thread(target=backup)
    copying.start()
    retrieving = None
    try:
        assert started.wait(2)
        retrieving = Thread(target=fetch)
        retrieving.start()
        available = fetched.wait(2)
    finally:
        release.set()
        copying.join(5)
        if retrieving is not None:
            retrieving.join(5)
    assert available
    assert errors == []
    assert content == [original]


def test_failed_snapshot_creation_removes_unregistered_local_copy(cold, monkeypatch):
    graph, manager, backend = cold
    original = b'original retained through failed local snapshot'
    blob = manager.blobs.put(io.BytesIO(original))
    manager.artifacts.register(blob)
    def broken_snapshot(path, backup_id):
        path.write_bytes(b'partial sqlite bytes')
        raise OSError('Synthetic local snapshot failure')
    monkeypatch.setattr(manager, '_snapshot', broken_snapshot)
    with pytest.raises(OSError):
        manager.backup()
    assert list((manager.data_dir / 'backups').glob('*.sqlite')) == []
    assert manager.blobs.open(blob['sha256']).read() == original


def test_complete_disk_loss_restore_uses_only_remote_storage(tmp_path):
    data = tmp_path / 'lost-disk'
    store = Store(data)
    Graph(store)
    backend = DirectoryColdFixture(tmp_path / 'remote')
    manager = ColdManager(Artifacts(store, BlobStore(data / 'blobs')), backend, data)
    blob = manager.blobs.put(io.BytesIO(b'irreplaceable recent voice note'))
    manager.artifacts.register(blob)
    episode, _ = store.capture(dict(source='test', event_id='disk-loss', conversation_id='c',
        kind='user', text='preserve exact evidence', occurred_at=None, metadata={}))
    try:
        backup = manager.backup()
    finally:
        store.close()
    shutil.rmtree(data)
    del manager, backend, store
    remote_only = DirectoryColdFixture(tmp_path / 'remote')
    destination = tmp_path / 'recovered'
    ColdManager.restore_from(remote_only, backup['id'], destination)
    recovered = Store(destination)
    try:
        assert recovered.get(episode['id'])['text'] == 'preserve exact evidence'
        assert BlobStore(destination / 'blobs').open(blob['sha256']).read() == b'irreplaceable recent voice note'
    finally:
        recovered.close()


class DriveTransport:
    """Synthetic HTTP boundary underneath the real official Drive SDK."""
    def __init__(self):
        self.data = bytearray()
        self.size = 0
        self.disconnect = False
        self.expired = False
        self.bad_checksum = False
        self.started = 0
        self.queries = 0
    def metadata(self):
        return dict(id='remote_object', size=str(len(self.data)),
                    md5Checksum=hashlib.md5(self.data).hexdigest(),
                    sha256Checksum='0'*64 if self.bad_checksum else hashlib.sha256(self.data).hexdigest())
    def close(self):
        pass
    def request(self, uri, method='GET', body=None, headers=None, **kwargs):
        import httplib2
        def response(data=None, status=200, headers=None, raw=None):
            return (httplib2.Response(dict(status=str(status), **(headers or {}))),
                    raw if raw is not None else json.dumps(data or {}).encode())
        url = urlsplit(uri)
        headers = {key.lower(): value for key, value in (headers or {}).items()}
        if method == 'GET':
            if url.path.endswith('/about'):
                return response({'user': {'emailAddress': 'personal@example.test'}})
            if url.path.endswith('/files/folder'):
                return response(dict(id='folder', mimeType='application/vnd.google-apps.folder', ownedByMe=True))
            if parse_qs(url.query).get('alt') == ['media']:
                return response(raw=bytes(self.data), headers={'content-length': str(len(self.data))})
            return response(self.metadata())
        if method == 'POST':
            assert json.loads(body)['parents'] == ['folder']
            self.size = int(headers['x-upload-content-length'])
            self.data.clear()
            self.started += 1
            return response(headers={'location': 'https://www.googleapis.com/upload/session'})
        assert method == 'PUT'
        if self.expired:
            self.expired = False
            return response(status=404)
        data = body.read() if hasattr(body, 'read') else body
        if data:
            match = re.fullmatch(r'bytes (\d+)-(\d+)/(\d+)', headers['content-range'])
            assert match and int(match[1]) == len(self.data)
            assert int(match[2]) + 1 == len(self.data) + len(data)
            self.data.extend(data)
            if self.disconnect:
                self.disconnect = False
                raise ConnectionError('Server accepted bytes but acknowledgement was lost')
        else:
            self.queries += 1
        if len(self.data) == self.size:
            return response(self.metadata())
        return response(status=308, headers={'range': f'bytes=0-{len(self.data)-1}'} if self.data else {})


def drive_backend(transport):
    discovery = pytest.importorskip('googleapiclient.discovery')
    service = discovery.build('drive', 'v3', http=transport, cache_discovery=False)
    return DriveColdStore('folder', 'personal@example.test', service=service)


def test_resumable_upload_queries_server_after_lost_acknowledgement(tmp_path, monkeypatch):
    monkeypatch.setattr('memento.cold.CHUNK', 256 * 1024)
    content = bytes(range(256)) * 2500
    source = tmp_path / 'source'
    source.write_bytes(content)
    transport = DriveTransport()
    backend = drive_backend(transport)
    checkpoint = {}
    def save(value):
        checkpoint.clear()
        checkpoint.update(value)
    transport.disconnect = True
    with pytest.raises(ConnectionError):
        backend.upload(source, 'artifact', 'application/octet-stream', None, save)
    result = backend.upload(source, 'artifact', 'application/octet-stream', checkpoint, save)
    assert bytes(transport.data) == content
    assert result.sha256 == hashlib.sha256(content).hexdigest()
    assert checkpoint['object_id'] == result.id
    assert transport.queries == 1 and transport.started == 1
    backend.close()


def test_drive_checksum_mismatch_never_acknowledges_verified_copy(tmp_path):
    source = tmp_path / 'source'
    source.write_bytes(b'original')
    transport = DriveTransport()
    transport.bad_checksum = True
    backend = drive_backend(transport)
    states = []
    with pytest.raises(ValueError, match='SHA-256 mismatch'):
        backend.upload(source, 'artifact', 'application/octet-stream', None, states.append)
    assert all('object_id' not in state for state in states)
    assert source.read_bytes() == b'original'
    backend.close()


def test_expired_sdk_upload_restarts_safely_on_next_attempt(tmp_path, monkeypatch):
    errors = pytest.importorskip('googleapiclient.errors')
    monkeypatch.setattr('memento.cold.CHUNK', 256 * 1024)
    source = tmp_path / 'source'
    source.write_bytes(bytes(range(256)) * 2500)
    transport = DriveTransport()
    backend = drive_backend(transport)
    transport.disconnect = True
    with pytest.raises(ConnectionError):
        backend.upload(source, 'artifact', 'application/octet-stream', None, lambda state: None)
    transport.expired = True
    with pytest.raises(errors.HttpError):
        backend.upload(source, 'artifact', 'application/octet-stream', None, lambda state: None)
    result = backend.upload(source, 'artifact', 'application/octet-stream', None, lambda state: None)
    assert transport.started == 2 and result.sha256 == _hashes(source)[0]
    backend.close()


def test_process_restart_copies_afresh_instead_of_trusting_stale_progress(tmp_path, monkeypatch):
    monkeypatch.setattr('memento.cold.CHUNK', 256 * 1024)
    source = tmp_path / 'source'
    source.write_bytes(bytes(range(256)) * 2500)
    transport = DriveTransport()
    backend = drive_backend(transport)
    transport.disconnect = True
    with pytest.raises(ConnectionError):
        backend.upload(source, 'artifact', 'application/octet-stream', None, lambda state: None)
    backend.close()
    restarted = drive_backend(transport)
    sha, _, size = _hashes(source)
    result = restarted.upload(source, 'artifact', 'application/octet-stream',
        dict(sha256=sha, size=size, offset=256 * 1024), lambda state: None)
    assert transport.started == 2 and result.sha256 == sha and bytes(transport.data) == source.read_bytes()
    restarted.close()


def test_corrupt_artifact_does_not_block_db_backup_and_partial_recovery_is_explicit(cold, tmp_path):
    graph, manager, backend = cold
    lost = manager.blobs.put(io.BytesIO(b'lost original photo'))
    good = manager.blobs.put(io.BytesIO(b'intact original voice note'))
    manager.artifacts.register(lost)
    manager.artifacts.register(good)
    manager.blobs.path(lost['sha256']).write_bytes(b'corrupt')
    episode, _ = graph.store.capture(dict(source='fixture', event_id='metadata-survives', conversation_id='c',
        kind='user', text='Keep the exact assertion even if a photo is lost.', occurred_at=None, metadata={}))
    node = graph.remember(command_id='surviving-assertion', actor='fixture', why='explicit assertion',
        type='knowledge', text=episode['text'], scope=['personal'],
        evidence=[dict(episode_id=episode['id'], quote=episode['text'])])
    backup = manager.backup()
    assert backup['complete'] is False and backup['gap_count'] == 1
    assert _hashes(backend.root / backup['remote_id'])[0] == backup['sha256']
    assert not Path(backup['path']).exists()
    manager.blobs.path(lost['sha256']).write_bytes(b'lost original photo')
    repaired = manager.backup()
    assert repaired['complete'] and repaired['gap_count'] == 0
    original_dir = manager.data_dir
    graph.store.close()
    shutil.rmtree(original_dir)
    remote_only = DirectoryColdFixture(backend.root)
    destination = tmp_path / 'partial-recovery'
    with pytest.raises(ValueError, match='incomplete'):
        ColdManager.restore_from(remote_only, backup['id'], destination)
    assert not destination.exists()
    result = ColdManager.restore_from(remote_only, backup['id'], destination, allow_incomplete=True)
    assert result['complete'] is False and result['gap_count'] == 1
    recovered = Store(destination)
    try:
        assert recovered.get(episode['id'])['text'] == episode['text']
        assert Graph(recovered).history(node['id'])[-1]['text'] == episode['text']
        assert BlobStore(destination / 'blobs').open(good['sha256']).read() == b'intact original voice note'
        assert not BlobStore(destination / 'blobs').path(lost['sha256']).exists()
        gaps = recovered._db.execute('SELECT sha256 FROM backup_gaps WHERE backup_id=?', (backup['id'],)).fetchall()
        assert [row['sha256'] for row in gaps] == [lost['sha256']]
    finally:
        recovered.close()


def test_sweep_failure_does_not_starve_other_artifacts_and_retries_after_backoff(cold, monkeypatch):
    graph, manager, backend = cold
    original = b'repairable original photograph'
    bad = manager.blobs.put(io.BytesIO(original))
    good = manager.blobs.put(io.BytesIO(b'healthy older photograph'))
    manager.artifacts.register(bad)
    manager.artifacts.register(good)
    graph.store._db.execute("UPDATE blobs SET created_at='2001-01-01T00:00:00+00:00'")
    graph.store._db.execute("UPDATE blobs SET created_at='2000-01-01T00:00:00+00:00' WHERE sha256=?", (bad['sha256'],))
    manager.blobs.path(bad['sha256']).write_bytes(b'corrupt')
    first = manager.sweep(min_bytes=1, limit=1)
    assert first['promoted'] == [] and first['failure_count'] == 1
    second = manager.sweep(min_bytes=1, limit=1)
    assert [row['sha256'] for row in second['promoted']] == [good['sha256']]
    assert not manager.blobs.path(good['sha256']).exists()
    manager.sweep(min_bytes=1, limit=1)
    failure = graph.store._db.execute('SELECT attempts,next_attempt FROM cold_failures WHERE sha256=?', (bad['sha256'],)).fetchone()
    assert failure['attempts'] == 1
    manager.blobs.path(bad['sha256']).write_bytes(original)
    monkeypatch.setattr('memento.cold_manager.time.time', lambda: failure['next_attempt'] + 1)
    recovered = manager.sweep(min_bytes=1, limit=1)
    assert [row['sha256'] for row in recovered['promoted']] == [bad['sha256']]
    assert recovered['failure_count'] == 0
    assert manager.fetch(bad['sha256']).read_bytes() == original


def test_protected_artifact_does_not_hide_unprotected_items_beyond_sweep_limit(cold):
    graph, manager, backend = cold
    protected = manager.blobs.put(io.BytesIO(b'important old document'))
    ordinary = manager.blobs.put(io.BytesIO(b'ordinary old document'))
    manager.artifacts.register(protected)
    manager.artifacts.register(ordinary)
    episode, _ = graph.store.capture(dict(source='fixture', event_id='important', conversation_id='c',
        kind='user', text='Important document.', occurred_at=None, metadata={}))
    node = graph.remember(command_id='important-document', actor='fixture', why='explicit source',
        type='media', text='Important document.', scope=['personal'],
        evidence=[dict(episode_id=episode['id'], quote='Important document.')])
    graph.store._db.execute('INSERT INTO media_files VALUES (?,?,?,?,?)',
        (node['id'], protected['sha256'], 'important.txt', 'document', graph._node(node['id'])['commit_id']))
    graph.store._db.execute('UPDATE node_stats SET importance=0.9 WHERE node_id=?', (node['id'],))
    graph.store._db.execute("UPDATE blobs SET created_at='2001-01-01T00:00:00+00:00'")
    graph.store._db.execute("UPDATE blobs SET created_at='2000-01-01T00:00:00+00:00' WHERE sha256=?", (protected['sha256'],))
    result = manager.sweep(min_bytes=1, limit=1)
    assert [row['sha256'] for row in result['promoted']] == [ordinary['sha256']]
    assert manager.blobs.path(protected['sha256']).read_bytes() == b'important old document'
