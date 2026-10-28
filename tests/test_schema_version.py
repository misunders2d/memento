"""Database header compatibility: pre-frame readers must refuse frame-capable storage."""

import hashlib
import io
import json
import sqlite3
import tarfile
from contextlib import closing

import pytest

from memento.blobs import BlobStore
from memento.cold_manager import ColdManager
from memento.graph import Graph
from memento.portable import export_archive, import_archive
from memento.schema import LEGACY_VERSION, PARKING_VERSION, VERSION
from memento.store import Store
from test_cold import DirectoryColdFixture
from test_raw_text import episode


def legacy_guard(path):
    """The pre-frame readers' exact header check (store.py and portable._registered before 35e0962).

    Those binaries accepted only user_version 1; past this check they returned ''
    for frame-compressed records instead of failing. This reproduces the guard;
    it does not execute a historical binary.
    """
    with closing(sqlite3.connect(path)) as db:
        version = db.execute('PRAGMA user_version').fetchone()[0]
    if version != 1:
        raise RuntimeError(f'Unsupported database schema version: {version}')
    return version


def header(path):
    with closing(sqlite3.connect(path)) as db:
        return db.execute('PRAGMA user_version').fetchone()[0]


def test_loop_lineage_indexes_upgrade_without_changing_evidence_or_header(tmp_path):
    data = tmp_path / 'data'
    store, original = compressed_store(data)
    store.close()
    path = data / 'memory.sqlite'
    before = physical_rows(path)
    with closing(sqlite3.connect(path)) as db:
        db.execute('DROP INDEX node_versions_superseded_by')
        db.execute('DROP INDEX candidate_decisions_promoted_target')
        db.commit()
        schema_before = db.execute('PRAGMA schema_version').fetchone()[0]
    store = Store(data)
    assert store.get(original['id']) == original
    assert store._db.execute('PRAGMA user_version').fetchone()[0] == VERSION
    schema_after = store._db.execute('PRAGMA schema_version').fetchone()[0]
    assert schema_after == schema_before + 2
    store.close()
    reopened = Store(data)
    assert reopened._db.execute('PRAGMA schema_version').fetchone()[0] == schema_after
    reopened.close()
    assert physical_rows(path) == before


def set_header(path, version):
    with closing(sqlite3.connect(path)) as db:
        db.execute(f'PRAGMA user_version={version}')


def physical_rows(path):
    with closing(sqlite3.connect(path)) as db:
        return {table: [tuple(row) for row in db.execute(f'SELECT * FROM {table} ORDER BY rowid')]
                for table in ('episodes', 'raw_frames')}


def compressed_store(data):
    store = Store(data)
    original, _ = store.capture(episode('Exact raw Ω source. ' * 100))
    assert store.compress_raw_step()
    assert store._db.execute('SELECT count(*) FROM raw_frames').fetchone()[0] >= 1
    return store, original


def test_fresh_and_compressed_storage_carry_the_header_pre_frame_readers_refuse(tmp_path):
    data = tmp_path / 'data'
    store, original = compressed_store(data)
    try:
        assert store._db.execute('PRAGMA user_version').fetchone()[0] == VERSION != LEGACY_VERSION
    finally:
        store.close()
    with pytest.raises(RuntimeError, match=f'Unsupported database schema version: {VERSION}'):
        legacy_guard(data / 'memory.sqlite')
    reopened = Store(data)
    try:
        assert reopened.get(original['id']) == original
    finally:
        reopened.close()


def test_legacy_header_with_frames_is_marked_atomically_and_reads_exactly(tmp_path, monkeypatch):
    import memento.store as store_module

    data = tmp_path / 'data'
    store, original = compressed_store(data)
    store.close()
    path = data / 'memory.sqlite'
    before = physical_rows(path)
    # A database converted by the currently deployed build: frames under header 1.
    set_header(path, LEGACY_VERSION)
    assert legacy_guard(path) == LEGACY_VERSION, 'the gap: a pre-frame reader accepts this file'
    upgrade = store_module.upgrade_captured_media

    def interrupted(db):
        upgrade(db)
        raise RuntimeError('Synthetic interruption after physical upgrades')

    with monkeypatch.context() as patch:
        patch.setattr(store_module, 'upgrade_captured_media', interrupted)
        with pytest.raises(RuntimeError, match='Synthetic interruption'):
            Store(data)
    assert header(path) == LEGACY_VERSION, 'the header change must roll back with the upgrade'
    reopened = Store(data)
    try:
        assert reopened._db.execute('PRAGMA user_version').fetchone()[0] == VERSION
        assert reopened.get(original['id']) == original
    finally:
        reopened.close()
    assert physical_rows(path) == before
    with pytest.raises(RuntimeError, match='Unsupported database schema version'):
        legacy_guard(path)


def _future_database(path):
    with closing(sqlite3.connect(path)) as db:
        db.execute('CREATE TABLE blobs (sha256 TEXT, size INTEGER, remote_id TEXT, tier TEXT)')
        # Header 3 is the explicit historical-parking header; the first unknown one follows it.
        db.execute(f'PRAGMA user_version={PARKING_VERSION + 1}')
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_unknown_future_header_is_refused_before_any_file_change(tmp_path):
    data = tmp_path / 'data'
    data.mkdir(mode=0o700)
    path = data / 'memory.sqlite'
    digest = _future_database(path)
    with pytest.raises(RuntimeError, match=f'Unsupported database schema version: {PARKING_VERSION + 1}'):
        Store(data)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
    assert not (data / 'memory.sqlite-wal').exists() and not (data / 'memory.sqlite-shm').exists()

    payload = path.read_bytes()
    manifest = dict(format='memento-portable', version=2, drive=None,
                    files={'memory.sqlite': dict(sha256=hashlib.sha256(payload).hexdigest(), size=len(payload))})
    archive = tmp_path / 'future.tar'
    with tarfile.open(archive, 'w', format=tarfile.USTAR_FORMAT) as tar:
        for name, content in [('manifest.json', json.dumps(manifest).encode()), ('memory.sqlite', payload)]:
            member = tarfile.TarInfo(name)
            member.size = len(content)
            tar.addfile(member, io.BytesIO(content))
    destination = tmp_path / 'restored'
    with pytest.raises(ValueError, match='Unsupported database schema version'):
        import_archive(archive, destination)
    assert not destination.exists()

    remote = DirectoryColdFixture(tmp_path / 'remote')
    (remote.root / digest).write_bytes(payload)
    (remote.root / f'memory-future-{digest}.sqlite').hardlink_to(remote.root / digest)
    downloads = []
    original_download = remote.download

    def counted(object_id, target, sha256):
        downloads.append(object_id)
        original_download(object_id, target, sha256)

    remote.download = counted
    with pytest.raises(ValueError, match='Unsupported database schema version'):
        ColdManager.restore_from(remote, 'future', tmp_path / 'drive-restored')
    assert downloads == [digest], 'only the database itself is fetched before the header check'


@pytest.mark.parametrize('legacy_header', [True, False])
def test_portable_restore_of_compressed_storage_ends_at_the_current_header(tmp_path, legacy_header):
    data = tmp_path / 'data'
    store, original = compressed_store(data)
    store.close()
    path = data / 'memory.sqlite'
    if legacy_header:
        # An archive exported by the currently deployed build: frames under header 1.
        set_header(path, LEGACY_VERSION)
    before = physical_rows(path)
    archive = tmp_path / 'portable.tar'
    export_archive(data, archive)
    destination = tmp_path / 'restored'
    result = import_archive(archive, destination)
    assert ('user_version' in result['rebased']) == legacy_header
    assert header(destination / 'memory.sqlite') == VERSION
    with pytest.raises(RuntimeError, match='Unsupported database schema version'):
        legacy_guard(destination / 'memory.sqlite')
    assert physical_rows(destination / 'memory.sqlite') == before
    restored = Store(destination)
    try:
        assert restored.get(original['id']) == original
    finally:
        restored.close()
    assert physical_rows(path) == before, 'export never changes the source'
    assert header(path) == (LEGACY_VERSION if legacy_header else VERSION)


def test_drive_backup_restore_of_compressed_storage_ends_at_the_current_header(tmp_path):
    data = tmp_path / 'data'
    store, original = compressed_store(data)
    Graph(store)
    backend = DirectoryColdFixture(tmp_path / 'remote')
    manager = ColdManager(store.artifacts, backend, data)
    blob = manager.blobs.put(io.BytesIO(b'hot artifact bytes'))
    manager.artifacts.register(blob)
    before = physical_rows(data / 'memory.sqlite')
    try:
        backup = manager.backup()
    finally:
        store.close()
    destination = tmp_path / 'drive-restored'
    ColdManager.restore_from(backend, backup['id'], destination)
    assert header(destination / 'memory.sqlite') == VERSION
    assert physical_rows(destination / 'memory.sqlite') == before
    recovered = Store(destination)
    try:
        assert recovered.get(original['id']) == original
        assert BlobStore(destination / 'blobs').open(blob['sha256']).read() == b'hot artifact bytes'
    finally:
        recovered.close()
