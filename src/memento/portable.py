"""Owner-only, self-contained plaintext archives; never a remote backup or API route."""

import hashlib
import io
import json
import os
import re
import shutil
import sqlite3
import stat
import tarfile
import tempfile
from contextlib import nullcontext
from pathlib import Path

import sqlite_vec

from .blobs import BlobStore
from .raw_text import runs

_FORMAT = 'memento-portable'
_VERSION = 1
_MAX_MANIFEST = 64 * 1024 * 1024
_DIGEST = re.compile(r'[0-9a-f]{64}')


def _plain_path(path):
    """Do not resolve away symlinks, including symlinked ancestors."""
    path = Path(os.path.abspath(path))
    for part in (path, *path.parents):
        if part.is_symlink():
            raise ValueError('Archive paths and destinations must not use symlinks')
    return path


def _private_directory(path):
    info = path.stat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError('Directory must be owned by the current UID and private (0700)')
    return info


def _connect(path, *, readonly=False):
    db = sqlite3.connect(path.as_uri() + ('?mode=ro' if readonly else '?mode=rw'), uri=True)
    try:
        db.enable_load_extension(True)
        try:
            sqlite_vec.load(db)
        finally:
            db.enable_load_extension(False)
        return db
    except BaseException:
        db.close()
        raise


def _registered(db):
    """All registered payloads, including historical or currently unreferenced blobs."""
    if db.execute('PRAGMA user_version').fetchone()[0] != 1:
        raise ValueError('Unsupported portable database schema version')
    if db.execute('PRAGMA integrity_check').fetchall() != [('ok',)]:
        raise ValueError('Portable database integrity check failed')
    result = {}
    for sha256, size, remote_id in db.execute('SELECT sha256,size,remote_id FROM blobs'):
        if not isinstance(sha256, str) or not _DIGEST.fullmatch(sha256) or type(size) is not int or size < 0:
            raise ValueError('Invalid registered blob metadata')
        result[sha256] = dict(size=size, remote_id=remote_id)
    references = db.execute('''SELECT sha256 FROM media_files
        UNION SELECT artifact_sha256 FROM segments
        UNION SELECT source_sha256 FROM segments WHERE source_sha256 IS NOT NULL''')
    for (sha256,) in references:
        if sha256 not in result:
            raise ValueError('Database references an unregistered blob')
    for (payload,) in db.execute("SELECT payload FROM jobs WHERE kind='media'"):
        if json.loads(payload).get('sha256') not in result:
            raise ValueError('Media job references an unregistered blob')
    columns = {row[1] for row in db.execute('PRAGMA table_info(episodes)')}
    if 'text_runs' in columns:
        for references, full_sha in db.execute('SELECT text_runs,text_sha256 FROM episodes WHERE text_runs IS NOT NULL'):
            if not isinstance(full_sha, str) or not _DIGEST.fullmatch(full_sha):
                raise ValueError('Invalid raw text checksum')
            for _, sha256, length in runs(references):
                if sha256 not in result or result[sha256]['size'] != length:
                    raise ValueError('Raw source references an unregistered or incorrectly sized blob')
    return result


def _file_metadata(path):
    with path.open('rb') as source:
        return dict(size=os.fstat(source.fileno()).st_size,
                    sha256=hashlib.file_digest(source, 'sha256').hexdigest())


def _copy_verified(source, destination, expected):
    digest = hashlib.sha256()
    size = 0
    with destination.open('xb') as target:
        destination.chmod(0o600)
        while chunk := source.read(1024 * 1024):
            size += len(chunk)
            if size > expected['size']:
                raise ValueError('Archive payload size mismatch')
            digest.update(chunk)
            target.write(chunk)
        target.flush()
        os.fsync(target.fileno())
    if size != expected['size'] or digest.hexdigest() != expected['sha256']:
        raise ValueError('Archive payload checksum or size mismatch')


def _add(archive, name, path, size):
    member = tarfile.TarInfo(name)
    member.size = size
    member.mode = 0o600
    with path.open('rb') as source:
        archive.addfile(member, source)


def export_archive(data_dir: Path, output: Path, *, cold=None):
    """Read-only SQLite/WAL snapshot plus verified hot/cold bytes, without uploads.

    The administration socket supplies the owner boundary. The caller must not
    expose this function on the agent HTTP/MCP surface.
    """
    data_dir = _plain_path(data_dir)
    output = _plain_path(output)
    _private_directory(data_dir)
    _private_directory(output.parent)
    if os.path.lexists(output):
        raise ValueError('Archive output must not already exist')
    # Serialize against eviction, not ordinary evidence capture. Downloads go
    # only into staging, never through fetch(), which changes source hot storage.
    with cold._mutex if cold is not None else nullcontext():
        with tempfile.TemporaryDirectory(prefix='.memento-export-', dir=output.parent) as name:
            stage = Path(name)
            snapshot = stage / 'memory.sqlite'
            with snapshot.open('xb'):
                snapshot.chmod(0o600)
            source = sqlite3.connect(_plain_path(data_dir / 'memory.sqlite').as_uri() + '?mode=ro', uri=True)
            target = sqlite3.connect(snapshot)
            try:
                source.backup(target, pages=-1)
                target.execute('PRAGMA journal_mode=DELETE')
            finally:
                target.close()
                source.close()
            db = _connect(snapshot, readonly=True)
            try:
                blobs = _registered(db)
            finally:
                db.close()
            files = {'memory.sqlite': _file_metadata(snapshot)}
            files.update({f'blobs/{sha[:2]}/{sha[2:]}': dict(sha256=sha, size=info['size'])
                          for sha, info in sorted(blobs.items())})
            manifest = json.dumps(dict(format=_FORMAT, version=_VERSION, files=files),
                                  sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
            if len(manifest) > _MAX_MANIFEST:
                raise ValueError('Portable manifest exceeds the supported 64 MiB limit')
            packed = stage / 'archive.tar'
            fd = os.open(packed, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, 'wb') as stream:
                with tarfile.open(fileobj=stream, mode='w|', format=tarfile.USTAR_FORMAT) as archive:
                    header = tarfile.TarInfo('manifest.json')
                    header.size, header.mode = len(manifest), 0o600
                    archive.addfile(header, io.BytesIO(manifest))
                    _add(archive, 'memory.sqlite', snapshot, files['memory.sqlite']['size'])
                    payload = stage / 'payload'
                    for sha256, info in sorted(blobs.items()):
                        hot = data_dir / 'blobs' / sha256[:2] / sha256[2:]
                        if os.path.lexists(hot):
                            _plain_path(hot)
                            if not stat.S_ISREG(hot.stat().st_mode):
                                raise ValueError('Blob payload must be a regular file')
                            with hot.open('rb') as source:
                                _copy_verified(source, payload, dict(sha256=sha256, size=info['size']))
                        else:
                            if cold is None or not info['remote_id']:
                                raise ValueError(f'Referenced blob is unavailable: {sha256}')
                            cold.backend.download(info['remote_id'], payload, sha256)
                            payload.chmod(0o600)
                            if _file_metadata(payload) != dict(sha256=sha256, size=info['size']):
                                raise ValueError(f'Cold blob checksum or size mismatch: {sha256}')
                        _add(archive, f'blobs/{sha256[:2]}/{sha256[2:]}', payload, info['size'])
                        payload.unlink()
                stream.flush()
                os.fsync(stream.fileno())
            # Hard-link publication is atomic and never overwrites another file.
            os.link(packed, output)
            BlobStore._sync(output.parent)
    return dict(path=str(output), format=_FORMAT, version=_VERSION, blob_count=len(blobs), complete=True)


def _unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError('Duplicate archive manifest key')
        value[key] = item
    return value


def _manifest(archive, member):
    if (member is None or member.name != 'manifest.json' or member.type != tarfile.REGTYPE
            or member.linkname or member.pax_headers or archive.pax_headers
            or member.offset != 0 or member.offset_data != tarfile.BLOCKSIZE
            or not 0 <= member.size <= _MAX_MANIFEST):
        raise ValueError('Expected a regular versioned portable manifest first')
    with archive.extractfile(member) as source:
        value = json.load(source, object_pairs_hook=_unique_object)
    if (not isinstance(value, dict) or set(value) != {'format', 'version', 'files'}
            or value['format'] != _FORMAT or type(value['version']) is not int
            or value['version'] != _VERSION or not isinstance(value['files'], dict)):
        raise ValueError('Unsupported portable archive manifest')
    files = value['files']
    if 'memory.sqlite' not in files:
        raise ValueError('Portable archive database is missing')
    for name, info in files.items():
        if (name != 'memory.sqlite' and not re.fullmatch(r'blobs/[0-9a-f]{2}/[0-9a-f]{62}', name)):
            raise ValueError('Invalid or extraneous archive payload path')
        if (not isinstance(info, dict) or set(info) != {'sha256', 'size'}
                or not isinstance(info['sha256'], str) or not _DIGEST.fullmatch(info['sha256'])
                or type(info['size']) is not int or info['size'] < 0):
            raise ValueError('Invalid archive payload metadata')
        if name.startswith('blobs/') and name.replace('blobs/', '', 1).replace('/', '') != info['sha256']:
            raise ValueError('Blob path differs from its checksum')
    return files


def _destination(destination):
    destination = _plain_path(destination)
    _private_directory(destination.parent)
    if os.path.lexists(destination):
        info = _private_directory(destination)
        if any(destination.iterdir()):
            raise ValueError('Restore destination must be empty and not live')
        return destination, (info.st_dev, info.st_ino)
    return destination, None


def import_archive(archive_path: Path, destination: Path):
    """Validate in private staging, then atomically publish a locally usable instance."""
    archive_path = _plain_path(archive_path)
    destination, original = _destination(destination)
    info = archive_path.stat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError('Portable archive must be a regular file')
    stage = Path(tempfile.mkdtemp(prefix='.memento-import-', dir=destination.parent))
    try:
        with archive_path.open('rb') as stream:
            with tarfile.open(fileobj=stream, mode='r:') as archive:
                files = _manifest(archive, archive.next())
                seen = set()
                while (member := archive.next()) is not None:
                    if (member.name in seen or member.name not in files or member.type != tarfile.REGTYPE
                            or member.linkname or member.pax_headers or archive.pax_headers
                            or member.offset_data != member.offset + tarfile.BLOCKSIZE):
                        raise ValueError('Duplicate, linked, or extraneous archive member')
                    expected = files[member.name]
                    if member.size != expected['size']:
                        raise ValueError('Archive member size differs from manifest')
                    target = stage / member.name
                    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                    with archive.extractfile(member) as source:
                        _copy_verified(source, target, expected)
                    seen.add(member.name)
                if seen != set(files):
                    raise ValueError('Portable archive payloads are missing')
                # Reject concatenated archives or hidden trailing members too.
                stream.seek(archive.offset)
                length = archive.offset
                while chunk := stream.read(1024 * 1024):
                    length += len(chunk)
                    if any(chunk):
                        raise ValueError('Extraneous data after portable archive members')
                if length % tarfile.BLOCKSIZE or length - archive.offset < 2 * tarfile.BLOCKSIZE:
                    raise ValueError('Truncated portable archive padding')
        db = _connect(stage / 'memory.sqlite')
        try:
            blobs = _registered(db)
            expected = {'memory.sqlite', *(f'blobs/{sha[:2]}/{sha[2:]}' for sha in blobs)}
            if set(files) != expected:
                raise ValueError('Portable archive differs from the complete registered blob set')
            for sha256, blob in blobs.items():
                if files[f'blobs/{sha256[:2]}/{sha256[2:]}']['size'] != blob['size']:
                    raise ValueError('Database and archive blob sizes differ')
            # These are host-specific mutable transfer/scheduling records, not
            # immutable graph/evidence history. Never resume an old Drive upload
            # or retry an old host's backup after connecting a different account.
            with db:
                db.execute("UPDATE blobs SET tier='hot',remote_id=NULL")
                db.execute('DELETE FROM cold_transfers')
                db.execute('DELETE FROM cold_failures')
                db.execute('DELETE FROM backups')
            if db.execute('PRAGMA foreign_key_check').fetchone() is not None:
                raise ValueError('Imported database foreign key check failed')
            if db.execute('PRAGMA integrity_check').fetchall() != [('ok',)]:
                raise ValueError('Imported database integrity check failed')
        finally:
            db.close()
        (stage / 'blobs').mkdir(mode=0o700, exist_ok=True)
        for directory, _, names in os.walk(stage):
            for name in names:
                with (Path(directory) / name).open('rb') as source:
                    os.fsync(source.fileno())
            BlobStore._sync(Path(directory))
        _, current = _destination(destination)
        if current != original:
            raise ValueError('Restore destination changed during validation')
        os.replace(stage, destination)
        BlobStore._sync(destination.parent)
        return dict(path=str(destination / 'memory.sqlite'), format=_FORMAT, version=_VERSION,
                    blob_count=len(blobs), complete=True,
                    rebased=['blobs.tier=hot', 'blobs.remote_id=NULL',
                             'cold_transfers', 'cold_failures', 'backups'])
    finally:
        if stage.exists():
            shutil.rmtree(stage)
