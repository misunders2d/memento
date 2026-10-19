"""Copy, verify, commit reference, then evict hot bytes; never purge remote data."""

import json
import os
import sqlite3
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import RLock
from uuid import uuid4

import sqlite_vec

from .cold import _hashes

_PROTECTION = '''SELECT 1 FROM node_versions n
    LEFT JOIN node_stats s ON s.node_id=n.id
    WHERE n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)
      AND n.state='active' AND (n.pinned=1 OR coalesce(s.importance,0.5)>=?) AND n.id IN (
        SELECT node_id FROM media_files WHERE sha256={sha}
        UNION SELECT node_id FROM segments WHERE artifact_sha256={sha} OR source_sha256={sha})'''


def _now():
    return datetime.now(timezone.utc).isoformat()


class ColdManager:
    def __init__(self, artifacts, backend, data_dir: Path, *, max_importance=0.75):
        self.artifacts = artifacts
        self.store = artifacts.store
        self.blobs = artifacts.blobs
        self.backend = backend
        self.data_dir = data_dir
        self.max_importance = max_importance
        self._mutex = RLock()

    def _protected(self, sha256):
        return self.store._db.execute(_PROTECTION.format(sha='?') + ' LIMIT 1',
            (self.max_importance, sha256, sha256, sha256)).fetchone() is not None

    def _phase(self, sha256, phase, remote_id):
        db = self.store._db
        db.execute('BEGIN IMMEDIATE')
        try:
            db.execute('UPDATE cold_transfers SET status=?,remote_id=?,updated_at=? WHERE sha256=?',
                       (phase, remote_id, _now(), sha256))
            db.execute('INSERT INTO blob_moves(sha256,phase,remote_id,at) VALUES (?,?,?,?)',
                       (sha256, phase, remote_id, _now()))
            if phase == 'evicted':
                db.execute('DELETE FROM cold_failures WHERE sha256=?', (sha256,))
            db.execute('COMMIT')
        except BaseException:
            db.execute('ROLLBACK')
            raise

    def promote(self, sha256):
        with self._mutex:
            info = self.artifacts.get(sha256)
            if info is None:
                raise ValueError('Unknown registered artifact')
            path = self.blobs.path(sha256)
            if not path.exists() and info['tier'] == 'cold':
                with self.store._mutex:
                    self._phase(sha256, 'evicted', info['remote_id'])
                return info
            with self.store._mutex:
                if self._protected(sha256):
                    raise ValueError('Pinned or high-importance artifact must remain hot')
            remote = self._copy(info)
            with self.store._mutex:
                if self._protected(sha256):
                    raise ValueError('Artifact became protected; both copies retained')
                db = self.store._db
                db.execute('BEGIN IMMEDIATE')
                try:
                    db.execute("UPDATE blobs SET tier='cold',remote_id=? WHERE sha256=?", (remote.id, sha256))
                    db.execute("UPDATE cold_transfers SET status='committed',remote_id=?,updated_at=? WHERE sha256=?",
                               (remote.id, _now(), sha256))
                    db.execute("INSERT INTO blob_moves(sha256,phase,remote_id,at) VALUES (?,'committed',?,?)",
                               (sha256, remote.id, _now()))
                    db.execute('COMMIT')
                except BaseException:
                    db.execute('ROLLBACK')
                    raise
                # The FULL-synchronous reference commit precedes removal. A crash
                # before here leaves two copies; after here the verified cold copy remains.
                path.unlink(missing_ok=True)
                self.blobs._sync(path.parent)
                self._phase(sha256, 'evicted', remote.id)
            return self.artifacts.get(sha256)

    def _copy(self, info, *, purpose='archive'):
        sha256 = info['sha256']
        path = self.blobs.verify(sha256)
        with self.store._mutex:
            transfer = self.store._db.execute('SELECT checkpoint FROM cold_transfers WHERE sha256=?',
                                              (sha256,)).fetchone()
            checkpoint = json.loads(transfer['checkpoint']) if transfer else None
        def save_checkpoint(state):
            phase = 'verified' if state.get('object_id') else 'copying'
            if purpose == 'backup':
                phase = 'backup_' + phase
            with self.store._mutex:
                self.store._db.execute('''INSERT INTO cold_transfers VALUES (?,?,?,NULL,?)
                    ON CONFLICT(sha256) DO UPDATE SET checkpoint=excluded.checkpoint,
                    status=excluded.status,updated_at=excluded.updated_at''',
                    (sha256, json.dumps(state), phase, _now()))
        remote = self.backend.upload(path, sha256, info['mime'], checkpoint, save_checkpoint)
        if remote.sha256 != sha256 or remote.size != info['size']:
            raise ValueError('Cold copy differs from immutable hot artifact')
        return remote

    def fetch(self, sha256):
        try:
            return self.blobs.verify(sha256)
        except (FileNotFoundError, OSError):
            pass
        with self._mutex:
            path = self.blobs.path(sha256)
            try:
                return self.blobs.verify(sha256)
            except (FileNotFoundError, OSError):
                info = self.artifacts.get(sha256)
                if info is None or not info['remote_id']:
                    raise
            path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            self.backend.download(info['remote_id'], path, sha256)
            return self.blobs.verify(sha256)

    def sweep(self, *, after_days=90, min_bytes=10 * 1024 * 1024, limit=10):
        cutoff = (datetime.now(timezone.utc) - timedelta(days=after_days)).isoformat()
        protection = _PROTECTION.format(sha='b.sha256')
        with self.store._mutex:
            rows = self.store._db.execute(f'''SELECT b.sha256 FROM blobs b
                LEFT JOIN cold_transfers t ON t.sha256=b.sha256
                LEFT JOIN cold_failures f ON f.sha256=b.sha256
                WHERE ((b.tier='hot' AND b.created_at<? AND b.size>=?)
                   OR t.status IN ('copying','verified','committed'))
                AND coalesce(f.next_attempt,0)<=? AND NOT EXISTS ({protection})
                ORDER BY coalesce(f.attempts,0),b.created_at,b.sha256 LIMIT ?''',
                (cutoff, min_bytes, time.time(), self.max_importance, limit)).fetchall()
        promoted = []
        for row in rows:
            try:
                promoted.append(self.promote(row['sha256']))
            except Exception as error:
                with self.store._mutex:
                    prior = self.store._db.execute('SELECT attempts FROM cold_failures WHERE sha256=?',
                                                   (row['sha256'],)).fetchone()
                    attempts = prior['attempts'] + 1 if prior else 1
                    retry_at = time.time() + min(3600, 60 * 2 ** min(attempts - 1, 6))
                    self.store._db.execute('''INSERT INTO cold_failures VALUES (?,?,?,?)
                        ON CONFLICT(sha256) DO UPDATE SET attempts=excluded.attempts,
                        error=excluded.error,next_attempt=excluded.next_attempt''',
                        (row['sha256'], attempts, type(error).__name__, retry_at))
        with self.store._mutex:
            failures = self.store._db.execute(f'''SELECT count(*) FROM cold_failures f
                JOIN blobs b ON b.sha256=f.sha256
                WHERE b.tier='hot' AND NOT EXISTS ({protection})''', (self.max_importance,)).fetchone()[0]
        return dict(promoted=promoted, failure_count=failures)

    def backup(self):
        with self._mutex:
            directory = self.data_dir / 'backups'
            directory.mkdir(mode=0o700, exist_ok=True)
            backup_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + '-' + uuid4().hex[:12]
            path = directory / (backup_id + '.sqlite')
            try:
                gaps = self._snapshot(path, backup_id)
                with path.open('rb') as snapshot:
                    os.fsync(snapshot.fileno())
                self.blobs._sync(directory)
                sha256, _, size = _hashes(path)
                with self.store._mutex:
                    db = self.store._db
                    db.execute('BEGIN IMMEDIATE')
                    try:
                        db.execute('INSERT INTO backups VALUES (?,?,?,?,NULL,?,?)',
                            (backup_id, str(path), sha256, size, '{}', _now()))
                        db.executemany('INSERT INTO backup_gaps VALUES (?,?,?,?)', gaps)
                        db.execute('COMMIT')
                    except BaseException:
                        db.execute('ROLLBACK')
                        raise
            except BaseException:
                # No published backup references this staging file.
                path.unlink(missing_ok=True)
                raise
            return self._upload_backup(backup_id)

    def _snapshot(self, path, backup_id):
        with path.open('xb'):
            path.chmod(0o600)
        source = sqlite3.connect((self.data_dir / 'memory.sqlite').resolve().as_uri() + '?mode=ro', uri=True)
        target = sqlite3.connect(path)
        target.row_factory = sqlite3.Row
        gaps = []
        try:
            # One read snapshot avoids incremental restart under continuous capture.
            # WAL writers remain independent of this separate read-only connection.
            source.backup(target, pages=-1)
            target.enable_load_extension(True)
            try:
                sqlite_vec.load(target)
            finally:
                target.enable_load_extension(False)
            for artifact in target.execute('SELECT * FROM blobs').fetchall():
                if artifact['remote_id']:
                    continue
                try:
                    remote = self._copy(dict(artifact), purpose='backup')
                except Exception as error:
                    gap = (backup_id, artifact['sha256'], type(error).__name__, _now())
                    target.execute('INSERT INTO backup_gaps VALUES (?,?,?,?)', gap)
                    gaps.append(gap)
                    continue
                target.execute('UPDATE blobs SET remote_id=? WHERE sha256=?',
                               (remote.id, artifact['sha256']))
                with self.store._mutex:
                    self.store._db.execute('UPDATE blobs SET remote_id=? WHERE sha256=? AND remote_id IS NULL',
                                          (remote.id, artifact['sha256']))
                    self._phase(artifact['sha256'], 'backed_up', remote.id)
            target.commit()
            if target.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                raise RuntimeError('Backup integrity check failed; source remains untouched')
            return gaps
        finally:
            target.close()
            source.close()

    def _upload_backup(self, backup_id):
        with self.store._mutex:
            row = self.store._db.execute('SELECT * FROM backups WHERE id=?', (backup_id,)).fetchone()
            gap_count = self.store._db.execute('SELECT count(*) FROM backup_gaps WHERE backup_id=?',
                                               (backup_id,)).fetchone()[0]
        if row is None:
            raise ValueError('Unknown backup')
        if row['remote_id']:
            return dict(row, gap_count=gap_count, complete=gap_count == 0)
        def save_checkpoint(state):
            with self.store._mutex:
                self.store._db.execute('UPDATE backups SET checkpoint=? WHERE id=?',
                                       (json.dumps(state), backup_id))
        status = f'incomplete-{gap_count}-' if gap_count else ''
        remote = self.backend.upload(Path(row['path']), 'memory-' + status + backup_id + '-' + row['sha256'] + '.sqlite',
            'application/vnd.sqlite3', json.loads(row['checkpoint']) or None, save_checkpoint)
        if remote.sha256 != row['sha256'] or remote.size != row['size']:
            raise ValueError('Remote backup differs from verified local snapshot')
        with self.store._mutex:
            self.store._db.execute('UPDATE backups SET remote_id=? WHERE id=?', (remote.id, backup_id))
            result = dict(self.store._db.execute('SELECT * FROM backups WHERE id=?', (backup_id,)).fetchone())
            result.update(gap_count=gap_count, complete=gap_count == 0)
        Path(row['path']).unlink(missing_ok=True)
        self.blobs._sync(Path(row['path']).parent)
        return result

    def latest_backup_gap_count(self):
        with self.store._mutex:
            return self.store._db.execute('''SELECT count(*) FROM backup_gaps WHERE backup_id=(
                SELECT id FROM backups WHERE remote_id IS NOT NULL
                ORDER BY created_at DESC,id DESC LIMIT 1)''').fetchone()[0]

    def retry_backups(self):
        with self.store._mutex:
            ids = [row['id'] for row in self.store._db.execute('SELECT id FROM backups WHERE remote_id IS NULL ORDER BY created_at')]
        with self._mutex:
            return [self._upload_backup(backup_id) for backup_id in ids]

    def restore(self, backup_id, destination: Path, *, allow_incomplete=False):
        return self.restore_from(self.backend, backup_id, destination, allow_incomplete=allow_incomplete)

    @staticmethod
    def restore_from(backend, backup_id, destination: Path, *, allow_incomplete=False):
        """Recover a point-in-time Drive snapshot into an empty directory."""
        matches = [item for item in backend.list_backups() if item['id'] == backup_id]
        if not matches or len({item['sha256'] for item in matches}) != 1:
            raise ValueError('Backup missing or ambiguous in remote storage')
        row = matches[0]
        if row.get('gap_count', 0) and not allow_incomplete:
            raise ValueError('Backup is incomplete; explicitly allow incomplete recovery to restore metadata and available artifacts')
        if destination.exists() and any(destination.iterdir()):
            raise ValueError('Restore destination must be empty')
        destination.mkdir(parents=True, mode=0o700, exist_ok=True)
        if destination.stat().st_mode & 0o077:
            raise ValueError('Restore destination must be private (0700)')
        backend.download(row['remote_id'], destination / 'memory.sqlite', row['sha256'])
        (destination / 'memory.sqlite').chmod(0o600)
        restored = sqlite3.connect(destination / 'memory.sqlite')
        try:
            artifacts = restored.execute("SELECT sha256,remote_id FROM blobs WHERE tier='hot'").fetchall()
            has_gaps = restored.execute("SELECT 1 FROM sqlite_schema WHERE name='backup_gaps'").fetchone()
            gaps = dict(restored.execute('SELECT sha256,error FROM backup_gaps WHERE backup_id=?',
                                         (backup_id,)).fetchall()) if has_gaps else {}
            if len(gaps) != row.get('gap_count', 0):
                raise ValueError('Backup gap manifest differs from remote completeness metadata')
        finally:
            restored.close()
        for sha256, remote_id in artifacts:
            if not remote_id:
                if allow_incomplete and sha256 in gaps:
                    continue
                raise RuntimeError('Backup contains an artifact without a verified cold copy')
            path = destination / 'blobs' / sha256[:2] / sha256[2:]
            path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            backend.download(remote_id, path, sha256)
        from .store import Store
        from .graph import Graph
        store = Store(destination)
        try:
            Graph(store)
        finally:
            store.close()
        return dict(path=str(destination / 'memory.sqlite'), backup_id=backup_id,
                    complete=not gaps, gap_count=len(gaps))


