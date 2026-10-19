"""SQLite evidence storage. This boundary has no model calls or derived memories."""

import fcntl
import json
import os
import stat
import sqlite3
import sqlite_vec
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from uuid import uuid4

from .schema import initialize


class ConflictError(Exception):
    """An event identifier was reused with different evidence."""


class Store:
    def __init__(self, data_dir: Path):
        data_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        permissions = data_dir.stat()
        if permissions.st_uid != os.getuid() or stat.S_IMODE(permissions.st_mode) & 0o077:
            raise RuntimeError('Data directory must be owned by the service UID and private (0700)')
        agent_uid = os.environ.get('MEMENTO_AGENT_UID')
        if agent_uid is not None and int(agent_uid) == os.getuid():
            raise RuntimeError('Memory service and agent must use distinct operating-system UIDs')
        self._mutex = RLock()
        self._writer_lock = (data_dir / 'writer.lock').open('a')
        self._db = None
        try:
            fcntl.flock(self._writer_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._writer_lock.close()
            raise RuntimeError('Another memento writer owns this data directory') from None
        try:
            db_path = data_dir / 'memory.sqlite'
            self._db = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
            db_path.chmod(0o600)
            self._db.row_factory = sqlite3.Row
            self._db.enable_load_extension(True)
            try:
                sqlite_vec.load(self._db)
            finally:
                self._db.enable_load_extension(False)
            mode = self._db.execute('PRAGMA journal_mode=WAL').fetchone()[0]
            if mode != 'wal':
                raise RuntimeError('Evidence storage requires SQLite WAL mode')
            self._db.execute('PRAGMA synchronous=FULL')
            version = self._db.execute('PRAGMA user_version').fetchone()[0]
            if version == 0:
                if self._db.execute("SELECT 1 FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'").fetchone():
                    raise RuntimeError('Refusing to initialize an unversioned, nonempty database')
                self._db.execute('BEGIN IMMEDIATE')
                try:
                    initialize(self._db)
                    self._db.execute('PRAGMA user_version=1')
                    self._db.execute('COMMIT')
                except BaseException:
                    self._db.execute('ROLLBACK')
                    raise
            elif version != 1:
                raise RuntimeError(f'Unsupported database schema version: {version}')
        except BaseException:
            self.close()
            raise

    def close(self):
        with self._mutex:
            if self._db is not None:
                self._db.close()
                self._db = None
            self._writer_lock.close()

    def health(self):
        """Check database accessibility and ability to acquire the writer transaction.

        This is not a guarantee against disk-full errors at the next commit.
        """
        with self._mutex:
            self._db.execute('BEGIN IMMEDIATE')
            self._db.execute('ROLLBACK')

    @staticmethod
    def _episode(row):
        if row is None:
            return None
        episode = dict(row)
        episode.pop('seq', None)
        episode['metadata'] = json.loads(episode['metadata'])
        return episode

    def capture(self, payload: dict) -> tuple[dict, bool]:
        fields = dict(payload)
        fields['metadata'] = json.dumps(fields['metadata'], ensure_ascii=False,
                                       sort_keys=True, separators=(',', ':'), allow_nan=False)
        with self._mutex:
            self._db.execute('BEGIN IMMEDIATE')
            try:
                old = self._db.execute(
                    'SELECT * FROM episodes WHERE source=? AND event_id=?',
                    (fields['source'], fields['event_id']),
                ).fetchone()
                if old is not None:
                    if any(old[key] != value for key, value in fields.items()):
                        raise ConflictError('Event identifier already contains different evidence')
                    self._db.execute('COMMIT')
                    return self._episode(old), False
                fields['id'] = uuid4().hex
                fields['captured_at'] = datetime.now(timezone.utc).isoformat()
                inserted = self._db.execute(
                    """INSERT INTO episodes
                    (id, source, event_id, conversation_id, kind, text, occurred_at, metadata, captured_at)
                    VALUES (:id, :source, :event_id, :conversation_id, :kind, :text,
                            :occurred_at, :metadata, :captured_at)""", fields,
                )
                self._db.execute('''INSERT INTO processing_progress
                    (source,conversation_id,latest_seq,latest_at) VALUES (?,?,?,?)
                    ON CONFLICT(source,conversation_id) DO UPDATE SET
                    latest_seq=excluded.latest_seq,latest_at=excluded.latest_at''',
                    (fields['source'], fields['conversation_id'], inserted.lastrowid, fields['captured_at']))
                # The FTS trigger and evidence insert commit together, before acknowledgement.
                self._db.execute('COMMIT')
                return self._episode(fields), True
            except BaseException:
                if self._db.in_transaction:
                    self._db.execute('ROLLBACK')
                raise

    def get(self, episode_id: str) -> dict | None:
        with self._mutex:
            return self._episode(self._db.execute(
                'SELECT * FROM episodes WHERE id=?', (episode_id,),
            ).fetchone())

    def search(self, query: str | None = None, limit: int = 20,
               conversation_id: str | None = None, source: str | None = None,
               since: str | None = None) -> list[dict]:
        if not 1 <= limit <= 100:
            raise ValueError('Search limit must be between 1 and 100')
        if query is not None and (len(query) > 4096 or '\x00' in query):
            raise ValueError('Search text must be at most 4096 characters without NUL')
        if since is not None:
            try:
                instant = datetime.fromisoformat(since)
            except (TypeError, ValueError) as error:
                raise ValueError('since must be an aware ISO timestamp') from error
            if instant.tzinfo is None or instant.utcoffset() is None:
                raise ValueError('since must be an aware ISO timestamp')
            since = instant.astimezone(timezone.utc).isoformat()
        # All user input is literal text, never FTS syntax (including AND/NEAR).
        terms = query.split() if query else []
        params = []
        columns = '''e.id, e.source, e.event_id, e.conversation_id, e.kind,
                     e.occurred_at, e.captured_at'''
        if terms:
            # Implicit AND ignores token-less punctuation phrases, unlike explicit AND.
            match = ' '.join('"' + term.replace('"', '""') + '"' for term in terms)
            sql = f'''SELECT {columns},
                substr(snippet(episodes_fts, 0, '', '', '…', 32), 1, 512) AS excerpt
                FROM episodes_fts JOIN episodes e ON e.seq=episodes_fts.rowid
                WHERE episodes_fts MATCH ?'''
            params.append(match)
        else:
            sql = f'''SELECT {columns}, substr(e.text, 1, 512) AS excerpt
                      FROM episodes e WHERE 1=1'''
        if conversation_id is not None:
            sql += ' AND e.conversation_id=?'
            params.append(conversation_id)
        if source is not None:
            sql += ' AND e.source=?'
            params.append(source)
        if since is not None:
            sql += ' AND julianday(coalesce(e.occurred_at,e.captured_at))>=julianday(?)'
            params.append(since)
        sql += ''' AND NOT EXISTS (SELECT 1 FROM forgotten_sources f
            WHERE f.source=e.source AND f.event_id=e.event_id)'''
        sql += (' ORDER BY bm25(episodes_fts), e.id' if terms else
                ' ORDER BY julianday(coalesce(e.occurred_at,e.captured_at)) DESC, e.seq DESC')
        sql += ' LIMIT ?'
        params.append(limit)
        with self._mutex:
            return [dict(row) for row in self._db.execute(sql, params).fetchall()]
