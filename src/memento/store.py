"""SQLite evidence storage. This boundary has no model calls or derived memories."""

import fcntl
import hashlib
import json
import os
import re
import stat
import sqlite3
import sqlite_vec
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from uuid import uuid4

from .schema import initialize, upgrade_episode_fts, upgrade_raw_text, upgrade_status_index
from .blobs import BlobStore
from .artifacts import Artifacts
from .raw_text import KINDS as RAW_KINDS, MIME as RAW_MIME, elide, hydrate
from .portable import _connect

LEGACY_CONTEXT_PREFIX = '# AGENTS.md instructions'
GOAL_CONTEXT_PREFIX = '<codex_internal_context source="goal">'


class ConflictError(Exception):
    """An event identifier was reused with different evidence."""


def legacy_native_context(source, kind, text, metadata):
    """Recognize only complete, metadata-less Codex instruction envelopes."""
    if kind != 'user' or not isinstance(source, str) or re.fullmatch(r'native:[^:]+:codex', source) is None:
        return False
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except ValueError:
            return False
    if (not isinstance(metadata, dict) or 'native_context_kinds' in metadata or 'content_item_kinds' in metadata
            or metadata.get('extraction_mode', 'message') != 'message' or not isinstance(text, str)):
        return False
    if text.startswith(GOAL_CONTEXT_PREFIX):
        return (text.rstrip().endswith('</codex_internal_context>')
                and text.count('<codex_internal_context') == 1 and text.count('</codex_internal_context>') == 1)
    opening = re.match(r'\A' + re.escape(LEGACY_CONTEXT_PREFIX) + r'(?: for [^\r\n]+)?\r?\n[ \t\r\n]*<INSTRUCTIONS>', text)
    if opening is None or text.count('<INSTRUCTIONS>') != 1 or text.count('</INSTRUCTIONS>') != 1:
        return False
    _, closing, suffix = text[opening.end():].partition('</INSTRUCTIONS>')
    if not closing:
        return False
    suffix = suffix.strip()
    return not suffix or (suffix.startswith('<environment_context>') and suffix.endswith('</environment_context>')
        and suffix.count('<environment_context>') == 1 and suffix.count('</environment_context>') == 1)


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
            # Extraction filters before its LIMIT using the same pure predicate
            # as authority/recall checks. No schema or persisted flags are added.
            self._db.create_function('legacy_native_context', 4, legacy_native_context, deterministic=True)
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
            else:
                self._db.execute('BEGIN IMMEDIATE')
                try:
                    upgrade_episode_fts(self._db)
                    upgrade_raw_text(self._db)
                    upgrade_status_index(self._db)
                    self._db.execute('COMMIT')
                except BaseException:
                    self._db.execute('ROLLBACK')
                    raise
            self.blobs = BlobStore(data_dir / 'blobs')
            self.artifacts = Artifacts(self, self.blobs)
            self.artifact_reader = self.blobs.open
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

    @contextmanager
    def read_snapshot(self):
        """Read one WAL snapshot without holding the capture mutex during queries."""
        with self._mutex:
            path = self._db.execute('PRAGMA database_list').fetchone()['file']
        with closing(_connect(Path(path), readonly=True)) as db:
            db.row_factory = sqlite3.Row
            db.execute('BEGIN')
            try:
                yield db
            finally:
                db.rollback()

    @staticmethod
    def _episode(row):
        if row is None:
            return None
        episode = dict(row)
        episode.pop('seq', None)
        episode.pop('text_runs', None)
        episode.pop('text_sha256', None)
        episode['metadata'] = json.loads(episode['metadata'])
        return episode

    def capture(self, payload: dict) -> tuple[dict, bool]:
        fields = dict(payload)
        fields['metadata'] = json.dumps(fields['metadata'], ensure_ascii=False,
                                       sort_keys=True, separators=(',', ':'), allow_nan=False)
        original = fields['text']

        def same(old):
            text_matches = (hashlib.sha256(original.encode('utf-8')).hexdigest() == old['text_sha256']
                            if old['text_sha256'] else old['text'] == original)
            return text_matches and all(old[key] == value for key, value in fields.items() if key != 'text')

        def accepted(row):
            return dict(self._episode(row), text=original)

        with self._mutex:
            old = self._db.execute('SELECT * FROM episodes WHERE source=? AND event_id=?',
                                   (fields['source'], fields['event_id'])).fetchone()
        if old is not None:
            if not same(old):
                raise ConflictError('Event identifier already contains different evidence')
            return accepted(old), False
        skeleton, references, full_sha, artifacts = (original, None, None, [])
        if fields['kind'] in RAW_KINDS and payload['metadata'].get('extraction_mode') == 'skip':
            # BlobStore fsyncs content before any SQLite reference is committed.
            skeleton, references, full_sha, artifacts = elide(original, self.blobs)
        with self._mutex:
            self._db.execute('BEGIN IMMEDIATE')
            try:
                old = self._db.execute(
                    'SELECT * FROM episodes WHERE source=? AND event_id=?',
                    (fields['source'], fields['event_id']),
                ).fetchone()
                if old is not None:
                    if not same(old):
                        raise ConflictError('Event identifier already contains different evidence')
                    self._db.execute('COMMIT')
                    return accepted(old), False
                for info in artifacts:
                    self.artifacts.register(info, RAW_MIME)
                fields.update(text=skeleton, text_runs=references, text_sha256=full_sha)
                fields['id'] = uuid4().hex
                fields['captured_at'] = datetime.now(timezone.utc).isoformat()
                inserted = self._db.execute(
                    """INSERT INTO episodes
                    (id, source, event_id, conversation_id, kind, text, occurred_at, metadata, captured_at,
                     text_runs, text_sha256)
                    VALUES (:id, :source, :event_id, :conversation_id, :kind, :text,
                            :occurred_at, :metadata, :captured_at, :text_runs, :text_sha256)""", fields,
                )
                self._db.execute('''INSERT INTO processing_progress
                    (source,conversation_id,latest_seq,latest_at) VALUES (?,?,?,?)
                    ON CONFLICT(source,conversation_id) DO UPDATE SET
                    latest_seq=excluded.latest_seq,latest_at=excluded.latest_at''',
                    (fields['source'], fields['conversation_id'], inserted.lastrowid, fields['captured_at']))
                # The FTS trigger and evidence insert commit together, before acknowledgement.
                self._db.execute('COMMIT')
                return accepted(fields), True
            except BaseException:
                if self._db.in_transaction:
                    self._db.execute('ROLLBACK')
                raise

    def get(self, episode_id: str) -> dict | None:
        with self._mutex:
            row = self._db.execute(
                'SELECT * FROM episodes WHERE id=?', (episode_id,),
            ).fetchone()
        return self.hydrate_episode(row)

    def hydrate_episode(self, row):
        episode = self._episode(row)
        if episode is not None and row['text_runs'] is not None:
            episode['text'] = hydrate(row['text'], row['text_runs'], row['text_sha256'], self.artifact_reader)
        return episode

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
