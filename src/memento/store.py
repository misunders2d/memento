"""SQLite evidence storage. This boundary has no model calls or derived memories."""

import fcntl
import hashlib
import json
import os
import re
import stat
import sqlite3
import sqlite_vec
import tempfile
import zlib
import subprocess
import sys
import logging
from contextlib import closing, contextmanager, ExitStack
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from uuid import uuid4

from .schema import (SUPPORTED_VERSIONS, VERSION as SCHEMA_VERSION, header_target, initialize, upgrade_episode_fts,
                     upgrade_historical_pool, upgrade_node_fts, upgrade_raw_text, upgrade_status_index,
                     upgrade_captured_media, upgrade_repair_groups, upgrade_loop_responsibility_indexes)
from .blobs import BlobStore
from .artifacts import Artifacts
from .captured_media import references as media_references, record_references
from .raw_text import (FRAME_BYTES, KINDS as RAW_KINDS, MIME as RAW_MIME,
                       RawTextUnavailable, decode_frame, elide, hydrate, hydrate_frames)
from .portable import _connect
from .ann import upgrade as upgrade_search

LEGACY_CONTEXT_PREFIX = '# AGENTS.md instructions'
GOAL_CONTEXT_PREFIX = '<codex_internal_context source="goal">'


class ConflictError(Exception):
    """An event identifier was reused with different evidence."""


def legacy_native_context(source, kind, text, metadata):
    """Recognize native Codex developer instructions and exact legacy user envelopes."""
    if kind not in ('user', 'developer') or not isinstance(source, str) or re.fullmatch(r'native:[^:]+:codex', source) is None:
        return False
    if kind == 'developer':
        return True
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
        self._data_dir = data_dir.absolute()
        self._raw_cursor = 0
        self._frame_writes = {}
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
            self._db.create_function('raw_frame_write', 3,
                lambda seq, refs, sha: int(self._frame_writes.get(seq) == (refs, sha)))
            # Extraction filters before its LIMIT using the same pure predicate
            # as authority/recall checks. No schema or persisted flags are added.
            self._db.create_function('legacy_native_context', 4, legacy_native_context, deterministic=True)
            self._db.enable_load_extension(True)
            try:
                sqlite_vec.load(self._db)
            finally:
                self._db.enable_load_extension(False)
            # Read the header before the journal-mode change so an unknown
            # future format is refused without touching the file.
            version = self._db.execute('PRAGMA user_version').fetchone()[0]
            if version != 0 and version not in SUPPORTED_VERSIONS:
                raise RuntimeError(f'Unsupported database schema version: {version}')
            mode = self._db.execute('PRAGMA journal_mode=WAL').fetchone()[0]
            if mode != 'wal':
                raise RuntimeError('Evidence storage requires SQLite WAL mode')
            self._db.execute('PRAGMA synchronous=FULL')
            if version == 0:
                if self._db.execute("SELECT 1 FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'").fetchone():
                    raise RuntimeError('Refusing to initialize an unversioned, nonempty database')
                self._db.execute('BEGIN IMMEDIATE')
                try:
                    initialize(self._db)
                    upgrade_search(self._db)
                    self._db.execute(f'PRAGMA user_version={SCHEMA_VERSION}')
                    self._db.execute('COMMIT')
                except BaseException:
                    self._db.execute('ROLLBACK')
                    raise
            else:
                self._db.execute('BEGIN IMMEDIATE')
                try:
                    upgrade_episode_fts(self._db)
                    upgrade_raw_text(self._db)
                    upgrade_status_index(self._db)
                    upgrade_captured_media(self._db)
                    upgrade_node_fts(self._db)
                    upgrade_historical_pool(self._db)
                    upgrade_repair_groups(self._db)
                    upgrade_loop_responsibility_indexes(self._db)
                    upgrade_search(self._db)
                    # The header marks frame-capable storage; it commits with the
                    # physical upgrades so pre-frame readers refuse this file. A
                    # database that ever activated historical parking keeps header 3;
                    # startup never lowers a header.
                    target = header_target(self._db, version)
                    if version != target:
                        self._db.execute(f'PRAGMA user_version={target}')
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

    # Bounded SQLite busy wait for the health probe; real writer contention
    # beyond this is reported as unhealthy rather than waited out.
    HEALTH_BUSY_SECONDS = 0.25

    def health(self):
        """Check database accessibility and ability to acquire the writer transaction.

        The probe is a separate short-lived connection, so health never waits on the
        application mutex while a request merely prepares context without a SQLite
        write transaction. It still fails on a closed store, an inaccessible or
        missing database file (mode=rw never creates one) and on actual writer
        contention past the bounded busy wait. Nothing is written or committed.
        This is not a guarantee against disk-full errors at the next commit; the
        closed-store guard is a plain attribute check, not a shutdown fence.
        """
        if self._db is None:
            raise RuntimeError('Evidence storage is closed')
        probe = sqlite3.connect((self._data_dir / 'memory.sqlite').as_uri() + '?mode=rw', uri=True,
                                timeout=self.HEALTH_BUSY_SECONDS, isolation_level=None)
        try:
            probe.execute('BEGIN IMMEDIATE')
            probe.execute('ROLLBACK')
        finally:
            probe.close()

    def compact(self):
        """Owner-only physical maintenance; a fresh child keeps SQLite scratch off tmpfs."""
        program = '''import json,sys,time
from pathlib import Path
from contextlib import closing
from memento.portable import _connect
p=Path(sys.argv[1]);wal=Path(str(p)+'-wal')
def sizes():
 return dict(database=p.stat().st_size,wal=wal.stat().st_size if wal.exists() else 0)
started=time.monotonic();before=sizes()
with closing(_connect(p)) as db:
 free_before=db.execute('PRAGMA freelist_count').fetchone()[0]
 db.execute('VACUUM')
 checkpoint=db.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()
 free_after=db.execute('PRAGMA freelist_count').fetchone()[0]
print(json.dumps(dict(complete=checkpoint[0]==0,before=before,after=sizes(),
 freelist_before=free_before,freelist_after=free_after,checkpoint_busy=checkpoint[0],
 elapsed_seconds=time.monotonic()-started)))
'''
        with self._mutex, tempfile.TemporaryDirectory(prefix='.memento-compact-', dir=self._data_dir) as scratch:
            log = logging.getLogger(__name__)
            log.warning('Database compaction started')
            result = subprocess.run([sys.executable, '-c', program, str(self._data_dir / 'memory.sqlite')],
                env=dict(os.environ, SQLITE_TMPDIR=scratch), capture_output=True, check=False)
            if result.returncode:
                raise RuntimeError('Database compaction failed; logical data retained')
            value = json.loads(result.stdout)
            log.warning('Database compaction completed in %.3f seconds (checkpoint busy=%s)',
                        value['elapsed_seconds'], value['checkpoint_busy'])
            return value

    @contextmanager
    def read_snapshot(self):
        """Read one WAL snapshot without holding the capture mutex during queries."""
        with closing(_connect(self._data_dir / 'memory.sqlite', readonly=True)) as db:
            db.row_factory = sqlite3.Row
            db.create_function('legacy_native_context', 4, legacy_native_context, deterministic=True)
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
        episode.pop('raw_frame', None)
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
        media_refs = media_references(fields['source'], fields['kind'], original, payload['metadata'])
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
                record_references(self._db, fields['id'], media_refs)
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

    def get(self, episode_id: str, db=None) -> dict | None:
        """Read one episode; db selects an open read snapshot instead of the writer."""
        if db is None:
            with self._mutex:
                row = self._db.execute('SELECT * FROM episodes WHERE id=?', (episode_id,)).fetchone()
        else:
            row = db.execute('SELECT * FROM episodes WHERE id=?', (episode_id,)).fetchone()
        return self.hydrate_episode(row, db)

    def hydrate_episode(self, row, db=None):
        episode = self._episode(row)
        if episode is not None:
            episode['text'] = self.original_text(row, db)
        return episode

    def _frame(self, identity, db=None):
        sql = '''SELECT source,conversation_id,raw_size,sha256,
            CASE WHEN length(data)<=? THEN data END AS data FROM raw_frames WHERE id=?'''
        if db is None:
            with self._mutex:
                row = self._db.execute(sql, (FRAME_BYTES + 1024, identity)).fetchone()
                frame = dict(row) if row is not None else None
        else:
            row = db.execute(sql, (FRAME_BYTES + 1024, identity)).fetchone()
            frame = dict(row) if row is not None else None
        if frame is not None:
            frame['decoded'] = decode_frame(frame.pop('data'), frame['raw_size'], frame['sha256'])
        return frame

    def original_text(self, row, db=None):
        if row['raw_frame'] is not None:
            return hydrate_frames(row['raw_frame'], row['text_sha256'], row['source'],
                                  row['conversation_id'], lambda identity: self._frame(identity, db))
        if row['text_runs'] is not None:
            return hydrate(row['text'], row['text_runs'], row['text_sha256'], self.artifact_reader)
        return row['text']

    def compress_raw_step(self):
        """One bounded scan/frame batch. Encoding and legacy large-row reads use no writer mutex."""
        with ExitStack() as files:
            with self.read_snapshot() as reader:
                rows = reader.execute('''SELECT seq,source,conversation_id,text_sha256,
                    CASE WHEN kind IN ('native_record','native_live_record','native_hook')
                        THEN text_runs IS NULL AND raw_frame IS NULL
                            AND coalesce(json_extract(metadata,'$.extraction_mode'),'message')='skip'
                        ELSE 0 END AS eligible
                    FROM episodes WHERE seq>? ORDER BY seq LIMIT 64''', (self._raw_cursor,))
                selected, total, through, origin = [], 0, self._raw_cursor, None
                for row in rows:
                    if row['eligible']:
                        current = (row['source'], row['conversation_id'])
                        if origin is not None and current != origin:
                            break
                        with reader.blobopen('episodes', 'text', row['seq'], readonly=True) as source:
                            size = len(source)
                        if selected and total + size > FRAME_BYTES:
                            break
                        origin = current
                        selected.append((row, size))
                        total += size
                    through = row['seq']
                    if total >= FRAME_BYTES:
                        break
                if not selected:
                    if through == self._raw_cursor:
                        return False
                    self._raw_cursor = through
                    return True
                packed = files.enter_context(tempfile.TemporaryFile(dir=self._data_dir))
                frames, references, checksums = [], {}, {}
                payload, count = bytearray(), 0
                def flush():
                    nonlocal payload, count
                    if payload or count:
                        encoded = zlib.compress(payload, 1)
                        frames.append((packed.tell(), len(encoded), len(payload), count,
                                       hashlib.sha256(payload).hexdigest()))
                        packed.write(encoded)
                        payload, count = bytearray(), 0
                for row, size in selected:
                    if len(payload) + size > FRAME_BYTES and (payload or count):
                        flush()
                    refs, digest, first = [], hashlib.sha256(), True
                    with reader.blobopen('episodes', 'text', row['seq'], readonly=True) as source:
                        remaining = size
                        while remaining or first:
                            chunk = source.read(min(remaining, FRAME_BYTES - len(payload)))
                            if remaining and not chunk:
                                raise RawTextUnavailable()
                            refs.append([len(frames), len(payload), len(chunk)])
                            count += int(first)
                            payload.extend(chunk)
                            digest.update(chunk)
                            remaining -= len(chunk)
                            first = False
                            if len(payload) == FRAME_BYTES:
                                flush()
                    references[row['seq']] = refs
                    checksums[row['seq']] = digest.hexdigest()
                    if row['text_sha256'] is not None and row['text_sha256'] != digest.hexdigest():
                        raise RawTextUnavailable()
                flush()
            # Reads/encoding precede the writer mutex. Each published payload is
            # bounded; a legacy outlier can span several frames in this transaction.
            with self._mutex:
                db = self._db
                db.execute('BEGIN IMMEDIATE')
                try:
                    identities = []
                    for offset, length, raw_size, records, sha in frames:
                        packed.seek(offset)
                        identities.append(db.execute('''INSERT INTO raw_frames
                            (source,conversation_id,raw_size,record_count,sha256,data)
                            VALUES (?,?,?,?,?,?)''', (*origin, raw_size, records, sha, packed.read(length))).lastrowid)
                    for row, _ in selected:
                        seq = row['seq']
                        refs = json.dumps([[identities[index], offset, length]
                            for index, offset, length in references[seq]], separators=(',', ':'))
                        self._frame_writes[seq] = (refs, checksums[seq])
                        changed = db.execute('''UPDATE episodes SET text='',raw_frame=?,text_sha256=?
                            WHERE seq=? AND raw_frame IS NULL AND text_runs IS NULL''',
                            (refs, checksums[seq], seq)).rowcount
                        if changed != 1:
                            raise ConflictError('Raw source storage changed during compression')
                    db.execute('COMMIT')
                    self._raw_cursor = through
                except BaseException:
                    db.execute('ROLLBACK')
                    raise
                finally:
                    self._frame_writes.clear()
        return True

    def search(self, query: str | None = None, limit: int = 20,
               conversation_id: str | None = None, source: str | None = None,
               since: str | None = None, db=None) -> list[dict]:
        """Literal raw search on a WAL read snapshot; the writer mutex is never held."""
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
        filters, params = '', []
        columns = '''e.id, e.source, e.event_id, e.conversation_id, e.kind,
                     e.occurred_at, e.captured_at'''
        if conversation_id is not None:
            filters += ' AND e.conversation_id=?'
            params.append(conversation_id)
        if source is not None:
            filters += ' AND e.source=?'
            params.append(source)
        if since is not None:
            filters += ' AND julianday(coalesce(e.occurred_at,e.captured_at))>=julianday(?)'
            params.append(since)
        filters += ''' AND NOT EXISTS (SELECT 1 FROM forgotten_sources f
            WHERE f.source=e.source AND f.event_id=e.event_id)'''
        if terms:
            # Implicit AND ignores token-less punctuation phrases, unlike explicit AND.
            match = ' '.join('"' + term.replace('"', '""') + '"' for term in terms)
            # Rank every match from index data alone; snippets read each document
            # body, so build them only for the rows that are actually returned.
            sql = f'''SELECT {columns},
                substr(snippet(episodes_fts, 0, '', '', '…', 32), 1, 512) AS excerpt
                FROM episodes_fts JOIN episodes e ON e.seq=episodes_fts.rowid
                WHERE episodes_fts MATCH ? AND episodes_fts.rowid IN (
                    SELECT episodes_fts.rowid FROM episodes_fts JOIN episodes e ON e.seq=episodes_fts.rowid
                    WHERE episodes_fts MATCH ?{filters} ORDER BY bm25(episodes_fts), e.id LIMIT ?)
                ORDER BY bm25(episodes_fts), e.id'''
            params = [match, match, *params, limit]
        else:
            sql = f'''SELECT {columns},e.raw_frame,e.text_sha256, substr(e.text, 1, 512) AS excerpt
                      FROM episodes e WHERE 1=1{filters}
                      ORDER BY julianday(coalesce(e.occurred_at,e.captured_at)) DESC, e.seq DESC LIMIT ?'''
            params = [*params, limit]

        def run(reader):
            result = [dict(row) for row in reader.execute(sql, params).fetchall()]
            for row in result:
                if row.pop('raw_frame', None) is not None:
                    row['excerpt'] = self.get(row['id'], db=reader)['text'][:512]
                row.pop('text_sha256', None)
            return result

        if db is not None:
            return run(db)
        with self.read_snapshot() as reader:
            return run(reader)
