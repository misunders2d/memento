"""Transactional invalidations and a snapshot-bound disposable vector projection."""

from contextlib import contextmanager
import os
from threading import Event, Lock, Thread
import time

from .qdrant import Qdrant, SearchUnavailable


def _targets(ids):
    return f'''SELECT i.seq FROM vector_items i WHERE i.target IN ({ids})
        UNION SELECT i.seq FROM vector_items i JOIN segments s ON s.id=i.target
        WHERE i.kind='segment' AND s.node_id IN ({ids})'''


def _sha(expression):
    return _targets(f'''SELECT node_id FROM captured_media WHERE sha256={expression}
                        UNION SELECT node_id FROM media_files WHERE sha256={expression}''')


def _episodes(ids):
    return f'''SELECT i.seq FROM vector_items i JOIN episode_chunks x ON x.id=i.target
        WHERE i.kind='episode' AND x.episode_id IN ({ids})
        UNION {_targets(f'SELECT target FROM graph_evidence WHERE episode_id IN ({ids})')}
        UNION SELECT i.seq FROM vector_items i JOIN candidates c ON c.id=i.target
        JOIN extraction_runs r ON r.id=c.run_id WHERE i.kind='candidate' AND r.episode_id IN ({ids})
        UNION {_targets(f'''SELECT cm.node_id FROM captured_media cm JOIN captured_media_refs r
                          ON r.sha256=cm.sha256 WHERE r.episode_id IN ({ids})''')}'''


def upgrade(db):
    """Only additive derived tables/triggers; old binaries need no Python UDF."""
    db.execute('''CREATE TABLE IF NOT EXISTS search_state (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),db_identity TEXT NOT NULL,
        ack_id INTEGER NOT NULL,ack_nonce TEXT NOT NULL) STRICT''')
    db.execute('''CREATE TABLE IF NOT EXISTS search_outbox (
        id INTEGER PRIMARY KEY AUTOINCREMENT,seq INTEGER,nonce TEXT NOT NULL) STRICT''')
    db.execute('''CREATE TRIGGER IF NOT EXISTS search_outbox_no_update BEFORE UPDATE ON search_outbox
        BEGIN SELECT RAISE(ABORT,'search events are immutable'); END''')
    for name, table, columns in [
            ('search_evidence_episode', 'graph_evidence', 'episode_id,target'),
            ('search_chunks_episode', 'episode_chunks', 'episode_id,id'),
            ('search_runs_episode', 'extraction_runs', 'episode_id,id'),
            ('search_segments_parent', 'segments', 'node_id,id'),
            ('search_node_type', 'node_versions', 'type,id'),
            ('search_candidate_type', 'candidates', 'type,id')]:
        db.execute(f'CREATE INDEX IF NOT EXISTS {name} ON {table}({columns})')
    fresh = db.execute('SELECT 1 FROM search_state').fetchone() is None
    if fresh:
        db.execute('INSERT INTO search_state VALUES(1,lower(hex(randomblob(16))),0,lower(hex(randomblob(16))))')
        db.execute('INSERT INTO search_outbox(seq,nonce) VALUES(NULL,lower(hex(randomblob(16))))')
    # Every selector captures identifiers before destructive changes. The
    # consumer reads final payloads only after the complete writer transaction.
    selectors = {
        'vector_items': lambda r: f'SELECT {r}.seq',
        'node_versions': lambda r: _targets(f'SELECT {r}.id'),
        'graph_evidence': lambda r: _targets(f'SELECT {r}.target'),
        'candidates': lambda r: _targets(f'SELECT {r}.id'),
        'candidate_decisions': lambda r: _targets(f'SELECT {r}.candidate_id'),
        'extraction_runs': lambda r: _targets(f'SELECT id FROM candidates WHERE run_id={r}.id'),
        'episodes': lambda r: _episodes(f'SELECT {r}.id'),
        'episode_chunks': lambda r: _targets(f'SELECT {r}.id'),
        'segments': lambda r: _targets(f'SELECT {r}.id UNION SELECT {r}.node_id'),
        'media_files': lambda r: _targets(f'SELECT {r}.node_id') + ' UNION ' + _sha(f'{r}.sha256'),
        'captured_media': lambda r: _targets(f'SELECT {r}.node_id') + ' UNION ' + _sha(f'{r}.sha256'),
        'captured_media_refs': lambda r: _sha(f'{r}.sha256'),
        'forgotten_sources': lambda r: _episodes(
            f'SELECT id FROM episodes WHERE source={r}.source AND event_id={r}.event_id'),
        'forgotten_targets': lambda r: _targets(f'SELECT {r}.target') + ' UNION ' + _targets(
            f'''SELECT node_id FROM captured_media WHERE sha256 IN
                (SELECT sha256 FROM media_files WHERE node_id={r}.target)'''),
        'historical_parking': lambda r: _episodes(
            f'SELECT id FROM episodes WHERE source={r}.source AND conversation_id={r}.conversation_id'),
        'historical_parked_candidates': lambda r: _targets(f'SELECT {r}.candidate_id'),
        'memory_types': lambda r: _targets(
            f'SELECT id FROM node_versions WHERE type={r}.name UNION SELECT id FROM candidates WHERE type={r}.name'),
    }
    for table, select in selectors.items():
        for operation, timing, rows in [('INSERT', 'AFTER', ('NEW',)),
                                         ('UPDATE', 'BEFORE', ('OLD', 'NEW')),
                                         ('DELETE', 'BEFORE', ('OLD',))]:
            ids = ' UNION '.join(select(row) for row in rows)
            when = ''
            if table == 'historical_parking' and operation == 'UPDATE':
                # Extraction's live cursor never changes historical eligibility.
                when = '''WHEN OLD.source IS NOT NEW.source
                    OR OLD.conversation_id IS NOT NEW.conversation_id
                    OR OLD.parked_through IS NOT NEW.parked_through'''
            # The outer DISTINCT also prevents a moved parent/target from
            # multiplying one event within a single mutation.
            name = f'search_{table}_{operation.lower()}'
            definition = f'''CREATE TRIGGER {name} {timing} {operation} ON {table} {when}
                BEGIN INSERT INTO search_outbox(seq,nonce)
                SELECT seq,lower(hex(randomblob(16))) FROM ({ids}) GROUP BY seq; END'''
            if table == 'historical_parking' and operation == 'UPDATE':
                previous = db.execute("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name=?", (name,)).fetchone()
                # Replace an earlier derived definition once. Healthy reopens
                # must leave the schema and its version completely unchanged.
                if previous is not None:
                    if ' '.join(previous[0].split()) == ' '.join(definition.split()):
                        continue
                    db.execute(f'DROP TRIGGER {name}')
            db.execute(definition.replace('CREATE TRIGGER ', 'CREATE TRIGGER IF NOT EXISTS ', 1))


class Projection:
    """One lock, one pinned SQLite snapshot, and one fully synchronized search cut."""

    def __init__(self, store, dimensions, signature, payloads, *, transport=None):
        self.store, self.signature, self.payloads = store, signature, payloads
        self.transport = transport or Qdrant(os.environ['MEMENTO_QDRANT_BINARY'], store._data_dir / 'search', dimensions)
        self._lock, self._stop = Lock(), Event()
        self._active = None
        self.error = None
        self.ready = False
        self._worker = None

    @property
    def available(self):
        process = self.transport.process
        return self.ready and process is not None and process.poll() is None

    @staticmethod
    def _cut(db):
        # This first actual read pins the WAL cut before any remote operation.
        state = dict(db.execute('SELECT * FROM search_state WHERE singleton=1').fetchone())
        last = db.execute('SELECT id,nonce FROM search_outbox ORDER BY id DESC LIMIT 1').fetchone()
        cut = dict(last) if last is not None else dict(id=state['ack_id'], nonce=state['ack_nonce'])
        return state, cut

    @staticmethod
    def _known(db, state, fence):
        if not isinstance(fence, dict) or type(fence.get('id')) is not int or not isinstance(fence.get('nonce'), str):
            return False
        if fence == dict(id=state['ack_id'], nonce=state['ack_nonce']):
            return True
        row = db.execute('SELECT nonce FROM search_outbox WHERE id=?', (fence['id'],)).fetchone()
        return row is not None and row['nonce'] == fence['nonce']

    def _marker(self, state, cursor, pending=None, *, rebuilding=False):
        return dict(db_identity=state['db_identity'], signature=self.signature,
                    cursor=cursor, pending=pending, rebuilding=rebuilding)

    def _ack(self, cut):
        with self.store._mutex:
            db = self.store._db
            db.execute('BEGIN IMMEDIATE')
            try:
                db.execute('UPDATE search_state SET ack_id=?,ack_nonce=? WHERE singleton=1 AND ack_id<=?',
                           (cut['id'], cut['nonce'], cut['id']))
                # Never delete by point/target: a concurrent later event for the
                # same point must survive this acknowledgment.
                db.execute('DELETE FROM search_outbox WHERE id<=?', (cut['id'],))
                db.execute('COMMIT')
            except BaseException:
                db.execute('ROLLBACK')
                raise

    def _points(self, db, rows):
        import numpy as np
        payloads = self.payloads(db, rows)
        return [dict(id=row['seq'] % (1 << 64), vector=np.frombuffer(row['embedding'], dtype=np.float32).tolist(),
                     payload=payloads[row['seq']]) for row in rows]

    def _check_stop(self):
        if self._stop.is_set():
            raise SearchUnavailable('Search synchronization stopped')

    def _rebuild(self, db, state, cut, deadline):
        self.ready = False
        self.transport.reset(deadline=deadline)
        self.transport.commit_checkpoint(self._marker(state, None, cut, rebuilding=True), deadline=deadline)
        cursor = None
        count = 0
        while True:
            self._check_stop()
            where = 'WHERE i.seq>?' if cursor is not None else ''
            rows = db.execute(f'''SELECT i.*,v.embedding,v.current FROM vector_items i
                JOIN memory_vectors v ON v.rowid=i.seq {where} ORDER BY i.seq LIMIT 256''',
                (cursor,) if cursor is not None else ()).fetchall()
            if not rows:
                break
            self.transport.upsert(self._points(db, rows), deadline=deadline)
            cursor = rows[-1]['seq']
            count += len(rows)
        self.transport.finish_build(deadline=deadline)
        while True:
            self._check_stop()
            info = self.transport.info(deadline=deadline)
            if (info and info['status'] == 'green' and info['points_count'] == count + 1
                    and info['indexed_vectors_count'] == count + 1):
                break
            self._stop.wait(.2)
        self.transport.commit_checkpoint(self._marker(state, cut), deadline=deadline)
        self._ack(cut)

    def _sync(self, db, state, cut, deadline, *, rebuild):
        info = self.transport.info(deadline=deadline)
        checkpoint = self.transport.checkpoint(deadline=deadline)
        valid = (self.transport.configured(info) and isinstance(checkpoint, dict)
                 and checkpoint.get('db_identity') == state['db_identity']
                 and checkpoint.get('signature') == self.signature and not checkpoint.get('rebuilding')
                 and self._known(db, state, checkpoint.get('cursor')))
        if valid and checkpoint.get('pending') is not None:
            valid = self._known(db, state, checkpoint['pending']) and checkpoint['pending']['id'] <= cut['id']
        if not valid or checkpoint['cursor']['id'] > cut['id']:
            if not rebuild:
                raise SearchUnavailable('Search index rebuild is pending')
            self._rebuild(db, state, cut, deadline)
            return
        cursor = checkpoint['cursor']
        if cursor == cut and checkpoint.get('pending') is None:
            self._ack(cut) if state['ack_id'] != cut['id'] else None
            return
        events = db.execute('SELECT id,seq FROM search_outbox WHERE id>? AND id<=? ORDER BY id',
                            (cursor['id'], cut['id'])).fetchall()
        if any(row['seq'] is None for row in events):
            if not rebuild:
                raise SearchUnavailable('Search index rebuild is pending')
            self._rebuild(db, state, cut, deadline)
            return
        ids = sorted({row['seq'] for row in events})
        if len(ids) > 512 and not rebuild:
            raise SearchUnavailable('Search synchronization backlog is pending')
        # This durable intent makes a partial remote update distinguishable from
        # a ready old cursor after restoring an older SQLite snapshot.
        self.transport.commit_checkpoint(self._marker(state, cursor, cut), deadline=deadline)
        for offset in range(0, len(ids), 256):
            self._check_stop()
            batch = ids[offset:offset + 256]
            marks = ','.join('?' for _ in batch)
            rows = db.execute(f'''SELECT i.*,v.embedding,v.current FROM vector_items i
                JOIN memory_vectors v ON v.rowid=i.seq WHERE i.seq IN ({marks})''', batch).fetchall()
            present = {row['seq'] for row in rows}
            self.transport.delete([seq % (1 << 64) for seq in batch if seq not in present], deadline=deadline)
            self.transport.upsert(self._points(db, rows), deadline=deadline)
        self.transport.commit_checkpoint(self._marker(state, cut), deadline=deadline)
        self._ack(cut)

    @contextmanager
    def snapshot(self):
        deadline = time.monotonic() + 2
        if not self._lock.acquire(timeout=max(0, deadline - time.monotonic())):
            raise SearchUnavailable('Search synchronization is pending')
        try:
            with self.store.read_snapshot() as db:
                state, cut = self._cut(db)
                try:
                    self._sync(db, state, cut, deadline, rebuild=False)
                except SearchUnavailable:
                    self.ready = False
                    raise
                self.ready = True
                self._active = (db, deadline)
                try:
                    yield db
                finally:
                    self._active = None
        finally:
            self._lock.release()

    def search(self, db, vector, limit, *, kind, pool, scope=None, type=None):
        if self._active is None or self._active[0] is not db:
            raise SearchUnavailable('Search requires the synchronized evidence snapshot')
        equals = dict(kind=kind, current=True, **{pool: True})
        if scope is not None:
            equals[scope] = True
        if type is not None:
            equals['type'] = type
        rows = self.transport.search(vector, limit, equals, deadline=self._active[1])
        result, records = [], []
        for row in rows:
            seq = row['seq'] if row['seq'] < 1 << 63 else row['seq'] - (1 << 64)
            item = db.execute('''SELECT i.*,v.current FROM vector_items i JOIN memory_vectors v
                ON v.rowid=i.seq WHERE i.seq=?''', (seq,)).fetchone()
            if item is None or item['kind'] != kind:
                raise SearchUnavailable('Search projection differs from the evidence snapshot')
            records.append(item)
            result.append(dict(kind=item['kind'], target=item['target'], version=item['version'], cosine=row['cosine']))
        # Defense only: mismatches fail the whole request. This is never a
        # global-top-K postfilter or a retry that silently loses eligible rows.
        payloads = self.payloads(db, records)
        for record in records:
            if any(payloads[record['seq']].get(key) != value for key, value in equals.items()):
                self.ready = False
                raise SearchUnavailable('Search eligibility differs from the evidence snapshot')
        return result

    def start(self):
        if self._worker is not None:
            raise RuntimeError('Search synchronizer already started')
        def run():
            restart_delay = 1
            while not self._stop.is_set():
                try:
                    with self._lock:
                        self._check_stop()
                        if not self.transport.running:
                            self.ready = False
                            self.transport.reopen()
                        with self.store.read_snapshot() as db:
                            state, cut = self._cut(db)
                            # RPCs remain bounded by the transport, with stop
                            # checks between batches. Corpus size must not turn
                            # a healthy progressing build into an hourly reset.
                            self._sync(db, state, cut, float('inf'), rebuild=True)
                    self.ready = True
                    self.error = None
                    restart_delay = 1
                    self._stop.wait(.2)
                except Exception as error:
                    self.ready = False
                    self.error = type(error).__name__
                    if not self.transport.running:
                        self._stop.wait(restart_delay)
                        restart_delay = min(30, restart_delay * 2)
                    else:
                        self._stop.wait(1)
        self._worker = Thread(target=run, name='memento-search', daemon=True)
        self._worker.start()

    def close(self):
        self._stop.set()
        if self._worker is not None:
            self._worker.join()
        self.transport.close()
