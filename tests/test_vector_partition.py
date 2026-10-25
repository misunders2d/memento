"""Kind partitioning changes physical vector layout, never identity or evidence."""

import sqlite3
import struct

import pytest

from memento.graph import Graph
from memento.portable import _connect, export_archive, import_archive
from memento.store import Store


LEGACY_VECTORS = '''CREATE VIRTUAL TABLE memory_vectors USING vec0(
    embedding float[768] distance_metric=cosine,current boolean)'''
VECTOR = struct.pack('768f', 1, *([0] * 767))
LEGACY_INVALIDATION = {
    'candidate_vector_terminal': '''CREATE TRIGGER candidate_vector_terminal AFTER INSERT ON candidate_decisions
        WHEN NEW.decision IN ('promote','reject')
        BEGIN UPDATE memory_vectors SET current=0 WHERE rowid IN (
            SELECT seq FROM vector_items WHERE kind='candidate' AND target=NEW.candidate_id); END''',
    'forgotten_candidate_vector': '''CREATE TRIGGER forgotten_candidate_vector AFTER INSERT ON forgotten_targets
        WHEN NEW.kind='candidate'
        BEGIN UPDATE memory_vectors SET current=0 WHERE rowid IN (
            SELECT seq FROM vector_items WHERE kind='candidate' AND target=NEW.target); END''',
    'forgotten_source_vectors': '''CREATE TRIGGER forgotten_source_vectors AFTER INSERT ON forgotten_sources
        BEGIN UPDATE memory_vectors SET current=0 WHERE rowid IN (
            SELECT i.seq FROM vector_items i JOIN episode_chunks x ON x.id=i.target
            JOIN episodes e ON e.id=x.episode_id WHERE i.kind='episode'
                AND e.source=NEW.source AND e.event_id=NEW.event_id
            UNION SELECT i.seq FROM vector_items i JOIN candidates c ON c.id=i.target
            JOIN extraction_runs r ON r.id=c.run_id JOIN episodes e ON e.id=r.episode_id
            WHERE i.kind='candidate' AND e.source=NEW.source AND e.event_id=NEW.event_id); END''',
}


def test_fresh_layout_keeps_current_only_in_regular_metadata(tmp_path):
    store = Store(tmp_path / 'data')
    try:
        db = store._db
        assert 'current' in {row[1] for row in db.execute('PRAGMA table_info(vector_items)')}
        assert 'current' not in {row[1] for row in db.execute('PRAGMA table_info(memory_vectors)')}
        db.execute("INSERT INTO vector_items(target,kind,version) VALUES ('pending','node',1)")
        assert db.execute("SELECT current FROM vector_items WHERE target='pending'").fetchone()[0] == 1
        for invalid in (2, -1, None):
            with pytest.raises(sqlite3.IntegrityError):
                db.execute("UPDATE vector_items SET current=? WHERE target='pending'", (invalid,))
    finally:
        store.close()


def _legacy_invalidation(db):
    for name, statement in LEGACY_INVALIDATION.items():
        db.execute(f'DROP TRIGGER {name}')
        db.execute(statement)


def _vectors(db):
    if 'current' in {column[1] for column in db.execute('PRAGMA table_info(memory_vectors)')}:
        return [tuple(row) for row in db.execute('SELECT rowid,embedding,current FROM memory_vectors ORDER BY rowid')]
    return [tuple(row) for row in db.execute('''SELECT v.rowid,v.embedding,i.current FROM memory_vectors v
        LEFT JOIN vector_items i ON i.seq=v.rowid ORDER BY v.rowid''')]


def _logical(db):
    tables = [row[0] for row in db.execute('''SELECT name FROM sqlite_schema
        WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'memory_vectors%' ''')]
    return {table: sorted((tuple(row) for row in db.execute(
                f'SELECT {"seq,target,kind,version" if table == "vector_items" else "*"} FROM "{table}"')), key=repr)
            for table in tables}


def _legacy(data, header=2):
    store = Store(data)
    Graph(store)
    episode, _ = store.capture(dict(source='partition-fixture', event_id='original', conversation_id='c',
        kind='user', text='Exact original Ω source for vector migration.', occurred_at=None, metadata={}))
    db = store._db
    for number, kind in enumerate(('node', 'candidate', 'episode', 'segment', 'node'), 1):
        seq = number * 17
        blob = struct.pack('768f', 0, 1, *([0] * 766)) if number == 5 else VECTOR
        db.execute('INSERT INTO vector_items(seq,target,kind,version,current) VALUES (?,?,?,1,?)',
                   (seq, f'target-{number}', kind, int(number != 5)))
        db.execute('INSERT INTO memory_vectors(rowid,embedding,kind) VALUES (?,?,?)', (seq, blob, kind))
    # An indexed metadata row without embedding is valid unfinished work.
    db.execute("INSERT INTO vector_items(seq,target,kind,version) VALUES (999,'not-yet-indexed','node',1)")
    db.execute('BEGIN IMMEDIATE')
    db.execute('''CREATE TABLE legacy_stage AS SELECT v.rowid AS seq,v.embedding,i.current
        FROM memory_vectors v JOIN vector_items i ON i.seq=v.rowid''')
    db.execute('DROP TABLE memory_vectors')
    db.execute(LEGACY_VECTORS)
    db.execute('INSERT INTO memory_vectors(rowid,embedding,current) SELECT seq,embedding,current FROM legacy_stage')
    db.execute('DROP TABLE legacy_stage')
    db.execute('COMMIT')
    _legacy_invalidation(db)
    db.execute('ALTER TABLE vector_items DROP COLUMN current')
    db.execute(f'PRAGMA user_version={header}')
    before = _vectors(db), _logical(db)
    store.close()
    return episode, before


@pytest.mark.parametrize('header', [1, 2, 3])
def test_legacy_vector_partition_upgrade_is_exact_atomic_and_idempotent(tmp_path, monkeypatch, caplog, header):
    import memento.store as store_module

    data = tmp_path / 'data'
    episode, before = _legacy(data, header)
    with _connect(data / 'memory.sqlite', readonly=True) as db:
        triggers = db.execute("SELECT name,sql FROM sqlite_schema WHERE type='trigger' ORDER BY name").fetchall()
    upgrade = store_module.upgrade_vector_partitions

    def interrupted(db):
        upgrade(db)
        raise RuntimeError('Synthetic interruption after vector rebuild')

    with monkeypatch.context() as patch:
        patch.setattr(store_module, 'upgrade_vector_partitions', interrupted)
        with pytest.raises(RuntimeError, match='Synthetic interruption'):
            Store(data)
    with _connect(data / 'memory.sqlite', readonly=True) as db:
        assert db.execute('PRAGMA user_version').fetchone()[0] == header
        assert 'kind' not in {row[1] for row in db.execute('PRAGMA table_info(memory_vectors)')}
        assert (_vectors(db), _logical(db)) == before
        assert db.execute("SELECT name,sql FROM sqlite_schema WHERE type='trigger' ORDER BY name").fetchall() == triggers
    caplog.clear()
    store = Store(data)
    try:
        db = store._db
        assert db.execute('PRAGMA user_version').fetchone()[0] == 4
        assert (_vectors(db), _logical(db)) == before
        assert store.get(episode['id']) == episode
        assert db.execute('SELECT current FROM vector_items WHERE seq=999').fetchone()[0] == 1
        assert db.execute('SELECT current FROM vector_items WHERE seq=85').fetchone()[0] == 0
        assert 'current' not in {column[1] for column in db.execute('PRAGMA table_info(memory_vectors)')}
        assert [(row['rowid'], row['kind']) for row in db.execute(
            'SELECT rowid,kind FROM memory_vectors ORDER BY rowid')] == [
                (17, 'node'), (34, 'candidate'), (51, 'episode'), (68, 'segment'), (85, 'node')]
    finally:
        store.close()
    caplog.clear()
    store = Store(data)
    try:
        assert (_vectors(store._db), _logical(store._db)) == before
        assert not [row for row in caplog.records if row.name == 'memento.schema']
    finally:
        store.close()


@pytest.mark.parametrize('invalid', ['orphan', 'unknown_kind'])
def test_invalid_legacy_vector_kind_fails_without_discarding_rows(tmp_path, invalid):
    data = tmp_path / 'data'
    _legacy(data)
    with _connect(data / 'memory.sqlite') as db:
        if invalid == 'orphan':
            db.execute('INSERT INTO memory_vectors(rowid,embedding,current) VALUES (1001,?,0)', (VECTOR,))
        else:
            db.execute("UPDATE vector_items SET kind='unknown-kind' WHERE seq=17")
        before = _vectors(db), _logical(db)
    with pytest.raises(RuntimeError, match='vector.*kind'):
        Store(data)
    with _connect(data / 'memory.sqlite', readonly=True) as db:
        assert db.execute('PRAGMA user_version').fetchone()[0] == 2
        assert (_vectors(db), _logical(db)) == before


@pytest.mark.parametrize('header', [1, 2, 3])
def test_legacy_vector_archive_restore_migrates_before_publication(tmp_path, header):
    data = tmp_path / 'data'
    episode, before = _legacy(data, header)
    archive = tmp_path / 'vectors.tar'
    export_archive(data, archive)
    destination = tmp_path / 'restored'
    result = import_archive(archive, destination)
    assert 'user_version' in result['rebased']
    with _connect(destination / 'memory.sqlite', readonly=True) as db:
        assert db.execute('PRAGMA user_version').fetchone()[0] == 4
        assert (_vectors(db), _logical(db)) == before
        assert 'kind' in {row[1] for row in db.execute('PRAGMA table_info(memory_vectors)')}
    restored = Store(destination)
    try:
        assert restored.get(episode['id']) == episode
    finally:
        restored.close()


def test_partition_header_without_partition_is_refused_before_mutation(tmp_path):
    data = tmp_path / 'data'
    _legacy(data, 4)
    with _connect(data / 'memory.sqlite', readonly=True) as db:
        before = _vectors(db), _logical(db)
    with pytest.raises(RuntimeError, match='requires immutable vector partitions and ordinary current metadata'):
        Store(data)
    with _connect(data / 'memory.sqlite', readonly=True) as db:
        assert (_vectors(db), _logical(db)) == before
        assert db.execute('PRAGMA user_version').fetchone()[0] == 4


def test_partition_header_fences_previous_writers(tmp_path):
    store = Store(tmp_path / 'data')
    try:
        version = store._db.execute('PRAGMA user_version').fetchone()[0]
        # The exact preceding release guard, which accepted headers 1/2/3.
        assert version not in (1, 2, 3)
        assert version == 4
    finally:
        store.close()


def test_vector_rebuild_requires_an_explicit_transaction(tmp_path):
    from memento.schema import upgrade_vector_partitions

    data = tmp_path / 'data'
    _legacy(data)
    with _connect(data / 'memory.sqlite') as db:
        before = db.execute('SELECT name,sql FROM sqlite_schema ORDER BY name').fetchall()
        assert not db.in_transaction
        with pytest.raises(RuntimeError, match='requires an active transaction'):
            upgrade_vector_partitions(db)
        assert db.execute('SELECT name,sql FROM sqlite_schema ORDER BY name').fetchall() == before


def test_interruption_after_dropping_old_vec0_rolls_back_exactly(tmp_path, monkeypatch):
    import memento.schema as schema

    data = tmp_path / 'data'
    _, before = _legacy(data)
    with monkeypatch.context() as patch:
        # Fail CREATE only after the old virtual table and shadows were dropped.
        patch.setattr(schema, '_MEMORY_VECTORS', 'CREATE VIRTUAL TABLE memory_vectors USING missing_module()')
        with pytest.raises(sqlite3.OperationalError, match='no such module'):
            Store(data)
    with _connect(data / 'memory.sqlite', readonly=True) as db:
        assert db.execute('PRAGMA user_version').fetchone()[0] == 2
        assert 'kind' not in {row[1] for row in db.execute('PRAGMA table_info(memory_vectors)')}
        assert (_vectors(db), _logical(db)) == before
    store = Store(data)
    try:
        assert (_vectors(store._db), _logical(store._db)) == before
    finally:
        store.close()


def test_bulk_current_updates_preserve_identity_and_embedding_bytes(tmp_path):
    data = tmp_path / 'data'
    _legacy(data)
    store = Store(data)
    try:
        db = store._db
        before = _vectors(db)
        # Metadata revisions are independent of ordinary mutable eligibility.
        db.execute('UPDATE vector_items SET version=version')
        db.execute('UPDATE vector_items SET version=2 WHERE seq=17')
        db.execute('UPDATE vector_items SET version=2 WHERE seq=34')
        assert _vectors(db) == before
        metadata = [tuple(row) for row in db.execute('SELECT seq,target,kind,version FROM vector_items ORDER BY seq')]
        db.execute('BEGIN IMMEDIATE')
        db.execute('UPDATE vector_items SET current=0 WHERE seq IN (17,51,999)')
        assert [tuple(row) for row in db.execute('SELECT seq,target,kind,version FROM vector_items ORDER BY seq')] == metadata
        assert [row[2] for row in _vectors(db)] == [0, 1, 0, 1, 0]
        assert db.execute('SELECT current FROM vector_items WHERE seq=999').fetchone()[0] == 0
        db.execute('ROLLBACK')
        assert _vectors(db) == before
        # Reactivation is ordinary metadata; literal-text repair updates only the
        # existing embedding, retaining its rowid and physical kind partition.
        db.execute('UPDATE vector_items SET current=0 WHERE seq=17')
        db.execute('UPDATE memory_vectors SET embedding=? WHERE rowid=17', (VECTOR,))
        assert db.execute('SELECT current FROM vector_items WHERE seq=17').fetchone()[0] == 0
        db.execute('UPDATE vector_items SET current=1 WHERE seq=17')
        assert _vectors(db) == before
        assert db.execute('SELECT kind FROM memory_vectors WHERE rowid=17').fetchone()[0] == 'node'
    finally:
        store.close()


def test_metadata_without_embedding_keeps_existing_inactive_flag(tmp_path):
    data = tmp_path / 'data'
    _, before = _legacy(data)
    with _connect(data / 'memory.sqlite') as db:
        db.execute('ALTER TABLE vector_items ADD COLUMN current INTEGER NOT NULL DEFAULT 1 CHECK(current IN (0,1))')
        db.execute('UPDATE vector_items SET current=0 WHERE seq=999')
    store = Store(data)
    try:
        assert (_vectors(store._db), _logical(store._db)) == before
        assert store._db.execute('SELECT current FROM vector_items WHERE seq=999').fetchone()[0] == 0
        assert store._db.execute('SELECT 1 FROM memory_vectors WHERE rowid=999').fetchone() is None
    finally:
        store.close()


def test_partitioned_storage_repairs_legacy_invalidation_triggers_on_reopen(tmp_path):
    data = tmp_path / 'data'
    _legacy(data)
    store = Store(data)
    _legacy_invalidation(store._db)
    store.close()
    store = Store(data)
    try:
        db = store._db
        db.execute("INSERT INTO candidate_decisions VALUES ('terminal','target-2','fixture','reject',NULL,NULL)")
        assert db.execute('SELECT current FROM vector_items WHERE seq=34').fetchone()[0] == 0
        assert db.execute('SELECT current FROM vector_items WHERE seq=17').fetchone()[0] == 1
    finally:
        store.close()


def test_vector_byte_mismatch_rejects_rebuild_even_when_counts_match(tmp_path):
    import sqlite_vec
    from memento.schema import upgrade_vector_partitions

    class TamperedConnection(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            result = super().execute(sql, parameters)
            if sql.startswith('INSERT INTO memory_vectors(rowid,embedding,kind)'):
                super().execute('UPDATE memory_vectors SET embedding=? WHERE rowid=17',
                                (struct.pack('768f', 0, 1, *([0] * 766)),))
            return result

    data = tmp_path / 'data'
    _, before = _legacy(data)
    db = sqlite3.connect(data / 'memory.sqlite', factory=TamperedConnection)
    try:
        db.enable_load_extension(True)
        sqlite_vec.load(db)
        db.enable_load_extension(False)
        db.execute('BEGIN IMMEDIATE')
        with pytest.raises(RuntimeError, match='preserve every stored vector'):
            upgrade_vector_partitions(db)
        db.rollback()
    finally:
        db.close()
    with _connect(data / 'memory.sqlite', readonly=True) as db:
        assert db.execute('PRAGMA user_version').fetchone()[0] == 2
        assert (_vectors(db), _logical(db)) == before
