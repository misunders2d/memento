import os
import sqlite3

import pytest

from memento.store import Store


@pytest.mark.parametrize('mode', [0o750, 0o755, 0o770, 0o777])
def test_shared_data_directory_is_refused(tmp_path, mode):
    tmp_path.chmod(mode)
    with pytest.raises(RuntimeError, match='private'):
        Store(tmp_path)
    assert not (tmp_path / 'memory.sqlite').exists()


def test_service_cannot_share_configured_agent_identity(tmp_path, monkeypatch):
    monkeypatch.setenv('MEMENTO_AGENT_UID', str(os.getuid()))
    with pytest.raises(RuntimeError, match='distinct'):
        Store(tmp_path)
    assert not (tmp_path / 'memory.sqlite').exists()


@pytest.mark.parametrize('old_physical_columns', [False, True])
def test_status_index_startup_upgrade_is_atomic_and_idempotent(tmp_path, monkeypatch, caplog, old_physical_columns):
    import memento.store as store_module

    data = tmp_path / 'instance'
    store = Store(data)
    assert len([record for record in caplog.records if record.name == 'memento.schema']) == 2
    record, _ = store.capture(dict(source='fixture', event_id='one', conversation_id='thread',
        kind='user', text='Exact original Ω\x00 source.', occurred_at=None, metadata={}))
    assert store._db.execute("SELECT 1 FROM sqlite_schema WHERE name='episodes_status_cover_v1'").fetchone()
    store._db.execute('DROP INDEX episodes_status_cover_v1')
    if old_physical_columns:
        store._db.execute('DROP TRIGGER episodes_no_update')
        for column in ('text_runs', 'text_sha256', 'raw_frame'):
            store._db.execute('ALTER TABLE episodes DROP COLUMN ' + column)
        store._db.execute('DROP TABLE raw_frames')
        store._db.execute("""CREATE TRIGGER episodes_no_update BEFORE UPDATE ON episodes
            BEGIN SELECT RAISE(ABORT,'episodes are immutable'); END""")
    store.close()
    upgrade = store_module.upgrade_status_index

    def interrupted(db):
        upgrade(db)
        raise RuntimeError('Synthetic interruption after index build')

    with monkeypatch.context() as patch:
        patch.setattr(store_module, 'upgrade_status_index', interrupted)
        with pytest.raises(RuntimeError, match='Synthetic interruption'):
            Store(data)
    with sqlite3.connect(data / 'memory.sqlite') as db:
        assert db.execute('PRAGMA user_version').fetchone()[0] == 1
        assert db.execute("SELECT 1 FROM sqlite_schema WHERE name='episodes_status_cover_v1'").fetchone() is None
        assert ('text_runs' in {row[1] for row in db.execute('PRAGMA table_info(episodes)')}) != old_physical_columns
    reopened = Store(data)
    try:
        assert reopened.get(record['id']) == record
        schema_version = reopened._db.execute('PRAGMA schema_version').fetchone()[0]
        rootpage = reopened._db.execute(
            "SELECT rootpage FROM sqlite_schema WHERE name='episodes_status_cover_v1'").fetchone()[0]
        assert reopened._db.execute('PRAGMA user_version').fetchone()[0] == 1
    finally:
        reopened.close()
    caplog.clear()
    reopened = Store(data)
    try:
        assert reopened.get(record['id']) == record
        assert reopened._db.execute('PRAGMA schema_version').fetchone()[0] == schema_version
        assert reopened._db.execute(
            "SELECT rootpage FROM sqlite_schema WHERE name='episodes_status_cover_v1'").fetchone()[0] == rootpage
        assert not [record for record in caplog.records if record.name == 'memento.schema']
    finally:
        reopened.close()
