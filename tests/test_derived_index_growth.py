"""Unchanged attach/pin versions reuse the derived vector and keep one current FTS document per node.

Canonical node_versions, graph_evidence and exact history stay immutable and
complete; only the derived vector_items/memory_vectors rows and node_fts change.
"""

import struct

import pytest

from memento.curation import attach
from memento.graph import Graph
from memento.portable import _connect
from memento.store import Store
from test_curation import curated  # noqa: F401  (fixture reuse)

VECTOR = struct.pack('768f', 1, *([0] * 767))

LEGACY_TRIGGER = """CREATE TRIGGER node_index AFTER INSERT ON node_versions
    BEGIN INSERT INTO node_fts(id,version,text) VALUES (NEW.id,NEW.version,NEW.text); END"""


def _vector(store, node_id, version=1, data=VECTOR):
    seq = store._db.execute("INSERT INTO vector_items(target,kind,version) VALUES (?,'node',?)",
                            (node_id, version)).lastrowid
    store._db.execute('INSERT INTO memory_vectors(rowid,embedding,current) VALUES (?,?,1)', (seq, data))
    return seq


def _indexed(store, node_id):
    return [(row['version'], row['current'], row['embedding']) for row in store._db.execute('''
        SELECT i.version,v.current,v.embedding FROM vector_items i JOIN memory_vectors v ON v.rowid=i.seq
        WHERE i.kind='node' AND i.target=? ORDER BY i.version''', (node_id,))]


def _fts(store, node_id):
    return [(row['rowid'], row['version']) for row in store._db.execute(
        'SELECT rowid,version FROM node_fts WHERE id=? ORDER BY version', (node_id,))]


def _attach(graph, key, target, evidence):
    request = dict(op='attach', target=target, evidence=evidence)
    return graph._command(key, 'model-fixture', 'explicit synthetic corroboration', request,
                          lambda commit: attach(graph, commit, target, evidence))


def _hits(graph, query, **options):
    return [(hit['id'], hit['version']) for hit in graph.recall_keywords(query, **options)]


def _consistent_documents(db):
    """Every FTS document is the latest version of its node, keyed by the stable mapping."""
    ids = db.execute('SELECT count(DISTINCT id) FROM node_versions').fetchone()[0]
    assert db.execute('SELECT count(*) FROM node_fts').fetchone()[0] == ids
    assert db.execute('SELECT count(*) FROM node_fts_documents').fetchone()[0] == ids
    assert db.execute('''SELECT count(*) FROM node_fts f JOIN node_fts_documents d ON d.seq=f.rowid AND d.id=f.id
        JOIN node_versions n ON n.id=f.id AND n.version=f.version
        WHERE n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)''').fetchone()[0] == ids


def _nodes(store, graph, count):
    episode, _ = store.capture(dict(source='test', event_id='fixture', conversation_id='c', kind='user',
                                    text='Fixture evidence for every synthetic node.', occurred_at=None, metadata={}))
    evidence = [dict(episode_id=episode['id'], quote='Fixture evidence for every synthetic node.')]
    ids = []
    for number in range(count):
        node = graph.remember(command_id=f'n{number}', actor='fixture', why='explicit synthetic assertion',
                              type='knowledge', text=f'Synthetic marker{number} fact.', scope=['professional'],
                              evidence=evidence)
        _attach(graph, f'a{number}', node['id'], evidence)
        ids.append(node['id'])
    return ids, evidence


def test_repeated_attach_and_pin_keep_one_live_vector_and_fts_document(curated):
    store, graph, evidence, remember, review, approve = curated
    node = remember('memory', 'knowledge', 'Preserve this decision.')
    _vector(store, node['id'])
    more, _ = store.capture(dict(source='test', event_id='more', conversation_id='c', kind='user',
                                 text='The decision was confirmed again.', occurred_at=None, metadata={}))
    corroboration = [dict(episode_id=more['id'], quote='The decision was confirmed again.')]
    assert _attach(graph, 'attach-1', node['id'], corroboration) == dict(id=node['id'], version=2)
    assert _attach(graph, 'attach-2', node['id'], evidence) == dict(id=node['id'], version=3)
    approve(review(dict(op='pin', node_id=node['id'], pinned=True), 'pin'), 'pin-yes')
    history = graph.history(node['id'])
    assert [row['version'] for row in history] == [1, 2, 3, 4]
    assert [row['pinned'] for row in history] == [0, 0, 0, 1]
    first = {(e['episode_id'], e['start'], e['end']) for e in history[0]['evidence']}
    assert first and all(first <= {(e['episode_id'], e['start'], e['end']) for e in row['evidence']} for row in history)
    assert [len(row['evidence']) for row in history] == [1, 2, 2, 2]
    assert store._db.execute('SELECT count(*) FROM graph_evidence WHERE target=?', (node['id'],)).fetchone()[0] == 7
    # Exactly one derived vector row, now owned by the current version, bytes untouched.
    assert _indexed(store, node['id']) == [(4, 1, VECTOR)]
    assert store._db.execute('SELECT count(*) FROM vector_items').fetchone()[0] == 1
    assert store._db.execute('SELECT count(*) FROM memory_vectors').fetchone()[0] == 1
    assert [version for _, version in _fts(store, node['id'])] == [4]
    _consistent_documents(store._db)
    assert _hits(graph, 'decision') == [(node['id'], 4)]


def test_attach_without_indexed_vector_leaves_the_new_version_pending(curated):
    pytest.importorskip('numpy')
    from memento.retrieval import _INDEXABLE
    store, graph, evidence, remember, review, approve = curated
    node = remember('memory', 'knowledge', 'Preserve this decision.')
    assert _attach(graph, 'attach', node['id'], evidence) == dict(id=node['id'], version=2)
    assert _indexed(store, node['id']) == []
    pending = store._db.execute(f'''WITH indexable AS ({_INDEXABLE})
        SELECT id,version FROM indexable WHERE kind='node' AND id=?''', (node['id'],)).fetchall()
    assert [(row['id'], row['version']) for row in pending] == [(node['id'], 2)]
    # Ordinary indexing of the pending unit then owns the single derived row.
    _vector(store, node['id'], 2)
    assert _indexed(store, node['id']) == [(2, 1, VECTOR)]
    assert _hits(graph, 'decision') == [(node['id'], 2)]


def test_text_change_invalidates_without_reusing_the_old_vector(curated):
    store, graph, evidence, remember, review, approve = curated
    node = remember('memory', 'knowledge', 'Preserve this decision.')
    _vector(store, node['id'])
    result = graph.supersede(command_id='correct', actor='fixture', why='explicit corrected decision',
        old=node['id'], new=dict(type='knowledge', text='Preserve the corrected decision.', scope=['professional']),
        evidence=evidence)
    replacement = result['superseded_by']
    assert _indexed(store, node['id']) == [(1, 0, VECTOR)]
    assert _indexed(store, replacement) == []
    assert store._db.execute('SELECT count(*) FROM memory_vectors').fetchone()[0] == 1
    assert [version for _, version in _fts(store, node['id'])] == [2]
    assert [version for _, version in _fts(store, replacement)] == [1]
    _consistent_documents(store._db)
    assert _hits(graph, 'corrected') == [(replacement, 1)]
    assert _hits(graph, 'decision') == [(replacement, 1)]
    assert [row['text'] for row in graph.history(node['id'])][:2] == ['Preserve this decision.'] * 2


def test_failed_attach_rolls_back_vector_reuse_and_fts_document(curated):
    store, graph, evidence, remember, review, approve = curated
    node = remember('memory', 'knowledge', 'Preserve this decision.')
    _vector(store, node['id'])
    before = (_indexed(store, node['id']), _fts(store, node['id']), graph.history(node['id']))
    bad = [dict(episode_id=evidence[0]['episode_id'], quote='This quote is not in the source.')]
    with pytest.raises(ValueError, match='exactly'):
        _attach(graph, 'attach-bad', node['id'], bad)
    assert not store._db.in_transaction
    assert (_indexed(store, node['id']), _fts(store, node['id']), graph.history(node['id'])) == before
    assert store._db.execute('SELECT count(*) FROM node_versions WHERE id=?', (node['id'],)).fetchone()[0] == 1
    assert _hits(graph, 'decision') == [(node['id'], 1)]


def test_older_version_insert_does_not_replace_the_current_document(curated):
    store, graph, evidence, remember, review, approve = curated
    node = remember('memory', 'knowledge', 'Preserve this decision.')
    _attach(graph, 'attach', node['id'], evidence)
    current = _fts(store, node['id'])
    row = store._db.execute('SELECT * FROM node_versions WHERE id=? AND version=1', (node['id'],)).fetchone()
    store._db.execute('INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)',
        (node['id'], 0, row['commit_id'], row['type'], 'Stale replayed historical text', row['state'],
         row['valid_from'], row['valid_to'], row['superseded_by'], row['pinned'], row['scope']))
    assert _fts(store, node['id']) == current and current[0][1] == 2
    assert _hits(graph, 'replayed') == []
    assert _hits(graph, 'decision') == [(node['id'], 2)]
    _consistent_documents(store._db)


def test_legacy_all_version_index_upgrades_once_atomically_and_idempotently(tmp_path, monkeypatch, caplog):
    import memento.store as store_module

    data = tmp_path / 'instance'
    store = Store(data)
    ids, _ = _nodes(store, Graph(store), 3)
    store.close()
    # Legacy derived state: every version indexed under automatic rowids, old trigger.
    db = _connect(data / 'memory.sqlite')
    db.isolation_level = None
    try:
        db.execute('DROP TRIGGER node_index_current_v1')
        db.execute('DROP TABLE node_fts_documents')
        db.execute('DELETE FROM node_fts')
        db.execute('INSERT INTO node_fts(id,version,text) SELECT id,version,text FROM node_versions')
        db.execute(LEGACY_TRIGGER)
        assert db.execute('SELECT count(*) FROM node_fts').fetchone()[0] == 6
    finally:
        db.close()
    upgrade = store_module.upgrade_node_fts

    def interrupted(db):
        upgrade(db)
        raise RuntimeError('Synthetic interruption after index rebuild')

    with monkeypatch.context() as patch:
        patch.setattr(store_module, 'upgrade_node_fts', interrupted)
        with pytest.raises(RuntimeError, match='Synthetic interruption'):
            Store(data)
    db = _connect(data / 'memory.sqlite', readonly=True)
    try:
        names = {row[0] for row in db.execute('SELECT name FROM sqlite_schema')}
        assert 'node_index' in names and 'node_index_current_v1' not in names and 'node_fts_documents' not in names
        assert db.execute('SELECT count(*) FROM node_fts').fetchone()[0] == 6
        assert db.execute('PRAGMA user_version').fetchone()[0] == 2
    finally:
        db.close()
    caplog.clear()
    reopened = Store(data)
    try:
        assert len([r for r in caplog.records if r.name == 'memento.schema' and 'text index' in r.getMessage()]) == 2
        names = {row[0] for row in reopened._db.execute('SELECT name FROM sqlite_schema')}
        assert 'node_index' not in names and 'node_index_current_v1' in names
        assert reopened._db.execute('PRAGMA user_version').fetchone()[0] == 2
        _consistent_documents(reopened._db)
        assert reopened._db.execute('SELECT count(*) FROM node_versions').fetchone()[0] == 6
        graph = Graph(reopened)
        for number, node_id in enumerate(ids):
            assert _hits(graph, f'marker{number}') == [(node_id, 2)]
            assert [row['version'] for row in graph.history(node_id)] == [1, 2]
        rowids = [row[0] for row in reopened._db.execute('SELECT rowid FROM node_fts ORDER BY rowid')]
    finally:
        reopened.close()
    caplog.clear()
    again = Store(data)
    try:
        assert not [r for r in caplog.records if r.name == 'memento.schema']
        assert [row[0] for row in again._db.execute('SELECT rowid FROM node_fts ORDER BY rowid')] == rowids
        _consistent_documents(again._db)
    finally:
        again.close()


def test_vacuum_then_attach_and_correction_keep_every_other_node_searchable(tmp_path, caplog):
    data = tmp_path / 'instance'
    store = Store(data)
    graph = Graph(store)
    ids, evidence = _nodes(store, graph, 6)
    for node_id in ids:
        _vector(store, node_id, 2)
    store.close()
    db = _connect(data / 'memory.sqlite')
    db.isolation_level = None
    try:
        db.execute('VACUUM')
    finally:
        db.close()
    caplog.clear()
    reopened = Store(data)
    try:
        assert not [r for r in caplog.records if r.name == 'memento.schema'], 'compaction must not force a rebuild'
        graph = Graph(reopened)
        _attach(graph, 'after-vacuum', ids[0], evidence)
        corrected = graph.supersede(command_id='correct', actor='fixture', why='explicit corrected fact',
            old=ids[1], new=dict(type='knowledge', text='Synthetic corrected replacement fact.', scope=['professional']),
            evidence=evidence)
        fresh = graph.remember(command_id='fresh', actor='fixture', why='explicit synthetic assertion',
                               type='knowledge', text='Synthetic newcomer fact.', scope=['professional'], evidence=evidence)
        _consistent_documents(reopened._db)
        assert _hits(graph, 'marker0') == [(ids[0], 3)]
        assert _hits(graph, 'marker1') == []
        assert _hits(graph, 'corrected') == [(corrected['superseded_by'], 1)]
        assert _hits(graph, 'newcomer') == [(fresh['id'], 1)]
        for number in range(2, 6):
            assert _hits(graph, f'marker{number}') == [(ids[number], 2)]
        assert _indexed(reopened, ids[0]) == [(3, 1, VECTOR)]
        assert _indexed(reopened, ids[1]) == [(2, 0, VECTOR)]
        assert reopened._db.execute('SELECT count(*) FROM memory_vectors').fetchone()[0] == 6
        assert [row['version'] for row in graph.history(ids[0])] == [1, 2, 3]
    finally:
        reopened.close()
