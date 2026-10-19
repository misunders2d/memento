import math
import json
from uuid import uuid4
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

np = pytest.importorskip('numpy')

from memento.graph import Graph
from memento.retrieval import Retrieval
from memento.store import Store


@pytest.fixture
def retrieval(tmp_path, monkeypatch):
    store = Store(tmp_path)
    graph = Graph(store)
    q = np.zeros(768, np.float32)
    q[0] = 1
    monkeypatch.setattr('memento.retrieval.embed_query', lambda model, text: q)
    tokenizer = lambda text, **kwargs: {'offset_mapping': [(i, i + 1) for i in range(len(text))]}
    result = Retrieval(graph, model=SimpleNamespace(tokenizer=tokenizer))
    yield result
    store.close()


def assertion(r, text, *, scope=None, type='knowledge'):
    key = uuid4().hex
    episode, _ = r.store.capture(dict(source='test', event_id=key, conversation_id='c',
        kind='user', text=text, occurred_at=None, metadata={}))
    evidence = [dict(episode_id=episode['id'], quote=text)]
    node = r.graph.remember(command_id=key, actor='user', why='explicit assertion',
                            type=type, text=text, scope=scope or ['professional'], evidence=evidence)
    return node['id'], evidence


def index(r, node_id, cosine):
    vector = np.zeros(768, np.float32)
    vector[0], vector[1] = cosine, math.sqrt(1 - cosine*cosine)
    with r.store._mutex:
        r.store._db.execute('BEGIN IMMEDIATE')
        try:
            r._put(node_id, 'node', 1, vector)
            r.store._db.execute('COMMIT')
        except BaseException:
            r.store._db.execute('ROLLBACK')
            raise


def test_keyword_bonus_uses_one_formula_even_below_semantic_floor(retrieval):
    r = retrieval
    stronger, _ = assertion(r, 'alpha needle')
    weaker, _ = assertion(r, 'beta needle')
    index(r, stronger, .70)
    index(r, weaker, .60)
    assert [hit['id'] for hit in r.recall('needle', effort='cheap')] == [stronger, weaker]


def test_superseded_vectors_cannot_crowd_out_current_matches(retrieval):
    r = retrieval
    replacements = set()
    for i in range(25):
        old, evidence = assertion(r, f'original assertion {i}')
        index(r, old, .95)
        changed = r.graph.supersede(command_id=f'correction-{i}', actor='user', why='corrected',
            old=old, new=dict(type='knowledge', text=f'current assertion {i}',
                             scope=['professional']), evidence=evidence)
        replacements.add(changed['superseded_by'])
        index(r, changed['superseded_by'], .80)
    hit = r.recall('unmatchedword', effort='cheap', limit=1)[0]
    assert hit['id'] in replacements
    assert hit['state'] == 'active'


def test_recall_is_bounded_without_losing_full_history(retrieval):
    r = retrieval
    text = 'needle ' + 'large evidence ' * 1000
    node, _ = assertion(r, text)
    hit = r.recall('needle', effort='cheap')[0]
    assert hit['id'] == node
    assert len(hit['text']) <= 240
    assert len(hit['evidence'][0]['quote_excerpt']) <= 240
    assert hit['evidence'][0]['end'] == len(text)
    full = r.graph.history(node)[0]
    assert full['text'] == text
    assert full['evidence'][0]['quote'] == text


def test_one_invalid_embedding_does_not_block_other_assertions(retrieval, monkeypatch):
    r = retrieval
    good = {assertion(r, f'valid assertion {i}')[0] for i in range(5)}
    bad, _ = assertion(r, 'bad assertion')
    def encode(model, texts):
        vectors = np.zeros((len(texts), 768), np.float32)
        for i, text in enumerate(texts):
            if 'bad assertion' not in text:
                vectors[i, 0] = 1
        return vectors
    monkeypatch.setattr('memento.retrieval.embed_texts', encode)
    r.index_pending()
    assert {hit['id'] for hit in r.recall('unmatchedword', effort='cheap')} == good
    with pytest.raises(RuntimeError, match='failed'):
        r.health()
    assert r.store._db.execute('SELECT resolved FROM index_failures WHERE target=?', (bad,)).fetchone()[0] == 0


def candidate(r, text, *, type='knowledge', kind='memory'):
    key = uuid4().hex
    episode, _ = r.store.capture(dict(source='candidate-fixture', event_id=key, conversation_id='c',
        kind='user', text=text, occurred_at='2000-01-01T00:00:00+00:00', metadata={}))
    target = 'c_' + uuid4().hex
    def create(commit):
        r.store._db.execute('INSERT INTO extraction_runs VALUES (?,?,?,?,?,?,?,?)',
            (key, commit, episode['id'], 'extract-v1', 'fixture-model', '[]', '[]', datetime.now(timezone.utc).isoformat()))
        r.store._db.execute('INSERT INTO candidates VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
            (target, key, kind, type, text, 'Explicit source candidate', None, None, .8,
             None, None, json.dumps(['professional'])))
        r.graph._evidence(target, 1, [dict(episode_id=episode['id'], quote=text)])
        return dict(id=target)
    r.graph._command(key, 'fixture', 'Create provenance-backed candidate fixture', dict(op='extract-fixture'), create)
    return target, episode


def test_unconsolidated_candidate_is_immediately_recalled_as_unconfirmed(retrieval):
    r = retrieval
    target, episode = candidate(r, '😀 Orion uses a blue launch checklist.')
    hit = r.recall('Orion', effort='cheap')[0]
    assert hit['id'] == target and hit['tier'] == 'candidate' and hit['confirmed'] is False
    assert hit['confidence'] == .8 and hit['valid_from'] is None
    span = hit['evidence'][0]
    assert episode['text'][span['start']:span['end']] == episode['text']
    assert r.store._db.execute('SELECT count(*) FROM node_versions').fetchone()[0] == 0


def test_episode_vectors_reach_exact_unicode_spans_only_in_deep_recall(retrieval, monkeypatch):
    r = retrieval
    text = ('Начало 😀\\t  строки\\r\\n ' * 100) + ' The Comet calibration succeeded.'
    episode, _ = r.store.capture(dict(source='raw-fixture', event_id='deep', conversation_id='c',
        kind='tool', text=text, occurred_at='2000-01-01T00:00:00+00:00', metadata={}))
    def encode(model, texts):
        vectors = np.zeros((len(texts), 768), np.float32)
        for position, passage in enumerate(texts):
            vectors[position, 0 if 'Comet' in passage else 1] = 1
        return vectors
    monkeypatch.setattr('memento.retrieval.embed_texts', encode)
    while r.index_pending():
        pass
    assert r.recall('Comet', effort='cheap') == []
    hit = r.recall('Comet', effort='deep')[0]
    span = hit['evidence'][0]
    assert hit['id'] == episode['id'] and hit['tier'] == 'raw' and hit['confirmed'] is False
    assert 'Comet' in text[span['start']:span['end']]
    assert span['quote_excerpt'] == text[span['start']:span['end']][:240]
    assert hit['valid_from'] is None
    assert r.graph.history(episode['id'])[0]['text'] == text


def test_candidate_entity_matching_is_tentative_and_never_merges_identities(retrieval, monkeypatch):
    r = retrieval
    target, episode = candidate(r, 'Orion', type='project', kind='entity')
    node = r.graph.remember(command_id='known-project', actor='fixture', why='Explicit known entity',
        type='project', text='Orion', scope=['professional'],
        evidence=[dict(episode_id=episode['id'], quote='Orion')])
    def encode(model, texts):
        vectors = np.zeros((len(texts), 768), np.float32)
        vectors[:, 0] = 1
        return vectors
    monkeypatch.setattr('memento.retrieval.embed_texts', encode)
    r.index_pending()
    hit = next(item for item in r.recall('Orion', effort='cheap') if item['id'] == target)
    assert [match['target'] for match in hit['entity_matches']] == [node['id']]
    assert hit['entity_matches'][0]['name_match'] == 1
    assert len(r.graph.history(node['id'])) == 1
    assert r.store._db.execute('SELECT count(*) FROM entity_merges').fetchone()[0] == 0
    assert r.graph.history(target)[0]['entity_matches'][0]['target'] == node['id']


def test_terminal_candidate_vectors_cannot_crowd_out_live_candidates(retrieval):
    r = retrieval
    live, _ = candidate(r, 'a surviving low-similarity candidate')
    def put(target, cosine):
        vector = np.zeros(768, np.float32)
        vector[0], vector[1] = cosine, math.sqrt(1 - cosine*cosine)
        r._put(target, 'candidate', 1, vector)
    put(live, .80)
    for position in range(25):
        target, _ = candidate(r, f'rejected source assertion {position}')
        put(target, .95)
        def reject(commit):
            r.store._db.execute('INSERT INTO candidate_decisions VALUES (?,?,?,?,?,?)',
                (uuid4().hex, target, commit, 'reject', None, None))
            return dict(id=target)
        r.graph._command(f'reject-{position}', 'consolidator-fixture', 'Unsupported assertion rejected',
                         dict(op='reject-candidate', id=target), reject)
    assert [hit['id'] for hit in r.recall('unmatchedword', effort='cheap', limit=1)] == [live]


def test_human_exclusion_hides_candidate_and_raw_vectors_without_deleting_history(retrieval, monkeypatch):
    r = retrieval
    target, episode = candidate(r, 'Orion has a private launch checklist.')
    def encode(model, texts):
        vectors = np.zeros((len(texts), 768), np.float32)
        vectors[:, 0] = 1
        return vectors
    monkeypatch.setattr('memento.retrieval.embed_texts', encode)
    r.index_pending()
    preview = r.graph._forget_preview(target)
    r.graph._forget(command_id='exclude-source', actor='human-fixture', why='Active-memory exclusion',
                    target=target, token=preview['token'])
    assert r.recall('Orion', effort='cheap') == []
    assert r.recall('Orion', effort='deep') == []
    assert r.graph.history(episode['id'])[0]['text'] == episode['text']


@pytest.mark.parametrize('channel', ['semantic', 'keyword'])
def test_scope_and_archive_filter_precede_retrieval_limits(retrieval, channel):
    r = retrieval
    wanted, _ = assertion(r, 'needle ' + 'background ' * 100, scope=['personal'])
    for position in range(25):
        wrong, _ = assertion(r, f'needle wrong {position}', scope=['professional'])
        archived, _ = assertion(r, f'needle archived {position}', scope=['personal'])
        r.store._db.execute('UPDATE node_stats SET archived=1 WHERE node_id=?', (archived,))
        if channel == 'semantic':
            index(r, wrong, .99)
            index(r, archived, .98)
    if channel == 'semantic':
        index(r, wanted, .80)
    query = 'unmatchedword' if channel == 'semantic' else 'needle'
    assert [hit['id'] for hit in r.recall(query, effort='cheap', limit=1,
                                         scope='personal')] == [wanted]


def test_dual_scope_entity_is_visible_in_both_scopes_and_default_recall(retrieval):
    r = retrieval
    shared, _ = assertion(r, 'Orion shared project', type='project',
                          scope=['professional', 'personal'])
    personal, _ = assertion(r, 'Orion private decision', scope=['personal'])
    professional, _ = assertion(r, 'Orion work decision', scope=['professional'])
    assert {hit['id'] for hit in r.recall('Orion', scope='personal')} == {shared, personal}
    assert {hit['id'] for hit in r.recall('Orion', scope='professional')} == {shared, professional}
    assert {hit['id'] for hit in r.recall('Orion')} == {shared, personal, professional}
    assert r.graph.history(shared)[0]['scope'] == ['personal', 'professional']


def test_committed_citation_is_idempotent_and_changes_rank_not_assertions(retrieval):
    from memento.importance import cite, refresh

    r = retrieval
    cited, evidence = assertion(r, 'Orion cited assertion')
    other, _ = assertion(r, 'Orion other assertion')
    index(r, cited, .80)
    index(r, other, .81)
    history = r.graph.history(cited)
    raw = r.store.get(evidence[0]['episode_id'])
    assert r.recall('unmatchedword', effort='cheap')[0]['id'] == other
    assert r.store._db.execute('SELECT access_count FROM node_stats WHERE node_id=?',
                               (cited,)).fetchone()[0] == 0
    result = cite(r.graph, event_id='committed-answer', ids=[cited, cited],
                  query='unmatchedword', shown=[cited, other])
    assert cite(r.graph, event_id='committed-answer', ids=[cited, cited],
                query='unmatchedword', shown=[cited, other]) == result
    assert r.store._db.execute('SELECT access_count FROM node_stats WHERE node_id=?',
                               (cited,)).fetchone()[0] == 1
    assert r.recall('unmatchedword', effort='cheap')[0]['id'] == cited
    assert r.graph.history(cited) == history
    assert r.store.get(raw['id']) == raw
    refresh(r.store, datetime(2100, 1, 1, tzinfo=timezone.utc))
    assert r.recall('Orion', effort='cheap') == []
    assert {hit['id'] for hit in r.recall('Orion', effort='deep')
            if hit['tier'] == 'durable'} == {cited, other}
    cite(r.graph, event_id='later-answer', ids=[cited])
    assert [hit['id'] for hit in r.recall('Orion', effort='cheap')] == [cited]
    assert r.graph.history(cited) == history


def test_deferred_loop_is_still_available_to_ordinary_recall(retrieval):
    r = retrieval
    target, _ = assertion(r, 'Orion is awaiting the report')
    r.graph.loop(action='open', id=target, command_id='open-wait')
    history = r.graph.history(target)
    state = r.graph.loop(action='later', id=target, days=2.5, command_id='later-wait')
    assert state['deferred_until'] is not None
    assert r.graph.loop(action='list')['loops'] == []
    assert [hit['id'] for hit in r.recall('Orion', scope='professional')] == [target]
    factual = lambda rows: [{key: value for key, value in row.items()
                             if key not in ('loop', 'loop_events')} for row in rows]
    assert factual(r.graph.history(target)) == factual(history)


def test_transport_skip_projection_is_not_a_semantic_raw_memory(retrieval, monkeypatch):
    r = retrieval
    transport, _ = r.store.capture(dict(source='native', event_id='transport', conversation_id='c',
        kind='native_record', text='{"message":"Orion uses cobalt"}', occurred_at=None,
        metadata={'extraction_mode': 'skip'}))
    message, _ = r.store.capture(dict(source='native', event_id='message', conversation_id='c',
        kind='user', text='Orion uses cobalt', occurred_at=None, metadata={}))
    vector = np.zeros((1, 768), np.float32)
    vector[0, 0] = 1
    monkeypatch.setattr('memento.retrieval.embed_texts', lambda model, texts: vector)
    # A previously indexed transport envelope must not leak through semantic recall.
    r.store._db.execute('INSERT INTO episode_chunks VALUES (?,?,?,?)',
        ('e_old_transport', transport['id'], 0, len(transport['text'])))
    r._put('e_old_transport', 'episode', 1, vector[0])
    while r.index_pending():
        pass
    hits = r.recall('unmatchedword', effort='deep')
    assert {hit['id'] for hit in hits} == {message['id']}
    assert r.graph.history(transport['id'])[0]['text'] == transport['text']


def test_failed_item_retries_after_index_cursor_has_passed_it(retrieval, monkeypatch):
    r = retrieval
    failed, _ = assertion(r, 'temporary encoder failure')
    later, _ = assertion(r, 'working encoder input')
    def encode(model, texts):
        if any('temporary encoder failure' in text for text in texts):
            raise RuntimeError('temporary encoder failure')
        vectors = np.zeros((len(texts), 768), np.float32)
        vectors[:, 0] = 1
        return vectors
    monkeypatch.setattr('memento.retrieval.embed_texts', encode)
    r.index_pending()
    assert {hit['id'] for hit in r.recall('unmatchedword', effort='cheap')} == {later}
    def recovered(model, texts):
        vectors = np.zeros((len(texts), 768), np.float32)
        vectors[:, 0] = 1
        return vectors
    monkeypatch.setattr('memento.retrieval.embed_texts', recovered)
    r.store._db.execute('UPDATE index_failures SET next_attempt=0')
    r.index_pending()
    assert {hit['id'] for hit in r.recall('unmatchedword', effort='cheap')} == {failed, later}
    r.health()


def test_restart_fills_older_index_gaps_and_later_appends(retrieval, monkeypatch):
    r = retrieval
    older, _ = assertion(r, 'older assertion with no vector')
    newer, _ = assertion(r, 'newer assertion already indexed')
    index(r, newer, .80)
    def encode(model, texts):
        vectors = np.zeros((len(texts), 768), np.float32)
        vectors[:, 0] = 1
        return vectors
    monkeypatch.setattr('memento.retrieval.embed_texts', encode)
    restarted = Retrieval(r.graph, model=r.model)
    while restarted.index_pending():
        pass
    appended, _ = assertion(r, 'appended after the first index drain')
    while restarted.index_pending():
        pass
    assert {hit['id'] for hit in restarted.recall('unmatchedword', effort='cheap')} == {
        older, newer, appended}
