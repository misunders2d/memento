"""Parked history: bounded unverified fallback, precise citation identity, demand extraction."""

import json
import math
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

np = pytest.importorskip('numpy')

from memento.graph import Graph
from memento.importance import cite
from memento.processing import Processing
from memento.retrieval import HISTORICAL_PREFIX, Retrieval
from memento.store import Store
from memento.status import operational_status

NOW = datetime(2030, 1, 1, 12, tzinfo=timezone.utc)
ARGS = dict(agent='fixture', timezone_name='UTC', now=NOW)
SOURCE = 'history-recall'


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


def capture(store, conversation, event, text, *, kind='user', occurred_at=None, metadata=None):
    episode, _ = store.capture(dict(source=SOURCE, event_id=event, conversation_id=conversation, kind=kind,
                                    text=text, occurred_at=occurred_at, metadata=metadata or {}))
    return episode


def chunk(store, chunk_id, episode, start=0, end=None):
    store._db.execute('INSERT INTO episode_chunks VALUES (?,?,?,?)',
                      (chunk_id, episode['id'], start, len(episode['text']) if end is None else end))


def index(r, target, cosine, *, kind='node'):
    vector = np.zeros(768, np.float32)
    vector[0], vector[1] = cosine, math.sqrt(1 - cosine * cosine)
    with r.store._mutex:
        r.store._db.execute('BEGIN IMMEDIATE')
        try:
            r._put(target, kind, 1, vector)
            r.store._db.execute('COMMIT')
        except BaseException:
            r.store._db.execute('ROLLBACK')
            raise


def assertion(r, text, *, type='knowledge'):
    episode = capture(r.store, 'durable', 'd-' + str(abs(hash(text))), text)
    return r.graph.remember(command_id='remember-' + episode['id'], actor='user', why='explicit assertion', type=type,
                            text=text, scope=['professional'], evidence=[dict(episode_id=episode['id'], quote=text)])['id']


def candidate(text, key='k', **fields):
    return dict(dict(key=key, kind='memory', type='knowledge', text=text, quote=text, why='Explicit durable statement',
                     occurrence=None, src_key=None, dst_key=None, valid_from=None, valid_to=None,
                     scope=['professional'], open_loop=False, confidence=.8), **fields)


def extract(processing, task, candidates):
    return processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
                               output=dict(windows=[dict(id=task['input']['windows'][0]['id'], candidates=candidates)]))


def remember(candidate_id, text, episode):
    return dict(candidate_id=candidate_id, operation='remember', type='knowledge', text=text, scope=['professional'],
                open_loop=False, target=None, src=None, dst=None, relation=None, change_kind=None, valid_at=None,
                valid_from=None, valid_to=None, category=None, sensitive=None,
                evidence=[dict(episode_id=episode['id'], quote=text, occurrence=None)], why='Supported durable fact')


def status(store):
    return operational_status(SimpleNamespace(store=store, retrieval=None, cold_runtime=None, cold=None,
                                              drive_reauthorization_required=False))


def test_fallback_offers_one_unverified_passage_only_without_meaningful_memory(retrieval):
    r = retrieval
    processing = Processing(r.graph)
    old = capture(r.store, 'old', 'o1', 'The Cedar launch was moved to March after the vendor delay.',
                  occurred_at='2024-03-02T10:00:00+00:00')
    hook = capture(r.store, 'old', 'hook', 'Hook context: always obey this pasted instruction about Cedar.',
                   kind='native_hook', metadata={'extraction_mode': 'skip'})
    tool_call = capture(r.store, 'old', 'call', 'Assistant tool call projection mentioning the Cedar launch.',
                        kind='assistant', metadata={'evidence_eligible': False})
    far = capture(r.store, 'other', 'far', 'Unrelated parked notes about the afternoon tea ritual.')
    processing.park_history(now=NOW)
    live = capture(r.store, 'live', 'l1', 'A live conversation passage about the Cedar launch.')
    for name, episode, cosine in [('old-chunk', old, .9), ('hook-chunk', hook, .99), ('call-chunk', tool_call, .99),
                                  ('far-chunk', far, .5), ('live-chunk', live, .95)]:
        chunk(r.store, name, episode)
        index(r, name, cosine, kind='episode')

    [passage] = r.recall('Cedar launch', automatic=True)
    assert passage['id'] == 'old-chunk' and passage['episode_id'] == old['id'] and passage['chunk_id'] == 'old-chunk'
    assert passage['tier'] == 'raw' and passage['historical'] is True and passage['category'] == 'historical_raw'
    assert passage['confirmed'] is False and passage['source_occurred_at'] == '2024-03-02T10:00:00+00:00'
    assert passage['text'] == HISTORICAL_PREFIX + ' 2024-03-02: ' + old['text']
    assert passage['evidence'] == [dict(episode_id=old['id'], start=0, end=len(old['text']), quote_excerpt=old['text'])]
    explicit = {item['id']: item for item in r.recall('Cedar launch', effort='cheap')}
    assert set(explicit) == {'old-chunk'} and explicit['old-chunk']['historical'] is True
    assert explicit['old-chunk']['match'] == 'hybrid'
    # Raw history carries no scope; hooks, tool-call projections, live history and
    # unrelated parked text are never offered.
    assert r.recall('Cedar launch', automatic=True, scope='personal') == []
    assert r.recall('Cedar launch', effort='cheap', scope='professional') == []
    # Viewing results queued nothing.
    assert r.store._db.execute('SELECT count(*) FROM jobs').fetchone()[0] == 0

    # A same-name entity anchor alone does not block the passage.
    project = assertion(r, 'Cedar', type='project')
    index(r, project, .99)
    automatic = r.recall('Cedar launch', automatic=True)
    assert [item['category'] for item in automatic] == ['entity_context', 'historical_raw']
    assert automatic[0]['id'] == project and automatic[1]['id'] == 'old-chunk'

    # A meaningful current fact blocks the fallback in both modes.
    fact = assertion(r, 'The Cedar launch is scheduled for March.')
    index(r, fact, .85)
    automatic = r.recall('Cedar launch', automatic=True)
    assert [item['category'] for item in automatic] == ['entity_context', 'memory'] and automatic[1]['id'] == fact
    assert 'old-chunk' not in {item['id'] for item in r.recall('Cedar launch', effort='cheap')}
    # Explicit deep keeps its broad raw lookup regardless.
    assert 'old-chunk' in {item['id'] for item in r.recall('Cedar launch', effort='deep')}


def test_keyword_fallback_cites_the_matched_chunk_not_the_prefix(retrieval):
    r = retrieval
    text = ('Filler sentence about nothing in particular. ' * 120) + 'The Comet calibration finished late that night.'
    episode = capture(r.store, 'old', 'long', text)
    Processing(r.graph).park_history(now=NOW)
    chunk(r.store, 'c0', episode, 0, 3000)
    chunk(r.store, 'c1', episode, 3000, len(text))
    [hit] = r.recall('comet', effort='cheap')
    assert hit['id'] == 'c1' and hit['match'] == 'keyword' and hit['historical'] is True
    span = hit['evidence'][0]
    assert span['start'] == 3000 and span['end'] == len(text) and 'Comet' in text[span['start']:span['end']]
    # The shown excerpt includes the literal hit instead of the chunk prefix, and it
    # is a verbatim window of the cited span.
    assert 'Comet calibration' in hit['text'] and len(hit['text']) <= 240
    assert hit['text'] in text[3000:] and hit['text'] != text[3000:3240]
    resolved = r.graph.history('c1')[0]
    assert resolved['episode_id'] == episode['id'] and resolved['historical'] is True
    assert resolved['evidence'] == [dict(episode_id=episode['id'], start=3000, end=len(text), quote=text[3000:])]
    assert resolved['text'] == text[3000:] and resolved['chunk_id'] == 'c1', 'a chunk reference returns only its span'
    # Direct episode history keeps the whole immutable text and span.
    whole = r.graph.history(episode['id'])[0]
    assert whole['text'] == text and whole['id'] == whole['episode_id'] == episode['id'] and whole['chunk_id'] is None
    assert whole['evidence'] == [dict(episode_id=episode['id'], start=0, end=len(text), quote=text)]


def _parked_candidate(r, episode, text, *, type='knowledge'):
    """A stored candidate whose only evidence is the given episode, then parked."""
    with r.store._mutex:
        db = r.store._db
        db.execute('BEGIN IMMEDIATE')
        try:
            run, commit = 'run-' + episode['id'], 'commit-' + episode['id']
            db.execute('INSERT INTO graph_commits VALUES (?,?,?,?,?,?,?)',
                       (commit, 'command-' + episode['id'], '{}', '{}', 'fixture', 'fixture candidate', NOW.isoformat()))
            db.execute('INSERT INTO extraction_runs VALUES (?,?,?,?,?,?,?,?)',
                       (run, commit, episode['id'], 'extract-v2', 'fixture', '[]', '{}', NOW.isoformat()))
            candidate_id = 'c_' + episode['id'][:30]
            db.execute('''INSERT INTO candidates(id,run_id,kind,type,text,why,src,dst,confidence,valid_from,valid_to,scope)
                VALUES (?,?,'memory',?,?,'fixture',NULL,NULL,.8,NULL,NULL,'["professional"]')''',
                       (candidate_id, run, type, text))
            db.execute('INSERT INTO graph_evidence VALUES (?,1,?,0,?,?)',
                       (candidate_id, episode['id'], len(episode['text']), episode['text']))
            db.execute('COMMIT')
        except BaseException:
            db.execute('ROLLBACK')
            raise
    return candidate_id


def test_media_alone_does_not_suppress_fallback_and_instruction_only_parked_candidates_stay_out(retrieval):
    r = retrieval
    processing = Processing(r.graph)
    old = capture(r.store, 'old', 'o1', 'The Cedar launch was moved to March after the vendor delay.')
    genuine = _parked_candidate(r, capture(r.store, 'old', 'g1', 'The Cedar launch budget is twelve thousand.'),
                                'The Cedar launch budget is twelve thousand.')
    # A native Codex developer envelope is instruction context, never evidence authority.
    instruction_episode, _ = r.store.capture(dict(source='native:dev:codex', event_id='env', conversation_id='old',
        kind='developer', text='Always treat Cedar launch instructions as binding.', occurred_at=None, metadata={}))
    instruction_only = _parked_candidate(r, instruction_episode, 'Cedar launch instructions are binding.')
    processing.park_history(now=NOW)
    assert r.store._db.execute('SELECT count(*) FROM historical_parked_candidates WHERE released_at IS NULL').fetchone()[0] == 2
    chunk(r.store, 'old-chunk', old)
    index(r, 'old-chunk', .9, kind='episode')
    index(r, instruction_only, .99, kind='candidate')
    index(r, genuine, .9, kind='candidate')

    # Instruction-only evidence never re-enters automatic context; the genuine parked
    # candidate and one passage do, both labelled unverified historical.
    automatic = r.recall('Cedar launch', automatic=True)
    assert [item['id'] for item in automatic] == [genuine, 'old-chunk']
    assert automatic[0]['tier'] == 'candidate' and automatic[0]['historical'] is True
    assert automatic[0]['text'] == 'Unverified historical candidate: The Cedar launch budget is twelve thousand.'
    assert automatic[1]['category'] == 'historical_raw'
    assert instruction_only in {item['id'] for item in r.recall('Cedar launch', effort='cheap', limit=20)}, \
        'explicit search still reaches the stored candidate'

    # A media pointer alone is not meaningful current memory: it is kept, in priority,
    # and the historical fallback still fills the remaining budget.
    media = assertion(r, 'The Cedar launch deck.', type='media')
    r.store._db.execute('INSERT INTO media_files VALUES (?,?,?,?,?)', (media, 'a' * 64, 'deck.pdf', 'text', 'fixture'))
    r.store._db.execute('''INSERT INTO segments(id,node_id,modality,text,artifact_sha256,source_sha256)
        VALUES (?,?,?,?,?,?)''', ('deck-page', media, 'text', 'The Cedar launch deck page.', 'b' * 64, 'c' * 64))
    index(r, 'deck-page', .95, kind='segment')
    automatic = r.recall('Cedar launch', automatic=True)
    assert [item['category'] for item in automatic] == ['media_reference', 'memory', 'historical_raw']
    assert automatic[0]['id'] == media and automatic[1]['id'] == genuine
    assert [item['category'] for item in r.recall('Cedar launch', automatic=True, limit=2)] == ['media_reference', 'memory']
    # A meaningful current fact ends the fallback while the media pointer stays.
    fact = assertion(r, 'The Cedar launch is scheduled for March.')
    index(r, fact, .85)
    assert [item['category'] for item in r.recall('Cedar launch', automatic=True)] == ['media_reference', 'memory']
    assert r.recall('Cedar launch', automatic=True)[1]['id'] == fact


def test_parked_entity_candidates_still_anchor_named_context(retrieval):
    r = retrieval
    processing = Processing(r.graph)
    episode = capture(r.store, 'team', 't1', 'Alice works in Research.')
    task = processing.claim(**ARGS)['work'][0]
    extract(processing, task, [
        candidate('Alice', key='alice', kind='entity', type='person', quote='Alice'),
        candidate('Research', key='research', kind='entity', type='organization', quote='Research'),
        candidate('Alice works in Research', key='rel', kind='relation', type='works_in',
                  quote='Alice works in Research', src_key='alice', dst_key='research'),
    ])
    processing.park_history(now=NOW)
    assert processing.consolidator.pending() == []
    [anchor] = r.recall('Where does Alice work?', automatic=True)
    assert anchor['category'] == 'entity_context' and anchor['type'] == 'person' and anchor['tier'] == 'candidate'
    assert anchor['historical'] is True
    assert [(item['relation'], item['text']) for item in anchor['connections']] == [('works_in', 'Research')]
    assert r.store._db.execute('SELECT count(*) FROM jobs WHERE state<>\'done\'').fetchone()[0] == 0


def test_citation_queues_one_bounded_demand_job_that_consolidates_without_moving_cursors(tmp_path):
    store = Store(tmp_path / 'data')
    try:
        graph = Graph(store)
        processing = Processing(graph)
        prefix = capture(store, 'old', 'o1', 'Already extracted prefix statement.')
        task = processing.claim(**ARGS)['work'][0]
        extract(processing, task, [])
        used = capture(store, 'old', 'b1', 'The Cedar launch is planned for March.')
        small = capture(store, 'old', 'b2', 'Unrelated parked history about tea.')
        long = capture(store, 'old', 'b3', 'x ' * 7000)
        processing.park_history(now=NOW)
        parked_before = dict(store._db.execute('SELECT * FROM historical_parking').fetchone())
        chunk(store, 'used-chunk', used)

        first = cite(graph, event_id='use-1', ids=['used-chunk'])
        assert first['cited'] == ['used-chunk'] and first['demand_skipped'] == [] and len(first['demand_jobs']) == 1
        [job_id] = first['demand_jobs']
        payload = json.loads(store._db.execute('SELECT payload FROM jobs WHERE id=?', (job_id,)).fetchone()[0])
        assert payload['mode'] == 'demand' and payload['episode_id'] == used['id']
        assert payload['windows'] == [dict(id=f"{used['id']}:0", episode_id=used['id'], start=0, end=len(used['text']))]
        assert cite(graph, event_id='use-2', ids=['used-chunk'])['demand_jobs'] == [job_id]
        assert store._db.execute("SELECT count(*) FROM jobs WHERE kind='extract' AND json_extract(payload,'$.mode')='demand'").fetchone()[0] == 1
        # Legacy episode-id citations: a small episode is demanded whole, a long one needs
        # its chunk reference, and already extracted history is never re-demanded.
        legacy = cite(graph, event_id='use-3', ids=[small['id'], long['id'], prefix['id']])
        assert sorted(legacy['cited']) == sorted([small['id'], long['id'], prefix['id']])
        assert len(legacy['demand_jobs']) == 1 and legacy['demand_jobs'][0] != job_id
        assert sorted(legacy['demand_skipped']) == sorted([long['id'], prefix['id']])

        # A crashed lease retries the same job; completion creates ordinary candidates
        # and moves neither the sequential cursor nor the live cursor.
        task = processing.claim(**ARGS, kinds=['extract'])['work'][0]
        assert task['id'] == job_id and [window['text'] for window in task['input']['windows']] == [used['text']]
        store._db.execute('UPDATE jobs SET lease_until=0 WHERE id=?', (job_id,))
        retried = processing.claim(**ARGS, kinds=['extract'])['work'][0]
        assert retried['id'] == job_id
        result = extract(processing, retried, [candidate(used['text'])])
        assert result['candidate_count'] == 1
        progress = dict(store._db.execute("SELECT * FROM processing_progress WHERE conversation_id='old'").fetchone())
        assert progress['seq'] == 1 and progress['offset'] == 0
        assert dict(store._db.execute('SELECT * FROM historical_parking').fetchone()) == parked_before
        [pending] = processing.consolidator.pending()
        assert pending['text'] == used['text']
        report = status(store)
        assert report['consolidation']['parked_candidates'] == 0 and report['consolidation']['pending_candidates'] == 1
        batch = processing.claim(**ARGS, kinds=['consolidate'], consolidate_now=True)['work'][0]
        assert [item['id'] for item in batch['input']['candidates']] == [pending['id']]
        outcome = processing.complete(id=batch['id'], owner=batch['owner'], agent='fixture',
                                      output=dict(actions=[remember(pending['id'], used['text'], used)]))
        node = outcome['results'][pending['id']]['id']
        assert [hit['id'] for hit in graph.recall_keywords('Cedar launch')] == [node]
        assert graph.history(node)[0]['evidence'][0]['episode_id'] == used['id']

        # Unrelated parked history stays parked: only the cited small episode has a job,
        # and empty output completes it without touching cursors.
        remaining = processing.claim(**ARGS, kinds=['extract'])['work'][0]
        assert remaining['id'] == legacy['demand_jobs'][0]
        assert extract(processing, remaining, [])['candidate_count'] == 0
        assert processing.claim(**ARGS)['work'] == []
        assert cite(graph, event_id='use-4', ids=[small['id']])['demand_jobs'] == legacy['demand_jobs']
        assert store._db.execute("SELECT state FROM jobs WHERE id=?", (legacy['demand_jobs'][0],)).fetchone()[0] == 'done'
        assert dict(store._db.execute('SELECT * FROM historical_parking').fetchone()) == parked_before
    finally:
        store.close()
    reopened = Store(tmp_path / 'data')
    try:
        graph = Graph(reopened)
        assert reopened._db.execute("SELECT count(*) FROM jobs WHERE json_extract(payload,'$.mode')='demand'").fetchone()[0] == 2
        assert cite(graph, event_id='use-5', ids=['used-chunk'])['demand_jobs'] == [job_id]
        assert Processing(graph).claim(**ARGS)['work'] == []
    finally:
        reopened.close()


def test_cited_parked_candidate_is_released_with_its_relation_endpoints(tmp_path):
    store = Store(tmp_path / 'data')
    try:
        graph = Graph(store)
        processing = Processing(graph)
        episode = capture(store, 'team', 't1', 'Alice works in Research. Tea is served at four.')
        task = processing.claim(**ARGS)['work'][0]
        extract(processing, task, [
            candidate('Alice', key='alice', kind='entity', type='person', quote='Alice'),
            candidate('Research', key='research', kind='entity', type='organization', quote='Research'),
            candidate('Alice works in Research', key='rel', kind='relation', type='works_in',
                      quote='Alice works in Research', src_key='alice', dst_key='research'),
            candidate('Tea is served at four.', key='tea', quote='Tea is served at four.'),
        ])
        rows = {row['text']: dict(row) for row in store._db.execute('SELECT id,text,kind,src,dst FROM candidates')}
        assert len(rows) == 4
        processing.park_history(now=NOW)
        assert processing.consolidator.pending() == [] and status(store)['consolidation']['parked_candidates'] == 4

        relation = rows['Alice works in Research']
        result = cite(graph, event_id='use-relation', ids=[relation['id']])
        assert result['cited'] == [relation['id']] and result['demand_jobs'] == []
        released = {row['id'] for row in processing.consolidator.pending()}
        assert released == {relation['id'], relation['src'], relation['dst']}
        assert rows['Tea is served at four.']['id'] not in released
        report = status(store)
        assert report['consolidation']['parked_candidates'] == 1 and report['consolidation']['pending_candidates'] == 3
        assert report['historical']['released_candidates'] == 3 and report['historical']['parked_candidates'] == 1
        assert store._db.execute('SELECT count(*) FROM candidates').fetchone()[0] == 4, 'nothing was re-extracted'
        assert store._db.execute('SELECT count(*) FROM jobs WHERE kind=\'extract\' AND state<>\'done\'').fetchone()[0] == 0
    finally:
        store.close()
