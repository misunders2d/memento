import math
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Event
from uuid import uuid4
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

np = pytest.importorskip('numpy')

from memento.graph import Graph
from memento.retrieval import Retrieval
from memento.store import Store
from memento.status import operational_status


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


def assertion(r, text, *, scope=None, type='knowledge', occurred_at=None):
    key = uuid4().hex
    episode, _ = r.store.capture(dict(source='test', event_id=key, conversation_id='c',
        kind='user', text=text, occurred_at=occurred_at, metadata={}))
    evidence = [dict(episode_id=episode['id'], quote=text)]
    node = r.graph.remember(command_id=key, actor='user', why='explicit assertion',
                            type=type, text=text, scope=scope or ['professional'], evidence=evidence)
    return node['id'], evidence


def index(r, node_id, cosine, *, kind='node', version=1):
    vector = np.zeros(768, np.float32)
    vector[0], vector[1] = cosine, math.sqrt(1 - cosine*cosine)
    with r.store._mutex:
        r.store._db.execute('BEGIN IMMEDIATE')
        try:
            r._put(node_id, kind, version, vector)
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


def test_automatic_eligibility_precedes_vector_limit_and_explicit_entities_remain(retrieval):
    r = retrieval
    for _ in range(30):
        entity, _ = assertion(r, 'Memento', type='project')
        index(r, entity, .99)
    useful, _ = assertion(r, 'Memento stores exact source evidence for each assertion.')
    index(r, useful, .85)
    # Entity nodes never enter the semantic pass, however similar; the spelled
    # name anchors one of them explicitly, and the fact follows within the limit.
    automatic = r.recall('Memento evidence', automatic=True, limit=3)
    assert [item['category'] for item in automatic] == ['entity_context', 'entity_context', 'memory']
    assert automatic[2]['id'] == useful and automatic[2]['related'] == []
    assert all(item['type'] == 'project' and item['same_name_count'] == 30 for item in automatic[:2])
    assert [item['id'] for item in r.recall('evidence for assertions', automatic=True, limit=1)] == [useful]
    # Entity-only data still answers a small requested limit with the entity itself.
    assert [item['category'] for item in r.recall('Memento evidence', automatic=True, limit=1)] == ['entity_context']
    assert any(item['type'] == 'project' for item in r.recall('Memento', effort='cheap', limit=100))


@pytest.mark.parametrize('text', [
    '/home/example/My Project/src/memento/retrieval.py',
    'src/memento/retrieval.py', 'Memento is related to memory',
])
def test_automatic_omits_paths_and_generic_associations_even_at_high_similarity(retrieval, text):
    target, _ = assertion(retrieval, text)
    index(retrieval, target, .99)
    assert retrieval.recall('unrelated current question', automatic=True) == []
    assert retrieval.recall(text, effort='cheap')[0]['id'] == target


def test_automatic_keyword_match_never_bypasses_relevance_floor(retrieval):
    from memento.retrieval import AUTOMATIC_COSINE_FLOOR
    target, _ = assertion(retrieval, 'Needle is the old project codename.')
    index(retrieval, target, AUTOMATIC_COSINE_FLOOR - .05)
    assert retrieval.recall('Needle', automatic=True) == []
    assert retrieval.recall('Needle', effort='cheap')[0]['id'] == target


def test_calibrated_automatic_floor_keeps_weak_useful_fact_above_ordinary_negative(retrieval):
    useful, _ = assertion(retrieval, 'The synthetic user avoids peanuts because of a food allergy.')
    unrelated, _ = assertion(retrieval, 'The release requires a cobalt checklist before shipment.')
    index(retrieval, useful, .678506)
    index(retrieval, unrelated, .656186)
    assert [item['id'] for item in retrieval.recall('ingredient to exclude from lunch', automatic=True)] == [useful]


def test_automatic_collapses_duplicate_assertions_without_losing_sources(retrieval):
    r = retrieval
    first, first_evidence = assertion(r, 'The release uses a cobalt checklist.')
    second, second_evidence = assertion(r, 'The release uses a cobalt checklist!')
    index(r, first, .90)
    index(r, second, .89)
    result = r.recall('release checklist', automatic=True)
    assert len(result) == 1 and result[0]['id'] == first
    assert result[0]['duplicate_ids'] == [second]
    assert {item['episode_id'] for item in result[0]['evidence']} == {
        first_evidence[0]['episode_id'], second_evidence[0]['episode_id']}
    assert r.graph.history(second)[0]['text'].endswith('!')


def test_automatic_does_not_collapse_different_assertions_after_display_excerpt(retrieval):
    prefix = 'The supported production release checklist ' + 'contains verified steps ' * 15
    first, _ = assertion(retrieval, prefix + 'for the blue environment.')
    second, _ = assertion(retrieval, prefix + 'for the green environment.')
    index(retrieval, first, .9)
    index(retrieval, second, .89)
    assert len(retrieval.recall('release checklist', automatic=True)) == 2


def test_automatic_duplicate_normalization_preserves_numeric_signs_and_operators(retrieval):
    r = retrieval
    for text in ('The blue account balance is -2 dollars.', 'The blue account balance is 2 dollars.',
                 'The accepted measurement is <3 units.', 'The accepted measurement is >3 units.'):
        target, _ = assertion(r, text)
        index(r, target, .9)
    assert len(r.recall('account balance and accepted measurement', automatic=True)) == 4


@pytest.fixture
def recency_clock(monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 10, 21, tzinfo=timezone.utc)
    monkeypatch.setattr('memento.importance.datetime', Clock)
    monkeypatch.setattr('memento.retrieval.datetime', Clock)


@pytest.mark.parametrize('tier', ['candidate', 'durable'])
@pytest.mark.parametrize('automatic', [False, True])
def test_original_source_recency_breaks_relevance_ties_without_hiding_old_imports(
        retrieval, recency_clock, monkeypatch, tier, automatic):
    r = retrieval
    make = candidate if tier == 'candidate' else assertion
    fresh, _ = make(r, 'The Cedar task uses the current report.', occurred_at='2026-10-20T00:00:00Z')
    old, _ = make(r, 'The Cedar task used the earlier report.', occurred_at='2026-05-23T07:30:02Z')
    kind = 'candidate' if tier == 'candidate' else 'node'
    index(r, old, .90, kind=kind)
    index(r, fresh, .90, kind=kind)
    r.store._db.execute('INSERT INTO node_stats(node_id,archived) VALUES (?,1) '
                        'ON CONFLICT(node_id) DO UPDATE SET archived=1', (old,))
    before = r.graph.history(old)
    if tier == 'durable':
        r.graph.link(command_id='time-related', actor='fixture', why='Related source facts',
            a=fresh, rel='related_to', b=old, evidence=before[0]['evidence'])
        assert old in {item['id'] for item in r.related(fresh, 1)}
    monkeypatch.setattr(r.store, 'get', lambda *_: pytest.fail('recency hydrated source text'))
    options = dict(automatic=automatic, effort='cheap')
    assert r.recall('unmatchedword', limit=1, **options)[0]['id'] == fresh
    hits = r.recall('unmatchedword', limit=2, **options)
    assert [hit['id'] for hit in hits] == [fresh, old]
    assert hits[1]['source_occurred_at'] == '2026-05-23T07:30:02+00:00'
    age = (datetime(2026, 10, 21, tzinfo=timezone.utc) - datetime.fromisoformat('2026-05-23T07:30:02+00:00')).total_seconds() / 86400
    assert hits[1]['decay'] == pytest.approx(math.exp2(-age / 90))
    assert hits[1]['text'].startswith('Source 2026-05-23: ') is automatic
    assert hits[1]['valid_from'] is hits[1]['valid_to'] is None
    r.store._db.execute('UPDATE memory_vectors SET current=0 WHERE rowid IN '
                        '(SELECT seq FROM vector_items WHERE target=?)', (fresh,))
    assert r.recall('unmatchedword', limit=1, **options)[0]['id'] == old
    assert r.graph.history(old) == before


@pytest.mark.parametrize('tier', ['candidate', 'durable'])
def test_recency_reads_complete_evidence_without_using_display_or_capture_time(
        retrieval, recency_clock, monkeypatch, tier):
    r = retrieval
    make = candidate if tier == 'candidate' else assertion
    target, _ = make(r, 'The Cedar report has independent corroboration.', occurred_at=None)
    for number in range(5):
        episode, _ = r.store.capture(dict(source='time-evidence', event_id=str(number), conversation_id='c',
            kind='user', text='Earlier corroboration.', occurred_at='2026-01-01T00:00:00Z', metadata={}))
        r.graph._evidence(target, 1, [dict(episode_id=episode['id'], quote=episode['text'])])
    with monkeypatch.context() as patch:
        patch.setattr('memento.store.uuid4', lambda: SimpleNamespace(hex='z' * 32))
        latest, _ = r.store.capture(dict(source='time-evidence', event_id='last', conversation_id='c',
            kind='user', text='Latest corroboration.', occurred_at='2026-10-20T23:30:00-03:00', metadata={}))
    r.graph._evidence(target, 1, [dict(episode_id=latest['id'], quote=latest['text'])])
    index(r, target, .9, kind='candidate' if tier == 'candidate' else 'node')
    monkeypatch.setattr(r.store, 'get', lambda *_: pytest.fail('recency hydrated source text'))
    hit = r.recall('unmatchedword', automatic=True)[0]
    assert latest['id'] not in {ref['episode_id'] for ref in hit['evidence']}
    assert hit['source_occurred_at'] == '2026-10-21T02:30:00+00:00'
    assert hit['text'].startswith('Source 2026-10-21: ')
    assert hit['decay'] == 1


@pytest.mark.parametrize('automatic', [False, True])
def test_citation_renews_recency_without_redating_original_evidence(retrieval, recency_clock, automatic):
    from memento.importance import cite
    r = retrieval
    old, _ = assertion(r, 'The Cedar task used an earlier report.', occurred_at='2026-05-23T07:30:02Z')
    fresh, _ = assertion(r, 'The Cedar task uses a recent report.', occurred_at='2026-10-20T00:00:00Z')
    index(r, old, .90)
    index(r, fresh, .90)
    options = dict(automatic=automatic, effort='cheap')
    assert r.recall('unmatchedword', limit=1, **options)[0]['id'] == fresh
    before = r.graph.history(old)
    cite(r.graph, event_id='renew-old', ids=[old])
    hit = r.recall('unmatchedword', limit=1, **options)[0]
    assert hit['id'] == old and hit['decay'] == 1
    assert hit['source_occurred_at'] == '2026-05-23T07:30:02+00:00'
    assert hit['ranking_anchor'] == '2026-10-21T00:00:00+00:00'
    assert hit['text'].startswith('Source 2026-05-23: ') is automatic
    assert r.graph.history(old) == before


@pytest.mark.parametrize('protection', ['pinned', 'explicit_user', 'inferred'])
def test_pins_and_explicit_preferences_keep_priority_and_bridge_source_provenance(
        retrieval, recency_clock, monkeypatch, protection):
    from memento import bridge
    r = retrieval
    text = 'Use the supported Cedar installer for ordinary setup.'
    episode, _ = r.store.capture(dict(source='behavior-time', event_id=protection, conversation_id='c',
        kind='user', text=text, occurred_at='2026-01-01T00:00:00Z', metadata={}))
    behavior = dict(applicability='Ordinary Cedar setup.', conduct=text, rationale='Avoid manual errors.',
                    exceptions=['Keep consent explicit.'], authority='explicit_user' if protection == 'explicit_user' else 'inferred')
    proposal = dict(candidate_id='fixture', operation='remember', type='experience', behavior=behavior)
    def apply(commit):
        result = r.graph._apply_remember(commit, dict(type='experience', text=text, scope=['professional'],
            evidence=[dict(episode_id=episode['id'], quote=text)], pinned=protection == 'pinned'))
        return dict(results={'fixture': result})
    target = r.graph._command('behavior-time', 'fixture', 'Supported behavior fixture',
        dict(op='consolidate', output=dict(actions=[proposal])), apply)['results']['fixture']['id']
    index(r, target, .9)
    fact, _ = assertion(r, 'The Cedar installer supports local setup.', occurred_at='2026-01-01T00:00:00Z')
    index(r, fact, .8)
    original = r.graph._behavior(target)
    hits = r.recall('unmatchedword', automatic=True)
    hit = next(item for item in hits if item['id'] == target)
    assert hit['decay'] == pytest.approx(1 if protection != 'inferred' else math.exp2(-293 / 90))
    assert hit['source_occurred_at'] == '2026-01-01T00:00:00+00:00'
    class Spool:
        db = sqlite3.connect(':memory:')
        config = {}
    spool = Spool()
    spool.db.execute('CREATE TABLE recall_cache(conversation TEXT PRIMARY KEY,query TEXT,context TEXT,shown TEXT,updated REAL)')
    monkeypatch.setattr(bridge, 'http', lambda *args, **kwargs: {'memories': hits})
    try:
        rendered = bridge.passive(spool, 'time', 'Ordinary Cedar setup')['context']
        assert 'Source 2026-01-01:' in rendered and text in rendered and len(rendered.encode()) <= 1500
        assert '[fact] Source 2026-01-01: The Cedar installer supports local setup.' in rendered
    finally:
        spool.db.close()
    assert r.graph._behavior(target) == original


def test_unknown_source_time_is_neutral_and_duplicate_keeps_best_recency_rank(retrieval, recency_clock):
    r = retrieval
    old, _ = assertion(r, 'The Cedar task uses the supported report.', occurred_at='2026-01-01T00:00:00Z')
    unknown, _ = assertion(r, 'The Cedar task uses the supported report!')
    index(r, old, .95)
    index(r, unknown, .95)
    hit = r.recall('unmatchedword', automatic=True, limit=1)[0]
    assert hit['id'] == unknown and hit['duplicate_ids'] == [old]
    assert hit['source_occurred_at'] is None and hit['ranking_anchor'] is None and hit['decay'] == 1
    assert hit['text'] == 'The Cedar task uses the supported report!'
    assert len(hit['evidence']) == 2


@pytest.mark.parametrize('kind,automatic', [('segment', False), ('segment', True), ('episode', False)])
def test_media_parent_and_raw_episode_use_original_source_recency(retrieval, recency_clock, kind, automatic):
    r = retrieval
    ids = []
    for name, occurred_at, cosine in [('old', '2026-01-01T00:00:00Z', .90),
                                      ('fresh', '2026-10-20T00:00:00Z', .90)]:
        if kind == 'segment':
            target, _ = assertion(r, f'The Cedar {name} document.', type='media', occurred_at=occurred_at)
            r.store._db.execute('INSERT INTO media_files VALUES (?,?,?,?,?)', (target, 'a' * 64, 'fixture.txt', 'text', 'fixture'))
            r.store._db.execute('''INSERT INTO segments(id,node_id,modality,text,artifact_sha256,source_sha256)
                VALUES (?,?,?,?,?,?)''', (name, target, 'text', f'The Cedar {name} page.', 'b' * 64, 'c' * 64))
        else:
            episode, _ = r.store.capture(dict(source='raw-time', event_id=name, conversation_id='c',
                kind='tool', text=f'The Cedar {name} observation.', occurred_at=occurred_at, metadata={}))
            r.store._db.execute('INSERT INTO episode_chunks VALUES (?,?,0,?)', (name, episode['id'], len(episode['text'])))
            target = name  # raw results carry their stable chunk reference as id
        index(r, name, cosine, kind=kind)
        ids.append(target)
    hits = r.recall('unmatchedword', effort='deep' if kind == 'episode' else 'cheap', automatic=automatic)
    assert [item['id'] for item in hits] == (ids[-1:] if automatic else ids[::-1])
    assert hits[0]['source_occurred_at'] == '2026-10-20T00:00:00+00:00'
    assert hits[0]['text'].startswith('Source 2026-10-20: ') is automatic


def test_recency_metadata_work_is_bounded_by_target_evidence(retrieval, recency_clock):
    from memento.importance import query_recency
    r = retrieval
    target, _ = assertion(r, 'The Cedar report has a source date.', occurred_at='2026-10-20T00:00:00Z')
    record = dict(id=target, version=1)
    def measure():
        ticks = []
        db = r.store._db
        db.set_authorizer(lambda action, table, column, *_: sqlite3.SQLITE_DENY
            if action == sqlite3.SQLITE_READ and table == 'episodes' and column == 'text' else sqlite3.SQLITE_OK)
        db.set_progress_handler(lambda: ticks.append(1) or 0, 1)
        try:
            value = query_recency(r.store, record)
        finally:
            db.set_progress_handler(None, 0)
            db.set_authorizer(None)
        return value, len(ticks)
    before, work = measure()
    for number in range(200):
        episode, _ = r.store.capture(dict(source='unrelated-time', event_id=str(number), conversation_id='other',
            kind='user', text='Unrelated source payload.', occurred_at='2026-10-21T00:00:00Z', metadata={}))
        r.graph._evidence('unrelated-' + str(number), 1, [dict(episode_id=episode['id'], quote=episode['text'])])
    after, later_work = measure()
    assert after == before and later_work < work * 2


@pytest.mark.parametrize('automatic', [False, True])
@pytest.mark.parametrize('reverse_ages', [False, True])
def test_query_relevance_precedes_age_importance_and_feedback(
        retrieval, recency_clock, automatic, reverse_ages):
    from memento.importance import query_key
    r = retrieval
    dates = ['2025-01-01T00:00:00Z', '2026-10-20T00:00:00Z']
    if reverse_ages:
        dates.reverse()
    relevant, _ = assertion(r, 'The Alder report describes the requested family record.', occurred_at=dates[0])
    hub, _ = assertion(r, 'The unrelated deployment tool has many recent connections.', occurred_at=dates[1])
    index(r, relevant, .90)
    index(r, hub, .89)
    r.store._db.execute('UPDATE node_stats SET importance=? WHERE node_id=?', (.1, relevant))
    r.store._db.execute('UPDATE node_stats SET importance=? WHERE node_id=?', (1, hub))
    for target, cited in [(relevant, 0), (hub, 10)]:
        r.store._db.execute('INSERT INTO query_feedback VALUES (?,?,10,?)',
                            (query_key('unmatchedword'), target, cited))
    history = r.graph.history(relevant)
    feedback = [tuple(row) for row in r.store._db.execute('SELECT * FROM query_feedback ORDER BY target')]
    options = dict(automatic=automatic, effort='normal')
    [hit] = r.recall('unmatchedword', limit=1, **options)
    assert hit['id'] == relevant and hit['score'] == pytest.approx(.90)
    assert hit['importance'] == .1 and hit['source_occurred_at'] == dates[0].replace('Z', '+00:00')
    hits = r.recall('unmatchedword', limit=2, **options)
    assert [row['id'] for row in hits] == [relevant, hub]
    assert [row['score'] for row in hits] == pytest.approx([.90, .89])
    assert r.graph.history(relevant) == history
    assert [tuple(row) for row in r.store._db.execute('SELECT * FROM query_feedback ORDER BY target')] == feedback


@pytest.mark.parametrize('automatic', [False, True])
def test_equal_relevance_orders_by_age_then_importance_then_feedback_and_id(
        retrieval, recency_clock, automatic):
    from memento.importance import query_key
    r = retrieval
    ids = []
    for label, date, importance in [
            ('old', '2025-01-01T00:00:00Z', 1),
            ('lower importance', '2026-10-20T00:00:00Z', .3),
            ('first', '2026-10-20T00:00:00Z', .8),
            ('second', '2026-10-20T00:00:00Z', .8)]:
        target, _ = assertion(r, 'The Alder report has ' + label + ' source details.', occurred_at=date)
        index(r, target, .90)
        r.store._db.execute('UPDATE node_stats SET importance=? WHERE node_id=?', (importance, target))
        ids.append(target)
    first, second = sorted(ids[2:])
    options = dict(automatic=automatic, effort='normal', limit=4)
    assert [hit['id'] for hit in r.recall('unmatchedword', **options)] == [first, second, ids[1], ids[0]]
    r.store._db.execute('INSERT INTO query_feedback VALUES (?,?,1,1)', (query_key('unmatchedword'), second))
    expected = [first, second] if automatic else [second, first]
    assert [hit['id'] for hit in r.recall('unmatchedword', **options)] == expected + [ids[1], ids[0]]


@pytest.mark.parametrize('query', ['Rowan', 'Who is Rowan?'])
def test_normal_name_and_question_recall_keep_entity_and_family_fact_before_recent_hubs(
        retrieval, recency_clock, query):
    r = retrieval
    person, evidence = assertion(r, 'Rowan Vale', type='person', scope=['personal'],
                                occurred_at='2026-01-01T00:00:00Z')
    biography, _ = assertion(r, 'Rowan Vale is the synthetic user\'s child.', scope=['personal'],
                             occurred_at='2025-01-01T00:00:00Z')
    parent, _ = assertion(r, 'Robin Vale', type='person', scope=['personal'])
    r.graph.link(command_id='family-link', actor='fixture', why='Recorded family connection',
                 a=person, rel='related_to', b=parent, evidence=evidence)
    hub, _ = assertion(r, 'The unrelated deployment tool has recent source evidence.',
                       occurred_at='2026-10-20T00:00:00Z')
    for target, cosine, importance in [(person, .80, .2), (biography, .78, .1), (hub, .74, 1)]:
        index(r, target, cosine)
        r.store._db.execute('UPDATE node_stats SET importance=? WHERE node_id=?', (importance, target))
    history = r.graph.history(person)
    hits = r.recall(query, effort='normal', limit=2)
    assert [hit['id'] for hit in hits] == [person, biography]
    assert [row['score'] for row in hits] == pytest.approx([1.05, 1.03] if query == 'Rowan' else [.80, .78])
    assert parent in {related['id'] for related in hits[0]['related']}
    assert biography not in {related['id'] for related in hits[0]['related']}
    assert r.graph.history(person) == history


def test_automatic_followup_uses_context_but_independent_request_does_not(retrieval, monkeypatch):
    r = retrieval
    captured = []
    def encode(model, text):
        captured.append(text)
        return np.eye(1, 768, dtype=np.float32)[0]
    monkeypatch.setattr('memento.retrieval.embed_query', encode)
    context = 'We were discussing source evidence for release decisions.'
    assert r.recall('What about that?', automatic=True, context=context) == []
    assert context in captured[-1]
    assert r.recall('Translate this restaurant menu', automatic=True, context=context) == []
    assert captured[-1] == 'Translate this restaurant menu'


def test_auxiliary_followup_uses_bounded_context_without_carrying_topic_into_lunch():
    from memento.retrieval import _automatic_query
    context = 'review Memento code against its goals/restrictions'
    query = 'does it introduce unnecessary overhead?'
    assert _automatic_query(query, context) == query + '\nRecent conversation:\n' + context
    assert len(_automatic_query(query, context * 200)) <= 4096
    lunch = 'later, I am off for lunch'
    assert _automatic_query(lunch, context) == lunch
    independent = 'Can you translate this restaurant menu?'
    assert _automatic_query(independent, context) == independent


def test_native_acceptance_cannot_pass_conduct_from_an_echoed_lesson(tmp_path, monkeypatch):
    from memory_acceptance import conduct_cases
    executable = tmp_path / 'existing-codex'
    executable.write_text('synthetic executable, never invoked')
    def invoke(argv, payload=b'', **kwargs):
        if '--help' in argv:
            return b'--ignore-user-config --ephemeral --disable --sandbox --json'
        return b'{"type":"item.completed","item":{"text":"I will follow the lesson."}}\n'
    monkeypatch.setattr('memento.runner.run_process', invoke)
    context = {'context': '[memory:fixture] [inferred] Applies: ordinary setup. Conduct: use installer. Exceptions: training.',
               'shown': ['fixture']}
    results = []
    conduct_cases(executable, tmp_path, context, context, results)
    assert [(item['case'], item['passed']) for item in results] == [
        ('transfer', False), ('nonapplicability', True), ('override', False), ('adaptation', False),
        ('transfer_control_empty', False), ('adaptation_control_initial', False)]


def test_native_conduct_preserves_sanitized_invocation_error_details(tmp_path, monkeypatch):
    from memory_acceptance import conduct_cases
    executable = tmp_path / 'existing-codex'
    executable.write_text('synthetic executable, never invoked')
    class InvocationError(Exception):
        details = dict(returncode=1, stderr='Sanitized startup error', stdout='{"type":"turn.failed"}')
    def invoke(argv, payload=b'', **kwargs):
        if '--help' in argv:
            return b'--ignore-user-config --ephemeral --disable --sandbox --json'
        assert argv[argv.index('--model') + 1] == 'gpt-6.1-sol'
        assert 'model_reasoning_effort="xhigh"' in argv
        raise InvocationError('Native startup failed')
    monkeypatch.setattr('memento.runner.run_process', invoke)
    context = dict(context='', shown=[], learned_ids=[])
    results = []
    conduct_cases(executable, tmp_path, context, context, results)
    assert len(results) == 6
    assert all(item['error_details'] == InvocationError.details for item in results)


def test_native_conduct_controls_do_not_claim_learning_from_identical_compatible_actions(tmp_path, monkeypatch):
    from memory_acceptance import conduct_cases
    executable = tmp_path / 'existing-codex'
    executable.write_text('synthetic executable, never invoked')
    def invoke(argv, payload=b'', **kwargs):
        if '--help' in argv:
            return b'--ignore-user-config --ephemeral --disable --sandbox --json'
        task = payload.decode().split('Current user request:\n')[-1]
        workspace = kwargs['cwd']
        if 'INI parser' not in task:
            (workspace / 'config.json').write_text('{"endpoint":"loopback","mode":"connected"}')
            if 'training workspace' not in task and 'editing its configuration manually' not in task:
                (workspace / 'installer.log').write_text('setup invoked\n')
        return b'{"type":"item.completed","item":{"type":"agent_message","text":"Completed."}}\n'
    monkeypatch.setattr('memento.runner.run_process', invoke)
    context = dict(context='[memory:fixture] Advisory setup suggestion.', shown=['fixture'], learned_ids=['fixture'])
    results = []
    conduct_cases(executable, tmp_path, context, context, results)
    assert len(results) == 6 and all(item['compatible_conduct'] for item in results)
    for item in results:
        if item['case'] in ('transfer', 'adaptation'):
            assert item['control_comparison']['outcome_differs'] is False
            assert item['control_comparison']['differs_in_expected_direction'] is False
            assert item['control_comparison']['causal_learning_claimed'] is False


def test_native_learning_keeps_automatic_failure_while_collecting_corrections(retrieval, tmp_path, monkeypatch):
    from memory_acceptance import learning
    lesson = dict(id='native-lesson', category='behavioral_suggestion', type='experience', tier='durable',
        text='Prefer the supported setup installer when configuring a client.', authority='inferred',
        behavior=dict(applicability='Configuring a software client.', conduct='Prefer a supported installer.',
                      exceptions=['Follow current instructions.'], rationale='Manual copying caused errors.'))
    status = dict(indexing=dict(available=True, pending=0, failed=0, unprepared_episodes=0),
                  captured=dict(episodes=3, conversations=1))
    class Service:
        base_url = 'http://127.0.0.1:1'
        def get(self, path):
            if path == '/recall?limit=1':
                return {'memories': []}
            if path.startswith('/recall?'):
                return {'memories': [lesson]}
            if path.startswith('/history/'):
                return {'versions': [lesson]}
            assert path.startswith('/status')
            return status
        def post(self, path, payload):
            assert path == '/episodes'
            return {'id': payload['event_id']}
    monkeypatch.setattr('memory_acceptance.shutil.which', lambda agent: '/existing/native')
    monkeypatch.setattr('memento.runner.tick', lambda *args, **kwargs: 0)
    monkeypatch.setattr('memento.bridge.http', lambda *args, **kwargs: {'memories': []})
    def native_actions(executable, directory, initial, revised, cases, **kwargs):
        cases.extend(dict(case=name, passed=True) for name in ('transfer', 'nonapplicability', 'override',
            'adaptation', 'transfer_control_empty', 'adaptation_control_initial'))
        cases.append(dict(case='explicit_recall', passed=None, compatible_conduct=True,
                          lookup_observed=False, lookup_observation='not_observed', failed_memory_requests=[]))
    monkeypatch.setattr('memory_acceptance.conduct_cases', native_actions)
    report = {}
    with pytest.raises(RuntimeError, match='Acceptance failed'):
        learning(Service(), 'pi', report, directory=tmp_path, store=retrieval.store,
                 codex_executable=tmp_path / 'never-invoked')
    assert [item['stage'] for item in report['stages']] == ['causal_incident', 'relevant_corrections']
    assert len(report['stages'][1]['originals']) == 2
    assert report['passed'] is False and report['automatic_reach']['passed'] is False
    assert len(report['required_path_failures']) == 2
    assert report['explicit_lookup']['passed'] is None
    assert report['explicit_lookup']['lookup_observation'] == 'not_observed'
    for stage in report['stages']:
        assert stage['native_context']['context'] == ''
        assert stage['native_context']['selection_bypassed'] is False
        supplied = stage['supplied_context']
        assert supplied['selection_bypassed'] is True and supplied['learned_ids'] == ['native-lesson']
        assert 'Applies: Configuring a software client.' in supplied['context']
        assert 'Exceptions: Follow current instructions.' in supplied['context']


def test_native_transfer_measurement_uses_existing_vectors_and_preserves_all_frozen_labels(retrieval, monkeypatch):
    from memory_acceptance import NATIVE_TRANSFER_QUERIES, transfer_measurement
    target, _ = assertion(retrieval, 'A supported installer avoids copying errors during setup.')
    index(retrieval, target, .7)
    observed = []
    def encode(model, query):
        observed.append(query)
        return np.eye(1, 768, dtype=np.float32)[0]
    monkeypatch.setattr('memento.embedder.embed_query', encode)
    report = transfer_measurement(retrieval, [dict(id=target)])
    assert len(report['queries']) == len(NATIVE_TRANSFER_QUERIES) == 9
    assert sum(item['expected_applicable'] for item in report['queries']) == 4
    assert all(item['cosines'][target] == pytest.approx(.7) for item in report['queries'])
    assert observed[0] == 'Connect the Birch agent in this workspace.'
    assert 'Recent conversation:\nreview Memento code against its goals/restrictions' in observed[-1]


@pytest.mark.parametrize('retrieved', [False, True])
def test_native_explicit_lookup_records_spontaneous_observation_separately_from_compatible_fixture(tmp_path, monkeypatch, retrieved):
    from memory_acceptance import conduct_cases
    executable = tmp_path / 'existing-codex'
    executable.write_text('synthetic executable, never invoked')
    seen = []
    def invoke(argv, payload=b'', **kwargs):
        if '--help' in argv:
            return b'--ignore-user-config --ephemeral --disable --sandbox --json'
        seen.append((argv, payload.decode()))
        workspace = kwargs['cwd']
        (workspace / 'config.json').write_text('{"endpoint":"loopback","mode":"connected"}')
        if retrieved or not any('memento_fixture=' in part for part in argv):
            (workspace / 'installer.log').write_text('setup invoked\n')
        events = []
        if retrieved:
            events.append(dict(type='item.completed', item=dict(type='mcp_tool_call', server='memento_fixture',
                tool='recall', status='completed', result={'structured_content': {'memories': [{'id': 'native-lesson'}]}})))
        return b'\n'.join(json.dumps(item).encode() for item in events)
    monkeypatch.setattr('memento.runner.run_process', invoke)
    context = dict(context='[memory:native-lesson] Advisory setup suggestion.', shown=['native-lesson'],
                   learned_ids=['native-lesson'], selection_bypassed=True)
    results = []
    conduct_cases(executable, tmp_path, context, context, results, mcp_url='http://127.0.0.1:18765/mcp/')
    explicit = results[-1]
    assert explicit['case'] == 'explicit_recall' and explicit['compatible_conduct'] is True
    assert explicit['passed'] is (True if retrieved else None)
    assert explicit['lookup_observed'] is retrieved
    assert explicit['lookup_observation'] == ('observed' if retrieved else 'not_observed')
    assert explicit['observed_outcome'] == ('installer' if retrieved else 'manual')
    assert explicit['retrieved_learned_ids'] == (['native-lesson'] if retrieved else [])
    argv, prompt = seen[-1]
    assert 'mcp_servers={memento_fixture={url="http://127.0.0.1:18765/mcp/",enabled_tools=["recall","history"]}}' in argv
    assert argv[argv.index('--model') + 1] == 'gpt-6.1-sol'
    assert explicit['model'] == 'gpt-6.1-sol' and explicit['reasoning_effort'] == 'xhigh'
    assert 'model_reasoning_effort="xhigh"' in argv
    assert 'Advisory setup suggestion' not in prompt and '[memory:native-lesson]' not in prompt
    assert prompt.endswith('Current user request:\nConnect the Birch agent in this workspace.')


def test_automatic_keeps_factual_candidates_but_not_unconsolidated_experiences(retrieval):
    r = retrieval
    factual, _ = candidate(r, 'Orion uses a blue launch checklist.')
    incident, _ = candidate(r, 'A manual installer caused repeated setup friction.', type='experience')
    relation, _ = candidate(r, 'Orion is related to the launch project.', type='related_to', kind='relation')
    vector = np.eye(1, 768, dtype=np.float32)[0]
    for target in (factual, incident, relation):
        r._put(target, 'candidate', 1, vector)
    result = r.recall('launch setup', automatic=True)
    assert [item['id'] for item in result] == [factual]
    assert result[0]['confirmed'] is False and result[0]['tier'] == 'candidate'


def relation_candidate(r, src, dst, rel, text, *, scope=None):
    """A pending relation between two pending entity candidates, as extraction stores it."""
    key = uuid4().hex
    episode, _ = r.store.capture(dict(source='candidate-fixture', event_id=key, conversation_id='c',
        kind='user', text=text, occurred_at='2026-10-20T00:00:00+00:00', metadata={}))
    target = 'c_' + uuid4().hex
    def create(commit):
        r.store._db.execute('INSERT INTO extraction_runs VALUES (?,?,?,?,?,?,?,?)',
            (key, commit, episode['id'], 'extract-v1', 'fixture-model', '[]', '[]', datetime.now(timezone.utc).isoformat()))
        r.store._db.execute('INSERT INTO candidates VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
            (target, key, 'relation', rel, text, 'Explicit source candidate', src, dst, .8,
             None, None, json.dumps(scope or ['professional'])))
        r.graph._evidence(target, 1, [dict(episode_id=episode['id'], quote=text)])
        return dict(id=target)
    r.graph._command(key, 'fixture', 'Create provenance-backed relation candidate fixture', dict(op='extract-fixture'), create)
    return target


MARGARITA_PROMPT = "let's do Margarita - what comes up?"


def test_automatic_anchors_durable_entity_spelled_in_request_with_one_hop_connections(retrieval):
    r = retrieval
    person, person_evidence = assertion(r, 'Margarita', type='person')
    company, _ = assertion(r, 'Mellanni', type='organization')
    folder, evidence = assertion(r, 'Margarita keeps the team Drive evidence folder for quarterly follow-ups.')
    r.graph.link(command_id='works', actor='fixture', why='fixture', a=person, rel='works_in', b=company, evidence=evidence)
    r.graph.link(command_id='owns', actor='fixture', why='fixture', a=folder, rel='mentions', b=person, evidence=evidence)
    index(r, person, .99)
    index(r, folder, .80)
    hits = r.recall(MARGARITA_PROMPT, automatic=True)
    assert [hit['id'] for hit in hits] == [person, folder]
    anchor = hits[0]
    assert anchor['category'] == 'entity_context' and anchor['match'] == 'entity_name'
    assert anchor['tier'] == 'durable' and anchor['confirmed'] is True and anchor['scope'] == ['professional']
    assert anchor['text'].startswith('Margarita [person; professional] Connections: ')
    assert 'works_in Mellanni [organization; edge ' in anchor['text']
    assert 'Margarita keeps the team Drive evidence folder' in anchor['text'] and 'mentions this' in anchor['text']
    assert {item['relation'] for item in anchor['connections']} == {'works_in', 'mentions'}
    # The anchor carries its own entity evidence; each rendered connection names
    # its edge ID, which history() resolves, so the association survives rendering.
    assert anchor['evidence'][0]['episode_id'] == person_evidence[0]['episode_id']
    assert all(item['basis'] == 'edge' and '; edge ' + item['relation_id'] + ']' in anchor['text']
               for item in anchor['connections'])
    assert all(r.graph.history(item['relation_id']) for item in anchor['connections'])
    # The ordinary fact keeps its semantic record; anchors share the bounded limit.
    assert hits[1]['category'] == 'memory' and hits[1]['match'] == 'semantic'
    # An ordinary cap: small limits still return the spelled entity first.
    assert [hit['id'] for hit in r.recall(MARGARITA_PROMPT, automatic=True, limit=2)] == [person, folder]
    assert [hit['id'] for hit in r.recall(MARGARITA_PROMPT, automatic=True, limit=1)] == [person]
    # Scope filtering applies to anchors exactly as to facts.
    assert r.recall(MARGARITA_PROMPT, automatic=True, scope='personal') == []


def test_automatic_anchors_pending_entity_candidates_with_pending_relations(retrieval):
    r = retrieval
    first, _ = candidate(r, 'Margarita', type='person', kind='entity')
    second, _ = candidate(r, 'Margarita', type='person', kind='entity')
    mellanni, _ = candidate(r, 'Mellanni', type='organization', kind='entity')
    acme, _ = candidate(r, 'Acme Linen', type='organization', kind='entity')
    first_relation = relation_candidate(r, first, mellanni, 'works_in', 'Margarita works in Mellanni.')
    second_relation = relation_candidate(r, second, acme, 'works_in', 'Margarita works in Acme Linen.')
    outreach, _ = candidate(r, 'Margarita asked for direct outreach about the bed-sheet line.')
    r._put(outreach, 'candidate', 1, np.eye(1, 768, dtype=np.float32)[0])
    durable, _ = assertion(r, 'Margarita Ivanova', type='person')
    with r.store._mutex:
        r.store._db.execute('INSERT INTO candidate_entity_matches VALUES (?,?,?,?,?,?,?,?)',
            (first, durable, 1, .91, 1, 'fixture', 'name match only', datetime.now(timezone.utc).isoformat()))
        r.store._db.commit()
    hits = r.recall(MARGARITA_PROMPT, automatic=True)
    # Two same-spelled pending people stay two identities, each with only its own
    # neighbours and relation IDs; the semantic fact follows in the shared limit.
    assert {hit['id'] for hit in hits[:2]} == {first, second} and hits[2]['id'] == outreach and len(hits) == 3
    by_id = {hit['id']: hit for hit in hits[:2]}
    for hit in by_id.values():
        assert hit['category'] == 'entity_context' and hit['tier'] == 'candidate' and hit['confirmed'] is False
        assert hit['same_name_count'] == 2 and hit['duplicate_ids'] == []
        assert hit['text'].startswith('One of 2 separate entries spelled alike; not merged. Margarita [person; professional] Connections: ')
    assert by_id[first]['same_name_ids'] == [second] and by_id[second]['same_name_ids'] == [first]
    assert 'works_in Mellanni [organization; pending ' + first_relation + ']' in by_id[first]['text']
    assert 'Acme' not in by_id[first]['text'] and second_relation not in by_id[first]['text']
    assert 'works_in Acme Linen [organization; pending ' + second_relation + ']' in by_id[second]['text']
    assert 'Mellanni' not in by_id[second]['text'] and first_relation not in by_id[second]['text']
    assert [item['relation_id'] for item in by_id[first]['connections'] if item['basis'] == 'pending_relation'] == [first_relation]
    assert [item['relation_id'] for item in by_id[second]['connections'] if item['basis'] == 'pending_relation'] == [second_relation]
    assert ('possibly the same as durable Margarita Ivanova [person; memory ' + durable + '] (unverified name match)'
            in by_id[first]['text'])
    assert 'Ivanova' not in by_id[second]['text']
    assert {ref['episode_id'] for ref in by_id[first]['evidence']} != {ref['episode_id'] for ref in by_id[second]['evidence']}
    assert r.store._db.execute('SELECT count(*) FROM node_versions').fetchone()[0] == 1
    # The two-word durable spelling is not in the request, so it is not an anchor itself.
    assert durable not in {hit['id'] for hit in hits}
    # A durable node with the same spelling is one more separate identity: it is
    # ordered first, and the pending entries are neither hidden nor merged into it.
    promoted, _ = assertion(r, 'Margarita', type='person')
    hits = r.recall(MARGARITA_PROMPT, automatic=True)
    assert hits[0]['id'] == promoted and hits[1]['id'] in (first, second) and hits[2]['id'] == outreach
    assert hits[0]['same_name_count'] == 3 and set(hits[0]['same_name_ids']) == {first, second}
    assert hits[0]['text'].startswith('One of 3 separate entries spelled alike; not merged. ')


def test_automatic_anchor_pending_connections_apply_endpoint_state_scope_and_promotion(retrieval):
    r = retrieval
    person, _ = candidate(r, 'Margarita', type='person', kind='entity')
    rejected, _ = candidate(r, 'Old Shop', type='organization', kind='entity')
    personal, _ = candidate(r, 'Home Club', type='organization', kind='entity', scope=['personal'])
    promoted, _ = candidate(r, 'Mellanni', type='organization', kind='entity')
    forgotten, _ = candidate(r, 'Gone Ltd', type='organization', kind='entity')
    relation_candidate(r, person, rejected, 'works_in', 'Margarita works in Old Shop.')
    relation_candidate(r, person, personal, 'belongs_to', 'Margarita belongs to Home Club.', scope=['personal'])
    relation_candidate(r, person, promoted, 'works_in', 'Margarita works in Mellanni.')
    relation_candidate(r, person, forgotten, 'works_in', 'Margarita works in Gone Ltd.')
    node, _ = assertion(r, 'Mellanni', type='organization')
    stamp = datetime.now(timezone.utc).isoformat()
    with r.store._mutex:
        r.store._db.execute('INSERT INTO candidate_decisions VALUES (?,?,?,?,?,?)',
                            ('d-reject', rejected, 'fixture', 'reject', None, None))
        r.store._db.execute('INSERT INTO candidate_decisions VALUES (?,?,?,?,?,?)',
                            ('d-promote', promoted, 'fixture', 'promote', node, None))
        r.store._db.execute('INSERT INTO forgotten_targets VALUES (?,?,?)', (forgotten, 'fixture', 'candidate'))
        r.store._db.commit()
    hits = r.recall(MARGARITA_PROMPT, automatic=True)
    assert [hit['id'] for hit in hits] == [person]
    lines = hits[0]['connections']
    # Rejected and forgotten endpoints vanish; the promoted endpoint resolves to its
    # durable node; the personal relation remains visible without a scope filter.
    assert [(item['id'], item['tier'], item['relation']) for item in lines] == [
        (node, 'durable', 'works_in'), (personal, 'candidate', 'belongs_to')] or \
        [(item['id'], item['tier'], item['relation']) for item in lines] == [
        (personal, 'candidate', 'belongs_to'), (node, 'durable', 'works_in')]
    assert 'works_in Mellanni [organization; pending ' in hits[0]['text']
    assert 'Old Shop' not in hits[0]['text'] and 'Gone Ltd' not in hits[0]['text']
    professional = r.recall(MARGARITA_PROMPT, automatic=True, scope='professional')
    assert [(item['id'], item['relation']) for item in professional[0]['connections']] == [(node, 'works_in')]
    # A reviewed merge of the promoted endpoint keeps the recorded promotion target;
    # the connection follows the existing merge chain to the active survivor.
    from memento.curation import apply_review
    survivor, _ = assertion(r, 'Mellanni Group', type='organization')
    r.graph._command('merge-mellanni', 'fixture', 'reviewed merge', dict(op='merge', a=node, b=survivor),
                     lambda commit: apply_review(r.graph, commit, dict(op='merge', a=node, b=survivor)))
    merged = r.recall(MARGARITA_PROMPT, automatic=True, scope='professional')[0]
    assert [(item['id'], item['tier'], item['relation']) for item in merged['connections']] == [(survivor, 'durable', 'works_in')]
    assert 'works_in Mellanni Group [organization; pending ' in merged['text']
    # Forgetting the survivor removes that connection under the same current-state check.
    preview = r.graph._forget_preview(survivor)
    r.graph._forget(command_id='forget-survivor', actor='operator', why='Synthetic forget', target=survivor, token=preview['token'])
    gone = r.recall(MARGARITA_PROMPT, automatic=True)[0]
    assert [(item['id'], item['relation']) for item in gone['connections']] == [(personal, 'belongs_to')]
    assert 'Mellanni' not in gone['text']
    # Forgetting the anchored person itself removes the anchor, not just a neighbour.
    with r.store._mutex:
        r.store._db.execute('INSERT INTO forgotten_targets VALUES (?,?,?)', (person, 'fixture', 'candidate'))
        r.store._db.commit()
    assert r.recall(MARGARITA_PROMPT, automatic=True) == []


def test_automatic_pending_anchor_connections_cap_includes_name_hints_with_relations_first(retrieval):
    from memento.retrieval import ENTITY_ANCHOR_CONNECTIONS
    r = retrieval
    person, _ = candidate(r, 'Margarita', type='person', kind='entity')
    hint, _ = assertion(r, 'Margarita Ivanova', type='person')
    with r.store._mutex:
        r.store._db.execute('INSERT INTO candidate_entity_matches VALUES (?,?,?,?,?,?,?,?)',
            (person, hint, 1, .91, 1, 'fixture', 'name match only', datetime.now(timezone.utc).isoformat()))
        r.store._db.commit()
    organizations = []
    for number in range(ENTITY_ANCHOR_CONNECTIONS - 1):
        organization, _ = candidate(r, f'Org {number}', type='organization', kind='entity')
        relation_candidate(r, person, organization, 'works_in', f'Margarita works in Org {number}.')
        organizations.append(organization)
    # Four recorded relations leave one slot: the uncertain hint fills it, labelled as such.
    anchor = r.recall(MARGARITA_PROMPT, automatic=True)[0]
    assert [item['basis'] for item in anchor['connections']] == ['pending_relation'] * 4 + ['candidate_entity_match']
    assert 'possibly the same as durable Margarita Ivanova' in anchor['text']
    # A fifth recorded relation takes priority over the hint; the cap holds in total.
    organization, _ = candidate(r, 'Org 4', type='organization', kind='entity')
    relation_candidate(r, person, organization, 'works_in', 'Margarita works in Org 4.')
    anchor = r.recall(MARGARITA_PROMPT, automatic=True)[0]
    assert len(anchor['connections']) == ENTITY_ANCHOR_CONNECTIONS == 5
    assert all(item['basis'] == 'pending_relation' for item in anchor['connections'])
    assert 'possibly the same as' not in anchor['text'] and anchor['text'].count('works_in Org') == 5


def test_automatic_pending_anchor_order_follows_original_source_time_not_extraction_time(retrieval):
    r = retrieval
    # The fresh conversation was extracted first; the old import was extracted later.
    fresh, _ = candidate(r, 'Margarita', type='person', kind='entity', occurred_at='2026-10-20T00:00:00+00:00')
    old, _ = candidate(r, 'Margarita', type='person', kind='entity', occurred_at='2020-01-01T00:00:00+00:00')
    undated, _ = candidate(r, 'Margarita', type='person', kind='entity', occurred_at=None)
    fresh_run = r.store._db.execute('SELECT r.recorded_at FROM candidates c JOIN extraction_runs r ON r.id=c.run_id WHERE c.id=?', (fresh,)).fetchone()[0]
    old_run = r.store._db.execute('SELECT r.recorded_at FROM candidates c JOIN extraction_runs r ON r.id=c.run_id WHERE c.id=?', (old,)).fetchone()[0]
    assert old_run >= fresh_run
    assert [hit['id'] for hit in r.recall(MARGARITA_PROMPT, automatic=True, limit=1)] == [fresh]
    assert [hit['id'] for hit in r.recall(MARGARITA_PROMPT, automatic=True)] == [fresh, old]
    # Old eligible entities still anchor by name; order, not eligibility, follows source time.
    assert all(hit['same_name_count'] == 3 for hit in r.recall(MARGARITA_PROMPT, automatic=True))
    assert undated in r.recall(MARGARITA_PROMPT, automatic=True)[0]['same_name_ids']


def test_automatic_anchor_selection_precedes_traversal_and_stays_bounded(retrieval, monkeypatch):
    r = retrieval
    people = {assertion(r, 'Margarita', type='person')[0] for _ in range(5)}
    pending = {candidate(r, 'Margarita', type='person', kind='entity')[0] for _ in range(3)}
    walks, details = [], []
    original_related, original_details = r.related, r._candidate_details
    monkeypatch.setattr(r, 'related', lambda node_id, hops, **options: walks.append(node_id) or original_related(node_id, hops, **options))
    monkeypatch.setattr(r, '_candidate_details', lambda target, **options: details.append(target) or original_details(target, **options))
    hits = r.recall(MARGARITA_PROMPT, automatic=True)
    assert len(hits) == 2 and all(hit['tier'] == 'durable' and hit['same_name_count'] == 8 for hit in hits)
    assert set(hits[0]['same_name_ids']) == (people | pending) - {hits[0]['id']}
    # Exactly one bounded walk per selected anchor; unselected identities cost no traversal or detail read.
    assert walks == [hit['id'] for hit in hits] and details == []


def test_automatic_anchors_keep_same_name_entities_separate_and_never_use_similarity(retrieval):
    r = retrieval
    one, _ = assertion(r, 'Margarita', type='person', scope=['personal'])
    two, _ = assertion(r, 'Margarita', type='person', scope=['professional'])
    sergey, _ = assertion(r, 'Sergey', type='person')
    sergey_fact, _ = assertion(r, 'Sergey plays drums every weekend with the band.')
    index(r, sergey, .99)
    index(r, sergey_fact, .747)
    hits = r.recall(MARGARITA_PROMPT, automatic=True)
    # Both same-named people anchor separately; the ordinary semantic fact follows.
    assert {hit['id'] for hit in hits[:2]} == {one, two} and [hit['id'] for hit in hits[2:]] == [sergey_fact]
    anchors = {hit['id']: hit for hit in hits[:2]}
    for hit in anchors.values():
        assert hit['same_name_count'] == 2
        assert hit['text'].startswith('One of 2 separate entries spelled alike; not merged. Margarita [person; ')
    assert anchors[one]['scope'] == ['personal'] and anchors[two]['scope'] == ['professional']
    assert [hit['id'] for hit in r.recall(MARGARITA_PROMPT, automatic=True, scope='personal')[:1]] == [one]
    # An unknown name anchors nothing: the similar person label stays out, and only
    # the ordinary semantic fact path answers, under its existing floor.
    leonid = r.recall('Leonid started to play drums', automatic=True)
    assert [hit['id'] for hit in leonid] == [sergey_fact]
    assert all(hit['category'] != 'entity_context' for hit in leonid)
    assert [hit['id'] for hit in r.recall('Леонид начал играть на барабанах', automatic=True)] == [sergey_fact]


def test_automatic_anchors_do_not_impose_length_or_script_limits_and_follow_merged_aliases(retrieval):
    r = retrieval
    short, _ = assertion(r, 'Al', type='person')
    acronym, _ = assertion(r, 'AI', type='concept')
    cyrillic, _ = assertion(r, 'Леонид', type='person')
    hits = r.recall('ask Al about AI tonight', automatic=True)
    assert {hit['id'] for hit in hits} == {short, acronym}
    assert [hit['id'] for hit in r.recall('Леонид начал играть на барабанах', automatic=True)] == [cyrillic]
    # The exact spelling must be present: an inflected form is not matched or guessed.
    assert r.recall('позвони Леониду', automatic=True) == []
    # The identical written name matches even where casefolding would rewrite it:
    # FTS unicode61 keeps "ß", so "Straße" must not be compared as "strasse".
    street, _ = assertion(r, 'Königstraße', type='place')
    assert [hit['id'] for hit in r.recall('Treffen in der Königstraße heute', automatic=True)] == [street]
    assert [hit['id'] for hit in r.recall('TREFFEN IN DER KÖNIGSTRASSE', automatic=True)] == []
    # A multi-token name must appear contiguously and in order; scattered tokens
    # never assert the identity, while the exact sequence does.
    full, _ = assertion(r, 'John Smith', type='person')
    assert r.recall('John asked whether Smith replied', automatic=True) == []
    assert r.recall('Smith, John called', automatic=True) == []
    assert [hit['id'] for hit in r.recall('call John Smith tomorrow', automatic=True)] == [full]
    from memento.curation import apply_review
    canonical, evidence = assertion(r, 'Margarita', type='person')
    alias, _ = assertion(r, 'Rita', type='person')
    assert {hit['id'] for hit in r.recall('call Rita today', automatic=True)} == {alias}
    # An existing reviewed merge is the only equivalence that lets one spelling stand for another.
    r.graph._command('merge-rita', 'fixture', 'reviewed merge', dict(op='merge', a=alias, b=canonical),
                     lambda commit: apply_review(r.graph, commit, dict(op='merge', a=alias, b=canonical)))
    hits = r.recall('call Rita today', automatic=True)
    assert [hit['id'] for hit in hits] == [canonical]
    assert hits[0]['text'].startswith('Margarita [person; professional] (matched alias "Rita") No recorded connections.')
    assert hits[0].get('same_name_count') is None


@pytest.mark.parametrize('tier', ['candidate', 'durable'])
@pytest.mark.parametrize('type,eligible', [('knowledge', True), ('decision', True), ('experience', False)])
def test_personal_factual_control_type_preserves_evidence_and_automatic_eligibility(retrieval, tier, type, eligible):
    from memory_acceptance import FACTUAL_SCOPE_CASES
    r = retrieval
    text = next(case['text'] for case in FACTUAL_SCOPE_CASES if case['id'] == 'personal_fact')
    if tier == 'candidate':
        target, _ = candidate(r, text, type=type, scope=['personal'])
        r._put(target, 'candidate', 1, np.eye(1, 768, dtype=np.float32)[0])
    else:
        target, _ = assertion(r, text, type=type, scope=['personal'])
        index(r, target, .99)
    explicit = next(hit for hit in r.recall('Where is the spare house key?', scope='personal') if hit['id'] == target)
    evidence = explicit['evidence'][0]
    raw = r.store.get(evidence['episode_id'])
    assert raw['text'][evidence['start']:evidence['end']] == text
    automatic = r.recall('Where is the spare house key?', automatic=True, scope='personal')
    assert [hit['id'] for hit in automatic] == ([target] if eligible else [])
    if eligible:
        assert automatic[0]['scope'] == ['personal']
        assert automatic[0]['evidence'][0]['episode_id'] == raw['id']


@pytest.mark.parametrize('tier', ['candidate', 'durable', 'segment'])
@pytest.mark.parametrize('envelope', ['instructions', 'goal'])
def test_legacy_context_backed_records_are_only_excluded_from_automatic_recall(retrieval, tier, envelope):
    r = retrieval
    text = 'Use concise responses for routine questions.'
    raw = ('<codex_internal_context source="goal">\n' + text + '\n</codex_internal_context>'
           if envelope == 'goal' else '# AGENTS.md instructions\n\n<INSTRUCTIONS>\n' + text + '\n</INSTRUCTIONS>')
    source = 'native:synthetic-device:codex'
    if tier == 'candidate':
        target, episode = candidate(r, text, source=source, raw=raw)
        r._put(target, 'candidate', 1, np.eye(1, 768, dtype=np.float32)[0])
    else:
        episode, _ = r.store.capture(dict(source=source, event_id='legacy', conversation_id='c',
            kind='user', text=raw, occurred_at=None, metadata={}))
        target = r.graph.remember(command_id='legacy-record', actor='fixture', why='Previously extracted source claim',
            type='media' if tier == 'segment' else 'decision', text=text, scope=['professional'],
            evidence=[dict(episode_id=episode['id'], quote=text)])['id']
        if tier == 'durable':
            index(r, target, .99)
        else:
            r.store._db.execute('INSERT INTO media_files VALUES (?,?,?,?,?)',
                (target, 'a' * 64, 'synthetic.txt', 'document', r.graph._node(target)['commit_id']))
            r.store._db.execute('''INSERT INTO segments(id,node_id,modality,text,artifact_sha256,source_sha256)
                VALUES (?,?,?,?,?,?)''', ('legacy-segment', target, 'text', text, 'b' * 64, 'c' * 64))
            r._put('legacy-segment', 'segment', 1, np.eye(1, 768, dtype=np.float32)[0])
    assert r.recall('concise responses', automatic=True) == []
    assert any(hit['id'] == target for hit in r.recall('concise responses', effort='cheap'))
    assert r.store.get(episode['id'])['text'] == raw
    assert r.store.capture(dict(source=source, event_id=episode['event_id'], conversation_id='c',
        kind='user', text=raw, occurred_at=episode['occurred_at'], metadata=episode['metadata']))[1] is False


@pytest.mark.parametrize('tier', ['candidate', 'durable'])
@pytest.mark.parametrize('envelope', ['instructions', 'goal'])
def test_genuine_evidence_after_five_context_spans_keeps_automatic_eligibility(retrieval, monkeypatch, tier, envelope):
    r = retrieval
    identities = iter(f'{index:032x}' for index in range(1, 20))
    monkeypatch.setattr('memento.store.uuid4', lambda: SimpleNamespace(hex=next(identities)))
    text = 'Use concise responses for routine questions.'
    raw = ('<codex_internal_context source="goal">\n' + text + '\n</codex_internal_context>'
           if envelope == 'goal' else '# AGENTS.md instructions\n\n<INSTRUCTIONS>\n' + text + '\n</INSTRUCTIONS>')
    source = 'native:synthetic-device:codex'
    target, _ = candidate(r, text, source=source, raw=raw) if tier == 'candidate' else (None, None)
    refs = []
    for index_value in range(6):
        episode, _ = r.store.capture(dict(source=source, event_id=f'context-{index_value}', conversation_id='c',
            kind='user', text=raw, occurred_at=None, metadata={}))
        refs.append(dict(episode_id=episode['id'], quote=text))
    genuine, _ = r.store.capture(dict(source=source, event_id='genuine', conversation_id='c',
        kind='user', text=text, occurred_at=None, metadata={}))
    refs.append(dict(episode_id=genuine['id'], quote=text))
    if tier == 'durable':
        target = r.graph.remember(command_id='mixed-provenance', actor='fixture', why='Corroborated source claim',
            type='knowledge', text=text, scope=['professional'], evidence=refs)['id']
        index(r, target, .99)
    else:
        def attach(commit):
            r.graph._evidence(target, 1, refs)
            return dict(id=target)
        r.graph._command('mixed-provenance', 'fixture', 'Corroborate existing fixture', dict(op='fixture'), attach)
        r._put(target, 'candidate', 1, np.eye(1, 768, dtype=np.float32)[0])
    monkeypatch.setattr(r.store, 'get', lambda *_: pytest.fail('automatic recall hydrated source text'))
    hits = r.recall('concise responses', automatic=True)
    assert [hit['id'] for hit in hits] == [target]
    assert len(hits[0]['evidence']) == 5
    assert genuine['id'] not in {ref['episode_id'] for ref in hits[0]['evidence']}


@pytest.mark.parametrize('source,kind,text', [
    ('native:synthetic-device:codex', 'user', 'Ordinary source ' + 'x' * 500000),
    ('native:synthetic-device:codex', 'tool', '# AGENTS.md instructions\n' + 'x' * 500000),
    ('native:synthetic-device:pi', 'user', '# AGENTS.md instructions\n' + 'x' * 500000),
    ('native:synthetic-device:codex', 'tool', '<codex_internal_context source="goal">' + 'x' * 500000),
    ('native:synthetic-device:pi', 'user', '<codex_internal_context source="goal">' + 'x' * 500000),
    ('native:synthetic-device:codex', 'user', '<codex_internal_context source="plan">' + 'x' * 500000),
])
def test_automatic_context_check_does_not_copy_obviously_unmatched_large_text(retrieval, monkeypatch, source, kind, text):
    r = retrieval
    episode, _ = r.store.capture(dict(source=source, event_id='large', conversation_id='c', kind=kind,
        text=text, occurred_at=None, metadata={}))
    target = r.graph.remember(command_id='large-source', actor='fixture', why='Supported ordinary fact',
        type='knowledge', text='The source uses a documented configuration.', scope=['professional'],
        evidence=[dict(episode_id=episode['id'], quote=text[:30])])['id']
    index(r, target, .99)
    monkeypatch.setattr('memento.retrieval.legacy_native_context', lambda *_: pytest.fail('large unmatched text reached classifier'))
    monkeypatch.setattr(r.store, 'get', lambda *_: pytest.fail('automatic recall hydrated source text'))
    r.store._db.create_function('legacy_native_context', 4, lambda *_: pytest.fail('recall called extraction UDF'))
    assert [hit['id'] for hit in r.recall('documented configuration', automatic=True)] == [target]


def test_index_status_counts_current_units_failures_and_unprepared_sources(retrieval):
    r = retrieval
    ready, _ = assertion(r, 'The cobalt release checklist is approved.')
    pending, _ = assertion(r, 'The violet release checklist remains pending.')
    failed, _ = assertion(r, 'The amber release checklist needs another embedding attempt.')
    index(r, ready, .9)
    r.store._db.execute('INSERT INTO index_failures VALUES (?,?,?,?,?,0)',
                        (failed, 1, 1, 'ValueError', 0))
    status = r.status(source='test')
    assert {key: status[key] for key in ('available', 'total', 'indexed', 'pending', 'failed', 'unprepared_episodes')} == {
        'available': True, 'total': 3, 'indexed': 1, 'pending': 1, 'failed': 1, 'unprepared_episodes': 3}
    assert r.status(source='other')['total'] == 0


def test_status_conversation_count_seeks_exact_keys_instead_of_all_episode_rows(retrieval, monkeypatch):
    r = retrieval
    for index_value, (source, conversation) in enumerate([
            ('selected', 'repeat'), ('selected', 'repeat'), ('selected', ''), ('', 'repeat'), ('', '')]):
        r.store.capture(dict(source=source, event_id=str(index_value), conversation_id=conversation,
            kind='native_record' if index_value == 1 else 'user', text='Synthetic source.', occurred_at=None,
            metadata={'extraction_mode': 'skip'} if index_value == 1 else {}))
    db = r.store._db
    db.executemany('''INSERT INTO episodes
        (seq,id,source,event_id,conversation_id,kind,text,metadata,captured_at)
        VALUES (?,?,'legacy',?,?,'user','Legacy synthetic source.','{}','2026-10-22T00:00:00Z')''',
        [(1000, 'legacy1', 'legacy1', 'without-progress'), (2000, 'legacy2', 'legacy2', 'Точный 😀 ключ')])
    db.execute("INSERT INTO forgotten_sources VALUES ('selected','2','fixture','fixture')")
    db.execute("INSERT INTO processing_progress(source,conversation_id) VALUES ('empty-compaction','none')")
    state = SimpleNamespace(store=r.store, retrieval=None, cold_runtime=None, cold=None,
                            drive_reauthorization_required=False)
    snapshot, work = r.store.read_snapshot, []

    @contextmanager
    def measured_snapshot():
        with snapshot() as reader:
            active, ticks = False, 0
            def trace(sql):
                nonlocal active
                active = 'GROUP BY source,conversation_id' in sql or 'conversations(source,conversation_id)' in sql
            def progress():
                nonlocal ticks
                ticks += int(active)
                return 0
            reader.set_trace_callback(trace)
            reader.set_progress_handler(progress, 100)
            yield reader
            work.append(ticks)

    monkeypatch.setattr(r.store, 'read_snapshot', measured_snapshot)
    expected = {None: (7, 6), 'selected': (3, 2), 'legacy': (2, 2), '': (2, 2), 'absent': (0, 0)}
    for added in (0, 2000):
        if added:
            db.executemany('''INSERT INTO episodes
                (seq,id,source,event_id,conversation_id,kind,text,metadata,captured_at)
                VALUES (?,?,'selected',?,'repeat','native_record','Repeated synthetic source.',
                    '{"extraction_mode":"skip"}','2026-10-22T00:00:00Z')''',
                ((3000 + i, f'bulk{i}', f'bulk{i}') for i in range(added)))
        for position, (source, (episodes, conversations)) in enumerate(expected.items()):
            count = episodes + (added if source is None or source == 'selected' else 0)
            assert operational_status(state, source)['captured'] == dict(episodes=count, conversations=conversations)
            if added:
                assert work[-1] <= baseline[position] + 2
        baseline = work.copy()


def test_status_cover_preserves_mixed_units_and_avoids_projected_episode_bodies(retrieval, monkeypatch):
    r, statements = retrieval, []
    db = r.store._db
    for prefix, source in (('selected', 'selected'), ('empty', '')):
        episodes = {}
        for name in ('normal', 'skipped', 'forgotten', 'unprepared'):
            episode, _ = r.store.capture(dict(source=source, event_id=name, conversation_id='c', kind='user',
                text='Synthetic evidence. ' + 'Large projected message. ' * 4096, occurred_at=None,
                metadata={'extraction_mode': 'skip'} if name == 'skipped' else {}))
            episodes[name] = episode['id']
        db.execute('INSERT INTO forgotten_sources VALUES (?,?,?,?)', (source, 'forgotten', 'fixture', 'fixture'))
        for name, episode in episodes.items():
            if name != 'unprepared':
                db.execute('INSERT INTO episode_chunks VALUES (?,?,0,20)', (prefix + '-' + name, episode))
        db.execute('INSERT INTO episode_chunks VALUES (?,?,20,40)', (prefix + '-failed', episodes['normal']))
        run = prefix + '-run'
        db.execute('INSERT INTO extraction_runs VALUES (?,?,?,?,?,?,?,?)',
            (run, 'fixture', episodes['normal'], 'extract-v2', 'fixture', '[]', 'null', '2026-10-22T00:00:00Z'))
        for name in ('candidate', 'rejected-candidate', 'forgotten-candidate'):
            db.execute('INSERT INTO candidates VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                (prefix + '-' + name, run, 'memory', 'knowledge', 'Synthetic evidence.', 'Fixture source',
                 None, None, .8, None, None, '["professional"]'))
        db.execute('INSERT INTO candidate_decisions VALUES (?,?,?,?,?,?)',
            (prefix + '-rejection', prefix + '-rejected-candidate', 'fixture', 'reject', None, None))
        db.execute('INSERT INTO forgotten_targets VALUES (?,?,?)',
            (prefix + '-forgotten-candidate', 'fixture', 'fixture'))
        for name, type, state in (('node', 'knowledge', 'active'), ('retired-node', 'knowledge', 'active'),
                                  ('media', 'media', 'active'), ('retired-media', 'media', 'rejected')):
            target = prefix + '-' + name
            db.execute('INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                (target, 1, 'fixture', type, 'Synthetic evidence.', state, None, None, None, 0, '["personal"]'))
            db.execute('INSERT INTO graph_evidence VALUES (?,?,?,?,?,?)',
                (target, 1, episodes['normal'], 0, 19, 'Synthetic evidence.'))
            if name == 'retired-node':
                db.execute('''INSERT INTO node_versions SELECT id,2,commit_id,type,text,'rejected',
                    valid_from,valid_to,superseded_by,pinned,scope FROM node_versions WHERE id=?''', (target,))
        for name in ('media', 'retired-media'):
            db.execute('''INSERT INTO segments(id,node_id,modality,text,artifact_sha256,source_sha256)
                VALUES (?,?,?,?,?,?)''',
                (prefix + '-' + name + '-segment', prefix + '-' + name, 'text', 'Synthetic page.', 'a' * 64, 'b' * 64))
        for name, kind in (('normal', 'episode'), ('skipped', 'episode'), ('forgotten', 'episode'),
                           ('node', 'node'), ('retired-node', 'node'), ('media-segment', 'segment'),
                           ('retired-media-segment', 'segment')):
            index(r, prefix + '-' + name, .9, kind=kind)
        db.execute('INSERT INTO index_failures VALUES (?,1,1,\'ValueError\',0,0)', (prefix + '-failed',))
        db.execute('INSERT INTO graph_evidence VALUES (?,?,?,?,?,?)',
            (prefix + '-candidate', 1, episodes['normal'], 0, 19, 'Synthetic evidence.'))

    snapshot = r.store.read_snapshot
    @contextmanager
    def observed_snapshot():
        with snapshot() as reader:
            reader.set_trace_callback(lambda sql: statements.append(sql)
                if 'indexed_units AS MATERIALIZED' in sql else None)
            yield reader
    monkeypatch.setattr(r.store, 'read_snapshot', observed_snapshot)
    for source, multiplier in ((None, 2), ('selected', 1), ('absent', 0), ('', 1)):
        assert r.status(source) == dict(available=True, total=5 * multiplier, indexed=3 * multiplier,
            pending=multiplier, failed=multiplier, unprepared_episodes=multiplier, error=None)
    if sqlite3.sqlite_version_info >= (3, 46, 1):
        with snapshot() as reader:
            page = reader.execute("SELECT rootpage FROM sqlite_schema WHERE name='episodes'").fetchone()[0]
            body_columns = {row['cid'] for row in reader.execute('PRAGMA table_info(episodes)')
                            if row['name'] in ('text', 'metadata')}
            assert len(statements) == 4
            for sql in statements:
                program = list(reader.execute('EXPLAIN ' + sql))
                cursors = {op['p1'] for op in program if op['opcode'] == 'OpenRead' and op['p2'] == page}
                assert not any(op['opcode'] == 'Column' and op['p1'] in cursors
                               and op['p2'] in body_columns for op in program), sql


def test_source_status_work_does_not_scan_unrelated_raw_history(retrieval, monkeypatch):
    r = retrieval
    raw = 'Exact synthetic Ω\x00 evidence.'
    r.store._db.executemany('''INSERT INTO episodes
        (seq,id,source,event_id,conversation_id,kind,text,metadata,captured_at,text_runs)
        VALUES (?,?, 'selected',?,'conversation',?,?,?,'2026-10-21T00:00:00Z',?)''', [
        (1, 'inline', 'inline', 'native_record', raw, '{"extraction_mode":"skip"}', None),
        (2, 'offloaded', 'offloaded', 'native_record', 'raw stub', '{"extraction_mode":"skip"}', '[]'),
        (3, 'message', 'message', 'user', 'Genuine synthetic user evidence.', '{}', None),
        (4, 'raw-default', 'raw-default', 'native_record', 'Raw kind with ordinary metadata.', '{}', None),
        (5, 'raw-null', 'raw-null', 'native_hook', 'Raw kind with null mode.', '{"extraction_mode":null}', None),
        (6, 'raw-message', 'raw-message', 'native_record', 'Explicit raw message mode.', '{"extraction_mode":"message"}', None),
        (7, 'user-skip', 'user-skip', 'user', 'Skipped user kind.', '{"extraction_mode":"skip"}', None),
        (8, 'live', 'live', 'native_live_record', raw, '{"extraction_mode":"skip"}', None),
        (9, 'raw-number', 'raw-number', 'native_record', 'Raw kind with numeric mode.', '{"extraction_mode":5}', None),
        (10, 'raw-array', 'raw-array', 'native_record', 'Raw kind with array mode.', '{"extraction_mode":["skip"]}', None),
        (11, 'raw-object', 'raw-object', 'native_record', 'Raw kind with object mode.', '{"extraction_mode":{"nested":"skip"}}', None),
        (12, 'raw-true', 'raw-true', 'native_record', 'Raw kind with true mode.', '{"extraction_mode":true}', None),
        (13, 'raw-false', 'raw-false', 'native_record', 'Raw kind with false mode.', '{"extraction_mode":false}', None),
    ])
    r.store._db.execute("INSERT INTO forgotten_sources VALUES ('selected','raw-default','fixture','fixture')")
    state = SimpleNamespace(store=r.store, retrieval=r, cold_runtime=None, cold=None,
                            drive_reauthorization_required=False)
    snapshot = r.store.read_snapshot
    work, queries = [], []

    @contextmanager
    def measured_snapshot():
        with snapshot() as reader:
            ticks = []
            reader.set_progress_handler(lambda: ticks.append(1) or 0, 100)
            reader.set_trace_callback(queries.append)
            yield reader
            work.append(len(ticks))

    monkeypatch.setattr(r.store, 'read_snapshot', measured_snapshot)
    before = {source: operational_status(state, source) for source in ('selected', 'absent')}
    baseline_work = work.copy()
    assert before['selected']['captured'] == dict(episodes=13, conversations=1)
    assert before['selected']['raw_storage'] == dict(inline_raw_episodes=2,
        inline_raw_bytes=2 * len(raw.encode('utf-8')), offloaded_raw_episodes=1,
        compressed_raw_episodes=0, compressed_original_bytes=0, compressed_raw_bytes=0)
    assert before['selected']['indexing']['unprepared_episodes'] == 8
    r.store._db.execute('BEGIN')
    try:
        r.store._db.executemany('''INSERT INTO episodes
            (seq,id,source,event_id,conversation_id,kind,text,metadata,captured_at)
            VALUES (?,?, 'unrelated',?,'history','native_record',?,
                '{"extraction_mode":"skip"}','2026-10-21T00:00:00Z')''',
            ((i, f'e{i}', f'v{i}', 'Unrelated synthetic history.' * 256) for i in range(14, 2014)))
        r.store._db.execute('COMMIT')
    except BaseException:
        r.store._db.execute('ROLLBACK')
        raise
    for position, source in enumerate(('selected', 'absent')):
        after = operational_status(state, source)
        discovery = after['media']['captured_attachments']['discovery']
        assert discovery == dict(scope='global', through_seq=0, latest_seq=2013)
        before[source]['media']['captured_attachments']['discovery'] = discovery
        assert after == before[source]
        # Count VM work, not wall time: unrelated large rows must not be visited.
        assert work[-1] <= baseline_work[position] + 1
    global_status = operational_status(state)
    assert global_status['captured']['episodes'] == 2013
    assert global_status['raw_storage'] == dict(inline_raw_episodes=2002,
        inline_raw_bytes=2 * len(raw.encode('utf-8')) + 2000 * len('Unrelated synthetic history.' * 256),
        offloaded_raw_episodes=1, compressed_raw_episodes=0, compressed_original_bytes=0, compressed_raw_bytes=0)
    assert global_status['indexing']['unprepared_episodes'] == 8
    if sqlite3.sqlite_version_info >= (3, 46, 1):
        # Qualification is for the target runtime; older planners retain exact counts.
        with snapshot() as reader:
            page = reader.execute("SELECT rootpage FROM sqlite_schema WHERE name='episodes'").fetchone()[0]
            for sql in set(queries):
                if 'AS inline_raw_episodes' not in sql and not sql.startswith('SELECT count(*) FROM episodes e'):
                    continue
                plan = list(reader.execute('EXPLAIN ' + sql))
                table_cursors = {op['p1'] for op in plan if op['opcode'] == 'OpenRead' and op['p2'] == page}
                assert not any(op['opcode'] == 'Column' and op['p1'] in table_cursors for op in plan), sql


def test_index_status_counts_real_vectors_without_repeated_per_unit_probes(retrieval, monkeypatch):
    r = retrieval
    total, prepared, vector_count = 2056, 2048, 1536
    vector = np.zeros(768, np.float32).tobytes()
    db = r.store._db
    db.execute('BEGIN')
    try:
        db.executemany('''INSERT INTO episodes
            (seq,id,source,event_id,conversation_id,kind,text,metadata,captured_at)
            VALUES (?,?,?,?,?,'user','Synthetic progress evidence.','{}','2026-10-20T00:00:00+00:00')''',
            ((i, f'e{i}', 'bulk' if i <= prepared else 'other', f'event{i}', 'conversation')
             for i in range(1, total + 1)))
        db.executemany('INSERT INTO episode_chunks VALUES (?,?,0,28)',
                       ((f'x{i}', f'e{i}') for i in range(1, prepared + 1)))
        db.executemany("INSERT INTO vector_items VALUES (?,?,'episode',1)",
                       ((i, f'x{i}') for i in range(1, vector_count + 1)))
        db.executemany('INSERT INTO memory_vectors(rowid,embedding,current) VALUES (?,?,?)',
                       ((i, vector, i % 13 != 0) for i in range(1, vector_count + 1)))
        db.executemany('INSERT INTO index_failures VALUES (?,1,1,\'ValueError\',0,?)',
                       ((f'x{i}', int(i % 11 == 0)) for i in range(7, prepared + 1, 7)))
        db.execute('COMMIT')
    except BaseException:
        db.execute('ROLLBACK')
        raise

    indexed = {i for i in range(1, vector_count + 1) if i % 13 != 0}
    failed = {i for i in range(7, prepared + 1, 7) if i % 11 != 0} - indexed
    probes = []
    snapshot = r.store.read_snapshot

    @contextmanager
    def observed_snapshot():
        with snapshot() as reader:
            reader.set_trace_callback(lambda sql: probes.append(1) if sql.startswith('--')
                                      and 'memory_vectors_rowids' in sql and 'WHERE rowid = ?' in sql else None)
            yield reader

    monkeypatch.setattr(r.store, 'read_snapshot', observed_snapshot)
    status = r.status(source='bulk')
    assert status == dict(available=True, total=prepared, indexed=len(indexed), failed=len(failed),
                         pending=prepared - len(indexed) - len(failed), unprepared_episodes=0, error=None)
    # A status observation must not perform several sqlite-vec point probes for
    # every indexed passage during a status observation.
    assert len(probes) <= prepared
    assert r.status(source='other') == dict(available=True, total=0, indexed=0, pending=0, failed=0,
                                          unprepared_episodes=8, error=None)


@pytest.mark.parametrize('stage', ['raw', 'index'])
@pytest.mark.parametrize('source', [None, 'test'])
def test_status_wal_snapshot_allows_capture_during_real_raw_and_index_queries(retrieval, monkeypatch, stage, source):
    r = retrieval
    target, _ = assertion(r, 'The initial status fixture has indexed evidence.')
    index(r, target, .9)
    for origin, kind in (('test', 'native_record'), ('other', 'user'), ('other', 'native_record')):
        r.store.capture(dict(source=origin, event_id=kind, conversation_id='c', kind=kind,
                             text=origin + ' exact raw Ω text', occurred_at=None,
                             metadata={'extraction_mode': 'skip'} if kind == 'native_record' else {}))
    state = SimpleNamespace(store=r.store, retrieval=r, cold_runtime=None, cold=None,
                            drive_reauthorization_required=False)
    expected = operational_status(state, source=source)
    entered, release = Event(), Event()
    snapshot = r.store.read_snapshot
    active, paused = False, False

    def trace(sql):
        nonlocal active
        active = ('AS inline_raw_episodes' in sql if stage == 'raw'
                  else 'indexed_units AS MATERIALIZED' in sql)

    def pause_query():
        nonlocal paused
        if active and not paused:
            paused = True
            entered.set()
            return 0 if release.wait(10) else 1
        return 0

    @contextmanager
    def gated_snapshot():
        with snapshot() as reader:
            reader.set_trace_callback(trace)
            reader.set_progress_handler(pause_query, 1)
            yield reader

    monkeypatch.setattr(r.store, 'read_snapshot', gated_snapshot)

    def capture_and_index():
        episode, created = r.store.capture(dict(source='test', event_id='during-status', conversation_id='c',
            kind='user', text='Captured while status is still reading.', occurred_at=None, metadata={}))
        assert created
        with r.store._mutex:
            db = r.store._db
            db.execute('BEGIN IMMEDIATE')
            try:
                db.execute('INSERT INTO episode_chunks VALUES (?,?,0,?)', ('later-chunk', episode['id'], len(episode['text'])))
                vector = np.zeros(768, np.float32)
                vector[0] = 1
                r._put('later-chunk', 'episode', 1, vector)
                db.execute('COMMIT')
            except BaseException:
                db.execute('ROLLBACK')
                raise
        return episode

    with ThreadPoolExecutor(max_workers=2) as pool:
        status = pool.submit(operational_status, state, source)
        try:
            assert entered.wait(5), 'the actual status SQL did not reach the pause'
            captured = pool.submit(capture_and_index).result(timeout=5)
            assert not release.is_set() and not status.done()
        finally:
            release.set()
        assert status.result(timeout=5) == expected
    assert r.store.get(captured['id'])['text'] == 'Captured while status is still reading.'
    current = operational_status(state, source=source)
    assert current['captured']['episodes'] == expected['captured']['episodes'] + 1
    assert current['indexing']['total'] == expected['indexing']['total'] + 1
    assert current['indexing']['indexed'] == expected['indexing']['indexed'] + 1
    assert current['raw_storage'] == expected['raw_storage']


def test_status_reader_is_read_only_and_closes_its_snapshot_on_failure(retrieval):
    store = retrieval.store
    statements = []
    with pytest.raises(RuntimeError, match='synthetic status failure'):
        with store.read_snapshot() as reader:
            reader.set_trace_callback(statements.append)
            assert reader.in_transaction
            assert reader.execute('SELECT count(*) FROM memory_vectors').fetchone()[0] == 0
            with pytest.raises(sqlite3.OperationalError, match='readonly'):
                reader.execute('DELETE FROM episodes')
            raise RuntimeError('synthetic status failure')
    assert 'ROLLBACK' in statements
    with pytest.raises(sqlite3.ProgrammingError, match='closed'):
        reader.execute('SELECT 1')
    assert store._db.execute('PRAGMA journal_mode').fetchone()[0] == 'wal'
    assert store._db.execute('PRAGMA synchronous').fetchone()[0] == 2
    assert store._db.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0] == 0


def test_legacy_serialized_media_never_sends_binary_to_passage_tokenizer(retrieval):
    r = retrieval
    caption = 'The original cobalt checklist image remains available.'
    raw = json.dumps([{'type': 'image', 'data': 'Z' * (1024 * 1024)},
                      {'type': 'text', 'text': caption}])
    episode, _ = r.store.capture(dict(source='legacy-image', event_id='opaque', conversation_id='c',
        kind='user', text=raw, occurred_at=None, metadata={}))
    seen = []
    def tokenizer(text, **kwargs):
        seen.append(text)
        assert 'Z' * 256 not in text
        return {'offset_mapping': [(i, i + 1) for i in range(len(text))]}
    r.model.tokenizer = tokenizer
    r.prepare_episodes()
    assert sum(map(len, seen)) < 200 and len(seen) == 2
    passages = r.store._db.execute('SELECT start,end FROM episode_chunks WHERE episode_id=?', (episode['id'],)).fetchall()
    assert any(caption in raw[row['start']:row['end']] for row in passages)
    assert all('Z' * 256 not in raw[row['start']:row['end']] for row in passages)


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


def candidate(r, text, *, type='knowledge', kind='memory', scope=None, source='candidate-fixture', raw=None,
              occurred_at='2000-01-01T00:00:00+00:00'):
    key = uuid4().hex
    episode, _ = r.store.capture(dict(source=source, event_id=key, conversation_id='c',
        kind='user', text=text if raw is None else raw, occurred_at=occurred_at, metadata={}))
    target = 'c_' + uuid4().hex
    def create(commit):
        r.store._db.execute('INSERT INTO extraction_runs VALUES (?,?,?,?,?,?,?,?)',
            (key, commit, episode['id'], 'extract-v1', 'fixture-model', '[]', '[]', datetime.now(timezone.utc).isoformat()))
        r.store._db.execute('INSERT INTO candidates VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
            (target, key, kind, type, text, 'Explicit source candidate', None, None, .8,
             None, None, json.dumps(scope or ['professional'])))
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
    assert hit['id'] == hit['chunk_id'] != episode['id'] and hit['episode_id'] == episode['id']
    assert hit['tier'] == 'raw' and hit['confirmed'] is False and hit['historical'] is False
    assert 'Comet' in text[span['start']:span['end']]
    assert span['quote_excerpt'] == text[span['start']:span['end']][:240]
    assert hit['valid_from'] is None
    assert r.graph.history(episode['id'])[0]['text'] == text
    resolved = r.graph.history(hit['id'])[0]
    assert resolved['episode_id'] == episode['id'] and resolved['evidence'][0]['quote'] == text[span['start']:span['end']]


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
def test_scope_filter_precedes_limits_without_hiding_legacy_archived_rows(retrieval, channel):
    r = retrieval
    wanted, _ = assertion(r, 'needle ' + 'background ' * 100, scope=['personal'])
    r.store._db.execute('UPDATE node_stats SET archived=1 WHERE node_id=?', (wanted,))
    for position in range(25):
        wrong, _ = assertion(r, f'needle wrong {position}', scope=['professional'])
        if channel == 'semantic':
            index(r, wrong, .99)
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


def test_committed_citation_is_idempotent_and_changes_rank_not_assertions(retrieval, recency_clock):
    from memento.importance import cite, refresh

    r = retrieval
    cited, evidence = assertion(r, 'Orion cited assertion', occurred_at='2026-10-20T00:00:00Z')
    other, _ = assertion(r, 'Orion other assertion', occurred_at='2026-10-20T00:00:00Z')
    index(r, cited, .80)
    index(r, other, .80)
    r.store._db.execute('UPDATE node_stats SET importance=.51 WHERE node_id=?', (other,))
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
    assert {hit['id'] for hit in r.recall('Orion', effort='cheap')} == {cited, other}
    assert r.store._db.execute('SELECT sum(archived) FROM node_stats').fetchone()[0] == 0
    assert {hit['id'] for hit in r.recall('Orion', effort='deep')
            if hit['tier'] == 'durable'} == {cited, other}
    cite(r.graph, event_id='later-answer', ids=[cited])
    assert r.recall('Orion', effort='cheap')[0]['id'] == cited
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
    # Raw results carry their chunk reference as id; the transport envelope stays out.
    assert {hit['episode_id'] for hit in hits} == {message['id']}
    assert all(hit['id'] == hit['chunk_id'] != transport['id'] for hit in hits)
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


def test_semantic_recall_does_not_wait_for_background_encoding(retrieval, monkeypatch):
    r = retrieval
    known, _ = assertion(r, 'established archive decision')
    index(r, known, .8)
    pending, _ = assertion(r, 'new background evidence')
    encoding = Event()
    release = Event()

    def encode(model, texts):
        encoding.set()
        if not release.wait(5):
            raise RuntimeError('background encoder was not released')
        vectors = np.zeros((len(texts), 768), np.float32)
        vectors[:, 0] = 1
        return vectors

    monkeypatch.setattr('memento.retrieval.embed_texts', encode)
    with ThreadPoolExecutor(max_workers=2) as pool:
        background = pool.submit(r.index_pending)
        try:
            assert encoding.wait(3)
            hits = pool.submit(r.recall, 'unmatchedword', effort='cheap').result(timeout=3)
            assert [hit['id'] for hit in hits] == [known]
        finally:
            release.set()
        background.result(timeout=3)
    assert {hit['id'] for hit in r.recall('unmatchedword', effort='cheap')} == {known, pending}
    r.health()


def test_shutdown_commits_completed_vectors_and_leaves_the_rest_indexable(retrieval, monkeypatch):
    r = retrieval
    targets = [assertion(r, f'shutdown backlog item {i}')[0] for i in range(3)]

    def encode(model, texts):
        r._stop.set()
        vectors = np.zeros((len(texts), 768), np.float32)
        vectors[:, 0] = 1
        return vectors

    monkeypatch.setattr('memento.retrieval.embed_texts', encode)
    r.index_pending(limit=3)
    assert [hit['id'] for hit in r.recall('unmatchedword', effort='cheap')] == targets[:1]
    r.health()

    resumed = Retrieval(r.graph, model=r.model)
    while resumed.index_pending():
        pass
    assert {hit['id'] for hit in resumed.recall('unmatchedword', effort='cheap')} == set(targets)
    resumed.health()


def test_legacy_control_token_vectors_upgrade_once_without_changing_history(retrieval, monkeypatch):
    r = retrieval
    ordinary, _ = assertion(r, 'ordinary archive decision')
    literal, evidence = assertion(r, 'Document the literal <eos> spelling.')
    index(r, ordinary, .8)
    index(r, literal, .7)
    db = r.store._db
    history = r.graph.history(literal)
    rows = {row['target']: dict(row) for row in db.execute(
        'SELECT i.target,i.seq,v.embedding FROM vector_items i JOIN memory_vectors v ON v.rowid=i.seq')}
    r.prepare_episodes()
    failed = db.execute('SELECT id FROM episode_chunks WHERE episode_id=?',
                       (evidence[0]['episode_id'],)).fetchone()[0]
    db.execute('INSERT INTO index_failures VALUES (?,1,8,?,1e20,0)', (failed, 'ValueError'))
    db.execute('ALTER TABLE vector_model DROP COLUMN text_encoding')
    r.model.tokenizer.all_special_tokens = ['<eos>', '<|image|>']

    def encode(model, texts):
        vectors = np.zeros((len(texts), 768), np.float32)
        vectors[:, 0] = 1
        return vectors

    monkeypatch.setattr('memento.retrieval.embed_texts', encode)
    upgraded = Retrieval(r.graph, model=r.model)
    assert upgraded.recall('unmatchedword', effort='cheap')[0]['id'] == literal
    assert r.graph.history(literal) == history
    after = {row['target']: dict(row) for row in db.execute(
        'SELECT i.target,i.seq,v.embedding FROM vector_items i JOIN memory_vectors v ON v.rowid=i.seq')}
    assert after[ordinary] == rows[ordinary]
    assert after[literal]['seq'] == rows[literal]['seq']
    assert after[literal]['embedding'] != rows[literal]['embedding']
    failure = db.execute('SELECT * FROM index_failures WHERE target=?', (failed,)).fetchone()
    assert (failure['attempts'], failure['next_attempt'], failure['resolved']) == (8, 0, 0)
    assert upgraded.index_pending(limit=1) == 1
    assert db.execute('SELECT resolved FROM index_failures WHERE target=?', (failed,)).fetchone()[0] == 1
    upgraded.health()

    def unexpected_reencode(*args):
        raise AssertionError('completed literal-text migration must not repeat')

    monkeypatch.setattr('memento.retrieval.embed_texts', unexpected_reencode)
    restarted = Retrieval(r.graph, model=r.model)
    assert restarted.recall('unmatchedword', effort='cheap')[0]['id'] == literal
