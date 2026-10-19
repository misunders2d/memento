from pathlib import Path

import pytest

from memento.graph import Graph
from memento.store import ConflictError, Store


@pytest.fixture
def graph(tmp_path: Path):
    store = Store(tmp_path)
    episode, _ = store.capture(dict(source='test', event_id='1', conversation_id='c',
        kind='user', text='a🧠б\r\nc old new repeat repeat', occurred_at=None, metadata={}))
    yield Graph(store), episode['id']
    store.close()


def remember(graph, command_id, text, quote):
    g, episode = graph
    return g.remember(command_id=command_id, actor='user', why='explicit memory',
        type='knowledge', text=text, scope=['personal'], evidence=[dict(episode_id=episode, quote=quote)])


def test_unicode_span_and_command_conflict(graph):
    g, _ = graph
    node = remember(graph, 'one', 'brain', '🧠б')
    assert remember(graph, 'one', 'brain', '🧠б') == node
    evidence = g.history(node['id'])[0]['evidence']
    assert evidence[0]['start'] == 1
    assert evidence[0]['end'] == 3
    with pytest.raises(ConflictError):
        remember(graph, 'one', 'different assertion', '🧠б')
    assert g.history(node['id'])[0]['text'] == 'brain'


def test_ambiguous_quote_rolls_back_command(graph):
    g, _ = graph
    with pytest.raises(ValueError, match='unambiguously'):
        remember(graph, 'retry', 'ambiguous', 'repeat')
    assert remember(graph, 'retry', 'unambiguous', 'old')['version'] == 1


def test_correction_preserves_original_and_reason(graph):
    g, episode = graph
    old = remember(graph, 'old', 'old belief', 'old')
    fixed = g.supersede(command_id='fix', actor='user', why='original claim was false',
        old=old['id'], new=dict(type='knowledge', text='new belief', scope=['professional']),
        evidence=[dict(episode_id=episode, quote='new')])
    lineage = g.history(old['id'])
    history = [row for row in lineage if row['id'] == old['id']]
    assert [(v['version'], v['state']) for v in history] == [(1, 'active'), (2, 'corrected')]
    assert history[0]['recorded_until'] == history[1]['recorded_at']
    assert history[1]['superseded_by'] == fixed['superseded_by']
    assert history[1]['why'] == 'original claim was false'
    assert history[1]['valid_to'] is None
    assert [row['scope'] for row in history] == [['personal'], ['personal']]
    assert next(row for row in lineage if row['id'] == fixed['superseded_by'])['scope'] == ['professional']


def test_world_change_requires_time_and_retains_validity(graph):
    g, episode = graph
    old = remember(graph, 'old', 'old belief', 'old')
    args = dict(command_id='change', actor='user', why='changed circumstances',
        old=old['id'], new=dict(type='knowledge', text='new belief', scope=['personal']), kind='change',
        evidence=[dict(episode_id=episode, quote='new')])
    with pytest.raises(ValueError, match='aware valid_at'):
        g.supersede(**args)
    g.supersede(**args, valid_at='2026-10-18T00:00:00+00:00')
    closed = [row for row in g.history(old['id']) if row['id'] == old['id']][-1]
    assert closed['valid_to'] == '2026-10-18T00:00:00+00:00'


def test_relation_direction_and_evidence_are_retained(graph):
    g, episode = graph
    a = remember(graph, 'a', 'first', 'old')['id']
    b = remember(graph, 'b', 'second', 'new')['id']
    edge = g.link(command_id='edge', actor='user', why='explicit relation',
        a=a, rel='follows_up_on', b=b, evidence=[dict(episode_id=episode, quote='old new')])
    row = g.store._db.execute('SELECT src,rel,dst FROM edge_versions WHERE id=?',
                              (edge['id'],)).fetchone()
    assert tuple(row) == (a, 'follows_up_on', b)


@pytest.mark.parametrize('table', ['node_versions', 'edge_versions', 'graph_commits', 'graph_evidence'])
def test_replace_cannot_rewrite_history(graph, table):
    import sqlite3
    g, episode = graph
    a = remember(graph, 'a', 'first', 'old')['id']
    b = remember(graph, 'b', 'second', 'new')['id']
    g.link(command_id='edge', actor='user', why='explicit relation', a=a, rel='related_to',
           b=b, evidence=[dict(episode_id=episode, quote='old new')])
    with pytest.raises(sqlite3.IntegrityError, match='immutable'):
        g.store._db.execute(f'INSERT OR REPLACE INTO {table} SELECT * FROM {table} LIMIT 1')
    assert g.history(a)[0]['text'] == 'first'


def test_failed_atomic_correction_does_not_publish_replacement(graph):
    g, episode = graph
    old = remember(graph, 'old', 'old belief', 'old')['id']
    with pytest.raises(ValueError, match='unambiguously'):
        g.supersede(command_id='bad', actor='user', why='correction', old=old,
            new=dict(type='knowledge', text='unpublished replacement', scope=['personal']),
            evidence=[dict(episode_id=episode, quote='not present')])
    assert g.recall_keywords('unpublished') == []
    assert [v['state'] for v in g.history(old)] == ['active']


@pytest.mark.parametrize('value', ['not a date', '2026-10-18T00:00:00'])
def test_invalid_world_time_cannot_be_committed(graph, value):
    g, episode = graph
    with pytest.raises(ValueError, match='aware ISO'):
        g.remember(command_id='invalid', actor='user', why='explicit', type='knowledge',
            text='invalid time', scope=['personal'], valid_from=value,
            evidence=[dict(episode_id=episode, quote='old')])


def test_evidence_duplicates_are_collapsed(graph):
    g, episode = graph
    source = dict(episode_id=episode, quote='old')
    node = g.remember(command_id='duplicate', actor='user', why='explicit', type='knowledge',
                      text='one assertion', scope=['personal'], evidence=[source, source])
    assert g.history(node['id'])[0]['evidence'] == [
        dict(episode_id=episode, start=7, end=10, quote='old')]


def test_person_proposal_is_not_an_assertion_until_human_review(graph):
    g, episode = graph
    proposal = g.remember(command_id='person', actor='agent', why='explicit mention',
        type='person', text='a person', scope=['personal'], evidence=[dict(episode_id=episode, quote='old')])
    assert proposal['status'] == 'pending_review'
    assert g.recall_keywords('person') == []
    assert [row['id'] for row in g.pending_reviews()] == [proposal['id']]
    approved = g._decide_review(command_id='approve', actor='human', why='checked evidence',
        proposal_id=proposal['id'], decision='approve')
    assert g.history(approved['id'])[0]['type'] == 'person'
    assert g.history(approved['id'])[0]['actor'] == 'human'
    assert g.pending_reviews() == []


def test_stale_sensitive_proposal_cannot_overwrite_approved_correction(graph):
    g, episode = graph
    p = g.remember(command_id='person', actor='agent', why='mention', type='person',
        text='original person', scope=['personal'], evidence=[dict(episode_id=episode, quote='old')])
    node = g._decide_review(command_id='approve', actor='human', why='checked',
        proposal_id=p['id'], decision='approve')['id']
    proposals = [g.supersede(command_id=f'correction-{i}', actor='agent', why='new evidence',
        old=node, new=dict(type='person', text=f'corrected person {i}', scope=['personal']),
        evidence=[dict(episode_id=episode, quote='new')]) for i in range(2)]
    fixed = g._decide_review(command_id='fix', actor='human', why='checked',
        proposal_id=proposals[0]['id'], decision='approve')
    with pytest.raises(ConflictError, match='stale'):
        g._decide_review(command_id='stale', actor='human', why='checked',
            proposal_id=proposals[1]['id'], decision='approve')
    closed = [row for row in g.history(node) if row['id'] == node][-1]
    assert closed['superseded_by'] == fixed['superseded_by']


def test_loop_reminders_normalize_utc_and_filter_strictly_without_rewriting_facts(graph):
    g, episode = graph
    nodes = {key: remember(graph, key, key + ' pending report', 'old')['id']
             for key in ('overdue', 'just-before', 'boundary', 'future', 'undated')}
    dates = {
        'overdue': '2000-10-17T12:00:00+00:00',
        'just-before': '2000-10-18T23:59:59.999999+00:00',
        'boundary': '2000-10-19T03:00:00+03:00',
        'future': '2000-10-20T00:00:00+00:00',
    }
    history = g.history(nodes['boundary'])
    for key, target in nodes.items():
        g.loop(action='open', id=target, command_id='open-' + key)
        if key in dates:
            args = dict(action='remind', id=target, due_at=dates[key], command_id='remind-' + key)
            result = g.loop(**args)
            assert g.loop(**args) == result
    assert [row['id'] for row in g.loop(action='list')['loops']] == [
        nodes[key] for key in ('overdue', 'just-before', 'boundary', 'future', 'undated')]
    cutoff = '2000-10-19T03:00:00+03:00'
    assert {row['id'] for row in g.loop(action='list', due_before=cutoff)['loops']} == {
        nodes['overdue'], nodes['just-before']}
    assert g.loop(action='list', scope='professional')['loops'] == []
    boundary = next(row for row in g.loop(action='list')['loops'] if row['id'] == nodes['boundary'])
    assert boundary['due_at'] == '2000-10-19T00:00:00+00:00'
    g.loop(action='close', id=nodes['boundary'], command_id='close-boundary')
    assert nodes['boundary'] not in {row['id'] for row in g.loop(action='list')['loops']}
    factual = lambda rows: [{key: value for key, value in row.items()
                             if key not in ('loop', 'loop_events')} for row in rows]
    assert factual(g.history(nodes['boundary'])) == factual(history)
    assert nodes['boundary'] in {row['id'] for row in g.recall_keywords('boundary')}


def test_correction_transfers_deferred_loop_without_hiding_replacement_fact(graph):
    g, episode = graph
    old = remember(graph, 'waiting', 'old pending report', 'old')['id']
    g.loop(action='remind', id=old, due_at='2030-10-20T15:00:00+03:00',
           command_id='reminder')
    deferred = g.loop(action='later', id=old, command_id='defer-old')
    fixed = g.supersede(command_id='correct-pending', actor='user', why='Correct original claim',
        old=old, new=dict(type='knowledge', text='new pending report', scope=['personal']),
        evidence=[dict(episode_id=episode, quote='new')])
    replacement = fixed['superseded_by']
    assert g.loop(action='list')['loops'] == []
    assert [row['id'] for row in g.recall_keywords('pending')] == [replacement]
    state = g.loop(action='open', id=replacement, command_id='resume-replacement')
    assert state['due_at'] == '2030-10-20T12:00:00+00:00'
    assert deferred['deferred_until'] is not None and state['deferred_until'] is None
    assert [row['id'] for row in g.loop(action='list')['loops']] == [replacement]
    assert len([row for row in g.history(replacement) if row['id'] == replacement]) == 1


@pytest.mark.parametrize('days', [None, 2.5])
def test_loop_deferral_duration_and_exact_expiry_boundary(graph, days, monkeypatch):
    from datetime import datetime, timedelta, timezone

    instant = datetime(2030, 1, 1, tzinfo=timezone.utc)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return instant if tz is not None else instant.replace(tzinfo=None)

    monkeypatch.setattr('memento.loops.datetime', Clock)
    g, _ = graph
    target = remember(graph, 'waiting-duration', 'a pending report', 'old')['id']
    state = g.loop(action='later', id=target, command_id='defer-duration',
                   **({} if days is None else dict(days=days)))
    duration = timedelta(days=7 if days is None else days)
    assert datetime.fromisoformat(state['deferred_until']) == instant + duration
    assert g.loop(action='list')['loops'] == []
    assert [row['id'] for row in g.recall_keywords('pending')] == [target]
    assert len(g.history(target)) == 1
    instant += duration - timedelta(microseconds=1)
    assert g.loop(action='list')['loops'] == []
    instant += timedelta(microseconds=1)
    assert [row['id'] for row in g.loop(action='list')['loops']] == [target]



def test_refinement_history_is_complete_from_every_lineage_member(graph):
    g, _ = graph
    texts = [
        'The report is pending.',
        'The launch report is pending.',
        'The launch report is pending legal approval.',
        'The launch report is pending legal approval from Alex.',
    ]
    episode, _ = g.store.capture(dict(source='refinement-fixture', event_id='chain',
        conversation_id='c', kind='user', text='\n'.join(texts), occurred_at=None, metadata={}))
    first = g.remember(command_id='initial-report', actor='user', why='Initial report status',
        type='knowledge', text=texts[0], scope=['professional'],
        evidence=[dict(episode_id=episode['id'], quote=texts[0])])['id']
    ids = [first]
    reasons = ['Initial report status']
    for index, text in enumerate(texts[1:], 1):
        why = f'Refinement {index} from explicit source details'
        result = g.supersede(command_id=f'refine-report-{index}', actor='user', why=why,
            old=ids[-1], new=dict(type='knowledge', text=text, scope=['professional']),
            kind='refinement', evidence=[dict(episode_id=episode['id'], quote=text)])
        ids.append(result['superseded_by'])
        reasons.append(why)
    lineage = g.history(first)
    assert all(g.history(target) == lineage for target in ids)
    introduced = [row for row in lineage if row['version'] == 1]
    assert [row['id'] for row in introduced] == ids
    assert [row['text'] for row in introduced] == texts
    assert [row['why'] for row in introduced] == reasons
    assert [row['evidence'][0]['quote'] for row in introduced] == texts
    for target in ids[:-1]:
        own = [row for row in lineage if row['id'] == target]
        assert [(row['version'], row['state']) for row in own] == [(1, 'active'), (2, 'refined')]
    assert [row['id'] for row in g.recall_keywords('pending')] == [ids[-1]]
    expected_additions = ['launch', 'legal approval', 'from Alex']
    for row, addition in zip(introduced[1:], expected_additions):
        changes = row['diff']
        assert any(change['old'] != change['new'] for change in changes)
        assert addition in ' '.join(change['new'] for change in changes if change['op'] != 'equal')


def test_quote_only_commands_use_eligible_current_conversation_and_stable_retries(graph):
    g, _ = graph
    quote = 'Orion awaits the launch report.'

    def capture(event, *, source='native-fixture', conversation='current', kind='user', metadata=None):
        return g.store.capture(dict(source=source, event_id=event, conversation_id=conversation,
            kind=kind, text=quote, occurred_at=None, metadata=metadata or {}))[0]

    original = capture('eligible')
    capture('other-conversation', conversation='elsewhere')
    capture('other-source', source='different-harness')
    for kind in ('native_record', 'native_hook', 'native_live_record'):
        capture(kind, kind=kind)
    capture('skipped-echo', metadata=dict(extraction_mode='skip'))
    capture('tool-call-echo', kind='tool', metadata=dict(evidence_eligible=False))
    args = dict(command_id='quote-only', actor='user', why='Save source-backed pending work',
        type='knowledge', text=quote, scope=['personal'], source='native-fixture',
        conversation_id='current', evidence=[dict(quote=quote)])
    node = g.remember(**args)
    assert g.history(node['id'])[0]['evidence'][0]['episode_id'] == original['id']
    newest = capture('newest-eligible')
    assert g.remember(**args) == node
    assert g.history(node['id'])[0]['evidence'][0]['episode_id'] == original['id']
    second = g.remember(**dict(args, command_id='new-quote-command'))
    assert g.history(second['id'])[0]['evidence'][0]['episode_id'] == newest['id']
    provenance = dict(source='native-fixture', conversation_id='current', evidence=[dict(quote=quote)])
    edge = g.link(command_id='quote-link', actor='user', why='Explicit followup relation',
        a=second['id'], rel='follows_up_on', b=node['id'], **provenance)
    assert g.history(edge['id'])[0]['evidence'][0]['episode_id'] == newest['id']
    corrected = g.supersede(command_id='quote-correction', actor='user', why='Correct wording',
        old=second['id'], new=dict(type='knowledge', text='Orion awaits its report.', scope=['personal']),
        **provenance)
    replacement = next(row for row in g.history(second['id'])
                       if row['id'] == corrected['superseded_by'])
    assert replacement['evidence'][0]['episode_id'] == newest['id']


def test_quote_only_missing_current_source_rolls_back_and_can_retry(graph):
    g, _ = graph
    quote = 'Only another conversation contains this promise.'
    payload = dict(source='quote-fixture', event_id='elsewhere', conversation_id='elsewhere',
        kind='user', text=quote, occurred_at=None, metadata={})
    g.store.capture(payload)
    args = dict(command_id='missing-current-quote', actor='user', why='Explicit assertion',
        type='knowledge', text=quote, scope=['personal'], source='quote-fixture',
        conversation_id='current', evidence=[dict(quote=quote)])
    with pytest.raises(ValueError, match='current conversation'):
        g.remember(**args)
    assert g.recall_keywords('promise') == []
    current, _ = g.store.capture(dict(payload, event_id='current', conversation_id='current'))
    node = g.remember(**args)
    assert g.history(node['id'])[0]['evidence'][0]['episode_id'] == current['id']


def test_loop_list_bounds_results_and_orders_undated_by_source_time_not_capture_order(graph):
    from datetime import datetime, timedelta, timezone

    g, _ = graph
    undated = []
    for position in range(25):
        text = f'Orion pending report {position}'
        timestamp = datetime(2035, 1, 25, tzinfo=timezone.utc) - timedelta(days=position)
        episode, _ = g.store.capture(dict(source='imported-loop-fixture', event_id=str(position),
            conversation_id='c', kind='user', text=text, occurred_at=timestamp.isoformat(), metadata={}))
        target = g.remember(command_id=f'undated-{position}', actor='user', why='Explicit pending report',
            type='knowledge', text=text, scope=['professional'],
            evidence=[dict(episode_id=episode['id'], quote=text)])['id']
        g.loop(action='open', id=target, command_id=f'open-undated-{position}')
        undated.append(target)
    dated = []
    for position, deadline in enumerate(('2051-01-01T00:00:00Z', '2050-01-01T00:00:00Z')):
        target = remember(graph, f'dated-{position}', f'dated report {position}', 'old')['id']
        g.loop(action='remind', id=target, due_at=deadline, command_id=f'dated-reminder-{position}')
        dated.append(target)
    expected = list(reversed(dated)) + undated
    default = g.loop(action='list')
    assert default['total'] == 27
    assert [row['id'] for row in default['loops']] == expected[:20]
    explicit = g.loop(action='list', limit=3)
    assert explicit['total'] == 27
    assert [row['id'] for row in explicit['loops']] == expected[:3]
    assert [row['id'] for row in g.loop(action='list', limit=100)['loops']] == expected
    professional = g.loop(action='list', scope='professional', limit=100)
    assert professional['total'] == 25
    assert [row['id'] for row in professional['loops']] == undated


@pytest.mark.parametrize('kind', ['correction', 'change', 'refinement'])
def test_citation_usage_follows_the_current_assertion_without_double_counting(graph, kind):
    from memento.importance import cite

    g, episode = graph
    old = remember(graph, 'original-cited', 'old belief', 'old')['id']
    for number in range(2):
        cite(g, event_id=f'original-answer-{number}', ids=[old], query='belief', shown=[old])
    previous = g.recall_keywords('old')[0]
    arguments = dict(command_id='update-cited', actor='user', why='Source-backed assertion update',
        old=old, new=dict(type='knowledge', text='new belief', scope=['personal']),
        kind=kind, evidence=[dict(episode_id=episode, quote='new')])
    if kind == 'change':
        arguments['valid_at'] = '2026-10-18T00:00:00Z'
    changed = g.supersede(**arguments)
    assert g.supersede(**arguments) == changed
    hit = g.recall_keywords('new')[0]
    assert hit['id'] == changed['superseded_by']
    assert hit['access_count'] == 2 and hit['last_cited_at'] == previous['last_cited_at']
    assert tuple(g.store._db.execute('SELECT shown,cited FROM query_feedback WHERE target=?',
                                    (hit['id'],)).fetchone()) == (2, 2)
    assert len(g.history(hit['id'])) == 3


def test_reviewed_entity_merge_combines_citation_usage_once(graph):
    from memento.curation import propose
    from memento.importance import cite

    g, episode = graph
    def project(key, text, quote):
        return g.remember(command_id=key, actor='user', why='Explicit project',
            type='project', text=text, scope=['personal'],
            evidence=[dict(episode_id=episode, quote=quote)])['id']
    old, target = project('old-project', 'old project', 'old'), project('target-project', 'target project', 'new')
    for number, node in enumerate([old, old, target]):
        cite(g, event_id=f'project-answer-{number}', ids=[node], query='project', shown=[node])
    latest = g.recall_keywords('target')[0]['last_cited_at']
    request = dict(op='merge', a=old, b=target, evidence=[dict(episode_id=episode, quote='old new')])
    proposal = g._command('merge-proposal', 'agent', 'Evidence-backed project identity',
                          request, lambda commit: propose(g, commit, request))
    approval = dict(command_id='approve-merge', actor='human', why='Confirmed identity',
                    proposal_id=proposal['id'], decision='approve')
    decided = g._decide_review(**approval)
    assert g._decide_review(**approval) == decided
    hit = g.recall_keywords('target')[0]
    assert hit['id'] == target and hit['access_count'] == 3 and hit['last_cited_at'] == latest
    assert tuple(g.store._db.execute('SELECT shown,cited FROM query_feedback WHERE target=?',
                                    (target,)).fetchone()) == (3, 3)
