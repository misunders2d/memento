"""Source-grounded recovery never bypasses loop chronology or explicit choices."""
import pytest

from memento.graph import Graph
from memento.loops import current, mark, snapshot
from memento.store import Store


@pytest.fixture
def world(tmp_path):
    store, count = Store(tmp_path), 0
    graph = Graph(store)

    def source(text, date='2026-01-01T00:00:00+00:00', *, kind='user', metadata=None):
        nonlocal count
        count += 1
        episode, _ = store.capture(dict(source='native:fixture:pi', event_id=str(count),
            conversation_id='thread', kind=kind, text=text, occurred_at=date, metadata=metadata or {}))
        return dict(episode_id=episode['id'], quote=text, occurrence=None)

    def target(ref, kind='node', *, src=None, dst=None):
        nonlocal count
        count += 1
        node = graph.remember(command_id=f'node-{count}', actor='fixture', why='Synthetic original premise',
            type='knowledge', text=ref['quote'], scope=['professional'], evidence=[ref])['id']
        id = node
        if kind == 'edge':
            count += 1
            other = graph.remember(command_id=f'node-{count}', actor='fixture', why='Synthetic context endpoint',
                type='knowledge', text=ref['quote'], scope=['professional'], evidence=[ref])['id']
            id = graph.link(command_id=f'link-{count}', actor='fixture', why='Synthetic directional relation',
                a=node, rel='related_to', b=other, evidence=[ref])['id']
        elif kind in ('candidate', 'relation_candidate'):
            id = f'candidate-{count}'
            def insert(commit):
                run = 'run-' + id
                store._db.execute("INSERT INTO extraction_runs VALUES(?,?,?,'fixture','fixture','[]','{}','2026-01-01T00:00:00+00:00')",
                                  (run, commit, ref['episode_id']))
                store._db.execute("INSERT INTO candidates VALUES(?,?,?,?,?,'Fixture',?,?,0.9,NULL,NULL,'[\"professional\"]')",
                                  (id, run, 'relation' if kind == 'relation_candidate' else 'memory',
                                   'owed_by' if kind == 'relation_candidate' else 'knowledge', ref['quote'], src, dst))
                store._db.execute('INSERT INTO graph_evidence VALUES(?,1,?,0,?,?)',
                                  (id, ref['episode_id'], len(ref['quote']), ref['quote']))
                return {}
            graph._command('candidate-' + id, 'extractor/fixture', 'Synthetic candidate', dict(op='extract'), insert)
        graph._command('automatic-' + id, 'extractor/fixture', 'Synthetic automatic marker', dict(op='extract'),
                       lambda commit: mark(graph, commit, id, True) or {})
        return id

    try:
        yield store, graph, source, target
    finally:
        store.close()


@pytest.mark.parametrize('kind', ['node', 'candidate', 'edge'])
def test_decline_false_automatic_marker_handles_every_target_without_erasing_evidence(world, kind):
    store, graph, source, target = world
    premise = source('If deployment happens, require a health check.')
    id = target(premise, kind)
    before = graph.history(id)[-1]['evidence']
    result = graph.loop(action='decline', id=id, evidence=[premise], command_id='decline',
                        actor='owner', why='Conditional policy, no actual outstanding deployment')
    assert result['status'] == 'declined_not_obligation'
    assert current(graph, id)['state'] == 'closed'
    assert graph.history(id)[-1]['evidence'] == before
    assert store.get(premise['episode_id'])['text'] == premise['quote']


@pytest.mark.parametrize('kind', ['node', 'candidate', 'edge'])
def test_grounded_close_preserves_source_chronology_and_allows_historical_reopening(world, kind):
    _, graph, source, target = world
    pending = source('I still owe the signed report.')
    id = target(pending, kind)
    graph.loop(action='assign', id=id, responsibility='owner', evidence=[pending], command_id='role',
               actor='owner', why='First-person original obligation')
    assert snapshot(graph, id, 1)['manual_choice_at'] is None
    completed = source('I sent the signed report; that obligation is completed.', '2026-01-02T00:00:00+00:00')
    closed = graph.loop(action='close', id=id, evidence=[completed], command_id='grounded-close',
                        actor='owner', why='Same report explicitly completed in later original evidence')
    assert closed['status'] == 'closed' and current(graph, id)['state'] == 'closed'
    renewed = source('The signed report needs my signature again.', '2026-01-03T00:00:00+00:00')
    evidence_target = target(renewed)
    graph._command('reopen', 'consolidator/fixture', 'Later original reopening evidence', dict(op='consolidate'),
        lambda commit: mark(graph, commit, id, True, evidence_target=evidence_target) or {})
    assert current(graph, id)['state'] == 'open'
    assert snapshot(graph, id, 1)['manual_choice_at'] is None
    history = graph.history(id)[-1]['loop_events']
    close_event = next(event for event in history if event['why'].startswith('Same report'))
    assert close_event['evidence'][0]['episode_id'] == completed['episode_id']


@pytest.mark.parametrize('choice', ['open', 'remind', 'later'])
def test_reconciliation_preserves_newer_manual_choice_and_dates(world, choice):
    _, graph, source, target = world
    premise = source('I still owe the signed report.')
    id = target(premise)
    args = dict(action=choice, id=id, command_id='manual-choice')
    if choice == 'remind':
        args['due_at'] = '2027-01-01T00:00:00+00:00'
    graph.loop(**args)
    before = current(graph, id)
    declined = graph.loop(action='decline', id=id, evidence=[premise], command_id='decline')
    assert declined['status'] == 'protected_loop_choice'
    completion = source('The signed report was sent.', '2026-01-02T00:00:00+00:00')
    closed = graph.loop(action='close', id=id, evidence=[completion], command_id='close')
    assert closed['status'] == 'not_newer'
    assert current(graph, id) == before


@pytest.mark.parametrize('kind', ['node', 'edge'])
def test_pins_protect_target_and_edge_endpoints(world, kind):
    store, graph, source, target = world
    premise = source('I still owe the signed report.')
    id = target(premise, kind)
    pinned = id if kind == 'node' else graph.history(id)[-1]['src']
    from memento.curation import pin
    graph._command('pin', 'owner', 'Synthetic owner pin', dict(op='pin', node_id=pinned, pinned=True),
                   lambda commit: pin(graph, commit, pinned, True))
    assert graph.loop(action='decline', id=id, evidence=[premise], command_id='decline')['status'] == 'protected_loop_choice'
    completion = source('The signed report was sent.', '2026-01-02T00:00:00+00:00')
    assert graph.loop(action='close', id=id, evidence=[completion], command_id='close')['status'] == 'protected_loop_choice'
    assert current(graph, id)['state'] == 'open'


def test_unknown_completion_time_stays_open_and_empty_quotes_are_rejected(world):
    _, graph, source, target = world
    premise = source('I still owe the signed report.')
    id = target(premise)
    completion = source('The signed report was sent.', None)
    assert graph.loop(action='close', id=id, evidence=[completion], command_id='close')['status'] == 'chronology_unknown'
    for action in ('close', 'decline', 'assign'):
        args = dict(action=action, id=id, evidence=[dict(premise, quote='', occurrence=0)], command_id=action + '-empty')
        if action == 'assign':
            args['responsibility'] = 'unknown'
        with pytest.raises(ValueError, match='nonempty'):
            graph.loop(**args)
    assert current(graph, id)['state'] == 'open'


def test_legacy_developer_envelope_only_supports_declining_false_marker(world):
    _, graph, source, target = world
    instruction = source('Standing developer rule: verify before claiming done.', kind='developer',
                         metadata={'evidence_eligible': False, 'extraction_mode': 'skip'})
    id = target(instruction)
    with pytest.raises(ValueError, match='eligible original evidence'):
        graph.loop(action='assign', id=id, responsibility='owner', evidence=[instruction], command_id='bad-owner')
    result = graph.loop(action='decline', id=id, evidence=[instruction], command_id='decline')
    assert result['status'] == 'declined_not_obligation'


def test_candidate_role_assignment_preserves_pending_review_and_scope(world):
    store, graph, source, target = world
    premise = source('I still owe the signed report.')
    id = target(premise, 'candidate')
    store._db.execute("INSERT INTO candidate_decisions VALUES('review-choice',?,'fixture','review','pending-review',NULL)", (id,))
    before = [tuple(row) for row in store._db.execute('SELECT * FROM candidate_decisions WHERE candidate_id=?', (id,))]
    graph.loop(action='assign', id=id, responsibility='owner', evidence=[premise], command_id='role')
    assert [tuple(row) for row in store._db.execute('SELECT * FROM candidate_decisions WHERE candidate_id=?', (id,))] == before
    listed = graph.loop(action='list', responsibility='owner')['loops']
    assert listed[0]['id'] == id and listed[0]['tier'] == 'candidate' and not listed[0]['confirmed']
    assert listed[0]['scope'] == ['professional']


def test_relation_candidate_resolves_promoted_endpoint_pins_and_forgetting(world):
    store, graph, source, target = world
    premise = source('I still owe the signed report.')
    person = target(premise)
    candidate_person = target(premise, 'candidate')
    relation = target(premise, 'relation_candidate', src=candidate_person, dst=person)
    store._db.execute("INSERT INTO candidate_decisions VALUES('promoted-person',?,'fixture','promote',?,NULL)",
                      (candidate_person, person))
    from memento.curation import pin
    graph._command('pin', 'owner', 'Synthetic owner pin', dict(op='pin', node_id=person, pinned=True),
                   lambda commit: pin(graph, commit, person, True))
    assert graph.loop(action='decline', id=relation, evidence=[premise], command_id='decline')['status'] == 'protected_loop_choice'
    completion = source('The signed report was sent.', '2026-01-02T00:00:00+00:00')
    assert graph.loop(action='close', id=relation, evidence=[completion], command_id='close')['status'] == 'protected_loop_choice'
    store._db.execute("INSERT INTO forgotten_targets VALUES(?,'fixture','node')", (person,))
    with pytest.raises(ValueError, match='active memory'):
        graph.loop(action='decline', id=relation, evidence=[premise], command_id='after-forget')
    listed = graph.loop(action='list', limit=100)
    assert relation not in {item['id'] for item in listed['loops']}
    assert listed['total'] == len(listed['loops'])
    from memento.viewer import _graph
    _, _, summary = _graph(store._db, None, '2026-10-28T00:00:00+00:00', full_candidate_loops=True)
    assert relation not in {item['id'] for item in summary['candidates']['items']}
    assert summary['open'] == listed['total']
