"""Responsibility is grounded metadata, independent of scope and lifecycle."""
from datetime import datetime, timezone

import pytest

from memento.graph import Graph
from memento.loops import assign, current, responsibility, snapshot
from memento.processing import Processing
from memento.store import Store
from memento.viewer import _graph


@pytest.fixture
def world(tmp_path):
    store = Store(tmp_path)
    graph = Graph(store)
    text = 'I need to renew my passport. Lena owes me signed approval.'
    episode, _ = store.capture(dict(source='native:fixture:pi', event_id='one', conversation_id='thread',
        kind='user', text=text, occurred_at='2026-01-01T00:00:00+00:00', metadata={}))
    evidence = [dict(episode_id=episode['id'], quote=quote, occurrence=None)
                for quote in ('I need to renew my passport.', 'Lena owes me signed approval.')]
    ids = [graph.remember(command_id=f'save-{index}', actor='owner', why='Synthetic original obligation',
        type='knowledge', text=ref['quote'], scope=[scope], evidence=[ref])['id']
        for index, (ref, scope) in enumerate(zip(evidence, ('professional', 'personal')))]
    for id in ids:
        graph.loop(action='open', id=id, command_id='open-' + id)
    try:
        yield store, graph, ids, evidence
    finally:
        store.close()


def tag(graph, id, role, evidence, key):
    return graph.loop(action='assign', id=id, responsibility=role, evidence=evidence,
        command_id=key, actor='owner', why='Explicit source-grounded responsibility')


def test_legacy_unknown_owner_and_other_wait_are_independent_of_scope(world):
    store, graph, (owner, other), evidence = world
    assert graph.loop(action='list', responsibility='unknown')['total'] == 2
    tag(graph, owner, 'owner', [evidence[0]], 'assign-owner')
    tag(graph, other, 'other', [evidence[1]], 'assign-other')
    result = graph.loop(action='list', responsibility='owner', limit=1)
    assert result['total'] == 1 and result['loops'][0]['id'] == owner
    assert result['loops'][0]['scope'] == ['professional']
    waiting = graph.loop(action='list', responsibility='other')['loops']
    assert len(waiting) == 1 and waiting[0]['id'] == other and waiting[0]['state'] == 'open'
    nodes, _, loops = _graph(store._db, None, '2026-10-28T00:00:00+00:00', responsibility='owner')
    assert nodes[owner]['loop']['responsibility'] == 'owner'
    assert nodes[other]['loop'] is None and loops['open'] == 1


def test_fresh_manual_clarification_preserves_dates_and_lifecycle_choice(world):
    store, graph, (owner, _), evidence = world
    graph.loop(action='remind', id=owner, due_at='2027-01-01T00:00:00+00:00', command_id='due')
    graph.loop(action='later', id=owner, days=7, command_id='later')
    before = current(graph, owner)
    choice = snapshot(graph, owner, 1)['manual_choice_at']
    clarification, _ = store.capture(dict(source='native:fixture:pi', event_id='clarification',
        conversation_id='thread', kind='user', text='That passport obligation is mine.',
        occurred_at='2026-10-28T00:00:00+00:00', metadata={}))
    original_evidence = graph.history(owner)[-1]['evidence']
    tag(graph, owner, 'owner', [dict(episode_id=clarification['id'], quote=clarification['text'])], 'clarify-owner')
    after = current(graph, owner)
    assert {key: after[key] for key in before if key != 'responsibility'} == {
        key: value for key, value in before.items() if key != 'responsibility'}
    assert after['responsibility'] == 'owner'
    assert snapshot(graph, owner, 1)['manual_choice_at'] == choice
    assert graph.history(owner)[-1]['evidence'] == original_evidence
    event = graph.history(owner)[-1]['loop_events'][-1]
    assert event['why'] == 'Explicit source-grounded responsibility'
    assert event['assignment'] == dict(responsibility='owner', manual=True, evidence=[dict(
        episode_id=clarification['id'], start=0, end=len(clarification['text']))])


def test_assignment_rejects_missing_or_ineligible_owner_evidence_and_automatic_unrelated_source(world):
    store, graph, (owner, _), evidence = world
    with pytest.raises(ValueError, match='exact source evidence'):
        tag(graph, owner, 'owner', [], 'empty')
    assistant, _ = store.capture(dict(source='native:fixture:pi', event_id='assistant',
        conversation_id='thread', kind='assistant', text='The passport is the owner task.', occurred_at=None, metadata={}))
    with pytest.raises(ValueError, match='native user evidence'):
        tag(graph, owner, 'owner', [dict(episode_id=assistant['id'], quote=assistant['text'])], 'assistant-owner')
    with pytest.raises(ValueError, match='original target source'):
        graph._command('automatic-unrelated', 'consolidator/fixture', 'Synthetic unsupported attribution',
            dict(op='consolidate'), lambda commit: assign(graph, commit, owner, 'other', [dict(
                episode_id=assistant['id'], quote=assistant['text'])]))
    assert responsibility(store._db, owner) == 'unknown'


@pytest.mark.parametrize('kind,metadata', [('native_record', {}), ('developer', {}),
    ('user', {'evidence_eligible': False}), ('user', {'extraction_mode': 'skip'})])
def test_role_assignment_rejects_forbidden_sources_even_for_unknown_role(world, kind, metadata):
    store, graph, (owner, _), _ = world
    forbidden, _ = store.capture(dict(source='native:fixture:pi', event_id='forbidden',
        conversation_id='thread', kind=kind, text='Untrusted source cannot assign responsibility.',
        occurred_at=None, metadata=metadata))
    with pytest.raises(ValueError, match='eligible original evidence'):
        tag(graph, owner, 'unknown', [dict(episode_id=forbidden['id'], quote=forbidden['text'])], 'forbidden')


def test_assignment_rejects_unknown_and_forgotten_targets(world):
    store, graph, (owner, _), evidence = world
    with pytest.raises(ValueError, match='active memory|active visible'):
        tag(graph, 'missing', 'unknown', [evidence[0]], 'missing')
    store._db.execute('INSERT INTO forgotten_targets VALUES(?,\'fixture\',\'node\')', (owner,))
    with pytest.raises(ValueError, match='active memory|active visible'):
        tag(graph, owner, 'unknown', [evidence[0]], 'forgotten')


def test_manual_attribution_survives_refinement_closure_and_automatic_relabel(world):
    store, graph, (owner, _), evidence = world
    tag(graph, owner, 'owner', [evidence[0]], 'manual-owner')
    refined = graph.supersede(command_id='refinement', actor='owner', why='Same obligation with clearer wording',
        old=owner, new=dict(type='knowledge', text='The passport renewal remains outstanding.', scope=['professional']),
        kind='refinement', evidence=[evidence[0]])['superseded_by']
    assert current(graph, refined)['responsibility'] == 'owner'
    result = graph._command('automatic-relabel', 'consolidator/fixture', 'Synthetic role conflict',
        dict(op='consolidate'), lambda commit: assign(graph, commit, refined, 'other', [evidence[0]]))
    assert result['status'] == 'protected_loop_responsibility'
    graph.loop(action='close', id=refined, command_id='closed')
    assert current(graph, refined)['state'] == 'closed'
    assert current(graph, refined)['responsibility'] == 'owner'


def test_assignment_source_forgetting_restores_unknown_without_raw_deletion(world):
    store, graph, (owner, _), evidence = world
    tag(graph, owner, 'owner', [evidence[0]], 'assigned')
    source = store.get(evidence[0]['episode_id'])
    store._db.execute('INSERT INTO forgotten_sources(source,event_id,target,commit_id) VALUES(?,?,?,?)',
                      (source['source'], source['event_id'], owner, 'fixture'))
    assert responsibility(store._db, owner) == 'unknown'
    assert store.get(source['id'])['text'] == source['text']


def test_existing_maintenance_assign_loop_uses_same_grounding_and_keeps_other_wait(world):
    store, graph, (owner, other), evidence = world
    processing = Processing(graph)
    processing.maintain(command_id='role-review', actor='owner', why='Review original loop responsibilities',
                        target_ids=[owner, other])
    work, = processing.claim(agent='fixture', timezone_name='UTC',
        now=datetime(2026, 10, 28, 3, tzinfo=timezone.utc), kinds=['consolidate'])['work']
    output = dict(actions=[], repairs=[dict(operation='assign_loop', target=id, responsibility=role,
        evidence=[ref], why='Exact original debtor evidence')
        for id, role, ref in zip((owner, other), ('owner', 'other'), evidence)], continue_with=[])
    processing.complete(id=work['id'], owner=work['owner'], agent='fixture', output=output)
    assert current(graph, owner)['responsibility'] == 'owner'
    assert current(graph, other)['responsibility'] == 'other'
    assert current(graph, other)['state'] == 'open'
