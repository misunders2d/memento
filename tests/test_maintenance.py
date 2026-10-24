"""Bounded existing-memory maintenance on private synthetic stores."""

import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from memento.api import create_app
from memento.graph import Graph
from memento.loops import _closed_at, current, mark, snapshot
from memento.processing import Processing
from memento.store import ConflictError, Store


NIGHT = datetime(2026, 10, 18, 3, tzinfo=timezone.utc)


@pytest.fixture
def world(tmp_path):
    store = Store(tmp_path)
    graph = Graph(store)
    processing = Processing(graph)
    count = 0

    def node(text, type='knowledge', *, quote=None, scope=('professional',), date='2026-01-01T00:00:00+00:00', **extra):
        nonlocal count
        count += 1
        episode, _ = store.capture(dict(source='maintenance-fixture', event_id=str(count),
            conversation_id=str(count), kind='user', text=quote or text, occurred_at=date, metadata={}))
        evidence = [dict(episode_id=episode['id'], quote=episode['text'], occurrence=None)]
        result = graph.remember(command_id=f'node-{count}', actor='fixture', why='Synthetic source-supported assertion',
            type=type, text=text, scope=list(scope), evidence=evidence, **extra)
        return result['id'], evidence[0]

    try:
        yield store, graph, processing, node
    finally:
        store.close()


def seed(processing, targets, key='seed'):
    return processing.maintain(command_id=key, actor='fixture', why='Inspect explicitly selected subjects',
                               target_ids=list(targets))


def claim(processing, now=NIGHT, **extra):
    return processing.claim(agent='fixture', timezone_name='UTC', now=now, **extra)['work']


def complete(processing, work, repairs=(), continuation=()):
    return processing.complete(id=work['id'], owner=work['owner'], agent='fixture',
        output=dict(actions=[], repairs=list(repairs), continue_with=list(continuation)))


def repair(operation, target, evidence, **extra):
    return dict(operation=operation, target=target, evidence=list(evidence),
                why='Grounded synthetic existing-memory repair', **extra)


def automatic(graph, target):
    return graph._command('automatic-' + target, 'consolidator/fixture', 'Synthetic automatic marker',
        dict(op='consolidate'), lambda commit: mark(graph, commit, target, True) or {})


def test_old_only_entities_and_claims_coalesce_with_original_evidence(world):
    store, graph, processing, node = world
    tool, a = node('Herdr', 'tool', quote='Herdr is my terminal multiplexer.')
    cli, b = node('Herdr CLI', 'tool', quote='I use the Herdr CLI to control that terminal multiplexer.')
    fact, c = node('The warehouse uses lane A.')
    copy, d = node('The warehouse uses lane A.')
    request = seed(processing, [tool, cli, fact, copy])
    task, = claim(processing)
    assert task['input']['candidates'] == []
    assert 0 < len(task['input']['existing_nodes']) <= 48
    ref = task['input']['existing_nodes'][0]['evidence'][0]
    assert {'source', 'kind', 'occurred_at', 'conversation_id', 'event_id'} <= set(ref)
    result = complete(processing, task, [repair('absorb', tool, [a, b], absorbed=[cli]),
                                        repair('absorb', fact, [c, d], absorbed=[copy])])
    assert result['candidate_count'] == 0
    assert graph._node(cli)['superseded_by'] == tool
    assert graph._node(copy)['superseded_by'] == fact
    assert len(graph.history(fact)[-1]['evidence']) == 2
    assert store._db.execute('SELECT state FROM repair_groups WHERE id=?',
                             (request['repair_group_id'],)).fetchone()[0] == 'done'


def test_absorb_eight_members_accepts_nine_distinct_original_sources(world):
    store, graph, processing, node = world
    members = [node('Herdr', 'tool', quote=f'Original event {number}: Herdr is my terminal multiplexer.')
               for number in range(9)]
    survivor = members[0][0]
    seed(processing, [survivor])
    task, = claim(processing)
    result = complete(processing, task, [repair('absorb', survivor, [source for _, source in members],
                                              absorbed=[target for target, _ in members[1:]])])
    assert result['repair_results'][survivor]['status'] == 'absorbed'
    assert len(graph.history(survivor)[-1]['evidence']) == 9


def test_seed_idempotence_pending_dedupe_and_no_daytime_work(world):
    store, graph, processing, node = world
    target, _ = node('A durable fact.')
    first = seed(processing, [target])
    assert seed(processing, [target]) == first
    assert seed(processing, [target], 'other-command')['repair_group_id'] == first['repair_group_id']
    assert claim(processing, NIGHT.replace(hour=12)) == []
    task, = claim(processing)
    complete(processing, task)
    assert claim(processing) == []


@pytest.mark.parametrize('mismatch', ['scope', 'validity'])
def test_claim_incompatibility_rejects_atomically_and_supplies_feedback(world, mismatch):
    store, graph, processing, node = world
    target, a = node('A stable assertion.')
    kwargs = dict(scope=('personal',)) if mismatch == 'scope' else dict(valid_from='2026-02-01T00:00:00+00:00')
    other, b = node('A stable assertion.', **kwargs)
    seed(processing, [target, other])
    task, = claim(processing)
    with pytest.raises(ValueError, match='consolidation_absorb_' + mismatch + '_mismatch'):
        complete(processing, task, [repair('absorb', target, [a, b], absorbed=[other])])
    assert graph._node(target)['version'] == graph._node(other)['version'] == 1
    context = json.loads(store._db.execute('SELECT context FROM jobs WHERE id=?', (task['id'],)).fetchone()[0])
    assert context['previous_rejection']['references'] == [dict(target=target, operation='absorb')]


def test_existing_refinement_uses_original_sources_and_fences_stale_context(world):
    store, graph, processing, node = world
    target, a = node('The application uses a local database.')
    detail, b = node('The application uses SQLite on local Linux storage.')
    seed(processing, [target, detail])
    task, = claim(processing)
    result = complete(processing, task, [repair('supersede', target, [a, b], type='knowledge',
        text='The application uses a local SQLite database on Linux storage.', change_kind='refinement')])
    assert graph._node(target)['state'] == 'refined'
    assert graph._node(result['repair_results'][target]['superseded_by'])['state'] == 'active'
    seed(processing, [detail], 'stale')
    stale, = claim(processing)
    graph.supersede(command_id='concurrent', actor='fixture', why='Concurrent correction', old=detail,
        new=dict(type='knowledge', text='A corrected assertion.', scope=['professional']),
        evidence=[b], kind='correction')
    with pytest.raises(ConflictError, match='graph context changed'):
        complete(processing, stale)


def test_decline_wrong_automatic_loop_protects_manual_choices(world):
    store, graph, processing, node = world
    false, a = node('Standing approval is required.', quote='Always ask before deploying; this is a standing policy.')
    chosen, b = node('Await a delivery confirmation.')
    automatic(graph, false)
    automatic(graph, chosen)
    graph.loop(action='open', id=chosen, command_id='manual', actor='human', why='Explicit commitment')
    seed(processing, [false, chosen])
    task, = claim(processing)
    result = complete(processing, task, [repair('decline_loop', false, [a]), repair('decline_loop', chosen, [b])])
    assert result['repair_results'][false]['status'] == 'declined_not_obligation'
    assert result['repair_results'][chosen]['status'] == 'protected_loop_choice'
    assert current(graph, false)['state'] == 'closed'
    assert current(graph, chosen)['state'] == 'open'


def test_close_loop_uses_completion_source_time_and_reopens_from_later_evidence(world):
    store, graph, processing, node = world
    pending, a = node('Send the warehouse report.', date='2026-01-01T00:00:00+00:00')
    done, b = node('The warehouse report was sent.', date='2026-01-02T00:00:00+00:00')
    automatic(graph, pending)
    seed(processing, [pending, done])
    task, = claim(processing)
    result = complete(processing, task, [repair('close_loop', pending, [b])])
    assert result['repair_results'][pending]['status'] == 'closed'
    assert current(graph, pending)['state'] == 'closed'
    later, _ = node('Send the warehouse report again.', date='2026-01-03T00:00:00+00:00')
    graph._command('reopen', 'consolidator/fixture', 'New pending evidence', dict(op='consolidate'),
        lambda commit: mark(graph, commit, pending, True, evidence_target=later) or {})
    assert current(graph, pending)['state'] == 'open'


def test_close_loop_mixed_pending_quote_remains_invalid_then_later_only_closes(world):
    store, graph, processing, node = world
    pending, a = node('Send the warehouse report.', date='2026-01-01T00:00:00+00:00')
    done, b = node('The warehouse report was sent.', date='2026-01-02T00:00:00+00:00')
    automatic(graph, pending)
    seed(processing, [pending, done])
    task, = claim(processing)
    result = complete(processing, task, [repair('close_loop', pending, [a, b])])
    assert result['repair_results'][pending]['status'] == 'not_newer'
    assert current(graph, pending)['state'] == 'open'
    seed(processing, [pending, done], 'later-only')
    task, = claim(processing)
    result = complete(processing, task, [repair('close_loop', pending, [b])])
    assert result['repair_results'][pending]['status'] == 'closed'
    assert current(graph, pending)['state'] == 'closed'


def test_retire_bookkeeping_preserves_history_sources_and_incident_edge_evidence(world):
    store, graph, processing, node = world
    noise, a = node('A synthetic recall query returned three scored hits.')
    project, b = node('Warehouse', 'project')
    edge = graph.link(command_id='noise-link', actor='fixture', why='Synthetic original relation',
        a=noise, rel='related_to', b=project, evidence=[a])['id']
    seed(processing, [noise])
    task, = claim(processing)
    assert noise in task['input']['retirement_ready']
    result = complete(processing, task, [repair('retire', noise, [a])])
    assert result['repair_results'][noise]['status'] == 'retired'
    assert graph._node(noise)['state'] == 'retired'
    assert graph.history(noise)[-1]['evidence'] == graph.history(noise)[0]['evidence']
    assert store.get(a['episode_id'])['text'] == a['quote']
    assert store._db.execute('SELECT count(*) FROM forgotten_sources').fetchone()[0] == 0
    assert store._db.execute('SELECT count(*) FROM forgotten_targets').fetchone()[0] == 0
    rows = store._db.execute('SELECT version,state FROM edge_versions WHERE id=? ORDER BY version', (edge,)).fetchall()
    assert [tuple(row) for row in rows] == [(1, 'active'), (2, 'inactive')]
    assert [row['quote'] for row in store._db.execute('SELECT quote FROM graph_evidence WHERE target=? ORDER BY version',
                                                   (edge,))] == [a['quote'], a['quote']]
    assert graph._node(project)['state'] == 'active'


@pytest.mark.parametrize('changed', ['new_edge', 'edge_version', 'edge_loop'])
def test_retire_rejects_changed_incident_edges_or_loop_choices(world, changed):
    store, graph, processing, node = world
    noise, a = node('A synthetic recall query returned three scored hits.')
    project, b = node('Warehouse', 'project')
    edge = graph.link(command_id='noise-link', actor='fixture', why='Synthetic original relation',
        a=noise, rel='related_to', b=project, evidence=[a])['id']
    seed(processing, [noise])
    task, = claim(processing)
    if changed == 'new_edge':
        graph.link(command_id='new-noise-link', actor='fixture', why='New relation after prepare',
            a=project, rel='mentions', b=noise, evidence=[a])
    elif changed == 'edge_version':
        from memento.curation import attach
        graph._command('attach-edge', 'fixture', 'Concurrent evidence', dict(op='fixture'),
                       lambda commit: attach(graph, commit, edge, [a]))
    else:
        graph.loop(action='open', id=edge, command_id='manual-edge-choice', actor='human', why='Explicit pending marker')
    with pytest.raises(ConflictError, match='retirement'):
        complete(processing, task, [repair('retire', noise, [a])])
    assert graph._node(noise)['state'] == 'active'


def test_repair_frontier_resumes_after_restart_and_night_pause(world, tmp_path):
    store, graph, processing, node = world
    targets = [node(f'Unique{number:02d}subject.')[0] for number in range(12)]
    request = seed(processing, targets)
    first, = claim(processing)
    complete(processing, first)
    row = dict(store._db.execute('SELECT * FROM repair_groups WHERE id=?', (request['repair_group_id'],)).fetchone())
    assert row['state'] == 'pending'
    assert row['active_job'] is None and json.loads(row['frontier'])
    assert claim(processing, NIGHT.replace(hour=12)) == []
    store.close()
    reopened = Store(tmp_path)
    try:
        resumed = Processing(Graph(reopened))
        following, = claim(resumed, NIGHT + timedelta(days=1))
        assert following['id'] != first['id']
        complete(resumed, following)
        assert reopened._db.execute('SELECT state FROM repair_groups WHERE id=?',
                                    (request['repair_group_id'],)).fetchone()[0] == 'done'
    finally:
        reopened.close()


def test_failed_group_rederives_after_cooldown_without_new_candidates(world):
    store, graph, processing, node = world
    target, _ = node('A fact requiring bounded maintenance.')
    request = seed(processing, [target])
    first, = claim(processing)
    processing.queue.fail(processing._leased(first['id'], first['owner'])[0], ValueError('invalid'))
    assert claim(processing) == []
    row = store._db.execute('SELECT * FROM repair_groups WHERE id=?', (request['repair_group_id'],)).fetchone()
    assert row['state'] == 'pending' and row['active_job'] is None and row['available_at'] > 0
    store._db.execute('UPDATE repair_groups SET available_at=0 WHERE id=?', (request['repair_group_id'],))
    again, = claim(processing)
    assert again['id'] != first['id']
    complete(processing, again)


def test_dense_subject_continuation_is_bounded_and_stops_without_progress(world):
    store, graph, processing, node = world
    targets = [node('Herdr', 'tool')[0] for _ in range(60)]
    request = seed(processing, [targets[0]])
    first, = claim(processing)
    assert len(first['input']['existing_nodes']) == 48 and first['input']['context_truncated']
    seen = {item['id'] for item in first['input']['existing_nodes']}
    complete(processing, first, continuation=[targets[0]])
    following, = claim(processing)
    assert len(following['input']['existing_nodes']) <= 48
    assert {item['id'] for item in following['input']['existing_nodes']} - seen
    complete(processing, following, continuation=[targets[0]])
    assert claim(processing) == []
    assert store._db.execute('SELECT state FROM repair_groups WHERE id=?',
                             (request['repair_group_id'],)).fetchone()[0] == 'done'


def test_absorbed_manual_open_choice_survives_occupied_survivor(world):
    store, graph, processing, node = world
    survivor, a = node('A pending warehouse report.')
    source, b = node('A pending warehouse report.')
    automatic(graph, survivor)
    graph.loop(action='open', id=source, command_id='source-manual', actor='human', why='Keep this task open')
    before = snapshot(graph, source, 1)['manual_choice_at']
    seed(processing, [survivor, source])
    task, = claim(processing)
    complete(processing, task, [repair('absorb', survivor, [a, b], absorbed=[source])])
    assert snapshot(graph, survivor, graph._node(survivor)['version'])['manual_choice_at'] == before
    seed(processing, [survivor], 'decline-after-merge')
    following, = claim(processing)
    result = complete(processing, following, [repair('decline_loop', survivor, [a])])
    assert result['repair_results'][survivor]['status'] == 'protected_loop_choice'
    assert current(graph, survivor)['state'] == 'open'


def test_absorbed_later_manual_close_keeps_original_reopen_boundary(world):
    store, graph, processing, node = world
    survivor, a = node('Send the warehouse report.')
    done, completion = node('The warehouse report was sent.', date='2026-01-02T00:00:00+00:00')
    automatic(graph, survivor)
    seed(processing, [survivor, done])
    task, = claim(processing)
    complete(processing, task, [repair('close_loop', survivor, [completion])])
    source, b = node('Send the warehouse report.')
    graph.loop(action='close', id=source, command_id='source-close', actor='human', why='Explicitly closed')
    boundary = _closed_at(graph, source)
    seed(processing, [survivor, source], 'merge-closed')
    task, = claim(processing)
    complete(processing, task, [repair('absorb', survivor, [a, b], absorbed=[source])])
    assert _closed_at(graph, survivor) == boundary
    later, _ = node('Send the warehouse report again.', date='2026-01-03T00:00:00+00:00')
    graph._command('do-not-reopen', 'consolidator/fixture', 'Evidence predates manual choice', dict(op='consolidate'),
        lambda commit: mark(graph, commit, survivor, True, evidence_target=later) or {})
    assert current(graph, survivor)['state'] == 'closed'


def test_normal_candidate_discovers_facets_and_repairs_visible_existing_duplicates(world):
    store, graph, processing, node = world
    tool, a = node('Herdr', 'tool')
    gloss, b = node('Herdr the terminal multiplexer', 'tool')
    for number in range(12):
        node(f'Unrelated{number} CLI', 'tool')
    episode, _ = store.capture(dict(source='normal-fixture', event_id='new', conversation_id='new', kind='user',
        text='I use Herdr CLI to control the same terminal multiplexer.', occurred_at='2026-02-01T00:00:00+00:00', metadata={}))
    extract, = claim(processing, compaction=dict(source='normal-fixture', conversation_id='new'))
    draft = dict(key='tool', kind='entity', type='tool', text='Herdr CLI', quote=episode['text'], why='Original tool identity',
        occurrence=None, src_key=None, dst_key=None, valid_from=None, valid_to=None, scope=['professional'],
        open_loop=False, confidence=.9)
    processing.complete(id=extract['id'], owner=extract['owner'], agent='fixture',
        output=dict(windows=[dict(id=extract['input']['windows'][0]['id'], candidates=[draft])]))
    task, = claim(processing, consolidate_now=True)
    shown = {item['id'] for item in task['input']['existing_nodes']}
    assert {tool, gloss} <= shown
    candidate = task['input']['candidates'][0]
    action = dict(candidate_id=candidate['id'], operation='duplicate', type=None, text=None, scope=None,
        open_loop=False, target=tool, src=None, dst=None, relation=None, change_kind=None, valid_at=None,
        valid_from=None, valid_to=None, category=None, sensitive=None,
        evidence=[dict(episode_id=episode['id'], quote=episode['text'], occurrence=None)], why='Same real-world tool')
    result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[action],
        repairs=[repair('absorb', tool, [a, b], absorbed=[gloss])]))
    assert result['results'][candidate['id']]['id'] == tool
    assert graph._node(gloss)['superseded_by'] == tool


def test_maintenance_route_rejects_unknown_or_duplicate_ids_before_seeding(tmp_path, monkeypatch):
    monkeypatch.setenv('MEMENTO_EMBEDDINGS', '0')
    with TestClient(create_app(tmp_path)) as client:
        command = dict(command_id='seed', actor='fixture', why='Inspect selected subject', target_ids=['unknown'])
        assert client.post('/work/maintenance', json=command).status_code == 422
        command['target_ids'] *= 2
        assert client.post('/work/maintenance', json=command).status_code == 422
        assert client.app.state.graph.store._db.execute('SELECT count(*) FROM repair_groups').fetchone()[0] == 0


def test_maintenance_route_persists_idempotent_bounded_request(tmp_path, monkeypatch):
    monkeypatch.setenv('MEMENTO_EMBEDDINGS', '0')
    with TestClient(create_app(tmp_path)) as client:
        graph = client.app.state.graph
        episode, _ = graph.store.capture(dict(source='route-fixture', event_id='one', conversation_id='one',
            kind='user', text='The warehouse uses lane A.', occurred_at=None, metadata={}))
        target = graph.remember(command_id='route-fact', actor='fixture', why='Synthetic original assertion',
            type='knowledge', text=episode['text'], scope=['professional'],
            evidence=[dict(episode_id=episode['id'], quote=episode['text'])])['id']
        command = dict(command_id='route-seed', actor='fixture', why='Inspect selected old assertion', target_ids=[target])
        first = client.post('/work/maintenance', json=command)
        assert first.status_code == 200 and first.json()['status'] == 'pending'
        assert client.post('/work/maintenance', json=command).json() == first.json()
        assert graph.store._db.execute('SELECT count(*) FROM repair_groups').fetchone()[0] == 1
