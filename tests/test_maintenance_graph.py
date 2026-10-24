"""Graph continuity and duplicate coalescence used by nightly maintenance."""

import json

import pytest

from memento.curation import absorb, absorb_check, canonical, propose
from memento.loops import _append, _closed_at, current, snapshot
from memento.graph import Graph
from memento.retrieval import Retrieval
from memento.store import ConflictError, Store

BEHAVIOR = dict(applicability='When a Herdr pane is busy', conduct='Queue the reply instead of interrupting.',
                rationale='Interrupting loses the active turn.', exceptions=[], authority='inferred')
TEXT = '\n'.join((
    'Herdr is our terminal multiplexer.', 'Herdr CLI controls panes.', 'herdr the terminal multiplexer runs agents.',
    'Atlas uses Herdr.', 'Atlas is our shared scheduling project.', 'Herdr is a person in the book.',
    'Releases ship on Fridays.', 'Releases ship on Fridays at noon.', 'Queue replies when a pane is busy.',
))


@pytest.fixture
def world(tmp_path):
    store = Store(tmp_path)
    graph = Graph(store)
    episode, _ = store.capture(dict(source='maintenance', event_id='1', conversation_id='c', kind='user',
                                    text=TEXT, occurred_at='2026-10-24T10:00:00+00:00', metadata={}))
    retrieval = object.__new__(Retrieval)
    retrieval.store, retrieval.graph = store, graph

    def ev(quote):
        return [dict(episode_id=episode['id'], quote=quote)]

    def remember(key, type, text, quote, scope=('professional',), **times):
        return graph.remember(command_id=key, actor='fixture', why='synthetic assertion', type=type, text=text,
                              scope=list(scope), evidence=ev(quote), **times)['id']

    def link(key, a, b, quote, rel='related_to'):
        return graph.link(command_id=key, actor='fixture', why='synthetic relation', a=a, rel=rel, b=b,
                          evidence=ev(quote))['id']

    def run(key, apply, request=None):
        return graph._command(key, 'fixture', 'synthetic maintenance', request or dict(op='fixture', key=key), apply)

    def active_edges():
        return {tuple(row) for row in store._db.execute('''SELECT e.src,e.rel,e.dst FROM edge_versions e
            WHERE e.state='active' AND e.rel<>'supersedes'
            AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id)''')}

    try:
        yield store, graph, retrieval, ev, remember, link, run, active_edges
    finally:
        store.close()


def related(retrieval, node):
    return {item['id'] for item in retrieval.related(node, 1)}


def test_refinement_keeps_relations_and_premises_but_correction_does_not(world):
    store, graph, retrieval, ev, remember, link, run, active_edges = world
    atlas = remember('atlas', 'project', 'Atlas', 'Atlas uses Herdr.')
    herdr = remember('herdr', 'tool', 'Herdr', 'Herdr is our terminal multiplexer.')
    link('uses', atlas, herdr, 'Atlas uses Herdr.')
    refined = graph.supersede(command_id='refine', actor='fixture', why='supported detail, same identity', old=atlas,
        new=dict(type='project', text='Atlas — shared scheduling project', scope=['professional']), kind='refinement',
        evidence=ev('Atlas is our shared scheduling project.'))['superseded_by']
    assert related(retrieval, refined) == {herdr} and related(retrieval, herdr) == {refined}
    assert {item['quote'] for item in graph.history(refined)[-1]['evidence']} >= {
        'Atlas uses Herdr.', 'Atlas is our shared scheduling project.'}
    # The lineage edge stays with history; the old version keeps its own relation versions.
    assert store._db.execute("SELECT src,dst FROM edge_versions WHERE rel='supersedes'").fetchall()[0][:] == (refined, atlas)
    assert len(graph.history(atlas)) == 3
    assert canonical(graph, atlas)['id'] == refined
    corrected = graph.supersede(command_id='correct', actor='fixture', why='the earlier project was wrong', old=refined,
        new=dict(type='project', text='Atlas scheduling', scope=['professional']), kind='correction',
        evidence=ev('Atlas is our shared scheduling project.'))['superseded_by']
    assert related(retrieval, corrected) == set() and related(retrieval, herdr) == set()
    assert canonical(graph, atlas)['id'] == refined  # A correction ends the identity chain.
    assert (refined, 'related_to', herdr) in active_edges()


def test_refinement_that_widens_scope_moves_relations_without_premises(world):
    store, graph, retrieval, ev, remember, link, run, active_edges = world
    atlas = remember('atlas', 'project', 'Atlas', 'Atlas uses Herdr.')
    herdr = remember('herdr', 'tool', 'Herdr', 'Herdr is our terminal multiplexer.')
    link('uses', atlas, herdr, 'Atlas uses Herdr.')
    wider = graph.supersede(command_id='refine', actor='fixture', why='shared project detail', old=atlas,
        new=dict(type='project', text='Atlas — shared scheduling project', scope=['personal', 'professional']),
        kind='refinement', evidence=ev('Atlas is our shared scheduling project.'))['superseded_by']
    assert [item['quote'] for item in graph.history(wider)[-1]['evidence']] == ['Atlas is our shared scheduling project.']
    assert related(retrieval, wider) == {herdr}


def test_pending_endpoint_follows_merges_and_refinements_only(world):
    store, graph, retrieval, ev, remember, link, run, active_edges = world
    atlas = remember('atlas', 'project', 'Atlas', 'Atlas uses Herdr.')
    run('promote', lambda commit: store._db.execute('INSERT INTO candidate_decisions VALUES (?,?,?,?,?,?)',
        ('d1', 'c_atlas', commit, 'promote', atlas, None)) and {})
    endpoint = lambda: retrieval._pending_endpoint('c_atlas', None, store._db)
    assert endpoint()['id'] == atlas
    refined = graph.supersede(command_id='refine', actor='fixture', why='detail', old=atlas,
        new=dict(type='project', text='Atlas scheduling', scope=['professional']), kind='refinement',
        evidence=ev('Atlas is our shared scheduling project.'))['superseded_by']
    assert endpoint() == dict(id=refined, type='project', text='Atlas scheduling', tier='durable')
    other = remember('other', 'project', 'Atlas Scheduling', 'Atlas is our shared scheduling project.')
    run('merge', lambda commit: absorb(graph, commit, other, [refined]))
    assert endpoint()['id'] == other
    graph.supersede(command_id='change', actor='fixture', why='world changed', old=other,
        new=dict(type='project', text='Atlas archive', scope=['professional']), kind='change',
        valid_at='2026-10-25T00:00:00+00:00', evidence=ev('Atlas is our shared scheduling project.'))
    assert endpoint() is None


def test_entity_facets_absorb_into_one_identity_with_relations_aliases_and_usage(world):
    store, graph, retrieval, ev, remember, link, run, active_edges = world
    herdr = remember('herdr', 'tool', 'Herdr', 'Herdr is our terminal multiplexer.')
    cli = remember('cli', 'tool', 'Herdr CLI', 'Herdr CLI controls panes.')
    mux = remember('mux', 'tool', 'herdr the terminal multiplexer', 'herdr the terminal multiplexer runs agents.')
    namesake = remember('namesake', 'person', 'Herdr', 'Herdr is a person in the book.')
    atlas = remember('atlas', 'project', 'Atlas', 'Atlas uses Herdr.')
    link('atlas-herdr', atlas, herdr, 'Atlas uses Herdr.')
    link('atlas-cli', atlas, cli, 'Atlas uses Herdr.')  # Folds into the same structural relation.
    link('facets', cli, mux, 'Herdr CLI controls panes.')  # Would become a self relation.
    link('namesake-mentions', namesake, mux, 'Herdr is a person in the book.', rel='mentions')
    store._db.execute("INSERT INTO node_stats(node_id,access_count) VALUES (?,3) ON CONFLICT(node_id) DO UPDATE SET access_count=3", (cli,))
    assert absorb_check(graph, herdr, [namesake]) == 'consolidation_absorb_type_mismatch'
    assert absorb_check(graph, herdr, [cli, cli]) == 'consolidation_absorb_member_invalid'
    result = run('absorb', lambda commit: absorb(graph, commit, herdr, [cli, mux], evidence=ev('Herdr CLI controls panes.')))
    assert result['status'] == 'absorbed' and result['absorbed'] == [cli, mux]
    assert {canonical(graph, item)['id'] for item in (herdr, cli, mux)} == {herdr}
    assert active_edges() == {(atlas, 'related_to', herdr), (namesake, 'mentions', herdr)}
    assert graph._node(namesake)['state'] == 'active'
    quotes = {item['quote'] for item in graph.history(herdr)[-1]['evidence']}
    assert {'Herdr is our terminal multiplexer.', 'Herdr CLI controls panes.',
            'herdr the terminal multiplexer runs agents.'} <= quotes
    assert [item['id'] for item in graph.recall_keywords('CLI')] == [herdr]
    assert store._db.execute('SELECT access_count FROM node_stats WHERE node_id=?', (herdr,)).fetchone()[0] == 3
    assert [row['state'] for row in graph.history(cli)] == ['active', 'merged']
    assert graph._exact_identities('tool', 'HERDR CLI') == [dict(id=herdr, version=graph._node(herdr)['version'],
                                                                commit_id=graph._node(herdr)['commit_id'])]
    # Replaying the identical command adds nothing; a merged member cannot be absorbed again.
    versions = store._db.execute('SELECT count(*) FROM node_versions').fetchone()[0]
    assert run('absorb', lambda commit: absorb(graph, commit, herdr, [cli, mux])) == result
    assert store._db.execute('SELECT count(*) FROM node_versions').fetchone()[0] == versions
    assert absorb_check(graph, herdr, [cli]) == 'consolidation_absorb_member_invalid'
    with pytest.raises(ConflictError):
        run('again', lambda commit: absorb(graph, commit, herdr, [cli]))


def test_equivalent_claims_absorb_without_aliases_and_respect_scope_validity(world):
    store, graph, retrieval, ev, remember, link, run, active_edges = world
    first = remember('first', 'knowledge', 'Releases ship on Fridays.', 'Releases ship on Fridays.')
    second = remember('second', 'knowledge', 'Releases ship on Fridays', 'Releases ship on Fridays at noon.')
    personal = remember('personal', 'knowledge', 'Releases ship on Fridays.', 'Releases ship on Fridays.',
                        scope=('personal',))
    dated = remember('dated', 'knowledge', 'Releases ship on Fridays.', 'Releases ship on Fridays.',
                     valid_from='2026-01-01T00:00:00+00:00')
    assert absorb_check(graph, first, [personal]) == 'consolidation_absorb_scope_mismatch'
    assert absorb_check(graph, first, [dated]) == 'consolidation_absorb_validity_mismatch'
    aliases = store._db.execute('SELECT count(*) FROM alias_fts').fetchone()[0]
    result = run('absorb', lambda commit: absorb(graph, commit, first, [second]))
    assert result['scope'] == ['professional']
    assert store._db.execute('SELECT count(*) FROM alias_fts').fetchone()[0] == aliases
    assert {item['quote'] for item in graph.history(first)[-1]['evidence']} == {
        'Releases ship on Fridays.', 'Releases ship on Fridays at noon.'}
    assert second not in {item['id'] for item in graph.recall_keywords('Fridays', scope='professional')}
    assert graph._node(personal)['state'] == graph._node(dated)['state'] == 'active'


def test_claim_behavior_authority_is_never_dropped(world):
    store, graph, retrieval, ev, remember, link, run, active_edges = world

    def experience(key, behavior):
        def apply(commit):
            node = graph._insert_node(commit, dict(type='experience', text='Queue replies when a pane is busy.',
                scope=['professional'], evidence=ev('Queue replies when a pane is busy.')))
            return dict(results={'c_' + key: node})
        request = dict(op='consolidate', output=dict(actions=[dict(candidate_id='c_' + key, operation='remember',
                       type='experience', behavior=behavior)]))
        return run(key, apply, request)['results']['c_' + key]['id']
    lesson = experience('lesson', BEHAVIOR)
    same = experience('same', BEHAVIOR)
    plain = experience('plain', None)
    explicit = experience('explicit', dict(BEHAVIOR, authority='explicit_user'))
    assert absorb_check(graph, lesson, [explicit]) == 'consolidation_absorb_behavior_mismatch'
    assert absorb_check(graph, plain, [lesson]) == 'consolidation_absorb_behavior_mismatch'
    run('absorb', lambda commit: absorb(graph, commit, lesson, [same, plain]))
    assert graph._behavior(lesson) == BEHAVIOR
    assert graph._node(explicit)['state'] == 'active'


def test_repair_supersede_keeps_only_explicitly_reemitted_behavior(world):
    store, graph, retrieval, ev, remember, link, run, active_edges = world
    lessons = {}
    for key, behavior in (('kept', BEHAVIOR), ('retired', None)):
        old = remember(key, 'experience', 'Queue replies.', 'Queue replies when a pane is busy.')
        repair = dict(operation='supersede', target=old, type='experience', behavior=behavior)

        def apply(commit, old=old):
            result = graph._apply_supersede(commit, dict(op='supersede', old=old, kind='refinement', valid_at=None,
                new=dict(type='experience', text='Queue replies when a pane is busy.', scope=['professional']),
                evidence=ev('Queue replies when a pane is busy.')))
            return dict(results={}, repair_results={old: result})
        result = run('repair-' + key, apply, dict(op='consolidate', output=dict(actions=[], repairs=[repair])))
        lessons[key] = result['repair_results'][old]['superseded_by']
    assert graph._behavior(lessons['kept']) == BEHAVIOR
    assert graph._behavior(lessons['retired']) is None
    assert graph.history(lessons['kept'])[-1]['behavior'] == BEHAVIOR


def test_protected_members_become_a_fenced_absorb_review(world):
    store, graph, retrieval, ev, remember, link, run, active_edges = world
    herdr = remember('herdr', 'tool', 'Herdr', 'Herdr is our terminal multiplexer.')
    cli = remember('cli', 'tool', 'Herdr CLI', 'Herdr CLI controls panes.', scope=('personal', 'professional'))
    pinned = remember('pinned', 'tool', 'herdr multiplexer', 'herdr the terminal multiplexer runs agents.')
    run('pin', lambda commit: __import__('memento.curation', fromlist=['pin']).pin(graph, commit, pinned, True))
    assert absorb_check(graph, herdr, [cli]) is None
    with pytest.raises(ValueError):
        run('no-evidence', lambda commit: absorb(graph, commit, herdr, [cli]))
    scoped = run('scoped', lambda commit: absorb(graph, commit, herdr, [cli], evidence=ev('Herdr CLI controls panes.')))
    guarded = run('pinned-absorb', lambda commit: absorb(graph, commit, herdr, [pinned],
                                                  evidence=ev('herdr the terminal multiplexer runs agents.')))
    assert scoped['status'] == guarded['status'] == 'pending_review'
    assert {graph._node(item)['state'] for item in (herdr, cli, pinned)} == {'active'}
    approved = graph._decide_review(command_id='yes', actor='human', why='same tool', proposal_id=scoped['id'],
                                    decision='approve')
    assert approved['status'] == 'absorbed' and json.loads(graph._node(herdr)['scope']) == ['personal', 'professional']
    with pytest.raises(ConflictError):  # The survivor changed after the pinned proposal.
        graph._decide_review(command_id='stale', actor='human', why='same tool', proposal_id=guarded['id'],
                             decision='approve')
    assert graph._node(pinned)['state'] == 'active'


def test_differing_loop_choices_are_not_silently_lost(world):
    store, graph, retrieval, ev, remember, link, run, active_edges = world
    first = remember('first', 'knowledge', 'Releases ship on Fridays.', 'Releases ship on Fridays.')
    second = remember('second', 'knowledge', 'Releases ship on Fridays', 'Releases ship on Fridays at noon.')
    third = remember('third', 'knowledge', 'Releases ship Fridays.', 'Releases ship on Fridays at noon.')
    graph.loop(command_id='open', actor='fixture', why='pending', action='open', id=second)
    graph.loop(command_id='close', actor='fixture', why='done', action='close', id=third)
    assert absorb_check(graph, first, [second, third]) == 'consolidation_absorb_loop_conflict'
    run('absorb', lambda commit: absorb(graph, commit, first, [second]))
    assert graph.history(first)[-1]['loop']['state'] == 'open'
    assert absorb_check(graph, first, [third]) == 'consolidation_absorb_loop_conflict'


def legacy_subject(world, monkeypatch, *, kind='refinement', count=2):
    store, graph, retrieval, ev, remember, link, run, active_edges = world
    from memento.processing import Processing
    from datetime import datetime, timezone
    atlas = remember('atlas', 'project', 'Atlas', 'Atlas uses Herdr.')
    herdr = remember('herdr', 'tool', 'Herdr', 'Herdr is our terminal multiplexer.')
    edges = [link(f'edge-{number}', atlas if number % 2 == 0 else herdr,
                  herdr if number % 2 == 0 else atlas, 'Atlas uses Herdr.') for number in range(count)]
    # Simulate the prior service, which refined the identity without moving its relations.
    with monkeypatch.context() as baseline:
        baseline.setattr('memento.curation.redirect_edges', lambda *_args, **_kwargs: [])
        refined = graph.supersede(command_id='legacy-refine', actor='fixture', why='same identity detail', old=atlas,
            new=dict(type='project', text='Atlas', scope=['professional']), kind=kind,
            valid_at='2026-10-24T11:00:00+00:00' if kind == 'change' else None,
            evidence=ev('Atlas is our shared scheduling project.'))['superseded_by']
    processing = Processing(graph)
    args = dict(agent='fixture', timezone_name='UTC', now=datetime(2026, 10, 24, 3, tzinfo=timezone.utc))
    return processing, args, atlas, refined, herdr, edges


def maintenance_task(processing, args, targets, key='repair'):
    processing.maintain(command_id=key, actor='fixture', why='Maintain selected current subject', target_ids=list(targets))
    task, = processing.claim(**args)['work']
    return task


def complete_maintenance(processing, task):
    return processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[]))


def test_legacy_refined_relations_repair_inside_ordinary_maintenance(world, monkeypatch):
    store, graph, retrieval, ev, remember, link, run, active_edges = world
    processing, args, old, refined, herdr, edges = legacy_subject(world, monkeypatch)
    before = {edge: [dict(row) for row in store._db.execute('SELECT * FROM graph_evidence WHERE target=?', (edge,))]
              for edge in edges}
    assert related(retrieval, refined) == set()
    task = maintenance_task(processing, args, [refined])
    assert task['input']['candidates'] == [] and '_continuity' not in task['input']
    result = complete_maintenance(processing, task)
    assert {item['id'] for item in result['redirected_relations']} == set(edges)
    assert related(retrieval, refined) == {herdr} and refined in related(retrieval, herdr)
    for edge in edges:
        assert store._db.execute('SELECT max(version) FROM edge_versions WHERE id=?', (edge,)).fetchone()[0] == 2
        after = [dict(row) for row in store._db.execute('SELECT * FROM graph_evidence WHERE target=? AND version=2', (edge,))]
        assert [{**row, 'version': 1} for row in after] == before[edge]
    again = maintenance_task(processing, args, [refined], 'repeat')
    assert complete_maintenance(processing, again)['redirected_relations'] == []
    assert all(store._db.execute('SELECT max(version) FROM edge_versions WHERE id=?', (edge,)).fetchone()[0] == 2
               for edge in edges)


@pytest.mark.parametrize('kind', ['correction', 'change'])
def test_maintenance_never_redirects_correction_or_world_change(world, monkeypatch, kind):
    store, graph, retrieval, ev, remember, link, run, active_edges = world
    processing, args, old, replacement, herdr, edges = legacy_subject(world, monkeypatch, kind=kind)
    task = maintenance_task(processing, args, [replacement])
    assert complete_maintenance(processing, task)['redirected_relations'] == []
    assert related(retrieval, replacement) == set()
    assert all(store._db.execute('SELECT max(version) FROM edge_versions WHERE id=?', (edge,)).fetchone()[0] == 1
               for edge in edges)


@pytest.mark.parametrize('change', ['old_node', 'edge', 'loop', 'collision_absence', 'collision_version'])
def test_maintenance_continuity_rejects_changed_node_edge_loop_and_collision(world, monkeypatch, change):
    store, graph, retrieval, ev, remember, link, run, active_edges = world
    processing, args, old, refined, herdr, edges = legacy_subject(world, monkeypatch, count=1)
    collider = link('collider', refined, herdr, 'Atlas uses Herdr.') if change == 'collision_version' else None
    task = maintenance_task(processing, args, [refined])
    if change == 'old_node':
        def append(commit):
            store._db.execute("""INSERT INTO node_versions SELECT id,version+1,?,type,text,state,
                valid_from,valid_to,superseded_by,pinned,scope FROM node_versions WHERE id=? AND version=2""", (commit, old))
            return {}
        run('old-version-change', append)
    elif change == 'edge':
        def append(commit):
            store._db.execute("""INSERT INTO edge_versions SELECT id,version+1,?,src,rel,dst,state
                FROM edge_versions WHERE id=? AND version=1""", (commit, edges[0]))
            return {}
        run('edge-version-change', append)
    elif change == 'loop':
        run('orphan-manual-choice', lambda commit: _append(graph, commit, edges[0], 'open', None, None))
    elif change == 'collision_absence':
        link('new-collider', refined, herdr, 'Atlas uses Herdr.')
    else:
        from memento.curation import attach
        run('collider-version-change', lambda commit: attach(graph, commit, collider, ev('Atlas uses Herdr.')))
    with pytest.raises(ConflictError, match='continuity'):
        complete_maintenance(processing, task)
    assert related(retrieval, refined) == ({herdr} if change.startswith('collision') else set())


def test_maintenance_continuity_keeps_conflicting_relation_loops(world, monkeypatch):
    store, graph, retrieval, ev, remember, link, run, active_edges = world
    processing, args, old, refined, herdr, edges = legacy_subject(world, monkeypatch, count=1)
    collider = link('collider', refined, herdr, 'Atlas uses Herdr.')
    run('orphan-open', lambda commit: _append(graph, commit, edges[0], 'open', None, None))
    graph.loop(command_id='collider-closed', actor='human', why='Explicit closed marker', action='close', id=collider)
    complete_maintenance(processing, maintenance_task(processing, args, [refined]))
    assert current(graph, edges[0])['state'] == 'open' and current(graph, collider)['state'] == 'closed'
    rows = store._db.execute("""SELECT e.id FROM edge_versions e WHERE src=? AND dst=? AND e.state='active'
        AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id)""", (refined, herdr)).fetchall()
    assert {row['id'] for row in rows} == {edges[0], collider}


def test_maintenance_continuity_defers_unfenced_final_destination_after_same_plan_merge(world, monkeypatch):
    store, graph, retrieval, ev, remember, link, run, active_edges = world
    processing, args, old, refined, herdr, edges = legacy_subject(world, monkeypatch, count=1)
    survivor = remember('canonical-atlas', 'project', 'Atlas', 'Atlas is our shared scheduling project.')
    task = maintenance_task(processing, args, [refined, survivor])
    collider = link('new-final-collider', survivor, herdr, 'Atlas uses Herdr.')
    result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[], repairs=[
        dict(operation='absorb', target=survivor, absorbed=[refined],
             evidence=[dict(**ev('Atlas is our shared scheduling project.')[0], occurrence=None)],
             why='Same project referent, retain the canonical identity')]))
    assert result['redirected_relations'] == []
    assert store._db.execute('SELECT max(version) FROM edge_versions WHERE id=?', (edges[0],)).fetchone()[0] == 1
    assert store._db.execute('SELECT max(version) FROM edge_versions WHERE id=?', (collider,)).fetchone()[0] == 1
    following, = processing.claim(**args)['work']
    result = complete_maintenance(processing, following)
    assert result['redirected_relations'][0]['id'] == edges[0]
    assert related(retrieval, survivor) == {herdr}


def test_maintenance_continuity_pages_without_sweeping_unfenced_edges(world, monkeypatch):
    store, graph, retrieval, ev, remember, link, run, active_edges = world
    processing, args, old, refined, herdr, edges = legacy_subject(world, monkeypatch, count=60)
    task = maintenance_task(processing, args, [refined])
    _, leased = processing._leased(task['id'], task['owner'])
    frozen = leased['_continuity']
    assert len(frozen['nodes']) + len(frozen['edges']) <= 48
    selected = {edge for move in frozen['moves'] for edge in move['edge_ids']}
    assert 0 < len(selected) < len(edges)
    first = complete_maintenance(processing, task)
    assert {item['id'] for item in first['redirected_relations']} == selected
    assert all(store._db.execute('SELECT max(version) FROM edge_versions WHERE id=?', (edge,)).fetchone()[0] == 1
               for edge in set(edges) - selected)
    for _ in range(4):
        work = processing.claim(**args)['work']
        if not work:
            break
        complete_maintenance(processing, work[0])
    assert all(not ({old} & {row['src'], row['dst']}) for row in store._db.execute("""SELECT e.* FROM edge_versions e
        WHERE e.state='active' AND e.rel<>'supersedes'
        AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id)"""))
    assert processing.claim(**args)['work'] == []


def test_refinement_keeps_known_world_interval_and_correction_does_not_invent_one(world):
    store, graph, retrieval, ev, remember, link, run, active_edges = world
    interval = dict(valid_from='2026-01-01T00:00:00+00:00', valid_to='2026-12-31T00:00:00+00:00')
    for kind, expected in (('refinement', tuple(interval.values())), ('correction', (None, None))):
        old = remember(kind, 'knowledge', 'Releases ship on Fridays.', 'Releases ship on Fridays.', **interval)
        new = graph.supersede(command_id='s-' + kind, actor='fixture', why='synthetic', old=old, kind=kind,
            new=dict(type='knowledge', text='Releases ship on Fridays at noon.', scope=['professional']),
            evidence=ev('Releases ship on Fridays at noon.'))['superseded_by']
        assert (graph._node(new)['valid_from'], graph._node(new)['valid_to']) == expected
        assert graph._node(old)['valid_to'] == interval['valid_to']


def test_merged_alias_anchors_the_refined_survivor_with_its_connections(world):
    store, graph, retrieval, ev, remember, link, run, active_edges = world
    herdr = remember('herdr', 'tool', 'Herdr', 'Herdr is our terminal multiplexer.')
    cli = remember('cli', 'tool', 'Herdr CLI', 'Herdr CLI controls panes.')
    atlas = remember('atlas', 'project', 'Atlas', 'Atlas uses Herdr.')
    link('uses', atlas, herdr, 'Atlas uses Herdr.')
    run('absorb', lambda commit: absorb(graph, commit, herdr, [cli]))
    refined = graph.supersede(command_id='refine', actor='fixture', why='same tool, supported name detail', old=herdr,
        new=dict(type='tool', text='Herdr multiplexer', scope=['professional']), kind='refinement',
        evidence=ev('herdr the terminal multiplexer runs agents.'))['superseded_by']
    anchors = retrieval._entity_anchors('How do I split panes with the Herdr CLI?', 4, None, store._db)
    assert [anchor['id'] for anchor in anchors] == [refined]
    assert [item['id'] for item in anchors[0]['connections']] == [atlas]
    assert [item['id'] for item in graph.recall_keywords('CLI')] == [refined]


def test_relation_folds_never_hide_a_differing_loop_marker(world):
    store, graph, retrieval, ev, remember, link, run, active_edges = world
    herdr = remember('herdr', 'tool', 'Herdr', 'Herdr is our terminal multiplexer.')
    cli = remember('cli', 'tool', 'Herdr CLI', 'Herdr CLI controls panes.')
    atlas = remember('atlas', 'project', 'Atlas', 'Atlas uses Herdr.')
    closed, pending = link('closed', atlas, herdr, 'Atlas uses Herdr.'), link('pending', atlas, cli, 'Atlas uses Herdr.')
    run('markers', lambda commit: (_append(graph, commit, closed, 'closed', None, None),
                                   _append(graph, commit, pending, 'open', None, None)) and {})
    assert absorb_check(graph, herdr, [cli]) == 'consolidation_absorb_loop_conflict'
    # A human-approved merge keeps the differing obligation on its own relation.
    proposal = run('propose', lambda commit: propose(graph, commit, dict(op='merge', a=cli, b=herdr,
                                                                          evidence=ev('Herdr CLI controls panes.'))))
    graph._decide_review(command_id='yes', actor='human', why='same tool', proposal_id=proposal['id'], decision='approve')
    assert graph.history(pending)[-1]['src'] == atlas and graph.history(pending)[-1]['dst'] == herdr
    assert graph.history(pending)[-1]['state'] == 'active' and current(graph, pending)['state'] == 'open'
    assert current(graph, closed)['state'] == 'closed'
    # A would-be self relation carrying an open obligation also refuses.
    mux = remember('mux', 'tool', 'herdr the terminal multiplexer', 'herdr the terminal multiplexer runs agents.')
    facet = link('facet', herdr, mux, 'herdr the terminal multiplexer runs agents.')
    run('facet-open', lambda commit: _append(graph, commit, facet, 'open', None, None) and {})
    assert absorb_check(graph, herdr, [mux]) == 'consolidation_absorb_loop_conflict'


def test_refinement_keeps_differing_duplicate_relation_markers_apart(world):
    store, graph, retrieval, ev, remember, link, run, active_edges = world
    atlas = remember('atlas', 'project', 'Atlas', 'Atlas uses Herdr.')
    herdr = remember('herdr', 'tool', 'Herdr', 'Herdr is our terminal multiplexer.')
    first, second = link('first', atlas, herdr, 'Atlas uses Herdr.'), link('second', atlas, herdr, 'Atlas uses Herdr.')
    run('markers', lambda commit: (_append(graph, commit, first, 'closed', None, None),
                                   _append(graph, commit, second, 'open', None, None)) and {})
    refined = graph.supersede(command_id='refine', actor='fixture', why='detail', old=atlas,
        new=dict(type='project', text='Atlas scheduling', scope=['professional']), kind='refinement',
        evidence=ev('Atlas is our shared scheduling project.'))['superseded_by']
    assert [(row['src'], row['state']) for row in (graph.history(first)[-1], graph.history(second)[-1])] == [
        (refined, 'active'), (refined, 'active')]
    assert (current(graph, first)['state'], current(graph, second)['state']) == ('closed', 'open')


@pytest.mark.parametrize('survivor_choice', ['automatic', 'older_manual'])
def test_absorbed_manual_loop_chronology_survives_on_the_survivor(world, survivor_choice):
    # Requires merged/refined ancestry in loops.snapshot and loops._closed_at (Codex-owned loops.py).
    store, graph, retrieval, ev, remember, link, run, active_edges = world
    survivor = remember('survivor', 'knowledge', 'Releases ship on Fridays.', 'Releases ship on Fridays.')
    absorbed = remember('absorbed', 'knowledge', 'Releases ship on Fridays', 'Releases ship on Fridays at noon.')
    if survivor_choice == 'automatic':
        run('automatic', lambda commit: _append(graph, commit, survivor, 'open', None, None) and {})
    else:
        graph.loop(command_id='older', actor='owner', why='older owner choice', action='open', id=survivor)
    graph.loop(command_id='newer', actor='owner', why='newer owner choice', action='open', id=absorbed)
    chosen = snapshot(graph, absorbed, graph._node(absorbed)['version'])['manual_choice_at']
    assert absorb_check(graph, survivor, [absorbed]) is None
    run('absorb', lambda commit: absorb(graph, commit, survivor, [absorbed]))
    assert snapshot(graph, survivor, graph._node(survivor)['version'])['manual_choice_at'] == chosen


def test_absorbed_explicit_close_remains_the_reopening_boundary(world):
    # Requires merged/refined ancestry in loops._closed_at (Codex-owned loops.py).
    store, graph, retrieval, ev, remember, link, run, active_edges = world
    survivor = remember('survivor', 'knowledge', 'Releases ship on Fridays.', 'Releases ship on Fridays.')
    absorbed = remember('absorbed', 'knowledge', 'Releases ship on Fridays', 'Releases ship on Fridays at noon.')
    graph.loop(command_id='close-old', actor='owner', why='done earlier', action='close', id=survivor)
    graph.loop(command_id='close-new', actor='owner', why='done again', action='close', id=absorbed)
    boundary = _closed_at(graph, absorbed)
    run('absorb', lambda commit: absorb(graph, commit, survivor, [absorbed]))
    assert _closed_at(graph, survivor) == boundary
