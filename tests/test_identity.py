"""Already-consolidated identity checks and owner-reviewed duplicate repair, on synthetic stores only."""

import json
import sys
from datetime import datetime, timezone

import pytest

from memento import admin
from memento.consolidation import IDENTITY_SLOTS
from memento.curation import propose
from memento.graph import Graph, identity_text
from memento.importance import query_recency
from memento.processing import Processing
from memento.store import ConflictError, Store


TEXT = 'Orion is the warehouse project. C++ and C are tools. Filler fact.'
NOW = datetime(2030, 1, 1, 12, tzinfo=timezone.utc)


def draft(key, kind, type, text, src=None, dst=None, scope=('professional',)):
    return dict(key=key, kind=kind, type=type, text=text, quote=TEXT, why='Explicit durable source assertion',
        occurrence=None, src_key=src, dst_key=dst, valid_from=None, valid_to=None,
        scope=list(scope), open_loop=False, confidence=.8)


def action(candidate, episode, operation, **fields):
    value = dict(candidate_id=candidate['id'], operation=operation, type=None, text=None, scope=None,
        open_loop=None, target=None, src=None, dst=None, relation=None, change_kind=None, valid_at=None,
        valid_from=None, valid_to=None, category=None, sensitive=None,
        evidence=[] if operation in ('reject', 'defer') else [dict(episode_id=episode['id'], quote=TEXT, occurrence=None)],
        why='Synthetic identity decision')
    value.update(fields)
    return value


@pytest.fixture
def world(tmp_path):
    store = Store(tmp_path)
    graph = Graph(store)
    processing = Processing(graph)
    episodes = []

    def capture(day=1):
        episode, _ = store.capture(dict(source='identity', event_id=f'e{len(episodes)}',
            conversation_id=f'c{len(episodes)}', kind='user', text=TEXT,
            occurred_at=f'2026-01-{day:02d}T00:00:00+00:00', metadata={}))
        episodes.append(episode)
        return episode

    def drain(target=None, drafts=(), **options):
        """Complete extraction work (drafts only for target), returning any consolidation work."""
        while True:
            work = processing.claim(agent='fixture', timezone_name='UTC', now=NOW, **options)['work']
            options.pop('compaction', None)
            if not work:
                return None
            for item in work:
                if item['kind'] == 'consolidate':
                    return item
                _, context = processing._leased(item['id'], item['owner'])
                processing.complete(id=item['id'], owner=item['owner'], agent='fixture', output=dict(windows=[
                    dict(id=shown['id'], candidates=list(drafts) if source['episode_id'] == target else [])
                    for shown, source in zip(item['input']['windows'], context['windows'], strict=True)]))

    def extract(drafts, day=1):
        episode = capture(day)
        drain(episode['id'], drafts, compaction=dict(source='identity', conversation_id=episode['conversation_id']))
        assert store._db.execute('''SELECT count(*) FROM candidates c JOIN extraction_runs r ON r.id=c.run_id
            WHERE r.episode_id=?''', (episode['id'],)).fetchone()[0] == len(drafts)
        return episode

    def consolidate():
        return drain(consolidate_now=True)

    def node(type, text, episode, scope=('professional',), key=None):
        return graph.remember(command_id=key or f'node-{type}-{text}-{episode["id"]}-{scope}', actor='fixture',
            why='Synthetic consolidated identity', type=type, text=text, scope=list(scope),
            evidence=[dict(episode_id=episode['id'], quote=TEXT)])['id']

    def merge(a, b, key):
        episode = episodes[0]
        request = dict(op='merge', a=a, b=b, evidence=[dict(episode_id=episode['id'], quote=TEXT)])
        proposal = graph._command('propose-' + key, 'model-fixture', 'Propose identity merge', request,
                                  lambda commit: propose(graph, commit, request))
        return proposal, lambda: graph._decide_review(command_id='approve-' + key, actor='human-fixture',
            why='Explicit synthetic approval', proposal_id=proposal['id'], decision='approve')

    try:
        yield store, graph, processing, capture, extract, consolidate, node, merge
    finally:
        store.close()


def many(graph, episode, type, texts):
    def apply(commit):
        return [graph._apply_remember(commit, dict(type=type, text=text, scope=['professional'],
            evidence=[dict(episode_id=episode['id'], quote=TEXT)]))['id'] for text in texts]
    return graph._command(f'many-{type}-{len(texts)}-{texts[0]}', 'fixture', 'Synthetic existing memories',
                          dict(op='fixture'), apply)


def active(store, type, text):
    return [row[0] for row in store._db.execute('''SELECT n.id FROM node_versions n WHERE n.type=? AND n.text=?
        AND n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)''',
        (type, text))]


def test_identity_spelling_is_case_and_whitespace_only(world):
    store, graph, _, capture, _, _, node, _ = world
    episode = capture()
    orion = node('project', 'Orion', episode)
    c = node('tool', 'C', episode)
    node('project', 'Project Orion', episode)
    assert identity_text('  ORION\n') == 'orion'
    assert [item['id'] for item in graph._exact_identities('project', ' orion ')] == [orion]
    assert [item['id'] for item in graph._exact_identities('tool', 'C')] == [c]
    assert graph._exact_identities('tool', 'C++') == []
    assert graph._exact_identities('organization', 'Orion') == []


def test_late_candidate_checks_unindexed_identity_before_neighbors_fill_the_cap(world, monkeypatch):
    store, graph, processing, capture, extract, consolidate, node, _ = world
    episode = capture()
    orion = node('project', 'Orion', episode)  # never encoded: no vector exists
    fillers = [f'Filler fact {index}' for index in range(15)]
    references = many(graph, episode, 'knowledge', [f'Unrelated reference {index}' for index in range(120)])
    neighbors = {text: references[index * 8:(index + 1) * 8] for index, text in enumerate(fillers)}
    monkeypatch.setattr(graph, 'recall_keywords',
        lambda query, limit: [dict(id=target) for target in neighbors.get(query, [])][:limit])
    extract([draft(f'filler-{index}', 'memory', 'knowledge', text) for index, text in enumerate(fillers)]
            + [draft('orion', 'entity', 'project', 'Orion')])
    context = consolidate()['input']
    assert len(context['candidates']) == 16 and context['candidates'][-1]['text'] == 'Orion'
    assert len(context['existing_nodes']) == 48
    assert orion in {item['id'] for item in context['existing_nodes']}
    assert context['candidates'][-1]['identity'] == dict(exact=[orion], exact_total=1, similar=[])
    assert all(item['identity']['exact_total'] == 0 for item in context['candidates'][:-1])
    # Remaining slots are shared fairly instead of drained by the first fillers.
    shown = {item['id'] for item in context['existing_nodes']}
    assert all(shown & set(neighbors[text]) for text in fillers)


def test_consecutive_batches_see_earlier_identity_and_may_keep_a_distinct_homonym(world):
    store, graph, processing, _, extract, consolidate, _, _ = world
    first = extract([draft('orion', 'entity', 'project', 'Orion')])
    task = consolidate()
    candidate = task['input']['candidates'][0]
    assert candidate['identity'] == dict(exact=[], exact_total=0, similar=[])
    created = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[
        action(candidate, first, 'remember', type='project', text='Orion', scope=['professional'])]))
    orion = created['results'][candidate['id']]['id']
    second = extract([draft('again', 'entity', 'project', ' ORION ')], day=2)
    task = consolidate()
    candidate = task['input']['candidates'][0]
    assert candidate['identity']['exact'] == [orion] and candidate['identity']['exact_total'] == 1
    assert orion in {item['id'] for item in task['input']['existing_nodes']}
    # The model judged a distinct same-name project from supplied evidence; homonyms remain allowed.
    result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[
        action(candidate, second, 'remember', type='project', text='Orion', scope=['personal'])]))
    assert result['results'][candidate['id']]['decision'] == 'promote'
    assert len(active(store, 'project', 'Orion')) == 2


def test_truncated_exact_check_goes_to_review_instead_of_blind_identity_or_replay(world):
    store, graph, processing, capture, extract, consolidate, _, _ = world
    episode = capture()
    many(graph, episode, 'project', ['Orion'] * 50)
    origin = extract([draft('orion', 'entity', 'project', 'Orion')])
    task = consolidate()
    candidate = task['input']['candidates'][0]
    assert candidate['identity']['exact_total'] == 50 and len(candidate['identity']['exact']) == 48 > IDENTITY_SLOTS
    result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[
        action(candidate, origin, 'remember', type='project', text='Orion', scope=['professional'])]))
    assert result['results'][candidate['id']]['decision'] == 'review'
    assert len(active(store, 'project', 'Orion')) == 50
    pending = graph.pending_reviews()
    assert [json.loads(item['command'])['identity'] for item in pending] == [
        dict(status='exact_matches_not_supplied', exact_total=50, supplied=candidate['identity']['exact'])]
    assert len(json.loads(pending[0]['base_versions'])) == 50 and pending[0]['stale'] is False
    assert candidate['id'] not in {item['id'] for item in processing.consolidator.pending()}
    # A later owner repair of the colliding identities stales the review and
    # returns its candidate to processing; the old approval cannot recreate a duplicate.
    cluster = graph._merge_inventory()['clusters'][0]
    ids = [item['id'] for item in cluster['members']]
    plan = graph._merge_preview(cluster['cluster'], into=ids[0], exclude=ids[3:])
    repaired = graph._merge(command_id='repair', actor='human-fixture', why='Owner repair',
                            cluster=cluster['cluster'], into=ids[0], exclude=ids[3:], token=plan['token'])
    assert repaired['merged'] == ids[1:3] and repaired['requeued_candidates'] == [candidate['id']]
    assert [item['stale'] for item in graph.pending_reviews()] == [True]
    with pytest.raises(ConflictError, match='stale'):
        graph._decide_review(command_id='late-approval', actor='human-fixture', why='Late approval',
                             proposal_id=pending[0]['id'], decision='approve')
    assert len(active(store, 'project', 'Orion')) == 48
    assert candidate['id'] in {item['id'] for item in processing.consolidator.pending()}


def test_identity_review_staled_by_ordinary_corroboration_is_derived_again(world):
    from memento.curation import attach
    store, graph, processing, capture, extract, consolidate, _, _ = world
    episode = capture()
    many(graph, episode, 'project', ['Orion'] * 50)
    origin = extract([draft('orion', 'entity', 'project', 'Orion')])
    task = consolidate()
    candidate = task['input']['candidates'][0]
    result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[
        action(candidate, origin, 'remember', type='project', text='Orion', scope=['professional'])]))
    review = result['results'][candidate['id']]['id']
    assert candidate['id'] not in {item['id'] for item in processing.consolidator.pending()}
    corroborated = candidate['identity']['exact'][0]
    later = capture(day=3)
    graph._command('corroborate', 'fixture', 'Ordinary corroboration', dict(op='fixture'),
        lambda commit: attach(graph, commit, corroborated, [dict(episode_id=later['id'], quote=TEXT)]))
    assert [(item['id'], item['stale']) for item in graph.pending_reviews()] == [(review, True)]
    with pytest.raises(ConflictError, match='stale'):
        graph._decide_review(command_id='late-approval', actor='human-fixture', why='Late approval',
                             proposal_id=review, decision='approve')
    assert candidate['id'] in {item['id'] for item in processing.consolidator.pending()}
    fresh = consolidate()
    again = fresh['input']['candidates'][0]
    assert again['id'] == candidate['id'] and again['identity']['exact_total'] == 50
    assert corroborated in again['identity']['exact']
    promoted = processing.complete(id=fresh['id'], owner=fresh['owner'], agent='fixture', output=dict(actions=[
        action(again, origin, 'duplicate', target=corroborated)]))
    assert promoted['results'][candidate['id']] == dict(id=corroborated, version=3, decision='promote')
    assert len(active(store, 'project', 'Orion')) == 50
    # The stale proposal stays an undecided review; a later rejection no longer touches the candidate.
    graph._decide_review(command_id='stale-no', actor='human-fixture', why='Superseded by fresh plan',
                         proposal_id=review, decision='reject')
    latest = store._db.execute('''SELECT decision,target_id FROM candidate_decisions WHERE candidate_id=?
        ORDER BY rowid DESC LIMIT 1''', (candidate['id'],)).fetchone()
    assert tuple(latest) == ('promote', corroborated)


def test_identity_created_after_prepare_requires_a_fresh_plan(world):
    store, graph, processing, capture, extract, consolidate, node, _ = world
    origin = extract([draft('orion', 'entity', 'project', 'Orion')])
    task = consolidate()
    candidate = task['input']['candidates'][0]
    assert candidate['identity']['exact_total'] == 0
    late = node('project', 'orion', capture(), key='concurrent-orion')
    with pytest.raises(ConflictError, match='identity context changed'):
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[
            action(candidate, origin, 'remember', type='project', text='Orion', scope=['professional'])]))
    assert active(store, 'project', 'Orion') == []
    store._db.execute('UPDATE jobs SET lease_until=0 WHERE id=?', (task['id'],))
    fresh = consolidate()
    assert fresh['id'] == task['id'] and fresh['input']['candidates'][0]['identity']['exact'] == [late]


def test_promoted_endpoint_reference_follows_a_reviewed_merge(world):
    store, graph, processing, capture, extract, consolidate, node, merge = world
    episode = capture()
    absorbed, survivor = node('project', 'Orion', episode, key='a'), node('project', 'orion', episode, key='b')
    vega = node('organization', 'Vega', episode)
    origin = extract([draft('left', 'entity', 'project', 'Orion'), draft('right', 'entity', 'organization', 'Vega'),
                      draft('relation', 'relation', 'owned_by', 'Orion is owned by Vega', 'left', 'right')])
    relation = store._db.execute("SELECT id,src,dst FROM candidates WHERE kind='relation'").fetchone()

    def promotions(commit):
        for index, (candidate, target) in enumerate(((relation['src'], absorbed), (relation['dst'], vega))):
            store._db.execute('INSERT INTO candidate_decisions VALUES (?,?,?,?,?,?)',
                (f'prior-{index}', candidate, commit, 'promote', target, None))
        return {}
    graph._command('prior-promotions', 'fixture', 'Earlier promoted endpoints', dict(op='fixture'), promotions)
    merge(absorbed, survivor, 'endpoint')[1]()
    task = consolidate()
    context = task['input']
    assert [item['id'] for item in context['candidates']] == [relation['id']]
    assert context['resolved_candidates'] == {relation['src']: survivor, relation['dst']: vega}
    assert survivor in {item['id'] for item in context['existing_nodes']}
    result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[
        action(context['candidates'][0], origin, 'link', src=relation['src'], dst=relation['dst'], relation='owned_by')]))
    edge = graph.history(result['results'][relation['id']]['id'])[-1]
    assert (edge['src'], edge['rel'], edge['dst']) == (survivor, 'owned_by', vega)


@pytest.mark.parametrize('reference', ['direct', 'candidate_alias'])
@pytest.mark.parametrize('kind', ['correction', 'refinement', 'change'])
def test_same_batch_supersession_dependents_follow_only_a_continuing_identity(world, kind, reference):
    store, graph, processing, capture, extract, consolidate, node, _ = world
    episode = capture()
    orion, vega = node('project', 'Orion', episode), node('organization', 'Vega', episode)
    origin = extract([draft('left', 'entity', 'project', 'Orion'), draft('right', 'entity', 'organization', 'Vega'),
                      draft('relation', 'relation', 'owned_by', 'Orion is owned by Vega', 'left', 'right'),
                      draft('fix', 'entity', 'project', 'Orion warehouse')])
    relation = store._db.execute("SELECT id,src,dst FROM candidates WHERE kind='relation'").fetchone()

    def promotions(commit):
        for index, (candidate, target) in enumerate(((relation['src'], orion), (relation['dst'], vega))):
            store._db.execute('INSERT INTO candidate_decisions VALUES (?,?,?,?,?,?)',
                (f'prior-{index}', candidate, commit, 'promote', target, None))
        return {}
    graph._command('prior-promotions', 'fixture', 'Earlier promoted endpoints', dict(op='fixture'), promotions)
    task = consolidate()
    relation_candidate, fix = task['input']['candidates']
    assert task['input']['resolved_candidates'] == {relation['src']: orion, relation['dst']: vega}
    result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[
        action(fix, origin, 'supersede', target=orion, type='project', text='Orion warehouse', scope=['professional'],
               change_kind=kind, valid_at='2026-01-01T00:00:00+00:00' if kind == 'change' else None),
        action(relation_candidate, origin, 'link', relation='owned_by', dst=relation['dst'],
               src=orion if reference == 'direct' else relation['src'])]))
    replacement = result['results'][fix['id']]['superseded_by']
    assert result['results'][fix['id']]['decision'] == 'promote'
    relations = store._db.execute("SELECT src,dst FROM edge_versions WHERE rel='owned_by'").fetchall()
    if kind in ('correction', 'change'):
        # Mistaken identity and world changes cannot silently inherit a relation.
        assert result['results'][relation_candidate['id']]['decision'] == 'defer' and relations == []
    else:
        assert result['results'][relation_candidate['id']]['decision'] == 'promote'
        assert [tuple(row) for row in relations] == [(replacement, vega)]


def test_ordinary_people_and_relationships_consolidate_without_review(world):
    store, graph, processing, capture, extract, consolidate, node, _ = world
    vega = node('organization', 'Vega', capture())
    origin = extract([draft('sergey', 'entity', 'person', 'Sergey', scope=('personal',)),
                      draft('vega', 'entity', 'organization', 'Vega'),
                      draft('works', 'relation', 'works_in', 'Sergey works in Vega', 'sergey', 'vega')])
    task = consolidate()
    person, organization, works = task['input']['candidates']
    result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[
        action(person, origin, 'remember', type='person', text='Sergey', scope=['personal']),
        action(organization, origin, 'duplicate', target=vega),
        action(works, origin, 'link', src=person['id'], dst=organization['id'], relation='works_in')]))
    assert {item['decision'] for item in result['results'].values()} == {'promote'}
    sergey = result['results'][person['id']]['id']
    edge = graph.history(result['results'][works['id']]['id'])[-1]
    assert (edge['src'], edge['rel'], edge['dst']) == (sergey, 'works_in', vega)
    again = extract([draft('again', 'entity', 'person', ' SERGEY ', scope=('personal',))], day=2)
    task = consolidate()
    candidate = task['input']['candidates'][0]
    assert candidate['identity']['exact'] == [sergey] and candidate['identity']['exact_total'] == 1
    result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[
        action(candidate, again, 'duplicate', target=sergey)]))
    assert result['results'][candidate['id']] == dict(id=sergey, version=2, decision='promote')
    assert active(store, 'person', 'Sergey') == [sergey] and graph.pending_reviews() == []


@pytest.mark.parametrize('rejected_before_rederivation', [False, True])
def test_legacy_person_review_is_derived_again_and_cannot_create_a_second_identity(
        world, monkeypatch, rejected_before_rederivation):
    store, graph, processing, _, extract, consolidate, _, _ = world
    origin = extract([draft('sergey', 'entity', 'person', 'Sergey', scope=('personal',))])
    task = consolidate()
    candidate = task['input']['candidates'][0]
    gate = graph._sensitive_type
    with monkeypatch.context() as legacy:
        # Before the owner's policy choice every built-in person memory was human-gated.
        legacy.setattr(graph, '_sensitive_type', lambda name: name == 'person' or gate(name))
        parked = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[
            action(candidate, origin, 'remember', type='person', text='Sergey', scope=['personal'])]))
    review = parked['results'][candidate['id']]['id']
    assert parked['results'][candidate['id']]['decision'] == 'review'
    assert [(item['id'], item['stale']) for item in graph.pending_reviews()] == [(review, True)]
    assert candidate['id'] in {item['id'] for item in processing.consolidator.pending()}
    with pytest.raises(ConflictError, match='stale'):
        graph._decide_review(command_id='late-approval', actor='human-fixture', why='Late approval',
                             proposal_id=review, decision='approve')
    reject = dict(command_id='legacy-no', actor='human-fixture', why='Obsolete person gate',
                  proposal_id=review, decision='reject')
    if rejected_before_rederivation:
        graph._decide_review(**reject)
        assert candidate['id'] in {item['id'] for item in processing.consolidator.pending()}
    fresh = consolidate()
    again = fresh['input']['candidates'][0]
    assert again['id'] == candidate['id'] and again['identity'] == dict(exact=[], exact_total=0, similar=[])
    promoted = processing.complete(id=fresh['id'], owner=fresh['owner'], agent='fixture', output=dict(actions=[
        action(again, origin, 'remember', type='person', text='Sergey', scope=['personal'])]))
    person = promoted['results'][candidate['id']]
    assert person['decision'] == 'promote'
    if not rejected_before_rederivation:
        with pytest.raises(ConflictError, match='stale'):
            graph._decide_review(command_id='later-approval', actor='human-fixture', why='Late approval',
                                 proposal_id=review, decision='approve')
        graph._decide_review(**reject)
    latest = store._db.execute('''SELECT decision,target_id FROM candidate_decisions WHERE candidate_id=?
        ORDER BY rowid DESC LIMIT 1''', (candidate['id'],)).fetchone()
    assert tuple(latest) == ('promote', person['id'])
    assert active(store, 'person', 'Sergey') == [person['id']] and graph.pending_reviews() == []


def test_merge_unions_survivor_evidence_scope_recency_and_folds_duplicate_relations(world):
    store, graph, _, capture, _, _, node, merge = world
    old, new = capture(day=1), capture(day=9)
    absorbed = node('project', 'Orion', new, scope=('personal',), key='absorbed')
    survivor = node('project', 'Orion', old, key='survivor')
    vega = node('organization', 'Vega', old)
    links = [graph.link(command_id=f'link-{identity}', actor='fixture', why='Explicit synthetic ownership',
                        a=identity, rel='owned_by', b=vega, evidence=[dict(episode_id=episode['id'], quote=TEXT)])['id']
             for identity, episode in ((absorbed, new), (survivor, old))]
    # An automatic marker folds with its relation; a manual choice the fold would
    # lose keeps its own relation instead (tests/test_retire.py).
    from memento.loops import _append
    graph._command('absorbed-relation-loop', 'fixture/extractor', 'Synthetic automatic marker', dict(op='fixture'),
                   lambda commit: _append(graph, commit, links[0], 'open', None, None) and {})
    def stable(rows):
        return [(row['version'], row['state'], row['scope'], row['evidence']) for row in rows]
    before = stable(graph.history(absorbed))
    result = merge(absorbed, survivor, 'union')[1]()
    assert result['merged_into'] == survivor and result['scope'] == ['personal', 'professional']
    current = graph.history(survivor)[-1]
    assert current['state'] == 'active' and current['scope'] == ['personal', 'professional']
    assert {item['episode_id'] for item in current['evidence']} == {old['id'], new['id']}
    assert query_recency(store, dict(id=survivor, version=current['version']))['source_occurred_at'] == new['occurred_at']
    assert [item['id'] for item in graph.recall_keywords('Orion')] == [survivor]
    assert stable(graph.history(absorbed))[:len(before)] == before and graph.history(absorbed)[-1]['state'] == 'merged'
    edges = store._db.execute('''SELECT e.id FROM edge_versions e WHERE e.src=? AND e.rel='owned_by' AND e.dst=?
        AND e.state='active' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id)''',
        (survivor, vega)).fetchall()
    assert [row['id'] for row in edges] == [links[1]]
    folded = graph.history(links[1])[-1]
    assert {item['episode_id'] for item in folded['evidence']} == {old['id'], new['id']}
    assert graph.history(links[0])[-1]['state'] == 'inactive'
    assert [item['id'] for item in graph.loop(action='list')['loops']] == [links[1]]


@pytest.mark.parametrize('changed', ['absorbed', 'survivor'])
def test_merge_approval_keeps_both_original_version_checks(world, changed):
    from memento.curation import attach
    store, graph, _, capture, _, _, node, merge = world
    episode = capture()
    nodes = dict(absorbed=node('project', 'Orion', episode, key='a'), survivor=node('project', 'orion', episode, key='b'))
    _, approve = merge(nodes['absorbed'], nodes['survivor'], 'fenced')
    later = capture()
    graph._command('concurrent-evidence', 'fixture', 'Concurrent corroboration', dict(op='fixture'),
        lambda commit: attach(graph, commit, nodes[changed], [dict(episode_id=later['id'], quote=TEXT)]))
    with pytest.raises(ConflictError, match='stale'):
        approve()
    assert store._db.execute('SELECT count(*) FROM entity_merges').fetchone()[0] == 0


def test_merge_fences_leased_context_and_requeues_stale_review_candidates(world):
    store, graph, processing, capture, extract, consolidate, node, merge = world
    episode = capture()
    absorbed, survivor = node('project', 'Orion', episode, key='a'), node('project', 'orion', episode, key='b')
    pin = dict(op='pin', node_id=survivor, pinned=True, evidence=[dict(episode_id=episode['id'], quote=TEXT)])
    proposal = graph._command('pin', 'fixture', 'Pin survivor', pin, lambda commit: propose(graph, commit, pin))
    graph._decide_review(command_id='pin-yes', actor='human-fixture', why='Pin', proposal_id=proposal['id'],
                         decision='approve')
    origin = extract([draft('corroborate', 'entity', 'project', 'Orion'), draft('same', 'entity', 'project', 'ORION')])
    task = consolidate()
    first, second = task['input']['candidates']
    assert set(first['identity']['exact']) == {absorbed, survivor}
    result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[
        action(first, origin, 'duplicate', target=survivor),
        action(second, origin, 'merge', src=absorbed, dst=survivor)]))
    assert {item['decision'] for item in result['results'].values()} == {'review'}
    attach_review = result['results'][first['id']]['id']
    merge_review = result['results'][second['id']]['id']
    # A different leased plan that saw both identities must be rederived after the merge.
    leased = extract([draft('leased', 'entity', 'project', 'Orion')], day=2)
    task = consolidate()
    graph._decide_review(command_id='merge-yes', actor='human-fixture', why='Same project',
                         proposal_id=merge_review, decision='approve')
    with pytest.raises(ConflictError, match='graph context changed'):
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[
            action(task['input']['candidates'][0], leased, 'duplicate', target=survivor)]))
    decisions = dict(store._db.execute('''SELECT d.candidate_id,d.decision FROM candidate_decisions d
        WHERE d.rowid=(SELECT max(x.rowid) FROM candidate_decisions x WHERE x.candidate_id=d.candidate_id)'''))
    assert decisions[second['id']] == 'promote' and decisions[first['id']] == 'defer'
    stale = {item['id']: item['stale'] for item in graph.pending_reviews()}
    assert stale == {attach_review: True}
    with pytest.raises(ConflictError, match='stale'):
        graph._decide_review(command_id='attach-yes', actor='human-fixture', why='Late approval',
                             proposal_id=attach_review, decision='approve')
    graph._decide_review(command_id='attach-no', actor='human-fixture', why='Superseded by merge',
                         proposal_id=attach_review, decision='reject')
    assert first['id'] in {item['id'] for item in processing.consolidator.pending()}


def test_owner_inventory_and_exact_plan_merge_a_selected_cluster_only(world):
    store, graph, _, capture, _, _, node, _ = world
    episode = capture()
    members = [node('project', text, episode, scope=scope, key=f'm{index}') for index, (text, scope) in enumerate(
        (('Orion', ('professional',)), ('  orion ', ('personal',)), ('ORION', ('professional',)), ('Orion', ('professional',))))]
    node('project', 'Project Orion', episode)
    node('tool', 'C', episode)
    node('tool', 'C++', episode)
    clusters = graph._merge_inventory()['clusters']
    assert [(item['type'], item['name'], len(item['members'])) for item in clusters] == [('project', 'orion', 4)]
    cluster = clusters[0]['cluster']
    distinct = members[3]
    with pytest.raises(ValueError, match='needs a survivor'):
        graph._merge_preview(cluster, exclude=members)
    plan = graph._merge_preview(cluster, into=members[0], exclude=[distinct])
    assert [item['id'] for item in plan['absorbed']] == members[1:3]
    assert plan['resulting_scope'] == ['personal', 'professional']
    assert [item['id'] for item in plan['excluded']] == [distinct]
    assert plan['survivor']['sources'] == [dict(episode_id=episode['id'], source='identity',
        conversation_id=episode['conversation_id'], occurred_at=episode['occurred_at'], quote_excerpt=TEXT)]
    assert plan['survivor']['loop']['event_id'] is None
    commands = store._db.execute('SELECT count(*) FROM graph_commits').fetchone()[0]
    with pytest.raises(ConflictError, match='stale'):
        graph._merge(command_id='wrong', actor='human-fixture', why='Owner repair', cluster=cluster,
                     into=members[0], exclude=[distinct], token='0' * 64)
    assert store._db.execute('SELECT count(*) FROM graph_commits').fetchone()[0] == commands
    result = graph._merge(command_id='repair', actor='human-fixture', why='Owner repair', cluster=cluster,
                          into=members[0], exclude=[distinct], token=plan['token'])
    assert result['merged'] == members[1:3] and result['scope'] == ['personal', 'professional']
    assert [graph._node(item)['state'] for item in members] == ['active', 'merged', 'merged', 'active']
    assert graph._node(members[0])['version'] == 3  # two sequential reviewed merges in one exact plan
    remaining = graph._merge_inventory()['clusters']
    assert [{item['id'] for item in group['members']} for group in remaining] == [{members[0], distinct}]
    with pytest.raises(ConflictError, match='stale'):
        graph._merge(command_id='replay', actor='human-fixture', why='Owner repair', cluster=cluster,
                     into=members[0], exclude=[], token=plan['token'])


def test_owner_plan_includes_only_chosen_active_same_type_variants(world):
    from memento.curation import attach
    store, graph, _, capture, _, _, node, merge = world
    episode = capture()
    members = [node('project', 'Orion', episode, key='m0'), node('project', 'orion', episode, scope=('personal',), key='m1')]
    variant = node('project', 'Orion warehouse', episode, key='variant')
    tool = node('tool', 'Orion warehouse', episode, key='tool')
    gone = node('project', 'Vega', episode, key='gone')
    merge(gone, node('project', 'vega', episode, key='keep'), 'retired')[1]()
    clusters = graph._merge_inventory()['clusters']
    assert [item['name'] for item in clusters] == ['orion']  # never grouped automatically
    cluster = clusters[0]['cluster']
    default = graph._merge_preview(cluster)
    assert 'included' not in default and variant not in {item['id'] for item in default['absorbed']}
    for include, exclude, into, message in (
            (['missing'], [], None, 'unknown'), ([tool], [], None, 'cluster type'), ([gone], [], None, 'cluster type'),
            ([variant], [variant], None, 'both included and excluded'),
            ([members[1]], [members[1]], None, 'both included and excluded'),
            ([], [variant], None, 'Exclusions must be members'), ([], [], variant, 'Survivor must belong')):
        with pytest.raises(ValueError, match=message):
            graph._merge_preview(cluster, into=into, exclude=exclude, include=include)
    assert graph._merge_preview(cluster, into=variant, include=[variant])['survivor']['id'] == variant
    plan = graph._merge_preview(cluster, into=members[0], include=[variant, members[0], variant])
    assert plan['included'] == [variant] and plan['resulting_scope'] == ['personal', 'professional']
    assert {item['id'] for item in plan['absorbed']} == {members[1], variant}
    repair = dict(actor='human-fixture', why='Owner repair', cluster=cluster, into=members[0], exclude=[])
    with pytest.raises(ConflictError, match='stale'):  # apply must repeat the previewed selection
        graph._merge(command_id='without-include', token=plan['token'], **repair)
    later = capture(day=4)
    graph._command('variant-evidence', 'fixture', 'Concurrent corroboration', dict(op='fixture'),
        lambda commit: attach(graph, commit, variant, [dict(episode_id=later['id'], quote=TEXT)]))
    with pytest.raises(ConflictError, match='stale'):
        graph._merge(command_id='stale-variant', token=plan['token'], include=[variant], **repair)
    assert graph._node(variant)['state'] == 'active'
    plan = graph._merge_preview(cluster, into=members[0], include=[variant])
    result = graph._merge(command_id='with-variant', token=plan['token'], include=[variant], **repair)
    assert set(result['merged']) == {members[1], variant}
    assert graph._node(variant)['state'] == 'merged'
    assert later['id'] in {item['episode_id'] for item in graph.history(members[0])[-1]['evidence']}
    assert sorted(item['id'] for item in graph.recall_keywords('warehouse')) == sorted([members[0], tool])
    atlas = many(graph, episode, 'project', ['Atlas'] * 33)
    extra = node('project', 'Atlas prime', episode, key='atlas-prime')
    atlas_cluster = next(item['cluster'] for item in graph._merge_inventory()['clusters'] if item['name'] == 'atlas')
    assert len(graph._merge_preview(atlas_cluster, into=atlas[0])['absorbed']) == 32
    with pytest.raises(ValueError, match='at most 32'):
        graph._merge_preview(atlas_cluster, into=atlas[0], include=[extra])


def test_manual_loop_choice_after_preview_rejects_the_plan(world):
    store, graph, _, capture, _, _, node, _ = world
    episode = capture()
    survivor, absorbed = node('project', 'Orion', episode, key='s'), node('project', 'orion', episode, key='a')
    graph.loop(action='open', id=absorbed, command_id='pending-obligation')
    cluster = graph._merge_inventory()['clusters'][0]['cluster']
    plan = graph._merge_preview(cluster, into=survivor)
    assert plan['absorbed'][0]['loop']['state'] == 'open'
    graph.loop(action='later', id=absorbed, command_id='owner-deferral')
    with pytest.raises(ConflictError, match='stale'):
        graph._merge(command_id='late', actor='human-fixture', why='Owner repair', cluster=cluster,
                     into=survivor, exclude=[], token=plan['token'])
    assert graph._node(absorbed)['state'] == 'active'


def test_vector_only_same_type_match_gets_a_reserved_check(world):
    import struct
    store, graph, processing, capture, extract, consolidate, node, _ = world
    episode = capture()
    related = node('knowledge', 'Freight cap rule', episode)
    origin = extract([draft('paraphrase', 'memory', 'knowledge', 'Shipping limit policy')])
    candidate = store._db.execute('SELECT id FROM candidates').fetchone()[0]
    vector = struct.pack('768f', 1, *([0] * 767))
    for target, kind in ((related, 'node'), (candidate, 'candidate')):
        seq = store._db.execute('INSERT INTO vector_items(target,kind,version) VALUES (?,?,1)', (target, kind)).lastrowid
        store._db.execute('INSERT INTO memory_vectors(rowid,embedding,kind) VALUES (?,?,?)', (seq, vector, kind))
    context = consolidate()['input']
    assert context['candidates'][0]['identity'] == dict(exact=[], exact_total=0, similar=[related])
    assert origin['id'] in {item['episode_id'] for item in context['raw_windows']}


@pytest.mark.parametrize('confirmed', [True, False])
def test_merge_cli_previews_and_requires_exact_human_confirmation(tmp_path, monkeypatch, capsys, confirmed):
    plan = dict(survivor=dict(id='survivor'), absorbed=[dict(id='a'), dict(id='b')], token='t' * 64)
    requests = []

    def request(path, value):
        requests.append(value)
        return dict(result=plan) if value['operation'] == 'merge_preview' else dict(result=dict(merged=['a', 'b']))
    monkeypatch.setattr(sys, 'argv', ['memento-admin', '--data-dir', str(tmp_path), 'merge', 'cluster1',
                                      '--exclude', 'distinct', '--include', 'v1', '--include', 'v2',
                                      '--why', 'Owner repair'])
    monkeypatch.setattr(sys.stdin, 'isatty', lambda: True)
    monkeypatch.setattr('builtins.input', lambda prompt: 'merge 2 into survivor' if confirmed else 'yes')
    monkeypatch.setattr(admin, '_request', request)
    if confirmed:
        admin.main()
    else:
        with pytest.raises(SystemExit):
            admin.main()
    assert requests[0] == dict(operation='merge_preview', cluster='cluster1', into=None, exclude=['distinct'],
                               include=['v1', 'v2'])
    if confirmed:
        assert {key: requests[1][key] for key in ('operation', 'cluster', 'into', 'exclude', 'include', 'token')} == dict(
            operation='merge', cluster='cluster1', into='survivor', exclude=['distinct'], include=['v1', 'v2'],
            token='t' * 64)
    else:
        assert len(requests) == 1
    capsys.readouterr()
