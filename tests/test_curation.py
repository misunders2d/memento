import struct

import pytest

from memento.curation import propose
from memento.graph import Graph
from memento.store import ConflictError, Store


@pytest.fixture
def curated(tmp_path):
    store = Store(tmp_path)
    graph = Graph(store)
    text = 'Vega Incorporated and Vega are the same organization. Orion belongs to Vega. Preserve this decision.'
    episode, _ = store.capture(dict(source='test', event_id='curation', conversation_id='c', kind='user',
                                    text=text, occurred_at=None, metadata={}))
    evidence = [dict(episode_id=episode['id'], quote=text)]
    def remember(key, type, text):
        return graph.remember(command_id=key, actor='fixture', why='explicit synthetic assertion',
                              type=type, text=text, scope=['professional'], evidence=evidence)
    def review(request, key):
        request = dict(request, evidence=evidence)
        return graph._command(key, 'model-fixture', 'propose guarded operation', request,
                              lambda commit: propose(graph, commit, request))
    def approve(proposal, key):
        return graph._decide_review(command_id=key, actor='human-fixture', why='explicit synthetic approval',
                                    proposal_id=proposal['id'], decision='approve')
    try:
        yield store, graph, evidence, remember, review, approve
    finally:
        store.close()


def test_pin_requires_review_preserves_evidence_and_reuses_unchanged_vector(curated):
    store, graph, evidence, remember, review, approve = curated
    node = remember('memory', 'knowledge', 'Preserve this decision.')
    seq = store._db.execute('INSERT INTO vector_items(target,kind,version) VALUES (?,\'node\',1)', (node['id'],)).lastrowid
    vector = struct.pack('768f', 1, *([0] * 767))
    store._db.execute('INSERT INTO memory_vectors(rowid,embedding,current) VALUES (?,?,1)', (seq, vector))
    proposal = review(dict(op='pin', node_id=node['id'], pinned=True), 'pin-request')
    assert graph.history(node['id'])[-1]['pinned'] == 0
    approve(proposal, 'pin-approved')
    history = graph.history(node['id'])
    assert [row['pinned'] for row in history] == [0, 1]
    assert history[0]['evidence'] == history[1]['evidence']
    indexed = store._db.execute('''SELECT i.version,v.current,v.embedding FROM vector_items i
        JOIN memory_vectors v ON v.rowid=i.seq WHERE i.target=? ORDER BY i.version''', (node['id'],)).fetchall()
    # The unchanged text's single derived vector row moves to the new version.
    assert [(row['version'], row['current']) for row in indexed] == [(2, 1)]
    assert indexed[-1]['embedding'] == vector
    correction = graph.supersede(command_id='correct', actor='fixture', why='explicit corrected decision',
        old=node['id'], new=dict(type='knowledge', text='Preserve the corrected decision.',
                               scope=['professional']), evidence=evidence)
    assert correction['status'] == 'pending_review'
    result = approve(correction, 'correction-approved')
    assert graph.history(result['superseded_by'])[-1]['pinned'] == 1


def test_sensitive_link_approval_is_fenced_by_endpoint_version(curated):
    store, graph, evidence, remember, review, approve = curated
    a = remember('a', 'knowledge', 'Preserve this decision.')
    b = remember('b', 'project', 'Orion')
    approve(review(dict(op='pin', node_id=a['id'], pinned=True), 'pin'), 'pin-yes')
    pending = graph.link(command_id='link', actor='fixture', why='connect guarded memory',
                         a=a['id'], rel='part_of', b=b['id'], evidence=evidence)
    assert pending['status'] == 'pending_review'
    assert store._db.execute('SELECT count(*) FROM edge_versions').fetchone()[0] == 0
    approve(review(dict(op='pin', node_id=a['id'], pinned=False), 'unpin'), 'unpin-yes')
    with pytest.raises(ConflictError):
        approve(pending, 'stale-link')
    assert store._db.execute('SELECT count(*) FROM edge_versions').fetchone()[0] == 0


def test_human_merge_rewrites_edges_preserves_pins_alias_lookup_and_history(curated):
    store, graph, evidence, remember, review, approve = curated
    a = remember('a', 'organization', 'Vega Incorporated')
    b = remember('b', 'organization', 'Vega')
    project = remember('project', 'project', 'Orion')
    edge = graph.link(command_id='ownership', actor='fixture', why='explicit organization ownership',
                       a=project['id'], rel='owned_by', b=a['id'], evidence=evidence)
    approve(review(dict(op='pin', node_id=a['id'], pinned=True), 'pin'), 'pin-yes')
    pending = review(dict(op='merge', a=a['id'], b=b['id']), 'merge')
    assert graph.history(a['id'])[-1]['state'] == 'active'
    result = approve(pending, 'merge-yes')
    assert result['merged_into'] == b['id']
    assert graph.history(a['id'])[-1]['state'] == 'merged'
    assert graph.history(b['id'])[-1]['pinned'] == 1
    history = graph.history(edge['id'])
    assert [row['dst'] for row in history] == [a['id'], b['id']]
    assert history[0]['evidence'] == history[1]['evidence']
    assert [row['id'] for row in graph.recall_keywords('Incorporated')] == [b['id']]


def test_new_types_do_not_become_active_without_human_approval(curated):
    store, graph, evidence, remember, review, approve = curated
    pending = remember('new-type', 'goal', 'Preserve this decision.')
    assert pending['status'] == 'pending_review'
    assert store._db.execute("SELECT 1 FROM memory_types WHERE name='goal'").fetchone() is None
    node = approve(pending, 'new-type-yes')
    assert graph.history(node['id'])[-1]['type'] == 'goal'
    assert remember('later-goal', 'goal', 'A second goal')['status'] == 'pending_review'
    definition = review(dict(op='register_type', name='event', category='entity', sensitive=False), 'define-event')
    assert approve(definition, 'define-event-yes') == dict(type='event', status='registered')
    event = remember('event', 'event', 'Orion launch')
    assert graph.history(event['id'])[-1]['state'] == 'active'


def test_sensitive_custom_type_still_requires_review_for_memories_and_links(curated):
    store, graph, evidence, remember, review, approve = curated
    definition = review(dict(op='register_type', name='diagnosis', category='memory', sensitive=True), 'define')
    approve(definition, 'define-yes')
    pending = remember('diagnosis', 'diagnosis', 'Preserve this decision.')
    assert pending['status'] == 'pending_review'
    node = approve(pending, 'diagnosis-yes')
    person = remember('person', 'person', 'Vega')
    assert graph.history(person['id'])[-1]['state'] == 'active'
    link = graph.link(command_id='link', actor='fixture', why='connect sensitive memory',
                      a=node['id'], rel='related_to', b=person['id'], evidence=evidence)
    assert link['status'] == 'pending_review'
    assert store._db.execute('SELECT count(*) FROM edge_versions').fetchone()[0] == 0


def test_pin_and_old_memories_remain_searchable_without_changing_evidence(curated):
    from datetime import datetime, timezone
    from memento.importance import refresh

    store, graph, evidence, remember, review, approve = curated
    pinned = remember('pinned', 'knowledge', 'Preserve this pinned decision.')
    ordinary = remember('ordinary', 'knowledge', 'Preserve this ordinary decision.')
    approve(review(dict(op='pin', node_id=pinned['id'], pinned=True), 'pin-old'), 'pin-old-yes')
    history = graph.history(pinned['id'])
    refresh(store, datetime(2100, 1, 1, tzinfo=timezone.utc))
    assert {hit['id'] for hit in graph.recall_keywords('Preserve', include_archived=False)} == {pinned['id'], ordinary['id']}
    assert store._db.execute('SELECT sum(archived) FROM node_stats').fetchone()[0] == 0
    assert {hit['id'] for hit in graph.recall_keywords('Preserve')} == {pinned['id'], ordinary['id']}
    assert graph.history(pinned['id']) == history
    assert history[-1]['evidence'] == history[0]['evidence']


@pytest.mark.parametrize('target_has_choice', [False, True])
def test_merge_transfers_loop_only_when_target_has_no_existing_choice(curated, target_has_choice):
    store, graph, evidence, remember, review, approve = curated
    source = remember('source', 'organization', 'Vega Incorporated')['id']
    target = remember('target', 'organization', 'Vega')['id']
    graph.loop(action='remind', id=source, due_at='2030-10-20T12:00:00Z',
               command_id='source-reminder')
    graph.loop(action='later', id=source, command_id='source-deferral')
    if target_has_choice:
        graph.loop(action='close', id=target, command_id='target-closed')
    approve(review(dict(op='merge', a=source, b=target), 'merge-loop'), 'merge-loop-approved')
    assert graph.loop(action='list')['loops'] == []
    state = graph.loop(action='open', id=target, command_id='resume-merged-loop')
    assert state['due_at'] == (None if target_has_choice else '2030-10-20T12:00:00+00:00')
    assert [row['id'] for row in graph.loop(action='list')['loops']] == [target]
