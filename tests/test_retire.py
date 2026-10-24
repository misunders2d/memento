"""Derived retirement of proven noise: append-only, no source suppression, protected choices untouched."""

from types import SimpleNamespace

import pytest

from memento.curation import pin, retire, retire_check
from memento.graph import Graph
from memento.loops import _append, current
from memento.store import ConflictError, Store

np = pytest.importorskip('numpy')

TEXT = '\n'.join((
    'Recall returned scores 0.81, 0.77 and 0.64 for herdr.', 'Herdr is our terminal multiplexer.',
    'Temporary: run the smoke test once before lunch.', 'Always queue replies when a pane is busy.',
))
VECTOR = np.eye(1, 768, dtype=np.float32)[0]
EXPLICIT = dict(applicability='When a Herdr pane is busy', conduct='Queue the reply instead of interrupting.',
                rationale='Interrupting loses the active turn.', exceptions=[], authority='explicit_user')


@pytest.fixture
def world(tmp_path, monkeypatch):
    from memento.retrieval import Retrieval
    store = Store(tmp_path)
    graph = Graph(store)
    episode, _ = store.capture(dict(source='retire', event_id='1', conversation_id='c', kind='user',
                                    text=TEXT, occurred_at='2026-10-24T10:00:00+00:00', metadata={}))
    monkeypatch.setattr('memento.retrieval.embed_query', lambda model, text: VECTOR)
    retrieval = Retrieval(graph, model=SimpleNamespace(tokenizer=lambda *args, **kwargs: {}))

    def ev(quote):
        return [dict(episode_id=episode['id'], quote=quote)]

    def remember(key, text, quote, type='knowledge'):
        node = graph.remember(command_id=key, actor='fixture', why='synthetic assertion', type=type, text=text,
                              scope=['professional'], evidence=ev(quote))['id']
        retrieval._put(node, 'node', 1, VECTOR)
        return node

    def run(key, apply, request=None):
        return graph._command(key, 'fixture', 'synthetic maintenance', request or dict(op='fixture', key=key), apply)

    def rows():
        return [store._db.execute(f'SELECT count(*) FROM {table}').fetchone()[0] for table in
                ('node_versions', 'edge_versions', 'graph_evidence', 'loop_events', 'episodes')]

    try:
        yield SimpleNamespace(store=store, graph=graph, retrieval=retrieval, episode=episode, ev=ev,
                              remember=remember, run=run, rows=rows)
    finally:
        retrieval.close()
        store.close()


def automatic(world):
    return {item['id'] for item in world.retrieval.recall('herdr recall scores', automatic=True)}


def test_retired_noise_leaves_active_memory_but_keeps_history_and_sources(world):
    graph, store, ev = world.graph, world.store, world.ev
    quote = 'Recall returned scores 0.81, 0.77 and 0.64 for herdr.'
    noise = world.remember('noise', 'Recall returned scores 0.81, 0.77 and 0.64 for herdr.', quote)
    fact = world.remember('fact', 'Herdr is our terminal multiplexer.', 'Herdr is our terminal multiplexer.')
    herdr = graph.remember(command_id='herdr', actor='fixture', why='entity', type='tool', text='Herdr',
                           scope=['professional'], evidence=ev('Herdr is our terminal multiplexer.'))['id']
    noisy_edge = graph.link(command_id='l1', actor='fixture', why='mention', a=noise, rel='mentions', b=herdr,
                            evidence=ev(quote))['id']
    graph.link(command_id='l2', actor='fixture', why='mention', a=fact, rel='mentions', b=herdr,
               evidence=ev('Herdr is our terminal multiplexer.'))
    world.run('marker', lambda commit: _append(graph, commit, noise, 'open', None, None))
    world.run('edge-marker', lambda commit: _append(graph, commit, noisy_edge, 'open', None, None))
    assert {noise, fact} <= automatic(world)
    before = graph.history(noise)[-1]
    episodes = store._db.execute('SELECT count(*) FROM episodes').fetchone()[0]

    result = world.run('retire', lambda commit: retire(graph, commit, noise, ev('scores 0.81, 0.77')))
    assert result == dict(id=noise, version=2, status='retired', relations=[dict(id=noisy_edge, version=2)],
                          declined_loops=[noise, noisy_edge])
    after = graph.history(noise)[-1]
    assert after['state'] == 'retired' and after['superseded_by'] is None
    assert {key: after[key] for key in ('text', 'scope', 'valid_from', 'valid_to', 'pinned', 'evidence')} == {
        key: before[key] for key in ('text', 'scope', 'valid_from', 'valid_to', 'pinned', 'evidence')}
    assert noise not in automatic(world) and fact in automatic(world)
    assert noise not in {item['id'] for item in world.retrieval.recall('scores', effort='cheap')}
    # Vector and search rows stay for history; only their recall eligibility ends.
    assert store._db.execute('''SELECT v.current FROM vector_items i JOIN memory_vectors v ON v.rowid=i.seq
        WHERE i.target=? AND i.kind='node' ''', (noise,)).fetchall()[0][0] == 0
    assert store._db.execute('SELECT version FROM node_fts WHERE id=?', (noise,)).fetchone()[0] == 2
    # The automatic markers close as never-obligations: no completion source time is invented.
    assert current(graph, noise)['state'] == current(graph, noisy_edge)['state'] == 'closed'
    assert 'loop_closures' not in result
    # Raw history, the entity, its alias and the unrelated relation survive; nothing is suppressed.
    assert store._db.execute('SELECT count(*) FROM episodes').fetchone()[0] == episodes
    assert store._db.execute('SELECT count(*) FROM forgotten_sources').fetchone()[0] == 0
    assert store._db.execute('SELECT count(*) FROM forgotten_targets').fetchone()[0] == 0
    assert graph._node(herdr)['state'] == 'active' and graph._node(fact)['state'] == 'active'
    assert [row['state'] for row in store._db.execute('''SELECT e.state FROM edge_versions e
        WHERE e.dst=? AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id) ORDER BY e.src=?''',
        (herdr, noise))] == ['active', 'inactive']
    reused = graph.remember(command_id='reuse', actor='fixture', why='same source still supports memory',
                            type='knowledge', text='Recall bookkeeping once listed herdr scores.',
                            scope=['professional'], evidence=ev(quote))
    assert graph._node(reused['id'])['state'] == 'active'
    # A replayed command returns its stored result without another version.
    rows = world.rows()
    assert world.run('retire', lambda commit: retire(graph, commit, noise, ev('scores 0.81, 0.77'))) == result
    assert world.rows() == rows


def test_retire_check_uses_fixed_codes(world):
    graph, ev = world.graph, world.ev
    noise = world.remember('noise', 'Recall returned scores.', 'Recall returned scores 0.81, 0.77 and 0.64 for herdr.')
    tool = graph.remember(command_id='herdr', actor='fixture', why='entity', type='tool', text='Herdr',
                          scope=['professional'], evidence=ev('Herdr is our terminal multiplexer.'))['id']
    own = ev('Recall returned scores 0.81, 0.77 and 0.64 for herdr.')
    assert retire_check(graph, noise, own) is None
    assert retire_check(graph, 'missing', own) == 'consolidation_retire_target_invalid'
    assert retire_check(graph, tool, ev('Herdr is our terminal multiplexer.')) == 'consolidation_retire_type_invalid'
    for evidence in ([], ev('Herdr is our terminal multiplexer.'), [dict(episode_id='other', quote='Recall')]):
        assert retire_check(graph, noise, evidence) == 'consolidation_retire_evidence_invalid'
    rows = world.rows()
    with pytest.raises(ConflictError, match='consolidation_retire_type_invalid'):
        world.run('entity', lambda commit: retire(graph, commit, tool, ev('Herdr is our terminal multiplexer.')))
    world.run('retire', lambda commit: retire(graph, commit, noise, own))
    assert retire_check(graph, noise, own) == 'consolidation_retire_target_invalid'
    assert world.rows()[0] == rows[0] + 1


def test_protected_records_return_status_and_change_nothing(world):
    graph, store, ev = world.graph, world.store, world.ev
    quote = 'Temporary: run the smoke test once before lunch.'

    def scaffold(key):
        return world.remember(key, 'Run the smoke test once before lunch.', quote)
    pinned = scaffold('pinned')
    world.run('pin', lambda commit: pin(graph, commit, pinned, True))
    world.run('private-type', lambda commit: store._db.execute(
        "INSERT INTO memory_types VALUES ('private_note','memory',1,?)", (commit,)) and {})
    sensitive = world.run('sensitive', lambda commit: graph._insert_node(commit, dict(type='private_note',
        text='Run the smoke test once before lunch.', scope=['professional'], evidence=ev(quote))))['id']
    manual = scaffold('manual')
    graph.loop(action='later', id=manual, command_id='defer', actor='human', why='Not now', days=3)
    reminded = scaffold('reminded')
    world.run('due', lambda commit: _append(graph, commit, reminded, 'open', '2026-11-01T00:00:00+00:00', None))
    linked = scaffold('linked')
    other = scaffold('other')
    edge = graph.link(command_id='edge', actor='fixture', why='relation', a=linked, rel='related_to', b=other,
                      evidence=ev(quote))['id']
    graph.loop(action='open', id=edge, command_id='edge-choice', actor='human', why='Explicit pending marker')

    def apply(commit):
        node = graph._insert_node(commit, dict(type='experience', text='Queue replies when a pane is busy.',
            scope=['professional'], evidence=ev('Always queue replies when a pane is busy.')))
        return dict(results={'c_explicit': node})
    explicit = world.run('explicit', apply, dict(op='consolidate', output=dict(actions=[dict(
        candidate_id='c_explicit', operation='remember', type='experience', behavior=EXPLICIT)])))[
        'results']['c_explicit']['id']

    expected = {pinned: 'protected_pin', sensitive: 'protected_sensitive', manual: 'protected_loop_choice',
                reminded: 'protected_loop_choice', linked: 'protected_loop_choice', explicit: 'protected_explicit_user'}
    for target, status in expected.items():
        node = graph._node(target)
        evidence = ev('Always queue replies when a pane is busy.' if target == explicit else quote)
        rows, marker = world.rows(), current(graph, target)
        result = world.run('retire-' + target, lambda commit: retire(graph, commit, target, evidence))
        assert result == dict(id=target, version=node['version'], status=status)
        assert graph._node(target) == node and current(graph, target) == marker
        assert world.rows() == rows
    assert current(graph, edge)['state'] == 'open'


def test_absorbing_facets_keeps_a_newer_manual_relation_choice_protecting_retirement(world):
    from memento.curation import absorb
    from memento.loops import snapshot
    graph, store, ev = world.graph, world.store, world.ev
    quote = 'Recall returned scores 0.81, 0.77 and 0.64 for herdr.'
    noise = world.remember('noise', 'Recall returned scores for herdr.', quote)

    def tool(key, text):
        return graph.remember(command_id=key, actor='fixture', why='entity', type='tool', text=text,
                              scope=['professional'], evidence=ev('Herdr is our terminal multiplexer.'))['id']
    herdr, cli = tool('herdr', 'Herdr'), tool('cli', 'Herdr CLI')
    automatic, manual = (graph.link(command_id='l-' + target, actor='fixture', why='mention', a=noise, rel='mentions',
                                    b=target, evidence=ev(quote))['id'] for target in (herdr, cli))
    world.run('marker', lambda commit: _append(graph, commit, automatic, 'open', None, None))
    graph.loop(action='open', id=manual, command_id='manual', actor='human', why='Explicit pending marker')
    own = ev(quote)
    assert world.run('before', lambda commit: retire(graph, commit, noise, own))['status'] == 'protected_loop_choice'
    world.run('absorb', lambda commit: absorb(graph, commit, herdr, [cli],
                                              evidence=ev('Herdr is our terminal multiplexer.')))
    # Equal open markers, but folding would drop the newer manual choice: both relations stay.
    edges = {row['id']: row['state'] for row in store._db.execute('''SELECT e.id,e.state FROM edge_versions e
        WHERE e.src=? AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id)''', (noise,))}
    assert edges == {automatic: 'active', manual: 'active'}
    assert snapshot(graph, manual, graph.history(manual)[-1]['version'])['manual_choice_at'] is not None
    assert world.run('after', lambda commit: retire(graph, commit, noise, own))['status'] == 'protected_loop_choice'
    assert graph._node(noise)['state'] == 'active'
