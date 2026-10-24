"""Existing memory enters consolidation as exact evidence references, not surrounding raw windows."""

import json
from datetime import datetime, timezone

import pytest

from memento.graph import Graph
from memento.processing import Processing
from memento.store import Store
from test_behavioral_learning import complete
from test_technical_learning import BEHAVIOR, captured_trace, extract_trace, proposal, reported_trace

SOURCE = 'native:synthetic-device:claude'
POLICY = 'The synthetic approval policy requires explicit operator consent.'
PRIOR_TEXT = 'Earlier context sentence. ' + POLICY + ' Later context sentence.'
ARGS = dict(agent='fixture', timezone_name='UTC')


def capture(store, event_id, text, conversation_id='c'):
    return store.capture(dict(source=SOURCE, event_id=event_id, conversation_id=conversation_id, kind='user',
                              text=text, occurred_at=None, metadata={}))[0]


def prepared(tmp_path, candidate_text=POLICY):
    store = Store(tmp_path)
    graph, processing = Graph(store), Processing(Graph(store))
    prior = capture(store, 'prior', PRIOR_TEXT, conversation_id='earlier')
    existing = graph.remember(command_id='existing', actor='user', why='Known policy', type='knowledge', text=POLICY,
                              scope=['professional'], evidence=[dict(episode_id=prior['id'], quote=POLICY)])['id']
    later = capture(store, 'later', 'Operators confirmed: ' + candidate_text)
    # Real current time: only the compacted conversation is extractable; the prior one is not idle.
    [work] = processing.claim(**ARGS, now=datetime.now(timezone.utc), compaction=dict(source=SOURCE, conversation_id='c'))['work']
    draft = dict(key='policy', kind='memory', type='knowledge', text=candidate_text, quote=candidate_text,
        why='Exact durable assertion', occurrence=None, src_key=None, dst_key=None, valid_from=None, valid_to=None,
        scope=['professional'], open_loop=False, confidence=.8)
    processing.complete(id=work['id'], owner=work['owner'], agent='fixture',
                        output=dict(windows=[dict(id='w0', candidates=[draft])]))
    [task] = processing.claim(**ARGS, now=datetime.now(timezone.utc), consolidate_now=True)['work']
    return store, graph, processing, existing, prior, later, task


def plan_action(task, operation, **extra):
    base = dict(candidate_id=task['input']['candidates'][0]['id'], operation=operation, type=None, text=None,
        scope=None, open_loop=False, target=None, src=None, dst=None, relation=None, change_kind=None, valid_at=None,
        valid_from=None, valid_to=None, category=None, sensitive=None, evidence=[],
        why='Compared with the supplied prior premise')
    base.update(extra)
    return base


def prior_reference(task, existing):
    [node] = [item for item in task['input']['existing_nodes'] if item['id'] == existing]
    [ref] = node['evidence']
    return dict(episode_id=ref['episode_id'], quote=ref['quote'], occurrence=ref['occurrence'])


def test_existing_memory_is_supplied_as_exact_references_without_raw_windows(tmp_path):
    store, graph, processing, existing, prior, later, task = prepared(tmp_path)
    try:
        [node] = [item for item in task['input']['existing_nodes'] if item['id'] == existing]
        [ref] = node['evidence']
        start = PRIOR_TEXT.index(POLICY)
        assert ref == dict(episode_id=prior['id'], quote=POLICY, occurrence=0, start=start, end=start + len(POLICY),
                           behavior_ground_truth=True)
        assert task['input']['candidates'][0]['identity']['exact'] == [existing]
        # Only the candidate's own evidence receives a surrounding raw window.
        assert {window['episode_id'] for window in task['input']['raw_windows']} == {later['id']}
        [window] = task['input']['raw_windows']
        assert window['start'] == 0 and window['text'] == later['text']
    finally:
        store.close()


@pytest.mark.parametrize('operation', ['duplicate', 'supersede'])
def test_prior_premise_is_citable_at_its_exact_supplied_span(tmp_path, operation):
    refined = POLICY[:-1] + ' before production changes.'
    store, graph, processing, existing, prior, later, task = prepared(tmp_path, refined if operation == 'supersede' else POLICY)
    try:
        if operation == 'supersede':
            assert existing in task['input']['candidates'][0]['identity']['similar']
        extra = dict(target=existing, evidence=[dict(episode_id=later['id'], quote=later['text'], occurrence=None),
                                                 prior_reference(task, existing)])
        if operation == 'supersede':
            extra.update(type='knowledge', text=refined, change_kind='refinement', scope=['professional'])
        result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
                                     output=dict(actions=[plan_action(task, operation, **extra)]))
        outcome = result['results'][task['input']['candidates'][0]['id']]
        assert outcome['decision'] == 'promote'
        target = outcome.get('superseded_by') or outcome['id']
        node = graph._node(target)
        quotes = {row[0] for row in store._db.execute('SELECT quote FROM graph_evidence WHERE target=? AND version=?',
                                                       (target, node['version']))}
        # The prior premise travels with the new assertion through its exact supplied span.
        assert quotes >= {POLICY, later['text']}
        if operation == 'supersede':
            assert target != existing and node['text'] == refined
            assert graph._node(existing)['state'] == 'refined'
        else:
            assert target == existing and node['version'] == 2
    finally:
        store.close()


def test_verbatim_subspan_of_a_supplied_existing_quote_is_supplied_evidence(tmp_path):
    store, graph, processing, existing, prior, later, task = prepared(tmp_path)
    try:
        subspan = POLICY[4:-9]
        action = plan_action(task, 'duplicate', target=existing, evidence=[
            dict(episode_id=later['id'], quote=later['text'], occurrence=None),
            dict(episode_id=prior['id'], quote=subspan, occurrence=None)])
        result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[action]))
        assert result['results'][task['input']['candidates'][0]['id']]['id'] == existing
        quotes = {row[0] for row in store._db.execute('SELECT quote FROM graph_evidence WHERE target=? AND version=2',
                                                       (existing,))}
        assert subspan in quotes
    finally:
        store.close()


@pytest.mark.parametrize('defect', ['overlapping_span', 'surrounding_context', 'unsupplied_episode'])
def test_citations_outside_supplied_spans_or_windows_are_rejected(tmp_path, defect):
    store, graph, processing, existing, prior, later, task = prepared(tmp_path)
    try:
        if defect == 'unsupplied_episode':
            other = capture(store, 'unsupplied', 'Operators also said: ' + POLICY, conversation_id='earlier')
            foreign = dict(episode_id=other['id'], quote=POLICY, occurrence=None)
            expected = 'consolidation_evidence_not_supplied'
        else:
            # Text reaching beyond the supplied quote, even by one word, is not supplied.
            quote = 'sentence. ' + POLICY[:-9] if defect == 'overlapping_span' else 'Earlier context sentence.'
            foreign = dict(episode_id=prior['id'], quote=quote, occurrence=None)
            expected = 'consolidation_evidence_outside_context'
        action = plan_action(task, 'duplicate', target=existing,
                             evidence=[dict(episode_id=later['id'], quote=later['text'], occurrence=None), foreign])
        with pytest.raises(ValueError, match=expected):
            processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[action]))
        assert graph._node(existing)['version'] == 1
        assert not store._db.execute('SELECT 1 FROM candidate_decisions').fetchone()
    finally:
        store.close()


def test_old_lease_context_with_existing_memory_windows_still_applies(tmp_path):
    store, graph, processing, existing, prior, later, task = prepared(tmp_path)
    try:
        old_window = dict(episode_id=prior['id'], start=0, end=len(PRIOR_TEXT), kind='user', text=PRIOR_TEXT,
            occurred_at=prior['occurred_at'], source=SOURCE, event_id=prior['event_id'], conversation_id='earlier',
            native_id=None, behavior_ground_truth=True, sequence=1, native_trace=None)
        saved = dict(task['input'], raw_windows=[*task['input']['raw_windows'], old_window])
        store._db.execute('UPDATE jobs SET context=? WHERE id=?', (json.dumps(saved), task['id']))
        action = plan_action(task, 'duplicate', target=existing, evidence=[
            dict(episode_id=later['id'], quote=later['text'], occurrence=None),
            dict(episode_id=prior['id'], quote='Earlier context sentence.', occurrence=None)])
        result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[action]))
        assert result['results'][task['input']['candidates'][0]['id']]['id'] == existing
        assert graph._node(existing)['version'] == 2
    finally:
        store.close()


INCIDENT = 'Current-scope lookup missed an archived report.'  # the candidate text extract_trace derives


def compact_trace(tmp_path, store):
    """Two actual attempt/result pairs, then enough later records to push them beyond the
    nearby-record bound, then the assistant's own later report of the lesson."""
    records = reported_trace()
    fillers = [dict(type='user', uuid=f'note-{index}', message=dict(role='user', content=f'Progress note {index}.'))
               for index in range(40)]
    _, episodes = captured_trace(tmp_path, store, records=[*records[:4], *fillers, *records[4:]])
    trace, report = episodes[:4], episodes[-1]
    assert [episode['kind'] for episode in trace] == ['assistant', 'tool'] * 2 and report['kind'] == 'assistant'
    return trace, report


@pytest.mark.parametrize('case', ['supported', 'later_attempt_without_result'])
def test_compact_existing_attempt_and_result_refs_ground_a_later_report(tmp_path, case):
    store = Store(tmp_path / 'store')
    try:
        graph, processing = Graph(store), Processing(Graph(store))
        trace, report = compact_trace(tmp_path, store)
        # An earlier same-type experience already quotes the actual attempts and results.
        existing = graph.remember(command_id='incident', actor='user', why='Observed incident', type='experience',
                                  text=INCIDENT, scope=['professional'],
                                  evidence=[dict(episode_id=episode['id'], quote=episode['text']) for episode in trace])['id']
        args = extract_trace(processing, report)
        task = processing.claim(**args, consolidate_now=True)['work'][0]
        [candidate] = task['input']['candidates']
        assert candidate['identity']['exact'] == [existing]
        [node] = [item for item in task['input']['existing_nodes'] if item['id'] == existing]
        assert {ref['episode_id'] for ref in node['evidence']} == {episode['id'] for episode in trace}
        # Beyond the nearby bound: only the report has a raw window, and the compact
        # existing references are the only supply of the attempts and results.
        assert {window['episode_id'] for window in task['input']['raw_windows']} == {report['id']}
        output = proposal(task, report, [report, *trace])
        output.update(operation='supersede', target=existing, change_kind='refinement')
        selected = trace if case == 'supported' else trace[:3]
        output['technical_evidence'] = [episode['id'] for episode in selected]
        if case != 'supported':
            # attempt, result, attempt: the latest cited attempt has no observed result.
            with pytest.raises(ValueError, match='behavior_requires_user_ground_truth'):
                complete(processing, task, output)
            assert graph._node(existing)['state'] == 'active' and graph._node(existing)['version'] == 1
            assert not store._db.execute('SELECT 1 FROM candidate_decisions').fetchone()
            return
        outcome = complete(processing, task, output)
        assert outcome['decision'] == 'promote'
        learned = outcome['superseded_by']
        # History carries the whole supersession lineage, so select the learned row itself.
        [history] = [row for row in graph.history(learned) if row['id'] == learned and row['version'] == 1]
        assert history['authority'] == 'inferred' and history['behavior'] == BEHAVIOR
        assert {ref['quote'] for ref in history['evidence']} == {episode['text'] for episode in [report, *trace]}
        assert graph._node(existing)['state'] == 'refined'
        # The references carry only bounded trace metadata, never surrounding source text.
        refs = {ref['episode_id']: ref for ref in node['evidence']}
        assert [refs[episode['id']]['native_trace'] for episode in trace] == ['attempt', 'result'] * 2
        sequences = [refs[episode['id']]['sequence'] for episode in trace]
        [report_window] = task['input']['raw_windows']
        assert sequences == sorted(sequences) and sequences[-1] < report_window['sequence']
        assert all((ref['source'], ref['conversation_id']) == (report['source'], report['conversation_id'])
                   for ref in refs.values())
        assert set(refs[trace[0]['id']]) == {'episode_id', 'quote', 'occurrence', 'start', 'end',
                                             'behavior_ground_truth', 'source', 'conversation_id',
                                             'sequence', 'native_trace'}
    finally:
        store.close()


def test_supplied_homonym_remains_the_models_distinct_identity_judgment(tmp_path):
    store, graph, processing, existing, prior, later, task = prepared(tmp_path)
    try:
        action = plan_action(task, 'remember', type='knowledge', text=POLICY, scope=['professional'],
                             evidence=[dict(episode_id=later['id'], quote=later['text'], occurrence=None)])
        result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[action]))
        outcome = result['results'][task['input']['candidates'][0]['id']]
        assert outcome['decision'] == 'promote' and outcome['id'] != existing
        assert graph._node(existing)['version'] == 1
    finally:
        store.close()
