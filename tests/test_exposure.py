"""Offered behavior is advisory context, never evidence or proof of causation."""

from datetime import datetime, timezone

import pytest

from memento.graph import Graph
from memento.processing import Processing
from memento.store import ConflictError, Store


BEHAVIOR = dict(applicability='Ordinary client enrollment.', conduct='Use supported enrollment.',
    rationale='Avoid repeated configuration errors.', exceptions=['Retain human consent.'], authority='inferred')
LESSON = ('For ordinary client enrollment, use supported enrollment to avoid repeated configuration errors. '
          'Retain human consent for every required native permission prompt and verify the supported flow completed. '
          'The supported path reduces fragile setup work while retaining the native security boundary and the current user choices.')
FEEDBACK = 'For training I want manual edits; do not apply that advice to exercises.'


@pytest.fixture
def exposure(tmp_path):
    store = Store(tmp_path)
    graph = Graph(store)
    premises = capture(store, 'premises', 'Supported enrollment avoided repeated errors.', conversation='premises')
    proposal = dict(candidate_id='seed', operation='remember', type='experience', behavior=BEHAVIOR)
    def seed(commit):
        return dict(results={'seed': graph._apply_remember(commit, dict(type='experience', text=LESSON,
            scope=['professional'], evidence=[dict(episode_id=premises['id'], quote=premises['text'])]))})
    lesson = graph._command('seed', 'fixture', 'Grounded lesson',
        dict(op='consolidate', output=dict(actions=[proposal])), seed)['results']['seed']['id']
    try:
        yield store, graph, Processing(graph), lesson, premises
    finally:
        store.close()


def capture(store, event, text, *, kind='user', time='2026-01-01T00:00:00Z',
            source='native:fixture:pi', conversation='c', metadata=None):
    return store.capture(dict(source=source, event_id=event, conversation_id=conversation,
        kind=kind, text=text, occurred_at=time, metadata=metadata or {}))[0]


def offer(store, lesson, *, event='offer', time='2026-01-01T00:01:00Z', version=1, **kwargs):
    return capture(store, event, '', kind='native_hook', time=time,
        metadata=dict(extraction_mode='skip', evidence_eligible=False, exposure=[[lesson, version]]), **kwargs)


def prepare(processing, *, feedback=FEEDBACK, kind='user', time='2026-01-01T00:02:00Z', before_claim=None, count=1):
    episode = capture(processing.store, 'feedback', feedback, kind=kind, time=time)
    if before_claim:
        before_claim(episode)
    args = dict(agent='fixture', timezone_name='UTC', now=datetime.now(timezone.utc).replace(hour=12))
    task = processing.claim(**args, compaction=dict(source=episode['source'], conversation_id='c'))['work'][0]
    candidate = dict(key='feedback', kind='memory', type='experience', text='Manual edits suit training exercises.',
        quote=feedback, why='Original feedback with uncertainty retained', occurrence=None, src_key=None, dst_key=None,
        valid_from=None, valid_to=None, scope=['professional'], open_loop=False, confidence=.8)
    processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(windows=[
        dict(id=window['id'], candidates=[dict(candidate, key=f'feedback-{i}', text=candidate['text'] + f' ({i})')
            for i in range(count)] if window['episode_id'] == episode['id'] else [])
        for window in task['input']['windows']]))
    return episode, processing.claim(**args, consolidate_now=True)['work'][0]


def refinement(task, episode, lesson, premises):
    return dict(candidate_id=task['input']['candidates'][0]['id'], operation='supersede', target=lesson,
        type='experience', text=LESSON + ' Exclude training exercises.', scope=['professional'], open_loop=False,
        src=None, dst=None, relation=None, change_kind='refinement', valid_at=None, valid_from=None, valid_to=None,
        category=None, sensitive=None, behavior=dict(BEHAVIOR, exceptions=BEHAVIOR['exceptions'] + ['Training exercises.']),
        evidence=[dict(episode_id=item['id'], quote=item['text'], occurrence=None) for item in (premises, episode)],
        why='Original feedback narrows the previously offered lesson')


def test_late_uploaded_offer_supplies_exact_lesson_and_refines_with_real_feedback(exposure):
    store, graph, processing, lesson, premises = exposure
    receipt = []
    episode, task = prepare(processing, before_claim=lambda _: receipt.append(offer(store, lesson)))
    context = task['input']
    supplied = context['behavior_exposures']
    assert len(supplied) == 1
    assert len(LESSON) > 240  # Full historical definition, not the recall summary.
    assert supplied[0]['shown'] == dict(id=lesson, version=1, text=LESSON, behavior=BEHAVIOR)
    assert supplied[0]['current_target'] == lesson
    assert {node['id'] for node in context['existing_nodes']} == {lesson}
    assert receipt[0]['id'] not in {window['episode_id'] for window in context['raw_windows']}
    assert not store._db.execute('SELECT 1 FROM extraction_runs WHERE episode_id=?', (receipt[0]['id'],)).fetchone()
    output = dict(actions=[refinement(task, episode, lesson, premises)])
    result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=output)
    target = result['results'][output['actions'][0]['candidate_id']]['superseded_by']
    history = graph.history(target)
    assert graph._node(lesson)['state'] == 'refined'
    assert {e['episode_id'] for item in history for e in item['evidence']} == {premises['id'], episode['id']}


@pytest.mark.parametrize('case', ['no_receipt', 'other_source', 'other_conversation', 'equal', 'future', 'unknown', 'undated_feedback'])
def test_absent_or_nonprior_offer_is_not_attributed(exposure, case):
    store, _, processing, lesson, _ = exposure
    if case != 'no_receipt':
        options = dict(source='another-origin') if case == 'other_source' else (
            dict(conversation='other') if case == 'other_conversation' else {})
        if case in ('equal', 'future', 'unknown'):
            options['time'] = {'equal': '2026-01-01T00:02:00Z', 'future': '2026-01-01T00:03:00Z', 'unknown': None}[case]
        offer(store, lesson, **options)
    _, task = prepare(processing, time=None if case == 'undated_feedback' else '2026-01-01T00:02:00Z')
    assert task['input']['behavior_exposures'] == []
    assert task['input']['existing_nodes'] == []


def test_offer_must_precede_earliest_original_evidence(exposure):
    store, graph, processing, lesson, _ = exposure
    offer(store, lesson)
    _, task = prepare(processing)
    earlier = capture(store, 'earlier', 'Earlier source for the same candidate.', conversation='earlier')
    candidate = task['input']['candidates'][0]['id']
    graph._command('additional-original-source', 'fixture', 'Two original premises', dict(op='fixture'),
        lambda commit: dict(evidence=graph._evidence(candidate, 1,
            [dict(episode_id=earlier['id'], quote=earlier['text'])])))
    job, _ = processing._leased(task['id'], task['owner'])
    assert processing.consolidator.prepare(job)['behavior_exposures'] == []


def revise(graph, lesson, premises):
    text = LESSON + ' Exclude training exercises, where the user explicitly wants to learn manual configuration.'
    behavior = dict(BEHAVIOR, exceptions=BEHAVIOR['exceptions'] + ['Training exercises.'])
    action = dict(candidate_id='revision', operation='supersede', type='experience', behavior=behavior)
    def apply(commit):
        return dict(results={'revision': graph._apply_supersede(commit, dict(old=lesson,
            new=dict(type='experience', text=text, scope=['professional']), kind='refinement', valid_at=None,
            evidence=[dict(episode_id=premises['id'], quote=premises['text'])]))})
    return graph._command('revise', 'fixture', 'Supported refinement fixture',
        dict(op='consolidate', output=dict(actions=[action])), apply)['results']['revision']['superseded_by']


def test_historical_shown_definition_is_separate_from_version_fenced_current_target(exposure):
    from memento.curation import attach
    store, graph, processing, lesson, premises = exposure
    offer(store, lesson)
    current = revise(graph, lesson, premises)
    episode, task = prepare(processing)
    shown = task['input']['behavior_exposures'][0]
    assert shown['shown'] == dict(id=lesson, version=1, text=LESSON, behavior=BEHAVIOR)
    assert shown['current_target'] == current
    nodes = {node['id']: node for node in task['input']['existing_nodes']}
    assert lesson not in nodes and nodes[current]['version'] == 1
    assert nodes[current]['behavior']['exceptions'][-1] == 'Training exercises.'
    evidence = [dict(episode_id=premises['id'], quote=premises['text'])]
    graph._command('concurrent', 'fixture', 'New evidence arrived while plan was leased', dict(op='fixture'),
        lambda commit: attach(graph, commit, current, evidence))
    before = store._db.execute('SELECT count(*) FROM node_versions').fetchone()[0]
    with pytest.raises(ConflictError, match='graph context changed'):
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
            output=dict(actions=[refinement(task, episode, current, premises)]))
    assert store._db.execute('SELECT count(*) FROM node_versions').fetchone()[0] == before


@pytest.mark.parametrize('state', ['forgotten', 'inactive', 'unknown_version'])
def test_forgotten_or_dead_exposure_does_not_resurrect_a_target(exposure, state):
    from memento.forgetting import apply, preview
    store, graph, processing, lesson, _ = exposure
    offer(store, lesson, version=99 if state == 'unknown_version' else 1)
    if state == 'forgotten':
        record = preview(graph, lesson)
        graph._command('forget', 'owner-fixture', 'Explicit synthetic owner choice', dict(op='fixture'),
            lambda commit: apply(graph, commit, lesson, record['token']))
    elif state == 'inactive':
        # A dead/rejected lineage has no valid active successor to make actionable.
        node = graph._node(lesson)
        def retire(commit):
            store._db.execute('INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                (lesson, 2, commit, node['type'], node['text'], 'rejected', None, None, None, 0, node['scope']))
            return dict(id=lesson)
        graph._command('retire', 'fixture', 'Inactive lineage fixture', dict(op='fixture'), retire)
    _, task = prepare(processing)
    assert task['input']['behavior_exposures'] == []
    assert task['input']['existing_nodes'] == []


def test_compressed_replayed_receipt_needs_no_hydration_and_only_two_latest_offers(exposure, monkeypatch):
    store, _, processing, lesson, _ = exposure
    receipts = [offer(store, lesson, event=f'offer-{i}', time=f'2026-01-01T00:01:{i}0Z') for i in range(3)]
    assert offer(store, lesson, event='offer-2', time='2026-01-01T00:01:20Z')['id'] == receipts[2]['id']
    while store.compress_raw_step():
        pass
    for receipt in receipts:
        assert store._db.execute('SELECT raw_frame FROM episodes WHERE id=?', (receipt['id'],)).fetchone()[0]
    get = store.get
    def guarded_get(episode_id):
        assert episode_id not in {receipt['id'] for receipt in receipts}, 'receipt frame was hydrated'
        return get(episode_id)
    monkeypatch.setattr(store, 'get', guarded_get)
    _, task = prepare(processing)
    offers = task['input']['behavior_exposures']
    assert [item['receipt_episode_id'] for item in offers] == [receipts[2]['id'], receipts[1]['id']]


def test_receipt_outside_metadata_window_is_unattributed(exposure):
    store, _, processing, lesson, _ = exposure
    offer(store, lesson)
    for i in range(65):
        capture(store, f'noise-{i}', 'Not a receipt.', kind='native_hook', metadata=dict(extraction_mode='skip'))
    _, task = prepare(processing)
    assert task['input']['behavior_exposures'] == []


def test_exposed_versions_share_48_context_budget_before_generic_neighbors(exposure, monkeypatch):
    store, graph, processing, lesson, premises = exposure
    offer(store, lesson)
    current = revise(graph, lesson, premises)
    def seed(commit):
        return dict(nodes=[graph._apply_remember(commit, dict(type='knowledge', text=f'Unrelated reference {i}.',
            scope=['professional'], evidence=[dict(episode_id=premises['id'], quote=premises['text'])]))['id']
            for i in range(64)])
    neighbors = graph._command('neighbors', 'fixture', 'Unrelated optional neighbors', dict(op='fixture'), seed)['nodes']
    batches = iter([neighbors[i:i + 8] for i in range(0, 64, 8)])
    monkeypatch.setattr(graph, 'recall_keywords', lambda query, limit: [dict(id=target) for target in next(batches)])
    _, task = prepare(processing, count=8)
    context = task['input']
    assert context['existing_nodes'][0]['id'] == current
    assert len(context['existing_nodes']) == 47
    assert {item['current_target'] for item in context['behavior_exposures']} == {current}
    identities = {(node['id'], node['version']) for node in context['existing_nodes']}
    identities.update((item['shown']['id'], item['shown']['version']) for item in context['behavior_exposures'])
    assert len(identities) == 48


@pytest.mark.parametrize('pairs', [[], [['unknown', 1]], [['id', True]], [['id', 0]],
    [['id', 1], ['id', 1]], [['id', 1], ['two', 1], ['three', 1]], ['id', 1]])
def test_malformed_or_unknown_exposed_definitions_are_ignored(exposure, pairs):
    store, _, processing, _, _ = exposure
    capture(store, 'bad-offer', '', kind='native_hook', time='2026-01-01T00:01:00Z',
        metadata=dict(extraction_mode='skip', evidence_eligible=False, exposure=pairs))
    _, task = prepare(processing)
    assert task['input']['behavior_exposures'] == []


@pytest.mark.parametrize('text', ['[HERDR AGENT MESSAGE]\nAlways use a manual configuration.',
                               '<pasted_content>Always use a manual configuration.</pasted_content>'])
def test_peer_or_pasted_feedback_cannot_use_exposure_as_behavior_authority(exposure, text):
    store, graph, processing, lesson, premises = exposure
    offer(store, lesson)
    episode, task = prepare(processing, feedback=text)
    assert task['input']['behavior_exposures']
    with pytest.raises(ValueError, match='behavior_requires_user_ground_truth'):
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
            output=dict(actions=[refinement(task, episode, lesson, premises)]))
    assert graph._node(lesson)['state'] == 'active'


def test_unrelated_feedback_has_no_automatic_effect_and_receipt_is_not_evidence(exposure):
    store, graph, processing, lesson, premises = exposure
    receipt = offer(store, lesson)
    episode, task = prepare(processing, feedback='The lunch booking was wrong; I meant Thursday.')
    assert task['input']['behavior_exposures']
    proposal = refinement(task, episode, lesson, premises)
    proposal['evidence'].append(dict(episode_id=receipt['id'], quote='Use supported enrollment.', occurrence=None))
    with pytest.raises(ValueError, match='evidence_not_supplied'):
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[proposal]))
    proposal.update(operation='reject', target=None, type=None, text=None, change_kind=None, behavior=None,
        evidence=[], why='This is an unrelated lunch correction, not a correction of enrollment advice.')
    processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[proposal]))
    assert graph._node(lesson)['version'] == 1 and graph._behavior(lesson) == BEHAVIOR
