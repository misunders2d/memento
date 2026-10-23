import json
import hashlib
from datetime import datetime, timezone

import pytest

from memento.graph import Graph
from memento.processing import Processing
from memento.store import Store


TEXT = '😀 Сергей, Mellanni, Memory. Связи: Сергей → Mellanni; Memory → Mellanni.'
NATIVE_SOURCE = 'native:synthetic-device:codex'
GOAL_CONTEXT = '<codex_internal_context source="goal">\n' + TEXT + '\n</codex_internal_context>'
LEGACY_CONTEXTS = [
    '# AGENTS.md instructions for /workspace/example\n\n<INSTRUCTIONS>\n' + TEXT +
        '\n</INSTRUCTIONS>\n<environment_context>\ncwd: /workspace/example\nshell: bash\n</environment_context>',
    '# AGENTS.md instructions\n\n<INSTRUCTIONS>\nThese AGENTS.md instructions replace all previously provided AGENTS.md instructions.\n' +
        TEXT + '\n</INSTRUCTIONS>',
    '# AGENTS.md instructions\n\n<INSTRUCTIONS>\n' + TEXT +
        '\n</INSTRUCTIONS>\n<environment_context>\ncurrent_date: 2026-01-01\n</environment_context>',
    '# AGENTS.md instructions for /workspace/other\n\n<INSTRUCTIONS>\n' + TEXT +
        '\n</INSTRUCTIONS>\n<environment_context>\nfilesystem: local\n</environment_context>',
    GOAL_CONTEXT, GOAL_CONTEXT + '\n \t',
]


def draft(key, kind, type, text, quote, src=None, dst=None):
    return dict(key=key, kind=kind, type=type, text=text, quote=quote, why='Explicit durable source assertion',
        occurrence=None, src_key=src, dst_key=dst, valid_from=None, valid_to=None,
        scope=['professional'], open_loop=False, confidence=.8)


def drafts():
    return [draft('person', 'entity', 'person', 'Сергей', 'Сергей, Mellanni'),
        draft('org', 'entity', 'organization', 'Mellanni', 'Mellanni, Memory'),
        draft('project', 'entity', 'project', 'Memory', 'Memory.'),
        draft('link1', 'relation', 'related_to', 'Связано с Mellanni', 'Связи: Сергей → Mellanni; Memory → Mellanni.', 'person', 'org'),
        draft('link2', 'relation', 'related_to', 'Связано с Mellanni', 'Связи: Сергей → Mellanni; Memory → Mellanni.', 'project', 'org')]


def source_windows(processing, work):
    """Inspect saved provenance while echoing only the IDs actually served."""
    _, context = processing._leased(work['id'], work['owner'])
    return [dict(source, **shown, canonical_id=source['id'])
        for source, shown in zip(context['windows'], work['input']['windows'], strict=True)]


@pytest.mark.parametrize('defect', [None, 'abstain', 'personal_experience', 'personal_decision',
    'subject', 'version', 'condition', 'record_limit', 'byte_limit', 'qualified_experience',
    'invented_subject', 'personal_key', 'personal_location', 'personal_scope', 'invalid_type', 'quote'])
def test_frozen_native_factual_scope_controls_detect_loss_and_invention(defect):
    from memory_acceptance import FACTUAL_SCOPE_CASES, factual_scope_report
    texts = [
        'Juniper Archive 4.2 limits signed offline snapshots to 50 records and 4 MiB per archive chunk.',
        'Archive chunks contain at most 50 records and at most 4 MiB of encoded payload.',
        'The user keeps their spare house key in the blue tin.',
    ]
    output = dict(windows=[])
    for case, text in zip(FACTUAL_SCOPE_CASES, texts):
        candidate = draft(case['id'], 'memory', 'knowledge', text, case['text'])
        if case['id'] == 'personal_fact':
            candidate['scope'] = ['personal']
        output['windows'].append(dict(id=case['id'], candidates=[candidate]))
    if defect in ('subject', 'version', 'condition', 'record_limit', 'byte_limit'):
        remove = dict(subject='Juniper Archive', version='4.2', condition='signed offline',
                      record_limit='50', byte_limit='4 MiB')[defect]
        output['windows'][0]['candidates'][0]['text'] = texts[0].replace(remove, '')
    elif defect == 'abstain':
        output['windows'][1]['candidates'] = []
    elif defect in ('personal_experience', 'personal_decision'):
        output['windows'][2]['candidates'][0]['type'] = defect.removeprefix('personal_')
    elif defect == 'qualified_experience':
        output['windows'][0]['candidates'][0]['type'] = 'experience'
    elif defect == 'invented_subject':
        output['windows'][1]['candidates'][0]['text'] = 'Juniper Archive: ' + texts[1]
    elif defect in ('personal_key', 'personal_location'):
        output['windows'][2]['candidates'][0]['text'] = texts[2].replace(
            'key' if defect == 'personal_key' else 'blue tin', '')
    elif defect == 'personal_scope':
        output['windows'][2]['candidates'][0]['scope'] = ['professional']
    elif defect == 'invalid_type':
        output['windows'][2]['candidates'][0]['type'] = 'fact'
        with pytest.raises(ValueError, match='candidate_memory_type_invalid'):
            factual_scope_report(output)
        return
    elif defect == 'quote':
        output['windows'][0]['candidates'][0]['quote'] = 'Invented evidence'
        with pytest.raises(ValueError, match='candidate_quote_not_in_source'):
            factual_scope_report(output)
        return
    result = factual_scope_report(output)
    assert result['passed'] is (defect in (None, 'abstain', 'personal_decision'))
    assert len(result['cases']) == 3


@pytest.fixture
def extraction(tmp_path):
    store = Store(tmp_path)
    graph = Graph(store)
    payload = dict(source='test', event_id='raw', conversation_id='c', kind='user', text=TEXT,
                   occurred_at='2020-01-01T00:00:00+00:00', metadata={})
    episode, _ = store.capture(payload)
    processing = Processing(graph)
    work = processing.claim(agent='fixture', timezone_name='UTC',
        compaction=dict(source='test', conversation_id='c'),
        now=datetime.now(timezone.utc).replace(hour=12))['work'][0]
    output = dict(windows=[dict(id=work['input']['windows'][0]['id'], candidates=drafts())])
    try:
        yield store, graph, processing, episode, payload, work, output
    finally:
        store.close()


def test_newer_due_conversation_precedes_imported_backlog_without_reordering_windows(tmp_path):
    store = Store(tmp_path)
    try:
        processing = Processing(Graph(store))
        episodes = {}
        for conversation in ('imported-history', 'live-conversation'):
            episodes[conversation] = []
            for index in range(2):
                episode, _ = store.capture(dict(source='priority', event_id=f'{conversation}-{index}',
                    conversation_id=conversation, kind='user', text=f'{conversation} message {index}',
                    occurred_at=None, metadata={}))
                episodes[conversation].append(episode['id'])
            processing.compact('priority', conversation)
        args = dict(agent='fixture', timezone_name='UTC',
                    now=datetime(2026, 10, 18, 12, tzinfo=timezone.utc))
        for expected in ('live-conversation', 'imported-history'):
            task = processing.claim(**args)['work'][0]
            assert [window['episode_id'] for window in source_windows(processing, task)] == episodes[expected]
            assert [window['text'] for window in task['input']['windows']] == [
                f'{expected} message 0', f'{expected} message 1']
            processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
                output=dict(windows=[dict(id=window['id'], candidates=[])
                                     for window in task['input']['windows']]))
    finally:
        store.close()


def test_finite_import_prefix_finishes_while_newer_conversation_keeps_growing(tmp_path):
    store = Store(tmp_path)
    try:
        processing = Processing(Graph(store))
        for conversation in ('skip-only', 'imported-history'):
            store.capture(dict(source='fairness', event_id=conversation + '-raw', conversation_id=conversation,
                kind='native_hook', text='Ignored raw envelope.', occurred_at=None, metadata={'extraction_mode': 'skip'}))
            processing.compact('fairness', conversation)
        text = '😀' + 'Old source evidence. ' * 3250
        original, _ = store.capture(dict(source='fairness', event_id='old', conversation_id='imported-history',
            kind='user', text=text, occurred_at=None, metadata={}))
        processing.compact('fairness', 'imported-history')
        boundary = store._db.execute("SELECT compact_through FROM processing_progress WHERE conversation_id='imported-history'").fetchone()[0]
        newer = []

        def append_live():
            episode, _ = store.capture(dict(source='fairness', event_id=f'live-{len(newer)}', conversation_id='live',
                kind='user', text=f'New live evidence {len(newer)}.', occurred_at=None, metadata={}))
            newer.append(episode['id'])
            processing.compact('fairness', 'live')

        append_live()
        args = dict(agent='fixture', timezone_name='UTC', now=datetime.now(timezone.utc).replace(hour=12))
        attempted, admissions, windows = [], [], []
        for _ in range(4):
            task = processing.claim(**args, exclude=attempted)['work'][0]
            attempted.append(task['id'])
            batch = source_windows(processing, task)
            conversation = store.get(batch[0]['episode_id'])['conversation_id']
            admissions.append(conversation)
            assert store._db.execute("""SELECT count(*) FROM (SELECT 1 FROM jobs
                WHERE kind='extract' AND state IN ('pending','leased')
                GROUP BY json_extract(payload,'$.source'),json_extract(payload,'$.conversation_id')
                HAVING count(*)>1)""").fetchone()[0] == 0
            if conversation == 'imported-history':
                windows.extend(batch)
                assert all(window['episode_id'] == original['id']
                           and window['text'] == text[window['start']:window['end']] for window in batch)
            else:
                assert conversation == 'live' and [window['episode_id'] for window in batch] == [newer[-1]]
            processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
                output=dict(windows=[dict(id=window['id'], candidates=[]) for window in batch]))
            if conversation == 'live':
                append_live()
        assert admissions == ['live', 'imported-history', 'live', 'imported-history']
        assert windows[0]['start'] == 0 and windows[-1]['end'] == len(text)
        assert all(right['start'] == left['end'] - 1000 for left, right in zip(windows, windows[1:]))
        progress = {row['conversation_id']: row for row in store._db.execute('SELECT * FROM processing_progress')}
        assert progress['imported-history']['seq'] == boundary and progress['imported-history']['offset'] == 0
        assert progress['skip-only']['seq'] == progress['skip-only']['compact_through']
        assert progress['live']['seq'] < progress['live']['compact_through']
    finally:
        store.close()


def test_extraction_admission_order_survives_restart_and_ignores_non_extract_jobs(tmp_path):
    store = Store(tmp_path)
    try:
        processing = Processing(Graph(store))
        for conversation, text in (('old', 'Old evidence. ' * 6000), ('new', 'New evidence.')):
            store.capture(dict(source='restart-fairness', event_id=conversation, conversation_id=conversation,
                kind='user', text=text, occurred_at=None, metadata={}))
            processing.compact('restart-fairness', conversation)
        # A completed pre-upgrade extraction has no admission tag.
        legacy = processing.queue._insert('legacy-extract', 'extract', {})
        store._db.execute("UPDATE jobs SET state='done' WHERE id=?", (legacy,))
        processing.queue._insert('unrelated-media-1', 'media', {'order': 'oldest'})
        store.close()
        store = Store(tmp_path)
        processing = Processing(Graph(store))
        args = dict(agent='fixture', timezone_name='UTC', now=datetime.now(timezone.utc).replace(hour=12))
        first = processing.claim(**args)['work'][0]
        assert store.get(source_windows(processing, first)[0]['episode_id'])['conversation_id'] == 'old'
        assert json.loads(store._db.execute('SELECT payload FROM jobs WHERE id=?', (first['id'],)).fetchone()[0])['order'] == 'oldest'
        processing.complete(id=first['id'], owner=first['owner'], agent='fixture',
            output=dict(windows=[dict(id=window['id'], candidates=[]) for window in first['input']['windows']]))
        assert store._db.execute("SELECT offset FROM processing_progress WHERE conversation_id='old'").fetchone()[0] > 0
        processing.queue._insert('unrelated-media-2', 'media', {'order': 'newest'})
        store.close()
        store = Store(tmp_path)
        processing = Processing(Graph(store))
        second = processing.claim(**args, exclude=[first['id']])['work'][0]
        assert store.get(source_windows(processing, second)[0]['episode_id'])['conversation_id'] == 'new'
        assert json.loads(store._db.execute('SELECT payload FROM jobs WHERE id=?', (second['id'],)).fetchone()[0])['order'] == 'newest'
    finally:
        store.close()


@pytest.mark.parametrize('legacy', [False, True])
@pytest.mark.parametrize('state', ['pending', 'failed'])
def test_extraction_admission_metadata_preserves_existing_job_identity_and_retry(tmp_path, legacy, state):
    store = Store(tmp_path)
    try:
        processing = Processing(Graph(store))
        store.capture(dict(source='retry-fairness', event_id='u', conversation_id='c', kind='user',
            text='Exact pending evidence.', occurred_at=None, metadata={}))
        processing.compact('retry-fairness', 'c')
        args = dict(agent='fixture', timezone_name='UTC', now=datetime.now(timezone.utc).replace(hour=12))
        first = processing.claim(**args)['work'][0]
        original = store._db.execute('SELECT job_key,payload FROM jobs WHERE id=?', (first['id'],)).fetchone()
        payload = json.loads(original['payload'])
        canonical = {key: value for key, value in payload.items() if key != 'order'}
        assert original['job_key'] == 'extract/' + hashlib.sha256(json.dumps(canonical, sort_keys=True).encode()).hexdigest()
        if legacy:
            payload = canonical
        serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False, allow_nan=False)
        store._db.execute('''UPDATE jobs SET payload=?,state=?,owner=NULL,lease_until=NULL,available_at=0
            WHERE id=?''', (serialized, state, first['id']))
        store.close()
        store = Store(tmp_path)
        processing = Processing(Graph(store))
        work = processing.claim(**args)['work']
        assert [item['id'] for item in work] == ([first['id']] if state == 'pending' else [])
        after = store._db.execute('SELECT job_key,payload,attempts FROM jobs WHERE id=?', (first['id'],)).fetchone()
        assert after['job_key'] == original['job_key'] and after['payload'] == serialized
        assert after['attempts'] == (2 if state == 'pending' else 1)
        assert store._db.execute("SELECT count(*) FROM jobs WHERE kind='extract'").fetchone()[0] == 1
    finally:
        store.close()


@pytest.mark.parametrize('text', LEGACY_CONTEXTS)
def test_exact_legacy_native_context_preserves_raw_identity_and_excludes_behavior(tmp_path, text):
    from memento.graph import behavior_ground_truth
    from memento.store import legacy_native_context
    payload = dict(source=NATIVE_SOURCE, event_id='legacy', conversation_id='c', kind='user',
                   text=text, occurred_at=None, metadata={'extraction_mode': 'message', 'native_id': 'old-item'})
    store = Store(tmp_path)
    try:
        episode, created = store.capture(payload)
        assert created and legacy_native_context(episode['source'], episode['kind'], episode['text'], episode['metadata'])
        assert not behavior_ground_truth(episode)
        assert not behavior_ground_truth(episode, text.index('Сергей'), text.index('Сергей') + len('Сергей'))
        assert store.capture(payload) == (episode, False)
        assert store.get(episode['id']) == episode
    finally:
        store.close()
    reopened = Store(tmp_path)
    try:
        assert reopened.get(episode['id']) == episode
        assert reopened._db.execute('SELECT legacy_native_context(source,kind,text,metadata) FROM episodes').fetchone()[0] == 1
    finally:
        reopened.close()


@pytest.mark.parametrize('change', [
    'prefix', 'suffix', 'quoted', 'incomplete_instructions', 'incomplete_environment',
    'standalone_environment', 'other_harness', 'ordinary_source', 'assistant', 'modern_metadata', 'closing_in_heading',
])
def test_legacy_context_predicate_keeps_unmatched_and_genuine_user_messages(change):
    from memento.store import legacy_native_context
    source, kind, metadata = NATIVE_SOURCE, 'user', {'extraction_mode': 'message'}
    text = LEGACY_CONTEXTS[0]
    if change == 'prefix':
        text = 'My actual request follows.\n' + text
    elif change == 'suffix':
        text += '\nPlease remember my actual correction.'
    elif change == 'quoted':
        text = '```markdown\n' + text + '\n```'
    elif change == 'incomplete_instructions':
        text = text.replace('</INSTRUCTIONS>', '')
    elif change == 'incomplete_environment':
        text = text.replace('</environment_context>', '')
    elif change == 'standalone_environment':
        text = '<environment_context>cwd: /workspace/example</environment_context>'
    elif change == 'other_harness':
        source = 'native:synthetic-device:pi'
    elif change == 'ordinary_source':
        source = 'user-document'
    elif change == 'assistant':
        kind = 'assistant'
    elif change == 'modern_metadata':
        metadata['native_context_kinds'] = []
    elif change == 'closing_in_heading':
        text = '# AGENTS.md instructions for /workspace/</INSTRUCTIONS>\n\n<INSTRUCTIONS>\nOrdinary incomplete content.'
    assert not legacy_native_context(source, kind, text, metadata)


@pytest.mark.parametrize('change', ['prefix', 'suffix', 'fenced', 'double', 'nested', 'unclosed',
    'source_plan', 'leading_newline', 'other_harness', 'assistant', 'native_context_kinds', 'content_item_kinds'])
def test_goal_context_predicate_keeps_unmatched_and_genuine_user_messages(change):
    from memento.store import legacy_native_context
    source, kind, metadata, text = NATIVE_SOURCE, 'user', {'extraction_mode': 'message'}, GOAL_CONTEXT
    if change == 'prefix':
        text = 'My genuine request.\n' + text
    elif change == 'suffix':
        text += '\nRemember my genuine correction.'
    elif change == 'fenced':
        text = '```text\n' + text + '\n```'
    elif change == 'double':
        text += text
    elif change == 'nested':
        text = text.replace(TEXT, '<codex_internal_context source="plan">' + TEXT)
    elif change == 'unclosed':
        text = text.replace('</codex_internal_context>', '')
    elif change == 'source_plan':
        text = text.replace('source="goal"', 'source="plan"')
    elif change == 'leading_newline':
        text = '\n' + text
    elif change == 'other_harness':
        source = 'native:synthetic-device:pi'
    elif change == 'assistant':
        kind = 'assistant'
    else:
        metadata[change] = ['goal.internal_context']
    assert not legacy_native_context(source, kind, text, metadata)


def test_large_repeated_or_incomplete_legacy_delimiters_are_not_context_only():
    from memento.store import legacy_native_context
    prefix = '# AGENTS.md instructions\n\n<INSTRUCTIONS>'
    for text in (prefix + '<INSTRUCTIONS>' * 30000 + '</INSTRUCTIONS>',
                 prefix + ' ' * 500000, LEGACY_CONTEXTS[0] + '\n' + 'own words ' * 50000,
                 '<codex_internal_context source="goal">' * 30000 + '</codex_internal_context>',
                 '<codex_internal_context source="goal">' + ' ' * 500000):
        assert not legacy_native_context(NATIVE_SOURCE, 'user', text, {})


def test_extraction_filters_more_than_65_legacy_contexts_before_its_window_limit(tmp_path):
    store = Store(tmp_path)
    try:
        processing = Processing(Graph(store))
        for index in range(70):
            store.capture(dict(source=NATIVE_SOURCE, event_id=str(index), conversation_id='c', kind='user',
                text=LEGACY_CONTEXTS[index % len(LEGACY_CONTEXTS)], occurred_at=None, metadata={'extraction_mode': 'message'}))
        genuine, _ = store.capture(dict(source=NATIVE_SOURCE, event_id='genuine', conversation_id='c',
            kind='user', text=TEXT, occurred_at=None, metadata={}))
        work = processing.claim(agent='fixture', timezone_name='UTC',
            compaction=dict(source=NATIVE_SOURCE, conversation_id='c'),
            now=datetime.now(timezone.utc).replace(hour=12))['work'][0]
        assert [window['episode_id'] for window in source_windows(processing, work)] == [genuine['id']]
        result = processing.complete(id=work['id'], owner=work['owner'], agent='fixture',
            output=dict(windows=[dict(id=work['input']['windows'][0]['id'], candidates=drafts())]))
        assert result['candidate_count'] == 5
        assert store._db.execute('SELECT seq=latest_seq FROM processing_progress').fetchone()[0] == 1
        assert len(store.search(source=NATIVE_SOURCE, limit=100)) == 71
    finally:
        store.close()


@pytest.mark.parametrize('mixed', [False, True])
@pytest.mark.parametrize('leased', [False, True])
@pytest.mark.parametrize('text', [LEGACY_CONTEXTS[0], GOAL_CONTEXT], ids=['instructions', 'goal'])
def test_preexisting_queued_and_leased_context_work_completes_without_losing_real_windows(
        tmp_path, monkeypatch, mixed, leased, text):
    from memento.store import legacy_native_context
    store = Store(tmp_path)
    try:
        processing = Processing(Graph(store))
        now = datetime.now(timezone.utc).replace(hour=12)
        context, _ = store.capture(dict(source=NATIVE_SOURCE, event_id='old-context', conversation_id='c',
            kind='user', text=text, occurred_at=None, metadata={}))
        genuine = None
        if mixed:
            genuine, _ = store.capture(dict(source=NATIVE_SOURCE, event_id='real-message', conversation_id='c',
                kind='user', text=TEXT, occurred_at=None, metadata={}))
        processing.compact(NATIVE_SOURCE, 'c')
        # Simulate a job created/leased by the old classifier without altering
        # any immutable raw source or its metadata.
        store._db.create_function('legacy_native_context', 4, lambda *_: False, deterministic=True)
        with monkeypatch.context() as old:
            old.setattr('memento.extraction.legacy_native_context', lambda *_: False)
            processing._enqueue_extract(now)
            job_id = store._db.execute("SELECT id FROM jobs WHERE kind='extract'").fetchone()[0]
            if leased:
                work = processing.claim(agent='fixture', timezone_name='UTC', now=now)['work'][0]
        store._db.create_function('legacy_native_context', 4, legacy_native_context, deterministic=True)
        if not leased:
            claimed = processing.claim(agent='fixture', timezone_name='UTC', now=now)['work']
            if not mixed:
                assert claimed == []
            else:
                work = claimed[0]
                assert [window['episode_id'] for window in source_windows(processing, work)] == [genuine['id']]
        if leased or mixed:
            output = dict(windows=[dict(id=window['id'], candidates=drafts()) for window in work['input']['windows']])
            result = processing.complete(id=work['id'], owner=work['owner'], agent='fixture', output=output)
            assert result['candidate_count'] == (5 if mixed else 0)
        assert processing.queue.get(job_id)['state'] == 'done'
        assert store._db.execute('SELECT seq=latest_seq FROM processing_progress').fetchone()[0] == 1
        assert store._db.execute('SELECT count(*) FROM candidates').fetchone()[0] == (5 if mixed else 0)
        assert not store._db.execute('SELECT 1 FROM graph_evidence WHERE episode_id=?', (context['id'],)).fetchone()
        assert store.get(context['id'])['text'] == text
        assert processing.claim(agent='fixture', timezone_name='UTC', now=now)['work'] == []
    finally:
        store.close()


def test_candidates_preserve_direction_exact_unicode_and_unknown_world_time(extraction):
    store, graph, processing, episode, payload, work, output = extraction
    result = processing.complete(id=work['id'], owner=work['owner'], output=output, agent='fixture')
    assert result['candidate_count'] == 5
    rows = store._db.execute('''SELECT a.text,b.text FROM candidates r
        JOIN candidates a ON a.id=r.src JOIN candidates b ON b.id=r.dst WHERE r.kind='relation'
        ORDER BY a.text''').fetchall()
    assert [tuple(row) for row in rows] == [('Memory', 'Mellanni'), ('Сергей', 'Mellanni')]
    evidence = store._db.execute('SELECT start,end,quote FROM graph_evidence WHERE episode_id=?', (episode['id'],)).fetchall()
    assert all(TEXT[row['start']:row['end']] == row['quote'] for row in evidence)
    assert store._db.execute('SELECT count(*) FROM node_versions').fetchone()[0] == 0
    assert store._db.execute('SELECT count(*) FROM candidates WHERE valid_from IS NOT NULL OR valid_to IS NOT NULL').fetchone()[0] == 0
    run = store._db.execute('SELECT * FROM extraction_runs').fetchone()
    assert json.loads(run['reported_models']) == []
    assert json.loads(run['usage']) is None
    stored, created = store.capture(payload)
    assert not created and stored['id'] == episode['id']
    assert processing.queue.get(work['id'])['state'] == 'done'


def test_committed_derivation_recovers_without_regeneration_after_lost_ack(extraction):
    store, graph, processing, episode, payload, work, output = extraction
    job, context = processing._leased(work['id'], work['owner'])
    result = processing.extractor.apply(job, context, output, agent='fixture')
    store._db.execute('UPDATE jobs SET lease_until=0 WHERE id=?', (job.id,))
    recovered = processing.claim(agent='fixture', timezone_name='UTC', now=datetime.now(timezone.utc).replace(hour=12))
    assert recovered['work'] == []
    assert processing.queue.get(job.id)['state'] == 'done'
    assert processing.queue.get(job.id)['result'] == result
    assert store._db.execute('SELECT count(*) FROM candidates').fetchone()[0] == 5


@pytest.mark.parametrize('kind', ['memory', 'entity', 'relation'])
@pytest.mark.parametrize('scope', [['personal'], ['professional'], ['personal', 'professional']],
                         ids=['personal', 'professional', 'both'])
def test_claim_schema_matches_candidate_scope_cardinality(extraction, kind, scope):
    from jsonschema import Draft202012Validator
    _, _, _, _, _, work, output = extraction
    candidates = output['windows'][0]['candidates']
    candidates.append(draft('fact', 'memory', 'knowledge', TEXT, TEXT))
    next(item for item in candidates if item['kind'] == kind)['scope'] = scope
    Draft202012Validator.check_schema(work['schema'])
    assert Draft202012Validator(work['schema']).is_valid(output) is (kind == 'entity' or len(scope) == 1)


@pytest.mark.parametrize('kind', ['memory', 'entity', 'relation'])
@pytest.mark.parametrize('scope', [['personal'], ['professional'], ['personal', 'professional']],
                         ids=['personal', 'professional', 'both'])
def test_extraction_completion_matches_candidate_scope_cardinality(extraction, kind, scope):
    store, _, processing, _, _, work, output = extraction
    candidates = output['windows'][0]['candidates']
    candidates.append(draft('fact', 'memory', 'knowledge', TEXT, TEXT))
    selected = next(item for item in candidates if item['kind'] == kind)
    selected['scope'] = scope
    if kind != 'entity' and len(scope) == 2:
        with pytest.raises(ValueError, match='^candidate_schema_invalid$'):
            processing.complete(id=work['id'], owner=work['owner'], agent='fixture', output=output)
        assert store._db.execute('SELECT count(*) FROM candidates').fetchone()[0] == 0
        assert store._db.execute('SELECT count(*) FROM extraction_runs').fetchone()[0] == 0
        assert processing.queue.get(work['id'])['state'] == 'leased'
    else:
        result = processing.complete(id=work['id'], owner=work['owner'], agent='fixture', output=output)
        assert result['candidate_count'] == len(candidates)
        stored = store._db.execute('''SELECT scope FROM candidates WHERE kind=? AND text=?
            ORDER BY rowid LIMIT 1''', (kind, selected['text'])).fetchone()
        assert json.loads(stored['scope']) == sorted(scope)
        assert processing.queue.get(work['id'])['state'] == 'done'


@pytest.mark.parametrize('failure', ['invented_quote', 'dangling_relation', 'ambiguous_quote', 'wrong_confidence_type'])
def test_invalid_candidate_batch_stays_uncommitted_and_retriable(extraction, failure):
    store, graph, processing, episode, payload, work, output = extraction
    candidates = output['windows'][0]['candidates']
    if failure == 'invented_quote':
        candidates[0]['quote'] = 'not in the source'
        expected = 'candidate_quote_not_in_source'
    elif failure == 'dangling_relation':
        candidates[-1]['dst_key'] = 'absent'
        expected = 'candidate_relation_endpoint_missing'
    elif failure == 'ambiguous_quote':
        candidates[1]['quote'] = 'Mellanni'
        expected = 'candidate_quote_ambiguous'
    else:
        candidates[0]['confidence'] = '0.8'
        expected = 'candidate_schema_invalid'
    with pytest.raises(ValueError, match=expected):
        processing.complete(id=work['id'], owner=work['owner'], output=output, agent='fixture')
    assert processing.queue.get(work['id'])['state'] == 'leased'
    assert store._db.execute('SELECT count(*) FROM candidates').fetchone()[0] == 0
    assert store._db.execute('SELECT count(*) FROM extraction_runs').fetchone()[0] == 0


def test_empty_night_can_consolidate_candidates_extracted_later_that_night(extraction):
    store, graph, processing, episode, payload, work, output = extraction
    night = datetime(2026, 10, 18, 3, tzinfo=timezone.utc)
    assert processing.claim(agent='fixture', timezone_name='UTC', now=night)['work'] == []
    processing.complete(id=work['id'], owner=work['owner'], output=output, agent='fixture')
    consolidation = processing.claim(agent='fixture', timezone_name='UTC', now=night)['work']
    assert len(consolidation) == 1 and consolidation[0]['kind'] == 'consolidate'
    task = consolidation[0]
    assert len(task['input']['candidates']) == 5
    actions = [dict(candidate_id=candidate['id'], operation='reject', type=None, text=None,
        scope=None, open_loop=None, target=None, src=None, dst=None, relation=None,
        change_kind=None, valid_at=None, valid_from=None, valid_to=None, category=None,
        sensitive=None, evidence=[], why='Synthetic batch intentionally rejected')
        for candidate in task['input']['candidates']]
    result = processing.complete(id=task['id'], owner=task['owner'],
                                output=dict(actions=actions), agent='fixture')
    assert result['candidate_count'] == 5
    assert all(graph.history(candidate['id'])[0]['decisions'][-1]['decision'] == 'reject'
               for candidate in task['input']['candidates'])


@pytest.mark.parametrize('kind,metadata', [
    ('tool', {}),
    ('turn', {'extraction_mode': 'tool_output'}),
])
def test_huge_original_tool_output_projects_only_first_2000_once(tmp_path, kind, metadata):
    store = Store(tmp_path)
    try:
        graph = Graph(store)
        processing = Processing(graph)
        text = '😀' + 'x' * 1999 + 'MUST NOT ENTER EXTRACTION' + 'tail ' * 12000
        episode, _ = store.capture(dict(source='tool-fixture', event_id='huge', conversation_id='c',
            kind=kind, text=text, occurred_at=None, metadata=metadata))
        now = datetime.now(timezone.utc).replace(hour=12)
        tasks = processing.claim(agent='fixture', timezone_name='UTC',
            compaction=dict(source='tool-fixture', conversation_id='c'), now=now)['work']
        assert len(tasks) == 1
        task = tasks[0]
        windows = source_windows(processing, task)
        assert len(windows) == 1
        assert windows[0]['text'] == text[:2000]
        assert (windows[0]['start'], windows[0]['end']) == (0, 2000)
        processing.complete(id=task['id'], owner=task['owner'],
            output=dict(windows=[dict(id=windows[0]['id'], candidates=[])]), agent='fixture')
        assert processing.claim(agent='fixture', timezone_name='UTC', now=now)['work'] == []
        assert store.get(episode['id'])['text'] == text
    finally:
        store.close()


def test_legacy_serialized_image_payload_is_skipped_without_losing_exact_text_offsets(tmp_path):
    store = Store(tmp_path)
    try:
        graph = Graph(store)
        processing = Processing(graph)
        caption = 'The release checklist is cobalt, with exact Unicode 😀 evidence.'
        raw = json.dumps([{'type': 'image', 'source': {'type': 'base64',
            'media_type': 'image/png', 'data': 'Z' * (1024 * 1024)}},
            {'type': 'text', 'text': caption}], ensure_ascii=False)
        episode, _ = store.capture(dict(source='legacy-image', event_id='image', conversation_id='c',
            kind='user', text=raw, occurred_at=None, metadata={'extraction_mode': 'message'}))
        args = dict(agent='fixture', timezone_name='UTC', now=datetime.now(timezone.utc).replace(hour=12))
        task = processing.claim(**args, compaction=dict(source='legacy-image', conversation_id='c'))['work'][0]
        windows = task['input']['windows']
        assert len(windows) == 2 and sum(len(window['text']) for window in windows) < 400
        assert all('Z' * 256 not in window['text'] for window in windows)
        outputs = []
        for window in windows:
            candidate = draft('caption', 'memory', 'knowledge', caption, caption) if caption in window['text'] else None
            outputs.append(dict(id=window['id'], candidates=[candidate] if candidate else []))
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(windows=outputs))
        evidence = store._db.execute('SELECT start,end,quote FROM graph_evidence').fetchone()
        assert raw[evidence['start']:evidence['end']] == caption
        assert store.get(episode['id'])['text'] == raw
        assert processing.claim(**args)['work'] == []
    finally:
        store.close()


@pytest.mark.parametrize('message_size', [32, 1000])
def test_interleaved_skip_envelopes_pack_eligible_messages_with_exact_coverage(tmp_path, message_size):
    store = Store(tmp_path)
    try:
        processing = Processing(Graph(store))
        expected = {}
        for index in range(70):
            store.capture(dict(source='packing', event_id=f'raw-{index}', conversation_id='c',
                kind='native_record', text='Untrusted envelope ' * 100,
                occurred_at=None, metadata={'extraction_mode': 'skip'}))
            episode, _ = store.capture(dict(source='packing', event_id=f'message-{index}', conversation_id='c',
                kind='user', text=f'{index}:😀'.ljust(message_size, 'x'), occurred_at=None, metadata={}))
            expected[episode['id']] = episode['text']
        store.capture(dict(source='packing', event_id='trailing-raw', conversation_id='c',
            kind='native_hook', text='Trailing envelope', occurred_at=None, metadata={'extraction_mode': 'skip'}))
        args = dict(agent='fixture', timezone_name='UTC',
            now=datetime.now(timezone.utc).replace(hour=12),
            compaction=dict(source='packing', conversation_id='c'))
        seen, jobs = [], 0
        while tasks := processing.claim(**args)['work']:
            assert len(tasks) == 1 and tasks[0]['kind'] == 'extract'
            task = tasks[0]
            windows = source_windows(processing, task)
            assert len(windows) <= 64
            assert sum(len(window['text']) for window in windows) <= 48000
            assert all(window['text'] == expected[window['episode_id']] for window in windows)
            output = dict(windows=[dict(id=window['id'], candidates=[]) for window in windows])
            with pytest.raises(ValueError, match='candidate_window_coverage_invalid'):
                processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
                    output=dict(windows=output['windows'][:-1]))
            processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=output)
            seen.extend(window['episode_id'] for window in windows)
            jobs += 1
            assert jobs <= 2
        assert jobs == 2
        assert len(seen) == len(set(seen)) == len(expected)
        assert set(seen) == set(expected)
        progress = store._db.execute("SELECT * FROM processing_progress WHERE source='packing'").fetchone()
        assert progress['seq'] == progress['latest_seq'] and progress['offset'] == 0
        assert processing.claim(**args)['work'] == []
        assert store._db.execute("SELECT count(*) FROM jobs WHERE kind='extract'").fetchone()[0] == jobs
    finally:
        store.close()


@pytest.mark.parametrize('eligible', [False, True])
def test_skip_only_and_trailing_ranges_advance_only_through_compaction_boundary(tmp_path, eligible):
    store = Store(tmp_path)
    try:
        processing = Processing(Graph(store))
        if eligible:
            store.capture(dict(source='skip-fixture', event_id='message', conversation_id='c',
                kind='user', text='Eligible evidence', occurred_at=None, metadata={}))
        for index in range(90):
            store.capture(dict(source='skip-fixture', event_id=f'raw-{index}', conversation_id='c',
                kind='native_hook', text='Ignored hook', occurred_at=None, metadata={'extraction_mode': 'skip'}))
        processing.compact('skip-fixture', 'c')
        boundary = store._db.execute('SELECT max(seq) FROM episodes').fetchone()[0]
        # Forcing noon can age later captures past the inactivity threshold.
        args = dict(agent='fixture', timezone_name='UTC', now=datetime.now(timezone.utc))
        tasks = processing.claim(**args)['work']
        assert len(tasks) == int(eligible)
        later, _ = store.capture(dict(source='skip-fixture', event_id='after-boundary', conversation_id='c',
            kind='user', text='Captured after the leased boundary', occurred_at=None, metadata={}))
        if tasks:
            task = tasks[0]
            processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
                output=dict(windows=[dict(id=window['id'], candidates=[]) for window in task['input']['windows']]))
        progress = store._db.execute("SELECT * FROM processing_progress WHERE source='skip-fixture'").fetchone()
        assert progress['seq'] == boundary and progress['offset'] == 0
        assert processing.claim(**args)['work'] == []
        task = processing.claim(**args, compaction=dict(source='skip-fixture', conversation_id='c'))['work'][0]
        assert [window['episode_id'] for window in source_windows(processing, task)] == [later['id']]
        store.capture(dict(source='skip-fixture', event_id='new-trailing-raw', conversation_id='c',
            kind='native_hook', text='Captured during lease', occurred_at=None, metadata={'extraction_mode': 'skip'}))
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
            output=dict(windows=[dict(id=window['id'], candidates=[]) for window in task['input']['windows']]))
        jobs = store._db.execute("SELECT count(*) FROM jobs WHERE kind='extract'").fetchone()[0]
        for _ in range(3):
            assert processing.claim(**args, compaction=dict(source='skip-fixture', conversation_id='c'))['work'] == []
        progress = store._db.execute("SELECT * FROM processing_progress WHERE source='skip-fixture'").fetchone()
        assert progress['seq'] == progress['latest_seq'] and progress['offset'] == 0
        assert store._db.execute("SELECT count(*) FROM jobs WHERE kind='extract'").fetchone()[0] == jobs
    finally:
        store.close()


def test_candidate_loop_keeps_explicit_reminder_and_deferral_through_promotion(tmp_path):
    store = Store(tmp_path)
    try:
        graph = Graph(store)
        processing = Processing(graph)
        text = 'Personally, I owe Alex the report.'
        episode, _ = store.capture(dict(source='loop-fixture', event_id='promise',
            conversation_id='c', kind='user', text=text, occurred_at=None, metadata={}))
        now = datetime.now(timezone.utc).replace(hour=12)
        task = processing.claim(agent='fixture', timezone_name='UTC',
            compaction=dict(source='loop-fixture', conversation_id='c'), now=now)['work'][0]
        obligation = draft('promise', 'memory', 'knowledge', text, text)
        obligation.update(scope=['personal'], open_loop=True)
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
            output=dict(windows=[dict(id=task['input']['windows'][0]['id'], candidates=[obligation])]))
        marked = graph.loop(action='list', scope='personal')['loops']
        assert len(marked) == 1 and marked[0]['tier'] == 'candidate'
        assert marked[0]['confirmed'] is False and marked[0]['due_at'] is None
        candidate_id = marked[0]['id']
        graph.loop(action='remind', id=candidate_id, due_at='2030-10-20T15:00:00+03:00',
                   command_id='explicit-reminder')
        deferred = graph.loop(action='later', id=candidate_id, command_id='defer-candidate')
        task = processing.claim(agent='fixture', timezone_name='UTC',
            consolidate_now=True, now=now)['work'][0]
        candidate = task['input']['candidates'][0]
        assert candidate['scope'] == ['personal']
        action = dict(candidate_id=candidate['id'], operation='remember', type='knowledge',
            text=text, scope=['personal'], open_loop=True, target=None,
            src=None, dst=None, relation=None, change_kind=None, valid_at=None,
            valid_from=None, valid_to=None, category=None, sensitive=None,
            evidence=[dict(episode_id=episode['id'], quote=text, occurrence=None)],
            why='Original promise explicitly supplies the pending obligation')
        result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
                                     output=dict(actions=[action]))
        node = result['results'][candidate['id']]['id']
        assert graph.loop(action='list')['loops'] == []
        assert graph.history(node)[0]['evidence'][0]['quote'] == text
        reopened = graph.loop(action='open', id=node, command_id='surface-promoted-loop')
        assert reopened['due_at'] == '2030-10-20T12:00:00+00:00'
        assert deferred['deferred_until'] is not None
        assert [row['id'] for row in graph.loop(action='list', scope='personal')['loops']] == [node]
        assert graph.loop(action='list', scope='professional')['loops'] == []
    finally:
        store.close()


def _loop_action(candidate, episode, *, operation='remember', target=None, open_loop=True, **extra):
    return dict(candidate_id=candidate['id'], operation=operation,
        type='knowledge' if operation == 'remember' else None,
        text=candidate['text'] if operation == 'remember' else None,
        scope=['professional'] if operation == 'remember' else None,
        open_loop=open_loop, target=target, src=None, dst=None, relation=None,
        change_kind=None, valid_at=None, valid_from=None, valid_to=None,
        category=None, sensitive=None,
        evidence=[dict(episode_id=episode['id'], quote=episode['text'], occurrence=None)],
        why='Original source determines the obligation state', **extra)


@pytest.mark.parametrize('open_loop,choice,target_choice', [
    (False, None, None), (None, None, None),
    (False, 'open', None), (False, 'later', None),
    (False, 'remind', None), (False, 'close', None),
    (False, None, 'open'),
])
def test_consolidation_declines_only_bare_extracted_loop_flags(extraction, open_loop, choice, target_choice):
    from memento.loops import current
    store, graph, processing, episode, _, work, output = extraction
    obligation = draft('pending', 'memory', 'knowledge', TEXT, TEXT)
    obligation['open_loop'] = True
    output['windows'][0]['candidates'] = [obligation]
    processing.complete(id=work['id'], owner=work['owner'], output=output, agent='fixture')
    candidate_id = store._db.execute('SELECT id FROM candidates').fetchone()[0]
    if choice:
        graph.loop(action=choice, id=candidate_id, command_id='candidate-choice',
                   **(dict(due_at='2050-01-01T00:00:00Z') if choice == 'remind' else {}))
    before = current(graph, candidate_id)
    target = None
    if target_choice:
        target = graph.remember(command_id='existing-obligation', actor='user', why='Known pending work',
            type='knowledge', text=TEXT, scope=['professional'],
            evidence=[dict(episode_id=episode['id'], quote=TEXT)])['id']
        graph.loop(action=target_choice, id=target, command_id='target-choice')
    target_before = current(graph, target) if target else None
    task = processing.claim(agent='fixture', timezone_name='UTC', consolidate_now=True)['work'][0]
    action = _loop_action(task['input']['candidates'][0], episode,
        operation='duplicate' if target else 'remember', target=target, open_loop=open_loop)
    result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[action]))
    destination = result['results'][candidate_id]['id']
    assert current(graph, candidate_id)['state'] == 'closed'
    if target:
        assert current(graph, destination) == target_before
    elif open_loop is False and choice is None:
        assert current(graph, destination) is None
        assert graph.loop(action='list')['loops'] == []
    else:
        assert current(graph, destination) == dict(before, id=destination)


def test_same_batch_duplicate_loops_share_one_promoted_obligation(extraction):
    from memento.loops import current
    store, graph, processing, episode, _, work, output = extraction
    obligations = [draft(key, 'memory', 'knowledge', text, TEXT)
                   for key, text in [('first', 'The report is pending.'), ('repeat', 'Waiting for that report.')]]
    for item in obligations:
        item['open_loop'] = True
    output['windows'][0]['candidates'] = obligations
    processing.complete(id=work['id'], owner=work['owner'], output=output, agent='fixture')
    task = processing.claim(agent='fixture', timezone_name='UTC', consolidate_now=True)['work'][0]
    first, repeat = task['input']['candidates']
    result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[
        _loop_action(first, episode),
        _loop_action(repeat, episode, operation='duplicate', target=first['id'])]))
    target = result['results'][first['id']]['id']
    assert result['results'][repeat['id']]['id'] == target
    assert [item['id'] for item in graph.loop(action='list')['loops']] == [target]
    assert all(current(graph, item['id'])['state'] == 'closed' for item in (first, repeat))


@pytest.mark.parametrize('previous', [None, 'consolidate-v4'])
def test_legacy_consolidation_lease_reprepares_same_job_before_loop_changes(extraction, previous):
    from memento.loops import current
    from memento.store import ConflictError
    store, graph, processing, episode, _, work, output = extraction
    obligation = draft('pending', 'memory', 'knowledge', TEXT, TEXT)
    obligation['open_loop'] = True
    output['windows'][0]['candidates'] = [obligation]
    processing.complete(id=work['id'], owner=work['owner'], output=output, agent='fixture')
    task = processing.claim(agent='fixture', timezone_name='UTC', consolidate_now=True)['work'][0]
    action = _loop_action(task['input']['candidates'][0], episode, open_loop=False)
    legacy = dict(task['input'])
    if previous is None:
        legacy.pop('loop_policy_version')
    else:
        legacy['loop_policy_version'] = previous
    payload = json.loads(store._db.execute('SELECT payload FROM jobs WHERE id=?', (task['id'],)).fetchone()[0])
    payload['version'] = previous or 'consolidate-v3'
    store._db.execute('UPDATE jobs SET payload=?,context=? WHERE id=?',
                      (json.dumps(payload), json.dumps(legacy), task['id']))
    commits = store._db.execute('SELECT count(*) FROM graph_commits').fetchone()[0]
    with pytest.raises(ConflictError, match='loop policy changed'):
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[action]))
    assert store._db.execute('SELECT count(*) FROM graph_commits').fetchone()[0] == commits
    assert current(graph, action['candidate_id'])['state'] == 'open'
    store._db.execute('UPDATE jobs SET lease_until=0 WHERE id=?', (task['id'],))
    fresh = processing.claim(agent='fixture', timezone_name='UTC', consolidate_now=True)['work'][0]
    assert fresh['id'] == task['id'] and fresh['owner'] != task['owner']
    assert fresh['input']['loop_policy_version'] == 'consolidate-v6'
    result = processing.complete(id=fresh['id'], owner=fresh['owner'], agent='fixture', output=dict(actions=[action]))
    assert current(graph, result['results'][action['candidate_id']]['id']) is None


def test_empty_consolidation_job_finishes_without_model_work(tmp_path):
    store = Store(tmp_path)
    try:
        processing = Processing(Graph(store))
        job_id = processing.queue._insert('empty-consolidation', 'consolidate',
            dict(version='consolidate-v3', night=None, candidate_ids=[], next_cursor=0))
        result = processing.claim(agent='fixture', timezone_name='UTC', consolidate_now=True)
        assert result['work'] == []
        job = processing.queue.get(job_id)
        assert job['state'] == 'done' and job['result']['candidate_count'] == 0
    finally:
        store.close()


@pytest.fixture
def loop_completion(tmp_path, monkeypatch):
    clock = {'now': datetime(2030, 1, 1, tzinfo=timezone.utc)}

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock['now'] if tz is not None else clock['now'].replace(tzinfo=None)

    monkeypatch.setattr('memento.graph.datetime', Clock)
    monkeypatch.setattr('memento.loops.datetime', Clock)
    stores = []

    def prepare(*, pending_at='2020-01-01T00:00:00Z', completed_at='2035-01-01T00:00:00Z',
                choice='open', text='I sent Alex the report.', include_pending=False, inherited_choice=False):
        store = Store(tmp_path)
        stores.append(store)
        graph = Graph(store)
        original, _ = store.capture(dict(source='completion-fixture', event_id='pending', conversation_id='c',
            kind='user', text='I owe Alex the report.', occurred_at=pending_at, metadata={}))
        processing = Processing(graph)
        if inherited_choice == 'promotion':
            task = processing.claim(agent='fixture', timezone_name='UTC', now=clock['now'],
                compaction=dict(source='completion-fixture', conversation_id='c'))['work'][0]
            pending = draft('pending', 'memory', 'knowledge', original['text'], original['text'])
            pending['open_loop'] = True
            result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
                output=dict(windows=[dict(id=w['id'], candidates=[pending]) for w in task['input']['windows']]))
            target = store._db.execute('SELECT id FROM candidates').fetchone()[0]
        else:
            target = graph.remember(command_id='known-report', actor='user', why='Source-supported pending report',
                type='knowledge', text=original['text'], scope=['professional'],
                evidence=[dict(episode_id=original['id'], quote=original['text'])])['id']
        graph.loop(action=choice, id=target, command_id='manual-choice',
                   **(dict(due_at='2050-01-01T00:00:00Z') if choice == 'remind' else {}))
        if inherited_choice == 'promotion':
            task = processing.claim(agent='fixture', timezone_name='UTC', consolidate_now=True)['work'][0]
            result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
                output=dict(actions=[_loop_action(task['input']['candidates'][0], original)]))
            target = result['results'][target]['id']
        elif inherited_choice == 'refinement':
            target = graph.supersede(command_id='refined-obligation', actor='user', why='Same pending obligation',
                old=target, new=dict(type='knowledge', text='The report for Alex remains pending.', scope=['professional']),
                kind='refinement', evidence=[dict(episode_id=original['id'], quote=original['text'])])['superseded_by']
        clock['now'] = datetime(2040, 1, 1, tzinfo=timezone.utc)
        source, _ = store.capture(dict(source='completion-fixture', event_id='outcome', conversation_id='c',
            kind='user', text=text, occurred_at=completed_at, metadata={}))
        task = processing.claim(agent='fixture', timezone_name='UTC', now=clock['now'],
            compaction=dict(source='completion-fixture', conversation_id='c'))['work'][0]
        candidate = draft('outcome', 'memory', 'knowledge', text, text)
        pending = draft('pending', 'memory', 'knowledge', original['text'], original['text'])
        pending['open_loop'] = True
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
            output=dict(windows=[dict(id=window['id'], candidates=[candidate]
                if window['episode_id'] == source['id'] else [pending] if include_pending else [])
                for window in source_windows(processing, task)]))
        # Only neighbor discovery is controlled; prepare/apply use real snapshots,
        # original source spans, leases, graph commits and loop history.
        monkeypatch.setattr(graph, 'recall_keywords', lambda *_args, **_kwargs: [dict(id=target)])
        task = processing.claim(agent='fixture', timezone_name='UTC', consolidate_now=True,
                                now=clock['now'])['work'][0]
        return store, graph, processing, target, source, task

    yield prepare
    for store in stores:
        store.close()


@pytest.mark.parametrize('pending_at,completed_at,choice,text,status', [
    ('2020-01-01T00:00:00Z', '2035-01-01T00:00:00Z', 'open', 'I sent Alex the report.', 'closed'),
    ('2020-01-01T00:00:00Z', '2035-01-01T00:00:00Z', 'later', 'I cancelled the report I owed Alex.', 'closed'),
    ('2020-01-01T00:00:00Z', '2035-01-01T00:00:00Z', 'remind', 'I sent Alex the report.', 'closed'),
    ('2020-01-01T00:00:00Z', '2025-01-01T00:00:00Z', 'open', 'I sent Alex the report.', 'not_newer'),
    ('2020-01-01T00:00:00Z', '2030-01-01T00:00:00Z', 'open', 'I sent Alex the report.', 'not_newer'),
    ('2038-01-01T00:00:00Z', '2035-01-01T00:00:00Z', 'open', 'I sent Alex the report.', 'not_newer'),
    ('2020-01-01T00:00:00Z', None, 'open', 'I sent Alex the report.', 'chronology_unknown'),
    (None, '2035-01-01T00:00:00Z', 'open', 'I sent Alex the report.', 'chronology_unknown'),
])
def test_consolidation_closes_only_from_strictly_newer_original_outcomes(
        loop_completion, pending_at, completed_at, choice, text, status):
    from memento.loops import current, _closed_at
    store, graph, processing, target, source, task = loop_completion(
        pending_at=pending_at, completed_at=completed_at, choice=choice, text=text)
    before = current(graph, target)
    assert next(w for w in task['input']['raw_windows'] if w['episode_id'] == source['id'])['occurred_at'] == completed_at
    action = _loop_action(task['input']['candidates'][0], source,
                         operation='duplicate', target=target, open_loop=False, closed_targets=[target])
    result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[action]))
    assert result['loop_closures'][0]['status'] == status
    assert current(graph, target)['state'] == ('closed' if status == 'closed' else 'open')
    assert current(graph, target)['due_at'] == before['due_at']
    if status == 'closed':
        assert datetime.fromisoformat(_closed_at(graph, target)) == datetime.fromisoformat(completed_at)
    else:
        assert current(graph, target) == before
    assert store.get(source['id'])['text'] == text
    assert graph.history(target)[-1]['evidence'][-1]['quote'] in (text, 'I owe Alex the report.')


def test_consolidation_closure_does_not_override_intervening_human_choice(loop_completion):
    from memento.loops import current
    from memento.store import ConflictError
    store, graph, processing, target, source, task = loop_completion()
    graph.loop(action='later', id=target, command_id='newer-human-choice')
    before = current(graph, target)
    version = graph._node(target)['version']
    commits = store._db.execute('SELECT count(*) FROM graph_commits').fetchone()[0]
    action = _loop_action(task['input']['candidates'][0], source,
                         operation='duplicate', target=target, open_loop=False, closed_targets=[target])
    with pytest.raises(ConflictError, match='loop state changed'):
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[action]))
    assert current(graph, target) == before and graph._node(target)['version'] == version
    assert store._db.execute('SELECT count(*) FROM graph_commits').fetchone()[0] == commits
    store._db.execute('UPDATE jobs SET lease_until=0 WHERE id=?', (task['id'],))
    fresh = processing.claim(agent='fixture', timezone_name='UTC', consolidate_now=True)['work'][0]
    assert fresh['id'] == task['id']
    result = processing.complete(id=fresh['id'], owner=fresh['owner'], agent='fixture', output=dict(actions=[action]))
    assert result['loop_closures'][0]['status'] == 'not_newer'
    assert current(graph, target) == before


def test_same_batch_closure_alias_preserves_existing_target_choice(loop_completion):
    from memento.loops import current
    from memento.store import ConflictError
    store, graph, processing, target, source, task = loop_completion(include_pending=True)
    pending, outcome = task['input']['candidates']
    original = store.get(pending['evidence'][0]['episode_id'])
    actions = [_loop_action(pending, original, operation='duplicate', target=target),
               _loop_action(outcome, source, operation='duplicate', target=target,
                            open_loop=False, closed_targets=[pending['id']])]
    graph.loop(action='later', id=target, command_id='alias-human-choice')
    before = current(graph, target)
    version = graph._node(target)['version']
    with pytest.raises(ConflictError, match='loop state changed'):
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=actions))
    assert current(graph, target) == before and graph._node(target)['version'] == version
    store._db.execute('UPDATE jobs SET lease_until=0 WHERE id=?', (task['id'],))
    fresh = processing.claim(agent='fixture', timezone_name='UTC', consolidate_now=True)['work'][0]
    result = processing.complete(id=fresh['id'], owner=fresh['owner'], agent='fixture', output=dict(actions=actions))
    assert result['loop_closures'][0]['target'] == target
    assert result['loop_closures'][0]['status'] == 'not_newer'
    assert current(graph, target) == before


@pytest.mark.parametrize('completed_at,status', [
    ('2025-01-01T00:00:00Z', 'not_newer'), ('2035-01-01T00:00:00Z', 'closed')])
@pytest.mark.parametrize('inherited_choice', ['promotion', 'refinement'])
def test_inherited_manual_loop_choice_keeps_original_time(loop_completion, completed_at, status, inherited_choice):
    from memento.loops import current
    _, graph, processing, target, source, task = loop_completion(
        inherited_choice=inherited_choice, completed_at=completed_at)
    assert datetime.fromisoformat(task['input']['loop_snapshots'][target]['manual_choice_at']).year == 2030
    action = _loop_action(task['input']['candidates'][0], source,
                         operation='duplicate', target=target, open_loop=False, closed_targets=[target])
    result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[action]))
    assert result['loop_closures'][0]['status'] == status
    assert current(graph, target)['state'] == ('closed' if status == 'closed' else 'open')


def test_consolidated_closure_reopens_from_later_source_after_reload(loop_completion, monkeypatch, tmp_path):
    from memento.loops import current
    store, graph, processing, target, source, task = loop_completion()
    action = _loop_action(task['input']['candidates'][0], source,
                         operation='duplicate', target=target, open_loop=False, closed_targets=[target])
    processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[action]))
    store.close()
    store = Store(tmp_path)
    try:
        graph = Graph(store)
        processing = Processing(graph)
        assert current(graph, target)['state'] == 'closed'
        later, _ = store.capture(dict(source='completion-fixture', event_id='new-obligation', conversation_id='c',
            kind='user', text='I owe Alex a follow-up report.', occurred_at='2036-01-01T00:00:00Z', metadata={}))
        task = processing.claim(agent='fixture', timezone_name='UTC',
            compaction=dict(source='completion-fixture', conversation_id='c'))['work'][0]
        candidate = draft('later-promise', 'memory', 'knowledge', later['text'], later['text'])
        candidate['open_loop'] = True
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
            output=dict(windows=[dict(id=w['id'], candidates=[candidate]) for w in task['input']['windows']]))
        monkeypatch.setattr(graph, 'recall_keywords', lambda *_args, **_kwargs: [dict(id=target)])
        task = processing.claim(agent='fixture', timezone_name='UTC', consolidate_now=True)['work'][0]
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[
            _loop_action(task['input']['candidates'][0], later, operation='duplicate', target=target)]))
        assert current(graph, target)['state'] == 'open'
        original = next(item for item in graph.history(target) if item['id'] == target and item['version'] == 1)
        assert [event['state'] for event in original['loop_events']] == ['open', 'closed', 'open']
    finally:
        store.close()


@pytest.mark.parametrize('text', ['I might send Alex the report.', 'I sent Jordan another report.'])
def test_uncertain_or_other_obligation_does_not_close_without_explicit_reconciliation(loop_completion, text):
    from memento.loops import current
    _, graph, processing, target, source, task = loop_completion(text=text)
    before = current(graph, target)
    result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[
        _loop_action(task['input']['candidates'][0], source, operation='duplicate', target=target, open_loop=False)]))
    assert result['loop_closures'] == [] and current(graph, target) == before


@pytest.mark.parametrize('defect', ['unprovided', 'contradictory'])
def test_consolidation_rejects_invalid_closure_plans_atomically(loop_completion, defect):
    from memento.loops import current
    _, graph, processing, target, source, task = loop_completion()
    before = current(graph, target)
    action = _loop_action(task['input']['candidates'][0], source, operation='duplicate', target=target,
        open_loop=defect == 'contradictory', closed_targets=['not-supplied'] if defect == 'unprovided' else [target])
    version = graph._node(target)['version']
    with pytest.raises(ValueError, match='consolidation_loop_'):
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[action]))
    assert current(graph, target) == before and graph._node(target)['version'] == version


def test_same_batch_completion_closes_resolved_candidate_without_merging_other_work(tmp_path):
    from memento.loops import current
    store = Store(tmp_path)
    try:
        graph = Graph(store)
        processing = Processing(graph)
        episodes = []
        for event, text, at in [('pending', 'I owe Alex the report.', '2020-01-01T00:00:00Z'),
                                ('done', 'I sent Alex the report.', '2035-01-01T00:00:00Z'),
                                ('other', 'Brian will send the videos I need.', '2035-01-01T00:00:00Z')]:
            episodes.append(store.capture(dict(source='same-batch', event_id=event, conversation_id='c',
                kind='user', text=text, occurred_at=at, metadata={}))[0])
        task = processing.claim(agent='fixture', timezone_name='UTC',
            compaction=dict(source='same-batch', conversation_id='c'))['work'][0]
        drafts_by_id = {}
        for episode in episodes:
            item = draft(episode['event_id'], 'memory', 'knowledge', episode['text'], episode['text'])
            item['open_loop'] = episode['event_id'] != 'done'
            drafts_by_id[episode['id']] = item
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(windows=[
            dict(id=w['id'], candidates=[drafts_by_id[w['episode_id']]]) for w in source_windows(processing, task)]))
        task = processing.claim(agent='fixture', timezone_name='UTC', consolidate_now=True)['work'][0]
        pending, done, other = [next(c for c in task['input']['candidates'] if c['text'] == e['text']) for e in episodes]
        result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[
            _loop_action(done, episodes[1], open_loop=False, closed_targets=[pending['id']]),
            _loop_action(pending, episodes[0]), _loop_action(other, episodes[2])]))
        target = result['results'][pending['id']]['id']
        unrelated = result['results'][other['id']]['id']
        assert current(graph, target)['state'] == 'closed'
        assert [item['id'] for item in graph.loop(action='list')['loops']] == [unrelated]
        assert result['loop_closures'][0]['target'] == target
    finally:
        store.close()


def test_completion_closes_superseding_destination_without_losing_lineage(loop_completion):
    from memento.loops import current
    _, graph, processing, target, source, task = loop_completion()
    action = _loop_action(task['input']['candidates'][0], source, operation='supersede',
                         target=target, open_loop=False, closed_targets=[target])
    action.update(type='knowledge', text=source['text'], scope=['professional'],
                  change_kind='change', valid_at=source['occurred_at'])
    result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=[action]))
    replacement = result['results'][action['candidate_id']]['superseded_by']
    assert current(graph, replacement)['state'] == 'closed'
    assert result['loop_closures'][0]['target'] == replacement
    assert {item['id'] for item in graph.history(replacement)} == {target, replacement}
    assert graph.loop(action='list')['loops'] == []


def test_partial_message_cursor_keeps_overlap_and_newly_captured_messages(tmp_path):
    store = Store(tmp_path)
    try:
        processing = Processing(Graph(store))
        tool_text = '😀' + 'raw tool output ' * 10000
        tool, _ = store.capture(dict(source='cursor-fixture', event_id='tool',
            conversation_id='c', kind='tool', text=tool_text, occurred_at=None, metadata={}))
        prefix, _ = store.capture(dict(source='cursor-fixture', event_id='ignored-prefix',
            conversation_id='c', kind='native_hook', text='Ignored envelope',
            occurred_at=None, metadata={'extraction_mode': 'skip'}))
        text = '😀' + 'long evidence ' * 5000
        original, _ = store.capture(dict(source='cursor-fixture', event_id='long',
            conversation_id='c', kind='user', text=text, occurred_at=None, metadata={}))
        now = datetime.now(timezone.utc).replace(hour=12)
        args = dict(agent='fixture', timezone_name='UTC', now=now,
                    compaction=dict(source='cursor-fixture', conversation_id='c'))
        task = processing.claim(**args)['work'][0]
        first = source_windows(processing, task)
        assert first[0]['episode_id'] == tool['id'] and first[0]['text'] == tool_text[:2000]
        long_first = [window for window in first if window['episode_id'] == original['id']]
        assert max(window['end'] for window in long_first) < len(text)
        projected_characters = sum(len(window['text']) for window in first)
        assert 48000 - 12000 < projected_characters <= 48000
        later, _ = store.capture(dict(source='cursor-fixture', event_id='new-during-lease',
            conversation_id='c', kind='user', text='New evidence captured during extraction.',
            occurred_at=None, metadata={}))
        episodes = {tool['id']: tool_text[:2000], original['id']: original['text'], later['id']: later['text']}
        windows = []
        for _ in range(4):
            batch = source_windows(processing, task)
            windows.extend(batch)
            assert all(window['text'] == episodes[window['episode_id']][window['start']:window['end']]
                       for window in batch)
            assert len(batch) <= 64 and sum(len(window['text']) for window in batch) <= 48000
            processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
                output=dict(windows=[dict(id=window['id'], candidates=[]) for window in batch]))
            tasks = processing.claim(**args)['work']
            if not tasks:
                break
            assert len(tasks) == 1 and tasks[0]['kind'] == 'extract'
            task = tasks[0]
        else:
            pytest.fail('Extraction cursor failed to finish the bounded source')
        for episode_id, raw in episodes.items():
            spans = sorted((window['start'], window['end']) for window in windows
                           if window['episode_id'] == episode_id)
            assert spans[0][0] == 0 and spans[-1][1] == len(raw)
            assert all(next_start == end - 1000 for (_, end), (next_start, _) in zip(spans, spans[1:]))
        assert len({window['canonical_id'] for window in windows}) == len(windows)
        assert processing.claim(**args)['work'] == []
        assert store.get(tool['id'])['text'] == tool_text
    finally:
        store.close()


def test_forgotten_partial_source_does_not_apply_its_offset_to_next_eligible_message(tmp_path):
    store = Store(tmp_path)
    try:
        graph = Graph(store)
        processing = Processing(graph)
        store.capture(dict(source='forgotten-cursor', event_id='prefix', conversation_id='c',
            kind='native_hook', text='Ignored prefix', occurred_at=None, metadata={'extraction_mode': 'skip'}))
        original, _ = store.capture(dict(source='forgotten-cursor', event_id='long', conversation_id='c',
            kind='user', text='😀 Unique source assertion. ' + 'long evidence ' * 5000,
            occurred_at=None, metadata={}))
        node = graph.remember(command_id='before-forget', actor='fixture', why='Explicit source assertion',
            type='knowledge', text='Unique source assertion.', scope=['professional'],
            evidence=[dict(episode_id=original['id'], quote='😀 Unique source assertion.')])
        store.capture(dict(source='forgotten-cursor', event_id='middle', conversation_id='c',
            kind='native_record', text='Ignored middle', occurred_at=None, metadata={'extraction_mode': 'skip'}))
        later, _ = store.capture(dict(source='forgotten-cursor', event_id='later', conversation_id='c',
            kind='user', text='The remaining evidence must start at zero.', occurred_at=None, metadata={}))
        store.capture(dict(source='forgotten-cursor', event_id='trailing', conversation_id='c',
            kind='native_hook', text='Ignored tail', occurred_at=None, metadata={'extraction_mode': 'skip'}))
        args = dict(agent='fixture', timezone_name='UTC', now=datetime.now(timezone.utc).replace(hour=12),
            compaction=dict(source='forgotten-cursor', conversation_id='c'))
        claimed = processing.claim(**args)
        assert claimed['backlog'] == dict(pending_extract_conversations=1, pending_extract_jobs=0,
            leased_extract_jobs=1)
        task = claimed['work'][0]
        assert max(window['end'] for window in source_windows(processing, task)
                   if window['episode_id'] == original['id']) < len(original['text'])
        preview = graph._forget_preview(node['id'])
        graph._forget(command_id='forget-partial', actor='human-fixture', why='Explicit source exclusion',
            target=node['id'], token=preview['token'])
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
            output=dict(windows=[dict(id=window['id'], candidates=[]) for window in task['input']['windows']]))
        task = processing.claim(**args)['work'][0]
        assert len(task['input']['windows']) == 1
        window = source_windows(processing, task)[0]
        assert window['episode_id'] == later['id'] and window['start'] == 0
        assert window['text'] == later['text']
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
            output=dict(windows=[dict(id=window['id'], candidates=[])]))
        claimed = processing.claim(**args)
        assert claimed['work'] == []
        assert claimed['backlog'] == dict(pending_extract_conversations=0, pending_extract_jobs=0,
            leased_extract_jobs=0)
        assert store.get(original['id'])['text'] == original['text']
    finally:
        store.close()


@pytest.mark.parametrize('choice', ['close', 'later'])
def test_exact_reextraction_preserves_distinct_facts_and_explicit_loop_choice(extraction, choice):
    from copy import deepcopy

    store, graph, processing, episode, payload, work, output = extraction
    first = draft('first', 'memory', 'knowledge', 'Сергей mentioned Mellanni.', TEXT)
    first['open_loop'] = True
    second = draft('second', 'memory', 'knowledge', 'Memory mentioned Mellanni.', TEXT)
    output['windows'][0]['candidates'] = [first, second]
    job, _ = processing._leased(work['id'], work['owner'])
    result = processing.complete(id=work['id'], owner=work['owner'], output=output, agent='fixture')
    assert result['candidate_count'] == 2
    candidates = store._db.execute('SELECT id,text FROM candidates ORDER BY text').fetchall()
    assert {row['text'] for row in candidates} == {first['text'], second['text']}
    target = next(row['id'] for row in candidates if row['text'] == first['text'])
    state = graph.loop(action=choice, id=target, command_id='explicit-choice')
    repeated = deepcopy(output)
    repeated['windows'][0]['candidates'][0].update(
        why='Different model rationale for the exact same supported assertion', confidence=.99)
    repeated['windows'][0]['candidates'][1]['open_loop'] = True
    graph._command('repeat-derivation-fixture', 'fixture', 'Replay original evidence for dedupe',
        dict(op='fixture-repeat'), lambda commit: dict(
            id=processing.queue._insert('repeat-extraction', 'extract', job.payload)))
    task = processing.claim(agent='fixture', timezone_name='UTC',
                            now=datetime.now(timezone.utc).replace(hour=12))['work'][0]
    result = processing.complete(id=task['id'], owner=task['owner'], output=repeated, agent='fixture')
    assert result['candidate_count'] == 0
    assert store._db.execute('SELECT count(*) FROM candidates').fetchone()[0] == 2
    assert target not in {row['id'] for row in graph.loop(action='list')['loops']}
    if choice == 'later':
        resumed = graph.loop(action='open', id=target, command_id='resume-after-repeat')
        assert resumed['deferred_until'] is None
        assert state['deferred_until'] is not None


@pytest.mark.parametrize('operation,choice,occurred_at,reopens', [
    ('duplicate', 'close', '2020-01-01T00:00:00Z', False),
    ('duplicate', 'close', '2030-01-01T15:00:00+03:00', False),
    ('duplicate', 'close', '2035-01-01T00:00:00Z', True),
    ('duplicate', 'close', None, False),
    ('supersede', 'close', '2020-01-01T00:00:00Z', False),
    ('supersede', 'close', '2035-01-01T00:00:00Z', True),
    ('supersede', 'close', None, False),
    ('duplicate', 'later', '2035-01-01T00:00:00Z', False),
    ('duplicate', 'remind', '2035-01-01T00:00:00Z', False),
])
def test_consolidation_reopens_closed_loop_only_from_newer_source_evidence(
        tmp_path, monkeypatch, operation, choice, occurred_at, reopens):
    instant = datetime(2030, 1, 1, 12, tzinfo=timezone.utc)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return instant if tz is not None else instant.replace(tzinfo=None)

    monkeypatch.setattr('memento.graph.datetime', Clock)
    monkeypatch.setattr('memento.loops.datetime', Clock)
    store = Store(tmp_path)
    try:
        graph = Graph(store)
        text = 'Orion awaits the report.'
        original, _ = store.capture(dict(source='reopen-fixture', event_id='original',
            conversation_id='c', kind='user', text=text,
            occurred_at='2020-01-01T00:00:00Z', metadata={}))
        target = graph.remember(command_id='known-fact', actor='user', why='Explicit report status',
            type='knowledge', text=text, scope=['professional'],
            evidence=[dict(episode_id=original['id'], quote=text)])['id']
        graph.loop(action='remind', id=target, due_at='2050-01-01T00:00:00Z',
                   command_id='explicit-reminder')
        if choice != 'remind':
            graph.loop(action=choice, id=target, command_id='explicit-choice',
                       **(dict(days=10000) if choice == 'later' else {}))
        previous = graph.history(target)[-1]['loop']
        # A refinement's transfer event happens later than the original close.
        # New evidence must be compared to the human close, not that transfer.
        instant = datetime(2040, 1, 1, 12, tzinfo=timezone.utc)
        quote = 'Orion awaits the report for launch.'
        source, _ = store.capture(dict(source='reopen-fixture', event_id='new-evidence',
            conversation_id='c', kind='user', text=quote, occurred_at=occurred_at, metadata={}))
        processing = Processing(graph)
        task = processing.claim(agent='fixture', timezone_name='UTC', now=instant,
            compaction=dict(source='reopen-fixture', conversation_id='c'))['work'][0]
        candidate = draft('pending-report', 'memory', 'knowledge', text, quote)
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
            output=dict(windows=[dict(id=window['id'],
                candidates=[candidate] if window['episode_id'] == source['id'] else [])
                for window in source_windows(processing, task)]))
        task = processing.claim(agent='fixture', timezone_name='UTC',
            consolidate_now=True, now=instant)['work'][0]
        candidate_id = task['input']['candidates'][0]['id']
        action = dict(candidate_id=candidate_id, operation=operation, target=target,
            type='knowledge' if operation == 'supersede' else None,
            text=quote if operation == 'supersede' else None,
            scope=['professional'] if operation == 'supersede' else None,
            open_loop=True, src=None, dst=None, relation=None,
            change_kind='refinement' if operation == 'supersede' else None,
            valid_at=None, valid_from=None, valid_to=None, category=None, sensitive=None,
            evidence=[dict(episode_id=source['id'], quote=quote, occurrence=None)],
            why='Source-backed pending report classification')
        result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
                                     output=dict(actions=[action]))
        promoted = result['results'][candidate_id]
        destination = promoted.get('superseded_by') or promoted['id']
        state = next(row for row in graph.history(destination) if row['id'] == destination)['loop']
        assert state['state'] == ('open' if reopens else previous['state'])
        assert state['due_at'] == previous['due_at']
        assert state['deferred_until'] == previous['deferred_until']
        visible = {row['id'] for row in graph.loop(action='list')['loops']}
        assert (destination in visible) == (reopens or choice == 'remind')
    finally:
        store.close()


@pytest.mark.parametrize('node_type,kind,operation', [
    ('knowledge', 'memory', 'remember'), ('knowledge', 'memory', 'duplicate'),
    ('person', 'entity', 'remember'),
])
def test_promotion_preserves_committed_citation_usage_and_feedback(tmp_path, node_type, kind, operation):
    from memento.importance import cite

    store = Store(tmp_path)
    try:
        graph = Graph(store)
        processing = Processing(graph)
        quote = 'Alex prefers cobalt orchard labels.'
        episode, _ = store.capture(dict(source='usage-promotion', event_id='fact',
            conversation_id='c', kind='user', text=quote, occurred_at=None, metadata={}))
        now = datetime.now(timezone.utc).replace(hour=12)
        args = dict(agent='fixture', timezone_name='UTC', now=now)
        task = processing.claim(**args,
            compaction=dict(source='usage-promotion', conversation_id='c'))['work'][0]
        fact = draft('fact', kind, node_type, 'Alex' if kind == 'entity' else quote, quote)
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
            output=dict(windows=[dict(id=task['input']['windows'][0]['id'], candidates=[fact])]))
        candidate_id = store._db.execute('SELECT id FROM candidates').fetchone()[0]
        for number in range(2):
            cite(graph, event_id=f'candidate-answer-{number}', ids=[candidate_id],
                 query='Alex orchard label', shown=[candidate_id])
        before = dict(store._db.execute('SELECT * FROM node_stats WHERE node_id=?',
                                       (candidate_id,)).fetchone())
        existing = None
        latest_citation = before['last_cited_at']
        if operation == 'duplicate':
            existing = graph.remember(command_id='known-alex', actor='user', why='Known source assertion',
                type=node_type, text=fact['text'], scope=['professional'],
                evidence=[dict(episode_id=episode['id'], quote=quote)])['id']
            cite(graph, event_id='known-answer', ids=[existing],
                 query='Alex orchard label', shown=[existing])
            latest_citation = graph.recall_keywords('Alex')[0]['last_cited_at']
        task = processing.claim(**args, consolidate_now=True)['work'][0]
        job, context = processing._leased(task['id'], task['owner'])
        action = dict(candidate_id=candidate_id, operation=operation, type=node_type,
            text=fact['text'], scope=['professional'], open_loop=False, target=existing,
            src=None, dst=None, relation=None, change_kind=None, valid_at=None,
            valid_from=None, valid_to=None, category=None, sensitive=None,
            evidence=[dict(episode_id=episode['id'], quote=quote, occurrence=None)],
            why='Promote the exact cited source assertion')
        output = dict(actions=[action])
        result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=output)
        assert processing.consolidator.apply(job, context, output, agent='fixture') == result
        promoted = result['results'][candidate_id]
        assert promoted['decision'] == 'promote'
        target = promoted['id']
        processing.claim(**args, consolidate_now=True)
        hit = graph.recall_keywords('Alex')[0]
        expected = 3 if existing else 2
        assert hit['id'] == target and hit['access_count'] == expected
        assert hit['last_cited_at'] == latest_citation
        assert dict(store._db.execute('SELECT * FROM node_stats WHERE node_id=?',
                                     (candidate_id,)).fetchone()) == before
        feedback = store._db.execute('SELECT shown,cited FROM query_feedback WHERE target=?',
                                    (target,)).fetchone()
        assert tuple(feedback) == (expected, expected)
    finally:
        store.close()


@pytest.mark.parametrize('reference', ['direct', 'candidate_alias'])
def test_consolidation_prioritizes_promoted_endpoint_snapshots(extraction, monkeypatch, reference):
    store, graph, processing, episode, _, work, _ = extraction
    evidence = [dict(episode_id=episode['id'], quote=TEXT)]
    nodes = [graph.remember(command_id=f'context-node-{index}', actor='fixture',
        why='Synthetic existing project context', type='project',
        text='Memory' if index == 48 else 'Mellanni' if index == 0 else f'Optional neighbor {index}',
        scope=['professional'], evidence=evidence)['id'] for index in range(49)]
    fillers = [draft(f'group-{index}', 'memory', 'knowledge', f'Optional group {index}', TEXT)
               for index in range(6)]
    candidates = fillers + [
        draft('left', 'entity', 'project', 'Memory', TEXT),
        draft('right', 'entity', 'project', 'Mellanni', TEXT),
        draft('relation', 'relation', 'related_to', 'Memory relates to Mellanni', TEXT, 'left', 'right')]
    processing.complete(id=work['id'], owner=work['owner'], agent='fixture',
        output=dict(windows=[dict(id=work['input']['windows'][0]['id'], candidates=candidates)]))
    relation = store._db.execute("SELECT id,src,dst FROM candidates WHERE kind='relation'").fetchone()

    def prior_promotions(commit):
        for index, (candidate, target) in enumerate(zip((relation['src'], relation['dst']), (nodes[-1], nodes[0]))):
            store._db.execute('INSERT INTO candidate_decisions VALUES (?,?,?,?,?,?)',
                (f'prior-endpoint-{index}', candidate, commit, 'promote', target, None))
        return {}

    graph._command('prior-endpoint-promotions', 'fixture', 'Previously promoted relation endpoints',
                   dict(op='fixture_promotions'), prior_promotions)
    # Six earlier candidates offer eight distinct optional neighbors each.
    # Only retrieval is stubbed; prepare must allocate its real bounded snapshot.
    neighbors = {item['text']: nodes[index * 8:(index + 1) * 8] for index, item in enumerate(fillers)}
    monkeypatch.setattr(graph, 'recall_keywords',
        lambda query, limit: [dict(id=node) for node in neighbors.get(query, [])][:limit])
    task = processing.claim(agent='fixture', timezone_name='UTC', consolidate_now=True,
        now=datetime(2030, 1, 1, 12, tzinfo=timezone.utc))['work'][0]
    context = task['input']
    assert len(context['candidates']) == 7
    assert len(context['existing_nodes']) == 48
    assert context['resolved_candidates'][relation['src']] == nodes[-1]
    assert context['resolved_candidates'][relation['dst']] == nodes[0]
    actions = [dict(candidate_id=item['id'], operation='reject', type=None, text=None,
        scope=None, open_loop=None, target=None, src=None, dst=None, relation=None,
        change_kind=None, valid_at=None, valid_from=None, valid_to=None, category=None, sensitive=None,
        evidence=[], why='Synthetic optional-neighbor candidate is not promoted')
        for item in context['candidates']]
    link = next(action for action in actions if action['candidate_id'] == relation['id'])
    link.update(operation='link', src='unknown-node', dst=nodes[0], relation='related_to',
        evidence=[dict(episode_id=episode['id'], quote=TEXT, occurrence=None)],
        why='Preserve the original source-supported relation direction')
    with pytest.raises(ValueError, match='^consolidation_reference_not_supplied$'):
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=actions))
    link.update(src=nodes[-1] if reference == 'direct' else relation['src'],
                dst=nodes[0] if reference == 'direct' else relation['dst'])
    result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=actions))
    edge = graph.history(result['results'][relation['id']]['id'])[-1]
    assert (edge['src'], edge['dst']) == (nodes[-1], nodes[0])
    assert edge['evidence'][0]['quote'] == TEXT
    assert nodes[-1] in {item['id'] for item in context['existing_nodes']}, 'Promoted endpoint lacks a versioned snapshot'
    assert processing.queue.get(task['id'])['state'] == 'done'


def test_night_promotion_with_existing_relations_preserves_loop_and_evidence(tmp_path):
    store = Store(tmp_path)
    try:
        graph = Graph(store)
        processing = Processing(graph)
        text = 'HarborGlass needs a report for release.'
        quote = 'HarborGlass needs a report'
        episode, _ = store.capture(dict(source='related-promotion', event_id='report',
            conversation_id='c', kind='user', text=text, occurred_at=None, metadata={}))
        evidence = [dict(episode_id=episode['id'], quote=text)]
        nodes = [graph.remember(command_id=f'known-{index}', actor='user',
            why='Explicit existing source assertion', type='knowledge', text=value,
            scope=['professional'], evidence=evidence)['id']
            for index, value in enumerate((quote, text))]
        edge = graph.link(command_id='known-relation', actor='user', why='Related source assertions',
            a=nodes[0], rel='related_to', b=nodes[1], evidence=evidence)['id']
        relation_before = graph.history(edge)
        args = dict(agent='fixture', timezone_name='UTC',
                    now=datetime(2030, 1, 1, 3, tzinfo=timezone.utc))
        task = processing.claim(**args,
            compaction=dict(source='related-promotion', conversation_id='c'))['work'][0]
        fact = draft('report', 'memory', 'knowledge', quote, quote)
        fact['open_loop'] = True
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
            output=dict(windows=[dict(id=task['input']['windows'][0]['id'], candidates=[fact])]))
        task = processing.claim(**args)['work'][0]
        assert [item['id'] for item in task['input']['existing_relations']] == [edge]
        candidate = task['input']['candidates'][0]
        action = dict(candidate_id=candidate['id'], operation='remember', type='knowledge',
            text=quote, scope=['professional'], open_loop=True, target=None,
            src=None, dst=None, relation=None, change_kind=None, valid_at=None,
            valid_from=None, valid_to=None, category=None, sensitive=None,
            evidence=[dict(episode_id=episode['id'], quote=quote, occurrence=None)],
            why='Promote the explicitly supported pending report')
        result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
                                     output=dict(actions=[action]))
        target = result['results'][candidate['id']]['id']
        history = graph.history(target)
        assert history[0]['text'] == quote
        assert history[0]['evidence'][0]['quote'] == quote
        assert [item['id'] for item in graph.loop(action='list')['loops']] == [target]
        assert graph.history(edge) == relation_before
        assert processing.queue.get(task['id'])['state'] == 'done'
    finally:
        store.close()
