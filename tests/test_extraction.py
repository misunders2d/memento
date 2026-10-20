import json
from datetime import datetime, timezone

import pytest

from memento.graph import Graph
from memento.processing import Processing
from memento.store import Store


TEXT = '😀 Сергей, Mellanni, Memory. Связи: Сергей → Mellanni; Memory → Mellanni.'
NATIVE_SOURCE = 'native:synthetic-device:codex'
LEGACY_CONTEXTS = [
    '# AGENTS.md instructions for /workspace/example\n\n<INSTRUCTIONS>\n' + TEXT +
        '\n</INSTRUCTIONS>\n<environment_context>\ncwd: /workspace/example\nshell: bash\n</environment_context>',
    '# AGENTS.md instructions\n\n<INSTRUCTIONS>\nThese AGENTS.md instructions replace all previously provided AGENTS.md instructions.\n' +
        TEXT + '\n</INSTRUCTIONS>',
    '# AGENTS.md instructions\n\n<INSTRUCTIONS>\n' + TEXT +
        '\n</INSTRUCTIONS>\n<environment_context>\ncurrent_date: 2026-01-01\n</environment_context>',
    '# AGENTS.md instructions for /workspace/other\n\n<INSTRUCTIONS>\n' + TEXT +
        '\n</INSTRUCTIONS>\n<environment_context>\nfilesystem: local\n</environment_context>',
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
            assert [window['episode_id'] for window in task['input']['windows']] == episodes[expected]
            assert [window['text'] for window in task['input']['windows']] == [
                f'{expected} message 0', f'{expected} message 1']
            processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
                output=dict(windows=[dict(id=window['id'], candidates=[])
                                     for window in task['input']['windows']]))
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


def test_large_repeated_or_incomplete_legacy_delimiters_are_not_context_only():
    from memento.store import legacy_native_context
    prefix = '# AGENTS.md instructions\n\n<INSTRUCTIONS>'
    for text in (prefix + '<INSTRUCTIONS>' * 30000 + '</INSTRUCTIONS>',
                 prefix + ' ' * 500000, LEGACY_CONTEXTS[0] + '\n' + 'own words ' * 50000):
        assert not legacy_native_context(NATIVE_SOURCE, 'user', text, {})


def test_extraction_filters_more_than_65_legacy_contexts_before_its_window_limit(tmp_path):
    store = Store(tmp_path)
    try:
        processing = Processing(Graph(store))
        for index in range(70):
            store.capture(dict(source=NATIVE_SOURCE, event_id=str(index), conversation_id='c', kind='user',
                text=LEGACY_CONTEXTS[index % 4], occurred_at=None, metadata={'extraction_mode': 'message'}))
        genuine, _ = store.capture(dict(source=NATIVE_SOURCE, event_id='genuine', conversation_id='c',
            kind='user', text=TEXT, occurred_at=None, metadata={}))
        work = processing.claim(agent='fixture', timezone_name='UTC',
            compaction=dict(source=NATIVE_SOURCE, conversation_id='c'),
            now=datetime.now(timezone.utc).replace(hour=12))['work'][0]
        assert [window['episode_id'] for window in work['input']['windows']] == [genuine['id']]
        result = processing.complete(id=work['id'], owner=work['owner'], agent='fixture',
            output=dict(windows=[dict(id=work['input']['windows'][0]['id'], candidates=drafts())]))
        assert result['candidate_count'] == 5
        assert store._db.execute('SELECT seq=latest_seq FROM processing_progress').fetchone()[0] == 1
        assert len(store.search(source=NATIVE_SOURCE, limit=100)) == 71
    finally:
        store.close()


@pytest.mark.parametrize('mixed', [False, True])
@pytest.mark.parametrize('leased', [False, True])
def test_preexisting_queued_and_leased_context_work_completes_without_losing_real_windows(tmp_path, monkeypatch, mixed, leased):
    from memento.store import legacy_native_context
    store = Store(tmp_path)
    try:
        processing = Processing(Graph(store))
        now = datetime.now(timezone.utc).replace(hour=12)
        context, _ = store.capture(dict(source=NATIVE_SOURCE, event_id='old-context', conversation_id='c',
            kind='user', text=LEGACY_CONTEXTS[0], occurred_at=None, metadata={}))
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
                assert [window['episode_id'] for window in work['input']['windows']] == [genuine['id']]
        if leased or mixed:
            output = dict(windows=[dict(id=window['id'], candidates=drafts()) for window in work['input']['windows']])
            result = processing.complete(id=work['id'], owner=work['owner'], agent='fixture', output=output)
            assert result['candidate_count'] == (5 if mixed else 0)
        assert processing.queue.get(job_id)['state'] == 'done'
        assert store._db.execute('SELECT seq=latest_seq FROM processing_progress').fetchone()[0] == 1
        assert store._db.execute('SELECT count(*) FROM candidates').fetchone()[0] == (5 if mixed else 0)
        assert not store._db.execute('SELECT 1 FROM graph_evidence WHERE episode_id=?', (context['id'],)).fetchone()
        assert store.get(context['id'])['text'] == LEGACY_CONTEXTS[0]
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
        windows = task['input']['windows']
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
            windows = task['input']['windows']
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
        assert [window['episode_id'] for window in task['input']['windows']] == [later['id']]
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
        first = task['input']['windows']
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
            batch = task['input']['windows']
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
        assert len({window['id'] for window in windows}) == len(windows)
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
        assert max(window['end'] for window in task['input']['windows']
                   if window['episode_id'] == original['id']) < len(original['text'])
        preview = graph._forget_preview(node['id'])
        graph._forget(command_id='forget-partial', actor='human-fixture', why='Explicit source exclusion',
            target=node['id'], token=preview['token'])
        processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
            output=dict(windows=[dict(id=window['id'], candidates=[]) for window in task['input']['windows']]))
        task = processing.claim(**args)['work'][0]
        assert len(task['input']['windows']) == 1
        window = task['input']['windows'][0]
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
                for window in task['input']['windows']]))
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
        if kind == 'entity':
            review = dict(command_id='approve-cited-person', actor='human-fixture',
                why='Approve the explicit person evidence', proposal_id=promoted['id'], decision='approve')
            promoted = graph._decide_review(**review)
            assert graph._decide_review(**review) == promoted
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
