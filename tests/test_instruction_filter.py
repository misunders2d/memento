import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from memento.graph import Graph, behavior_ground_truth
from memento.processing import Processing
from memento.store import Store, legacy_native_context


SOURCE = 'native:synthetic-device:codex'
TEXT = 'The synthetic approval policy requires explicit operator consent.'
NOW = datetime(2026, 10, 21, 12, tzinfo=timezone.utc)


def capture(store, event_id, *, source=SOURCE, kind='developer', text=TEXT, metadata=None):
    return store.capture(dict(source=source, event_id=event_id, conversation_id='c', kind=kind,
        text=text, occurred_at=None, metadata={} if metadata is None else metadata))[0]


def candidate(text=TEXT):
    return dict(key='policy', kind='memory', type='knowledge', text=text, quote=text,
        why='Exact durable source assertion', occurrence=None, src_key=None, dst_key=None,
        valid_from=None, valid_to=None, scope=['professional'], open_loop=False, confidence=.8)


def complete(processing, work):
    return processing.complete(id=work['id'], owner=work['owner'], agent='fixture',
        output=dict(windows=[dict(id=window['id'], candidates=[candidate(window['text'])])
                             for window in work['input']['windows']]))


@pytest.mark.parametrize('metadata', [{}, {'extraction_mode': 'message', 'native_id': 'setup'},
    {'native_context_kinds': []}, {'content_item_kinds': []}, '{"extraction_mode":"message"}'])
def test_codex_developer_role_identifies_instructions_without_text_or_metadata_rewrites(metadata):
    assert legacy_native_context(SOURCE, 'developer', TEXT, metadata)


@pytest.mark.parametrize('source,kind', [
    (SOURCE, 'user'), (SOURCE, 'assistant'), (SOURCE, 'tool'), (SOURCE, 'custom'), (SOURCE, 'system'),
    ('native:synthetic-device:pi', 'developer'), ('native:synthetic-device:omp', 'developer'),
    ('native:synthetic-device:claude', 'developer'), ('document', 'developer'),
    ('native:device:extra:codex', 'developer'), ('native::codex', 'developer'),
])
def test_developer_filter_keeps_other_roles_harnesses_and_sources(source, kind):
    assert not legacy_native_context(source, kind, TEXT, {})


@pytest.mark.parametrize('text,excluded', [
    ('# AGENTS.md instructions\n\n<INSTRUCTIONS>\n' + TEXT + '\n</INSTRUCTIONS>', True),
    ('# AGENTS.md instructions for /workspace/example\n<INSTRUCTIONS>\n' + TEXT +
     '\n</INSTRUCTIONS>\n<environment_context>cwd: /workspace/example</environment_context>', True),
    ('<codex_internal_context source="goal">\n' + TEXT + '\n</codex_internal_context>', True),
    ('# AGENTS.md instructions\n\n<INSTRUCTIONS>\n' + TEXT, False),
    ('<codex_internal_context source="goal">\n' + TEXT, False),
    ('My own request.\n# AGENTS.md instructions\n<INSTRUCTIONS>' + TEXT + '</INSTRUCTIONS>', False),
    ('<codex_internal_context source="goal">' + TEXT + '</codex_internal_context>\nMy own request.', False),
])
def test_existing_exact_user_envelope_semantics_are_preserved(text, excluded):
    assert legacy_native_context(SOURCE, 'user', text, {}) is excluded


def test_developer_raw_source_and_reimport_identity_survive_reopen(tmp_path):
    text = '\nNative developer policy: 😀 preserve точные bytes.\n'
    metadata = {'extraction_mode': 'message', 'native_id': 'native-setup', 'custom': ['unchanged']}
    payload = dict(source=SOURCE, event_id='developer-policy', conversation_id='c', kind='developer',
                   text=text, occurred_at='2026-10-20T10:00:00+00:00', metadata=metadata)
    store = Store(tmp_path)
    try:
        episode, created = store.capture(payload)
        assert created and not behavior_ground_truth(episode)
        assert store.capture(payload) == (episode, False)
        assert store.get(episode['id']) == episode
        assert store.search('точные')[0]['id'] == episode['id']
        assert store._db.execute('SELECT legacy_native_context(source,kind,text,metadata) FROM episodes').fetchone()[0] == 1
    finally:
        store.close()
    reopened = Store(tmp_path)
    try:
        assert reopened.get(episode['id']) == episode
        assert reopened.capture(payload) == (episode, False)
    finally:
        reopened.close()


def test_more_than_65_developer_rows_do_not_starve_real_evidence_or_cursor(tmp_path):
    store = Store(tmp_path)
    try:
        processing = Processing(Graph(store))
        for index in range(70):
            capture(store, f'instruction-{index}')
        genuine = capture(store, 'genuine', kind='user')
        for index in range(3):
            capture(store, f'trailing-instruction-{index}')
        work = processing.claim(agent='fixture', timezone_name='UTC', now=NOW,
            compaction=dict(source=SOURCE, conversation_id='c'))['work'][0]
        assert [window['text'] for window in work['input']['windows']] == [TEXT]
        context = json.loads(store._db.execute('SELECT context FROM jobs WHERE id=?', (work['id'],)).fetchone()[0])
        assert [window['episode_id'] for window in context['windows']] == [genuine['id']]
        assert complete(processing, work)['candidate_count'] == 1
        assert store._db.execute('SELECT seq=latest_seq AND offset=0 FROM processing_progress').fetchone()[0] == 1
        assert len(store.search(source=SOURCE, limit=100)) == 74
        assert processing.claim(agent='fixture', timezone_name='UTC', now=NOW)['work'] == []
    finally:
        store.close()


@pytest.mark.parametrize('source,kind', [
    (SOURCE, 'user'), (SOURCE, 'assistant'), (SOURCE, 'tool'), (SOURCE, 'custom'), (SOURCE, 'system'),
    ('native:synthetic-device:pi', 'developer'), ('native:synthetic-device:omp', 'developer'),
    ('native:synthetic-device:claude', 'developer'), ('document', 'developer'),
])
def test_other_message_controls_remain_extractable(tmp_path, source, kind):
    store = Store(tmp_path)
    try:
        episode = capture(store, 'control', source=source, kind=kind)
        processing = Processing(Graph(store))
        work = processing.claim(agent='fixture', timezone_name='UTC', now=NOW,
            compaction=dict(source=source, conversation_id='c'))['work'][0]
        assert work['input']['windows'][0]['text'] == TEXT
        assert complete(processing, work)['candidate_count'] == 1
        assert store._db.execute('SELECT episode_id FROM graph_evidence').fetchone()[0] == episode['id']
    finally:
        store.close()


@pytest.mark.parametrize('mixed', [False, True])
@pytest.mark.parametrize('leased', [False, True])
def test_old_queued_and_leased_developer_work_cannot_commit_instruction_candidates(tmp_path, mixed, leased):
    store = Store(tmp_path)
    try:
        processing = Processing(Graph(store))
        instruction = capture(store, 'old-instruction')
        genuine = capture(store, 'genuine', kind='user') if mixed else None
        processing.compact(SOURCE, 'c')
        # Admit the old payload without changing immutable source/metadata. For
        # an already leased job, save the full-ID context served by old code.
        store._db.create_function('legacy_native_context', 4, lambda *_: False, deterministic=True)
        processing._enqueue_extract(NOW)
        row = store._db.execute("SELECT id,payload,job_key FROM jobs WHERE kind='extract'").fetchone()
        if leased:
            job = processing.queue.claim(['extract'])
            context = dict(windows=[])
            for item in job.payload['windows']:
                episode = store.get(item['episode_id'])
                context['windows'].append(dict(item, source=episode['source'], kind=episode['kind'],
                    text=episode['text'][item['start']:item['end']]))
            store._db.execute('UPDATE jobs SET context=? WHERE id=?', (json.dumps(context), job.id))
            work = dict(id=job.id, owner=job.owner, input=context)
        store._db.create_function('legacy_native_context', 4, legacy_native_context, deterministic=True)
        if not leased:
            claimed = processing.claim(agent='fixture', timezone_name='UTC', now=NOW)['work']
            assert len(claimed) == int(mixed)
            if mixed:
                work = claimed[0]
                assert [window['text'] for window in work['input']['windows']] == [TEXT]
        if leased or mixed:
            assert complete(processing, work)['candidate_count'] == int(mixed)
        assert processing.queue.get(row['id'])['state'] == 'done'
        assert tuple(store._db.execute('SELECT payload,job_key FROM jobs WHERE id=?', (row['id'],)).fetchone()) == (row['payload'], row['job_key'])
        assert store._db.execute('SELECT seq=latest_seq AND offset=0 FROM processing_progress').fetchone()[0] == 1
        assert store._db.execute('SELECT count(*) FROM candidates').fetchone()[0] == int(mixed)
        assert not store._db.execute('SELECT 1 FROM extraction_runs WHERE episode_id=?', (instruction['id'],)).fetchone()
        assert not store._db.execute('SELECT 1 FROM graph_evidence WHERE episode_id=?', (instruction['id'],)).fetchone()
        if genuine:
            assert store._db.execute('SELECT episode_id FROM graph_evidence').fetchone()[0] == genuine['id']
        assert store.get(instruction['id']) == instruction
    finally:
        store.close()


@pytest.mark.parametrize('mixed', [False, True])
def test_developer_backed_memories_remain_explicit_but_need_genuine_evidence_for_automatic_recall(
        tmp_path, monkeypatch, mixed):
    np = pytest.importorskip('numpy')
    from memento.retrieval import Retrieval

    store = Store(tmp_path)
    try:
        graph = Graph(store)
        refs = [dict(episode_id=capture(store, f'context-{index}')['id'], quote=TEXT) for index in range(6)]
        if mixed:
            genuine = capture(store, 'genuine', kind='user')
            refs.append(dict(episode_id=genuine['id'], quote=TEXT))
        target = graph.remember(command_id='old-policy', actor='fixture', why='Preexisting supported memory',
            type='knowledge', text=TEXT, scope=['professional'], evidence=refs)['id']
        vector = np.eye(1, 768, dtype=np.float32)[0]
        monkeypatch.setattr('memento.retrieval.embed_query', lambda *_: vector)
        retrieval = Retrieval(graph, model=SimpleNamespace(tokenizer=lambda text, **_: {
            'offset_mapping': [(index, index + 1) for index in range(len(text))]}))
        retrieval._put(target, 'node', 1, vector)
        assert any(hit['id'] == target for hit in retrieval.recall('approval policy', effort='cheap'))
        monkeypatch.setattr(store, 'get', lambda *_: pytest.fail('automatic recall hydrated source text'))
        automatic = retrieval.recall('approval policy', automatic=True)
        assert [hit['id'] for hit in automatic] == ([target] if mixed else [])
    finally:
        store.close()


def reject_action(candidate_id):
    return dict(candidate_id=candidate_id, operation='reject', type=None, text=None, scope=None, open_loop=None,
        target=None, src=None, dst=None, relation=None, change_kind=None, valid_at=None, valid_from=None,
        valid_to=None, category=None, sensitive=None, evidence=[], why='Instruction text is not a durable user fact.')


def test_instruction_only_candidates_leave_consolidation_but_stay_stored_and_counted(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from memento.api import create_app
    from memento.extraction import ACTIVE_CANDIDATE_SQL
    monkeypatch.setenv('MEMENTO_EMBEDDINGS', '0')
    with TestClient(create_app(tmp_path)) as client:
        store, processing = client.app.state.store, client.app.state.processing
        capture(store, 'first')
        second = capture(store, 'second', text='The second synthetic policy forbids silent merges.')
        genuine = capture(store, 'genuine', source='native:synthetic-device:claude', kind='user',
                          text='Silent merges are forbidden here.')
        processing.compact(SOURCE, 'c')
        # Seed the pre-fix derivation pathway without rewriting any evidence.
        store._db.create_function('legacy_native_context', 4, lambda *_: False, deterministic=True)
        with monkeypatch.context() as old:
            old.setattr('memento.extraction.legacy_native_context', lambda *_: False)
            [work] = processing.claim(agent='fixture', timezone_name='UTC', now=NOW)['work']
            assert complete(processing, work)['candidate_count'] == 2
        store._db.create_function('legacy_native_context', 4, legacy_native_context, deterministic=True)
        ids = {row['text']: row['id'] for row in store._db.execute('SELECT id,text FROM candidates')}
        legacy_only, mixed = ids[TEXT], ids[second['text']]
        store._db.execute('INSERT INTO graph_evidence VALUES (?,?,?,?,?,?)',
                          (mixed, 1, genuine['id'], 0, len(genuine['text']), genuine['text']))
        counts = client.get('/status').json()['consolidation']
        assert (counts['pending_candidates'], counts['ready_candidates'], counts['excluded_legacy_candidates']) == (1, 1, 1)
        assert client.get('/status', params={'source': SOURCE}).json()['consolidation']['excluded_legacy_candidates'] == 1
        assert client.get('/status', params={'source': 'other'}).json()['consolidation']['excluded_legacy_candidates'] == 0
        [task] = processing.claim(agent='fixture', timezone_name='UTC', now=NOW, consolidate_now=True)['work']
        assert [item['id'] for item in task['input']['candidates']] == [mixed]
        assert {window['episode_id'] for window in task['input']['raw_windows']} == {second['id'], genuine['id']}
        result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
                                     output=dict(actions=[reject_action(mixed)]))
        assert result['results'][mixed]['decision'] == 'reject'
        # The instruction-only row is untouched, undecided and still an active (explicitly searchable) candidate.
        assert store._db.execute(f'SELECT c.id FROM candidates c WHERE {ACTIVE_CANDIDATE_SQL}').fetchone()[0] == legacy_only
        assert not store._db.execute('SELECT 1 FROM candidate_decisions WHERE candidate_id=?', (legacy_only,)).fetchone()
        counts = client.get('/status').json()['consolidation']
        assert (counts['pending_candidates'], counts['ready_candidates'], counts['excluded_legacy_candidates']) == (0, 0, 1)
        assert processing.claim(agent='fixture', timezone_name='UTC', now=NOW, consolidate_now=True)['work'] == []


def test_eligibility_predicate_reads_bodies_only_for_codex_user_rows(tmp_path):
    from memento.consolidation import Consolidator
    store = Store(tmp_path)
    try:
        consolidator = Consolidator(Graph(store))
        rows = dict(
            developer=capture(store, 'developer'),
            codex_user=capture(store, 'codex-user', kind='user', text='Genuine Codex user text.'),
            other_user=capture(store, 'other-user', source='native:synthetic-device:claude', kind='user',
                               text='Claude user text.'),
            tool=capture(store, 'tool', kind='tool', text='tool output body'),
        )
        for name, episode in rows.items():
            store._db.execute('INSERT INTO extraction_runs VALUES (?,?,?,?,?,?,?,?)',
                (f'run-{name}', 'commit', episode['id'], 'extract-v2', 'fixture', '[]', 'null', NOW.isoformat()))
            store._db.execute('''INSERT INTO candidates (id,run_id,kind,type,text,why,src,dst,confidence,valid_from,valid_to,scope)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''', (f'c_{name}', f'run-{name}', 'memory', 'knowledge', episode['text'],
                'fixture', None, None, .8, None, None, '["professional"]'))
            store._db.execute('INSERT INTO graph_evidence VALUES (?,?,?,?,?,?)',
                              (f'c_{name}', 1, episode['id'], 0, len(episode['text']), episode['text']))
        calls = []

        def recording(source, kind, text, metadata):
            calls.append((source, kind, text))
            return legacy_native_context(source, kind, text, metadata)
        store._db.create_function('legacy_native_context', 4, recording, deterministic=True)
        admitted = {item['id'] for item in consolidator.pending()}
        assert admitted == {'c_codex_user', 'c_other_user', 'c_tool'}
        assert consolidator._ineligible([f'c_{name}' for name in rows]) == ['c_developer']
        # Developer rows decide without their body; non-Codex and tool rows never reach the function.
        assert calls and all(text is None for source, kind, text in calls if kind == 'developer')
        assert all(kind in ('developer', 'user') and source == SOURCE for source, kind, text in calls)
        assert any(kind == 'user' and text == rows['codex_user']['text'] for source, kind, text in calls)
    finally:
        store.close()


def test_status_evaluates_the_eligibility_predicate_once_per_candidate(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from memento.api import create_app
    monkeypatch.setenv('MEMENTO_EMBEDDINGS', '0')
    calls = []

    def recording(source, kind, text, metadata):
        calls.append((source, kind))
        return legacy_native_context(source, kind, text, metadata)
    monkeypatch.setattr('memento.store.legacy_native_context', recording)
    with TestClient(create_app(tmp_path)) as client:
        store = client.app.state.store
        episodes = [capture(store, f'developer-{index}', text=f'Instruction {index}.') for index in range(3)]
        episodes.append(capture(store, 'claude', source='native:synthetic-device:claude', kind='user', text='Genuine.'))
        for index, episode in enumerate(episodes):
            store._db.execute('INSERT INTO extraction_runs VALUES (?,?,?,?,?,?,?,?)',
                (f'run-{index}', 'commit', episode['id'], 'extract-v2', 'fixture', '[]', 'null', NOW.isoformat()))
            store._db.execute('''INSERT INTO candidates (id,run_id,kind,type,text,why,src,dst,confidence,valid_from,valid_to,scope)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''', (f'c_{index}', f'run-{index}', 'memory', 'knowledge', episode['text'],
                'fixture', None, None, .8, None, None, '["professional"]'))
            store._db.execute('INSERT INTO graph_evidence VALUES (?,?,?,?,?,?)',
                              (f'c_{index}', 1, episode['id'], 0, len(episode['text']), episode['text']))
        store._db.commit()
        calls.clear()
        counts = client.get('/status').json()['consolidation']
        assert (counts['pending_candidates'], counts['ready_candidates'], counts['excluded_legacy_candidates']) == (1, 1, 3)
        # One evaluation per Codex row; the Claude row never reaches the function.
        assert sorted(calls) == [(SOURCE, 'developer')] * 3
