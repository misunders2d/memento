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
