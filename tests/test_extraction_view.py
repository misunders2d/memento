import hashlib
import json
from datetime import datetime, timezone

import pytest

from memento.extraction import VERSION
from memento.graph import Graph
from memento.processing import Processing
from memento.store import Store


def claim(processing):
    return processing.claim(agent='fixture', timezone_name='UTC',
        compaction=dict(source='native:synthetic-device:omp', conversation_id='c'),
        now=datetime.now(timezone.utc).replace(hour=12))['work'][0]


def capture(store, index, text, kind='user'):
    return store.capture(dict(source='native:synthetic-device:omp', event_id=str(index),
        conversation_id='c', kind=kind, text=text, occurred_at=None, metadata={}))[0]


def candidate(quote, occurrence=None):
    return dict(key='fact', kind='memory', type='knowledge', text=quote, quote=quote,
        why='Explicit synthetic source assertion', occurrence=occurrence,
        src_key=None, dst_key=None, valid_from=None, valid_to=None,
        scope=['professional'], open_loop=False, confidence=.8)


@pytest.fixture
def packed(tmp_path):
    store = Store(tmp_path)
    processing = Processing(Graph(store))
    episodes = [capture(store, index,
        f'😀 Quoted\tclaim {index}:\n retain whitespace exactly.',
        ('user', 'assistant', 'tool')[index % 3]) for index in range(64)]
    work = claim(processing)
    try:
        yield store, processing, episodes, work
    finally:
        store.close()


def test_64_window_model_view_preserves_exact_text_and_measures_bytes(packed):
    store, processing, episodes, work = packed
    _, context = processing._leased(work['id'], work['owner'])
    windows = work['input']['windows']
    assert len(windows) == 64
    assert [window['id'] for window in windows] == [f'w{index}' for index in range(64)]
    assert all(set(window) == {'id', 'kind', 'text'} for window in windows)
    assert [window['text'] for window in windows] == [episode['text'] for episode in episodes]
    assert [window['kind'] for window in windows] == [episode['kind'] for episode in episodes]
    assert [window['episode_id'] for window in context['windows']] == [episode['id'] for episode in episodes]
    assert context['window_view'] == 'handles-v1'
    assert all(window['id'] == window['episode_id'] + ':0' for window in context['windows'])
    encode = lambda value: json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode()
    old_bytes = len(encode(dict(windows=context['windows'])))
    compact_bytes = len(encode(work['input']))
    assert compact_bytes < old_bytes
    print(json.dumps(dict(windows=64, original_bytes=old_bytes, compact_bytes=compact_bytes,
        reduction_bytes=old_bytes - compact_bytes,
        reduction_percent=round(100 * (old_bytes - compact_bytes) / old_bytes, 2))))
    assert all(store.get(episode['id'])['text'] == episode['text'] for episode in episodes)


def test_w7_maps_to_its_original_source_even_when_output_order_changes(packed):
    store, processing, episodes, work = packed
    _, context = processing._leased(work['id'], work['owner'])
    quote = episodes[7]['text']
    output = dict(windows=[dict(id=window['id'],
        candidates=[candidate(quote)] if window['id'] == 'w7' else [])
        for window in reversed(work['input']['windows'])])
    result = processing.complete(id=work['id'], owner=work['owner'], agent='fixture', output=output)
    assert result['candidate_count'] == 1 and result['window_count'] == 64
    evidence = store._db.execute('SELECT * FROM graph_evidence').fetchone()
    assert (evidence['episode_id'], evidence['start'], evidence['end'], evidence['quote']) == (
        episodes[7]['id'], 0, len(quote), quote)
    row = store._db.execute('SELECT id,run_id FROM candidates').fetchone()
    assert row['run_id'] == work['id'] + '/' + context['windows'][7]['id']
    fingerprint = dict(episode_id=episodes[7]['id'], kind='memory', type='knowledge',
        text=quote, start=0, end=len(quote), valid_from=None, valid_to=None)
    expected_id = 'c_' + hashlib.sha256(json.dumps(
        fingerprint, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:32]
    assert row['id'] == expected_id
    assert store._db.execute('SELECT count(*) FROM extraction_runs').fetchone()[0] == 64


@pytest.mark.parametrize('defect', ['duplicate', 'missing', 'unknown', 'mixed', 'full_ids'])
def test_compact_window_coverage_rejects_bad_handles_atomically(packed, defect):
    store, processing, _, work = packed
    _, context = processing._leased(work['id'], work['owner'])
    windows = [dict(id=window['id'], candidates=[]) for window in work['input']['windows']]
    windows[0]['candidates'] = [candidate(work['input']['windows'][0]['text'])]
    if defect == 'duplicate':
        windows[-1]['id'] = windows[0]['id']
    elif defect == 'missing':
        windows.pop()
    elif defect == 'unknown':
        windows[-1]['id'] = 'w64'
    elif defect == 'mixed':
        windows[-1]['id'] = context['windows'][-1]['id']
    else:
        for batch, window in zip(windows, context['windows'], strict=True):
            batch['id'] = window['id']
    with pytest.raises(ValueError, match='^candidate_window_coverage_invalid$'):
        processing.complete(id=work['id'], owner=work['owner'], agent='fixture', output=dict(windows=windows))
    assert store._db.execute('SELECT count(*) FROM extraction_runs').fetchone()[0] == 0
    assert store._db.execute('SELECT count(*) FROM candidates').fetchone()[0] == 0
    assert store._db.execute('SELECT count(*) FROM graph_evidence').fetchone()[0] == 0
    assert store._db.execute('SELECT seq,offset FROM processing_progress').fetchone()[:] == (0, 0)
    assert processing.queue.get(work['id'])['state'] == 'leased'


@pytest.mark.parametrize('legacy', [False, True], ids=['compact', 'old-inflight-full-id'])
def test_saved_context_survives_restart_and_preserves_original_offsets_and_identity(tmp_path, legacy):
    store = Store(tmp_path)
    try:
        quote = '😀 Stable second-window assertion.'
        episode = capture(store, 0, 'x' * 12000 + quote)
        processing = Processing(Graph(store))
        work = claim(processing)
        _, context = processing._leased(work['id'], work['owner'])
        assert len(context['windows']) == 2 and context['windows'][1]['start'] == 11000
        if legacy:
            # A pre-update lease saved the original full context and supplied IDs.
            context.pop('window_view', None)
            store._db.execute('UPDATE jobs SET context=? WHERE id=?',
                (json.dumps(context, ensure_ascii=False), work['id']))
            input_windows = context['windows']
        else:
            input_windows = work['input']['windows']
        output = dict(windows=[dict(id=window['id'], candidates=[candidate(quote)] if index == 1 else [])
            for index, window in enumerate(input_windows)])
        before = tuple(store._db.execute('SELECT job_key,payload FROM jobs WHERE id=?', (work['id'],)).fetchone())
        store.close()
        store = Store(tmp_path)
        processing = Processing(Graph(store))
        if legacy:
            invalid = dict(windows=[dict(batch, id=f'w{index}') for index, batch in enumerate(output['windows'])])
            with pytest.raises(ValueError, match='^candidate_window_coverage_invalid$'):
                processing.complete(id=work['id'], owner=work['owner'], agent='fixture', output=invalid)
        result = processing.complete(id=work['id'], owner=work['owner'], agent='fixture', output=output)
        assert result['candidate_count'] == 1
        evidence = store._db.execute('SELECT episode_id,start,end,quote FROM graph_evidence').fetchone()
        assert tuple(evidence) == (episode['id'], 12000, 12000 + len(quote), quote)
        assert store._db.execute('SELECT run_id FROM candidates').fetchone()[0] == work['id'] + '/' + context['windows'][1]['id']
        assert {row[0] for row in store._db.execute('SELECT version FROM extraction_runs')} == {VERSION}
        assert tuple(store._db.execute('SELECT job_key,payload FROM jobs WHERE id=?', (work['id'],)).fetchone()) == before
        assert processing.queue.get(work['id'])['state'] == 'done'
    finally:
        store.close()


@pytest.mark.parametrize(('quote', 'occurrence', 'error'), [
    ('invented quote', None, 'candidate_quote_not_in_source'),
    ('repeated assertion', None, 'candidate_quote_ambiguous'),
    ('repeated assertion', 2, 'candidate_quote_occurrence_invalid'),
])
def test_handle_resolution_does_not_relax_exact_quote_validation(tmp_path, quote, occurrence, error):
    store = Store(tmp_path)
    try:
        capture(store, 0, 'repeated assertion; repeated assertion')
        processing = Processing(Graph(store))
        work = claim(processing)
        with pytest.raises(ValueError, match='^' + error + '$'):
            processing.complete(id=work['id'], owner=work['owner'], agent='fixture',
                output=dict(windows=[dict(id=work['input']['windows'][0]['id'],
                    candidates=[candidate(quote, occurrence)])]))
        assert store._db.execute('SELECT count(*) FROM candidates').fetchone()[0] == 0
    finally:
        store.close()
