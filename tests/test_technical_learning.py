"""Native trace grounding contracts; semantic pattern recognition needs model acceptance."""

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from memento import bridge
from memento.graph import Graph, behavior_ground_truth
from memento.processing import Processing
from memento.store import Store
from test_behavioral_learning import action, complete


ATTEMPTS = ['{"command":"search --scope current --name report"}',
            '{"command":"search --scope current --name report"}',
            '{"command":"search --scope archive --name report"}']
RESULTS = ['No matching report in current scope.', 'Still no matching report in current scope.',
           'archive/report.csv: expected report found.']
BEHAVIOR = dict(applicability='When a report lookup repeatedly returns empty results in the current scope.',
    conduct='Check whether the report is archived and search the archive scope when it is.',
    rationale='Repeating the current-scope lookup missed the archived report; archive scope returned it.',
    exceptions=['Keep the current scope when the report belongs there.'], authority='inferred')
LESSON = ('When a report lookup repeatedly returns empty results in the current scope, check whether '
          'the report is archived and search archive scope when it is. Keep current scope for current reports.')


def native_records(harness):
    records = []
    for index, (arguments, output) in enumerate(zip(ATTEMPTS, RESULTS, strict=True)):
        call = f'call-{index}'
        if harness == 'claude':
            records.extend([
                dict(type='assistant', uuid=call, message=dict(role='assistant', content=[
                    dict(type='tool_use', id=call, name='shell', input=json.loads(arguments))])),
                dict(type='user', uuid=f'result-{index}', message=dict(role='user', content=[
                    dict(type='tool_result', tool_use_id=call, content=output)])),
            ])
        elif harness == 'codex':
            records.extend([
                dict(type='response_item', payload=dict(type='function_call', call_id=call,
                    name='shell', arguments=arguments)),
                dict(type='response_item', payload=dict(type='function_call_output', call_id=call, output=output)),
            ])
        else:
            records.extend([
                dict(type='message', id=call, message=dict(role='assistant', content=[
                    dict(type='toolCall', id=call, name='shell', arguments=json.loads(arguments))])),
                dict(type='message', id=f'result-{index}', message=dict(role='toolResult',
                    toolCallId=call, content=[dict(type='text', text=output)])),
            ])
    return records


def captured_trace(tmp_path, store, harness='claude', *, transform=None, records=None):
    spool = bridge.Spool(tmp_path / 'runner.json', dict(client_uid='technical-fixture'))
    try:
        for index, record in enumerate(records if records is not None else native_records(harness)):
            spool.record(harness, 'lookup', record, identity=f'record-{index}')
        bodies = [json.loads(row[0]) for row in spool.db.execute(
            "SELECT body FROM outbox WHERE endpoint='/episodes' ORDER BY id")]
        episodes = []
        for body in bodies:
            if transform:
                body = transform(body)
            episode = store.capture(body)[0]
            if episode['kind'] != 'native_record':
                episodes.append(episode)
        return bodies, episodes
    finally:
        spool.close()


def extract_trace(processing, anchor, *, context=None):
    args = dict(agent='fixture', timezone_name='UTC', now=datetime.now(timezone.utc))
    task = processing.claim(**args, compaction=dict(source=anchor['source'],
        conversation_id=anchor['conversation_id']))['work'][0]
    item = dict(key='lookup', kind='memory', type='experience', text='Current-scope lookup missed an archived report.',
        quote=anchor['text'], why='Observed tool approach and result, not a user preference.', occurrence=None,
        src_key=None, dst_key=None, valid_from=None, valid_to=None, scope=['professional'],
        open_loop=False, confidence=.8)
    processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(windows=[
        dict(id=window['id'], candidates=[dict(item, quote=window['text'])]
             if window['text'] == anchor['text'] or context and window['text'] == context['text'] else [])
        for window in task['input']['windows']]))
    return args


def proposal(task, anchor, evidence):
    result = action(task, anchor, behavior=dict(BEHAVIOR), text=LESSON)
    result['candidate_id'] = next(candidate['id'] for candidate in task['input']['candidates']
        if anchor['id'] in {ref['episode_id'] for ref in candidate['evidence']})
    result['evidence'] = [dict(episode_id=episode['id'], quote=episode['text'], occurrence=None)
                          for episode in evidence]
    return result


@pytest.mark.parametrize('harness', ['claude', 'codex', 'pi', 'omp'])
def test_native_empty_results_and_changed_approach_reach_automatic_recall(tmp_path, monkeypatch, harness):
    np = pytest.importorskip('numpy')
    from memento.retrieval import Retrieval
    store = Store(tmp_path / 'store')
    try:
        graph, processing = Graph(store), Processing(Graph(store))
        bodies, episodes = captured_trace(tmp_path, store, harness)
        args = extract_trace(processing, episodes[-1])
        task = processing.claim(**args, consolidate_now=True)['work'][0]
        windows = task['input']['raw_windows']
        assert {episode['id'] for episode in episodes} <= {window['episode_id'] for window in windows}
        assert all(not window['behavior_ground_truth'] for window in windows)
        # Native evidence references carry the same per-quote marker the validator applies.
        assert all(ref['behavior_ground_truth'] is False
                   for candidate in task['input']['candidates'] for ref in candidate['evidence'])
        assert [window['sequence'] for window in windows] == sorted(window['sequence'] for window in windows)
        learned = complete(processing, task, proposal(task, episodes[-1], episodes))['id']
        history = graph.history(learned)[0]
        assert history['authority'] == 'inferred' and history['behavior'] == BEHAVIOR
        assert {ref['quote'] for ref in history['evidence']} == {episode['text'] for episode in episodes}
        # Model-free recall plumbing only; this does not test causal model judgment.
        vector = np.eye(1, 768, dtype=np.float32)[0]
        monkeypatch.setattr('memento.retrieval.embed_query', lambda model, text: vector)
        retrieval = Retrieval(graph, model=SimpleNamespace(tokenizer=lambda *args, **kwargs: {}))
        retrieval._put(learned, 'node', 1, vector)
        recalled = retrieval.recall('find an archived report after empty current scope results', automatic=True)
        assert recalled[0]['id'] == learned and recalled[0]['authority'] == 'inferred'
        assert all(store.capture(body)[1] is False for body in bodies)
    finally:
        store.close()


@pytest.mark.parametrize('defect', ['explicit_user', 'only_result', 'only_attempt', 'result_before_attempt',
                                  'alternative_without_result', 'invented_quote', 'out_of_context'])
def test_technical_trace_structural_rejections(tmp_path, defect):
    store = Store(tmp_path / 'store')
    try:
        processing = Processing(Graph(store))
        _, episodes = captured_trace(tmp_path, store)
        anchor = (episodes[1] if defect == 'result_before_attempt' else episodes[0] if defect == 'only_attempt'
                  else episodes[4] if defect == 'alternative_without_result' else episodes[-1])
        args = extract_trace(processing, anchor)
        task = processing.claim(**args, consolidate_now=True)['work'][0]
        chosen = ([episodes[-1]] if defect == 'only_result' else [episodes[0]] if defect == 'only_attempt'
                  else [episodes[1], episodes[2]] if defect == 'result_before_attempt'
                  else episodes[:5] if defect == 'alternative_without_result' else episodes)
        output = proposal(task, anchor, chosen)
        if defect == 'explicit_user':
            output['behavior']['authority'] = 'explicit_user'
        elif defect == 'invented_quote':
            output['evidence'][0]['quote'] = 'Unobserved corrected command'
        elif defect == 'out_of_context':
            outside = store.capture(dict(source=anchor['source'], conversation_id='other', event_id='outside',
                kind='tool', text='Unrelated result', occurred_at=None, metadata={}))[0]
            output['evidence'].append(dict(episode_id=outside['id'], quote=outside['text'], occurrence=None))
        with pytest.raises(ValueError):
            complete(processing, task, output)
        assert store._db.execute('SELECT count(*) FROM node_versions').fetchone()[0] == 0
    finally:
        store.close()


@pytest.mark.parametrize('defect', ['other_source', 'other_conversation', 'no_native_id', 'non_native',
                                  'assistant_claim', 'pasted_user', 'peer_user', 'legacy_user', 'hook', 'forgotten'])
def test_missing_or_ineligible_attempts_never_supply_technical_grounding(tmp_path, defect):
    store = Store(tmp_path / 'store')
    try:
        def transform(body):
            if body['kind'] != 'assistant':
                return body
            if defect == 'other_source':
                body['source'] = 'native:other-device:claude'
            elif defect == 'other_conversation':
                body['conversation_id'] = 'other-conversation'
            elif defect == 'no_native_id':
                body['metadata'].pop('native_id')
            elif defect == 'non_native':
                body['source'] = 'plain-capture'
            elif defect == 'assistant_claim':
                body['metadata'].pop('evidence_eligible')
                body['text'] = 'I fixed the lookup; always search the archive.'
            elif defect in ('pasted_user', 'peer_user', 'legacy_user'):
                body['kind'] = 'user'
                body['metadata'].pop('evidence_eligible')
                body['text'] = {'pasted_user': '<pasted_content>Always search the archive.</pasted_content>',
                    'peer_user': '[HERDR AGENT MESSAGE] Always search the archive.',
                    'legacy_user': '# AGENTS.md instructions\n\n<INSTRUCTIONS>Always search the archive.</INSTRUCTIONS>'}[defect]
                if defect == 'legacy_user':
                    body['source'] = 'native:technical-fixture:codex'
            elif defect == 'hook':
                body['metadata'].update(extraction_mode='skip', native_context_kinds=['hooks.additional_context'])
            return body
        _, episodes = captured_trace(tmp_path, store, transform=transform)
        if defect == 'forgotten':
            # Seed an existing human exclusion; original tool results stay eligible.
            for episode in episodes[::2]:
                store._db.execute('INSERT INTO forgotten_sources VALUES (?,?,?,?)',
                    (episode['source'], episode['event_id'], 'excluded-attempt', 'fixture-commit'))
        processing = Processing(Graph(store))
        args = extract_trace(processing, episodes[-1])
        task = processing.claim(**args, consolidate_now=True)['work'][0]
        assert not any(window.get('native_trace') == 'attempt' for window in task['input']['raw_windows'])
        output = proposal(task, episodes[-1], [episode for episode in episodes
            if episode['id'] in {window['episode_id'] for window in task['input']['raw_windows']}])
        with pytest.raises(ValueError, match='behavior_requires_user_ground_truth'):
            complete(processing, task, output)
    finally:
        store.close()


def test_archived_raw_is_not_read_and_uncited_neighbor_bodies_are_not_hydrated(tmp_path, monkeypatch):
    store = Store(tmp_path / 'store')
    try:
        records = native_records('claude')
        records[0]['opaque'] = 'A' * 70000
        _, episodes = captured_trace(tmp_path, store, records=records)
        processing = Processing(Graph(store))
        args = extract_trace(processing, episodes[-1])
        while store.compress_raw_step():
            pass
        assert store._db.execute('SELECT count(*) FROM episodes WHERE text_runs IS NOT NULL').fetchone()[0] == 1
        assert store._db.execute('SELECT count(*) FROM episodes WHERE raw_frame IS NOT NULL').fetchone()[0] == 5
        original_get, loaded = store.get, []
        def guarded_get(identity):
            assert store._db.execute('SELECT kind FROM episodes WHERE id=?', (identity,)).fetchone()[0] != 'native_record'
            loaded.append(identity)
            return original_get(identity)
        monkeypatch.setattr(store, 'get', guarded_get)
        task = processing.claim(**args, consolidate_now=True)['work'][0]
        assert loaded == [episodes[-1]['id']]
        loaded.clear()
        complete(processing, task, proposal(task, episodes[-1], episodes[-2:]))
        assert set(loaded) == {episode['id'] for episode in episodes[-2:]}
    finally:
        store.close()


def test_neighborhood_has_shared_batch_and_per_candidate_budgets(tmp_path):
    store = Store(tmp_path / 'store')
    try:
        records = []
        for number in range(3):
            for record in native_records('claude'):
                block = record['message']['content'][0]
                if block['type'] == 'tool_use':
                    block['input']['detail'] = 'context ' * 190
                else:
                    block['content'] += 'result detail ' * 200
                records.append(record)
            records.append(dict(type='assistant', uuid=f'incident-{number}',
                message=dict(role='assistant', content=f'Observed lookup incident {number}.')))
        _, episodes = captured_trace(tmp_path, store, records=records)
        processing = Processing(Graph(store))
        args = dict(agent='fixture', timezone_name='UTC', now=datetime.now(timezone.utc))
        work = processing.claim(**args, compaction=dict(source=episodes[0]['source'], conversation_id='lookup'))['work'][0]
        processing.complete(id=work['id'], owner=work['owner'], agent='fixture', output=dict(windows=[
            dict(id=window['id'], candidates=[dict(key='incident', kind='memory', type='experience',
                text=window['text'], quote=window['text'], why='Synthetic bounded neighborhood', occurrence=None,
                src_key=None, dst_key=None, valid_from=None, valid_to=None, scope=['professional'],
                open_loop=False, confidence=.8)] if window['text'].startswith('Observed lookup incident') else [])
            for window in work['input']['windows']]))
        task = processing.claim(**args, consolidate_now=True)['work'][0]
        assert len(task['input']['candidates']) == 3
        original = {ref['episode_id'] for candidate in task['input']['candidates'] for ref in candidate['evidence']}
        added = [window for window in task['input']['raw_windows'] if window['episode_id'] not in original]
        assert len(added) <= 32 and sum(len(window['text']) for window in added) <= 16000
        assert all(len(window['text']) <= 2000 for window in added)
        assert len({window['episode_id'] for window in added}) == len(added)
        one = processing.consolidator._snapshot([task['input']['candidates'][0]['id']])
        original_one = {ref['episode_id'] for ref in one[0][0]['evidence']}
        added_one = [window for window in one[4].values() if window['episode_id'] not in original_one]
        assert len(added_one) <= 12 and sum(len(window['text']) for window in added_one) <= 8000
        # A real quote beyond the supplied prefix must not become usable merely
        # because the entire episode is retained on disk.
        outside = next(episode for episode in episodes if episode['kind'] == 'tool' and len(episode['text']) > 2000
            and episode['id'] in {window['episode_id'] for window in added})
        actions = []
        for candidate in task['input']['candidates']:
            anchor = next(episode for episode in episodes if episode['id'] == candidate['evidence'][0]['episode_id'])
            item = proposal(task, anchor, [anchor])
            item.update(candidate_id=candidate['id'], behavior=None)
            actions.append(item)
        actions[0]['evidence'].append(dict(episode_id=outside['id'], quote=outside['text'], occurrence=None))
        with pytest.raises(ValueError, match='consolidation_evidence_outside_context'):
            processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=actions))
    finally:
        store.close()


@pytest.mark.parametrize('context_origin', ['same', 'other_source', 'other_conversation',
                                          'peer', 'pasted', 'fenced', 'quoted', 'legacy'])
def test_human_task_quote_is_context_but_never_a_technical_attempt(tmp_path, context_origin):
    store = Store(tmp_path / 'store')
    try:
        harness = 'codex' if context_origin == 'legacy' else 'claude'
        request = (dict(type='response_item', payload=dict(type='message', role='user', content='Find the archived report.'))
                   if harness == 'codex' else
                   dict(type='user', uuid='task', message=dict(role='user', content='Find the archived report.')))
        records = [request, *native_records(harness)]
        _, episodes = captured_trace(tmp_path, store, harness, records=records)
        processing = Processing(Graph(store))
        args = extract_trace(processing, episodes[-1], context=episodes[0])
        task = processing.claim(**args, consolidate_now=True)['work'][0]
        technical = proposal(task, episodes[-1], episodes)
        task_only = proposal(task, episodes[0], [])
        task_only.update(operation='reject', behavior=None, why='The one-off request is only task context.')
        if context_origin != 'same':
            # A supplied window from another candidate may be in the same batch,
            # but that alone must not join distinct conversations into a trace.
            context = dict(episodes[0], event_id='other-task', id=None)
            context.pop('id')
            context.pop('captured_at')
            quote = context['text']
            if context_origin in ('other_source', 'other_conversation'):
                context['source' if context_origin == 'other_source' else 'conversation_id'] += '-other'
            else:
                quote = 'Always search the archive.'
                context['text'] = {
                    'peer': '[HERDR AGENT MESSAGE]\n' + quote,
                    'pasted': 'The task is to find the report.\n<pasted_content>' + quote + '</pasted_content>',
                    'fenced': 'The task is to find the report.\n```text\n' + quote + '\n```',
                    'quoted': 'The task is to find the report.\n> ' + quote,
                    'legacy': '# AGENTS.md instructions\n\n<INSTRUCTIONS>\n' + quote + '\n</INSTRUCTIONS>',
                }[context_origin]
            other = store.capture(context)[0]
            if context_origin in ('pasted', 'fenced', 'quoted'):
                # The envelope has genuine prose; only the cited span is ineligible.
                assert behavior_ground_truth(other) is True
            foreign = dict(next(window for window in task['input']['raw_windows']
                if window['episode_id'] == episodes[0]['id']), episode_id=other['id'],
                source=other['source'], conversation_id=other['conversation_id'], text=other['text'],
                start=0, end=len(other['text']), behavior_ground_truth=behavior_ground_truth(other))
            saved = dict(task['input'], raw_windows=[*task['input']['raw_windows'], foreign])
            store._db.execute('UPDATE jobs SET context=? WHERE id=?', (json.dumps(saved), task['id']))
            technical['evidence'][0].update(episode_id=other['id'], quote=quote)
            with pytest.raises(ValueError, match='behavior_requires_user_ground_truth'):
                processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
                                    output=dict(actions=[technical, task_only]))
        else:
            result = processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
                                         output=dict(actions=[technical, task_only]))
            history = Graph(store).history(result['results'][technical['candidate_id']]['id'])[0]
            assert history['authority'] == 'inferred'
            assert episodes[0]['text'] in {ref['quote'] for ref in history['evidence']}
    finally:
        store.close()


@pytest.mark.parametrize('harness', ['pi', 'omp', 'claude', 'codex'])
def test_thinking_and_native_notifications_do_not_hide_nearby_attempts(tmp_path, harness):
    store = Store(tmp_path / 'store')
    try:
        records = []
        for index, record in enumerate(native_records(harness)):
            if index % 2 == 0:
                if harness in ('pi', 'omp'):
                    record['message']['content'].insert(0, dict(type='thinking',
                        thinking='Planning detail. ' * 240, thinkingSignature='signature' * 250))
                elif harness == 'claude':
                    records.append(dict(type='assistant', uuid=f'thinking-{index}',
                        message=dict(role='assistant', content=[dict(type='thinking', thinking='Planning detail. ' * 240)])))
                else:
                    records.extend([
                        dict(type='response_item', payload=dict(type='reasoning', summary=[
                            dict(type='summary_text', text='Planning detail. ' * 240)])),
                        dict(type='event_msg', payload=dict(type='agent_reasoning', text='Planning detail. ' * 240)),
                    ])
            records.append(record)
        _, episodes = captured_trace(tmp_path, store, harness, records=records)
        processing = Processing(Graph(store))
        args = extract_trace(processing, episodes[-1])
        task = processing.claim(**args, consolidate_now=True)['work'][0]
        windows = task['input']['raw_windows']
        attempts = [window for window in windows if window.get('native_trace') == 'attempt']
        results = [window for window in windows if window.get('native_trace') == 'result']
        assert len(attempts) == len(results) == 3
        assert all(window.get('native_trace') in ('attempt', 'result') for window in windows)
        assert sum(len(window['text']) for window in windows if window['episode_id'] != episodes[-1]['id']) <= 8000
        for window, arguments in zip(attempts, ATTEMPTS, strict=True):
            assert json.loads(arguments)['command'] in window['text']
            original = store.get(window['episode_id'])
            assert original['text'][window['start']:window['end']] == window['text']
            if harness in ('pi', 'omp'):
                assert window['start'] > 2000
        # Cite the observed arguments themselves rather than the omitted thinking.
        output = proposal(task, episodes[-1], [])
        output['evidence'] = [dict(episode_id=window['episode_id'],
            quote=json.loads(arguments)['command'], occurrence=None)
            for window, arguments in zip(attempts, ATTEMPTS, strict=True)]
        output['evidence'].extend(dict(episode_id=window['episode_id'], quote=window['text'], occurrence=None)
                                  for window in results)
        learned = complete(processing, task, output)['id']
        assert Graph(store)._behavior(learned)['authority'] == 'inferred'
    finally:
        store.close()


def reported_trace(harness='claude', *, acknowledged=False):
    """Two actual attempt/result pairs, then the assistant's own report tool call."""
    records = []
    for index, (arguments, output) in enumerate(((ATTEMPTS[0], RESULTS[0]), (ATTEMPTS[2], RESULTS[2]))):
        call = f'call-{index}'
        records.extend([
            dict(type='assistant', uuid=call, message=dict(role='assistant', content=[
                dict(type='tool_use', id=call, name='shell', input=json.loads(arguments))])),
            dict(type='user', uuid=f'result-{index}', message=dict(role='user', content=[
                dict(type='tool_result', tool_use_id=call, content=output)])),
        ])
    records.append(dict(type='assistant', uuid='report', message=dict(role='assistant', content=[
        dict(type='tool_use', id='report', name='advise', input=dict(
            lesson='Current-scope search failed; archive scope found the report.'))])))
    if acknowledged:
        records.append(dict(type='user', uuid='ack', message=dict(role='user', content=[
            dict(type='tool_result', tool_use_id='report', content='queued')])))
    return records


@pytest.mark.parametrize('case', ['selected', 'legacy_all_cited', 'report_selected', 'results_only',
                                  'uncited', 'unknown', 'duplicate', 'explicit_user', 'explicit_user_selected',
                                  'mixed_source'])
def test_later_report_source_needs_explicit_actual_support(tmp_path, case):
    store = Store(tmp_path / 'store')
    try:
        processing = Processing(Graph(store))
        _, episodes = captured_trace(tmp_path, store, records=reported_trace())
        trace, report = episodes[:4], episodes[4]
        args = extract_trace(processing, report)
        task = processing.claim(**args, consolidate_now=True)['work'][0]
        windows = {window['episode_id']: window for window in task['input']['raw_windows']}
        assert windows[report['id']]['native_trace'] == 'attempt'
        assert [windows[episode['id']]['native_trace'] for episode in trace] == ['attempt', 'result'] * 2
        output = proposal(task, report, episodes)
        output['technical_evidence'] = [episode['id'] for episode in trace]
        expected = None
        if case == 'legacy_all_cited':
            # Old plans keep the all-cited rule: the report is the latest attempt.
            output.pop('technical_evidence')
            expected = 'behavior_requires_user_ground_truth'
        elif case == 'report_selected':
            output['technical_evidence'].append(report['id'])
            expected = 'behavior_requires_user_ground_truth'
        elif case == 'results_only':
            output['technical_evidence'] = [trace[1]['id'], trace[3]['id']]
            expected = 'behavior_requires_user_ground_truth'
        elif case == 'uncited':
            output['evidence'] = [ref for ref in output['evidence'] if ref['episode_id'] != trace[0]['id']]
            expected = 'technical_evidence_invalid'
        elif case == 'unknown':
            output['technical_evidence'] = ['unknown-episode']
            expected = 'technical_evidence_invalid'
        elif case == 'duplicate':
            output['technical_evidence'] = [trace[0]['id'], trace[0]['id'], trace[1]['id']]
            expected = 'technical_evidence_invalid'
        elif case == 'explicit_user':
            output.pop('technical_evidence')
            output['behavior']['authority'] = 'explicit_user'
            expected = 'behavior_requires_user_ground_truth'
        elif case == 'explicit_user_selected':
            output['behavior']['authority'] = 'explicit_user'
            expected = 'technical_evidence_invalid'
        elif case == 'mixed_source':
            foreign = store.capture(dict(trace[0], id=None, event_id='foreign', source='native:other-device:claude',
                                         captured_at=None))[0]
            window = dict(windows[trace[0]['id']], episode_id=foreign['id'], source=foreign['source'])
            store._db.execute('UPDATE jobs SET context=? WHERE id=?', (json.dumps(dict(task['input'],
                raw_windows=[*task['input']['raw_windows'], window])), task['id']))
            output['evidence'].append(dict(episode_id=foreign['id'], quote=foreign['text'], occurrence=None))
            output['technical_evidence'].append(foreign['id'])
            expected = 'behavior_requires_user_ground_truth'
        if expected:
            with pytest.raises(ValueError, match=expected):
                complete(processing, task, output)
            assert store._db.execute('SELECT count(*) FROM node_versions').fetchone()[0] == 0
            return
        learned = complete(processing, task, output)['id']
        history = Graph(store).history(learned)[0]
        assert history['authority'] == 'inferred'
        # The required later report is ordinary supporting evidence; no acknowledgment was captured.
        assert report['text'] in {ref['quote'] for ref in history['evidence']}
        assert not any(window.get('text') == 'queued' for window in task['input']['raw_windows'])
    finally:
        store.close()
