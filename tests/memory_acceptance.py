"""Bounded opt-in real-model/native acceptance; never part of the ordinary suite.

Run ``python tests/memory_acceptance.py --help``. All generated evidence lives in
an isolated temporary service. No native installation, timer, consent or live
configuration is changed. Native generation uses the selected CLI's own login.
"""

import argparse
import json
import math
import os
from pathlib import Path
import shutil
import socket
import tempfile
from threading import Thread
import time
from urllib.parse import urlencode
from unittest.mock import patch
from uuid import uuid4


# Fixed before observing model scores. These synthetic facts intentionally mix
# useful semantic matches, topical false matches and a new-project transfer.
DOCUMENTS = [
    dict(id='timezone', type='knowledge', text='The synthetic user works in the Europe/Berlin timezone.'),
    dict(id='release', type='decision', text='The Orion release requires the cobalt checklist before shipment.'),
    dict(id='release_copy', duplicate_of='release', type='decision', text='The Orion release requires the cobalt checklist before shipment!'),
    dict(id='food', type='knowledge', text='The synthetic user avoids peanuts because of a food allergy.'),
    dict(id='setup', type='experience', text=(
        'When ordinary agent setup repeatedly requires manual configuration, reuse the supported installer '
        'and remove avoidable manual editing because the installer prevented the observed setup errors. '
        'Keep mandatory trust and consent explicit; follow a current request for a manual training exercise.')),
    dict(id='entity', type='project', text='Memento'),
    dict(id='path', type='knowledge', text='/home/example/My Project/src/memento/retrieval.py'),
]
QUERIES = [
    dict(id='useful_timezone', query='Which time zone should I use when arranging this person\'s meeting?', expected=['timezone']),
    dict(id='useful_release', query='What needs to be checked before shipping the Orion release?', expected=['release']),
    dict(id='useful_food', query='Which ingredient must be excluded when preparing this person\'s lunch?', expected=['food']),
    dict(id='transfer_setup', query='Connect an agent on the new Maple laptop with the fewest manual configuration steps.', expected=['setup']),
    dict(id='followup_setup', query='What about the other laptop?', context='We are simplifying ordinary agent setup using the supported installer.', expected=['setup']),
    dict(id='followup_release', query='And before shipping it?', context='We were discussing the Orion release checklist.', expected=['release']),
    dict(id='irrelevant_weather', query='Explain how clouds form above a mountain.', expected=[]),
    dict(id='irrelevant_menu', query='Translate this restaurant menu into Ukrainian.', expected=[]),
    dict(id='topical_false_match', query='Design a cobalt and violet color palette for a landing page.', expected=[]),
    dict(id='nonapplicable_setup_topic', query='Explain INI configuration syntax for a parser; do not install or connect an agent.', expected=[]),
    dict(id='empty_project_label', query='What is Memento?', expected=[]),
]

# Separately frozen holdout from actual project requests. Only anonymized
# requirements and the owner's supplied phrasing are retained; no private
# conversations, device paths or identifiers are copied into this fixture.
PROJECT_DOCUMENTS = [
    dict(id='operations', type='knowledge', text=(
        'Memento must be easy to deploy and use: set up the service, add native agents, import native history '
        'and export the whole database through supported operations without custom scripts or avoidable recurring overhead.')),
    dict(id='import', type='knowledge', text=(
        'Import existing native sessions created before Memento so their evidence can be remembered. '
        'This is historical memory ingestion, not resuming those sessions in a native harness.')),
    dict(id='drive', type='knowledge', text=(
        'Keep Memento lean by saving retained heavy objects and media to its existing Drive storage promptly, '
        'with verified uploads and a bounded local cache.')),
]
PROJECT_QUERIES = [
    dict(id='actual_overhead_followup', query='does it introduce unnecessary overhead?',
         context='review Memento code against its goals/restrictions', expected=['operations']),
    dict(id='actual_easy_operations', query='memento must be EASY to use in the first place', expected=['operations']),
    dict(id='actual_history_intent', query='import existing native sessions from pre-memento times — for remembering', expected=['import']),
    dict(id='actual_lean_drive', query='memento should remain lean, so all heavy objects should be saved to drive', expected=['drive']),
    dict(id='actual_unrelated_lunch', query='later, I am off for lunch',
         context='review Memento code against its goals/restrictions', expected=[]),
]

# Root froze these ordinary phrasings before the next native generation/scores.
# They are a disclosed diagnostic set, not a new threshold-selection corpus.
NATIVE_TRANSFER_QUERIES = [
    dict(id='neutral_connect', query='Connect the Birch agent in this workspace.', expected_applicable=True),
    dict(id='neutral_setup', query='Set up the Larch tool on this laptop.', expected_applicable=True),
    dict(id='neutral_ready', query='Get this agent ready to use.', expected_applicable=True),
    dict(id='neutral_configure', query='Configure the new desktop client.', expected_applicable=True),
    *[dict(id=item['id'], query=item['query'], expected_applicable=False)
      for item in QUERIES if item['id'] in ('irrelevant_menu', 'irrelevant_weather',
                                           'topical_false_match', 'nonapplicable_setup_topic')],
    dict(**{key: value for key, value in PROJECT_QUERIES[0].items() if key != 'expected'},
         expected_applicable=False),
]


def calibration_report(documents, queries, scores, *, fixed_threshold=None):
    """Find the lowest empirical cutoff with zero labeled false matches.

    Missed positives and overlap remain explicit. A failed separation cannot be
    hidden by changing labels or reporting only the chosen threshold.
    """
    from memento.retrieval import _automatic_content
    from memento.graph import ENTITY_TYPES
    eligible = [item['type'] not in ENTITY_TYPES and _automatic_content(item['text']) for item in documents]
    pairs = [dict(query=query['id'], document=document['id'],
                  expected=document.get('duplicate_of', document['id']) in query['expected'],
                  eligible=eligible[column], cosine=float(scores[row][column]))
             for row, query in enumerate(queries) for column, document in enumerate(documents)]
    negative = [item['cosine'] for item in pairs if item['eligible'] and not item['expected']]
    positive = [item['cosine'] for item in pairs if item['eligible'] and item['expected']]
    if fixed_threshold is None:
        threshold = math.nextafter(max(negative), 1.0) if negative else min(positive, default=1.0)
        # Rounded upward so the printed value is also a safe reproducible cutoff.
        threshold = math.ceil(threshold * 1_000_000) / 1_000_000
        criterion = 'highest recall with zero labeled false matches'
    else:
        threshold = fixed_threshold
        criterion = 'fixed threshold selected from the separate synthetic corpus; no holdout retuning'
    missed = [item for item in pairs if item['expected'] and (not item['eligible'] or item['cosine'] < threshold)]
    false_matches = [item for item in pairs if item['eligible'] and not item['expected'] and item['cosine'] >= threshold]
    positive_count = sum(item['expected'] for item in pairs)
    return dict(threshold=threshold, criterion=criterion,
        duplicate_groups={item['id']: item['duplicate_of'] for item in documents if 'duplicate_of' in item},
        positive_count=positive_count, recalled_positive_count=positive_count - len(missed),
        minimum_positive=min(positive, default=None), maximum_negative=max(negative, default=None),
        fully_separable=not missed and not false_matches, missed_positives=missed, false_matches=false_matches, pairs=pairs)


def calibrate(model):
    from memento.embedder import embed_query, embed_texts
    from memento.retrieval import _automatic_query, MODEL_ID, MODEL_REVISION
    vectors = embed_texts(model, ['title: none | text: ' + item['text'] for item in DOCUMENTS])
    queries = [embed_query(model, _automatic_query(item['query'], item.get('context', ''))) for item in QUERIES]
    report = calibration_report(DOCUMENTS, QUERIES, [[float(query @ vector) for vector in vectors] for query in queries])
    project_vectors = embed_texts(model, ['title: none | text: ' + item['text'] for item in PROJECT_DOCUMENTS])
    project_queries = [embed_query(model, _automatic_query(item['query'], item.get('context', ''))) for item in PROJECT_QUERIES]
    holdout = calibration_report(PROJECT_DOCUMENTS, PROJECT_QUERIES,
        [[float(query @ vector) for vector in project_vectors] for query in project_queries], fixed_threshold=report['threshold'])
    return dict(model_id=MODEL_ID, revision=MODEL_REVISION, documents=DOCUMENTS, queries=QUERIES, **report,
                project_intent_holdout=dict(documents=PROJECT_DOCUMENTS, queries=PROJECT_QUERIES, **holdout))


def formatted_context(service, client_path, query, *, suggestions=None):
    """Use the shipped formatter; supplied lessons explicitly bypass selection."""
    from memento.bridge import Spool, passive
    config = dict(uid=os.getuid(), client_uid='acceptance-' + uuid4().hex,
                  mode='client', agent='pi', timezone='UTC', url=service.base_url)
    spool = Spool(client_path, config)
    try:
        if suggestions is None:
            value = passive(spool, uuid4().hex, query)
        else:
            # These are actual current native-learned records, never authored
            # expected lessons. This tests formatting/conduct, not retrieval.
            with patch('memento.bridge.http', return_value={'memories': suggestions}):
                value = passive(spool, uuid4().hex, query)
        value.update(selection_bypassed=suggestions is not None,
                     evidence_kind='supplied_actual_learned_suggestions' if suggestions is not None
                                   else 'real_service_automatic_recall')
        spool.commit()
        return value
    finally:
        spool.close()


def transfer_measurement(retrieval, suggestions):
    """Measure every frozen phrase against existing current lesson vectors."""
    import numpy as np
    from memento.embedder import embed_query
    from memento.retrieval import AUTOMATIC_COSINE_FLOOR, _automatic_query
    identifiers = {item['id'] for item in suggestions}
    with retrieval.store._mutex:
        rows = retrieval.store._db.execute(f'''SELECT i.target,v.embedding FROM vector_items i
            JOIN memory_vectors v ON v.rowid=i.seq WHERE i.kind='node' AND v.current=1
            AND i.target IN ({','.join('?' for _ in identifiers)})''', tuple(identifiers)).fetchall() if identifiers else []
        vectors = {row['target']: np.frombuffer(row['embedding'], dtype=np.float32).copy()
                   for row in rows if row['target'] in identifiers}
    results = []
    for item in NATIVE_TRANSFER_QUERIES:
        started = time.perf_counter()
        with retrieval._query_inference:
            query = embed_query(retrieval.model, _automatic_query(item['query'], item.get('context', '')))
        results.append(dict(**item, query_elapsed_ms=(time.perf_counter() - started) * 1000,
                            cosines={identifier: float(query @ vector) for identifier, vector in vectors.items()}))
    return dict(automatic_floor=AUTOMATIC_COSINE_FLOOR, queries=results,
                interpretation='Frozen diagnostic labels; no retuning or claim that every positive must be recalled.')


def conduct_cases(executable, directory, initial, revised, report, *, mcp_url=None, mcp_expected_ids=None):
    """Fresh native actions with supplied context; this is not hook evidence."""
    from memento.runner import run_process
    executable = str(Path(executable).resolve(strict=True))
    help_text = run_process([executable, 'exec', '--help'], cwd=directory, timeout=20).decode()
    if any(flag not in help_text for flag in ('--ignore-user-config', '--ephemeral', '--disable', '--sandbox', '--json')):
        raise RuntimeError('Native Codex lacks the required isolated invocation controls')
    cases = [
        ('transfer', 'Connect the Birch agent in this workspace.', initial, 'installer'),
        ('nonapplicability', 'Explain how an INI parser handles duplicate keys.', initial, 'unchanged'),
        ('override', 'Connect the Birch agent by editing its configuration manually for this run. Do not run the setup command.', initial, 'manual'),
        ('adaptation', 'This is a training workspace. Configure the Birch agent and explain the settings as you go.', revised, 'manual'),
        ('transfer_control_empty', 'Connect the Birch agent in this workspace.',
         dict(context='', shown=[], learned_ids=[]), 'connected'),
        ('adaptation_control_initial', 'This is a training workspace. Configure the Birch agent and explain the settings as you go.',
         initial, 'connected'),
    ]
    if mcp_url is not None:
        cases.append(('explicit_recall', 'Connect the Birch agent in this workspace.',
                      dict(context='', shown=[], learned_ids=(revised.get('learned_ids', [])
                           if mcp_expected_ids is None else mcp_expected_ids)), 'connected'))
    for name, task, context, expected in cases:
        # Case/control labels stay in the report, not in the model's working path.
        workspace = directory / uuid4().hex
        workspace.mkdir(mode=0o700)
        (workspace / 'README.txt').write_text(
            'Birch agent fixture. Connect using python3 setup.py, or create config.json containing '
            '{"endpoint":"loopback","mode":"connected"}. The setup command writes that same file '
            'and records its invocation in installer.log. The endpoint and mode fields have those meanings.\n')
        (workspace / 'setup.py').write_text(
            'from pathlib import Path\n'
            'Path("config.json").write_text(\'{"endpoint":"loopback","mode":"connected"}\')\n'
            'Path("installer.log").write_text("setup invoked\\n")\n')
        before = {path.name: path.read_bytes() for path in workspace.iterdir()}
        explicit = name == 'explicit_recall'
        # Codex 0.163.0's local `mcp get --json` validated these exact config
        # keys/streamable-HTTP transport; no integration/config is installed.
        mcp_config = ('mcp_servers={memento_fixture={url=' + json.dumps(mcp_url) +
                      ',enabled_tools=["recall","history"]}}') if explicit else 'mcp_servers={}'
        argv = [executable, 'exec', '--ignore-user-config', '--ephemeral', '--skip-git-repo-check',
                '--disable', 'hooks', '--sandbox', 'workspace-write', '--model', 'gpt-6.1-sol',
                '-c', 'model_reasoning_effort="xhigh"', '-c', 'approval_policy="never"',
                '-c', mcp_config, '-c', 'features.apps=false', '-c', 'web_search="disabled"',
                '--json', '--cd', str(workspace), '-']
        # The expected action is retained only in the report, never in the prompt.
        boundary = ('Available memory tools may read the isolated service; do not access any other network '
                    'or external configuration.' if explicit else
                    'Do not access the network or external configuration.')
        prompt = ('Work only inside the current fixture directory, with its local files. ' + boundary +
                  '\n\n' + context['context'] + '\n\nCurrent user request:\n' + task)
        record = dict(case=name, task=task, context=context, expected_outcome=expected,
                      model='gpt-6.1-sol', reasoning_effort='xhigh',
                      evidence_kind='fresh_native_explicit_mcp_actions' if explicit else 'supplied_context_native_actions',
                      expected_learned_ids=context.get('learned_ids', []),
                      interpretation='Fixture outcomes show compatible conduct; lesson use requires trace/citation review.')
        report.append(record)
        try:
            raw = run_process(argv, prompt.encode(), cwd=workspace, timeout=180)
            record['native_events'] = [json.loads(line) for line in raw.splitlines() if line.strip()]
            answers = [event['item'].get('text', '') for event in record['native_events']
                       if event.get('type') == 'item.completed' and event.get('item', {}).get('type') == 'agent_message']
            cited = [identifier for identifier in record['expected_learned_ids']
                     if any('[memory:' + identifier + ']' in answer for answer in answers)]
            record.update(cited_learned_ids=cited, expected_lesson_cited=bool(cited))
            if explicit:
                calls = [event['item'] for event in record['native_events']
                         if event.get('type') == 'item.completed'
                         and event.get('item', {}).get('type') == 'mcp_tool_call']
                recalls = [item for item in calls if item.get('server') == 'memento_fixture'
                           and item.get('tool') == 'recall' and item.get('status') == 'completed'
                           and not item.get('error')]
                failed_requests = [item for item in calls if item.get('server') == 'memento_fixture'
                                   and (item.get('status') != 'completed' or item.get('error'))]
                retrieved = [identifier for identifier in record['expected_learned_ids']
                             if any(identifier in json.dumps(item.get('result')) for item in recalls)]
                record.update(mcp_calls=calls, enabled_memory_tools=['recall', 'history'],
                              recall_called=any(item.get('server') == 'memento_fixture' and item.get('tool') == 'recall'
                                                for item in calls),
                              history_called=any(item.get('server') == 'memento_fixture' and item.get('tool') == 'history'
                                                 for item in calls),
                              lookup_observed=bool(recalls),
                              lookup_observation='observed' if recalls else 'not_observed',
                              failed_memory_requests=failed_requests,
                              automatic_context_supplied=False, retrieved_learned_ids=retrieved,
                              lookup_interpretation='Spontaneous lookup is optional; absence does not diagnose backend failure.')
            configured = ((workspace / 'config.json').exists()
                and json.loads((workspace / 'config.json').read_text()) == dict(endpoint='loopback', mode='connected'))
            installed = (workspace / 'installer.log').exists()
            unchanged = before == {path.name: path.read_bytes() if path.is_file() else None
                                   for path in workspace.iterdir()}
            outcome = 'installer' if configured and installed else 'manual' if configured else 'unchanged' if unchanged else 'other'
            compatible = (outcome == expected if expected != 'connected' else configured)
            record.update(configured=configured, installer_invoked=installed,
                          observed_outcome=outcome, compatible_conduct=compatible,
                          workspace_unchanged=unchanged,
                          fixture_files=sorted(path.name for path in workspace.iterdir()),
                          fixture_state={path.name: path.read_text(errors='replace')[:4096] if path.is_file() else '<directory>'
                                         for path in workspace.iterdir()},
                          passed=((compatible and not record['failed_memory_requests']
                                   and bool(record['retrieved_learned_ids'])) if record['lookup_observed'] else None)
                                 if explicit else compatible)
        except Exception as error:
            record.update(passed=False, compatible_conduct=False, error=type(error).__name__,
                          error_details=getattr(error, 'details', {'message': str(error)[:1000]}))
    indexed = {item['case']: item for item in report}
    for treatment, control, desired, alternative in (
        ('transfer', 'transfer_control_empty', 'installer', 'manual'),
        ('adaptation', 'adaptation_control_initial', 'manual', 'installer'),
    ):
        result, baseline = indexed[treatment], indexed[control]
        complete = 'observed_outcome' in result and 'observed_outcome' in baseline
        differs = result.get('observed_outcome') != baseline.get('observed_outcome') if complete else None
        expected_direction = (complete and result['compatible_conduct'] and baseline['compatible_conduct']
                              and result['observed_outcome'] == desired and baseline['observed_outcome'] == alternative)
        result['control_comparison'] = dict(control=control, outcome_differs=differs,
            differs_in_expected_direction=bool(expected_direction), causal_learning_claimed=False,
            interpretation=('Compatible outcomes are identical; no context-dependent conduct difference demonstrated.'
                            if complete and not differs else
                            'Inspect actions, citations and history; a compatible outcome alone does not establish lesson use.'))


def extension_injection(service, directory, report, store):
    """Actual fresh Pi extension; no installation or modified native settings."""
    from memento.bridge import Spool, upload
    from memento.runner import run_process, private_write
    executable = shutil.which('pi')
    if not executable:
        raise RuntimeError('Native Pi is unavailable for the explicit extension check')
    client_path = directory / 'injection-client.json'
    config = dict(uid=os.getuid(), client_uid='injection-' + uuid4().hex, mode='client', agent='pi',
                  preset='pi', timezone='UTC', url=service.base_url, deadline_seconds=180)
    private_write(client_path, json.dumps(config).encode())
    entrypoint = Path(__file__).resolve().parents[1] / 'src/memento/native_pi.ts'
    argv = [executable, '--print', '--mode', 'json', '--no-session', '--no-tools', '--no-extensions',
            '--no-mcp', '--no-skills', '--no-prompt-templates', '--no-context-files',
            '--extension', str(entrypoint), '--memento-config', str(client_path)]
    task = 'How should I connect the Birch agent for ordinary operational use?'
    raw = run_process(argv, task.encode(), cwd=directory, timeout=180, processing=False)
    events = [json.loads(line) for line in raw.splitlines() if line.strip()]
    spool = Spool(client_path, config)
    try:
        cache = [dict(query=query, context=context, shown=json.loads(shown)) for query, context, shown
                 in spool.db.execute('SELECT query,context,shown FROM recall_cache')]
        spool.commit()
    finally:
        spool.close()
    uploaded = upload(client_path, config)
    answers = []
    used_tools = []
    for event in events:
        message = event.get('message', {})
        if message.get('role') == 'assistant':
            for block in message.get('content', []):
                if isinstance(block, dict) and block.get('type') == 'text':
                    answers.append(block.get('text', ''))
                if isinstance(block, dict) and block.get('type') in ('toolCall', 'tool_use', 'function_call'):
                    used_tools.append(block.get('name'))
    shown = {identifier for row in cache for identifier in row['shown']}
    cited = {identifier for identifier in shown if any('[memory:' + identifier + ']' in answer for answer in answers)}
    report.update(task=task, native_events=events, recall_cache=cache, used_tools=used_tools,
                  cited_ids=sorted(cited), upload=uploaded,
                  evidence_kind='fresh_native_extension_injection')
    with store._mutex:
        from memento.graph import Graph
        behavioral_citations = [identifier for identifier in cited if Graph(store)._behavior(identifier)]
        source = 'native:' + config['client_uid'] + ':pi'
        captured, persisted_injection = store._db.execute('''SELECT count(*),
            coalesce(sum(instr(text,'Memento recalled context (')>0 OR instr(text,'Memento recalled evidence (')>0),0)
            FROM episodes WHERE source=?''', (source,)).fetchone()
    report.update(captured_episodes=captured, persisted_injection_records=persisted_injection,
                  behavioral_cited_ids=sorted(behavioral_citations),
                  passed=bool(behavioral_citations) and not used_tools and captured > 0 and persisted_injection == 0
                    and not uploaded.get('pending') and not uploaded.get('conflicts'))


def learning(service, agent, report, *, directory, store, retrieval=None,
             codex_executable=None, check_injection=False):
    """Drive actual extraction/consolidation through the shipped HTTP runner."""
    from memento.runner import tick
    command = shutil.which(agent)
    if command is None:
        raise RuntimeError('The requested native CLI is unavailable')
    if service.get('/recall?limit=1')['memories']:
        raise RuntimeError('Acceptance requires an empty isolated service')
    config = dict(mode='server', agent=agent, preset=agent, command=[command], timezone='UTC',
                  url=service.base_url, deadline_seconds=180)
    source, conversation = 'memory-acceptance-' + uuid4().hex, 'setup-incidents'
    reports = []
    report['stages'] = reports
    failures = report['required_path_failures'] = []

    def stage(name, texts):
        originals = []
        for number, text in enumerate(texts):
            payload = dict(source=source, event_id=f'{name}-{number}', conversation_id=conversation,
                kind='user', text=text, occurred_at=None, metadata={'native_id': f'{name}-{number}'})
            result = service.post('/episodes', payload)
            originals.append(dict(id=result['id'], payload=payload))
        print(json.dumps(dict(stage=name, state='native_processing')), flush=True)
        if tick(config, dict(source=source, conversation_id=conversation), consolidate_now=True):
            raise RuntimeError('Native work failed; inspect the isolated service pending jobs')
        # Indexing is separate from model extraction. Wait only for this small
        # isolated fixture, not an open-ended production backlog.
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            status = service.get('/status?' + urlencode({'source': source}))
            indexing = status.get('indexing', {})
            if indexing.get('available') and not any(indexing.get(key, 0) for key in ('pending', 'failed', 'unprepared_episodes')):
                break
            time.sleep(.1)
        recalls = service.get('/recall?' + urlencode(dict(q='ordinary agent setup supported installer manual configuration',
                                                        effort='deep', limit=100)))['memories']
        suggestions = [item for item in recalls if item.get('category') == 'behavioral_suggestion']
        histories = {item['id']: service.get('/history/' + item['id']) for item in suggestions}
        snapshot = dict(stage=name, originals=originals, memories=recalls,
                        suggestions=suggestions, histories=histories, status=status)
        reports.append(snapshot)
        snapshot['native_context'] = formatted_context(service, directory / 'snapshot-client.json',
                                                      'Connect the Birch agent in this workspace.')
        learned_ids = {item['id'] for item in suggestions}
        snapshot['native_context']['learned_ids'] = [identifier for identifier in snapshot['native_context']['shown']
                                                    if identifier in learned_ids]
        snapshot['automatic_reach'] = dict(passed=bool(snapshot['native_context']['learned_ids']),
            query='Connect the Birch agent in this workspace.',
            interpretation='Actual automatic selection and native bounded formatting, not supplied-context conduct.')
        if not snapshot['automatic_reach']['passed']:
            failures.append(name + ': required neutral automatic transfer missed the learned suggestion')
        snapshot['supplied_context'] = formatted_context(service, directory / 'supplied-client.json',
            'Connect the Birch agent in this workspace.', suggestions=suggestions)
        snapshot['supplied_context']['learned_ids'] = [identifier for identifier in snapshot['supplied_context']['shown']
                                                     if identifier in learned_ids]
        if retrieval is not None:
            snapshot['frozen_transfer_phrasing'] = transfer_measurement(retrieval, suggestions)
        if not suggestions:
            failures.append(name + ': native consolidation produced no inspectable behavioral suggestion')
        return originals

    stage('causal_incident', [
        'For the Cedar agent setup I had to paste three settings by hand, made two mistakes, and setup failed. '
        'I then used the supported installer for the same setup. It filled the settings correctly and setup succeeded '
        'without the repeated manual editing. The mandatory trust prompt still needed my explicit approval.',
    ])
    correction_texts = [
        'For this one disposable sandbox setup I explicitly want to hand-edit the configuration as a learning exercise. '
        'Do not apply the ordinary installer suggestion to this temporary training task.',
        'In a separate Maple training setup, I again want manual configuration because learning the settings is the goal. '
        'Please narrow the installer suggestion to ordinary operational setup and exclude manual training exercises.',
    ]
    originals = stage('relevant_corrections', correction_texts)
    # Exact event replay is deliberately sent through the same capture endpoint.
    replay = service.post('/episodes', originals[-1]['payload'])
    if replay['id'] != originals[-1]['id']:
        raise RuntimeError('Original event replay created a second identity')
    scenarios = []
    for name, query, context in [
        ('transfer', 'Connect an agent on the new Birch laptop using the supported setup flow.', ''),
        ('irrelevant', 'Translate this restaurant menu into Ukrainian.', ''),
        ('nonapplicability', 'Explain INI configuration syntax for a parser; do not install or connect an agent.', ''),
        ('override', 'For this one training sandbox I explicitly want to hand-edit the configuration. Explain the settings.', ''),
        ('followup', 'What about the other laptop?', 'We are simplifying ordinary agent setup using the supported installer.'),
    ]:
        started = time.perf_counter()
        result = service.get('/recall?' + urlencode(dict(q=query, context=context, automatic='true', limit=6)))
        scenarios.append(dict(name=name, query=query, context=context, result=result,
                              elapsed_ms=(time.perf_counter() - started) * 1000))
    started = time.perf_counter()
    status = service.get('/status')
    report['measurement'] = dict(scale='small isolated native fixture; imported scale not measured',
        captured=status['captured'], indexing=status['indexing'],
        status_elapsed_ms=(time.perf_counter() - started) * 1000)
    with store._mutex:
        report['measurement'].update(vector_rows=store._db.execute('SELECT count(*) FROM vector_items').fetchone()[0],
            current_vector_rows=store._db.execute('SELECT count(*) FROM memory_vectors WHERE current=1').fetchone()[0])
    report.update(replay_id=replay['id'], scenarios=scenarios,
        fresh_native_behavior='Root must independently test actual conduct using these current requests and native injected context.')
    report['learning_evidence'] = dict(stage_suggestion_ids={item['stage']: [value['id'] for value in item['suggestions']]
                                                            for item in reports},
        interpretation='Inspect original evidence, behavior and immutable histories for generalization and adaptation.')
    report['automatic_reach'] = dict(passed=all(item['automatic_reach']['passed'] for item in reports),
        interpretation='A miss remains an end-to-end failure even when supplied-context conduct is compatible.')
    if codex_executable:
        cases = []
        report['fresh_native_behavior'] = ('Fresh native action traces and fixture outcomes are recorded. '
            'Root must review citations and history; compatible outcomes alone do not establish causal lesson use.')
        conduct_directory = directory / 'conduct'
        conduct_directory.mkdir(mode=0o700)
        try:
            conduct_cases(codex_executable, conduct_directory, reports[0]['supplied_context'],
                          reports[-1]['supplied_context'], cases, mcp_url=service.base_url + '/mcp/',
                          mcp_expected_ids=[item['id'] for item in reports[-1]['suggestions']])
        except Exception as error:
            failures.append('Native conduct invocation failed: ' + type(error).__name__)
        report['conduct_given_lesson'] = [item for item in cases if item['case'] != 'explicit_recall']
        report['controls'] = [dict(case=item['case'], passed=item['passed'],
                                  observed_outcome=item.get('observed_outcome'))
                              for item in cases if '_control_' in item['case']]
        report['explicit_lookup'] = next((item for item in cases if item['case'] == 'explicit_recall'),
                                         dict(passed=False, error='Explicit case did not run'))
        lookup = report['explicit_lookup']
        if lookup.get('error') or not lookup.get('compatible_conduct') or lookup.get('failed_memory_requests'):
            failures.append('Fresh native explicit case had an invocation, tool request or fixture action failure')
        failures.extend('fresh native case failed: ' + item['case'] for item in cases
                        if item['case'] != 'explicit_recall' and not item['passed'])
        if any(not item['supplied_context']['learned_ids'] for item in reports):
            failures.append('Native conduct lacked a bounded supplied real learned suggestion')
    if check_injection:
        report['native_injection'] = {}
        try:
            extension_injection(service, directory, report['native_injection'], store)
        except Exception as error:
            report['native_injection'].update(passed=False, error=type(error).__name__)
        if not report['native_injection']['passed']:
            failures.append('Actual fresh native extension injection failed')
    report['passed'] = not failures
    if failures:
        raise RuntimeError('Acceptance failed; inspect required_path_failures and the independent evidence sections')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['calibrate', 'learning'])
    parser.add_argument('--agent', choices=['pi', 'omp'], default='pi')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--calibrate', action='store_true', help='Reuse the isolated service model for frozen calibration first')
    parser.add_argument('--codex-executable', type=Path,
                        help='Opt in to fresh gpt-6.1/xhigh conduct, two paired controls and explicit MCP lookup using this existing binary')
    parser.add_argument('--injection', action='store_true', help='Opt in to the fresh Pi extension injection check; no native installation')
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit('Choose a new output file')
    if args.mode != 'learning' and (args.codex_executable or args.injection):
        parser.error('Native conduct/injection options require learning mode')
    report = {}
    try:
        if args.mode == 'calibrate':
            from memento.embedder import load_model
            from memento.retrieval import MODEL_ID, MODEL_REVISION
            report['calibration'] = calibrate(load_model(MODEL_ID, MODEL_REVISION, text_only=False))
        else:
            import uvicorn
            from memento.api import create_app
            from memento.runner import Service
            os.environ['MEMENTO_EMBEDDINGS'] = '1'
            with tempfile.TemporaryDirectory(prefix='memento-memory-acceptance-') as directory:
                app = create_app(Path(directory))
                listener = socket.socket()
                listener.bind(('127.0.0.1', 0))
                port = listener.getsockname()[1]
                server = uvicorn.Server(uvicorn.Config(app, log_level='warning'))
                thread = Thread(target=server.run, kwargs={'sockets': [listener]}, daemon=True)
                thread.start()
                try:
                    deadline = time.monotonic() + 300
                    while not server.started and thread.is_alive() and time.monotonic() < deadline:
                        time.sleep(.1)
                    if not server.started:
                        raise RuntimeError('Isolated real-model startup did not complete')
                    if args.calibrate:
                        report['calibration'] = calibrate(app.state.retrieval.model)
                    report['learning'] = {}
                    from memento.retrieval import AUTOMATIC_COSINE_FLOOR
                    report['automatic_floor_used'] = AUTOMATIC_COSINE_FLOOR
                    learning(Service(f'http://127.0.0.1:{port}'), args.agent, report['learning'],
                             directory=Path(directory), store=app.state.store, retrieval=app.state.retrieval,
                             codex_executable=args.codex_executable, check_injection=args.injection)
                finally:
                    if getattr(app.state, 'store', None) is not None and app.state.store._db is not None:
                        with app.state.store._mutex:
                            report['processing_audit'] = [dict(row) for row in app.state.store._db.execute('''
                                SELECT actor,recorded_at,
                                    json_extract(request,'$.command.op') AS operation,
                                    json_extract(request,'$.command.version') AS version,
                                    json_extract(request,'$.command.reported_model') AS reported_model,
                                    json_extract(request,'$.command.usage') AS usage
                                FROM graph_commits WHERE json_extract(request,'$.command.op') IN ('extract','consolidate')
                                ORDER BY rowid''')]
                    server.should_exit = True
                    thread.join(timeout=60)
                    listener.close()
    except Exception as error:
        report['failure'] = str(error)
        raise
    finally:
        fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as stream:
            json.dump(report, stream, ensure_ascii=False, indent=2)
            stream.write('\n')


if __name__ == '__main__':
    main()
