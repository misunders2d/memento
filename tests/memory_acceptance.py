"""Bounded opt-in real-model/native acceptance; never part of the ordinary suite.

Run ``python tests/memory_acceptance.py --help``. Generated evidence stays in
isolated temporary work; factual-scope needs no service. No installation, timer, consent or live
configuration is changed. Native generation uses the selected CLI's own login.
"""

import argparse
import contextlib
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import socket
import stat
import subprocess
import tempfile
import threading
from threading import Thread
import time
import urllib.error
import urllib.parse
import urllib.request
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


# Frozen before native generation. The first case reproduces a subject in the
# source context outside a compact numerical quote; no private source is copied.
FACTUAL_SCOPE_CASES = [
    dict(id='qualified_product', kind='tool', text=(
        'Retrieved tracker issue.\nProject: Juniper Archive\nRelease: 4.2\n'
        'The archive constraints below apply only to signed offline snapshots. '
        'Online snapshots use a different policy that is not specified here.\n'
        'Issue description: Each archive chunk contains at most 50 records and '
        'at most 4 MiB of encoded payload.'),
        required=['juniper archive', '4.2', 'signed', 'offline', '50', '4 mib'], forbidden=[]),
    dict(id='missing_subject', kind='tool', text=(
        'Archive chunks contain at most 50 records and at most 4 MiB of encoded payload. '
        'No product, release or snapshot condition is identified in this result.'),
        required=['50', '4 mib'], forbidden=['juniper', '4.2', 'signed', 'offline']),
    dict(id='personal_fact', kind='user', text='I keep my spare house key in the blue tin.',
        required=['spare', 'key', 'blue tin'], forbidden=['juniper', '4.2', 'signed', 'offline']),
]


def factual_scope_report(output):
    from memento.extraction import BatchDrafts, MEMORY_TYPES, source_span
    drafts = BatchDrafts.model_validate(output)
    windows = {item.id: item for item in drafts.windows}
    if len(drafts.windows) != len(FACTUAL_SCOPE_CASES) or set(windows) != {case['id'] for case in FACTUAL_SCOPE_CASES}:
        raise ValueError('candidate_window_coverage_invalid')
    cases = []
    for case in FACTUAL_SCOPE_CASES:
        facts = []
        for candidate in windows[case['id']].candidates:
            start, end = source_span(case['text'], candidate.quote, candidate.occurrence)
            if candidate.kind == 'memory':
                if candidate.type not in MEMORY_TYPES:
                    raise ValueError('candidate_memory_type_invalid')
                facts.append(dict(type=candidate.type, text=candidate.text, scope=candidate.scope,
                                  quote=candidate.quote, start=start, end=end))
        # The unnamed case checks invention, not extraction yield. Both factual
        # controls require knowledge/decision: incidents without consolidated
        # behavior are intentionally ineligible for automatic recall.
        complete = case['id'] == 'missing_subject' or any(
            all(term in fact['text'].casefold() for term in case['required'])
            and (case['id'] != 'personal_fact' or fact['scope'] == ['personal'])
            and fact['type'] in ('knowledge', 'decision')
            for fact in facts)
        invented = any(term in candidate.text.casefold() for term in case['forbidden']
                       for candidate in windows[case['id']].candidates)
        cases.append(dict(id=case['id'], facts=facts, abstained=not windows[case['id']].candidates, retained_scope=complete,
                          invented_fixture_context=invented, passed=complete and not invented))
    return dict(cases=cases, passed=all(case['passed'] for case in cases))


def factual_scope(agent, report):
    """One native extraction batch, using the unchanged tool-free safety adapter."""
    from memento.extraction import INSTRUCTION, BatchDrafts
    from memento.runner import check_cli, encode, parse_result, preset_flags, run_process
    windows = [dict(id=case['id'], episode_id=case['id'], source='synthetic-factual-scope',
                    kind=case['kind'], start=0, end=len(case['text']), text=case['text'])
               for case in FACTUAL_SCOPE_CASES]
    payload = dict(instruction=INSTRUCTION, input=dict(windows=windows), schema=BatchDrafts.model_json_schema())
    report.update(frozen_cases=FACTUAL_SCOPE_CASES, task=payload, native_agent=agent,
                  requested_model='native default', requested_effort='native default',
                  comparison='Single post-change batch; historical failure is not a paired comparison')
    with tempfile.TemporaryDirectory(prefix='memento-factual-scope-') as directory:
        argv = check_cli([agent], agent) + preset_flags(agent, directory)
        report['argv'] = argv
        started = time.monotonic()
        raw = run_process(argv, encode(payload), timeout=300, cwd=directory)
        report.update(elapsed_seconds=time.monotonic() - started,
                      native_events=[json.loads(line) for line in raw.splitlines() if line.strip()])
        output, model, usage = parse_result(raw, agent)
    report.update(output=output, reported_model=model, usage=usage)
    message = next(event['message'] for event in reversed(report['native_events'])
                   if event.get('type') == 'message_end' and event.get('message', {}).get('role') == 'assistant')
    report['reported_effort'] = message.get('thinkingLevel')
    report.update(factual_scope_report(output))
    if not report['passed']:
        raise RuntimeError('Native factual-scope controls failed; inspect exact output and evidence')


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


# Native retrieval/delivery baseline (MEL-721 H1). Opt-in, model-free client
# measurement against one owned, isolated native service that a launcher
# describes in a private manifest. S0 replays one Pi history through the
# shipped bridge import/upload, S3 seeds explicitly labelled retrieval-only
# fixtures through public routes and S4 renders automatic context with the
# shipped bridge. No /work, extraction, consolidation or native generation.
NATIVE_SCHEMA = 'memento-native-acceptance-v1'
NATIVE_REPORT_SCHEMA = 'memento-native-retrieval-report-v1'
NATIVE_CORPUS_VERSION = 'native-retrieval-v1'
NATIVE_SEED_ACTOR = 'claude-native-retrieval-fixture'
NATIVE_FIXTURE_SOURCE = 'api-capture:memento-native-retrieval-fixture'
NATIVE_PRODUCTION_PORT = 8765
NATIVE_READY_TIMEOUT = 900
NATIVE_CLIENT_FILES = {'__init__.py', 'bridge.py', 'runner.py'}
# Every count must be exactly zero before the first write: the target is fresh.
NATIVE_STATUS_ZERO = (
    ('captured', 'episodes'), ('captured', 'conversations'), ('indexing', 'total'),
    *(('extraction', key) for key in ('pending_jobs', 'leased_jobs', 'completed_jobs', 'failed_jobs',
                                      'pending_conversations')),
    *(('consolidation', key) for key in ('pending_jobs', 'leased_jobs', 'completed_jobs', 'failed_jobs',
                                         'pending_candidates', 'ready_candidates', 'parked_candidates',
                                         'excluded_legacy_candidates')),
    ('artifact_storage', 'registered', 'objects'))
NATIVE_GATES = ('scope_leak', 'superseded_surfaced', 'unrelated_offered', 'budget_overrun',
                'partial_fact_delivered', 'out_of_quote_match')

NATIVE_JUNIPER_QUOTE = ('juniper-archive: upload rejected with ERR_CHUNK_QUOTA_4096 for release 4.2.17 '
                        'because chunk 51 exceeded the 50-record limit.')


def _native_build_log():
    # A large tool result: the cited line is one span; other text (including
    # "saffron") lies outside every quote and must never match its memory.
    lines = [f'build step {n:03d}: compiled module juniper_{n:03d} with cached dependencies in {n % 7 + 2} seconds'
             for n in range(1, 61)]
    lines.insert(20, 'Cafeteria notice copied into this log: saffron rice is served on Fridays.')
    lines.insert(45, NATIVE_JUNIPER_QUOTE)
    return '\n'.join(lines + ['cleanup: removed temporary chunk files', 'cleanup: archive session closed']) + '\n'


NATIVE_KESTREL = (
    'The Kestrel ingest pipeline reads partner CSV drops from the north bucket every twenty minutes, validates each '
    'header row against the frozen column contract, and moves every rejected file into the quarantine prefix for manual review.',
    'The Kestrel ingest pipeline deduplicates partner rows by the composite of account number, invoice date and line '
    'sequence, keeping the earliest arrival and writing every discarded duplicate row to the audit table with its arrival time.',
    "The Kestrel ingest pipeline converts all partner currency amounts to euros using the previous business day's closing "
    'rate, and records the rate source and the rate timestamp beside each converted amount for later reconciliation checks.',
    'The Kestrel ingest pipeline must finish its nightly load before 05:30 Europe/Berlin, because the morning finance '
    'dashboard refresh starts at that time and reads only the tables that the pipeline has already marked as complete.',
    'The Kestrel ingest pipeline pages the data on-call engineer when more than two percent of a partner drop is '
    'quarantined, and then suppresses repeat pages for the same partner for ninety minutes after the first page was sent.',
    'The Kestrel ingest pipeline keeps raw partner drops for thirty-five days in cold storage and then deletes them '
    'automatically, while the cleaned tables and the audit table are retained for seven years under the retention policy.',
)
NATIVE_ORRERY = (
    'The Orrery release signing key rotates every ninety days, the new public key is published to the internal key '
    'server one week before activation, and build agents fetch it during their nightly refresh so that no manual copy '
    'is ever needed; the rotation schedule applies only to signed offline snapshots, while online builds keep using '
    'the hardware-backed key without rotation.')
NATIVE_ORRERY_QUALIFIER = 'applies only to signed offline snapshots'

NATIVE_CAPTURE = dict(
    conversation='eval-capture-northwindow', search='NorthWindow', offset_days=2,
    user='My personal NorthWindow project uses accent code BLUE7294.',
    assistant='Noted: NorthWindow uses accent code BLUE7294.')
# Retrieval-only fixtures (evidence kind api_capture): direct /episodes rows,
# never presented as replayed native capture or extraction output.
NATIVE_EPISODES = {
    'timeout_a': dict(conversation='native-retrieval-larkspur', kind='user', offset_days=10,
                      text='The deploy timeout for the Larkspur service is 15 seconds.'),
    'timeout_b': dict(conversation='native-retrieval-larkspur', kind='user', offset_days=6,
                      text='Correction: the Larkspur deploy timeout is 45 seconds, not 15.'),
    'conflict_a': dict(conversation='native-retrieval-quillon-a', kind='user', offset_days=5,
                       text='The Quillon backup window opens at 01:00 UTC.'),
    'conflict_b': dict(conversation='native-retrieval-quillon-b', kind='user', offset_days=4,
                       text='The Quillon backup window opens at 03:00 UTC.'),
    'heron': dict(conversation='native-retrieval-heron', kind='user', offset_days=8,
                  text='Nightly Heron backups retain seven daily copies; the start time is not recorded here.'),
    'key': dict(conversation='native-retrieval-key', kind='user', offset_days=9,
                text='I keep the spare house key in the blue tin on the hallway shelf.'),
    'juniper_log': dict(conversation='native-retrieval-juniper', kind='tool', offset_days=3, text=_native_build_log()),
    'marlow_log': dict(conversation='native-retrieval-marlow', kind='tool', offset_days=3,
                       text='vendor-portal sync: Marlow Freight invoice attachment upload returned ERR_CHUNK_QUOTA_4096 after 3 retries.'),
    'lease': dict(conversation='native-retrieval-lease', kind='user', offset_days=7,
                  text='Договор аренды склада подписал Тарас Мирошниченко в понедельник.'),
    'kestrel': dict(conversation='native-retrieval-kestrel', kind='user', offset_days=5, text=' '.join(NATIVE_KESTREL)),
    'orrery': dict(conversation='native-retrieval-orrery', kind='user', offset_days=5, text=NATIVE_ORRERY),
}
NATIVE_MEMORIES = {
    'timeout': dict(episode='timeout_a', quote='The deploy timeout for the Larkspur service is 15 seconds.',
                    text='Larkspur service deploy timeout is 15 seconds.'),
    'conflict_a': dict(episode='conflict_a', quote='The Quillon backup window opens at 01:00 UTC.',
                       text='The Quillon backup window opens at 01:00 UTC.'),
    'conflict_b': dict(episode='conflict_b', quote='The Quillon backup window opens at 03:00 UTC.',
                       text='The Quillon backup window opens at 03:00 UTC.'),
    'heron': dict(episode='heron', quote='Nightly Heron backups retain seven daily copies',
                  text='Nightly Heron backups retain seven daily copies.'),
    'key': dict(episode='key', quote='I keep the spare house key in the blue tin on the hallway shelf.',
                text='The spare house key is kept in the blue tin on the hallway shelf.', scope=('personal',)),
    # Exact-token (R1) probes: the identifier lives only in the quote, not the memory text.
    'juniper': dict(episode='juniper_log', quote=NATIVE_JUNIPER_QUOTE,
                    text='The Juniper archive rejects uploads whose chunks exceed the 50-record limit.'),
    'marlow': dict(episode='marlow_log', quote=NATIVE_EPISODES['marlow_log']['text'],
                   text='Marlow Freight invoice attachment sync failed on an upload quota.'),
    'lease': dict(episode='lease', quote=NATIVE_EPISODES['lease']['text'],
                  text='The warehouse lease agreement was signed on Monday.'),
    **{f'kestrel_{n}': dict(episode='kestrel', quote=sentence, text=sentence)
       for n, sentence in enumerate(NATIVE_KESTREL, 1)},
    'orrery': dict(episode='orrery', quote=NATIVE_ORRERY, text=NATIVE_ORRERY),
}
NATIVE_SUPERSESSIONS = {
    'timeout': dict(episode='timeout_b', quote='the Larkspur deploy timeout is 45 seconds',
                    text='Larkspur service deploy timeout is 45 seconds.', kind='correction'),
}
NATIVE_QUERIES = [
    dict(id='current_timeout', slice='current_version', q='What is the deploy timeout for the Larkspur service?',
         modes=('normal', 'automatic'), expected=['timeout'], gold=['45 seconds']),
    dict(id='scope_key_professional', slice='scope', q='Where is the spare house key kept?',
         modes=('scoped',), scope='professional', forbidden=['key']),
    dict(id='scope_key_unscoped', slice='scope_control', q='Where is the spare house key kept?',
         modes=('normal', 'automatic'), expected=['key'], gold=['blue tin']),
    dict(id='unrelated_clouds', slice='unrelated', q='Explain how clouds form above a mountain.', modes=('automatic',)),
    dict(id='unrelated_menu', slice='unrelated', q='Translate this restaurant menu into Ukrainian.', modes=('automatic',)),
    dict(id='insufficient_heron', slice='relevant_insufficient', q='What time does the nightly Heron backup start?',
         modes=('normal', 'automatic'), relevant=['heron']),
    dict(id='conflict_quillon', slice='conflicting', q='When does the Quillon backup window open?',
         modes=('normal', 'automatic'), members=['conflict_a', 'conflict_b'], gold=['01:00', '03:00']),
    dict(id='r1_error_code', slice='r1_exact_token', q='ERR_CHUNK_QUOTA_4096', modes=('normal', 'automatic'),
         expected=['juniper', 'marlow']),
    dict(id='r1_version', slice='r1_exact_token', q='4.2.17', modes=('normal', 'automatic'), expected=['juniper']),
    dict(id='r1_cyrillic', slice='r1_exact_token', q='Мирошниченко', modes=('normal', 'automatic'), expected=['lease']),
    dict(id='r1_distractor', slice='r1_lexical_distractor',
         q='Why did the Juniper archive upload fail with ERR_CHUNK_QUOTA_4096?', modes=('normal', 'automatic'),
         expected=['juniper'], distractors=['marlow']),
    dict(id='r1_out_of_quote', slice='r1_out_of_quote_guard', q='saffron', modes=('normal', 'automatic'),
         forbidden=['juniper']),
    dict(id='delivery_kestrel', slice='delivery_budget_14a', q='How does the Kestrel ingest pipeline work?',
         modes=('normal', 'automatic'), expected=[f'kestrel_{n}' for n in range(1, 7)]),
    dict(id='delivery_orrery', slice='delivery_summary_14b', q='How often does the Orrery release signing key rotate?',
         modes=('normal', 'automatic'), expected=['orrery'], gold=['ninety days', NATIVE_ORRERY_QUALIFIER]),
]
NATIVE_QUERY_BY_ID = {spec['id']: spec for spec in NATIVE_QUERIES}
NATIVE_NOT_RUN = {
    '14c_behavioral_suggestion_budget': 'not seedable: remember/supersede accept no behavior field, and model '
                                        'consolidation stages are not released',
    'model_stages_S1_S2_S5': 'extraction, consolidation and generation are not released; no /work request is made',
    'compaction_processing': 'import compaction markers are withheld locally because /work is out of scope',
    'native_hooks_and_detached_delivery': 'bridge fetch/offer primitives run in-process; native hooks, the detached '
                                          'recall worker, delayed offers and exposure receipts are not exercised',
}


class NativePreflightError(Exception):
    """A refusal before the first write; carries a fixed reason and safe detail."""

    def __init__(self, reason, detail=None):
        super().__init__(reason)
        self.reason, self.detail = reason, detail


def native_sha256(data):
    return hashlib.sha256(data).hexdigest()


def native_corpus():
    return dict(version=NATIVE_CORPUS_VERSION, seed_actor=NATIVE_SEED_ACTOR, fixture_source=NATIVE_FIXTURE_SOURCE,
                capture=NATIVE_CAPTURE, episodes=NATIVE_EPISODES, memories=NATIVE_MEMORIES,
                supersessions=NATIVE_SUPERSESSIONS, queries=NATIVE_QUERIES)


def native_corpus_digest(corpus=None):
    return native_sha256(json.dumps(corpus or native_corpus(), ensure_ascii=False, sort_keys=True,
                                    separators=(',', ':')).encode('utf-8'))


def _native_error(error):
    """Fixed categories only; response bodies can carry private text."""
    cause = error.__cause__ if error.__cause__ is not None else error
    result = {'error': type(error).__name__, 'cause': type(cause).__name__}
    if isinstance(cause, urllib.error.HTTPError):
        result['http_status'] = cause.code
    return result


def _native_dig(value, path):
    for key in path:
        value = value.get(key) if isinstance(value, dict) else None
    return value


def native_manifest(path):
    """Validate the launcher manifest; refusal never contacts the service."""
    path = Path(path)
    try:
        info = path.lstat()
    except OSError:
        raise NativePreflightError('manifest_missing') from None
    if not stat.S_ISREG(info.st_mode):
        raise NativePreflightError('manifest_not_regular_file')
    if info.st_uid != os.getuid():
        raise NativePreflightError('manifest_not_owned')
    if info.st_mode & 0o077:
        raise NativePreflightError('manifest_not_private')
    raw = path.read_bytes()
    try:
        manifest = json.loads(raw)
    except ValueError:
        raise NativePreflightError('manifest_invalid_json') from None
    if not isinstance(manifest, dict) or manifest.get('schema') != NATIVE_SCHEMA:
        raise NativePreflightError('manifest_schema')
    if manifest.get('owned_isolated') is not True:
        raise NativePreflightError('manifest_not_owned_isolated')
    url, port = manifest.get('url'), manifest.get('port')
    if not isinstance(url, str) or type(port) is not int:
        raise NativePreflightError('manifest_url')
    parsed = urllib.parse.urlsplit(url)
    try:
        parsed_port = parsed.port
    except ValueError:
        parsed_port = None
    if (parsed.scheme != 'http' or parsed.hostname not in ('127.0.0.1', '::1') or parsed.username
            or parsed.password or parsed.path not in ('', '/') or parsed.query or parsed.fragment
            or parsed_port != port):
        raise NativePreflightError('manifest_url')
    if not 1024 <= port <= 65535 or port == NATIVE_PRODUCTION_PORT:
        raise NativePreflightError('manifest_port')
    for field in ('created_at', 'container_id', 'container_name', 'image', 'fresh_data_volume',
                  'baseline_pythonpath', 'model_identity_source'):
        if not isinstance(manifest.get(field), str) or not manifest[field].strip():
            raise NativePreflightError('manifest_field', field)
    for field, pattern in (('image_id', r'sha256:[0-9a-f]{64}'), ('asserted_source_commit', r'[0-9a-f]{40}'),
                           ('launcher_sha256', r'[0-9a-f]{64}')):
        if not isinstance(manifest.get(field), str) or not re.fullmatch(pattern, manifest[field]):
            raise NativePreflightError('manifest_field', field)
    if '/' in manifest['fresh_data_volume'] or manifest.get('data_mount') != '/data':
        raise NativePreflightError('manifest_data_volume')
    assets = manifest.get('model_assets')
    if not isinstance(assets, list) or not assets:
        raise NativePreflightError('manifest_model_assets')
    for asset in assets:
        destination = asset.get('destination') if isinstance(asset, dict) else None
        named = [isinstance(asset.get(key), str) and bool(asset[key]) for key in ('volume', 'path')] if destination else []
        if (not isinstance(destination, str) or not destination.startswith('/') or asset.get('read_only') is not True
                or named.count(True) != 1):
            raise NativePreflightError('manifest_model_assets')
        normalized = PurePosixPath(os.path.normpath(destination))
        if normalized == PurePosixPath('/data') or PurePosixPath('/data') in normalized.parents:
            raise NativePreflightError('manifest_model_assets')
    if not isinstance(manifest.get('resources'), dict):
        raise NativePreflightError('manifest_field', 'resources')
    files = manifest.get('client_files')
    if (not isinstance(files, dict) or 'bridge.py' not in files or not set(files) <= NATIVE_CLIENT_FILES
            or any(not isinstance(entry, dict) or not isinstance(entry.get('path'), str)
                   or not isinstance(entry.get('sha256'), str) or not re.fullmatch(r'[0-9a-f]{64}', entry['sha256'])
                   for entry in files.values())):
        raise NativePreflightError('manifest_client_files')
    return manifest, native_sha256(raw)


def native_executed_client(manifest, harness=None):
    """Digest the client module bytes this process imported, not a checkout."""
    import memento
    import memento.bridge
    import memento.runner
    package = Path(memento.__file__).resolve().parent
    if package.parent != Path(manifest['baseline_pythonpath']).resolve():
        raise NativePreflightError('client_not_baseline', str(package))
    modules = {'__init__.py': memento, 'bridge.py': memento.bridge, 'runner.py': memento.runner}
    executed, data = {}, {}
    for name, module in modules.items():
        location = Path(module.__file__).resolve()
        if location.parent != package:
            raise NativePreflightError('client_not_baseline', name)
        data[name] = location.read_bytes()
        executed[name] = {'path': str(location), 'sha256': native_sha256(data[name])}
    for name, expected in manifest['client_files'].items():
        if Path(expected['path']).resolve() != Path(executed[name]['path']):
            raise NativePreflightError('client_path_mismatch', name)
        if expected['sha256'] != executed[name]['sha256']:
            raise NativePreflightError('client_digest_mismatch', name)
    harness = Path(harness or __file__).resolve()
    executed['harness'] = {'path': str(harness), 'sha256': native_sha256(harness.read_bytes())}
    return executed, data['bridge.py'], data['runner.py']


def native_target(service, bridge_bytes, runner_bytes):
    """GET-only readiness/emptiness/staleness checks before any write."""
    try:
        health = service.get('/health')
        status = service.get('/status')
        served_bridge = service.download('/bridge')
        served_runner = service.download('/runner')
    except Exception as error:
        raise NativePreflightError('target_unreachable', _native_error(error)) from None
    if health.get('status') != 'ok' or health.get('search_ready') is not True:
        raise NativePreflightError('target_not_ready', {key: health.get(key) for key in ('status', 'search_ready')})
    values = {'.'.join(path): _native_dig(status, path) for path in NATIVE_STATUS_ZERO}
    missing = sorted(key for key, value in values.items() if type(value) is not int)
    if missing:
        raise NativePreflightError('target_status_shape', missing)
    nonzero = sorted(key for key, value in values.items() if value != 0)
    if nonzero:
        raise NativePreflightError('target_not_empty', nonzero)
    if _native_dig(status, ('indexing', 'available')) is not True:
        raise NativePreflightError('target_index_unavailable')
    # The service stamps its bundle revision into the one shipped marker.
    stamps = re.findall(rb"CLIENT_REVISION = '([0-9a-f]{64})'", served_bridge)
    if len(stamps) != 1:
        raise NativePreflightError('served_bridge_unstamped')
    revision = stamps[0]
    if served_bridge.replace(b"CLIENT_REVISION = '" + revision + b"'", b'CLIENT_REVISION = None', 1) != bridge_bytes:
        raise NativePreflightError('stale_executed_bridge')
    if served_runner != runner_bytes:
        raise NativePreflightError('stale_executed_runner')
    return dict(health=health, status_zero=values, served_revision=revision.decode(),
                served_bridge_sha256=native_sha256(served_bridge), served_runner_sha256=native_sha256(served_runner))


def _native_time(value):
    return value.isoformat(timespec='seconds')


def _native_same_time(stored, expected):
    try:
        return datetime.fromisoformat(stored.replace('Z', '+00:00')) == datetime.fromisoformat(expected)
    except (AttributeError, TypeError, ValueError):
        return False


def native_capture(service, config_path, config, run_start, *, sleep=time.sleep):
    """S0: replay one Pi JSONL history through shipped import/upload."""
    from memento import bridge
    spec = NATIVE_CAPTURE
    work = Path(config_path).parent
    user_at = run_start - timedelta(days=spec['offset_days'])
    assistant_at = user_at + timedelta(seconds=5)
    journal = work / 'pi-history' / 'northwindow.jsonl'
    journal.parent.mkdir(mode=0o700)
    records = [
        {'type': 'session', 'id': spec['conversation'], 'timestamp': _native_time(user_at - timedelta(seconds=5)),
         'cwd': str(work)},
        {'type': 'message', 'id': 'capture-user', 'parentId': None, 'timestamp': _native_time(user_at),
         'message': {'role': 'user', 'content': [{'type': 'text', 'text': spec['user']}]}},
        {'type': 'message', 'id': 'capture-assistant', 'parentId': 'capture-user',
         'timestamp': _native_time(assistant_at),
         'message': {'role': 'assistant', 'content': [{'type': 'text', 'text': spec['assistant']}]}},
    ]
    journal.write_text(''.join(json.dumps(record, ensure_ascii=False) + '\n' for record in records), encoding='utf-8')

    def queue_without_work():
        spool = bridge.Spool(config_path, config)
        try:
            endpoints = dict(spool.db.execute("SELECT endpoint,COUNT(*) FROM outbox WHERE state='pending' GROUP BY endpoint"))
            if not set(endpoints) <= {'/episodes', '/work/compaction'}:
                raise RuntimeError('capture_unexpected_endpoint')
            # Import makes the history eligible for extraction; /work is out of scope.
            withheld = spool.db.execute("DELETE FROM outbox WHERE endpoint='/work/compaction'").rowcount
            spool.commit()
            return endpoints.get('/episodes', 0), withheld
        finally:
            spool.close()

    spool = bridge.Spool(config_path, config)
    try:
        imported = bridge.import_history(spool, 'pi', [journal])
    finally:
        spool.close()
    queued, withheld = queue_without_work()
    uploaded = bridge.upload(config_path, config)
    if uploaded.get('pending') or uploaded.get('conflicts') or uploaded.get('error'):
        raise RuntimeError('capture_upload_incomplete')
    source = bridge.source(config, 'pi')
    query = '/episodes/search?' + urlencode({'q': spec['search'], 'conversation_id': spec['conversation'],
                                             'source': source, 'limit': 20})
    expected = {'user': (spec['user'], user_at), 'assistant': (spec['assistant'], assistant_at)}
    found = {}
    for _ in range(30):
        # Search rows carry excerpts; exact text comes only from /episodes/{id}.
        episodes = service.get(query).get('episodes') or []
        for episode in episodes:
            kind = episode.get('kind') if isinstance(episode, dict) else None
            if kind in expected and kind not in found:
                stored = service.get('/episodes/' + urllib.parse.quote(str(episode.get('id')), safe=''))
                if stored.get('text') == expected[kind][0]:
                    found[kind] = stored
        if len(found) == len(expected):
            break
        sleep(1)
    else:
        raise RuntimeError('capture_not_searchable')
    verified = {}
    for kind, (text, at) in expected.items():
        stored = found[kind]
        checks = dict(text=stored.get('text') == text, kind=stored.get('kind') == kind,
                      source=stored.get('source') == source,
                      conversation=stored.get('conversation_id') == spec['conversation'],
                      occurred_at=_native_same_time(stored.get('occurred_at'), _native_time(at)))
        if not all(checks.values()):
            raise RuntimeError('capture_mismatch:' + kind)
        verified[kind] = dict(id=stored['id'], text_bytes=len(text.encode('utf-8')), checks=checks)
    searched = len(episodes)
    spool = bridge.Spool(config_path, config)
    try:
        again = bridge.import_history(spool, 'pi', [journal])
    finally:
        spool.close()
    requeued, rewithheld = queue_without_work()
    if again.get('records_added') or requeued:
        raise RuntimeError('capture_reimport_not_idempotent')
    if len(service.get(query).get('episodes') or []) != searched:
        raise RuntimeError('capture_reimport_changed_search')
    return dict(evidence_kind='replay_native_projection', harness='pi', source=source,
                conversation=spec['conversation'], records=len(records), import_records_added=imported.get('records_added'),
                import_coverage=imported.get('coverage'), episodes_uploaded=queued, compaction_markers_withheld=withheld,
                upload=uploaded, verified=verified, search_hits=searched,
                reimport=dict(records_added=again.get('records_added'), episodes_queued=requeued,
                              compaction_markers_withheld=rewithheld))


def native_seed(service, nonce, run_start):
    """S3: retrieval-only fixtures through public routes, verified via /history."""
    episodes, episode_ids, memories, memory_ids, oracle = {}, {}, {}, {}, {}
    why = 'Frozen native-retrieval fixture: api_capture evidence, retrieval-only, not extracted'
    for key, spec in NATIVE_EPISODES.items():
        mode = 'tool_output' if spec['kind'] == 'tool' else 'message'
        result = service.post('/episodes', dict(
            source=NATIVE_FIXTURE_SOURCE, event_id=f'{nonce}:{key}', conversation_id=spec['conversation'],
            kind=spec['kind'], text=spec['text'], occurred_at=_native_time(run_start - timedelta(days=spec['offset_days'])),
            metadata={'extraction_mode': mode, 'fixture': key, 'evidence_kind': 'api_capture'}))
        identifier = result.get('id')
        if not isinstance(identifier, str) or not identifier:
            raise RuntimeError('seed_episode_rejected:' + key)
        stored = service.get('/episodes/' + urllib.parse.quote(identifier, safe=''))
        if (stored.get('text') != spec['text'] or stored.get('kind') != spec['kind']
                or stored.get('source') != NATIVE_FIXTURE_SOURCE):
            raise RuntimeError('seed_episode_mismatch:' + key)
        episodes[key], episode_ids[identifier] = identifier, key
    for key, spec in NATIVE_MEMORIES.items():
        result = service.post('/remember', dict(
            command_id=f'native-retrieval:{nonce}:{key}', actor=NATIVE_SEED_ACTOR, why=why,
            evidence=[{'episode_id': episodes[spec['episode']], 'quote': spec['quote']}],
            type='knowledge', text=spec['text'], scope=list(spec.get('scope', ('professional',)))))
        if result.get('status') == 'pending_review' or not isinstance(result.get('id'), str):
            raise RuntimeError('seed_memory_not_committed:' + key)
        memories[key], memory_ids[result['id']] = result['id'], key
    returned = {}
    for key, spec in NATIVE_SUPERSESSIONS.items():
        base = NATIVE_MEMORIES[key]
        result = service.post('/supersede', dict(
            command_id=f'native-retrieval:{nonce}:{key}:supersede', actor=NATIVE_SEED_ACTOR, why=why,
            evidence=[{'episode_id': episodes[spec['episode']], 'quote': spec['quote']}], old=memories[key],
            new={'type': 'knowledge', 'text': spec['text'], 'scope': list(base.get('scope', ('professional',)))},
            kind=spec['kind']))
        if result.get('status') == 'pending_review':
            raise RuntimeError('seed_supersede_not_committed:' + key)
        returned[key] = result.get('id')
        if isinstance(result.get('id'), str) and result['id'] != memories[key]:
            memory_ids[result['id']] = key
    for key, identifier in memories.items():
        versions = service.get('/history/' + urllib.parse.quote(identifier, safe='')).get('versions')
        versions = [entry for entry in versions or [] if isinstance(entry, dict) and type(entry.get('version')) is int]
        if not versions:
            raise RuntimeError('seed_history_missing:' + key)
        current = max(versions, key=lambda entry: entry['version'])
        final = NATIVE_SUPERSESSIONS.get(key, NATIVE_MEMORIES[key])
        if current.get('text') != final['text']:
            raise RuntimeError('seed_history_mismatch:' + key)
        if final['quote'] not in [item.get('quote') for item in current.get('evidence') or [] if isinstance(item, dict)]:
            raise RuntimeError('seed_history_evidence:' + key)
        oracle[key] = dict(id=identifier, version=current['version'], text=current['text'], scope=current.get('scope'),
                           state=current.get('state'),
                           superseded_texts=sorted({entry['text'] for entry in versions if entry is not current
                                                    and isinstance(entry.get('text'), str) and entry['text'] != current['text']}))
    return dict(evidence_kind='api_capture', note='retrieval-only fixtures; not replayed native capture or extraction output',
                episodes=episodes, episode_ids=episode_ids, memories=memories, memory_ids=memory_ids,
                supersede_returned_ids=returned, oracle=oracle)


def native_status_summary(status):
    indexing = status.get('indexing') if isinstance(status.get('indexing'), dict) else {}
    search = indexing.get('search') if isinstance(indexing.get('search'), dict) else {}
    return dict(counts={'.'.join(path): _native_dig(status, path) for path in NATIVE_STATUS_ZERO},
                indexing={key: indexing.get(key) for key in ('available', 'total', 'indexed', 'pending', 'failed',
                                                            'unprepared_episodes')},
                search={key: search.get(key) for key in ('engine', 'ready', 'cold_ready')})


def native_wait_ready(service, timeout, *, poll=2.0, clock=time.monotonic, sleep=time.sleep):
    """Complete indexing: zero pending/failed/unprepared and a stable snapshot."""
    started, previous, timeline = clock(), None, []
    while True:
        summary = native_status_summary(service.get('/status'))
        snapshot = dict(**summary['indexing'], search_ready=summary['search']['ready'],
                        cold_ready=summary['search']['cold_ready'])
        if not timeline or timeline[-1]['snapshot'] != snapshot:
            timeline.append(dict(elapsed_s=round(clock() - started, 1), snapshot=snapshot))
        if type(snapshot['failed']) is int and snapshot['failed'] > 0:
            raise RuntimeError('index_failed')
        ready = (snapshot['available'] is True and snapshot['pending'] == 0 and snapshot['failed'] == 0
                 and snapshot['unprepared_episodes'] == 0 and snapshot['search_ready'] is True
                 and type(snapshot['total']) is int and snapshot['total'] > 0)
        if ready and snapshot == previous:
            return dict(elapsed_s=round(clock() - started, 1), final=snapshot, timeline=timeline[-20:],
                        cold_ready_basis='recorded only; Hot readiness does not imply Cold readiness')
        if clock() - started > timeout:
            raise RuntimeError('index_not_ready')
        previous = snapshot if ready else None
        sleep(poll)


def native_rows(memories, seeded):
    rows = []
    for rank, memory in enumerate(memories, 1):
        if not isinstance(memory, dict):
            rows.append(dict(rank=rank, invalid=True))
            continue
        identifier = str(memory.get('id'))
        fixture = seeded['memory_ids'].get(identifier)
        if fixture is None and memory.get('tier') == 'raw':
            fixture = 'raw:' + seeded['episode_ids'].get(str(memory.get('episode_id')), 'unmapped')
        oracle = seeded['oracle'].get(fixture)
        text, scope = memory.get('text'), memory.get('scope')
        if isinstance(scope, str):
            with contextlib.suppress(ValueError):
                scope = json.loads(scope)
        row = dict(rank=rank, id=identifier, fixture=fixture, version=memory.get('version'),
                   current_version=oracle['version'] if oracle else None,
                   tier=memory.get('tier'), type=memory.get('type'), category=memory.get('category'),
                   scope=scope, server_fields={key: memory.get(key) for key in ('score', 'cosine', 'match')},
                   text_bytes=len(text.encode('utf-8')) if isinstance(text, str) else None,
                   text_sha256=native_sha256(text.encode('utf-8')) if isinstance(text, str) else None)
        if oracle and isinstance(text, str):
            row['text_state'] = ('complete' if oracle['text'] and oracle['text'] in text else
                                 'superseded' if text in oracle['superseded_texts'] else
                                 'server_summary' if oracle['text'].startswith(text) else 'differs')
        rows.append(row)
    return rows


def native_offer_analysis(fetched, offer, budget, seeded, *, returned=()):
    """Completeness against the current /history body, never the recall summary."""
    context = offer.get('context') or ''
    lines = context.split('\n')[1:] if context else []
    shown = list(offer.get('shown') or [])
    items, delivered = [], {}
    for item in fetched['items']:
        for line in lines:
            if line.startswith(item['prefix']):
                delivered[item['id']] = line[len(item['prefix']):]
                break
    for rank, item in enumerate(fetched['items'], 1):
        fixture = seeded['memory_ids'].get(item['id'])
        oracle = seeded['oracle'].get(fixture)
        kind = 'suggestion' if item['suggestion'] else 'media' if item.get('media') else 'fact'
        text = delivered.get(item['id'])
        entry = dict(rank=rank, id=item['id'], fixture=fixture, kind=kind, shown=item['id'] in shown,
                     fetched_bytes=len(item['text'].encode('utf-8')),
                     delivered_bytes=len(text.encode('utf-8')) if text is not None else None,
                     body_bytes=len(oracle['text'].encode('utf-8')) if oracle else None)
        if text is None:
            entry['state'] = 'omitted'
        elif kind != 'fact':
            entry['state'] = 'whole_or_skip'
            entry['rationale_included'] = ' Rationale: ' in text
        elif oracle is None:
            entry['state'] = 'unknown_body'
        elif oracle['text'] and oracle['text'] in text:
            entry['state'] = 'complete'
        else:
            entry['state'] = 'partial'
            cut = []
            if item['text'] != oracle['text']:
                cut.append('server_summary' if oracle['text'].startswith(item['text']) else 'server_text_differs')
            if text != item['text']:
                cut.append('bridge_budget' if item['text'].startswith(text) else 'bridge_render_differs')
            entry['cut_by'] = cut
        items.append(entry)
    fetched_ids = [item['id'] for item in fetched['items']]
    returned_ids = list(dict.fromkeys(row['id'] for row in returned
                                     if isinstance(row, dict) and isinstance(row.get('id'), str)))
    omitted = [identifier for identifier in dict.fromkeys(returned_ids + fetched_ids) if identifier not in shown]
    withheld = [identifier for identifier in returned_ids if identifier not in fetched_ids]
    size = len(context.encode('utf-8'))
    return dict(budget=budget, context_bytes=size, overrun=size > budget, shown=shown,
                shown_fixtures=[seeded['memory_ids'].get(identifier) for identifier in shown],
                omitted_ids=omitted, omitted_fixtures=[seeded['memory_ids'].get(identifier) for identifier in omitted],
                withheld_before_packing_ids=withheld,
                withheld_before_packing_fixtures=[seeded['memory_ids'].get(identifier) for identifier in withheld],
                context_sha256=native_sha256(context.encode('utf-8')), update_notice='update_notice' in offer,
                gold_delivered=None, items=items)


def native_http_recall(service, params, seeded):
    started = time.perf_counter()
    try:
        result = service.get('/recall?' + urlencode(params))
    except Exception as error:
        return dict(params=params, elapsed_ms=round((time.perf_counter() - started) * 1000, 1), error=_native_error(error))
    elapsed = round((time.perf_counter() - started) * 1000, 1)
    if not isinstance(result.get('memories'), list):
        return dict(params=params, elapsed_ms=elapsed, error={'error': 'invalid_recall_response'})
    return dict(params=params, elapsed_ms=elapsed, client_revision=result.get('client_revision'),
                rows=native_rows(result['memories'], seeded))


def native_automatic(spec, config, spool, seeded, label):
    """S4: the shipped bridge fetch/offer on the real automatic /recall response."""
    from memento import bridge
    original, observed = bridge.http, {}

    def observe(*args, **kwargs):
        observed['response'] = original(*args, **kwargs)
        return observed['response']
    params = {'q': spec['q'], 'effort': 'cheap', 'limit': 6, 'automatic': 'true'}
    started = time.perf_counter()
    try:
        with patch.object(bridge, 'http', observe):
            # The full detached lifetime keeps a late result measurable; exceeding
            # the foreground wait is reported, not hidden.
            fetched = bridge.recall_fetch(config, {'query': spec['q'], 'previous': ''}, bridge.RECALL_LIFETIME)
    except (OSError, ValueError, TypeError, bridge._http_exception()) as error:
        return dict(params=params, elapsed_ms=round((time.perf_counter() - started) * 1000, 1), error=_native_error(error))
    elapsed = round((time.perf_counter() - started) * 1000, 1)
    offers = {}
    for channel, budget, step in (('human', bridge.CONTEXT_BYTES, False), ('step', bridge.STEP_CONTEXT_BYTES, True)):
        offer = bridge.recall_offer(spool, f'{label}:{channel}', spec['q'], fetched, budget=budget,
                                    notice_in_context=not step, harness=None, step=step)
        offers[channel] = native_offer_analysis(fetched, offer, budget, seeded,
                                                returned=observed['response']['memories'])
        offers[channel]['gold_delivered'] = {token: token in (offer.get('context') or '') for token in spec.get('gold', ())}
    return dict(params=params, elapsed_ms=elapsed, fetch_timeout_s=bridge.RECALL_LIFETIME,
                foreground_wait_s=bridge.RECALL_WAIT, exceeded_automatic_wait=elapsed > bridge.RECALL_WAIT * 1000,
                client_revision=fetched.get('revision'), rows=native_rows(observed['response']['memories'], seeded),
                offers=offers)


def native_pass(service, config, spool, seeded, nonce, index):
    queries = []
    for spec in NATIVE_QUERIES:
        modes = {}
        for mode in spec['modes']:
            if mode == 'automatic':
                modes[mode] = native_automatic(spec, config, spool, seeded,
                                               f'native-retrieval:{nonce}:{index}:{spec["id"]}')
            else:
                params = {'q': spec['q'], 'effort': 'normal', 'limit': 10,
                          **({'scope': spec['scope']} if mode == 'scoped' else {})}
                modes[mode] = native_http_recall(service, params, seeded)
        queries.append(dict(id=spec['id'], slice=spec['slice'], query=spec['q'], modes=modes))
    return {'pass': index, 'loadavg_after': list(os.getloadavg()), 'queries': queries}


def native_gates(passes, seeded):
    gates = {name: [] for name in NATIVE_GATES}
    oracle = seeded['oracle']
    for run in passes:
        for record in run['queries']:
            spec = NATIVE_QUERY_BY_ID[record['id']]
            for mode, result in record['modes'].items():
                where = {'pass': run['pass'], 'query': record['id'], 'mode': mode}
                for row in result.get('rows') or []:
                    fixture, at = row.get('fixture'), {**where, 'id': row.get('id'), 'fixture': row.get('fixture')}
                    stale = type(row.get('version')) is int and row['version'] != row.get('current_version')
                    if fixture in oracle and (stale or row.get('text_state') == 'superseded'):
                        gates['superseded_surfaced'].append(at)
                    if mode == 'scoped' and (fixture in spec.get('forbidden', ())
                                             or (fixture in oracle and spec['scope'] not in (oracle[fixture]['scope'] or []))
                                             or (isinstance(row.get('scope'), list) and spec['scope'] not in row['scope'])):
                        gates['scope_leak'].append(at)
                    if spec['slice'] == 'r1_out_of_quote_guard' and fixture in spec.get('forbidden', ()):
                        gates['out_of_quote_match'].append(at)
                for channel, offer in (result.get('offers') or {}).items():
                    at = {**where, 'channel': channel}
                    if offer['overrun']:
                        gates['budget_overrun'].append({**at, 'context_bytes': offer['context_bytes'],
                                                        'budget': offer['budget']})
                    if spec['slice'] == 'unrelated' and offer['shown']:
                        gates['unrelated_offered'].append({**at, 'shown_fixtures': offer['shown_fixtures']})
                    if spec['slice'] == 'r1_out_of_quote_guard' and set(offer['shown_fixtures']) & set(spec['forbidden']):
                        gates['out_of_quote_match'].append({**at, 'offered': True})
                    for item in offer['items']:
                        if item['state'] == 'partial':
                            gates['partial_fact_delivered'].append({**at, 'id': item['id'], 'fixture': item['fixture'],
                                                                    'cut_by': item['cut_by']})
    return {name: dict(violations=len(found), details=found) for name, found in gates.items()}


def native_observations(passes):
    out = dict(expected_ranks={}, relevant_insufficient={}, conflicting={}, r1={}, delivery={}, raw_rows={},
               automatic_wait_exceeded=[], update_notices=[], gold_delivered={}, rows_missing_version=[],
               rationale_included='not_applicable: no behavioral suggestion is seedable (14c not run)')
    for run in passes:
        number = str(run['pass'])
        for record in run['queries']:
            spec = NATIVE_QUERY_BY_ID[record['id']]
            for mode, result in record['modes'].items():
                key = record['id'] + '/' + mode
                ranks = {}
                for row in result.get('rows') or []:
                    ranks.setdefault(row.get('fixture'), row['rank'])
                offered = {channel: offer['shown_fixtures'] for channel, offer in (result.get('offers') or {}).items()}
                if spec.get('expected'):
                    out['expected_ranks'].setdefault(key, {})[number] = {name: ranks.get(name) for name in spec['expected']}
                if spec['slice'] == 'relevant_insufficient':
                    out['relevant_insufficient'].setdefault(key, {})[number] = dict(
                        ranks={name: ranks.get(name) for name in spec['relevant']}, offered=offered)
                if spec['slice'] == 'conflicting':
                    out['conflicting'].setdefault(key, {})[number] = dict(
                        returned={name: ranks.get(name) for name in spec['members']},
                        delivered={channel: sorted(set(spec['members']) & set(shown)) for channel, shown in offered.items()})
                if spec['slice'].startswith('r1_'):
                    out['r1'].setdefault(key, {})[number] = dict(
                        expected={name: ranks.get(name) for name in spec.get('expected', ())},
                        distractors={name: ranks.get(name) for name in spec.get('distractors', ())},
                        forbidden={name: ranks.get(name) for name in spec.get('forbidden', ())})
                for row in result.get('rows') or []:
                    if row.get('fixture') and type(row.get('version')) is not int:
                        out['rows_missing_version'].append({'pass': run['pass'], 'query': record['id'],
                                                            'mode': mode, 'id': row.get('id')})
                raw = sum(1 for row in result.get('rows') or [] if row.get('tier') == 'raw')
                if raw:
                    out['raw_rows'].setdefault(key, {})[number] = raw
                if result.get('exceeded_automatic_wait'):
                    out['automatic_wait_exceeded'].append({'pass': run['pass'], 'query': record['id'],
                                                            'elapsed_ms': result['elapsed_ms']})
                for channel, offer in (result.get('offers') or {}).items():
                    out['delivery'].setdefault(key + '/' + channel, {})[number] = dict(
                        context_bytes=offer['context_bytes'], shown=offer['shown_fixtures'],
                        omitted=offer['omitted_fixtures'],
                        withheld_before_packing=offer.get('withheld_before_packing_fixtures', []),
                        cuts={item['fixture'] or item['id']: item['cut_by'] for item in offer['items'] if item.get('cut_by')})
                    if offer['update_notice']:
                        out['update_notices'].append({'pass': run['pass'], 'query': record['id'], 'channel': channel})
                    if offer['gold_delivered']:
                        out['gold_delivered'].setdefault(key + '/' + channel, {})[number] = offer['gold_delivered']
    return out


def native_differences(first, second):
    """Same-instance repeat: any ordering/offer change stays visible."""
    changed, drift = [], []
    for a, b in zip(first['queries'], second['queries']):
        for mode, x in a['modes'].items():
            y = b['modes'].get(mode, {})
            order = [[row.get('id'), row.get('version')] for row in x.get('rows') or []]
            again = [[row.get('id'), row.get('version')] for row in y.get('rows') or []]
            if order != again or bool(x.get('error')) != bool(y.get('error')):
                changed.append(dict(query=a['id'], mode=mode, field='rows', first=order, second=again))
            for row, other in zip(x.get('rows') or [], y.get('rows') or []):
                if row.get('id') != other.get('id'):
                    continue
                for field in ('score', 'cosine'):
                    p, q = row['server_fields'].get(field), other['server_fields'].get(field)
                    if isinstance(p, (int, float)) and isinstance(q, (int, float)) and p != q:
                        drift.append(dict(query=a['id'], mode=mode, id=row['id'], field=field, delta=abs(p - q)))
            for channel, offer in (x.get('offers') or {}).items():
                again_offer = (y.get('offers') or {}).get(channel) or {}
                if (offer['shown'], offer['context_sha256']) != (again_offer.get('shown'), again_offer.get('context_sha256')):
                    changed.append(dict(query=a['id'], mode=mode, field='offer:' + channel,
                                        first=offer['shown'], second=again_offer.get('shown')))
    return dict(identical=not changed, nondeterministic=changed, score_drift=drift,
                basis='two passes against the same instance after readiness')


def native_mcp_parity(service, url, first):
    """One recall through the existing MCP endpoint with stdlib HTTP only."""
    spec = NATIVE_QUERY_BY_ID['current_timeout']
    arguments = {'query': spec['q'], 'effort': 'normal', 'limit': 10}
    body = json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': 'tools/call',
                       'params': {'name': 'recall', 'arguments': arguments}}).encode('utf-8')
    request = urllib.request.Request(url.rstrip('/') + '/mcp/', data=body, method='POST', headers={
        'Content-Type': 'application/json', 'Accept': 'application/json, text/event-stream'})
    http_rows = next(record for record in first['queries'] if record['id'] == spec['id'])['modes']['normal'].get('rows')
    expected = [[row.get('id'), row.get('version')] for row in http_rows or []]
    base = dict(query=spec['id'], arguments=arguments, http_order=expected,
                transport='stdlib urllib through the runner Service opener (no proxies or redirects)')
    started = time.perf_counter()
    try:
        with service.opener.open(request, timeout=service.timeout) as response:
            raw = response.read(8 * 1024 * 1024 + 1)
        payload = json.loads(raw)
    except Exception as error:
        return dict(base, state='error', error=_native_error(error))
    elapsed = round((time.perf_counter() - started) * 1000, 1)
    result = payload.get('result') if isinstance(payload, dict) else None
    structured = result.get('structuredContent') if isinstance(result, dict) else None
    if not isinstance(result, dict) or result.get('isError') or not isinstance(structured, dict) \
            or not isinstance(structured.get('memories'), list):
        return dict(base, state='shape_unrecognized', elapsed_ms=elapsed,
                    keys=sorted(structured) if isinstance(structured, dict) else None)
    order = [[item.get('id'), item.get('version')] for item in structured['memories'] if isinstance(item, dict)]
    return dict(base, state='run', elapsed_ms=elapsed, mcp_order=order, equal=order == expected)


def native_latency(passes, loadavg_start):
    samples = {}
    for run in passes:
        for record in run['queries']:
            for mode, result in record['modes'].items():
                if 'elapsed_ms' in result and not result.get('error'):
                    samples.setdefault(mode, []).append(result['elapsed_ms'])

    def nearest(values, fraction):
        return values[max(0, math.ceil(fraction * len(values)) - 1)]
    summary = {mode: dict(n=len(values), p50_ms=nearest(sorted(values), .5), p95_ms=nearest(sorted(values), .95),
                          max_ms=max(values)) for mode, values in samples.items()}
    return dict(per_mode=summary, cpu_count=os.cpu_count(), loadavg_start=loadavg_start,
                loadavg_end=list(os.getloadavg()),
                basis='descriptive client wall-clock on this host (nearest-rank); not a capacity or latency claim')


def native_retrieval(manifest_path, report, *, service_factory=None, ready_timeout=NATIVE_READY_TIMEOUT,
                     poll=2.0, sleep=time.sleep):
    """Return 0 (gates clear), 3 (completed with gate violations) or 2 (refused before any write)."""
    corpus_digest = native_corpus_digest()
    report.update(schema=NATIVE_REPORT_SCHEMA, mode='native-retrieval', stage='preflight',
                  corpus=dict(version=NATIVE_CORPUS_VERSION, sha256=corpus_digest, frozen_before_first_query=None))
    try:
        manifest, manifest_sha = native_manifest(manifest_path)
        report['launcher_attested'] = dict(
            manifest_path=str(Path(manifest_path).resolve()), manifest_sha256=manifest_sha,
            basis='copied from the launcher manifest; provenance is the launcher\'s claim, not harness-verified',
            **{key: manifest.get(key) for key in (
                'created_at', 'url', 'port', 'container_id', 'container_name', 'image', 'image_id',
                'asserted_source_commit', 'fresh_data_volume', 'data_mount', 'baseline_pythonpath', 'model_assets',
                'model_identity_source', 'resources', 'launcher_sha256')})
        executed, bridge_bytes, runner_bytes = native_executed_client(manifest)
        report['executed_client'] = executed
        if service_factory is None:
            from memento.runner import Service as service_factory
        service = service_factory(manifest['url'])
        report['target'] = native_target(service, bridge_bytes, runner_bytes)
    except NativePreflightError as refusal:
        report['stage'] = 'preflight_refused'
        report['preflight_refusal'] = dict(reason=refusal.reason, **({'detail': refusal.detail} if refusal.detail else {}))
        report['exit_code'] = 2
        return 2
    from memento import bridge
    loadavg_start = list(os.getloadavg())
    run_start = datetime.now(timezone.utc).replace(microsecond=0)
    nonce = uuid4().hex[:12]
    report['run'] = dict(started_at=_native_time(run_start), nonce=nonce,
                         date_basis='fixture occurred_at values are whole-day offsets before started_at')
    with tempfile.TemporaryDirectory(prefix='memento-native-retrieval-') as directory:
        work = Path(directory)
        os.chmod(work, 0o700)
        config_path = work / 'runner.json'
        config = dict(uid=os.getuid(), client_uid='native-retrieval-' + nonce, mode='client', agent='pi',
                      preset='pi', timezone='UTC', url=manifest['url'])
        bridge.private_write(config_path, bridge.encode(config))
        report['stage'] = 'capture'
        report['capture'] = native_capture(service, config_path, config, run_start, sleep=sleep)
        report['stage'] = 'seed'
        seeded = native_seed(service, nonce, run_start)
        report['seed'] = seeded
        report['stage'] = 'readiness'
        report['readiness'] = native_wait_ready(service, ready_timeout, poll=poll, sleep=sleep)
        if native_corpus_digest() != corpus_digest:
            raise RuntimeError('corpus_changed')
        report['corpus']['frozen_before_first_query'] = True
        report['stage'] = 'queries'
        # Installed clients receive the service-stamped bridge; mirror that stamp
        # in-process. The executed file digest above remains the unstamped bytes.
        report['client_revision_runtime'] = report['target']['served_revision']
        spool = bridge.Spool(config_path, config)
        try:
            with patch.object(bridge, 'CLIENT_REVISION', report['target']['served_revision']):
                passes = [native_pass(service, config, spool, seeded, nonce, index) for index in (1, 2)]
        finally:
            spool.close()
        report['passes'] = passes
        report['mcp_parity'] = native_mcp_parity(service, manifest['url'], passes[0])
        report['final_status'] = native_status_summary(service.get('/status'))
    report['gates'] = native_gates(passes, seeded)
    report['observations'] = native_observations(passes)
    report['differences'] = native_differences(*passes)
    report['latency'] = native_latency(passes, loadavg_start)
    report['costs'] = dict(
        generative_model_calls=dict(value=0, basis='by construction: no /work, extraction, consolidation or native CLI'),
        non_generative_inference=dict(encoder='present', reranker='present', count='unknown: not exposed by the service'),
        native_usage='unknown: no native session runs', tokens='unknown')
    report['not_run'] = NATIVE_NOT_RUN
    report['not_measured'] = [
        'retrieval quality beyond this frozen corpus', 'production-scale latency, capacity or concurrency',
        'cold-tier readiness beyond the recorded flag', 'encoder/reranker invocation counts',
        'MCP beyond one parity query', 'update notices beyond the observed client revision']
    errors = [{'pass': run['pass'], 'query': record['id'], 'mode': mode, 'error': result['error']}
              for run in passes for record in run['queries'] for mode, result in record['modes'].items()
              if result.get('error')]
    report['query_errors'] = errors  # MCP parity state is reported separately and never sets the exit code
    violated = sorted(name for name, gate in report['gates'].items() if gate['violations'])
    report['interpretation'] = dict(
        hard_gates_zero=not violated, violated=violated,
        note=('Baseline observation, not an overall quality claim. Partial delivery (14a bridge budget, 14b server '
              'summary) is an expected observed defect at this baseline; gates are not tuned to pass. '
              'out_of_quote_match passes by construction before R1 and guards its later regression.'))
    report['stage'] = 'completed'
    report['exit_code'] = 1 if errors else 3 if violated else 0
    return report['exit_code']


# ---------------------------------------------------------------------------
# lesson-loop: synthetic native failure -> changed approach -> inferred lesson ->
# automatic offer to a fresh session -> paired behavior sample. Opt-in only.
# The harness launches and removes its own disposable native container.

LESSON_SCHEMA = 'memento-lesson-loop-v1'
LESSON_CONTAINER_PREFIX = 'memento-lesson-loop-'
LESSON_DEFAULT_PORT = 18766
LESSON_TOOL = 'stockq'
# Frozen before the first run. The tool, SKUs and outputs are invented. Run 2: the useful approach is an
# undocumented flag; `--help`, errors and the tool source never name it, so only the seeded trace teaches it.
LESSON_TOKEN = '--ledger archive'
LESSON_SEED = dict(
    conversation='lesson-loop-seed-session', offset_days=2,
    task='Find the reorder quantity for SKU KX-4471 using the stockq command.',
    steps=[('stockq find KX-4471', '(no output)'),
           ('stockq find KX-4471', '(no output)'),
           ('stockq find --ledger archive KX-4471', 'KX-4471 reorder_qty=240')],
    answer=('The reorder quantity for KX-4471 is 240. `stockq find KX-4471` returned no output twice without '
            'an error: plain `stockq find` silently skips SKUs kept in the archive ledger. The undocumented flag '
            '`--ledger archive` (not listed in `stockq --help`) searches it: `stockq find --ledger archive KX-4471` '
            'found it. Next time an SKU lookup with stockq is empty, rerun it once with `--ledger archive` instead '
            'of repeating the same command.'))
LESSON_SIMILAR_TASK = 'Find the reorder quantity for SKU MB-2290 using the stockq command. Reply with the number.'
LESSON_SIMILAR_ANSWER = '180'
LESSON_STOCKQ_HELP = 'usage: stockq find SKU\n  look up the reorder quantity of an inventory SKU'
LESSON_K = 2
LESSON_SCORE_RULE = (
    'frozen before any behavior session. A `find` invocation is any logged stockq call whose first argument is '
    '`find` and that is not a help request; `hit` = the tool printed a matching row. Per session: '
    'first_find_learned = the first find passes `--ledger archive` (or `--ledger=archive`); '
    'empty_finds_before_success = finds without a hit before the first hit (all finds if none hit); '
    f'success_within_k = a hit among the first {LESSON_K} finds; answered_correctly = final answers contain '
    f'{LESSON_SIMILAR_ANSWER!r}. Primary: first_find_learned count per arm; lesson-arm metrics are also reported '
    'restricted to sessions whose offered context carried the flag only through lesson ids.')
# The fake executable prints nothing (exit 0) unless the archive ledger is searched. It compares argument
# hashes so reading the script does not reveal the flag; unknown options get a generic error.
LESSON_STOCKQ = r'''#!/usr/bin/env python3
import hashlib, json, sys
LOG = %(log)r
HELP = %(help)r
H = lambda value: hashlib.sha256(value.encode()).hexdigest()
JOINED = 'a99ae075c3a00d15f2be4d55eb20ed9ef64432ea81eb4f3e360e47c8c05cb5a8'
NAME = 'd7a6f345840c1aead5cdb27372d9aacaac8cf9432246bf207f8922b8eaafb8b7'
VALUE = '0eb3e36bfb24dcd9bb1d1bece1531216b59539a8fde17ee80224af0653c92aa3'
args = sys.argv[1:]

def finish(outcome, code=0, out=None, err=None):
    with open(LOG, 'a') as stream:
        stream.write(json.dumps(dict(args=args, outcome=outcome)) + '\n')
    if out:
        print(out)
    if err:
        print(err, file=sys.stderr)
    sys.exit(code)
if not args or args[0] in ('-h', '--help', 'help') or (args[0] == 'find' and ({'-h', '--help'} & set(args))):
    finish('help', out=HELP)
if args[0] != 'find':
    finish('error', 2, err='stockq: unknown command')
wide, skus, rest, index = False, [], args[1:], 0
while index < len(rest):
    value = rest[index]
    if H(value) == JOINED:
        wide = True
    elif H(value) == NAME and index + 1 < len(rest) and H(rest[index + 1]) == VALUE:
        wide, index = True, index + 1
    elif value.startswith('-'):
        finish('error', 2, err='stockq: unknown option')
    else:
        skus.append(value)
    index += 1
if not skus:
    finish('error', 2, err=HELP)
rows = ['MB-2290 reorder_qty=180' for sku in skus if wide and sku.upper() == 'MB-2290']
finish('hit' if rows else 'empty', out='\n'.join(rows))
'''
LESSON_PI_FLAGS = ['--print', '--mode', 'json', '--no-session', '--no-extensions', '--no-mcp', '--no-skills',
                   '--no-prompt-templates', '--no-context-files', '--no-approve', '--no-themes', '--tools', 'bash']


def _lesson_cmd(argv, *, timeout=600, check=True):
    """Docker/git helper; private output stays in the caller's report."""
    result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f'command_failed:{argv[0]} {argv[1] if len(argv) > 1 else ""}:{result.returncode}:'
                           + result.stderr.strip()[-400:])
    return result


def lesson_asset_mounts(override_path):
    """Read-only model assets and environment exactly as the production override names them."""
    override = json.loads(Path(override_path).read_text())
    service = override['services']['memory']
    named = override.get('volumes') or {}
    mounts, environment = [], {}
    for volume in service.get('volumes') or []:
        if not isinstance(volume, dict):
            raise NativePreflightError('override_volume_shape')
        if volume.get('target') == '/data':
            continue  # Production data is never mounted.
        if volume.get('read_only') is not True:
            raise NativePreflightError('override_asset_not_read_only', volume.get('target'))
        source = volume['source']
        if volume.get('type') == 'volume':
            source = (named.get(source) or {}).get('name') or source
        elif volume.get('type') != 'bind' or not Path(source).is_dir():
            raise NativePreflightError('override_asset_missing', volume.get('target'))
        mounts.append(dict(type=volume['type'], source=source, destination=volume['target'], read_only=True))
    for key, value in (service.get('environment') or {}).items():
        if key.startswith('MEMENTO_') or key == 'OMP_NUM_THREADS':
            environment[key] = str(value)
    environment.pop('MEMENTO_ALLOWED_HOSTS', None)
    if not mounts:
        raise NativePreflightError('override_no_assets')
    return mounts, environment


def lesson_launch(args, nonce, report):
    """Build/launch one fresh container on a fresh volume; refuse production first."""
    repo = Path(__file__).resolve().parents[1]
    port = args.port
    if port == NATIVE_PRODUCTION_PORT or not 1024 <= port <= 65535:
        raise NativePreflightError('production_or_invalid_port', port)
    with socket.socket() as probe:
        try:
            probe.bind(('127.0.0.1', port))
        except OSError:
            raise NativePreflightError('port_in_use', port) from None
    published = _lesson_cmd(['docker', 'ps', '--format', '{{.Names}}\t{{.Ports}}']).stdout
    if any(f':{port}->' in line for line in published.splitlines()):
        raise NativePreflightError('port_in_use', port)
    mounts, environment = lesson_asset_mounts(args.compose_override)
    commit = _lesson_cmd(['git', '-C', str(repo), 'rev-parse', 'HEAD']).stdout.strip()
    dirty = bool(_lesson_cmd(['git', '-C', str(repo), 'status', '--porcelain', '--', 'crates', 'src',
                              'Cargo.toml', 'Cargo.lock', 'packaging', 'Dockerfile.native']).stdout.strip())
    image = args.image
    if image is None:
        image = f'memento-lesson-loop:{commit[:7]}'
        built = _lesson_cmd(['docker', 'build', '-f', str(repo / 'Dockerfile.native'), '-t', image, str(repo)],
                            timeout=7200)
        report['build'] = dict(image=image, log_tail=built.stdout[-2000:])
    image_id = _lesson_cmd(['docker', 'image', 'inspect', image, '--format', '{{.Id}}']).stdout.strip()
    name = LESSON_CONTAINER_PREFIX + nonce
    volume = name
    url = f'http://127.0.0.1:{port}'
    launch = dict(container_name=name, data_volume=volume, image=image, image_id=image_id, url=url, port=port,
                  source_commit=commit, source_dirty=dirty, model_assets=mounts, environment=environment,
                  compose_override=str(Path(args.compose_override).resolve()), container_id=None)
    report['launch'] = launch
    _lesson_cmd(['docker', 'volume', 'create', '--label', 'memento-lesson-loop=' + nonce, volume])
    launch['volume_created'] = True
    argv = ['docker', 'run', '-d', '--name', name, '--label', 'memento-lesson-loop=' + nonce,
            '--user', '10001:10001', '--read-only', '--tmpfs', '/tmp', '--cap-drop', 'ALL',
            '--security-opt', 'no-new-privileges:true', '--cpus', '2', '--memory', '5g', '--pids-limit', '256',
            '-p', f'127.0.0.1:{port}:8765', '-v', f'{volume}:/data',
            '-e', 'MEMENTO_EMBEDDINGS=1', '-e', f'MEMENTO_ALLOWED_HOSTS=127.0.0.1:{port}']
    for key, value in environment.items():
        argv += ['-e', f'{key}={value}']
    for mount in mounts:
        argv += ['-v', f'{mount["source"]}:{mount["destination"]}:ro']
    launch['container_id'] = _lesson_cmd(argv + [image]).stdout.strip()
    return launch


def lesson_teardown(launch, log_path):
    """Preserve container logs privately, then remove the container and its fresh volume."""
    result = {}
    if launch.get('container_id'):
        logs = _lesson_cmd(['docker', 'logs', launch['container_name']], check=False)
        fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as stream:
            stream.write(logs.stdout + logs.stderr)
        result['container_log'] = str(log_path)
        result['container_removed'] = _lesson_cmd(['docker', 'rm', '-f', launch['container_name']],
                                                  check=False).returncode == 0
    if launch.get('volume_created'):
        result['volume_removed'] = _lesson_cmd(['docker', 'volume', 'rm', launch['data_volume']],
                                               check=False).returncode == 0
    return result


def lesson_wait_health(service, timeout=300, sleep=time.sleep):
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        with contextlib.suppress(Exception):
            health = service.get('/health')
            if health.get('status') == 'ok' and health.get('search_ready') is True:
                return dict(elapsed_s=round(time.monotonic() - started, 1), health=health)
        sleep(2)
    raise RuntimeError('instance_not_ready')


def lesson_seed_records(run_start):
    """Pi JSONL: task, empty attempt, identical repeat, changed approach with result, answer."""
    spec = LESSON_SEED
    at = run_start - timedelta(days=spec['offset_days'])
    tick = iter(range(0, 3600, 5))

    def stamp():
        return _native_time(at + timedelta(seconds=next(tick)))
    records = [{'type': 'session', 'version': 3, 'id': spec['conversation'], 'timestamp': stamp(), 'cwd': '/tmp/inventory'},
               {'type': 'message', 'id': 'seed-user', 'parentId': None, 'timestamp': stamp(),
                'message': {'role': 'user', 'content': [{'type': 'text', 'text': spec['task']}]}}]
    parent = 'seed-user'
    for index, (command, output) in enumerate(spec['steps']):
        call = f'seed-call-{index}'
        records.append({'type': 'message', 'id': call, 'parentId': parent, 'timestamp': stamp(),
                        'message': {'role': 'assistant', 'content': [
                            {'type': 'toolCall', 'id': call, 'name': 'bash', 'arguments': {'command': command}}]}})
        records.append({'type': 'message', 'id': f'seed-result-{index}', 'parentId': call, 'timestamp': stamp(),
                        'message': {'role': 'toolResult', 'toolCallId': call, 'toolName': 'bash',
                                    'content': [{'type': 'text', 'text': output}], 'isError': False}})
        parent = f'seed-result-{index}'
    records.append({'type': 'message', 'id': 'seed-answer', 'parentId': parent, 'timestamp': stamp(),
                    'message': {'role': 'assistant', 'content': [{'type': 'text', 'text': spec['answer']}]}})
    return records


def lesson_seed(service, work, nonce, run_start):
    """Stage 0: replay the synthetic trace through the shipped Pi import/upload, keeping its boundary."""
    from memento import bridge
    config_path = work / 'seed-client.json'
    config = dict(uid=os.getuid(), client_uid='lesson-seed-' + nonce, mode='client', agent='pi', preset='pi',
                  timezone='UTC', url=service.base_url)
    bridge.private_write(config_path, bridge.encode(config))
    journal = work / 'seed-history' / 'stockq.jsonl'
    journal.parent.mkdir(mode=0o700)
    records = lesson_seed_records(run_start)
    journal.write_text(''.join(json.dumps(record) + '\n' for record in records), encoding='utf-8')
    spool = bridge.Spool(config_path, config)
    try:
        imported = bridge.import_history(spool, 'pi', [journal])
        endpoints = dict(spool.db.execute("SELECT endpoint,COUNT(*) FROM outbox WHERE state='pending' GROUP BY endpoint"))
    finally:
        spool.close()
    uploaded = bridge.upload(config_path, config)
    if uploaded.get('pending') or uploaded.get('conflicts') or uploaded.get('error'):
        raise RuntimeError('seed_upload_incomplete')
    source = bridge.source(config, 'pi')
    query = '/episodes/search?' + urlencode({'conversation_id': LESSON_SEED['conversation'], 'source': source,
                                             'limit': 100})
    episodes = []
    for _ in range(30):
        episodes = [item for item in service.get(query).get('episodes') or []
                    if isinstance(item, dict) and item.get('kind') != 'native_record']
        if len(episodes) >= 2 + 2 * len(LESSON_SEED['steps']):
            break
        time.sleep(1)
    stored = [service.get('/episodes/' + urllib.parse.quote(str(item['id']), safe='')) for item in episodes]
    stored.sort(key=lambda item: (str(item.get('occurred_at')), str(item.get('id'))))
    roles = []
    for item in stored:
        text = item.get('text') or ''
        role = ('attempt' if item.get('kind') == 'assistant' and '"toolCall"' in text else
                'result' if item.get('kind') == 'tool' else
                'task' if item.get('kind') == 'user' else
                'answer' if item.get('kind') == 'assistant' else 'other')
        roles.append(dict(id=item.get('id'), kind=item.get('kind'), role=role, occurred_at=item.get('occurred_at'),
                          text=text, metadata=item.get('metadata')))
    expected = ['task'] + ['attempt', 'result'] * len(LESSON_SEED['steps']) + ['answer']
    return dict(evidence_kind='replay_native_projection', source=source, conversation=LESSON_SEED['conversation'],
                import_records_added=imported.get('records_added'), queued_endpoints=endpoints, upload=uploaded,
                episodes=roles, order_matches=[item['role'] for item in roles] == expected,
                compaction_boundary_kept='/work/compaction' in endpoints)


def lesson_process(service, processor_command, max_ticks, captured, compaction):
    """Stage 1/2: the existing legacy runner lanes, recording claims and /work/complete bodies test-side."""
    from memento import runner
    config = dict(mode='server', preset='pi', agent='pi', command=list(processor_command), timezone='UTC',
                  deadline_seconds=900, url=service.base_url)
    original, lock = runner.Service.post, threading.Lock()

    def recording(self, path, value):
        entry = dict(path=path, at=_native_time(datetime.now(timezone.utc)))
        if path in ('/work/complete', '/work/release'):
            entry['request'] = value
        try:
            result = original(self, path, value)
        except Exception as error:
            entry['error'] = _native_error(error)
            if path in ('/work/claim', '/work/complete', '/work/release'):
                with lock:
                    captured.append(entry)
            raise
        if path in ('/work/claim', '/work/complete', '/work/release'):
            entry['response'] = result
            with lock:
                captured.append(entry)
        return result
    ticks = []
    with patch.object(runner.Service, 'post', recording):
        for index in range(max_ticks):
            before = len(captured)
            started = time.monotonic()
            try:
                # The first claim carries the seeded conversation's boundary, as a native compaction would.
                code = runner.legacy_tick(config, compaction if index == 0 else None, consolidate_now=True)
                error = None
            except Exception as exc:  # Preserve the failed tick; later ticks may retry.
                code, error = None, _native_error(exc)
            claims = [entry for entry in captured[before:] if entry['path'] == '/work/claim']
            worked = [item['kind'] for entry in claims for item in (entry.get('response') or {}).get('work') or []]
            ticks.append(dict(tick=index + 1, exit=code, error=error, elapsed_s=round(time.monotonic() - started, 1),
                              claimed_kinds=worked))
            if not worked:
                break
    pool = {kind: service.post('/work/pool', {'kind': kind}) for kind in ('extract', 'consolidate')}
    return dict(runner='memento.runner.legacy_tick (process_lane -> /work/complete)', processor_command=config['command'],
                preset='pi', consolidate_now=True, ticks=ticks,
                final_pool={kind: {key: value for key, value in result.items() if not isinstance(value, (list, dict))}
                            for kind, result in pool.items()})


def _lesson_kind(captured, complete):
    ident = (complete.get('request') or {}).get('id')
    for entry in captured:
        for item in (entry.get('response') or {}).get('work') or [] if entry['path'] == '/work/claim' else []:
            if item.get('id') == ident:
                return item.get('kind')
    return None


def lesson_stages_processing(service, seed, captured):
    """Separate stage verdicts from captured submissions plus current /history."""
    seeded = {item['id']: item for item in seed['episodes']}
    order = [item['id'] for item in seed['episodes']]
    completes = [dict(entry, kind=_lesson_kind(captured, entry)) for entry in captured
                 if entry['path'] == '/work/complete']
    extraction = []
    for entry in completes:
        if entry['kind'] != 'extract' or 'error' in entry:
            continue
        for window in (entry['request'].get('output') or {}).get('windows') or []:
            for candidate in window.get('candidates') or []:
                extraction.append(dict(window=window.get('id'), **candidate))
    tool_candidates = [item for item in extraction if LESSON_TOOL in json.dumps(item).lower()]
    stage1 = dict(passed=bool(tool_candidates), committed_extractions=sum(entry['kind'] == 'extract' and 'error' not in entry
                                                                          for entry in completes),
                  rejected_submissions=[dict(kind=entry['kind'], error=entry['error']) for entry in completes if 'error' in entry],
                  candidates=extraction, tool_related=len(tool_candidates),
                  basis='candidate whose text/quote names the seeded tool, from the committed extraction submission')
    actions = []
    for entry in completes:
        if entry['kind'] != 'consolidate' or 'error' in entry:
            continue
        for action in (entry['request'].get('output') or {}).get('actions') or []:
            selected = action.get('technical_evidence')
            if selected is None and not action.get('behavior'):
                continue
            selected = selected or []
            roles = [seeded.get(ident, {}).get('role', 'not_seeded') for ident in selected]
            positions = [order.index(ident) for ident in selected if ident in seeded]
            attempts = [order.index(ident) for ident in selected if seeded.get(ident, {}).get('role') == 'attempt']
            results = [order.index(ident) for ident in selected if seeded.get(ident, {}).get('role') == 'result']
            changed = [ident for ident in selected if seeded.get(ident, {}).get('role') == 'attempt'
                       and LESSON_TOKEN in seeded[ident]['text']]
            useful = [ident for ident in selected if seeded.get(ident, {}).get('role') == 'result'
                      and 'reorder_qty' in seeded[ident]['text']]
            actions.append(dict(operation=action.get('operation'), type=action.get('type'), text=action.get('text'),
                                behavior=action.get('behavior'), candidate_id=action.get('candidate_id'),
                                technical_evidence=selected, technical_roles=roles,
                                evidence_quotes=[ref.get('quote') for ref in action.get('evidence') or []],
                                grounded=dict(all_seeded=bool(selected) and len(positions) == len(selected),
                                              attempt_then_later_result=bool(attempts and results and max(results) > max(attempts)),
                                              names_changed_attempt=bool(changed), names_useful_result=bool(useful))))
    lesson_ids, histories = [], {}
    recalled = service.get('/recall?' + urlencode({'q': LESSON_SIMILAR_TASK, 'effort': 'normal', 'limit': 10}))
    for memory in recalled.get('memories') or []:
        if not isinstance(memory, dict) or memory.get('tier') == 'raw' or not isinstance(memory.get('id'), str):
            continue
        history = service.get('/history/' + urllib.parse.quote(memory['id'], safe=''))
        versions = [item for item in history.get('versions') or [] if isinstance(item, dict)]
        current = max(versions, key=lambda item: item.get('version') or 0) if versions else {}
        evidence = [ref.get('episode_id') for ref in current.get('evidence') or [] if isinstance(ref, dict)]
        histories[memory['id']] = dict(type=current.get('type'), text=current.get('text'), state=current.get('state'),
                                       authority=current.get('authority'), behavior=current.get('behavior'),
                                       version=current.get('version'), evidence_episode_ids=evidence,
                                       evidence_seeded=[ident in seeded for ident in evidence])
        if current.get('behavior') and any(ident in seeded for ident in evidence):
            lesson_ids.append(memory['id'])
    grounded = [action for action in actions if action['behavior'] and all(action['grounded'].values())]
    flag = LESSON_TOKEN.split()[0]
    specificity = {ident: dict(text_names_flag=flag in (histories[ident]['text'] or ''),
                               conduct_names_flag=flag in str((histories[ident]['behavior'] or {}).get('conduct') or ''),
                               offered_fields_name_flag=flag in json.dumps({key: value for key, value in
                                                                            (histories[ident]['behavior'] or {}).items()
                                                                            if key != 'authority'}))
                   for ident in lesson_ids}
    stage2 = dict(passed=bool(grounded and lesson_ids), lesson_ids=lesson_ids, submitted_lesson_actions=actions,
                  committed_histories=histories, lesson_specificity=specificity,
                  specificity_note=('non-gating: does the committed lesson name the undocumented flag? The automatic '
                                    'offer renders behavior fields, so offered_fields_name_flag decides usefulness'),
                  basis=('a committed consolidation submission whose behavior action selects technical_evidence that '
                         'are all seeded episodes, include an attempt followed by a later result, and name the changed '
                         'attempt and its useful result; plus a current /history behavior node citing seeded evidence. '
                         'Provenance/order only: not proof the model judged causality correctly.'))
    return stage1, stage2


def lesson_session(agent_command, work, label, task, client_path=None):
    """One fresh Pi print session in its own directory with the fake tool on PATH."""
    from memento import bridge
    directory = work / label
    (directory / 'bin').mkdir(parents=True, mode=0o700)
    log = directory / 'stockq.log'
    log.touch(mode=0o600)
    executable = directory / 'bin' / LESSON_TOOL
    executable.write_text(LESSON_STOCKQ % {'log': str(log), 'help': LESSON_STOCKQ_HELP})
    executable.chmod(0o700)
    argv = [*agent_command, *LESSON_PI_FLAGS]
    if client_path is not None:
        entrypoint = Path(__file__).resolve().parents[1] / 'src/memento/native_pi.ts'
        argv += ['--extension', str(entrypoint), '--memento-config', str(client_path)]
    environment = {key: value for key, value in os.environ.items() if key not in ('MEMENTO_PROCESSING', 'MEMENTO_CONFIG')}
    environment['PATH'] = str(directory / 'bin') + os.pathsep + environment.get('PATH', '')
    environment['MEMENTO_BRIDGE'] = str(Path(bridge.__file__).resolve())
    started = time.monotonic()
    try:
        result = subprocess.run(argv + [task], cwd=directory, env=environment, stdin=subprocess.DEVNULL,
                                capture_output=True, timeout=900)
        exit_code, stdout = result.returncode, result.stdout
    except subprocess.TimeoutExpired as error:
        exit_code, stdout = 'timeout', error.stdout or b''
    events = []
    for line in stdout.splitlines():
        with contextlib.suppress(ValueError):
            events.append(json.loads(line))
    session_id = next((event.get('id') for event in events if event.get('type') == 'session'), None)
    commands = [event.get('args', {}).get('command') for event in events if event.get('type') == 'tool_execution_start']
    answers = [block.get('text', '') for event in events if event.get('type') == 'message_end'
               and event.get('message', {}).get('role') == 'assistant'
               for block in event['message'].get('content') or [] if isinstance(block, dict) and block.get('type') == 'text']
    model = next((event['message'].get('model') for event in events if event.get('type') == 'message_end'
                  and event.get('message', {}).get('role') == 'assistant'), None)
    invocations = [json.loads(line) for line in log.read_text().splitlines() if line.strip()]
    finds = [entry for entry in invocations if entry['args'][:1] == ['find'] and entry['outcome'] != 'help']
    hits = [index for index, entry in enumerate(finds) if entry['outcome'] == 'hit']
    first = finds[0]['args'] if finds else None
    learned = bool(first) and ('--ledger=archive' in first or any(
        first[index] == '--ledger' and index + 1 < len(first) and first[index + 1] == 'archive'
        for index in range(len(first))))
    # Heuristic only: did the agent try to read the fake tool instead of using it?
    inspected = [command for command in commands if isinstance(command, str) and re.search(
        r'\b(cat|less|more|head|tail|sed|awk|strings|xxd|od|python3?|grep|file)\b[^|;&]*stockq', command)]
    return dict(label=label, extension=client_path is not None, exit=exit_code, model=model, session_id=session_id,
                elapsed_s=round(time.monotonic() - started, 1), shell_commands=commands, stockq_invocations=invocations,
                first_find=first, first_find_learned=learned, find_attempts=len(finds),
                empty_finds_before_success=hits[0] if hits else len(finds), succeeded=bool(hits),
                success_within_k=bool(hits) and hits[0] < LESSON_K, inspected_tool_source=inspected,
                answered_correctly=any(LESSON_SIMILAR_ANSWER in answer for answer in answers), final_answer=answers[-1:] or None,
                cited_memory_ids=sorted(set(re.findall(r'\[memory:([0-9a-f]+)\]', '\n'.join(answers)))))


def lesson_offer_and_receipt(service, client_path, config, session, lesson_ids):
    """Stage 3/4 for one extension session: the spool's actual offer, then the uploaded exposure receipt."""
    from memento import bridge
    spool = bridge.Spool(client_path, config)
    try:
        rows = [dict(conversation=conversation, query=query, context=context, shown=json.loads(shown))
                for conversation, query, context, shown in spool.db.execute(
                    'SELECT conversation,query,context,shown FROM recall_cache')]
        spool.commit()
    finally:
        spool.close()
    mine = [row for row in rows if row['conversation'] == session['session_id']]
    shown = sorted({ident for row in mine for ident in row['shown']})
    offered = [ident for ident in lesson_ids if ident in shown]
    uploaded = bridge.upload(client_path, config)
    source = bridge.source(config, 'pi')
    receipts = []
    if session['session_id']:
        found = service.get('/episodes/search?' + urlencode({'conversation_id': session['session_id'], 'source': source,
                                                             'limit': 100})).get('episodes') or []
        for item in found:
            if isinstance(item, dict) and item.get('kind') == 'native_hook':
                stored = service.get('/episodes/' + urllib.parse.quote(str(item['id']), safe=''))
                exposure = (stored.get('metadata') or {}).get('exposure')
                receipts.append(dict(id=item['id'], exposure=exposure))
    receipt_ids = {pair[0] for receipt in receipts for pair in receipt.get('exposure') or [] if isinstance(pair, list) and pair}
    flag = LESSON_TOKEN.split()[0]
    carriers = sorted({ident for row in mine for line in (row['context'] or '').splitlines()
                       for ident in re.findall(r'^\[memory:([0-9a-f]+)\]', line) if flag in line})
    return dict(session=session['label'], conversation=session['session_id'], recall_rows=mine, shown=shown,
                lesson_offered=offered, flag_via_lesson_ids=[ident for ident in carriers if ident in lesson_ids],
                flag_via_other_ids=[ident for ident in carriers if ident not in lesson_ids], other_shown=[ident for ident in shown if ident not in lesson_ids],
                upload=uploaded, receipts=receipts, lesson_receipted=[ident for ident in offered if ident in receipt_ids])


def lesson_loop(args, report, *, sleep=time.sleep):
    """Return 0 (all four stages passed), 3 (a stage failed), 2 (refused before any write)."""
    from memento import bridge
    from memento.runner import Service
    nonce = uuid4().hex[:10]
    run_start = datetime.now(timezone.utc).replace(microsecond=0)
    output = args.output.resolve()
    repo = Path(__file__).resolve().parents[1]
    report.update(schema=LESSON_SCHEMA, mode='lesson-loop', stage='preflight', nonce=nonce,
                  started_at=_native_time(run_start), score_rule=LESSON_SCORE_RULE,
                  agent_command=list(args.agent_command), processor_command=list(args.processor_command),
                  frozen=dict(seed=LESSON_SEED, similar_task=LESSON_SIMILAR_TASK, stockq_help=LESSON_STOCKQ_HELP,
                              token=LESSON_TOKEN, k=LESSON_K,
                              sha256=native_sha256(json.dumps([LESSON_SEED, LESSON_SIMILAR_TASK, LESSON_STOCKQ,
                                                               LESSON_STOCKQ_HELP, LESSON_TOKEN, LESSON_K,
                                                               LESSON_SCORE_RULE]).encode())))
    launch = {}
    try:
        try:
            if repo == output or repo in output.parents:
                raise NativePreflightError('report_inside_repository')
            if args.trials < 1:
                raise NativePreflightError('trials')
            launch = lesson_launch(args, nonce, report)
            service = Service(launch['url'])
            report['stage'] = 'startup'
            report['startup'] = lesson_wait_health(service, sleep=sleep)
            status = service.get('/status')
            nonzero = sorted('.'.join(path) for path in NATIVE_STATUS_ZERO if _native_dig(status, path) != 0)
            if nonzero:
                raise NativePreflightError('target_not_empty', nonzero)
        except NativePreflightError as refusal:
            report['stage'] = 'preflight_refused'
            report['preflight_refusal'] = dict(reason=refusal.reason, **({'detail': refusal.detail} if refusal.detail else {}))
            report['exit_code'] = 2
            return 2
        with tempfile.TemporaryDirectory(prefix='memento-lesson-loop-') as directory:
            work = Path(directory)
            os.chmod(work, 0o700)
            report['stage'] = 'seed'
            seed = report['seed'] = lesson_seed(service, work, nonce, run_start)
            report['stage'] = 'processing'
            captured = report['captured_work'] = []
            report['processing'] = lesson_process(service, args.processor_command, args.max_ticks, captured,
                                                  dict(source=seed['source'], conversation_id=seed['conversation']))
            stage1, stage2 = lesson_stages_processing(service, seed, captured)
            report['stages'] = {'1_candidate_extracted': stage1, '2_lesson_promoted_grounded': stage2}
            report['stage'] = 'readiness'
            with contextlib.suppress(RuntimeError):
                report['readiness'] = native_wait_ready(service, 300, sleep=sleep)
            report['stage'] = 'behavior'
            client_path = work / 'client.json'
            config = dict(uid=os.getuid(), client_uid='lesson-client-' + nonce, mode='client', agent='pi', preset='pi',
                          timezone='UTC', url=launch['url'])
            bridge.private_write(client_path, bridge.encode(config))
            sessions, offers = [], []
            # Alternate arms so time drift affects both; every session is a fresh process and directory.
            for index in range(args.trials):
                for arm in ('control', 'lesson'):
                    session = lesson_session(args.agent_command, work, f'{arm}-{index + 1}', LESSON_SIMILAR_TASK,
                                             client_path if arm == 'lesson' else None)
                    sessions.append(session)
                    if arm == 'lesson':
                        offers.append(lesson_offer_and_receipt(service, client_path, config, session, stage2['lesson_ids']))
            report['sessions'] = sessions
            report['offers'] = offers
            report['stages']['3_automatic_offer_fresh_session'] = dict(
                passed=bool(offers) and all(offer['lesson_offered'] for offer in offers),
                offered_sessions=sum(bool(offer['lesson_offered']) for offer in offers), sessions=len(offers),
                basis='lesson id in the shipped Pi extension/bridge recall_cache offer for that fresh session',
                contamination_note=('extension sessions are captured into this instance, so later lesson sessions can '
                                    'also be offered raw episodes of earlier ones; see offers[].other_shown'))
            report['stages']['4_receipt_recorded'] = dict(
                passed=bool(offers) and all(offer['lesson_receipted'] for offer in offers if offer['lesson_offered'])
                and any(offer['lesson_offered'] for offer in offers),
                receipted_sessions=sum(bool(offer['lesson_receipted']) for offer in offers),
                basis='uploaded native_hook exposure episode in the session conversation naming the lesson id')
            by_label = {offer['session']: offer for offer in offers}

            def summary(runs):
                return dict(n=len(runs), first_find_learned=sum(run['first_find_learned'] for run in runs),
                            succeeded=sum(run['succeeded'] for run in runs),
                            success_within_k=sum(run['success_within_k'] for run in runs),
                            answered_correctly=sum(run['answered_correctly'] for run in runs),
                            empty_finds_before_success=[run['empty_finds_before_success'] for run in runs],
                            mean_empty_finds_before_success=round(sum(run['empty_finds_before_success'] for run in runs)
                                                                  / len(runs), 2) if runs else None,
                            mean_find_attempts=round(sum(run['find_attempts'] for run in runs) / len(runs), 2) if runs else None,
                            inspected_tool_source=sum(bool(run['inspected_tool_source']) for run in runs),
                            models=sorted({str(run['model']) for run in runs}))
            arms = {arm: summary([session for session in sessions if session['label'].startswith(arm)])
                    for arm in ('control', 'lesson')}
            arms['lesson_flag_only_via_lesson'] = summary([
                session for session in sessions if session['label'].startswith('lesson')
                and by_label[session['label']]['flag_via_lesson_ids'] and not by_label[session['label']]['flag_via_other_ids']])
            report['behavior'] = dict(arms=arms, rule=LESSON_SCORE_RULE, k=LESSON_K,
                                      note=('small-sample signal, not causality; control runs without the Memento '
                                            'extension, so it also lacks capture and any other offered context'))
            report['final_status'] = native_status_summary(service.get('/status'))
    finally:
        report['teardown'] = lesson_teardown(launch, output.with_suffix('.container.log'))
    failed = sorted(name for name, stage in report['stages'].items() if not stage['passed'])
    weakest = failed[0] if failed else None
    report['interpretation'] = dict(failed_stages=failed, weakest_link=weakest)
    report['stage'] = 'completed'
    report['exit_code'] = 3 if failed else 0
    return report['exit_code']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['calibrate', 'learning', 'factual-scope', 'native-retrieval', 'lesson-loop'])
    parser.add_argument('--agent', choices=['pi', 'omp'], default='pi')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--calibrate', action='store_true', help='Reuse the isolated service model for frozen calibration first')
    parser.add_argument('--codex-executable', type=Path,
                        help='Opt in to fresh gpt-6.1/xhigh conduct, two paired controls and explicit MCP lookup using this existing binary')
    parser.add_argument('--injection', action='store_true', help='Opt in to the fresh Pi extension injection check; no native installation')
    parser.add_argument('--instance-manifest', type=Path,
                        help='native-retrieval: private launcher manifest of one owned, isolated native service')
    lesson = parser.add_argument_group('lesson-loop (self-launched disposable native instance)')
    lesson.add_argument('--compose-override', type=Path,
                        help='production compose override whose read-only model-asset mounts are reused (never /data)')
    lesson.add_argument('--image', help='existing image built from this checkout; default builds Dockerfile.native')
    lesson.add_argument('--port', type=int, default=LESSON_DEFAULT_PORT, help='loopback port; 8765 is refused')
    lesson.add_argument('--processor-command', default='pi',
                        help='single native Pi executable for the legacy runner lanes (extraction/consolidation)')
    lesson.add_argument('--agent-command', default='pi', help='single native Pi executable for behavior sessions')
    lesson.add_argument('--trials', type=int, default=3, help='fresh sessions per arm')
    lesson.add_argument('--max-ticks', type=int, default=4, help='bounded runner ticks')
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit('Choose a new output file')
    if args.mode != 'learning' and (args.codex_executable or args.injection):
        parser.error('Native conduct/injection options require learning mode')
    if (args.mode == 'native-retrieval') != (args.instance_manifest is not None):
        parser.error('--instance-manifest is required by, and only valid with, native-retrieval')
    if args.mode == 'native-retrieval' and args.calibrate:
        parser.error('--calibrate does not apply to native-retrieval')
    if (args.mode == 'lesson-loop') != (args.compose_override is not None):
        parser.error('--compose-override is required by, and only valid with, lesson-loop')
    report = {}
    code = 0
    try:
        if args.mode == 'lesson-loop':
            args.processor_command, args.agent_command = [args.processor_command], [args.agent_command]
            code = lesson_loop(args, report)
        elif args.mode == 'native-retrieval':
            code = native_retrieval(args.instance_manifest, report)
        elif args.mode == 'calibrate':
            from memento.embedder import load_model
            from memento.retrieval import MODEL_ID, MODEL_REVISION
            report['calibration'] = calibrate(load_model(MODEL_ID, MODEL_REVISION, text_only=False))
        elif args.mode == 'factual-scope':
            factual_scope(args.agent, report)
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
    if code:
        raise SystemExit(code)


if __name__ == '__main__':
    main()
