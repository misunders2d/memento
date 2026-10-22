"""Deferred recall reuses one request at supported native delivery boundaries."""
import json
from concurrent.futures import ThreadPoolExecutor
import os
import selectors
import shutil
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

from memento import bridge


@pytest.fixture
def client(tmp_path, monkeypatch):
    probe = SimpleNamespace(routes={}, calls=[], active=0, maximum=0, children=[], gates=[])
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            query = parse_qs(urlsplit(self.path).query)['q'][0]
            probe.calls.append(query)
            probe.active += 1
            probe.maximum = max(probe.maximum, probe.active)
            gate, payload = probe.routes.get(query, (None, {'memories': []}))
            try:
                if gate is not None:
                    gate.wait(10)
                raw = bridge.encode(payload)
                self.send_response(200)
                self.send_header('Content-Length', str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
            except OSError:
                pass
            finally:
                probe.active -= 1
        def log_message(self, *_args):
            pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .02}, daemon=True)
    thread.start()
    config = dict(uid=os.getuid(), client_uid='deferred-fixture', preset='pi',
                  url=f'http://127.0.0.1:{server.server_port}')
    path = tmp_path / 'runner.json'
    bridge.private_write(path, bridge.encode(config))
    probe.spool = bridge.Spool(path, config)
    original = bridge.kick_recall
    def launch(*args):
        child = original(*args)
        probe.children.append(child)
        return child
    monkeypatch.setattr(bridge, 'kick_recall', launch)
    try:
        yield probe
    finally:
        for gate in probe.gates:
            gate.set()
        for child in probe.children:
            try:
                child.wait(timeout=2)
            except subprocess.TimeoutExpired:
                child.terminate()
                child.wait(timeout=2)
        probe.spool.close()
        server.shutdown()
        server.server_close()
        thread.join()


def wait_for(check):
    deadline = time.monotonic() + 3
    while not check():
        assert time.monotonic() < deadline
        time.sleep(.01)


def receipts(spool):
    return [json.loads(raw) for raw, in spool.db.execute("SELECT body FROM outbox WHERE endpoint='/episodes'")
            if 'exposure' in json.loads(raw)['metadata']]


def test_existing_cache_migrates_once_under_concurrent_open_without_changing_cached_offer(tmp_path):
    config = dict(uid=os.getuid(), client_uid='upgrade-fixture', preset='pi', url='http://127.0.0.1:1')
    path = tmp_path / 'runner.json'
    bridge.private_write(path, bridge.encode(config))
    with sqlite3.connect(path.with_suffix('.bridge.sqlite3')) as db:
        db.execute('CREATE TABLE recall_cache(conversation TEXT PRIMARY KEY,query TEXT NOT NULL,context TEXT NOT NULL,shown TEXT NOT NULL,updated REAL NOT NULL)')
        db.execute("INSERT INTO recall_cache VALUES('s','original query','cached offer','[]',123)")
    def opened(_index):
        spool = bridge.Spool(path, config)
        try:
            return spool.db.execute('SELECT query,context,shown,updated,state FROM recall_cache').fetchone()
        finally:
            spool.close()
    with ThreadPoolExecutor(max_workers=4) as workers:
        assert list(workers.map(opened, range(4))) == [('original query', 'cached offer', '[]', 123, None)] * 4


def test_same_request_survives_three_seconds_and_offers_once_on_next_human_turn(client, monkeypatch):
    spool = client.spool
    gate = threading.Event()
    client.gates.append(gate)
    behavior = dict(applicability='When using this synthetic service.', conduct='Use its supported installer.',
                    exceptions=['Retain current human choices.'])
    client.routes['original topic'] = gate, {'memories': [dict(id='lesson', version=7,
        category='behavioral_suggestion', behavior=behavior)], 'client_revision': 'b' * 64}
    monkeypatch.setattr(bridge, 'CLIENT_REVISION', 'a' * 64)
    started = time.monotonic()
    pending = bridge.native(spool, dict(harness='pi', session_id='s', action='recall', query='original topic'))
    assert 2.5 <= time.monotonic() - started < 4
    assert pending['pending'] and 'error' not in pending and not pending['context']
    assert not receipts(spool)
    assert not spool.db.execute('SELECT 1 FROM recall_cache WHERE conversation=?', (bridge._UPDATE_NOTICE_KEY,)).fetchone()
    gate.set()
    client.children[0].wait(timeout=2)
    assert not receipts(spool)  # Completing retrieval is not an exposure.
    assert spool.db.execute("SELECT context FROM recall_cache WHERE conversation='s'").fetchone()[0] == ''
    offered = bridge.native(spool, dict(harness='pi', session_id='s', action='recall', query='different topic'))
    assert offered['query'] == 'original topic'
    assert 'Delayed recall for an earlier request: "original topic"' in offered['context']
    assert all(value in offered['context'] for value in (behavior['applicability'], behavior['conduct'], *behavior['exceptions']))
    assert len((offered['update_notice'] + '\n' + offered['context']).encode()) <= bridge.CONTEXT_BYTES
    assert receipts(spool)[0]['metadata']['exposure'] == [['lesson', 7]]
    peer = bridge.automatic(spool, 's', '[HERDR AGENT MESSAGE] peer result', harness='pi')
    assert peer['context'] == offered['context']
    assert len(receipts(spool)) == 1 and 'update_notice' not in peer
    assert bridge.recall_anchor(spool, 's') == 'different topic'
    client.children[-1].wait(timeout=2)
    assert 'ready' not in bridge.recall_state(spool, 's')  # Current healthy-empty result was consumed in its turn.
    assert not offered.get('pending')
    assert client.calls == ['original topic', 'different topic']
    assert len(receipts(spool)) == 1


def test_current_second_and_third_queries_receive_own_memories_after_first_is_deferred(client, monkeypatch):
    monkeypatch.setattr(bridge, 'RECALL_WAIT', .01)
    gate = threading.Event()
    client.gates.append(gate)
    client.routes['first query'] = gate, {'memories': [dict(id='old-lesson', version=1,
        category='behavioral_suggestion', behavior=dict(applicability='For the old query.',
            conduct='Use its old context.', exceptions=['Respect the current request.']))]}
    client.routes['second query'] = None, {'memories': [{'id': 'second-memory', 'text': 'Second topic only.'}]}
    client.routes['third query'] = None, {'memories': [{'id': 'third-memory', 'text': 'Third topic only.'}]}
    assert bridge.automatic(client.spool, 's', 'first query', harness='pi')['pending']
    gate.set()
    client.children[0].wait(timeout=2)
    monkeypatch.setattr(bridge, 'RECALL_WAIT', .5)
    for query, identifier in [('second query', 'second-memory'), ('third query', 'third-memory')]:
        offered = bridge.automatic(client.spool, 's', query, harness='pi')
        assert offered['query'] == query and offered['shown'] == [identifier]
        assert '[memory:old-lesson]' not in offered['context']
        assert len(offered['context'].encode()) <= bridge.CONTEXT_BYTES
    assert client.calls == ['first query', 'second query', 'third query']
    assert not receipts(client.spool)  # The superseded old offer was never emitted.


def test_latest_demand_replaces_intermediate_without_parallel_http_or_stale_anchor(client, monkeypatch):
    monkeypatch.setattr(bridge, 'RECALL_WAIT', .05)
    gate = threading.Event()
    client.gates.append(gate)
    client.routes['A'] = gate, {'memories': [{'id': 'old', 'text': 'Old topic.'}]}
    assert bridge.automatic(client.spool, 's', 'A')['pending']
    wait_for(lambda: client.calls == ['A'])
    assert bridge.automatic(client.spool, 's', 'B')['pending']
    assert bridge.automatic(client.spool, 's', 'C')['pending']
    assert len(client.children) == 1
    gate.set()
    client.children[0].wait(timeout=2)
    state = bridge.recall_state(client.spool, 's')
    assert state['ready']['query'] == state['anchor'] == 'C'
    assert client.calls == ['A', 'C'] and client.maximum == 1
    assert client.spool.db.execute("SELECT query FROM recall_cache WHERE conversation='s'").fetchone()[0] == ''


def test_stale_owner_cannot_publish_after_replacement(client, monkeypatch):
    monkeypatch.setattr(bridge, 'RECALL_WAIT', .05)
    gate = threading.Event()
    client.gates.append(gate)
    client.routes['A'] = gate, {'memories': [{'id': 'old', 'text': 'Old result.'}]}
    bridge.automatic(client.spool, 's', 'A')
    wait_for(lambda: bool(client.calls))
    state = bridge.recall_state(client.spool, 's')
    state.update(owner='replacement', anchor='new human query')
    bridge.save_recall_state(client.spool, 's', state)
    client.spool.commit()
    gate.set()
    client.children[0].wait(timeout=2)
    assert bridge.recall_state(client.spool, 's') == state
    assert 'ready' not in state


def test_dead_worker_and_expired_offer_recover_without_exposure_or_cross_conversation(client, monkeypatch):
    monkeypatch.setattr(bridge, 'RECALL_WAIT', .01)
    monkeypatch.setattr(bridge, 'RECALL_LIFETIME', .03)
    monkeypatch.setattr(bridge, 'kick_recall', lambda *_args: None)
    assert bridge.automatic(client.spool, 's', 'A')['pending']
    owner = bridge.recall_state(client.spool, 's')['owner']
    time.sleep(.04)
    assert bridge.automatic(client.spool, 's', 'B')['error'] == 'recall_unavailable'
    state = bridge.recall_state(client.spool, 's')
    assert state['owner'] != owner and state['anchor'] == 'B'
    state.update(owner=None, ready=dict(query='A', token='expired', fetched=time.time() - 301,
                                       items=[], revision=None))
    bridge.save_recall_state(client.spool, 's', state)
    client.spool.commit()
    assert bridge.automatic(client.spool, 's', 'B')['pending']
    assert 'ready' not in bridge.recall_state(client.spool, 's')
    assert bridge.automatic(client.spool, 'other', '[HERDR AGENT MESSAGE] unrelated') == {'context': '', 'shown': []}
    assert not receipts(client.spool) and client.calls == []


def test_first_deferred_request_retains_fresh_legacy_followup_anchor(client, monkeypatch):
    monkeypatch.setattr(bridge, 'RECALL_WAIT', .01)
    monkeypatch.setattr(bridge, 'kick_recall', lambda *_args: None)
    client.spool.db.execute('INSERT INTO recall_cache(conversation,query,context,shown,updated) VALUES(?,?,?,?,?)',
                            ('s', 'Original human topic.', 'Cached offer.', '[]', time.time()))
    assert bridge.automatic(client.spool, 's', 'And for tomorrow?')['pending']
    state = bridge.recall_state(client.spool, 's')
    assert state['latest']['previous'] == 'Original human topic.'
    assert state['anchor'] == 'And for tomorrow?'
    assert client.calls == []


def test_worker_has_finite_lifetime_and_terminal_failure_differs_from_healthy_empty(client, monkeypatch):
    monkeypatch.setattr(bridge, 'RECALL_WAIT', .05)
    monkeypatch.setattr(bridge, 'RECALL_LIFETIME', .3)
    gate = threading.Event()
    client.gates.append(gate)
    client.routes['stalled'] = gate, {'memories': []}
    empty_gate = threading.Event()
    client.gates.append(empty_gate)
    client.routes['empty'] = empty_gate, {'memories': []}
    assert bridge.automatic(client.spool, 's', 'stalled')['pending']
    client.children[0].wait(timeout=2)
    assert bridge.automatic(client.spool, 's', 'empty')['error'] == 'recall_unavailable'
    empty_gate.set()
    client.children[-1].wait(timeout=2)
    assert bridge.automatic(client.spool, 's', 'empty') == {'context': '', 'shown': [], 'query': 'empty'}
    client.routes['bad'] = None, {'status': 'error'}
    monkeypatch.setattr(bridge, 'RECALL_WAIT', 1)
    failed = bridge.automatic(client.spool, 's', 'bad')
    assert failed['error'] == 'recall_unavailable' and 'pending' not in failed


def test_foreground_process_group_cancellation_leaves_owned_worker_to_finish(client, tmp_path):
    gate = threading.Event()
    client.gates.append(gate)
    client.routes['owned cancellation query'] = gate, {'memories': [{'id': 'kept', 'text': 'Owned result.'}]}
    pidfile = tmp_path / 'worker.pid'
    script = '''from memento import bridge
import pathlib
original=bridge.kick_recall
def launch(*args):
 child=original(*args)
 pathlib.Path(PIDFILE).write_text(str(child.pid))
 return child
bridge.kick_recall=launch
raise SystemExit(bridge.main(['native','--config',CONFIG]))
'''.replace('PIDFILE', repr(str(pidfile))).replace('CONFIG', repr(client.spool.config_path))
    environment = dict(os.environ)
    environment.pop('MEMENTO_PROCESSING', None)
    parent = subprocess.Popen([sys.executable, '-c', script], stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True, env=environment)
    worker = None
    try:
        parent.stdin.write(bridge.encode(dict(harness='pi', session_id='cancel', action='recall', query='owned cancellation query')))
        parent.stdin.close()
        wait_for(lambda: pidfile.exists() and bool(client.calls))
        worker = int(pidfile.read_text())
        assert os.getpgid(worker) != parent.pid
        os.killpg(parent.pid, signal.SIGTERM)
        parent.wait(timeout=2)
        gate.set()
        wait_for(lambda: 'ready' in bridge.recall_state(client.spool, 'cancel'))
        assert bridge.recall_state(client.spool, 'cancel')['ready']['query'] == 'owned cancellation query'
        assert client.calls == ['owned cancellation query'] and not receipts(client.spool)
    finally:
        if parent.poll() is None:
            os.killpg(parent.pid, signal.SIGTERM)
            parent.wait(timeout=2)
        if worker and bridge.recall_state(client.spool, 'cancel').get('owner'):
            os.kill(worker, signal.SIGTERM)


def test_ready_claim_is_turn_bound_reserved_for_foreground_and_consumed_once(client, monkeypatch):
    monkeypatch.setattr(bridge, 'RECALL_WAIT', .01)
    gate = threading.Event()
    client.gates.append(gate)
    client.routes['same query'] = gate, {'memories': [{'id': 'late', 'text': 'Late synthetic memory.'}]}
    pending = bridge.automatic(client.spool, 's', 'same query', turn='first')
    gate.set()
    client.children[0].wait(timeout=2)
    state = bridge.recall_state(client.spool, 's')
    state.update(foreground='sync-owner', foreground_until=time.time() + 5)
    bridge.save_recall_state(client.spool, 's', state)
    client.spool.commit()
    assert bridge.recall_ready(client.spool, 's', 'first', token=pending['recall_token']) == {'pending': True}
    state.pop('foreground')
    state['turn'] = 'second'  # An identical new prompt is still a different turn.
    bridge.save_recall_state(client.spool, 's', state)
    client.spool.commit()
    assert bridge.recall_ready(client.spool, 's', 'first', token=pending['recall_token']) == {'pending': False}
    assert 'ready' in bridge.recall_state(client.spool, 's')
    offered = bridge.recall_ready(client.spool, 's', 'second', token=pending['recall_token'])
    assert offered['shown'] == ['late'] and offered['query'] == 'same query'
    assert bridge.recall_ready(client.spool, 's', 'second', token=pending['recall_token']) == {'pending': False}
    assert client.calls == ['same query']


def test_expired_claim_rolls_back_ready_cache_notice_and_exposure_then_delivers(client, monkeypatch):
    monkeypatch.setattr(bridge, 'RECALL_WAIT', .01)
    monkeypatch.setattr(bridge, 'CLIENT_REVISION', 'a' * 64)
    gate = threading.Event()
    client.gates.append(gate)
    client.routes['slow claim'] = gate, {'memories': [dict(id='claim-lesson', version=4,
        category='behavioral_suggestion', behavior=dict(applicability='For this synthetic request.',
            conduct='Use the supported path.', exceptions=['Retain human consent.']))], 'client_revision': 'b' * 64}
    pending = bridge.automatic(client.spool, 'claim', 'slow claim', turn='owned-turn', harness='pi')
    gate.set()
    client.children[0].wait(timeout=2)
    ready = bridge.recall_state(client.spool, 'claim')
    options = dict(token=pending['recall_token'], harness='pi')
    assert bridge.recall_ready(client.spool, 'claim', 'owned-turn', claim_before=time.time() - 1, **options) == {'pending': True}
    clock = [time.time()]
    monkeypatch.setattr(bridge.time, 'time', lambda: clock[0])
    original = bridge.recall_offer
    def slow_offer(*args, **kwargs):
        offered = original(*args, **kwargs)
        clock[0] += .03  # Deterministically expire after writes, before commit.
        return offered
    monkeypatch.setattr(bridge, 'recall_offer', slow_offer)
    assert bridge.recall_ready(client.spool, 'claim', 'owned-turn', claim_before=time.time() + .01, **options) == {'pending': True}
    assert bridge.recall_state(client.spool, 'claim') == ready
    assert client.spool.db.execute("SELECT query,context,shown,updated FROM recall_cache WHERE conversation='claim'").fetchone() == ('', '', '[]', 0)
    assert not receipts(client.spool) and not client.spool.db.in_transaction
    assert not client.spool.db.execute('SELECT 1 FROM recall_cache WHERE conversation=?', (bridge._UPDATE_NOTICE_KEY,)).fetchone()
    monkeypatch.setattr(bridge, 'recall_offer', original)
    offered = bridge.recall_ready(client.spool, 'claim', 'owned-turn', claim_before=time.time() + .2, **options)
    assert offered['shown'] == ['claim-lesson'] and offered['query'] == 'slow claim'
    assert offered['update_notice'] and len(receipts(client.spool)) == 1
    assert bridge.recall_ready(client.spool, 'claim', 'owned-turn', claim_before=time.time() + .2, **options) == {'pending': False}
    assert client.calls == ['slow claim']


def test_codex_async_can_start_before_sync_and_never_starts_another_request(client, monkeypatch):
    monkeypatch.setattr(bridge, 'RECALL_WAIT', .1)
    monkeypatch.setattr(bridge, 'RECALL_LIFETIME', 2)
    gate = threading.Event()
    client.gates.append(gate)
    client.routes['codex topic'] = gate, {'memories': [{'id': 'codex-late', 'text': 'Synthetic late memory. ' * 200}]}
    data = dict(session_id='codex-session', turn_id='native-turn', hook_event_name='UserPromptSubmit', prompt='codex topic')
    def async_hook():
        spool = bridge.Spool(client.spool.config_path, client.spool.config)
        try:
            return bridge.codex_late(spool, data)
        finally:
            spool.close()
    with ThreadPoolExecutor(max_workers=1) as executor:
        late = executor.submit(async_hook)
        sync = bridge.hook(client.spool, 'codex', data)
        assert 'codex-late' not in sync['hookSpecificOutput']['additionalContext']
        gate.set()
        output = late.result(timeout=3)
    assert '[memory:codex-late]' in output['hookSpecificOutput']['additionalContext']
    assert len((sync['hookSpecificOutput']['additionalContext'] + '\n' +
                output['hookSpecificOutput']['additionalContext']).encode()) <= bridge.CONTEXT_BYTES
    assert client.calls == ['codex topic']
    assert bridge.codex_late(client.spool, data) == {}
    assert bridge.codex_late(client.spool, {**data, 'turn_id': ''}) == {}
    assert bridge.codex_late(client.spool, {key: value for key, value in data.items() if key != 'turn_id'}) == {}


def test_codex_new_turn_fences_async_delivery_even_for_identical_prompts(client, monkeypatch):
    monkeypatch.setattr(bridge, 'RECALL_WAIT', .05)
    gate, attached = threading.Event(), threading.Event()
    client.gates.append(gate)
    client.routes['repeated prompt'] = gate, {'memories': [{'id': 'current-only', 'text': 'Synthetic result.'}]}
    data = dict(session_id='repeated-session', hook_event_name='UserPromptSubmit', prompt='repeated prompt')
    bridge.hook(client.spool, 'codex', dict(data, turn_id='A'))
    original = bridge.recall_ready
    def observed(spool, conversation, turn, **options):
        if turn == 'codex:A':
            attached.set()
        return original(spool, conversation, turn, **options)
    monkeypatch.setattr(bridge, 'recall_ready', observed)
    def old_async():
        spool = bridge.Spool(client.spool.config_path, client.spool.config)
        try:
            return bridge.codex_late(spool, dict(data, turn_id='A'))
        finally:
            spool.close()
    with ThreadPoolExecutor(max_workers=1) as executor:
        old = executor.submit(old_async)
        assert attached.wait(2)
        bridge.hook(client.spool, 'codex', dict(data, turn_id='B'))
        assert old.result(timeout=2) == {}
    gate.set()
    client.children[0].wait(timeout=2)
    assert '[memory:current-only]' in bridge.codex_late(client.spool, dict(data, turn_id='B'))['hookSpecificOutput']['additionalContext']
    assert client.calls == ['repeated prompt']


def test_codex_full_sync_budget_preserves_ready_behavior_for_next_human_turn(client, monkeypatch):
    monkeypatch.setattr(bridge, 'RECALL_WAIT', .01)
    gate = threading.Event()
    client.gates.append(gate)
    client.routes['full budget'] = gate, {'memories': [dict(id='budget-lesson', version=3,
        category='behavioral_suggestion', behavior=dict(applicability='For this synthetic setup.',
            conduct='Use the supported path.', exceptions=['Retain human consent.']))]}
    data = dict(session_id='budget-session', turn_id='budget-turn', prompt='full budget', hook_event_name='UserPromptSubmit')
    provenance = bridge.hook(client.spool, 'codex', dict(data, hook_event_name='SessionStart'))['hookSpecificOutput']['additionalContext']
    available = bridge.CONTEXT_BYTES - len((provenance + '\n').encode())
    client.spool.db.execute('INSERT INTO recall_cache(conversation,query,context,shown,updated) VALUES(?,?,?,?,?)',
                           ('budget-session', 'full budget', 'x' * available, '[]', time.time()))
    sync = bridge.hook(client.spool, 'codex', data)
    assert len(sync['hookSpecificOutput']['additionalContext'].encode()) == bridge.CONTEXT_BYTES
    assert bridge.recall_state(client.spool, 'budget-session')['late_budget'] == 0
    gate.set()
    client.children[0].wait(timeout=2)
    ready = bridge.recall_state(client.spool, 'budget-session')['ready']
    assert bridge.codex_late(client.spool, data) == {}
    assert bridge.recall_state(client.spool, 'budget-session')['ready'] == ready
    assert not receipts(client.spool)
    next_turn = bridge.hook(client.spool, 'codex', dict(data, prompt='next topic', turn_id='next-turn'))
    assert '[memory:budget-lesson]' in next_turn['hookSpecificOutput']['additionalContext']
    assert len(receipts(client.spool)) == 1


def test_codex_foreground_release_between_snapshot_and_claim_never_consumes_zero_budget(client, monkeypatch):
    monkeypatch.setattr(bridge, 'RECALL_WAIT', .01)
    gate = threading.Event()
    client.gates.append(gate)
    client.routes['release race'] = gate, {'memories': [{'id': 'race-memory', 'text': 'Synthetic memory.'}]}
    bridge.automatic(client.spool, 'race', 'release race', turn='codex:race-turn')
    gate.set()
    client.children[0].wait(timeout=2)
    state = bridge.recall_state(client.spool, 'race')
    state.update(foreground='reserved', foreground_until=time.time() + 5, late_budget=0)
    bridge.save_recall_state(client.spool, 'race', state)
    client.spool.commit()
    original_state, original_ready = bridge.recall_state, bridge.recall_ready
    budgets = []
    released = False
    def snapshot(spool, conversation):
        nonlocal released
        sampled = original_state(spool, conversation)
        if not released:
            released = True
            fresh = dict(sampled, late_budget=bridge.CONTEXT_BYTES)
            fresh.pop('foreground')
            bridge.save_recall_state(spool, conversation, fresh)
            spool.commit()
        return sampled  # Foreground releases after the reader took its snapshot.
    def claimed(*args, **options):
        budgets.append(options['budget'])
        return original_ready(*args, **options)
    monkeypatch.setattr(bridge, 'recall_state', snapshot)
    monkeypatch.setattr(bridge, 'recall_ready', claimed)
    output = bridge.codex_late(client.spool, dict(session_id='race', turn_id='race-turn', hook_event_name='UserPromptSubmit'))
    assert '[memory:race-memory]' in output['hookSpecificOutput']['additionalContext']
    assert budgets == [bridge.CONTEXT_BYTES] and client.calls == ['release race']


@pytest.mark.parametrize('harness', ['pi', 'omp'])
def test_real_extension_context_claims_ready_same_turn_without_http_or_next_turn_duplicate(client, tmp_path, harness):
    node = shutil.which('node')
    if not node or int(subprocess.check_output([node, '--version'], text=True).lstrip('v').split('.')[0]) < 23:
        pytest.skip('Requires Node >=23 TypeScript stripping')
    gate = threading.Event()
    client.gates.append(gate)
    client.routes['native topic'] = gate, {'memories': [{'id': 'boundary-memory', 'text': 'Synthetic same-turn fact.'}]}
    source = Path(bridge.__file__).with_name('native_shared.ts')
    observer = tmp_path / 'boundary.mjs'
    observer.write_text('import {installNative} from ' + json.dumps(source.as_uri()) + ';\n' + '''
import {createInterface} from 'node:readline';
const input=createInterface({input:process.stdin})[Symbol.asyncIterator]();
const handlers=new Map();
const api={on:(name,fn)=>handlers.set(name,fn),registerFlag:()=>{},getFlag:()=>process.env.MEMENTO_CONFIG,registerTool:()=>{}};
const Type=new Proxy({},{get:()=>()=>({})});
installNative(api,Type,process.env.TEST_HARNESS);
const ctx={cwd:process.cwd(),sessionManager:{getSessionId:()=>"native-boundary",getSessionFile:()=>undefined,getEntries:()=>[]}};
await handlers.get("session_start")({},ctx);
await handlers.get("before_agent_start")({prompt:"native topic",messages:[]},ctx);
await handlers.get("session_compact")({},ctx);
console.log(JSON.stringify({pending:!(await handlers.get("context")({messages:[]},ctx))}));
await input.next();
const first=await handlers.get("context")({messages:[]},ctx);
console.log(JSON.stringify({first:first??null}));
await input.next();
const late=first??await handlers.get("context")({messages:[]},ctx);
const replay=await handlers.get("context")({messages:[]},ctx);
console.log(JSON.stringify({late:late??null,replay:replay??null}));
await input.next();
await handlers.get("before_agent_start")({prompt:"new topic",messages:[]},ctx);
console.log(JSON.stringify({next:await handlers.get("context")({messages:[]},ctx)??null}));
await handlers.get("session_shutdown")({},ctx);
process.stdin.destroy();
''')
    environment = {**os.environ, 'MEMENTO_CONFIG': client.spool.config_path,
        'MEMENTO_BRIDGE': str(Path(bridge.__file__).resolve()), 'MEMENTO_PYTHON': sys.executable, 'TEST_HARNESS': harness}
    environment.pop('MEMENTO_PROCESSING', None)
    process = subprocess.Popen([node, str(observer)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True, cwd=tmp_path, env=environment)
    def line():
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            assert selector.select(timeout=6), 'Native callback exceeded its test bound'
        return json.loads(process.stdout.readline())
    try:
        assert line() == {'pending': True}
        gate.set()
        wait_for(lambda: 'ready' in bridge.recall_state(client.spool, 'native-boundary'))
        process.stdin.write('continue\n')
        process.stdin.flush()
        first = line()['first']
        if first is None:
            assert 'ready' in bridge.recall_state(client.spool, 'native-boundary'), 'Undelivered claim consumed its ready result'
        process.stdin.write('next model request\n')
        process.stdin.flush()
        offered = line()
        assert offered['late'] is not None
        assert offered['late'] == offered['replay']
        context = offered['late']['messages'][-1]['content'][0]['text']
        assert '[memory:boundary-memory]' in context and len(context.encode()) <= bridge.CONTEXT_BYTES
        assert 'ready' not in bridge.recall_state(client.spool, 'native-boundary')
        assert client.calls == ['native topic']  # Both context callbacks are local only.
        process.stdin.write('next human\n')
        process.stdin.flush()
        assert line() == {'next': None}
        process.communicate(timeout=3)
        assert process.returncode == 0
        assert client.calls == ['native topic', 'new topic']
    finally:
        if process.poll() is None:
            process.terminate()
            process.communicate(timeout=3)
