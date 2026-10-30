"""Autonomous recall shares retrieval ownership, not the human offer budget."""
import threading
import time
import pytest

from memento import bridge
from test_deferred_recall import client, receipts, wait_for


def begin(client, monkeypatch):
    monkeypatch.setattr(bridge, 'RECALL_WAIT', .5)
    client.routes['human task'] = None, {'memories': [dict(id='human', version=1, text='Original task context.')]}
    human = bridge.automatic(client.spool, 's', 'human task', harness='pi')
    client.children[-1].wait(timeout=2)
    if human.get('pending'):
        human = bridge.recall_ready(client.spool, 's', bridge.recall_state(client.spool, 's')['turn'], harness='pi')
    return bridge.recall_state(client.spool, 's')['turn'], human


def route(client, tool='Bash', command='cargo test | tail -40', identifier='lesson', version=1):
    arguments = {'command': command}
    query = bridge.step_query(tool, arguments)
    client.routes[query] = None, {'memories': [dict(id=identifier, version=version,
        category='behavioral_suggestion', behavior=dict(applicability='When reporting piped test results.',
            conduct='Preserve the test process exit status with pipefail.',
            exceptions=['Do not rerun destructive commands.']))]}
    return arguments, query


def test_action_query_never_copies_argument_values_inline_scripts_or_raw_errors():
    query = bridge.step_query('Bash', {'command':
        "API_TOKEN=supersecret cargo test -p private-project | tail -40; curl --header 'Authorization: Bearer hidden' https://user:password@host/?token=hidden"}, True)
    assert all(word in query for word in ('cargo', 'test', '|', 'tail', 'curl', 'failed'))
    assert all(word not in query for word in ('supersecret', 'private-project', 'hidden', 'password', 'Authorization'))
    assert not bridge.step_query('mcp__memento__recall', {'query': 'anything'})
    assert not bridge.step_query('memento_work', {})
    assert not bridge.step_query('recall', {})
    assert 'sensitive-code' not in bridge.step_query('Bash', {'command': "python -c 'sensitive-code'"})


def test_reminder_has_separate_budget_and_does_not_replace_human_cache(client, monkeypatch):
    turn, human = begin(client, monkeypatch)
    arguments, query = route(client)
    client.routes[query][1]['memories'].insert(0, dict(id='human', version=1, text='Original task context.'))
    started = bridge.step_recall(client.spool, 's', turn, 'Bash', arguments)
    client.children[-1].wait(timeout=2)
    assert not receipts(client.spool)
    result = bridge.step_ready(client.spool, 's', turn, token=started['step_token'], harness='pi')
    assert result['shown'] == ['lesson'] and 'pipefail' in result['context']
    assert 'mid-run' in result['context'] and 'Original task context.' not in result['context']
    assert len(result['context'].encode()) <= bridge.STEP_CONTEXT_BYTES
    assert bridge.recall_cached(client.spool, 's', 'human task', bridge.CONTEXT_BYTES)['context'] == human['context']
    assert bridge.recall_anchor(client.spool, 's') == 'human task'
    assert receipts(client.spool)[0]['conversation_id'] == 's'
    assert receipts(client.spool)[0]['metadata']['exposure'] == [['lesson', 1]]
    assert bridge.step_ready(client.spool, 's', turn, harness='pi') == {'pending': False}


def test_human_request_keeps_priority_and_its_own_result_with_one_http_flight(client, monkeypatch):
    monkeypatch.setattr(bridge, 'RECALL_WAIT', .01)
    gate = threading.Event(); client.gates.append(gate)
    client.routes['human task'] = gate, {'memories': [dict(id='human', version=1, text='Human result.')]}
    assert bridge.automatic(client.spool, 's', 'human task', harness='pi')['pending']
    wait_for(lambda: client.calls == ['human task'])
    turn = bridge.recall_state(client.spool, 's')['turn']
    arguments, query = route(client)
    bridge.step_recall(client.spool, 's', turn, 'Bash', arguments)
    assert len(client.children) == 1 and client.calls == ['human task']
    gate.set(); client.children[0].wait(timeout=2)
    assert client.calls == ['human task', query] and client.maximum == 1
    assert bridge.recall_ready(client.spool, 's', turn, harness='pi')['shown'] == ['human']
    assert bridge.step_ready(client.spool, 's', turn, harness='pi')['shown'] == ['lesson']


def test_new_human_turn_fences_an_inflight_step_without_parallel_retrieval(client, monkeypatch):
    turn, _ = begin(client, monkeypatch)
    arguments, query = route(client)
    gate = threading.Event(); client.gates.append(gate)
    client.routes[query] = gate, client.routes[query][1]
    bridge.step_recall(client.spool, 's', turn, 'Bash', arguments)
    wait_for(lambda: query in client.calls)
    client.routes['new task'] = None, {'memories': [dict(id='new', text='New task only.')]}
    monkeypatch.setattr(bridge, 'RECALL_WAIT', .01)
    assert bridge.automatic(client.spool, 's', 'new task', harness='pi')['pending']
    assert len(client.children) == 2
    gate.set(); client.children[-1].wait(timeout=2)
    state = bridge.recall_state(client.spool, 's')
    assert state['ready']['query'] == 'new task' and 'step' not in state
    assert client.maximum == 1 and client.calls[-1] == 'new task'
    assert bridge.step_ready(client.spool, 's', turn, harness='pi') == {'pending': False}
    assert not receipts(client.spool)


def test_stop_discards_late_reminder_and_prevents_late_tool_callbacks(client, monkeypatch):
    turn, _ = begin(client, monkeypatch)
    arguments, query = route(client)
    gate = threading.Event(); client.gates.append(gate)
    client.routes[query] = gate, client.routes[query][1]
    bridge.step_recall(client.spool, 's', turn, 'Bash', arguments)
    wait_for(lambda: query in client.calls)
    bridge.end_step_recall(client.spool, 's', turn)
    gate.set(); client.children[-1].wait(timeout=2)
    assert bridge.step_ready(client.spool, 's', turn) == {'pending': False}
    assert bridge.step_recall(client.spool, 's', turn, 'Bash', {'command': 'git push'}) == {'pending': False}
    assert not receipts(client.spool)


def test_expired_claim_rolls_back_receipt_seen_ids_and_ready_result(client, monkeypatch):
    turn, _ = begin(client, monkeypatch)
    arguments, _ = route(client)
    bridge.step_recall(client.spool, 's', turn, 'Bash', arguments)
    client.children[-1].wait(timeout=2)
    for deadline in (time.time() - 1, float('nan')):
        assert bridge.step_ready(client.spool, 's', turn, harness='pi', claim_before=deadline) == {'pending': True}
        assert 'ready' in bridge.recall_state(client.spool, 's')['step']
        assert not receipts(client.spool)
    original = bridge.recall_offer
    def slow(*args, **kwargs):
        result = original(*args, **kwargs)
        time.sleep(.03)
        return result
    monkeypatch.setattr(bridge, 'recall_offer', slow)
    assert bridge.step_ready(client.spool, 's', turn, harness='pi', claim_before=time.time()+.01) == {'pending': True}
    assert not receipts(client.spool)
    state = bridge.recall_state(client.spool, 's')
    assert 'ready' in state['step'] and 'lesson' not in state['seen']
    monkeypatch.setattr(bridge, 'recall_offer', original)
    assert bridge.step_ready(client.spool, 's', turn, harness='pi')['shown'] == ['lesson']


def test_same_action_is_not_refetched_and_new_versions_can_be_offered(client, monkeypatch):
    turn, _ = begin(client, monkeypatch)
    monkeypatch.setattr(bridge, 'STEP_INTERVAL', 0)
    arguments, query = route(client)
    bridge.step_recall(client.spool, 's', turn, 'Bash', arguments)
    client.children[-1].wait(timeout=2)
    assert bridge.step_ready(client.spool, 's', turn, harness='pi')['shown'] == ['lesson']
    children = len(client.children)
    bridge.step_recall(client.spool, 's', turn, 'Bash', arguments)
    assert len(client.children) == children
    route(client, command='cargo test --locked | tail -40', version=2)
    bridge.step_recall(client.spool, 's', turn, 'Bash', {'command': 'cargo test --locked | tail -40'})
    client.children[-1].wait(timeout=2)
    assert bridge.step_ready(client.spool, 's', turn, harness='pi')['shown'] == ['lesson']
    assert [r['metadata']['exposure'] for r in receipts(client.spool)] == [[['lesson', 1]], [['lesson', 2]]]


def test_expired_step_warns_once_and_healthy_empty_clears_warning(client, monkeypatch):
    turn, _ = begin(client, monkeypatch)
    arguments, _ = route(client)
    monkeypatch.setattr(bridge, 'kick_recall', lambda *_: None)
    bridge.step_recall(client.spool, 's', turn, 'Bash', arguments)
    state = bridge.recall_state(client.spool, 's')
    state['deadline'] = time.time() - 1
    bridge.save_recall_state(client.spool, 's', state); client.spool.commit()
    failed = bridge.step_ready(client.spool, 's', turn, harness='pi')
    assert failed['warning'] and failed['error'] == 'recall_unavailable'
    assert bridge.step_ready(client.spool, 's', turn, harness='pi') == {'pending': False}
    state = bridge.recall_state(client.spool, 's')
    state['step']['ready'] = dict(items=[], query='empty', fetched=time.time())
    bridge.save_recall_state(client.spool, 's', state); client.spool.commit()
    assert not bridge.step_ready(client.spool, 's', turn, harness='pi')['context']
    assert not bridge.recall_state(client.spool, 's')['warned']


@pytest.mark.parametrize('harness,key,delivery', [('claude','prompt_id','PostToolBatch'), ('codex','turn_id','PostToolUse')])
def test_native_tool_hooks_offer_once_under_exact_human_identity_without_execution_decisions(client, monkeypatch, harness, key, delivery):
    monkeypatch.setattr(bridge, 'RECALL_WAIT', .5)
    client.routes['human task'] = None, {'memories': []}
    data = dict(session_id='s', **{key:'native-id'})
    bridge.hook(client.spool, harness, dict(data, hook_event_name='UserPromptSubmit', prompt='human task'))
    client.children[-1].wait(timeout=2)
    arguments, _ = route(client)
    pre = dict(data, hook_event_name='PreToolUse', tool_name='Bash', tool_input=arguments)
    assert bridge.hook(client.spool, harness, pre) == {}
    client.children[-1].wait(timeout=2)
    assert bridge.hook(client.spool, harness, dict(data, **{key:'older'}, hook_event_name=delivery)) == {}
    result = bridge.hook(client.spool, harness, dict(data, hook_event_name=delivery))
    assert list(result) == ['hookSpecificOutput']
    assert result['hookSpecificOutput']['hookEventName'] == delivery
    assert 'pipefail' in result['hookSpecificOutput']['additionalContext']
    assert len(result['hookSpecificOutput']['additionalContext'].encode()) <= bridge.STEP_CONTEXT_BYTES
    assert bridge.hook(client.spool, harness, dict(data, hook_event_name=delivery)) == {}
    assert bridge.hook(client.spool, harness, dict(pre, agent_id='other-agent')) == {}
    assert bridge.hook(client.spool, harness, {k:v for k,v in pre.items() if k != key}) == {}
    bridge.hook(client.spool, harness, dict(data, hook_event_name='Stop'))
    assert bridge.hook(client.spool, harness, dict(pre, tool_input={'command':'git push'})) == {}
    assert len(client.calls) == 2


def test_equivalent_projected_actions_do_not_repeat_search_after_cooldown(client, monkeypatch):
    turn, _ = begin(client, monkeypatch)
    monkeypatch.setattr(bridge, 'STEP_INTERVAL', 0)
    arguments, _ = route(client, command='echo 1; sleep 0.5')
    bridge.step_recall(client.spool, 's', turn, 'Bash', arguments)
    client.children[-1].wait(timeout=2)
    bridge.step_ready(client.spool, 's', turn, harness='pi')
    count = len(client.children)
    bridge.step_recall(client.spool, 's', turn, 'Bash', {'command':'echo 2; sleep 0.5'})
    assert len(client.children) == count


def test_cached_foreground_offer_is_not_repeated_by_the_step_lookup(client, monkeypatch):
    begin(client, monkeypatch)
    gate = threading.Event(); client.gates.append(gate)
    client.routes['human task'] = gate, {'memories': []}
    monkeypatch.setattr(bridge, 'RECALL_WAIT', .01)
    offered = bridge.automatic(client.spool, 's', 'human task', harness='pi')
    assert offered['shown'] == ['human'] and offered['pending']
    turn = bridge.recall_state(client.spool, 's')['turn']
    arguments, query = route(client)
    client.routes[query][1]['memories'].insert(0, dict(id='human', version=1, text='Original task context.'))
    bridge.step_recall(client.spool, 's', turn, 'Bash', arguments)
    gate.set(); client.children[-1].wait(timeout=2)
    assert bridge.step_ready(client.spool, 's', turn, harness='pi')['shown'] == ['lesson']


def test_persisted_claude_hook_attachments_and_request_replays_stay_raw_only(client):
    text = 'Memento mid-run reminder. [memory:lesson] Synthetic advice.'
    for index, record in enumerate([
        dict(type='attachment', attachment={'type':'hook_additional_context','content':text}),
        dict(type='attachment', rendered=text, renderedRole='user', attachment={'type':'hook_additional_context'}),
        dict(type='api-request-blob', message={'role':'user','content':text}),
        dict(type='user', message={'role':'user','content':text}),
    ]):
        client.spool.record('claude','s',record,identity=str(index))
    episodes=[__import__('json').loads(raw) for raw, in client.spool.db.execute("SELECT body FROM outbox WHERE endpoint='/episodes'")]
    eligible=[e for e in episodes if e['metadata']['extraction_mode']=='message']
    assert len(eligible) == 1 and eligible[0]['metadata']['native_id'] == '3'
    assert len([e for e in episodes if e['kind']=='native_record']) == 4


@pytest.mark.parametrize('harness', ['pi', 'omp'])
def test_native_reminder_is_request_local_and_survives_compaction_then_stops(tmp_path, harness):
    import json
    import os
    from pathlib import Path
    import shutil
    import subprocess
    import sys
    node = shutil.which('node')
    if not node or int(subprocess.check_output([node, '--version'], text=True).lstrip('v').split('.')[0]) < 23:
        pytest.skip('Requires Node >=23 TypeScript stripping')
    fake = tmp_path / 'bridge.py'
    fake.write_text('import json,sys\nrequest=json.load(sys.stdin)\n'
        + 'result={}\nif request["action"]=="recall": result={"context":"Human context.","shown":[],"query":"human task"}\n'
        + 'if request["action"]=="step-recall": result={"scheduled":True,"pending":True,"step_token":"step"}\n'
        + 'if request["action"]=="step-ready": result={"context":"[memory:lesson] Mid-run advice.","shown":["lesson"],"pending":False}\n'
        + 'print(json.dumps(result))\n')
    observer = tmp_path / 'observer.mjs'
    observer.write_text('import {installNative} from ' + json.dumps(Path(bridge.__file__).with_name('native_shared.ts').as_uri()) + ';\n' + r'''
const handlers=new Map();
const api={on:(name,fn)=>handlers.set(name,fn),registerFlag:()=>{},getFlag:()=>process.env.MEMENTO_CONFIG,registerTool:()=>{}};
const Type=new Proxy({},{get:()=>()=>({})});
installNative(api,Type,process.env.TEST_HARNESS);
const ctx={cwd:process.cwd(),sessionManager:{getSessionId:()=>"s",getSessionFile:()=>undefined,getEntries:()=>[]}};
await handlers.get("session_start")({},ctx);
await handlers.get("before_agent_start")({prompt:"human task",messages:[]},ctx);
const original=await handlers.get("context")({messages:[]},ctx);
await handlers.get("tool_call")({toolName:"bash",input:{command:"cargo test"}},ctx);
const offered=await handlers.get("context")({messages:[]},ctx);
if(!JSON.stringify(offered).includes("Mid-run advice.")) throw new Error("No autonomous reminder");
if(!JSON.stringify(offered).includes("Human context.")) throw new Error("Human allowance replaced");
await handlers.get("session_compact")({},ctx);
if(JSON.stringify(await handlers.get("context")({messages:[]},ctx))!==JSON.stringify(offered)) throw new Error("Compaction lost reminder");
await handlers.get("agent_end")({},ctx);
if(JSON.stringify(await handlers.get("context")({messages:[]},ctx))!==JSON.stringify(original)) throw new Error("Stop retained reminder");
await handlers.get("session_shutdown")({},ctx);
console.log(JSON.stringify({journalEntries:ctx.sessionManager.getEntries().length}));
''')
    environment = {**os.environ, 'MEMENTO_CONFIG': str(tmp_path / 'runner.json'),
        'MEMENTO_BRIDGE': str(fake), 'MEMENTO_PYTHON': sys.executable, 'TEST_HARNESS': harness}
    environment.pop('MEMENTO_PROCESSING', None)
    result = subprocess.run([node, str(observer)], env=environment, cwd=tmp_path, text=True,
                            capture_output=True, timeout=8)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {'journalEntries': 0}
