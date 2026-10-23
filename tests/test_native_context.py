"""User-visible conditional guidance must survive the shared injection budget."""

import pytest

from memento import bridge


@pytest.fixture
def spool(tmp_path):
    config = {'uid': 1000, 'client_uid': 'synthetic-device', 'url': 'http://127.0.0.1:1', 'preset': 'codex'}
    path = tmp_path / 'runner.json'
    path.write_bytes(bridge.encode(config))
    result = bridge.Spool(path, config)
    try:
        yield result
    finally:
        result.close()


def suggestion(**changes):
    behavior = {
        'applicability': 'When installing a private service using an available supported installer.',
        'conduct': 'Use the shipped installer instead of asking the human to edit configuration.',
        'exceptions': ['Keep native trust and Google consent explicit.',
                       'Follow a current user request to edit configuration manually.'],
        'rationale': 'Repeated manual editing caused avoidable installation failures.',
        'authority': 'inferred',
        **changes,
    }
    return {'id': 'behavior-id', 'text': 'Always automate every installation step.',
            'category': 'behavioral_suggestion', 'tier': 'confirmed', 'authority': behavior['authority'],
            'behavior': behavior}


def test_full_conditions_and_exceptions_survive_summary_and_hook_provenance(spool, monkeypatch):
    memory = suggestion(rationale='Long supporting rationale. ' * 200)
    monkeypatch.setattr(bridge, 'http', lambda *_args, **_kwargs: {'memories': [memory]})
    direct = bridge.passive(spool, 's', 'Connect another native harness.')
    hooked = bridge.hook(spool, 'codex', {'session_id': 's', 'hook_event_name': 'UserPromptSubmit',
                                        'prompt': 'Connect another native harness.'}, recall=bridge.passive)
    context = hooked['hookSpecificOutput']['additionalContext']
    for rendered in (direct['context'], context):
        assert memory['behavior']['applicability'] in rendered
        assert memory['behavior']['conduct'] in rendered
        assert all(exception in rendered for exception in memory['behavior']['exceptions'])
        assert memory['text'] not in rendered
        assert 'Long supporting rationale.' not in rendered
        assert '[inferred]' in rendered and 'current instructions take priority' in rendered
        assert len(rendered.encode('utf-8')) <= bridge.CONTEXT_BYTES
    assert direct['shown'] == ['behavior-id']


def test_entity_anchor_rendering_keeps_relation_ids_and_candidate_tier_within_budget(spool, monkeypatch):
    edge, relation, hint = 'e' * 32, 'c_' + 'r' * 32, 'n' * 32
    durable = {'id': 'person-id', 'category': 'entity_context', 'tier': 'durable', 'match': 'entity_name',
               'text': 'Margarita [person; professional] Connections: works_in Mellanni [organization; edge ' + edge
                       + ']; Margarita keeps the Drive evidence folder [knowledge; edge ' + edge[::-1] + '] mentions this.'}
    pending = {'id': 'c_pending-id', 'category': 'entity_context', 'tier': 'candidate', 'match': 'entity_name',
               'text': 'One of 2 separate entries spelled alike; not merged. Margarita [person; professional] '
                       'Connections: works_in Acme Linen [organization; pending ' + relation + ']; possibly the same as '
                       'durable Margarita Ivanova [person; memory ' + hint + '] (unverified name match).'}
    fact = {'id': 'fact-id', 'text': 'Margarita asked for direct outreach about the bed-sheet line.', 'category': 'memory'}
    monkeypatch.setattr(bridge, 'http', lambda *_args, **_kwargs: {'memories': [durable, pending, fact]})
    result = bridge.passive(spool, 's', "let's do Margarita - what comes up?")
    context = result['context']
    assert result['shown'] == ['person-id', 'c_pending-id', 'fact-id']
    assert len(context.encode('utf-8')) <= bridge.CONTEXT_BYTES
    lines = context.splitlines()
    assert lines[1].startswith('[memory:person-id] [fact] Margarita [person; professional] Connections: ')
    assert lines[2].startswith('[memory:c_pending-id] [fact] [unconfirmed candidate] One of 2 separate entries spelled alike; not merged. ')
    # The existing IDs that history() resolves survive unchanged client rendering.
    for reference in (edge, edge[::-1], relation, hint):
        assert reference in context
    assert 'pending ' + relation in lines[2] and 'edge ' + edge in lines[1]
    assert lines[3].startswith('[memory:fact-id] [fact] Margarita asked for direct outreach')


def test_user_authority_candidate_and_rationale_remain_distinct(spool, monkeypatch):
    memory = suggestion(authority='explicit_user')
    memory['tier'] = 'candidate'
    monkeypatch.setattr(bridge, 'http', lambda *_args, **_kwargs: {'memories': [memory]})
    result = bridge.passive(spool, 's', 'Install the service.')
    assert '[explicit user guidance]' in result['context']
    assert '[unconfirmed candidate]' in result['context']
    assert memory['behavior']['rationale'] in result['context']
    assert all(exception in result['context'] for exception in memory['behavior']['exceptions'])


def test_a_suggestion_that_cannot_fit_all_exceptions_is_omitted_without_losing_facts(spool, monkeypatch):
    memory = suggestion(exceptions=['Do not automate human approval. ' + '猫' * 500])
    fact = {'id': 'fact-id', 'text': 'The service uses the existing native login.', 'category': 'memory'}
    monkeypatch.setattr(bridge, 'http', lambda *_args, **_kwargs: {'memories': [memory, fact]})
    result = bridge.passive(spool, 's', 'Install the service.')
    assert result['shown'] == ['fact-id']
    assert '[memory:behavior-id]' not in result['context']
    assert memory['behavior']['conduct'] not in result['context']
    assert fact['text'] in result['context']
    assert len(result['context'].encode('utf-8')) <= bridge.CONTEXT_BYTES


@pytest.mark.parametrize('harness', ['pi', 'omp', 'claude', 'codex'])
@pytest.mark.parametrize('empty', [False, True])
def test_client_notice_reaches_request_context_without_memory_or_journal_persistence(spool, monkeypatch, harness, empty):
    spool.config['preset'] = harness
    monkeypatch.setattr(bridge, 'CLIENT_REVISION', 'a' * 64)
    memories = [] if empty else [{'id': 'fact-id', 'text': 'Useful synthetic evidence. ' + '猫' * 2000}]
    monkeypatch.setattr(bridge, 'http', lambda *_args, **_kwargs: {'memories': memories, 'client_revision': 'b' * 64})
    if harness in ('pi', 'omp'):
        result = bridge.native(spool, dict(harness=harness, session_id='s', action='recall', query='Ordinary request.'), recall=bridge.passive)
        notice = result['update_notice']
        context = '\n'.join(part for part in (notice, result['context']) if part)
    else:
        result = bridge.hook(spool, harness, dict(session_id='s', hook_event_name='UserPromptSubmit', prompt='Ordinary request.'), recall=bridge.passive)
        context = result['hookSpecificOutput']['additionalContext']
    assert 'Memento client notice (not memory evidence)' in context
    assert 'memento agent update ' + harness + ' --config ' in context
    assert spool.config_path in context and '--native-config-dir' in context
    assert len(context.encode('utf-8')) <= bridge.CONTEXT_BYTES
    cached = spool.db.execute('SELECT context,shown FROM recall_cache WHERE conversation=?', ('s',)).fetchone()
    assert 'Memento client notice' not in cached[0]
    assert ('fact-id' in cached[1]) is not empty
    assert all('Memento client notice' not in bytes(raw).decode('utf-8')
               for (raw,) in spool.db.execute('SELECT body FROM outbox'))


@pytest.mark.parametrize('harness', ['claude', 'codex'])
def test_notice_budget_preserves_complete_behavior_conditions_or_omits_suggestion(spool, monkeypatch, harness):
    spool.config['preset'] = harness
    monkeypatch.setattr(bridge, 'CLIENT_REVISION', 'a' * 64)
    memory = suggestion(rationale='Long optional rationale. ' * 200)
    monkeypatch.setattr(bridge, 'http', lambda *_args, **_kwargs: {'memories': [memory], 'client_revision': 'b' * 64})
    result = bridge.hook(spool, harness, dict(session_id='s', hook_event_name='UserPromptSubmit', prompt='Ordinary request.'), recall=bridge.passive)
    context = result['hookSpecificOutput']['additionalContext']
    assert 'Memento client notice' in context
    if '[memory:behavior-id]' in context:
        assert memory['behavior']['applicability'] in context
        assert memory['behavior']['conduct'] in context
        assert all(exception in context for exception in memory['behavior']['exceptions'])
    else:
        assert memory['behavior']['conduct'] not in context
    assert len(context.encode('utf-8')) <= bridge.CONTEXT_BYTES


@pytest.mark.parametrize('harness', ['pi', 'omp'])
def test_native_shared_notice_reaches_headless_context_and_warning_surface(tmp_path, harness):
    import json
    import os
    from pathlib import Path
    import shutil
    import subprocess
    import sys
    node = shutil.which('node')
    if node is None:
        pytest.skip('Native shared callback check requires Node with TypeScript stripping')
    version = subprocess.run([node, '--version'], capture_output=True, text=True, timeout=5)
    if int(version.stdout.lstrip('v').split('.')[0]) < 23:
        pytest.skip('Native shared callback check requires Node >=23')
    notice = 'Memento client notice (not memory evidence): different bundle; update only when asked.'
    memory = '[memory:fact] Synthetic delayed fact.'
    captures = tmp_path / 'capture.requests'
    fake_bridge = tmp_path / 'fake_bridge.py'
    fake_bridge.write_text('import json,sys,time\nrequest=json.load(sys.stdin)\n'
        + 'result={}\nif request.get("action")=="recall":\n'
        + ' query=request["query"]\n result={"context":"","shown":[]}\n'
        + ' if query.startswith("Offline"): result["error"]="recall_unavailable"\n'
        + ' elif query.startswith("Pending"): result["pending"]=True\n'
        + ' elif query.startswith("Ordinary"):\n  time.sleep(1.7)\n  result.update('
        + repr(dict(context=memory, shown=['fact'], query='Originating request.', update_notice=notice)) + ')\n'
        + 'if request.get("passive"):\n with open(' + repr(str(captures)) + ',"a") as output: output.write(json.dumps(request["passive"])+"\\n")\n'
        + 'print(json.dumps(result))\n')
    observer = tmp_path / 'observer.mjs'
    source = Path(bridge.__file__).with_name('native_shared.ts')
    observer.write_text('import {installNative} from ' + json.dumps(source.as_uri()) + ';\n' + '''
const handlers = new Map();
const api = {on: (name, callback) => handlers.set(name, callback), registerFlag: () => {},
  getFlag: () => process.env.MEMENTO_CONFIG, registerTool: () => {}};
const Type = new Proxy({}, {get: () => () => ({})});
installNative(api, Type, process.env.TEST_HARNESS);
const entries = [];
const ctx = {cwd: process.cwd(), sessionManager: {
  getSessionId: () => "synthetic-session", getSessionFile: () => undefined, getEntries: () => entries}};
await handlers.get("session_start")({}, ctx);
await handlers.get("before_agent_start")({prompt:"Ordinary synthetic request.", messages:[]}, ctx);
const result = await handlers.get("context")({messages:[]}, ctx);
await handlers.get("session_compact")({messages:[]}, ctx);
const successor = await handlers.get("context")({messages:[]}, ctx);
if (JSON.stringify(successor) !== JSON.stringify(result)) throw new Error("Compaction dropped request-local memory context");
await new Promise(resolve => setTimeout(resolve, 0));
entries.push({id:"after-offer", message:{role:"assistant",content:"Synthetic response."}});
handlers.get("turn_end")({messages:[]}, ctx);
for (const prompt of ["Offline once", "Pending request", "Offline twice", "Healthy empty", "Offline again"]) {
  await handlers.get("before_agent_start")({prompt, messages:[]}, ctx);
  if (await handlers.get("context")({messages:[]}, ctx)) throw new Error("Unexpected context after empty recall");
}
await handlers.get("session_shutdown")({}, ctx);
console.log(JSON.stringify(result));
''')
    environment = {**os.environ, 'MEMENTO_CONFIG': str(tmp_path / 'runner.json'),
        'MEMENTO_BRIDGE': str(fake_bridge), 'MEMENTO_PYTHON': sys.executable, 'TEST_HARNESS': harness}
    environment.pop('MEMENTO_PROCESSING', None)
    result = subprocess.run([node, str(observer)], capture_output=True, text=True,
                            env=environment, cwd=tmp_path, timeout=10)
    assert result.returncode == 0, result.stderr
    injected = json.loads(result.stdout)['messages'][-1]
    assert injected['content'][0]['text'] == notice + '\n' + memory
    assert len(injected['content'][0]['text'].encode('utf-8')) <= bridge.CONTEXT_BYTES
    assert result.stderr.count(notice) == 1
    assert result.stderr.count("Memento passive recall is unavailable") == 2
    assert json.loads(captures.read_text().splitlines()[0]) == {'query': 'Originating request.', 'shown': ['fact']}
    assert not list(tmp_path.glob('*.jsonl'))


@pytest.mark.parametrize('harness', ['pi', 'omp'])
def test_native_pending_claim_stops_after_deadline_even_when_children_time_out(tmp_path, harness):
    import json
    import os
    from pathlib import Path
    import shutil
    import subprocess
    import sys
    node = shutil.which('node')
    if not node or int(subprocess.check_output([node, '--version'], text=True).lstrip('v').split('.')[0]) < 23:
        pytest.skip('Requires Node >=23 TypeScript stripping')
    requests = tmp_path / 'bridge.requests'
    fake = tmp_path / 'bridge.py'
    fake.write_text('import json,sys,time\nrequest=json.load(sys.stdin)\n'
        + 'with open(' + repr(str(requests)) + ',"a") as output: output.write(request["action"]+"\\n")\n'
        + 'if request["action"]=="recall-ready": time.sleep(2)\n'
        + 'print(json.dumps({"pending":True,"recall_token":"owned","context":"","shown":[]}))\n')
    observer = tmp_path / 'deadline.mjs'
    source = Path(bridge.__file__).with_name('native_shared.ts')
    observer.write_text('import {installNative} from ' + json.dumps(source.as_uri()) + ';\n' + '''
let clock=Date.now();
Date.now=()=>clock;
const handlers=new Map();
const api={on:(name,fn)=>handlers.set(name,fn),registerFlag:()=>{},getFlag:()=>process.env.MEMENTO_CONFIG,registerTool:()=>{}};
const Type=new Proxy({},{get:()=>()=>({})});
installNative(api,Type,process.env.TEST_HARNESS);
const ctx={cwd:process.cwd(),sessionManager:{getSessionId:()=>"deadline-session",getSessionFile:()=>undefined,getEntries:()=>[]}};
await handlers.get("session_start")({},ctx);
await handlers.get("before_agent_start")({prompt:"Synthetic pending query.",messages:[]},ctx);
await handlers.get("context")({messages:[]},ctx);
clock+=16001;
await handlers.get("context")({messages:[]},ctx);
await handlers.get("context")({messages:[]},ctx);
await handlers.get("session_shutdown")({},ctx);
''')
    environment = {**os.environ, 'MEMENTO_CONFIG': str(tmp_path / 'runner.json'),
        'MEMENTO_BRIDGE': str(fake), 'MEMENTO_PYTHON': sys.executable, 'TEST_HARNESS': harness}
    environment.pop('MEMENTO_PROCESSING', None)
    result = subprocess.run([node, str(observer)], capture_output=True, text=True, cwd=tmp_path,
                            env=environment, timeout=5)
    assert result.returncode == 0, result.stderr
    assert requests.read_text().splitlines() == ['recall', 'recall-ready']
