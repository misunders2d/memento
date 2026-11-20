"""Runs the real native worker adapter under node with a stubbed pi API and a scripted service.

No model, native CLI or network: `fetch` is replaced, and the pi tool loop is mimicked as
tool_call -> execute -> tool_result (a blocked call skips the last two, a thrown error becomes
an error result carrying the message)."""
import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest

from memento import runner

ROOT = Path(__file__).resolve().parents[1]
ADAPTER = ROOT / 'src' / 'memento' / 'native_worker.ts'
PROBE = ROOT / 'tests' / 'native_worker_probe.ts'
SECRET = 'lease-capability'
REJECTION = 'consolidation_schema_invalid'
RELEASE = dict(action='release', command=dict(id='job', owner=SECRET, error='service_rejected'))

LOADER = r'''
import { registerHooks } from "node:module";
import { pathToFileURL } from "node:url";
const stub = "data:text/javascript," + encodeURIComponent(
  "export const Type={Object:v=>v,Union:v=>v,Literal:v=>v,Any:()=>({})};");
registerHooks({ resolve(specifier, context, next) {
  if (specifier === "@earendil-works/pi-ai") return { url: stub, shortCircuit: true };
  return next(specifier, context); } });
process.env.MEMENTO_WORKER_OPTIONS = '{"agent":"synthetic","timezone":"UTC"}';
process.env.MEMENTO_WORKER_URL = "http://service.invalid";
export const load = async (path) => (await import(pathToFileURL(path).href)).default;
'''

PROBE_RUN = r'''
import { load } from "./loader.mjs";
await (await load(process.argv[2]))();
'''

SCENARIOS = r'''
import { load } from "./loader.mjs";
const worker = await load(process.argv[2]);
const SECRET = "lease-capability", EVIDENCE = "synthetic evidence text";
const records = [];
console.error = (text) => { records.push(JSON.parse(text)); };
// The host's validation codes, as worker.py passes them; a scenario may replace or drop them.
const HOST_CODES = process.env.MEMENTO_WORKER_VALIDATION_CODES;
const reply = (status, value) => ({ ok: status < 300, status,
  text: async () => typeof value === "string" ? value : JSON.stringify(value) });

// One adapter instance against a scripted service: script[action] answers that action's request
// (a function receives the request's init). `codes` is the validation-code variable, absent when undefined.
async function boot(script, { codes } = { codes: HOST_CODES }) {
  const handlers = {}, posts = [];
  let definition, calls = 0;
  if (codes === undefined) delete process.env.MEMENTO_WORKER_VALIDATION_CODES;
  else process.env.MEMENTO_WORKER_VALIDATION_CODES = codes;
  worker({ registerTool(value) { definition = value; }, on(name, handler) { handlers[name] = handler; },
    getActiveTools() { return ["memento_work"]; },
    getAllTools() { return [{ name: "memento_work", exposure: "direct", reachesTools: false }]; } });
  globalThis.fetch = async (target, init) => {
    const body = JSON.parse(init.body);
    posts.push({ target, body });
    const answer = script[body.action];
    if (answer === undefined) throw new Error("unscripted request " + body.action);
    if (answer === "network") throw new TypeError("fetch failed");
    return typeof answer === "function" ? answer(init) : answer;
  };
  await handlers.session_start();
  handlers.context({ messages: [] });  // the session's first model request: nothing to keep yet
  const from = () => records.length;
  const call = async (input, ctx = { signal: undefined }) => {
    const event = { toolName: "memento_work", toolCallId: "call-" + ++calls, input };
    const start = from(), sent = posts.length;
    const blocked = handlers.tool_call(event);
    if (blocked?.block)
      return { blocked: true, events: records.slice(start), posts: posts.slice(sent).map(post => post.body) };
    let content, isError = false;
    try { content = (await definition.execute(event.toolCallId, event.input, undefined)).content; }
    catch (error) { content = [{ type: "text", text: error.message }]; isError = true; }
    const patch = await handlers.tool_result({ ...event, content, isError }, ctx);
    return { blocked: false, patched: !!patch, isError: patch?.isError ?? isError,
      text: (patch?.content ?? content).map(part => part.text).join("\n"),
      events: records.slice(start), posts: posts.slice(sent).map(post => post.body),
      targets: posts.slice(sent).map(post => post.target) };
  };
  // What the next model request would contain, given three earlier native messages.
  const context = () => {
    const history = [{ role: "user", content: "a" }, { role: "assistant", content: "b" }, { role: "toolResult", content: "c" }];
    const messages = handlers.context({ messages: history }).messages;
    return { state: JSON.parse(messages[0].content[0].text), kept: messages.length - 1, text: JSON.stringify(messages) };
  };
  return { call, context, handlers, definition: () => definition };
}
const work = { work: [{ id: "job", owner: SECRET, kind: "consolidate", instruction: "fixed", schema: {},
  input: { candidates: [{ text: EVIDENCE }] } }], lease_seconds: 180, backlog: { claimable: { extract: 1, consolidate: 1 } } };
const claimInput = () => ({ action: "claim", command: { kinds: ["consolidate"] } });
const inputs = {
  submit: () => ({ action: "submit", command: { complete: { id: "job", output: { decisions: [] } } } }),
  submit_next: () => ({ action: "submit", command: { complete: { id: "job", output: { decisions: [] } },
    next: { kinds: ["extract"], batch_size: 4 } } }),
  complete: () => ({ action: "complete", command: { id: "job", output: { decisions: [] } } }),
};
const rejection = reply(422, { detail: "consolidation_schema_invalid" });
const out = {};

// A rejected submission of the held job: every model-facing and host-facing consequence.
for (const [name, input] of Object.entries(inputs)) {
  const script = { claim: reply(200, work), submit: rejection, complete: rejection, release: reply(200, { released: true }) };
  const adapter = await boot(script);
  await adapter.call(claimInput());
  const held = adapter.context();
  const rejected = await adapter.call(input());
  const after = adapter.context();
  const again = await adapter.call({ action: "release", command: { id: "job" } });
  script.claim = reply(200, { work: [], lease_seconds: 180, backlog: { claimable: { extract: 0, consolidate: 0 } } });
  const reclaim = await adapter.call(claimInput());
  out["rejected_" + name] = { held, rejected, after, again, reclaim };
}

// Failures that are not a rejection of the submitted held job never release it.
for (const [name, answer] of Object.entries({ conflict: reply(409, { detail: "Processing lease is not held" }),
  server_error: reply(500, { detail: "Internal error" }), network: "network" })) {
  const script = { claim: reply(200, work), submit: answer, release: reply(200, { released: true }) };
  const adapter = await boot(script);
  await adapter.call(claimInput());
  const failed = await adapter.call(inputs.submit());
  const after = adapter.context();
  const manual = await adapter.call({ action: "release", command: { id: "job" } });
  out["kept_" + name] = { failed, after, manual, released: adapter.context() };
}
// A 422 that is not a model-output validation code (request shape, no detail), or a host that
// supplied no usable codes: the job stays held and the model sees the error, as before.
for (const [name, answer, codes] of [
  ["without_detail", reply(422, "not json"), HOST_CODES],
  ["invalid_command", reply(422, { detail: "Invalid processing command" }), HOST_CODES],
  ["output_not_object", reply(422, { detail: "Model output and usage must be JSON objects" }), HOST_CODES],
  ["invalid_fields", reply(422, { detail: "Invalid request fields" }), HOST_CODES],
  ["codes_missing", rejection, undefined],
  ["codes_empty", rejection, "[]"],
  ["codes_malformed", rejection, "not json"],
  ["codes_not_list", rejection, JSON.stringify({ consolidation_schema_invalid: true })],
]) {
  const script = { claim: reply(200, work), submit: answer, release: reply(200, { released: true }) };
  const adapter = await boot(script, { codes });
  await adapter.call(claimInput());
  const failed = await adapter.call(inputs.submit());
  const after = adapter.context();
  const manual = await adapter.call({ action: "release", command: { id: "job" } });
  out["held_" + name] = { failed, after, manual, released: adapter.context() };
}
{
  // Sixteen commits, then a rejection: the released job leaves nothing held, so the session boundary is due.
  const script = { claim: reply(200, work), release: reply(200, { released: true }) };
  const adapter = await boot(script);
  for (let index = 0; index < 16; index++) {
    script.claim = reply(200, { ...work, work: [{ id: "done-" + index, owner: SECRET, kind: "consolidate", input: {} }] });
    await adapter.call(claimInput());
    script.complete = reply(200, { candidate_count: 0 });
    await adapter.call({ action: "complete", command: { id: "done-" + index, output: {} } });
  }
  script.claim = reply(200, work);
  await adapter.call(claimInput());
  script.complete = rejection;
  out.rejected_at_boundary = { rejected: await adapter.call(inputs.complete()) };
}
{
  const adapter = await boot({ claim: rejection });
  out.rejected_claim = { failed: await adapter.call(claimInput()), after: adapter.context() };
}
{
  // A blocked submission for a job this worker does not hold reaches no service at all.
  const adapter = await boot({ claim: reply(200, work) });
  await adapter.call(claimInput());
  out.foreign_submit = { failed: await adapter.call({ action: "complete", command: { id: "other", output: {} } }),
    after: adapter.context() };
}

// The automatic release is not confirmed: the lease stays held and nothing reports a release.
for (const [name, answer] of Object.entries({ unconfirmed: reply(200, { released: false }),
  refused: reply(409, { detail: "Processing lease is not held" }), network: "network", not_json: reply(200, "released") })) {
  const script = { claim: reply(200, work), submit: rejection, release: answer };
  const adapter = await boot(script);
  await adapter.call(claimInput());
  const rejected = await adapter.call(inputs.submit());
  const after = adapter.context();
  script.release = reply(200, { released: true });
  const manual = await adapter.call({ action: "release", command: { id: "job" } });
  out["unreleased_" + name] = { rejected, after, manual, released: adapter.context() };
}
// A release reply that waits until `settle` is called; an abort of its request rejects it as fetch would.
const realSetTimeout = globalThis.setTimeout;
const pause = () => new Promise(resolve => realSetTimeout(resolve, 1));
async function until(ready) {
  for (let index = 0; !ready(); index++) { if (index > 2000) throw new Error("request never sent"); await pause(); }
}
function deferred() {
  const state = { sent: false, settle: undefined };
  state.answer = (init) => new Promise((resolve, reject) => {
    state.sent = true;
    state.settle = resolve;
    init.signal?.addEventListener("abort", () => reject(new DOMException("This operation was aborted", "AbortError")));
  });
  return state;
}
{
  // The service commits the automatic release only after more than fifteen seconds. A fake clock
  // fires every long timer while the request is in flight: no adapter timer may give up on it.
  const timers = [];
  globalThis.setTimeout = (callback, ms, ...rest) => ms >= 1000 ? (timers.push(callback), 0)
    : realSetTimeout(callback, ms, ...rest);
  try {
    const release = deferred();
    const adapter = await boot({ claim: reply(200, work), submit: rejection, release: release.answer });
    await adapter.call(claimInput());
    const pending = adapter.call(inputs.submit());
    await until(() => release.sent);
    for (const callback of timers.splice(0)) callback();
    await pause();
    release.settle(reply(200, { released: true }));
    out.slow_release = { rejected: await pending, after: adapter.context() };
  } finally { globalThis.setTimeout = realSetTimeout; }
}
// Pi's abort signal still ends the automatic release, in flight or already aborted: the job stays held.
for (const name of ["aborted_release", "pre_aborted_release"]) {
  const controller = new AbortController(), release = deferred();
  const script = { claim: reply(200, work), submit: rejection, release: release.answer };
  const adapter = await boot(script);
  await adapter.call(claimInput());
  if (name === "pre_aborted_release") controller.abort();
  const pending = adapter.call(inputs.submit(), { signal: controller.signal });
  if (name === "aborted_release") { await until(() => release.sent); controller.abort(); }
  const rejected = await pending;
  const after = adapter.context();
  script.release = reply(200, { released: true });
  const manual = await adapter.call({ action: "release", command: { id: "job" } });
  out[name] = { rejected, after, manual, released: adapter.context() };
}
{
  // Pi's parallel mode: a rejected submit and a model release of the same job in one message.
  // The release's result lands while the adapter's own release is still waiting for the service.
  const automatic = deferred();
  let sent = 0;
  const script = { claim: reply(200, work), submit: rejection,
    release: (init) => ++sent === 1 ? reply(200, { released: true }) : automatic.answer(init) };
  const adapter = await boot(script);
  await adapter.call(claimInput());
  const submit = { toolName: "memento_work", toolCallId: "parallel-submit", input: inputs.submit() };
  const release = { toolName: "memento_work", toolCallId: "parallel-release", input: { action: "release", command: { id: "job" } } };
  const start = records.length;
  // Preflighted in source order, then executed concurrently.
  const blocked = [adapter.handlers.tool_call(submit), adapter.handlers.tool_call(release)].map(result => !!result?.block);
  const run = async (event) => {
    try { return { content: (await adapter.definition().execute(event.toolCallId, event.input, undefined)).content, isError: false }; }
    catch (error) { return { content: [{ type: "text", text: error.message }], isError: true }; }
  };
  const [rejected, released] = await Promise.all([run(submit), run(release)]);
  const first = adapter.handlers.tool_result({ ...submit, ...rejected }, { signal: undefined });
  await until(() => automatic.sent);
  const second = await adapter.handlers.tool_result({ ...release, ...released }, { signal: undefined });
  automatic.settle(reply(409, { detail: "Processing lease is not held" }));
  const patch = await first;
  out.parallel_release = { blocked, sent, text: (patch?.content ?? rejected.content).map(part => part.text).join("\n"),
    release: second.content.map(part => part.text).join("\n"), events: records.slice(start), after: adapter.context() };
}
{
  const adapter = await boot({});
  out.definition = { executionMode: adapter.definition().executionMode };
}
process.stdout.write(JSON.stringify(out));
'''


def node(tmp_path, source, target):
    if not shutil.which('node'):
        pytest.skip('node is not installed')
    version = subprocess.run(['node', '-p', 'process.versions.node'], capture_output=True, text=True).stdout.strip()
    # Type stripping without a flag (23.6) and synchronous module.registerHooks (23.5).
    if tuple(int(part) for part in version.split('.')[:2]) < (23, 6):
        pytest.skip('node cannot load the TypeScript adapter directly')
    (tmp_path / 'loader.mjs').write_text(LOADER)
    (tmp_path / 'run.mjs').write_text(source)
    # The variable worker.py passes the adapter.
    env = dict(os.environ, MEMENTO_WORKER_VALIDATION_CODES=json.dumps(sorted(runner.WORK_VALIDATION_CODES)))
    return subprocess.run(['node', str(tmp_path / 'run.mjs'), str(target)], capture_output=True, text=True,
                          cwd=tmp_path, timeout=120, env=env)


@pytest.fixture(scope='module')
def observed(tmp_path_factory):
    result = node(tmp_path_factory.mktemp('native-worker'), SCENARIOS, ADAPTER)
    assert result.returncode == 0, result.stderr[-4000:]
    return json.loads(result.stdout)


def releases(events):
    return [event for event in events if event.get('released')]


def test_existing_callback_probe_still_passes_unmodified(tmp_path):
    result = node(tmp_path, PROBE_RUN, PROBE)
    assert result.returncode == 0, result.stderr[-4000:]
    summary = [line for line in result.stderr.splitlines() if line.startswith('WORKER_CALLBACK_PROBE ')]
    assert len(summary) == 1 and json.loads(summary[0].split(' ', 1)[1])['passed'] is True


@pytest.mark.parametrize('name', ['submit', 'submit_next', 'complete'])
def test_rejected_submission_is_released_by_the_adapter_without_a_model_turn(observed, name):
    seen = observed['rejected_' + name]
    assert seen['held']['state']['current'][0]['id'] == 'job' and 'synthetic evidence text' in seen['held']['text']
    rejected = seen['rejected']
    # Exactly the release the model used to send through the tool, to the same endpoint.
    assert rejected['targets'] == ['http://service.invalid/work'] * 2
    assert [post['action'] for post in rejected['posts']] == ['complete' if name == 'complete' else 'submit', 'release']
    assert rejected['posts'][1] == RELEASE
    # The service issues no next lease for a rejected submit; the adapter claims none either.
    assert not any(post['action'] == 'claim' for post in rejected['posts'])
    # Host sequence: the failed-attempt receipt exactly as before, then one ordinary release state.
    action = 'complete' if name == 'complete' else 'submit'
    assert rejected['events'] == [
        dict(type='memento_worker_state', error=True, code=REJECTION, action=action, http_status=422),
        dict(type='memento_worker_state', claimed=[], empty_kinds=[], completed=None, released='job',
             boundary=False, empty=False)]
    # Model-facing result: compact, says rejected and released, no evidence or lease capability.
    assert rejected['isError'] is True
    result = json.loads(rejected['text'])
    assert result['current_work'] == [] and result['session_boundary'] is False
    assert result['receipt']['rejected'] == 'job' and result['receipt']['released'] is True
    assert result['receipt']['code'] == REJECTION and result['receipt']['http_status'] == 422
    assert 'claim' in result['receipt']['next'].lower() and 'do not release' in result['receipt']['next'].lower()
    assert SECRET not in rejected['text'] and 'synthetic evidence text' not in rejected['text']
    assert len(rejected['text']) < 600
    # Context reset like a model-driven release: no current work, no finished history, same receipt.
    after = seen['after']
    assert after['state']['current'] == [] and after['kept'] == 0
    assert after['state']['receipt'] == result['receipt']
    assert SECRET not in after['text'] and 'synthetic evidence text' not in after['text']
    # The job is no longer held: a model release is refused locally and reaches no service.
    assert seen['again']['blocked'] and seen['again']['posts'] == []
    assert seen['again']['events'] == [dict(type='memento_worker_state', error=True, code='invalid_or_foreign_work',
                                            cause='foreign_job', action='release')]
    # Cooldown: the released job is excluded from this worker's following claims.
    assert seen['reclaim']['posts'][0]['action'] == 'claim'
    assert seen['reclaim']['posts'][0]['command']['exclude'] == ['job']


@pytest.mark.parametrize('name,code', [
    ('without_detail', 'processing_request_failed'), ('invalid_command', 'Invalid processing command'),
    ('output_not_object', 'Model output and usage must be JSON objects'), ('invalid_fields', 'Invalid request fields'),
    ('codes_missing', REJECTION), ('codes_empty', REJECTION), ('codes_malformed', REJECTION), ('codes_not_list', REJECTION)])
def test_422_without_a_known_validation_code_keeps_the_job_held(observed, name, code):
    seen = observed['held_' + name]
    failed = seen['failed']
    # No automatic release request: only the submit reached the service.
    assert [post['action'] for post in failed['posts']] == ['submit']
    assert failed['events'] == [dict(type='memento_worker_state', error=True, code=code, action='submit', http_status=422)]
    # The model sees exactly the service error and no adapter rewrite, so it can correct its call.
    assert failed['isError'] is True and failed['patched'] is False
    assert failed['text'] == 'MEMENTO_HTTP_422: ' + code
    assert seen['after']['state']['current'][0]['id'] == 'job' and seen['after']['kept'] == 3
    # Still held: the model-driven release works exactly as before.
    assert seen['manual']['posts'] == [RELEASE]
    assert releases(seen['manual']['events']) == [dict(type='memento_worker_state', claimed=[], empty_kinds=[],
                                                       completed=None, released='job', boundary=False, empty=False)]
    assert seen['released']['state']['current'] == [] and seen['released']['kept'] == 0


def test_rejection_at_the_session_boundary(observed):
    # Same boundary rule as a model-driven release: sixteen commits and nothing held.
    boundary = observed['rejected_at_boundary']['rejected']
    assert boundary['events'][1] == dict(type='memento_worker_state', claimed=[], empty_kinds=[], completed=None,
                                         released='job', boundary=True, empty=False)
    assert json.loads(boundary['text'])['session_boundary'] is True


@pytest.mark.parametrize('name,status,code', [('conflict', 409, 'Processing lease is not held'),
                                              ('server_error', 500, 'Internal error'), ('network', None, None)])
def test_other_submission_failures_keep_the_lease_and_the_previous_behavior(observed, name, status, code):
    seen = observed['kept_' + name]
    failed = seen['failed']
    assert [post['action'] for post in failed['posts']] == ['submit']
    expected = dict(type='memento_worker_state', error=True, action='submit')
    if status:
        expected.update(code=code, http_status=status)
    assert failed['events'] == [expected]
    # The model sees exactly the previous error text and no adapter rewrite.
    assert failed['isError'] is True and failed['patched'] is False
    assert failed['text'] == (f'MEMENTO_HTTP_{status}: {code}' if status else 'fetch failed')
    assert seen['after']['state']['current'][0]['id'] == 'job' and seen['after']['kept'] == 3
    # Still held, so the model-driven release works exactly as before.
    assert seen['manual']['posts'] == [RELEASE]
    assert releases(seen['manual']['events']) == [dict(type='memento_worker_state', claimed=[], empty_kinds=[],
                                                       completed=None, released='job', boundary=False, empty=False)]
    assert seen['released']['state']['current'] == [] and seen['released']['kept'] == 0


def test_rejection_of_other_actions_and_foreign_jobs_never_releases(observed):
    claim = observed['rejected_claim']
    assert [post['action'] for post in claim['failed']['posts']] == ['claim']
    assert claim['failed']['events'] == [dict(type='memento_worker_state', error=True, code=REJECTION,
                                              action='claim', http_status=422)]
    assert claim['failed']['patched'] is False
    foreign = observed['foreign_submit']
    assert foreign['failed']['blocked'] and foreign['failed']['posts'] == []
    assert foreign['after']['state']['current'][0]['id'] == 'job'


@pytest.mark.parametrize('name', ['unconfirmed', 'refused', 'network', 'not_json'])
def test_unconfirmed_automatic_release_keeps_the_lease_held_and_says_so(observed, name):
    seen = observed['unreleased_' + name]
    rejected = seen['rejected']
    assert [post['action'] for post in rejected['posts']] == ['submit', 'release']
    assert rejected['posts'][1] == RELEASE
    # Only the failed-attempt receipt reaches the host: nothing claims a release.
    assert rejected['events'] == [dict(type='memento_worker_state', error=True, code=REJECTION,
                                       action='submit', http_status=422)]
    assert rejected['isError'] is True and rejected['text'].startswith('MEMENTO_HTTP_422: ' + REJECTION + '\n')
    assert 'still held' in rejected['text'] and 'release' in rejected['text'] and 'status' in rejected['text']
    assert SECRET not in rejected['text']
    assert seen['after']['state']['current'][0]['id'] == 'job' and seen['after']['kept'] == 3
    # The model can still release it, and only the service's confirmation marks it released.
    assert not seen['manual']['blocked'] and seen['manual']['posts'] == [RELEASE]
    assert [event['released'] for event in releases(seen['manual']['events'])] == ['job']
    assert seen['released']['state']['current'] == []


def test_a_slow_automatic_release_is_not_given_up_by_an_adapter_timer(observed):
    rejected = observed['slow_release']['rejected']
    assert [post['action'] for post in rejected['posts']] == ['submit', 'release']
    assert rejected['posts'][1] == RELEASE
    # The service's late released:true is what the adapter reports, as for a prompt reply.
    assert rejected['events'] == [
        dict(type='memento_worker_state', error=True, code=REJECTION, action='submit', http_status=422),
        dict(type='memento_worker_state', claimed=[], empty_kinds=[], completed=None, released='job',
             boundary=False, empty=False)]
    result = json.loads(rejected['text'])
    assert result['current_work'] == [] and result['receipt']['released'] is True
    assert 'still held' not in rejected['text']
    assert observed['slow_release']['after']['state']['current'] == []


@pytest.mark.parametrize('name,sent', [('aborted_release', True), ('pre_aborted_release', False)])
def test_pi_abort_ends_the_automatic_release_and_keeps_the_job_held(observed, name, sent):
    seen = observed[name]
    rejected = seen['rejected']
    assert [post['action'] for post in rejected['posts']] == (['submit', 'release'] if sent else ['submit'])
    assert rejected['events'] == [dict(type='memento_worker_state', error=True, code=REJECTION,
                                       action='submit', http_status=422)]
    assert rejected['isError'] is True and 'still held' in rejected['text']
    assert seen['after']['state']['current'][0]['id'] == 'job' and seen['after']['kept'] == 3
    assert seen['manual']['posts'] == [RELEASE]
    assert [event['released'] for event in releases(seen['manual']['events'])] == ['job']
    assert seen['released']['state']['current'] == []


def test_a_job_released_meanwhile_is_reported_released_not_still_held(observed):
    seen = observed['parallel_release']
    assert seen['blocked'] == [False, False] and seen['sent'] == 2
    # The model's own release committed first and was reported once; nothing else claims a release.
    assert [event for event in seen['events'] if 'released' in event] == [
        dict(type='memento_worker_state', claimed=[], empty_kinds=[], completed=None, released='job',
             boundary=False, empty=False)]
    assert json.loads(seen['release'])['receipt'] == {'released': True}
    # The rejected submit says the job is released, not still held.
    assert 'still held' not in seen['text']
    result = json.loads(seen['text'])
    assert result['current_work'] == [] and result['session_boundary'] is False
    assert result['receipt']['rejected'] == 'job' and result['receipt']['released'] is True
    assert seen['after']['state']['current'] == []


def test_work_tool_is_registered_for_sequential_execution(observed):
    # Pi runs one assistant message's sibling calls of a sequential tool one after another
    # (tool_call, execute, tool_result each), so they cannot interleave on the shared state.
    assert observed['definition'] == {'executionMode': 'sequential'}
