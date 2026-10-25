"""Native tool diagnostics across the shipped JS/Python subprocess boundary."""
import contextlib
import http.server
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.parse

import pytest

from memento import bridge


PRIVATE = 'private-query-token-config-response-marker'


@pytest.fixture
def native_tool(tmp_path):
    node = shutil.which('node')
    if not node or int(subprocess.check_output([node, '--version'], text=True).lstrip('v').split('.')[0]) < 23:
        pytest.skip('Requires Node >=23 TypeScript stripping')
    config = tmp_path / (PRIVATE + '.json')
    config.write_text(json.dumps(dict(uid=1000, client_uid='synthetic-device', url='http://127.0.0.1:1')))
    observer = tmp_path / 'tool.mjs'
    observer.write_text('import {installNative} from ' + json.dumps(
        Path(bridge.__file__).with_name('native_shared.ts').as_uri()) + ';\n' + '''
const tools = new Map();
const api = {on: () => {}, registerFlag: () => {}, getFlag: () => process.env.MEMENTO_CONFIG,
  registerTool: tool => tools.set(tool.name, tool)};
const Type = new Proxy({}, {get: () => () => ({})});
installNative(api, Type, process.env.TEST_HARNESS);
const ctx = {cwd: process.cwd(), sessionManager: {
  getSessionId: () => "synthetic-session", getSessionFile: () => undefined, getEntries: () => []}};
try {
  const result = await tools.get("memento_recall").execute("call", JSON.parse(process.env.TEST_ARGUMENTS), undefined, undefined, ctx);
  console.log(JSON.stringify({result}));
} catch (error) { console.log(JSON.stringify({error: error.message})); }
''')

    def run(*, harness='pi', url=None, fake=None, python=None, query=PRIVATE):
        if url is not None:
            value = json.loads(config.read_text())
            config.write_text(json.dumps({**value, 'url': url}))
        environment = {**os.environ, 'MEMENTO_CONFIG': str(config),
            'MEMENTO_BRIDGE': str(fake or Path(bridge.__file__)),
            'MEMENTO_PYTHON': python or sys.executable, 'TEST_HARNESS': harness,
            'TEST_ARGUMENTS': json.dumps({'query': query})}
        environment.pop('MEMENTO_PROCESSING', None)
        result = subprocess.run([node, str(observer)], cwd=tmp_path, env=environment,
            capture_output=True, text=True, timeout=20)
        assert result.returncode == 0, result.stderr
        assert PRIVATE not in result.stdout + result.stderr
        return json.loads(result.stdout)

    return run, config


@pytest.fixture
def service():
    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            case = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)['q'][0]
            if case == 'timeout':
                time.sleep(5.2)  # Exercise the unchanged five-second Python HTTP timeout.
            status = 503 if case == 'http' else 200
            self.send_response(status)
            self.end_headers()
            body = (PRIVATE.encode() if case in ('http', 'malformed') else
                PRIVATE.encode() + b'x' * (4 * bridge.MAX_BODY) if case == 'oversized' else
                b'{"memories":[],"client_revision":"synthetic"}')
            with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                self.wfile.write(body)

    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield 'http://127.0.0.1:' + str(server.server_port)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.mark.parametrize('harness', ['pi', 'omp'])
@pytest.mark.parametrize('case,category', [
    ('timeout', 'timeout'), ('http', 'HTTP 503'), ('malformed', 'invalid response'),
    ('oversized', 'response too large'), ('success', None)])
def test_real_native_tool_http_boundary(native_tool, service, harness, case, category):
    run, _ = native_tool
    result = run(harness=harness, url=service, query=case)
    if category is not None:
        assert result['error'].startswith('Memento tool failed (' + category + ').')
        assert 'retry' in result['error']
    else:
        expected = {'result': {'memories': [], 'client_revision': 'synthetic'}}
        assert result == {'result': {'content': [{'type': 'text', 'text': json.dumps(expected, separators=(',', ':'))}],
            'details': expected}}


def test_real_native_tool_connection_failure(native_tool):
    run, _ = native_tool
    # The fixture starts with the same closed loopback endpoint as existing bridge tests.
    assert run()['error'].startswith('Memento tool failed (connection).')


def test_real_native_tool_local_lock(native_tool):
    run, config = native_tool
    spool = bridge.Spool(config, json.loads(config.read_text()))
    spool.close()
    with sqlite3.connect(config.with_suffix('.bridge.sqlite3')) as blocker:
        blocker.execute('BEGIN EXCLUSIVE')
        try:
            assert run()['error'].startswith('Memento tool failed (local lock).')
        finally:
            blocker.rollback()


def test_real_native_tool_invalid_configuration(native_tool):
    run, config = native_tool
    config.write_text(PRIVATE)
    assert run()['error'].startswith('Memento tool failed (invalid data).')


@pytest.mark.parametrize('payload,expected', [
    ({'error': 'bridge_failed', 'diagnostic': {'category': 'timeout', 'message': PRIVATE}, 'body': PRIVATE}, '(timeout)'),
    ({'error': 'bridge_failed', 'diagnostic': {'category': 'http', 'http_status': 429, 'body': PRIVATE}}, '(HTTP 429)'),
    ({'error': 'bridge_failed', 'diagnostic': {'category': PRIVATE}}, None),
    ({'error': 'bridge_failed', 'diagnostic': {'category': '__proto__'}}, None),
    ({'error': 'bridge_failed', 'diagnostic': {'category': 'constructor'}}, None),
    ({'error': 'bridge_failed', 'diagnostic': {'category': 'http', 'http_status': PRIVATE}}, None),
    ({'error': 'bridge_failed', 'diagnostic': {'category': 'http', 'http_status': 503.5}}, None),
    ({'error': 'bridge_failed', 'diagnostic': {'category': 'http', 'http_status': 600}}, None),
    ({'error': PRIVATE, 'diagnostic': {'category': 'timeout'}}, None),
    ({'error': 'bridge_failed'}, None),
    ([PRIVATE], None),
    (PRIVATE, None),
])
def test_native_tool_allowlists_diagnostics_and_discards_raw_child_errors(native_tool, tmp_path, payload, expected):
    run, _ = native_tool
    fake = tmp_path / 'untrusted.py'
    fake.write_text('import json,sys\njson.load(sys.stdin)\nprint(' + repr(json.dumps(payload)) + ')\n'
        'print(' + repr(PRIVATE) + ',file=sys.stderr)\nraise SystemExit(1)\n')
    result = run(fake=fake)
    if expected:
        assert expected in result['error']
    else:
        assert result == {'error': 'Memento bridge failed'}


@pytest.mark.parametrize('case', ['malformed', 'oversized', 'exit', 'spawn'])
def test_native_tool_child_failures_stay_safe(native_tool, tmp_path, case):
    run, _ = native_tool
    fake = tmp_path / 'untrusted.py'
    fake.write_text('import json,sys\njson.load(sys.stdin)\nprint(' + repr(PRIVATE)
        + (' + "x" * 2_000_000' if case == 'oversized' else '') + ')\n'
        'print(' + repr(PRIVATE) + ',file=sys.stderr)\n'
        + ('raise SystemExit(23)\n' if case == 'exit' else ''))
    result = run(fake=fake, python=str(tmp_path / PRIVATE) if case == 'spawn' else None)
    if case == 'malformed':
        assert '(invalid response)' in result['error']
    elif case == 'oversized':
        assert '(response too large)' in result['error']
    elif case == 'spawn':
        assert 'could not start' in result['error']
    else:
        assert result == {'error': 'Memento bridge failed'}
