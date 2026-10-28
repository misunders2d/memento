"""Persistent launcher lifecycle tests; real native qualification is separate."""
import json
import os
import sys
import textwrap
import time

import pytest

from memento import runner, worker


@pytest.fixture
def native(tmp_path,monkeypatch):
    trace=tmp_path/'trace.jsonl'
    script=tmp_path/'native'
    script.write_text('#!'+sys.executable+'\n'+textwrap.dedent('''
        import json,os,sys,time
        def emit(value,err=False):
            print(json.dumps(value),file=sys.stderr if err else sys.stdout,flush=True)
        if '--help' in sys.argv:
            print('--mode --no-session --no-builtin-tools --no-extensions --no-mcp --no-skills --no-prompt-templates --no-context-files --no-approve --no-themes')
            raise SystemExit
        def catalog():
            tool='memento_work'
            all=[dict(name=tool,exposure='direct',reachesTools=False)]
            if os.environ.get('EXTRA_TOOL'):all.append(dict(name='bash',exposure='direct',reachesTools=False))
            emit(dict(type='memento_worker_catalog',active=[tool],all=all),True)
        catalog()
        boundary_sent=False
        for line in sys.stdin:
            message=json.loads(line)
            with open(os.environ['WORKER_TEST_TRACE'],'a') as file:file.write(json.dumps(dict(pid=os.getpid(),type=message['type']))+'\\n')
            if message['type']=='prompt':
                if os.environ.get('BOUNDARY') and not boundary_sent:
                    boundary_sent=True
                    emit(dict(type='memento_worker_state',claimed=[dict(id='checkpoint',owner='lease-c',kind='consolidate')],lease_seconds=.12),True)
                    emit(dict(type='memento_worker_state',completed='checkpoint',boundary=True,claimed=[]),True)
                    continue
                if os.environ.get('RETRY'):
                    emit(dict(type='memento_worker_model',model='synthetic',usage={},stop_reason='error'),True)
                    emit(dict(type='message_end',message=dict(role='assistant',stopReason='error')))
                    emit(dict(type='agent_end',willRetry=True))
                emit(dict(type='memento_worker_state',claimed=[dict(id='first',owner='lease-a',kind='consolidate')],lease_seconds=.12),True)
                if os.environ.get('HANG'):time.sleep(30)
                time.sleep(.15)
                emit(dict(type='message_end',message=dict(role='assistant',model='synthetic',usage=dict(input=10,output=2,secret='do not print'),content='do not print')),False)
                emit(dict(type='memento_worker_model',model='synthetic',usage=dict(input=10,output=2),stop_reason='toolUse'),True)
                emit(dict(type='memento_worker_state',completed='first',claimed=[dict(id='second',owner='lease-b',kind='consolidate')],lease_seconds=.12),True)
                time.sleep(.15)
                emit(dict(type='memento_worker_state',completed='second',claimed=[]),True)
                emit(dict(type='memento_worker_state',empty=True,kind='extract',claimed=[]),True)
                emit(dict(type='memento_worker_state',empty=True,kind='consolidate',claimed=[]),True)
                emit(dict(type='agent_end'))
                emit(dict(type='agent_settled',aborted=False))
            elif message['type']=='abort':
                emit(dict(type='agent_end',willRetry=False))
                emit(dict(type='agent_settled',aborted=True))
                emit(dict(type='response',id=message['id'],command='abort',success=True))
            elif message['type']=='new_session':
                catalog()
                emit(dict(type='response',id=message['id'],command='new_session',success=True,data=dict(cancelled=False)))
    '''))
    script.chmod(0o700)
    monkeypatch.setenv('WORKER_TEST_TRACE',str(trace))
    calls=[]
    class Service:
        def __init__(self,url,timeout=15):self.base_url=url
        def post(self,path,command):
            calls.append((path,command))
            if path=='/work/pool':
                return dict(worker_protocol=1,lease_seconds=.12,policies=dict(extract={},consolidate={}),
                            pending_extract_conversations=0,ready_candidates=2,jobs=[],pending_maintenance_groups=0)
            if path=='/work/renew':return {'renewed':True}
            if path=='/work/release':return {'released':True}
            raise AssertionError(path)
    monkeypatch.setattr(runner,'Service',Service)
    config=dict(mode='server',preset='pi',agent='pi',command=[str(script)],timezone='UTC',
                url='http://127.0.0.1:1',deadline_seconds=3)
    return config,trace,calls


def test_two_completed_units_use_one_native_process_with_independent_renewal(native,capsys):
    config,trace,calls=native
    assert worker.tick(runner,config,consolidate_now=True)==0
    records=[json.loads(row) for row in trace.read_text().splitlines()]
    assert len({row['pid'] for row in records})==1
    assert [row['type'] for row in records]==['prompt']
    assert any(path=='/work/renew' for path,_ in calls)
    assert not any(path=='/work/release' for path,_ in calls)
    output=capsys.readouterr().out
    assert '"completed_jobs":2' in output
    assert '"model_calls":1' in output
    assert 'do not print' not in output and 'secret' not in output


def test_unexpected_native_tool_blocks_prompt_and_all_evidence(native,monkeypatch):
    config,trace,calls=native
    monkeypatch.setenv('EXTRA_TOOL','1')
    with pytest.raises(runner.RunnerError,match='tool catalog'):
        worker.tick(runner,config,consolidate_now=True)
    assert not trace.exists()
    assert [path for path,_ in calls]==['/work/pool']


def test_per_unit_timeout_kills_child_and_releases_only_unfinished_work(native,monkeypatch):
    config,trace,calls=native
    config['deadline_seconds']=.3
    monkeypatch.setenv('HANG','1')
    began=time.monotonic()
    with pytest.raises(runner.RunnerError,match='deadline'):
        worker.tick(runner,config,consolidate_now=True)
    assert time.monotonic()-began<3
    released=[command for path,command in calls if path=='/work/release']
    assert released==[dict(id='first',owner='lease-a',error='cli_failed')]
    pid=json.loads(trace.read_text().splitlines()[0])['pid']
    with pytest.raises(ProcessLookupError):os.kill(pid,0)


def test_failed_pool_reports_safe_cause_without_running_native(monkeypatch,capsys):
    class Service:
        def __init__(self,*args,**kwargs):pass
        def post(self,*args,**kwargs):
            try:raise TimeoutError('private details')
            except TimeoutError as cause:raise runner.RunnerError('service_rejected') from cause
    monkeypatch.setattr(runner,'Service',Service)
    with pytest.raises(runner.RunnerError):worker.tick(runner,dict(mode='server',url='http://127.0.0.1:1'))
    output=capsys.readouterr().out
    assert '"stage":"pool"' in output and '"cause":"timeout"' in output
    assert 'private details' not in output


def test_native_retry_is_allowed_to_settle_before_host_lifecycle_decisions(native,monkeypatch):
    config,trace,calls=native
    monkeypatch.setenv('RETRY','1')
    assert worker.tick(runner,config,consolidate_now=True)==0
    assert not any(path=='/work/release' for path,_ in calls)


def test_committed_boundary_rotates_session_in_same_native_process(native,monkeypatch):
    config,trace,calls=native
    monkeypatch.setenv('BOUNDARY','1')
    assert worker.tick(runner,config,consolidate_now=True)==0
    records=[json.loads(row) for row in trace.read_text().splitlines()]
    assert len({row['pid'] for row in records})==1
    assert [row['type'] for row in records]==['prompt','abort','new_session','prompt']
