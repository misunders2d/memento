"""Agent-neutral pool access and the existing checked processing authority."""
import json
from datetime import timedelta

import pytest

from memento.consolidation import coalesce_windows
from memento.processing import processing_view
from test_consolidation_scheduling import ARGS, NIGHT, claim, reject_all, seeded


def test_full_pool_is_paginated_metadata_and_policies_are_shared(tmp_path):
    store, processing = seeded(tmp_path, count=40)
    after, found = 0, []
    while True:
        page = processing.work(action='pool', command=dict(kind='consolidate',after=after,limit=7,policies=True))
        assert page['ready_candidates']==40
        assert page['policies']['consolidate']['schema']['properties']['actions']['maxItems']==128
        assert all('text' not in item and 'quote' not in item for item in page['items'])
        found.extend(item['id'] for item in page['items'])
        if page['next'] is None:
            break
        after = page['next']
    assert len(found)==len(set(found))==40
    with pytest.raises(ValueError):
        processing.work(action='pool',command=dict(kind='consolidate',limit=101))
    with pytest.raises(ValueError):
        processing.work(action='pool',command=dict(kind='consolidate',path='/private'))
    store.close()


def test_larger_related_batch_finishes_all_with_raw_evidence_preserved(tmp_path):
    store, processing = seeded(tmp_path,count=40)
    raw = store._db.execute('SELECT text FROM episodes').fetchall()
    task = processing.claim(**ARGS,now=NIGHT,continuous=True,batch_size=64,kinds=['consolidate'])['work'][0]
    assert len(task['input']['candidates'])==40
    reject_all(processing,task)
    status=processing.work(action='status',command={'id':task['id']})
    assert status['state']=='done'
    assert store._db.execute('SELECT text FROM episodes').fetchall()==raw
    assert processing.pool(kind='consolidate')['ready_candidates']==0
    assert store._db.execute('SELECT count(*) FROM candidate_decisions').fetchone()[0]==40
    store.close()


def test_released_oversized_batch_can_be_repartitioned_without_losing_its_payload(tmp_path):
    store,processing=seeded(tmp_path,count=40)
    old=processing.claim(**ARGS,now=NIGHT,continuous=True,batch_size=64,kinds=['consolidate'])['work'][0]
    processing.release(id=old['id'],owner=old['owner'],error='service_rejected')
    small=processing.claim(**ARGS,now=NIGHT,continuous=True,batch_size=1,kinds=['consolidate'],exclude=[old['id']])['work'][0]
    assert small['id']!=old['id'] and len(small['input']['candidates'])==1
    retained=dict(store._db.execute('SELECT * FROM jobs WHERE id=?',(old['id'],)).fetchone())
    assert retained['state']=='failed' and retained['error']=='rebatch_requested'
    assert len(json.loads(retained['payload'])['candidate_ids'])==40
    assert store._db.execute('SELECT count(*) FROM candidate_decisions').fetchone()[0]==0
    assert processing.pool(kind='consolidate')['ready_candidates']==40
    store.close()


def test_rebatching_recovers_an_accepted_commit_instead_of_replanning_it(tmp_path):
    store,processing=seeded(tmp_path,count=40)
    old=processing.claim(**ARGS,now=NIGHT,continuous=True,batch_size=64,kinds=['consolidate'])['work'][0]
    reject_all(processing,old)
    # Simulate the durable graph commit preceding a lost queue-completion acknowledgement.
    store._db.execute("UPDATE jobs SET state='pending',result=NULL,owner=NULL,lease_until=NULL WHERE id=?",(old['id'],))
    assert processing.claim(**ARGS,now=NIGHT,continuous=True,batch_size=1,kinds=['consolidate'])['work']==[]
    assert processing.queue.get(old['id'])['state']=='done'
    assert store._db.execute('SELECT count(*) FROM candidate_decisions').fetchone()[0]==40
    store.close()


def test_mcp_pages_reconstruct_complete_work_and_cannot_read_foreign_or_forgotten_lease(tmp_path):
    from copy import deepcopy
    store,processing=seeded(tmp_path,count=40)
    task=processing.claim(**ARGS,now=NIGHT,continuous=True,batch_size=64,kinds=['consolidate'])['work'][0]
    descriptor=processing_view(dict(work=[deepcopy(task)]))['work'][0]
    assert 'input' not in descriptor and descriptor['policy']=={'policy':'consolidate'}
    after,parts=0,[]
    while True:
        page=processing.work(action='read',command=dict(descriptor['read'],after=after))
        assert len(json.dumps(page,ensure_ascii=False).encode())<16000
        parts.append(page['text'])
        if page['next'] is None:break
        after=page['next']
    payload=json.loads(''.join(parts))
    assert payload['input']==task['input']
    assert 'instruction' not in payload  # Read/cache each kind's policy separately.
    with pytest.raises(Exception):
        processing.work(action='read',command=dict(id=task['id'],owner='foreign'))
    store._db.execute("INSERT INTO forgotten_sources VALUES ('schedule-fixture','facts','synthetic-forgotten','synthetic-command')")
    with pytest.raises(Exception,match='source changed'):
        processing.work(action='read',command=descriptor['read'])
    store.close()


def test_mcp_policy_pages_are_complete_even_above_native_output_limit(tmp_path):
    store,processing=seeded(tmp_path,count=1)
    after,parts=0,[]
    while True:
        page=processing.work(action='read',command=dict(policy='consolidate',after=after))
        assert len(json.dumps(page,ensure_ascii=False).encode())<16000
        parts.append(page['text'])
        if page['next'] is None:break
        after=page['next']
    policy=json.loads(''.join(parts))
    assert policy==processing.pool(kind='consolidate',policies=True)['policies']['consolidate']
    store.close()


def test_continuous_admission_includes_new_arrivals_but_obeys_night_window(tmp_path):
    store, processing = seeded(tmp_path,count=1)
    first=claim(processing)[0]
    reject_all(processing,first)
    text='The orbital laboratory uses a copper spectrometer.'
    store.capture(dict(source='schedule-fixture',event_id='late',conversation_id='c',kind='user',
                       text=text,occurred_at=None,metadata={}))
    extract=processing.claim(**ARGS,now=NIGHT,kinds=['extract'],
        compaction=dict(source='schedule-fixture',conversation_id='c'))['work'][0]
    draft=dict(key='late',kind='memory',type='knowledge',text=text,quote=text,
        why='Explicit durable source assertion',occurrence=None,src_key=None,dst_key=None,
        valid_from=None,valid_to=None,scope=['professional'],open_loop=False,confidence=.8)
    processing.complete(id=extract['id'],owner=extract['owner'],agent='fixture',
        output=dict(windows=[dict(id=window['id'],candidates=[draft]) for window in extract['input']['windows']]))
    assert processing.claim(**ARGS,now=NIGHT+timedelta(hours=12),continuous=True,
                            kinds=['consolidate'])['work']==[]
    work=processing.claim(**ARGS,now=NIGHT,continuous=True,kinds=['consolidate'])['work'][0]
    assert any(item['text']==text for item in work['input']['candidates'])
    store.close()


def test_submit_preserves_committed_receipt_when_next_claim_fails(tmp_path):
    from memento.store import Store
    from memento.graph import Graph
    from memento.processing import Processing
    store=Store(tmp_path)
    processing=Processing(Graph(store))
    store.capture(dict(source='fresh',event_id='e',conversation_id='c',kind='user',
                       text='Bounded source.',occurred_at=None,metadata={}))
    task=processing.claim(**ARGS,now=NIGHT,kinds=['extract'],
        compaction=dict(source='fresh',conversation_id='c'))['work'][0]
    output=dict(windows=[dict(id=item['id'],candidates=[]) for item in task['input']['windows']])
    complete=dict(id=task['id'],owner=task['owner'],agent='fixture',output=output)
    receipt=processing.work(action='submit',command=dict(complete=complete,
        next=dict(agent='fixture',timezone='No/Such_Zone',kinds=['extract'])))
    assert receipt['completed']==task['id']
    assert receipt['next_error']=='next_claim_failed'
    assert processing.work(action='status',command={'id':task['id']})['state']=='done'
    commits=store._db.execute('SELECT count(*) FROM graph_commits').fetchone()[0]
    with pytest.raises(Exception):
        processing.work(action='complete',command=complete)
    assert store._db.execute('SELECT count(*) FROM graph_commits').fetchone()[0]==commits
    store.close()


def test_evidence_union_removes_repetition_without_adding_characters():
    raw='🧠abc'*400
    windows=[]
    for n in range(16):
        quote_start=100+30*n
        start,end=max(0,quote_start-400),quote_start+50+400
        windows.append(dict(episode_id='e',start=start,end=end,text=raw[start:end],authority='original'))
    original=sum(len(w['text']) for w in windows)
    merged=coalesce_windows(windows)
    assert original==11950
    assert len(merged)==1 and len(merged[0]['text'])==1000
    before={index for w in windows for index in range(w['start'],w['end'])}
    after={index for w in merged for index in range(w['start'],w['end'])}
    assert before==after
    assert merged[0]['text']==raw[:1000]


@pytest.mark.parametrize('second,expected',[
    (dict(start=2,end=5,text='бcd',authority=True),1),
    (dict(start=1,end=2,text='🧠',authority=True),1),
    (dict(start=4,end=5,text='d',authority=True),2),
    (dict(start=2,end=5,text='бcd',authority=False),2),
])
def test_union_preserves_unicode_gaps_and_authority(second,expected):
    first=dict(episode_id='e',start=0,end=3,text='a🧠б',authority=True)
    assert len(coalesce_windows([first,dict(episode_id='e',**second)]))==expected
