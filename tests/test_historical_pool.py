"""One explicit cutover parks the current backlog; new history keeps its full pipeline."""

import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from memento.graph import Graph
from memento.jobs import LostLease
from memento.portable import export_archive, import_archive
from memento.processing import PARKED_ERROR, Processing
from memento.schema import VERSION
from memento.status import operational_status
from memento.store import Store

# Every capture is idle by this instant, and the hour is outside the night window.
NOW = datetime(2030, 1, 1, 12, tzinfo=timezone.utc)
ARGS = dict(agent='fixture', timezone_name='UTC', now=NOW)
SOURCE = 'history-fixture'


def capture(store, conversation, event, text, *, occurred_at=None):
    episode, _ = store.capture(dict(source=SOURCE, event_id=event, conversation_id=conversation, kind='user',
                                    text=text, occurred_at=occurred_at, metadata={}))
    return episode


def extract_all(processing, *, candidates=False):
    """Drain extraction; return every window text the model would have seen."""
    texts = []
    while True:
        work = processing.claim(**ARGS, kinds=['extract'])['work']
        if not work:
            return texts
        for task in work:
            windows = []
            for window in task['input']['windows']:
                texts.append(window['text'])
                items = [dict(key='k', kind='memory', type='knowledge', text=window['text'], quote=window['text'],
                              why='Explicit durable statement', occurrence=None, src_key=None, dst_key=None,
                              valid_from=None, valid_to=None, scope=['professional'], open_loop=False, confidence=.8)
                         ] if candidates else []
                windows.append(dict(id=window['id'], candidates=items))
            processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(windows=windows))


def progress(store, conversation):
    return dict(store._db.execute('SELECT * FROM processing_progress WHERE conversation_id=?', (conversation,)).fetchone())


def parking(store, conversation):
    row = store._db.execute('SELECT * FROM historical_parking WHERE conversation_id=?', (conversation,)).fetchone()
    return dict(row) if row else None


def job(store, job_id):
    return dict(store._db.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone())


def status(store):
    state = SimpleNamespace(store=store, retrieval=None, cold_runtime=None, cold=None, drive_reauthorization_required=False)
    return operational_status(state)


def header(path):
    with closing(sqlite3.connect(path)) as db:
        return db.execute('PRAGMA user_version').fetchone()[0]


def header_two_reader(path):
    """The exact header guard of images that know only headers 1 and 2."""
    version = header(path)
    if version not in (1, 2):
        raise RuntimeError(f'Unsupported database schema version: {version}')
    return version


def test_park_freezes_current_backlog_and_new_history_keeps_processing(tmp_path):
    store = Store(tmp_path / 'data')
    try:
        processing = Processing(Graph(store))
        capture(store, 'old', 'o1', 'Old prefix statement one.')
        capture(store, 'old', 'o2', 'Old prefix statement two.')
        assert len(extract_all(processing)) == 2
        for number in range(3):
            capture(store, 'old', f'b{number}', f'Old backlog statement {number}.')
        for number in range(2):
            capture(store, 'junk', f'j{number}', f'Junk backlog statement {number}.')
        before = progress(store, 'old')
        assert before['seq'] < before['latest_seq']

        state = processing.park_history(now=NOW)
        assert state['active'] and state['parked_conversations'] == 2 and state['header'] == VERSION
        assert state['live_pending_conversations'] == 0 and state['parked_candidates'] == 0
        parked = parking(store, 'old')
        assert parked['parked_through'] == parked['live_seq'] == before['latest_seq'] and parked['live_offset'] == 0
        # The ordinary cursor is untouched: seq is still exactly the extracted prefix.
        assert progress(store, 'old') == before
        assert processing.claim(**ARGS)['work'] == [] and processing.claim(**ARGS)['backlog']['pending_extract_conversations'] == 0
        report = status(store)
        assert report['extraction']['pending_conversations'] == 0 and report['extraction']['parked_conversations'] == 2
        old_row = next(row for row in report['extraction']['progress'] if row['conversation_id'] == 'old')
        assert old_row['cursor'] == before['seq'] and old_row['parked_through'] == before['latest_seq']
        assert old_row['live_cursor'] == before['latest_seq'] and old_row['live_offset'] == 0
        assert report['historical']['parked_conversations'] == 2 and report['historical']['active']

        # New history: a delayed record with an old original date in a parked
        # conversation, and a fresh conversation. Both get ordinary extraction.
        late = capture(store, 'old', 'late', 'Delayed offline record with an old original date.',
                       occurred_at='2019-01-01T00:00:00+00:00')
        fresh = capture(store, 'fresh', 'f1', 'Fresh conversation statement.')
        assert status(store)['extraction']['pending_conversations'] == 2
        assert set(extract_all(processing)) == {late['text'], fresh['text']}
        assert progress(store, 'old')['seq'] == before['seq'], 'live work never claims the parked prefix'
        assert parking(store, 'old')['live_seq'] == progress(store, 'old')['latest_seq']
        assert progress(store, 'fresh')['seq'] == progress(store, 'fresh')['latest_seq']
        assert parking(store, 'fresh') is None
        report = status(store)
        assert report['extraction']['pending_conversations'] == 0
        fresh_row = next(row for row in report['extraction']['progress'] if row['conversation_id'] == 'fresh')
        assert set(fresh_row) == {'source', 'conversation_id', 'cursor', 'offset', 'compact_through', 'latest_seq'}

        # Repeating the cutover is a no-op: the boundary never extends to newer captures.
        capture(store, 'old', 'after', 'Captured after activation, still live.')
        again = processing.park_history(now=NOW)
        assert again['activated_at'] == state['activated_at'] and again['parked_conversations'] == 2
        assert parking(store, 'old')['parked_through'] == before['latest_seq']
        assert status(store)['extraction']['pending_conversations'] == 1
    finally:
        store.close()
    reopened = Store(tmp_path / 'data')
    try:
        assert reopened._db.execute('PRAGMA user_version').fetchone()[0] == VERSION
        assert parking(reopened, 'old')['parked_through'] == before['latest_seq']
        assert Processing(Graph(reopened)).claim(**ARGS)['work'][0]['input']['windows'][0]['text'] == \
            'Captured after activation, still live.'
    finally:
        reopened.close()


def test_park_fences_pending_and_leased_extraction_inside_parked_ranges(tmp_path):
    store = Store(tmp_path / 'data')
    try:
        processing = Processing(Graph(store))
        capture(store, 'a', 'a1', 'Conversation a statement.')
        capture(store, 'b', 'b1', 'Conversation b statement.')
        leased = processing.claim(**ARGS)['work'][0]
        released = processing.claim(**ARGS)['work'][0]
        assert {leased['kind'], released['kind']} == {'extract'} and leased['id'] != released['id']
        assert processing.release(id=released['id'], owner=released['owner'], error='cli_failed') == dict(released=True)
        assert job(store, released['id'])['state'] == 'pending' and job(store, leased['id'])['state'] == 'leased'

        processing.park_history(now=NOW)
        for item in (leased, released):
            fenced = job(store, item['id'])
            assert fenced['state'] == 'failed' and fenced['error'] == PARKED_ERROR
            assert fenced['owner'] is None and fenced['lease_until'] is None
        # A stale completion cannot bypass the cutover.
        with pytest.raises(LostLease):
            processing.complete(id=leased['id'], owner=leased['owner'], agent='fixture',
                                output=dict(windows=[dict(id=window['id'], candidates=[])
                                                     for window in leased['input']['windows']]))
        assert processing.renew(id=leased['id'], owner=leased['owner']) == dict(renewed=False)
        assert processing.claim(**ARGS)['work'] == []
        assert progress(store, 'a')['seq'] == 0 and progress(store, 'b')['seq'] == 0
        assert store._db.execute('SELECT count(*) FROM extraction_runs').fetchone()[0] == 0
        extraction = status(store)['extraction']
        assert extraction['parked_jobs'] == 2 and extraction['failed_jobs'] == 0 and extraction['errors'] == []
        assert status(store)['historical']['scope'] == 'global'

        # Live work on the same conversation is admitted and completes normally.
        capture(store, 'a', 'a2', 'Conversation a continues after the cutover.')
        assert extract_all(processing) == ['Conversation a continues after the cutover.']
        assert progress(store, 'a')['seq'] == 0 and parking(store, 'a')['live_seq'] == progress(store, 'a')['latest_seq']
    finally:
        store.close()


def test_park_parks_every_active_candidate_and_fences_consolidation(tmp_path):
    store = Store(tmp_path / 'data')
    try:
        processing = Processing(Graph(store))
        for number in range(3):
            capture(store, 'done', f'd{number}', f'Fully extracted statement {number}.')
        assert len(extract_all(processing, candidates=True)) == 3
        done = progress(store, 'done')
        assert done['seq'] == done['latest_seq'], 'no extraction backlog remains in this conversation'
        forced = processing.claim(**ARGS, consolidate_now=True)['work'][0]
        assert forced['kind'] == 'consolidate' and len(forced['input']['candidates']) == 3

        state = processing.park_history(now=NOW)
        assert state['parked_candidates'] == 3 and state['parked_conversations'] == 0
        assert parking(store, 'done') is None, 'a fully extracted conversation needs no parking row'
        fenced = job(store, forced['id'])
        assert fenced['state'] == 'failed' and fenced['error'] == PARKED_ERROR
        rejections = [dict(candidate_id=candidate['id'], operation='reject', type=None, text=None, scope=None,
                           open_loop=None, target=None, src=None, dst=None, relation=None, change_kind=None,
                           valid_at=None, valid_from=None, valid_to=None, category=None, sensitive=None,
                           evidence=[], why='stale') for candidate in forced['input']['candidates']]
        with pytest.raises(LostLease):
            processing.complete(id=forced['id'], owner=forced['owner'], agent='fixture', output=dict(actions=rejections))
        assert store._db.execute('SELECT count(*) FROM candidate_decisions').fetchone()[0] == 0
        assert processing.consolidator.pending() == []
        assert processing.claim(**ARGS, consolidate_now=True)['work'] == []
        report = status(store)
        assert report['consolidation']['pending_candidates'] == 0 and report['consolidation']['ready_candidates'] == 0
        assert report['consolidation']['parked_candidates'] == 3 and report['consolidation']['excluded_legacy_candidates'] == 0
        assert report['historical']['parked_candidates'] == 3 and report['historical']['released_candidates'] == 0
        # Parked candidates stay stored and searchable; only admission excludes them.
        assert store._db.execute('SELECT count(*) FROM candidates').fetchone()[0] == 3

        # Candidates from history captured after the cutover are ordinary work.
        capture(store, 'done', 'd3', 'Statement captured after the cutover.')
        assert extract_all(processing, candidates=True) == ['Statement captured after the cutover.']
        [pending] = processing.consolidator.pending()
        assert pending['text'] == 'Statement captured after the cutover.'
        batch = processing.claim(**ARGS, consolidate_now=True)['work'][0]
        assert [candidate['text'] for candidate in batch['input']['candidates']] == ['Statement captured after the cutover.']
        assert status(store)['consolidation']['parked_candidates'] == 3
        assert processing.park_history(now=NOW)['parked_candidates'] == 3, 'repeat parking is a no-op'
    finally:
        store.close()


def test_activation_header_survives_restart_and_restore_and_header_two_readers_refuse_it(tmp_path):
    data = tmp_path / 'data'
    store = Store(data)
    try:
        processing = Processing(Graph(store))
        capture(store, 'old', 'o1', 'Unprocessed historical statement.')
        assert header(data / 'memory.sqlite') == VERSION
        with pytest.raises(RuntimeError, match=f'Unsupported database schema version: {VERSION}'):
            header_two_reader(data / 'memory.sqlite')
        processing.park_history(now=NOW)
        rows = store._db.execute('SELECT * FROM historical_parking').fetchall()
        assert [tuple(row) for row in rows] == [(SOURCE, 'old', 1, 1, 0)]
    finally:
        store.close()
    path = data / 'memory.sqlite'
    assert header(path) == VERSION
    with pytest.raises(RuntimeError, match=f'Unsupported database schema version: {VERSION}'):
        header_two_reader(path)
    reopened = Store(data)
    try:
        assert reopened._db.execute('PRAGMA user_version').fetchone()[0] == VERSION, 'startup never lowers the header'
        assert Processing(Graph(reopened)).claim(**ARGS)['work'] == []
    finally:
        reopened.close()
    archive = tmp_path / 'portable.tar'
    export_archive(data, archive)
    destination = tmp_path / 'restored'
    result = import_archive(archive, destination)
    assert 'user_version' not in result['rebased']
    assert header(destination / 'memory.sqlite') == VERSION
    with pytest.raises(RuntimeError, match='Unsupported database schema version'):
        header_two_reader(destination / 'memory.sqlite')
    restored = Store(destination)
    try:
        assert [tuple(row) for row in restored._db.execute('SELECT * FROM historical_parking')] == [(SOURCE, 'old', 1, 1, 0)]
        assert restored._db.execute('SELECT count(*) FROM historical_activation').fetchone()[0] == 1
        processing = Processing(Graph(restored))
        assert processing.claim(**ARGS)['work'] == [], 'the parking policy travels with the database'
        assert processing.history_status()['active'] and processing.history_status()['header'] == VERSION
    finally:
        restored.close()
