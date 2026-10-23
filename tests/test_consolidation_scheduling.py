"""Consolidation admission around failed batches: existing jobs, reservations and leases only."""

import json
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

from memento.graph import Graph
from memento.processing import Processing
from memento.store import Store


NIGHT = datetime(2026, 10, 18, 3, tzinfo=timezone.utc)
NIGHT_ID = 'UTC/2026-10-18'
ARGS = dict(agent='fixture', timezone_name='UTC')


def seeded(path, count=40):
    """Extract `count` undecided candidates from one captured user message."""
    store = Store(path)
    processing = Processing(Graph(store))
    facts = [f'Fact {number} is stable.' for number in range(count)]
    store.capture(dict(source='schedule-fixture', event_id='facts', conversation_id='c', kind='user',
                       text=' '.join(facts), occurred_at=None, metadata={}))
    task = processing.claim(**ARGS, now=NIGHT - timedelta(hours=12),
                            compaction=dict(source='schedule-fixture', conversation_id='c'))['work'][0]
    candidates = [dict(key=f'fact-{number}', kind='memory', type='knowledge', text=fact, quote=fact,
        why='Explicit durable source assertion', occurrence=None, src_key=None, dst_key=None,
        valid_from=None, valid_to=None, scope=['professional'], open_loop=False, confidence=.8)
        for number, fact in enumerate(facts)]
    processing.complete(id=task['id'], owner=task['owner'], agent='fixture',
        output=dict(windows=[dict(id=task['input']['windows'][0]['id'], candidates=candidates)]))
    assert store._db.execute('SELECT count(*) FROM candidates').fetchone()[0] == count
    return store, processing


def claim(processing, *, now=NIGHT, exclude=(), force=False):
    work = processing.claim(**ARGS, now=now, exclude=list(exclude), consolidate_now=force)['work']
    assert all(item['kind'] == 'consolidate' for item in work)
    return work


def reject_all(processing, task):
    actions = [dict(candidate_id=candidate['id'], operation='reject', type=None, text=None, scope=None,
        open_loop=None, target=None, src=None, dst=None, relation=None, change_kind=None, valid_at=None,
        valid_from=None, valid_to=None, category=None, sensitive=None, evidence=[], why='Synthetic batch rejected')
        for candidate in task['input']['candidates']]
    return processing.complete(id=task['id'], owner=task['owner'], agent='fixture', output=dict(actions=actions))


def job(store, job_id):
    row = dict(store._db.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone())
    row['payload'] = json.loads(row['payload'])
    return row


def ids(task):
    return {candidate['id'] for candidate in task['input']['candidates']}


def consolidate_jobs(store):
    return store._db.execute("SELECT count(*) FROM jobs WHERE kind='consolidate'").fetchone()[0]


def live_consolidators(store):
    return store._db.execute("SELECT count(*) FROM jobs WHERE kind='consolidate' AND state='leased' AND lease_until>?",
                             (time.time(),)).fetchone()[0]


def night(store, night_id=NIGHT_ID):
    row = store._db.execute('SELECT * FROM consolidation_nights WHERE id=?', (night_id,)).fetchone()
    return dict(row) if row else None


def test_excluded_failed_batch_lets_disjoint_batches_progress_then_reclaims_intact(tmp_path):
    store, processing = seeded(tmp_path)
    try:
        a, = claim(processing)
        assert len(ids(a)) == 16
        processing.release(id=a['id'], owner=a['owner'], error='service_rejected')
        before = job(store, a['id'])
        assert before['state'] == 'pending' and before['attempts'] == 1
        # The runner excludes A for fifteen minutes; a disjoint batch must still be admitted.
        b, = claim(processing, exclude=[a['id']])
        assert b['id'] != a['id'] and ids(b).isdisjoint(ids(a)) and len(ids(b)) == 16
        assert job(store, a['id']) == before
        assert reject_all(processing, b)['candidate_count'] == 16
        assert night(store)['cursor'] == job(store, b['id'])['payload']['next_cursor'] > 0
        c, = claim(processing, exclude=[a['id'], b['id']])
        assert ids(c).isdisjoint(ids(a)) and len(ids(c)) == 8
        reject_all(processing, c)
        cursor = night(store)['cursor']
        assert cursor == job(store, c['id'])['payload']['next_cursor']
        # Everything else is reserved by A: no new job, no finished night, no duplicate admission.
        for _ in range(3):
            assert claim(processing, exclude=[a['id'], b['id'], c['id']]) == []
        assert consolidate_jobs(store) == 3 and night(store)['finished_at'] is None
        assert job(store, a['id']) == before
        # Cooldown passes; A is reclaimed under its existing identity and payload.
        store._db.execute('UPDATE jobs SET available_at=0 WHERE id=?', (a['id'],))
        again, = claim(processing)
        assert again['id'] == a['id'] and again['owner'] != a['owner'] and ids(again) == ids(a)
        assert job(store, a['id'])['attempts'] == 2 and job(store, a['id'])['payload'] == before['payload']
        reject_all(processing, again)
        # An older batch finishing later never regresses the cursor.
        assert night(store)['cursor'] == cursor
        assert claim(processing) == []
        assert night(store)['finished_at'] is not None
        assert consolidate_jobs(store) == 3
    finally:
        store.close()


def test_live_consolidator_blocks_every_other_claimant_even_with_misleading_exclusions(tmp_path):
    store, processing = seeded(tmp_path)
    try:
        a, = claim(processing)
        processing.release(id=a['id'], owner=a['owner'], error='service_rejected')
        store._db.execute('UPDATE jobs SET available_at=0 WHERE id=?', (a['id'],))
        b, = claim(processing, exclude=[a['id']])
        assert live_consolidators(store) == 1
        # Another claimant without exclusions, or excluding only B, must not lease the ready A.
        assert claim(processing) == []
        assert claim(processing, exclude=[b['id']]) == []
        assert claim(processing, exclude=[b['id']], force=True) == []
        assert processing.queue.claim(['consolidate']) is None
        assert live_consolidators(store) == 1 and consolidate_jobs(store) == 2
        assert job(store, a['id'])['state'] == 'pending'
        # Once B's lease expires, exactly one consolidator is live again.
        store._db.execute('UPDATE jobs SET lease_until=0 WHERE id=?', (b['id'],))
        reclaimed, = claim(processing)
        assert reclaimed['id'] in {a['id'], b['id']}
        assert live_consolidators(store) == 1
        assert claim(processing) == []
    finally:
        store.close()


def test_concurrent_queue_claims_lease_one_consolidator_without_serializing_extraction(tmp_path):
    store = Store(tmp_path)
    try:
        processing = Processing(Graph(store))
        payload = dict(version='consolidate-v6', night=None, candidate_ids=[], next_cursor=0)
        for number in range(3):
            processing.queue._insert(f'consolidate/manual/{number}', 'consolidate', dict(payload, batch=number))
            processing.queue._insert(f'extract/{number}', 'extract', dict(version='extract', batch=number))
        barrier = threading.Barrier(6)
        results, lock = [], threading.Lock()

        def claimant(kind):
            barrier.wait(5)
            leased = processing.queue.claim([kind])
            with lock:
                results.append((kind, leased))

        threads = [threading.Thread(target=claimant, args=(kind,)) for kind in ('consolidate', 'extract') * 3]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
        assert not any(thread.is_alive() for thread in threads)
        consolidations = [leased for kind, leased in results if kind == 'consolidate' and leased is not None]
        extractions = [leased for kind, leased in results if kind == 'extract' and leased is not None]
        assert len(consolidations) == 1 and live_consolidators(store) == 1
        assert len({leased.id for leased in extractions}) == 3
        # Media/extraction leasing stays independent; a second consolidator waits for the lease to end.
        assert processing.queue.claim(['consolidate']) is None
        processing.queue.complete(consolidations[0], dict(candidate_count=0))
        assert processing.queue.claim(['consolidate']) is not None
    finally:
        store.close()


@pytest.mark.parametrize('lease', ['live', 'expired'])
def test_only_expired_or_pending_excluded_work_can_be_bypassed(tmp_path, lease):
    store, processing = seeded(tmp_path)
    try:
        a, = claim(processing)
        before = job(store, a['id'])
        if lease == 'expired':
            store._db.execute('UPDATE jobs SET lease_until=0 WHERE id=?', (a['id'],))
        work = claim(processing, exclude=[a['id']])
        if lease == 'live':
            assert work == [] and consolidate_jobs(store) == 1
        else:
            b, = work
            assert b['id'] != a['id'] and ids(b).isdisjoint(ids(a))
            expired = job(store, a['id'])
            assert expired['state'] == 'leased' and expired['payload'] == before['payload']
            assert expired['owner'] == before['owner'] and expired['attempts'] == before['attempts']
    finally:
        store.close()


def test_empty_exclude_keeps_sequential_admission_through_cooldown(tmp_path):
    store, processing = seeded(tmp_path)
    try:
        a, = claim(processing)
        processing.release(id=a['id'], owner=a['owner'], error='cli_failed')
        for _ in range(3):
            assert claim(processing) == []
        assert consolidate_jobs(store) == 1 and job(store, a['id'])['state'] == 'pending'
        assert night(store)['finished_at'] is None
    finally:
        store.close()


def test_forced_catch_up_skips_reserved_candidates_and_normal_close_keeps_them_eligible(tmp_path):
    store, processing = seeded(tmp_path)
    try:
        a, = claim(processing)
        processing.release(id=a['id'], owner=a['owner'], error='service_rejected')
        b, = claim(processing, exclude=[a['id']], force=True)
        manual = job(store, b['id'])
        assert manual['payload']['night'] is None and manual['job_key'].startswith('consolidate/manual/')
        assert ids(b).isdisjoint(ids(a)) and len(ids(b)) == 16
        reject_all(processing, b)
        assert night(store)['cursor'] == 0
        c, = claim(processing, exclude=[a['id'], b['id']], force=True)
        assert len(ids(c)) == 8 and ids(c).isdisjoint(ids(a))
        processing.release(id=c['id'], owner=c['owner'], error='service_rejected')
        # No unreserved candidate remains; forced catch-up does not loop or invent work.
        assert claim(processing, exclude=[a['id'], b['id'], c['id']], force=True) == []
        assert consolidate_jobs(store) == 3
        # Daytime sweep retires the night envelope only; the forced job stays durable.
        assert claim(processing, now=NIGHT.replace(hour=9)) == []
        assert job(store, a['id'])['state'] == 'failed' and job(store, a['id'])['error'] == 'night_window_closed'
        assert job(store, c['id'])['state'] == 'pending'
        # Next night: A's undecided candidates are eligible again; C's stay reserved.
        d, = claim(processing, now=NIGHT + timedelta(days=1), exclude=[c['id']])
        assert ids(d) == ids(a)
        assert job(store, d['id'])['payload']['night'] == 'UTC/2026-10-19'
    finally:
        store.close()


def test_all_candidates_reserved_by_excluded_batch_leaves_night_open_until_decided(tmp_path):
    store, processing = seeded(tmp_path, count=16)
    try:
        a, = claim(processing)
        processing.release(id=a['id'], owner=a['owner'], error='service_rejected')
        for _ in range(2):
            assert claim(processing, exclude=[a['id']]) == []
        assert consolidate_jobs(store) == 1 and night(store)['finished_at'] is None
        store._db.execute('UPDATE jobs SET available_at=0 WHERE id=?', (a['id'],))
        again, = claim(processing)
        assert again['id'] == a['id']
        reject_all(processing, again)
        assert claim(processing) == []
        assert night(store)['finished_at'] is not None
    finally:
        store.close()
