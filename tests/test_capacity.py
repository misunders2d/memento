"""Capacity summary arithmetic over synthetic snapshots and receipts."""
import json

from memento import capacity


def status(pending, ready, conversations, extracted, consolidated, indexed, unindexed):
    jobs = lambda done: dict(completed_jobs=done, failed_jobs=1)
    return dict(extraction=dict(jobs(extracted), pending_conversations=conversations),
                consolidation=dict(jobs(consolidated), pending_candidates=pending, ready_candidates=ready),
                indexing=dict(available=True, indexed=indexed, pending=unindexed, failed=0))


def test_net_drain_indexing_and_receipts_over_a_half_hour():
    before = dict(at='2026-10-29T15:00:00+00:00', status=status(1000, 900, 12, 40, 10, 5000, 300))
    after = dict(at='2026-10-29T15:30:00+00:00', status=dict(service=status(940, 850, 10, 48, 18, 5600, 100)))
    commit = lambda kind: json.dumps(dict(stage='worker_committed', id='x', kind=kind, completed_jobs=1))
    lines = [
        'Oct 29 runner[1]: ' + commit('consolidate'),
        commit('extract'),
        json.dumps(dict(MESSAGE=commit('extract'), __REALTIME_TIMESTAMP='1')),
        json.dumps(dict(stage='worker_model', model='m', usage=dict(input=100, output=7, secret=9), stop_reason='toolUse')),
        json.dumps(dict(stage='worker', error='processing_request_failed', detail_code='candidate_quote_ambiguous',
                        action='submit', http_status=422)),
        json.dumps(dict(stage='worker', error='processing_request_failed', cause='empty_kind', action='claim')),
        json.dumps(dict(stage='worker_done', completed_jobs=3, failed_attempts=2)),
        'not a receipt {broken', 'private model text without json',
    ]
    result = capacity.summarize(before, after, lines)
    assert result['hours'] == 0.5
    assert result['consolidation']['pending_candidates'] == dict(after=940, net_drain=60, net_drain_per_hour=120.0, eta_hours=7.83)
    assert result['consolidation']['completed_jobs_per_hour'] == 16.0
    assert result['extraction']['net_conversation_drain'] == 2
    assert result['indexing'] == dict(available=True, indexed=600, indexed_per_hour=1200.0, pending=100,
                                      net_pending_drain=200, eta_hours=0.25, failed=0)
    assert result['receipts'] == dict(
        commits=dict(consolidate=1, extract=2), commits_per_hour=6.0, model_calls=1,
        tokens=dict(input=100, output=7), worker_done=1,
        failed_attempts={'worker|processing_request_failed|candidate_quote_ambiguous|submit|422': 1,
                         'worker|processing_request_failed|empty_kind|claim|': 1})


def test_growing_backlog_has_no_eta():
    before = dict(at='2026-10-29T15:00:00+00:00', status=status(100, 100, 1, 0, 0, 0, 0))
    after = dict(at='2026-10-29T16:00:00+00:00', status=status(150, 150, 1, 0, 0, 0, 0))
    assert capacity.summarize(before, after)['consolidation']['ready_candidates']['eta_hours'] is None
