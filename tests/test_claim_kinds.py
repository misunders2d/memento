"""An optional claim kinds filter serves one free lane without changing scheduling or fences."""

from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from memento.api import create_app
from memento.graph import Graph
from memento.processing import Processing
from memento.store import Store

SOURCE = 'native:synthetic-device:claude'
NIGHT = datetime(2026, 10, 24, 3, tzinfo=timezone.utc)
DAY = datetime(2026, 10, 24, 12, tzinfo=timezone.utc)


def seeded(tmp_path):
    """One pending consolidation candidate plus one extractable conversation."""
    store = Store(tmp_path)
    processing = Processing(Graph(store))
    store.capture(dict(source=SOURCE, event_id='fact', conversation_id='done', kind='user',
                       text='The approval policy requires explicit operator consent.', occurred_at=None, metadata={}))
    [work] = processing.claim(agent='fixture', timezone_name='UTC', now=DAY,
                              compaction=dict(source=SOURCE, conversation_id='done'))['work']
    draft = dict(key='policy', kind='memory', type='knowledge', text='The approval policy requires explicit operator consent.',
        quote='The approval policy requires explicit operator consent.', why='Exact assertion', occurrence=None,
        src_key=None, dst_key=None, valid_from=None, valid_to=None, scope=['professional'], open_loop=False, confidence=.8)
    processing.complete(id=work['id'], owner=work['owner'], agent='fixture',
                        output=dict(windows=[dict(id='w0', candidates=[draft])]))
    store.capture(dict(source=SOURCE, event_id='next', conversation_id='next', kind='user',
                       text='A second conversation waits for extraction.', occurred_at=None, metadata={}))
    processing.compact(SOURCE, 'next')
    return store, processing


def kinds_of(store):
    return sorted(row[0] for row in store._db.execute("SELECT kind FROM jobs WHERE state='leased'"))


@pytest.mark.parametrize('kinds,expected', [(None, ['consolidate', 'extract']), (['extract'], ['extract']),
                                            (['consolidate'], ['consolidate'])])
def test_claim_serves_only_requested_kinds_and_leases_nothing_else(tmp_path, kinds, expected):
    store, processing = seeded(tmp_path)
    try:
        work = processing.claim(agent='fixture', timezone_name='UTC', now=NIGHT, kinds=kinds)['work']
        assert sorted(item['kind'] for item in work) == expected
        assert kinds_of(store) == expected
    finally:
        store.close()


def test_kinds_filter_keeps_night_window_force_and_single_consolidator_fence(tmp_path):
    store, processing = seeded(tmp_path)
    try:
        # Daytime: consolidation is not due, with or without the filter; force still bypasses the window.
        assert processing.claim(agent='fixture', timezone_name='UTC', now=DAY, kinds=['consolidate'])['work'] == []
        [forced] = processing.claim(agent='fixture', timezone_name='UTC', now=DAY, kinds=['consolidate'],
                                    consolidate_now=True)['work']
        assert forced['kind'] == 'consolidate'
        # A live consolidation lease blocks another one even when asked for explicitly.
        assert processing.claim(agent='fixture', timezone_name='UTC', now=NIGHT, kinds=['consolidate'])['work'] == []
        [extract] = processing.claim(agent='fixture', timezone_name='UTC', now=NIGHT, kinds=['extract'])['work']
        assert extract['kind'] == 'extract' and kinds_of(store) == ['consolidate', 'extract']
    finally:
        store.close()


@pytest.mark.parametrize('kinds,status', [(None, 200), (['extract'], 200), (['extract', 'consolidate'], 200),
                                          ([], 422), (['media'], 422), (['extract', 'extract'], 422)])
def test_claim_input_accepts_omitted_and_distinct_known_kinds_only(tmp_path, monkeypatch, kinds, status):
    monkeypatch.setenv('MEMENTO_EMBEDDINGS', '0')
    with TestClient(create_app(tmp_path)) as client:
        request = dict(agent='fixture', timezone='UTC')
        if kinds is not None:
            request['kinds'] = kinds
        response = client.post('/work/claim', json=request)
        assert response.status_code == status
        if status == 200:
            assert response.json()['work'] == [] and 'backlog' in response.json()
