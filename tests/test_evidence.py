import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from memento.api import create_app
from memento.store import Store


@pytest.fixture
def client(tmp_path):
    with TestClient(create_app(tmp_path)) as client:
        yield client


def evidence(**changes):
    return dict(source='pi', event_id='turn-1', conversation_id='chat-1', kind='turn',
                text='  Позвони Ирине 🧠\r\nlaunch\x00 afterzero  ',
                occurred_at='2026-10-18T12:00:00Z',
                metadata={'tool': ['read', {'exit': 0}]}) | changes


def test_exact_capture_retry_and_conflict(client):
    payload = evidence()
    created = client.post('/episodes', json=payload)
    assert created.status_code == 201
    episode_id = created.json()['id']
    got = client.get(f'/episodes/{episode_id}').json()
    assert got['text'].encode() == payload['text'].encode()
    assert got['metadata'] == payload['metadata']
    assert got['occurred_at'] == '2026-10-18T12:00:00+00:00'
    again = client.post('/episodes', json=payload)
    assert (again.status_code, again.json()) == (200, {'id': episode_id})
    for field, replacement in [('text', 'corrected'), ('conversation_id', 'other'),
                               ('metadata', {'tool': 'changed'}), ('kind', 'email'),
                               ('occurred_at', '2026-10-19T12:00:00Z')]:
        conflict = client.post('/episodes', json=payload | {field: replacement})
        assert conflict.status_code == 409
        assert client.get(f'/episodes/{episode_id}').json() == got


def test_duplicate_capture_is_atomic_under_concurrency(client):
    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(lambda _: client.post('/episodes', json=evidence()), range(16)))
    assert sum(r.status_code == 201 for r in responses) == 1
    assert {r.status_code for r in responses} == {200, 201}
    assert len({r.json()['id'] for r in responses}) == 1
    assert len(client.get('/episodes/search', params={'q': 'Ирине'}).json()['episodes']) == 1


def test_search_literals_and_scope(client):
    first = client.post('/episodes', json=evidence()).json()['id']
    second = client.post('/episodes', json=evidence(source='mail')).json()['id']
    third = client.post('/episodes', json=evidence(event_id='turn-2', conversation_id='chat-2')).json()['id']
    assert {e['id'] for e in client.get('/episodes/search', params={'q': 'Ирине launch'}).json()['episodes']} == {first, second, third}
    scoped = client.get('/episodes/search', params={'q': 'launch', 'source': 'pi', 'conversation_id': 'chat-1'})
    assert [e['id'] for e in scoped.json()['episodes']] == [first]
    assert [e['id'] for e in client.get('/episodes/search', params={
        'q': 'Ирине -', 'source': 'pi', 'conversation_id': 'chat-1',
    }).json()['episodes']] == [first]
    assert client.get('/episodes/search', params={'q': 'unmentioned'}).json() == {'episodes': []}
    # Tokens after embedded NUL remain searchable; raw evidence itself is unchanged.
    assert {e['id'] for e in client.get('/episodes/search', params={'q': 'afterzero'}).json()['episodes']} == {first, second, third}
    client.post('/episodes', json=evidence(event_id='operators', text='AND NEAR OR hello'))
    literal = client.get('/episodes/search', params={'q': 'AND NEAR'})
    assert [e['event_id'] for e in literal.json()['episodes']] == ['operators']


@pytest.mark.parametrize('query', ['"', '-', '*', ':', '(', ')', 'AND', 'NEAR',
                                 '" - * : ( ) AND NEAR', 'hello" OR *', '   ', '\x00'])
def test_search_operator_input_is_not_executable(client, query):
    response = client.get('/episodes/search', params={'q': query})
    assert response.status_code in (200, 422)
    if response.status_code == 200:
        assert response.json() == {'episodes': []}


def test_immutability_including_replace_and_search_consistency(tmp_path):
    with TestClient(create_app(tmp_path)) as client:
        episode_id = client.post('/episodes', json=evidence()).json()['id']
        original = client.get(f'/episodes/{episode_id}').json()
        with sqlite3.connect(tmp_path / 'memory.sqlite') as db:
            for sql in ['UPDATE episodes SET text=\'rewritten\'', 'DELETE FROM episodes',
                        'INSERT OR REPLACE INTO episodes SELECT * FROM episodes',
                        'REPLACE INTO episodes SELECT * FROM episodes',
                        "INSERT INTO episodes SELECT * FROM episodes WHERE true ON CONFLICT(id) DO UPDATE SET text='changed'",
                        """INSERT OR REPLACE INTO episodes
                           SELECT seq, id || 'new', source || 'new', event_id, conversation_id,
                                  kind, text, occurred_at, metadata, captured_at, text_runs, text_sha256 FROM episodes"""]:
                with pytest.raises(sqlite3.IntegrityError, match='immutable'):
                    db.execute(sql)
                db.rollback()
        assert client.get(f'/episodes/{episode_id}').json() == original
        assert [e['id'] for e in client.get('/episodes/search', params={'q': 'Ирине'}).json()['episodes']] == [episode_id]


def test_reopen_preserves_evidence_and_single_writer(tmp_path):
    with TestClient(create_app(tmp_path)) as client:
        episode_id = client.post('/episodes', json=evidence()).json()['id']
        with pytest.raises(RuntimeError, match='Another memento writer'):
            Store(tmp_path)
    with TestClient(create_app(tmp_path)) as client:
        assert client.get(f'/episodes/{episode_id}').json()['text'] == evidence()['text']
        assert [e['id'] for e in client.get('/episodes/search', params={'q': 'launch'}).json()['episodes']] == [episode_id]
        assert client.get('/episodes/does-not-exist').status_code == 404


@pytest.mark.parametrize('kind', ['native_record', 'native_live_record', 'native_hook'])
def test_native_raw_bytes_stay_exact_without_duplicate_fts_index(client, kind):
    raw = '{"image":"opaqueuniquebinarytoken", "text":"source projection sentence"}'
    original = client.post('/episodes', json=evidence(event_id='opaque', kind=kind,
        text=raw, metadata={'extraction_mode': 'skip'})).json()['id']
    projected = client.post('/episodes', json=evidence(event_id='projection', kind='user',
        text='source projection sentence', metadata={})).json()['id']
    assert client.get('/episodes/' + original).json()['text'] == raw
    assert client.get('/episodes/search', params={'q': 'opaqueuniquebinarytoken'}).json()['episodes'] == []
    assert [item['id'] for item in client.get('/episodes/search', params={'q': 'projection sentence'}).json()['episodes']] == [projected]


def test_legacy_raw_fts_migration_is_transactional_and_restarts_without_redeleting(tmp_path):
    from memento.schema import upgrade_episode_fts
    store = Store(tmp_path)
    try:
        db = store._db
        db.execute('DROP TRIGGER episodes_text_index')
        db.execute('''CREATE TRIGGER episodes_index AFTER INSERT ON episodes BEGIN
            INSERT INTO episodes_fts(rowid,text) VALUES (NEW.seq,NEW.text); END''')
        raw, _ = store.capture(evidence(event_id='raw', kind='native_record',
            text='opaqueuniquebinarytoken', metadata={'extraction_mode': 'skip'}))
        text, _ = store.capture(evidence(event_id='text', kind='user', text='source projection sentence', metadata={}))
        assert [item['id'] for item in store.search('opaqueuniquebinarytoken')] == [raw['id']]
        db.execute('BEGIN IMMEDIATE')
        upgrade_episode_fts(db)
        assert store.search('opaqueuniquebinarytoken') == []
        db.execute('ROLLBACK')
        assert [item['id'] for item in store.search('opaqueuniquebinarytoken')] == [raw['id']]
    finally:
        store.close()
    for _ in range(2):
        reopened = Store(tmp_path)
        try:
            assert reopened.get(raw['id'])['text'] == 'opaqueuniquebinarytoken'
            assert reopened.search('opaqueuniquebinarytoken') == []
            assert [item['id'] for item in reopened.search('projection sentence')] == [text['id']]
            assert reopened._db.execute('PRAGMA user_version').fetchone()[0] == 1
        finally:
            reopened.close()


@pytest.mark.parametrize('change', [{'text': '\ud800'}, {'metadata': {'bad': '\ud800'}},
                                  {'occurred_at': '2026-10-18T12:00:00'}, {'text': 123},
                                  {'metadata': {'bad': float('inf')}}, {'unexpected': 'field'}])
def test_invalid_evidence_is_rejected_without_insertion(client, change):
    # Raw JSON encoding permits malformed Unicode/non-finite numbers to reach validation.
    import json
    response = client.post('/episodes', content=json.dumps(evidence() | change),
                           headers={'content-type': 'application/json'})
    assert response.status_code == 422
    assert client.get('/episodes/search', params={'q': 'launch'}).json() == {'episodes': []}


def test_index_failure_rolls_back_evidence(tmp_path):
    with TestClient(create_app(tmp_path)) as client:
        with sqlite3.connect(tmp_path / 'memory.sqlite') as db:
            db.execute("""CREATE TRIGGER reject_capture AFTER INSERT ON episodes
                          BEGIN SELECT RAISE(ABORT, 'simulated index failure'); END""")
        with pytest.raises(sqlite3.IntegrityError, match='simulated index failure'):
            client.post('/episodes', json=evidence())
        with sqlite3.connect(tmp_path / 'memory.sqlite') as db:
            assert db.execute('SELECT id FROM episodes').fetchall() == []
            assert db.execute("SELECT rowid FROM episodes_fts WHERE episodes_fts MATCH 'launch'").fetchall() == []
            db.execute('DROP TRIGGER reject_capture')
        assert client.post('/episodes', json=evidence()).status_code == 201


def test_oversized_body_rejected_before_reading(client):
    from memento.api import MAX_CAPTURE_BYTES

    def unread_body():
        raise AssertionError('Oversized declared body must not be read')
        yield b''

    response = client.post('/episodes', content=unread_body(), headers={
        'content-type': 'application/json', 'content-length': str(MAX_CAPTURE_BYTES + 1),
    })
    assert response.status_code == 413
    assert client.get('/health').status_code == 200


def test_chunked_body_cannot_bypass_limit(client):
    from memento.api import MAX_CAPTURE_BYTES
    chunks = (b'x' * 16384 for _ in range(MAX_CAPTURE_BYTES // 16384 + 1))
    response = client.post('/episodes', content=chunks,
                           headers={'content-type': 'application/json'})
    assert response.status_code == 413
    assert client.get('/episodes/search', params={'q': 'x'}).json() == {'episodes': []}


def test_search_results_are_bounded_but_get_is_exact(client):
    payload = evidence(text='needle ' + 'x' * 200000, metadata={'large': 'y' * 100000})
    ids = [client.post('/episodes', json=payload | {'event_id': str(i)}).json()['id']
           for i in range(3)]
    response = client.get('/episodes/search', params={'q': 'needle', 'limit': 100})
    hits = response.json()['episodes']
    assert {hit['id'] for hit in hits} == set(ids)
    assert all('needle' in hit['excerpt'] and len(hit['excerpt']) <= 512 for hit in hits)
    assert len(response.content) < 20000
    for episode_id in ids:
        exact = client.get(f'/episodes/{episode_id}').json()
        assert exact['text'] == payload['text']
        assert exact['metadata'] == payload['metadata']


def test_source_timestamp_offset_is_preserved(client):
    episode_id = client.post('/episodes', json=evidence(
        occurred_at='2026-10-18T15:00:00+03:00',
    )).json()['id']
    assert client.get(f'/episodes/{episode_id}').json()['occurred_at'] == '2026-10-18T15:00:00+03:00'


def test_recent_activity_without_query_uses_source_instants_and_bounded_raw_excerpts(client, monkeypatch):
    from datetime import datetime, timezone

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            instant = cls(2030, 1, 3, tzinfo=timezone.utc)
            return instant if tz is not None else instant.replace(tzinfo=None)

    monkeypatch.setattr('memento.store.datetime', Clock)
    ids = {}
    for key, timestamp, source in [
        ('old', '2030-01-01T00:00:00Z', 'pi'),
        ('boundary', '2030-01-02T03:00:00+03:00', 'pi'),
        ('newer', '2030-01-02T01:00:00Z', 'pi'),
        ('undated', None, 'pi'),
        ('other-source', None, 'mail'),
    ]:
        ids[key] = client.post('/episodes', json=evidence(
            event_id=key, source=source, text='raw ' + key + ' ' + 'x' * 4000,
            occurred_at=timestamp, metadata={'large': 'y' * 10000})).json()['id']
    params = dict(source='pi', since='2030-01-02T00:00:00Z', limit=3)
    response = client.get('/recall', params=params)
    assert response.status_code == 200
    hits = response.json()['memories']
    assert [hit['id'] for hit in hits] == [ids['undated'], ids['newer'], ids['boundary']]
    assert all(hit['tier'] == 'raw' and hit['confirmed'] is False for hit in hits)
    assert all(hit['source'] == 'pi' and len(hit['text']) <= 512 for hit in hits)
    assert all('metadata' not in hit and 'scope' not in hit for hit in hits)
    assert len(response.content) < 6000
    assert len(client.get(f"/episodes/{ids['undated']}").json()['text']) > 4000
    assert client.get('/recall', params=dict(params, scope='personal')).json() == {'memories': []}
