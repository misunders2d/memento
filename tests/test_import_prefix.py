"""A supported history import marks a finite extraction boundary, without models."""

from datetime import datetime, timezone
from types import SimpleNamespace

from memento import bridge
from memento.graph import Graph
from memento.processing import Processing
from memento.status import operational_status
from memento.store import Store


def imported_history(tmp_path):
    config = {'uid': 1000, 'client_uid': 'synthetic-device', 'url': 'http://127.0.0.1:1', 'preset': 'pi'}
    config_path = tmp_path / 'runner.json'
    config_path.write_bytes(bridge.encode(config))
    journal = tmp_path / 'history.jsonl'
    records = [{'type': 'session', 'id': 'conversation'},
               {'type': 'message', 'id': 'user', 'message': {'role': 'user', 'content': 'Imported finite evidence.'}}]
    journal.write_bytes(b''.join(bridge.encode(record) + b'\n' for record in records))
    return config_path, config, journal


def test_import_boundary_follows_records_even_with_an_earlier_pending_native_marker(tmp_path):
    config_path, config, journal = imported_history(tmp_path)
    spool = bridge.Spool(config_path, config)
    try:
        body = {'source': bridge.source(config, 'pi'), 'conversation_id': 'conversation'}
        spool.queue('/work/compaction', body)
        result = bridge.import_history(spool, 'pi', [journal])
        episodes = spool.db.execute("SELECT id,key FROM outbox WHERE endpoint='/episodes' ORDER BY id").fetchall()
        markers = spool.db.execute("SELECT id,key FROM outbox WHERE endpoint='/work/compaction' ORDER BY id").fetchall()
        assert len(markers) == 2 and markers[0][0] < episodes[0][0]
        assert markers[-1][0] > episodes[-1][0]
        assert markers[-1][1].startswith('import-compaction:') and markers[-1][1] != markers[0][1]
        assert result['conversations'] == ['conversation']
        assert bridge.import_history(spool, 'pi', [journal])['records_added'] == 0
        assert spool.db.execute("SELECT id,key FROM outbox WHERE endpoint='/episodes' ORDER BY id").fetchall() == episodes
        assert spool.db.execute("SELECT count(*) FROM outbox WHERE endpoint='/work/compaction'").fetchone()[0] == 3
    finally:
        spool.close()


def test_import_upload_marks_old_prefix_eligible_while_later_live_capture_stays_pending(tmp_path, monkeypatch):
    config_path, config, journal = imported_history(tmp_path)
    spool = bridge.Spool(config_path, config)
    bridge.import_history(spool, 'pi', [journal])
    spool.close()
    store = Store(tmp_path / 'service')
    try:
        processing = Processing(Graph(store))
        boundary = []
        later = []

        def accepted(_config, endpoint, body, **_kwargs):
            if endpoint == '/episodes':
                episode, _ = store.capture(body)
                return {'id': episode['id']}
            assert endpoint == '/work/compaction'
            result = processing.compact(**body)
            boundary.append(store._db.execute('SELECT compact_through FROM processing_progress').fetchone()[0])
            episode, _ = store.capture(dict(source=body['source'], conversation_id=body['conversation_id'],
                event_id='later-live', kind='user', text='Later live work remains separate.', occurred_at=None, metadata={}))
            later.append(episode['id'])
            return result

        monkeypatch.setattr(bridge, 'http', accepted)
        assert bridge.upload(config_path, config) == {'pending': 0, 'conflicts': 0}
        state = SimpleNamespace(store=store, retrieval=None, cold_runtime=None, cold=None,
                                drive_reauthorization_required=False)
        source = bridge.source(config, 'pi')
        store.capture(dict(source='other-device', conversation_id='other-conversation', event_id='other',
            kind='user', text='Unrelated live work.', occurred_at=None, metadata={}))
        receipt = operational_status(state, source=source)['extraction']['progress']
        assert receipt == [dict(source=source, conversation_id='conversation', cursor=0, offset=0,
                                compact_through=boundary[0], latest_seq=boundary[0] + 1)]
        task = processing.claim(agent='synthetic', timezone_name='UTC', now=datetime.now(timezone.utc))['work'][0]
        assert [window['text'] for window in task['input']['windows']] == ['Imported finite evidence.']
        _, context = processing._leased(task['id'], task['owner'])
        assert all(window['episode_id'] not in later for window in context['windows'])
        processing.complete(id=task['id'], owner=task['owner'], agent='synthetic',
            output={'windows': [{'id': window['id'], 'candidates': []} for window in task['input']['windows']]})
        progress = store._db.execute('SELECT seq,compact_through,latest_seq FROM processing_progress').fetchone()
        assert progress['seq'] == progress['compact_through'] == boundary[0]
        assert progress['latest_seq'] > progress['seq']
        assert processing.claim(agent='synthetic', timezone_name='UTC', now=datetime.now(timezone.utc))['work'] == []
        processing.compact(source=source, conversation_id='conversation')
        current = operational_status(state, source=source)['extraction']['progress'][0]
        assert current['compact_through'] > receipt[0]['compact_through']
        assert current['cursor'] >= receipt[0]['compact_through']
        assert current['cursor'] < current['latest_seq']
    finally:
        store.close()
