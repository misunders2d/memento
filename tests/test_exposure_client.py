"""Fresh bridge offers are durable advisory metadata, never citation evidence."""
import datetime
import json

import pytest

from memento import bridge


@pytest.fixture
def spool(tmp_path):
    config = {'uid': 1000, 'client_uid': 'fixture-device', 'url': 'http://127.0.0.1:1',
              'preset': 'pi', 'timezone': 'UTC'}
    path = tmp_path / 'runner.json'
    path.write_bytes(bridge.encode(config))
    value = bridge.Spool(path, config)
    try:
        yield value
    finally:
        value.close()


def lesson(identifier='lesson', version=1, **changes):
    return {'id': identifier, 'version': version, 'category': 'behavioral_suggestion',
            'text': 'This summary must not enter the receipt.', 'authority': 'inferred',
            'behavior': {'applicability': 'For ordinary setup.', 'conduct': 'Use the supported installer.',
                         'exceptions': ['Honor current manual requests.'], **changes}}


def queued(spool):
    return [json.loads(raw) for raw, in spool.db.execute(
        "SELECT body FROM outbox WHERE endpoint='/episodes' ORDER BY id")]


def receipts(spool):
    return [row for row in queued(spool) if 'exposure' in row['metadata']]


@pytest.mark.parametrize('harness', ['pi', 'omp', 'claude', 'codex'])
def test_each_fresh_offer_is_unique_even_for_identical_prompts_and_time(spool, monkeypatch, harness):
    class FrozenDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, 21, 12, tzinfo=datetime.timezone.utc)

    monkeypatch.setattr(bridge.datetime, 'datetime', FrozenDateTime)
    monkeypatch.setattr(bridge, 'http', lambda *_args, **_kwargs: {'memories': [lesson(version=7)]})
    spool.config['preset'] = harness
    for _ in range(2):
        if harness in ('pi', 'omp'):
            result = bridge.native(spool, {'harness': harness, 'session_id': 'conversation',
                                          'action': 'recall', 'query': 'Connect another agent.'}, recall=bridge.passive)
            context = result['context']
            assert result['shown'] == ['lesson']
        else:
            result = bridge.hook(spool, harness, {'session_id': 'conversation',
                'hook_event_name': 'UserPromptSubmit', 'prompt': 'Connect another agent.'}, recall=bridge.passive)
            context = result['hookSpecificOutput']['additionalContext']
        assert 'Use the supported installer.' in context
    offered = receipts(spool)
    assert len(offered) == 2 and offered[0]['event_id'] != offered[1]['event_id']
    assert offered[0]['occurred_at'] == offered[1]['occurred_at']
    for value in offered:
        assert value['source'] == 'native:fixture-device:' + harness
        assert value['conversation_id'] == 'conversation'
        assert value['kind'] == 'native_hook' and value['text'] == ''
        assert datetime.datetime.fromisoformat(value['occurred_at']).tzinfo is not None
        assert {key: item for key, item in value['metadata'].items() if not key.startswith('chunk_')} == {
            'extraction_mode': 'skip', 'evidence_eligible': False, 'exposure': [['lesson', 7]]}
        assert lesson()['text'] not in bridge.encode(value).decode()
    assert json.loads(spool.db.execute("SELECT shown FROM recall_cache WHERE conversation='conversation'").fetchone()[0]) == ['lesson']
    assert not spool.db.execute("SELECT 1 FROM outbox WHERE endpoint='/usage'").fetchone()


@pytest.mark.parametrize('memory', [
    {'id': 'fact', 'version': 1, 'category': 'memory', 'text': 'A fact.'},
    lesson(exceptions=['Complete exception ' + '猫' * 1000]),
    lesson(version=None), lesson(version=True), lesson(version=0), lesson(version=-1), lesson(version='1'),
    lesson(applicability=''),
])
def test_no_receipt_for_fact_clipped_or_unversioned_output(spool, monkeypatch, memory):
    monkeypatch.setattr(bridge, 'http', lambda *_args, **_kwargs: {'memories': [memory]})
    bridge.passive(spool, 'conversation', 'Connect another agent.', harness='pi')
    assert receipts(spool) == []


def test_only_rendered_versioned_behaviors_enter_receipt(spool, monkeypatch):
    memories = [lesson('clipped', 8, exceptions=['猫' * 2000]),
                {'id': 'fact', 'text': 'A fact.'}, lesson('first', 2), lesson('second', 4)]
    monkeypatch.setattr(bridge, 'http', lambda *_args, **_kwargs: {'memories': memories})
    result = bridge.passive(spool, 'conversation', 'Connect another agent.', harness='pi')
    assert result['shown'] == ['fact', 'first', 'second']
    assert receipts(spool)[0]['metadata']['exposure'] == [['first', 2], ['second', 4]]


def test_cached_peer_and_failed_output_do_not_create_or_relabel_receipts(spool, monkeypatch):
    calls = []
    def online(*_args, **_kwargs):
        calls.append(True)
        return {'memories': [lesson(version=1)]}
    monkeypatch.setattr(bridge, 'http', online)
    first = bridge.passive(spool, 'conversation', 'Connect another agent.', harness='pi')
    original = bridge.encode(receipts(spool)[0])
    assert bridge.passive(spool, 'conversation', '[HERDR AGENT MESSAGE] result', harness='pi') == first
    assert len(calls) == 1
    def offline(*_args, **_kwargs):
        raise OSError('synthetic outage')
    monkeypatch.setattr(bridge, 'http', offline)
    cached = bridge.passive(spool, 'conversation', 'Connect another agent.', harness='pi')
    assert cached == {**first, 'error': 'recall_unavailable'}
    assert bridge.passive(spool, 'other', 'No cache.', harness='pi')['context'] == ''
    assert [bridge.encode(row) for row in receipts(spool)] == [original]
    monkeypatch.setattr(bridge, 'http', lambda *_args, **_kwargs: {'memories': [lesson(version=2)]})
    bridge.passive(spool, 'conversation', 'Connect another agent.', harness='pi')
    assert [row['metadata']['exposure'] for row in receipts(spool)] == [[['lesson', 1]], [['lesson', 2]]]
    assert bridge.encode(receipts(spool)[0]) == original


def test_offline_restart_and_upload_retry_preserve_original_offer(spool, monkeypatch):
    monkeypatch.setattr(bridge, 'http', lambda *_args, **_kwargs: {'memories': [lesson(version=3)]})
    bridge.passive(spool, 'conversation', 'Connect another agent.', harness='pi')
    original = bridge.encode(receipts(spool)[0])
    path, config = spool.config_path, spool.config
    spool.commit()
    spool.close()
    attempts = []
    def offline(_config, endpoint, body, **_kwargs):
        attempts.append((endpoint, bridge.encode(body)))
        raise OSError('synthetic outage')
    monkeypatch.setattr(bridge, 'http', offline)
    result = bridge.upload(path, config)
    assert result['pending'] == 1 and result['conflicts'] == 0
    reopened = bridge.Spool(path, config)
    try:
        assert [bridge.encode(row) for row in receipts(reopened)] == [original]
    finally:
        reopened.close()
    def online(_config, endpoint, body, **_kwargs):
        attempts.append((endpoint, bridge.encode(body)))
        return {'id': 'acknowledged'}
    monkeypatch.setattr(bridge, 'http', online)
    assert bridge.upload(path, config) == {'pending': 0, 'conflicts': 0}
    assert attempts == [('/episodes', original), ('/episodes', original)]
