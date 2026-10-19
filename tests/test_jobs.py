import pytest

from memento.graph import Graph
from memento.jobs import LostLease, Queue, Worker
from memento.store import ConflictError, Store


def setup_queue(path):
    store = Store(path)
    graph = Graph(store)
    episode, _ = store.capture(dict(source='test', event_id='job', conversation_id='c',
        kind='user', text='keep the original image', occurred_at=None, metadata={}))
    queue = Queue(graph, lease_seconds=10)
    command = dict(command_id='capture-image', actor='user', why='explicit image',
        sha256='a' * 64, kind='image', filename='photo.png', scope=['personal'],
        evidence=[dict(episode_id=episode['id'], quote='the original image')])
    return store, graph, queue, command


def test_accepted_job_survives_restart_and_rejects_different_retry(tmp_path):
    store, graph, queue, command = setup_queue(tmp_path)
    try:
        accepted = queue.submit_media(**command)
        assert queue.submit_media(**command) == accepted
        with pytest.raises(ConflictError):
            queue.submit_media(**dict(command, filename='different.png'))
    finally:
        store.close()
    store = Store(tmp_path)
    try:
        queue = Queue(Graph(store))
        job = queue.claim(['media'])
        assert job.id == accepted['job_id']
        assert job.payload == command
    finally:
        store.close()


def test_expired_attempt_cannot_commit_after_recovery(tmp_path, monkeypatch):
    clock = [100.0]
    monkeypatch.setattr('memento.jobs.time.time', lambda: clock[0])
    store, graph, queue, command = setup_queue(tmp_path)
    try:
        queue.submit_media(**command)
        old = queue.claim(['media'])
        clock[0] += 11
        current = queue.claim(['media'])
        assert current.owner != old.owner
        def stale_commit(commit):
            old.check()
            return graph._insert_node(commit, dict(type='knowledge', text='stale write',
                scope=['personal'], evidence=command['evidence']))
        with pytest.raises(LostLease):
            graph._command('stale', 'worker', 'must fence', {}, stale_commit)
        assert store._db.execute('SELECT count(*) FROM node_versions').fetchone()[0] == 0
        queue.complete(current, dict(recovered=True))
        assert queue.get(current.id)['result'] == dict(recovered=True)
        with pytest.raises(LostLease):
            queue.complete(old, dict(recovered=False))
        assert queue.get(current.id)['result'] == dict(recovered=True)
    finally:
        store.close()


def test_failed_work_does_not_starve_later_input_or_disclose_error_body(tmp_path):
    store, graph, queue, command = setup_queue(tmp_path)
    try:
        bad = queue.submit_media(**command)
        good = queue.submit_media(**dict(command, command_id='second', filename='good.png'))
        def process(job):
            if job.payload['filename'] == 'photo.png':
                raise ValueError('private source and signed capability must not be exposed')
            return graph.remember(command_id='result/' + job.id, actor='worker', why='source assertion',
                type='knowledge', text='keep the original image', scope=['personal'], evidence=command['evidence'])
        worker = Worker(queue, {'media': process})
        assert worker.run_once()
        assert worker.run_once()
        assert queue.get(bad['job_id'])['state'] == 'failed'
        assert queue.get(bad['job_id'])['error'] == 'ValueError'
        result = queue.get(good['job_id'])
        assert result['state'] == 'done'
        assert graph.history(result['result']['id'])[0]['text'] == 'keep the original image'
    finally:
        store.close()
