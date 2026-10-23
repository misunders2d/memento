"""Durable leased work. Inputs survive process loss; commits fence expired owners."""

import json
import logging
import time
from dataclasses import dataclass
from threading import Event, Thread
from uuid import uuid4

from .captured_media import active_source
from .store import ConflictError


class LostLease(RuntimeError):
    pass


@dataclass(frozen=True)
class Job:
    queue: object
    id: str
    kind: str
    payload: dict
    owner: str
    attempts: int

    def check(self):
        with self.queue.store._mutex:
            row = self.queue.store._db.execute(
                'SELECT state,owner,lease_until FROM jobs WHERE id=?', (self.id,)).fetchone()
            if (row is None or row['state'] != 'leased' or row['owner'] != self.owner
                    or row['lease_until'] <= time.time()):
                raise LostLease('Work lease expired or was reassigned')


class Queue:
    def __init__(self, graph, *, lease_seconds=180):
        if lease_seconds <= 0:
            raise ValueError('Lease duration must be positive')
        self.graph, self.store = graph, graph.store
        self.lease_seconds = lease_seconds

    def _insert(self, key, kind, payload):
        """Caller holds the writer transaction, including its source write."""
        serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False, allow_nan=False)
        db = self.store._db
        prior = db.execute('SELECT * FROM jobs WHERE job_key=?', (key,)).fetchone()
        if prior:
            if prior['kind'] != kind or prior['payload'] != serialized:
                raise ConflictError('Job identifier already has different input')
            return prior['id']
        job_id = uuid4().hex
        db.execute('''INSERT INTO jobs
            (id,job_key,kind,payload,state,available_at,attempts)
            VALUES (?,?,?,?,'pending',?,0)''', (job_id, key, kind, serialized, time.time()))
        return job_id

    def submit_media(self, **command):
        request = dict(op='queue_media', input=command)
        def apply(commit):
            resolved = self.graph._resolve_evidence(command)
            resolved['scope'] = self.graph._scope('media', resolved['scope'])
            # Resolve conversation-local quotes while accepting the command,
            # before later messages could change which episode is selected.
            resolved.pop('source', None)
            resolved.pop('conversation_id', None)
            job_id = self._insert('media/' + command['command_id'], 'media', resolved)
            self.graph._evidence(job_id, 1, resolved['evidence'])
            return dict(job_id=job_id, status='accepted', status_url='/jobs/' + job_id)
        return self.graph._command(command['command_id'], command['actor'], command['why'], request, apply)

    def get(self, job_id):
        with self.store._mutex:
            row = self.store._db.execute('''SELECT id,kind,state,attempts,error,result
                FROM jobs WHERE id=?''', (job_id,)).fetchone()
        if row is None:
            return None
        value = dict(row)
        value['result'] = json.loads(value['result']) if value['result'] is not None else None
        return value

    def claim(self, kinds, *, exclude=()):
        kinds = tuple(kinds)
        if not kinds:
            return None
        now, owner = time.time(), uuid4().hex
        excluded = f"AND id NOT IN ({','.join('?' for _ in exclude)})" if exclude else ''
        with self.store._mutex:
            db = self.store._db
            db.execute('BEGIN IMMEDIATE')
            try:
                row = db.execute(f'''SELECT * FROM jobs WHERE kind IN ({','.join('?' for _ in kinds)})
                    AND ((state='pending' AND available_at<=?) OR (state='leased' AND lease_until<=?))
                    {excluded} ORDER BY available_at,id LIMIT 1''', (*kinds, now, now, *exclude)).fetchone()
                if row is not None:
                    db.execute('''UPDATE jobs SET state='leased',owner=?,lease_until=?,attempts=attempts+1
                        WHERE id=?''', (owner, now + self.lease_seconds, row['id']))
                db.execute('COMMIT')
            except BaseException:
                db.execute('ROLLBACK')
                raise
        return (Job(self, row['id'], row['kind'], json.loads(row['payload']), owner, row['attempts'] + 1)
                if row is not None else None)

    def renew(self, job):
        now = time.time()
        with self.store._mutex:
            return self.store._db.execute('''UPDATE jobs SET lease_until=? WHERE id=?
                AND state='leased' AND owner=? AND lease_until>?''',
                (now + self.lease_seconds, job.id, job.owner, now)).rowcount == 1

    def complete(self, job, result):
        with self.store._mutex:
            job.check()
            self.store._db.execute('''UPDATE jobs SET state='done',result=?,error=NULL,
                owner=NULL,lease_until=NULL WHERE id=? AND owner=?''',
                (json.dumps(result, ensure_ascii=False, allow_nan=False), job.id, job.owner))

    def fail(self, job, error):
        retryable = getattr(error, 'retryable', not isinstance(error, (ValueError, ConflictError)))
        delay = getattr(error, 'retry_after', None)
        if delay is None:
            delay = min(3600, 5 * 2 ** min(job.attempts - 1, 10))
        # Exception messages can contain private evidence, credentials or signed URLs.
        code = type(error).__name__
        with self.store._mutex:
            if code == 'CapturedSourceUnavailable' and job.kind == 'media' and job.payload.get('captured') is True:
                # An occurrence captured after the source check saw only a leased job.
                retryable = active_source(self.store._db, job.payload['sha256']) is not None
            self.store._db.execute('''UPDATE jobs SET state=?,available_at=?,error=?,
                owner=NULL,lease_until=NULL WHERE id=? AND state='leased' AND owner=?''',
                ('pending' if retryable else 'failed', time.time() + delay, code, job.id, job.owner))


class Worker:
    def __init__(self, queue, handlers, *, maintenance=None):
        self.queue, self.handlers = queue, handlers
        self.maintenance = maintenance
        self.stop = Event()
        self.thread = None

    def run_once(self):
        maintained = False
        if self.maintenance is not None:
            try:
                maintained = self.maintenance()
            except Exception as error:
                logging.getLogger(__name__).warning('Background maintenance failed (%s); data retained',
                                                    type(error).__name__)
        job = self.queue.claim(self.handlers)
        if job is None:
            return bool(maintained)
        finished = Event()
        def heartbeat():
            while not finished.wait(self.queue.lease_seconds / 3):
                if not self.queue.renew(job):
                    return
        pulse = Thread(target=heartbeat, name='memento-lease', daemon=True)
        pulse.start()
        try:
            result = self.handlers[job.kind](job)
            self.queue.complete(job, result)
        except LostLease:
            pass
        except Exception as error:
            self.queue.fail(job, error)
        finally:
            finished.set()
            pulse.join()
        return True

    def start(self):
        if self.thread is not None:
            raise RuntimeError('Job worker already started')
        def run():
            while not self.stop.is_set():
                try:
                    worked = self.run_once()
                except Exception as error:
                    logging.getLogger(__name__).warning('Job dispatch failed (%s)', type(error).__name__)
                    worked = False
                if not worked:
                    self.stop.wait(1)
        self.thread = Thread(target=run, name='memento-jobs', daemon=True)
        self.thread.start()

    def close(self):
        self.stop.set()
        if self.thread is not None:
            self.thread.join()
