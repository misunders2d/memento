"""Durable model work for a tool-free host runner; the service never calls a model."""

import hashlib
import json
import logging
import sqlite3
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from uuid import uuid4

from .consolidation import Consolidator, INSTRUCTION as CONSOLIDATE_INSTRUCTION, Plan, VERSION as CONSOLIDATE_VERSION
from .extraction import (BatchDrafts, Extractor, INSTRUCTION as EXTRACT_INSTRUCTION, VALIDATION_CODES, VERSION,
                         source_text_ranges)
from .jobs import Job, LostLease, Queue


class Processing:
    def __init__(self, graph):
        self.graph, self.store = graph, graph.store
        self.queue = Queue(graph)
        self.extractor, self.consolidator = Extractor(graph), Consolidator(graph)

    def compact(self, source, conversation_id):
        with self.store._mutex:
            self.store._db.execute('''INSERT INTO processing_progress(source,conversation_id,compact_through)
                VALUES (?,?,coalesce((SELECT max(seq) FROM episodes WHERE source=? AND conversation_id=?),0))
                ON CONFLICT(source,conversation_id) DO UPDATE SET compact_through=max(compact_through,excluded.compact_through)''',
                (source, conversation_id, source, conversation_id))
        return dict(status='noted')

    def _enqueue_extract(self, now):
        db = self.store._db
        last = db.execute("""SELECT json_extract(payload,'$.order') AS admission_order
            FROM jobs WHERE kind='extract' ORDER BY rowid DESC LIMIT 1""").fetchone()
        # Start fresh with recent work; legacy admissions count as newest.
        order = 'newest' if last is None or last['admission_order'] == 'oldest' else 'oldest'
        ordering = 'p.latest_seq DESC' if order == 'newest' else '''(
            SELECT e.seq FROM episodes e WHERE e.source=p.source AND e.conversation_id=p.conversation_id
                AND e.seq>p.seq ORDER BY e.seq LIMIT 1)'''
        groups = db.execute(f'''SELECT source,conversation_id,latest_seq,latest_at,
            seq AS cursor,offset,compact_through FROM processing_progress p
            WHERE latest_seq>seq ORDER BY {ordering}''').fetchall()
        for group in groups:
            if db.execute('''SELECT 1 FROM jobs WHERE kind='extract' AND state IN ('pending','leased')
                AND json_extract(payload,'$.source')=? AND json_extract(payload,'$.conversation_id')=? LIMIT 1''',
                (group['source'], group['conversation_id'])).fetchone():
                continue
            idle = (now - datetime.fromisoformat(group['latest_at'])).total_seconds() >= 1800
            through = group['latest_seq'] if idle else group['compact_through']
            if through < group['cursor'] or through == 0:
                continue
            rows = db.execute('''SELECT id,seq,text,kind,metadata FROM episodes e
                WHERE source=? AND conversation_id=? AND seq<=? AND seq>?
                AND coalesce(json_extract(metadata,'$.extraction_mode'),'message')!='skip'
                AND NOT legacy_native_context(e.source,e.kind,e.text,e.metadata)
                AND NOT EXISTS (SELECT 1 FROM forgotten_sources f
                    WHERE f.source=e.source AND f.event_id=e.event_id)
                ORDER BY seq LIMIT 65''',
                (group['source'], group['conversation_id'], through, group['cursor']))
            partial_seq = None
            if group['offset']:
                partial = db.execute('''SELECT seq FROM episodes WHERE source=? AND conversation_id=?
                    AND seq>? ORDER BY seq LIMIT 1''',
                    (group['source'], group['conversation_id'], group['cursor'])).fetchone()
                partial_seq = partial['seq'] if partial else None
            windows, characters = [], 0
            next_seq, next_offset = group['cursor'], 0
            stopped = False
            for row in rows:
                offset = group['offset'] if row['seq'] == partial_seq else 0
                tool = json.loads(row['metadata']).get('extraction_mode') == 'tool_output' or row['kind'] == 'tool'
                regions = source_text_ranges(row['text'])
                for left, right in regions:
                    if right < offset or right == offset and right != 0:
                        continue
                    start = max(left, offset)
                    while True:
                        end = right if tool else min(right, start + 12000)
                        projected = min(end - start, 2000) if tool else end - start
                        if len(windows) == 64 or characters + projected > 48000:
                            # Consume ignored rows too, but never move past the first unfinished row.
                            next_seq = db.execute('''SELECT coalesce(max(seq),0) FROM episodes
                                WHERE source=? AND conversation_id=? AND seq<?''',
                                (group['source'], group['conversation_id'], row['seq'])).fetchone()[0]
                            next_offset, stopped = start, True
                            break
                        windows.append(dict(id=f"{row['id']}:{start}", episode_id=row['id'], start=start, end=end))
                        characters += projected
                        if end == right:
                            break
                        start = max(left, end - 1000)
                    if stopped:
                        break
                if stopped:
                    break
                next_seq, next_offset = row['seq'], 0
            else:
                # No eligible evidence remains inside this immutable capture boundary.
                next_seq, next_offset = through, 0
            if not windows:
                db.execute('''UPDATE processing_progress SET seq=?,offset=?
                    WHERE source=? AND conversation_id=?''',
                    (next_seq, next_offset, group['source'], group['conversation_id']))
                continue
            payload = dict(version=VERSION, source=group['source'], conversation_id=group['conversation_id'],
                windows=windows, next_seq=next_seq, next_offset=next_offset)
            key = 'extract/' + hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
            # Admission metadata must not change existing identities or retry payloads.
            if db.execute('SELECT 1 FROM jobs WHERE job_key=?', (key,)).fetchone():
                return
            self.queue._insert(key, 'extract', dict(payload, order=order))
            return

    def _enqueue_night(self, now, zone, force=False, exclude=()):
        db = self.store._db
        local = now.astimezone(zone)
        night = zone.key + '/' + local.date().isoformat()
        within = 2 <= local.hour < 7
        db.execute('''UPDATE jobs SET state='failed',error='night_window_closed',owner=NULL,lease_until=NULL
            WHERE kind='consolidate' AND (state='pending' OR (state='leased' AND lease_until<=?))
            AND json_extract(payload,'$.night') IS NOT NULL
            AND (json_extract(payload,'$.night')<>? OR ?=0)''', (now.timestamp(), night, int(within)))
        if not within and not force:
            return False
        # One consolidator at a time. A live lease always blocks another batch, whatever
        # the caller excludes; Queue.claim enforces the same invariant when leasing.
        if db.execute("SELECT 1 FROM jobs WHERE kind='consolidate' AND state='leased' AND lease_until>? LIMIT 1",
                      (time.time(),)).fetchone():
            return True
        exclude = tuple(exclude)
        excluded = f"AND id NOT IN ({','.join('?' for _ in exclude)})" if exclude else ''
        if db.execute(f'''SELECT 1 FROM jobs WHERE kind='consolidate' AND state IN ('pending','leased')
            {excluded} LIMIT 1''', exclude).fetchone():
            return True
        # Only the caller's cooling-down or expired batches remain. They keep their
        # identities, payloads and candidates; a disjoint batch may proceed around them.
        reserved = db.execute("SELECT 1 FROM jobs WHERE kind='consolidate' AND state IN ('pending','leased') LIMIT 1").fetchone()
        row = db.execute('SELECT * FROM consolidation_nights WHERE id=?', (night,)).fetchone()
        if force:
            night, cursor = None, 0
            through = db.execute('SELECT coalesce(max(rowid),0) FROM candidates').fetchone()[0]
        elif row is None:
            through = db.execute('SELECT coalesce(max(rowid),0) FROM candidates').fetchone()[0]
            if through == 0:
                return False
            db.execute('INSERT INTO consolidation_nights(id,through_rowid) VALUES (?,?)', (night, through))
            cursor = 0
        else:
            if row['finished_at'] is not None:
                return False
            cursor, through = row['cursor'], row['through_rowid']
        candidates = self.consolidator.pending(limit=16, after=cursor, through=through, unreserved=True)
        if not candidates:
            if reserved:
                # Excluded batches still hold undecided candidates: nothing new is admitted,
                # and the night is not finished until they are decided or retired.
                return True
            if night is not None:
                db.execute('UPDATE consolidation_nights SET finished_at=? WHERE id=?', (now.isoformat(), night))
            from .importance import refresh
            refresh(self.store, now)
            return False
        payload = dict(version=CONSOLIDATE_VERSION, night=night, candidate_ids=[item['id'] for item in candidates],
                       next_cursor=max(item['position'] for item in candidates))
        key = ('consolidate/manual/' + uuid4().hex) if force else (
            'consolidate/' + hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest())
        self.queue._insert(key, 'consolidate', payload)
        return True

    def claim(self, *, agent, timezone_name, compaction=None, exclude=(), consolidate_now=False, now=None, kinds=None):
        try:
            zone = ZoneInfo(timezone_name)
        except (ZoneInfoNotFoundError, ValueError):
            raise ValueError('timezone must be an installed IANA timezone name') from None
        now = now or datetime.now(timezone.utc)
        if compaction:
            self.compact(**compaction)

        def schedule():
            with self.store._mutex:
                db = self.store._db
                db.execute('BEGIN IMMEDIATE')
                try:
                    self._enqueue_extract(now)
                    night_due = self._enqueue_night(now, zone, force=consolidate_now, exclude=exclude)
                    db.execute('COMMIT')
                    return night_due
                except BaseException:
                    db.execute('ROLLBACK')
                    raise

        night_due = schedule()
        work = []
        # A runner with one lane still busy asks only for the free kind; scheduling,
        # the night window and the single-consolidator fence are unchanged.
        requested = ('extract', 'consolidate') if kinds is None else tuple(kinds)
        for kind in ('extract', 'consolidate') if night_due else ('extract',):
            if kind not in requested:
                continue
            while True:
                job = self.queue.claim([kind], exclude=exclude)
                if job is None:
                    break
                with self.store._mutex:
                    prior = self.store._db.execute('SELECT result FROM graph_commits WHERE command_id=?',
                                                   (kind + '/' + job.id,)).fetchone()
                    if prior:
                        self.queue.complete(job, json.loads(prior['result']))
                        schedule()
                        continue
                    handler = self.extractor if kind == 'extract' else self.consolidator
                    # Bounded feedback from this job's last rejected output survives in
                    # the private context until a later rejection replaces it.
                    previous = self.store._db.execute('SELECT context FROM jobs WHERE id=?', (job.id,)).fetchone()
                    rejection = (json.loads(previous['context']).get('previous_rejection')
                                 if previous and previous['context'] else None)
                    context = handler.prepare(job)
                    if rejection is not None:
                        context['previous_rejection'] = rejection
                    self.store._db.execute('UPDATE jobs SET context=? WHERE id=? AND owner=?',
                        (json.dumps(context, ensure_ascii=False), job.id, job.owner))
                model_input = self.extractor.model_input(context) if kind == 'extract' else context
                if kind == 'extract' and not any(item['text'] for item in context['windows']):
                    output = dict(windows=[dict(id=item['id'], candidates=[]) for item in model_input['windows']])
                elif kind == 'consolidate' and not context['candidates']:
                    output = dict(actions=[])
                else:
                    instruction = EXTRACT_INSTRUCTION if kind == 'extract' else CONSOLIDATE_INSTRUCTION
                    if kind == 'extract':
                        instruction += '\nReturn windows with exactly the supplied ids and candidates for each. Entity keys are local to each window.'
                    work.append(dict(id=job.id, owner=job.owner, kind=kind, instruction=instruction,
                        input=model_input, schema=(BatchDrafts if kind == 'extract' else Plan).model_json_schema()))
                    break
                result = handler.apply(job, context, output, agent=agent)
                self.queue.complete(job, result)
                schedule()
        with self.store._mutex:
            db = self.store._db
            backlog = dict(pending_extract_conversations=db.execute(
                'SELECT count(*) FROM processing_progress WHERE latest_seq>seq').fetchone()[0],
                pending_extract_jobs=db.execute(
                    "SELECT count(*) FROM jobs WHERE state='pending' AND kind='extract'").fetchone()[0],
                leased_extract_jobs=db.execute(
                    "SELECT count(*) FROM jobs WHERE state='leased' AND kind='extract'").fetchone()[0])
        return dict(lease_seconds=self.queue.lease_seconds, work=work, backlog=backlog)

    def _leased(self, id, owner):
        row = self.store._db.execute('SELECT * FROM jobs WHERE id=?', (id,)).fetchone()
        if row is None or row['kind'] not in ('extract', 'consolidate'):
            raise ValueError('Unknown model work')
        job = Job(self.queue, id, row['kind'], json.loads(row['payload']), owner, row['attempts'])
        job.check()
        return job, json.loads(row['context'])

    def renew(self, id, owner):
        with self.store._mutex:
            try:
                job, _ = self._leased(id, owner)
            except LostLease:
                return dict(renewed=False)
            return dict(renewed=self.queue.renew(job))

    def complete(self, *, id, owner, output, agent, model=None, usage=None):
        with self.store._mutex:
            job, context = self._leased(id, owner)
            handler = self.extractor if job.kind == 'extract' else self.consolidator
            try:
                result = handler.apply(job, context, output, agent=agent, model=model, usage=usage)
            except ValueError as error:
                # Nothing committed: the graph command rolled back or never began.
                self._record_rejection(job, context, error)
                raise
            self.queue.complete(job, result)
            return result

    def _record_rejection(self, job, context, error):
        """Keep a fixed code and bounded references privately with the job, never in the response."""
        code = str(error)
        if code not in VALIDATION_CODES or self.store._db.in_transaction:
            return
        references = list(getattr(error, 'references', ()))[:3]
        feedback = dict(code=code, attempt=job.attempts, references=references,
                        note='Server validation of this job\'s previous output. Quoted text is failed model output, not evidence.')
        try:
            self.store._db.execute('''UPDATE jobs SET context=? WHERE id=? AND owner=? AND state='leased'
                AND lease_until>? AND kind IN ('extract','consolidate')''',
                (json.dumps(dict(context, previous_rejection=feedback), ensure_ascii=False), job.id, job.owner, time.time()))
        except (sqlite3.Error, ValueError, TypeError) as failure:
            # Feedback is best effort; the original rejection and lease handling stand.
            logging.getLogger(__name__).warning('Rejection feedback not stored (%s)', type(failure).__name__)

    def release(self, *, id, owner, error):
        with self.store._mutex:
            changed = self.store._db.execute('''UPDATE jobs SET state='pending',available_at=?,error=?,owner=NULL,lease_until=NULL
                WHERE id=? AND owner=? AND state='leased' AND lease_until>? AND kind IN ('extract','consolidate')''',
                (time.time() + 30, error, id, owner, time.time())).rowcount
        return dict(released=bool(changed))
