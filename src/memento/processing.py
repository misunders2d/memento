"""Durable model work for a tool-free host runner; the service never calls a model."""

import hashlib
import json
import logging
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from uuid import uuid4

from .consolidation import Consolidator, INSTRUCTION as CONSOLIDATE_INSTRUCTION, Plan, VERSION as CONSOLIDATE_VERSION
from .extraction import (ACTIVE_CANDIDATE_SQL, BatchDrafts, Extractor, INSTRUCTION as EXTRACT_INSTRUCTION,
                         VALIDATION_CODES, VERSION, source_text_ranges)
from .jobs import Job, LostLease, Queue
from .schema import PARKING_VERSION

# The extraction cursor a conversation admits from. A parked conversation (a
# historical_parking row) admits only history after its separate live cursor, so
# processing_progress.seq keeps meaning "extracted prefix" and the parked range
# (seq, parked_through] is never admitted sequentially again.
_CURSOR_SQL = 'coalesce(h.live_seq,p.seq)'
_OFFSET_SQL = 'coalesce(h.live_offset,p.offset)'
_PROGRESS_SQL = '''FROM processing_progress p LEFT JOIN historical_parking h
    ON h.source=p.source AND h.conversation_id=p.conversation_id'''
# The cutover fence uses one fixed error code; status reports only known categories.
PARKED_ERROR = 'historical_parked'


# A legacy episode-id citation may demand at most one ordinary extraction window;
# a longer episode needs the chunk reference that new raw results carry.
DEMAND_EPISODE_LIMIT = 12000

# The consolidation night window a runner that sends no schedule still gets.
DEFAULT_CONSOLIDATION_WINDOW = ('02:00', '07:00')
_CLOCK = re.compile(r'^([01][0-9]|2[0-3]):([0-5][0-9])$')


def night_window(start=None, end=None):
    """Validate a local-time window into minutes since midnight: (start, end), start-inclusive, end-exclusive.

    Both endpoints are strict HH:MM or both omitted; a window never covers zero or every minute.
    """
    if (start is None) != (end is None):
        raise ValueError('consolidation_start and consolidation_end must be given together')
    if start is None:
        start, end = DEFAULT_CONSOLIDATION_WINDOW
    values = []
    for value in (start, end):
        match = _CLOCK.fullmatch(value) if isinstance(value, str) else None
        if match is None:
            raise ValueError('consolidation window endpoints must be HH:MM local time')
        values.append(int(match.group(1)) * 60 + int(match.group(2)))
    if values[0] == values[1]:
        raise ValueError('consolidation window endpoints must differ')
    return tuple(values)


def demand_extract(graph, episode, start=None, end=None):
    """Caller holds the writer transaction. One deterministic extract job for a used parked span.

    Only parked history (after the extracted prefix, at or before the cutover boundary)
    is demanded: everything else is covered by ordinary extraction or already done.
    Repeated use of the same span resolves to the same job, whatever its state, so
    nothing is replayed; completion never touches a conversation cursor.
    """
    from .store import legacy_native_context
    db = graph.store._db
    row = db.execute('''SELECT p.seq AS cursor,h.parked_through FROM historical_parking h
        JOIN processing_progress p ON p.source=h.source AND p.conversation_id=h.conversation_id
        WHERE h.source=? AND h.conversation_id=?''', (episode['source'], episode['conversation_id'])).fetchone()
    if row is None or not row['cursor'] < episode['seq'] <= row['parked_through']:
        return None
    metadata = json.loads(episode['metadata']) if isinstance(episode['metadata'], str) else episode['metadata']
    if (metadata.get('extraction_mode') == 'skip'
            or episode['kind'] in ('developer', 'native_record', 'native_live_record', 'native_hook')):
        return None
    text = graph.store.get(episode['id'])['text']
    if legacy_native_context(episode['source'], episode['kind'], text, metadata):
        return None
    if start is None or end is None:
        if len(text) > DEMAND_EPISODE_LIMIT:
            return None
        start, end = 0, len(text)
    tool = metadata.get('extraction_mode') == 'tool_output' or episode['kind'] == 'tool'
    windows = []
    for left, right in source_text_ranges(text):
        left, right = max(left, start), min(right, end)
        if left >= right:
            continue
        at = left
        while True:
            stop = right if tool else min(right, at + 12000)
            windows.append(dict(id=f"{episode['id']}:{at}", episode_id=episode['id'], start=at, end=stop))
            if stop >= right or len(windows) == 64:
                break
            at = max(left, stop - 1000)
    if not windows:
        return None
    payload = dict(version=VERSION, mode='demand', source=episode['source'], conversation_id=episode['conversation_id'],
                   episode_id=episode['id'], start=start, end=end, windows=windows)
    key = 'extract/demand/' + hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    return Queue(graph)._insert(key, 'extract', payload)


def pending_conversations_sql(source_filter=''):
    """Conversations with capture beyond their effective extraction cursor."""
    return f'''SELECT count(*) {_PROGRESS_SQL} WHERE p.latest_seq>p.seq AND p.latest_seq>{_CURSOR_SQL} {source_filter}'''


def historical_status(db):
    """Honest parking state for status and the owner console; reads only."""
    activation = db.execute('SELECT activated_at FROM historical_activation').fetchone()
    parking = db.execute('''SELECT count(*) AS parked_conversations,
        coalesce(sum(p.latest_seq>h.live_seq),0) AS live_pending_conversations
        FROM historical_parking h JOIN processing_progress p
        ON p.source=h.source AND p.conversation_id=h.conversation_id''').fetchone()
    candidates = db.execute('''SELECT coalesce(sum(released_at IS NULL),0) AS parked_candidates,
        coalesce(sum(released_at IS NOT NULL),0) AS released_candidates FROM historical_parked_candidates''').fetchone()
    return dict(active=activation is not None, activated_at=activation['activated_at'] if activation else None,
                header=db.execute('PRAGMA user_version').fetchone()[0], **dict(parking), **dict(candidates))


class Processing:
    def __init__(self, graph):
        self.graph, self.store = graph, graph.store
        self.queue = Queue(graph)
        self.extractor, self.consolidator = Extractor(graph), Consolidator(graph)

    def maintain(self, *, command_id, actor, why, target_ids, origin=None):
        """Durably seed only explicitly selected subjects, never the historical pool."""
        if (not isinstance(target_ids, list) or not 1 <= len(target_ids) <= 48
                or any(not isinstance(value, str) or not value or len(value) > 64 for value in target_ids)
                or len(set(target_ids)) != len(target_ids)):
            raise ValueError('target_ids must be 1..48 distinct existing identifiers')
        target_ids = sorted(target_ids)
        request = dict(op='maintenance', target_ids=target_ids, origin=origin)
        def seed(commit):
            from .captured_media import visible_sql
            db = self.store._db
            for target in target_ids:
                if db.execute(f'''SELECT 1 FROM node_versions n WHERE n.id=? AND n.state='active'
                    AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)
                    AND {visible_sql()}''', (target,)).fetchone() is None:
                    raise ValueError('Maintenance requires active visible existing nodes')
            stamp = datetime.now(timezone.utc).isoformat()
            existing = db.execute("SELECT id,state FROM repair_groups WHERE seed_ids=? AND state='pending' LIMIT 1",
                                  (json.dumps(target_ids),)).fetchone()
            if existing:
                return dict(repair_group_id=existing['id'], status=existing['state'], target_ids=target_ids)
            group = 'r_' + uuid4().hex[:24]
            db.execute('''INSERT INTO repair_groups
                (id,seed_ids,frontier,reviewed_ids,state,origin,created_at,updated_at)
                VALUES (?,?,?,'[]','pending',?,?,?)''',
                (group, json.dumps(target_ids), json.dumps(target_ids), origin, stamp, stamp))
            return dict(repair_group_id=group, status='pending', target_ids=target_ids)
        return self.graph._command(command_id, actor, why, request, seed)

    def _enqueue_repair(self, exclude=()):
        """Admit one bounded page, retaining groups even if a job fails permanently."""
        db = self.store._db
        for row in db.execute("SELECT * FROM repair_groups WHERE state='pending' ORDER BY created_at,id").fetchall():
            if row['active_job'] is not None:
                job = db.execute('SELECT state FROM jobs WHERE id=?', (row['active_job'],)).fetchone()
                if job and job['state'] in ('pending', 'leased'):
                    continue
                # A failed job has no candidate cursor to rederive it. Keep its group
                # and wait a full retry interval before giving it a new job identity.
                db.execute('''UPDATE repair_groups SET active_job=NULL,generation=generation+1,
                    available_at=?,updated_at=? WHERE id=?''',
                    (time.time() + 900, datetime.now(timezone.utc).isoformat(), row['id']))
                continue
            if row['available_at'] > time.time():
                continue
            frontier = json.loads(row['frontier'])
            if not frontier:
                db.execute("UPDATE repair_groups SET state='done' WHERE id=?", (row['id'],))
                continue
            reviewed = json.loads(row['reviewed_ids'])
            # A repeated anchor compares the remaining seed survivors together;
            # ordinary maintenance still admits eight seeds per page.
            limit = 48 if frontier[0] in reviewed else 8
            payload = dict(version=CONSOLIDATE_VERSION, night=None, candidate_ids=[], next_cursor=0,
                repair_group_id=row['id'], maintenance_ids=frontier[:limit], reviewed_ids=reviewed)
            job = self.queue._insert('consolidate/repair/' + row['id'] + '/' + str(row['generation']),
                                     'consolidate', payload)
            db.execute('UPDATE repair_groups SET active_job=? WHERE id=?', (job, row['id']))
            return True
        return False

    def compact(self, source, conversation_id):
        with self.store._mutex:
            self.store._db.execute('''INSERT INTO processing_progress(source,conversation_id,compact_through)
                VALUES (?,?,coalesce((SELECT max(seq) FROM episodes WHERE source=? AND conversation_id=?),0))
                ON CONFLICT(source,conversation_id) DO UPDATE SET compact_through=max(compact_through,excluded.compact_through)''',
                (source, conversation_id, source, conversation_id))
        return dict(status='noted')

    def history_status(self):
        with self.store._mutex:
            return historical_status(self.store._db)

    def park_history(self, now=None):
        """One explicit cutover: the CURRENT remaining backlog becomes parked history.

        One transaction: the singleton activation receipt, one parking row per
        conversation with unextracted history (parked range (seq, latest_seq], live
        cursor at latest_seq), fences on pending/leased extraction inside those
        ranges and on every pending or leased consolidation batch, every active
        undecided candidate parked, and the header raised to 3. Repeating is a
        no-op: the boundary never extends to newer captures. Everything captured
        afterwards, including delayed offline records of old conversations, processes
        normally through the live cursor. Raw history and the ordinary cursor are untouched.
        """
        stamp = (now or datetime.now(timezone.utc)).isoformat()
        with self.store._mutex:
            db = self.store._db
            db.execute('BEGIN IMMEDIATE')
            try:
                if db.execute('SELECT 1 FROM historical_activation').fetchone() is None:
                    db.execute('INSERT INTO historical_activation(singleton,activated_at) VALUES (1,?)', (stamp,))
                    db.execute('''INSERT INTO historical_parking(source,conversation_id,parked_through,live_seq,live_offset)
                        SELECT p.source,p.conversation_id,p.latest_seq,p.latest_seq,0 FROM processing_progress p
                        WHERE p.latest_seq>p.seq''')
                    # Ordinary extraction inside a parked range: no stale completion may
                    # bypass the cutover. Existing jobs at activation are exactly those.
                    db.execute(f'''UPDATE jobs SET state='failed',error='{PARKED_ERROR}',owner=NULL,lease_until=NULL
                        WHERE kind='extract' AND state IN ('pending','leased')
                        AND json_extract(payload,'$.mode') IS NULL AND json_extract(payload,'$.cursor') IS NULL
                        AND EXISTS (SELECT 1 FROM historical_parking h
                            WHERE h.source=json_extract(jobs.payload,'$.source')
                            AND h.conversation_id=json_extract(jobs.payload,'$.conversation_id')
                            AND json_extract(jobs.payload,'$.next_seq')<=h.parked_through)''')
                    db.execute(f'''UPDATE jobs SET state='failed',error='{PARKED_ERROR}',owner=NULL,lease_until=NULL
                        WHERE kind='consolidate' AND state IN ('pending','leased')''')
                    db.execute(f'''INSERT INTO historical_parked_candidates(candidate_id)
                        SELECT c.id FROM candidates c WHERE {ACTIVE_CANDIDATE_SQL}''')
                    db.execute(f'PRAGMA user_version={PARKING_VERSION}')
                result = historical_status(db)
                db.execute('COMMIT')
                return result
            except BaseException:
                db.execute('ROLLBACK')
                raise

    def _enqueue_extract(self, now):
        db = self.store._db
        last = db.execute("""SELECT json_extract(payload,'$.order') AS admission_order
            FROM jobs WHERE kind='extract' ORDER BY rowid DESC LIMIT 1""").fetchone()
        # Start fresh with recent work; legacy admissions count as newest.
        order = 'newest' if last is None or last['admission_order'] == 'oldest' else 'oldest'
        ordering = 'p.latest_seq DESC' if order == 'newest' else f'''(
            SELECT e.seq FROM episodes e WHERE e.source=p.source AND e.conversation_id=p.conversation_id
                AND e.seq>{_CURSOR_SQL} ORDER BY e.seq LIMIT 1)'''
        groups = db.execute(f'''SELECT p.source,p.conversation_id,p.latest_seq,p.latest_at,p.compact_through,
            {_CURSOR_SQL} AS cursor,{_OFFSET_SQL} AS offset,(h.source IS NOT NULL) AS live
            {_PROGRESS_SQL} WHERE p.latest_seq>p.seq AND p.latest_seq>{_CURSOR_SQL} ORDER BY {ordering}''').fetchall()
        for group in groups:
            if db.execute('''SELECT 1 FROM jobs WHERE kind='extract' AND state IN ('pending','leased')
                AND json_extract(payload,'$.source')=? AND json_extract(payload,'$.conversation_id')=?
                AND json_extract(payload,'$.mode') IS NULL LIMIT 1''',
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
                db.execute('''UPDATE historical_parking SET live_seq=?,live_offset=?
                    WHERE source=? AND conversation_id=?''' if group['live'] else
                    '''UPDATE processing_progress SET seq=?,offset=? WHERE source=? AND conversation_id=?''',
                    (next_seq, next_offset, group['source'], group['conversation_id']))
                continue
            payload = dict(version=VERSION, source=group['source'], conversation_id=group['conversation_id'],
                windows=windows, next_seq=next_seq, next_offset=next_offset)
            if group['live']:
                # Ordinary payloads and identities are unchanged; only parked
                # conversations report into the separate live cursor.
                payload['cursor'] = 'live'
            key = 'extract/' + hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
            # Admission metadata must not change existing identities or retry payloads.
            if db.execute('SELECT 1 FROM jobs WHERE job_key=?', (key,)).fetchone():
                return
            self.queue._insert(key, 'extract', dict(payload, order=order))
            return

    def _enqueue_night(self, now, zone, window=None, force=False, exclude=()):
        db = self.store._db
        local = now.astimezone(zone)
        start, end = window or night_window()
        minute = local.hour * 60 + local.minute
        if start < end:
            within = start <= minute < end
            anchor = local.date()
        else:
            # A window across midnight is one night named after the date it started on,
            # so passing midnight neither reopens nor refreezes the candidate boundary.
            within = minute >= start or minute < end
            anchor = local.date() if minute >= start else local.date() - timedelta(days=1)
        night = zone.key + '/' + anchor.isoformat()
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
        previous = db.execute("SELECT payload FROM jobs WHERE kind='consolidate' ORDER BY rowid DESC LIMIT 1").fetchone()
        prefer_repair = previous is None or not json.loads(previous['payload']).get('repair_group_id')
        if prefer_repair and self._enqueue_repair(exclude):
            return True
        if force:
            night, cursor = None, 0
            through = db.execute('SELECT coalesce(max(rowid),0) FROM candidates').fetchone()[0]
        elif row is None:
            through = db.execute('SELECT coalesce(max(rowid),0) FROM candidates').fetchone()[0]
            if through == 0:
                return self._enqueue_repair(exclude)
            db.execute('INSERT INTO consolidation_nights(id,through_rowid) VALUES (?,?)', (night, through))
            cursor = 0
        else:
            if row['finished_at'] is not None:
                return self._enqueue_repair(exclude)
            cursor, through = row['cursor'], row['through_rowid']
        candidates = self.consolidator.pending(limit=16, after=cursor, through=through, unreserved=True)
        if not candidates:
            if self._enqueue_repair(exclude):
                return True
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

    def claim(self, *, agent, timezone_name, compaction=None, exclude=(), consolidate_now=False, now=None, kinds=None,
              consolidation_start=None, consolidation_end=None):
        try:
            zone = ZoneInfo(timezone_name)
        except (ZoneInfoNotFoundError, ValueError):
            raise ValueError('timezone must be an installed IANA timezone name') from None
        # The schedule is validated before compaction or any other mutation.
        window = night_window(consolidation_start, consolidation_end)
        now = now or datetime.now(timezone.utc)
        if compaction:
            self.compact(**compaction)

        def schedule():
            with self.store._mutex:
                db = self.store._db
                db.execute('BEGIN IMMEDIATE')
                try:
                    self._enqueue_extract(now)
                    night_due = self._enqueue_night(now, zone, window, force=consolidate_now, exclude=exclude)
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
                handler = self.extractor if kind == 'extract' else self.consolidator
                discovery = self.consolidator.prepare_discovery(job) if kind == 'consolidate' else None
                with self.store._mutex:
                    prior = self.store._db.execute('SELECT result FROM graph_commits WHERE command_id=?',
                                                   (kind + '/' + job.id,)).fetchone()
                    if prior:
                        self.queue.complete(job, json.loads(prior['result']))
                        schedule()
                        continue
                    # Bounded feedback from this job's last rejected output survives in
                    # the private context until a later rejection replaces it.
                    previous = self.store._db.execute('SELECT context FROM jobs WHERE id=?', (job.id,)).fetchone()
                    rejection = (json.loads(previous['context']).get('previous_rejection')
                                 if previous and previous['context'] else None)
                    context = (handler.prepare(job, discovery=discovery) if kind == 'consolidate' else handler.prepare(job))
                    if rejection is not None:
                        context['previous_rejection'] = rejection
                    self.store._db.execute('UPDATE jobs SET context=? WHERE id=? AND owner=?',
                        (json.dumps(context, ensure_ascii=False), job.id, job.owner))
                model_input = handler.model_input(context)
                if kind == 'extract' and not any(item['text'] for item in context['windows']):
                    output = dict(windows=[dict(id=item['id'], candidates=[]) for item in model_input['windows']])
                elif kind == 'consolidate' and not context['candidates'] and not context.get('maintenance_ids'):
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
            backlog = dict(pending_extract_conversations=db.execute(pending_conversations_sql()).fetchone()[0],
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
