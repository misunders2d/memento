"""Bounded discovery of captured attachment references; no raw-history hydration."""

import hashlib
import json
import mimetypes
import re
import time
from pathlib import PurePath

BATCH = 64
MAX_PROJECTED_CHARS = 1024 * 1024
MAX_STUBS = 128
MAX_AUTOMATIC_JOBS = 2
_SHA = re.compile(r'[0-9a-f]{64}')
_TYPES = {'image': 'image', 'input_image': 'image', 'local_image': 'image', 'image_url': 'image',
          'audio': 'audio', 'input_audio': 'audio', 'video': 'video', 'document': 'document', 'file': 'document'}
_CONTEXT_KINDS = {'hooks.additional_context', 'agents_md.instructions', 'environments.environment_context'}
_ACTIVE = '''NOT EXISTS (SELECT 1 FROM forgotten_sources f
    WHERE f.source=e.source AND f.event_id=e.event_id)'''
_REF_SELECT = '''SELECT r.episode_id,e.source,e.conversation_id,e.occurred_at,e.captured_at,
    r.start,r.end,r.quote FROM captured_media_refs r JOIN episodes e ON e.id=r.episode_id'''


def visible_sql(node='n.id'):
    """Internal SQL expression; node is a source-code expression, never user input."""
    return f'''(NOT EXISTS (SELECT 1 FROM captured_media cm WHERE cm.node_id={node}) OR EXISTS (
        SELECT 1 FROM captured_media cm JOIN captured_media_refs r ON r.sha256=cm.sha256
        JOIN episodes e ON e.id=r.episode_id WHERE cm.node_id={node} AND {_ACTIVE}
        AND NOT EXISTS (SELECT 1 FROM media_files m JOIN forgotten_targets f ON f.target=m.node_id
            WHERE m.sha256=cm.sha256)))'''


def _target_forgotten(db, sha256):
    # A separate explicit media record can have been forgotten before discovery.
    return db.execute('''SELECT 1 FROM media_files m JOIN forgotten_targets f ON f.target=m.node_id
        WHERE m.sha256=? LIMIT 1''', (sha256,)).fetchone() is not None


def active_source(db, sha256):
    """One exact, still-active occurrence for internal ingestion; caller owns snapshot/lock."""
    if _target_forgotten(db, sha256):
        return None
    row = db.execute(_REF_SELECT + f''' WHERE r.sha256=? AND {_ACTIVE}
        ORDER BY e.seq DESC LIMIT 1''', (sha256,)).fetchone()
    return dict(row) if row is not None else None


def active_references(db, node_id):
    """None means explicit media; [] references means captured media is no longer visible."""
    media = db.execute('SELECT sha256 FROM captured_media WHERE node_id=?', (node_id,)).fetchone()
    if media is None:
        return None
    hidden = db.execute('''SELECT 1 FROM forgotten_targets WHERE target=? UNION ALL
        SELECT 1 FROM node_versions WHERE id=? AND version=(SELECT max(version) FROM node_versions WHERE id=?)
        AND state<>'active' LIMIT 1''', (node_id, node_id, node_id)).fetchone()
    if hidden or _target_forgotten(db, media['sha256']):
        return dict(references=[], occurred_at=None, origin=None)
    refs = [dict(row) for row in db.execute(_REF_SELECT + f''' WHERE r.sha256=? AND {_ACTIVE}
        ORDER BY e.seq DESC LIMIT 5''', (media['sha256'],))]
    newest = db.execute(_REF_SELECT + f''' WHERE r.sha256=? AND {_ACTIVE}
        AND e.occurred_at IS NOT NULL ORDER BY julianday(e.occurred_at) DESC LIMIT 1''',
        (media['sha256'],)).fetchone()
    return dict(references=refs, occurred_at=newest['occurred_at'] if newest else None,
                origin=dict(newest) if newest else refs[0] if refs else None)


def _stubs(value):
    """Walk only bounded projected JSON or metadata, not opaque native records."""
    pending, count = [(value, 0)], 0
    while pending and count < 4096:
        value, depth = pending.pop()
        count += 1
        if isinstance(value, dict):
            typ = value.get('type')
            if 'artifact_status' in value and isinstance(typ, str) and typ.lower() in _TYPES:
                yield value
            elif depth < 16:
                for item in value.values():
                    if len(pending) + count >= 4096:
                        break
                    pending.append((item, depth + 1))
        elif isinstance(value, list) and depth < 16:
            pending.extend((item, depth + 1) for item in reversed(value[:max(0, 4096 - len(pending) - count)]))


def references(source, kind, text, metadata):
    """Normalize an immutable native message projection without changing its payload."""
    if not isinstance(source, str) or re.fullmatch(r'native:[^:]+:[^:]+', source) is None:
        return []
    if kind not in ('user', 'assistant', 'tool') or not isinstance(metadata, dict):
        return []
    def context(key):
        kinds = metadata.get(key)
        return isinstance(kinds, list) and any(isinstance(item, str) and item in _CONTEXT_KINDS for item in kinds)
    if metadata.get('extraction_mode') == 'skip' or context('native_context_kinds') or context('content_item_kinds'):
        return []
    # Use the shared conservative context predicate without a module import cycle.
    from .store import legacy_native_context
    if legacy_native_context(source, kind, text or '', metadata):
        return []
    candidates = list(_stubs(metadata.get('artifacts', [])))
    # Split tool-result projections have normalized stubs in text but no artifacts metadata.
    if isinstance(text, str) and len(text) <= MAX_PROJECTED_CHARS and text[:1] in ('[', '{'):
        try:
            candidates.extend(_stubs(json.loads(text)))
        except (ValueError, RecursionError):
            pass
    found = {}
    for stub in candidates[:MAX_STUBS]:
        sha = stub.get('sha256')
        if not isinstance(sha, str) or not _SHA.fullmatch(sha) or sha in found:
            continue
        typ = str(stub['type']).lower()
        mime = str(stub.get('mime', 'application/octet-stream')).lower()
        media_kind = _TYPES[typ]
        filename = stub.get('filename')
        filename = PurePath(filename).name[:256] if isinstance(filename, str) and filename else ''
        if not filename:
            filename = sha + (mimetypes.guess_extension(mime) or '')
        if typ in ('document', 'file') and mime.startswith(('image/', 'audio/', 'video/')):
            media_kind = mime.split('/', 1)[0]
        unsupported = mime == 'application/x-memento-raw-run'
        # Evidence is the captured reference when rendered, otherwise its true message context.
        start = text.find(sha) if isinstance(text, str) else -1
        quote = sha if start >= 0 else (text[:256] if isinstance(text, str) else '')
        start = max(0, start)
        if not quote:
            continue  # No invented evidence for a metadata-only, empty projection.
        found[sha] = dict(sha256=sha, kind=None if unsupported else media_kind,
                         filename=filename, start=start, end=start + len(quote), quote=quote)
    return list(found.values())


def record_references(db, episode_id, refs):
    """Caller owns capture/backfill transaction; bytes may arrive on either side."""
    for ref in refs:
        exists = db.execute('SELECT 1 FROM blobs WHERE sha256=?', (ref['sha256'],)).fetchone()
        state = 'unsupported' if ref['kind'] is None else 'ready' if exists else 'waiting'
        db.execute('''INSERT OR IGNORE INTO captured_media(sha256,kind,filename,state,error)
            VALUES(?,?,?,?,?)''', (ref['sha256'], ref['kind'], ref['filename'], state,
                                  'unsupported_type' if state == 'unsupported' else None))
        inserted = db.execute('''INSERT OR IGNORE INTO captured_media_refs(sha256,episode_id,start,end,quote)
            VALUES(?,?,?,?,?)''', (ref['sha256'], episode_id, ref['start'], ref['end'], ref['quote']))
        if exists:
            wake_blob(db, ref['sha256'])
        if not _target_forgotten(db, ref['sha256']) and active_source(db, ref['sha256']) is not None:
            db.execute('''UPDATE captured_media SET state=?,error=NULL WHERE sha256=?
                AND state='forgotten' AND error='forgotten_source' AND job_id IS NULL''',
                ('ready' if exists else 'waiting', ref['sha256']))
            if inserted.rowcount:
                # Only genuinely new evidence can recover work whose last carrier was forgotten.
                # Decoder errors, conflicting commands and forgotten media targets are never retried here.
                db.execute('''UPDATE jobs SET state='pending',available_at=?,error=NULL
                    WHERE id=(SELECT job_id FROM captured_media WHERE sha256=?)
                    AND state='failed' AND error='CapturedSourceUnavailable' ''',
                    (time.time(), ref['sha256']))


def wake_blob(db, sha256):
    db.execute("UPDATE captured_media SET state='ready' WHERE sha256=? AND state='waiting'", (sha256,))


class CapturedMedia:
    def __init__(self, queue):
        self.queue, self.store = queue, queue.store

    def discover(self):
        # CASE skips metadata/body reads for huge raw records before touching overflow pages.
        with self.store.read_snapshot() as db:
            cursor = db.execute('SELECT seq FROM captured_media_progress WHERE singleton=1').fetchone()[0]
            rows = db.execute('''SELECT seq,id,source,kind,
                CASE WHEN kind IN ('user','assistant','tool') THEN substr(text,1,?) END AS text,
                CASE WHEN kind IN ('user','assistant','tool') THEN metadata END AS metadata
                FROM episodes WHERE seq>? ORDER BY seq LIMIT ?''',
                (MAX_PROJECTED_CHARS + 1, cursor, BATCH)).fetchall()
            projected = []
            for row in rows:
                if row['metadata'] is None:
                    continue
                metadata = json.loads(row['metadata'])
                text = row['text']
                # Large bodies can still carry usable metadata stubs; no JSON fragment parsing.
                projected.append((row['id'], references(row['source'], row['kind'], text, metadata)))
        if not rows:
            return False
        with self.store._mutex:
            db = self.store._db
            db.execute('BEGIN IMMEDIATE')
            try:
                for episode_id, refs in projected:
                    record_references(db, episode_id, refs)
                db.execute('UPDATE captured_media_progress SET seq=max(seq,?) WHERE singleton=1', (rows[-1]['seq'],))
                db.execute('COMMIT')
            except BaseException:
                db.execute('ROLLBACK')
                raise
        return True

    def admit(self):
        from .ingest import representation_identity
        with self.store._mutex:
            db = self.store._db
            db.execute('BEGIN IMMEDIATE')
            try:
                outstanding = db.execute('''SELECT count(*) FROM captured_media cm
                    JOIN jobs j ON j.id=cm.job_id WHERE j.state IN ('pending','leased')''').fetchone()[0]
                admitted = False
                if outstanding < MAX_AUTOMATIC_JOBS:
                    rows = db.execute("SELECT * FROM captured_media WHERE state='ready' ORDER BY rowid LIMIT ?",
                                      (BATCH,)).fetchall()
                    for row in rows:
                        if outstanding >= MAX_AUTOMATIC_JOBS:
                            break
                        if _target_forgotten(db, row['sha256']):
                            db.execute("UPDATE captured_media SET state='forgotten',error='forgotten_target' WHERE sha256=?",
                                       (row['sha256'],))
                            continue
                        source = active_source(db, row['sha256'])
                        if source is None:
                            # No active carrier; a later capture can make this eligible again.
                            db.execute("UPDATE captured_media SET state='forgotten',error='forgotten_source' WHERE sha256=?",
                                       (row['sha256'],))
                            continue
                        identity = representation_identity(db, row['kind'], row['filename'])
                        key = 'captured-media/' + hashlib.sha256(json.dumps(
                            [row['sha256'], identity], sort_keys=True).encode()).hexdigest()
                        payload = dict(actor='memento:captured-media', why='Index a captured attachment reference',
                            sha256=row['sha256'], kind=row['kind'], filename=row['filename'], evidence=[], scope=[], captured=True)
                        job_id = self.queue._insert(key, 'media', payload)
                        # Job provenance is exact reference context; the live occurrence is rechecked at commit.
                        self.queue.graph._evidence(job_id, 1, [dict(episode_id=source['episode_id'], quote=source['quote'], occurrence=0)])
                        db.execute("UPDATE captured_media SET state='queued',job_id=?,error=NULL WHERE sha256=?",
                                   (job_id, row['sha256']))
                        admitted, outstanding = True, outstanding + 1
                db.execute('COMMIT')
                return admitted
            except BaseException:
                db.execute('ROLLBACK')
                raise

    def step(self):
        discovered = self.discover()
        return self.admit() or discovered


def status(db, source=None):
    where, args = ('', ()) if source is None else (''' WHERE EXISTS (
        SELECT 1 FROM captured_media_refs r JOIN episodes e ON e.id=r.episode_id
        WHERE r.sha256=cm.sha256 AND e.source=?)''', (source,))
    rows = db.execute('''SELECT cm.state,j.state AS job_state,count(*) AS count
        FROM captured_media cm LEFT JOIN jobs j ON j.id=cm.job_id''' + where +
        ' GROUP BY cm.state,j.state', args).fetchall()
    counts = dict(waiting=0, ready=0, queued=0, indexed=0, failed=0, unsupported=0, forgotten=0)
    for row in rows:
        state = row['state']
        if row['job_state'] == 'failed':
            state = 'failed'
        elif state == 'indexed' and row['job_state'] != 'done':
            state = 'queued'
        counts[state] += row['count']
    progress = db.execute('''SELECT seq,(SELECT coalesce(max(seq),0) FROM episodes) AS latest_seq
        FROM captured_media_progress WHERE singleton=1''').fetchone()
    return dict(**counts, discovery=dict(scope='global', through_seq=progress['seq'], latest_seq=progress['latest_seq']))
