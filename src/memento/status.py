"""Read-only operational counts; observing status never schedules processing."""

import time

from .extraction import ACTIVE_CANDIDATE_SQL, AWAITING_DECISION_SQL
from .consolidation import CONSOLIDATION_ELIGIBLE_SQL
from .captured_media import status as captured_media_status
from .processing import PARKED_ERROR, historical_status, pending_conversations_sql


_JOB_ERROR_CATEGORIES = frozenset({
    'cli_failed', 'invalid_output', 'service_rejected', 'night_window_closed',
    'ValueError', 'ConflictError', 'OSError', 'FileNotFoundError',
    'PermissionError', 'TimeoutError', 'RuntimeError',
    'CapturedSourceUnavailable', PARKED_ERROR,
})


def _conversation_count(db, source):
    # Seek distinct keys in the existing index, including raw/forgotten history
    # that may have no processing progress. Empty strings are ordinary keys.
    sources = '''SELECT min(source) FROM episodes
        UNION ALL
        SELECT (SELECT min(e.source) FROM episodes e WHERE e.source>s.source)
        FROM sources s WHERE s.source IS NOT NULL''' if source is None else 'VALUES (?)'
    return db.execute(f'''WITH RECURSIVE sources(source) AS ({sources}),
        conversations(source,conversation_id) AS (
        SELECT s.source,(SELECT min(e.conversation_id) FROM episodes e WHERE e.source=s.source)
            FROM sources s WHERE s.source IS NOT NULL
        UNION ALL
        SELECT c.source,(SELECT min(e.conversation_id) FROM episodes e
            WHERE e.source=c.source AND e.conversation_id>c.conversation_id)
        FROM conversations c WHERE c.conversation_id IS NOT NULL)
        SELECT count(*) FROM conversations WHERE conversation_id IS NOT NULL''',
        () if source is None else (source,)).fetchone()[0]


def operational_status(state, source=None):
    # A bound equality lets source-specific observations use the existing index.
    source_where = ' WHERE source=?' if source is not None else ''
    source_args = (source,) if source is not None else ()
    with state.store.read_snapshot() as db:
        captured = dict(episodes=db.execute(
            'SELECT count(*) FROM episodes' + source_where, source_args).fetchone()[0],
            conversations=_conversation_count(db, source))
        raw_storage = dict(db.execute(f'''SELECT
            coalesce(sum(text_runs IS NULL),0) AS inline_raw_episodes,
            coalesce(sum(CASE (text_runs IS NULL) WHEN 1 THEN length(CAST(text AS BLOB)) ELSE 0 END),0) AS inline_raw_bytes,
            coalesce(sum(NOT (text_runs IS NULL)),0) AS offloaded_raw_episodes
            FROM episodes INDEXED BY episodes_status_cover_v1
            WHERE kind IN ('native_record','native_live_record','native_hook')
            AND CAST(coalesce(json_extract(metadata,'$.extraction_mode'),'message') AS TEXT)='skip'
            {'AND source=?' if source is not None else ''}''', source_args).fetchone())
        compressed = db.execute('''SELECT coalesce(sum(record_count),0),coalesce(sum(raw_size),0),
            coalesce(sum(length(data)),0) FROM raw_frames INDEXED BY raw_frames_status_cover_v1'''
            + source_where, source_args).fetchone()
        raw_storage['inline_raw_episodes'] -= compressed[0]
        raw_storage.update(compressed_raw_episodes=compressed[0], compressed_original_bytes=compressed[1],
                           compressed_raw_bytes=compressed[2])
        objects = db.execute('''SELECT count(*) AS objects,coalesce(sum(size),0) AS bytes,
            coalesce(sum(remote_id IS NULL),0) AS local_objects,
            coalesce(sum(CASE WHEN remote_id IS NULL THEN size ELSE 0 END),0) AS local_bytes,
            coalesce(sum(remote_id IS NOT NULL),0) AS remote_objects,
            coalesce(sum(CASE WHEN remote_id IS NOT NULL THEN size ELSE 0 END),0) AS remote_bytes
            FROM blobs''').fetchone()
        artifact_storage = dict(scope='global',
            registered=dict(objects=objects['objects'], bytes=objects['bytes']),
            local_only=dict(objects=objects['local_objects'], bytes=objects['local_bytes']),
            verified_remote=dict(objects=objects['remote_objects'], bytes=objects['remote_bytes']))

        def jobs(kind):
            # A consolidation can contain several origins; count the job for
            # every source it actually cites rather than inventing an owner.
            origin = "json_extract(j.payload,'$.source')=?" if kind == 'extract' else (
                '''EXISTS (SELECT 1 FROM json_each(j.payload,'$.candidate_ids') c
                    JOIN graph_evidence g ON g.target=c.value
                    JOIN episodes e ON e.id=g.episode_id WHERE e.source=?)''' if kind == 'consolidate' else
                '''EXISTS (SELECT 1 FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id
                    WHERE g.target=j.id AND e.source=?)''')
            rows = db.execute(f'''SELECT state,error,count(*) AS count FROM jobs j
                WHERE kind=? AND (? IS NULL OR {origin}) GROUP BY state,error''',
                (kind, source, source)).fetchall()
            result = dict(available=True, pending_jobs=0, leased_jobs=0, completed_jobs=0,
                          failed_jobs=0, retrying_jobs=0, parked_jobs=0)
            fields = dict(pending='pending_jobs', leased='leased_jobs', done='completed_jobs', failed='failed_jobs')
            errors = {}
            for row in rows:
                if row['state'] == 'failed' and row['error'] == PARKED_ERROR:
                    # Fenced by the historical cutover, not an operational failure; the
                    # stored job state and its diagnostic code stay as they are.
                    result['parked_jobs'] += row['count']
                    continue
                result[fields[row['state']]] += row['count']
                if row['state'] == 'pending' and row['error'] is not None:
                    result['retrying_jobs'] += row['count']
                if row['state'] in ('pending', 'failed') and row['error'] is not None:
                    # Only fixed categories may leave the service, never arbitrary stored error text.
                    category = row['error'] if row['error'] in _JOB_ERROR_CATEGORIES else 'unknown_error'
                    key = row['state'], category
                    errors[key] = errors.get(key, 0) + row['count']
            result['errors'] = [dict(state=state, category=category, count=count)
                                for (state, category), count in sorted(errors.items())]
            return result

        extraction = jobs('extract')
        # Pending counts what admission can still extract. A parked conversation is
        # pending only beyond its live cursor; its parked range is reported separately.
        extraction['pending_conversations'] = db.execute(
            pending_conversations_sql('AND (? IS NULL OR p.source=?)'), (source, source)).fetchone()[0]
        extraction['parked_conversations'] = db.execute('''SELECT count(*) FROM historical_parking h
            WHERE (? IS NULL OR h.source=?)''', (source, source)).fetchone()[0]
        # Rows without parking keep their exact previous shape; in every row cursor >=
        # compact_through remains the prefix-extracted proof, parked ranges are explicit.
        extraction['progress'] = [
            {key: value for key, value in dict(row).items()
             if row['parked_through'] is not None or key not in ('parked_through', 'live_cursor', 'live_offset')}
            for row in db.execute('''SELECT p.source,p.conversation_id,p.seq AS cursor,p.offset,p.compact_through,
                p.latest_seq,h.parked_through,h.live_seq AS live_cursor,h.live_offset
                FROM processing_progress p LEFT JOIN historical_parking h
                ON h.source=p.source AND h.conversation_id=p.conversation_id
                WHERE (? IS NULL OR p.source=?) ORDER BY p.source,p.conversation_id''', (source, source))]
        consolidation, media = jobs('consolidate'), jobs('media')
        media['captured_attachments'] = captured_media_status(db, source)
        # Pending/ready count what consolidation can still admit. Active candidates whose
        # only evidence is legacy Codex instruction text are reported separately: they
        # stay stored, searchable and in history, and never reach a model call.
        # MATERIALIZED keeps SQLite from flattening the subquery and evaluating the
        # eligibility predicate once per output column (measured 3x UDF calls otherwise).
        # Parked candidates (explicit historical activation) are counted apart from
        # pending/ready and from the legacy exclusion: a drop in pending is parking,
        # not model completion. They stay stored, searchable and in history.
        pending = db.execute(f'''WITH candidate_status AS MATERIALIZED (
            SELECT ({CONSOLIDATION_ELIGIBLE_SQL}) AS eligible,
                NOT EXISTS (SELECT 1 FROM candidate_decisions d WHERE d.candidate_id=c.id
                    AND d.rowid=(SELECT max(x.rowid) FROM candidate_decisions x WHERE x.candidate_id=c.id)
                    AND {AWAITING_DECISION_SQL}) AS ready,
                EXISTS (SELECT 1 FROM historical_parked_candidates h
                    WHERE h.candidate_id=c.id AND h.released_at IS NULL) AS parked
            FROM candidates c WHERE {ACTIVE_CANDIDATE_SQL}
            AND (? IS NULL OR EXISTS (SELECT 1 FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id
                WHERE g.target=c.id AND e.source=?)))
            SELECT coalesce(sum(eligible AND NOT parked),0) AS pending_candidates,
            coalesce(sum(eligible AND ready AND NOT parked),0) AS ready_candidates,
            coalesce(sum(NOT eligible),0) AS excluded_legacy_candidates,
            coalesce(sum(eligible AND parked),0) AS parked_candidates
            FROM candidate_status''', (time.time(), source, source)).fetchone()
        consolidation.update(dict(pending))
        consolidation['repair_groups'] = dict(scope='global', **dict(db.execute('''SELECT
            coalesce(sum(state='pending'),0) AS pending,coalesce(sum(state='done'),0) AS done
            FROM repair_groups''').fetchone()))
        # Parking is one database-wide cutover, so this section is global even in
        # source-filtered status; the consolidation counts above stay scoped.
        historical = dict(scope='global', **historical_status(db))
        indexing = (state.retrieval._status(db, source) if state.retrieval is not None else
                    dict(available=False, reason='embedding_runtime_disabled'))
    cold = state.cold_runtime
    return dict(source=source, captured=captured, raw_storage=raw_storage, artifact_storage=artifact_storage,
                indexing=indexing, extraction=extraction,
                consolidation=consolidation, historical=historical, media=media,
                drive=dict(connected=state.cold is not None,
                           reauthorization_required=state.drive_reauthorization_required,
                           archive_failure_count=cold.sweep_failure_count if cold is not None else None,
                           archive_error=cold.last_error if cold is not None else None))
