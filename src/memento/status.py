"""Read-only operational counts; observing status never schedules processing."""

import time

from .extraction import ACTIVE_CANDIDATE_SQL


_JOB_ERROR_CATEGORIES = frozenset({
    'cli_failed', 'invalid_output', 'service_rejected', 'night_window_closed',
    'ValueError', 'ConflictError', 'OSError', 'FileNotFoundError',
    'PermissionError', 'TimeoutError', 'RuntimeError',
})


def operational_status(state, source=None):
    # A bound equality lets source-specific observations use the existing index.
    source_where = ' WHERE source=?' if source is not None else ''
    source_args = (source,) if source is not None else ()
    with state.store.read_snapshot() as db:
        captured = dict(episodes=db.execute(
            'SELECT count(*) FROM episodes' + source_where, source_args).fetchone()[0],
            conversations=db.execute(f'''SELECT count(*) FROM (
                SELECT source,conversation_id FROM episodes{source_where}
                GROUP BY source,conversation_id)''', source_args).fetchone()[0])
        raw_storage = dict(db.execute(f'''SELECT
            coalesce(sum(text_runs IS NULL),0) AS inline_raw_episodes,
            coalesce(sum(CASE (text_runs IS NULL) WHEN 1 THEN length(CAST(text AS BLOB)) ELSE 0 END),0) AS inline_raw_bytes,
            coalesce(sum(NOT (text_runs IS NULL)),0) AS offloaded_raw_episodes
            FROM episodes INDEXED BY episodes_status_cover_v1
            WHERE kind IN ('native_record','native_live_record','native_hook')
            AND CAST(coalesce(json_extract(metadata,'$.extraction_mode'),'message') AS TEXT)='skip'
            {'AND source=?' if source is not None else ''}''', source_args).fetchone())
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
                          failed_jobs=0, retrying_jobs=0)
            fields = dict(pending='pending_jobs', leased='leased_jobs', done='completed_jobs', failed='failed_jobs')
            errors = {}
            for row in rows:
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
        extraction['pending_conversations'] = db.execute('''SELECT count(*) FROM processing_progress
            WHERE latest_seq>seq AND (? IS NULL OR source=?)''', (source, source)).fetchone()[0]
        extraction['progress'] = [dict(row) for row in db.execute('''SELECT source,conversation_id,
            seq AS cursor,offset,compact_through,latest_seq FROM processing_progress
            WHERE (? IS NULL OR source=?) ORDER BY source,conversation_id''', (source, source))]
        consolidation, media = jobs('consolidate'), jobs('media')
        pending = db.execute(f'''SELECT count(*) AS pending_candidates,
            coalesce(sum(NOT EXISTS (SELECT 1 FROM candidate_decisions d WHERE d.candidate_id=c.id
                AND d.rowid=(SELECT max(x.rowid) FROM candidate_decisions x WHERE x.candidate_id=c.id)
                AND (d.decision='review' OR d.next_review_at>?))),0) AS ready_candidates
            FROM candidates c WHERE {ACTIVE_CANDIDATE_SQL}
            AND (? IS NULL OR EXISTS (SELECT 1 FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id
                WHERE g.target=c.id AND e.source=?))''', (time.time(), source, source)).fetchone()
        consolidation.update(dict(pending))
        indexing = (state.retrieval._status(db, source) if state.retrieval is not None else
                    dict(available=False, reason='embedding_runtime_disabled'))
    cold = state.cold_runtime
    return dict(source=source, captured=captured, raw_storage=raw_storage, artifact_storage=artifact_storage,
                indexing=indexing, extraction=extraction,
                consolidation=consolidation, media=media,
                drive=dict(connected=state.cold is not None,
                           reauthorization_required=state.drive_reauthorization_required,
                           archive_failure_count=cold.sweep_failure_count if cold is not None else None,
                           archive_error=cold.last_error if cold is not None else None))
