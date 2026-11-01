-- Disposable covering indexes; no authority fields, UDFs or format change.
CREATE INDEX IF NOT EXISTS vector_blobs_status_current_v1 ON vector_blobs(seq,current);
-- Communicated user/assistant messages, the only raw episodes with vectors.
-- WHERE text must match indexing.rs `message!` so lookups stay index-only.
CREATE INDEX IF NOT EXISTS episodes_status_indexable_v2 ON episodes(id)
    WHERE kind IN ('user','assistant')
    AND CAST(coalesce(json_extract(metadata,'$.extraction_mode'),'message') AS TEXT)<>'skip'
    AND coalesce(json_extract(metadata,'$.evidence_eligible'),1)<>0;
DROP INDEX IF EXISTS episodes_status_indexable_v1;
CREATE INDEX IF NOT EXISTS episodes_status_raw_v1 ON episodes((text_runs IS NULL),length(CAST(text AS BLOB)))
    WHERE kind IN ('native_record','native_live_record','native_hook')
    AND CAST(coalesce(json_extract(metadata,'$.extraction_mode'),'message') AS TEXT)='skip';
