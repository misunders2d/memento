-- Disposable covering indexes; no authority fields, UDFs or format change.
CREATE INDEX IF NOT EXISTS vector_blobs_status_current_v1 ON vector_blobs(seq,current);
CREATE INDEX IF NOT EXISTS episodes_status_indexable_v1 ON episodes(id)
    WHERE CAST(coalesce(json_extract(metadata,'$.extraction_mode'),'message') AS TEXT)<>'skip';
CREATE INDEX IF NOT EXISTS episodes_status_raw_v1 ON episodes((text_runs IS NULL),length(CAST(text AS BLOB)))
    WHERE kind IN ('native_record','native_live_record','native_hook')
    AND CAST(coalesce(json_extract(metadata,'$.extraction_mode'),'message') AS TEXT)='skip';
