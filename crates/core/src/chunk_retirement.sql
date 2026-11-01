-- Chunk rows of raw episodes that are never indexable (tool output, tool-call turns,
-- hooks, instructions, skip or evidence-ineligible) are bookkeeping once vectorless
-- and uncited; only those may be deleted. Every other delete stays refused.
-- The message test must match status_indexes.sql `episodes_status_indexable_v2`;
-- INDEXED BY keeps it index-only and fails closed if that index is missing.
DROP TRIGGER IF EXISTS episode_chunks_no_DELETE;
CREATE TRIGGER IF NOT EXISTS episode_chunks_no_DELETE_v2 BEFORE DELETE ON episode_chunks
    WHEN NOT (EXISTS(SELECT 1 FROM episodes e WHERE e.id=OLD.episode_id)
        AND NOT EXISTS(SELECT 1 FROM episodes m INDEXED BY episodes_status_indexable_v2
            WHERE m.id=OLD.episode_id AND m.kind IN ('user','assistant')
            AND CAST(coalesce(json_extract(m.metadata,'$.extraction_mode'),'message') AS TEXT)<>'skip'
            AND coalesce(json_extract(m.metadata,'$.evidence_eligible'),1)<>0)
        AND NOT EXISTS(SELECT 1 FROM vector_items i WHERE i.target=OLD.id)
        AND NOT EXISTS(SELECT 1 FROM node_stats s WHERE s.node_id=OLD.id))
    BEGIN SELECT RAISE(ABORT, 'graph history is immutable'); END;
