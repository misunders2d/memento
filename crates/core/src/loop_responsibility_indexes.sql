CREATE INDEX IF NOT EXISTS node_versions_superseded_by
    ON node_versions(superseded_by,id) WHERE superseded_by IS NOT NULL;
CREATE INDEX IF NOT EXISTS candidate_decisions_promoted_target
    ON candidate_decisions(target_id,candidate_id) WHERE decision='promote';
