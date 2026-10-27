-- Derived endpoint access paths for the unchanged pending relation query.
CREATE INDEX IF NOT EXISTS search_candidate_relation_src_v1 ON candidates(src,id)
    WHERE kind='relation';
CREATE INDEX IF NOT EXISTS search_candidate_relation_dst_v1 ON candidates(dst,id)
    WHERE kind='relation';
