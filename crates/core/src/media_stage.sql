-- Mutable derived unit progress only; original artifacts/history stay in authority tables.
CREATE TABLE media_stage_units (
    job_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK(ordinal>=0),
    identity TEXT NOT NULL,
    metadata TEXT NOT NULL CHECK(json_valid(metadata)),
    embedding BLOB NOT NULL CHECK(length(embedding)=3072),
    PRIMARY KEY(job_id,ordinal)
) STRICT;
