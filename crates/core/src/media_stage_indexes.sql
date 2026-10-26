CREATE INDEX IF NOT EXISTS media_stage_artifact ON media_stage_units(json_extract(metadata,'$.artifact_sha256')) WHERE json_extract(metadata,'$.artifact_sha256') IS NOT NULL;
CREATE INDEX IF NOT EXISTS media_stage_source ON media_stage_units(json_extract(metadata,'$.source_sha256')) WHERE json_extract(metadata,'$.source_sha256') IS NOT NULL;
