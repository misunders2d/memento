-- Same-identity content revisions: node_versions(id,version) holds the new
-- content; this records why that version replaced the previous content.
-- source_id/source_version name the legacy replacement node whose content an
-- owner recovery folded back into this identity; NULL for ordinary revisions.
CREATE TABLE IF NOT EXISTS node_revisions (
       id TEXT NOT NULL, version INTEGER NOT NULL,
       kind TEXT NOT NULL CHECK(kind IN ('refinement','correction','change')),
       valid_at TEXT, commit_id TEXT NOT NULL,
       source_id TEXT, source_version INTEGER,
       PRIMARY KEY(id,version)) STRICT;

CREATE TRIGGER IF NOT EXISTS node_revisions_no_UPDATE BEFORE UPDATE ON node_revisions BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER IF NOT EXISTS node_revisions_no_DELETE BEFORE DELETE ON node_revisions BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER IF NOT EXISTS node_revisions_no_replace BEFORE INSERT ON node_revisions WHEN EXISTS(SELECT 1 FROM node_revisions WHERE id=NEW.id AND version=NEW.version) BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;
