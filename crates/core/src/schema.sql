-- Header5 authority: baseline domain/immutability/FTS; plain FP32 blobs; no ANN invalidation triggers.

CREATE TABLE episodes (
           seq INTEGER PRIMARY KEY CHECK(seq > 0),
           id TEXT UNIQUE NOT NULL,
           source TEXT NOT NULL,
           event_id TEXT NOT NULL,
           conversation_id TEXT NOT NULL,
           kind TEXT NOT NULL,
           text TEXT NOT NULL,
           occurred_at TEXT,
           metadata TEXT NOT NULL,
           captured_at TEXT NOT NULL,
           text_runs TEXT,
           text_sha256 TEXT,
           raw_frame TEXT,
           UNIQUE(source, event_id)
       ) STRICT;

CREATE INDEX episodes_conversation ON episodes(source, conversation_id);

CREATE INDEX episodes_activity ON episodes(julianday(coalesce(occurred_at,captured_at)) DESC,seq DESC);

CREATE VIRTUAL TABLE episodes_fts USING fts5(
           text, content='episodes', content_rowid='seq', tokenize='unicode61'
       );

CREATE TRIGGER episodes_no_update BEFORE UPDATE ON episodes
    WHEN NOT (raw_frame_write(OLD.seq,NEW.raw_frame,NEW.text_sha256)=1
        AND OLD.raw_frame IS NULL AND OLD.text_runs IS NULL AND NEW.text=''
        AND OLD.kind IN ('native_record','native_live_record','native_hook')
        AND coalesce(json_extract(OLD.metadata,'$.extraction_mode'),'message')='skip'
        AND NEW.seq IS OLD.seq AND NEW.id IS OLD.id AND NEW.source IS OLD.source
        AND NEW.event_id IS OLD.event_id AND NEW.conversation_id IS OLD.conversation_id
        AND NEW.kind IS OLD.kind AND NEW.occurred_at IS OLD.occurred_at
        AND NEW.metadata IS OLD.metadata AND NEW.captured_at IS OLD.captured_at
        AND NEW.text_runs IS OLD.text_runs)
    BEGIN SELECT RAISE(ABORT, 'episodes are immutable'); END;

CREATE TRIGGER episodes_no_delete BEFORE DELETE ON episodes BEGIN
           SELECT RAISE(ABORT, 'episodes are immutable');
       END;

CREATE TRIGGER episodes_no_replace BEFORE INSERT ON episodes
       WHEN EXISTS (SELECT 1 FROM episodes WHERE seq = NEW.seq OR id = NEW.id
                    OR (source = NEW.source AND event_id = NEW.event_id))
       BEGIN SELECT RAISE(ABORT, 'episodes are immutable'); END;

CREATE TRIGGER episodes_text_index AFTER INSERT ON episodes
    WHEN NEW.kind NOT IN ('native_record','native_live_record','native_hook')
    AND coalesce(json_extract(NEW.metadata,'$.extraction_mode'),'message')<>'skip'
    BEGIN INSERT INTO episodes_fts(rowid,text) VALUES (NEW.seq,NEW.text); END;

CREATE TABLE IF NOT EXISTS raw_frames (
    id INTEGER PRIMARY KEY,source TEXT NOT NULL,conversation_id TEXT NOT NULL,
    raw_size INTEGER NOT NULL CHECK(raw_size BETWEEN 0 AND 262144),
    record_count INTEGER NOT NULL CHECK(record_count>=0),sha256 TEXT NOT NULL,
    data BLOB NOT NULL) STRICT;

CREATE INDEX IF NOT EXISTS raw_frames_status_cover_v1 ON raw_frames (
    source,record_count,raw_size,length(data));

CREATE TRIGGER raw_frames_no_update BEFORE UPDATE ON raw_frames
       BEGIN SELECT RAISE(ABORT, 'raw frames are immutable'); END;

CREATE TRIGGER raw_frames_no_delete BEFORE DELETE ON raw_frames
       BEGIN SELECT RAISE(ABORT, 'raw frames are immutable'); END;

CREATE TRIGGER raw_frames_no_replace BEFORE INSERT ON raw_frames
       WHEN EXISTS (SELECT 1 FROM raw_frames WHERE id=NEW.id)
       BEGIN SELECT RAISE(ABORT, 'raw frames are immutable'); END;

CREATE TABLE graph_commits (
       id TEXT PRIMARY KEY, command_id TEXT UNIQUE NOT NULL,
       request TEXT NOT NULL, result TEXT NOT NULL,
       actor TEXT NOT NULL, why TEXT NOT NULL, recorded_at TEXT NOT NULL) STRICT;

CREATE TABLE node_versions (
       id TEXT NOT NULL, version INTEGER NOT NULL, commit_id TEXT NOT NULL,
       type TEXT NOT NULL, text TEXT NOT NULL, state TEXT NOT NULL,
       valid_from TEXT, valid_to TEXT, superseded_by TEXT,
       pinned INTEGER NOT NULL, scope TEXT NOT NULL,
       PRIMARY KEY(id,version)) STRICT;

CREATE TABLE edge_versions (
       id TEXT NOT NULL, version INTEGER NOT NULL, commit_id TEXT NOT NULL,
       src TEXT NOT NULL, rel TEXT NOT NULL, dst TEXT NOT NULL,
       state TEXT NOT NULL, PRIMARY KEY(id,version)) STRICT;

CREATE TABLE loop_events (
       id TEXT PRIMARY KEY, target TEXT NOT NULL, commit_id TEXT NOT NULL,
       state TEXT NOT NULL CHECK(state IN ('open','closed')),
       due_at TEXT, deferred_until TEXT) STRICT;

CREATE INDEX loop_target ON loop_events(target);

CREATE TABLE graph_evidence (
       target TEXT NOT NULL, version INTEGER NOT NULL, episode_id TEXT NOT NULL,
       start INTEGER NOT NULL, end INTEGER NOT NULL, quote TEXT NOT NULL,
       PRIMARY KEY(target,version,episode_id,start,end)) STRICT;

CREATE VIRTUAL TABLE node_fts USING
       fts5(id UNINDEXED, version UNINDEXED, text, tokenize='unicode61');

CREATE TABLE IF NOT EXISTS node_fts_documents (
    seq INTEGER PRIMARY KEY, id TEXT NOT NULL UNIQUE) STRICT;

CREATE TRIGGER node_index_current_v1 AFTER INSERT ON node_versions
    WHEN NEW.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=NEW.id)
    BEGIN
        INSERT OR IGNORE INTO node_fts_documents(id) VALUES (NEW.id);
        DELETE FROM node_fts WHERE rowid=(SELECT d.seq FROM node_fts_documents d WHERE d.id=NEW.id);
        INSERT INTO node_fts(rowid,id,version,text)
            SELECT d.seq,NEW.id,NEW.version,NEW.text FROM node_fts_documents d WHERE d.id=NEW.id;
    END;

CREATE INDEX edge_src ON edge_versions(src,rel);

CREATE INDEX edge_dst ON edge_versions(dst,rel);

CREATE TABLE vector_model (
       singleton INTEGER PRIMARY KEY CHECK(singleton=1),
       model_id TEXT NOT NULL, revision TEXT NOT NULL, dimensions INTEGER NOT NULL,
       text_encoding INTEGER NOT NULL DEFAULT 0 CHECK(text_encoding IN (0,1))) STRICT;

CREATE TABLE vector_items (
       seq INTEGER PRIMARY KEY, target TEXT NOT NULL, kind TEXT NOT NULL,
       version INTEGER NOT NULL, UNIQUE(target,kind,version)) STRICT;

CREATE TABLE review_proposals (
       id TEXT PRIMARY KEY, commit_id TEXT NOT NULL,
       command TEXT NOT NULL, base_versions TEXT NOT NULL) STRICT;

CREATE TABLE review_decisions (
       proposal_id TEXT PRIMARY KEY, commit_id TEXT NOT NULL,
       decision TEXT NOT NULL CHECK(decision IN ('approve','reject'))) STRICT;

CREATE TABLE blobs (
       sha256 TEXT PRIMARY KEY, size INTEGER NOT NULL, mime TEXT NOT NULL,
       tier TEXT NOT NULL CHECK(tier IN ('hot','cold')), remote_id TEXT,
       created_at TEXT NOT NULL) STRICT;

CREATE TABLE media_files (
       node_id TEXT PRIMARY KEY, sha256 TEXT NOT NULL, filename TEXT NOT NULL,
       kind TEXT NOT NULL, commit_id TEXT NOT NULL) STRICT;

CREATE TABLE segments (
       id TEXT PRIMARY KEY, node_id TEXT NOT NULL, modality TEXT NOT NULL,
       text TEXT NOT NULL, artifact_sha256 TEXT NOT NULL,
       start_s REAL, end_s REAL, page INTEGER, char_start INTEGER, char_end INTEGER, source_sha256 TEXT,
       CHECK(start_s IS NULL OR (start_s>=0 AND end_s>start_s))) STRICT;

CREATE VIRTUAL TABLE segment_fts USING
       fts5(id UNINDEXED,text,tokenize='unicode61');

CREATE TRIGGER segment_index AFTER INSERT ON segments
       BEGIN INSERT INTO segment_fts(id,text) VALUES (NEW.id,NEW.text); END;

CREATE TABLE index_failures (
       target TEXT NOT NULL, version INTEGER NOT NULL, attempts INTEGER NOT NULL,
       error TEXT NOT NULL, next_attempt REAL NOT NULL, resolved INTEGER NOT NULL,
       PRIMARY KEY(target,version)) STRICT;

CREATE INDEX index_failures_due ON index_failures(next_attempt) WHERE resolved=0;

CREATE TABLE node_stats (
       node_id TEXT PRIMARY KEY, importance REAL NOT NULL DEFAULT 0.5,
       decay REAL NOT NULL DEFAULT 1, archived INTEGER NOT NULL DEFAULT 0,
       access_count INTEGER NOT NULL DEFAULT 0, last_cited_at TEXT) STRICT;

CREATE TABLE cold_transfers (
       sha256 TEXT PRIMARY KEY, checkpoint TEXT NOT NULL, status TEXT NOT NULL,
       remote_id TEXT, updated_at TEXT NOT NULL) STRICT;

CREATE TABLE blob_moves (
       id INTEGER PRIMARY KEY, sha256 TEXT NOT NULL, phase TEXT NOT NULL,
       remote_id TEXT NOT NULL, at TEXT NOT NULL) STRICT;

CREATE TABLE backups (
       id TEXT PRIMARY KEY, path TEXT UNIQUE NOT NULL, sha256 TEXT NOT NULL,
       size INTEGER NOT NULL, remote_id TEXT, checkpoint TEXT NOT NULL,
       created_at TEXT NOT NULL) STRICT;

CREATE TABLE backup_gaps (
       backup_id TEXT NOT NULL, sha256 TEXT NOT NULL, error TEXT NOT NULL,
       recorded_at TEXT NOT NULL, PRIMARY KEY(backup_id,sha256)) STRICT;

CREATE TABLE cold_failures (
       sha256 TEXT PRIMARY KEY, attempts INTEGER NOT NULL,
       error TEXT NOT NULL, next_attempt REAL NOT NULL) STRICT;

CREATE TABLE jobs (
       id TEXT PRIMARY KEY, job_key TEXT UNIQUE NOT NULL,
       kind TEXT NOT NULL CHECK(kind IN ('media','extract','consolidate')),
       payload TEXT NOT NULL, state TEXT NOT NULL
       CHECK(state IN ('pending','leased','done','failed')),
       available_at REAL NOT NULL, attempts INTEGER NOT NULL,
       owner TEXT, lease_until REAL, error TEXT, result TEXT, context TEXT) STRICT;

CREATE INDEX jobs_ready ON jobs(state,kind,available_at);

CREATE TABLE extraction_runs (
       id TEXT PRIMARY KEY, commit_id TEXT NOT NULL, episode_id TEXT NOT NULL,
       version TEXT NOT NULL, requested_model TEXT NOT NULL, reported_models TEXT NOT NULL,
       usage TEXT NOT NULL, recorded_at TEXT NOT NULL) STRICT;

CREATE TABLE candidates (
       id TEXT PRIMARY KEY, run_id TEXT NOT NULL, kind TEXT NOT NULL
       CHECK(kind IN ('memory','entity','relation')), type TEXT NOT NULL,
       text TEXT NOT NULL, why TEXT NOT NULL, src TEXT, dst TEXT,
       confidence REAL NOT NULL CHECK(confidence>=0 AND confidence<=1),
       valid_from TEXT, valid_to TEXT, scope TEXT NOT NULL) STRICT;

CREATE TABLE candidate_decisions (
       id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL, commit_id TEXT NOT NULL,
       decision TEXT NOT NULL CHECK(decision IN ('promote','reject','defer','review')),
       target_id TEXT, next_review_at REAL) STRICT;

CREATE INDEX candidate_run ON candidates(run_id);

CREATE INDEX candidate_decision_target ON candidate_decisions(candidate_id);

CREATE VIRTUAL TABLE candidate_fts USING
       fts5(id UNINDEXED,text,tokenize='unicode61');

CREATE TRIGGER candidate_index AFTER INSERT ON candidates
       BEGIN INSERT INTO candidate_fts(id,text) VALUES(NEW.id,NEW.text); END;

CREATE TABLE episode_chunks (
       id TEXT PRIMARY KEY, episode_id TEXT NOT NULL, start INTEGER NOT NULL,
       end INTEGER NOT NULL CHECK(end>=start AND start>=0),
       UNIQUE(episode_id,start,end)) STRICT;

CREATE TABLE candidate_entity_matches (
       candidate_id TEXT NOT NULL, target TEXT NOT NULL, version INTEGER NOT NULL,
       cosine REAL, name_match INTEGER NOT NULL, actor TEXT NOT NULL,
       why TEXT NOT NULL, recorded_at TEXT NOT NULL,
       PRIMARY KEY(candidate_id,target,version)) STRICT;

CREATE TABLE memory_types (
       name TEXT PRIMARY KEY, category TEXT NOT NULL CHECK(category IN ('memory','entity')),
       sensitive INTEGER NOT NULL CHECK(sensitive IN (0,1)), commit_id TEXT NOT NULL) STRICT;

CREATE TABLE entity_merges (
       source TEXT PRIMARY KEY, target TEXT NOT NULL, commit_id TEXT NOT NULL) STRICT;

CREATE VIRTUAL TABLE alias_fts USING
       fts5(id UNINDEXED,text,tokenize='unicode61');

CREATE TABLE forgotten_targets (
       target TEXT PRIMARY KEY, commit_id TEXT NOT NULL, kind TEXT NOT NULL) STRICT;

CREATE TABLE forgotten_sources (
       source TEXT NOT NULL, event_id TEXT NOT NULL, target TEXT NOT NULL, commit_id TEXT NOT NULL,
       PRIMARY KEY(source,event_id,target)) STRICT;

CREATE TABLE processing_progress (
       source TEXT NOT NULL, conversation_id TEXT NOT NULL,
       seq INTEGER NOT NULL DEFAULT 0, offset INTEGER NOT NULL DEFAULT 0,
       compact_through INTEGER NOT NULL DEFAULT 0,
       latest_seq INTEGER NOT NULL DEFAULT 0, latest_at TEXT,
       PRIMARY KEY(source,conversation_id)) STRICT;

CREATE INDEX processing_pending ON processing_progress(latest_seq) WHERE latest_seq>seq;

CREATE TABLE consolidation_nights (
       id TEXT PRIMARY KEY, through_rowid INTEGER NOT NULL,
       cursor INTEGER NOT NULL DEFAULT 0, finished_at TEXT) STRICT;

CREATE TABLE query_feedback (
       query_key TEXT NOT NULL, target TEXT NOT NULL,
       shown INTEGER NOT NULL DEFAULT 0, cited INTEGER NOT NULL DEFAULT 0,
       PRIMARY KEY(query_key,target)) STRICT;

CREATE INDEX media_blob ON media_files(sha256);

CREATE INDEX segment_blob ON segments(artifact_sha256);

CREATE INDEX segment_source_blob ON segments(source_sha256);

CREATE INDEX vector_kind ON vector_items(kind,seq);

CREATE INDEX IF NOT EXISTS episodes_status_cover_v1 ON episodes (
    source,CAST(coalesce(json_extract(metadata,'$.extraction_mode'),'message') AS TEXT),kind,
    (text_runs IS NULL),length(CAST(text AS BLOB)),id,event_id);

CREATE TABLE IF NOT EXISTS captured_media (
        sha256 TEXT PRIMARY KEY,kind TEXT,filename TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('waiting','ready','queued','indexed','unsupported','forgotten')),
        job_id TEXT UNIQUE,node_id TEXT,error TEXT) STRICT;

CREATE INDEX IF NOT EXISTS captured_media_ready ON captured_media(state);

CREATE INDEX IF NOT EXISTS captured_media_node ON captured_media(node_id);

CREATE TABLE IF NOT EXISTS captured_media_refs (
        sha256 TEXT NOT NULL,episode_id TEXT NOT NULL,start INTEGER NOT NULL,end INTEGER NOT NULL,
        quote TEXT NOT NULL,PRIMARY KEY(sha256,episode_id)) STRICT;

CREATE INDEX IF NOT EXISTS captured_media_episode ON captured_media_refs(episode_id);

CREATE TABLE IF NOT EXISTS captured_media_progress (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),seq INTEGER NOT NULL) STRICT;

INSERT OR IGNORE INTO captured_media_progress VALUES(1,0);

CREATE TABLE IF NOT EXISTS media_representations (
        node_id TEXT PRIMARY KEY,identity TEXT NOT NULL) STRICT;

CREATE INDEX IF NOT EXISTS media_representation_identity ON media_representations(identity);

CREATE TABLE IF NOT EXISTS historical_activation (
           singleton INTEGER PRIMARY KEY CHECK(singleton=1), activated_at TEXT NOT NULL) STRICT;

CREATE TABLE IF NOT EXISTS historical_parking (
           source TEXT NOT NULL, conversation_id TEXT NOT NULL,
           parked_through INTEGER NOT NULL, live_seq INTEGER NOT NULL,
           live_offset INTEGER NOT NULL DEFAULT 0,
           PRIMARY KEY(source, conversation_id)) STRICT;

CREATE TABLE IF NOT EXISTS historical_parked_candidates (
           candidate_id TEXT PRIMARY KEY, released_at TEXT) STRICT;

CREATE TABLE IF NOT EXISTS repair_groups (
        id TEXT PRIMARY KEY,seed_ids TEXT NOT NULL,frontier TEXT NOT NULL,reviewed_ids TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('pending','done')),generation INTEGER NOT NULL DEFAULT 0,
        active_job TEXT,available_at REAL NOT NULL DEFAULT 0,origin TEXT,
        created_at TEXT NOT NULL,updated_at TEXT NOT NULL) STRICT;

CREATE INDEX IF NOT EXISTS repair_groups_pending ON repair_groups(state,available_at,created_at);

CREATE TRIGGER graph_commits_no_UPDATE BEFORE UPDATE ON graph_commits BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER graph_commits_no_DELETE BEFORE DELETE ON graph_commits BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER graph_commits_no_replace BEFORE INSERT ON graph_commits WHEN EXISTS(SELECT 1 FROM graph_commits WHERE id=NEW.id OR command_id=NEW.command_id) BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER node_versions_no_UPDATE BEFORE UPDATE ON node_versions BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER node_versions_no_DELETE BEFORE DELETE ON node_versions BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER node_versions_no_replace BEFORE INSERT ON node_versions WHEN EXISTS(SELECT 1 FROM node_versions WHERE id=NEW.id AND version=NEW.version) BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER edge_versions_no_UPDATE BEFORE UPDATE ON edge_versions BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER edge_versions_no_DELETE BEFORE DELETE ON edge_versions BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER edge_versions_no_replace BEFORE INSERT ON edge_versions WHEN EXISTS(SELECT 1 FROM edge_versions WHERE id=NEW.id AND version=NEW.version) BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER graph_evidence_no_UPDATE BEFORE UPDATE ON graph_evidence BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER graph_evidence_no_DELETE BEFORE DELETE ON graph_evidence BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER graph_evidence_no_replace BEFORE INSERT ON graph_evidence WHEN EXISTS(SELECT 1 FROM graph_evidence WHERE target=NEW.target AND version=NEW.version
                        AND episode_id=NEW.episode_id AND start=NEW.start AND end=NEW.end) BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER blob_moves_no_UPDATE BEFORE UPDATE ON blob_moves BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER blob_moves_no_DELETE BEFORE DELETE ON blob_moves BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER blob_moves_no_replace BEFORE INSERT ON blob_moves WHEN EXISTS(SELECT 1 FROM blob_moves WHERE id=NEW.id) BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER review_proposals_no_UPDATE BEFORE UPDATE ON review_proposals BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER review_proposals_no_DELETE BEFORE DELETE ON review_proposals BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER review_proposals_no_replace BEFORE INSERT ON review_proposals WHEN EXISTS(SELECT 1 FROM review_proposals WHERE id=NEW.id) BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER review_decisions_no_UPDATE BEFORE UPDATE ON review_decisions BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER review_decisions_no_DELETE BEFORE DELETE ON review_decisions BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER review_decisions_no_replace BEFORE INSERT ON review_decisions WHEN EXISTS(SELECT 1 FROM review_decisions WHERE proposal_id=NEW.proposal_id) BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER media_files_no_UPDATE BEFORE UPDATE ON media_files BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER media_files_no_DELETE BEFORE DELETE ON media_files BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER media_files_no_replace BEFORE INSERT ON media_files WHEN EXISTS(SELECT 1 FROM media_files WHERE node_id=NEW.node_id) BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER segments_no_UPDATE BEFORE UPDATE ON segments BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER segments_no_DELETE BEFORE DELETE ON segments BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER segments_no_replace BEFORE INSERT ON segments WHEN EXISTS(SELECT 1 FROM segments WHERE id=NEW.id) BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER extraction_runs_no_UPDATE BEFORE UPDATE ON extraction_runs BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER extraction_runs_no_DELETE BEFORE DELETE ON extraction_runs BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER extraction_runs_no_replace BEFORE INSERT ON extraction_runs WHEN EXISTS(SELECT 1 FROM extraction_runs WHERE id=NEW.id) BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER candidates_no_UPDATE BEFORE UPDATE ON candidates BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER candidates_no_DELETE BEFORE DELETE ON candidates BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER candidates_no_replace BEFORE INSERT ON candidates WHEN EXISTS(SELECT 1 FROM candidates WHERE id=NEW.id) BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER candidate_decisions_no_UPDATE BEFORE UPDATE ON candidate_decisions BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER candidate_decisions_no_DELETE BEFORE DELETE ON candidate_decisions BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER candidate_decisions_no_replace BEFORE INSERT ON candidate_decisions WHEN EXISTS(SELECT 1 FROM candidate_decisions WHERE id=NEW.id) BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER loop_events_no_UPDATE BEFORE UPDATE ON loop_events BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER loop_events_no_DELETE BEFORE DELETE ON loop_events BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER loop_events_no_replace BEFORE INSERT ON loop_events WHEN EXISTS(SELECT 1 FROM loop_events WHERE id=NEW.id) BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER memory_types_no_UPDATE BEFORE UPDATE ON memory_types BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER memory_types_no_DELETE BEFORE DELETE ON memory_types BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER memory_types_no_replace BEFORE INSERT ON memory_types WHEN EXISTS(SELECT 1 FROM memory_types WHERE name=NEW.name) BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER entity_merges_no_UPDATE BEFORE UPDATE ON entity_merges BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER entity_merges_no_DELETE BEFORE DELETE ON entity_merges BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER entity_merges_no_replace BEFORE INSERT ON entity_merges WHEN EXISTS(SELECT 1 FROM entity_merges WHERE source=NEW.source) BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER forgotten_targets_no_UPDATE BEFORE UPDATE ON forgotten_targets BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER forgotten_targets_no_DELETE BEFORE DELETE ON forgotten_targets BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER forgotten_targets_no_replace BEFORE INSERT ON forgotten_targets WHEN EXISTS(SELECT 1 FROM forgotten_targets WHERE target=NEW.target) BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER forgotten_sources_no_UPDATE BEFORE UPDATE ON forgotten_sources BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER forgotten_sources_no_DELETE BEFORE DELETE ON forgotten_sources BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER forgotten_sources_no_replace BEFORE INSERT ON forgotten_sources WHEN EXISTS(SELECT 1 FROM forgotten_sources WHERE source=NEW.source AND event_id=NEW.event_id AND target=NEW.target) BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER backup_gaps_no_UPDATE BEFORE UPDATE ON backup_gaps BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER backup_gaps_no_DELETE BEFORE DELETE ON backup_gaps BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER backup_gaps_no_replace BEFORE INSERT ON backup_gaps WHEN EXISTS(SELECT 1 FROM backup_gaps WHERE backup_id=NEW.backup_id AND sha256=NEW.sha256) BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER episode_chunks_no_UPDATE BEFORE UPDATE ON episode_chunks BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER episode_chunks_no_DELETE BEFORE DELETE ON episode_chunks BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER episode_chunks_no_replace BEFORE INSERT ON episode_chunks WHEN EXISTS(SELECT 1 FROM episode_chunks WHERE id=NEW.id OR (episode_id=NEW.episode_id AND start=NEW.start AND end=NEW.end)) BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER candidate_entity_matches_no_UPDATE BEFORE UPDATE ON candidate_entity_matches BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER candidate_entity_matches_no_DELETE BEFORE DELETE ON candidate_entity_matches BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TRIGGER candidate_entity_matches_no_replace BEFORE INSERT ON candidate_entity_matches WHEN EXISTS(SELECT 1 FROM candidate_entity_matches WHERE candidate_id=NEW.candidate_id AND target=NEW.target AND version=NEW.version) BEGIN SELECT RAISE(ABORT,'graph history is immutable'); END;

CREATE TABLE vector_blobs(seq INTEGER PRIMARY KEY,embedding BLOB NOT NULL CHECK(length(embedding)=3072),current INTEGER NOT NULL DEFAULT 1 CHECK(current IN(0,1)),generation INTEGER NOT NULL DEFAULT 0,tier TEXT NOT NULL CHECK(tier IN('hot','cold'))) STRICT;

CREATE TABLE runtime_state(singleton INTEGER PRIMARY KEY CHECK(singleton=1),db_uuid TEXT NOT NULL,generation INTEGER NOT NULL DEFAULT 0) STRICT;

CREATE TABLE projection_events(id INTEGER PRIMARY KEY AUTOINCREMENT,vector_seq INTEGER NOT NULL,tier TEXT NOT NULL CHECK(tier IN('hot','cold')),generation INTEGER NOT NULL,nonce TEXT NOT NULL) STRICT;

CREATE INDEX projection_pending_tier ON projection_events(tier,id);

CREATE TABLE projection_ack(tier TEXT PRIMARY KEY CHECK(tier IN('hot','cold')),id INTEGER NOT NULL,nonce TEXT NOT NULL,generation INTEGER NOT NULL) STRICT;

CREATE INDEX search_evidence_episode ON graph_evidence(episode_id,target);

CREATE INDEX search_chunks_episode ON episode_chunks(episode_id,id);

CREATE INDEX search_runs_episode ON extraction_runs(episode_id,id);

CREATE INDEX search_segments_parent ON segments(node_id,id);

CREATE INDEX search_node_type ON node_versions(type,id);

CREATE INDEX search_candidate_type ON candidates(type,id);
