"""SQLite schema initialization for a fresh database."""

import logging
import time

# TEXT removes the unused JSON subtype so SQLite 3.46 can substitute this
# expression from the index; status only distinguishes the string 'skip'.
_EPISODES_STATUS_INDEX = """CREATE INDEX IF NOT EXISTS episodes_status_cover_v1 ON episodes (
    source,CAST(coalesce(json_extract(metadata,'$.extraction_mode'),'message') AS TEXT),kind,
    (text_runs IS NULL),length(CAST(text AS BLOB)),id,event_id)"""

_EPISODES_TEXT_INDEX = """CREATE TRIGGER episodes_text_index AFTER INSERT ON episodes
    WHEN NEW.kind NOT IN ('native_record','native_live_record','native_hook')
    AND coalesce(json_extract(NEW.metadata,'$.extraction_mode'),'message')<>'skip'
    BEGIN INSERT INTO episodes_fts(rowid,text) VALUES (NEW.seq,NEW.text); END"""

_SCHEMA = (
    """CREATE TABLE episodes (
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
           UNIQUE(source, event_id)
       ) STRICT""",
    'CREATE INDEX episodes_conversation ON episodes(source, conversation_id)',
    'CREATE INDEX episodes_activity ON episodes(julianday(coalesce(occurred_at,captured_at)) DESC,seq DESC)',
    """CREATE VIRTUAL TABLE episodes_fts USING fts5(
           text, content='episodes', content_rowid='seq', tokenize='unicode61'
       )""",
    """CREATE TRIGGER episodes_no_update BEFORE UPDATE ON episodes BEGIN
           SELECT RAISE(ABORT, 'episodes are immutable');
       END""",
    """CREATE TRIGGER episodes_no_delete BEFORE DELETE ON episodes BEGIN
           SELECT RAISE(ABORT, 'episodes are immutable');
       END""",
    """CREATE TRIGGER episodes_no_replace BEFORE INSERT ON episodes
       WHEN EXISTS (SELECT 1 FROM episodes WHERE seq = NEW.seq OR id = NEW.id
                    OR (source = NEW.source AND event_id = NEW.event_id))
       BEGIN SELECT RAISE(ABORT, 'episodes are immutable'); END""",
    _EPISODES_TEXT_INDEX,
    """CREATE TABLE graph_commits (
       id TEXT PRIMARY KEY, command_id TEXT UNIQUE NOT NULL,
       request TEXT NOT NULL, result TEXT NOT NULL,
       actor TEXT NOT NULL, why TEXT NOT NULL, recorded_at TEXT NOT NULL) STRICT""",
    """CREATE TABLE node_versions (
       id TEXT NOT NULL, version INTEGER NOT NULL, commit_id TEXT NOT NULL,
       type TEXT NOT NULL, text TEXT NOT NULL, state TEXT NOT NULL,
       valid_from TEXT, valid_to TEXT, superseded_by TEXT,
       pinned INTEGER NOT NULL, scope TEXT NOT NULL,
       PRIMARY KEY(id,version)) STRICT""",
    """CREATE TABLE edge_versions (
       id TEXT NOT NULL, version INTEGER NOT NULL, commit_id TEXT NOT NULL,
       src TEXT NOT NULL, rel TEXT NOT NULL, dst TEXT NOT NULL,
       state TEXT NOT NULL, PRIMARY KEY(id,version)) STRICT""",
    """CREATE TABLE loop_events (
       id TEXT PRIMARY KEY, target TEXT NOT NULL, commit_id TEXT NOT NULL,
       state TEXT NOT NULL CHECK(state IN ('open','closed')),
       due_at TEXT, deferred_until TEXT) STRICT""",
    'CREATE INDEX loop_target ON loop_events(target)',
    """CREATE TABLE graph_evidence (
       target TEXT NOT NULL, version INTEGER NOT NULL, episode_id TEXT NOT NULL,
       start INTEGER NOT NULL, end INTEGER NOT NULL, quote TEXT NOT NULL,
       PRIMARY KEY(target,version,episode_id,start,end)) STRICT""",
    """CREATE VIRTUAL TABLE node_fts USING
       fts5(id UNINDEXED, version UNINDEXED, text, tokenize='unicode61')""",
    """CREATE TRIGGER node_index AFTER INSERT ON node_versions
       BEGIN INSERT INTO node_fts(id,version,text)
       VALUES (NEW.id,NEW.version,NEW.text); END""",
    'CREATE INDEX edge_src ON edge_versions(src,rel)',
    'CREATE INDEX edge_dst ON edge_versions(dst,rel)',
    """CREATE TABLE vector_model (
       singleton INTEGER PRIMARY KEY CHECK(singleton=1),
       model_id TEXT NOT NULL, revision TEXT NOT NULL, dimensions INTEGER NOT NULL,
       text_encoding INTEGER NOT NULL DEFAULT 0 CHECK(text_encoding IN (0,1))) STRICT""",
    """CREATE TABLE vector_items (
       seq INTEGER PRIMARY KEY, target TEXT NOT NULL, kind TEXT NOT NULL,
       version INTEGER NOT NULL, UNIQUE(target,kind,version)) STRICT""",
    """CREATE VIRTUAL TABLE memory_vectors USING vec0(
       embedding float[768] distance_metric=cosine,current boolean)""",
    """CREATE TABLE review_proposals (
       id TEXT PRIMARY KEY, commit_id TEXT NOT NULL,
       command TEXT NOT NULL, base_versions TEXT NOT NULL) STRICT""",
    """CREATE TABLE review_decisions (
       proposal_id TEXT PRIMARY KEY, commit_id TEXT NOT NULL,
       decision TEXT NOT NULL CHECK(decision IN ('approve','reject'))) STRICT""",
    """CREATE TABLE blobs (
       sha256 TEXT PRIMARY KEY, size INTEGER NOT NULL, mime TEXT NOT NULL,
       tier TEXT NOT NULL CHECK(tier IN ('hot','cold')), remote_id TEXT,
       created_at TEXT NOT NULL) STRICT""",
    """CREATE TABLE media_files (
       node_id TEXT PRIMARY KEY, sha256 TEXT NOT NULL, filename TEXT NOT NULL,
       kind TEXT NOT NULL, commit_id TEXT NOT NULL) STRICT""",
    """CREATE TABLE segments (
       id TEXT PRIMARY KEY, node_id TEXT NOT NULL, modality TEXT NOT NULL,
       text TEXT NOT NULL, artifact_sha256 TEXT NOT NULL,
       start_s REAL, end_s REAL, page INTEGER, char_start INTEGER, char_end INTEGER, source_sha256 TEXT,
       CHECK(start_s IS NULL OR (start_s>=0 AND end_s>start_s))) STRICT""",
    """CREATE VIRTUAL TABLE segment_fts USING
       fts5(id UNINDEXED,text,tokenize='unicode61')""",
    """CREATE TRIGGER segment_index AFTER INSERT ON segments
       BEGIN INSERT INTO segment_fts(id,text) VALUES (NEW.id,NEW.text); END""",
    """CREATE TABLE index_failures (
       target TEXT NOT NULL, version INTEGER NOT NULL, attempts INTEGER NOT NULL,
       error TEXT NOT NULL, next_attempt REAL NOT NULL, resolved INTEGER NOT NULL,
       PRIMARY KEY(target,version)) STRICT""",
    'CREATE INDEX index_failures_due ON index_failures(next_attempt) WHERE resolved=0',
    """CREATE TABLE node_stats (
       node_id TEXT PRIMARY KEY, importance REAL NOT NULL DEFAULT 0.5,
       decay REAL NOT NULL DEFAULT 1, archived INTEGER NOT NULL DEFAULT 0,
       access_count INTEGER NOT NULL DEFAULT 0, last_cited_at TEXT) STRICT""",
    """CREATE TABLE cold_transfers (
       sha256 TEXT PRIMARY KEY, checkpoint TEXT NOT NULL, status TEXT NOT NULL,
       remote_id TEXT, updated_at TEXT NOT NULL) STRICT""",
    """CREATE TABLE blob_moves (
       id INTEGER PRIMARY KEY, sha256 TEXT NOT NULL, phase TEXT NOT NULL,
       remote_id TEXT NOT NULL, at TEXT NOT NULL) STRICT""",
    """CREATE TABLE backups (
       id TEXT PRIMARY KEY, path TEXT UNIQUE NOT NULL, sha256 TEXT NOT NULL,
       size INTEGER NOT NULL, remote_id TEXT, checkpoint TEXT NOT NULL,
       created_at TEXT NOT NULL) STRICT""",
    """CREATE TABLE backup_gaps (
       backup_id TEXT NOT NULL, sha256 TEXT NOT NULL, error TEXT NOT NULL,
       recorded_at TEXT NOT NULL, PRIMARY KEY(backup_id,sha256)) STRICT""",
    """CREATE TABLE cold_failures (
       sha256 TEXT PRIMARY KEY, attempts INTEGER NOT NULL,
       error TEXT NOT NULL, next_attempt REAL NOT NULL) STRICT""",
    """CREATE TABLE jobs (
       id TEXT PRIMARY KEY, job_key TEXT UNIQUE NOT NULL,
       kind TEXT NOT NULL CHECK(kind IN ('media','extract','consolidate')),
       payload TEXT NOT NULL, state TEXT NOT NULL
       CHECK(state IN ('pending','leased','done','failed')),
       available_at REAL NOT NULL, attempts INTEGER NOT NULL,
       owner TEXT, lease_until REAL, error TEXT, result TEXT, context TEXT) STRICT""",
    'CREATE INDEX jobs_ready ON jobs(state,kind,available_at)',
    """CREATE TABLE extraction_runs (
       id TEXT PRIMARY KEY, commit_id TEXT NOT NULL, episode_id TEXT NOT NULL,
       version TEXT NOT NULL, requested_model TEXT NOT NULL, reported_models TEXT NOT NULL,
       usage TEXT NOT NULL, recorded_at TEXT NOT NULL) STRICT""",
    """CREATE TABLE candidates (
       id TEXT PRIMARY KEY, run_id TEXT NOT NULL, kind TEXT NOT NULL
       CHECK(kind IN ('memory','entity','relation')), type TEXT NOT NULL,
       text TEXT NOT NULL, why TEXT NOT NULL, src TEXT, dst TEXT,
       confidence REAL NOT NULL CHECK(confidence>=0 AND confidence<=1),
       valid_from TEXT, valid_to TEXT, scope TEXT NOT NULL) STRICT""",
    """CREATE TABLE candidate_decisions (
       id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL, commit_id TEXT NOT NULL,
       decision TEXT NOT NULL CHECK(decision IN ('promote','reject','defer','review')),
       target_id TEXT, next_review_at REAL) STRICT""",
    'CREATE INDEX candidate_run ON candidates(run_id)',
    'CREATE INDEX candidate_decision_target ON candidate_decisions(candidate_id)',
    """CREATE VIRTUAL TABLE candidate_fts USING
       fts5(id UNINDEXED,text,tokenize='unicode61')""",
    """CREATE TRIGGER candidate_index AFTER INSERT ON candidates
       BEGIN INSERT INTO candidate_fts(id,text) VALUES(NEW.id,NEW.text); END""",
    """CREATE TABLE episode_chunks (
       id TEXT PRIMARY KEY, episode_id TEXT NOT NULL, start INTEGER NOT NULL,
       end INTEGER NOT NULL CHECK(end>=start AND start>=0),
       UNIQUE(episode_id,start,end)) STRICT""",
    """CREATE TABLE candidate_entity_matches (
       candidate_id TEXT NOT NULL, target TEXT NOT NULL, version INTEGER NOT NULL,
       cosine REAL, name_match INTEGER NOT NULL, actor TEXT NOT NULL,
       why TEXT NOT NULL, recorded_at TEXT NOT NULL,
       PRIMARY KEY(candidate_id,target,version)) STRICT""",
    """CREATE TABLE memory_types (
       name TEXT PRIMARY KEY, category TEXT NOT NULL CHECK(category IN ('memory','entity')),
       sensitive INTEGER NOT NULL CHECK(sensitive IN (0,1)), commit_id TEXT NOT NULL) STRICT""",
    """CREATE TABLE entity_merges (
       source TEXT PRIMARY KEY, target TEXT NOT NULL, commit_id TEXT NOT NULL) STRICT""",
    """CREATE VIRTUAL TABLE alias_fts USING
       fts5(id UNINDEXED,text,tokenize='unicode61')""",
    """CREATE TABLE forgotten_targets (
       target TEXT PRIMARY KEY, commit_id TEXT NOT NULL, kind TEXT NOT NULL) STRICT""",
    """CREATE TABLE forgotten_sources (
       source TEXT NOT NULL, event_id TEXT NOT NULL, target TEXT NOT NULL, commit_id TEXT NOT NULL,
       PRIMARY KEY(source,event_id,target)) STRICT""",
    """CREATE TABLE processing_progress (
       source TEXT NOT NULL, conversation_id TEXT NOT NULL,
       seq INTEGER NOT NULL DEFAULT 0, offset INTEGER NOT NULL DEFAULT 0,
       compact_through INTEGER NOT NULL DEFAULT 0,
       latest_seq INTEGER NOT NULL DEFAULT 0, latest_at TEXT,
       PRIMARY KEY(source,conversation_id)) STRICT""",
    'CREATE INDEX processing_pending ON processing_progress(latest_seq) WHERE latest_seq>seq',
    """CREATE TABLE consolidation_nights (
       id TEXT PRIMARY KEY, through_rowid INTEGER NOT NULL,
       cursor INTEGER NOT NULL DEFAULT 0, finished_at TEXT) STRICT""",
    """CREATE TABLE query_feedback (
       query_key TEXT NOT NULL, target TEXT NOT NULL,
       shown INTEGER NOT NULL DEFAULT 0, cited INTEGER NOT NULL DEFAULT 0,
       PRIMARY KEY(query_key,target)) STRICT""",
    'CREATE INDEX media_blob ON media_files(sha256)',
    'CREATE INDEX segment_blob ON segments(artifact_sha256)',
    'CREATE INDEX segment_source_blob ON segments(source_sha256)',
    'CREATE INDEX vector_kind ON vector_items(kind,seq)',
    """CREATE TRIGGER candidate_vector_terminal AFTER INSERT ON candidate_decisions
       WHEN NEW.decision IN ('promote','reject')
       BEGIN UPDATE memory_vectors SET current=0 WHERE rowid IN (
           SELECT seq FROM vector_items WHERE kind='candidate' AND target=NEW.candidate_id); END""",
    """CREATE TRIGGER forgotten_candidate_vector AFTER INSERT ON forgotten_targets
       WHEN NEW.kind='candidate'
       BEGIN UPDATE memory_vectors SET current=0 WHERE rowid IN (
           SELECT seq FROM vector_items WHERE kind='candidate' AND target=NEW.target); END""",
    """CREATE TRIGGER forgotten_source_vectors AFTER INSERT ON forgotten_sources
       BEGIN UPDATE memory_vectors SET current=0 WHERE rowid IN (
           SELECT i.seq FROM vector_items i JOIN episode_chunks x ON x.id=i.target
           JOIN episodes e ON e.id=x.episode_id WHERE i.kind='episode'
               AND e.source=NEW.source AND e.event_id=NEW.event_id
           UNION SELECT i.seq FROM vector_items i JOIN candidates c ON c.id=i.target
           JOIN extraction_runs r ON r.id=c.run_id JOIN episodes e ON e.id=r.episode_id
           WHERE i.kind='candidate' AND e.source=NEW.source AND e.event_id=NEW.event_id); END""",
)

_IMMUTABLE_KEYS = {'graph_commits': 'id=NEW.id OR command_id=NEW.command_id',
 'node_versions': 'id=NEW.id AND version=NEW.version',
 'edge_versions': 'id=NEW.id AND version=NEW.version',
 'graph_evidence': 'target=NEW.target AND version=NEW.version\n'
                   '                        AND episode_id=NEW.episode_id AND start=NEW.start AND '
                   'end=NEW.end',
 'blob_moves': 'id=NEW.id',
 'review_proposals': 'id=NEW.id',
 'review_decisions': 'proposal_id=NEW.proposal_id',
 'media_files': 'node_id=NEW.node_id',
 'segments': 'id=NEW.id',
 'extraction_runs': 'id=NEW.id',
 'candidates': 'id=NEW.id',
 'candidate_decisions': 'id=NEW.id',
 'loop_events': 'id=NEW.id',
 'memory_types': 'name=NEW.name',
 'entity_merges': 'source=NEW.source',
 'forgotten_targets': 'target=NEW.target',
 'forgotten_sources': 'source=NEW.source AND event_id=NEW.event_id AND target=NEW.target',
 'backup_gaps': 'backup_id=NEW.backup_id AND sha256=NEW.sha256',
 'episode_chunks': 'id=NEW.id OR (episode_id=NEW.episode_id AND start=NEW.start AND end=NEW.end)',
 'candidate_entity_matches': 'candidate_id=NEW.candidate_id AND target=NEW.target AND '
                             'version=NEW.version'}


def initialize(db):
    for statement in _SCHEMA:
        db.execute(statement)
    upgrade_status_index(db)
    for table, predicate in _IMMUTABLE_KEYS.items():
        for operation in ("UPDATE", "DELETE"):
            db.execute(f"""CREATE TRIGGER {table}_no_{operation} BEFORE {operation} ON {table}
                BEGIN SELECT RAISE(ABORT, 'graph history is immutable'); END""")
        db.execute(f"""CREATE TRIGGER {table}_no_replace BEFORE INSERT ON {table}
            WHEN EXISTS (SELECT 1 FROM {table} WHERE {predicate})
            BEGIN SELECT RAISE(ABORT, 'graph history is immutable'); END""")


def upgrade_episode_fts(db):
    """Caller owns the transaction; keep raw native records out of derived FTS.

    The canonical filtered trigger is also the migration marker. Both its
    publication and removal of legacy terms roll back together after interruption.
    No evidence table or portable database format changes.
    """
    if db.execute("SELECT 1 FROM sqlite_schema WHERE type='trigger' AND name='episodes_text_index'").fetchone():
        return
    db.execute('''INSERT INTO episodes_fts(episodes_fts,rowid,text)
        SELECT 'delete',seq,text FROM episodes
        WHERE kind IN ('native_record','native_live_record','native_hook')
        OR coalesce(json_extract(metadata,'$.extraction_mode'),'message')='skip' ''')
    db.execute('DROP TRIGGER IF EXISTS episodes_index')
    db.execute(_EPISODES_TEXT_INDEX)


def upgrade_raw_text(db):
    """Only physical columns: existing immutable source text is never rewritten."""
    columns = {row[1] for row in db.execute('PRAGMA table_info(episodes)')}
    for column in ('text_runs', 'text_sha256'):
        if column not in columns:
            db.execute(f'ALTER TABLE episodes ADD COLUMN {column} TEXT')


def upgrade_status_index(db):
    """Caller owns the transaction; raw physical columns must already exist."""
    if db.execute("SELECT 1 FROM sqlite_schema WHERE type='index' AND name='episodes_status_cover_v1'").fetchone():
        return
    log = logging.getLogger(__name__)
    log.warning('Building episode status index')
    started = time.perf_counter()
    db.execute(_EPISODES_STATUS_INDEX)
    log.warning('Episode status index build completed in %.3f seconds', time.perf_counter() - started)
