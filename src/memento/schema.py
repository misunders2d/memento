"""SQLite schema initialization for a fresh database."""

import logging
import time

# Database headers fence readers/writers that cannot preserve the physical
# storage contract: raw frames (2), historical parking (3), vector partitions (4).
LEGACY_VERSION = 1
FRAME_VERSION = 2
VERSION = 4
# Header 3 is written by an explicit historical-pool activation. Images that know
# only headers 1/2 would ignore the parking policy and resume bulk extraction of
# the parked backlog, so they must refuse the file. Nothing ever lowers a header.
PARKING_VERSION = 3
SUPPORTED_VERSIONS = (LEGACY_VERSION, FRAME_VERSION, PARKING_VERSION, VERSION)


def _clean_vector_layout(db):
    row = db.execute("SELECT sql FROM sqlite_schema WHERE name='memory_vectors'").fetchone()
    items = db.execute("SELECT sql FROM sqlite_schema WHERE name='vector_items'").fetchone()
    current = next((column for column in db.execute('PRAGMA table_info(vector_items)')
                    if column[1] == 'current'), None)
    return (row is not None and 'kind text partition key' in ' '.join(row[0].lower().split())
            and 'current' not in {column[1] for column in db.execute('PRAGMA table_info(memory_vectors)')}
            and current is not None and current[2].upper() == 'INTEGER' and current[3] == 1
            and current[4] in ('1', '(1)')
            and items is not None and 'check(currentin(0,1))' in ''.join(items[0].lower().split()))


def check_version(db):
    """Reject an unknown header before any mutation; legacy headers stay readable."""
    version = db.execute('PRAGMA user_version').fetchone()[0]
    if version not in SUPPORTED_VERSIONS:
        raise ValueError(f'Unsupported database schema version: {version}')
    if version >= VERSION and not _clean_vector_layout(db):
        raise ValueError('Database schema version 4 requires immutable vector partitions and ordinary current metadata')
    return version


def header_target(db, current):
    """The current physical format fence; never lower an existing header."""
    return max(current, VERSION)


_HISTORICAL_POOL = (
    # The one explicit cutover receipt. Repeating the operation is a no-op.
    """CREATE TABLE IF NOT EXISTS historical_activation (
           singleton INTEGER PRIMARY KEY CHECK(singleton=1), activated_at TEXT NOT NULL) STRICT""",
    # Parked remaining backlog per conversation: (processing_progress.seq, parked_through]
    # is parked raw history; live_seq/live_offset is the separate cursor for history
    # captured after activation. processing_progress.seq keeps its exact meaning.
    """CREATE TABLE IF NOT EXISTS historical_parking (
           source TEXT NOT NULL, conversation_id TEXT NOT NULL,
           parked_through INTEGER NOT NULL, live_seq INTEGER NOT NULL,
           live_offset INTEGER NOT NULL DEFAULT 0,
           PRIMARY KEY(source, conversation_id)) STRICT""",
    # Candidates parked out of consolidation admission at activation. Actual use
    # records a release; rows are never deleted.
    """CREATE TABLE IF NOT EXISTS historical_parked_candidates (
           candidate_id TEXT PRIMARY KEY, released_at TEXT) STRICT""",
)


def upgrade_historical_pool(db):
    """Small durable policy tables; creating them never parks anything."""
    for statement in _HISTORICAL_POOL:
        db.execute(statement)


def upgrade_repair_groups(db):
    """Explicit bounded maintenance requests; startup never seeds graph work."""
    db.execute('''CREATE TABLE IF NOT EXISTS repair_groups (
        id TEXT PRIMARY KEY,seed_ids TEXT NOT NULL,frontier TEXT NOT NULL,reviewed_ids TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('pending','done')),generation INTEGER NOT NULL DEFAULT 0,
        active_job TEXT,available_at REAL NOT NULL DEFAULT 0,origin TEXT,
        created_at TEXT NOT NULL,updated_at TEXT NOT NULL) STRICT''')
    db.execute('CREATE INDEX IF NOT EXISTS repair_groups_pending ON repair_groups(state,available_at,created_at)')


# TEXT removes the unused JSON subtype so SQLite 3.46 can substitute this
# expression from the index; status only distinguishes the string 'skip'.
_EPISODES_STATUS_INDEX = """CREATE INDEX IF NOT EXISTS episodes_status_cover_v1 ON episodes (
    source,CAST(coalesce(json_extract(metadata,'$.extraction_mode'),'message') AS TEXT),kind,
    (text_runs IS NULL),length(CAST(text AS BLOB)),id,event_id)"""

_EPISODES_TEXT_INDEX = """CREATE TRIGGER episodes_text_index AFTER INSERT ON episodes
    WHEN NEW.kind NOT IN ('native_record','native_live_record','native_hook')
    AND coalesce(json_extract(NEW.metadata,'$.extraction_mode'),'message')<>'skip'
    BEGIN INSERT INTO episodes_fts(rowid,text) VALUES (NEW.seq,NEW.text); END"""

_RAW_FRAMES = """CREATE TABLE IF NOT EXISTS raw_frames (
    id INTEGER PRIMARY KEY,source TEXT NOT NULL,conversation_id TEXT NOT NULL,
    raw_size INTEGER NOT NULL CHECK(raw_size BETWEEN 0 AND 262144),
    record_count INTEGER NOT NULL CHECK(record_count>=0),sha256 TEXT NOT NULL,
    data BLOB NOT NULL) STRICT"""

_RAW_FRAMES_STATUS_INDEX = """CREATE INDEX IF NOT EXISTS raw_frames_status_cover_v1 ON raw_frames (
    source,record_count,raw_size,length(data))"""

_CAPTURED_MEDIA = (
    """CREATE TABLE IF NOT EXISTS captured_media (
        sha256 TEXT PRIMARY KEY,kind TEXT,filename TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('waiting','ready','queued','indexed','unsupported','forgotten')),
        job_id TEXT UNIQUE,node_id TEXT,error TEXT) STRICT""",
    'CREATE INDEX IF NOT EXISTS captured_media_ready ON captured_media(state)',
    'CREATE INDEX IF NOT EXISTS captured_media_node ON captured_media(node_id)',
    """CREATE TABLE IF NOT EXISTS captured_media_refs (
        sha256 TEXT NOT NULL,episode_id TEXT NOT NULL,start INTEGER NOT NULL,end INTEGER NOT NULL,
        quote TEXT NOT NULL,PRIMARY KEY(sha256,episode_id)) STRICT""",
    'CREATE INDEX IF NOT EXISTS captured_media_episode ON captured_media_refs(episode_id)',
    """CREATE TABLE IF NOT EXISTS captured_media_progress (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),seq INTEGER NOT NULL) STRICT""",
    'INSERT OR IGNORE INTO captured_media_progress VALUES(1,0)',
    """CREATE TABLE IF NOT EXISTS media_representations (
        node_id TEXT PRIMARY KEY,identity TEXT NOT NULL) STRICT""",
    'CREATE INDEX IF NOT EXISTS media_representation_identity ON media_representations(identity)',
)

# node_fts is a derived current-node index: one document per node identity,
# keyed by a small stable mapping (INTEGER PRIMARY KEY survives VACUUM, unlike the
# implicit rowid of node_versions). Canonical node_versions history is untouched.
_NODE_FTS_DOCUMENTS = """CREATE TABLE IF NOT EXISTS node_fts_documents (
    seq INTEGER PRIMARY KEY, id TEXT NOT NULL UNIQUE) STRICT"""

_NODE_FTS_INDEX = """CREATE TRIGGER node_index_current_v1 AFTER INSERT ON node_versions
    WHEN NEW.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=NEW.id)
    BEGIN
        INSERT OR IGNORE INTO node_fts_documents(id) VALUES (NEW.id);
        DELETE FROM node_fts WHERE rowid=(SELECT d.seq FROM node_fts_documents d WHERE d.id=NEW.id);
        INSERT INTO node_fts(rowid,id,version,text)
            SELECT d.seq,NEW.id,NEW.version,NEW.text FROM node_fts_documents d WHERE d.id=NEW.id;
    END"""

_EPISODES_NO_UPDATE = """CREATE TRIGGER episodes_no_update BEFORE UPDATE ON episodes
    WHEN NOT (raw_frame_write(OLD.seq,NEW.raw_frame,NEW.text_sha256)=1
        AND OLD.raw_frame IS NULL AND OLD.text_runs IS NULL AND NEW.text=''
        AND OLD.kind IN ('native_record','native_live_record','native_hook')
        AND coalesce(json_extract(OLD.metadata,'$.extraction_mode'),'message')='skip'
        AND NEW.seq IS OLD.seq AND NEW.id IS OLD.id AND NEW.source IS OLD.source
        AND NEW.event_id IS OLD.event_id AND NEW.conversation_id IS OLD.conversation_id
        AND NEW.kind IS OLD.kind AND NEW.occurred_at IS OLD.occurred_at
        AND NEW.metadata IS OLD.metadata AND NEW.captured_at IS OLD.captured_at
        AND NEW.text_runs IS OLD.text_runs)
    BEGIN SELECT RAISE(ABORT, 'episodes are immutable'); END"""

_MEMORY_VECTORS = """CREATE VIRTUAL TABLE memory_vectors USING vec0(
    embedding float[768] distance_metric=cosine,kind text partition key)"""
_VECTOR_CURRENT = 'current INTEGER NOT NULL DEFAULT 1 CHECK(current IN (0,1))'

# Mutable eligibility belongs to the ordinary metadata table; vec0 stores only
# embeddings and their immutable kind partition. Invalidation is ordinary SQL.
_VECTOR_INVALIDATION_TRIGGERS = (
    ('candidate_vector_terminal', """CREATE TRIGGER candidate_vector_terminal AFTER INSERT ON candidate_decisions
        WHEN NEW.decision IN ('promote','reject')
        BEGIN UPDATE vector_items SET current=0 WHERE kind='candidate' AND target=NEW.candidate_id; END"""),
    ('forgotten_candidate_vector', """CREATE TRIGGER forgotten_candidate_vector AFTER INSERT ON forgotten_targets
        WHEN NEW.kind='candidate'
        BEGIN UPDATE vector_items SET current=0 WHERE kind='candidate' AND target=NEW.target; END"""),
    ('forgotten_source_vectors', """CREATE TRIGGER forgotten_source_vectors AFTER INSERT ON forgotten_sources
        BEGIN UPDATE vector_items SET current=0 WHERE seq IN (
            SELECT i.seq FROM vector_items i JOIN episode_chunks x ON x.id=i.target
            JOIN episodes e ON e.id=x.episode_id WHERE i.kind='episode'
                AND e.source=NEW.source AND e.event_id=NEW.event_id
            UNION SELECT i.seq FROM vector_items i JOIN candidates c ON c.id=i.target
            JOIN extraction_runs r ON r.id=c.run_id JOIN episodes e ON e.id=r.episode_id
            WHERE i.kind='candidate' AND e.source=NEW.source AND e.event_id=NEW.event_id); END"""),
)


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
           raw_frame TEXT,
           UNIQUE(source, event_id)
       ) STRICT""",
    'CREATE INDEX episodes_conversation ON episodes(source, conversation_id)',
    'CREATE INDEX episodes_activity ON episodes(julianday(coalesce(occurred_at,captured_at)) DESC,seq DESC)',
    """CREATE VIRTUAL TABLE episodes_fts USING fts5(
           text, content='episodes', content_rowid='seq', tokenize='unicode61'
       )""",
    _EPISODES_NO_UPDATE,
    """CREATE TRIGGER episodes_no_delete BEFORE DELETE ON episodes BEGIN
           SELECT RAISE(ABORT, 'episodes are immutable');
       END""",
    """CREATE TRIGGER episodes_no_replace BEFORE INSERT ON episodes
       WHEN EXISTS (SELECT 1 FROM episodes WHERE seq = NEW.seq OR id = NEW.id
                    OR (source = NEW.source AND event_id = NEW.event_id))
       BEGIN SELECT RAISE(ABORT, 'episodes are immutable'); END""",
    _EPISODES_TEXT_INDEX,
    _RAW_FRAMES,
    _RAW_FRAMES_STATUS_INDEX,
    """CREATE TRIGGER raw_frames_no_update BEFORE UPDATE ON raw_frames
       BEGIN SELECT RAISE(ABORT, 'raw frames are immutable'); END""",
    """CREATE TRIGGER raw_frames_no_delete BEFORE DELETE ON raw_frames
       BEGIN SELECT RAISE(ABORT, 'raw frames are immutable'); END""",
    """CREATE TRIGGER raw_frames_no_replace BEFORE INSERT ON raw_frames
       WHEN EXISTS (SELECT 1 FROM raw_frames WHERE id=NEW.id)
       BEGIN SELECT RAISE(ABORT, 'raw frames are immutable'); END""",
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
    _NODE_FTS_DOCUMENTS,
    _NODE_FTS_INDEX,
    'CREATE INDEX edge_src ON edge_versions(src,rel)',
    'CREATE INDEX edge_dst ON edge_versions(dst,rel)',
    """CREATE TABLE vector_model (
       singleton INTEGER PRIMARY KEY CHECK(singleton=1),
       model_id TEXT NOT NULL, revision TEXT NOT NULL, dimensions INTEGER NOT NULL,
       text_encoding INTEGER NOT NULL DEFAULT 0 CHECK(text_encoding IN (0,1))) STRICT""",
    f"""CREATE TABLE vector_items (
       seq INTEGER PRIMARY KEY, target TEXT NOT NULL, kind TEXT NOT NULL,
       version INTEGER NOT NULL, {_VECTOR_CURRENT}, UNIQUE(target,kind,version)) STRICT""",
    _MEMORY_VECTORS,
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
    *(statement for _, statement in _VECTOR_INVALIDATION_TRIGGERS),
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
    upgrade_captured_media(db)
    upgrade_historical_pool(db)
    upgrade_repair_groups(db)
    for table, predicate in _IMMUTABLE_KEYS.items():
        for operation in ("UPDATE", "DELETE"):
            db.execute(f"""CREATE TRIGGER {table}_no_{operation} BEFORE {operation} ON {table}
                BEGIN SELECT RAISE(ABORT, 'graph history is immutable'); END""")
        db.execute(f"""CREATE TRIGGER {table}_no_replace BEFORE INSERT ON {table}
            WHEN EXISTS (SELECT 1 FROM {table} WHERE {predicate})
            BEGIN SELECT RAISE(ABORT, 'graph history is immutable'); END""")


def upgrade_captured_media(db):
    """Small durable discovery/admission state; historical work stays off startup."""
    for statement in _CAPTURED_MEDIA:
        db.execute(statement)


def _upgrade_vector_triggers(db):
    for name, statement in _VECTOR_INVALIDATION_TRIGGERS:
        previous = db.execute("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name=?", (name,)).fetchone()
        if previous is None or previous[0] != statement:
            db.execute(f'DROP TRIGGER IF EXISTS {name}')
            db.execute(statement)


def upgrade_vector_partitions(db):
    """Atomically separate mutable eligibility from the immutable vector index.

    Caller owns the transaction and header-4 fence. A regular disk-backed staging
    table preserves exact vectors and legacy flags while vec0 is recreated under
    the same name; its shadow tables cannot safely be renamed. No model call or
    Python vector cache is involved. Metadata without an embedding stays stored.
    """
    if not db.in_transaction:
        raise RuntimeError('Vector partition upgrade requires an active transaction')
    if _clean_vector_layout(db):
        _upgrade_vector_triggers(db)
        return
    if db.execute('''SELECT 1 FROM memory_vectors v LEFT JOIN vector_items i ON i.seq=v.rowid
        WHERE i.seq IS NULL OR i.kind IS NULL
        OR i.kind NOT IN ('node','candidate','episode','segment') LIMIT 1''').fetchone():
        raise RuntimeError('Cannot partition vector rows with missing or invalid kind')
    log = logging.getLogger(__name__)
    log.warning('Rebuilding kind-partitioned vector index')
    started = time.perf_counter()
    db.execute('''CREATE TABLE _memory_vector_partition_stage (
        seq INTEGER PRIMARY KEY,embedding BLOB NOT NULL,current INTEGER NOT NULL,kind TEXT NOT NULL) STRICT''')
    db.execute('''INSERT INTO _memory_vector_partition_stage
        SELECT v.rowid,v.embedding,v.current,i.kind FROM memory_vectors v
        JOIN vector_items i ON i.seq=v.rowid''')
    if 'current' not in {column[1] for column in db.execute('PRAGMA table_info(vector_items)')}:
        db.execute(f'ALTER TABLE vector_items ADD COLUMN {_VECTOR_CURRENT}')
    db.execute('''UPDATE vector_items SET current=(
        SELECT s.current FROM _memory_vector_partition_stage s WHERE s.seq=vector_items.seq)
        WHERE seq IN (SELECT seq FROM _memory_vector_partition_stage)''')
    db.execute('DROP TABLE memory_vectors')
    db.execute(_MEMORY_VECTORS)
    db.execute('''INSERT INTO memory_vectors(rowid,embedding,kind)
        SELECT seq,embedding,kind FROM _memory_vector_partition_stage ORDER BY seq''')
    expected = db.execute('SELECT count(*) FROM _memory_vector_partition_stage').fetchone()[0]
    actual = db.execute('SELECT count(*) FROM memory_vectors').fetchone()[0]
    mismatch = db.execute('''SELECT 1 FROM _memory_vector_partition_stage s
        LEFT JOIN memory_vectors v ON v.rowid=s.seq
        LEFT JOIN vector_items i ON i.seq=s.seq
        WHERE v.rowid IS NULL OR v.embedding IS NOT s.embedding
        OR i.current IS NOT s.current OR v.kind IS NOT s.kind OR i.kind IS NOT s.kind LIMIT 1''').fetchone()
    if expected != actual or mismatch is not None or not _clean_vector_layout(db):
        raise RuntimeError('Vector partition rebuild did not preserve every stored vector')
    db.execute('DROP TABLE _memory_vector_partition_stage')
    _upgrade_vector_triggers(db)
    log.warning('Kind-partitioned vector index rebuild completed in %.3f seconds', time.perf_counter() - started)


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


def upgrade_node_fts(db):
    """Caller owns the transaction; index only the latest version of each node.

    The versioned trigger is the migration marker: the mapping table, the
    rebuilt derived index and the trigger commit together, so an interruption
    leaves the legacy all-version index and trigger in place for the next start.
    No canonical table, header or portable format changes.
    """
    if db.execute("SELECT 1 FROM sqlite_schema WHERE type='trigger' AND name='node_index_current_v1'").fetchone():
        return
    log = logging.getLogger(__name__)
    log.warning('Rebuilding current-node text index')
    started = time.perf_counter()
    db.execute(_NODE_FTS_DOCUMENTS)
    db.execute('INSERT OR IGNORE INTO node_fts_documents(id) SELECT DISTINCT id FROM node_versions')
    db.execute('DELETE FROM node_fts')
    db.execute('''INSERT INTO node_fts(rowid,id,version,text)
        SELECT d.seq,n.id,n.version,n.text FROM node_fts_documents d JOIN node_versions n ON n.id=d.id
        WHERE n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)''')
    db.execute('DROP TRIGGER IF EXISTS node_index')
    db.execute(_NODE_FTS_INDEX)
    log.warning('Current-node text index rebuild completed in %.3f seconds', time.perf_counter() - started)


def upgrade_raw_text(db):
    """Upgrade physical storage only; conversion happens in bounded background work."""
    columns = {row[1] for row in db.execute('PRAGMA table_info(episodes)')}
    for column in ('text_runs', 'text_sha256'):
        if column not in columns:
            db.execute(f'ALTER TABLE episodes ADD COLUMN {column} TEXT')
    if 'raw_frame' not in columns:
        db.execute('ALTER TABLE episodes ADD COLUMN raw_frame TEXT')
    db.execute(_RAW_FRAMES)
    if not db.execute("SELECT 1 FROM sqlite_schema WHERE name='raw_frames_status_cover_v1'").fetchone():
        log = logging.getLogger(__name__)
        log.warning('Building raw frame status index')
        started = time.perf_counter()
        db.execute(_RAW_FRAMES_STATUS_INDEX)
        log.warning('Raw frame status index build completed in %.3f seconds', time.perf_counter() - started)
    db.execute('DROP INDEX IF EXISTS raw_frames_source')
    for operation in ('UPDATE', 'DELETE'):
        db.execute(f"""CREATE TRIGGER IF NOT EXISTS raw_frames_no_{operation.lower()}
            BEFORE {operation} ON raw_frames
            BEGIN SELECT RAISE(ABORT, 'raw frames are immutable'); END""")
    db.execute("""CREATE TRIGGER IF NOT EXISTS raw_frames_no_replace BEFORE INSERT ON raw_frames
        WHEN EXISTS (SELECT 1 FROM raw_frames WHERE id=NEW.id)
        BEGIN SELECT RAISE(ABORT, 'raw frames are immutable'); END""")
    trigger = db.execute("SELECT sql FROM sqlite_schema WHERE name='episodes_no_update'").fetchone()
    if trigger is None or trigger[0] != _EPISODES_NO_UPDATE:
        db.execute('DROP TRIGGER IF EXISTS episodes_no_update')
        db.execute(_EPISODES_NO_UPDATE)


def upgrade_status_index(db):
    """Caller owns the transaction; raw physical columns must already exist."""
    if db.execute("SELECT 1 FROM sqlite_schema WHERE type='index' AND name='episodes_status_cover_v1'").fetchone():
        return
    log = logging.getLogger(__name__)
    log.warning('Building episode status index')
    started = time.perf_counter()
    db.execute(_EPISODES_STATUS_INDEX)
    log.warning('Episode status index build completed in %.3f seconds', time.perf_counter() - started)
