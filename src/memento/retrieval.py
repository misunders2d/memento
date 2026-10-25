"""Native local embeddings and SQLite hybrid retrieval, outside capture transactions."""

import hashlib
import json
import logging
import os
import re
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from threading import Event, RLock, Thread

import numpy as np

from .embedder import EMBED_DIM, embed_query, embed_texts, load_model
from .graph import Graph, NODE_SUMMARY, ENTITY_TYPES, identity_text
from .extraction import ACTIVE_CANDIDATE_SQL, UNPARKED_CANDIDATE_SQL, source_text_ranges
from .text_chunks import TextChunk, chunks
from .importance import query_key, query_recency
from .captured_media import active_references, visible_sql
from .store import GOAL_CONTEXT_PREFIX, LEGACY_CONTEXT_PREFIX, legacy_native_context
from .ann import Projection
from .qdrant import SearchUnavailable

MODEL_ID = 'google/embeddinggemma-2'
MODEL_REVISION = '914f7f89142e33e77833254d9c9b90c3cef7303b'
TEXT_COSINE_FLOOR = 0.685  # corporate_search's measured native text relevance floor
# Pinned-model calibration: ordinary negatives reached .6562; actual native
# learning yielded a useful neutral-query transfer at .66375. At .66, frozen
# synthetic/holdout pairs are unchanged from .67; topical behavior overlap still
# requires the receiving agent's applicability check, not a confidence claim.
AUTOMATIC_COSINE_FLOOR = 0.66
AUTOMATIC_RESULT_LIMIT = 6
AUTOMATIC_SUGGESTION_LIMIT = 2
# Entity anchors: at most two named entities whose stored spelling appears in the
# current request, each with bounded one-hop connections. Lexical containment
# only; semantic neighbours never become identity anchors.
ENTITY_ANCHOR_LIMIT = 2
ENTITY_ANCHOR_CONNECTIONS = 5
ENTITY_ANCHOR_TERMS = 64
MEDIA_COSINE_FLOORS = {'image': 0.67, 'audio': 0.645, 'text': TEXT_COSINE_FLOOR}


def _relevance_order(record, feedback=0):
    """Age and usage distinguish equal query relevance; neither can replace it."""
    return (-record['score'], -record.get('decay', 1), -record.get('importance', .5),
            -feedback, record['id'])


def _episode_units(episodes='episodes'):
    return f'''SELECT x.id,'episode' AS kind,1 AS version,NULL AS text,NULL AS type,
        x.episode_id,x.start,x.end,2 AS priority,NULL AS scope,x.rowid AS position
        FROM episode_chunks x JOIN {episodes} e ON e.id=x.episode_id'''


_INDEX_SOURCES = (
    ('node', 'node_versions', '''SELECT n.id,'node' AS kind,n.version,n.text,n.type,
        NULL AS episode_id,NULL AS start,NULL AS end,0 AS priority,n.scope,n.rowid AS position
        FROM node_versions n WHERE n.state='active' AND n.type<>'media'
        AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)'''),
    ('candidate', 'candidates', f'''SELECT c.id,'candidate' AS kind,1 AS version,c.text,c.type,
        NULL AS episode_id,NULL AS start,NULL AS end,1 AS priority,c.scope,c.rowid AS position
        FROM candidates c WHERE {ACTIVE_CANDIDATE_SQL}'''),
    ('episode', 'episode_chunks', _episode_units() + '''
        WHERE coalesce(json_extract(e.metadata,'$.extraction_mode'),'message')<>'skip'
        AND NOT EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id)'''),
)
_INDEXABLE = ' UNION ALL '.join(source for _, _, source in _INDEX_SOURCES)

def _recall_eligible(indexable):
    return f'''SELECT a.id,a.kind,a.version,a.scope,a.id AS rank_target FROM ({indexable}) a
    UNION ALL SELECT s.id,'segment',1,n.scope,n.id FROM segments s JOIN node_versions n ON n.id=s.node_id
    WHERE n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)
    AND {visible_sql()}'''


_RECALL_ELIGIBLE = _recall_eligible(_INDEXABLE)

# Parked raw history (alias e): captured at or before an explicit cutover boundary,
# ordinary projected text only. Hooks, instructions, tool-call projections, native
# raw records and forgotten sources never become fallback context.
HISTORICAL_RAW_SQL = '''EXISTS (SELECT 1 FROM historical_parking h WHERE h.source=e.source
        AND h.conversation_id=e.conversation_id AND e.seq<=h.parked_through)
    AND e.kind NOT IN ('developer','native_record','native_live_record','native_hook')
    AND coalesce(json_extract(e.metadata,'$.extraction_mode'),'message')<>'skip'
    AND coalesce(json_extract(e.metadata,'$.evidence_eligible'),1)<>0
    AND NOT EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id)'''
HISTORICAL_PREFIX = 'Unverified historical source'

_ENTITY_SQL = ','.join("'" + kind + "'" for kind in sorted(ENTITY_TYPES))
_AUTOMATIC_ELIGIBLE = f'''SELECT n.id,'node' AS kind,n.version,n.scope,n.id AS rank_target
    FROM node_versions n WHERE n.state='active'
    AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)
    AND n.type NOT IN ({_ENTITY_SQL},'media')
    AND NOT EXISTS (SELECT 1 FROM memory_types t WHERE t.name=n.type AND t.category='entity')
    UNION ALL SELECT c.id,'candidate',1,c.scope,c.id FROM candidates c
    WHERE c.kind='memory' AND c.type NOT IN ({_ENTITY_SQL},'experience')
    AND NOT EXISTS (SELECT 1 FROM memory_types t WHERE t.name=c.type AND t.category='entity')
    AND {ACTIVE_CANDIDATE_SQL} AND {UNPARKED_CANDIDATE_SQL}
    UNION ALL SELECT s.id,'segment',1,n.scope,n.id FROM segments s JOIN node_versions n ON n.id=s.node_id
    WHERE n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)
    AND {visible_sql()}'''


def _automatic_content(text):
    """Do not spend injection space on labels, paths or generic associations."""
    text = text.strip().strip('`\"\'')
    words = re.findall(r'[^\W_]+', text, re.UNICODE)
    if len(words) < 3:
        return False
    if '\n' not in text and re.fullmatch(r'\S+(?:[/\\]\S+|\.[A-Za-z0-9]{1,8})', text):
        return False
    if re.fullmatch(r'(?:~?/|[A-Za-z]:[/\\]|\.{1,2}/).+\.[A-Za-z0-9]{1,8}', text):
        return False
    if re.fullmatch(r'.{1,120}\s+(?:is\s+)?(?:related to|associated with|mentions)\s+.{1,120}[.!]?', text, re.I):
        return False
    return True


def _name_tokens(text):
    """Lowercased word tokens, split like the unicode61 FTS tokenizer (no underscores).

    str.lower, as in identity_text: casefold would turn "Straße" into "strasse",
    which the FTS prefilter never matches, so the identical written name would fail.
    """
    return [token.lower() for token in re.findall(r'[^\W_]+', text, re.UNICODE)]


def _anchor_terms(query):
    """Distinct request tokens, in request order, bounded only by count; no word lists."""
    terms = []
    for token in _name_tokens(query):
        if token not in terms:
            terms.append(token)
            if len(terms) == ENTITY_ANCHOR_TERMS:
                break
    return terms


def _name_in_request(name, request_tokens):
    """The stored name's tokens occur contiguously, in order, in the request tokens.

    Any length or script qualifies; a scattered "John … Smith" does not assert
    "John Smith". Alternate spellings come only from reviewed merged aliases.
    """
    tokens = _name_tokens(name)
    if not tokens or len(tokens) > len(request_tokens):
        return False
    return any(request_tokens[start:start + len(tokens)] == tokens
               for start in range(len(request_tokens) - len(tokens) + 1))


def _automatic_query(query, context):
    # A follow-up needs its antecedent; an independent new request should not be
    # pulled toward the previous topic merely because context was supplied.
    followup = (len(query.split()) <= 2 or re.search(
        r'^(?:and\b|what about\b|how about\b|continue\b|go on\b|а\b|как насчет\b|что насчет\b)|'
        r'^(?:what|how|why|when|where|which)\b.{0,100}\b(?:it|that|this|those|them|same)\b|'
        r'^(?:does|do|can|could|would|should|is|are|will)\s+(?:it|that|this|those|they|them)\b|'
        r'^(?:please )?(?:do|use|apply|repeat) (?:it|that|this|the same)\b', query, re.I))
    if context.strip() and followup:
        return (query + '\nRecent conversation:\n' + context.strip())[:4096]
    return query


class Retrieval:
    def __init__(self, graph: Graph, *, model=None):
        self.graph = graph
        self.store = graph.store
        self._inference = RLock()
        self._query_inference = RLock()
        self._stop = Event()
        self._worker = None
        self.index_error = None
        self._episode_cursor = 0
        self._index_cursors = dict(node=0, candidate=0, episode=0)
        self.model = model if model is not None else load_model(MODEL_ID, MODEL_REVISION, text_only=False)
        self._tokenization = getattr(self.model, 'tokenization_lock', None) or RLock()
        with self.store._mutex:
            db = self.store._db
            db.execute('BEGIN IMMEDIATE')
            try:
                if 'text_encoding' not in {column['name'] for column in db.execute('PRAGMA table_info(vector_model)')}:
                    db.execute('ALTER TABLE vector_model ADD COLUMN text_encoding INTEGER NOT NULL DEFAULT 0')
                old = db.execute('SELECT model_id,revision,dimensions FROM vector_model').fetchone()
                signature = (MODEL_ID, MODEL_REVISION, EMBED_DIM)
                if old is None:
                    db.execute('''INSERT INTO vector_model(singleton,model_id,revision,dimensions,text_encoding)
                        VALUES (1,?,?,?,1)''', signature)
                elif tuple(old) != signature:
                    raise RuntimeError('Embedding model differs from the index; explicit reindex required')
                if db.execute('SELECT text_encoding FROM vector_model').fetchone()[0] == 0:
                    self._upgrade_text_encoding()
                    db.execute('UPDATE vector_model SET text_encoding=1')
                db.execute('COMMIT')
            except BaseException:
                db.execute('ROLLBACK')
                raise
        self._projection = (Projection(self.store, EMBED_DIM,
            dict(model_id=MODEL_ID, revision=MODEL_REVISION, dimensions=EMBED_DIM, projection=1),
            self._projection_payloads) if 'MEMENTO_QDRANT_BINARY' in os.environ else None)
        self.graph._retrieval = self

    @contextmanager
    def _snapshot(self):
        with (self._projection.snapshot() if self._projection is not None else self.store.read_snapshot()) as db:
            yield db

    def _vector_rows(self, db, vector, limit, *, kind, pool, scope=None, type=None):
        if self._projection is not None:
            return self._projection.search(db, vector, limit, kind=kind, pool=pool, scope=scope, type=type)
        # Explicitly unconfigured standalone installations retain their existing
        # SQLite engine. A configured projection never reaches this branch.
        if pool in ('ordinary', 'automatic'):
            eligible = _RECALL_ELIGIBLE if pool == 'ordinary' else _AUTOMATIC_ELIGIBLE
            where = f'''v.rowid IN (SELECT i.seq FROM vector_items i JOIN ({eligible}) a
                ON a.id=i.target AND a.kind=i.kind AND a.version=i.version
                WHERE i.kind=? AND (? IS NULL OR EXISTS (SELECT 1 FROM json_each(a.scope) WHERE value=?)))'''
            parameters = (kind, scope, scope)
        elif pool == 'entity':
            where = '''v.rowid IN (SELECT i.seq FROM vector_items i JOIN node_versions n
                ON n.id=i.target AND n.version=i.version WHERE i.kind='node' AND n.type=?
                AND n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id))'''
            parameters = (type,)
        elif pool == 'historical':
            where = f'''v.rowid IN (SELECT i.seq FROM vector_items i JOIN episode_chunks x ON x.id=i.target
                JOIN episodes e ON e.id=x.episode_id WHERE i.kind='episode' AND {HISTORICAL_RAW_SQL})'''
            parameters = ()
        elif pool == 'parked_candidate':
            where = f'''v.rowid IN (SELECT i.seq FROM vector_items i JOIN candidates c ON c.id=i.target
                JOIN historical_parked_candidates h ON h.candidate_id=c.id AND h.released_at IS NULL
                WHERE i.kind='candidate' AND c.kind='memory' AND c.type NOT IN ({_ENTITY_SQL})
                AND {ACTIVE_CANDIDATE_SQL})'''
            parameters = ()
        else:
            raise ValueError('Unknown vector eligibility pool')
        return db.execute(f'''SELECT i.kind,i.target,i.version,1-v.distance AS cosine
            FROM memory_vectors v JOIN vector_items i ON i.seq=v.rowid
            WHERE v.embedding MATCH ? AND k=? AND v.current=1 AND {where}
            ORDER BY v.distance''', (vector.tobytes(), limit, *parameters)).fetchall()

    def _projection_payloads(self, db, rows):
        """Query-independent eligibility from the very same evidence snapshot.

        Every lookup uses stable indexed IDs; no source-body hydration or global
        vector scan is needed for an incremental payload update.
        """
        result, entity_types = {}, {}
        for row in rows:
            kind, target, version = row['kind'], row['target'], row['version']
            flags = dict(kind=kind, type='', current=bool(row['current']), ordinary=False,
                automatic=False, historical=False, parked_candidate=False, entity=False,
                personal=False, professional=False)
            record = None
            if kind == 'node':
                record = db.execute('''SELECT n.id,n.type,substr(n.text,1,240) AS text,n.scope
                    FROM node_versions n WHERE n.id=? AND n.version=? AND n.state='active'
                    AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)
                    AND NOT EXISTS (SELECT 1 FROM forgotten_targets f WHERE f.target=n.id)
                    AND ''' + visible_sql(), (target, version)).fetchone()
                if record is not None:
                    flags['ordinary'] = record['type'] != 'media'
                    flags['entity'] = True  # _match_entity also supplies the exact type.
            elif kind == 'candidate':
                record = self._candidate_record(target, db=db)
                flags['ordinary'] = record is not None
            elif kind == 'segment':
                record = self._automatic_record(row, db=db)
                flags['ordinary'] = record is not None
            elif kind == 'episode':
                episode = db.execute(f'''SELECT e.source,e.kind,e.metadata,
                    coalesce(json_extract(e.metadata,'$.extraction_mode'),'message')<>'skip'
                    AND NOT EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id)
                    AS ordinary,({HISTORICAL_RAW_SQL}) AS historical,
                    CASE WHEN e.kind='user' AND e.source GLOB 'native:*:codex'
                        AND (substr(e.text,1,?)=? OR substr(e.text,1,?)=?) THEN e.text ELSE '' END AS legacy_text
                    FROM episode_chunks x JOIN episodes e ON e.id=x.episode_id WHERE x.id=?''',
                    (len(LEGACY_CONTEXT_PREFIX), LEGACY_CONTEXT_PREFIX,
                     len(GOAL_CONTEXT_PREFIX), GOAL_CONTEXT_PREFIX, target)).fetchone()
                if episode is not None:
                    flags['type'] = 'raw_episode'
                    flags['ordinary'] = bool(episode['ordinary'])
                    flags['historical'] = bool(episode['historical']) and not legacy_native_context(
                        episode['source'], episode['kind'], episode['legacy_text'], episode['metadata'])
            if record is not None:
                flags['type'] = record['type']
                scope = json.loads(record['scope']) if isinstance(record['scope'], str) else record['scope']
                flags.update(personal='personal' in scope, professional='professional' in scope)
                if record['type'] not in entity_types:
                    entity_types[record['type']] = record['type'] in ENTITY_TYPES or db.execute(
                        "SELECT 1 FROM memory_types WHERE name=? AND category='entity'", (record['type'],)).fetchone() is not None
                entity = entity_types[record['type']]
                parent = record['id']
                parent_version = record.get('version', version) if isinstance(record, dict) else version
                if kind == 'segment':
                    parent_version = record['version']
                legacy = self._legacy_context_only(parent, parent_version, db=db)
                content = kind == 'segment' or _automatic_content(record['text'])
                if kind == 'candidate':
                    memory = record['candidate_kind'] == 'memory' and record['type'] not in ENTITY_TYPES
                    flags['automatic'] = (memory and not entity and record['type'] != 'experience'
                                          and not record['historical'] and content and not legacy)
                    flags['parked_candidate'] = memory and record['historical'] and content and not legacy
                elif kind == 'segment':
                    flags['automatic'] = content and not legacy
                else:
                    flags['automatic'] = (not entity and record['type'] != 'media' and content and not legacy
                        and (record['type'] != 'experience' or self.graph._behavior(parent, db=db) is not None))
            result[row['seq']] = flags
        return result

    def _upgrade_text_encoding(self):
        """Re-encode legacy control-token text once; keep all raw data and vector IDs."""
        db = self.store._db
        tokens = getattr(self.model.tokenizer, 'all_special_tokens', ())
        if not tokens:
            return
        special = re.compile('|'.join(re.escape(token) for token in tokens))
        rows = db.execute('''SELECT i.seq,CASE i.kind
            WHEN 'node' THEN n.text WHEN 'candidate' THEN c.text
            WHEN 'episode' THEN substr(e.text,x.start+1,x.end-x.start)
            WHEN 'segment' THEN s.text END AS text
            FROM vector_items i
            LEFT JOIN node_versions n ON i.kind='node' AND n.id=i.target AND n.version=i.version
            LEFT JOIN candidates c ON i.kind='candidate' AND c.id=i.target
            LEFT JOIN episode_chunks x ON i.kind='episode' AND x.id=i.target
            LEFT JOIN episodes e ON e.id=x.episode_id
            LEFT JOIN segments s ON i.kind='segment' AND s.id=i.target
            WHERE i.kind<>'segment' OR s.modality='text' ''').fetchall()
        for row in rows:
            if row['text'] is not None and special.search(row['text']):
                vector = embed_texts(self.model, ['title: none | text: ' + row['text']])[0]
                if not np.isfinite(vector).all() or not np.isclose(np.linalg.norm(vector), 1, atol=0.001):
                    raise ValueError('Invalid literal-text migration embedding')
                db.execute('UPDATE memory_vectors SET embedding=? WHERE rowid=?', (vector.tobytes(), row['seq']))
                db.execute('INSERT INTO search_outbox(seq,nonce) VALUES (?,lower(hex(randomblob(16))))', (row['seq'],))
        # Retry the fixed failure class now, but only the normal worker may resolve it.
        for failure in db.execute('SELECT target,version FROM index_failures WHERE resolved=0').fetchall():
            row = db.execute(f'''WITH indexable AS ({_INDEXABLE})
                SELECT * FROM indexable WHERE id=? AND version=?''', tuple(failure)).fetchone()
            if row is None:
                continue
            text = row['text']
            if row['kind'] == 'episode':
                text = db.execute('SELECT substr(text,?,?) FROM episodes WHERE id=?',
                    (row['start'] + 1, row['end'] - row['start'], row['episode_id'])).fetchone()[0]
            if special.search(text):
                db.execute('UPDATE index_failures SET next_attempt=0 WHERE target=? AND version=?', tuple(failure))

    def prepare_episodes(self, limit=2):
        with self.store._mutex:
            db = self.store._db
            through = db.execute('SELECT coalesce(max(seq),0) FROM episodes').fetchone()[0]
            episodes = db.execute('''SELECT e.* FROM episodes e
                WHERE e.seq>? AND e.seq<=?
                AND coalesce(json_extract(e.metadata,'$.extraction_mode'),'message')<>'skip'
                AND NOT EXISTS (SELECT 1 FROM episode_chunks x WHERE x.episode_id=e.id)
                AND NOT EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id)
                ORDER BY e.seq LIMIT ?''', (self._episode_cursor, through, limit)).fetchall()
            cursor = episodes[-1]['seq'] if len(episodes) == limit else through
        prepared = []
        for episode in episodes:
            with self._inference, self._tokenization:
                passages = []
                for start, end in source_text_ranges(episode['text']):
                    text = episode['text'][start:end]
                    local = chunks(text, self.model.tokenizer)
                    passages.extend(TextChunk(start + part.start, start + part.end, part.text) for part in local)
            prepared.append((episode, passages or [TextChunk(0, 0, '')]))
        with self.store._mutex:
            db = self.store._db
            db.execute('BEGIN IMMEDIATE')
            try:
                for episode, passages in prepared:
                    if db.execute('SELECT 1 FROM episode_chunks WHERE episode_id=? LIMIT 1', (episode['id'],)).fetchone():
                        continue
                    if db.execute('SELECT 1 FROM forgotten_sources WHERE source=? AND event_id=? LIMIT 1',
                                  (episode['source'], episode['event_id'])).fetchone():
                        continue
                    for passage in passages:
                        identity = f"{episode['id']}:{passage.start}:{passage.end}"
                        target = 'e_' + hashlib.sha256(identity.encode()).hexdigest()[:32]
                        db.execute('INSERT INTO episode_chunks VALUES (?,?,?,?)',
                                   (target, episode['id'], passage.start, passage.end))
                db.execute('COMMIT')
                self._episode_cursor = cursor
            except BaseException:
                db.execute('ROLLBACK')
                raise

    def _indexable(self, row):
        return self.store._db.execute(f'''WITH indexable AS ({_INDEXABLE})
            SELECT 1 FROM indexable WHERE id=? AND kind=? AND version=?''',
            (row['id'], row['kind'], row['version'])).fetchone() is not None

    def index_pending(self, limit=16):
        self.prepare_episodes()
        with self.store._mutex:
            db = self.store._db
            now = time.time()
            rows = []
            failed = db.execute('''SELECT target,version FROM index_failures
                WHERE resolved=0 AND next_attempt<=? ORDER BY next_attempt LIMIT ?''',
                (now, limit)).fetchall()
            for failure in failed:
                row = db.execute(f'''WITH indexable AS ({_INDEXABLE})
                    SELECT a.* FROM indexable a WHERE a.id=? AND a.version=?
                      AND NOT EXISTS (SELECT 1 FROM vector_items i
                        WHERE i.target=a.id AND i.kind=a.kind AND i.version=a.version)''',
                    (failure['target'], failure['version'])).fetchone()
                if row:
                    rows.append(dict(row))
                else:
                    db.execute('UPDATE index_failures SET resolved=1 WHERE target=? AND version=?',
                               (failure['target'], failure['version']))
            cursors = self._index_cursors.copy()
            for kind, table, source in _INDEX_SOURCES:
                remaining = limit - len(rows)
                if not remaining:
                    break
                selected = [dict(row) for row in db.execute(f'''
                    SELECT a.* FROM ({source}) a WHERE a.position>?
                      AND NOT EXISTS (SELECT 1 FROM vector_items i
                        WHERE i.target=a.id AND i.kind=a.kind AND i.version=a.version)
                      AND NOT EXISTS (SELECT 1 FROM index_failures f WHERE f.target=a.id
                        AND f.version=a.version AND f.resolved=0)
                    ORDER BY a.position LIMIT ?''', (cursors[kind], remaining))]
                rows.extend(selected)
                cursors[kind] = selected[-1]['position'] if len(selected) == remaining else db.execute(
                    f'SELECT coalesce(max(rowid),0) FROM {table}').fetchone()[0]
        if not rows:
            self._index_cursors = cursors
            return 0
        sources = {}
        for row in rows:
            if row['kind'] == 'episode':
                if row['episode_id'] not in sources:
                    sources[row['episode_id']] = self.store.get(row['episode_id'])['text']
                row['text'] = sources[row['episode_id']][row['start']:row['end']]
        prepared = []
        for row in rows:
            if self._stop.is_set():
                break
            try:
                # Bound background activations independently of foreground queries.
                with self._inference:
                    vector = embed_texts(self.model, ['title: none | text: ' + row['text']])[0]
                # Association hints are advisory. All search/network work must
                # finish before entering the indexing writer transaction.
                try:
                    hints = self._match_entity(row, vector) if row['kind'] == 'candidate' else []
                except SearchUnavailable:
                    hints = []
                prepared.append((row, vector, None, hints))
            except Exception as error:
                prepared.append((row, None, error, []))
        with self.store._mutex:
            db = self.store._db
            db.execute('BEGIN IMMEDIATE')
            try:
                count = 0
                for row, vector, error, hints in prepared:
                    if not self._indexable(row):
                        continue
                    db.execute('SAVEPOINT index_item')
                    try:
                        if error is not None:
                            raise error
                        inserted = self._put(row['id'], row['kind'], row['version'], vector)
                        if row['kind'] == 'candidate':
                            self._commit_entity_matches(row, hints)
                        db.execute('UPDATE index_failures SET resolved=1 WHERE target=? AND version=?',
                                   (row['id'], row['version']))
                        count += inserted
                    except Exception as error:
                        db.execute('ROLLBACK TO index_item')
                        prior = db.execute('SELECT attempts FROM index_failures WHERE target=? AND version=?',
                                           (row['id'], row['version'])).fetchone()
                        attempts = prior['attempts'] + 1 if prior else 1
                        retry_at = time.time() + min(3600, 5 * 2 ** min(attempts - 1, 10))
                        db.execute('''INSERT INTO index_failures VALUES (?,?,?,?,?,0)
                            ON CONFLICT(target,version) DO UPDATE SET attempts=excluded.attempts,
                            error=excluded.error,next_attempt=excluded.next_attempt,resolved=0''',
                            (row['id'], row['version'], attempts, type(error).__name__, retry_at))
                    finally:
                        db.execute('RELEASE index_item')
                db.execute('COMMIT')
                if len(prepared) == len(rows):
                    self._index_cursors = cursors
                return count
            except BaseException:
                db.execute('ROLLBACK')
                raise

    def _match_entity(self, candidate, vector):
        # Most indexed candidates are ordinary facts. Do not acquire the search
        # lock or make network requests for those non-entity units.
        with self.store.read_snapshot() as db:
            current = db.execute(f"SELECT kind FROM candidates c WHERE c.id=? AND {ACTIVE_CANDIDATE_SQL}",
                                 (candidate['id'],)).fetchone()
            if current is None or current['kind'] != 'entity':
                return []
        vector = np.asarray(vector, dtype=np.float32)
        with self._snapshot() as db:
            current = db.execute(f"SELECT kind FROM candidates c WHERE c.id=? AND {ACTIVE_CANDIDATE_SQL}",
                                 (candidate['id'],)).fetchone()
            if current is None or current['kind'] != 'entity':
                return []
            matches = {}
            for node in self.graph.recall_keywords(candidate['text'], 20, db=db):
                if node['type'] != candidate['type']:
                    continue
                old = db.execute('''SELECT v.embedding FROM memory_vectors v JOIN vector_items i ON i.seq=v.rowid
                    WHERE i.target=? AND i.kind='node' AND i.version=? AND v.current=1''',
                    (node['id'], node['version'])).fetchone()
                cosine = float(np.clip(np.dot(vector, np.frombuffer(old['embedding'], np.float32)), -1, 1)) if old else None
                matches[node['id']] = dict(target=node['id'], version=node['version'], cosine=cosine, name_match=1)
            for node in self._vector_rows(db, vector, 5, kind='node', pool='entity', type=candidate['type']):
                if node['cosine'] >= 0.78 and node['target'] not in matches:
                    matches[node['target']] = dict(target=node['target'], version=node['version'],
                                                   cosine=node['cosine'], name_match=0)
            return sorted(matches.values(), key=lambda item:
                (-item['name_match'], -(item['cosine'] if item['cosine'] is not None else -1), item['target']))[:5]

    def _commit_entity_matches(self, candidate, hints):
        db = self.store._db
        if db.execute(f"SELECT 1 FROM candidates c WHERE c.id=? AND c.kind='entity' AND {ACTIVE_CANDIDATE_SQL}",
                      (candidate['id'],)).fetchone() is None:
            return
        for node in hints:
            if db.execute('''SELECT 1 FROM node_versions n WHERE n.id=? AND n.version=? AND n.type=?
                AND n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)
                AND NOT EXISTS (SELECT 1 FROM forgotten_targets f WHERE f.target=n.id)''',
                (node['target'], node['version'], candidate['type'])).fetchone() is None:
                continue
            db.execute('INSERT OR IGNORE INTO candidate_entity_matches VALUES (?,?,?,?,?,?,?,?)',
                (candidate['id'], node['target'], node['version'], node['cosine'], node['name_match'],
                 'native-indexer/' + MODEL_ID + '@' + MODEL_REVISION,
                 'Tentative name/vector association only; no identity merge or primary assertion',
                 datetime.now(timezone.utc).isoformat()))

    def _put(self, target, kind, version, vector):
        vector = np.asarray(vector, dtype=np.float32)
        if vector.shape != (EMBED_DIM,) or not np.isfinite(vector).all():
            raise ValueError('Invalid embedding vector')
        if not np.isclose(np.linalg.norm(vector), 1, atol=0.001):
            raise ValueError('Embedding must be unit normalized')
        db = self.store._db
        old = db.execute('SELECT seq FROM vector_items WHERE target=? AND kind=? AND version=?',
                         (target, kind, version)).fetchone()
        if old:
            return 0
        cursor = db.execute('INSERT INTO vector_items(target,kind,version) VALUES (?,?,?)',
                            (target, kind, version))
        db.execute('INSERT INTO memory_vectors(rowid,embedding,current) VALUES (?,?,1)',
                   (cursor.lastrowid, vector.tobytes()))
        return 1

    def _candidate_record(self, target, db=None):
        row = (self.store._db if db is None else db).execute(f'''SELECT c.id,c.type,substr(c.text,1,240) AS text,
            c.kind AS candidate_kind,c.src,c.dst,c.confidence,c.valid_from,c.valid_to,c.scope,
            coalesce((SELECT importance FROM node_stats WHERE node_id=c.id),0.5) AS importance,
            coalesce((SELECT decay FROM node_stats WHERE node_id=c.id),1) AS decay,
            coalesce((SELECT archived FROM node_stats WHERE node_id=c.id),0) AS archived,
            substr(c.why,1,240) AS why,r.episode_id,r.recorded_at AS created_at
            FROM candidates c JOIN extraction_runs r ON r.id=c.run_id
            WHERE c.id=? AND {ACTIVE_CANDIDATE_SQL}''', (target,)).fetchone()
        if row is None:
            return None
        parked = (self.store._db if db is None else db).execute(
            'SELECT 1 FROM historical_parked_candidates WHERE candidate_id=? AND released_at IS NULL', (target,)).fetchone()
        return dict(row, scope=json.loads(row['scope']), version=1, tier='candidate',
                    state='candidate', confirmed=False, pinned=0, historical=parked is not None)

    def _episode_record(self, episode_id, start=0, end=None, chunk_id=None, db=None, *, historical=False, focus=None):
        """A raw passage. Its id is the stable chunk reference when one exists, never a whole huge episode.

        The shown excerpt is a 240-character window of the span; focus moves that
        window so a literal hit late in the span is visible. Identity and evidence
        span are unchanged.
        """
        episode = self.store.get(episode_id, db=db)
        if episode is None:
            return None
        if (self.store._db if db is None else db).execute(
                'SELECT 1 FROM forgotten_sources WHERE source=? AND event_id=? LIMIT 1',
                (episode['source'], episode['event_id'])).fetchone():
            return None
        if historical and legacy_native_context(episode['source'], episode['kind'], episode['text'], episode['metadata']):
            return None
        end = len(episode['text']) if end is None else end
        left = start if focus is None else max(start, min(focus - 80, end - 240))
        quote = episode['text'][left:min(end, left + 240)]
        return dict(id=chunk_id or episode_id, episode_id=episode_id, type='raw_episode', tier='raw', version=1,
            state='raw', confirmed=False, historical=historical,
            text=quote, source=episode['source'], source_kind=episode['kind'], conversation_id=episode['conversation_id'],
            occurred_at=episode['occurred_at'], pinned=0,
            created_at=episode['captured_at'], valid_from=None, valid_to=None, chunk_id=chunk_id,
            evidence=[dict(episode_id=episode_id, start=start, end=end, quote_excerpt=episode['text'][start:end][:240])],
            source_url='/episodes/' + episode_id, change_count=1,
            last_change=dict(recorded_at=episode['captured_at'], actor=episode['source'], why='Immutable captured evidence'))

    def _matched_passage(self, episode_id, query, db, *, historical=False):
        """The chunk containing the first literal query term, so a late match is not reported as the prefix."""
        episode = self.store.get(episode_id, db=db)
        if episode is None:
            return None
        # Case-insensitive search on the original string: a folded copy could change
        # lengths and shift offsets.
        hits = [found.start() for found in (re.search(re.escape(term), episode['text'], re.IGNORECASE)
                                            for term in query.split()) if found is not None]
        position = min(hits) if hits else 0
        passage = db.execute('''SELECT id,start,end FROM episode_chunks WHERE episode_id=? AND start<=? AND end>?
            ORDER BY start LIMIT 1''', (episode_id, position, position)).fetchone()
        if passage is not None:
            return self._episode_record(episode_id, passage['start'], passage['end'], passage['id'], db=db,
                                        historical=historical, focus=position)
        # Not yet chunked: cite the bounded matched span under the episode id.
        start = max(0, position - 200)
        return self._episode_record(episode_id, start, min(len(episode['text']), start + 1000), db=db,
                                    historical=historical, focus=position)

    def _meaningful(self, record, db):
        """A current fact or lesson: not an entity, media, raw passage or parked candidate."""
        if record.get('tier') in ('raw', 'reference') or record.get('historical'):
            return False
        kind = record['type']
        if kind == 'media' or kind in ENTITY_TYPES or db.execute(
                "SELECT 1 FROM memory_types WHERE name=? AND category='entity'", (kind,)).fetchone():
            return False
        if record.get('tier') == 'candidate':
            return record.get('candidate_kind') == 'memory'
        return True

    def _historical_passages(self, query, vector, limit, db, *, keyword=True, ranked_at=None):
        """Bounded unverified passages from parked raw history: existing chunk vectors, existing floor, literal FTS."""
        found = {}
        rows = self._vector_rows(db, vector, max(8, limit * 4), kind='episode', pool='historical')
        for row in rows:
            if row['cosine'] < TEXT_COSINE_FLOOR:
                continue
            passage = db.execute('SELECT * FROM episode_chunks WHERE id=?', (row['target'],)).fetchone()
            record = self._episode_record(passage['episode_id'], passage['start'], passage['end'], passage['id'],
                                          db=db, historical=True)
            if record is not None and record['id'] not in found:
                found[record['id']] = dict(record, cosine=float(row['cosine']), score=float(row['cosine']), match='semantic')
        if keyword:
            for episode in self.store.search(query, min(100, limit * 4), db=db):
                if db.execute(f'SELECT 1 FROM episodes e WHERE e.id=? AND {HISTORICAL_RAW_SQL}', (episode['id'],)).fetchone() is None:
                    continue
                record = self._matched_passage(episode['id'], query, db, historical=True)
                if record is None:
                    continue
                previous = found.get(record['id'])
                if previous is None:
                    found[record['id']] = dict(record, score=TEXT_COSINE_FLOOR + 0.25, match='keyword')
                else:
                    previous.update(score=previous['cosine'] + 0.25, match='hybrid')
        ranked_at = ranked_at or datetime.now(timezone.utc)
        for record in found.values():
            record.update(self._recency(record, ranked_at, db=db))
        return sorted(found.values(), key=_relevance_order)[:limit]

    def _automatic_fallback(self, query, vector, ranked_at, limit, db):
        """No meaningful current memory: at most one parked candidate and one unverified passage."""
        items = []
        candidates = []
        rows = self._vector_rows(db, vector, 4, kind='candidate', pool='parked_candidate')
        for row in rows:
            if row['cosine'] < AUTOMATIC_COSINE_FLOOR:
                break
            record = self._candidate_record(row['target'], db=db)
            # The same exclusions as the primary pool: instruction-only evidence never
            # re-enters automatic context through the historical path.
            if (record is None or not _automatic_content(record['text'])
                    or self._legacy_context_only(record['id'], 1, db=db)):
                continue
            record.update(category='memory', cosine=float(row['cosine']), match='semantic', related=[], duplicate_ids=[])
            record.update(self._recency(record, ranked_at, db=db))
            record['score'] = float(row['cosine'])
            candidates.append(record)
        if candidates:
            record = min(candidates, key=_relevance_order)
            record.update(self._candidate_details(record['id'], db=db))
            date = (record['source_occurred_at'] or '')[:10]
            record['text'] = 'Unverified historical candidate' + (' (source ' + date + ')' if date else '') + ': ' + record['text']
            items.append(record)
        for record in self._historical_passages(query, vector, 1, db, keyword=False, ranked_at=ranked_at):
            if not _automatic_content(record['text']):
                continue
            record.update(category='historical_raw', related=[], duplicate_ids=[], importance=.5, archived=0)
            date = (record['source_occurred_at'] or '')[:10]
            full = self.store.get(record['episode_id'], db=db)['text'][record['evidence'][0]['start']:record['evidence'][0]['end']]
            record['text'] = HISTORICAL_PREFIX + (' ' + date if date else '') + ': ' + ' '.join(full.split())[:480]
            items.append(record)
        return sorted(items, key=_relevance_order)[:limit]

    def related(self, node_id, hops, *, scope=None, include_archived=False, db=None):
        # Retain the caller option; age-archived rows remain eligible.
        if hops not in (1, 2):
            raise ValueError('Related traversal is limited to one or two hops')

        def run(db):
            rows = db.execute(f'''WITH RECURSIVE
                current_nodes AS (
                    SELECT n.* FROM node_versions n WHERE n.state='active'
                    AND n.version=(SELECT max(v.version) FROM node_versions v WHERE v.id=n.id)
                    AND {visible_sql()}
                    AND (? IS NULL OR EXISTS (SELECT 1 FROM json_each(n.scope) WHERE value=?))),
                current_edges AS (
                    SELECT e.* FROM edge_versions e WHERE e.state='active'
                    AND e.version=(SELECT max(v.version) FROM edge_versions v WHERE v.id=e.id)),
                adjacency AS (
                    SELECT src AS origin,dst AS target,rel,'out' AS direction,id FROM current_edges
                    UNION ALL SELECT dst,src,rel,'in',id FROM current_edges),
                bounded AS (
                    SELECT a.*,row_number() OVER (PARTITION BY origin ORDER BY a.id) AS fanout
                    FROM adjacency a JOIN current_nodes n ON n.id=a.target),
                walk(id,depth,via,relation,direction,edge_id) AS (
                    VALUES (?,0,NULL,NULL,NULL,NULL)
                    UNION ALL
                    SELECT a.target,w.depth+1,w.id,a.rel,a.direction,a.id
                    FROM walk w JOIN bounded a ON a.origin=w.id WHERE w.depth<? AND a.fanout<=8),
                reached AS (
                    SELECT n.id,n.type,substr(n.text,1,160) AS one_line,n.valid_from,n.valid_to,n.scope,
                    coalesce((SELECT importance FROM node_stats WHERE node_id=n.id),0.5) AS importance,
                    w.depth AS hops,w.via,w.relation,w.direction,w.edge_id,
                    row_number() OVER (PARTITION BY n.id ORDER BY w.depth,w.edge_id,w.via) AS choice
                    FROM walk w JOIN current_nodes n ON n.id=w.id WHERE w.depth>0 AND n.id<>?)
                SELECT id,type,one_line,valid_from,valid_to,scope,importance,hops,via,relation,direction,edge_id
                FROM reached WHERE choice=1 ORDER BY hops,importance DESC,id LIMIT 16''',
                (scope, scope, node_id, hops, node_id)).fetchall()
            from .loops import current
            return [dict(row, scope=json.loads(row['scope']), loop=current(self.graph, row['id'], db=db)) for row in rows]

        if db is not None:
            return run(db)
        with self.store._mutex:
            return run(self.store._db)

    def _media_sources(self, record, db=None):
        """Captured files are references, with current occurrence provenance."""
        if record['type'] != 'media':
            return True
        active = active_references(self.store._db if db is None else db, record['id'])
        if active is None:
            return True  # An explicit save retains its own evidence and scope.
        references = active['references']
        if not references:
            return False
        record.update(self.graph._recall_details(record['id'], record['version'], db=db))
        first = active['origin']
        record.update(category='media_reference', tier='reference', confirmed=False,
            references=references,
            evidence=[dict(episode_id=ref['episode_id'], start=ref['start'], end=ref['end'],
                           quote_excerpt=ref['quote'][:240]) for ref in references],
            source=first['source'], conversation_id=first['conversation_id'],
            source_url='/episodes/' + first['episode_id'],
            _source_occurred_at=active['occurred_at'])
        return True

    def _recency(self, record, now, *, authority=None, db=None):
        source = ({'source_occurred_at': record.pop('_source_occurred_at')}
                  if '_source_occurred_at' in record else {})
        return query_recency(self.store, record, authority=authority, now=now, db=db, **source)

    def _media_pointer(self, record, db=None):
        # The model matched a file, not an inferred assertion about its contents.
        # JSON quoting keeps a captured filename from adding new context lines.
        text = (record['media_kind'].capitalize() + ' attachment ' + json.dumps(record['filename'], ensure_ascii=False)
                + '; original ' + record['blob_url'] + '.')
        source = record.get('source')
        if not source:
            row = (self.store._db if db is None else db).execute('''SELECT e.source FROM graph_evidence g
                JOIN episodes e ON e.id=g.episode_id WHERE g.target=? AND g.version=?
                AND NOT EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id)
                ORDER BY julianday(e.occurred_at) DESC,e.id LIMIT 1''', (record['id'], record['version'])).fetchone()
            source = row['source'] if row else None
        if source:
            text += ' From ' + json.dumps(source, ensure_ascii=False) + '.'
        record.update(text=text, category='media_reference')

    def _automatic_record(self, row, db=None):
        db = self.store._db if db is None else db
        if row['kind'] == 'candidate':
            return self._candidate_record(row['target'], db=db)
        if row['kind'] == 'node':
            record = db.execute(f'''SELECT {NODE_SUMMARY} FROM node_versions n
                WHERE n.id=? AND n.version=? AND n.state='active' ''',
                (row['target'], row['version'])).fetchone()
            return dict(record) if record else None
        record = db.execute(f'''SELECT {NODE_SUMMARY},s.id AS segment_id,s.modality,
            s.start_s,s.end_s,s.page,s.char_start,s.char_end,substr(s.text,1,240) AS match_text,
            '/blobs/'||s.source_sha256 AS source_text_url,'/blobs/'||f.sha256 AS blob_url,
            '/blobs/'||s.artifact_sha256 AS artifact_url,
            f.sha256,f.filename,f.kind AS media_kind
            FROM segments s JOIN media_files f ON f.node_id=s.node_id
            JOIN node_versions n ON n.id=s.node_id WHERE s.id=? AND n.state='active'
            AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)
            AND {visible_sql()}''',
            (row['target'],)).fetchone()
        return dict(record) if record else None

    def _legacy_context_only(self, target, version, db=None):
        # Indexed target/version lookup, independent of the five display spans.
        # Ordinary episode text is read directly; never hydrate cold/raw data.
        context_only = False
        for episode in (self.store._db if db is None else db).execute('''SELECT e.source,e.kind,e.metadata,
            CASE WHEN e.kind='developer' AND e.source GLOB 'native:*:codex' THEN ''
                 WHEN e.kind='user' AND e.source GLOB 'native:*:codex'
                 AND (substr(e.text,1,?)=? OR substr(e.text,1,?)=?)
                 THEN e.text END AS text
            FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id
            WHERE g.target=? AND g.version=?''', (len(LEGACY_CONTEXT_PREFIX), LEGACY_CONTEXT_PREFIX,
                len(GOAL_CONTEXT_PREFIX), GOAL_CONTEXT_PREFIX, target, version)):
            if (episode['text'] is None
                    or not legacy_native_context(episode['source'], episode['kind'], episode['text'], episode['metadata'])):
                return False
            context_only = True
        return context_only

    def _entity_anchors(self, query, limit, scope, db):
        """Named entities spelled in the current request, with bounded connections.

        Identity is lexical containment of the stored spelling (or a reviewed merged
        alias), never semantic similarity, so an unknown name selects nothing.
        Durable nodes come first, then pending entity candidates. Every durable node
        and every pending candidate is its own identity: entries spelled alike are
        labelled with their count and the other IDs, and nothing is merged here.
        """
        budget = min(ENTITY_ANCHOR_LIMIT, limit)
        terms = _anchor_terms(query)
        if budget <= 0 or not terms:
            return []
        request_tokens = _name_tokens(query)
        match = ' OR '.join('"' + term + '"' for term in terms)
        # Bounded prefilter in bm25 order: rarer request terms and shorter stored
        # spellings rank first, so a true name is not hidden behind common-word hits.
        # Exact containment then decides; nothing below is ranked by similarity.
        entity_type = f'''(n.type IN ({_ENTITY_SQL}) OR EXISTS (
            SELECT 1 FROM memory_types t WHERE t.name=n.type AND t.category='entity'))'''
        visible = f'''n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)
            AND {entity_type} AND {visible_sql()}
            AND (? IS NULL OR EXISTS (SELECT 1 FROM json_each(n.scope) WHERE value=?))
            AND NOT EXISTS (SELECT 1 FROM entity_merges m WHERE m.source=n.id)'''
        evidence_count = '(SELECT count(*) FROM graph_evidence g WHERE g.target=n.id AND g.version=n.version) AS evidence_count'
        found = {}  # identity id -> light row; no traversal or detail reads yet
        for row in db.execute(f'''SELECT {NODE_SUMMARY},{evidence_count},NULL AS alias,'durable' AS tier
                FROM node_fts JOIN node_versions n ON n.id=node_fts.id AND n.version=node_fts.version
                WHERE node_fts MATCH ? AND {visible} ORDER BY bm25(node_fts),n.id LIMIT 64''', (match, scope, scope)):
            if _name_in_request(row['text'], request_tokens):
                found.setdefault(row['id'], dict(row))
        # A merged alias follows its survivor through later merges and refinements.
        for row in db.execute(f'''WITH RECURSIVE canonical(source,target,depth) AS (
                SELECT source,target,0 FROM entity_merges
                UNION ALL SELECT c.source,m.superseded_by,c.depth+1 FROM canonical c
                JOIN node_versions m ON m.id=c.target AND m.state IN ('merged','refined')
                AND m.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=m.id)
                WHERE m.superseded_by IS NOT NULL AND c.depth<64)
                SELECT {NODE_SUMMARY},{evidence_count},alias_fts.text AS alias,'durable' AS tier
                FROM alias_fts JOIN canonical a ON a.source=alias_fts.id JOIN node_versions n ON n.id=a.target
                WHERE alias_fts MATCH ? AND {visible} ORDER BY bm25(alias_fts),n.id LIMIT 64''', (match, scope, scope)):
            if _name_in_request(row['alias'], request_tokens):
                found.setdefault(row['id'], dict(row))
        # Pending entries are ordered by original source time, as recall decay is,
        # so importing old history later never makes its person look newest.
        for row in db.execute(f'''SELECT c.id,c.type,c.text,c.scope,'candidate' AS tier,
                (SELECT max(julianday(e.occurred_at)) FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id
                 WHERE g.target=c.id AND g.version=1 AND e.occurred_at IS NOT NULL) AS source_at
                FROM candidate_fts JOIN candidates c ON c.id=candidate_fts.id
                WHERE candidate_fts MATCH ? AND c.kind='entity' AND {ACTIVE_CANDIDATE_SQL}
                AND (? IS NULL OR EXISTS (SELECT 1 FROM json_each(c.scope) WHERE value=?))
                ORDER BY bm25(candidate_fts),c.id LIMIT 64''', (match, scope, scope)):
            if _name_in_request(row['text'], request_tokens):
                found.setdefault(row['id'], dict(row))
        if not found:
            return []
        # Every identity stays separate: a durable node, or each pending candidate.
        # Only existing canonical merges (aliases above) equate spellings.
        spelled = {}
        for item in found.values():
            spelled.setdefault((item['type'], identity_text(item['text'])), []).append(item['id'])
        anchors = []
        for item in self._select_anchors(found.values(), budget):
            same = spelled[(item['type'], identity_text(item['text']))]
            anchor = (self._durable_anchor(item, scope, db) if item['tier'] == 'durable'
                      else self._pending_anchor(item, scope, db))
            if anchor is None:
                continue
            if len(same) > 1:
                anchor['same_name_count'] = len(same)
                anchor['same_name_ids'] = [other for other in same if other != anchor['id']]
                anchor['text'] = f'One of {len(same)} separate entries spelled alike; not merged. ' + anchor['text']
            anchors.append(anchor)
            if len(anchors) == budget:
                break
        return anchors

    @staticmethod
    def _select_anchors(found, budget):
        """Deterministic order, decided before any traversal: resolved durable
        identities by evidence count, then pending candidates by latest original
        source time (extraction or import time is never used), then ID."""
        durable = sorted((item for item in found if item['tier'] == 'durable'),
                         key=lambda item: (-item['evidence_count'], item['id']))
        pending = sorted((item for item in found if item['tier'] != 'durable'),
                         key=lambda item: (-(item['source_at'] if item['source_at'] is not None else float('-inf')),
                                           item['id']))
        # One extra candidate per slot covers an identity excluded during enrichment.
        return (durable + pending)[:budget * 2]

    def _durable_anchor(self, node, scope, db):
        if self._legacy_context_only(node['id'], node['version'], db=db):
            return None
        record = {key: node[key] for key in node.keys() if key not in ('evidence_count', 'alias', 'tier')}
        record.update(tier='durable', confirmed=True, scope=json.loads(record['scope']))
        record.update(self.graph._recall_details(record['id'], record['version'], db=db))
        # The existing one-hop walk already restricts endpoints to active, visible,
        # scope-matching nodes over current active edges.
        connections = [dict(id=item['id'], type=item['type'], relation=item['relation'], direction=item['direction'],
                            relation_id=item['edge_id'], basis='edge', text=item['one_line'])
                       for item in self.related(record['id'], 1, scope=scope, db=db)[:ENTITY_ANCHOR_CONNECTIONS]]
        return self._anchor_record(record, node['text'], connections, alias=node['alias'])

    def _pending_anchor(self, item, scope, db):
        record = self._candidate_record(item['id'], db=db)
        if record is None or self._legacy_context_only(record['id'], 1, db=db):
            return None
        record.update(self._candidate_details(record['id'], db=db))
        connections = []
        for row in db.execute(f'''SELECT c.id,c.type AS relation,c.src,c.dst FROM candidates c
                WHERE c.kind='relation' AND (c.src=? OR c.dst=?) AND {ACTIVE_CANDIDATE_SQL}
                AND (? IS NULL OR EXISTS (SELECT 1 FROM json_each(c.scope) WHERE value=?))
                ORDER BY c.id LIMIT 32''', (item['id'], item['id'], scope, scope)):
            outgoing = row['src'] == item['id']
            endpoint = self._pending_endpoint(row['dst'] if outgoing else row['src'], scope, db)
            if endpoint is None:
                continue  # Rejected, forgotten, out-of-scope or no longer active endpoints are omitted.
            connections.append(dict(endpoint, relation=row['relation'], direction='out' if outgoing else 'in',
                                    relation_id=row['id'], basis='pending_relation'))
            if len(connections) == ENTITY_ANCHOR_CONNECTIONS:
                break
        for row in db.execute(f'''SELECT m.target,n.type,substr(n.text,1,160) AS text
                FROM candidate_entity_matches m JOIN node_versions n ON n.id=m.target
                WHERE m.candidate_id=? AND m.name_match=1 AND n.state='active'
                AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)
                AND {visible_sql()} AND NOT EXISTS (SELECT 1 FROM entity_merges e WHERE e.source=n.id)
                AND (? IS NULL OR EXISTS (SELECT 1 FROM json_each(n.scope) WHERE value=?))
                ORDER BY m.target LIMIT 2''', (item['id'], scope, scope)):
            # Recorded relations take priority; uncertain name hints use remaining
            # room only, so the total never exceeds the connection cap.
            if len(connections) >= ENTITY_ANCHOR_CONNECTIONS:
                break
            connections.append(dict(id=row['target'], type=row['type'], text=row['text'], relation='possibly_same_as',
                                    direction='out', relation_id=None, basis='candidate_entity_match'))
        return self._anchor_record(record, item['text'], connections, alias=None)

    def _pending_endpoint(self, candidate_id, scope, db):
        """The other end of a pending relation, as it currently stands.

        An active candidate is shown as pending; a promoted one resolves through
        the merge/refinement chain to its current durable survivor under the same
        active/visible/scope checks; anything else (rejected, forgotten, out of
        scope, or a chain ending in no active survivor) yields None.
        """
        row = db.execute(f'''SELECT c.id,c.type,substr(c.text,1,160) AS text FROM candidates c
            WHERE c.id=? AND {ACTIVE_CANDIDATE_SQL}
            AND (? IS NULL OR EXISTS (SELECT 1 FROM json_each(c.scope) WHERE value=?))''',
            (candidate_id, scope, scope)).fetchone()
        if row is not None:
            return dict(row, tier='candidate')
        promoted = db.execute('''SELECT d.target_id FROM candidate_decisions d WHERE d.candidate_id=?
            AND d.rowid=(SELECT max(x.rowid) FROM candidate_decisions x WHERE x.candidate_id=?)
            AND d.decision='promote' AND d.target_id IS NOT NULL''', (candidate_id, candidate_id)).fetchone()
        if promoted is None:
            return None
        # The promotion target keeps its recorded ID after a merge or refinement;
        # follow that bounded identity chain to the current survivor before checking
        # it. Corrections and world changes end the chain.
        row = db.execute(f'''WITH RECURSIVE canonical(id,depth) AS (
                SELECT ?,0 UNION ALL SELECT m.superseded_by,c.depth+1 FROM canonical c
                JOIN node_versions m ON m.id=c.id AND m.state IN ('merged','refined')
                AND m.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=m.id)
                WHERE m.superseded_by IS NOT NULL AND c.depth<64)
            SELECT n.id,n.type,substr(n.text,1,160) AS text FROM node_versions n
            WHERE n.id=(SELECT id FROM canonical ORDER BY depth DESC LIMIT 1)
            AND n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)
            AND {visible_sql()} AND NOT EXISTS (SELECT 1 FROM entity_merges e WHERE e.source=n.id)
            AND (? IS NULL OR EXISTS (SELECT 1 FROM json_each(n.scope) WHERE value=?))''',
            (promoted['target_id'], scope, scope)).fetchone()
        return dict(row, tier='durable') if row is not None else None

    def _anchor_record(self, record, name, connections, *, alias):
        """Compose the injected line from stored spellings only; nothing is inferred."""
        name = ' '.join(name.split())
        head = name + ' [' + record['type'] + '; ' + '/'.join(record['scope']) + ']'
        if alias:
            head += ' (matched alias ' + json.dumps(' '.join(alias.split()), ensure_ascii=False) + ')'
        lines = []
        for item in connections:
            # Each line names the existing edge, pending relation or node ID that
            # history() resolves, so the association survives client rendering.
            reference = ('edge ' + item['relation_id'] if item['basis'] == 'edge' else
                         'pending ' + item['relation_id'] if item['basis'] == 'pending_relation' else
                         'memory ' + item['id'])
            other = ' '.join(item['text'].split())[:60] + ' [' + item['type'] + '; ' + reference + ']'
            if item['basis'] == 'candidate_entity_match':
                lines.append('possibly the same as durable ' + other + ' (unverified name match)')
            elif item['direction'] == 'out':
                lines.append(item['relation'] + ' ' + other)
            else:
                lines.append(other + ' ' + item['relation'] + ' this')
        body = 'Connections: ' + '; '.join(lines) + '.' if lines else 'No recorded connections.'
        record.update(text=head + ' ' + body, category='entity_context', match='entity_name',
                      connections=connections, cosine=None, score=1.0, related=[])
        record.setdefault('duplicate_ids', [])
        return record

    def _automatic(self, query, limit, scope, context):
        limit = min(limit, AUTOMATIC_RESULT_LIMIT)
        with self._query_inference:
            vector = embed_query(self.model, _automatic_query(query, context))
        ranked_at = datetime.now(timezone.utc)
        # One WAL snapshot serves every read of this request; the writer
        # connection and its mutex stay free for capture, health and commits.
        with self._snapshot() as db:
            # Named entities spelled in the request anchor the offer; semantic
            # facts fill the remaining slots of the same bounded limit.
            anchors = self._entity_anchors(query, limit, scope, db)
            limit -= len(anchors)
            if limit <= 0:
                return anchors
            rows = []
            for kind in ('node', 'candidate', 'segment'):
                rows.extend(self._vector_rows(db, vector, min(120, max(24, limit * 20)),
                                             kind=kind, pool='automatic', scope=scope))
            matches = {}
            context_only = {}
            for row in sorted(rows, key=lambda item: (-item['cosine'], item['target'])):
                if row['cosine'] < min(AUTOMATIC_COSINE_FLOOR, MEDIA_COSINE_FLOORS['audio']):
                    continue
                record = self._automatic_record(row, db=db)
                if record is None or record['id'] in matches:
                    continue
                media = record['type'] == 'media'
                floor = MEDIA_COSINE_FLOORS.get(record.get('modality'), 1.0) if media else AUTOMATIC_COSINE_FLOOR
                if row['cosine'] < floor:
                    continue
                if not media and not _automatic_content(record['text']):
                    continue
                identity = (record['id'], record['version'])
                if identity not in context_only:
                    context_only[identity] = self._legacy_context_only(*identity, db=db)
                if context_only[identity]:
                    continue
                if record.get('tier') == 'candidate':
                    record.update(self._candidate_details(record['id'], db=db))
                else:
                    record.update(tier='durable', confirmed=True)
                    record.update(self.graph._recall_details(record['id'], record['version'], db=db))
                    # Incidents remain explicitly searchable. Only consolidated
                    # reusable behavior enters automatic context as experience.
                    if record['type'] == 'experience' and not record.get('behavior'):
                        continue
                if not self._media_sources(record, db=db):
                    continue
                if media:
                    self._media_pointer(record, db=db)
                if isinstance(record.get('scope'), str):
                    record['scope'] = json.loads(record['scope'])
                record.setdefault('category', 'memory')
                record.update(self._recency(record, ranked_at, authority=record.get('authority'), db=db))
                if record.get('tier') == 'candidate':
                    full = db.execute('SELECT text FROM candidates WHERE id=?', (record['id'],)).fetchone()
                else:
                    full = db.execute('SELECT text FROM node_versions WHERE id=? AND version=?',
                                      (record['id'], record['version'])).fetchone()
                record.update(cosine=float(row['cosine']), score=float(row['cosine']),
                              match='semantic', related=[], _full_text=full['text'] if full else record['text'])
                matches[record['id']] = record
            # Collapse equivalent assertions while retaining every returned source
            # identity and evidence span. Similar but different facts stay separate.
            grouped = {}
            for record in sorted(matches.values(), key=_relevance_order):
                signature = (record['category'], record.get('authority'), tuple(record['scope']),
                             record['sha256'] if record['type'] == 'media' else
                             ' '.join(record['_full_text'].casefold().split()).rstrip('.!'))
                record.pop('_full_text')
                previous = grouped.get(signature)
                if previous is None:
                    record['duplicate_ids'] = []
                    grouped[signature] = record
                    continue
                previous['duplicate_ids'].append(record['id'])
                seen = {(ref['episode_id'], ref['start'], ref['end']) for ref in previous.get('evidence', [])}
                for ref in record.get('evidence', []):
                    identity = (ref['episode_id'], ref['start'], ref['end'])
                    if identity not in seen:
                        previous.setdefault('evidence', []).append(ref)
                        seen.add(identity)
            results, suggestions, media = [], 0, 0
            for record in grouped.values():
                if record['category'] == 'behavioral_suggestion':
                    if suggestions == AUTOMATIC_SUGGESTION_LIMIT:
                        continue
                    suggestions += 1
                if record['category'] == 'media_reference':
                    if media:
                        continue
                    media += 1
                if record['source_occurred_at']:
                    prefix = 'Source ' + record['source_occurred_at'][:10] + ': '
                    record['text'] = prefix + record['text']
                    if record.get('behavior'):
                        record['behavior'] = dict(record['behavior'],
                            applicability=prefix + record['behavior']['applicability'])
                results.append(record)
                if len(results) == limit:
                    break
            # Named-entity anchors and a media pointer alone are not meaningful current
            # memory: without any fact or lesson, parked history may offer bounded
            # unverified context in the remaining budget. Raw history has no scope, so a
            # scoped request gets none.
            if scope is None and len(results) < limit and not any(self._meaningful(item, db) for item in results):
                results.extend(self._automatic_fallback(query, vector, ranked_at, limit - len(results), db))
            return anchors + results

    def recall(self, query, effort='normal', limit=10, *, scope=None, automatic=False, context=''):
        if effort not in ('cheap', 'normal', 'deep') or not 1 <= limit <= 100 or scope not in (None, 'personal', 'professional'):
            raise ValueError('Invalid effort, limit or scope')
        if not query.strip() or len(query) > 4096 or '\x00' in query:
            raise ValueError('Recall requires 1..4096 characters without NUL')
        if not isinstance(automatic, bool) or not isinstance(context, str) or len(context) > 4096 or '\x00' in context:
            raise ValueError('Invalid automatic recall context')
        if automatic:
            return self._automatic(query, limit, scope, context)
        with self._query_inference:
            query_vector = embed_query(self.model, query)
        # One WAL snapshot serves every read of this request; the writer
        # connection and its mutex stay free for capture, health and commits.
        with self._snapshot() as db:
            keywords = self.graph.recall_keywords(query, min(100, limit * 4),
                                                 scope=scope, include_archived=effort == 'deep', db=db)
            matches = {row['id']: dict(row, score=TEXT_COSINE_FLOOR + 0.25, match='keyword') for row in keywords}
            ranked_at = datetime.now(timezone.utc)
            match = ' '.join('"' + term.replace('"', '""') + '"' for term in query.split())
            segment_keywords = db.execute(f'''SELECT {NODE_SUMMARY},
                s.id AS segment_id,s.modality,s.start_s,s.end_s,s.page,
                s.char_start,s.char_end,'/blobs/'||s.source_sha256 AS source_text_url,
                substr(s.text,1,240) AS match_text,'/blobs/'||f.sha256 AS blob_url,
                '/blobs/'||s.artifact_sha256 AS artifact_url
                FROM segment_fts JOIN segments s ON s.id=segment_fts.id
                JOIN media_files f ON f.node_id=s.node_id JOIN node_versions n ON n.id=s.node_id
                WHERE segment_fts MATCH ? AND n.state='active'
                  AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)
                  AND {visible_sql()}
                  AND (? IS NULL OR EXISTS (SELECT 1 FROM json_each(n.scope) WHERE value=?))
                ORDER BY bm25(segment_fts),s.id LIMIT ?''',
                (match, scope, scope, min(100, limit * 4))).fetchall()
            seen_segments = set()
            for record in segment_keywords:
                if record['id'] in seen_segments:
                    continue
                seen_segments.add(record['id'])
                matches[record['id']] = dict(record, score=TEXT_COSINE_FLOOR + 0.25, match='keyword')
            candidate_keywords = db.execute(f'''SELECT c.id FROM candidate_fts
                JOIN candidates c ON c.id=candidate_fts.id WHERE candidate_fts MATCH ?
                AND {ACTIVE_CANDIDATE_SQL}
                AND (? IS NULL OR EXISTS (SELECT 1 FROM json_each(c.scope) WHERE value=?))
                ORDER BY bm25(candidate_fts),c.id LIMIT ?''',
                (match, scope, scope, min(100, limit * 4))).fetchall()
            for candidate in candidate_keywords:
                record = self._candidate_record(candidate['id'], db=db)
                if record is not None:
                    matches[record['id']] = dict(record, score=TEXT_COSINE_FLOOR + 0.25, match='keyword')
            if effort == 'deep' and scope is None:
                for episode in self.store.search(query, min(100, limit * 4), db=db):
                    record = self._matched_passage(episode['id'], query, db)
                    if record is not None:
                        matches[record['id']] = dict(record, score=TEXT_COSINE_FLOOR + 0.25, match='keyword')
            rows = []
            kinds = ('node', 'segment', 'candidate', 'episode') if effort == 'deep' and scope is None else ('node', 'segment', 'candidate')
            for kind in kinds:
                rows.extend(self._vector_rows(db, query_vector, min(1000, limit * 20),
                                             kind=kind, pool='ordinary', scope=scope))
            keyword_ids = set(matches)
            seen_raw = set()
            for row in rows:
                if row['kind'] == 'node':
                    record = db.execute(f'''SELECT {NODE_SUMMARY}
                        FROM node_versions n WHERE n.id=? AND n.version=? AND n.state='active' ''',
                        (row['target'], row['version'])).fetchone()
                    floor = TEXT_COSINE_FLOOR
                elif row['kind'] == 'segment':
                    record = db.execute(f'''SELECT {NODE_SUMMARY},s.id AS segment_id,
                        s.modality,s.start_s,s.end_s,s.page,substr(s.text,1,240) AS match_text,
                        s.char_start,s.char_end,'/blobs/'||s.source_sha256 AS source_text_url,
                        '/blobs/'||f.sha256 AS blob_url,
                        '/blobs/'||s.artifact_sha256 AS artifact_url
                        FROM segments s JOIN media_files f ON f.node_id=s.node_id
                        JOIN node_versions n ON n.id=s.node_id
                        WHERE s.id=? AND n.state='active' AND n.version=(
                            SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)
                        AND {visible_sql()}''',
                        (row['target'],)).fetchone()
                    floor = MEDIA_COSINE_FLOORS.get(
                        record['modality'] if record else '', 1.0)
                elif row['kind'] == 'candidate':
                    record = self._candidate_record(row['target'], db=db)
                    floor = TEXT_COSINE_FLOOR
                elif row['kind'] == 'episode':
                    passage = db.execute('SELECT * FROM episode_chunks WHERE id=?', (row['target'],)).fetchone()
                    if passage is None or passage['episode_id'] in seen_raw:
                        continue
                    seen_raw.add(passage['episode_id'])
                    record = self._episode_record(passage['episode_id'], passage['start'], passage['end'],
                                                  passage['id'], db=db)
                    floor = TEXT_COSINE_FLOOR
                else:
                    continue
                if record is None:
                    continue
                keyword = record['id'] in keyword_ids
                if row['cosine'] < floor and not keyword:
                    continue
                item = dict(record, cosine=float(row['cosine']),
                            score=float(row['cosine']) + (0.25 if keyword else 0))
                item['match'] = 'hybrid' if keyword and row['cosine'] >= floor else 'keyword' if keyword else 'semantic'
                previous = matches.get(item['id'])
                if previous is None or 'cosine' not in previous or item['score'] > previous['score']:
                    matches[item['id']] = item
            # Explicit cheap/normal recall falls back to parked raw history only when no
            # current fact or lesson matched; deep keeps its broad raw lookup above.
            if effort != 'deep' and scope is None and not any(self._meaningful(item, db) for item in matches.values()):
                for record in self._historical_passages(query, query_vector, limit, db, ranked_at=ranked_at):
                    matches.setdefault(record['id'], record)
            matches = {target: record for target, record in matches.items() if self._media_sources(record, db=db)}
            feedback = {}
            if matches:
                marks = ','.join('?' for _ in matches)
                feedback = {row['target']: max(-1, min(1, (2 * row['cited'] - row['shown']) / max(1, row['shown'])))
                    for row in db.execute(
                    f'SELECT target,shown,cited FROM query_feedback WHERE query_key=? AND target IN ({marks})',
                    (query_key(query), *matches))}
            for item in matches.values():
                if isinstance(item.get('scope'), str):
                    item['scope'] = json.loads(item['scope'])
                item.setdefault('importance', .5)
                item.setdefault('archived', 0)
                behavior = self.graph._behavior(item['id'], db=db) if item['type'] == 'experience' else None
                item.update(self._recency(item, ranked_at,
                    authority=behavior.get('authority') if behavior else None, db=db))
                # A behavioral lesson is conditional guidance: not being cited for a query
                # does not make it wrong, so negative explicit-query feedback is neutral for
                # it. Citation reward, ordinary facts and incidents keep the full signal.
                if behavior:
                    feedback[item['id']] = max(0, feedback.get(item['id'], 0))
            results = sorted(matches.values(), key=lambda item: _relevance_order(
                item, feedback.get(item['id'], 0)))[:limit]
            for item in results:
                if item.get('tier') == 'candidate':
                    item.update(self._candidate_details(item['id'], db=db))
                elif item.get('tier') not in ('raw', 'reference'):
                    item['tier'], item['confirmed'] = 'durable', True
                    item.update(self.graph._recall_details(item['id'], item['version'], db=db))
                if item['type'] == 'media' and 'blob_url' not in item:
                    file = db.execute('SELECT sha256 FROM media_files WHERE node_id=?', (item['id'],)).fetchone()
                    if file:
                        item['blob_url'] = '/blobs/' + file['sha256']
                item['related'] = self.related(item['id'], 2 if effort == 'deep' else 1, scope=scope,
                    include_archived=effort == 'deep', db=db) if effort != 'cheap' and item['tier'] == 'durable' else []
        return results

    def _candidate_details(self, target, db=None):
        db = self.store._db if db is None else db
        evidence = [dict(row) for row in db.execute('''SELECT episode_id,start,end,substr(quote,1,240) AS quote_excerpt
            FROM graph_evidence WHERE target=? AND version=1 ORDER BY episode_id,start LIMIT 5''', (target,))]
        run = db.execute('''SELECT g.recorded_at,g.actor,substr(g.why,1,240) AS why
            FROM candidates c JOIN extraction_runs r ON r.id=c.run_id JOIN graph_commits g ON g.id=r.commit_id
            WHERE c.id=?''', (target,)).fetchone()
        matches = [dict(row) for row in db.execute('''SELECT m.target,m.cosine,m.name_match,n.type,
            substr(n.text,1,160) AS one_line FROM candidate_entity_matches m
            JOIN node_versions n ON n.id=m.target AND n.version=m.version
            WHERE m.candidate_id=? AND n.state='active' AND n.version=(
                SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) LIMIT 5''', (target,))]
        from .loops import current
        return dict(evidence=evidence, change_count=1, last_change=dict(run), entity_matches=matches,
                    loop=current(self.graph, target, db=db))

    def start(self):
        if self._worker is not None:
            raise RuntimeError('Indexer already started')
        if self._projection is not None:
            self._projection.start()
        def run():
            initial_search_ready = self._projection is None
            while not self._stop.is_set():
                # Match the qualified initial-build CPU/RAM coexistence: the
                # foreground model stays available, but background encoding
                # waits for the first coherent projection only.
                if not initial_search_ready:
                    if not self._projection.available:
                        self._stop.wait(.1)
                        continue
                    initial_search_ready = True
                try:
                    processed = self.index_pending(limit=16)
                    self.index_error = None
                    self._stop.wait(0.05 if processed else 1)
                except Exception as error:
                    self.index_error = error
                    logging.getLogger(__name__).exception('Native indexing failed; work remains pending')
                    self._stop.wait(5)
        self._worker = Thread(target=run, name='memento-index', daemon=True)
        self._worker.start()

    def close(self):
        self._stop.set()
        if self._worker is not None:
            self._worker.join()
        if self._projection is not None:
            self._projection.close()

    def status(self, source=None):
        """Exact current index-unit counts; unchunked episodes are reported separately."""
        with self.store.read_snapshot() as db:
            return self._status(db, source)

    def _status(self, db, source):
        indexable = ' UNION ALL '.join(_episode_units('status_episodes') if kind == 'episode' else query
            for kind, _, query in _INDEX_SOURCES)
        eligible = _recall_eligible(indexable)
        source_args = (source,) if source is not None else ()
        # Read eligibility once from the covering index, rather than visiting
        # large episode bodies for metadata again for every prepared chunk.
        # Probe vector metadata once, rather than repeating a virtual-table
        # row lookup for every unit and aggregate expression.
        row = db.execute(f'''WITH status_episodes AS MATERIALIZED (
                SELECT e.id FROM episodes e INDEXED BY episodes_status_cover_v1
                WHERE {'e.source=? AND' if source is not None else ''}
                CAST(coalesce(json_extract(e.metadata,'$.extraction_mode'),'message') AS TEXT)<>'skip'
                AND NOT EXISTS (SELECT 1 FROM forgotten_sources f
                    WHERE f.source=e.source AND f.event_id=e.event_id)),
                indexed_units AS MATERIALIZED (
                SELECT i.target,i.kind,i.version FROM memory_vectors v
                JOIN vector_items i ON i.seq=v.rowid WHERE v.current=1),
                units AS (
                SELECT a.* FROM ({eligible}) a WHERE ? IS NULL
                OR EXISTS (SELECT 1 FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id
                    WHERE g.target=a.rank_target AND e.source=?)
                OR (a.kind='episode' AND EXISTS (SELECT 1 FROM episode_chunks x
                    JOIN episodes e ON e.id=x.episode_id WHERE x.id=a.id AND e.source=?))),
                states AS (SELECT a.*,i.target IS NOT NULL AS indexed,
                    EXISTS (SELECT 1 FROM index_failures f WHERE f.target=a.id
                        AND f.version=a.version AND f.resolved=0) AS failed FROM units a
                    LEFT JOIN indexed_units i ON i.target=a.id AND i.kind=a.kind AND i.version=a.version)
                SELECT count(*) AS total,coalesce(sum(indexed),0) AS indexed,
                    coalesce(sum(NOT indexed AND NOT failed),0) AS pending,
                    coalesce(sum(NOT indexed AND failed),0) AS failed FROM states''',
                (*source_args, source, source, source)).fetchone()
        unprepared = db.execute(f'''SELECT count(*) FROM episodes e
                WHERE {'e.source=? AND' if source is not None else ''}
                CAST(coalesce(json_extract(e.metadata,'$.extraction_mode'),'message') AS TEXT)<>'skip'
                AND NOT EXISTS (SELECT 1 FROM episode_chunks x WHERE x.episode_id=e.id)
                AND NOT EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id)''',
                (source,) if source is not None else ()).fetchone()[0]
        result = dict(available=True, **dict(row), unprepared_episodes=unprepared,
                      error=type(self.index_error).__name__ if self.index_error else None)
        if self._projection is not None:
            result['search'] = dict(engine='qdrant', ready=self._projection.available, error=self._projection.error)
        return result

    def health(self):
        if self.index_error is not None:
            raise RuntimeError('Native indexing failed; work remains pending')
        # One WAL snapshot for every failure eligibility read; the writer mutex
        # is never taken, so health does not queue behind context preparation.
        with self.store.read_snapshot() as db:
            failures = db.execute('SELECT target,version FROM index_failures WHERE resolved=0').fetchall()
            for failure in failures:
                if db.execute(f'''WITH indexable AS ({_INDEXABLE})
                    SELECT 1 FROM indexable WHERE id=? AND version=? LIMIT 1''',
                    (failure['target'], failure['version'])).fetchone():
                    raise RuntimeError('Some native embeddings failed; valid work continues and failures remain retryable')
