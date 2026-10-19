"""Native local embeddings and SQLite hybrid retrieval, outside capture transactions."""

import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from threading import Event, RLock, Thread

import numpy as np

from .embedder import EMBED_DIM, embed_query, embed_texts, load_model
from .graph import Graph, NODE_SUMMARY
from .extraction import ACTIVE_CANDIDATE_SQL
from .text_chunks import TextChunk, chunks
from .importance import query_key

MODEL_ID = 'google/embeddinggemma-2'
MODEL_REVISION = '914f7f89142e33e77833254d9c9b90c3cef7303b'
TEXT_COSINE_FLOOR = 0.685  # corporate_search's measured native text relevance floor

_INDEX_SOURCES = (
    ('node', 'node_versions', '''SELECT n.id,'node' AS kind,n.version,n.text,n.type,
        NULL AS episode_id,NULL AS start,NULL AS end,0 AS priority,n.scope,n.rowid AS position
        FROM node_versions n WHERE n.state='active' AND n.type<>'media'
        AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)'''),
    ('candidate', 'candidates', f'''SELECT c.id,'candidate' AS kind,1 AS version,c.text,c.type,
        NULL AS episode_id,NULL AS start,NULL AS end,1 AS priority,c.scope,c.rowid AS position
        FROM candidates c WHERE {ACTIVE_CANDIDATE_SQL}'''),
    ('episode', 'episode_chunks', '''SELECT x.id,'episode' AS kind,1 AS version,NULL AS text,NULL AS type,
        x.episode_id,x.start,x.end,2 AS priority,NULL AS scope,x.rowid AS position
        FROM episode_chunks x JOIN episodes e ON e.id=x.episode_id
        WHERE coalesce(json_extract(e.metadata,'$.extraction_mode'),'message')<>'skip'
        AND NOT EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id)'''),
)
_INDEXABLE = ' UNION ALL '.join(source for _, _, source in _INDEX_SOURCES)

_RECALL_ELIGIBLE = f'''SELECT a.id,a.kind,a.version,a.scope,a.id AS rank_target FROM ({_INDEXABLE}) a
    UNION ALL SELECT s.id,'segment',1,n.scope,n.id FROM segments s JOIN node_versions n ON n.id=s.node_id
    WHERE n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)'''


class Retrieval:
    def __init__(self, graph: Graph, *, model=None):
        self.graph = graph
        self.store = graph.store
        self._inference = RLock()
        self._stop = Event()
        self._worker = None
        self.index_error = None
        self._episode_cursor = 0
        self._index_cursors = dict(node=0, candidate=0, episode=0)
        self.model = model if model is not None else load_model(MODEL_ID, MODEL_REVISION, text_only=False)
        with self.store._mutex:
            db = self.store._db
            db.execute('BEGIN IMMEDIATE')
            try:
                old = db.execute('SELECT model_id,revision,dimensions FROM vector_model').fetchone()
                signature = (MODEL_ID, MODEL_REVISION, EMBED_DIM)
                if old is None:
                    db.execute('INSERT INTO vector_model VALUES (1,?,?,?)', signature)
                elif tuple(old) != signature:
                    raise RuntimeError('Embedding model differs from the index; explicit reindex required')
                db.execute('COMMIT')
            except BaseException:
                db.execute('ROLLBACK')
                raise

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
            with self._inference:
                passages = chunks(episode['text'], self.model.tokenizer)
            prepared.append((episode, passages or [TextChunk(0, len(episode['text']), episode['text'])]))
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
            try:
                # Release the model between passages so passive recall can interleave.
                with self._inference:
                    vector = embed_texts(self.model, ['title: none | text: ' + row['text']])[0]
                prepared.append((row, vector, None))
            except Exception as error:
                prepared.append((row, None, error))
        with self.store._mutex:
            db = self.store._db
            db.execute('BEGIN IMMEDIATE')
            try:
                count = 0
                for row, vector, error in prepared:
                    if not self._indexable(row):
                        continue
                    db.execute('SAVEPOINT index_item')
                    try:
                        if error is not None:
                            raise error
                        inserted = self._put(row['id'], row['kind'], row['version'], vector)
                        if row['kind'] == 'candidate':
                            self._match_entity(row, vector)
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
                self._index_cursors = cursors
                return count
            except BaseException:
                db.execute('ROLLBACK')
                raise

    def _match_entity(self, candidate, vector):
        db = self.store._db
        if db.execute("SELECT kind FROM candidates WHERE id=?", (candidate['id'],)).fetchone()['kind'] != 'entity':
            return
        vector = np.asarray(vector, dtype=np.float32)
        matches = {}
        named = self.graph.recall_keywords(candidate['text'], 20)
        for node in named:
            if node['type'] != candidate['type']:
                continue
            old = db.execute('''SELECT v.embedding FROM memory_vectors v JOIN vector_items i ON i.seq=v.rowid
                WHERE i.target=? AND i.kind='node' AND i.version=? AND v.current=1''',
                (node['id'], node['version'])).fetchone()
            cosine = float(np.clip(np.dot(vector, np.frombuffer(old['embedding'], np.float32)), -1, 1)) if old else None
            matches[node['id']] = dict(target=node['id'], version=node['version'], cosine=cosine, name_match=1)
        semantic = db.execute('''SELECT i.target,i.version,1-v.distance AS cosine FROM memory_vectors v
            JOIN vector_items i ON i.seq=v.rowid WHERE v.embedding MATCH ? AND k=5 AND v.current=1
            AND v.rowid IN (SELECT i.seq FROM vector_items i JOIN node_versions n
                ON n.id=i.target AND n.version=i.version WHERE i.kind='node' AND n.type=?
                AND n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id))
            ORDER BY v.distance''', (vector.tobytes(), candidate['type'])).fetchall()
        for node in semantic:
            if node['cosine'] >= 0.78 and node['target'] not in matches:
                matches[node['target']] = dict(node, name_match=0)
        selected = sorted(matches.values(), key=lambda item:
            (-item['name_match'], -(item['cosine'] if item['cosine'] is not None else -1), item['target']))[:5]
        for node in selected:
            db.execute('INSERT INTO candidate_entity_matches VALUES (?,?,?,?,?,?,?,?)',
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

    def _candidate_record(self, target):
        row = self.store._db.execute(f'''SELECT c.id,c.type,substr(c.text,1,240) AS text,
            c.kind AS candidate_kind,c.src,c.dst,c.confidence,c.valid_from,c.valid_to,c.scope,
            coalesce((SELECT importance FROM node_stats WHERE node_id=c.id),0.5) AS importance,
            coalesce((SELECT decay FROM node_stats WHERE node_id=c.id),1) AS decay,
            coalesce((SELECT archived FROM node_stats WHERE node_id=c.id),0) AS archived,
            substr(c.why,1,240) AS why,r.episode_id,r.recorded_at AS created_at
            FROM candidates c JOIN extraction_runs r ON r.id=c.run_id
            WHERE c.id=? AND {ACTIVE_CANDIDATE_SQL}''', (target,)).fetchone()
        return dict(row, scope=json.loads(row['scope']), version=1, tier='candidate',
                    state='candidate', confirmed=False, pinned=0) if row else None

    def _episode_record(self, episode_id, start=0, end=None, chunk_id=None):
        episode = self.store.get(episode_id)
        if episode is None:
            return None
        if self.store._db.execute('SELECT 1 FROM forgotten_sources WHERE source=? AND event_id=? LIMIT 1',
                                  (episode['source'], episode['event_id'])).fetchone():
            return None
        end = len(episode['text']) if end is None else end
        quote = episode['text'][start:end][:240]
        return dict(id=episode_id, type='raw_episode', tier='raw', version=1, state='raw', confirmed=False,
            text=quote, source=episode['source'], source_kind=episode['kind'], pinned=0,
            created_at=episode['captured_at'], valid_from=None, valid_to=None, chunk_id=chunk_id,
            evidence=[dict(episode_id=episode_id, start=start, end=end, quote_excerpt=quote)],
            source_url='/episodes/' + episode_id, change_count=1,
            last_change=dict(recorded_at=episode['captured_at'], actor=episode['source'], why='Immutable captured evidence'))

    def related(self, node_id, hops, *, scope=None, include_archived=False):
        if hops not in (1, 2):
            raise ValueError('Related traversal is limited to one or two hops')
        with self.store._mutex:
            rows = self.store._db.execute('''WITH RECURSIVE
                current_nodes AS (
                    SELECT n.* FROM node_versions n WHERE n.state='active'
                    AND n.version=(SELECT max(v.version) FROM node_versions v WHERE v.id=n.id)
                    AND (? IS NULL OR EXISTS (SELECT 1 FROM json_each(n.scope) WHERE value=?))
                    AND (? OR coalesce((SELECT archived FROM node_stats WHERE node_id=n.id),0)=0)),
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
                (scope, scope, include_archived, node_id, hops, node_id)).fetchall()
            from .loops import current
            return [dict(row, scope=json.loads(row['scope']), loop=current(self.graph, row['id'])) for row in rows]

    def recall(self, query, effort='normal', limit=10, *, scope=None):
        if effort not in ('cheap', 'normal', 'deep') or not 1 <= limit <= 100 or scope not in (None, 'personal', 'professional'):
            raise ValueError('Invalid effort, limit or scope')
        if not query.strip() or len(query) > 4096 or '\x00' in query:
            raise ValueError('Recall requires 1..4096 characters without NUL')
        keywords = self.graph.recall_keywords(query, min(100, limit * 4),
                                             scope=scope, include_archived=effort == 'deep')
        matches = {row['id']: dict(row, score=TEXT_COSINE_FLOOR + 0.25, match='keyword') for row in keywords}
        with self._inference:
            query_vector = embed_query(self.model, query)
        with self.store._mutex:
            match = ' '.join('"' + term.replace('"', '""') + '"' for term in query.split())
            segment_keywords = self.store._db.execute(f'''SELECT {NODE_SUMMARY},
                s.id AS segment_id,s.modality,s.start_s,s.end_s,s.page,
                s.char_start,s.char_end,'/blobs/'||s.source_sha256 AS source_text_url,
                substr(s.text,1,240) AS match_text,'/blobs/'||f.sha256 AS blob_url,
                '/blobs/'||s.artifact_sha256 AS artifact_url
                FROM segment_fts JOIN segments s ON s.id=segment_fts.id
                JOIN media_files f ON f.node_id=s.node_id JOIN node_versions n ON n.id=s.node_id
                WHERE segment_fts MATCH ? AND n.state='active'
                  AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)
                  AND (? IS NULL OR EXISTS (SELECT 1 FROM json_each(n.scope) WHERE value=?))
                  AND (? OR coalesce((SELECT archived FROM node_stats WHERE node_id=n.id),0)=0)
                ORDER BY bm25(segment_fts),s.id LIMIT ?''',
                (match, scope, scope, effort == 'deep', min(100, limit * 4))).fetchall()
            seen_segments = set()
            for record in segment_keywords:
                if record['id'] in seen_segments:
                    continue
                seen_segments.add(record['id'])
                matches[record['id']] = dict(record, score=TEXT_COSINE_FLOOR + 0.25, match='keyword')
            candidate_keywords = self.store._db.execute(f'''SELECT c.id FROM candidate_fts
                JOIN candidates c ON c.id=candidate_fts.id WHERE candidate_fts MATCH ?
                AND {ACTIVE_CANDIDATE_SQL}
                AND (? IS NULL OR EXISTS (SELECT 1 FROM json_each(c.scope) WHERE value=?))
                AND (? OR coalesce((SELECT archived FROM node_stats WHERE node_id=c.id),0)=0)
                ORDER BY bm25(candidate_fts),c.id LIMIT ?''',
                (match, scope, scope, effort == 'deep', min(100, limit * 4))).fetchall()
            for candidate in candidate_keywords:
                record = self._candidate_record(candidate['id'])
                if record is not None:
                    matches[record['id']] = dict(record, score=TEXT_COSINE_FLOOR + 0.25, match='keyword')
            if effort == 'deep' and scope is None:
                for episode in self.store.search(query, min(100, limit * 4)):
                    record = self._episode_record(episode['id'])
                    if record is not None:
                        matches[record['id']] = dict(record, text=episode['excerpt'][:240],
                            score=TEXT_COSINE_FLOOR + 0.25, match='keyword')
            rows = []
            kinds = ('node', 'segment', 'candidate', 'episode') if effort == 'deep' and scope is None else ('node', 'segment', 'candidate')
            for kind in kinds:
                rows.extend(self.store._db.execute(f'''SELECT i.kind,i.target,i.version,1-v.distance AS cosine
                    FROM memory_vectors v JOIN vector_items i ON i.seq=v.rowid
                    WHERE v.embedding MATCH ? AND k=? AND v.current=1
                    AND v.rowid IN (SELECT i.seq FROM vector_items i JOIN ({_RECALL_ELIGIBLE}) a
                        ON a.id=i.target AND a.kind=i.kind AND a.version=i.version
                        WHERE i.kind=? AND (? IS NULL OR EXISTS (SELECT 1 FROM json_each(a.scope) WHERE value=?))
                        AND (? OR coalesce((SELECT archived FROM node_stats WHERE node_id=a.rank_target),0)=0))
                    ORDER BY v.distance''',
                    (query_vector.tobytes(), min(1000, limit * 20), kind, scope, scope, effort == 'deep')).fetchall())
            keyword_ids = set(matches)
            seen_raw = set()
            for row in rows:
                if row['kind'] == 'node':
                    record = self.store._db.execute(f'''SELECT {NODE_SUMMARY}
                        FROM node_versions n WHERE n.id=? AND n.version=? AND n.state='active' ''',
                        (row['target'], row['version'])).fetchone()
                    floor = TEXT_COSINE_FLOOR
                elif row['kind'] == 'segment':
                    record = self.store._db.execute(f'''SELECT {NODE_SUMMARY},s.id AS segment_id,
                        s.modality,s.start_s,s.end_s,s.page,substr(s.text,1,240) AS match_text,
                        s.char_start,s.char_end,'/blobs/'||s.source_sha256 AS source_text_url,
                        '/blobs/'||f.sha256 AS blob_url,
                        '/blobs/'||s.artifact_sha256 AS artifact_url
                        FROM segments s JOIN media_files f ON f.node_id=s.node_id
                        JOIN node_versions n ON n.id=s.node_id
                        WHERE s.id=? AND n.state='active' AND n.version=(
                            SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)''',
                        (row['target'],)).fetchone()
                    floor = {'image': 0.67, 'audio': 0.645, 'text': TEXT_COSINE_FLOOR}.get(
                        record['modality'] if record else '', 1.0)
                elif row['kind'] == 'candidate':
                    record = self._candidate_record(row['target'])
                    floor = TEXT_COSINE_FLOOR
                elif row['kind'] == 'episode':
                    passage = self.store._db.execute('SELECT * FROM episode_chunks WHERE id=?', (row['target'],)).fetchone()
                    if passage is None or passage['episode_id'] in seen_raw:
                        continue
                    seen_raw.add(passage['episode_id'])
                    record = self._episode_record(passage['episode_id'], passage['start'], passage['end'], passage['id'])
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
            feedback = {}
            if matches:
                marks = ','.join('?' for _ in matches)
                feedback = {row['target']: max(-1, min(1, (2 * row['cited'] - row['shown']) / max(1, row['shown'])))
                    for row in self.store._db.execute(
                    f'SELECT target,shown,cited FROM query_feedback WHERE query_key=? AND target IN ({marks})',
                    (query_key(query), *matches))}
            for item in matches.values():
                if isinstance(item.get('scope'), str):
                    item['scope'] = json.loads(item['scope'])
                item.setdefault('importance', .5)
                item.setdefault('decay', 1.0)
                item.setdefault('archived', 0)
                item['score'] *= item['importance'] * item['decay'] * (1 + .1 * feedback.get(item['id'], 0))
        results = sorted(matches.values(), key=lambda item: (-item['score'], item['id']))[:limit]
        for item in results:
            with self.store._mutex:
                if item.get('tier') == 'candidate':
                    item.update(self._candidate_details(item['id']))
                elif item.get('tier') != 'raw':
                    item['tier'], item['confirmed'] = 'durable', True
                    item.update(self.graph._recall_details(item['id'], item['version']))
                if item['type'] == 'media' and 'blob_url' not in item:
                    file = self.store._db.execute('SELECT sha256 FROM media_files WHERE node_id=?',
                                                  (item['id'],)).fetchone()
                    if file:
                        item['blob_url'] = '/blobs/' + file['sha256']
            item['related'] = self.related(item['id'], 2 if effort == 'deep' else 1,
                scope=scope, include_archived=effort == 'deep') if effort != 'cheap' and item['tier'] == 'durable' else []
        return results

    def _candidate_details(self, target):
        db = self.store._db
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
                    loop=current(self.graph, target))

    def start(self):
        if self._worker is not None:
            raise RuntimeError('Indexer already started')
        def run():
            while not self._stop.is_set():
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

    def health(self):
        if self.index_error is not None:
            raise RuntimeError('Native indexing failed; work remains pending')
        with self.store._mutex:
            db = self.store._db
            failures = db.execute('SELECT target,version FROM index_failures WHERE resolved=0').fetchall()
            for failure in failures:
                if db.execute(f'''WITH indexable AS ({_INDEXABLE})
                    SELECT 1 FROM indexable WHERE id=? AND version=? LIMIT 1''',
                    (failure['target'], failure['version'])).fetchone():
                    raise RuntimeError('Some native embeddings failed; valid work continues and failures remain retryable')
