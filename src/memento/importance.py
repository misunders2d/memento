"""Ranking-only decay and honest committed-citation feedback; never evidence deletion."""

import hashlib
import math
import re
from datetime import datetime, timezone

from .extraction import ACTIVE_CANDIDATE_SQL

HALF_LIFE_DAYS = 90
_EVIDENCE_TIME = object()


def query_recency(store, record, *, authority=None, now=None, source_occurred_at=_EVIDENCE_TIME):
    """Rank bounded recall results from original evidence, without hydrating text."""
    db = store._db
    if source_occurred_at is not _EVIDENCE_TIME:
        row = db.execute('''SELECT ? AS occurred_at,
            (SELECT last_cited_at FROM node_stats WHERE node_id=?) AS last_cited_at''',
            (source_occurred_at, record['id'])).fetchone()
    elif record.get('tier') == 'raw':
        row = db.execute('''SELECT occurred_at,
            (SELECT last_cited_at FROM node_stats WHERE node_id=episodes.id) AS last_cited_at
            FROM episodes WHERE id=?''', (record['id'],)).fetchone()
    else:
        row = db.execute('''SELECT (SELECT e.occurred_at FROM graph_evidence g
            JOIN episodes e ON e.id=g.episode_id WHERE g.target=? AND g.version=?
            AND e.occurred_at IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id)
            ORDER BY julianday(e.occurred_at) DESC,e.id LIMIT 1) AS occurred_at,
            (SELECT last_cited_at FROM node_stats WHERE node_id=?) AS last_cited_at''',
            (record['id'], record['version'], record['id'])).fetchone()

    def instant(value):
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(value)
            return parsed.astimezone(timezone.utc) if parsed.tzinfo is not None else None
        except (TypeError, ValueError):
            return None

    source = instant(row['occurred_at']) if row else None
    cited = instant(row['last_cited_at']) if row else None
    anchor = max(value for value in (source, cited) if value is not None) if source or cited else None
    age = max(0, ((now or datetime.now(timezone.utc)) - anchor).total_seconds() / 86400) if anchor else 0
    decay = 1.0 if record.get('pinned') or authority == 'explicit_user' else math.exp2(-age / HALF_LIFE_DAYS)
    return dict(source_occurred_at=source.isoformat() if source else None,
                ranking_anchor=anchor.isoformat() if anchor else None, decay=decay)


def query_key(query):
    shape = ' '.join(sorted(set(re.findall(r'\w+', query.casefold()))))
    return hashlib.sha256(shape.encode()).hexdigest()


def refresh(store, now=None):
    """Caller holds the writer transaction. Normalize links within each node type."""
    now = now or datetime.now(timezone.utc)
    db = store._db
    rows = db.execute('''WITH current_nodes AS (
        SELECT n.* FROM node_versions n WHERE n.state='active'
        AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)),
        current_edges AS (SELECT e.* FROM edge_versions e WHERE e.state='active'
        AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id)),
        neighbors AS (SELECT src AS id,dst AS neighbor FROM current_edges UNION SELECT dst,src FROM current_edges),
        degrees AS (SELECT n.id,n.type,count(DISTINCT l.neighbor) AS degree FROM current_nodes n
        LEFT JOIN neighbors l ON l.id=n.id GROUP BY n.id),
        norms AS (SELECT type,avg(degree) AS mean_degree FROM degrees GROUP BY type)
        SELECT n.id,n.pinned,s.access_count,s.last_cited_at,d.degree,t.mean_degree,
        (SELECT c.recorded_at FROM node_versions v JOIN graph_commits c ON c.id=v.commit_id
            WHERE v.id=n.id ORDER BY v.version LIMIT 1) AS created_at,
        (SELECT count(DISTINCT e.episode_id) FROM graph_evidence e WHERE e.target=n.id AND e.version=n.version) AS sources
        FROM current_nodes n JOIN node_stats s ON s.node_id=n.id JOIN degrees d ON d.id=n.id
        JOIN norms t ON t.type=n.type''').fetchall()
    values = []
    for row in rows:
        base = datetime.fromisoformat(row['created_at']) if row['created_at'] else now
        if row['last_cited_at']:
            base = max(base, datetime.fromisoformat(row['last_cited_at']))
        age_days = max(0, (now - base).total_seconds() / 86400)
        links = row['degree'] / (row['degree'] + row['mean_degree'] + 1)
        citations = row['access_count'] / (row['access_count'] + 3)
        sources = row['sources'] / (row['sources'] + 3)
        importance = 1.0 if row['pinned'] else min(.95, .30 + .25 * links + .30 * citations + .15 * sources)
        decay = 1.0 if row['pinned'] else math.exp2(-age_days / HALF_LIFE_DAYS)
        # Age affects query ranking, never ordinary-search eligibility.
        values.append((importance, decay, 0, row['id']))
    db.executemany('UPDATE node_stats SET importance=?,decay=?,archived=? WHERE node_id=?', values)
    return len(values)


def transfer_usage(store, source, target):
    """Carry usage to the surviving assertion inside its idempotent graph transaction."""
    if source == target:
        return
    db = store._db
    db.execute('''INSERT INTO node_stats(node_id,importance,decay,archived,access_count,last_cited_at)
        SELECT ?,importance,decay,archived,access_count,last_cited_at FROM node_stats WHERE node_id=?
        ON CONFLICT(node_id) DO UPDATE SET
          importance=max(node_stats.importance,excluded.importance),
          decay=max(node_stats.decay,excluded.decay),
          archived=min(node_stats.archived,excluded.archived),
          access_count=node_stats.access_count+excluded.access_count,
          last_cited_at=CASE WHEN node_stats.last_cited_at IS NULL
            OR julianday(excluded.last_cited_at)>julianday(node_stats.last_cited_at)
            THEN excluded.last_cited_at ELSE node_stats.last_cited_at END''', (target, source))
    db.execute('''INSERT INTO query_feedback(query_key,target,shown,cited)
        SELECT query_key,?,shown,cited FROM query_feedback WHERE target=?
        ON CONFLICT(query_key,target) DO UPDATE SET
          shown=query_feedback.shown+excluded.shown,cited=query_feedback.cited+excluded.cited''',
        (target, source))


def cite(graph, *, event_id, ids, query=None, shown=()):
    ids, shown = sorted(set(ids)), sorted(set(shown))
    request = dict(op='cite', ids=ids, query=query, shown=shown)

    def apply(commit):
        db = graph.store._db
        accepted = []
        now = datetime.now(timezone.utc).isoformat()
        for target in ids:
            node = db.execute('''SELECT state FROM node_versions WHERE id=? ORDER BY version DESC LIMIT 1''', (target,)).fetchone()
            candidate = db.execute(f'SELECT 1 FROM candidates c WHERE c.id=? AND {ACTIVE_CANDIDATE_SQL}', (target,)).fetchone()
            episode = db.execute('''SELECT 1 FROM episodes e WHERE e.id=? AND NOT EXISTS (
                SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id)''', (target,)).fetchone()
            if not ((node and node['state'] == 'active') or candidate or episode):
                continue
            db.execute('''INSERT INTO node_stats(node_id,access_count,last_cited_at) VALUES (?,1,?)
                ON CONFLICT(node_id) DO UPDATE SET access_count=access_count+1,last_cited_at=excluded.last_cited_at,
                archived=0,decay=1,importance=min(1,importance+0.05)''', (target, now))
            accepted.append(target)
        if query:
            key = query_key(query)
            for target in sorted(set(shown) | set(accepted)):
                db.execute('''INSERT INTO query_feedback(query_key,target,shown,cited) VALUES (?,?,?,?)
                    ON CONFLICT(query_key,target) DO UPDATE SET shown=shown+excluded.shown,cited=cited+excluded.cited''',
                    (key, target, int(target in shown), int(target in accepted)))
        return dict(cited=accepted, unknown_or_inactive=sorted(set(ids) - set(accepted)))

    return graph._command('cite/' + event_id, 'harness/citation', 'Memory citations in committed assistant output', request, apply)
