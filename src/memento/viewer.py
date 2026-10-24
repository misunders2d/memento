"""Read-only memory viewer: a static page plus bounded queries over short read snapshots."""

import gzip
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import FileResponse, RedirectResponse, Response

from .extraction import ACTIVE_CANDIDATE_SQL
from .graph import commit_behaviors
from .raw_text import RawTextUnavailable

UI = Path(__file__).with_name('ui')
STATIC = frozenset(('app.js', 'app.css', 'layout-worker.js', 'vendor/graphology.umd.min.js',
                    'vendor/sigma.min.js', 'vendor/forceatlas2.min.js'))
TOP_WEIGHT, TOP_RECENT, MAX_NODES = 200, 100, 50_000
LOOPBACK = frozenset(('localhost', '127.0.0.1', '::1'))
HEADERS = {
    # Memory text is attacker-influenced and this origin also serves unauthenticated writes.
    'Content-Security-Policy': "default-src 'self'; img-src 'self' data:; object-src 'none'; "
                               "base-uri 'none'; frame-ancestors 'none'; form-action 'none'",
    'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer'}
CURRENT_NODE = '''n.state='active' AND n.version=(SELECT max(v.version) FROM node_versions v WHERE v.id=n.id)
    AND NOT EXISTS (SELECT 1 FROM forgotten_targets f WHERE f.target=n.id)'''
OPEN_LOOP = "l.rowid=(SELECT max(x.rowid) FROM loop_events x WHERE x.target=l.target) AND l.state='open'"
Scope = Annotated[Literal['personal', 'professional'] | None, Query()]


def _hostname(host):
    if host.startswith('['):
        return host[1:host.find(']')]
    return host.rsplit(':', 1)[0] if host.count(':') == 1 else host


def _label(text):
    return ' '.join(text.split())[:80]


def _json(request, value):
    body = json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode()
    headers = dict(HEADERS, **{'Cache-Control': 'no-store'})
    if len(body) > 1024 and 'gzip' in request.headers.get('accept-encoding', ''):
        body = gzip.compress(body, 5)
        headers.update({'Content-Encoding': 'gzip', 'Vary': 'Accept-Encoding'})
    return Response(body, media_type='application/json', headers=headers)


def _loop(row, now):
    return dict(due_at=row['due_at'], deferred=bool(row['deferred_until'] and row['deferred_until'] > now),
                overdue=bool(row['due_at'] and row['due_at'] < now))


def _behaviors(db, ids=None):
    """Parse each consolidation commit once, keyed by each experience's creating commit."""
    where, args = ('AND n.id=?', ids) if ids else ('', ())
    owners = defaultdict(list)
    for row in db.execute(f'''SELECT n.id,n.commit_id FROM node_versions n
            WHERE n.version=1 AND n.type='experience' {where}''', args):
        owners[row['commit_id']].append(row['id'])
    behaviors = {}
    for row in db.execute(f'''SELECT id,request,result FROM graph_commits WHERE id IN (SELECT n.commit_id
            FROM node_versions n WHERE n.version=1 AND n.type='experience' {where})''', args):
        found = commit_behaviors(row['request'], row['result'])
        behaviors.update({node: found[node] for node in owners[row['id']] if node in found})
    return behaviors


def _graph(db, scope, now):
    nodes = {}
    for row in db.execute(f'''SELECT n.id,n.type,substr(n.text,1,240) AS text,n.scope,n.pinned,c.recorded_at,
            coalesce(s.importance,0.5)*coalesce(s.decay,1) AS weight FROM node_versions n
            JOIN graph_commits c ON c.id=n.commit_id LEFT JOIN node_stats s ON s.node_id=n.id
            WHERE {CURRENT_NODE}'''):
        nodes[row['id']] = dict(id=row['id'], type=row['type'], label=_label(row['text']),
            scope=json.loads(row['scope']), weight=1.0 if row['pinned'] else round(row['weight'], 4),
            at=row['recorded_at'], behavior=None, loop=None)
    edges = {row['id']: dict(id=row['id'], src=row['src'], rel=row['rel'], dst=row['dst'], loop=None)
             for row in db.execute('''SELECT e.id,e.src,e.rel,e.dst FROM edge_versions e WHERE e.state='active'
                AND e.version=(SELECT max(v.version) FROM edge_versions v WHERE v.id=e.id)''')
             if row['src'] in nodes and row['dst'] in nodes}
    for node, behavior in _behaviors(db).items():
        if node in nodes:
            nodes[node]['behavior'] = behavior['authority']
    for row in db.execute(f'SELECT l.target,l.due_at,l.deferred_until FROM loop_events l WHERE {OPEN_LOOP}'):
        holder = nodes.get(row['target']) or edges.get(row['target'])
        if holder is not None:
            holder['loop'] = _loop(row, now)
    candidates = [dict(id=row['id'], label=_label(row['text']), scope=json.loads(row['scope']),
                       loop=_loop(row, now)) for row in db.execute(f'''SELECT c.id,substr(c.text,1,240) AS text,
        c.scope,l.due_at,l.deferred_until FROM loop_events l JOIN candidates c ON c.id=l.target
        WHERE {OPEN_LOOP} AND {ACTIVE_CANDIDATE_SQL} ORDER BY l.due_at IS NULL,l.due_at,c.rowid DESC''')]
    within = lambda item: scope is None or scope in item['scope']
    edge_within = lambda edge: within(nodes[edge['src']]) or within(nodes[edge['dst']])
    candidates = [item for item in candidates if within(item)]
    looped = [item['loop'] for item in nodes.values() if item['loop'] and within(item)]
    looped += [edge['loop'] for edge in edges.values() if edge['loop'] and edge_within(edge)]
    looped += [item['loop'] for item in candidates]
    loops = dict(open=sum(not loop['deferred'] for loop in looped), deferred=sum(loop['deferred'] for loop in looped),
                 candidates=dict(total=len(candidates), items=candidates[:100]))
    return {key: value for key, value in nodes.items() if within(value)}, edges, loops


def _neighbors(nodes, edges, focus):
    linked = set(focus)
    for edge in edges.values():
        if edge['src'] in focus and edge['dst'] in nodes:
            linked.add(edge['dst'])
        if edge['dst'] in focus and edge['src'] in nodes:
            linked.add(edge['src'])
    return linked


def install(app, allowed_hosts):
    hosts = frozenset(host.lower() for host in allowed_hosts)

    def same_host(request: Request):
        # Only the browser viewer gets a Host check: it serves the whole graph in one response.
        host = request.headers.get('host', '').lower()
        if host not in hosts and _hostname(host) not in LOOPBACK | hosts:
            raise HTTPException(403, 'Viewer host not allowed')

    router = APIRouter(prefix='/ui', include_in_schema=False, dependencies=[Depends(same_host)])

    @router.get('')
    def root():
        return RedirectResponse('/ui/', status_code=307)

    @router.get('/')
    def page():
        return FileResponse(UI / 'index.html', headers=dict(HEADERS, **{'Cache-Control': 'no-cache'}))

    @router.get('/static/{name:path}')
    def static(name: str):
        if name not in STATIC:
            raise HTTPException(404, 'Not found')
        return FileResponse(UI / name, headers=dict(HEADERS, **{'Cache-Control': 'no-cache'}))

    @router.get('/api/graph')
    def graph(request: Request, view: Literal['memories', 'behaviors', 'loops', 'around'] = 'memories',
              scope: Scope = None, all: bool = False, id: str | None = None):
        now = datetime.now(timezone.utc).isoformat()
        with app.state.store.read_snapshot() as db:
            nodes, edges, loops = _graph(db, scope, now)
        focus = set()
        if view == 'behaviors':
            focus = {key for key, value in nodes.items() if value['behavior']}
        elif view == 'loops':
            focus = {key for key, value in nodes.items() if value['loop']}
            for edge in edges.values():
                if edge['loop']:
                    focus.update(key for key in (edge['src'], edge['dst']) if key in nodes)
        elif view == 'around':
            if id not in nodes:
                raise HTTPException(404, 'Memory not found in this scope')
            focus = {id}
        if all:
            chosen = set(nodes)
        elif view == 'memories':
            ranked = sorted(nodes.values(), key=lambda item: (-item['weight'], item['id']))
            recent = sorted(nodes.values(), key=lambda item: item['at'], reverse=True)
            chosen = {item['id'] for item in ranked[:TOP_WEIGHT] + recent[:TOP_RECENT]}
        else:
            chosen = _neighbors(nodes, edges, focus)
        result = dict(view=view, total_nodes=len(nodes), total_edges=sum(
            edge['src'] in nodes and edge['dst'] in nodes for edge in edges.values()), loops=loops)
        if len(chosen) > MAX_NODES:
            return _json(request, dict(result, too_large=True, nodes=[], edges=[]))
        return _json(request, dict(result, too_large=False,
            nodes=[dict(nodes[key], focus=key in focus) for key in sorted(chosen)],
            edges=[edge for edge in edges.values() if edge['src'] in chosen and edge['dst'] in chosen]))

    @router.get('/api/node/{node_id}')
    def node(request: Request, node_id: str):
        with app.state.store.read_snapshot() as db:
            row = db.execute(f'SELECT n.id,n.type,n.text,n.scope FROM node_versions n WHERE n.id=? AND {CURRENT_NODE}',
                             (node_id,)).fetchone()
            if row is None:
                raise HTTPException(404, 'Memory not found')
            loop = db.execute(f'SELECT l.due_at,l.deferred_until FROM loop_events l WHERE l.target=? AND {OPEN_LOOP}',
                              (node_id,)).fetchone()
            links = [dict(row) for row in db.execute(f'''SELECT e.rel,e.src,e.dst,n.id,n.type,
                substr(n.text,1,240) AS text FROM edge_versions e JOIN node_versions n
                ON n.id=CASE WHEN e.src=:id THEN e.dst ELSE e.src END
                WHERE (e.src=:id OR e.dst=:id) AND e.state='active'
                AND e.version=(SELECT max(v.version) FROM edge_versions v WHERE v.id=e.id) AND {CURRENT_NODE}
                LIMIT 200''', dict(id=node_id))]
            behavior = _behaviors(db, (node_id,)).get(node_id) if row['type'] == 'experience' else None
        now = datetime.now(timezone.utc).isoformat()
        return _json(request, dict(id=row['id'], type=row['type'], text=row['text'], scope=json.loads(row['scope']),
            behavior=behavior, loop=_loop(loop, now) if loop else None,
            links=[dict(rel=link['rel'], outgoing=link['src'] == node_id, id=link['id'], type=link['type'],
                        label=_label(link['text'])) for link in links]))

    @router.get('/api/search')
    def search(request: Request, q: Annotated[str, Query(min_length=1, max_length=512)], scope: Scope = None,
               limit: Annotated[int, Query(ge=1, le=200)] = 100):
        # All input is literal text, never FTS syntax, as in episode search.
        match = ' '.join('"' + term.replace('"', '""') + '"' for term in q.split())
        if not match:
            return _json(request, dict(results=[]))
        with app.state.store.read_snapshot() as db:
            rows = db.execute(f'''SELECT n.id,n.type,substr(n.text,1,240) AS text,n.scope FROM node_fts
                JOIN node_versions n ON n.id=node_fts.id AND n.version=node_fts.version
                WHERE node_fts MATCH ? AND {CURRENT_NODE}
                AND (? IS NULL OR EXISTS (SELECT 1 FROM json_each(n.scope) WHERE value=?))
                ORDER BY bm25(node_fts),n.id LIMIT ?''', (match, scope, scope, limit)).fetchall()
        return _json(request, dict(results=[dict(id=row['id'], type=row['type'], label=_label(row['text']))
                                            for row in rows]))

    @router.get('/api/conversations')
    def conversations(request: Request, offset: Annotated[int, Query(ge=0)] = 0,
                      limit: Annotated[int, Query(ge=1, le=100)] = 50):
        with app.state.store.read_snapshot() as db:
            # Unextracted history stays visible. A parked conversation is labelled: its
            # parked range is not queued work, and live_cursor shows where new history resumes.
            total = db.execute('SELECT count(*) FROM processing_progress WHERE latest_seq>seq').fetchone()[0]
            rows = [dict(row) for row in db.execute('''SELECT p.source,p.conversation_id,p.seq AS cursor,p.latest_at,
                h.parked_through,h.live_seq AS live_cursor,(h.source IS NOT NULL) AS parked
                FROM processing_progress p LEFT JOIN historical_parking h
                ON h.source=p.source AND h.conversation_id=p.conversation_id
                WHERE p.latest_seq>p.seq ORDER BY p.latest_at DESC,p.source,p.conversation_id
                LIMIT ? OFFSET ?''', (limit, offset))]
            for row in rows:
                row['parked'] = bool(row['parked'])
        return _json(request, dict(total=total, items=rows))

    @router.get('/api/conversation')
    def conversation(request: Request, source: Annotated[str, Query(min_length=1, max_length=256)],
                     conversation_id: Annotated[str, Query(min_length=1, max_length=256)],
                     after: Annotated[int, Query(ge=0)] = 0, limit: Annotated[int, Query(ge=1, le=100)] = 50):
        with app.state.store.read_snapshot() as db:
            rows = [dict(row) for row in db.execute('''SELECT e.seq,e.id,e.source,e.conversation_id,e.kind,
                e.occurred_at,e.captured_at,e.text,e.text_runs,e.text_sha256,e.raw_frame FROM episodes e
                WHERE e.source=? AND e.conversation_id=? AND e.seq>?
                AND NOT EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id)
                ORDER BY e.seq LIMIT ?''', (source, conversation_id, after, limit))]
        items = []
        for row in rows:
            try:
                text = app.state.store.original_text(row)
            except RawTextUnavailable:
                text = None
            items.append(dict(seq=row['seq'], id=row['id'], kind=row['kind'],
                at=row['occurred_at'] or row['captured_at'], text=None if text is None else text[:4000],
                truncated=text is not None and len(text) > 4000))
        return _json(request, dict(items=items))

    @router.get('/api/candidates')
    def candidates(request: Request, q: Annotated[str | None, Query(max_length=512)] = None,
                   offset: Annotated[int, Query(ge=0)] = 0, limit: Annotated[int, Query(ge=1, le=100)] = 50):
        match = ' '.join('"' + term.replace('"', '""') + '"' for term in (q or '').split()) or None
        where = f'''{ACTIVE_CANDIDATE_SQL} AND (? IS NULL OR c.id IN
            (SELECT id FROM candidate_fts WHERE candidate_fts MATCH ?))'''
        with app.state.store.read_snapshot() as db:
            total = db.execute(f'SELECT count(*) FROM candidates c WHERE {where}', (match, match)).fetchone()[0]
            rows = db.execute(f'''SELECT c.id,c.kind,c.type,substr(c.text,1,400) AS text,c.confidence,c.scope,
                g.recorded_at,(SELECT d.decision FROM candidate_decisions d WHERE d.candidate_id=c.id
                    ORDER BY d.rowid DESC LIMIT 1) AS decision,
                (SELECT l.state FROM loop_events l WHERE l.target=c.id ORDER BY l.rowid DESC LIMIT 1) AS loop
                FROM candidates c JOIN extraction_runs r ON r.id=c.run_id JOIN graph_commits g ON g.id=r.commit_id
                WHERE {where} ORDER BY c.rowid DESC LIMIT ? OFFSET ?''', (match, match, limit, offset)).fetchall()
        return _json(request, dict(total=total, items=[dict(row, scope=json.loads(row['scope']),
                                                            loop=row['loop'] == 'open') for row in rows]))

    app.include_router(router)
