"""Human-gated active-memory exclusion. Raw history and backup bytes are retained."""

import hashlib
import json

from .store import ConflictError


def source_blocked(store, source, event_id):
    with store._mutex:
        return store._db.execute('SELECT 1 FROM forgotten_sources WHERE source=? AND event_id=? LIMIT 1',
                                  (source, event_id)).fetchone() is not None


def preview(graph, target):
    db = graph.store._db
    existing = db.execute('SELECT 1 FROM forgotten_targets WHERE target=?', (target,)).fetchone()
    node = db.execute('SELECT * FROM node_versions WHERE id=? ORDER BY version DESC LIMIT 1', (target,)).fetchone()
    edge = db.execute('SELECT * FROM edge_versions WHERE id=? ORDER BY version DESC LIMIT 1', (target,)).fetchone()
    candidate = db.execute('SELECT * FROM candidates WHERE id=?', (target,)).fetchone()
    if node is not None:
        if node['state'] != 'active' and existing is None:
            raise ValueError('Choose the active memory identifier; this node is no longer active')
        kind, label = 'node', node['text'][:240]
        family = db.execute('''WITH RECURSIVE family(id) AS (
            SELECT ? UNION SELECT n.id FROM node_versions n JOIN family f ON n.superseded_by=f.id
            WHERE n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id))
            SELECT n.id,n.version FROM node_versions n JOIN family f ON f.id=n.id
            WHERE n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id) ORDER BY n.id''', (target,)).fetchall()
        nodes = {row['id']: row['version'] for row in family}
        placeholders = ','.join('?' for _ in nodes)
        edges = {row['id']: row['version'] for row in db.execute(f'''SELECT e.id,e.version FROM edge_versions e
            WHERE (e.src IN ({placeholders}) OR e.dst IN ({placeholders})) AND e.state='active'
            AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id) ORDER BY e.id''',
            (*nodes, *nodes))}
        targets = [*nodes, *edges]
    elif edge is not None:
        if edge['state'] != 'active' and existing is None:
            raise ValueError('Relation is no longer active')
        kind, label, nodes, edges = 'edge', edge['rel'], {}, {target: edge['version']}
        targets = [target]
    elif candidate is not None:
        decision = db.execute('SELECT decision,target_id FROM candidate_decisions WHERE candidate_id=? ORDER BY rowid DESC LIMIT 1',
                              (target,)).fetchone()
        if decision is not None and decision['decision'] == 'promote':
            raise ValueError('Candidate was promoted; choose its active target: ' + str(decision['target_id']))
        kind, label, nodes, edges, targets = 'candidate', candidate['text'][:240], {}, {}, [target]
    else:
        raise ValueError('Unknown active-memory identifier')
    placeholders = ','.join('?' for _ in targets)
    sources = [dict(row) for row in db.execute(f'''SELECT DISTINCT e.source,e.event_id FROM graph_evidence g
        JOIN episodes e ON e.id=g.episode_id WHERE g.target IN ({placeholders}) ORDER BY e.source,e.event_id''', targets)]
    value = dict(target=target, kind=kind, label=label, nodes=nodes, edges=edges, targets=targets, sources=sources,
                 already_forgotten=existing is not None, policy='active_memory_only', raw_history='retained', backups='retained')
    value['token'] = hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    return value


def _exclude(graph, commit, record):
    db = graph.store._db
    kinds = {target: 'node' for target in record['nodes']}
    kinds.update({target: 'edge' for target in record['edges']})
    kinds[record['target']] = record['kind']
    for target in record['targets']:
        db.execute('''INSERT INTO forgotten_targets SELECT ?,?,? WHERE NOT EXISTS (
            SELECT 1 FROM forgotten_targets WHERE target=?)''', (target, commit, kinds[target], target))
    for source in record['sources']:
        db.execute('''INSERT INTO forgotten_sources SELECT ?,?,?,? WHERE NOT EXISTS (
            SELECT 1 FROM forgotten_sources WHERE source=? AND event_id=? AND target=?)''',
            (source['source'], source['event_id'], record['target'], commit,
             source['source'], source['event_id'], record['target']))
    for target in record['nodes']:
        node = db.execute('SELECT * FROM node_versions WHERE id=? ORDER BY version DESC LIMIT 1', (target,)).fetchone()
        if node is not None and node['state'] == 'active':
            version = node['version'] + 1
            db.execute('INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                (target, version, commit, node['type'], node['text'], 'forgotten', node['valid_from'], node['valid_to'],
                 node['superseded_by'], node['pinned'], node['scope']))
            db.execute('''INSERT INTO graph_evidence SELECT target,?,episode_id,start,end,quote
                FROM graph_evidence WHERE target=? AND version=?''', (version, target, node['version']))
        db.execute('''UPDATE vector_items SET current=0
            WHERE (kind='node' AND target=?) OR (kind='segment' AND target IN (SELECT id FROM segments WHERE node_id=?))''',
            (target, target))
    for target in record['edges']:
        edge = db.execute('SELECT * FROM edge_versions WHERE id=? ORDER BY version DESC LIMIT 1', (target,)).fetchone()
        if edge is not None and edge['state'] == 'active':
            version = edge['version'] + 1
            db.execute('INSERT INTO edge_versions VALUES (?,?,?,?,?,?,?)',
                (target, version, commit, edge['src'], edge['rel'], edge['dst'], 'forgotten'))
            db.execute('''INSERT INTO graph_evidence SELECT target,?,episode_id,start,end,quote
                FROM graph_evidence WHERE target=? AND version=?''', (version, target, edge['version']))


def apply(graph, commit, target, token):
    record = preview(graph, target)
    if record['token'] != token:
        raise ConflictError('Forget preview is stale; inspect and confirm the current scope')
    if record['already_forgotten']:
        return dict(id=target, status='already_forgotten', policy='active_memory_only')
    _exclude(graph, commit, record)
    return dict(id=target, status='forgotten', policy='active_memory_only', raw_history='retained', backups='retained')


