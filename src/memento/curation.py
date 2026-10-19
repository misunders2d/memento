"""Protected graph operations: proposals are public to processing, decisions are private admin only."""

import re

from .graph import ENTITY_TYPES
from .store import ConflictError


def propose(graph, commit, request):
    op = request['op']
    if op == 'register_type':
        validate_type(request)
        if graph._known_type(request['name']):
            raise ConflictError('Type already registered')
        bases = {}
    elif op == 'pin':
        if not isinstance(request['pinned'], bool):
            raise ValueError('Pin state must be boolean')
        node = graph._node(request['node_id'])
        if node['state'] != 'active':
            raise ValueError('Only active nodes can be pinned')
        bases = {node['id']: node['version']}
    elif op == 'merge':
        a, b = graph._node(request['a']), graph._node(request['b'])
        category = graph.store._db.execute('SELECT category FROM memory_types WHERE name=?', (a['type'],)).fetchone()
        if (a['id'] == b['id'] or a['type'] != b['type'] or a['state'] != 'active' or b['state'] != 'active'
                or not (a['type'] in ENTITY_TYPES or category and category['category'] == 'entity')):
            raise ValueError('Merge requires distinct active entities of the same type')
        bases = {a['id']: a['version'], b['id']: b['version']}
    else:
        raise ValueError('Unknown protected operation')
    return graph._propose(commit, request, bases)


def validate_type(request):
    if (not isinstance(request['name'], str) or re.fullmatch(r'[a-z][a-z0-9_]{0,63}', request['name']) is None
            or request['category'] not in ('memory', 'entity') or not isinstance(request['sensitive'], bool)):
        raise ValueError('Invalid reviewed type definition')


def pin(graph, commit, node_id, pinned):
    node = graph._node(node_id)
    if node['state'] != 'active':
        raise ValueError('Pin requires an active node')
    if bool(node['pinned']) == pinned:
        return dict(id=node_id, version=node['version'], pinned=pinned)
    version = node['version'] + 1
    db = graph.store._db
    db.execute('INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)',
        (node_id, version, commit, node['type'], node['text'], node['state'],
         node['valid_from'], node['valid_to'], node['superseded_by'], int(pinned), node['scope']))
    db.execute('''INSERT INTO graph_evidence SELECT target,?,episode_id,start,end,quote
        FROM graph_evidence WHERE target=? AND version=?''', (version, node_id, node['version']))
    _copy_vector(graph, node_id, node['version'], version)
    return dict(id=node_id, version=version, pinned=pinned)


def _copy_vector(graph, node_id, old_version, new_version):
    db = graph.store._db
    db.execute('''UPDATE memory_vectors SET current=0 WHERE rowid IN
        (SELECT seq FROM vector_items WHERE kind='node' AND target=?)''', (node_id,))
    previous = db.execute('''SELECT i.seq FROM vector_items i JOIN memory_vectors v ON v.rowid=i.seq
        WHERE i.target=? AND i.kind='node' AND i.version=?''', (node_id, old_version)).fetchone()
    if previous is not None:
        seq = db.execute('INSERT INTO vector_items(target,kind,version) VALUES (?,?,?)',
                         (node_id, 'node', new_version)).lastrowid
        db.execute('''INSERT INTO memory_vectors(rowid,embedding,current)
            SELECT ?,embedding,1 FROM memory_vectors WHERE rowid=?''', (seq, previous['seq']))


def attach(graph, commit, target, evidence, *, approved=False, expected_version=None):
    """Corroborate an existing assertion without creating or merging an identity."""
    db = graph.store._db
    row = db.execute('SELECT * FROM node_versions WHERE id=? ORDER BY version DESC LIMIT 1', (target,)).fetchone()
    node = dict(row) if row else None
    if node is None:
        row = db.execute('SELECT * FROM edge_versions WHERE id=? ORDER BY version DESC LIMIT 1', (target,)).fetchone()
    if row is None or row['state'] != 'active':
        raise ConflictError('Corroboration requires an active assertion')
    if expected_version is not None and row['version'] != expected_version:
        raise ConflictError('Corroboration proposal is stale')
    bases = {target: row['version']} if node else {
        endpoint: graph._node(endpoint)['version'] for endpoint in (row['src'], row['dst'])}
    endpoints = [node] if node else [graph._node(endpoint) for endpoint in bases]
    if any(endpoint['state'] != 'active' for endpoint in endpoints):
        raise ConflictError('Corroboration endpoints are no longer active')
    if not approved and any(endpoint['pinned'] or graph._sensitive_type(endpoint['type']) for endpoint in endpoints):
        return graph._propose(commit, dict(op='attach', target=target, evidence=evidence,
                                          expected_version=row['version']), bases)
    version = row['version'] + 1
    if node:
        db.execute('INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)',
            (target, version, commit, node['type'], node['text'], node['state'], node['valid_from'],
             node['valid_to'], node['superseded_by'], node['pinned'], node['scope']))
        _copy_vector(graph, target, node['version'], version)
    else:
        db.execute('INSERT INTO edge_versions VALUES (?,?,?,?,?,?,?)',
            (target, version, commit, row['src'], row['rel'], row['dst'], row['state']))
    db.execute('''INSERT INTO graph_evidence SELECT target,?,episode_id,start,end,quote
        FROM graph_evidence WHERE target=? AND version=?''', (version, target, row['version']))
    graph._evidence(target, version, evidence)
    return dict(id=target, version=version)


def apply_review(graph, commit, request):
    db = graph.store._db
    if request['op'] == 'register_type':
        validate_type(request)
        if graph._known_type(request['name']):
            raise ConflictError('Type was registered after this proposal')
        db.execute('INSERT INTO memory_types VALUES (?,?,?,?)',
            (request['name'], request['category'], int(request['sensitive']), commit))
        return dict(type=request['name'], status='registered')
    if request['op'] == 'pin':
        return pin(graph, commit, request['node_id'], request['pinned'])
    if request['op'] == 'attach':
        return attach(graph, commit, request['target'], request['evidence'], approved=True,
                      expected_version=request['expected_version'])
    if request['op'] != 'merge':
        raise ValueError('Unknown protected operation')
    a, b = graph._node(request['a']), graph._node(request['b'])
    if a['state'] != 'active' or b['state'] != 'active' or a['id'] == b['id'] or a['type'] != b['type']:
        raise ConflictError('Entity merge is no longer applicable')
    version = a['version'] + 1
    db.execute('INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)',
        (a['id'], version, commit, a['type'], a['text'], 'merged', a['valid_from'], a['valid_to'],
         b['id'], a['pinned'], a['scope']))
    db.execute('''INSERT INTO graph_evidence SELECT target,?,episode_id,start,end,quote
        FROM graph_evidence WHERE target=? AND version=?''', (version, a['id'], a['version']))
    db.execute('INSERT INTO entity_merges VALUES (?,?,?)', (a['id'], b['id'], commit))
    db.execute('INSERT INTO alias_fts(id,text) VALUES (?,?)', (a['id'], a['text']))
    edges = db.execute('''SELECT e.* FROM edge_versions e WHERE (e.src=? OR e.dst=?) AND e.state='active'
        AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id)''', (a['id'], a['id'])).fetchall()
    for edge in edges:
        src = b['id'] if edge['src'] == a['id'] else edge['src']
        dst = b['id'] if edge['dst'] == a['id'] else edge['dst']
        db.execute('INSERT INTO edge_versions VALUES (?,?,?,?,?,?,?)',
            (edge['id'], edge['version'] + 1, commit, src, edge['rel'], dst, 'inactive' if src == dst else 'active'))
        db.execute('''INSERT INTO graph_evidence SELECT target,?,episode_id,start,end,quote
            FROM graph_evidence WHERE target=? AND version=?''', (edge['version'] + 1, edge['id'], edge['version']))
    db.execute('''UPDATE memory_vectors SET current=0 WHERE rowid IN
        (SELECT seq FROM vector_items WHERE kind='node' AND target=?)''', (a['id'],))
    if a['pinned'] and not b['pinned']:
        pin(graph, commit, b['id'], True)
    from .importance import transfer_usage
    transfer_usage(graph.store, a['id'], b['id'])
    from .loops import transfer
    transfer(graph, commit, a['id'], b['id'])
    return dict(id=a['id'], version=version, merged_into=b['id'])


def canonical(graph, node_id):
    for _ in range(64):
        node = graph._node(node_id)
        if node['state'] != 'merged':
            return node
        node_id = node['superseded_by']
    raise ValueError('Entity merge chain is too deep')
