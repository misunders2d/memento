"""Protected graph operations: proposals are public to processing, decisions are private admin only."""

import hashlib
import json
import re
from uuid import uuid4

from .extraction import MEMORY_TYPES
from .graph import ENTITY_TYPES, identity_text
from .store import ConflictError

MAX_MERGE_PLAN = 32
MAX_ABSORB = 8


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
    """Carry the unchanged text's derived vector to the new version without duplicating it.

    Callers copy a node version whose text is identical, so the existing
    vector_items row moves to the new version and its stored embedding bytes stay
    exactly as indexed. Changed text never comes here (supersede invalidates and
    the new identity is indexed afresh). Without an indexed vector the new version
    simply stays pending for ordinary indexing.
    """
    db = graph.store._db
    db.execute('''UPDATE memory_vectors SET current=0 WHERE rowid IN
        (SELECT seq FROM vector_items WHERE kind='node' AND target=?)''', (node_id,))
    previous = db.execute('''SELECT i.seq FROM vector_items i JOIN memory_vectors v ON v.rowid=i.seq
        WHERE i.target=? AND i.kind='node' AND i.version=?''', (node_id, old_version)).fetchone()
    if previous is not None:
        db.execute('UPDATE vector_items SET version=? WHERE seq=?', (new_version, previous['seq']))
        db.execute('UPDATE memory_vectors SET current=1 WHERE rowid=?', (previous['seq'],))


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


def _union_evidence(db, target, version, sources):
    """Copy current evidence rows into a new version; excluded sources never move."""
    for source, source_version in sources:
        db.execute('''INSERT INTO graph_evidence SELECT ?,?,g.episode_id,g.start,g.end,g.quote
            FROM graph_evidence g WHERE g.target=? AND g.version=?
            AND NOT EXISTS (SELECT 1 FROM graph_evidence x WHERE x.target=? AND x.version=?
                AND x.episode_id=g.episode_id AND x.start=g.start AND x.end=g.end)
            AND (g.target=? OR NOT EXISTS (SELECT 1 FROM episodes e JOIN forgotten_sources f
                ON f.source=e.source AND f.event_id=e.event_id WHERE e.id=g.episode_id))''',
            (target, version, source, source_version, target, version, target))


def _requeue_stale_reviews(graph, commit, targets, exclude=None):
    """Return candidates of proposals a merge made stale to processing, never reject them."""
    db = graph.store._db
    placeholders = ','.join('?' for _ in targets)
    proposals = db.execute(f'''SELECT DISTINCT p.id,p.base_versions FROM review_proposals p, json_each(p.base_versions) j
        WHERE j.key IN ({placeholders}) AND p.id IS NOT ?
        AND NOT EXISTS (SELECT 1 FROM review_decisions d WHERE d.proposal_id=p.id) ORDER BY p.id''',
        (*targets, exclude)).fetchall()
    requeued = []
    for proposal in proposals:
        bases = json.loads(proposal['base_versions'])
        if all(graph._node(node)['version'] == version for node, version in bases.items()):
            continue
        for row in db.execute('''SELECT d.candidate_id FROM candidate_decisions d
                WHERE d.decision='review' AND d.target_id=? AND d.rowid=(
                    SELECT max(x.rowid) FROM candidate_decisions x WHERE x.candidate_id=d.candidate_id)
                ORDER BY d.candidate_id''', (proposal['id'],)).fetchall():
            db.execute('INSERT INTO candidate_decisions VALUES (?,?,?,?,?,?)',
                       (uuid4().hex, row['candidate_id'], commit, 'defer', None, None))
            requeued.append(row['candidate_id'])
    return requeued


def apply_review(graph, commit, request, *, proposal_id=None):
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
    if request['op'] == 'absorb':
        return absorb(graph, commit, request['survivor'], request['absorbed'], evidence=request['evidence'],
                      approved=True, proposal_id=proposal_id)
    if request['op'] != 'merge':
        raise ValueError('Unknown protected operation')
    return _absorb_one(graph, commit, request['a'], request['b'], entity=True, proposal_id=proposal_id)


def redirect_edges(graph, commit, old, new, *, edge_ids=None):
    """Move old's current relations onto new; lineage edges stay with their history.

    A relation that would join new to itself becomes inactive, and one that
    already exists on new folds its evidence and loop marker into that relation
    when their markers and manual-choice chronology agree; a differing marker or
    a manual choice the fold would lose keeps its own relation.
    """
    if old == new:
        return []
    from .loops import snapshot, transfer
    db = graph.store._db
    selected = '' if edge_ids is None else ' AND e.id IN (SELECT value FROM json_each(?))'
    arguments = (old, old) if edge_ids is None else (old, old, json.dumps(list(edge_ids)))
    edges = db.execute('''SELECT e.* FROM edge_versions e WHERE (e.src=? OR e.dst=?) AND e.state='active'
        AND e.rel<>'supersedes' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id)
        ''' + selected + ' ORDER BY e.id', arguments).fetchall()
    moved = []
    for edge in edges:
        src = new if edge['src'] == old else edge['src']
        dst = new if edge['dst'] == old else edge['dst']
        # Fold only into a relation whose loop marker cannot hide this one's; a
        # differing obligation stays on its own relation rather than disappearing.
        # A transferred marker is not a manual choice, so this relation's manual
        # chronology must already be the survivor's.
        def marker(row):
            choice = _loop_choice(graph, row['id'])
            return None if choice is None else (choice, snapshot(graph, row['id'], row['version'])['manual_choice_at'])
        mine = marker(edge)
        same = None if src == dst else next((row for row in db.execute('''SELECT e.* FROM edge_versions e
            WHERE e.src=? AND e.rel=? AND e.dst=? AND e.id<>? AND e.state='active'
            AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id) ORDER BY e.id''',
            (src, edge['rel'], dst, edge['id'])).fetchall()
            if mine is None or mine == marker(row) or marker(row) is None and mine[1] is None), None)
        state = 'inactive' if src == dst or same is not None else 'active'
        db.execute('INSERT INTO edge_versions VALUES (?,?,?,?,?,?,?)',
            (edge['id'], edge['version'] + 1, commit, src, edge['rel'], dst, state))
        db.execute('''INSERT INTO graph_evidence SELECT target,?,episode_id,start,end,quote
            FROM graph_evidence WHERE target=? AND version=?''', (edge['version'] + 1, edge['id'], edge['version']))
        if same is not None:
            # A redirected duplicate relation folds into the existing one.
            db.execute('INSERT INTO edge_versions VALUES (?,?,?,?,?,?,?)',
                (same['id'], same['version'] + 1, commit, same['src'], same['rel'], same['dst'], 'active'))
            _union_evidence(db, same['id'], same['version'] + 1,
                            [(same['id'], same['version']), (edge['id'], edge['version'])])
            transfer(graph, commit, edge['id'], same['id'])
        moved.append(dict(id=edge['id'], version=edge['version'] + 1, src=src, dst=dst, state=state,
                          folded_into=same['id'] if same is not None else None))
    return moved


def _absorb_one(graph, commit, source, target, *, entity, proposal_id=None):
    """Absorb one active identity into a survivor of the same type; history stays append-only."""
    db = graph.store._db
    a, b = graph._node(source), graph._node(target)
    if a['state'] != 'active' or b['state'] != 'active' or a['id'] == b['id'] or a['type'] != b['type']:
        raise ConflictError('Entity merge is no longer applicable')
    version = a['version'] + 1
    db.execute('INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)',
        (a['id'], version, commit, a['type'], a['text'], 'merged', a['valid_from'], a['valid_to'],
         b['id'], a['pinned'], a['scope']))
    db.execute('''INSERT INTO graph_evidence SELECT target,?,episode_id,start,end,quote
        FROM graph_evidence WHERE target=? AND version=?''', (version, a['id'], a['version']))
    db.execute('INSERT INTO entity_merges VALUES (?,?,?)', (a['id'], b['id'], commit))
    if entity:
        # Only an entity name is an alternate spelling; an absorbed claim's text is not.
        db.execute('INSERT INTO alias_fts(id,text) VALUES (?,?)', (a['id'], a['text']))
    # The survivor's new version carries both identities' evidence and scopes,
    # so ordinary recall, history and recency see the absorbed sources; the
    # version change also fences plans and proposals based on the old survivor.
    survivor = b['version'] + 1
    scope = sorted(set(json.loads(a['scope'])) | set(json.loads(b['scope'])))
    db.execute('INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)',
        (b['id'], survivor, commit, b['type'], b['text'], b['state'], b['valid_from'], b['valid_to'],
         b['superseded_by'], b['pinned'], json.dumps(scope)))
    _union_evidence(db, b['id'], survivor, [(b['id'], b['version']), (a['id'], a['version'])])
    _copy_vector(graph, b['id'], b['version'], survivor)
    redirect_edges(graph, commit, a['id'], b['id'])
    db.execute('''UPDATE memory_vectors SET current=0 WHERE rowid IN
        (SELECT seq FROM vector_items WHERE kind='node' AND target=?)''', (a['id'],))
    if a['pinned'] and not b['pinned']:
        pin(graph, commit, b['id'], True)
    from .importance import transfer_usage
    from .loops import transfer
    transfer_usage(graph.store, a['id'], b['id'])
    transfer(graph, commit, a['id'], b['id'])
    requeued = _requeue_stale_reviews(graph, commit, (a['id'], b['id']), exclude=proposal_id)
    return dict(id=a['id'], version=version, merged_into=b['id'], survivor_version=graph._node(b['id'])['version'],
                scope=scope, requeued_candidates=requeued)


def _entity_type(graph, type):
    if type in ENTITY_TYPES:
        return True
    row = graph.store._db.execute('SELECT category FROM memory_types WHERE name=?', (type,)).fetchone()
    return bool(row and row['category'] == 'entity')


def _absorb_members(graph, survivor, absorbed):
    """Current survivor and absorbed versions, or a fixed validation code."""
    if (not isinstance(survivor, str) or not isinstance(absorbed, (list, tuple))
            or not 1 <= len(absorbed) <= MAX_ABSORB or any(not isinstance(item, str) for item in absorbed)
            or len(set(absorbed)) != len(absorbed) or survivor in absorbed):
        return None, 'consolidation_absorb_member_invalid'
    try:
        nodes = [graph._node(target) for target in (survivor, *absorbed)]
    except ValueError:
        return None, 'consolidation_absorb_member_invalid'
    if any(node['state'] != 'active' for node in nodes):
        return None, 'consolidation_absorb_member_invalid'
    if any(node['type'] != nodes[0]['type'] for node in nodes):
        return None, 'consolidation_absorb_type_mismatch'
    return nodes, None


def _loop_choice(graph, target):
    from .loops import current
    marker = current(graph, target)
    if marker is None:
        return None
    return ('closed',) if marker['state'] == 'closed' else (marker['state'], marker['due_at'], marker['deferred_until'])


def absorb_check(graph, survivor, absorbed):
    """Fixed code when absorbed cannot coalesce into survivor; None when absorb may proceed.

    Pinned, sensitive and differently scoped entities are protected rather than
    invalid: they return None and absorb proposes a human review. A claim keeps
    its single scope, world validity and behavior authority, so mismatches there
    are not duplicates. Differing loop markers on members or on relations that
    would fold together would be silently lost by transfer, so they refuse.
    """
    nodes, code = _absorb_members(graph, survivor, absorbed)
    if code is not None:
        return code
    if not _entity_type(graph, nodes[0]['type']):
        if any(set(json.loads(node['scope'])) != set(json.loads(nodes[0]['scope'])) for node in nodes):
            return 'consolidation_absorb_scope_mismatch'
        if any((node['valid_from'], node['valid_to']) != (nodes[0]['valid_from'], nodes[0]['valid_to'])
               for node in nodes):
            return 'consolidation_absorb_validity_mismatch'
        # The survivor keeps its own creation behavior; an absorbed one must be
        # absent or identical, so no suggestion or explicit_user authority is dropped.
        behavior = graph._behavior(survivor)
        if any(graph._behavior(target) not in (None, behavior) for target in absorbed):
            return 'consolidation_absorb_behavior_mismatch'
    if (len({choice for choice in (_loop_choice(graph, node['id']) for node in nodes) if choice is not None}) > 1
            or _relation_loop_conflict(graph, survivor, absorbed)):
        return 'consolidation_absorb_loop_conflict'
    return None


def _relation_loop_conflict(graph, survivor, absorbed):
    """Would absorbing fold relations with differing loop markers, or hide an open one in a self relation?"""
    members = set(absorbed)
    ids = [survivor, *absorbed]
    placeholders = ','.join('?' for _ in ids)
    groups = {}
    for row in graph.store._db.execute(f'''SELECT e.id,e.src,e.rel,e.dst FROM edge_versions e
            WHERE (e.src IN ({placeholders}) OR e.dst IN ({placeholders})) AND e.state='active'
            AND e.rel<>'supersedes' AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id)''',
            (*ids, *ids)):
        src = survivor if row['src'] in members else row['src']
        dst = survivor if row['dst'] in members else row['dst']
        choice = _loop_choice(graph, row['id'])
        if src == dst and members & {row['src'], row['dst']}:
            if choice is not None and choice[0] == 'open':
                return True
            continue
        groups.setdefault((src, row['rel'], dst), set()).add(choice)
    return any(len(choices - {None}) > 1 for choices in groups.values())


def absorb(graph, commit, survivor, absorbed, *, evidence=None, approved=False, proposal_id=None):
    """Coalesce same-referent entities or equivalent claims into survivor (the target ID).

    Absorbed identities become merged versions pointing at survivor; the survivor's
    new version carries their evidence, relations, usage and loop markers. Pinned,
    sensitive or differently scoped entities become an absorb review proposal
    fenced by every member version; only the private review transport approves it.
    """
    code = absorb_check(graph, survivor, absorbed)
    if code is not None:
        raise ConflictError(code)
    nodes, _ = _absorb_members(graph, survivor, absorbed)
    absorbed = list(absorbed)
    protected = (any(node['pinned'] or graph._sensitive_type(node['type']) for node in nodes)
                 or any(set(json.loads(node['scope'])) != set(json.loads(nodes[0]['scope'])) for node in nodes))
    if protected and not approved:
        if not evidence:
            raise ValueError('A protected absorb requires evidence for human review')
        return graph._propose(commit, dict(op='absorb', survivor=survivor, absorbed=absorbed, evidence=list(evidence)),
                              {node['id']: node['version'] for node in nodes})
    entity = _entity_type(graph, nodes[0]['type'])
    merged = [_absorb_one(graph, commit, target, survivor, entity=entity, proposal_id=proposal_id)
              for target in absorbed]
    if evidence:
        graph._evidence(survivor, merged[-1]['survivor_version'], evidence)
    return dict(id=survivor, version=merged[-1]['survivor_version'], absorbed=absorbed,
                scope=merged[-1]['scope'], status='absorbed',
                requeued_candidates=sorted({c for item in merged for c in item['requeued_candidates']}))


def _memory_type(graph, type):
    if type in MEMORY_TYPES:
        return True
    row = graph.store._db.execute('SELECT category FROM memory_types WHERE name=?', (type,)).fetchone()
    return bool(row and row['category'] == 'memory')


def _incident_edges(graph, node_id):
    """Current active relations touching node_id; supersedes lineage is history, not a relation."""
    return graph.store._db.execute('''SELECT e.* FROM edge_versions e WHERE (e.src=? OR e.dst=?)
        AND e.state='active' AND e.rel<>'supersedes'
        AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id) ORDER BY e.id''',
        (node_id, node_id)).fetchall()


def retire_check(graph, target, evidence):
    """Fixed code when target cannot be retired; None when retire may proceed or is protected.

    Only a current active memory-category record is retirable, never an entity,
    alias or media file, and only on its own current original evidence: a
    retirement adds no authority beyond the sources already behind the record.
    """
    try:
        node = graph._node(target) if isinstance(target, str) else None
    except ValueError:
        node = None
    if node is None or node['state'] != 'active':
        return 'consolidation_retire_target_invalid'
    if not _memory_type(graph, node['type']):
        return 'consolidation_retire_type_invalid'
    rows = graph.store._db.execute('SELECT episode_id,quote FROM graph_evidence WHERE target=? AND version=?',
                                   (target, node['version'])).fetchall()
    if (not isinstance(evidence, (list, tuple)) or not evidence
            or any(not isinstance(item, dict) or not isinstance(item.get('quote'), str) or not item['quote']
                   or not any(row['episode_id'] == item.get('episode_id') and item['quote'] in row['quote']
                              for row in rows) for item in evidence)):
        return 'consolidation_retire_evidence_invalid'
    return None


def _retire_protection(graph, node):
    """Status naming why a retirable record stays untouched, or None."""
    from .loops import current, snapshot
    if node['pinned']:
        return 'protected_pin'
    if graph._sensitive_type(node['type']):
        return 'protected_sensitive'
    behavior = graph._behavior(node['id'])
    if behavior and behavior['authority'] == 'explicit_user':
        return 'protected_explicit_user'
    for id, version in [(node['id'], node['version'])] + [
            (edge['id'], edge['version']) for edge in _incident_edges(graph, node['id'])]:
        marker = current(graph, id)
        if (snapshot(graph, id, version)['manual_choice_at'] is not None
                or marker is not None and (marker['due_at'] is not None or marker['deferred_until'] is not None)):
            return 'protected_loop_choice'
    return None


def retire(graph, commit, target, evidence):
    """Take proven noise or one-off scaffolding out of active memory; nothing is deleted.

    The record gains a retired version with its unchanged assertion and evidence,
    its vectors leave recall, and its active relations gain inactive versions.
    Raw history, sources and search/history rows stay; no source is suppressed,
    so the same evidence can still support other memory. Untouched automatic
    loop markers close as never-an-obligation, without a completion date. Pins,
    sensitive types, explicit user authority and any manual or dated loop choice
    on the record or its relations protect it: those return a protected status
    and change nothing.
    """
    from .loops import _append, current
    code = retire_check(graph, target, evidence)
    if code is not None:
        raise ConflictError(code)
    db = graph.store._db
    node = graph._node(target)
    status = _retire_protection(graph, node)
    if status is not None:
        return dict(id=target, version=node['version'], status=status)
    edges = _incident_edges(graph, target)
    version = node['version'] + 1
    db.execute('INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)',
        (target, version, commit, node['type'], node['text'], 'retired', node['valid_from'], node['valid_to'],
         None, node['pinned'], node['scope']))
    db.execute('''INSERT INTO graph_evidence SELECT target,?,episode_id,start,end,quote
        FROM graph_evidence WHERE target=? AND version=?''', (version, target, node['version']))
    db.execute('''UPDATE memory_vectors SET current=0 WHERE rowid IN (
        SELECT seq FROM vector_items WHERE (kind='node' AND target=?)
        OR (kind='segment' AND target IN (SELECT id FROM segments WHERE node_id=?)))''', (target, target))
    relations, declined = [], []
    for edge in edges:
        db.execute('INSERT INTO edge_versions VALUES (?,?,?,?,?,?,?)',
            (edge['id'], edge['version'] + 1, commit, edge['src'], edge['rel'], edge['dst'], 'inactive'))
        db.execute('''INSERT INTO graph_evidence SELECT target,?,episode_id,start,end,quote
            FROM graph_evidence WHERE target=? AND version=?''', (edge['version'] + 1, edge['id'], edge['version']))
        relations.append(dict(id=edge['id'], version=edge['version'] + 1))
    for id in [target, *(edge['id'] for edge in edges)]:
        marker = current(graph, id)
        if marker is not None and marker['state'] == 'open':
            _append(graph, commit, id, 'closed', None, None)
            declined.append(id)
    return dict(id=target, version=version, status='retired', relations=relations, declined_loops=declined)


def canonical(graph, node_id):
    """Follow identity-continuing merges and refinements, never corrections or world changes."""
    for _ in range(64):
        node = graph._node(node_id)
        if node['state'] not in ('merged', 'refined') or not node['superseded_by']:
            return node
        node_id = node['superseded_by']
    raise ValueError('Entity merge chain is too deep')


def _active_entities(db, condition, parameters):
    rows = db.execute(f'''SELECT n.id,n.version,n.type,n.text,n.scope,n.pinned,
        (SELECT count(*) FROM graph_evidence g WHERE g.target=n.id AND g.version=n.version) AS evidence,
        (SELECT count(*) FROM edge_versions e WHERE (e.src=n.id OR e.dst=n.id) AND e.state='active'
            AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id)) AS relations,
        (SELECT c.recorded_at FROM node_versions v JOIN graph_commits c ON c.id=v.commit_id
            WHERE v.id=n.id AND v.version=1) AS created_at
        FROM node_versions n LEFT JOIN memory_types t ON t.name=n.type
        WHERE n.state='active' AND n.version=(SELECT max(x.version) FROM node_versions x WHERE x.id=n.id)
        AND {condition} ORDER BY n.id''', parameters).fetchall()
    return [dict(row, scope=json.loads(row['scope']), pinned=bool(row['pinned'])) for row in rows]


def _survivor_order(item):
    # Default survivor first: pinned, best evidenced, then oldest.
    return (not item['pinned'], -item['evidence'], item['created_at'] or '', item['id'])


def inventory(graph):
    """Read-only suspicion: active same-type entities spelled alike up to case/whitespace.

    Members of a cluster may legitimately be distinct; only an owner-selected
    exact plan merges anything.
    """
    types = ','.join('?' for _ in ENTITY_TYPES)
    groups = {}
    for row in _active_entities(graph.store._db, f"(n.type IN ({types}) OR t.category='entity')", sorted(ENTITY_TYPES)):
        groups.setdefault((row['type'], identity_text(row['text'])), []).append(row)
    clusters = []
    for (type, name), members in sorted(groups.items()):
        if len(members) > 1:
            members.sort(key=_survivor_order)
            cluster = hashlib.sha256(json.dumps([type, name], ensure_ascii=False).encode()).hexdigest()[:12]
            clusters.append(dict(cluster=cluster, type=type, name=name, members=members))
    return dict(clusters=clusters, policy='suspicion_only')


def merge_plan(graph, cluster, *, into=None, exclude=(), include=()):
    """Exact version-bound plan; the token fences every original base before mutation.

    include adds owner-chosen active spelling variants of the cluster's type;
    the inventory never groups them automatically.
    """
    from .loops import snapshot
    found = next((item for item in inventory(graph)['clusters'] if item['cluster'] == cluster), None)
    if found is None:
        raise ValueError('Unknown or already resolved duplicate cluster')
    known = {member['id'] for member in found['members']}
    include = sorted(set(include))
    if set(include) & set(exclude):
        raise ValueError('An identity cannot be both included and excluded')
    if not set(exclude) <= known:
        raise ValueError('Exclusions must be members of this cluster')
    variants = [target for target in include if target not in known]
    for target in variants:
        try:
            node = graph._node(target)
        except ValueError:
            raise ValueError('Included identity is unknown') from None
        if node['state'] != 'active' or node['type'] != found['type']:
            raise ValueError('Included identities must be active entities of the cluster type')
    extra = _active_entities(graph.store._db, f"n.id IN ({','.join('?' for _ in variants)})", variants) if variants else []
    members = sorted([member for member in found['members'] if member['id'] not in exclude] + extra,
                     key=_survivor_order)
    if into is not None and into not in {member['id'] for member in members}:
        raise ValueError('Survivor must belong to the selected members')
    if len(members) < 2:
        raise ValueError('A merge plan needs a survivor and at least one absorbed member')
    survivor = next(member for member in members if member['id'] == into) if into else members[0]
    absorbed = [member for member in members if member['id'] != survivor['id']]
    if len(absorbed) > MAX_MERGE_PLAN:
        raise ValueError(f'A merge plan absorbs at most {MAX_MERGE_PLAN} members')
    db = graph.store._db

    def summary(member):
        # Bounded original sources let the owner tell same-named identities apart;
        # loop snapshots fence transferred obligations and manual loop choices.
        sources = [dict(row) for row in db.execute('''SELECT g.episode_id,e.source,e.conversation_id,e.occurred_at,
            CASE WHEN EXISTS (SELECT 1 FROM forgotten_sources f WHERE f.source=e.source AND f.event_id=e.event_id)
                THEN NULL ELSE substr(g.quote,1,160) END AS quote_excerpt
            FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id WHERE g.target=? AND g.version=?
            ORDER BY julianday(e.occurred_at) DESC,g.episode_id,g.start LIMIT 3''', (member['id'], member['version']))]
        return dict({key: member[key] for key in ('id', 'version', 'text', 'scope', 'pinned', 'evidence', 'relations')},
                    sources=sources, loop=snapshot(graph, member['id'], member['version']))
    ids = [member['id'] for member in members]
    placeholders = ','.join('?' for _ in ids)
    relations = {row['id']: dict(version=row['version'], loop=snapshot(graph, row['id'], row['version']))
        for row in db.execute(f'''SELECT e.id,e.version FROM edge_versions e
        WHERE (e.src IN ({placeholders}) OR e.dst IN ({placeholders})) AND e.state='active'
        AND e.version=(SELECT max(x.version) FROM edge_versions x WHERE x.id=e.id) ORDER BY e.id''', (*ids, *ids))}
    reviews = [row['id'] for row in db.execute(f'''SELECT DISTINCT p.id FROM review_proposals p, json_each(p.base_versions) j
        WHERE j.key IN ({placeholders}) AND NOT EXISTS (SELECT 1 FROM review_decisions d WHERE d.proposal_id=p.id)
        ORDER BY p.id''', ids)]
    scope = sorted({label for member in members for label in member['scope']})
    plan = dict(cluster=cluster, type=found['type'], name=found['name'], survivor=summary(survivor),
        absorbed=[summary(member) for member in absorbed],
        excluded=[summary(member) for member in found['members'] if member['id'] in exclude],
        resulting_scope=scope, relations=relations, pending_reviews=reviews,
        policy='owner_selected_exact_plan', raw_history='retained')
    if variants:
        # Their versions, evidence and loops are already in survivor/absorbed.
        plan['included'] = variants
    plan['token'] = hashlib.sha256(json.dumps(plan, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    return plan


def apply_merge_plan(graph, commit, cluster, into, exclude, token, include=()):
    plan = merge_plan(graph, cluster, into=into, exclude=exclude, include=include)
    if plan['token'] != token:
        raise ConflictError('Merge plan is stale; preview and confirm the current plan')
    # All original bases matched above; each pair then uses the reviewed merge.
    merged = [apply_review(graph, commit, dict(op='merge', a=member['id'], b=plan['survivor']['id']))
              for member in plan['absorbed']]
    return dict(cluster=cluster, survivor=plan['survivor']['id'], survivor_version=merged[-1]['survivor_version'],
                merged=[item['id'] for item in merged], scope=merged[-1]['scope'],
                requeued_candidates=sorted({c for item in merged for c in item['requeued_candidates']}))


def _continuation(graph, node_id):
    """Exact identity chain through refinements and merges, or None at any other step."""
    db = graph.store._db
    chain = [node_id]
    for _ in range(64):
        node = graph._node(chain[-1])
        if node['state'] == 'active':
            return chain
        following = node['superseded_by']
        if node['state'] == 'refined':
            # The atomic supersede wrote exactly this lineage edge.
            proven = following and db.execute('''SELECT 1 FROM edge_versions WHERE src=? AND rel='supersedes'
                AND dst=? AND version=1''', (following, node['id'])).fetchone()
        elif node['state'] == 'merged':
            proven = following and db.execute('SELECT 1 FROM entity_merges WHERE source=? AND target=?',
                                              (node['id'], following)).fetchone()
        else:
            return None
        if not proven or following in chain:
            return None
        chain.append(following)
    return None
