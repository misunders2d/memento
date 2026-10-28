"""Reactive loop markers, independent of assertion versions and world-time facts."""

import json
import math
from datetime import datetime, timedelta, timezone
from uuid import uuid4

RESPONSIBILITIES = frozenset(('owner', 'other', 'unknown'))


def responsibilities(db, targets):
    """Batch only loop-event commits and incoming identity/promotion branches."""
    result = {}
    for row in db.execute('''WITH RECURSIVE ancestry(root,id) AS (
        SELECT value,value FROM json_each(?) UNION
        SELECT a.root,n.id FROM ancestry a JOIN node_versions n ON n.superseded_by=a.id
          WHERE EXISTS(SELECT 1 FROM loop_events l WHERE l.target=n.id) UNION
        SELECT a.root,d.candidate_id FROM ancestry a JOIN candidate_decisions d ON d.target_id=a.id
          AND d.decision='promote' WHERE EXISTS(SELECT 1 FROM loop_events l WHERE l.target=d.candidate_id))
        SELECT a.root,r.value AS attribution FROM ancestry a CROSS JOIN loop_events l INDEXED BY loop_target ON l.target=a.id
        JOIN graph_commits c ON c.id=l.commit_id
        JOIN json_each(c.result,'$.loop_responsibilities') r ON r.key=l.target
        WHERE NOT EXISTS(SELECT 1 FROM json_each(r.value,'$.evidence') ref
          JOIN episodes e ON e.id=json_extract(ref.value,'$.episode_id')
          JOIN forgotten_sources f ON f.source=e.source AND f.event_id=e.event_id)
        ORDER BY json_extract(r.value,'$.manual') DESC,c.recorded_at DESC,l.rowid DESC''',
        (json.dumps(list(targets)),)):
        value = json.loads(row['attribution'])
        if value.get('responsibility') in RESPONSIBILITIES:
            result.setdefault(row['root'], value)
    return result


def responsibility(db, target):
    return responsibilities(db, [target]).get(target, {}).get('responsibility', 'unknown')


def grounded_sources(graph, target, sources, *, original=False, allow_instruction=False):
    """Validate exact eligible source spans without altering assertion evidence."""
    from .extraction import source_span
    from .graph import behavior_ground_truth
    from .store import legacy_native_context
    from .store import ConflictError
    if not sources:
        raise ValueError('Loop responsibility requires exact source evidence')
    references, source_times, grounded_owner = [], [], False
    db = graph.store._db
    for source in sources:
        quote = source.get('quote')
        if not isinstance(quote, str) or not quote:
            raise ValueError('Evidence quote must be a nonempty string')
        episode = db.execute('SELECT * FROM episodes WHERE id=?', (source.get('episode_id'),)).fetchone()
        if episode is None:
            raise ValueError('Unknown source episode')
        if original and db.execute('SELECT 1 FROM graph_evidence WHERE target=? AND episode_id=? LIMIT 1',
                                    (target, source['episode_id'])).fetchone() is None:
            raise ValueError('Loop responsibility requires an original target source')
        if db.execute('SELECT 1 FROM forgotten_sources WHERE source=? AND event_id=?',
                      (episode['source'], episode['event_id'])).fetchone():
            raise ConflictError('This evidence was excluded from active memory by a human decision')
        metadata = json.loads(episode['metadata'])
        text = graph.store.original_text(episode)
        instruction = episode['kind'] == 'developer' or legacy_native_context(episode['source'], episode['kind'], text, metadata)
        if (episode['text_runs'] is not None
                or episode['kind'] in ('native_record', 'native_hook', 'native_live_record')
                or (instruction and not allow_instruction)
                or ((metadata.get('evidence_eligible') is False or metadata.get('extraction_mode') == 'skip')
                    and not (instruction and allow_instruction))):
            raise ValueError('Loop responsibility requires eligible original evidence')
        if 'start' in source and 'end' in source and text[source['start']:source['end']] == quote:
            start, end = source['start'], source['end']
        else:
            start, end = source_span(text, quote, source.get('occurrence'))
        if original and db.execute('''SELECT 1 FROM graph_evidence WHERE target=? AND episode_id=?
                AND start<=? AND end>=? LIMIT 1''', (target, source['episode_id'], start, end)).fetchone() is None:
            raise ValueError('Loop responsibility requires an original target source')
        full = dict(episode, text=text, metadata=metadata)
        grounded_owner |= episode['source'].startswith('native:') and behavior_ground_truth(full, start, end)
        references.append(dict(episode_id=source['episode_id'], start=start, end=end))
        source_times.append(episode['occurred_at'])
    return references, source_times, grounded_owner


def assign_automatic(graph, commit, target, value, sources):
    """Keep optional roles unassigned when fact evidence cannot authorize them."""
    if value not in RESPONSIBILITIES:
        raise ValueError('Invalid loop responsibility')
    if not sources:
        raise ValueError('Loop responsibility requires exact source evidence')
    eligible = []
    for source in sources:
        try:
            _, _, grounded_owner = grounded_sources(graph, target, [source], original=True)
        except ValueError as error:
            if str(error) not in ('Loop responsibility requires eligible original evidence',
                                  'Loop responsibility requires an original target source'):
                raise
        else:
            if value != 'owner' or grounded_owner:
                eligible.append(source)
    if not eligible:
        return dict(id=target, status='unattributed_loop_responsibility')
    return assign(graph, commit, target, value, eligible)


def assign(graph, commit, target, value, sources, *, manual=False):
    """Ground role metadata without changing lifecycle, deadlines or deferrals."""
    if value not in RESPONSIBILITIES:
        raise ValueError('Invalid loop responsibility')
    db = graph.store._db
    # New assertions precede their graph_commits row within the same write.
    # Role validation needs state/protection, not committed viewer details.
    node = db.execute('''SELECT n.* FROM node_versions n WHERE n.id=? AND n.state='active'
        AND n.version=(SELECT max(v.version) FROM node_versions v WHERE v.id=n.id)''', (target,)).fetchone()
    node = dict(node) if node is not None else _target(graph, target)
    if node is None or db.execute('SELECT 1 FROM forgotten_targets WHERE target=?', (target,)).fetchone():
        raise ValueError('Responsibility requires an active visible target')
    previous = current(graph, target)
    if previous is None:
        raise ValueError('Responsibility requires an existing loop')
    references, _, grounded_owner = grounded_sources(graph, target, sources, original=not manual)
    if value == 'owner' and not grounded_owner:
        raise ValueError('Owner responsibility requires original native user evidence')
    attribution = responsibilities(db, [target]).get(target, {})
    if not manual and (attribution.get('manual') or protected(graph, node)):
        return dict(id=target, status='protected_loop_responsibility')
    result = _append(graph, commit, target, previous['state'], previous['due_at'], previous['deferred_until'])
    return dict(result, responsibility=value, loop_responsibilities={target:
        dict(responsibility=value, manual=manual, evidence=references)})


def current(graph, id, db=None):
    sql = '''SELECT target AS id,state,due_at,deferred_until
        FROM loop_events WHERE target=? ORDER BY rowid DESC LIMIT 1'''
    if db is not None:
        row = db.execute(sql, (id,)).fetchone()
        return dict(row, responsibility=responsibility(db, id)) if row is not None else None
    with graph.store._mutex:
        row = graph.store._db.execute(sql, (id,)).fetchone()
        return dict(row, responsibility=responsibility(graph.store._db, id)) if row is not None else None


def events(graph, id):
    with graph.store._mutex:
        return [dict(row, assignment=json.loads(row['assignment']) if row['assignment'] else None,
                     evidence=json.loads(row['evidence']) if row['evidence'] else None)
            for row in graph.store._db.execute('''SELECT l.state,l.due_at,
            l.deferred_until,l.commit_id,c.actor,c.why,c.recorded_at,r.value AS assignment,
            json_extract(c.result,'$.evidence') AS evidence FROM loop_events l
            JOIN graph_commits c ON c.id=l.commit_id
            LEFT JOIN json_each(c.result,'$.loop_responsibilities') r ON r.key=l.target
            WHERE l.target=? ORDER BY l.rowid''', (id,))]


def _transferred_from(graph, target, commit):
    return [row['id'] for row in graph.store._db.execute('''SELECT id,0 AS priority FROM node_versions
        WHERE superseded_by=? AND commit_id=?
        UNION SELECT candidate_id,1 FROM candidate_decisions
        WHERE target_id=? AND commit_id=? AND decision='promote'
        ORDER BY priority,id''', (target, commit, target, commit))]


def _closed_at(graph, id):
    """Latest effective closure boundary behind copied/coalesced marker state."""
    db = graph.store._db
    boundaries = []
    for target in _loop_lineage(graph, id):
        for event in db.execute('''SELECT l.commit_id,l.state,c.request,c.result,c.recorded_at
            FROM loop_events l LEFT JOIN graph_commits c ON c.id=l.commit_id
            WHERE l.target=? ORDER BY l.rowid DESC''', (target,)):
            if event['state'] != 'closed':
                break  # A later open supersedes an older closure in this branch.
            request = json.loads(event['request'])['command'] if event['request'] else {}
            if request.get('op') == 'loop' and request.get('action') == 'close':
                result = json.loads(event['result']) if event['result'] else {}
                if request.get('evidence'):
                    if result.get('source_occurred_at'):
                        boundaries.append(result['source_occurred_at'])
                else:
                    boundaries.append(event['recorded_at'])
                break
            if request.get('op') == 'consolidate' and event['result']:
                closure = next((item for item in json.loads(event['result']).get('loop_closures', [])
                    if item.get('target') == target and item.get('status') == 'closed'), None)
                if closure:
                    boundaries.append(closure['source_occurred_at'])
                    break
    return max(boundaries, key=datetime.fromisoformat) if boundaries else None


def _loop_lineage(graph, id):
    """Actual incoming identity/promotion history, including an occupied survivor."""
    db = graph.store._db
    pending, seen = [id], set()
    while pending:
        target = pending.pop()
        if target in seen:
            continue
        seen.add(target)
        yield target
        pending.extend(row['id'] for row in db.execute('''SELECT DISTINCT n.id FROM node_versions n
            WHERE n.superseded_by=? AND EXISTS (SELECT 1 FROM loop_events l WHERE l.target=n.id)
            UNION SELECT d.candidate_id FROM candidate_decisions d WHERE d.target_id=? AND d.decision='promote'
            AND EXISTS (SELECT 1 FROM loop_events l WHERE l.target=d.candidate_id)''', (target, target)))


def _append(graph, commit, id, state, due_at, deferred_until):
    graph.store._db.execute('INSERT INTO loop_events VALUES (?,?,?,?,?,?)',
        (uuid4().hex, id, commit, state, due_at, deferred_until))
    return dict(id=id, state=state, due_at=due_at, deferred_until=deferred_until)


def mark(graph, commit, id, open_loop, *, evidence_target=None, evidence_loop=None):
    """Only newly dated evidence may reopen a closed, previously chosen marker."""
    if open_loop is not True:
        return
    previous = current(graph, id)
    if previous is None:
        _append(graph, commit, id, 'open', None, None)
    elif previous['state'] == 'closed' and evidence_target is not None:
        if evidence_loop is not None and evidence_loop['state'] == 'closed':
            return
        closed_at = _closed_at(graph, id)
        if closed_at is None:
            return
        boundary = datetime.fromisoformat(closed_at)
        sources = graph.store._db.execute('''SELECT e.occurred_at FROM graph_evidence g
            JOIN episodes e ON e.id=g.episode_id WHERE g.target=? AND e.occurred_at IS NOT NULL''',
            (evidence_target,)).fetchall()
        if any(datetime.fromisoformat(row['occurred_at']) > boundary for row in sources):
            due_at = evidence_loop['due_at'] if evidence_loop and evidence_loop['due_at'] is not None else previous['due_at']
            deferred = evidence_loop['deferred_until'] if evidence_loop else None
            _append(graph, commit, id, 'open', due_at, deferred)


def transfer(graph, commit, source, target):
    """Move marker state with a promoted/refined assertion; target choices win."""
    if source == target:
        return
    previous = current(graph, source)
    if previous is None:
        return
    if current(graph, target) is None:
        _append(graph, commit, target, previous['state'], previous['due_at'], previous['deferred_until'])
    if previous['state'] == 'open':
        _append(graph, commit, source, 'closed', previous['due_at'], None)


def decline_extracted(graph, commit, id):
    """Discard only an untouched extraction flag, never a chosen loop state."""
    previous = current(graph, id)
    if (previous is None or previous['state'] != 'open' or previous['due_at'] is not None
            or previous['deferred_until'] is not None):
        return False
    requests = graph.store._db.execute('''SELECT c.request FROM loop_events l
        LEFT JOIN graph_commits c ON c.id=l.commit_id WHERE l.target=?''', (id,)).fetchall()
    if not requests or any(not row['request'] or json.loads(row['request'])['command'].get('op') != 'extract'
                           for row in requests):
        return False
    _append(graph, commit, id, 'closed', None, None)
    return True


def decline_automatic(graph, commit, id, expected):
    """Retract an automatic non-obligation marker, preserving every manual choice."""
    from .store import ConflictError
    node = _target(graph, id)
    if node is None:
        raise ValueError('Loop markers require an active memory or unconfirmed candidate')
    live = snapshot(graph, id, node['version'])
    if live != expected:
        raise ConflictError('Consolidation loop state changed; derive a fresh plan')
    previous = current(graph, id)
    if ((node.get('state') != 'active' and node.get('tier') != 'candidate') or protected(graph, node)
            or live['manual_choice_at'] is not None
            or previous is None or previous['state'] != 'open'
            or previous['due_at'] is not None or previous['deferred_until'] is not None):
        return dict(id=id, status='protected_loop_choice')
    _append(graph, commit, id, 'closed', None, None)
    return dict(id=id, status='declined_not_obligation')


def protected(graph, node):
    if node.get('pinned'):
        return True
    for id in (node.get('src'), node.get('dst')):
        if id:
            resolved = endpoint(graph.store._db, id)
            if resolved is None or resolved.get('pinned'):
                return True
    return False


def endpoint(db, id):
    """Resolve only existing candidate promotions and identity continuations."""
    from .extraction import ACTIVE_CANDIDATE_SQL
    seen = set()
    for _ in range(64):
        if id in seen or db.execute('SELECT 1 FROM forgotten_targets WHERE target=?', (id,)).fetchone():
            return None
        seen.add(id)
        row = db.execute('SELECT * FROM node_versions WHERE id=? ORDER BY version DESC LIMIT 1', (id,)).fetchone()
        if row:
            if row['state'] == 'active':
                return dict(row)
            if row['state'] in ('merged', 'refined') and row['superseded_by']:
                id = row['superseded_by']
                continue
            return None
        candidate = db.execute(f'SELECT c.id FROM candidates c WHERE c.id=? AND {ACTIVE_CANDIDATE_SQL}', (id,)).fetchone()
        if candidate:
            return dict(id=id, pinned=False)
        promoted = db.execute('SELECT decision,target_id FROM candidate_decisions WHERE candidate_id=? ORDER BY rowid DESC LIMIT 1', (id,)).fetchone()
        if promoted and promoted['decision'] == 'promote' and promoted['target_id']:
            id = promoted['target_id']
        else:
            return None
    return None


def visible_loop_targets(db, rows):
    """Filter IDs/endpoints only; resolve each shared endpoint once per response."""
    checked, result = {}, []
    for raw in rows:
        row = dict(raw)
        relation = row.get('candidate_kind') == 'relation' or row.get('loop_src') or row.get('loop_dst')
        visible = True
        if relation:
            for id in (row.get('loop_src'), row.get('loop_dst')):
                if not id:
                    visible = False
                    break
                if id not in checked:
                    checked[id] = endpoint(db, id) is not None
                if not checked[id]:
                    visible = False
                    break
        if visible:
            result.append(row)
    return result


def snapshot(graph, id, version):
    """Freeze marker identity and original chronology for a consolidation plan."""
    db = graph.store._db
    row = db.execute('''SELECT id,state,due_at,deferred_until FROM loop_events
        WHERE target=? ORDER BY rowid DESC LIMIT 1''', (id,)).fetchone()
    manual_dates = []
    for target in _loop_lineage(graph, id):
        manual = db.execute('''SELECT c.recorded_at FROM loop_events l
            JOIN graph_commits c ON c.id=l.commit_id WHERE l.target=?
            AND json_extract(c.request,'$.command.op')='loop'
            AND coalesce(json_extract(c.request,'$.command.action'),'') NOT IN ('assign','decline')
            AND NOT (json_extract(c.request,'$.command.action')='close'
                AND coalesce(json_array_length(json_extract(c.request,'$.command.evidence')),0)>0)
            ORDER BY l.rowid DESC LIMIT 1''', (target,)).fetchone()
        if manual:
            manual_dates.append(manual['recorded_at'])
    dates = [item[0] for item in db.execute('''SELECT DISTINCT e.occurred_at
        FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id
        WHERE g.target=? AND g.version=?''', (id, version))]
    return dict(event_id=row['id'] if row else None, state=row['state'] if row else None,
                pending_source_at=max(dates, key=datetime.fromisoformat) if dates and None not in dates else None,
                manual_choice_at=max(manual_dates, key=datetime.fromisoformat) if manual_dates else None)


def closure_time(snapshot, sources):
    """Unknown chronology never changes a marker; every closing premise is newer."""
    if snapshot['state'] != 'open':
        return None, 'not_open'
    if not snapshot['pending_source_at'] or not sources or any(value is None for value in sources):
        return None, 'chronology_unknown'
    boundary = datetime.fromisoformat(snapshot['pending_source_at'])
    if snapshot['manual_choice_at']:
        boundary = max(boundary, datetime.fromisoformat(snapshot['manual_choice_at']))
    dates = [datetime.fromisoformat(value) for value in sources]
    if min(dates) <= boundary:
        return None, 'not_newer'
    return max(dates).astimezone(timezone.utc).isoformat(), None


def _target(graph, id):
    from .extraction import ACTIVE_CANDIDATE_SQL
    from .graph import NODE_SUMMARY
    db = graph.store._db
    node = db.execute(f'''SELECT {NODE_SUMMARY} FROM node_versions n WHERE n.id=?
        AND n.state='active' AND n.version=(SELECT max(v.version) FROM node_versions v WHERE v.id=n.id)''',
        (id,)).fetchone()
    if node is not None:
        result = dict(node, scope=json.loads(node['scope']), tier='durable', confirmed=True)
        result.update(graph._recall_details(id, node['version']))
        return result
    candidate = db.execute(f'''SELECT c.id,c.type,c.kind,c.src,c.dst,substr(c.text,1,240) AS text,c.scope,
        c.valid_from,c.valid_to FROM candidates c WHERE c.id=? AND {ACTIVE_CANDIDATE_SQL}''', (id,)).fetchone()
    if candidate is not None:
        if candidate['kind'] == 'relation' and any(not id or endpoint(db, id) is None
                for id in (candidate['src'], candidate['dst'])):
            return None
        result = dict(candidate, scope=json.loads(candidate['scope']), version=1,
                      tier='candidate', confirmed=False)
    else:
        edge = db.execute('''SELECT e.* FROM edge_versions e WHERE e.id=? AND e.state='active'
            AND e.version=(SELECT max(v.version) FROM edge_versions v WHERE v.id=e.id)''', (id,)).fetchone()
        if edge is None:
            return None
        a, b = graph._node(edge['src']), graph._node(edge['dst'])
        if a['state'] != 'active' or b['state'] != 'active':
            return None
        if any(endpoint(db, id) is None for id in (edge['src'], edge['dst'])):
            return None
        result = dict(edge, type='relation', text=f"{a['text'][:100]} {edge['rel']} {b['text'][:100]}",
                      scope=sorted(set(json.loads(a['scope'])) | set(json.loads(b['scope']))),
                      tier='durable', confirmed=True)
    result['evidence'] = [dict(row) for row in db.execute('''SELECT episode_id,start,end,
        substr(quote,1,240) AS quote_excerpt FROM graph_evidence WHERE target=? AND version=?
        ORDER BY episode_id,start LIMIT 5''', (id, result['version']))]
    return result


def command(graph, *, action, id=None, days=7, due_at=None, due_before=None, scope=None,
            command_id=None, actor=None, why=None, limit=20, responsibility=None, evidence=()):
    if action not in ('list', 'open', 'close', 'later', 'remind', 'assign', 'decline'):
        raise ValueError('Unknown loop action')
    if scope not in (None, 'personal', 'professional'):
        raise ValueError('Invalid scope')
    if due_at is not None and action != 'remind':
        raise ValueError('due_at is only supported by remind')
    if due_before is not None and action != 'list':
        raise ValueError('due_before is only supported by list')
    if responsibility is not None and responsibility not in RESPONSIBILITIES:
        raise ValueError('Invalid loop responsibility')
    if responsibility is not None and action not in ('assign', 'list'):
        raise ValueError('responsibility is only supported by assign or list')
    if evidence and action not in ('assign', 'close', 'decline'):
        raise ValueError('Loop evidence is only supported by assign, close or decline')
    if len(evidence) > 8:
        raise ValueError('Loop evidence limit is eight')
    due_before = graph._time(due_before)
    if action == 'list':
        from .extraction import ACTIVE_CANDIDATE_SQL
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
            raise ValueError('Loop list limit must be 1..100')
        now = datetime.now(timezone.utc).isoformat()
        with graph.store._mutex:
            rows = graph.store._db.execute(f'''WITH eligible AS (
                SELECT l.target AS id,l.state,l.due_at,l.deferred_until FROM loop_events l
                WHERE l.rowid=(SELECT max(x.rowid) FROM loop_events x WHERE x.target=l.target)
                AND l.state='open' AND NOT EXISTS(SELECT 1 FROM forgotten_targets f WHERE f.target=l.target) AND (l.deferred_until IS NULL OR l.deferred_until<=:now)
                AND (:due_before IS NULL OR l.due_at<:due_before)
                AND EXISTS (
                    SELECT 1 FROM node_versions n WHERE n.id=l.target AND n.state='active'
                    AND n.version=(SELECT max(v.version) FROM node_versions v WHERE v.id=n.id)
                    AND (:scope IS NULL OR EXISTS (SELECT 1 FROM json_each(n.scope) WHERE value=:scope))
                    UNION ALL
                    SELECT 1 FROM candidates c WHERE c.id=l.target AND {ACTIVE_CANDIDATE_SQL}
                    AND (:scope IS NULL OR EXISTS (SELECT 1 FROM json_each(c.scope) WHERE value=:scope))
                    UNION ALL
                    SELECT 1 FROM edge_versions e JOIN node_versions a ON a.id=e.src
                    JOIN node_versions b ON b.id=e.dst
                    WHERE e.id=l.target AND e.state='active' AND a.state='active' AND b.state='active'
                    AND e.version=(SELECT max(v.version) FROM edge_versions v WHERE v.id=e.id)
                    AND a.version=(SELECT max(v.version) FROM node_versions v WHERE v.id=a.id)
                    AND b.version=(SELECT max(v.version) FROM node_versions v WHERE v.id=b.id)
                    AND (:scope IS NULL OR EXISTS (SELECT 1 FROM json_each(a.scope) WHERE value=:scope)
                        OR EXISTS (SELECT 1 FROM json_each(b.scope) WHERE value=:scope))))
                SELECT eligible.*,c.kind AS candidate_kind,coalesce(c.src,e.src) AS loop_src,coalesce(c.dst,e.dst) AS loop_dst,
                (SELECT max(julianday(e.occurred_at)) FROM graph_evidence g
                    JOIN episodes e ON e.id=g.episode_id WHERE g.target=eligible.id) AS newest_evidence
                FROM eligible LEFT JOIN candidates c ON c.id=eligible.id LEFT JOIN edge_versions e ON e.id=eligible.id AND e.version=(SELECT max(v.version) FROM edge_versions v WHERE v.id=e.id) ORDER BY due_at IS NULL,due_at,newest_evidence DESC,eligible.id''',
                dict(now=now, due_before=due_before, scope=scope)).fetchall()
            rows = visible_loop_targets(graph.store._db, rows)
            roles = responsibilities(graph.store._db, [row['id'] for row in rows])
            selected = [row for row in rows if responsibility is None or
                        roles.get(row['id'], {}).get('responsibility', 'unknown') == responsibility]
            result = []
            for row in selected[:limit]:
                marker = {key: row[key] for key in ('id', 'state', 'due_at', 'deferred_until')}
                marker['responsibility'] = roles.get(row['id'], {}).get('responsibility', 'unknown')
                result.append(dict(_target(graph, row['id']), **marker))
            return dict(loops=result, total=len(selected))
    if not isinstance(id, str) or not id:
        raise ValueError('A loop mutation requires id')
    if action == 'later' and (isinstance(days, bool) or not isinstance(days, (int, float))
                              or not math.isfinite(days) or days <= 0):
        raise ValueError('days must be a positive finite number')
    if action == 'remind':
        due_at = graph._time(due_at)
        if due_at is None:
            raise ValueError('remind requires an aware due_at')
    request = dict(op='loop', action=action, id=id)
    if action == 'later':
        request['days'] = days
    if action == 'remind':
        request['due_at'] = due_at
    if action == 'assign':
        if responsibility is None:
            raise ValueError('assign requires responsibility')
        request.update(responsibility=responsibility, evidence=list(evidence))
    elif evidence or action == 'decline':
        request['evidence'] = list(evidence)

    def apply(commit):
        target = _target(graph, id)
        if target is None or graph.store._db.execute('SELECT 1 FROM forgotten_targets WHERE target=?', (id,)).fetchone():
            raise ValueError('Loop markers require an active memory or unconfirmed candidate')
        previous = current(graph, id)
        if action == 'assign':
            return assign(graph, commit, id, responsibility, evidence, manual=True)
        if action == 'decline' or (action == 'close' and evidence):
            references, times, _ = grounded_sources(graph, id, evidence,
                original=action == 'decline', allow_instruction=action == 'decline')
            frozen = snapshot(graph, id, target['version'])
            if action == 'decline':
                result = decline_automatic(graph, commit, id, frozen)
            else:
                source_at, status = closure_time(frozen, times)
                if protected(graph, target):
                    status = 'protected_loop_choice'
                result = dict(id=id, status=status or 'closed', source_occurred_at=source_at)
                if status is None:
                    result.update(_append(graph, commit, id, 'closed', previous['due_at'], None))
            return dict(result, evidence=references)
        deadline = previous['due_at'] if previous else None
        if action == 'close':
            return _append(graph, commit, id, 'closed', deadline, None)
        if action == 'later':
            try:
                deferred = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()
            except OverflowError:
                raise ValueError('Deferral is outside the timestamp range') from None
            return _append(graph, commit, id, 'open', deadline, deferred)
        return _append(graph, commit, id, 'open', due_at if action == 'remind' else deadline, None)

    return graph._command(command_id or uuid4().hex, actor or 'harness/loop',
                          why if why is not None else 'Loop state adjustment', request, apply)
