"""Reactive loop markers, independent of assertion versions and world-time facts."""

import json
import math
from datetime import datetime, timedelta, timezone
from uuid import uuid4


def current(graph, id, db=None):
    sql = '''SELECT target AS id,state,due_at,deferred_until
        FROM loop_events WHERE target=? ORDER BY rowid DESC LIMIT 1'''
    if db is not None:
        row = db.execute(sql, (id,)).fetchone()
        return dict(row) if row is not None else None
    with graph.store._mutex:
        row = graph.store._db.execute(sql, (id,)).fetchone()
        return dict(row) if row is not None else None


def events(graph, id):
    with graph.store._mutex:
        return [dict(row) for row in graph.store._db.execute('''SELECT l.state,l.due_at,
            l.deferred_until,l.commit_id,c.actor,c.why,c.recorded_at FROM loop_events l
            JOIN graph_commits c ON c.id=l.commit_id WHERE l.target=? ORDER BY l.rowid''', (id,))]


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
    node = graph._node(id)
    live = snapshot(graph, id, node['version'])
    if live != expected:
        raise ConflictError('Consolidation loop state changed; derive a fresh plan')
    previous = current(graph, id)
    if (node['state'] != 'active' or node['pinned'] or live['manual_choice_at'] is not None
            or previous is None or previous['state'] != 'open'
            or previous['due_at'] is not None or previous['deferred_until'] is not None):
        return dict(id=id, status='protected_loop_choice')
    _append(graph, commit, id, 'closed', None, None)
    return dict(id=id, status='declined_not_obligation')


def snapshot(graph, id, version):
    """Freeze marker identity and original chronology for a consolidation plan."""
    db = graph.store._db
    row = db.execute('''SELECT id,state,due_at,deferred_until FROM loop_events
        WHERE target=? ORDER BY rowid DESC LIMIT 1''', (id,)).fetchone()
    manual_dates = []
    for target in _loop_lineage(graph, id):
        manual = db.execute('''SELECT c.recorded_at FROM loop_events l
            JOIN graph_commits c ON c.id=l.commit_id WHERE l.target=?
            AND json_extract(c.request,'$.command.op')='loop' ORDER BY l.rowid DESC LIMIT 1''', (target,)).fetchone()
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
    candidate = db.execute(f'''SELECT c.id,c.type,substr(c.text,1,240) AS text,c.scope,
        c.valid_from,c.valid_to FROM candidates c WHERE c.id=? AND {ACTIVE_CANDIDATE_SQL}''', (id,)).fetchone()
    if candidate is not None:
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
        result = dict(edge, type='relation', text=f"{a['text'][:100]} {edge['rel']} {b['text'][:100]}",
                      scope=sorted(set(json.loads(a['scope'])) | set(json.loads(b['scope']))),
                      tier='durable', confirmed=True)
    result['evidence'] = [dict(row) for row in db.execute('''SELECT episode_id,start,end,
        substr(quote,1,240) AS quote_excerpt FROM graph_evidence WHERE target=? AND version=?
        ORDER BY episode_id,start LIMIT 5''', (id, result['version']))]
    return result


def command(graph, *, action, id=None, days=7, due_at=None, due_before=None, scope=None,
            command_id=None, actor=None, why=None, limit=20):
    if action not in ('list', 'open', 'close', 'later', 'remind'):
        raise ValueError('Unknown loop action')
    if scope not in (None, 'personal', 'professional'):
        raise ValueError('Invalid scope')
    if due_at is not None and action != 'remind':
        raise ValueError('due_at is only supported by remind')
    if due_before is not None and action != 'list':
        raise ValueError('due_before is only supported by list')
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
                AND l.state='open' AND (l.deferred_until IS NULL OR l.deferred_until<=:now)
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
                SELECT eligible.*,count(*) OVER () AS total,
                (SELECT max(julianday(e.occurred_at)) FROM graph_evidence g
                    JOIN episodes e ON e.id=g.episode_id WHERE g.target=eligible.id) AS newest_evidence
                FROM eligible ORDER BY due_at IS NULL,due_at,newest_evidence DESC,id LIMIT :limit''',
                dict(now=now, due_before=due_before, scope=scope, limit=limit)).fetchall()
            result = []
            for row in rows:
                marker = {key: row[key] for key in ('id', 'state', 'due_at', 'deferred_until')}
                result.append(dict(_target(graph, row['id']), **marker))
            return dict(loops=result, total=rows[0]['total'] if rows else 0)
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

    def apply(commit):
        if _target(graph, id) is None:
            raise ValueError('Loop markers require an active memory or unconfirmed candidate')
        previous = current(graph, id)
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
