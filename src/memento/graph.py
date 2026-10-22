"""Append-only assertions and commits over the evidence store's single writer."""

import json
import re
from difflib import SequenceMatcher
from datetime import datetime, timezone
from uuid import uuid4

from .store import ConflictError, Store, legacy_native_context

NODE_TYPES = frozenset(('experience', 'knowledge', 'decision', 'media', 'person',
                        'organization', 'project', 'concept', 'place', 'tool'))
ENTITY_TYPES = frozenset(('person', 'organization', 'project', 'concept', 'place', 'tool'))
RELATIONS = frozenset(('belongs_to', 'follows_up_on', 'works_in', 'decided_by',
                      'supersedes', 'mentions', 'part_of', 'caused_by', 'owned_by', 'related_to'))


def behavior_ground_truth(episode, start=None, end=None):
    """A user envelope is not sufficient authority for pasted peer/tool instructions."""
    metadata = episode.get('metadata', {})
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    text = episode.get('text', '')
    if legacy_native_context(episode.get('source'), episode.get('kind'), text, metadata):
        return False
    pasted, depth, opened = [], 0, None
    for tag in re.finditer(r'<(/?)pasted_content\b[^>]*>', text, re.I):
        if not tag.group(1):
            if depth == 0:
                opened = tag.start()
            depth += 1
        elif depth:
            depth -= 1
            if depth == 0:
                pasted.append((opened, tag.end()))
    if depth:
        pasted.append((opened, len(text)))
    visible, cursor = [], 0
    for left, right in pasted:
        visible.append(text[cursor:left])
        cursor = right
    visible.append(text[cursor:])
    own_text = ''.join(visible)
    eligible = (episode.get('kind') == 'user' and metadata.get('evidence_eligible', True) is not False
        and metadata.get('extraction_mode', 'message') == 'message'
        and bool(own_text.strip())
        and re.search(r'\[\s*(?:HERDR\s+)?AGENT MESSAGE\s*\]|'
                      r'Message Type:\s*(?:NEW_TASK|MESSAGE|FINAL_ANSWER)|'
                      r'<(?:system-reminder|collaboration[^>]*|tool_result)\b', own_text, re.I) is None)
    if not eligible or start is None:
        return eligible
    if any(start < right and (end if end is not None else len(text)) > left for left, right in pasted):
        return False
    # Fenced examples and Markdown quotations are source data, even in a user
    # message. The model still decides whether an ordinary incident is causal.
    prefix = text[:start]
    for left, right in reversed(pasted):
        if left < start:
            prefix = prefix[:left] + prefix[min(right, start):]
    if len(re.findall(r'^\s*(?:```|~~~)', prefix, re.M)) % 2:
        return False
    line_start = text.rfind('\n', 0, start) + 1
    if text[line_start:start].lstrip().startswith('>'):
        return False
    quote = text[start:end].strip()
    if quote.startswith(('```', '~~~', '>')):
        return False
    return True


NODE_SUMMARY = ('n.id,n.version,n.type,substr(n.text,1,240) AS text,n.state,'
                'n.valid_from,n.valid_to,n.pinned,n.superseded_by,n.commit_id,n.scope,'
                '(SELECT c.recorded_at FROM node_versions v JOIN graph_commits c ON c.id=v.commit_id '
                'WHERE v.id=n.id ORDER BY v.version LIMIT 1) AS created_at,'
                'coalesce((SELECT importance FROM node_stats WHERE node_id=n.id),0.5) AS importance,'
                'coalesce((SELECT decay FROM node_stats WHERE node_id=n.id),1) AS decay,'
                'coalesce((SELECT archived FROM node_stats WHERE node_id=n.id),0) AS archived,'
                'coalesce((SELECT access_count FROM node_stats WHERE node_id=n.id),0) AS access_count,'
                '(SELECT last_cited_at FROM node_stats WHERE node_id=n.id) AS last_cited_at')


def commit_behaviors(request, result):
    """Map experience identities created by one consolidation commit to their behavior."""
    command = json.loads(request).get('command', {})
    if command.get('op') != 'consolidate':
        return {}
    results, behaviors = json.loads(result).get('results', {}), {}
    for action in command.get('output', {}).get('actions', []):
        outcome = results.get(action.get('candidate_id'), {})
        target = outcome.get('superseded_by') or outcome.get('id')
        if (target and action.get('operation') in ('remember', 'supersede')
                and action.get('type') == 'experience' and action.get('behavior')):
            behaviors.setdefault(target, action['behavior'])
    return behaviors


class Graph:
    def __init__(self, store: Store):
        self.store = store

    def _node(self, node_id):
        row = self.store._db.execute(
            'SELECT * FROM node_versions WHERE id=? ORDER BY version DESC LIMIT 1',
            (node_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f'Unknown node: {node_id}')
        return dict(row)

    def _behavior(self, node_id):
        """Read immutable creation metadata, independent of later citation/attach versions.

        One primary-key node lookup and one commit are sufficient. The existing
        consolidation result maps actions to newly created/replacement identities;
        evidence and ordinary assertion history remain in their original tables.
        """
        row = self.store._db.execute('''SELECT g.request,g.result FROM node_versions n
            JOIN graph_commits g ON g.id=n.commit_id WHERE n.id=? AND n.version=1''',
            (node_id,)).fetchone()
        return None if row is None else commit_behaviors(row['request'], row['result']).get(node_id)

    def _resolve_evidence(self, request):
        sources = []
        for source in request['evidence']:
            if source.get('episode_id'):
                sources.append(source)
                continue
            quote = source.get('quote')
            if not isinstance(quote, str) or not quote:
                raise ValueError('Evidence quote must be a nonempty string')
            if not request.get('source') or not request.get('conversation_id'):
                raise ValueError('Quote-only evidence requires source and conversation_id')
            row = self.store._db.execute('''SELECT id FROM episodes
                WHERE source=? AND conversation_id=? AND instr(text,?)>0
                AND coalesce(json_extract(metadata,'$.extraction_mode'),'message')!='skip'
                AND coalesce(json_extract(metadata,'$.evidence_eligible'),1)!=0
                AND kind NOT IN ('native_record','native_hook','native_live_record')
                AND NOT EXISTS (SELECT 1 FROM forgotten_sources f
                    WHERE f.source=episodes.source AND f.event_id=episodes.event_id)
                ORDER BY seq DESC LIMIT 1''',
                (request['source'], request['conversation_id'], quote)).fetchone()
            if row is None:
                raise ValueError('Evidence quote was not found in the current conversation')
            sources.append(dict(source, episode_id=row['id']))
        return dict(request, evidence=sources)

    def _evidence(self, target, version, sources):
        if not sources:
            raise ValueError('An assertion requires exact source evidence')
        db = self.store._db
        seen = set()
        for source in sources:
            row = db.execute('''SELECT text,source,event_id,text_runs,raw_frame,text_sha256,conversation_id
                FROM episodes WHERE id=?''',
                             (source['episode_id'],)).fetchone()
            if row is None:
                raise ValueError('Unknown source episode')
            if db.execute('SELECT 1 FROM forgotten_sources WHERE source=? AND event_id=? LIMIT 1',
                          (row['source'], row['event_id'])).fetchone():
                raise ConflictError('This evidence was excluded from active memory by a human decision')
            quote = source['quote']
            if row['text_runs'] is not None:
                raise ValueError('Offloaded raw source is not assertion evidence; cite the message projection')
            text = self.store.original_text(row)
            if not isinstance(quote, str) or not quote:
                raise ValueError('Evidence quote must be a nonempty string')
            matches = []
            at = text.find(quote)
            while at >= 0:
                matches.append(at)
                at = text.find(quote, at + 1)
            occurrence = source.get('occurrence')
            if not matches or (occurrence is None and len(matches) != 1):
                raise ValueError('Evidence must match exactly and unambiguously')
            if occurrence is None:
                occurrence = 0
            if not isinstance(occurrence, int) or not 0 <= occurrence < len(matches):
                raise ValueError('Invalid evidence occurrence')
            start = matches[occurrence]
            identity = (source['episode_id'], start, start + len(quote))
            if identity in seen:
                continue
            seen.add(identity)
            db.execute('''INSERT INTO graph_evidence SELECT ?,?,?,?,?,? WHERE NOT EXISTS (
                SELECT 1 FROM graph_evidence WHERE target=? AND version=? AND episode_id=? AND start=? AND end=?)''',
                (target, version, source['episode_id'], start, start + len(quote), quote,
                 target, version, source['episode_id'], start, start + len(quote)))

    def _command(self, command_id, actor, why, request, apply):
        if not command_id or not actor or not why.strip():
            raise ValueError('command_id, actor and why are required')
        serialized = json.dumps({'actor': actor, 'why': why, 'command': request},
                                sort_keys=True, ensure_ascii=False, allow_nan=False)
        with self.store._mutex:
            db = self.store._db
            db.execute('BEGIN IMMEDIATE')
            try:
                prior = db.execute('SELECT request,result FROM graph_commits WHERE command_id=?',
                                   (command_id,)).fetchone()
                if prior:
                    if prior['request'] != serialized:
                        raise ConflictError('Command identifier already contains a different command')
                    db.execute('COMMIT')
                    return json.loads(prior['result'])
                commit_id = uuid4().hex
                result = apply(commit_id)
                db.execute('INSERT INTO graph_commits VALUES (?,?,?,?,?,?,?)',
                           (commit_id, command_id, serialized, json.dumps(result), actor, why,
                            datetime.now(timezone.utc).isoformat()))
                db.execute('COMMIT')
                return result
            except BaseException:
                if db.in_transaction:
                    db.execute('ROLLBACK')
                raise

    @staticmethod
    def _time(value):
        if value is None:
            return None
        try:
            parsed = datetime.fromisoformat(value)
        except (ValueError, TypeError):
            raise ValueError('World time must be an aware ISO timestamp') from None
        if parsed.tzinfo is None:
            raise ValueError('World time must be an aware ISO timestamp')
        return parsed.astimezone(timezone.utc).isoformat()

    def _scope(self, type, scope):
        if (not isinstance(scope, list) or not 1 <= len(scope) <= 2
                or any(not isinstance(label, str) or label not in ('personal', 'professional') for label in scope)
                or len(set(scope)) != len(scope)):
            raise ValueError('scope requires distinct personal/professional labels')
        if len(scope) == 2 and type not in ENTITY_TYPES:
            with self.store._mutex:
                category = self.store._db.execute('SELECT category FROM memory_types WHERE name=?', (type,)).fetchone()
            if category is None or category['category'] != 'entity':
                raise ValueError('Only an entity may have both scopes')
        return sorted(scope)

    def remember(self, *, command_id, actor, why, type, text, evidence, scope,
                 valid_from=None, valid_to=None, source=None, conversation_id=None):
        valid_from, valid_to = self._time(valid_from), self._time(valid_to)
        if valid_from is not None and valid_to is not None and valid_from >= valid_to:
            raise ValueError('valid_from must precede valid_to')
        if not isinstance(type, str) or re.fullmatch(r'[a-z][a-z0-9_]{0,63}', type) is None or not text.strip():
            raise ValueError('Invalid node type or empty assertion')
        request = dict(op='remember', type=type, text=text, evidence=evidence,
                       scope=self._scope(type, scope), source=source, conversation_id=conversation_id,
                       valid_from=valid_from, valid_to=valid_to)
        return self._command(command_id, actor, why, request,
                             lambda commit: self._apply_remember(commit, request))

    def _known_type(self, name):
        return name in NODE_TYPES or self.store._db.execute(
            'SELECT 1 FROM memory_types WHERE name=?', (name,)).fetchone() is not None

    def _sensitive_type(self, name):
        if name == 'person':
            return True
        row = self.store._db.execute('SELECT sensitive FROM memory_types WHERE name=?', (name,)).fetchone()
        return bool(row and row['sensitive'])

    def _apply_remember(self, commit, request, *, approved=False):
        request = self._resolve_evidence(dict(request, scope=self._scope(request['type'], request['scope'])))
        known = self._known_type(request['type'])
        if not approved and (not known or self._sensitive_type(request['type'])):
            return self._propose(commit, request, {})
        if not known:
            self.store._db.execute('INSERT INTO memory_types VALUES (?,?,?,?)',
                                  (request['type'], 'memory', 1, commit))
        return self._insert_node(commit, request)

    def _insert_node(self, commit, request):
        node_id = uuid4().hex[:12]
        self.store._db.execute('INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)',
            (node_id, 1, commit, request['type'], request['text'], 'active',
             request.get('valid_from'), request.get('valid_to'), None, int(request.get('pinned', False)),
             json.dumps(request['scope'])))
        self.store._db.execute('INSERT INTO node_stats(node_id) VALUES (?)', (node_id,))
        self._evidence(node_id, 1, request['evidence'])
        return {'id': node_id, 'version': 1}

    def _propose(self, commit, request, base_versions):
        proposal_id = 'r_' + uuid4().hex[:16]
        self._evidence(proposal_id, 1, request['evidence'])
        self.store._db.execute('INSERT INTO review_proposals VALUES (?,?,?,?)',
            (proposal_id, commit, json.dumps(request), json.dumps(base_versions)))
        return {'id': proposal_id, 'status': 'pending_review'}

    def link(self, *, command_id, actor, why, a, rel, b, evidence, source=None, conversation_id=None):
        if rel == 'supersedes':
            raise ValueError('Use atomic supersede; direct supersession links are not allowed')
        if rel not in RELATIONS:
            raise ValueError('Unknown directional relation')
        request = dict(op='link', a=a, rel=rel, b=b, evidence=evidence, source=source, conversation_id=conversation_id)
        return self._command(command_id, actor, why, request,
                             lambda commit: self._apply_link(commit, request))

    def _apply_link(self, commit, request, *, approved=False):
        request = self._resolve_evidence(request)
        endpoints = {node_id: self._node(node_id) for node_id in (request['a'], request['b'])}
        if any(node['state'] != 'active' for node in endpoints.values()):
            raise ValueError('Relations require active endpoints')
        if not approved and any(node['pinned'] or self._sensitive_type(node['type']) for node in endpoints.values()):
            return self._propose(commit, request, {key: node['version'] for key, node in endpoints.items()})
        edge_id = uuid4().hex[:12]
        self.store._db.execute('INSERT INTO edge_versions VALUES (?,?,?,?,?,?,?)',
            (edge_id, 1, commit, request['a'], request['rel'], request['b'], 'active'))
        self._evidence(edge_id, 1, request['evidence'])
        return {'id': edge_id, 'version': 1}

    def history(self, node_id):
        with self.store._mutex:
            raw = self.store._db.execute('SELECT * FROM episodes WHERE id=?', (node_id,)).fetchone()
            blocked = raw is not None and self.store._db.execute(
                'SELECT 1 FROM forgotten_sources WHERE source=? AND event_id=? LIMIT 1',
                (raw['source'], raw['event_id'])).fetchone() is not None
        if raw is not None:
            episode = self.store.hydrate_episode(raw)
            return [dict(episode, version=1, type='raw_episode', state='raw', confirmed=False,
                actor=episode['source'], why='Immutable captured evidence',
                recorded_at=episode['captured_at'], recorded_until=None, valid_from=None, valid_to=None,
                forgotten=blocked, evidence=[dict(episode_id=node_id, start=0, end=len(episode['text']),
                                                  quote=episode['text'])])]
        with self.store._mutex:
            rows = self.store._db.execute('''WITH RECURSIVE lineage(id) AS (
                SELECT id FROM node_versions WHERE id=?
                UNION SELECT CASE WHEN e.src=l.id THEN e.dst ELSE e.src END
                FROM edge_versions e JOIN lineage l ON e.src=l.id OR e.dst=l.id
                WHERE e.rel='supersedes')
                SELECT n.*,c.actor,c.why,c.recorded_at,
                lead(c.recorded_at) OVER (PARTITION BY n.id ORDER BY n.version) AS recorded_until
                FROM lineage l JOIN node_versions n ON n.id=l.id JOIN graph_commits c ON c.id=n.commit_id
                ORDER BY (SELECT v.rowid FROM node_versions v WHERE v.id=n.id ORDER BY v.version LIMIT 1),
                n.version''', (node_id,)).fetchall()
            if not rows:
                rows = self.store._db.execute('''SELECT e.*,c.actor,c.why,c.recorded_at,
                    lead(c.recorded_at) OVER (ORDER BY e.version) AS recorded_until
                    FROM edge_versions e JOIN graph_commits c ON c.id=e.commit_id
                    WHERE e.id=? ORDER BY e.version''', (node_id,)).fetchall()
            if not rows:
                rows = self.store._db.execute('''SELECT p.*,1 AS version,'candidate' AS state,
                    r.version AS extractor_version,r.requested_model,r.reported_models,r.usage,r.commit_id,
                    c.actor,c.why AS extraction_why,c.recorded_at,NULL AS recorded_until
                    FROM candidates p JOIN extraction_runs r ON r.id=p.run_id
                    JOIN graph_commits c ON c.id=r.commit_id WHERE p.id=?''', (node_id,)).fetchall()
            if not rows:
                return []
            result = []
            previous_text = ''
            for row in rows:
                item = dict(row)
                if 'scope' in item:
                    item['scope'] = json.loads(item['scope'])
                if item.get('type') == 'experience':
                    behavior = self._behavior(item['id'])
                    if behavior:
                        item.update(behavior=behavior, authority=behavior['authority'],
                                    category='behavioral_suggestion')
                item['forgotten'] = self.store._db.execute(
                    'SELECT 1 FROM forgotten_targets WHERE target=?', (item['id'],)).fetchone() is not None
                item['evidence'] = [dict(e) for e in self.store._db.execute(
                    'SELECT episode_id,start,end,quote FROM graph_evidence WHERE target=? AND version=?',
                    (item['id'], row['version']))]
                text = item.get('text', '')
                old_words = re.findall(r'\w+|[^\w\s]+|\s+', previous_text)
                new_words = re.findall(r'\w+|[^\w\s]+|\s+', text)
                item['diff'] = [dict(op=op, old=''.join(old_words[a:b]), new=''.join(new_words[c:d]))
                    for op,a,b,c,d in SequenceMatcher(None, old_words, new_words, autojunk=False).get_opcodes()
                    if op != 'equal']
                previous_text = text
                from .loops import current, events
                item['loop'] = current(self, item['id'])
                if item['version'] == 1:
                    item['loop_events'] = events(self, item['id'])
                if item['state'] == 'candidate':
                    item['reported_models'] = json.loads(item['reported_models'])
                    item['usage'] = json.loads(item['usage'])
                    item['decisions'] = [dict(event) for event in self.store._db.execute(
                        '''SELECT d.decision,d.target_id,d.next_review_at,c.actor,c.why,c.recorded_at
                           FROM candidate_decisions d JOIN graph_commits c ON c.id=d.commit_id
                           WHERE d.candidate_id=? ORDER BY c.recorded_at,d.rowid''', (item['id'],))]
                    item['entity_matches'] = [dict(match) for match in self.store._db.execute(
                        'SELECT * FROM candidate_entity_matches WHERE candidate_id=? ORDER BY target,version', (item['id'],))]
                result.append(item)
            return result

    def supersede(self, *, command_id, actor, why, old, new, evidence,
                  kind='correction', valid_at=None, source=None, conversation_id=None):
        if kind not in ('correction', 'change', 'refinement'):
            raise ValueError('Supersession kind must be correction, change or refinement')
        valid_at = self._time(valid_at)
        if kind == 'change' and valid_at is None:
            raise ValueError('A world-time change requires an aware valid_at')
        if not isinstance(new, dict) or set(new) != {'type', 'text', 'scope'}:
            raise ValueError('Replacement requires type, text and scope')
        if (not isinstance(new['type'], str) or re.fullmatch(r'[a-z][a-z0-9_]{0,63}', new['type']) is None
                or not isinstance(new['text'], str) or not new['text'].strip()):
            raise ValueError('Invalid replacement assertion')
        new = dict(new, scope=self._scope(new['type'], new['scope']))
        request = dict(op='supersede', old=old, new=new, evidence=evidence,
                       kind=kind, valid_at=valid_at, source=source, conversation_id=conversation_id)
        return self._command(command_id, actor, why, request,
                             lambda commit: self._apply_supersede(commit, request))

    def _apply_supersede(self, commit, request, *, approved=False):
        request = self._resolve_evidence(request)
        old, new = request['old'], request['new']
        kind, valid_at, evidence = request['kind'], request['valid_at'], request['evidence']
        previous = self._node(old)
        new = dict(new, scope=self._scope(new['type'], new['scope']))
        if previous['state'] != 'active':
            raise ValueError('Supersession requires an active node')
        sensitive = (previous['pinned'] or self._sensitive_type(previous['type'])
                     or self._sensitive_type(new['type']) or not self._known_type(new['type']))
        if sensitive and not approved:
            return self._propose(commit, request, {old: previous['version']})
        if not self._known_type(new['type']):
            self.store._db.execute('INSERT INTO memory_types VALUES (?,?,?,?)',
                                  (new['type'], 'memory', 1, commit))
        if kind == 'change' and previous['valid_from'] is not None and valid_at <= previous['valid_from']:
            raise ValueError('Change must follow the previous valid_from')
        replacement = self._insert_node(commit, dict(type=new['type'], text=new['text'],
            scope=new['scope'],
            valid_from=valid_at if kind == 'change' else None, evidence=evidence, pinned=previous['pinned']))
        replacement_id = replacement['id']
        version = previous['version'] + 1
        self.store._db.execute('INSERT INTO node_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)',
            (old, version, commit, previous['type'], previous['text'],
             {'correction': 'corrected', 'change': 'superseded', 'refinement': 'refined'}[kind],
             previous['valid_from'], valid_at if kind == 'change' else previous['valid_to'],
             replacement_id, previous['pinned'], previous['scope']))
        self._evidence(old, version, evidence)
        self.store._db.execute('''UPDATE memory_vectors SET current=0 WHERE rowid IN (
            SELECT seq FROM vector_items WHERE (kind='node' AND target=?)
            OR (kind='segment' AND target IN (SELECT id FROM segments WHERE node_id=?)))''',
            (old, old))
        edge_id = uuid4().hex[:12]
        self.store._db.execute('INSERT INTO edge_versions VALUES (?,?,?,?,?,?,?)',
            (edge_id, 1, commit, replacement_id, 'supersedes', old, 'active'))
        self._evidence(edge_id, 1, evidence)
        from .loops import transfer
        transfer(self, commit, old, replacement_id)
        from .importance import transfer_usage
        transfer_usage(self.store, old, replacement_id)
        return {'id': old, 'version': version, 'superseded_by': replacement_id, 'kind': kind}

    def pending_reviews(self):
        with self.store._mutex:
            return [dict(row) for row in self.store._db.execute('''
                SELECT p.*,c.actor,c.why,c.recorded_at FROM review_proposals p
                JOIN graph_commits c ON c.id=p.commit_id
                WHERE NOT EXISTS (SELECT 1 FROM review_decisions d WHERE d.proposal_id=p.id)
                ORDER BY c.recorded_at,p.id''')]

    def _decide_review(self, *, command_id, actor, why, proposal_id, decision):
        """Called only by the private, OS-permission-gated admin transport."""
        if decision not in ('approve', 'reject'):
            raise ValueError('Decision must be approve or reject')
        def apply(commit):
            db = self.store._db
            proposal = db.execute('SELECT * FROM review_proposals WHERE id=?', (proposal_id,)).fetchone()
            if proposal is None:
                raise ValueError('Unknown proposal')
            if db.execute('SELECT 1 FROM review_decisions WHERE proposal_id=?', (proposal_id,)).fetchone():
                raise ValueError('Proposal already decided')
            result = {'id': proposal_id, 'status': 'rejected'}
            if decision == 'approve':
                if db.execute('''SELECT 1 FROM graph_evidence g JOIN episodes e ON e.id=g.episode_id
                    JOIN forgotten_sources f ON f.source=e.source AND f.event_id=e.event_id
                    WHERE g.target=? LIMIT 1''', (proposal_id,)).fetchone():
                    raise ConflictError('Proposal evidence was excluded by a human forget decision')
                for node_id, version in json.loads(proposal['base_versions']).items():
                    if self._node(node_id)['version'] != version:
                        raise ConflictError('Proposal is stale; submit a new proposal')
                request = json.loads(proposal['command'])
                if request['op'] == 'remember':
                    result = self._apply_remember(commit, request, approved=True)
                elif request['op'] == 'supersede':
                    result = self._apply_supersede(commit, request, approved=True)
                elif request['op'] == 'link':
                    result = self._apply_link(commit, request, approved=True)
                elif request['op'] in ('pin', 'merge', 'register_type', 'attach'):
                    from .curation import apply_review
                    result = apply_review(self, commit, request)
                else:
                    raise ValueError('Unsupported proposal operation')
            db.execute('INSERT INTO review_decisions VALUES (?,?,?)', (proposal_id, commit, decision))
            candidates = db.execute('''SELECT d.candidate_id FROM candidate_decisions d
                WHERE d.decision='review' AND d.target_id=? AND d.rowid=(
                    SELECT max(x.rowid) FROM candidate_decisions x WHERE x.candidate_id=d.candidate_id)''',
                (proposal_id,)).fetchall()
            target = result.get('superseded_by') or result.get('merged_into') or result.get('id')
            if decision == 'reject':
                target = None
            for candidate in candidates:
                state = 'reject' if decision == 'reject' else ('promote' if target else 'defer')
                db.execute('INSERT INTO candidate_decisions VALUES (?,?,?,?,?,?)',
                    (uuid4().hex, candidate['candidate_id'], commit, state, target, None))
                if state == 'promote' and target:
                    from .loops import current, mark, transfer
                    candidate_loop = current(self, candidate['candidate_id'])
                    transfer(self, commit, candidate['candidate_id'], target)
                    from .importance import transfer_usage
                    transfer_usage(self.store, candidate['candidate_id'], target)
                    mark(self, commit, target, bool(candidate_loop and candidate_loop['state'] == 'open'),
                         evidence_target=candidate['candidate_id'], evidence_loop=candidate_loop)
            return result
        return self._command(command_id, actor, why,
            dict(op='review', proposal_id=proposal_id, decision=decision), apply)

    def recall_keywords(self, query, limit=20, *, scope=None, include_archived=True):
        """Literal keyword lookup, filtering before either FTS result limit."""
        # Keep include_archived compatible; age affects ranking, not eligibility.
        if not 1 <= limit <= 100 or '\x00' in query or scope not in (None, 'personal', 'professional'):
            raise ValueError('Invalid recall limit, query or scope')
        match = ' '.join('"' + term.replace('"', '""') + '"' for term in query.split())
        if not match:
            return []
        visible = '''n.state='active'
            AND n.version=(SELECT max(v.version) FROM node_versions v WHERE v.id=n.id)
            AND (? IS NULL OR EXISTS (SELECT 1 FROM json_each(n.scope) WHERE value=?))'''
        with self.store._mutex:
            matches = [dict(row) for row in self.store._db.execute(f'''
                SELECT {NODE_SUMMARY},bm25(node_fts) AS keyword_rank
                FROM node_fts JOIN node_versions n ON n.id=node_fts.id AND n.version=node_fts.version
                WHERE node_fts MATCH ? AND {visible}
                ORDER BY keyword_rank,n.id LIMIT ?''', (match, scope, scope, limit))]
            aliases = self.store._db.execute(f'''WITH RECURSIVE canonical(source,target,depth) AS (
                SELECT source,target,0 FROM entity_merges
                UNION ALL SELECT c.source,m.target,c.depth+1 FROM canonical c
                JOIN entity_merges m ON m.source=c.target WHERE c.depth<64)
                SELECT {NODE_SUMMARY},bm25(alias_fts) AS keyword_rank
                FROM alias_fts JOIN canonical a ON a.source=alias_fts.id
                JOIN node_versions n ON n.id=a.target
                WHERE alias_fts MATCH ? AND {visible}
                AND NOT EXISTS (SELECT 1 FROM entity_merges WHERE source=a.target)
                ORDER BY keyword_rank,n.id LIMIT ?''', (match, scope, scope, limit))
            seen = {item['id'] for item in matches}
            for row in aliases:
                if row['id'] not in seen:
                    matches.append(dict(row))
                    seen.add(row['id'])
            for item in matches:
                item['scope'] = json.loads(item['scope'])
            return sorted(matches, key=lambda item: (item['keyword_rank'], item['id']))[:limit]

    def loop(self, **arguments):
        from .loops import command
        return command(self, **arguments)

    def _recall_details(self, node_id, version):
        db = self.store._db
        change = db.execute('''SELECT c.recorded_at,substr(c.actor,1,128) AS actor,
            substr(c.why,1,240) AS why,
            (SELECT count(*) FROM node_versions WHERE id=?) AS change_count
            FROM node_versions n JOIN graph_commits c ON c.id=n.commit_id
            WHERE n.id=? AND n.version=?''', (node_id, node_id, version)).fetchone()
        sources = db.execute('''SELECT g.episode_id,g.start,g.end,
            CASE WHEN EXISTS (SELECT 1 FROM episodes e JOIN forgotten_sources f
                ON f.source=e.source AND f.event_id=e.event_id WHERE e.id=g.episode_id)
                THEN NULL ELSE substr(g.quote,1,240) END AS quote_excerpt
            FROM graph_evidence g WHERE g.target=? AND g.version=? ORDER BY g.episode_id,g.start LIMIT 5''',
            (node_id, version)).fetchall()
        from .loops import current
        result = dict(evidence=[dict(row) for row in sources],
                    change_count=change['change_count'],
                    last_change={key: change[key] for key in ('recorded_at', 'actor', 'why')},
                    loop=current(self, node_id))
        behavior = self._behavior(node_id)
        if behavior:
            result.update(behavior=behavior, authority=behavior['authority'], category='behavioral_suggestion')
        return result

    def _forget_preview(self, target):
        from .forgetting import preview
        with self.store._mutex:
            return preview(self, target)

    def _forget(self, *, command_id, actor, why, target, token):
        from .forgetting import apply
        return self._command(command_id, actor, why, dict(op='forget_active', target=target),
            lambda commit: apply(self, commit, target, token))
