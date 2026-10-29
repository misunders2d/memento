"""Deterministic processing-capacity summary; stdlib only, read-only.

  python3 capacity.py snapshot URL > before.json      # GET URL/status, stamped
  python3 capacity.py summarize before.json after.json [receipts|-]

Receipts are the runner's JSON journal lines for the same interval (plain,
prefixed or `journalctl -o json`). Status counts are net: new captures and
extractions offset drain, so they bound capacity rather than gross work.
"""
import json
import sys
import urllib.request
from collections import Counter
from datetime import datetime, timezone

TOKENS = ('input', 'output', 'cacheRead', 'cacheWrite', 'totalTokens')


def snapshot(url):
    with urllib.request.urlopen(url.rstrip('/') + '/status', timeout=120) as response:
        return {'at': datetime.now(timezone.utc).isoformat(), 'status': json.load(response)}


def receipts(lines):
    for line in lines:
        start = line.find('{')
        if start < 0:
            continue
        try:
            value = json.loads(line[start:])
        except ValueError:
            continue
        if isinstance(value, dict) and isinstance(value.get('MESSAGE'), str):
            try:
                value = json.loads(value['MESSAGE'])
            except ValueError:
                continue
        if isinstance(value, dict) and isinstance(value.get('stage'), str):
            yield value


def summarize(before, after, lines=()):
    hours = (datetime.fromisoformat(after['at']) - datetime.fromisoformat(before['at'])).total_seconds() / 3600
    if hours <= 0:
        raise ValueError('the after snapshot must be later than the before snapshot')
    # Accept raw /status or runner status output.
    old, new = (value['status'].get('service', value['status']) for value in (before, after))
    per_hour = lambda amount: round(amount / hours, 2)
    eta = lambda remaining, drained: round(remaining * hours / drained, 2) if drained > 0 else None
    result = {'hours': round(hours, 4)}
    for kind in ('extraction', 'consolidation'):
        done = new[kind]['completed_jobs'] - old[kind]['completed_jobs']
        result[kind] = {'completed_jobs': done, 'completed_jobs_per_hour': per_hour(done),
                        'failed_jobs': new[kind]['failed_jobs'] - old[kind]['failed_jobs']}
    pending = 'pending_conversations'
    result['extraction'].update({pending: new['extraction'][pending],
                                 'net_conversation_drain': old['extraction'][pending] - new['extraction'][pending]})
    for field in ('pending_candidates', 'ready_candidates'):
        drained = old['consolidation'][field] - new['consolidation'][field]
        result['consolidation'][field] = {'after': new['consolidation'][field], 'net_drain': drained,
                                          'net_drain_per_hour': per_hour(drained),
                                          'eta_hours': eta(new['consolidation'][field], drained)}
    indexing = {'available': new['indexing'].get('available')}
    if 'indexed' in old['indexing'] and 'indexed' in new['indexing']:
        indexed = new['indexing']['indexed'] - old['indexing']['indexed']
        drained = old['indexing']['pending'] - new['indexing']['pending']
        indexing.update(indexed=indexed, indexed_per_hour=per_hour(indexed), pending=new['indexing']['pending'],
                        net_pending_drain=drained, eta_hours=eta(new['indexing']['pending'], drained),
                        failed=new['indexing'].get('failed'))
    result['indexing'] = indexing
    commits, failures, tokens = Counter(), Counter(), Counter()
    model_calls = finished = 0
    for receipt in receipts(lines):
        stage = receipt['stage']
        if stage == 'worker_committed':
            commits[str(receipt.get('kind'))] += 1
        elif stage == 'worker_model':
            model_calls += 1
            tokens.update({key: value for key, value in (receipt.get('usage') or {}).items()
                           if key in TOKENS and type(value) in (int, float)})
        elif stage == 'worker_done':
            finished += 1
        elif receipt.get('error'):
            key = (stage, receipt['error'], receipt.get('detail_code') or receipt.get('cause'),
                   receipt.get('action'), receipt.get('http_status'))
            failures['|'.join('' if part is None else str(part) for part in key)] += 1
    result['receipts'] = {'commits': dict(commits), 'commits_per_hour': per_hour(sum(commits.values())),
                          'model_calls': model_calls, 'tokens': dict(tokens), 'worker_done': finished,
                          'failed_attempts': dict(failures)}
    return result


def main(argv):
    if len(argv) == 2 and argv[0] == 'snapshot':
        print(json.dumps(snapshot(argv[1])))
        return 0
    if argv[:1] == ['summarize'] and len(argv) in (3, 4):
        before, after = (json.load(open(path, encoding='utf-8')) for path in argv[1:3])
        lines = () if len(argv) == 3 else sys.stdin if argv[3] == '-' else open(argv[3], encoding='utf-8')
        print(json.dumps(summarize(before, after, lines), indent=1, sort_keys=True))
        return 0
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
