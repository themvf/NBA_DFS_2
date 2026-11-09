"""Descriptive cumulative pool diagnostics; no new forecast or selection policy."""
from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path

from research.nba_research_archive import MATH_SHA, MODEL_SHA, checked, read
from research.nba_points_forward import write_new
from research.nba_trio_math_runner import signed


def summarize(audits, pending=0):
    dates, originals = set(), set()
    for a in audits:
        checked(a)
        if a['record_sha256'] in originals or a['game_date'] in dates:
            raise ValueError('duplicate original/date in pool summary')
        originals.add(a['record_sha256']); dates.add(a['game_date'])
        if a['mode'] != 'prospective':
            raise ValueError('reconstruction is not prospective pool evidence')
    complete = [a for a in audits if not a['provisional']]
    provisional = [a for a in audits if a['provisional']]

    def cohort(rows):
        gaps = [a['pool_gap'] for a in rows if a['pool_gap'] is not None]
        return {'dates': len(rows), 'resolved_benchmarks': len(gaps),
                'positive_pool_gap_dates': sum(g > 0 for g in gaps),
                'mean_pool_gap': sum(gaps) / len(gaps) if gaps else None,
                'max_pool_gap': max(gaps) if gaps else None}

    methods = {}
    for method in sorted({key for a in audits for key in a['portfolios']}):
        rows = [a['portfolios'][method] for a in complete if method in a['portfolios'] and a['portfolios'][method]['best_entry'] is not None]
        methods[method] = {'complete_dates': len(rows),
            'mean_selection_gap': sum(r['selection_gap'] for r in rows) / len(rows) if rows else None,
            'mean_total_gap': sum(r['total_gap'] for r in rows) / len(rows) if rows else None,
            'mean_fractional_top_three': sum(r['fractional_top_three'] for r in rows) / len(rows) if rows else None}
    return signed({'version': 'nba-trio-pool-summary-v1', 'protocol_sha256': MATH_SHA,
        'base_model_sha256': MODEL_SHA, 'audited_originals': len(audits), 'pending_latest_audits': pending,
        'complete': cohort(complete), 'provisional': cohort(provisional), 'methods': methods,
        'unresolved': dict(sum((Counter(a['full_pool']['unresolved']) for a in audits), Counter())),
        'dates': [{'date': a['game_date'], 'provisional': a['provisional'], 'pool_gap': a['pool_gap'],
                   'full_pool_verified': a['full_pool']['verified'], 'full_pool_eligible': a['full_pool']['eligible']}
                  for a in sorted(audits, key=lambda a: a['game_date'])],
        'decision': 'Descriptive monitoring only. Keep the registered top-12 policy fixed. Any expansion requires a new preregistration.'})


def report(root):
    originals = {}
    for p in (root / 'trio-math-index').glob('*.json'):
        i = read(p)
        if i['protocol_sha256'] != MATH_SHA:
            continue
        r = checked(read(i['record_path']))
        if r['sha256'] != i['record_sha256'] or r['protocol_sha256'] != MATH_SHA or r['base_model_sha256'] != MODEL_SHA:
            raise ValueError('indexed original identity mismatch')
        originals[r['sha256']] = r
    latest = {}
    for p in (root / 'trio-math-grades').glob('*.json'):
        g = checked(read(p))
        if g['record_sha256'] not in originals:
            continue
        r = originals[g['record_sha256']]
        if any(g[k] != r[k] for k in ['protocol_sha256', 'model_sha256', 'source_snapshot_sha256', 'game_date', 'mode']):
            raise ValueError('grade differs from indexed original')
        old = latest.get(r['sha256'])
        if old is None or (g['graded_at'], g['sha256']) > (old['graded_at'], old['sha256']):
            latest[r['sha256']] = g
    audits = {}
    for p in (root / 'trio-math-grades' / 'pool-audits').glob('*.json'):
        a = checked(read(p)); g = latest.get(a['record_sha256'])
        if g and a['math_grade_sha256'] == g['sha256']:
            if a['game_date'] != g['game_date']:
                raise ValueError('audit date mismatch')
            audits[a['record_sha256']] = a
    return summarize(list(audits.values()), len(originals) - len(audits))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args()
    write_new(a.output, report(a.root))


if __name__ == '__main__':
    main()
