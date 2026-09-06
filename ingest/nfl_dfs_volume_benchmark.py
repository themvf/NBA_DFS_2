"""Pair volume/share forecasts with the frozen production-algorithm replay."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import numpy as np
from ingest.nfl_dfs_target_share import read_sources
from model.nfl_dfs_target_share import replay

ROOT = Path(__file__).resolve().parents[1]
STUDY = ROOT / 'artifacts/nfl_dfs_research_36cbc63d06d706a9/8bab909112d93a5d/predictions.json.gz'
EXPECTED = 'f2ca4d3a7caa8008ca4fc2ca57e4f4aac12bd78ccdcd1f4ceecd1947985f5be8'


def pair(candidate, saved):
    baseline = {}
    for r in saved:
        if r['model'] != 'baseline' or r['position'] != 'WR' or r['season'] not in [2024, 2025]:
            continue
        key = (r['sample_key'], r['game_id'])
        if key in baseline:
            raise ValueError('Duplicate baseline identity')
        baseline[key] = r
    paired, seen = [], set()
    for r in candidate:
        if r['season'] not in [2024, 2025]:
            continue
        key = (f"{r['season']}:{r['week']}:{r['identity']}", r['game_id'])
        if key in seen:
            raise ValueError('Duplicate candidate identity')
        seen.add(key)
        b = baseline.get(key)
        if b is None:
            continue
        if abs(b['actual'] - r['actual']) > 1e-6:
            raise ValueError('Paired actual scoring mismatch')
        if tuple(b['history_cutoff']) >= (r['season'], r['week']):
            raise ValueError('Baseline history leaks target week')
        tier = 'Under 4 targets' if r['targets_baseline'] < 4 else '4–7 targets' if r['targets_baseline'] < 7 else '7+ targets'
        paired.append({**r, 'tier': tier, 'production': {'mean': b['prediction'], 'p10': b['p10'], 'p90': b['p90']}})
    return paired, {'candidate_unmatched': len(seen - baseline.keys()), 'production_unmatched': len(baseline.keys() - seen)}


def measure(rows):
    result = {'n': len(rows)}
    for name in ['production', 'candidate']:
        result[name] = {
            'mae': float(np.mean([abs(r['actual']-r[name]['mean']) for r in rows])),
            'interval80': float(np.mean([r[name]['p90']-r[name]['p10']+10*(max(r[name]['p10']-r['actual'],0)+max(r['actual']-r[name]['p90'],0)) for r in rows])),
            'below_p10': float(np.mean([r['actual'] < r[name]['p10'] for r in rows])),
            'above_p90': float(np.mean([r['actual'] > r[name]['p90'] for r in rows])),
        }
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root', type=Path, required=True)
    args = parser.parse_args()
    raw = STUDY.read_bytes()
    if hashlib.sha256(raw).hexdigest() != EXPECTED:
        raise ValueError('Frozen study digest mismatch')
    history, teams, sources = read_sources(args.source_root)
    forecasts, _ = replay(history, teams)
    rows, unmatched = pair(forecasts, json.loads(gzip.decompress(raw)))
    seasons = {str(year): measure([r for r in rows if r['season'] == year]) for year in [2024, 2025]}
    tiers = {str(year): {tier: measure([r for r in rows if r['season'] == year and r['tier'] == tier]) for tier in ['Under 4 targets', '4–7 targets', '7+ targets']} for year in [2024, 2025]}
    result = {'version': 'nfl-volume-production-comparison-v1', 'seasons': seasons, 'tiers': tiers, 'unmatched': unmatched,
              'study_sha256': EXPECTED, 'sources': sources,
              'recipe_sha256': {name: hashlib.sha256((ROOT / name).read_bytes().replace(b'\r\n', b'\n')).hexdigest() for name in ['model/nfl_dfs_target_share.py', 'ingest/nfl_dfs_target_share.py', 'ingest/nfl_dfs_volume_benchmark.py']},
              'passes_mean_and_interval_screen': all(s['candidate'][m] < s['production'][m] for s in seasons.values() for m in ['mae', 'interval80']),
              'optimizer_enabled': False,
              'limits': ['Production algorithm replay with market inputs disabled; not archived live projections.', '2024 and 2025 were previously inspected; no untouched holdout.', 'Only paired recorded WR games; missing/DNP games excluded.', 'Workload tiers use prior targets, never target-game outcomes.', 'No historical injury adjustment, contest payout test, or joint lineup calibration.']}
    payload = json.dumps(rows, sort_keys=True, allow_nan=False).encode()
    result['paired_predictions_sha256'] = hashlib.sha256(payload).hexdigest()
    archive = ROOT / 'artifacts/nfl_volume_share'
    archive.mkdir(exist_ok=True, parents=True)
    (archive / f"paired-{result['paired_predictions_sha256']}.json.gz").write_bytes(gzip.compress(payload, mtime=0))
    (ROOT / 'web/src/data/nfl-volume-benchmark.json').write_text(json.dumps(result, indent=2, allow_nan=False)+'\n', encoding='utf-8')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
