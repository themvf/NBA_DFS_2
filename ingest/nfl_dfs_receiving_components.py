"""Compare receiving components with frozen production and role forecasts."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from ingest.nfl_dfs_target_share import read_sources
from ingest.nfl_dfs_volume_benchmark import STUDY, EXPECTED, pair
from model.nfl_dfs_receiving_components import CONFIG, replay
from model.nfl_dfs_target_share import normalize_team

ROOT = Path(__file__).resolve().parents[1]


def key(row):
    return row['season'], row['week'], row['game_id'], row['identity']


def unique_rows(rows):
    result = {}
    for row in rows:
        if key(row) in result:
            raise ValueError('Duplicate comparison identity')
        result[key(row)] = row
    return result


def losses(row, model):
    forecast = row[model]
    actual = row['actual']
    return [abs(actual-forecast['mean']),
            forecast['p90']-forecast['p10']+10*(max(forecast['p10']-actual, 0)+max(actual-forecast['p90'], 0)),
            (row[f'{model}_boom_probability']-(actual >= 25))**2]


def metrics(rows, model):
    if not rows:
        return None
    errors = np.asarray([losses(r, model) for r in rows])
    return dict(zip(['mae', 'interval80', 'brier25'], map(float, errors.mean(axis=0)))) | {
        'below_p10': float(np.mean([r['actual'] < r[model]['p10'] for r in rows])),
        'above_p90': float(np.mean([r['actual'] > r[model]['p90'] for r in rows])),
    }


def paired_week_intervals(rows):
    """Bootstrap whole weeks to avoid treating same-slate WR outcomes as independent."""
    weeks = sorted({r['week'] for r in rows})
    totals = np.asarray([np.sum([np.asarray(losses(r, 'candidate'))-losses(r, 'production') for r in rows if r['week'] == w], axis=0) for w in weeks])
    counts = np.asarray([sum(r['week'] == w for r in rows) for w in weeks])
    rng = np.random.default_rng(20260906)
    sample = rng.integers(0, len(weeks), size=(1000, len(weeks)))
    differences = totals[sample].sum(axis=1)/counts[sample].sum(axis=1)[:, None]
    return {name: {'lower95': float(np.quantile(differences[:, i], .025)), 'upper95': float(np.quantile(differences[:, i], .975))}
            for i, name in enumerate(['mae', 'interval80', 'brier25'])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root', type=Path, required=True)
    args = parser.parse_args()
    history, teams, sources = read_sources(args.source_root)
    manifest = json.loads((args.source_root/'artifacts/ff_v2_historical_context_2020_2025.json').read_text())
    components, primary = {}, {}
    for season in [2023, 2024, 2025]:
        source = manifest['sources'][f'weekly-stats:{season}']
        path = args.source_root/source['cachePath']
        if hashlib.sha256(path.read_bytes()).hexdigest() != source['responseHash']:
            raise ValueError('Component source changed')
        frame = pd.read_parquet(path)
        frame = frame[frame.season_type == 'REG']
        for row in frame.itertuples():
            components[(row.game_id, row.player_id)] = {f: float(getattr(row, f)) for f in ['receptions', 'receiving_yards', 'receiving_tds']}
        for (game, team), rows in frame[(frame.position == 'QB') & (frame.attempts > 0)].groupby(['game_id', 'team']):
            best = rows[rows.attempts == rows.attempts.max()]
            primary[(game, normalize_team(team))] = best.player_id.iloc[0] if len(best) == 1 else None
    history = [{**r, **components[(r['game_id'], r['identity'])]} for r in history]
    teams = [{**r, 'primary_qb': primary.get((r['game_id'], r['team']))} for r in teams]
    predictions, excluded = replay(history, teams)

    raw = STUDY.read_bytes()
    if hashlib.sha256(raw).hexdigest() != EXPECTED:
        raise ValueError('Frozen production study changed')
    saved = json.loads(gzip.decompress(raw))
    paired, unmatched = pair(predictions, saved)
    production = {(r['sample_key'], r['game_id']): r for r in saved if r['model'] == 'baseline' and r['position'] == 'WR'}
    old_report = json.loads((ROOT/'web/src/data/nfl-role-context.json').read_text())
    old_payload = gzip.decompress((ROOT/f"artifacts/nfl_volume_share/role-context-{old_report['predictions_sha256']}.json.gz").read_bytes())
    if hashlib.sha256(old_payload).hexdigest() != old_report['predictions_sha256']:
        raise ValueError('Frozen role comparator changed')
    old_rows = unique_rows(json.loads(old_payload))
    rows = []
    for row in paired:
        old = old_rows.get(key(row))
        if old is None:
            continue
        if abs(old['actual']-row['actual']) > 1e-6 or abs(old['targets_candidate']-row['targets_candidate']) > 1e-9:
            raise ValueError('Comparator actual or target budget mismatch')
        baseline = production[(f"{row['season']}:{row['week']}:{row['identity']}", row['game_id'])]
        rows.append({**row, 'role': old['candidate'], 'role_boom_probability': old['boom_probability'],
                     'production_boom_probability': baseline['boom_probability'], 'candidate_boom_probability': row['boom_probability']})
    seasons = {}
    for year in [2024, 2025]:
        current = [r for r in rows if r['season'] == year]
        if not current:
            raise ValueError('No paired evaluation rows')
        seasons[str(year)] = {'n': len(current), 'models': {m: metrics(current, m) for m in ['production', 'role', 'candidate']},
                             'candidate_minus_production': paired_week_intervals(current),
                             'component_mae': {f: float(np.mean([abs(r['components'][f]-r['component_actuals'][f]) for r in current])) for f in ['receptions', 'receiving_yards', 'receiving_tds']}}
    examples = []
    for team in sorted({r['team'] for r in rows}):
        latest = max((r['season'], r['week']) for r in rows if r['team'] == team)
        group = [r for r in rows if r['team'] == team and (r['season'], r['week']) == latest]
        # Choose using pregame target estimates, not actual winners.
        examples.extend(sorted(group, key=lambda r: (-r['targets_candidate'], r['identity']))[:3])
    passes = all(y['models']['candidate'][m] < y['models']['production'][m] for y in seasons.values() for m in ['mae', 'interval80', 'brier25'])
    result = {
        'version': 'nfl-receiving-components-v1', 'config': CONFIG, 'seasons': seasons, 'examples': examples,
        'excluded': excluded, 'production_pairing': unmatched, 'role_unmatched': len(paired)-len(rows),
        'sources': sources, 'production_sha256': EXPECTED, 'role_comparator_sha256': old_report['predictions_sha256'],
        'passes_screen': passes, 'optimizer_enabled': False,
        'improves_role': all(y['models']['candidate'][m] < y['models']['role'][m] for y in seasons.values() for m in ['mae', 'interval80', 'brier25']),
        'limits': ['Previously inspected 2024/2025; diagnostic, not untouched validation.',
                   'Identical projected targets to the frozen role model; only workload-to-points estimation changes.',
                   'Catch, yardage and TD rates shrink toward WR population evidence strictly before the target week.',
                   'Bonuses and other scoring use prior team-game averages, not bonuses awarded at mean yardage.',
                   'Uncentered prior residuals add a separately reported calibration offset to component points.',
                   'Recorded WR games only; no missing/DNP evaluation outcomes or rookies without sufficient history.',
                   'No current coaching, injury, defense, historical salary or contest-payout adjustment.'],
    }
    recipe = ['model/nfl_dfs_receiving_components.py', 'ingest/nfl_dfs_receiving_components.py', 'model/nfl_dfs_role_context.py', 'model/nfl_dfs_target_share.py', 'ingest/nfl_dfs_target_share.py', 'ingest/nfl_dfs_volume_benchmark.py']
    result['recipe_sha256'] = {f: hashlib.sha256((ROOT/f).read_bytes().replace(b'\r\n', b'\n')).hexdigest() for f in recipe}
    payload = json.dumps(rows, sort_keys=True, allow_nan=False).encode()
    result['predictions_sha256'] = hashlib.sha256(payload).hexdigest()
    archive = ROOT/'artifacts/nfl_volume_share'
    archive.mkdir(parents=True, exist_ok=True)
    (archive/f"receiving-components-{result['predictions_sha256']}.json.gz").write_bytes(gzip.compress(payload, mtime=0))
    (ROOT/'web/src/data/nfl-receiving-components.json').write_text(json.dumps(result, indent=2, allow_nan=False)+'\n', encoding='utf-8')
    print(json.dumps({'seasons': seasons, 'passes_screen': passes}, indent=2))


if __name__ == '__main__':
    main()
