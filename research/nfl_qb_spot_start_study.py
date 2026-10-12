"""`nfl-qb-spot-start-v1` runner (docs/nfl-qb-spot-start-study.md).

    python -m research.nfl_qb_spot_start_study run --seasons 2020-2025 --output <dev.jsonl> [--grid]
    python -m research.nfl_qb_spot_start_study fit --development <dev.jsonl> --output <fit.json>
    python -m research.nfl_qb_spot_start_study grade --records <jsonl> --fit <fit.json> --output <report.json>

`run` walks every qualifying quarterback game, projects it with the current
model (v5, no transfer) and with the expected-starter construction for every
grid point (or the fitted one), and appends one record per game. Rows are
read from the cached nflverse weekly files (data/nflverse_cache).
"""
from __future__ import annotations

import argparse
import io
import itertools
import json
from collections import defaultdict
from pathlib import Path

import numpy as np

from model.nfl_dfs_historical import (MODEL_CONFIG, HistoricalWeek, ProjectionContext, artifact_digest,
                                      draftkings_points, project_player)
from model.nfl_qb_starter import CONFIG as OVERLAY, project_expected_starter, team_pass_attempts

STUDY = 'nfl-qb-spot-start-v1'
CACHE = Path('data/nflverse_cache')
GRID = {'qb_team_volume_half_life': (2., 4., 8.), 'qb_team_volume_prior_games': (2., 4., 8.),
        'qb_spot_start_prior_games': (4, 8, 16), 'qb_min_line_attempts': (10., 15.)}
DRAWS = 400
SEED = 20260902
BOOT_SEED = 20261012
TRAIL, SPOT_LIMIT, EST_SAMPLE = 6, 15., 4
CANON = {'LA': 'LAR', 'WAS': 'WSH', 'OAK': 'LV', 'SD': 'LAC', 'STL': 'LAR'}


def load_rows(seasons):
    import pandas as pd
    rows = []
    for season in seasons:
        frame = pd.read_csv(CACHE / f'stats_player_week_{season}.csv', low_memory=False)
        frame = frame[(frame.season_type == 'REG') & frame.position.isin(('QB', 'RB', 'WR', 'TE'))]
        for r in frame.to_dict('records'):
            pid = str(r.get('player_id') or '').strip()
            if not pid:
                continue
            rows.append(HistoricalWeek(player_id=int(artifact_digest({'gsis_id': pid})[:15], 16), player_gsis_id=pid,
                                       player_name=str(r.get('player_display_name') or pid), position=str(r['position']),
                                       season=int(r['season']), week=int(r['week']),
                                       team=CANON.get(str(r.get('team') or ''), str(r.get('team') or '')) or None,
                                       opponent=CANON.get(str(r.get('opponent_team') or ''), str(r.get('opponent_team') or '')) or None,
                                       stats={k: (0.0 if v != v else v) if isinstance(v, float) else v for k, v in r.items()}))
    return sorted(rows, key=lambda r: (r.season, r.week, r.player_id))


def events(rows, seasons):
    """Team-games with a 15+ attempt leader in `seasons`, classified spot/established."""
    qb = [r for r in rows if r.position == 'QB']
    by_game = defaultdict(list)
    for r in qb:
        by_game[r.season, r.week, r.team].append(r)
    history = defaultdict(list)
    for r in qb:
        history[r.player_id].append(r)
    out = []
    for (season, week, team), lines in sorted(by_game.items()):
        if season not in seasons:
            continue
        leader = max(lines, key=lambda r: float(r.stats.get('attempts') or 0))
        if float(leader.stats.get('attempts') or 0) < 15:
            continue
        earlier = [r for r in history[leader.player_id] if (r.season, r.week) < (season, week)][-TRAIL:]
        trailing = float(np.mean([float(r.stats.get('attempts') or 0) for r in earlier])) if earlier else 0.
        cohort = 'spot' if len(earlier) < 2 or trailing < SPOT_LIMIT else 'established'
        out.append({'target': leader, 'cohort': cohort, 'trailing_attempts': trailing, 'prior_games': len(earlier)})
    est = [e for e in out if e['cohort'] == 'established']
    keep = set(id(e) for e in est[::EST_SAMPLE])
    return [e for e in out if e['cohort'] == 'spot' or id(e) in keep]


def old_model_rows(target, prior_qb, config):
    """Own lines plus DK-points-similar peers, as the production path selects them."""
    own = [r for r in prior_qb if r.player_id == target.player_id][-int(config['max_player_games']):]
    others = [r for r in prior_qb if r.player_id != target.player_id]
    if not own:
        return others[-int(config['max_prior_games']):]
    center = float(np.mean([r.dk_points for r in own]))
    by_peer = defaultdict(list)
    for r in others:
        by_peer[r.player_id].append(r)
    ordered = sorted(by_peer, key=lambda pid: (abs(float(np.mean([r.dk_points for r in by_peer[pid]])) - center), pid))
    selected = []
    for pid in ordered:
        selected.extend(sorted(by_peer[pid], key=lambda r: r.chronological_key, reverse=True))
        if len(selected) >= int(config['max_prior_games']):
            break
    return own + selected[:int(config['max_prior_games'])]


def overlay_keys(cfg):
    """Grid / fit keys carry the historical 'qb_' prefix; the overlay's do not."""
    return {k[3:] if k.startswith('qb_') else k: v for k, v in cfg.items()}


def summarize(projection, target):
    actual = target.stats
    return {'proj_points': projection.model_proj_fpts, 'proj_attempts': projection.stat_means.get('attempts'),
            'proj_pass_yds': projection.stat_means.get('passing_yards'), 'status': projection.projection_status,
            'history_games': projection.history_games}


def run(seasons, output, grid=False, fitted=None, limit=None, log=print):
    rows = load_rows(range(min(seasons) - 2, max(seasons) + 1))
    todo = events(rows, set(seasons))
    done = set()
    if output.exists():
        done = {json.loads(l)['key'] for l in output.read_text().splitlines() if l.strip()}
    configs = {}
    if grid:
        for combo in itertools.product(*GRID.values()):
            configs['|'.join(f'{k}={v}' for k, v in zip(GRID, combo))] = dict(zip(GRID, combo))
    else:
        configs['fitted'] = fitted or {}
    base = {**MODEL_CONFIG, 'draws': DRAWS}
    if grid:
        # The grid is chosen on the spot cohort only (registration); the
        # established cohort is graded with the fitted point afterwards.
        todo = [e for e in todo if e['cohort'] == 'spot']
    with output.open('a') as out:
        n = 0
        for e in todo:
            t = e['target']
            key = f'{t.season}_{t.week}_{t.team}_{t.player_gsis_id}'
            if key in done:
                continue
            if limit and n >= limit:
                break
            prior = [r for r in rows if (r.season, r.week) < (t.season, t.week)]
            prior_qb = [r for r in prior if r.position == 'QB']
            actual = {'points': t.dk_points, 'attempts': float(t.stats.get('attempts') or 0), 'pass_yds': float(t.stats.get('passing_yards') or 0)}
            record = {'study': STUDY, 'key': key, 'season': t.season, 'week': t.week, 'team': t.team, 'player': t.player_name,
                      'cohort': e['cohort'], 'trailing_attempts': e['trailing_attempts'], 'actual': actual}
            current = project_player(player_id=t.player_id, player_gsis_id=t.player_gsis_id, player_name=t.player_name, position='QB',
                                     historical_rows=old_model_rows(t, prior_qb, base), cutoff_season=t.season, cutoff_week=t.week,
                                     context=ProjectionContext(), seed=SEED, config=base)
            record['current'] = summarize(current, t)
            record['variants'] = {}
            for name, cfg in configs.items():
                overlay = {**OVERLAY, **overlay_keys(cfg)}
                volume = team_pass_attempts(prior, t.team, overlay)
                # The starter is taken as given (named); the overlay's own thin-history
                # gate decides whether the construction applies, exactly as in production.
                new = project_expected_starter(player_id=t.player_id, player_gsis_id=t.player_gsis_id, player_name=t.player_name,
                                               historical_rows=prior_qb, cutoff_season=t.season, cutoff_week=t.week,
                                               context=ProjectionContext(), team_pass_attempts=volume, seed=SEED, config=base,
                                               overlay=overlay)
                record['variants'][name] = ({**summarize(new, t), 'team_pass_attempts': volume, 'construction_applied': True} if new is not None
                                            else {**record['current'], 'team_pass_attempts': volume, 'construction_applied': False})
            out.write(json.dumps(record) + '\n')
            out.flush()
            n += 1
            log(json.dumps({'done': key, 'cohort': e['cohort']}), flush=True)


def load(path):
    return [json.loads(l) for l in Path(path).read_text().splitlines() if l.strip()]


def paired(records, variant, field='points', seed=BOOT_SEED):
    """Mean |error| change, variant minus current, bootstrap resampling weeks."""
    err_new = np.array([abs(r['variants'][variant]['proj_' + ('points' if field == 'points' else 'pass_yds')] - r['actual'][field]) for r in records])
    err_old = np.array([abs(r['current']['proj_' + ('points' if field == 'points' else 'pass_yds')] - r['actual'][field]) for r in records])
    diff = err_new - err_old
    weeks = sorted({(r['season'], r['week']) for r in records})
    idx = {w: i for i, w in enumerate(weeks)}
    g = np.array([idx[r['season'], r['week']] for r in records])
    sums, counts = np.bincount(g, diff, len(weeks)), np.bincount(g, None, len(weeks))
    rng = np.random.default_rng(seed)
    boot = [sums[s].sum() / counts[s].sum() for s in (rng.integers(0, len(weeks), len(weeks)) for _ in range(2000))]
    return {'n': len(records), 'weeks': len(weeks), 'current_mae': float(err_old.mean()), 'variant_mae': float(err_new.mean()),
            'mean_change': float(diff.mean()), 'ci95': np.quantile(boot, [.025, .975]).tolist()}


def bias(records, variant):
    return {k: {'current': float(np.mean([r['current'][f'proj_{k}'] - r['actual'][k] for r in records])),
                'variant': float(np.mean([r['variants'][variant][f'proj_{k}'] - r['actual'][k] for r in records]))}
            for k in ('attempts', 'pass_yds', 'points')}


def fit(development):
    records = [r for r in load(development) if r['cohort'] == 'spot']
    names = list(records[0]['variants'])
    scores = {name: paired(records, name)['variant_mae'] for name in names}
    best = min(scores, key=scores.get)
    config = {k: (int(v) if k == 'qb_spot_start_prior_games' else float(v)) for k, v in (kv.split('=') for kv in best.split('|'))}
    return {'study': STUDY, 'spot_records': len(records), 'chosen': best, 'config': config,
            'spot_points_mae_by_grid_point': dict(sorted(scores.items(), key=lambda kv: kv[1])[:10]),
            'current_spot_points_mae': paired(records, best)['current_mae']}


def grade(records, variant):
    report = {}
    for cohort in ('spot', 'established'):
        rs = [r for r in records if r['cohort'] == cohort and variant in r['variants']]
        if not rs:
            continue
        report[cohort] = {'points': paired(rs, variant, 'points'), 'pass_yds': paired(rs, variant, 'pass_yds'), 'bias': bias(rs, variant)}
    spot, est = report.get('spot', {}).get('points'), report.get('established', {}).get('points')
    report['gates'] = {'spot_ci_upper_below_zero': bool(spot and spot['ci95'][1] < 0),
                       'established_ci_upper_at_most_0.10': bool(est and est['ci95'][1] <= .10)}
    report['passes'] = all(report['gates'].values())
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    r = sub.add_parser('run')
    r.add_argument('--seasons', required=True)
    r.add_argument('--output', type=Path, required=True)
    r.add_argument('--grid', action='store_true')
    r.add_argument('--fit', type=Path)
    r.add_argument('--limit', type=int)
    f = sub.add_parser('fit')
    f.add_argument('--development', type=Path, required=True)
    f.add_argument('--output', type=Path, required=True)
    g = sub.add_parser('grade')
    g.add_argument('--records', type=Path, required=True)
    g.add_argument('--fit', type=Path, required=True)
    g.add_argument('--output', type=Path, required=True)
    g.add_argument('--variant', default='fitted')
    args = parser.parse_args()
    if args.command == 'run':
        first, last = (int(x) for x in args.seasons.split('-'))
        fitted = json.loads(args.fit.read_text())['config'] if args.fit else None
        args.output.parent.mkdir(parents=True, exist_ok=True)
        run(list(range(first, last + 1)), args.output, args.grid, fitted, args.limit)
        return
    if args.output.exists():
        raise SystemExit(f'Refusing to overwrite {args.output}')
    if args.command == 'fit':
        result = fit(args.development)
    else:
        fitted = json.loads(args.fit.read_text())
        variant = args.variant if args.variant != 'fitted' else ('fitted' if 'fitted' in load(args.records)[0]['variants'] else fitted['chosen'])
        result = {'study': STUDY, 'fit': fitted, 'variant': variant, **grade(load(args.records), variant)}
    args.output.write_text(json.dumps(result, indent=1))
    print(json.dumps({k: v for k, v in result.items() if k != 'fit'}, indent=1)[:3000])


if __name__ == '__main__':
    main()
