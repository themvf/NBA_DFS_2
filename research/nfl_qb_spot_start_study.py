"""`nfl-qb-spot-start-v1` runner (docs/nfl-qb-spot-start-study.md).

    python -m research.nfl_qb_spot_start_study run --seasons 2020-2025 --output <dev.jsonl> [--grid]
    python -m research.nfl_qb_spot_start_study fit --development <dev.jsonl> --output <fit.json>
    python -m research.nfl_qb_spot_start_study grade --records <jsonl> --fit <fit.json> --output <report.json>

v3 (eligibility test only; constants unchanged):

    python -m research.nfl_qb_spot_start_study run-v3 --seasons 2020-2025 --output <dev-v3.jsonl>
    python -m research.nfl_qb_spot_start_study fit-v3 --development <dev-v3.jsonl> --output <fit-v3.json>
    python -m research.nfl_qb_spot_start_study grade-v3 --records <test-v3.jsonl> --fit <fit-v3.json> --output <report.json>

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


# --- nfl-qb-spot-start-v3 (docs/nfl-qb-spot-start-study.md, "v3") -------------------------
#
# v3 changes only who receives the construction: an expected starter is eligible
# when v2 applies the construction to him, OR when fewer than K of his previous
# min(6, n) games had 15+ attempts. Every game records the current model, v2 as
# production runs it, and the construction with the thin-history gate forced
# open, so v3 at every K in the grid is derived from one record per game.

STUDY_V3 = 'nfl-qb-spot-start-v3'
BOOT_SEED_V3 = 20261018
K_GRID = (3, 4, 5)
STARTER_ATTEMPTS = 15.
GAP_MIN_TEST_GAMES = 60
ATTEMPTS_BIAS_LIMIT = 5.0
FORCED = {**OVERLAY, 'trailing_limit': float('inf')}


def events_v3(rows, seasons):
    """Every qualifying team-game (no established sampling), with the starter-game count."""
    qb = [r for r in rows if r.position == 'QB']
    by_game, history = defaultdict(list), defaultdict(list)
    for r in qb:
        by_game[r.season, r.week, r.team].append(r)
        history[r.player_id].append(r)
    out = []
    for (season, week, team), lines in sorted(by_game.items()):
        if season not in seasons:
            continue
        leader = max(lines, key=lambda r: float(r.stats.get('attempts') or 0))
        if float(leader.stats.get('attempts') or 0) < 15:
            continue
        earlier = [r for r in history[leader.player_id] if (r.season, r.week) < (season, week)][-TRAIL:]
        attempts = [float(r.stats.get('attempts') or 0) for r in earlier]
        out.append({'target': leader, 'window': len(earlier),
                    'trailing_attempts': float(np.mean(attempts)) if attempts else 0.,
                    'starter_games': sum(a >= STARTER_ATTEMPTS for a in attempts)})
    return out


def run_v3(seasons, output, limit=None, log=print):
    rows = load_rows(range(min(seasons) - 2, max(seasons) + 1))
    done = set()
    if output.exists():
        done = {json.loads(l)['key'] for l in output.read_text().splitlines() if l.strip()}
    base = {**MODEL_CONFIG, 'draws': DRAWS}
    n = 0
    with output.open('a') as out:
        for e in events_v3(rows, set(seasons)):
            t = e['target']
            key = f'{t.season}_{t.week}_{t.team}_{t.player_gsis_id}'
            if key in done:
                continue
            if limit and n >= limit:
                break
            prior = [r for r in rows if (r.season, r.week) < (t.season, t.week)]
            prior_qb = [r for r in prior if r.position == 'QB']
            ident = dict(player_id=t.player_id, player_gsis_id=t.player_gsis_id, player_name=t.player_name)
            current = project_player(**ident, position='QB', historical_rows=old_model_rows(t, prior_qb, base),
                                     cutoff_season=t.season, cutoff_week=t.week, context=ProjectionContext(), seed=SEED, config=base)
            volume = team_pass_attempts(prior, t.team, OVERLAY)
            starter = dict(**ident, historical_rows=prior_qb, cutoff_season=t.season, cutoff_week=t.week,
                           context=ProjectionContext(), team_pass_attempts=volume, seed=SEED, config=base)
            v2 = project_expected_starter(**starter, overlay=OVERLAY)
            forced = project_expected_starter(**starter, overlay=FORCED)
            # The gate is the only difference between the two calls, so wherever v2
            # applies, the forced construction must be the same projection.
            if v2 is not None and (forced is None or forced.model_proj_fpts != v2.model_proj_fpts):
                raise AssertionError(f'forced construction differs from v2 for {key}')
            record = {'study': STUDY_V3, 'key': key, 'season': t.season, 'week': t.week, 'team': t.team, 'player': t.player_name,
                      'window': e['window'], 'trailing_attempts': e['trailing_attempts'], 'starter_games': e['starter_games'],
                      'team_pass_attempts': volume, 'v2_applies': v2 is not None,
                      'actual': {'points': t.dk_points, 'attempts': float(t.stats.get('attempts') or 0),
                                 'pass_yds': float(t.stats.get('passing_yards') or 0)},
                      'current': summarize(current, t),
                      'construction': summarize(forced, t) if forced is not None else None}
            out.write(json.dumps(record) + '\n')
            out.flush()
            n += 1
            log(json.dumps({'done': key, 'starter_games': e['starter_games'], 'v2_applies': record['v2_applies']}), flush=True)


def v3_eligible(record, k):
    return record['construction'] is not None and (record['v2_applies'] or record['window'] < 2 or record['starter_games'] < k)


def projection(record, arm, k=None):
    """`current`, `v2` (as production runs it) or `v3` at threshold k."""
    if arm == 'current':
        return record['current']
    if arm == 'v2':
        return record['construction'] if record['v2_applies'] else record['current']
    return record['construction'] if v3_eligible(record, k) else projection(record, 'v2')


def gap(records, k):
    """Games v3 newly covers at k: eligible under v3, not under v2."""
    return [r for r in records if v3_eligible(r, k) and not r['v2_applies']]


def paired_v3(records, new, old, field='points', seed=BOOT_SEED_V3):
    """Mean |error| change new minus old, bootstrap resampling weeks (2,000)."""
    key = 'proj_points' if field == 'points' else 'proj_pass_yds'
    err_new = np.array([abs(new(r)[key] - r['actual'][field]) for r in records])
    err_old = np.array([abs(old(r)[key] - r['actual'][field]) for r in records])
    diff = err_new - err_old
    weeks = sorted({(r['season'], r['week']) for r in records})
    idx = {w: i for i, w in enumerate(weeks)}
    g = np.array([idx[r['season'], r['week']] for r in records])
    sums, counts = np.bincount(g, diff, len(weeks)), np.bincount(g, None, len(weeks))
    rng = np.random.default_rng(seed)
    boot = [sums[s].sum() / counts[s].sum() for s in (rng.integers(0, len(weeks), len(weeks)) for _ in range(2000))]
    return {'n': len(records), 'weeks': len(weeks), 'old_mae': float(err_old.mean()), 'new_mae': float(err_new.mean()),
            'mean_change': float(diff.mean()), 'ci95': np.quantile(boot, [.025, .975]).tolist()}


def bias_v3(records, arm, k=None):
    return {f: float(np.mean([projection(r, arm, k)[f'proj_{f}'] - r['actual'][f] for r in records]))
            for f in ('attempts', 'pass_yds', 'points')} if records else None


def cohort_report(records, k):
    rs = gap(records, k)
    if not rs:
        return {'n': 0}
    v3, v2 = (lambda r: projection(r, 'v3', k)), (lambda r: projection(r, 'v2'))
    return {'n': len(rs), 'points': paired_v3(rs, v3, v2), 'pass_yds': paired_v3(rs, v3, v2, 'pass_yds'),
            'bias': {'v2': bias_v3(rs, 'v2'), 'v3': bias_v3(rs, 'v3', k)}}


def fit_v3(development):
    records = [r for r in load(development) if r.get('study') == STUDY_V3]
    def mae(arm, k=None):
        return float(np.mean([abs(projection(r, arm, k)['proj_points'] - r['actual']['points']) for r in records]))
    by_k = {k: mae('v3', k) for k in K_GRID}
    chosen = min(K_GRID, key=lambda k: (round(by_k[k], 9), k))
    return {'study': STUDY_V3, 'window': 'development', 'records': len(records),
            'seasons': sorted({r['season'] for r in records}),
            'objective': 'DraftKings-points MAE over every qualifying game; ties to the smallest K',
            'population_mae': {'current': mae('current'), 'v2': mae('v2'), **{f'v3_k{k}': v for k, v in by_k.items()}},
            'chosen_k': chosen,
            'gap_cohort_by_k': {f'k{k}': cohort_report(records, k) for k in K_GRID},
            'construction_unavailable': sum(r['construction'] is None for r in records)}


def grade_v3(records, k):
    """The registered gates at the fitted k. Run once, on 2016-2019."""
    report = cohort_report(records, k)
    n, ci, att = report.get('n', 0), report.get('points', {}).get('ci95'), (report.get('bias') or {}).get('v3') or {}
    gates = {'sample_floor_met': n >= GAP_MIN_TEST_GAMES,
             'gap_ci_upper_below_zero': bool(ci and ci[1] < 0),
             'attempts_bias_within_5': bool(att and abs(att['attempts']) <= ATTEMPTS_BIAS_LIMIT)}
    verdict = 'insufficient' if not gates['sample_floor_met'] else ('pass' if all(gates.values()) else 'fail')
    return {'study': STUDY_V3, 'k': k, 'gap': report, 'gates': gates, 'verdict': verdict}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    r3 = sub.add_parser('run-v3')
    r3.add_argument('--seasons', required=True)
    r3.add_argument('--output', type=Path, required=True)
    r3.add_argument('--limit', type=int)
    f3 = sub.add_parser('fit-v3')
    f3.add_argument('--development', type=Path, required=True)
    f3.add_argument('--output', type=Path, required=True)
    g3 = sub.add_parser('grade-v3')
    g3.add_argument('--records', type=Path, required=True)
    g3.add_argument('--fit', type=Path, required=True)
    g3.add_argument('--output', type=Path, required=True)
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
    if args.command == 'run-v3':
        first, last = (int(x) for x in args.seasons.split('-'))
        args.output.parent.mkdir(parents=True, exist_ok=True)
        run_v3(list(range(first, last + 1)), args.output, args.limit)
        return
    if args.command in ('fit-v3', 'grade-v3'):
        if args.output.exists():
            raise SystemExit(f'Refusing to overwrite {args.output}')
        if args.command == 'fit-v3':
            result = fit_v3(args.development)
        else:
            fitted = json.loads(args.fit.read_text())
            result = {**grade_v3(load(args.records), int(fitted['chosen_k'])), 'fit': fitted}
        args.output.write_text(json.dumps(result, indent=1))
        print(json.dumps({k: v for k, v in result.items() if k != 'fit'}, indent=1)[:4000])
        return
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
