"""Last-season usage prior for game-leader roles (`nfl-leaders-volume-prior-v1`).

Pre-registered in docs/nfl-game-leaders-volume-prior-study.md. Compares the
roles-v1 projected targets/carries (C0) with a version that blends each player's
previous-season per-game rate into his current-season rate (V1).

Projected volume = team budget x share, rebuilt from the same inputs the model
uses: the team's current-season window (up to six games), the three-game
half-life team budget shrunk with three league pseudo-games, roles-v1 rates
(games-played rate shrunk by one game toward the all-games rate) and its newcomer
reserve. The opponent volume effect is omitted for every variant alike.

The active list is the players with a box score in the game being projected.

    python -m research.nfl_game_leaders_volume_prior tune    [--snapshot PATH]
    python -m research.nfl_game_leaders_volume_prior holdout --k-targets K --rho-targets R --k-carries K --rho-carries R
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

import numpy as np

VERSION = 'nfl-leaders-volume-prior-v1'
SNAPSHOT = Path('artifacts/nfl-game-leaders/final-repaired-capture-20261008.json.gz')
ACTIONS = ('targets', 'carries')
WINDOW = 6
HALF_LIFE = 3.
TEAM_PRIOR_GAMES = 3.
ROLES_SHRINK_GAMES = 1.
RESERVE = {'targets': .04, 'carries': .05}   # model/nfl_game_leaders_roles.RESERVE
MIN_PRIOR_GAMES = 4
K_GRID = (1, 2, 3, 4, 6, 8, 12, 16)
RHO_GRID = (.5, .6, .7, .8, .9, 1.)
TUNE = {'season': 2024, 'weeks': (2, 18)}
HOLDOUT = {'season': 2025, 'weeks': (9, 18)}
BOOT = 2000
SEED = 20261011


def roles_rate(n_played, total, window_games):
    """roles-v1 usage_rates: games-played rate shrunk by one game toward the all-games rate."""
    if n_played == 0:
        return 0.
    on, overall = total / n_played, total / window_games
    return (n_played * on + ROLES_SHRINK_GAMES * overall) / (n_played + ROLES_SHRINK_GAMES)


def blended_rate(r_cur, n_played, r_prev, k, rho):
    """V1: previous-season rate as k pseudo-games; a player with no current games gets rho * r_prev."""
    if r_prev is None:
        return r_cur
    if n_played == 0:
        return rho * r_prev
    return (n_played * r_cur + k * r_prev) / (n_played + k)


def shares(rates, reserve):
    total = sum(rates.values())
    return {i: (v / total * (1 - reserve) if total > 0 else 0.) for i, v in rates.items()}


def team_budget(window_totals, league_mean):
    w = np.array([.5 ** ((len(window_totals) - 1 - j) / HALF_LIFE) for j in range(len(window_totals))])
    return (np.dot(window_totals, w) + TEAM_PRIOR_GAMES * league_mean) / (w.sum() + TEAM_PRIOR_GAMES)


def load(snapshot_path=SNAPSHOT):
    from research.nfl_game_leaders import read
    from model.nfl_longest_touchdown import timestamp
    snap = read(Path(snapshot_path))
    verified = set(snap.get('box_verified_game_ids', []))
    games = {g['game_id']: g for g in snap['games'] if g.get('completed') and g['game_id'] in verified}
    order = sorted(games, key=lambda g: (timestamp(games[g]['kickoff']), g))
    boxes = defaultdict(lambda: defaultdict(dict))
    for b in snap['boxes']:
        if b['game_id'] in games:
            boxes[b['game_id']][b['team']][b['identity']] = b
    return games, order, boxes


def build_records(games, order, boxes, season, weeks):
    """One record per (player-game, action) in the evaluation window with everything the variants need."""
    rank = {g: i for i, g in enumerate(order)}
    team_games = defaultdict(list)
    for g in order:
        for t in boxes[g]:
            team_games[t].append(g)
    prev = defaultdict(lambda: [0, 0, 0])   # (identity, season) -> [games played, targets, carries], any team
    for g in order:
        for bx in boxes[g].values():
            for i, b in bx.items():
                if b['targets'] + b['carries'] > 0:
                    r = prev[(i, games[g]['season'])]
                    r[0] += 1; r[1] += b['targets']; r[2] += b['carries']
    # league mean of team totals from strictly earlier games
    running = {a: [0., 0] for a in ACTIONS}
    league_before = {}
    for g in order:
        league_before[g] = {a: (running[a][0] / running[a][1] if running[a][1] else np.nan) for a in ACTIONS}
        for bx in boxes[g].values():
            for a in ACTIONS:
                running[a][0] += sum(b[a] for b in bx.values()); running[a][1] += 1
    out = []
    for g in order:
        gm = games[g]
        if gm['season'] != season or not (weeks[0] <= gm['week'] <= weeks[1]):
            continue
        for t, bx in boxes[g].items():
            window = [h for h in team_games[t] if rank[h] < rank[g] and games[h]['season'] == season][-WINDOW:]
            if not window:
                continue
            for a in ACTIONS:
                totals = np.array([sum(b[a] for b in boxes[h][t].values()) for h in window], float)
                budget = float(team_budget(totals, league_before[g][a]))
                played = defaultdict(lambda: [0, 0.])
                for h in window:
                    for i, b in boxes[h][t].items():
                        if b['carries'] + b['targets'] > 0:
                            played[i][0] += 1; played[i][1] += b[a]
                players = []
                for i, b in bx.items():
                    n, tot = played[i] if i in played else (0, 0.)
                    p = prev.get((i, season - 1))
                    r_prev = (p[1 if a == 'targets' else 2] / p[0]) if p and p[0] >= MIN_PRIOR_GAMES else None
                    players.append({'ident': i, 'actual': b[a], 'n': n, 'r_cur': roles_rate(n, tot, len(window)),
                                    'r_prev': r_prev})
                out.append({'game': g, 'team': t, 'week': gm['week'], 'action': a, 'budget': budget, 'players': players})
    return out


def project(team_rows, action, k=None, rho=None):
    """List of (game, actual, projected, n, r_prev, r_cur) for every player; k=None gives C0."""
    rows = []
    for tr in team_rows:
        if tr['action'] != action:
            continue
        if k is None:
            rates = {p['ident']: p['r_cur'] for p in tr['players']}
        else:
            rates = {p['ident']: blended_rate(p['r_cur'], p['n'], p['r_prev'], k, rho) for p in tr['players']}
        sh = shares(rates, RESERVE[action])
        for p in tr['players']:
            rows.append((tr['game'], tr['week'], p['actual'], tr['budget'] * sh[p['ident']], p['n'], p['r_prev'], p['r_cur']))
    return rows


def mae(rows):
    return float(np.mean([abs(r[2] - r[3]) for r in rows]))


def paired_boot(rows_a, rows_b, seed=SEED, b=BOOT):
    """MAE(a) - MAE(b) with a 95% interval resampling whole games; rows aligned one to one."""
    by = defaultdict(list)
    for ra, rb in zip(rows_a, rows_b):
        by[ra[0]].append(abs(ra[2] - ra[3]) - abs(rb[2] - rb[3]))
    keys = list(by)
    sums = np.array([sum(by[key]) for key in keys]); counts = np.array([len(by[key]) for key in keys])
    rng = np.random.default_rng(seed)
    draws = []
    for _ in range(b):
        idx = rng.integers(len(keys), size=len(keys))
        draws.append(sums[idx].sum() / counts[idx].sum())
    return float(sums.sum() / counts.sum()), [float(x) for x in np.percentile(draws, [2.5, 97.5])]


def gap_slope(rows):
    xy = [(r[5] - r[6], r[2] - r[3]) for r in rows if r[5] is not None and r[4] >= 1]
    x, y = np.array(xy).T
    return float(np.polyfit(x, y, 1)[0])


def tune(team_rows):
    result = {}
    for a in ACTIONS:
        c0 = mae(project(team_rows, a))
        grid = sorted(((mae(project(team_rows, a, k, rho)), k, rho) for k in K_GRID for rho in RHO_GRID),
                      key=lambda x: (round(x[0], 9), x[1]))
        best = grid[0]
        result[a] = {'c0_mae': c0, 'best': {'k': best[1], 'rho': best[2], 'mae': best[0]},
                     'grid': [{'k': k, 'rho': rho, 'mae': m} for m, k, rho in grid]}
    return result


def holdout(team_rows, params):
    report = {}
    for a in ACTIONS:
        k, rho = params[a]
        c0, v1 = project(team_rows, a), project(team_rows, a, k, rho)
        diff, ci = paired_boot(v1, c0)
        weeks = {}
        for lo, hi in ((9, 12), (13, 18)):
            sub0 = [r for r in c0 if lo <= r[1] <= hi]; sub1 = [r for r in v1 if lo <= r[1] <= hi]
            weeks[f'{lo}-{hi}'] = {'c0': mae(sub0), 'v1': mae(sub1), 'n': len(sub0)}
        ret0 = [r for r in c0 if r[4] == 0 and r[5] is not None]; ret1 = [r for r in v1 if r[4] == 0 and r[5] is not None]
        report[a] = {'k': k, 'rho': rho, 'n_player_games': len(c0), 'n_games': len({r[0] for r in c0}),
                     'mae_c0': mae(c0), 'mae_v1': mae(v1), 'diff_v1_minus_c0': diff, 'ci95': ci,
                     'pass': ci[1] < 0, 'gap_slope_c0': gap_slope(c0), 'gap_slope_v1': gap_slope(v1), 'by_weeks': weeks,
                     'returners': {'n': len(ret0), 'actual': float(np.mean([r[2] for r in ret0])) if ret0 else None,
                                   'c0': float(np.mean([r[3] for r in ret0])) if ret0 else None,
                                   'v1': float(np.mean([r[3] for r in ret1])) if ret1 else None}}
    report['verdict'] = ('PASS' if report['targets']['pass'] else 'FAIL') + (
        '; carries adopted' if report['carries']['pass'] else '; carries keep roles-v1')
    return report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('command', choices=('tune', 'holdout'))
    ap.add_argument('--snapshot', default=str(SNAPSHOT))
    for a in ACTIONS:
        ap.add_argument(f'--k-{a}', type=float); ap.add_argument(f'--rho-{a}', type=float)
    ap.add_argument('--output')
    args = ap.parse_args()
    games, order, boxes = load(args.snapshot)
    if args.command == 'tune':
        result = {'version': VERSION, 'window': TUNE, **tune(build_records(games, order, boxes, TUNE['season'], TUNE['weeks']))}
    else:
        params = {a: (getattr(args, f'k_{a}'), getattr(args, f'rho_{a}')) for a in ACTIONS}
        if any(v is None for p in params.values() for v in p):
            raise SystemExit('holdout needs the tuned K and rho for both actions (Amendment 1)')
        result = {'version': VERSION, 'window': HOLDOUT,
                  **holdout(build_records(games, order, boxes, HOLDOUT['season'], HOLDOUT['weeks']), params)}
    text = json.dumps(result, indent=1)
    if args.output:
        Path(args.output).write_text(text)
    for a in ACTIONS:
        r = result[a]
        print(a, json.dumps({k: v for k, v in r.items() if k != 'grid'}))
    if 'verdict' in result:
        print('verdict:', result['verdict'])


if __name__ == '__main__':
    main()
