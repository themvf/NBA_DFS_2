"""`nfl-target-concentration-v1` (docs/nfl-target-concentration-study.md).

    python -m research.nfl_target_concentration --stage development --output <report.json>
    python -m research.nfl_target_concentration --stage test --fit <development-report.json> --output <report.json>

Pure box-score arithmetic on the frozen captures plus nflverse inactives.
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

import numpy as np

STUDY = 'nfl-target-concentration-v1'
CAPTURES = {'development': ('artifacts/nfl-game-leaders/final-repaired-capture-20261008.json.gz', range(2023, 2026)),
            'test': ('artifacts/nfl-game-leaders/calibration-v1/capture-2021-2022.json.gz', range(2021, 2023))}
K_GRID = np.round(np.arange(.5, 2.5001, .05), 2)
LAMBDAS = (8, 17, 34, 68)
WINDOW, K_WINDOW, MIN_PRIOR, MIN_GAMES_H1 = 6, 17, 3, 12
OTHER_FLOOR = .01
SEED = 20261011


def team_games(snapshot, seasons):
    """team -> chronological list of {game_id, season, week, boxes:[non-QB rows]}."""
    from model.nfl_longest_touchdown import timestamp
    games = {g['game_id']: g for g in snapshot['games'] if g['season'] in seasons and g.get('completed')}
    by = defaultdict(list)
    for b in snapshot['boxes']:
        if b['game_id'] in games and b.get('position') != 'QB':
            by[b['game_id'], b['team']].append(b)
    out = defaultdict(list)
    for (gid, team), boxes in by.items():
        g = games[gid]
        out[team].append({'game_id': gid, 'season': g['season'], 'week': g['week'], 'kickoff': timestamp(g['kickoff']), 'boxes': boxes})
    for team in out:
        out[team].sort(key=lambda r: r['kickoff'])
    return out


def hhi(counts):
    total = sum(counts)
    return sum((c / total) ** 2 for c in counts) if total else None


def h1(teams):
    rows = []
    for team, games in teams.items():
        for season in {g['season'] for g in games}:
            gs = [g for g in games if g['season'] == season]
            vals = [(g['week'], hhi([b['targets'] for b in g['boxes'] if b['targets'] > 0])) for g in gs]
            vals = [(w, v) for w, v in vals if v is not None]
            if len(vals) >= MIN_GAMES_H1:
                rows.append({'team': team, 'season': season, 'odd': np.mean([v for w, v in vals if w % 2]),
                             'even': np.mean([v for w, v in vals if not w % 2]), 'all': [v for _, v in vals]})
    odd, even = np.array([r['odd'] for r in rows]), np.array([r['even'] for r in rows])
    rng = np.random.default_rng(SEED)
    boots = [np.corrcoef(odd[i], even[i])[0, 1] for i in rng.integers(0, len(rows), (2000, len(rows)))]
    # One-way ANOVA intraclass correlation: share of game-level HHI variance between team-seasons.
    groups = [np.array(r['all']) for r in rows]
    n_bar = np.mean([len(g) for g in groups])
    grand = np.concatenate(groups).mean()
    msb = sum(len(g) * (g.mean() - grand) ** 2 for g in groups) / (len(groups) - 1)
    msw = sum(((g - g.mean()) ** 2).sum() for g in groups) / (sum(len(g) for g in groups) - len(groups))
    icc = (msb - msw) / (msb + (n_bar - 1) * msw)
    by_team = {(r['team'], r['season']): np.mean(r['all']) for r in rows}
    pairs = [(v, by_team[t, s + 1]) for (t, s), v in by_team.items() if (t, s + 1) in by_team]
    yoy = float(np.corrcoef(*zip(*pairs))[0, 1]) if len(pairs) > 2 else None
    ranked = sorted(((t, s, round(float(v), 4)) for (t, s), v in by_team.items()), key=lambda x: -x[2])
    return {'team_seasons': len(rows), 'split_half_r': float(np.corrcoef(odd, even)[0, 1]),
            'ci95': np.quantile(boots, [.025, .975]).tolist(), 'passes': bool(np.quantile(boots, .025) > 0),
            'icc': float(icc), 'year_over_year_r': yoy, 'year_pairs': len(pairs),
            'league_mean_hhi': float(grand), 'most_concentrated': ranked[:5], 'most_spread': ranked[-5:]}


def records(teams, absent):
    """Per team-game: candidate weights, other share, observed counts (targets and receptions)."""
    out = []
    for team, games in teams.items():
        first_seen = []
        for j, g in enumerate(games):
            prior = games[max(0, j - WINDOW):j]
            seen_before = set()
            for p in games[max(0, j - 2 * WINDOW):max(0, j - WINDOW)]:
                seen_before |= {b['identity'] for b in p['boxes'] if b['targets'] > 0}
            newcomer = []
            for i, p in enumerate(prior):
                earlier = seen_before | {b['identity'] for q in prior[:i] for b in q['boxes'] if b['targets'] > 0}
                tot = sum(b['targets'] for b in p['boxes'])
                if tot and (i > 0 or j - WINDOW > 0):
                    newcomer.append(sum(b['targets'] for b in p['boxes'] if b['identity'] not in earlier) / tot)
            if len(prior) < MIN_PRIOR:
                continue
            played = defaultdict(lambda: [0, 0.])
            for p in prior:
                for b in p['boxes']:
                    if b['carries'] + b['targets'] > 0:
                        played[b['identity']][0] += 1
                        played[b['identity']][1] += b['targets']
            out_now = absent.get((g['season'], g['week']), set())
            cands = {i: (n * (t / n) + 1 * (t / len(prior))) / (n + 1) for i, (n, t) in played.items() if t > 0 and i not in out_now}
            if not cands:
                continue
            other = max(OTHER_FLOOR, float(np.mean(newcomer)) if newcomer else OTHER_FLOOR)
            ids = sorted(cands)
            actual = {b['identity']: b for b in g['boxes']}
            counts = [actual[i]['targets'] if i in actual else 0 for i in ids]
            rec = [actual[i]['receptions'] if i in actual else 0 for i in ids]
            other_t = sum(b['targets'] for b in g['boxes'] if b['identity'] not in cands)
            other_r = sum(b['receptions'] for b in g['boxes'] if b['identity'] not in cands)
            if sum(counts) + other_t == 0:
                continue
            leader = max(played, key=lambda i: played[i][1])
            out.append({'team': team, 'game_id': g['game_id'], 'season': g['season'], 'week': g['week'], 'index': j,
                        'ids': ids, 'w': [cands[i] for i in ids], 'other': other,
                        'targets': counts + [other_t], 'receptions': rec + [other_r],
                        'prior_hhi': float(np.mean([hhi([b['targets'] for b in p['boxes'] if b['targets'] > 0]) or 0 for p in prior])),
                        'leader_out': leader in out_now and played[leader][1] > 0})
    return out


def shares(w, other, k):
    p = np.asarray(w, float) ** k
    return np.append(p / p.sum() * (1 - other), other)


def loglik(rec, k, field='targets'):
    c = np.asarray(rec[field], float)
    if c.sum() == 0:
        return None
    return float((c * np.log(np.maximum(shares(rec['w'], rec['other'], k), 1e-12))).sum() / c.sum())


def team_k(history):
    """Maximum-likelihood k on a team's recent records."""
    totals = [sum(loglik(r, k) or 0 for r in history) for k in K_GRID]
    return float(K_GRID[int(np.argmax(totals))])


def m2_values(recs, k_league, lam):
    by_team = defaultdict(list)
    for r in recs:
        by_team[r['team']].append(r)
    out = {}
    for team, rs in by_team.items():
        rs.sort(key=lambda r: r['index'])
        for j, r in enumerate(rs):
            hist = rs[max(0, j - K_WINDOW):j]
            if len(hist) < K_WINDOW:
                continue
            n = len(hist)
            out[r['game_id'], team] = (n * team_k(hist) + lam * k_league) / (n + lam)
    return out


def score(recs, k_of):
    vals = {}
    for r in recs:
        k = k_of(r)
        if k is None:
            continue
        v = loglik(r, k)
        if v is not None:
            vals[r['game_id'], r['team']] = v
    return vals


def paired(a, b, seed=SEED):
    keys = sorted(set(a) & set(b))
    diff = np.array([a[k] - b[k] for k in keys])
    games = sorted({k[0] for k in keys})
    pos = {g: i for i, g in enumerate(games)}
    gi = np.array([pos[k[0]] for k in keys])
    rng = np.random.default_rng(seed)
    sums = np.bincount(gi, diff, len(games))
    cnt = np.bincount(gi, None, len(games))
    boot = [sums[s].sum() / cnt[s].sum() for s in (rng.integers(0, len(games), len(games)) for _ in range(2000))]
    return {'team_games': len(keys), 'games': len(games), 'mean_difference': float(diff.mean()),
            'ci95': np.quantile(boot, [.025, .975]).tolist()}


def h3(recs):
    from statistics import mean
    ev = [r for r in recs if r['leader_out'] and sum(r['targets']) > 0]
    if not ev:
        return {'events': 0}
    cut = np.quantile([r['prior_hhi'] for r in recs], [1 / 3, 2 / 3])
    out = {}
    for name, lo, hi in (('spread', -1, cut[0]), ('middle', cut[0], cut[1]), ('concentrated', cut[1], 9)):
        group = [r for r in ev if lo < r['prior_hhi'] <= hi]
        gains = []
        for r in group:
            j = int(np.argmax(r['w']))
            actual = r['targets'][j] / sum(r['targets'])
            gains.append(actual - shares(r['w'], r['other'], 1.)[j])
        out[name] = {'events': len(group), 'next_man_actual_minus_proportional': mean(gains) if gains else None}
    return {'events': len(ev), 'by_prior_concentration': out}


def run(stage, fitted=None):
    from research.nfl_game_leaders import read
    from research.nfl_game_leaders_calibration import inactives
    path, seasons = CAPTURES[stage]
    snapshot = read(Path(path))
    teams = team_games(snapshot, set(seasons))
    recs = records(teams, inactives(sorted(seasons)))
    report = {'study': STUDY, 'stage': stage, 'seasons': list(seasons), 'team_games': len(recs), 'h1': h1(teams)}
    if stage == 'development':
        k_league = float(K_GRID[int(np.argmax([np.mean([v for r in recs if (v := loglik(r, k)) is not None]) for k in K_GRID]))])
        m2_dev = {lam: score([r for r in recs], lambda r, m=m2_values(recs, k_league, lam): m.get((r['game_id'], r['team'])))
                  for lam in LAMBDAS}
        lam = max(LAMBDAS, key=lambda l: np.mean(list(m2_dev[l].values())))
        report['fit'] = {'k_league': k_league, 'lambda': lam,
                         'lambda_dev_mean_loglik': {l: float(np.mean(list(v.values()))) for l, v in m2_dev.items()}}
    else:
        k_league, lam = fitted['fit']['k_league'], fitted['fit']['lambda']
    m2 = m2_values(recs, k_league, lam)
    s0 = score(recs, lambda r: 1.)
    s1 = score(recs, lambda r: k_league)
    s2 = score(recs, lambda r: m2.get((r['game_id'], r['team'])))
    report['h2'] = {'m1_vs_m0_all_games': paired(s1, s0), 'm2_vs_m1': paired(s2, s1),
                    'mean_loglik_per_target': {'M0': float(np.mean([s0[k] for k in s2])), 'M1': float(np.mean([s1[k] for k in s2])),
                                               'M2': float(np.mean(list(s2.values())))},
                    'receptions_m1_vs_m0': paired({(r['game_id'], r['team']): v for r in recs if (v := loglik(r, k_league, 'receptions')) is not None},
                                                  {(r['game_id'], r['team']): v for r in recs if (v := loglik(r, 1., 'receptions')) is not None})}
    report['h2']['passes'] = bool(report['h2']['m2_vs_m1']['ci95'][0] > 0)
    report['team_k_spread'] = {'min': float(min(m2.values())), 'max': float(max(m2.values())), 'sd': float(np.std(list(m2.values())))}
    report['h3'] = h3(recs)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage', choices=('development', 'test'), required=True)
    parser.add_argument('--fit', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit(f'Refusing to overwrite {args.output}')
    if args.stage == 'test' and not args.fit:
        raise SystemExit('The test stage needs the frozen development report (--fit)')
    report = run(args.stage, json.loads(args.fit.read_text()) if args.fit else None)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=1, default=str))
    print(json.dumps({k: v for k, v in report.items() if k not in ('h1',)} | {'h1': {k: v for k, v in report['h1'].items()
                                                                                 if k not in ('most_concentrated', 'most_spread')}}, indent=1, default=str))


if __name__ == '__main__':
    main()
