"""Runner for `nfl-game-leaders-calibration-v1` (docs/nfl-game-leaders-calibration-study.md).

    python -m research.nfl_game_leaders_calibration run --input <capture.json.gz> --seasons 2023-2025 \
        --output artifacts/nfl-game-leaders/calibration-v1/development.jsonl
    python -m research.nfl_game_leaders_calibration fit --development <jsonl> --output <fit.json>
    python -m research.nfl_game_leaders_calibration grade-test --test <jsonl> --fit <fit.json> --output <report.json>

`run` forecasts every eligible regular-season game under each registered
variant and appends one compact record per game (resumable: games already in
the output are skipped). `fit` makes the development choices the registration
allows. `grade-test` applies them, unchanged, to the test season once.
"""
from __future__ import annotations

import argparse
import io
import json
from collections import defaultdict
from datetime import timedelta
from pathlib import Path

import numpy as np

STUDY = 'nfl-game-leaders-calibration-v1'
VARIANTS = ('A', 'B', 'C2', 'C4', 'C8')
METRICS = ('rushing_yards', 'receiving_yards', 'receptions', 'total_yards')
DRAWS = 2000
SEED = 20261008
BOOT_SEED = 20261011
ABSENT = {'INA', 'RES'}
T_GRID = np.round(np.arange(.5, 2.0001, .05), 2)
S_GRID = {'receptions': np.round(np.arange(.25, 4.0001, .25), 2), 'default': np.arange(2, 41, 2)}
R_GRID = (.01, .03, .05)
CACHE = Path('data/nflverse_cache')


def inactives(seasons):
    """(season, week) -> gsis ids listed INA or RES in the nflverse weekly roster."""
    import pandas as pd
    from model.nfl_absence_reallocation import SOURCES, fetch
    out = defaultdict(set)
    for season in seasons:
        raw = fetch(SOURCES['roster'].format(season=season), CACHE)
        frame = pd.read_csv(io.BytesIO(raw), low_memory=False, usecols=['season', 'week', 'gsis_id', 'status', 'game_type'])
        frame = frame[(frame['game_type'] == 'REG') & frame['gsis_id'].notna() & frame['status'].isin(ABSENT)]
        for row in frame.itertuples(index=False):
            out[int(row.season), int(row.week)].add(row.gsis_id)
    return out


def usage_depth(players, training, team, decision_at, recent=3):
    """Depth ranks from recent usage (carries for QB/RB, targets for WR/TE); stands in for historical depth charts."""
    prior = [h for h in training if team in (h['game']['away'], h['game']['home'])][-recent:]
    use = defaultdict(float)
    for h in prior:
        for b in h['boxes']:
            if b['team'] == team:
                use[b['identity']] += b['carries'] if b['position'] in ('QB', 'RB') else b['targets']
    comparisons = []
    for pos in {p['position'] for p in players}:
        ranked = sorted((p for p in players if p['position'] == pos), key=lambda p: -use[p['identity']])
        comparisons += [{'name': p['name'], 'position': pos, 'fantasypros': {'rank': r + 1}, 'sleeper': {}}
                        for r, p in enumerate(ranked)]
    return {'captured_at': decision_at, 'comparisons': comparisons, 'sleeper_only': [],
            'basis': 'recent-usage ranks (historical depth charts not available)'}


def prior_season_rates(boxes_by_season, season):
    """identity -> per-game carries/targets over games played the previous season."""
    played = defaultdict(lambda: {'n': 0, 'carries': 0., 'targets': 0.})
    for b in boxes_by_season.get(season - 1, []):
        if b['carries'] + b['targets'] > 0:
            v = played[b['identity']]
            v['n'] += 1
            v['carries'] += b['carries']
            v['targets'] += b['targets']
    return {i: {'carries': v['carries'] / v['n'], 'targets': v['targets'] / v['n']} for i, v in played.items()}


def requests_for(game, training, snapshot, decision_at, absent, index, prior_rates, recent_games):
    from model.nfl_game_leaders_roles import attach
    from research.nfl_game_leaders import expected_history, roster
    expected = expected_history(snapshot, game, decision_at, recent_games)
    base = {'game': game, 'decision_at': decision_at, 'players': roster(training, game),
            'expected_prior_game_ids': expected, 'availability_verified': False,
            'roster_evidence': f'{STUDY}: reconstructed previous-three-game usage'}
    out = {'A': base}
    players = [{**p, 'status': 'out' if p['identity'] in absent else 'unresolved'} for p in base['players']]
    with_inactives = {**base, 'players': players,
                      'dual_depth': {t: usage_depth([p for p in players if p['team'] == t], training, t, decision_at)
                                     for t in (game['away'], game['home'])}}
    for name, weight in (('B', 0.), ('C2', 2.), ('C4', 4.), ('C8', 8.)):
        out[name] = attach(with_inactives, index, expected, prior_rates, weight)
    return out


def compact(prediction, graded):
    record = {}
    for metric in METRICS:
        rows = prediction['metrics'][metric]['players']
        g = graded['metrics'][metric]
        winners = set(g['winner_ids'])
        named = {r['identity'] for r in rows if not r['residual']}
        target = _target(rows, winners, named, prediction)
        mine = game_log_loss({r['identity']: r['win_share'] for r in rows}, target)
        if abs(mine - g['log_loss']) > 1e-9:
            raise ValueError(f'{metric}: recomputed log loss {mine} differs from grade() {g["log_loss"]}')
        record[metric] = {
            'rows': [[r['identity'], bool(r['residual']), r['win_share'], r.get('baseline_mean')] for r in rows],
            'target': target,
            'log_loss': g['log_loss'], 'brier': g['brier'],
            'top_choice_credit': g['top_choice_credit'], 'baseline_credit': g['mean_baseline_credit']}
    return record


def _target(rows, winners, named, prediction):
    """Credit per row identity, mirroring grade(): unnamed winners go to OTHER:<team>."""
    teams = prediction['_winner_teams']
    credit = defaultdict(float)
    for w in winners:
        credit[w if w in named else 'OTHER:' + teams[w]] += 1 / len(winners)
    return dict(credit)


def run(snapshot_path, seasons, output, limit=None, variants=VARIANTS, log=print):
    from model.nfl_game_leaders import Settings, forecast, grade, prepare
    from model.nfl_game_leaders_roles import boxes_index
    from model.nfl_longest_touchdown import timestamp
    from research.nfl_game_leaders import read
    snapshot = read(snapshot_path)
    end = max(timestamp(g['kickoff']) for g in snapshot['games']) + timedelta(days=3)
    history, rejected = prepare(snapshot, end.isoformat(), retrospective=True)
    accepted = {h['game']['game_id']: h for h in history}
    index = boxes_index(snapshot)
    by_season = defaultdict(list)
    for b in snapshot['boxes']:
        by_season[int(b['game_id'][:4])].append(b)
    absent = inactives(sorted(set(seasons)))
    done = set()
    if output.exists():
        done = {json.loads(line)['game_id'] for line in output.read_text().splitlines() if line.strip()}
    cfg = Settings(draws=DRAWS, seed=SEED)
    games = sorted((g for g in snapshot['games'] if g['season'] in seasons and g.get('completed')),
                   key=lambda g: (timestamp(g['kickoff']), g['game_id']))
    graded_now = 0
    with output.open('a') as out:
        for game in games:
            gid = game['game_id']
            if gid in done:
                continue
            if limit and graded_now >= limit:
                break
            record = {'study': STUDY, 'game_id': gid, 'season': game['season'], 'week': game['week'], 'variants': {}}
            if gid not in accepted:
                record['skipped'] = 'target PBP/box coverage unresolved'
                out.write(json.dumps(record) + '\n')
                continue
            cutoff = timestamp(game['kickoff']) - timedelta(minutes=1)
            training = [h for h in history if timestamp(h['game']['kickoff']) < cutoff]
            try:
                reqs = requests_for(game, training, snapshot, cutoff.isoformat(), absent[game['season'], game['week']],
                                    index, prior_season_rates(by_season, game['season']), cfg.recent_games)
                boxes = accepted[gid]['boxes']
                teams = {b['identity']: b['team'] for b in boxes}
                for name in variants:
                    prediction = forecast(training, reqs[name], cfg)
                    graded = grade(prediction, {**snapshot, 'boxes': boxes})
                    if graded.get('status') != 'graded':
                        raise ValueError(graded.get('reason', 'not graded'))
                    record['variants'][name] = compact({**prediction, '_winner_teams': teams}, graded)
                record['out_players'] = sorted(p['identity'] for p in reqs['B']['players'] if p['status'] == 'out')
            except ValueError as exc:
                record = {k: v for k, v in record.items() if k != 'variants'}
                record['skipped'] = str(exc)
            out.write(json.dumps(record) + '\n')
            out.flush()
            graded_now += 'skipped' not in record
            log(json.dumps({'game': gid, 'skipped': record.get('skipped')}), flush=True)


# ---------------------------------------------------------------- analysis

def load(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def game_log_loss(probabilities, target):
    return -sum(t * np.log(max(probabilities.get(i, 0.), 1e-12)) for i, t in target.items())


def tempered(rows, temperature):
    ids = [r[0] for r in rows]
    p = np.array([max(r[2], 1e-12) for r in rows]) ** (1 / temperature)
    return dict(zip(ids, p / p.sum()))


def softmax_baseline(rows, scale, residual_share):
    named = [r for r in rows if not r[1]]
    others = [r for r in rows if r[1]]
    means = np.array([r[3] or 0. for r in named])
    z = np.exp((means - means.max()) / scale)
    out = dict(zip([r[0] for r in named], (1 - residual_share) * z / z.sum()))
    for r in others:
        out[r[0]] = residual_share / len(others) if others else 0.
    return out


def graded_records(records, variants):
    return [r for r in records if 'skipped' not in r and all(v in r['variants'] for v in variants)]


def fit(development):
    records = load(development)
    usable = graded_records(records, ('B', 'C2', 'C4', 'C8'))
    with_prior = [r for r in usable if r['season'] >= min(r2['season'] for r2 in usable) + 1]
    totals = {v: sum(np.mean([r['variants'][v][m]['log_loss'] for r in with_prior]) for m in METRICS)
              for v in ('B', 'C2', 'C4', 'C8')}
    chosen = min(totals, key=totals.get)
    games = graded_records(records, (chosen,))
    temperatures, baselines = {}, {}
    for m in METRICS:
        losses = {float(t): np.mean([game_log_loss(tempered(r['variants'][chosen][m]['rows'], t), r['variants'][chosen][m]['target'])
                                     for r in games]) for t in T_GRID}
        temperatures[m] = min(losses, key=losses.get)
        grid = S_GRID['receptions'] if m == 'receptions' else S_GRID['default']
        best = min(((float(s), rs, np.mean([game_log_loss(softmax_baseline(r['variants'][chosen][m]['rows'], s, rs),
                                                         r['variants'][chosen][m]['target']) for r in games]))
                    for s in grid for rs in R_GRID), key=lambda x: x[2])
        baselines[m] = {'scale': best[0], 'residual_share': best[1], 'development_log_loss': best[2]}
    return {'study': STUDY, 'development': str(development), 'games': len(games), 'skipped': len(records) - len(usable),
            'variant_choice': {'chosen': chosen, 'summed_log_loss_2nd_season_on': totals, 'games_compared': len(with_prior)},
            'temperature': temperatures, 'baseline': baselines}


def evaluate(records, fitted):
    """Per market: final vs probabilistic baseline (primary), plus reported comparisons."""
    chosen = fitted['variant_choice']['chosen']
    games = graded_records(records, ('A', 'B', chosen))
    rng = np.random.default_rng(BOOT_SEED)
    report = {'games': len(games), 'skipped': len(records) - len(games), 'markets': {}}
    idx = rng.integers(0, len(games), size=(2000, len(games))) if games else None
    for m in METRICS:
        t = fitted['temperature'][m]
        b = fitted['baseline'][m]
        final = np.array([game_log_loss(tempered(r['variants'][chosen][m]['rows'], t), r['variants'][chosen][m]['target']) for r in games])
        base = np.array([game_log_loss(softmax_baseline(r['variants'][chosen][m]['rows'], b['scale'], b['residual_share']),
                                       r['variants'][chosen][m]['target']) for r in games])
        diff = final - base
        boot = diff[idx].mean(axis=1)
        top = np.array([r['variants'][chosen][m]['top_choice_credit'] - r['variants'][chosen][m]['baseline_credit'] for r in games])
        tboot = top[idx].mean(axis=1)
        report['markets'][m] = {
            'primary': {'final_log_loss': float(final.mean()), 'baseline_log_loss': float(base.mean()),
                        'mean_difference': float(diff.mean()), 'ci95': np.quantile(boot, [.025, .975]).tolist(),
                        'passes': bool(np.quantile(boot, .975) < 0)},
            'variant_log_loss': {v: float(np.mean([r['variants'][v][m]['log_loss'] for r in games])) for v in ('A', 'B', chosen)},
            'top_choice_minus_naive': {'mean': float(top.mean()), 'ci95': np.quantile(tboot, [.025, .975]).tolist()},
            'calibration': calibration_bins(games, chosen, m, t)}
    return report


def calibration_bins(games, variant, metric, temperature):
    bins = defaultdict(lambda: [0, 0., 0.])
    for r in games:
        f = r['variants'][variant][metric]
        probs = tempered(f['rows'], temperature)
        for row in f['rows']:
            if row[1]:
                continue
            p = probs[row[0]]
            b = bins[min(9, int(p * 10))]
            b[0] += 1
            b[1] += p
            b[2] += f['target'].get(row[0], 0.)
    total = sum(b[0] for b in bins.values())
    return {'bins': [{'range': [k / 10, (k + 1) / 10], 'rows': b[0], 'mean_probability': b[1] / b[0], 'observed': b[2] / b[0]}
                     for k, b in sorted(bins.items())],
            'expected_calibration_error': sum(b[0] / total * abs(b[1] / b[0] - b[2] / b[0]) for b in bins.values()) if total else None}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    r = sub.add_parser('run')
    r.add_argument('--input', type=Path, required=True)
    r.add_argument('--seasons', required=True)
    r.add_argument('--output', type=Path, required=True)
    r.add_argument('--limit', type=int)
    f = sub.add_parser('fit')
    f.add_argument('--development', type=Path, required=True)
    f.add_argument('--output', type=Path, required=True)
    d = sub.add_parser('describe-development')
    d.add_argument('--development', type=Path, required=True)
    d.add_argument('--fit', type=Path, required=True)
    d.add_argument('--output', type=Path, required=True)
    t = sub.add_parser('grade-test')
    t.add_argument('--test', type=Path, required=True)
    t.add_argument('--fit', type=Path, required=True)
    t.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'run':
        first, last = (int(x) for x in args.seasons.split('-'))
        args.output.parent.mkdir(parents=True, exist_ok=True)
        run(args.input, set(range(first, last + 1)), args.output, args.limit)
        return
    if args.output.exists():
        raise SystemExit(f'Refusing to overwrite {args.output}')
    if args.command == 'fit':
        result = fit(args.development)
    elif args.command == 'describe-development':
        result = {'authority': 'development_in_sample', **evaluate(load(args.development), json.loads(args.fit.read_text()))}
    else:
        result = {'authority': 'registered_single_look_test', 'fit': json.loads(args.fit.read_text()),
                  **evaluate(load(args.test), json.loads(args.fit.read_text()))}
    args.output.write_text(json.dumps(result, indent=1))
    print(json.dumps({k: v for k, v in result.items() if k != 'fit'}, indent=1)[:4000])


if __name__ == '__main__':
    main()
