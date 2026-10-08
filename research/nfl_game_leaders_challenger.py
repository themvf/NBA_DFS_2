"""Exact-outcome ranking experiment; deliberately separate from public forecasts.

Train: 2023 and 2024 weeks 1-10. Select: 2024 weeks 11-18.
One untouched test: 2025 weeks 13-18. Never select settings on 2026 results.
Corrected box history is retrospective, not archived pregame availability.
"""
from collections import defaultdict
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
import argparse
import io
import json

import numpy as np
from model.nfl_game_leaders import SOURCE_METRICS as METRICS, canonical, timestamp
from research.nfl_longest_touchdown import read, write

FIELDS = ('carries', 'targets', *METRICS)
REGISTRATION = {
    'train': '2023 all weeks + 2024 weeks 1-10',
    'selection': '2024 weeks 11-18', 'untouched_test': '2025 weeks 13-18',
    'penalties': [1., 10., 100.], 'baseline_windows': [1, 3, 6],
    'selection_rule': 'highest fractional named first-choice accuracy, then lowest log loss',
    'promotion_rule': 'paired game bootstrap lower 95% improvement bound above zero vs selected baseline for each metric separately',
    'scope': 'research ranking only; no calibrated probability or live-role claim',
}


def verify_boxes(snapshot, sources):
    """Full player sums versus separately published same-provider team aggregates."""
    indexed = {}
    covered_seasons = {int(r['season']) for s in sources for r in s['rows']}
    for source in sources:
        for r in source['rows']:
            if r['season_type'] != 'REG':
                continue
            key = (r['game_id'], canonical(r['team']))
            if key in indexed:
                raise ValueError('Duplicate team aggregate')
            indexed[key] = r
    boxes = defaultdict(list)
    seen = set()
    for b in snapshot['boxes']:
        key = (b['game_id'], b['identity'])
        if key in seen:
            raise ValueError('Duplicate player game identity')
        seen.add(key)
        if any(not np.isfinite(float(b[f])) for f in FIELDS):
            raise ValueError('Nonfinite player statistic')
        boxes[b['game_id']].append({**b, 'team': canonical(b['team'])})
    good, rejected = [], []
    for g in sorted(snapshot['games'], key=lambda g: (timestamp(g['kickoff']), g['game_id'])):
        if g['season'] not in covered_seasons or not g.get('completed') or g['game_id'] not in boxes:
            continue
        issues = []
        for team, opponent in ((g['away'], g['home']), (g['home'], g['away'])):
            r = indexed.get((g['game_id'], team))
            if not r or canonical(r['opponent_team']) != opponent or int(r['week']) != g['week'] or int(r['season']) != g['season']:
                issues.append('canonical team source missing or mismatched')
                continue
            for field in FIELDS:
                total = sum(float(b[field]) for b in boxes[g['game_id']] if b['team'] == team)
                if abs(total - float(r[field])) > .01:
                    issues.append(f'{team}:{field}')
        if issues:
            rejected.append({'game_id': g['game_id'], 'issues': issues})
        else:
            good.append({'game': g, 'boxes': boxes[g['game_id']]})
    return good, rejected


def examples(history, metric):
    """Sequential features. No target-game rows participate in candidates/features."""
    team_history = defaultdict(list)
    result = []
    for h in history:
        g = h['game']
        teams = (g['away'], g['home'])
        prior = {t: [p for p in team_history[t] if timestamp(p['game']['kickoff']) < timestamp(g['kickoff'])] for t in teams}
        candidates = {}
        for t in teams:
            for p in prior[t][-3:]:
                for b in p['boxes']:
                    if b['team'] == t and b['carries'] + b['targets'] > 0:
                        if b['identity'] not in candidates or timestamp(p['game']['kickoff']) > candidates[b['identity']][0]:
                            candidates[b['identity']] = (timestamp(p['game']['kickoff']), b)
        rows = []
        for _, b in candidates.values():
            t = b['team']
            hist = prior[t][-6:]
            values = np.array([[sum(float(q[f]) for q in p['boxes'] if q['team'] == t and q['identity'] == b['identity']) for f in FIELDS] for p in hist])
            if not len(values):
                continue
            feature = []
            means = {}
            for n in (1, 3, 6):
                means[n] = values[-n:].mean(axis=0)
                feature.extend(np.log1p(np.maximum(means[n], 0)))
            opp = teams[1] if t == teams[0] else teams[0]
            allowed = [sum(float(q[metric]) for q in p['boxes'] if q['team'] != opp) for p in prior[opp][-6:]]
            team_total = np.mean([sum(float(q[metric]) for q in p['boxes'] if q['team'] == t) for p in hist])
            feature.extend([means[3][FIELDS.index(metric)] / max(team_total, 1),
                            np.log1p(max(float(np.mean(allowed)) if allowed else 0, 0)),
                            *(float(b['position'] == pos) for pos in ('QB', 'RB', 'WR', 'TE')), 0.])
            rows.append({'identity': b['identity'], 'name': b['name'], 'features': feature,
                         'baselines': {n: float(means[n][FIELDS.index(metric)]) for n in means}})
        if all(prior[t] for t in teams) and rows:
            # OTHER is a winner category, never a pooled yardage competitor.
            # If it is selected, primary player-pick accuracy receives zero credit.
            for t in teams:
                rows.append({'identity': 'OTHER:' + t, 'name': 'Unresolved ' + t,
                             'features': [0.] * (len(rows[0]['features']) - 1) + [1.],
                             'baselines': {n: -float('inf') for n in (1, 3, 6)}})
            maximum = max(float(b[metric]) for b in h['boxes'])
            winners = [b for b in h['boxes'] if float(b[metric]) == maximum]
            labels = defaultdict(float)
            for b in winners:
                observed = candidates.get(b['identity'])
                key = b['identity'] if observed and observed[1]['team'] == b['team'] else 'OTHER:' + b['team']
                labels[key] += 1. / len(winners)
            result.append({'game': g, 'rows': rows, 'y': [labels[r['identity']] for r in rows]})
        for t in teams:
            team_history[t].append(h)
    return result


def fit(data, penalty):
    from scipy.optimize import minimize
    counts = np.array([len(d['rows']) for d in data])
    starts = np.r_[0, np.cumsum(counts)[:-1]]
    x = np.array([r['features'] for d in data for r in d['rows']])
    y = np.concatenate([d['y'] for d in data])
    mean, scale = x.mean(axis=0), x.std(axis=0)
    scale[scale < 1e-8] = 1.
    x = (x - mean) / scale
    groups = np.repeat(np.arange(len(counts)), counts)
    def objective(w):
        logits = x @ w
        maxima = np.maximum.reduceat(logits, starts)
        exp = np.exp(logits - maxima[groups])
        denom = np.add.reduceat(exp, starts)
        logp = logits - maxima[groups] - np.log(denom[groups])
        loss = -y @ logp + penalty * (w @ w) / 2
        gradient = x.T @ (exp / denom[groups] - y) + penalty * w
        return loss, gradient
    fitted = minimize(objective, np.zeros(x.shape[1]), jac=True, method='L-BFGS-B', options={'maxiter': 300})
    if not fitted.success:
        raise ValueError(f'Fit did not converge: {fitted.message}')
    return {'weights': fitted.x.tolist(), 'mean': mean.tolist(), 'scale': scale.tolist(), 'penalty': penalty}


def assess(data, fitted=None, window=6):
    output = []
    for d in data:
        rows = d['rows']
        if fitted:
            x = np.array([r['features'] for r in rows])
            scores = ((x - fitted['mean']) / fitted['scale']) @ fitted['weights']
            p = np.exp(scores - max(scores)); p /= p.sum()
        else:
            p = None
            scores = [r['baselines'][window] for r in rows]
        chosen = int(np.argmax(scores)); r = rows[chosen]
        credit = 0. if r['identity'].startswith('OTHER:') else d['y'][chosen]
        output.append({'game_id': d['game']['game_id'], 'pick': r['name'], 'identity': r['identity'],
                       'credit': credit, 'unresolved': r['identity'].startswith('OTHER:'),
                       'log_loss': -float(np.dot(d['y'], np.log(np.maximum(p, 1e-12)))) if p is not None else None})
    return output


def study(snapshot, sources, output):
    history, rejected = verify_boxes(snapshot, sources)
    report = {'registration': REGISTRATION, 'implementation_sha256': sha256(Path(__file__).read_bytes()).hexdigest(), 'source_rejections': rejected,
              'source_games': len(history), 'sources': [{k: v for k, v in s.items() if k != 'rows'} for s in sources], 'metrics': {}}
    rng = np.random.default_rng(20261008)
    for metric in METRICS:
        data = examples(history, metric)
        train = [d for d in data if d['game']['season'] == 2023 or (d['game']['season'] == 2024 and d['game']['week'] <= 10)]
        select = [d for d in data if d['game']['season'] == 2024 and d['game']['week'] >= 11]
        test = [d for d in data if d['game']['season'] == 2025 and d['game']['week'] >= 13]
        baseline_trials = {n: assess(select, window=n) for n in REGISTRATION['baseline_windows']}
        window = max(baseline_trials, key=lambda n: np.mean([r['credit'] for r in baseline_trials[n]]))
        fits = [fit(train, penalty) for penalty in REGISTRATION['penalties']]
        trials = [assess(select, fitted=f) for f in fits]
        best = max(range(len(fits)), key=lambda i: (np.mean([r['credit'] for r in trials[i]]), -np.mean([r['log_loss'] for r in trials[i]])))
        # Selection is complete before accessing the test outcome scores.
        learned = assess(test, fitted=fits[best]); baseline = assess(test, window=window)
        delta = np.array([a['credit'] - b['credit'] for a, b in zip(learned, baseline)])
        interval = np.quantile(delta[rng.integers(0, len(delta), (10000, len(delta)))].mean(axis=1), [.025, .975]).tolist()
        report['metrics'][metric] = {'train_games': len(train), 'selection_games': len(select), 'test_games': len(test),
            'baseline_window': window, 'selected_fit': fits[best], 'selection_baselines': {n: float(np.mean([r['credit'] for r in v])) for n, v in baseline_trials.items()},
            'selection_models': [{'penalty': f['penalty'], 'accuracy': float(np.mean([r['credit'] for r in t]))} for f, t in zip(fits, trials)],
            'test_accuracy': float(np.mean([r['credit'] for r in learned])), 'test_baseline_accuracy': float(np.mean([r['credit'] for r in baseline])),
            'improvement_interval': interval, 'promotion_passed': interval[0] > 0,
            'unresolved_picks': sum(r['unresolved'] for r in learned), 'test_model': learned, 'test_baseline': baseline}
        print(json.dumps({'metric': metric, **{k: report['metrics'][metric][k] for k in ('test_games', 'test_accuracy', 'test_baseline_accuracy', 'improvement_interval', 'promotion_passed')}}), flush=True)
    report['limits'] = ['Corrected historical sources and reconstructed prior-usage rosters, not pregame archived availability.',
        'Normalized ranking scores are not calibrated game-leader probabilities.', 'Opponent aggregates are not opponent-adjusted causal effects.',
        'Three regularization values and three baseline windows selected on 2024, never on the 2025 test.',
        '2026 outcomes not used in training, selection, or test; no public forecast changed.']
    write(output, report)


def main():
    parser = argparse.ArgumentParser(); parser.add_argument('--capture-sources', action='store_true')
    parser.add_argument('--root', type=Path, default=Path('artifacts/nfl-game-leaders'))
    parser.add_argument('--output', type=Path)
    args = parser.parse_args(); root = args.root
    registration_path = root / 'challenger-registration.json'
    if registration_path.exists():
        if read(registration_path) != REGISTRATION:
            raise ValueError('Registered study changed; use a new study directory')
    else:
        write(registration_path, REGISTRATION)
    output = args.output or root / 'challenger-study.json'
    if output.exists():
        raise FileExistsError('Preserve the existing study; supply a new --output path')
    if args.capture_sources:
        import pandas as pd
        import requests
        from ingest.ff_independent import NFLVERSE_WEEKLY_TEAM_STATS_URL
        for season in (2023, 2024, 2025):
            url = NFLVERSE_WEEKLY_TEAM_STATS_URL.format(season=season)
            response = requests.get(url, timeout=90); response.raise_for_status()
            write(root / f'challenger-team-source-{season}.json.gz', {'url': url, 'sha256': sha256(response.content).hexdigest(),
                'captured_at': datetime.now(timezone.utc).isoformat(), 'rows': json.loads(pd.read_csv(io.BytesIO(response.content)).to_json(orient='records'))})
    sources = [read(root / f'challenger-team-source-{season}.json.gz') for season in (2023, 2024, 2025)]
    study(read(root / 'full-capture-20261008.json.gz'), sources, output)


if __name__ == '__main__':
    main()
