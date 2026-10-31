"""Harness for `nfl-leaders-regime-roles-v1` (docs/nfl-leaders-regime-roles-study.md).

Roles v3 (`nfl-game-leaders-roles-v3`, research only) is the v2 role scenario
(model/nfl_game_leaders_roles.py, pinned and untouched) with two fitted
parameters:

* **K** (prior strength, pseudo-games). When a player ruled OUT recorded usage
  in the window, every active teammate at his position group (RB for a back,
  WR/TE for a receiver) is estimated from the window games the ruled-out
  player did NOT play ("regime games"), as a Dirichlet-multinomial posterior
  whose prior mean is the v2 allocation:

      rate_i = (sum_{g in regime, i played} w_g * c_i,g + K * v2_rate_i) / (sum_{g in regime, i played} w_g + K)

  `v2_rate_i` is exactly what v2 gives the player per game: his own shrunk,
  prior-blended rate plus, for carries, the RB depth split (55/35/10) of the
  ruled-out back's shrunk rate. In the share form of the registration,
  alpha_i = K * v2_rate_i and sum_j alpha_j = K * (the group's v2 per-game
  total), so the rate form above and the share form are the same estimate.
  With no regime game (the starter missed nothing yet, or K is infinite) the
  estimate is v2. Games a teammate did not play are absences, as in v2, not
  games of zero use. Quarterback carries, newcomer targets, the reserve,
  departed players at zero and the prior-season role gate are v2's.
* **h** (half-life in games). Window usage is weighted w_g = 0.5^((G-1-g)/h);
  h = inf is v2's equal weight. A player's games played become the weighted
  count sum_{played} w_g, which v2's shrinkage and prior-season blend then use
  in place of the plain count (identical at h = inf).

Primary metric: the Dirichlet-multinomial log score of the next game's
observed carries and targets under the predicted field, with the production
model's fixed role concentration (55 carries / 40 targets, the same for v2 and
v3) as the Dirichlet precision. It is carried as a LOSS (minus the log
probability), so every paired difference below is v3 minus v2 with lower
better and the registered PASS reads "upper bound below zero", as for the
leader log-loss guard. Counts for an identity outside the field go to the
newcomer reserve; a share below SHARE_FLOOR is floored before scoring,
identically for both estimators.

    python -m research.nfl_game_leaders_regime_roles develop --input <capture.json.gz> --output <dev.jsonl>
    python -m research.nfl_game_leaders_regime_roles fit --development <dev.jsonl> --output fit.json
    python -m research.nfl_game_leaders_regime_roles guard --input <capture.json.gz> --fit fit.json
    python -m research.nfl_game_leaders_regime_roles shadow [--page web/src/data/game-leaders.json]
    python -m research.nfl_game_leaders_regime_roles grade [--season 2026]
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path

import numpy as np
from scipy.special import gammaln

from model.nfl_game_leaders_roles import (NEWCOMER_TARGETS, RB_DEPTH_SPLIT, RESERVE, SHRINK_GAMES, SURPRISE_SLOTS,
                                          VERSION as V2_VERSION, _field, blend_prior, depth_ranks,
                                          prior_role_plausible)

STUDY = 'nfl-leaders-regime-roles-v1'
ESTIMATOR = 'nfl-game-leaders-roles-v3'
K_GRID = (0.5, 1.0, 2.0, 4.0)
H_GRID = (math.inf, 4.0, 3.0, 2.0, 1.0)
DEVELOPMENT = {2023: (4, 18), 2024: (1, 18), 2025: (1, 18)}
CONFIRMATION = {'season': 2026, 'first_week': 7, 'last_week': 18}
FLOORS = {'affected_team_games': 60, 'game_weeks': 8}
BOOTSTRAP = {'resamples': 2000, 'seed': 20261018, 'cluster': 'season_week', 'interval': 0.95}
GUARD_UPPER_BOUND = 0.005
SHARE_FLOOR = 1e-4
GROUPS = {'RB': ('RB',), 'WR': ('WR', 'TE'), 'TE': ('WR', 'TE')}
AFFECTED_RANK = {'RB': 2, 'WR': 3, 'TE': 3}
ROOT = Path('artifacts/nfl-game-leaders/regime-roles-v1')
FIT_PATH = ROOT / 'fit.json'
SHADOW_DIR = ROOT / 'shadow'
GRADE_DIR = ROOT / 'grade-runs'
VERDICT_PATH = ROOT / 'verdict.json'
PAGE = Path('web/src/data/game-leaders.json')
METRICS = ('rushing_yards', 'receiving_yards', 'receptions', 'total_yards')
ACTIONS = ('carries', 'targets')


def cell_key(K, h):
    return f'K={K:g},h={"inf" if h == math.inf else f"{h:g}"}'


def parse_cell(key):
    k, h = key.split(',')
    h = h.split('=')[1]
    return float(k.split('=')[1]), (math.inf if h == 'inf' else float(h))


def grid_cells():
    return [(K, h) for K in K_GRID for h in H_GRID]


# ---------------------------------------------------------------- estimator

def window_weights(count, h):
    """w_g = 0.5^((G-1-g)/h); the latest game weighs 1, h = inf is equal weight."""
    if h == math.inf:
        return [1.0] * count
    if not (h > 0):
        raise ValueError('Half-life must be positive')
    return [0.5 ** ((count - 1 - g) / h) for g in range(count)]


def ordered(games):
    """Window game ids chronologically (ids are YYYY_WW_AWAY_HOME)."""
    return sorted(games)


def weighted_usage_rates(team_boxes, games, h):
    """v2's usage_rates with half-life weights; identical to it at h = inf."""
    games = ordered(games)
    weights = window_weights(len(games), h)
    total_weight = sum(weights)
    played = defaultdict(lambda: {'n': 0., 'carries': 0., 'targets': 0.})
    for w, game_id in zip(weights, games):
        for b in team_boxes.get(game_id, []):
            if b['carries'] + b['targets'] > 0:
                v = played[b['identity']]
                v['n'] += w
                v['carries'] += w * b['carries']
                v['targets'] += w * b['targets']
    rates = {}
    for identity, v in played.items():
        out = {'games_played': v['n']}
        for action in ACTIONS:
            on = v[action] / v['n']
            overall = v[action] / total_weight
            out[action] = (v['n'] * on + SHRINK_GAMES * overall) / (v['n'] + SHRINK_GAMES)
        rates[identity] = out
    return rates


def regime_games(team_boxes, games, outs):
    """Window games in which none of `outs` (identities) recorded a touch."""
    played_by = {}
    for game_id in games:
        played_by[game_id] = {b['identity'] for b in team_boxes.get(game_id, []) if b['carries'] + b['targets'] > 0}
    return [g for g in ordered(games) if not (played_by[g] & set(outs))]


def team_scenario_v3(team, players, dual_depth, team_boxes, games, prior_rates=None, prior_weight=0.,
                     K=None, h=math.inf):
    """Roles v3 for one team: (scenario, note). K=None or inf and h=inf reproduce v2 exactly."""
    games = ordered(games)
    weights = dict(zip(games, window_weights(len(games), h)))
    active = [p for p in players if p['status'] != 'out']
    outs = [p for p in players if p['status'] == 'out']
    ranks = depth_ranks(dual_depth)

    def rank(p):
        return ranks.get((p['name'], p['position']), 99)

    current = weighted_usage_rates(team_boxes, games, h)
    denied = [p for p in players if p['identity'] not in current and p['identity'] in (prior_rates or {})
              and not prior_role_plausible(p, rank(p))]
    eligible_prior = {i: r for i, r in (prior_rates or {}).items() if i not in {p['identity'] for p in denied}}
    rates = blend_prior(current, eligible_prior, prior_weight)

    request_ids = {p['identity'] for p in players}
    departed = sorted({b['identity'] for g in games for b in team_boxes.get(g, []) if b['identity'] not in request_ids})
    roster = [p['identity'] for p in active] + departed + [f'NEW:{team}:{j}' for j in range(SURPRISE_SLOTS)]

    # v2 allocation per active player, per game.
    carries = {p['identity']: rates.get(p['identity'], {}).get('carries', 0.) for p in active}
    vacated_rb = sum(rates.get(p['identity'], {}).get('carries', 0.) for p in outs if p['position'] == 'RB')
    vacated_qb = sum(rates.get(p['identity'], {}).get('carries', 0.) for p in outs if p['position'] == 'QB')
    backs = sorted((p for p in active if p['position'] == 'RB'), key=rank)[:len(RB_DEPTH_SPLIT)]
    if vacated_rb and backs:
        split = RB_DEPTH_SPLIT[:len(backs)]
        for p, share in zip(backs, split):
            carries[p['identity']] += vacated_rb * share / sum(split)
    quarterbacks = sorted((p for p in active if p['position'] == 'QB'), key=rank)
    if vacated_qb and quarterbacks:
        carries[quarterbacks[0]['identity']] += vacated_qb
    targets = {}
    for p in active:
        rate = rates.get(p['identity'], {}).get('targets')
        if rate is None:
            high = (p['position'] in ('WR', 'TE') and rank(p) <= 3) or (p['position'] == 'RB' and rank(p) <= 2)
            rate = NEWCOMER_TARGETS if high else 0.
        targets[p['identity']] = rate
    v2_alloc = {'carries': dict(carries), 'targets': dict(targets)}

    # Regime conditioning: the window without the ruled-out player, for his position group.
    regime = {}
    affected_by = []
    for key, positions in (('RB', ('RB',)), ('WR/TE', ('WR', 'TE'))):
        group_outs = [p for p in outs if p['position'] in positions and p['identity'] in current]
        if not group_outs:
            continue
        kept = regime_games(team_boxes, games, [p['identity'] for p in group_outs])
        regime[key] = {'outs': [p['name'] for p in group_outs], 'games': kept}
        if not kept:
            continue
        # Affected subset: a top-two back by window carries or top-three receiver by window targets.
        usage_action = 'carries' if key == 'RB' else 'targets'
        order = sorted((p for p in players if p['position'] in positions and p['identity'] in current),
                       key=lambda p: -current[p['identity']][usage_action])
        for p in group_outs:
            position_rank = order.index(p) + 1
            if position_rank <= AFFECTED_RANK[p['position']]:
                affected_by.append({'name': p['name'], 'position': p['position'], 'window_rank': position_rank,
                                    'regime_games': len(kept)})
        if K is None or K == math.inf:
            continue
        if not (K > 0):
            raise ValueError('Prior strength must be positive')
        for p in active:
            if p['position'] not in positions:
                continue
            for action in ACTIONS:
                evidence, n_w = 0., 0.
                for g in kept:
                    box = next((b for b in team_boxes.get(g, []) if b['identity'] == p['identity']), None)
                    if box is not None and box['carries'] + box['targets'] > 0:
                        evidence += weights[g] * box[action]
                        n_w += weights[g]
                alloc = v2_alloc[action][p['identity']]
                v2_alloc[action][p['identity']] = (evidence + K * alloc) / (n_w + K)

    scenario = {'carries': _field(v2_alloc['carries'], roster, team, RESERVE['carries']),
                'targets': _field(v2_alloc['targets'], roster, team, RESERVE['targets'])}
    name = {p['identity']: p['name'] for p in players}
    note = {'estimator': ESTIMATOR, 'K': None if K is None else (K if K != math.inf else 'inf'),
            'h': 'inf' if h == math.inf else h,
            'affected': bool(affected_by), 'affected_by': affected_by, 'regime': regime,
            'out_with_usage': [f"{p['name']} ({p['position']}, {rates[p['identity']]['carries']:.1f} car, "
                               f"{rates[p['identity']]['targets']:.1f} tgt a game)" for p in outs if p['identity'] in rates],
            'vacated_rb_carries': round(vacated_rb, 1), 'rb_depth_order': [p['name'] for p in backs],
            'departed_named_at_zero': len(departed),
            'prior_role_denied': [f"{p['name']} ({p['position']} rank {rank(p)}, {p.get('sleeper_status') or 'no status'})"
                                  for p in denied],
            'top_carries': {name[i]: round(v, 3) for i, v in sorted(scenario['carries'].items(), key=lambda kv: -kv[1])[:3] if i in name},
            'top_targets': {name[i]: round(v, 3) for i, v in sorted(scenario['targets'].items(), key=lambda kv: -kv[1])[:4] if i in name}}
    return scenario, note


def attach_v3(request, boxes_by_game_team, prior_games, prior_rates=None, prior_weight=0., K=None, h=math.inf,
              K_targets=None, h_targets=None):
    """A copy of `request` whose role scenario is roles v3; carries and targets may use different (K, h)."""
    game = request['game']
    scenarios, notes = {}, {}
    pairs = {'carries': (K, h), 'targets': (K if K_targets is None else K_targets, h if h_targets is None else h_targets)}
    for team in (game['away'], game['home']):
        team_boxes = {g: boxes_by_game_team.get((g, team), []) for g in prior_games[team]}
        players = [p for p in request['players'] if p['team'] == team]
        scenarios[team], notes[team] = {}, {}
        for action, (k, hl) in pairs.items():
            scenario, note = team_scenario_v3(team, players, request['dual_depth'][team], team_boxes,
                                              prior_games[team], prior_rates, prior_weight, k, hl)
            scenarios[team][action] = scenario[action]
            notes[team][action] = note
    captured = max(request['dual_depth'][t]['captured_at'] for t in scenarios)
    if captured > request['decision_at']:
        raise ValueError('Depth evidence captured after the decision time')
    games = sorted({g for v in prior_games.values() for g in v})
    evidence = {'source_ref': f'{ESTIMATOR}: request.dual_depth + recent box scores {games}', 'captured_at': captured,
                'method': ESTIMATOR, 'study': STUDY, 'prior_weight': prior_weight,
                'parameters': {a: {'K': None if k is None else ('inf' if k == math.inf else k), 'h': 'inf' if hl == math.inf else hl}
                               for a, (k, hl) in pairs.items()},
                'description': 'Roles v3 research scenario (regime-conditioned window, fitted K and half-life). '
                               'Shadow only; see research/nfl_game_leaders_regime_roles.py.',
                'notes': notes}
    return {**request, 'role_scenarios': scenarios, 'scenario_evidence': evidence}


# ---------------------------------------------------------------- scoring

def dm_log_score(shares, counts, concentration):
    """log DM(counts | alpha = concentration * shares). Unknown identities count in the reserve."""
    field = {i: max(float(s), 0.) for i, s in shares.items()}
    reserve_ids = [i for i in field if i.startswith('NEW:')]
    merged = {i: s for i, s in field.items() if not i.startswith('NEW:')}
    merged['NEW'] = sum(field[i] for i in reserve_ids)
    observed = defaultdict(float)
    for i, c in counts.items():
        observed[i if i in merged else 'NEW'] += float(c)
    ids = sorted(merged)
    p = np.array([max(merged[i], SHARE_FLOOR) for i in ids])
    p = p / p.sum()
    alpha = concentration * p
    n = np.array([observed.get(i, 0.) for i in ids])
    total = n.sum()
    return float(gammaln(total + 1) - gammaln(n + 1).sum() + gammaln(alpha.sum()) - gammaln(total + alpha.sum())
                 + (gammaln(n + alpha) - gammaln(alpha)).sum())


def score_team(scenario, boxes, named, concentrations):
    """Per action: DM log LOSS (minus the log score) and the named players' count MAE (share x observed team total)."""
    out = {}
    for action in ACTIONS:
        counts = {b['identity']: b[action] for b in boxes}
        total = sum(counts.values())
        out[action] = -dm_log_score(scenario[action], counts, concentrations[action])
        errors = [abs(scenario[action].get(i, 0.) * total - counts.get(i, 0.)) for i in named]
        out[f'mae_{action}'] = float(np.mean(errors)) if errors else None
    return out


# ---------------------------------------------------------------- development

def eligible(season, week):
    first, last = DEVELOPMENT.get(season, (None, None))
    return first is not None and first <= week <= last


def development(snapshot_path, output, limit=None, log=print):
    """One record per eligible development game: v2 and every grid cell scored on both teams."""
    from model.nfl_game_leaders import concentration, prepare
    from model.nfl_game_leaders_calibration import PRIOR_WEIGHT
    from model.nfl_game_leaders_roles import attach, boxes_index
    from model.nfl_longest_touchdown import timestamp
    from research.nfl_game_leaders import expected_history, read, roster
    from research.nfl_game_leaders_calibration import inactives, prior_season_rates, usage_depth
    snapshot = read(snapshot_path)
    end = max(timestamp(g['kickoff']) for g in snapshot['games']) + timedelta(days=3)
    history, _ = prepare(snapshot, end.isoformat(), retrospective=True)
    accepted = {h['game']['game_id']: h for h in history}
    index = boxes_index(snapshot)
    by_season = defaultdict(list)
    for b in snapshot['boxes']:
        by_season[int(b['game_id'][:4])].append(b)
    absent = inactives(sorted(DEVELOPMENT))
    concentrations = {a: concentration(history, a) for a in ACTIONS}
    done = set()
    if output.exists():
        done = {json.loads(line)['game_id'] for line in output.read_text().splitlines() if line.strip()}
    games = sorted((g for g in snapshot['games'] if eligible(g['season'], g['week']) and g.get('completed')),
                   key=lambda g: (timestamp(g['kickoff']), g['game_id']))
    written = 0
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open('a') as out:
        for game in games:
            gid = game['game_id']
            if gid in done:
                continue
            if limit and written >= limit:
                break
            record = {'study': STUDY, 'game_id': gid, 'season': game['season'], 'week': game['week']}
            if gid not in accepted:
                record['skipped'] = 'target PBP/box coverage unresolved'
            else:
                cutoff = timestamp(game['kickoff']) - timedelta(minutes=1)
                training = [h for h in history if timestamp(h['game']['kickoff']) < cutoff]
                try:
                    record['teams'] = development_game(game, training, snapshot, cutoff.isoformat(),
                                                       absent[game['season'], game['week']], index,
                                                       prior_season_rates(by_season, game['season']), PRIOR_WEIGHT,
                                                       accepted[gid]['boxes'], concentrations, attach, roster,
                                                       usage_depth, expected_history)
                except ValueError as exc:
                    record['skipped'] = str(exc)
            out.write(json.dumps(record) + '\n')
            out.flush()
            written += 'skipped' not in record
            log(json.dumps({'game': gid, 'skipped': record.get('skipped')}), flush=True)


def base_request(game, training, snapshot, decision_at, absent, roster, usage_depth, expected_history):
    """The calibration harness's request: usage roster, INA/RES as out, recent-usage depth."""
    players = [{**p, 'status': 'out' if p['identity'] in absent else 'unresolved'} for p in roster(training, game)]
    return {'game': game, 'decision_at': decision_at, 'players': players,
            'expected_prior_game_ids': expected_history(snapshot, game, decision_at),
            'availability_verified': False, 'roster_evidence': f'{STUDY}: reconstructed previous-three-game usage',
            'dual_depth': {t: usage_depth([p for p in players if p['team'] == t], training, t, decision_at)
                           for t in (game['away'], game['home'])}}


def development_game(game, training, snapshot, decision_at, absent, index, prior_rates, prior_weight, boxes,
                     concentrations, attach, roster, usage_depth, expected_history):
    request = base_request(game, training, snapshot, decision_at, absent, roster, usage_depth, expected_history)
    prior_games = request['expected_prior_game_ids']
    v2 = attach(request, index, prior_games, prior_rates, prior_weight)
    teams = {}
    for team in (game['away'], game['home']):
        team_boxes = {g: index.get((g, team), []) for g in prior_games[team]}
        players = [p for p in request['players'] if p['team'] == team]
        named = [p['identity'] for p in players if p['status'] != 'out']
        actual = [b for b in boxes if b['team'] == team]
        reduced, note = team_scenario_v3(team, players, request['dual_depth'][team], team_boxes, prior_games[team],
                                         prior_rates, prior_weight)
        for action in ACTIONS:
            stored = v2['role_scenarios'][team][action]
            if set(stored) != set(reduced[action]) or any(abs(stored[i] - reduced[action][i]) > 1e-9 for i in stored):
                raise AssertionError(f'{team}: roles v3 at its defaults does not reproduce roles v2 ({action})')
        cells = {'v2': score_team(v2['role_scenarios'][team], actual, named, concentrations)}
        for K, h in grid_cells():
            scenario, _ = team_scenario_v3(team, players, request['dual_depth'][team], team_boxes, prior_games[team],
                                           prior_rates, prior_weight, K, h)
            cells[cell_key(K, h)] = score_team(scenario, actual, named, concentrations)
        teams[team] = {'affected': note['affected'], 'affected_by': note['affected_by'],
                       'regime': {k: len(v['games']) for k, v in note['regime'].items()},
                       'out_with_usage': note['out_with_usage'], 'named': len(named),
                       'totals': {a: sum(b[a] for b in actual) for a in ACTIONS}, 'cells': cells}
    return teams


# ---------------------------------------------------------------- analysis

def load(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def team_rows(records, affected=True):
    """(cluster, team record) rows; affected=None keeps every team-game."""
    rows = []
    for r in records:
        if 'skipped' in r:
            continue
        for team, t in r['teams'].items():
            if affected is None or t['affected'] == affected:
                rows.append(((r['season'], r['week']), r['game_id'], team, t))
    return rows


def cluster_bootstrap(values, clusters, resamples=BOOTSTRAP['resamples'], seed=BOOTSTRAP['seed']):
    """Mean and a 95% interval resampling clusters (season-week) with replacement."""
    values = np.asarray(values, float)
    if len(values) == 0:
        return {'mean': None, 'ci95': None, 'n': 0, 'clusters': 0}
    groups = defaultdict(list)
    for v, c in zip(values, clusters):
        groups[c].append(v)
    keys = sorted(groups)
    sums = np.array([sum(groups[k]) for k in keys])
    counts = np.array([len(groups[k]) for k in keys])
    rng = np.random.default_rng(seed)
    draws = rng.integers(0, len(keys), size=(resamples, len(keys)))
    means = sums[draws].sum(axis=1) / counts[draws].sum(axis=1)
    lo, hi = (1 - BOOTSTRAP['interval']) / 2, 1 - (1 - BOOTSTRAP['interval']) / 2
    return {'mean': float(values.mean()), 'ci95': np.quantile(means, [lo, hi]).tolist(), 'n': int(len(values)),
            'clusters': len(keys)}


def summarize_cells(rows, cells):
    """Per action and cell: paired mean (v3 - v2) DM log loss and MAE on `rows`; lower is better."""
    out = {}
    for action in ACTIONS:
        out[action] = {}
        for cell in cells:
            diffs = [t['cells'][cell][action] - t['cells']['v2'][action] for _, _, _, t in rows]
            mae = [t['cells'][cell][f'mae_{action}'] - t['cells']['v2'][f'mae_{action}'] for _, _, _, t in rows
                   if t['cells'][cell][f'mae_{action}'] is not None]
            out[action][cell] = {'mean_dm_log_loss_difference': float(np.mean(diffs)) if diffs else None,
                                 'mean_mae_difference': float(np.mean(mae)) if mae else None, 'n': len(diffs)}
    return out


def choose(summary):
    """Per action, the grid cell with the lowest mean paired DM log loss; ties go to the first in grid order."""
    chosen = {}
    for action in ACTIONS:
        best = min(((summary[action][cell_key(K, h)]['mean_dm_log_loss_difference'], i, cell_key(K, h))
                    for i, (K, h) in enumerate(grid_cells())), key=lambda x: (x[0], x[1]))
        K, h = parse_cell(best[2])
        chosen[action] = {'K': K, 'h': 'inf' if h == math.inf else h, 'cell': best[2]}
    return chosen


def subset_counts(records):
    affected = team_rows(records, True)
    unaffected = team_rows(records, False)
    by_season = defaultdict(lambda: {'affected': 0, 'unaffected': 0})
    for (season, _), _, _, _ in affected:
        by_season[season]['affected'] += 1
    for (season, _), _, _, _ in unaffected:
        by_season[season]['unaffected'] += 1
    return {'affected_team_games': len(affected), 'unaffected_team_games': len(unaffected),
            'affected_game_weeks': len({c for c, _, _, _ in affected}),
            'games': len([r for r in records if 'skipped' not in r]), 'skipped': len([r for r in records if 'skipped' in r]),
            'by_season': {str(k): v for k, v in sorted(by_season.items())}}


def fit(development_path):
    records = load(development_path)
    affected = team_rows(records, True)
    unaffected = team_rows(records, False)
    cells = [cell_key(K, h) for K, h in grid_cells()]
    summary = summarize_cells(affected, cells)
    chosen = choose(summary)
    development_ci = {}
    for action in ACTIONS:
        cell = chosen[action]['cell']
        diffs = [t['cells'][cell][action] - t['cells']['v2'][action] for _, _, _, t in affected]
        development_ci[action] = cluster_bootstrap(diffs, [c for c, _, _, _ in affected])
    return {'study': STUDY, 'estimator': ESTIMATOR, 'v2': V2_VERSION, 'development': str(development_path),
            'frozen_at': datetime.now(timezone.utc).isoformat(), 'grid': {'K': list(K_GRID), 'h': ['inf', 4, 3, 2, 1]},
            'chosen': chosen, 'subset': subset_counts(records),
            'affected_grid': summary, 'unaffected_grid': summarize_cells(unaffected, cells),
            'development_in_sample_ci': development_ci,
            'notes': ['Development is tuning data; the interval here is in-sample and supports no claim.',
                      '2023 has no prior season in the capture, so the prior-season blend is inactive there (as in calibration-v1).',
                      'Historical depth order is recent-usage rank; the live page uses the dual depth chart.',
                      'Primary metric: Dirichlet-multinomial log score with the production concentration (55/40) as precision.',
                      'alpha_i = K x the v2 per-game allocation (own blended rate plus the RB depth split of the ruled-out back).']}


# ---------------------------------------------------------------- guard (leader log loss)

def leader_rows(prediction):
    return {m: [[r['identity'], bool(r['residual']), r['win_share']] for r in prediction['metrics'][m]['players']]
            for m in METRICS}


def leader_target(metric, boxes, named):
    """Credit per identity, as grade(): tied leaders split, unnamed winners go to OTHER:<team>."""
    from model.nfl_game_leaders import observed_value
    actual = {b['identity']: observed_value(b, metric) for b in boxes}
    best = max(actual.values())
    winners = [i for i, v in actual.items() if v == best]
    teams = {b['identity']: b['team'] for b in boxes}
    credit = defaultdict(float)
    for w in winners:
        credit[w if w in named else 'OTHER:' + teams[w]] += 1 / len(winners)
    return dict(credit)


def leader_log_loss(rows, target):
    probabilities = {r[0]: r[2] for r in rows}
    return -sum(t * math.log(max(probabilities.get(i, 0.), 1e-12)) for i, t in target.items())


def chosen_pairs(fitted):
    def pair(action):
        c = fitted['chosen'][action]
        return float(c['K']), (math.inf if c['h'] == 'inf' else float(c['h']))
    return pair('carries'), pair('targets')


def scenarios_differ(a, b):
    for team in a:
        for action in ACTIONS:
            x, y = a[team][action], b[team][action]
            if set(x) != set(y) or any(abs(x[i] - y[i]) > 1e-9 for i in x):
                return True
    return False


_WORKER = {}


def _guard_init(snapshot_path, fit_path, draws):
    from model.nfl_game_leaders import prepare
    from model.nfl_game_leaders_roles import boxes_index
    from model.nfl_longest_touchdown import timestamp
    from research.nfl_game_leaders import read
    from research.nfl_game_leaders_calibration import inactives
    snapshot = read(Path(snapshot_path))
    end = max(timestamp(g['kickoff']) for g in snapshot['games']) + timedelta(days=3)
    history, _ = prepare(snapshot, end.isoformat(), retrospective=True)
    _WORKER.update(snapshot=snapshot, history=history, accepted={h['game']['game_id']: h for h in history},
                   index=boxes_index(snapshot), absent=inactives(sorted(DEVELOPMENT)),
                   fitted=json.loads(Path(fit_path).read_text()), draws=draws)
    by_season = defaultdict(list)
    for b in snapshot['boxes']:
        by_season[int(b['game_id'][:4])].append(b)
    _WORKER['by_season'] = by_season


def _guard_game(gid):
    from model.nfl_game_leaders import Settings, forecast, grade
    from model.nfl_game_leaders_calibration import PRIOR_WEIGHT
    from model.nfl_game_leaders_roles import attach
    from model.nfl_longest_touchdown import timestamp
    from research.nfl_game_leaders import expected_history, roster
    from research.nfl_game_leaders_calibration import DRAWS, SEED, prior_season_rates, usage_depth
    w = _WORKER
    game = w['accepted'][gid]['game']
    cutoff = timestamp(game['kickoff']) - timedelta(minutes=1)
    training = [h for h in w['history'] if timestamp(h['game']['kickoff']) < cutoff]
    try:
        request = base_request(game, training, w['snapshot'], cutoff.isoformat(), w['absent'][game['season'], game['week']],
                               roster, usage_depth, expected_history)
        prior = prior_season_rates(w['by_season'], game['season'])
        (kc, hc), (kt, ht) = chosen_pairs(w['fitted'])
        v2 = attach(request, w['index'], request['expected_prior_game_ids'], prior, PRIOR_WEIGHT)
        v3 = attach_v3(request, w['index'], request['expected_prior_game_ids'], prior, PRIOR_WEIGHT, kc, hc, kt, ht)
        boxes = w['accepted'][gid]['boxes']
        named = {p['identity'] for p in request['players'] if p['status'] != 'out'}
        if not scenarios_differ(v2['role_scenarios'], v3['role_scenarios']):
            return {'game_id': gid, 'season': game['season'], 'week': game['week'], 'identical': True,
                    'difference': {m: 0. for m in METRICS}}
        cfg = Settings(draws=w['draws'] or DRAWS, seed=SEED)
        losses = {}
        for label, req in (('v2', v2), ('v3', v3)):
            prediction = forecast(training, req, cfg)
            graded = grade(prediction, {**w['snapshot'], 'boxes': boxes})
            if graded.get('status') != 'graded':
                raise ValueError(graded.get('reason', 'not graded'))
            rows = leader_rows(prediction)
            losses[label] = {m: leader_log_loss(rows[m], leader_target(m, boxes, named)) for m in METRICS}
            for m in METRICS:
                if abs(losses[label][m] - graded['metrics'][m]['log_loss']) > 1e-9:
                    raise ValueError(f'{m}: recomputed log loss differs from grade()')
        return {'game_id': gid, 'season': game['season'], 'week': game['week'], 'identical': False,
                'log_loss': losses, 'difference': {m: losses['v3'][m] - losses['v2'][m] for m in METRICS}}
    except ValueError as exc:
        return {'game_id': gid, 'season': game['season'], 'week': game['week'], 'skipped': str(exc)}


def guard_summary(results):
    graded = [r for r in results if 'skipped' not in r]
    clusters = [(r['season'], r['week']) for r in graded]
    markets = {}
    for m in METRICS:
        boot = cluster_bootstrap([r['difference'][m] for r in graded], clusters)
        markets[m] = {**boot, 'held': bool(boot['ci95'] is not None and boot['ci95'][1] <= GUARD_UPPER_BOUND)}
    return {'games': len(graded), 'skipped': len(results) - len(graded),
            'forecast_rerun': sum(1 for r in graded if not r['identical']),
            'identical_scenarios': sum(1 for r in graded if r['identical']),
            'upper_bound': GUARD_UPPER_BOUND, 'markets': markets, 'held': all(v['held'] for v in markets.values())}


def guard(snapshot_path, fit_path, workers=4, draws=None, limit=None, log=print):
    """Leader log loss of the published model with v3 shares minus v2 shares, every development game."""
    from concurrent.futures import ProcessPoolExecutor
    from research.nfl_game_leaders import read
    fitted = json.loads(Path(fit_path).read_text())
    if 'guard' in fitted:
        raise SystemExit('fit.json already carries a guard reading; the fit is frozen')
    snapshot = read(Path(snapshot_path))
    games = sorted(g['game_id'] for g in snapshot['games'] if eligible(g['season'], g['week']) and g.get('completed'))
    if limit:
        games = games[:limit]
    results = []
    with ProcessPoolExecutor(max_workers=workers, initializer=_guard_init, initargs=(str(snapshot_path), str(fit_path), draws)) as pool:
        for r in pool.map(_guard_game, games, chunksize=2):
            results.append(r)
            log(json.dumps({'guard': r['game_id'], 'identical': r.get('identical'), 'skipped': r.get('skipped')}), flush=True)
    summary = guard_summary(results)
    fitted['guard'] = {'authority': 'development_in_sample', 'computed_at': datetime.now(timezone.utc).isoformat(),
                       'draws': draws, **summary}
    Path(fit_path).write_text(json.dumps(fitted, indent=1))
    (Path(fit_path).parent / 'guard-development.jsonl').write_text('\n'.join(json.dumps(r) for r in results) + '\n')
    return fitted['guard']


# ---------------------------------------------------------------- shadow (2026 weeks 7+)

def shadow_path(season, week):
    return SHADOW_DIR / f'{season}-w{week:02d}.json'


def shadow_game(request, snapshot, fitted, cfg, index, by_season, now):
    """v3 and v2 scenarios plus both leader forecasts for one stored pregame request."""
    from model.nfl_game_leaders import forecast
    from model.nfl_game_leaders_calibration import PRIOR_WEIGHT
    from model.nfl_game_leaders_roles import attach
    from model.nfl_longest_touchdown import timestamp
    from research.nfl_game_leaders import expected_history, prepare
    from research.nfl_game_leaders_calibration import prior_season_rates
    from research.nfl_game_leaders_reviewed import with_reviewed_ranks
    game = request['game']
    base = {k: v for k, v in request.items() if k not in ('role_scenarios', 'scenario_evidence')}
    review = (request.get('scenario_evidence') or {}).get('reviewed_override')
    if review:
        base = with_reviewed_ranks(base, review)
    prior_games = expected_history(snapshot, game, request['decision_at'])
    prior = prior_season_rates(by_season, game['season']) if PRIOR_WEIGHT else None
    (kc, hc), (kt, ht) = chosen_pairs(fitted)
    # The publisher drops a zero-share player who moved between the two teams
    # (research.nfl_game_leaders_auto.drop_moved_players); do the same, or the
    # model rejects a field that lists him on his old team.
    from research.nfl_game_leaders_auto import drop_moved_players
    v2 = drop_moved_players(attach(base, index, prior_games, prior, PRIOR_WEIGHT))
    v3 = drop_moved_players(attach_v3(base, index, prior_games, prior, PRIOR_WEIGHT, kc, hc, kt, ht))
    stored = request.get('role_scenarios') or {}
    reproduced = not scenarios_differ(stored, v2['role_scenarios']) if stored else None
    history, _ = prepare(snapshot, request['decision_at'])
    forecasts = {}
    for label, req in (('v2', v2), ('v3', v3)):
        req = {**req, 'expected_prior_game_ids': prior_games}
        forecasts[label] = leader_rows(forecast(history, req, cfg))
    named = {p['identity'] for p in base['players'] if p['status'] != 'out'}
    teams = {}
    for team in (game['away'], game['home']):
        notes = v3['scenario_evidence']['notes'][team]
        teams[team] = {'affected': notes['carries']['affected'], 'affected_by': notes['carries']['affected_by'],
                       'regime': {k: len(v['games']) for k, v in notes['carries']['regime'].items()},
                       'named': sorted(p for p in named if any(q['identity'] == p and q['team'] == team for q in base['players'])),
                       'v2': v2['role_scenarios'][team], 'v3': v3['role_scenarios'][team]}
    return {'game': game, 'decision_at': request['decision_at'], 'shadow_at': now.isoformat(),
            'pregame': timestamp(now.isoformat()) < timestamp(game['kickoff']),
            'estimator': ESTIMATOR, 'fit': fitted['chosen'], 'v2_scenario_reproduced': reproduced,
            'reviewed_override': bool(review), 'settings': cfg.__dict__ | {'temperatures': dict(cfg.temperatures or {})},
            'teams': teams, 'forecasts': forecasts}


def shadow(page_path=PAGE, fit_path=FIT_PATH, draws=None, log=print):
    """Write v3 shares and forecasts beside each published game's stored request. Never touches the page."""
    from model.nfl_game_leaders_calibration import production_settings
    from model.nfl_game_leaders_roles import boxes_index
    from model.nfl_longest_touchdown import timestamp
    from research.nfl_game_leaders import read
    fitted = json.loads(Path(fit_path).read_text())
    page = json.loads(Path(page_path).read_text(encoding='utf-8'))
    season, week = page['season'], page['week']
    if season != CONFIRMATION['season'] or week < CONFIRMATION['first_week']:
        return f'Week {week} of {season} is outside the confirmation window; nothing written'
    out_path = shadow_path(season, week)
    existing = json.loads(out_path.read_text()) if out_path.exists() else {'study': STUDY, 'season': season, 'week': week, 'games': {}}
    cfg = production_settings(**({'draws': draws} if draws else {}))
    loaded = {}
    now = datetime.now(timezone.utc)
    added = []
    for entry in page['games']:
        gid = entry['game']['game_id']
        src = entry['sources']
        # Pair v3 with the request the page actually published: a later pregame republish
        # (fresh availability) replaces the shadow; a started game's shadow is frozen.
        held = existing['games'].get(gid)
        if held and (held['sources'] == src or timestamp(entry['game']['kickoff']) <= now):
            continue
        if src['snapshot'] not in loaded:
            snapshot = read(Path(src['snapshot']))
            by_season = defaultdict(list)
            for b in snapshot['boxes']:
                by_season[int(b['game_id'][:4])].append(b)
            loaded[src['snapshot']] = (snapshot, boxes_index(snapshot), by_season)
        snapshot, index, by_season = loaded[src['snapshot']]
        request = read(Path(src['requests']))[gid]
        if request.get('capture_error') or 'players' not in request:
            log(json.dumps({'shadow_skipped': gid, 'reason': 'no request'}), flush=True)
            continue
        try:
            existing['games'][gid] = {**shadow_game(request, snapshot, fitted, cfg, index, by_season, now),
                                      'sources': src}
            added.append(gid)
            log(json.dumps({'shadow': gid, 'affected': {t: v['affected'] for t, v in existing['games'][gid]['teams'].items()},
                            'v2_reproduced': existing['games'][gid]['v2_scenario_reproduced']}), flush=True)
        except ValueError as exc:
            log(json.dumps({'shadow_error': gid, 'reason': str(exc)}), flush=True)
    if added:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        existing['updated_at'] = now.isoformat()
        out_path.write_text(json.dumps(existing, indent=1))
    return f'Week {week}: {len(added)} games shadowed now, {len(existing["games"])} in {out_path.as_posix()}'


# ---------------------------------------------------------------- grade (one look)

def completed_boxes(game_id, captures):
    """Boxes of a completed, fully covered game from the newest capture that has them."""
    for snapshot in captures:
        game = next((g for g in snapshot['games'] if g['game_id'] == game_id), None)
        if not game or not game.get('completed'):
            continue
        if not snapshot.get('game_coverage', {}).get(game_id, {}).get('regulation_end_observed'):
            continue
        boxes = [b for b in snapshot['boxes'] if b['game_id'] == game_id]
        if boxes and all(any(b['team'] == t for b in boxes) for t in (game['away'], game['home'])):
            return boxes
    return None


def grade_rows(shadows, captures, concentrations):
    rows, pending, excluded = [], [], []
    for week_file in shadows:
        for gid, entry in week_file['games'].items():
            if not entry['pregame']:
                excluded.append({'game_id': gid, 'reason': 'shadow written after kickoff'})
                continue
            boxes = completed_boxes(gid, captures)
            if boxes is None:
                pending.append(gid)
                continue
            named = {p for t in entry['teams'].values() for p in t['named']}
            cluster = (entry['game']['season'], entry['game']['week'])
            leader = {m: leader_log_loss(entry['forecasts']['v3'][m], leader_target(m, boxes, named))
                      - leader_log_loss(entry['forecasts']['v2'][m], leader_target(m, boxes, named)) for m in METRICS}
            for team, t in entry['teams'].items():
                actual = [b for b in boxes if b['team'] == team]
                scores = {label: score_team(t[label], actual, t['named'], concentrations) for label in ('v2', 'v3')}
                rows.append({'cluster': cluster, 'game_id': gid, 'team': team, 'affected': t['affected'],
                             'dm': {a: scores['v3'][a] - scores['v2'][a] for a in ACTIONS},
                             'mae': {a: (scores['v3'][f'mae_{a}'] - scores['v2'][f'mae_{a}'])
                                     if scores['v3'][f'mae_{a}'] is not None else None for a in ACTIONS},
                             'leader': leader})
    return rows, pending, excluded


def floors_met(rows):
    affected = [r for r in rows if r['affected']]
    weeks = {r['cluster'] for r in affected}
    return {'affected_team_games': len(affected), 'game_weeks': len(weeks),
            'met': len(affected) >= FLOORS['affected_team_games'] and len(weeks) >= FLOORS['game_weeks']}


def verdict(rows):
    """PASS needs the primary interval's upper bound below zero for both actions and the guard held."""
    affected = [r for r in rows if r['affected']]
    unaffected = [r for r in rows if not r['affected']]
    clusters = [r['cluster'] for r in affected]
    primary = {a: cluster_bootstrap([r['dm'][a] for r in affected], clusters) for a in ACTIONS}
    secondary = {a: cluster_bootstrap([r['mae'][a] for r in affected if r['mae'][a] is not None],
                                      [r['cluster'] for r in affected if r['mae'][a] is not None]) for a in ACTIONS}
    games = {}
    for r in rows:
        games.setdefault(r['game_id'], (r['cluster'], r['leader']))
    guard = {m: cluster_bootstrap([v[1][m] for v in games.values()], [v[0] for v in games.values()]) for m in METRICS}
    for m in METRICS:
        guard[m]['held'] = bool(guard[m]['ci95'] is not None and guard[m]['ci95'][1] <= GUARD_UPPER_BOUND)
    passed = all(primary[a]['ci95'] is not None and primary[a]['ci95'][1] < 0 for a in ACTIONS) and all(v['held'] for v in guard.values())
    return {'primary_dm_log_loss_v3_minus_v2': primary,
            'secondary_mae_v3_minus_v2': secondary, 'guard_leader_log_loss_v3_minus_v2': guard,
            'unaffected_reported': {a: cluster_bootstrap([r['dm'][a] for r in unaffected], [r['cluster'] for r in unaffected]) for a in ACTIONS},
            'passes': bool(passed), 'family': 'two questions, one confirmation look'}


def grade(season=CONFIRMATION['season'], captures_glob='artifacts/nfl-game-leaders/*/capture.json.gz', now=None, log=print):
    """Grade shadowed weeks. Blinded to the effect until both floors are met; the verdict is written once."""
    from model.nfl_game_leaders import concentration
    from research.nfl_game_leaders import read
    now = now or datetime.now(timezone.utc)
    import glob as globbing
    shadows = [json.loads(p.read_text()) for p in sorted(SHADOW_DIR.glob(f'{season}-w*.json'))]
    paths = sorted((Path(p) for p in globbing.glob(captures_glob)), key=lambda p: p.stat().st_mtime, reverse=True)
    captures = [read(p) for p in paths]
    concentrations = {a: concentration([], a) for a in ACTIONS}
    rows, pending, excluded = grade_rows(shadows, captures, concentrations)
    floors = floors_met(rows)
    run = {'study': STUDY, 'graded_at': now.isoformat(), 'season': season, 'floors': {**FLOORS, **floors},
           'graded_games': len({r['game_id'] for r in rows}), 'pending_games': pending, 'excluded': excluded,
           'captures': [p.as_posix() for p in paths], 'rows': rows}
    GRADE_DIR.mkdir(parents=True, exist_ok=True)
    (GRADE_DIR / f"{now.strftime('%Y%m%dT%H%M%SZ')}.json").write_text(json.dumps(run, indent=1))
    if not floors['met']:
        return {**{k: v for k, v in run.items() if k != 'rows'},
                'status': 'blinded: floors not met; no effect estimate is computed before them'}
    if VERDICT_PATH.exists():
        return {**{k: v for k, v in run.items() if k != 'rows'}, 'status': 'verdict already written; one look only',
                'verdict': json.loads(VERDICT_PATH.read_text())}
    result = {'study': STUDY, 'verdict_at': now.isoformat(), 'floors': run['floors'], 'graded_games': run['graded_games'],
              **verdict(rows)}
    VERDICT_PATH.write_text(json.dumps(result, indent=1))
    return {**{k: v for k, v in run.items() if k != 'rows'}, 'status': 'verdict written', 'verdict': result}


# ---------------------------------------------------------------- CLI

def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='command', required=True)
    d = sub.add_parser('develop')
    d.add_argument('--input', type=Path, required=True)
    d.add_argument('--output', type=Path, default=ROOT / 'development.jsonl')
    d.add_argument('--limit', type=int)
    f = sub.add_parser('fit')
    f.add_argument('--development', type=Path, default=ROOT / 'development.jsonl')
    f.add_argument('--output', type=Path, default=FIT_PATH)
    g = sub.add_parser('guard')
    g.add_argument('--input', type=Path, required=True)
    g.add_argument('--fit', type=Path, default=FIT_PATH)
    g.add_argument('--workers', type=int, default=4)
    g.add_argument('--draws', type=int)
    g.add_argument('--limit', type=int)
    s = sub.add_parser('shadow')
    s.add_argument('--page', type=Path, default=PAGE)
    s.add_argument('--fit', type=Path, default=FIT_PATH)
    s.add_argument('--draws', type=int)
    r = sub.add_parser('grade')
    r.add_argument('--season', type=int, default=CONFIRMATION['season'])
    args = parser.parse_args()
    if args.command == 'develop':
        development(args.input, args.output, args.limit)
    elif args.command == 'fit':
        if args.output.exists():
            raise SystemExit(f'Refusing to overwrite the frozen fit {args.output}')
        result = fit(args.development)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=1))
        print(json.dumps({k: result[k] for k in ('chosen', 'subset', 'development_in_sample_ci')}, indent=1))
    elif args.command == 'guard':
        print(json.dumps(guard(args.input, args.fit, args.workers, args.draws, args.limit), indent=1))
    elif args.command == 'shadow':
        print(shadow(args.page, args.fit, args.draws))
    else:
        print(json.dumps(grade(args.season), indent=1)[:6000])


if __name__ == '__main__':
    main()
