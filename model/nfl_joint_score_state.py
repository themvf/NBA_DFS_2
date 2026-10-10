"""Study 1, `nfl-joint-score-state-v1`: pregame market line -> game-state profile -> team opportunity.

Registered in docs/nfl-joint-pregame-registration.md before any outcome was examined.

Nothing here simulates a game sequentially, and nothing here is observed after
kickoff. Score and clock enter only through HISTORICAL state-time profiles:

* Part A: the distribution of a game's state-time profile (share of regulation
  seconds the home team spent leading by 15+, 8-14, 1-7, tied, trailing by the
  same bands) conditional on the closing spread and total. Nearest-neighbour
  resampling on standardised (spread, total), so the joint shape of the profile
  is kept rather than parameterised.
* Part B: team pass attempts (targets) and carries as a Poisson log-linear
  function of that profile, ridge-regularised, fitted on seasons before the cutoff.

At pregame the only inputs are this game's market spread and total, captured
before the decision time. One profile is drawn per scenario and both teams read
it with opposite signs, so their totals stay coherent.

Two registered variants:

* V1 `market_weighted_blocks`: the baseline's whole-game block sampler weights
  historical blocks by (spread, total) similarity. Isolates "does the line alone
  help".
* V2 `state_profile`: Parts A and B above, applied as a per-draw multiplicative
  ratio on the parent team totals through the same hook the ridge market fit uses.

Neither variant may be re-tuned after the 2025 evaluation.
"""
from __future__ import annotations

from collections import defaultdict

import numpy as np
from scipy.optimize import minimize

from model.nfl_joint_contracts import digest, require_evidence
from model.nfl_longest_touchdown import timestamp

VERSION = 'nfl-joint-score-state-v1'
BANDS = ('lead15', 'lead8', 'lead1', 'tied', 'trail1', 'trail8', 'trail15')
# Away perspective is the mirror image of the home perspective.
FLIP = (6, 5, 4, 3, 2, 1, 0)
REGULATION_SECONDS = 3600
ACTIONS = ('targets', 'carries')


def band_index(home_margin):
    if home_margin >= 15:
        return 0
    if home_margin >= 8:
        return 1
    if home_margin >= 1:
        return 2
    if home_margin == 0:
        return 3
    if home_margin >= -7:
        return 4
    if home_margin >= -14:
        return 5
    return 6


def team_game_profiles(plays):
    """One row per game from play rows (any order).

    Each play row needs: game_id, home_team, away_team, posteam, score_differential
    (possessing team's perspective, the nflverse convention), game_seconds_remaining,
    play_type, had_sack, scramble, spread_line (home-positive), total_line, kickoff.
    The state at a play applies from that play until the next one; the stretch
    before the first play is tied; overtime carries game_seconds_remaining 0 and
    contributes no time, so profiles describe regulation only.
    """
    by_game = defaultdict(list)
    for row in plays:
        by_game[row['game_id']].append(row)
    games = []
    for game_id, rows in by_game.items():
        rows = [r for r in rows if r.get('game_seconds_remaining') is not None]
        if not rows:
            continue
        first = rows[0]
        if any(r['home_team'] != first['home_team'] or r['away_team'] != first['away_team'] for r in rows):
            raise ValueError(f'Inconsistent teams in game {game_id}')
        if first.get('spread_line') is None or first.get('total_line') is None:
            continue
        seconds = np.zeros(len(BANDS))
        stateful = sorted((r for r in rows if r.get('posteam') and r.get('score_differential') is not None),
                          key=lambda r: -r['game_seconds_remaining'])
        if not stateful:
            continue
        seconds[3] += REGULATION_SECONDS - stateful[0]['game_seconds_remaining']
        for j, r in enumerate(stateful):
            margin = r['score_differential'] if r['posteam'] == first['home_team'] else -r['score_differential']
            until = stateful[j + 1]['game_seconds_remaining'] if j + 1 < len(stateful) else 0
            seconds[band_index(margin)] += max(0, r['game_seconds_remaining'] - until)
        if abs(seconds.sum() - REGULATION_SECONDS) > 1e-6:
            raise AssertionError(f'Profile seconds do not sum to regulation in {game_id}')
        volumes = {side: {a: 0 for a in ACTIONS} for side in ('home', 'away')}
        for r in rows:
            if not r.get('posteam'):
                continue
            side = 'home' if r['posteam'] == first['home_team'] else 'away'
            if r.get('play_type') == 'pass' and not r.get('had_sack'):
                volumes[side]['targets'] += 1
            elif r.get('play_type') == 'run' and not r.get('scramble'):
                volumes[side]['carries'] += 1
        games.append({'game_id': game_id, 'kickoff': first['kickoff'], 'home': first['home_team'], 'away': first['away_team'],
                      'spread': float(first['spread_line']), 'total': float(first['total_line']),
                      'home_profile': (seconds / REGULATION_SECONDS).tolist(),
                      'home_targets': volumes['home']['targets'], 'home_carries': volumes['home']['carries'],
                      'away_targets': volumes['away']['targets'], 'away_carries': volumes['away']['carries']})
    return sorted(games, key=lambda g: (timestamp(g['kickoff']), g['game_id']))


def _design(profiles):
    """Drop the tied share (shares sum to one) so the design is full rank."""
    profiles = np.asarray(profiles, float)
    return np.column_stack([np.ones(len(profiles)), np.delete(profiles, 3, axis=1)])


def _fit_poisson(design, counts, ridge):
    def objective(beta):
        eta = design @ beta
        mu = np.exp(eta)
        loss = np.sum(mu - counts * eta) + ridge / 2 * (beta[1:] @ beta[1:])
        gradient = design.T @ (mu - counts)
        gradient[1:] += ridge * beta[1:]
        return loss, gradient
    start = np.zeros(design.shape[1])
    start[0] = np.log(max(counts.mean(), 1.))
    solved = minimize(objective, start, jac=True, method='L-BFGS-B')
    if not solved.success:
        raise ValueError('Opportunity profile fit failed')
    return solved.x


def _check_games(games, cutoff):
    if len(games) < 50:
        raise ValueError('At least 50 historical profile games required')
    seen = set()
    for g in games:
        if timestamp(g['kickoff']) >= timestamp(cutoff):
            raise ValueError('Profile training boundary violation')
        if g['game_id'] in seen:
            raise ValueError('Duplicate profile game')
        seen.add(g['game_id'])
        if abs(sum(g['home_profile']) - 1) > 1e-6 or any(v < 0 for v in g['home_profile']):
            raise ValueError('Invalid state profile')
        if any(g[f'{s}_{a}'] < 0 for s in ('home', 'away') for a in ACTIONS):
            raise ValueError('Invalid volume')


def fit_state_profile(games, cutoff, k=40, ridge=10.):
    """V2 fit. `games` come from team_game_profiles; all must precede `cutoff`."""
    if not (1 <= k <= len(games)) or not np.isfinite(ridge) or ridge <= 0:
        raise ValueError('Invalid state profile settings')
    _check_games(games, cutoff)
    lines = np.array([[g['spread'], g['total']] for g in games], float)
    center, scale = lines.mean(axis=0), lines.std(axis=0)
    scale[scale == 0] = 1
    # Part B sees every team-game from its own perspective: home rows as-is, away rows mirrored.
    profiles = [g['home_profile'] for g in games] + [[g['home_profile'][i] for i in FLIP] for g in games]
    design = _design(profiles)
    coefficients = {}
    for action in ACTIONS:
        counts = np.array([g[f'home_{action}'] for g in games] + [g[f'away_{action}'] for g in games], float)
        coefficients[action] = _fit_poisson(design, counts, ridge).tolist()
    mean_mu = {a: float(np.exp(design @ np.asarray(coefficients[a])).mean()) for a in ACTIONS}
    return {'version': VERSION, 'method': 'state_profile', 'training_cutoff': cutoff, 'k': k, 'ridge': ridge,
            'rows': len(games), 'bands': list(BANDS), 'center': center.tolist(), 'scale': scale.tolist(),
            'games': [{'game_id': g['game_id'], 'spread': g['spread'], 'total': g['total'], 'home_profile': g['home_profile']} for g in games],
            'coefficients': coefficients, 'mean_mu': mean_mu, 'source_sha256': digest(games),
            'limits': ['Profiles are regulation only; overtime contributes no state time.',
                       'Part A resamples historical profiles by closing-line similarity; no parametric game-state model.',
                       'Part B is a league-level Poisson rate; team level comes from the parent draws it scales.',
                       'Applied as a multiplicative ratio on parent totals, so parent script variance is retained and dispersion may widen.']}


def fit_market_weighted(games, cutoff, k=40):
    """V1 fit: the same neighbour index, used to weight the baseline block sampler."""
    if not (1 <= k <= len(games)):
        raise ValueError('Invalid neighbour count')
    _check_games(games, cutoff)
    lines = np.array([[g['spread'], g['total']] for g in games], float)
    center, scale = lines.mean(axis=0), lines.std(axis=0)
    scale[scale == 0] = 1
    return {'version': VERSION, 'method': 'market_weighted_blocks', 'training_cutoff': cutoff, 'k': k, 'rows': len(games),
            'center': center.tolist(), 'scale': scale.tolist(),
            'games': [{'game_id': g['game_id'], 'spread': g['spread'], 'total': g['total']} for g in games],
            'source_sha256': digest(games),
            'limits': ['Historical blocks outside the fitted line index receive zero weight and are reported.']}


def _market_point(fit, market, decision_at):
    require_evidence(market['evidence'], decision_at)
    if timestamp(fit['training_cutoff']) > timestamp(decision_at):
        raise ValueError('Profile fit after decision')
    point = (np.array([market['home_spread'], market['total']], float) - fit['center']) / fit['scale']
    if not np.isfinite(point).all():
        raise ValueError('Invalid game market features')
    return point


def neighbours(fit, market, decision_at):
    """Indices into fit['games'] of the k nearest closing lines."""
    point = _market_point(fit, market, decision_at)
    lines = (np.array([[g['spread'], g['total']] for g in fit['games']], float) - fit['center']) / fit['scale']
    distance = np.sqrt(((lines - point) ** 2).sum(axis=1))
    return np.argsort(distance, kind='stable')[:fit['k']]


def block_weights(fit, history, market, decision_at):
    """V1: a weight per history game for the baseline block sampler; neighbours get 1/k, the rest 0."""
    if fit.get('method') != 'market_weighted_blocks':
        raise ValueError('Block weights require the market-weighted fit')
    chosen = {fit['games'][int(i)]['game_id'] for i in neighbours(fit, market, decision_at)}
    weights = np.array([1. if h['game']['game_id'] in chosen else 0. for h in history])
    missing = sorted(chosen - {h['game']['game_id'] for h in history})
    if weights.sum() == 0:
        raise ValueError('No history game lies inside the fitted line neighbourhood')
    return weights / weights.sum(), {'neighbours': fit['k'], 'in_history': int(weights.sum()), 'missing_from_history': missing}


def draw_profiles(rng, fit, market, decision_at, draws):
    """V2 Part A: one home-perspective profile per scenario, resampled from the neighbourhood."""
    index = neighbours(fit, market, decision_at)
    picks = rng.choice(index, size=draws)
    return np.array([fit['games'][int(i)]['home_profile'] for i in picks]), picks


def profile_rates(fit, profiles):
    """V2 Part B: expected (home, away) targets and carries per scenario, relative to the training mean."""
    home = _design(profiles)
    away = _design(profiles[:, list(FLIP)])
    return {side: {a: np.exp(design @ np.asarray(fit['coefficients'][a])) / fit['mean_mu'][a] for a in ACTIONS}
            for side, design in (('home', home), ('away', away))}


def conditioned_volumes_state(bank, fit, market, seed, clip=(.5, 1.5)):
    """Same contract as nfl_opportunity_process.conditioned_volumes: {(team, action): int draws}."""
    if fit.get('method') != 'state_profile':
        raise ValueError('State conditioning requires the state profile fit')
    draws = len(bank['scenario_ids'])
    rng = np.random.default_rng(seed)
    profiles, picks = draw_profiles(rng, fit, market, bank['decision_at'], draws)
    rates = profile_rates(fit, profiles)
    result, diagnostics = {}, {'profile_game_ids': [fit['games'][int(i)]['game_id'] for i in picks[:50]],
                               'mean_home_profile': profiles.mean(axis=0).tolist()}
    for side, team in (('away', bank['game']['away']), ('home', bank['game']['home'])):
        players = [p for p in bank['players'] if p['team'] == team]
        for action in ACTIONS:
            parent = np.sum([p['draws'][action] for p in players], axis=0)
            ratio = np.clip(rates[side][action], *clip)
            result[(team, action)] = np.rint(parent * ratio).astype(int)
            diagnostics[f'{side}_{action}_mean_ratio'] = float(ratio.mean())
    return result, diagnostics
