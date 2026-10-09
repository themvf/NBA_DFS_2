from __future__ import annotations

from collections import defaultdict

import numpy as np

from model.nfl_joint_contracts import digest
from model.nfl_longest_touchdown import timestamp


def fit_exits(observations, cutoff, segments=8, prior_exposure=100.):
    if segments < 2 or prior_exposure <= 0 or not np.isfinite(prior_exposure):
        raise ValueError('Invalid exit fit settings')
    exposure, exits = defaultdict(float), defaultdict(float)
    transitions = defaultdict(lambda: defaultdict(float))
    durations, source_games = defaultdict(list), set()
    keys = set()
    for row in observations:
        key = (row['game_id'], row['identity'])
        if key in keys:
            raise ValueError('Duplicate exit observation')
        keys.add(key)
        if timestamp(row['game_ended_at']) >= timestamp(cutoff):
            raise ValueError('Exit label reaches training boundary')
        if row.get('confidence') != 'adjudicated' or not row.get('source_ref'):
            raise ValueError('Adjudicated sourced exit labels required')
        if row.get('observed_at') and timestamp(row['observed_at']) > timestamp(cutoff) and not row.get('retrospective'):
            raise ValueError('Later exit observation requires retrospective designation')
        if row.get('reason') not in ('injury', 'none', 'benching', 'rest'):
            raise ValueError('Unsupported exit cause')
        role = row['role']
        risk = float(row['at_risk_segments'])
        if not 0 < risk <= segments or not np.isfinite(risk):
            raise ValueError('Invalid exit exposure')
        exposure[role] += risk
        if row['reason'] == 'injury':
            start = row['exit_segment']
            end = row.get('return_segment', segments)
            if not isinstance(start, int) or not isinstance(end, int) or not 0 <= start < end <= segments:
                raise ValueError('Invalid exit/return timing')
            exits[role] += 1
            durations[role].append(end - start)
            source_games.add(row['game_id'])
            for action, recipients in row.get('replacement_counts', {}).items():
                if action not in ('targets', 'carries'):
                    raise ValueError('Invalid replacement action')
                for recipient_role, count in recipients.items():
                    if not np.isfinite(count) or count < 0:
                        raise ValueError('Invalid replacement count')
                    transitions[(role, action)][recipient_role] += count
    if not exposure:
        raise ValueError('Exit exposure labels required')
    pooled = sum(exits.values()) / sum(exposure.values())
    roles = {}
    for role, risk in exposure.items():
        roles[role] = {'hazard': (exits[role] + prior_exposure * pooled) / (risk + prior_exposure),
                       'exits': exits[role], 'exposure': risk, 'durations': durations[role] or [segments],
                       'replacement': {action: dict(transitions[(role, action)]) for action in ('targets', 'carries')}}
    return {'version': 'nfl-midgame-exits-v1', 'training_cutoff': cutoff, 'segments': segments,
            'roles': roles, 'pooled_hazard': pooled, 'observations': len(observations),
            'injury_game_ids': sorted(source_games), 'source_sha256': digest(observations),
            'limits': ['Only adjudicated injury exits fitted; benching/rest are censored exposure.',
                       'Segment hazard is constant within role; timing/return duration is a pooled approximation.']}


def participation(rng, players, draws, fit, decision_at):
    if timestamp(fit['training_cutoff']) > timestamp(decision_at):
        raise ValueError('Exit fit after decision')
    segments = fit['segments']
    result = np.ones((draws, segments, len(players)), dtype=bool)
    counts = {p['identity']: 0 for p in players}
    for j, player in enumerate(players):
        if player.get('status') == 'out':
            result[:, :, j] = False
            continue
        role = player.get('position', 'UNKNOWN')
        if role not in fit['roles']:
            if not player.get('residual'):
                raise ValueError(f'No exit exposure evidence for role {role}')
            evidence = {'hazard': fit['pooled_hazard'], 'durations': [duration for r in fit['roles'].values() for duration in r['durations']]}
        else:
            evidence = fit['roles'][role]
        hazard = evidence['hazard']
        if not np.isfinite(hazard) or not 0 <= hazard <= 1:
            raise ValueError('Invalid exit hazard')
        for draw in range(draws):
            opportunities = np.flatnonzero(rng.random(segments) < hazard)
            if len(opportunities):
                start = int(opportunities[0])
                duration = int(rng.choice(evidence['durations']))
                if duration < 1:
                    raise ValueError('Invalid return duration')
                result[draw, start:min(segments, start + duration), j] = False
                counts[player['identity']] += 1
    return result, counts


def redistributed_shares(shares, available, players, fit, action):
    shares = np.asarray(shares, float)
    available = np.asarray(available, bool)
    if shares.shape != available.shape or not np.isfinite(shares).all() or (shares < 0).any() or abs(shares.sum() - 1) > 1e-8:
        raise ValueError('Invalid remaining shares')
    result = shares * available
    for j in np.flatnonzero(~available):
        if shares[j] == 0:
            continue
        role = players[j].get('position', 'UNKNOWN')
        transitions = fit['roles'].get(role, {}).get('replacement', {}).get(action, {})
        recipients = np.array([transitions.get(p.get('position', 'UNKNOWN'), 0.) * shares[k]
                               if available[k] else 0. for k, p in enumerate(players)])
        if recipients.sum() == 0:
            recipients = np.array([float(available[k] and p.get('residual', False)) for k, p in enumerate(players)])
        if recipients.sum() == 0:
            raise ValueError('No evidenced or unresolved replacement recipient')
        result += shares[j] * recipients / recipients.sum()
    if abs(result.sum() - 1) > 1e-8 or (result[~available] != 0).any():
        raise AssertionError('Post-exit allocation is not conserved')
    return result / result.sum()
