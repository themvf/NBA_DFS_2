from __future__ import annotations

from collections import defaultdict

import numpy as np


def depth_bucket(depth):
    if depth is None or not np.isfinite(depth):
        return 'unknown'
    return 'short' if depth < 10 else 'intermediate' if depth < 20 else 'deep'


def fit_gain_profiles(history, prior_events=30.):
    if not np.isfinite(prior_events) or prior_events <= 0:
        raise ValueError('Positive gain prior required')
    profiles, peers = defaultdict(list), defaultdict(list)
    for h in history:
        if h.get('event_reconciled') is False:
            continue
        for event in h['events']:
            if event['action'] not in ('targets', 'carries'):
                continue
            record = {'action': event['action'], 'caught': bool(event['caught']), 'yards': float(event['yards']),
                      'depth': depth_bucket(event.get('air_yards')), 'air_yards': event.get('air_yards'),
                      'yac': event.get('yards_after_catch'), 'yardline': event.get('yardline_100')}
            if not np.isfinite(record['yards']):
                raise ValueError('Nonfinite credited gain')
            profiles[event['identity']].append(record)
            peers[event.get('position', 'UNKNOWN')].append(record)
    if not peers:
        raise ValueError('Reconciled gain evidence required')
    return {'method': 'role_depth_conditioned_empirical', 'prior_events': prior_events,
            'players': dict(profiles), 'peers': dict(peers),
            'measured_depth_events': sum(e['air_yards'] is not None for events in peers.values() for e in events),
            'measured_yac_events': sum(e['yac'] is not None for events in peers.values() for e in events),
            'limits': ['Empirical support does not extrapolate beyond observed gains.',
                       'Historical field-position context is sampled, not a sequential live field process.']}


def sample_touch(rng, fit, player, action, count, catch_adjustment=0., yard_adjustment=0.):
    catches, yards = sample_draws(rng, fit, player, action, np.array([count]), catch_adjustment, yard_adjustment)
    return int(catches[0]), int(yards[0])


def sampling_population(fit, player, action, caught_only=False, cache=None):
    key = (id(fit), player['identity'], player.get('position', 'UNKNOWN'), action, caught_only)
    if cache is not None and key in cache:
        return cache[key]
    def eligible(event):
        return event['action'] == action and (not caught_only or event['caught'])
    own = [e for e in fit['players'].get(player['identity'], []) if eligible(e)]
    peers = [e for e in fit['peers'].get(player.get('position', 'UNKNOWN'), []) if eligible(e)]
    if not peers:
        peers = [e for events in fit['peers'].values() for e in events if eligible(e)]
    if not peers:
        raise ValueError('Action efficiency evidence required')
    population = own + peers
    probabilities = np.r_[np.ones(len(own)), np.full(len(peers), fit['prior_events'] / len(peers))]
    probabilities /= probabilities.sum()
    if cache is not None:
        cache[key] = (population, probabilities)
    return population, probabilities


def sample_completed_yards(rng, fit, player, catches, cache=None):
    return sample_gained_yards(rng, fit, player, 'targets', catches, True, cache)


def sample_gained_yards(rng, fit, player, action, count, caught_only=False, cache=None):
    if not isinstance(count, (int, np.integer)) or count < 0:
        raise ValueError('Nonnegative completed count required')
    if count == 0:
        return 0
    population, probabilities = sampling_population(fit, player, action, caught_only, cache)
    key = ('cdf', id(probabilities))
    if cache is not None and key in cache:
        cumulative = cache[key]
    else:
        cumulative = np.cumsum(probabilities)
        if cache is not None:
            cache[key] = cumulative
    sampled = np.minimum(np.searchsorted(cumulative, rng.random(count)), len(population) - 1)
    return int(np.rint(sum(min(population[i]['yards'], population[i]['yardline']) if population[i]['yardline'] is not None else population[i]['yards'] for i in sampled)))


def sample_draws(rng, fit, player, action, counts, catch_adjustment=0., yard_adjustment=0., cache=None):
    counts = np.asarray(counts)
    if counts.ndim != 1 or (counts < 0).any() or not np.equal(counts, np.rint(counts)).all():
        raise ValueError('Invalid realized opportunity counts')
    catches = np.zeros(len(counts), dtype=int)
    yards = np.zeros(len(counts), dtype=float)
    owners = np.repeat(np.arange(len(counts)), counts.astype(int))
    if not len(owners):
        return catches, yards.astype(int)
    population, weights = sampling_population(fit, player, action, cache=cache)
    depths = np.array([e['depth'] for e in population])
    adjustments = np.asarray(catch_adjustment, dtype=float)
    if adjustments.ndim > 1 or (adjustments.ndim == 1 and len(adjustments) != len(counts)) or not np.isfinite(adjustments).all():
        raise ValueError('Misaligned catch context')
    opportunities = rng.choice(len(population), size=len(owners), p=weights)
    for bucket in np.unique(depths[opportunities]):
        members = np.flatnonzero(depths[opportunities] == bucket)
        matching = [(e, w) for e, w in zip(population, weights) if e['depth'] == bucket]
        probability = sum(w * e['caught'] for e, w in matching) / sum(w for _, w in matching)
        if action == 'targets':
            local = adjustments if adjustments.ndim == 0 else adjustments[owners[members]]
            members = members[rng.random(len(members)) < np.clip(probability + local, 0, 1)]
        successful = [(e, w) for e, w in matching if action == 'carries' or e['caught']]
        if not successful:
            successful = [(e, w) for e, w in zip(population, weights) if e['caught']]
        if not successful:
            continue
        gain_weights = np.array([w for _, w in successful])
        gain_weights /= gain_weights.sum()
        selected = rng.choice(len(successful), size=len(members), p=gain_weights)
        values = np.array([e['yards'] for e, _ in successful]) + yard_adjustment
        bounds = np.array([e['yardline'] if e['yardline'] is not None else np.inf for e, _ in successful])
        gained = np.minimum(values[selected], bounds[selected])
        catches += np.bincount(owners[members], minlength=len(counts))
        yards += np.bincount(owners[members], weights=gained, minlength=len(counts))
    return catches, np.rint(yards).astype(int)
