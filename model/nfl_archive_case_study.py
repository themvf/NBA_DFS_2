"""Auditable archive pilot; never treats an absent result as zero fantasy points."""
from __future__ import annotations

import math
import re
import unicodedata
from collections import Counter

import numpy as np
from scipy.optimize import Bounds, LinearConstraint, milp

from model.nfl_dfs_historical import OFFENSE_FIELDS, draftkings_points

K_FIELDS = ('pat_made', 'fg_made_0_19', 'fg_made_20_29', 'fg_made_30_39',
            'fg_made_40_49', 'fg_made_50_59', 'fg_made_60_')


def normalized_name(name):
    value = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode().lower()
    words = re.sub(r'[^a-z0-9 ]', '', value).split()
    while words and words[-1] in ('jr', 'sr', 'ii', 'iii', 'iv'):
        words.pop()
    return ''.join(words)


def team_key(team):
    return {'LAR': 'LA', 'WSH': 'WAS', 'JAC': 'JAX'}.get(team, team)


def checked_points(position, stats):
    fields = K_FIELDS if position == 'K' else OFFENSE_FIELDS
    if position not in ('QB', 'RB', 'WR', 'TE', 'K'):
        raise ValueError('DST requires the component-backed results ledger')
    missing = [k for k in fields if not isinstance(stats.get(k), (float, int))
               or not math.isfinite(stats[k])]
    if missing:
        raise ValueError('Missing/nonfinite scoring fields: ' + ', '.join(missing))
    return round(draftkings_points(position, stats), 4)


def solve_lineup(entries, contest_format, objective):
    """Exact binary assignment over provided scored candidates, not the unknown pool.

    Archived roster-slot salaries/multipliers are authoritative. One physical
    player cannot fill two slots. Missing objective values are excluded explicitly.
    """
    slots = {'CPT': 1, 'FLEX': 5} if contest_format == 'showdown' else {
        'QB': 1, 'RB': 2, 'WR': 3, 'TE': 1, 'FLEX': 1, 'DST': 1}
    if contest_format not in ('classic', 'showdown'):
        raise ValueError('Only full-game Classic and Showdown supported')
    pool = sorted([e for e in entries if e.get(objective) is not None
                   and e['salary'] is not None and e['roster_slot'] in slots],
                  key=lambda e: (e['player_id'], e['roster_slot']))
    if not pool:
        raise ValueError('No scored candidates')
    rows, lower, upper = [], [], []

    def constraint(values, lo=-np.inf, hi=np.inf):
        rows.append(values); lower.append(lo); upper.append(hi)

    for slot, count in slots.items():
        constraint([int(e['roster_slot'] == slot) for e in pool], count, count)
    for player in sorted({e['player_id'] for e in pool}):
        constraint([int(e['player_id'] == player) for e in pool], hi=1)
    constraint([e['salary'] for e in pool], hi=50000)
    if contest_format == 'showdown':
        for team in sorted({e['team_nflverse'] for e in pool}):
            constraint([int(e['team_nflverse'] == team) for e in pool], hi=5)
    else:
        # At least two different games: no single game can supply all nine slots.
        for game in sorted({e['game_id'] for e in pool}):
            constraint([int(e['game_id'] == game) for e in pool], hi=8)
    scores = np.array([e[objective] * float(e['scoring_multiplier']) for e in pool])
    result = milp(-scores, integrality=np.ones(len(pool)), bounds=Bounds(0, 1),
                  constraints=LinearConstraint(np.array(rows), lower, upper),
                  options={'mip_rel_gap': 0.0, 'time_limit': 120})
    if result.status != 0 or result.mip_gap > 1e-8:
        raise ValueError(f'Optimality not certified: {result.message}')
    chosen = [dict(e, slot_points=round(float(scores[i]), 4))
              for i, e in enumerate(pool) if result.x[i] > .5]
    assert len({e['player_id'] for e in chosen}) == sum(slots.values())
    assert Counter(e['roster_slot'] for e in chosen) == Counter(slots)
    assert sum(e['salary'] for e in chosen) <= 50000
    known = [e['actual'] * float(e['scoring_multiplier']) for e in chosen if e.get('actual') is not None]
    return {'objective': objective, 'objective_points': round(sum(e['slot_points'] for e in chosen), 4),
            'salary': sum(e['salary'] for e in chosen), 'players': chosen,
            'actual_points': round(sum(known), 4) if len(known) == len(chosen) else None,
            'known_actual_subtotal': round(sum(known), 4),
            'unresolved_selected': [e['player_name'] for e in chosen if e.get('actual') is None],
            'solver': 'scipy.optimize.milp/HiGHS', 'solver_status': int(result.status),
            'mip_gap': float(result.mip_gap), 'legality_passed': True,
            'candidate_players': len({e['player_id'] for e in pool})}
