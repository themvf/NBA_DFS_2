from __future__ import annotations

import numpy as np

from model.nfl_midgame_exits import participation
from model.nfl_opportunity_process import allocate_segments


def exit_allocator(forecast, fitted, draws, seed, decision_at):
    players = [{**p, 'team': forecast['team'], 'residual': False} for p in forecast['players'] if p['position'] in ('QB', 'RB', 'WR', 'TE') and p.get('components')]
    rng = np.random.default_rng(seed)
    presence, exited = participation(rng, players, draws, fitted['exit_fit'], decision_at)
    players.append({'identity': 'UNALLOCATED', 'position': 'UNKNOWN', 'residual': True})
    presence = np.concatenate([presence, np.ones((draws, fitted['exit_fit']['segments'], 1), dtype=bool)], axis=2)
    indexes = {p['identity']: j for j, p in enumerate(players)}
    diagnostics = {'exit_draw_counts': exited, 'opportunity_allocation': []}
    class Allocator:
        def participation_fraction(self, draw, identity):
            return float(presence[draw, :, indexes[identity]].mean())

        def __call__(self, draw, generator, action, total, recipients, concentration):
            return allocate(draw, generator, action, total, recipients, concentration)
    def allocate(draw, generator, action, total, recipients, concentration):
        identities = [p['identity'] for p in recipients] + ['UNALLOCATED']
        subset = [players[indexes[i]] for i in identities]
        roles = fitted['roles'].get(forecast['team'], {}).get(action)
        if roles and action in ('targets', 'carries'):
            raw = np.array([roles.get(i, 0.) for i in identities[:-1]] + [0.])
            raw[-1] = max(0., sum(roles.values()) - raw.sum())
        else:
            raw = np.array([p['components'][action]['share'] for p in recipients] + [0.])
            raw[-1] = max(0., 1 - raw.sum())
        if raw.sum() <= 0:
            raw[-1] = 1.
        shares = raw / raw.sum()
        available = presence[draw:draw + 1, :, [indexes[i] for i in identities]]
        allocated = allocate_segments(generator, subset, [total], shares, concentration or 1000.,
                                      available, fitted['exit_fit'], action)[0]
        diagnostics['opportunity_allocation'].append({'draw': draw, 'action': action, 'counts': allocated.tolist(), 'identities': identities})
        counts = allocated.sum(axis=0)
        return counts[:-1].tolist(), int(counts[-1])
    return Allocator(), diagnostics
