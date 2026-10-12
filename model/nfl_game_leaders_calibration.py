"""Production calibration for the game-leader model, from `nfl-game-leaders-calibration-v1`.

docs/nfl-game-leaders-calibration-study.md, artifacts/nfl-game-leaders/calibration-v1/fit.json:
tuned on 2023-2025 (689 games), graded once on 2022 (245 games). Two choices,
both registered before the 2022 look:

* roles use last season's usage as PRIOR_WEIGHT pseudo-games (variant C2);
* each market's leader shares are tempered, p ~ p^(1/T), with T fitted per
  market. Every T is above 1: the raw model is overconfident everywhere.

On 2022 the calibrated model beat the recent-average baseline for rushing and
total yards (95% intervals below zero) and did not for receiving yards or
receptions. Those two carry no claim. Changing any number here is a new
calibration version and a new study.
"""
from __future__ import annotations

VERSION = 'nfl-game-leaders-calibration-v1'
PRIOR_WEIGHT = 2.0
TEMPERATURES = {'rushing_yards': 1.30, 'receiving_yards': 1.05, 'receptions': 1.15, 'total_yards': 1.25}
PASSED_MARKETS = ('rushing_yards', 'total_yards')


def production_settings(**overrides):
    """The game-leader Settings every production forecast uses (batch, single forecast, market refresh)."""
    from model.nfl_game_leaders import Settings
    return Settings(temperatures=dict(TEMPERATURES), **overrides)


def temper(shares, temperature):
    """p_i ~ p_i^(1/T), renormalised. T=1 is the identity; T>1 flattens."""
    if temperature == 1:
        return dict(shares)
    powered = {k: max(v, 0.) ** (1 / temperature) for k, v in shares.items()}
    total = sum(powered.values())
    return {k: v / total for k, v in powered.items()} if total > 0 else dict(shares)
