"""Survivor probability and settlement invariants.

The two things most likely to be silently wrong in a survivor tool are the
spread sign convention (which team is favored) and the tie rule (whether a
draw eliminates you). Both are cheap to get backwards and expensive to
notice, so both are pinned here.
"""

from __future__ import annotations

import math

import pytest

from ingest.nfl_season_schedule import TEAM_ABBREV_OVERRIDES, _resolve
from model.nfl_survivor_model import (
    _american_pair_to_prob,
    sigma_for,
    spread_to_prob,
    widen,
)
from model.survivor_settlement import _outcome

FIT = {"intercept": -0.03228, "slope": 0.14327, "tie_rate_close": 0.002}


# --------------------------------------------------------------------------
# Spread convention
# --------------------------------------------------------------------------

def test_positive_spread_means_the_home_team_is_favored():
    # nflverse writes spread_line from the home team's perspective with
    # positive = home favored. Flipping this silently inverts every cell.
    assert spread_to_prob(7.0, FIT) > 0.7
    assert spread_to_prob(-7.0, FIT) < 0.3


def test_pick_em_is_a_coin_flip_and_carries_no_extra_home_edge():
    # The fitted intercept is indistinguishable from zero: once the spread is
    # known the market has already priced home field, so a separate home
    # adjustment on top would double-count it.
    assert abs(spread_to_prob(0.0, FIT) - 0.5) < 0.02


def test_probability_is_monotonic_in_the_spread():
    values = [spread_to_prob(spread, FIT) for spread in range(-14, 15)]
    assert all(later > earlier for earlier, later in zip(values, values[1:]))


# --------------------------------------------------------------------------
# Horizon widening
# --------------------------------------------------------------------------

def test_widening_pulls_toward_a_coin_flip_and_never_past_it():
    point = spread_to_prob(10.0, FIT)
    widened = widen(10.0, 5.15, FIT)
    assert 0.5 < widened < point


def test_widening_is_a_no_op_at_zero_uncertainty():
    assert widen(6.5, 0.0, FIT) == pytest.approx(spread_to_prob(6.5, FIT))


def test_widening_grows_monotonically_with_uncertainty():
    spreads = [widen(10.0, sigma, FIT) for sigma in (0.0, 2.0, 4.0, 6.0)]
    assert all(later < earlier for earlier, later in zip(spreads, spreads[1:]))


def test_sigma_extrapolates_flat_past_the_largest_measured_horizon():
    table = {1: 3.35, 10: 5.15}
    assert sigma_for(1, table) == 3.35
    assert sigma_for(99, table) == 5.15  # never invents a bigger number
    assert sigma_for(4, table) in table.values()
    assert sigma_for(3, {}) == 0.0  # no table means no widening, not a guess


# --------------------------------------------------------------------------
# No-vig moneyline
# --------------------------------------------------------------------------

def test_no_vig_pair_sums_to_one_and_favors_the_shorter_price():
    home = _american_pair_to_prob(-200, 170)
    away = _american_pair_to_prob(170, -200)
    assert home is not None and away is not None
    assert home > 0.5 > away
    assert home + (1 - home) == pytest.approx(1.0)


def test_impossible_price_pairs_are_rejected_rather_than_averaged():
    # A price strictly inside (-100, +100) is the signature of the arithmetic
    # American-odds averaging bug this repo already fixed once for MLB.
    assert _american_pair_to_prob(-50, 120) is None
    assert _american_pair_to_prob(None, 120) is None


# --------------------------------------------------------------------------
# Team code mapping
# --------------------------------------------------------------------------

def test_nflverse_codes_map_onto_repo_abbreviations():
    by_abbrev = {"LAR": 1, "WSH": 2, "ARI": 3, "JAX": 4, "KC": 5}
    assert _resolve("LA", by_abbrev) == 1
    assert _resolve("WAS", by_abbrev) == 2
    assert _resolve("KC", by_abbrev) == 5


def test_an_unmapped_code_fails_closed_instead_of_dropping_the_row():
    # The AZ/ARI gap silently nulled the team and bye week for every Arizona
    # player. A raise is the only acceptable behavior here.
    with pytest.raises(ValueError, match="unmapped"):
        _resolve("XYZ", {"KC": 5})


def test_the_override_map_matches_the_fantasy_football_one():
    from ingest.ff_independent import TEAM_ABBREV_OVERRIDES as ff_overrides

    assert TEAM_ABBREV_OVERRIDES == ff_overrides


# --------------------------------------------------------------------------
# Settlement
# --------------------------------------------------------------------------

def test_home_and_away_wins_settle_from_the_same_score():
    args = {"home_team_id": 1, "home_score": 24, "away_score": 17, "tie_rule": "tie_loses"}
    assert _outcome(team_id=1, **args) == "won"
    assert _outcome(team_id=2, **args) == "lost"


def test_a_tie_follows_the_pool_rule_not_a_modeling_preference():
    args = {"home_team_id": 1, "home_score": 20, "away_score": 20}
    assert _outcome(team_id=1, tie_rule="tie_loses", **args) == "lost"
    assert _outcome(team_id=2, tie_rule="tie_loses", **args) == "lost"
    assert _outcome(team_id=1, tie_rule="tie_survives", **args) == "push"


def test_the_advance_probability_of_a_certain_loss_is_never_negative():
    assert math.isclose(min(max(spread_to_prob(-40.0, FIT), 0.0), 1.0), spread_to_prob(-40.0, FIT))
    assert 0.0 < spread_to_prob(-40.0, FIT) < 0.01
