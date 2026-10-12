"""Prop lines read back as expected stats and DK points (model/nfl_prop_implied.py)."""

import math

import pytest

from model import nfl_prop_implied as pim


def test_even_two_way_price_reads_as_the_line():
    assert pim.p_over(-110, -110) == pytest.approx(0.5)
    assert pim.market_mean("player_receptions", 3.5, -110, -110) == pytest.approx(3.5)


def test_a_juiced_over_moves_the_mean_above_the_line():
    assert pim.market_mean("player_reception_yds", 50.5, -140, 110) > 50.5
    assert pim.market_mean("player_reception_yds", 50.5, 110, -140) < 50.5


def test_poisson_read_of_an_over_price():
    # P(X >= 2) = 0.5 for passing TDs at 1.5 -> lambda ~ 1.68.
    lam = pim.market_mean("player_pass_tds", 1.5, -110, -110)
    assert 1 - math.exp(-lam) * (1 + lam) == pytest.approx(0.5, abs=1e-6)


def test_missing_price_is_missing_not_zero():
    assert pim.market_mean("player_rush_yds", 60.5, None, -110) is None
    assert pim.stat_line("WR", {"player_receptions": {"line": 4.5, "over": -110, "under": -110}}) is None


def test_receiver_stat_line_and_points():
    lines = {"player_receptions": {"line": 4.5, "over": -110, "under": -110},
             "player_reception_yds": {"line": 55.5, "over": -110, "under": -110},
             "player_anytime_td": {"yes": 150}}
    s = pim.stat_line("WR", lines)
    td = -math.log(1 - (100 / 250) / pim.ANYTIME_TD_MARGIN)
    assert s["receptions"] == pytest.approx(4.5) and s["receiving_yards"] == pytest.approx(55.5)
    assert s["scrimmage_tds"] == pytest.approx(td)
    base = 4.5 + 5.55 + 6 * td
    pts = pim.dk_points("WR", s)
    assert base < pts < base + 3  # plus the probability-weighted 100-yard bonus


def test_quarterback_without_an_interception_line_uses_the_prior():
    lines = {"player_pass_yds": {"line": 240.5, "over": -110, "under": -110},
             "player_pass_tds": {"line": 1.5, "over": -110, "under": -110}}
    s = pim.stat_line("QB", lines)
    assert s["passing_interceptions"] == 0.8 and s["rushing_yards"] == 0.0
