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


def test_team_must_be_one_of_the_two_teams_in_the_game():
    from research.nfl_prop_implied import resolve_team
    games = {"ARI", "DET"}
    # Roster says HOU (stale), but he played for ARI weeks 1-4: the line belongs to ARI.
    assert resolve_team("HOU", [(1, "ARI"), (4, "ARI")], 5, games) == "ARI"
    # Already on one of the game's teams: unchanged.
    assert resolve_team("DET", [], 5, games) == "DET"
    # No evidence for either team: unknown, never a third team.
    assert resolve_team("HOU", [(3, "HOU")], 5, games) is None
    # Nearest week wins when he played for both (a trade between the two teams).
    assert resolve_team("MIN", [(1, "ARI"), (4, "DET")], 5, games) == "DET"
    # Alias codes are normalized before comparing.
    assert resolve_team("LA", [], 2, {"LAR", "NYG"}) == "LAR"


def test_qb_rush_fallback_fills_only_missing_markets():
    """nfl-market-anchor-v2: a QB with no anytime-TD / rushing-yards line takes
    the model's own rushing means; without the fallback the research reading
    (missing = 0) is unchanged."""
    from model.nfl_prop_implied import CALIBRATION_V1, dk_points, stat_line
    lines = {"player_pass_yds": {"line": 245.5, "over": -118, "under": -108},
             "player_pass_tds": {"line": 1.5, "over": -130, "under": 102}}
    model = {"rushing_yards": 38.2, "rushing_tds": 0.31}
    used: list[str] = []
    s = stat_line("QB", lines, calibration=CALIBRATION_V1, qb_rush_fallback=model, used=used)
    assert s["rushing_yards"] == 38.2 and s["scrimmage_tds"] == 0.31
    assert used == ["player_anytime_td", "player_rush_yds"]
    plain = stat_line("QB", lines, calibration=CALIBRATION_V1)
    assert plain["rushing_yards"] == 0.0 and plain["scrimmage_tds"] == 0.0
    assert dk_points("QB", s) > dk_points("QB", plain) + 5
    wr = {"player_receptions": {"line": 5.5, "over": -129, "under": 104},
          "player_reception_yds": {"line": 65.5, "over": -111, "under": -115}, "player_anytime_td": {"yes": 155}}
    assert stat_line("WR", wr, qb_rush_fallback=model)["rushing_yards"] == 0.0
