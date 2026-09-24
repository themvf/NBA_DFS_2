"""The opponent (defense) term: wired into production, disabled by evidence.

model/nfl_dfs_opponent_screen.py tested it on top of the Vegas team total and
found no held-out gain (delta -0.0019, CI [-0.0084, +0.0044]), so the shipped
default is "off". These tests pin the plumbing, so enabling it later is a
one-key change rather than new code.
"""

import pytest

from model.nfl_dfs_historical import (
    MODEL_CONFIG, HistoricalWeek, ProjectionContext, adjust_stat_line, opponent_factors,
)


def week(pid, position, season, opponent, receiving_yards):
    return HistoricalWeek(player_id=pid, player_gsis_id=str(pid), player_name=str(pid), position=position,
                          season=season, week=1, team="AAA", opponent=opponent,
                          stats={"receiving_yards": receiving_yards, "receptions": 0})


def history():
    rows = []
    # A soft defence (SOFT) allows 20 points to WRs; the rest of the league 10.
    for i in range(40):
        rows.append(week(i, "WR", 2025, "SOFT", 200.0))
        rows.append(week(100 + i, "WR", 2025, f"D{i % 8}", 100.0))
    # An old season in which SOFT was stingy.
    for i in range(200):
        rows.append(week(300 + i, "WR", 2021, "SOFT", 0.0))
    return rows


def test_shipped_default_is_off():
    assert MODEL_CONFIG["opponent_mode"] == "off"
    assert opponent_factors(history(), 2026, "off") == {}


def test_recent_sees_the_current_defence_and_all_is_dragged_by_history():
    recent = opponent_factors(history(), 2026, "recent")[("WR", "SOFT")]
    everything = opponent_factors(history(), 2026, "all")[("WR", "SOFT")]
    assert recent > 1.0, "a defence allowing more than the league scores above 1"
    assert everything < recent, "four-year-old games pull the all-history factor down"


def test_factor_is_shrunk_and_clipped():
    factors = opponent_factors(history(), 2026, "recent")
    assert all(0.80 <= value <= 1.20 for value in factors.values())
    # SOFT allows 23 (200 yards + the 100-yard bonus) against a league mean of
    # 18; sixteen league-average games of shrinkage pull that to 21.57 / 18.
    assert factors[("WR", "SOFT")] == pytest.approx((40 * 23 + 16 * 18) / 56 / 18)


def test_non_skill_positions_are_ignored():
    rows = [HistoricalWeek(player_id=1, player_gsis_id="1", player_name="k", position="K", season=2025,
                           week=1, team="AAA", opponent="SOFT", stats={"pat_made": 3})]
    assert opponent_factors(rows, 2026, "all") == {}


def test_unknown_mode_is_rejected():
    with pytest.raises(ValueError):
        opponent_factors(history(), 2026, "last_three")


def test_factor_moves_yardage_not_touchdowns():
    line = {"receiving_yards": 100.0, "receptions": 5.0, "receiving_tds": 1.0}
    plain = adjust_stat_line("WR", line, ProjectionContext())
    soft = adjust_stat_line("WR", line, ProjectionContext(opponent_factor=1.2))
    assert soft["receiving_yards"] == pytest.approx(100.0 * 1.2 ** MODEL_CONFIG["opponent_exponent"])
    assert soft["receiving_tds"] == plain["receiving_tds"]
