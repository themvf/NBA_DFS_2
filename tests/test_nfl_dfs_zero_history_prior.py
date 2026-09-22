"""v4 first-appearance cohort prior: construction and study guards.

The defect: v3's "position prior" is the 400 most recent stat rows of the
position, i.e. starters, applied at full weight to anyone with < 2 games.
v4 swaps the peer population for each player's FIRST recorded game and
leaves everyone with 6+ games on v3 unchanged.
"""

from __future__ import annotations

import pytest

from model.nfl_dfs_historical import HistoricalWeek, ProjectionContext, project_player
from model.nfl_dfs_zero_history_prior import (
    DELEGATE_TO_V3_FROM_GAMES, MODEL_VERSION, first_appearance_cohort, project_player_v4,
)
from model.nfl_dfs_zero_history_prior_study import (
    INSPECTED_WEEKS, MIN_HIST0_ROWS, MIN_WEEKS, WINDOW_WEEKS, paired_rows, paired_summary, verdicts,
)


def row(pid, season, week, yards, *, position="WR", tds=0):
    return HistoricalWeek(player_id=pid, player_gsis_id=f"g{pid}", player_name=f"P{pid}", position=position,
                          season=season, week=week, team="T", opponent="O",
                          stats={"receiving_yards": yards, "receptions": yards / 10, "receiving_tds": tds})


def starters_and_backups():
    rows = []
    # 30 starters with 12 games each at ~80 yards (first game 20 yards).
    for pid in range(1, 31):
        rows.append(row(pid, 2025, 1, 20))
        rows.extend(row(pid, 2025, w, 80) for w in range(2, 13))
    # 20 backups who appeared once for 5 yards.
    rows.extend(row(pid, 2025, 6, 5) for pid in range(100, 120))
    return rows


def test_cohort_is_one_first_game_per_other_player_before_cutoff():
    rows = starters_and_backups() + [row(500, 2026, 3, 150)]      # after the cutoff: invisible
    prior = [r for r in rows if (r.season, r.week) < (2026, 1)]
    cohort = first_appearance_cohort("WR", prior, exclude_player_id=1)
    ids = [r.player_id for r in cohort]
    assert len(ids) == len(set(ids)) == 49                        # 30 starters + 20 backups - self
    assert 1 not in ids and 500 not in ids
    assert all(r.week == 1 for r in cohort if r.player_id < 100)  # starters contribute their FIRST game
    assert all(r.week == 6 for r in cohort if r.player_id >= 100)


def test_zero_history_player_is_projected_as_a_first_game_not_a_starter():
    rows = starters_and_backups()
    v3 = project_player(player_id=999, player_gsis_id=None, player_name="New", position="WR",
                        historical_rows=rows, cutoff_season=2026, cutoff_week=1)
    v4 = project_player_v4(player_id=999, player_gsis_id=None, player_name="New", position="WR",
                           historical_rows=rows, cutoff_season=2026, cutoff_week=1)
    assert v3.projection_status == "position_prior" and v4.projection_status == "cohort_prior"
    assert v4.model_version == MODEL_VERSION and v4.feature_snapshot["v4_path"] == "cohort"
    # v3 draws from recent starter weeks (~80 yds); v4 from first games (20 or 5 yds).
    assert v3.model_proj_fpts > 7.0
    assert v4.model_proj_fpts < 3.5
    assert v4.feature_snapshot["cohort_rows"] == 50
    assert v4.feature_snapshot["player_weight"] == 0.0


def test_one_to_five_games_delegate_to_v3_exactly():
    """The cohort-shrink variant for 1-5 game players regressed that cohort on
    the discovery weeks and was dropped before registration; v4 must leave
    anyone with a single game of history on v3."""
    assert DELEGATE_TO_V3_FROM_GAMES == 1
    rows = starters_and_backups() + [row(777, 2025, 10, 100), row(777, 2025, 11, 100)]
    common = dict(player_id=777, player_gsis_id=None, player_name="Two", position="WR",
                  historical_rows=rows, cutoff_season=2026, cutoff_week=1)
    v3, v4 = project_player(**common), project_player_v4(**common)
    assert v4.feature_snapshot["v4_path"] == "delegated_to_v3"
    assert (v4.model_proj_fpts, v4.floor_fpts, v4.ceiling_fpts) == (v3.model_proj_fpts, v3.floor_fpts, v3.ceiling_fpts)
    one = dict(common, player_id=778, player_name="One", historical_rows=rows + [row(778, 2025, 12, 40)])
    assert project_player_v4(**one).feature_snapshot["v4_path"] == "delegated_to_v3"


def test_six_plus_games_delegate_to_v3_exactly():
    rows = starters_and_backups()
    common = dict(player_id=5, player_gsis_id=None, player_name="P5", position="WR",
                  historical_rows=rows, cutoff_season=2026, cutoff_week=1,
                  context=ProjectionContext(team_implied_total=24.0))
    v3, v4 = project_player(**common), project_player_v4(**common)
    assert v4.feature_snapshot["v4_path"] == "delegated_to_v3"
    assert (v4.model_proj_fpts, v4.floor_fpts, v4.ceiling_fpts) == (v3.model_proj_fpts, v3.floor_fpts, v3.ceiling_fpts)
    assert v4.history_games >= DELEGATE_TO_V3_FROM_GAMES


def test_paired_rows_only_where_both_streams_scored():
    def rep(rows):
        return {"week": 4, "rows": rows}
    a = rep([{"ff_player_id": 1, "position": "RB", "cohort": "hist_0", "name": "a", "projected": 8.0, "actual": 2.0, "error": -6.0, "absolute_error": 6.0},
             {"ff_player_id": 2, "position": "RB", "cohort": "hist_0", "name": "b", "projected": 8.0, "actual": 2.0, "error": -6.0, "absolute_error": 6.0}])
    b = rep([{"ff_player_id": 1, "position": "RB", "cohort": "hist_0", "name": "a", "projected": 3.0, "actual": 2.0, "error": -1.0, "absolute_error": 1.0}])
    pairs = paired_rows(a, b)
    assert [p["ff_player_id"] for p in pairs] == [1]
    assert pairs[0]["d_abs"] == pytest.approx(-5.0)


def test_no_verdict_before_the_window_is_complete_and_floors_are_enforced():
    assert INSPECTED_WEEKS == (1, 2, 3) and WINDOW_WEEKS == tuple(range(4, 11))
    assert (MIN_WEEKS, MIN_HIST0_ROWS) == (5, 40)
    rows = [{"week": w, "position": "RB", "cohort": "hist_0", "d_abs": -2.0, "d_err": 1.0, "v3": 8.0, "v4": 3.0, "actual": 2.0}
            for w in WINDOW_WEEKS for _ in range(10)]
    summary = paired_summary(rows, iters=300)
    partial = verdicts(summary, weeks_scorable={4, 5, 6})
    assert partial["status"] == "no_verdict" and "not yet scorable" in partial["reason"]
    full = verdicts(summary, weeks_scorable=set(WINDOW_WEEKS))
    assert full["status"] == "graded"
    assert full["RB"]["verdict"] == "PASS"                         # 70 rows, 7 weeks, dMAE -2 everywhere
    assert full["QB"]["verdict"] == "insufficient"
    assert full["promote"] is False                               # QB/WR/TE insufficient => no promotion
