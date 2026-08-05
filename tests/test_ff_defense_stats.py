import pandas as pd

from ingest.ff_defense_stats import MIN_TEAMS_FOR_REGRESSION, compute_season_ratings


def _row(team: str, opponent: str, position: str, std: float, ppr: float) -> dict:
    return {
        "season_type": "REG", "team": team, "opponent_team": opponent, "position": position,
        "fantasy_points": std, "fantasy_points_ppr": ppr,
    }


def test_games_played_denominator_comes_from_schedule_not_stat_rows() -> None:
    """A team that played 3 real games but only has 2 rows of conceded stats
    (e.g. a shutout game with no offensive stat line at all) must still
    divide by 3 -- the schedule is the source of truth for games played,
    never the count of rows that happen to have stats (Refinement #2)."""
    weekly = pd.DataFrame([
        _row("B", "A", "RB", 10.0, 15.0),
        _row("C", "A", "RB", 5.0, 5.0),
        # No third row for opponent_team=A -- e.g. A's defense pitched a
        # shutout against its week-3 opponent and no RB touched the ball.
        _row("A", "B", "RB", 1.0, 1.0),
        _row("A", "C", "RB", 1.0, 1.0),
    ])
    schedule_ctx = {
        "games_played": {"A": 3, "B": 1, "C": 1},
        "opponents": {"A": ["B", "C"], "B": ["A"], "C": ["A"]},
    }
    out = compute_season_ratings(weekly, schedule_ctx, 2025)
    a_row = out[(out["team_abbrev"] == "A") & (out["position"] == "RB")].iloc[0]
    assert a_row["games"] == 3
    assert a_row["fpts_allowed_std_pg"] == (10.0 + 5.0) / 3


def test_rank_one_is_the_stingiest_defense() -> None:
    """Rank 1 = allows the FEWEST points (hardest matchup) -- decided
    explicitly to avoid a sign-flip bug, never assumed (Refinement #1)."""
    weekly = pd.DataFrame([
        _row("A", "B", "RB", 6.0, 9.0), _row("C", "A", "RB", 4.0, 6.0),
        _row("A", "C", "RB", 20.0, 25.0), _row("B", "A", "RB", 6.0, 9.0),
        _row("B", "C", "RB", 8.0, 12.0), _row("C", "B", "RB", 14.0, 18.0),
    ])
    schedule_ctx = {
        "games_played": {"A": 2, "B": 2, "C": 2},
        "opponents": {"A": ["B", "C"], "B": ["A", "C"], "C": ["A", "B"]},
    }
    out = compute_season_ratings(weekly, schedule_ctx, 2025)
    ranked = out[out["position"] == "RB"].set_index("team_abbrev")
    # A allowed 10/2=5 pg std (stingiest) -> rank 1; C allowed 28/2=14 pg -> rank 3
    assert ranked.loc["A", "fpts_allowed_std_pg"] == 5.0
    assert ranked.loc["A", "rank_std"] == 1
    assert ranked.loc["C", "fpts_allowed_std_pg"] == 14.0
    assert ranked.loc["C", "rank_std"] == 3


def test_adjustment_falls_back_to_raw_below_min_teams() -> None:
    """The Option A residual regression needs enough teams to fit a stable
    2-parameter OLS. Below MIN_TEAMS_FOR_REGRESSION it must fall back to the
    raw number, never fit noise and call it 'adjusted'."""
    weekly = pd.DataFrame([
        _row("A", "B", "RB", 6.0, 9.0), _row("B", "A", "RB", 6.0, 9.0),
    ])
    schedule_ctx = {"games_played": {"A": 1, "B": 1}, "opponents": {"A": ["B"], "B": ["A"]}}
    assert 2 < MIN_TEAMS_FOR_REGRESSION
    out = compute_season_ratings(weekly, schedule_ctx, 2025)
    row = out[(out["team_abbrev"] == "A") & (out["position"] == "RB")].iloc[0]
    assert row["fpts_allowed_std_pg_adj"] == row["fpts_allowed_std_pg"]
