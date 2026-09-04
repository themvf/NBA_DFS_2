from model.nfl_dfs_historical import (
    HistoricalWeek,
    ProjectionContext,
    adjust_stat_line,
    artifact_digest,
    draftkings_points,
    project_player,
)


def week(player_id: int, season: int, number: int, points_seed: float, position: str = "WR") -> HistoricalWeek:
    return HistoricalWeek(
        player_id=player_id,
        player_gsis_id=f"gsis-{player_id}",
        player_name=f"Player {player_id}",
        position=position,
        season=season,
        week=number,
        team="BUF",
        opponent="MIA",
        stats={"receptions": 5, "receiving_yards": points_seed, "receiving_tds": 0},
    )


def test_draftkings_scoring_preserves_all_three_yardage_bonuses() -> None:
    stats = {
        "passing_yards": 300,
        "passing_tds": 2,
        "passing_interceptions": 1,
        "rushing_yards": 100,
        "rushing_tds": 1,
        "receiving_yards": 100,
        "receiving_tds": 1,
        "receptions": 5,
        "passing_2pt_conversions": 1,
        "fumbles_lost_total": 1,
    }
    assert draftkings_points("QB", stats) == 66.0


def test_cutoff_excludes_target_and_future_weeks() -> None:
    rows = [week(1, 2025, n, float(n * 10)) for n in range(1, 5)]
    rows.extend(week(2, 2025, n, 50.0) for n in range(1, 5))
    result = project_player(
        player_id=1,
        player_gsis_id="gsis-1",
        player_name="Player 1",
        position="WR",
        historical_rows=rows,
        cutoff_season=2025,
        cutoff_week=3,
        seed=7,
    )
    assert result.history_games == 2
    assert result.feature_snapshot["cutoff_week"] == 3
    assert result.model_proj_fpts is not None


def test_projection_is_deterministic_and_monotonic() -> None:
    rows = [week(1, 2024, n, 30.0 + n) for n in range(1, 12)]
    rows.extend(week(2, 2024, n, 20.0 + n) for n in range(1, 12))
    args = dict(
        player_id=1,
        player_gsis_id="gsis-1",
        player_name="Player 1",
        position="WR",
        historical_rows=rows,
        cutoff_season=2025,
        cutoff_week=1,
        seed=42,
    )
    first = project_player(**args)
    second = project_player(**args)
    assert first.as_dict() == second.as_dict()
    assert first.floor_fpts <= first.median_fpts <= first.ceiling_fpts


def test_environment_is_pregame_and_directional() -> None:
    line = {"receiving_yards": 99, "receptions": 5, "receiving_tds": 1}
    low = adjust_stat_line("WR", line, ProjectionContext(team_implied_total=18.0, opponent_factor=0.9))
    high = adjust_stat_line("WR", line, ProjectionContext(team_implied_total=28.0, opponent_factor=1.1))
    assert high["receiving_yards"] > low["receiving_yards"]
    assert high["receiving_tds"] > low["receiving_tds"]


def test_sparse_player_is_explicit_position_prior() -> None:
    rows = [week(2, 2024, n, 40.0) for n in range(1, 6)]
    result = project_player(
        player_id=1,
        player_gsis_id="rookie",
        player_name="Rookie",
        position="WR",
        historical_rows=rows,
        cutoff_season=2025,
        cutoff_week=1,
    )
    assert result.projection_status == "position_prior"
    assert result.confidence == 0.0


def test_dst_projects_from_exact_dk_week_results() -> None:
    rows = [
        HistoricalWeek(
            player_id=10,
            player_gsis_id=None,
            player_name="Buffalo DST",
            position="DST",
            season=2025,
            week=number,
            team="BUF",
            opponent="MIA",
            stats={"fantasy_points": points},
        )
        for number, points in enumerate((8.0, 12.0, 16.0, 10.0), start=1)
    ]
    result = project_player(
        player_id=10,
        player_gsis_id=None,
        player_name="Buffalo DST",
        position="DST",
        historical_rows=rows,
        cutoff_season=2026,
        cutoff_week=1,
        seed=4,
    )

    assert result.projection_status == "historical"
    assert result.model_proj_fpts is not None
    assert result.floor_fpts <= result.median_fpts <= result.ceiling_fpts
    assert result.confidence > 0


def test_artifact_digest_is_order_stable() -> None:
    assert artifact_digest({"b": 2, "a": 1}) == artifact_digest({"a": 1, "b": 2})
