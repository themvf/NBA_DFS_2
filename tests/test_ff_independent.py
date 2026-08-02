import pandas as pd

from ingest.ff_independent import build_adp_lookup, compute_bye_weeks, normalize_team, project_player, rank_rows


def test_team_abbreviations_match_application_canonicals() -> None:
    assert normalize_team("LA") == "LAR"
    assert normalize_team("WAS") == "WSH"
    assert normalize_team("BUF") == "BUF"


def test_bye_weeks_are_derived_from_missing_regular_season_week() -> None:
    rows = []
    for week in range(1, 19):
        if week != 11:
            rows.append({"season": 2026, "game_type": "REG", "week": week, "home_team": "LA", "away_team": "ARI"})
    result = compute_bye_weeks(pd.DataFrame(rows), 2026)
    assert result["LAR"] == 11


def test_adp_lookup_normalizes_positions_and_teams() -> None:
    lookup = build_adp_lookup({"players": [
        {"name": "Example Kicker", "position": "PK", "team": "WAS", "adp": 190.2},
        {"name": "Washington Commanders", "position": "DEF", "team": "WAS", "adp": 175.0},
    ]})
    assert lookup[("examplekicker", "K")]["adp"] == 190.2
    assert lookup[("WSH", "DST")]["adp"] == 175.0


def test_recent_history_has_more_weight_than_old_history() -> None:
    player = {"position": "WR", "rookie": False, "depth_order": 1, "injury_status": None, "draft_number": None}
    histories = [
        {"season": 2023, "games": 17, "fantasy_points_std": 85, "fantasy_points_ppr": 170},
        {"season": 2024, "games": 17, "fantasy_points_std": 102, "fantasy_points_ppr": 187},
        {"season": 2025, "games": 17, "fantasy_points_std": 170, "fantasy_points_ppr": 272},
    ]
    projection = project_player(player, histories, "PPR", 2026)
    assert projection.points > 200
    assert projection.explanation["market_data_used"] is False
    assert projection.explanation["method"] == "history_regression"
    assert [row["weight"] for row in projection.explanation["season_inputs"]] == [0.15, 0.30, 0.55]
    assert projection.explanation["weighted_history_ppg"] is not None
    assert projection.explanation["regressed_ppg"] is not None
    assert projection.explanation["regression_prior_games"] == 4.0
    assert projection.explanation["regression_sample_games"] == 51
    assert projection.explanation["baseline_games"] == 17.0
    assert projection.points == 224.89
    assert projection.explanation["not_modeled"] == [
        "current teammates", "offensive line", "coaching/play-caller", "future schedule",
    ]


def test_small_sample_is_still_strongly_regressed() -> None:
    player = {"position": "WR", "rookie": False, "depth_order": 1, "injury_status": None, "draft_number": None}
    history = [{"season": 2025, "games": 1, "fantasy_points_std": 20, "fantasy_points_ppr": 30}]
    projection = project_player(player, history, "PPR", 2026)
    assert projection.explanation["regression_sample_games"] == 1
    assert projection.explanation["regressed_ppg"] == 12.8
    assert projection.points == 217.6


def test_injury_changes_availability_but_not_seventeen_game_baseline() -> None:
    history = [{"season": 2025, "games": 17, "fantasy_points_std": 170, "fantasy_points_ppr": 272}]
    healthy = {"position": "WR", "rookie": False, "depth_order": 1, "injury_status": None, "draft_number": None}
    injured = {**healthy, "injury_status": "OUT"}
    healthy_projection = project_player(healthy, history, "PPR", 2026)
    injured_projection = project_player(injured, history, "PPR", 2026)
    assert injured_projection.points == healthy_projection.points
    assert injured_projection.expected_games < healthy_projection.expected_games
    assert injured_projection.explanation["availability_adjustment_applied_to_baseline"] is False


def test_rookie_projection_uses_draft_capital_and_depth() -> None:
    starter = {"position": "RB", "rookie": True, "depth_order": 1, "injury_status": None, "draft_number": 25}
    backup = {"position": "RB", "rookie": True, "depth_order": 4, "injury_status": None, "draft_number": 180}
    assert project_player(starter, [], "PPR", 2026).points > project_player(backup, [], "PPR", 2026).points
    assert project_player(starter, [], "PPR", 2026).explanation["method"] == "rookie_prior"


def test_rank_rows_uses_value_over_replacement() -> None:
    rows = []
    for position, points in (("QB", 300), ("QB", 250), ("RB", 240), ("RB", 180), ("WR", 220), ("TE", 160), ("K", 120), ("DST", 105)):
        rows.append({"name": f"{position}-{points}", "position": position, "our_projected_points": points})
    ranked = rank_rows(rows)
    assert sorted(row["our_rank"] for row in ranked) == list(range(1, len(rows) + 1))
    assert all(row["position_rank"] >= 1 for row in ranked)
