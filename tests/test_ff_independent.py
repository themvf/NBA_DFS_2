import pandas as pd

from ingest.ff_independent import (
    _points_allowed_fpts,
    _team_points_allowed_fpts_by_season,
    build_adp_lookup,
    compute_bye_weeks,
    normalize_team,
    project_player,
    rank_rows,
)


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
    assert [row["weight"] for row in projection.explanation["season_inputs"]] == [0.05, 0.20, 0.75]
    assert projection.explanation["weighted_history_ppg"] is not None
    assert projection.explanation["regressed_ppg"] is not None
    assert projection.explanation["regression_prior_games"] == 4.0
    assert projection.explanation["regression_sample_games"] == 51
    assert projection.explanation["baseline_games"] == 17.0
    assert projection.points == 242.23
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
    starter_projection = project_player(starter, [], "PPR", 2026)
    assert starter_projection.explanation["method"] == "rookie_prior"
    assert starter_projection.explanation["model"] == "ff-independent-v1.5"
    assert starter_projection.explanation["draft_number"] == 25
    assert starter_projection.explanation["rookie_prior_points"] == 197.0
    assert starter_projection.explanation["role_factor"] == 1.0
    assert starter_projection.low == round(starter_projection.points * 0.62, 2)
    assert starter_projection.high == round(starter_projection.points * 1.42, 2)


def test_yahoo_points_allowed_tier_boundaries() -> None:
    # Verified 2026-08-07 against Yahoo's own live express-settings default
    # page -- exact tier edges are where an off-by-one is most likely.
    assert _points_allowed_fpts(0) == 10.0
    assert _points_allowed_fpts(1) == 7.0
    assert _points_allowed_fpts(6) == 7.0
    assert _points_allowed_fpts(7) == 4.0
    assert _points_allowed_fpts(13) == 4.0
    assert _points_allowed_fpts(14) == 1.0
    assert _points_allowed_fpts(20) == 1.0
    assert _points_allowed_fpts(21) == 0.0
    assert _points_allowed_fpts(27) == 0.0
    assert _points_allowed_fpts(28) == -1.0
    assert _points_allowed_fpts(34) == -1.0
    assert _points_allowed_fpts(35) == -4.0
    assert _points_allowed_fpts(52) == -4.0


def test_team_points_allowed_credits_the_opponents_score_not_own() -> None:
    schedule = pd.DataFrame([
        {"season": 2025, "game_type": "REG", "home_team": "KC", "away_team": "BUF", "home_score": 24, "away_score": 20},
        {"season": 2025, "game_type": "REG", "home_team": "BUF", "away_team": "MIA", "home_score": 30, "away_score": 3},
        # Future/unplayed game (no final score yet) must be skipped, not treated as 0 allowed.
        {"season": 2025, "game_type": "REG", "home_team": "KC", "away_team": "MIA", "home_score": None, "away_score": None},
    ])
    allowed = _team_points_allowed_fpts_by_season(schedule, 2025)
    # KC allowed 20 (BUF's score) -> tier 14-20 -> 1.0. Only one played game.
    assert allowed["KC"] == 1.0
    # BUF allowed 24 (vs KC, tier 21-27 -> 0.0) then 3 (vs MIA, tier 1-6 -> 7.0) = 7.0
    assert allowed["BUF"] == 7.0
    # MIA allowed 30 (BUF's score) -> tier 28-34 -> -1.0
    assert allowed["MIA"] == -1.0


def test_dst_uses_real_history_regression_not_a_flat_placeholder() -> None:
    # Two teams with different real defensive performance must project
    # differently -- the old model.py behavior (flat 105.0 for every team,
    # regardless of input) is exactly what this guards against regressing to.
    strong = {"position": "DST", "rookie": False, "depth_order": 1, "injury_status": None, "draft_number": None}
    weak = {**strong}
    strong_history = [
        {"season": 2023, "games": 17, "fantasy_points_std": 140.0, "fantasy_points_ppr": 140.0},
        {"season": 2024, "games": 17, "fantasy_points_std": 150.0, "fantasy_points_ppr": 150.0},
        {"season": 2025, "games": 17, "fantasy_points_std": 160.0, "fantasy_points_ppr": 160.0},
    ]
    weak_history = [
        {"season": 2023, "games": 17, "fantasy_points_std": 70.0, "fantasy_points_ppr": 70.0},
        {"season": 2024, "games": 17, "fantasy_points_std": 65.0, "fantasy_points_ppr": 65.0},
        {"season": 2025, "games": 17, "fantasy_points_std": 60.0, "fantasy_points_ppr": 60.0},
    ]
    strong_projection = project_player(strong, strong_history, "PPR", 2026)
    weak_projection = project_player(weak, weak_history, "PPR", 2026)
    assert strong_projection.explanation["method"] == "history_regression"
    assert strong_projection.points > weak_projection.points
    assert strong_projection.points != 105.0
    assert weak_projection.points != 105.0
    # Scoring is identical across STD/HALF/PPR (no reception bonus for DST).
    assert project_player(strong, strong_history, "STD", 2026).points == strong_projection.points


def test_dst_with_no_history_falls_back_to_flat_prior() -> None:
    # A DST with no matched history (e.g. an ingestion hiccup) must not error
    # or silently project zero -- it falls back to the same flat prior the
    # old placeholder always returned.
    no_history = {"position": "DST", "rookie": False, "depth_order": 1, "injury_status": None, "draft_number": None}
    projection = project_player(no_history, [], "PPR", 2026)
    assert projection.points == 105.0
    assert projection.explanation["method"] == "position_prior"


def test_rank_rows_uses_value_over_replacement() -> None:
    rows = []
    for position, points in (("QB", 300), ("QB", 250), ("RB", 240), ("RB", 180), ("WR", 220), ("TE", 160), ("K", 120), ("DST", 105)):
        rows.append({"name": f"{position}-{points}", "position": position, "our_projected_points": points})
    ranked = rank_rows(rows)
    assert sorted(row["our_rank"] for row in ranked) == list(range(1, len(rows) + 1))
    assert all(row["position_rank"] >= 1 for row in ranked)
