import pandas as pd

from ingest.ff_independent import (
    build_adp_lookup,
    build_schedule_context,
    compute_bye_weeks,
    normalize_team,
    project_player,
    rank_rows,
    schedule_strength_factor,
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


def _synthetic_schedule_context() -> dict:
    """Two teams' worth of a schedule-strength context: DAL's RB opponents
    face a generous predicted rating (25), NE's face a stingy one (15)."""
    return {
        "opponents": {"DAL": ["EASY1", "EASY2"], "NE": ["HARD1", "HARD2"]},
        "predicted_rating": {
            ("EASY1", "RB"): {"STD": 20.0, "PPR": 25.0},
            ("EASY2", "RB"): {"STD": 20.0, "PPR": 25.0},
            ("HARD1", "RB"): {"STD": 12.0, "PPR": 15.0},
            ("HARD2", "RB"): {"STD": 12.0, "PPR": 15.0},
        },
        "league_avg": {("RB", "STD"): 16.0, ("RB", "PPR"): 20.0},
    }


def test_schedule_strength_factor_only_applies_to_rb() -> None:
    ctx = _synthetic_schedule_context()
    ctx["opponents"]["DAL"] = ["EASY1", "EASY2"]
    for position in ("QB", "WR", "TE", "DST"):
        factor, evidence, skip_reason = schedule_strength_factor("DAL", position, "PPR", ctx)
        assert factor == 1.0
        assert evidence == []
        assert skip_reason == f"not_validated_for_{position.lower()}"
    factor, evidence, skip_reason = schedule_strength_factor("DAL", "RB", "PPR", ctx)
    assert skip_reason is None
    assert len(evidence) == 2


def test_schedule_strength_factor_direction_and_clamp() -> None:
    ctx = _synthetic_schedule_context()
    easy_factor, _, _ = schedule_strength_factor("DAL", "RB", "PPR", ctx)
    hard_factor, _, _ = schedule_strength_factor("NE", "RB", "PPR", ctx)
    assert easy_factor > 1.0 > hard_factor  # generous opponents -> factor above 1, stingy -> below
    from ingest.ff_independent import SCHEDULE_FACTOR_MAX, SCHEDULE_FACTOR_MIN
    assert SCHEDULE_FACTOR_MIN <= hard_factor <= easy_factor <= SCHEDULE_FACTOR_MAX


def test_schedule_strength_factor_never_silently_defaults() -> None:
    """Every non-applied path must name why, never a bare neutral 1.0."""
    factor, evidence, skip_reason = schedule_strength_factor("DAL", "RB", "PPR", None)
    assert (factor, evidence) == (1.0, [])
    assert skip_reason == "no_schedule_context"
    factor, evidence, skip_reason = schedule_strength_factor("UNKNOWN", "RB", "PPR", _synthetic_schedule_context())
    assert skip_reason == "opponents_not_published"


def test_project_player_applies_schedule_factor_for_rb_only() -> None:
    ctx = _synthetic_schedule_context()
    history = [{"season": 2025, "games": 17, "fantasy_points_std": 170, "fantasy_points_ppr": 272}]
    rb = {"position": "RB", "team": "DAL", "rookie": False, "depth_order": 1, "injury_status": None, "draft_number": None}
    wr = {"position": "WR", "team": "DAL", "rookie": False, "depth_order": 1, "injury_status": None, "draft_number": None}

    rb_projection = project_player(rb, history, "PPR", 2026, ctx)
    assert rb_projection.explanation["schedule_strength_applied"] is True
    assert rb_projection.explanation["schedule_strength_factor"] > 1.0
    assert "future schedule" not in rb_projection.explanation["not_modeled"]

    wr_projection = project_player(wr, history, "PPR", 2026, ctx)
    assert wr_projection.explanation["schedule_strength_applied"] is False
    assert wr_projection.explanation["schedule_strength_skip_reason"] == "not_validated_for_wr"
    assert wr_projection.explanation["schedule_strength_factor"] == 1.0
    assert "future schedule" in wr_projection.explanation["not_modeled"]


def test_build_schedule_context_matches_compute_bye_weeks() -> None:
    """compute_bye_weeks() must keep its exact prior behavior after being
    refactored to delegate to build_schedule_context()."""
    rows = []
    for week in range(1, 19):
        if week != 11:
            rows.append({"season": 2026, "game_type": "REG", "week": week, "home_team": "LA", "away_team": "ARI"})
    schedule = pd.DataFrame(rows)
    ctx = build_schedule_context(schedule, 2026)
    assert ctx["bye_weeks"] == compute_bye_weeks(schedule, 2026)
    assert ctx["games_played"]["LAR"] == 17
    assert ctx["opponents"]["LAR"] == ["ARI"] * 17


def test_rank_rows_uses_value_over_replacement() -> None:
    rows = []
    for position, points in (("QB", 300), ("QB", 250), ("RB", 240), ("RB", 180), ("WR", 220), ("TE", 160), ("K", 120), ("DST", 105)):
        rows.append({"name": f"{position}-{points}", "position": position, "our_projected_points": points})
    ranked = rank_rows(rows)
    assert sorted(row["our_rank"] for row in ranked) == list(range(1, len(rows) + 1))
    assert all(row["position_rank"] >= 1 for row in ranked)
