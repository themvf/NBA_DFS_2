import pandas as pd

from ingest.ff_independent import (
    yahoo_kicker_points,
    DST_CARRY_FORWARD_WEIGHT,
    MODEL_VERSION,
    ROOKIE_RANGE_RATIO,
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
    # A drafted RB routes through the fitted draft-capital curve, not the flat
    # pick-bucket table (ROOKIE_CURVE_POSITIONS). This assertion was stale --
    # it still expected the pre-curve "rookie_prior" behavior and a pinned
    # v1.5 model string, so it had been failing since the curve shipped.
    assert starter_projection.explanation["method"] == "rookie_draft_curve"
    assert starter_projection.explanation["model"] == MODEL_VERSION
    assert starter_projection.explanation["draft_number"] == 25
    assert starter_projection.explanation["role_factor"] == 1.0
    # The low/high band comes from the position's fitted range ratio. Compared
    # with a tolerance, not exact equality: the source derives the band from
    # the UNROUNDED point estimate, so re-deriving it here from the rounded
    # `.points` can differ by a cent.
    low_ratio, high_ratio = ROOKIE_RANGE_RATIO["RB"]
    assert abs(starter_projection.low - starter_projection.points * low_ratio) < 0.02
    assert abs(starter_projection.high - starter_projection.points * high_ratio) < 0.02


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


DST = {"position": "DST", "rookie": False, "depth_order": 1, "injury_status": None, "draft_number": None}


def _dst_history(by_season: dict[int, float]) -> list[dict[str, object]]:
    return [
        {"season": season, "games": 17, "fantasy_points_std": points, "fantasy_points_ppr": points}
        for season, points in sorted(by_season.items())
    ]


def test_dst_ranks_on_prior_season_only_not_a_multi_year_blend() -> None:
    # Ordering must follow the MOST RECENT season alone. Prior-season
    # carry-forward ranked better in the backtest (Spearman 0.18) than the
    # 3-year weighted blend v1.7 used (0.15), so a team that was bad for two
    # years and then great last year must outrank the reverse.
    improving = project_player(DST, _dst_history({2023: 60.0, 2024: 65.0, 2025: 160.0}), "PPR", 2026)
    declining = project_player(DST, _dst_history({2023: 160.0, 2024: 155.0, 2025: 60.0}), "PPR", 2026)
    assert improving.points > declining.points
    assert improving.explanation["method"] == "dst_prior_season_carry_forward"
    assert improving.explanation["season_inputs"][0]["season"] == 2025


def test_dst_projection_is_shrunk_hard_toward_the_league_prior() -> None:
    # Shrinkage is what makes carry-forward beat both raw carry-forward and a
    # flat constant on held-out MAE. A 160-point defense must NOT project
    # anywhere near 160 -- the year-over-year signal does not support it.
    projection = project_player(DST, _dst_history({2023: 60.0, 2024: 65.0, 2025: 160.0}), "PPR", 2026)
    assert projection.points == round(105.4 + DST_CARRY_FORWARD_WEIGHT * (160.0 - 105.4), 2)
    assert 105.4 < projection.points < 110.0
    assert projection.explanation["dst_carry_forward_weight"] == DST_CARRY_FORWARD_WEIGHT
    # Identical across scoring formats (DST has no reception bonus).
    assert project_player(DST, _dst_history({2025: 160.0}), "STD", 2026).points == projection.points


def test_dst_shrinkage_preserves_order_exactly() -> None:
    # The whole justification for shrinking is that it is monotonic: it
    # changes the displayed spread but never the draft order. If this breaks,
    # raising DST_CARRY_FORWARD_WEIGHT would silently reshuffle the board.
    actuals = [158.0, 140.0, 130.0, 98.0, 67.0, 44.0]
    projected = [project_player(DST, _dst_history({2025: value}), "PPR", 2026).points for value in actuals]
    assert projected == sorted(projected, reverse=True)


def test_dst_with_no_history_falls_back_to_the_flat_prior() -> None:
    projection = project_player(DST, [], "PPR", 2026)
    assert projection.points == 105.4
    assert projection.explanation["method"] == "position_baseline_no_history"


def test_yahoo_kicker_scoring_is_distance_tiered() -> None:
    # Verified 2026-08-07 against Yahoo's own live express-settings default
    # page: 0-39 = 3, 40-49 = 4, 50+ = 5, PAT = 1. The previous flat 3-per-FG
    # formula undercounted kickers by ~15 points a season and reshuffled 22 of
    # 42 kicker ranks, so the tier edges matter.
    assert yahoo_kicker_points({"fg_made_30_39": 1}) == 3.0
    assert yahoo_kicker_points({"fg_made_40_49": 1}) == 4.0
    assert yahoo_kicker_points({"fg_made_50_59": 1}) == 5.0
    assert yahoo_kicker_points({"fg_made_60_": 1}) == 5.0
    assert yahoo_kicker_points({"pat_made": 1}) == 1.0
    # A realistic season: 10 short, 8 mid, 5 long, 40 PATs.
    assert yahoo_kicker_points({
        "fg_made_0_19": 2, "fg_made_20_29": 3, "fg_made_30_39": 5,
        "fg_made_40_49": 8, "fg_made_50_59": 4, "fg_made_60_": 1, "pat_made": 40,
    }) == 2 * 3 + 3 * 3 + 5 * 3 + 8 * 4 + 4 * 5 + 1 * 5 + 40


def test_kicker_scoring_falls_back_when_distance_buckets_are_absent() -> None:
    # Rows written before the tiered buckets shipped must degrade to the old
    # flat-3 formula rather than silently scoring zero.
    assert yahoo_kicker_points({"fg_made": 30, "pat_made": 40}) == 30 * 3 + 40


def test_kicker_projection_is_shrunk_harder_than_a_skill_position() -> None:
    # Kicker history IS predictive (unlike DST), so kickers keep the 3-year
    # regression -- but the default 4-game prior leaves far too little
    # shrinkage. model/ff_kicker_projection_backtest.py fits 37 prior games
    # (effective lambda 0.58 at a 51-game sample), improving held-out MAE
    # 23.2 -> 22.1.
    kicker_history = [
        {"season": season, "games": 17, "fg_made_40_49": 10, "fg_made_50_59": 6, "pat_made": 45}
        for season in (2023, 2024, 2025)
    ]
    kicker = {"position": "K", "rookie": False, "depth_order": 1, "injury_status": None, "draft_number": None}
    projection = project_player(kicker, kicker_history, "PPR", 2026)
    assert projection.explanation["method"] == "history_regression"
    assert projection.explanation["regression_prior_games"] == 37.0
    # A skill position keeps the light default prior.
    receiver = {"position": "WR", "rookie": False, "depth_order": 1, "injury_status": None, "draft_number": None}
    receiver_history = [{"season": season, "games": 17, "fantasy_points_std": 150, "fantasy_points_ppr": 250} for season in (2023, 2024, 2025)]
    assert project_player(receiver, receiver_history, "PPR", 2026).explanation["regression_prior_games"] == 4.0


def test_kicker_projection_still_uses_multi_year_history_not_carry_forward() -> None:
    # The DST-style prior-season carry-forward was tested for kickers and lost
    # (held-out MAE 23.1 vs 22.1). Older seasons must therefore still move a
    # kicker's projection -- if they stop mattering, someone has wrongly
    # applied the DST treatment here.
    strong_recent_only = [
        {"season": 2023, "games": 17, "fg_made_30_39": 5, "pat_made": 20},
        {"season": 2024, "games": 17, "fg_made_30_39": 5, "pat_made": 20},
        {"season": 2025, "games": 17, "fg_made_50_59": 20, "pat_made": 50},
    ]
    strong_throughout = [
        {"season": season, "games": 17, "fg_made_50_59": 20, "pat_made": 50}
        for season in (2023, 2024, 2025)
    ]
    kicker = {"position": "K", "rookie": False, "depth_order": 1, "injury_status": None, "draft_number": None}
    assert project_player(kicker, strong_throughout, "PPR", 2026).points > project_player(kicker, strong_recent_only, "PPR", 2026).points


def test_rank_rows_uses_value_over_replacement() -> None:
    rows = []
    for position, points in (("QB", 300), ("QB", 250), ("RB", 240), ("RB", 180), ("WR", 220), ("TE", 160), ("K", 120), ("DST", 105)):
        rows.append({"name": f"{position}-{points}", "position": position, "our_projected_points": points})
    ranked = rank_rows(rows)
    assert sorted(row["our_rank"] for row in ranked) == list(range(1, len(rows) + 1))
    assert all(row["position_rank"] >= 1 for row in ranked)
