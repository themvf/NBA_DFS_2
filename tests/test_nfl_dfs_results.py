from ingest.nfl_dfs_results import SCORING_VERSION, input_digest, score_source_row


def test_skill_player_scoring_preserves_dk_bonuses() -> None:
    result = score_source_row(
        "QB",
        {
            "passing_yards": 300,
            "passing_tds": 2,
            "passing_interceptions": 1,
            "rushing_yards": 20,
            "rushing_tds": 1,
            "fumbles_lost_total": 1,
        },
    )

    assert result.status == "exact"
    assert result.actual_dk_fpts == 29.0
    assert result.evidence["eligible_for_dk_backtest"] is True


def test_kicker_scoring_uses_distance_bands() -> None:
    result = score_source_row(
        "K",
        {"pat_made": 2, "fg_made_30_39": 1, "fg_made_40_49": 1, "fg_made_50_59": 1},
    )

    assert result.status == "exact"
    assert result.actual_dk_fpts == 14.0


def test_dst_is_excluded_when_exact_context_is_missing() -> None:
    result = score_source_row("DST", {"raw_team_stats": {"def_sacks": 3}})

    assert result.status == "excluded"
    assert result.actual_dk_fpts is None
    assert "opponent final score" in (result.exclusion_reason or "")


def test_dst_exact_dk_scoring_and_offensive_score_exclusion() -> None:
    result = score_source_row(
        "DST",
        {"raw_team_stats": {
            "def_sacks": 3,
            "def_interceptions": 2,
            "fumble_recovery_opp": 1,
            "def_safeties": 1,
            "def_tds": 1,
            "special_teams_tds": 1,
            "def_fg_blocks": 1,
            "def_pat_blocks": 0,
            "def_punt_blocks": 1,
            "def_2pt_made": 1,
        }},
        dst_context={
            "opponent_final_points": 21,
            "opponent_raw_team_stats": {"def_tds": 1, "def_safeties": 0, "def_2pt_made": 0},
        },
    )

    # Adjusted points allowed are 15 (21 minus an opponent defensive TD),
    # worth +1. Component points total 30 before that tier.
    assert result.status == "exact"
    assert result.actual_dk_fpts == 30.0
    assert result.evidence["scoring_components"]["dk_points_allowed"] == 15
    assert result.evidence["redraft_reusable_components"] is True


def test_input_digest_is_order_invariant_and_versioned() -> None:
    first = input_digest(position="WR", source="nflverse", source_row={"receptions": 5, "receiving_yards": 80})
    second = input_digest(position="WR", source="nflverse", source_row={"receiving_yards": 80, "receptions": 5})

    assert first == second
    assert len(first) == 64
    assert SCORING_VERSION == "nfl-dk-realized-v2"
