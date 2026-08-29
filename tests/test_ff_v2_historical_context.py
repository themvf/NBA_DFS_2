from datetime import datetime, timezone

import pandas as pd

from ingest.ff_v2_historical_context import (
    TRANSFORM_VERSION,
    artifact_digest,
    build_roster_rows,
    build_schedule_context,
    build_team_week_facts,
    build_transactions,
)


UTC = timezone.utc


def _schedule() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "game_id": "2025_01_A_B", "season": 2025, "game_type": "REG", "week": 1,
            "gameday": "2025-09-07", "gametime": "13:00", "home_team": "B", "away_team": "A",
            "location": "Home", "stadium": "Test Field", "stadium_id": "B01", "roof": "outdoors",
            "surface": "grass", "home_qb_id": "QB-B", "home_qb_name": "Home QB",
            "away_qb_id": "QB-A", "away_qb_name": "Away QB", "home_coach": "Coach B", "away_coach": "Coach A",
        },
        {
            "game_id": "2025_01_C_D", "season": 2025, "game_type": "REG", "week": 1,
            "gameday": "2025-09-07", "gametime": "16:00", "home_team": "D", "away_team": "C",
            "location": "Home", "stadium": "Other Field", "stadium_id": "D01", "roof": "dome",
            "surface": "turf", "home_qb_id": "QB-D", "home_qb_name": "D QB",
            "away_qb_id": "QB-C", "away_qb_name": "C QB", "home_coach": "Coach D", "away_coach": "Coach C",
        },
        {
            "game_id": "2025_02_A_C", "season": 2025, "game_type": "REG", "week": 2,
            "gameday": "2025-09-14", "gametime": "13:00", "home_team": "C", "away_team": "A",
            "location": "Home", "stadium": "Other Field", "stadium_id": "C01", "roof": "outdoors",
            "surface": "grass", "home_qb_id": "QB-C", "home_qb_name": "C QB",
            "away_qb_id": "QB-A", "away_qb_name": "Away QB", "home_coach": "Coach C", "away_coach": "Coach A",
        },
    ])


def test_schedule_context_includes_game_qb_venue_and_explicit_byes() -> None:
    rows, week_starts, game_context = build_schedule_context(_schedule(), [2025], 10)
    assert len(rows) == 8  # four teams x two regular-season weeks
    assert sum(row["is_bye"] for row in rows) == 2
    team_a_week_1 = next(row for row in rows if row["team"] == "A" and row["week"] == 1)
    assert team_a_week_1["opponent"] == "B"
    assert team_a_week_1["quarterback_gsis_id"] == "QB-A"
    assert team_a_week_1["stadium"] == "Test Field"
    assert game_context[("2025_01_A_B", "A")]["is_home"] is False
    assert week_starts[(2025, 2)] > week_starts[(2025, 1)]


def test_roster_conflict_uses_same_week_stats_not_current_roster() -> None:
    roster = pd.DataFrame([
        {"season": 2025, "week": 1, "game_type": "REG", "gsis_id": "P1", "full_name": "Player One", "position": "RB", "depth_chart_position": "HB", "team": "A", "status": "ACT"},
        {"season": 2025, "week": 1, "game_type": "REG", "gsis_id": "P1", "full_name": "Player One", "position": "RB", "depth_chart_position": "HB", "team": "B", "status": "ACT"},
    ])
    stats = pd.DataFrame([{"season": 2025, "week": 1, "player_id": "P1", "team": "B"}])
    rows, report = build_roster_rows(
        roster, stats, source_snapshot_id=11,
        week_starts={(2025, 1): datetime(2025, 9, 7, tzinfo=UTC)},
    )
    assert rows[0]["team"] == "B"
    assert rows[0]["resolution_method"] == "weekly_stats_team"
    assert report == {"multi_team_conflicts": 1, "skipped_multi_team_conflicts": 0}


def test_effective_transactions_are_derived_only_from_ordered_roster_weeks() -> None:
    base = {
        "player_gsis_id": "P1", "player_name": "Player One", "position": "WR",
        "depth_chart_position": "WR", "roster_status": "ACT", "resolution_method": "single_weekly_roster_row",
        "source_snapshot_id": 11, "row_digest": "x",
    }
    rows = [
        {**base, "season": 2024, "week": 18, "team": "A", "effective_at": datetime(2025, 1, 1, tzinfo=UTC), "observed_at": datetime(2025, 1, 1, tzinfo=UTC)},
        {**base, "season": 2025, "week": 1, "team": "B", "source_snapshot_id": 12, "effective_at": datetime(2025, 9, 1, tzinfo=UTC), "observed_at": datetime(2025, 9, 1, tzinfo=UTC)},
    ]
    transactions = build_transactions(rows)
    assert len(transactions) == 1
    assert transactions[0]["from_team"] == "A"
    assert transactions[0]["to_team"] == "B"
    assert transactions[0]["evidence"]["new_season"] == 2025


def test_team_week_count_semantics_exclude_kneels_and_no_plays() -> None:
    schedule = pd.DataFrame([_schedule().iloc[0].to_dict()])
    _, _, game_context = build_schedule_context(schedule, [2025], 10)
    common = {
        "game_id": "2025_01_A_B", "season": 2025, "season_type": "REG", "week": 1,
        "posteam": "A", "defteam": "B", "qtr": 1, "score_differential": 0,
        "two_point_attempt": 0, "pass_touchdown": 0, "rush_touchdown": 0,
    }
    pbp = pd.DataFrame([
        {**common, "play_id": 1, "play_type": "pass", "pass_attempt": 1, "sack": 0, "qb_dropback": 1, "rush_attempt": 0, "qb_kneel": 0, "receiver_player_id": "RB1", "rusher_player_id": None, "yardline_100": 5, "air_yards": 6, "fixed_drive": 1, "game_seconds_remaining": 3500, "pass_touchdown": 1},
        {**common, "play_id": 2, "play_type": "pass", "pass_attempt": 1, "sack": 1, "qb_dropback": 1, "rush_attempt": 0, "qb_kneel": 0, "receiver_player_id": None, "rusher_player_id": None, "yardline_100": 40, "air_yards": None, "fixed_drive": 1, "game_seconds_remaining": 3470},
        {**common, "play_id": 3, "play_type": "pass", "pass_attempt": 1, "sack": 0, "qb_dropback": 1, "rush_attempt": 0, "qb_kneel": 0, "receiver_player_id": None, "rusher_player_id": None, "yardline_100": 35, "air_yards": None, "fixed_drive": 1, "game_seconds_remaining": 3440},
        {**common, "play_id": 4, "play_type": "run", "pass_attempt": 0, "sack": 0, "qb_dropback": 0, "rush_attempt": 1, "qb_kneel": 0, "receiver_player_id": None, "rusher_player_id": "RB1", "yardline_100": 4, "air_yards": None, "fixed_drive": 2, "game_seconds_remaining": 3300, "rush_touchdown": 1},
        {**common, "play_id": 5, "play_type": "run", "pass_attempt": 0, "sack": 0, "qb_dropback": 1, "rush_attempt": 1, "qb_kneel": 0, "receiver_player_id": None, "rusher_player_id": "QB1", "yardline_100": 30, "air_yards": None, "fixed_drive": 2, "game_seconds_remaining": 3270},
        {**common, "play_id": 6, "play_type": "qb_kneel", "pass_attempt": 0, "sack": 0, "qb_dropback": 0, "rush_attempt": 1, "qb_kneel": 1, "receiver_player_id": None, "rusher_player_id": "QB1", "yardline_100": 50, "air_yards": None, "fixed_drive": 3, "game_seconds_remaining": 10},
        {**common, "play_id": 7, "play_type": "no_play", "pass_attempt": 1, "sack": 0, "qb_dropback": 1, "rush_attempt": 0, "qb_kneel": 0, "receiver_player_id": "WR1", "rusher_player_id": None, "yardline_100": 20, "air_yards": 10, "fixed_drive": 2, "game_seconds_remaining": 3200},
    ])
    stats = pd.DataFrame([
        {"season": 2025, "week": 1, "season_type": "REG", "player_id": "RB1", "position": "RB", "team": "A"},
        {"season": 2025, "week": 1, "season_type": "REG", "player_id": "QB1", "position": "QB", "team": "A"},
    ])
    roster = pd.DataFrame([
        {"season": 2025, "week": 1, "gsis_id": "RB1", "position": "RB"},
        {"season": 2025, "week": 1, "gsis_id": "QB1", "position": "QB"},
    ])
    facts, missingness = build_team_week_facts(
        pbp, stats, roster, season=2025,
        source_snapshot_ids={"play_by_play": 1, "weekly_stats": 2, "weekly_rosters": 3, "schedule": 4, "participation": 5},
        game_team_context=game_context,
    )
    fact = facts[0]
    assert fact["plays"] == 5
    assert fact["pass_attempts"] == 2
    assert fact["sacks"] == 1
    assert fact["allocatable_targets"] == 1
    assert fact["rush_attempts"] == 2
    assert fact["rb_carries"] == 1
    assert fact["rb_targets"] == 1
    assert fact["pass_touchdowns"] == 1
    assert fact["rush_touchdowns"] == 1
    assert fact["red_zone_trips"] == 2
    assert fact["goal_line_carries"] == 1
    assert fact["end_zone_targets"] == 1
    assert fact["derivation"]["kneels_removed"] == 1
    assert missingness == {"unknown_rusher_positions": 0, "unknown_receiver_positions": 0}


def test_artifact_digest_is_order_independent_and_versioned() -> None:
    context = [{"row_digest": "c"}]
    rosters = [{"row_digest": "r2"}, {"row_digest": "r1"}]
    transactions = [{"row_digest": "t"}]
    facts = [{"fact_digest": "f"}]
    first = artifact_digest({"b": "2", "a": "1"}, context, rosters, transactions, facts)
    second = artifact_digest({"a": "1", "b": "2"}, context, list(reversed(rosters)), transactions, facts)
    assert first == second
    assert TRANSFORM_VERSION == "ff-v2-context-v1"
