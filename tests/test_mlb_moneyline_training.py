from __future__ import annotations

from datetime import datetime, timezone

from ingest.mlb_moneyline_training import parse_team_game_outcomes


def test_parse_team_game_outcomes_captures_both_sides_and_actual_starters() -> None:
    game = {
        "id": 77,
        "game_id": "999",
        "game_date": "2026-07-17",
        "commence_time": "2026-07-17T23:05:00Z",
        "home_team_id": 1,
        "away_team_id": 2,
    }
    batting = {
        "runs": 5, "hits": 9, "doubles": 2, "triples": 0, "homeRuns": 1,
        "baseOnBalls": 3, "hitByPitch": 1, "strikeOuts": 8,
        "atBats": 34, "plateAppearances": 39,
    }
    pitching = {
        "outs": 27, "hits": 7, "earnedRuns": 3, "homeRuns": 1,
        "baseOnBalls": 2, "hitBatsmen": 0, "strikeOuts": 10,
    }
    starter_stats = {
        "outs": 18, "hits": 5, "earnedRuns": 2, "homeRuns": 1,
        "baseOnBalls": 1, "hitBatsmen": 0, "strikeOuts": 7,
        "airOuts": 4, "groundOuts": 6,
    }
    team = {
        "teamStats": {"batting": batting, "pitching": pitching},
        "pitchers": [123, 456],
        "players": {"ID123": {"person": {"fullName": "Test Starter"}, "stats": {"pitching": starter_stats}}},
    }
    boxscore = {"teams": {"home": team, "away": team}}

    rows = parse_team_game_outcomes(
        game,
        boxscore,
        fetched_at=datetime(2026, 7, 18, tzinfo=timezone.utc),
    )

    assert len(rows) == 2
    assert {row["team_id"] for row in rows} == {1, 2}
    assert rows[0]["starter_id"] == 123
    assert rows[0]["starter_outs"] == 18
    assert rows[0]["plate_appearances"] == 39
    assert rows[0]["origin"] == "retrospective_backfill"
