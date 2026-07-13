from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from ingest.mlb_bullpen import derive_bullpen_metrics, parse_relief_appearances


def test_boxscore_parser_excludes_starter() -> None:
    game = {"id": 1, "game_id": "g1", "game_date": "2026-07-12", "home_team_id": 10, "away_team_id": 20}
    boxscore = {"teams": {"home": {"pitchers": [1, 2], "players": {
        "ID1": {"person": {"fullName": "Starter"}, "stats": {"pitching": {"gamesStarted": 1, "outs": 15}}},
        "ID2": {"person": {"fullName": "Reliever"}, "stats": {"pitching": {"gamesStarted": 0, "outs": 3, "numberOfPitches": 14, "battersFaced": 4, "strikeOuts": 2}}},
    }}, "away": {"pitchers": [], "players": {}}}}
    rows = parse_relief_appearances(game, boxscore, captured_at=datetime.now(timezone.utc))
    assert len(rows) == 1
    assert rows[0]["pitcher_name"] == "Reliever"
    assert rows[0]["outs"] == 3


def test_bullpen_metrics_use_only_prior_windows() -> None:
    appearances = [
        {"game_date": date(2026, 7, 16), "pitcher_id": 1, "outs": 3, "pitches": 20, "batters_faced": 4, "earned_runs": 0, "home_runs": 0, "walks": 0, "intentional_walks": 0, "hit_batters": 0, "strikeouts": 2},
        {"game_date": date(2026, 7, 15), "pitcher_id": 1, "outs": 3, "pitches": 18, "batters_faced": 5, "earned_runs": 1, "home_runs": 1, "walks": 1, "intentional_walks": 0, "hit_batters": 0, "strikeouts": 1},
        {"game_date": date(2026, 7, 14), "pitcher_id": 2, "outs": 6, "pitches": 30, "batters_faced": 7, "earned_runs": 0, "home_runs": 0, "walks": 0, "intentional_walks": 0, "hit_batters": 1, "strikeouts": 3},
    ]
    result = derive_bullpen_metrics(appearances, event_date=date(2026, 7, 17))
    assert result["quality_outs"] == 12
    assert result["pitches_1d"] == 20
    assert result["pitches_3d"] == 68
    assert result["relievers_back_to_back"] == 1
    assert result["reliever_era"] == pytest.approx(2.25)
