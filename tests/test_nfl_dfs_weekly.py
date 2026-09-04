from datetime import datetime, timezone
import pandas as pd
import pytest

from ingest.nfl_dfs_weekly import validate_partial_feed, target_season, refresh_results, DST_FIELDS
from ingest.nfl_dfs_results import SCORING_FIELDS


def player_frame():
    return pd.DataFrame([{**{field: 0 for field in (*SCORING_FIELDS["QB"], *SCORING_FIELDS["K"])},
        **dict(season=2026, week=1, season_type="REG", team="BUF", game_id="g1",
        opponent_team="MIA", player_id="p1", position="WR", passing_yards=0,
        receiving_yards=70, receptions=5)}])


def test_one_completed_game_does_not_require_full_season_row_count():
    assert len(validate_partial_feed(player_frame(), 2026, team=False)) == 1
    teams = pd.DataFrame([{**{field: 0 for field in DST_FIELDS},
        **dict(season=2026, week=1, season_type="REG", team="BUF", game_id="g1",
        opponent_team="MIA", def_sacks=1, def_interceptions=0)}])
    assert len(validate_partial_feed(teams, 2026, team=True)) == 1


def test_malformed_missing_duplicate_and_wrong_season_are_not_zero_results():
    frame = player_frame()
    for broken in (frame.iloc[:0], frame.drop(columns=["receptions"]), pd.concat([frame, frame]),
                   frame.assign(season=2025), frame.assign(week=19),
                   frame.drop(columns=["fumbles_lost_total"]), frame.assign(rushing_tds=None)):
        with pytest.raises(ValueError):
            validate_partial_feed(broken, 2026, team=False)


def test_january_uses_prior_nfl_season():
    assert target_season(None, datetime(2027, 1, 10, tzinfo=timezone.utc)) == 2026
    assert target_season(None, datetime(2026, 9, 4, tzinfo=timezone.utc)) == 2026
    assert target_season(2025, datetime(2026, 9, 4, tzinfo=timezone.utc)) == 2025


def test_no_completed_games_is_explicit_pending_and_does_not_fetch_stats(monkeypatch):
    import ingest.nfl_dfs_weekly as module
    monkeypatch.setattr(module, "fetch_schedule", lambda: pd.DataFrame([
        dict(season=2026, game_type="REG", home_score=None, away_score=None)]))
    monkeypatch.setattr(module, "load_season", lambda *args: 272)
    monkeypatch.setattr(module, "verify_season", lambda *args: [])
    monkeypatch.setattr(module, "fetch_partial", lambda *args, **kwargs: pytest.fail("not due"))
    assert refresh_results(None, 2026)["status"] == "awaiting_completed_games"
