from __future__ import annotations

import pandas as pd

from model.mlb_moneyline_independent_model import build_point_in_time_features


def _outcome(game_id: str, game_date: str, team: int, opponent: int, *, home: bool, runs: int, starter: int) -> dict:
    return {
        "matchup_id": int(game_id), "game_id": game_id, "game_date": game_date,
        "team_id": team, "opponent_team_id": opponent, "is_home": home,
        "runs": runs, "hits": 8, "doubles": 2, "triples": 0, "home_runs": 1,
        "walks": 3, "hit_by_pitch": 0, "strikeouts": 8, "at_bats": 34,
        "plate_appearances": 38, "starter_id": starter, "starter_outs": 18,
        "starter_home_runs": 1, "starter_walks": 2, "starter_hit_batters": 0,
        "starter_strikeouts": 6, "team_pitching_outs": 27,
        "team_pitching_home_runs": 1, "team_pitching_walks": 3,
        "team_pitching_hit_batters": 0, "team_pitching_strikeouts": 9,
        "vegas_prob_home": 0.52, "home_ml": -110, "away_ml": 100,
    }


def test_same_date_games_cannot_use_each_others_outcomes() -> None:
    rows = []
    for game_id in ("1", "2"):
        rows.extend([
            _outcome(game_id, "2026-04-01", 1, 2, home=True, runs=5, starter=11),
            _outcome(game_id, "2026-04-01", 2, 1, home=False, runs=3, starter=22),
        ])
    features = build_point_in_time_features(rows)
    assert features["offense_woba_adv"].isna().all()
    assert features["rest_days_adv"].isna().all()


def test_point_in_time_features_are_reproducible() -> None:
    rows = []
    for day in range(1, 13):
        game_id = str(day)
        game_date = f"2026-04-{day:02d}"
        rows.extend([
            _outcome(game_id, game_date, 1, 2, home=True, runs=5, starter=11),
            _outcome(game_id, game_date, 2, 1, home=False, runs=3, starter=22),
        ])
    first = build_point_in_time_features(rows)
    second = build_point_in_time_features(list(reversed(rows)))
    pd.testing.assert_frame_equal(first, second)
    assert first.iloc[-1]["runs_per_game_adv"] == 2.0
