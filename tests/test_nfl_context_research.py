import numpy as np
import pandas as pd

from model.nfl_context_research import evaluate_volume_candidate, pregame_features


def test_pregame_features_never_include_current_game() -> None:
    rows = []
    for week in range(1, 6):
        rows.extend([
            dict(game_id=f"g{week}", season=2025, week=week, team="A", opponent="B",
                 plays=50 + week, interval_seconds_sum=(30 + week) * 10, interval_count=10),
            dict(game_id=f"g{week}", season=2025, week=week, team="B", opponent="A",
                 plays=60 + week, interval_seconds_sum=(35 + week) * 10, interval_count=10),
        ])
    featured = pregame_features(pd.DataFrame(rows), lookback=4, minimum=3)
    a_week4 = featured[(featured.team == "A") & (featured.week == 4)].iloc[0]
    assert a_week4.team_prior_plays == np.mean([51, 52, 53])
    assert a_week4.opponent_prior_plays == np.mean([61, 62, 63])
    assert a_week4.team_prior_interval_seconds == np.mean([31, 32, 33])


def test_context_stays_unqualified_when_it_adds_no_signal() -> None:
    rng = np.random.default_rng(7)
    rows = []
    for season in range(2020, 2025):
        for game in range(140):
            team_prior = rng.normal(64, 4)
            opponent_prior = rng.normal(64, 4)
            rows.append({
                "game_id": f"{season}-{game // 2}", "season": season,
                "week": game // 16 + 1, "team": f"T{game % 32}",
                "plays": 0.7 * team_prior + 0.3 * opponent_prior + rng.normal(0, 3),
                "team_prior_plays": team_prior,
                "opponent_prior_plays": opponent_prior,
                "team_prior_interval_seconds": rng.normal(31, 2),
                "opponent_prior_interval_seconds": rng.normal(31, 2),
            })
    report = evaluate_volume_candidate(pd.DataFrame(rows), bootstrap_draws=200)
    assert report["status"] == "not_qualified"
    assert report["productionProjectionEffect"] == "none"
