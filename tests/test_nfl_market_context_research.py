import pandas as pd

from model.nfl_market_context_research import market_pregame_features


def test_market_features_use_prior_games_only() -> None:
    rows = []
    for week in range(1, 6):
        for team, opponent, home, away, points in (
            ("A", "B", "A", "B", 20 + week),
            ("B", "A", "A", "B", 10 + week),
        ):
            rows.append({
                "game_id": f"g{week}", "season": 2025, "week": week,
                "team": team, "opponent": opponent, "home_team": home, "away_team": away,
                "points_for": points, "points_against": 30 - points,
                "offensive_epa": week if team == "A" else -week,
                "success_rate": 0.4, "explosive_rate": 0.1, "pass_rate": 0.6,
                "interval_seconds_sum": 300, "interval_count": 10,
                "turnover_rate": 0.02, "fumble_lost_rate": 0.01, "sack_rate": 0.05,
            })
    featured = market_pregame_features(pd.DataFrame(rows), lookback=4, minimum=3)
    week4 = featured[featured.week.eq(4)].iloc[0]
    assert week4.prior_offensive_epa == 2
    assert week4.away_prior_offensive_epa == -2
    assert week4.epa_difference == 4
