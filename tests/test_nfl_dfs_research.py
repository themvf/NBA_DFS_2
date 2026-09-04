import numpy as np
import pytest

from ingest.nfl_dfs_research import build_samples
from model.nfl_dfs_historical import HistoricalWeek
from model.nfl_dfs_research import fit, predict, implied_totals, clustered_mae_delta, evaluate


def test_nflverse_sign_is_not_odds_api_sign():
    assert implied_totals(48, 6) == (27, 21)
    assert implied_totals(48, -6) == (21, 27)


def sample(season=2023, i=0):
    return dict(season=season, week=5+i//20, game_id=f"{season}:{i//4}", position="WR",
                baseline=10+i%7, history_games=12, prior_opportunity=8+i%4,
                actual=12+i%7, p10=2, p90=25, boom_threshold=25, boom_probability=0.1)


def test_scaler_and_fit_only_see_training_rows():
    train = [sample(i=i) for i in range(120)]
    recipe = fit(train, ("baseline", "prior_opportunity"), 1)
    assert recipe["training_seasons"] == [2023]
    assert recipe["center"][0] == pytest.approx(np.mean([r["baseline"] for r in train]))
    assert predict(recipe, sample(2025)) == pytest.approx(12)


def test_future_outcomes_cannot_change_forecast_features():
    rows = [HistoricalWeek(1, "p1", "A", "WR", 2022, w, "BUF", "MIA",
                           {"receptions": 4, "receiving_yards": 40, "targets": 6}) for w in (1, 2)]
    target = HistoricalWeek(1, "p1", "A", "WR", 2023, 1, "BUF", "MIA", {"receiving_yards": 70})
    games = {(2023, 1, "BUF"): {"game_id": "g"}}
    a, _ = build_samples(rows+[target], games, 20)
    changed = HistoricalWeek(1, "p1", "A", "WR", 2023, 1, "BUF", "MIA", {"receiving_yards": 700})
    b, _ = build_samples(rows+[changed], games, 20)
    assert a[0]["baseline"] == b[0]["baseline"]
    assert a[0]["prior_opportunity"] == b[0]["prior_opportunity"] == 6
    assert a[0]["actual"] != b[0]["actual"]
    assert a[0]["history_cutoff"] == [2022, 2]


def test_bootstrap_groups_players_by_game():
    rows = [{**sample(i=i), "prediction": sample(i=i)["actual"]} for i in range(20)]
    result = clustered_mae_delta(rows, 100)
    assert result["games"] == 5
    assert result["delta"] == -2
    assert result["ci95"] == [-2, -2]


def test_2025_is_never_called_untouched_and_closing_cannot_promote():
    rows = [{**sample(season, i), "team_implied": 25, "opponent_implied": 20, "team_spread": -5}
            for season in (2023, 2024, 2025) for i in range(120)]
    report, _ = evaluate(rows)
    assert not report["2025_is_untouched"]
    assert not report["production_promotion"]
    assert report["candidates"]["WR:closing_exploratory"]["status"] == "not_eligible"


def test_2025_outcomes_do_not_select_regularization_or_change_recipe():
    rows = [sample(season, i) for season in (2023,2024,2025) for i in range(120)]
    first, _ = evaluate(rows)
    second, _ = evaluate([{**r, "actual": 999 if r["season"]==2025 else r["actual"]} for r in rows])
    a = first["candidates"]["WR:opportunity"]
    b = second["candidates"]["WR:opportunity"]
    assert a["selected_alpha"] == b["selected_alpha"]
    assert a["recipe"] == b["recipe"]
