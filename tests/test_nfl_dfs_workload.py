from copy import deepcopy
from model.nfl_dfs_workload import allocate, backtest, build, metrics, team_forecast, weighted_mean


def team(team, week, attempts, carries, targets=None, season=2025):
    return {"team": team, "season": season, "week": week, "stats": {"attempts": attempts, "carries": carries, "targets": attempts if targets is None else targets}}


def player(identity, team_name, week, position, **stats):
    return {"identity": identity, "team": team_name, "season": 2025, "week": week, "position": position, "stats": stats}


def test_weighted_mean_favors_recent_values():
    assert weighted_mean([10, 20]) > 15


def test_missing_values_are_not_zero():
    result = team_forecast([team("BUF", 1, None, 20)], "attempts")
    assert result is None


def test_prior_shrinkage_is_reproducible_and_disclosed():
    own = [team("BUF", 1, 40, 20), team("BUF", 2, 42, 20)]
    league = [{**team("NYJ", 1, 20, 20), "scope": "league"}]
    result = team_forecast(own + league, "attempts")
    assert result["history_mean"] > result["mean"] > result["prior"]
    assert result["games"] == 2 and result["weight"] == 2/6


def test_allocations_reconcile_and_retain_unallocated_share():
    teams = [team("BUF", 1, 40, 20)]
    history = [player("a", "BUF", 1, "WR", targets=20), player("b", "BUF", 1, "WR", targets=10)]
    budgets = {"targets": {"mean": 40.0}}
    output = allocate("BUF", [{"identity": "a", "position": "WR"}, {"identity": "b", "position": "WR"}], history, teams, budgets)
    assert round(sum(p["components"]["targets"]["mean"] for p in output), 6) == 30
    assert budgets["targets"]["unallocated_share"] == .25


def test_overallocated_observations_are_scaled_not_hidden():
    teams = [team("BUF", 1, 10, 20)]
    history = [player("a", "BUF", 1, "WR", targets=8), player("b", "BUF", 1, "WR", targets=8)]
    budgets = {"targets": {"mean": 10.0}}
    output = allocate("BUF", [{"identity": "a", "position": "WR"}, {"identity": "b", "position": "WR"}], history, teams, budgets)
    assert round(sum(p["components"]["targets"]["mean"] for p in output), 6) == 10
    assert output[0]["components"]["targets"]["normalization"] == "scaled_to_team_budget"
    assert 0 <= budgets["targets"]["allocated_share"] <= 1
    assert budgets["targets"]["allocated_share"] + budgets["targets"]["unallocated_share"] == 1


def test_player_without_history_is_unavailable_not_zero():
    output = allocate("BUF", [{"identity": "rookie", "position": "RB"}], [], [], {"carries": {"mean": 20.0}})
    assert output[0]["components"] == {}
    assert output[0]["components"].get("carries") is None


def test_target_constraint_and_pregame_history():
    history = [team("BUF", 1, 20, 30, targets=25), team("NYJ", 1, 20, 30, targets=25)]
    rows = deepcopy(history)
    result = build(history, [], [{"game_id": 1, "season": 2025, "week": 2, "kickoff": "x", "home_team": "BUF", "away_team": "NYJ"}], [], "now")
    assert result[0]["budgets"]["targets"]["mean"] <= result[0]["budgets"]["attempts"]["mean"]
    assert history == rows


def test_target_week_player_outcome_cannot_change_allocation():
    teams = [team("BUF", 1, 30, 20), team("NYJ", 1, 30, 20)]
    past = [player("a", "BUF", 1, "WR", targets=6)]
    target = {**player("a", "BUF", 2, "WR", targets=30), "season": 2025}
    game = [{"game_id": 1, "season": 2025, "week": 2, "kickoff": "x", "home_team": "BUF", "away_team": "NYJ"}]
    roster = [{"identity": "a", "name": "Receiver", "position": "WR", "team": "BUF"}]
    without = build(teams, past, game, roster, "now")
    with_target = build(teams, past + [target], game, roster, "now")
    assert without == with_target


def test_backtest_never_uses_same_week_outcome():
    history = [team("BUF", 1, 10, 20, season=2023), team("NYJ", 1, 30, 20, season=2023),
               team("BUF", 1, 100, 20, season=2024)]
    rows = backtest(history)
    prediction = next(r for r in rows if r["field"] == "attempts")
    assert prediction["candidate"] < 100 and prediction["baseline"] == 10


def test_metrics_bias_sign_is_actual_minus_projected():
    report = metrics([{"field": "attempts", "actual": 10, "candidate": 8, "baseline": 9}])
    assert report[0]["candidate_bias_actual_minus_projected"] == 2


def test_source_row_order_cannot_change_forecast_or_recent_order():
    teams = [team("BUF", 2, 40, 20), team("BUF", 1, 20, 20)]
    history = [player("a", "BUF", 2, "WR", targets=20), player("a", "BUF", 1, "WR", targets=5)]
    budgets = {"targets": {"mean": 30.0}}
    out = allocate("BUF", [{"identity": "a", "position": "WR"}], history, teams, budgets)
    recent = out[0]["components"]["targets"]["recent"]
    assert [r["week"] for r in recent] == [1, 2]
    assert team_forecast(teams, "attempts")["mean"] == team_forecast(list(reversed(teams)), "attempts")["mean"]


def test_reported_sample_and_weight_match_the_capped_window():
    rows = [team("BUF", week, 20 + week, 20, season=2025) for week in range(1, 19)]
    result = team_forecast(rows, "attempts")
    assert result["games"] == 17
    assert result["weight"] == 17 / 21
