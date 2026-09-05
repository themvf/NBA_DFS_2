from model.nfl_dfs_efficiency import (
    CONFIG,
    backtest,
    build,
    estimate_rate,
    metrics,
    scoring_contributions,
    simulate_dst,
    simulate_player,
    simulate_team,
)
from model.nfl_dfs_historical import draftkings_points


def row(identity, position, week, **stats):
    return {
        "identity": identity,
        "position": position,
        "season": 2025,
        "week": week,
        "team": "BUF",
        "stats": stats,
    }


def qb_history(identity="qb"):
    return [
        row(identity, "QB", week, attempts=30, completions=20, passing_yards=240,
            passing_tds=2, passing_interceptions=1, carries=5, rushing_yards=25,
            rushing_tds=0, receptions=0, targets=0, receiving_yards=0,
            receiving_tds=0, fumbles_lost_total=0, passing_2pt_conversions=0,
            rushing_2pt_conversions=0, special_teams_tds=0,
            fumble_recovery_tds=0)
        for week in range(1, 7)
    ]


def wr_history(identity="wr"):
    return [
        row(identity, "WR", week, targets=8, receptions=5, receiving_yards=70,
            receiving_tds=1 if week % 3 == 0 else 0, carries=1, rushing_yards=5,
            rushing_tds=0, attempts=0, completions=0, passing_yards=0,
            passing_tds=0, passing_interceptions=0, fumbles_lost_total=0,
            receiving_2pt_conversions=0, rushing_2pt_conversions=0,
            special_teams_tds=0, fumble_recovery_tds=0)
        for week in range(1, 7)
    ]


def test_rate_shrinkage_reports_player_and_prior_support():
    own = [row("a", "WR", 1, targets=10, receptions=10)]
    peers = [row("b", "WR", 1, targets=10, receptions=5)]
    result = estimate_rate(own, peers, "catch_rate")
    assert result["player_rate"] == 1
    assert result["position_prior"] == .5
    assert .5 < result["mean"] < 1
    assert result["player_opportunities"] == 10


def test_zero_denominator_is_unknown_not_zero_efficiency():
    own = [row("a", "WR", 1, targets=0, receptions=0)]
    peers = [row("b", "WR", 1, targets=10, receptions=5)]
    result = estimate_rate(own, peers, "catch_rate")
    assert result["player_rate"] is None
    assert result["mean"] == .5


def test_scoring_bridge_matches_exact_draftkings_scorer_and_bonus():
    stats = {
        "passing_yards": 300, "passing_tds": 2, "passing_interceptions": 1,
        "rushing_yards": 100, "rushing_tds": 1, "receiving_yards": 100,
        "receiving_tds": 1, "receptions": 5, "passing_2pt_conversions": 1,
        "fumbles_lost_total": 1,
    }
    bridge = scoring_contributions(stats)
    assert bridge["yardage_bonuses"] == 9
    assert sum(bridge.values()) == draftkings_points("QB", stats)


def test_simulation_is_deterministic_and_player_coherent():
    own = wr_history()
    peers = wr_history("peer")
    player = {"identity": "wr", "name": "Receiver", "position": "WR", "components": {
        "targets": {"mean": 8.0}, "carries": {"mean": 1.0},
    }}
    first = simulate_player(player, own, peers, {**CONFIG, "draws": 200})
    second = simulate_player(player, own, peers, {**CONFIG, "draws": 200})
    assert first == second
    assert first["p10_fpts"] <= first["median_fpts"] <= first["p90_fpts"]
    assert abs(first["mean_fpts"] - sum(first["scoring_contributions"].values())) < 1e-8
    assert first["coherence_scope"] == "within_player_only"


def test_backtest_uses_only_prior_weeks_and_bias_is_actual_minus_projected():
    rows = qb_history("qb") + qb_history("peer")
    output = backtest(rows, start=(2025, 2), config={**CONFIG, "max_player_games": 4})
    assert output
    assert all(item["week"] >= 2 for item in output)
    report = metrics([{"rate": "completion_rate", "actual": 10, "candidate": 8, "baseline": 9}])
    completion = next(item for item in report if item["rate"] == "completion_rate")
    assert completion["candidate_bias_actual_minus_projected"] == 2


def test_target_week_row_cannot_change_its_own_rate_prediction():
    past = qb_history("qb")[:2] + qb_history("peer")[:2]
    target_low = row("qb", "QB", 3, attempts=30, completions=0, passing_yards=0,
                     passing_tds=0, passing_interceptions=0, carries=1,
                     rushing_yards=0, rushing_tds=0)
    target_high = {**target_low, "stats": {**target_low["stats"], "completions": 30}}
    low = backtest(past + [target_low], start=(2025, 3))
    high = backtest(past + [target_high], start=(2025, 3))
    low_prediction = next(item["candidate"] for item in low if item["rate"] == "completion_rate")
    high_prediction = next(item["candidate"] for item in high if item["rate"] == "completion_rate")
    assert low_prediction == high_prediction


def test_dst_resamples_exact_whole_game_components_and_reconciles():
    stats = {"sacks": 3, "interceptions": 1, "fumble_recoveries": 1,
             "safeties": 0, "defensive_tds": 0, "special_teams_return_tds": 0,
             "blocked_kicks": 0, "two_point_returns": 0, "points_allowed_fpts": 4}
    score = 3 + 2 + 2 + 4
    own = [row("DST:BUF", "DST", week, **stats, fantasy_points=score) for week in range(1, 5)]
    peers = [{**row("DST:MIA", "DST", week, **stats, fantasy_points=score), "team": "MIA"} for week in range(1, 5)]
    result = simulate_dst("BUF", "NYJ", own, peers, {**CONFIG, "draws": 100})
    assert result["mean_fpts"] == score
    assert sum(result["scoring_contributions"].values()) == score
    assert result["coherence_scope"] == "separate_dst_whole_game_resample"
    assert result["opponent_context"]["opponent"] == "NYJ"


def test_dst_backtest_is_separate_from_offensive_rates():
    stats = {"sacks": 2, "interceptions": 1, "fumble_recoveries": 0,
             "safeties": 0, "defensive_tds": 0, "special_teams_return_tds": 0,
             "blocked_kicks": 0, "two_point_returns": 0, "points_allowed_fpts": 1,
             "fantasy_points": 5}
    rows = [row(team, "DST", week, **stats) for week in range(1, 4) for team in ("DST:BUF", "DST:MIA")]
    evaluated = backtest(rows, start=(2025, 2))
    assert any(item["rate"] == "dst_dk_points" for item in evaluated)
    report = metrics(evaluated)
    assert next(item for item in report if item["rate"] == "dst_dk_points")["n"] > 0


def test_team_simulation_reconciles_passing_receiving_and_opportunity_budgets():
    forecast = {"game_id": 1, "team": "BUF", "budgets": {
        "attempts": {"mean": 32}, "carries": {"mean": 24}, "targets": {"mean": 30}}, "players": [
        {"identity": "qb", "name": "Quarterback", "position": "QB", "components": {
            "attempts": {"share": .95}, "carries": {"share": .2}}},
        {"identity": "wr", "name": "Receiver", "position": "WR", "components": {
            "targets": {"share": .3}}},
    ]}
    history = qb_history() + qb_history("peer-qb") + wr_history() + wr_history("peer-wr")
    by_player = {identity: [item for item in history if item["identity"] == identity] for identity in ("qb", "wr")}
    by_position = {position: [item for item in history if item["position"] == position] for position in ("QB", "WR")}
    players, audit = simulate_team(forecast, by_player, by_position, {**CONFIG, "draws": 100})
    assert len(players) == 2
    assert all(player["coherence_scope"] == "team_coupled_offense" for player in players)
    assert all(value < 1e-8 for value in audit["max_absolute_mismatch"].values())
    assert audit["mean_unallocated"]["targets"] > 0
    assert all(abs(player["mean_fpts"] - sum(player["scoring_contributions"].values())) < 1e-8 for player in players)


def test_target_week_outcome_cannot_change_forward_efficiency_forecast():
    workload = {"forecasts": [{"game_id": 1, "season": 2026, "week": 1, "kickoff": "x", "team": "BUF", "opponent": "NYJ",
        "budgets": {"attempts": {"mean": 32}, "carries": {"mean": 24}, "targets": {"mean": 30}}, "players": [
            {"identity": "qb", "name": "Quarterback", "position": "QB", "components": {"attempts": {"share": .95}, "carries": {"share": .2}}},
            {"identity": "wr", "name": "Receiver", "position": "WR", "components": {"targets": {"share": .3}}},
        ]}]}
    past = qb_history() + qb_history("peer-qb") + wr_history() + wr_history("peer-wr")
    leaked = {**row("wr", "WR", 1, targets=100, receptions=100, receiving_yards=500, receiving_tds=10,
                    carries=0, rushing_yards=0, rushing_tds=0), "season": 2026}
    assert build(workload, past, {**CONFIG, "draws": 50}) == build(workload, past + [leaked], {**CONFIG, "draws": 50})
