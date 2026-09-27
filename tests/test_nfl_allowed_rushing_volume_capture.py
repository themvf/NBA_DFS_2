from model.nfl_dfs_environment_variants import _project_with_rush_factor
from model.nfl_dfs_historical import HistoricalWeek, MODEL_CONFIG, ProjectionContext
from model.nfl_matchup_projection import sample_baseline_draws, summarize_draws
from research.nfl_allowed_rushing_volume_capture import frozen_volume_projection


def test_full_volume_distribution_reproduces_frozen_compact_draw_loop():
    history = [HistoricalWeek(player_id, gsis, name, "RB", 2025, week, team, "SF",
                              {"carries": 14 + week, "rushing_yards": 50 + 11 * week,
                               "rushing_tds": week % 2, "receptions": 2,
                               "receiving_yards": 12, "receiving_tds": 0})
               for player_id, gsis, name, team in [(1, "gsis-1", "Runner One", "ARI"),
                                                     (2, "gsis-2", "Runner Two", "NYG")]
               for week in range(1, 5)]
    config = {**MODEL_CONFIG, "draws": 256}
    baseline_player = {"player_id": 1, "player_gsis_id": "gsis-1", "player_name": "Runner One",
                       "position": "RB", "season": 2026, "week": 3,
                       "feature_snapshot": {"cutoff_season": 2026, "cutoff_week": 3,
                                            "team_implied_total": 24.0}}
    draws = sample_baseline_draws(baseline_player, history, config=config, seed=20260902)
    baseline = summarize_draws("RB", draws)
    player = {**baseline_player, "model_proj_fpts": round(baseline["mean"], 4),
              "floor_fpts": round(baseline["p10"], 4),
              "median_fpts": round(baseline["p50"], 4),
              "ceiling_fpts": round(baseline["p90"], 4),
              "boom_rate": round(baseline["boom"], 6),
              "stat_means": {k: round(v, 4) for k, v in baseline["stat_means"].items()}}
    rush = {"factor": 1.1, "own_carries": 20, "opp_allowed_carries": 24,
            "opp_games": 4, "league_carries": 20}
    full = frozen_volume_projection(player, draws, rush, "2026_03_ARI_SF")
    compact = _project_with_rush_factor(player_id=1, player_gsis_id="gsis-1",
        player_name="Runner One", position="RB", historical_rows=history,
        cutoff_season=2026, cutoff_week=3, context=ProjectionContext(team_implied_total=24.0),
        seed=20260902, config=config, factor=1.1)
    assert full["status"] == "under_evaluation"
    assert round(full["candidate"]["mean"], 4) == compact["mean"]
    assert round(full["candidate"]["p10"], 4) == compact["p10"]
    assert round(full["candidate"]["p90"], 4) == compact["p90"]
    assert round(full["candidate"]["boom"], 6) == compact["boom_probability"]
    assert full["candidate"]["p50"] is not None
    assert set(full["candidate"]["stat_means"]) == set(baseline["stat_means"])
