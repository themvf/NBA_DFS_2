"""Context-bearing shadow variants: each differs from env_baseline in exactly
one stated way, constants are the registered ones, and nothing is fitted."""

from __future__ import annotations

import pytest

from model.nfl_dfs_environment_variants import (
    OPP_CARRIES_WEIGHT, PRIOR8, TRAILING_CLAMP, VARIANTS_VERSION, context_variants, residual_quantiles,
    rush_factor, trailing_points,
)
from model.nfl_dfs_historical import MODEL_CONFIG, HistoricalWeek


def row(pid, season, week, rush=60, rec=40, *, position="RB"):
    return HistoricalWeek(player_id=pid, player_gsis_id=f"g{pid}", player_name=f"P{pid}", position=position,
                          season=season, week=week, team="T", opponent="O",
                          stats={"rushing_yards": rush, "rushing_tds": 0.5, "receiving_yards": rec, "receptions": 3, "carries": 14})


def history():
    rows = []
    for pid in range(1, 25):
        for season in (2024, 2025):
            rows.extend(row(pid, season, w, rush=50 + pid, rec=30) for w in range(1, 18))
    return rows


def test_trailing_points_is_shrunk_toward_league_and_walk_forward():
    games = [{"season": 2026, "week": w, "home_team": "A", "away_team": "B", "home_score": 30, "away_score": 10, "completed": True}
             for w in (1, 2)] + [{"season": 2026, "week": 3, "home_team": "A", "away_team": "B", "home_score": 99, "away_score": 0, "completed": True}]
    t = trailing_points(games, "A", cutoff=(2026, 3))
    assert t["own_games"] == 2 and t["league_ppg"] == 20.0
    assert 20.0 < t["trailing_ppg"] < 30.0                 # shrunk between own (30) and league (20)
    assert trailing_points(games, "Z", cutoff=(2026, 3)) is None


def test_rush_factor_uses_the_registered_weight_and_is_bounded():
    class Prior:
        league_mean = {"carries": 26.0}
        def own(self, team, field): return {"mean": 26.0}
        def allowed(self, opp, field): return (32.0, 10)
    f = rush_factor(Prior(), "T", "O")
    assert f["factor"] == pytest.approx((26.0 + OPP_CARRIES_WEIGHT * 6.0) / 26.0)
    class Extreme(Prior):
        def allowed(self, opp, field): return (200.0, 10)
    assert rush_factor(Extreme(), "T", "O")["factor"] == 1.5


def test_residual_quantiles_are_walk_forward_and_bucketed():
    q = residual_quantiles(history())
    assert "RB:hist_2_5" in q and "RB:hist_6_16" in q
    assert q["RB:hist_2_5"]["q10"] <= q["RB:hist_2_5"]["q90"]
    assert all(v["n"] >= 30 for v in q.values())


def test_each_variant_changes_exactly_what_it_claims():
    hist = history()
    trailing = {"trailing_ppg": 27.0, "own_games": 10, "league_ppg": 22.5}
    rush = {"factor": 1.2, "own_carries": 26.0, "opp_allowed_carries": 36.4, "opp_games": 10, "league_carries": 26.0}
    v = context_variants(player_id=3, player_gsis_id="g3", player_name="P3", position="RB", historical_rows=hist,
                         cutoff_season=2026, cutoff_week=1, seed=1, config={**MODEL_CONFIG, "draws": 400},
                         team_implied_total=24.0, trailing=trailing, rush=rush,
                         quantiles=residual_quantiles(hist), history_games=17)
    assert v["version"] == VARIANTS_VERSION
    base = v["env_baseline"]["mean"]
    # A 27-ppg team at a 24 implied total is BELOW its own pace: factor < 1 vs 24/22.5 > 1.
    assert v["env_trailing"]["mean"] < base
    # Rushing up-scaled by 1.2 must raise an RB with rushing yards.
    assert v["opp_carries"]["mean"] > base
    # interval_rq keeps the mean and re-draws the interval from residuals.
    assert v["interval_rq"]["mean"] == base and v["interval_rq"]["bucket"] == "RB:hist_17_plus"
    assert v["interval_rq"]["p10"] <= base <= v["interval_rq"]["p90"]
    # prior8 shrinks a 17-game player harder toward peers than prior 4 does.
    assert v["prior8"]["mean"] != base
    assert TRAILING_CLAMP == (0.70, 1.30) and PRIOR8 == 8.0


def test_variants_are_none_when_their_inputs_are_missing():
    hist = history()
    v = context_variants(player_id=3, player_gsis_id="g3", player_name="P3", position="RB", historical_rows=hist,
                         cutoff_season=2026, cutoff_week=1, seed=1, config={**MODEL_CONFIG, "draws": 200},
                         team_implied_total=None, trailing=None, rush=None, quantiles={}, history_games=17)
    assert v["env_trailing"] is None and v["opp_carries"] is None and v["interval_rq"] is None
    assert v["env_baseline"]["mean"] is not None and v["prior8"]["mean"] is not None
