from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pytest

from model.ff_v2_team_opportunity import (
    ContextFeature,
    FALLBACK_CONFIDENCE,
    FALLBACK_UNCERTAINTY,
    build_historical_artifact,
    build_game_latents,
    estimate_parameters,
    forecast_team_week,
    simulate_game_scripts,
)


CUTOFF = datetime(2025, 9, 3, 23, 59, 59, tzinfo=timezone.utc)


def _snapshots(*ids: int, available_at: datetime = CUTOFF) -> list[dict]:
    return [
        {"id": snapshot_id, "available_at": available_at, "model_eligible": True}
        for snapshot_id in ids
    ]

def _row(
    row_id: int,
    *,
    season: int = 2024,
    team: str = "TB",
    opponent: str = "ATL",
    plays: int = 64,
    quarterback: str = "QB1",
    caller: str = "PC1",
) -> dict:
    return {
        "id": row_id,
        "season": season,
        "week": (row_id % 17) + 1,
        "game_id": f"{season}_{row_id}",
        "game_date": f"{season}-10-01",
        "team": team,
        "opponent": opponent,
        "plays": plays,
        "pass_attempts": 36,
        "sacks": 3,
        "allocatable_targets": 34,
        "rush_attempts": plays - 39,
        "rb_carries": max(0, plays - 44),
        "rb_targets": 7,
        "pass_touchdowns": 2,
        "rush_touchdowns": 1,
        "quarterback_gsis_id": quarterback,
        "play_caller_id": caller,
        "source_snapshot_ids": {"pbp": 11, "schedule": 12},
        "fact_digest": f"fact-{row_id}",
        "observed_at": datetime(season, 12, 1, tzinfo=timezone.utc),
    }


def _training() -> list[dict]:
    rows = []
    for index in range(1, 41):
        rows.append(
            _row(
                index,
                season=2023 if index <= 20 else 2024,
                team="TB" if index % 2 else "ATL",
                opponent="ATL" if index % 2 else "TB",
            )
        )
    return rows


def _identity(**overrides) -> dict:
    return {
        "id": 900,
        "season": 2025,
        "week": 1,
        "game_id": "2025_01_TB_ATL",
        "game_date": "2025-09-07",
        "team": "TB",
        "opponent": "ATL",
        "fact_digest": "heldout-fact",
        **overrides,
    }


def test_seeded_draws_reconcile_every_pool_and_repeat_exactly():
    forecast, draws = forecast_team_week(
        _training(),
        _identity(),
        cutoff=CUTOFF,
        root_seed=77,
        draws=2000,
        source_snapshot_ids=[11, 12],
    )
    repeated, repeated_draws = forecast_team_week(
        _training(),
        _identity(),
        cutoff=CUTOFF,
        root_seed=77,
        draws=2000,
        source_snapshot_ids=[11, 12],
    )
    assert forecast == repeated
    assert all(np.array_equal(draws[key], repeated_draws[key]) for key in draws)
    assert np.array_equal(draws["plays"], draws["pass_attempts"] + draws["sacks"] + draws["rush_attempts"])
    assert np.all(draws["allocatable_targets"] <= draws["pass_attempts"])
    assert np.all(draws["rb_targets"] <= draws["allocatable_targets"])
    assert np.all(draws["rb_carries"] <= draws["rush_attempts"])
    assert np.all(draws["pass_touchdowns"] <= draws["pass_attempts"])
    assert np.all(draws["rush_touchdowns"] <= draws["rush_attempts"])
    for distribution in forecast["distributions"].values():
        assert distribution["p10"] <= distribution["p50"] <= distribution["p90"]
    declared = forecast["feature_provenance"]["game_script"]["scenario_probabilities"]
    realized = forecast["feature_provenance"]["game_script"]["realized_scenario_probabilities"]
    assert sum(declared.values()) == pytest.approx(1.0)
    assert sum(realized.values()) == pytest.approx(1.0, abs=1e-6)


def test_opponents_share_latents_and_receive_complementary_scripts():
    latents = build_game_latents(99, "2025_01_TB_ATL", 2000)
    parameters, _, _ = estimate_parameters(
        _training(),
        evaluation_season=2025,
        team="TB",
        opponent="ATL",
        quarterback_id=None,
        play_caller_id=None,
    )
    home = simulate_game_scripts(parameters, tier="B", seed=1, draws=2000, game_latents=latents)
    away = simulate_game_scripts(
        parameters,
        tier="B",
        seed=2,
        draws=2000,
        game_latents=latents,
        complement_scenario=True,
    )
    assert np.array_equal(home["scenario"] + away["scenario"], np.full(2000, 2))
    home_leading = home["pass_attempts"][home["scenario"] == 0].mean()
    home_trailing = home["pass_attempts"][home["scenario"] == 2].mean()
    assert home_trailing > home_leading
    declared = np.asarray([0.25, 0.5, 0.25])
    realized = np.bincount(home["scenario"], minlength=3) / len(home["scenario"])
    assert declared.sum() == pytest.approx(1.0)
    assert realized.sum() == pytest.approx(1.0)
    assert np.all(np.abs(realized - declared) < 0.04)


def test_small_samples_shrink_more_toward_the_league_prior():
    league = [_row(index, team="OTHER", opponent="OTHER2", plays=60) for index in range(1, 41)]
    small = league + [_row(100, team="NEW", opponent="ATL", plays=82)]
    large = league + [_row(100 + index, team="NEW", opponent="ATL", plays=82) for index in range(12)]
    _, small_evidence, _ = estimate_parameters(
        small, evaluation_season=2025, team="NEW", opponent="ATL", quarterback_id=None, play_caller_id=None
    )
    _, large_evidence, _ = estimate_parameters(
        large, evaluation_season=2025, team="NEW", opponent="ATL", quarterback_id=None, play_caller_id=None
    )
    small_team = small_evidence["parameters"]["plays"]["components"]["team"]
    large_team = large_evidence["parameters"]["plays"]["components"]["team"]
    assert small_team["reliability"] < large_team["reliability"]
    assert abs(small_team["shrunk_mean"] - small_team["league_prior"]) < abs(
        large_team["shrunk_mean"] - large_team["league_prior"]
    )


def test_fallbacks_record_missingness_and_widen_uncertainty_monotonically():
    training = _training()
    eligible = ContextFeature("QB1", datetime(2025, 8, 1, tzinfo=timezone.utc))
    caller = ContextFeature("PC1", datetime(2025, 8, 1, tzinfo=timezone.utc))
    forecast_a, _ = forecast_team_week(
        training, _identity(), cutoff=CUTOFF, root_seed=5, draws=8000,
        quarterback=eligible, play_caller=caller, source_snapshot_ids=[11, 12],
    )
    forecast_b, _ = forecast_team_week(
        training, _identity(), cutoff=CUTOFF, root_seed=5, draws=8000, source_snapshot_ids=[11, 12]
    )
    forecast_c, _ = forecast_team_week(
        training, _identity(team="EXP", opponent="NEW"), cutoff=CUTOFF,
        root_seed=5, draws=8000, source_snapshot_ids=[11, 12],
    )
    assert [forecast_a["fallback_tier"], forecast_b["fallback_tier"], forecast_c["fallback_tier"]] == ["A", "B", "C"]
    assert [forecast_a["confidence_multiplier"], forecast_b["confidence_multiplier"], forecast_c["confidence_multiplier"]] == [
        FALLBACK_CONFIDENCE["A"], FALLBACK_CONFIDENCE["B"], FALLBACK_CONFIDENCE["C"]
    ]
    assert [
        forecast_a["feature_provenance"]["game_script"]["uncertainty_multiplier"],
        forecast_b["feature_provenance"]["game_script"]["uncertainty_multiplier"],
        forecast_c["feature_provenance"]["game_script"]["uncertainty_multiplier"],
    ] == [FALLBACK_UNCERTAINTY["A"], FALLBACK_UNCERTAINTY["B"], FALLBACK_UNCERTAINTY["C"]]
    assert forecast_b["feature_provenance"]["context"]["quarterback_missing_reason"] == "no_eligible_as_of_source"

    fixed = estimate_parameters(
        training, evaluation_season=2025, team="TB", opponent="ATL", quarterback_id=None, play_caller_id=None
    )[0]
    widths = []
    for tier in ("A", "B", "C"):
        draws = simulate_game_scripts(fixed, tier=tier, seed=123, draws=20000)
        widths.append(float(np.quantile(draws["plays"], 0.9) - np.quantile(draws["plays"], 0.1)))
    assert widths[0] < widths[1] < widths[2]
    assert FALLBACK_UNCERTAINTY["C"] >= 1 / FALLBACK_CONFIDENCE["C"]


def test_declared_archive_gaps_enforce_tier_c_even_with_large_team_samples():
    forecast, _ = forecast_team_week(
        _training(),
        _identity(),
        cutoff=CUTOFF,
        root_seed=5,
        draws=1000,
        source_snapshot_ids=[11, 12],
        minimum_fallback_tier="C",
        declared_missing_sources=["weekly-stats", "quarterback"],
    )
    assert forecast["fallback_tier"] == "C"
    basis = forecast["feature_provenance"]["fallback_tier_basis"]
    assert basis == {
        "estimated": "B",
        "enforced_minimum": "C",
        "effective": "C",
        "declared_missing_sources": ["quarterback", "weekly-stats"],
    }


def test_eligible_qb_and_play_caller_evidence_is_sample_aware():
    forecast, _ = forecast_team_week(
        _training(),
        _identity(),
        cutoff=CUTOFF,
        root_seed=91,
        draws=1000,
        quarterback=ContextFeature("QB1", datetime(2025, 8, 1, tzinfo=timezone.utc)),
        play_caller=ContextFeature("PC1", datetime(2025, 8, 1, tzinfo=timezone.utc)),
        source_snapshot_ids=[11, 12],
    )
    assert forecast["fallback_tier"] == "A"
    components = forecast["feature_provenance"]["shrinkage"]["parameters"]["plays"]["components"]
    for name in ("quarterback", "play_caller"):
        component = components[name]
        assert component["rows"] > 0
        assert 0 < component["reliability"] < 1
        low, high = sorted((component["raw_mean"], component["league_prior"]))
        assert low <= component["shrunk_mean"] <= high


def test_zero_allocatable_offense_cannot_score_touchdowns():
    parameters, _, _ = estimate_parameters(
        _training(), evaluation_season=2025, team="TB", opponent="ATL",
        quarterback_id=None, play_caller_id=None,
    )
    parameters = {**parameters, "dropback_share": 1.0, "sack_share": 1.0}
    draws = simulate_game_scripts(parameters, tier="B", seed=5, draws=5000)
    zero_opportunity = (draws["pass_attempts"] + draws["rush_attempts"]) == 0
    assert zero_opportunity.any()
    assert np.all((draws["pass_touchdowns"] + draws["rush_touchdowns"])[zero_opportunity] == 0)


def test_touchdown_draws_never_exceed_corresponding_attempts_under_extreme_rates():
    parameters, _, _ = estimate_parameters(
        _training(), evaluation_season=2025, team="TB", opponent="ATL",
        quarterback_id=None, play_caller_id=None,
    )
    parameters = {
        **parameters,
        "touchdowns": 5.5,
        "dropback_share": 0.5,
        "pass_td_share": 0.5,
    }
    draws = simulate_game_scripts(parameters, tier="C", seed=198, draws=200000)
    assert np.all(draws["pass_touchdowns"] <= draws["pass_attempts"])
    assert np.all(draws["rush_touchdowns"] <= draws["rush_attempts"])
    assert np.all(draws["pass_touchdowns"] + draws["rush_touchdowns"] <= draws["plays"])


def test_post_cutoff_context_and_training_fail_closed():
    future = ContextFeature("QB1", datetime(2025, 9, 4, tzinfo=timezone.utc))
    with pytest.raises(ValueError, match="after the simulated cutoff"):
        forecast_team_week(
            _training(), _identity(), cutoff=CUTOFF, root_seed=1, draws=100,
            quarterback=future, source_snapshot_ids=[11],
        )
    training = _training()
    training[0] = {**training[0], "observed_at": datetime(2025, 9, 4, tzinfo=timezone.utc)}
    with pytest.raises(ValueError, match="post-cutoff"):
        forecast_team_week(
            training, _identity(), cutoff=CUTOFF, root_seed=1, draws=100, source_snapshot_ids=[11]
        )


def test_heldout_outcome_fields_cannot_change_forecast():
    base, _ = forecast_team_week(
        _training(), _identity(plays=40, pass_touchdowns=0), cutoff=CUTOFF,
        root_seed=22, draws=1000, source_snapshot_ids=[11, 12],
    )
    mutated, _ = forecast_team_week(
        _training(), _identity(plays=99, pass_touchdowns=9), cutoff=CUTOFF,
        root_seed=22, draws=1000, source_snapshot_ids=[11, 12],
    )
    assert base == mutated


def test_historical_artifact_is_order_independent_and_canonical_context_is_tier_b():
    training = _training()
    home = {**_row(901, season=2025, team="TB", opponent="ATL"), "game_id": "2025_01_TB_ATL"}
    away = {**_row(902, season=2025, team="ATL", opponent="TB"), "game_id": "2025_01_TB_ATL"}
    facts = training + [home, away]
    first = build_historical_artifact(
        facts,
        context_run_id="9077ad91-e258-5e47-beb8-f41b68c6651b",
        evaluation_season=2025,
        cutoff=CUTOFF,
        source_snapshots=_snapshots(11, 12),
        seed=44,
        draws=500,
    )
    second = build_historical_artifact(
        list(reversed(facts)),
        context_run_id="9077ad91-e258-5e47-beb8-f41b68c6651b",
        evaluation_season=2025,
        cutoff=CUTOFF,
        source_snapshots=_snapshots(11, 12),
        seed=44,
        draws=500,
    )
    assert first["artifact_digest"] == second["artifact_digest"]
    assert first["run_id"] == second["run_id"]
    assert {row["fallback_tier"] for row in first["forecasts"]} == {"B"}
    for row in first["forecasts"]:
        context = row["feature_provenance"]["context"]
        assert context["quarterback_missing_reason"] == "no_eligible_as_of_source"
        assert context["play_caller_missing_reason"] == "no_eligible_as_of_source"


def test_historical_artifact_can_separate_archived_training_from_evaluation_facts():
    home = {**_row(901, season=2025, team="TB", opponent="ATL"), "game_id": "2025_01_TB_ATL"}
    away = {**_row(902, season=2025, team="ATL", opponent="TB"), "game_id": "2025_01_TB_ATL"}
    artifact = build_historical_artifact(
        [home, away],
        context_run_id="evaluation-run",
        evaluation_season=2025,
        cutoff=CUTOFF,
        source_snapshots=_snapshots(11, 12),
        training_facts=_training(),
        training_context_run_id="eligible-archive-run",
        minimum_fallback_tier="C",
        declared_missing_sources=["weekly-stats"],
        seed=44,
        draws=100,
    )
    assert artifact["model_config"]["training_context_run_id"] == "eligible-archive-run"
    assert artifact["model_config"]["training_source_mode"] == "separate_archived_context"
    assert artifact["model_config"]["training_seasons"] == [2023, 2024]
    assert {row["fallback_tier"] for row in artifact["forecasts"]} == {"C"}


def test_historical_artifact_rejects_exact_snapshots_fetched_after_cutoff():
    training = _training()
    home = {**_row(901, season=2025, team="TB", opponent="ATL"), "game_id": "2025_01_TB_ATL"}
    away = {**_row(902, season=2025, team="ATL", opponent="TB"), "game_id": "2025_01_TB_ATL"}
    with pytest.raises(ValueError, match="unavailable by the simulated cutoff"):
        build_historical_artifact(
            training + [home, away],
            context_run_id="9077ad91-e258-5e47-beb8-f41b68c6651b",
            evaluation_season=2025,
            cutoff=CUTOFF,
            source_snapshots=_snapshots(
                11, 12, available_at=datetime(2026, 8, 13, tzinfo=timezone.utc)
            ),
            seed=44,
            draws=100,
        )
