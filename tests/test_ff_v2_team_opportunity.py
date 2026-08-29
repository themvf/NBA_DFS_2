from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ingest.ff_v2_team_opportunity import (
    REQUIRED_FORECAST_POOLS,
    persist_forecast_artifact,
    prepare_forecast_artifact,
)


def _fact(game_id: str = "2025_01_TB_ATL", team: str = "TB") -> dict:
    return {
        "id": 101,
        "season": 2025,
        "week": 1,
        "game_id": game_id,
        "game_date": "2025-09-07",
        "team": team,
        "opponent": "ATL",
        "fact_digest": "fact-sha-101",
        "source_snapshot_ids": [11, 12],
    }


def _forecast(game_id: str = "2025_01_TB_ATL", team: str = "TB") -> dict:
    # Contract-only synthetic values: never persisted to the configured DB.
    distributions = {
        pool: {
            "expected_value": 10.0,
            "dispersion": 2.0,
            "p10": 7.0,
            "p50": 10.0,
            "p90": 13.0,
            "distribution_family": "test_fixture",
            "parameters": {"fixture": True},
        }
        for pool in REQUIRED_FORECAST_POOLS
    }
    return {
        "game_id": game_id,
        "team": team,
        "fallback_tier": "A",
        "confidence_multiplier": 0.9,
        "source_snapshot_ids": [11, 12],
        "feature_provenance": {"fixture": "contract-test-only"},
        "distributions": distributions,
    }


def _artifact(forecasts: list[dict] | None = None, facts: list[dict] | None = None) -> dict:
    return prepare_forecast_artifact(
        context_run_id="9077ad91-e258-5e47-beb8-f41b68c6651b",
        model_version="unfitted-contract-test",
        calibration_version="none",
        as_of_at=datetime(2025, 8, 26, 16, tzinfo=timezone.utc),
        source_snapshot_ids=[11, 12],
        facts=facts or [_fact()],
        forecasts=forecasts or [_forecast()],
        model_config={"fixture": True},
    )


class MemoryRepository:
    def __init__(self) -> None:
        self.runs: dict[str, str] = {}
        self.forecasts: dict[tuple[str, str, str], tuple[int, str]] = {}
        self.distributions: dict[int, dict[str, str]] = {}
        self.next_id = 1

    def get_run_digest(self, run_id: str):
        return self.runs.get(run_id)

    def insert_run(self, artifact):
        self.runs[artifact["run_id"]] = artifact["artifact_digest"]
        return True

    def get_forecast(self, run_id: str, game_id: str, team: str):
        return self.forecasts.get((run_id, game_id, team))

    def insert_forecast(self, run_id: str, row):
        forecast_id = self.next_id
        self.next_id += 1
        self.forecasts[(run_id, row["game_id"], row["team"])] = (
            forecast_id,
            row["forecast_digest"],
        )
        self.distributions[forecast_id] = {}
        return forecast_id

    def get_distribution_digests(self, forecast_id: int):
        return dict(self.distributions[forecast_id])

    def insert_distribution(self, forecast_id: int, pool: str, row):
        self.distributions[forecast_id][pool] = row["distribution_digest"]
        return True


def test_duplicate_forecast_identity_is_rejected_before_persistence():
    with pytest.raises(ValueError, match="Duplicate forecast identity"):
        _artifact([_forecast(), _forecast()])


def test_duplicate_fact_identity_is_rejected():
    with pytest.raises(ValueError, match="Duplicate context fact identity"):
        _artifact(facts=[_fact(), {**_fact(), "id": 102}])


def test_forecast_for_missing_game_is_rejected():
    with pytest.raises(ValueError, match="missing context game"):
        _artifact([_forecast("2025_02_TB_BUF")])


def test_all_required_distribution_pools_and_monotonic_quantiles_are_enforced():
    forecast = _forecast()
    forecast["distributions"].pop("rb_targets")
    with pytest.raises(ValueError, match="Missing forecast pools"):
        _artifact([forecast])

    forecast = _forecast()
    forecast["distributions"]["plays"]["p10"] = 11
    with pytest.raises(ValueError, match="not monotonic"):
        _artifact([forecast])


def test_exact_refresh_is_idempotent_and_preserves_digests():
    repository = MemoryRepository()
    artifact = _artifact()

    first = persist_forecast_artifact(repository, artifact)
    second = persist_forecast_artifact(repository, artifact)

    assert first == {
        "inserted_run": 1,
        "inserted_forecasts": 1,
        "inserted_distributions": len(REQUIRED_FORECAST_POOLS),
    }
    assert second == {
        "inserted_run": 0,
        "inserted_forecasts": 0,
        "inserted_distributions": 0,
    }
    assert repository.runs[artifact["run_id"]] == artifact["artifact_digest"]


def test_tampered_artifact_is_rejected_before_write():
    repository = MemoryRepository()
    artifact = _artifact()
    artifact["forecasts"][0]["confidence_multiplier"] = 0.1
    with pytest.raises(ValueError, match="identity does not match"):
        persist_forecast_artifact(repository, artifact)
    assert repository.runs == {}


def test_artifact_identity_is_order_independent():
    first_fact = _fact()
    second_fact = {
        **_fact("2025_01_CAR_JAX", "CAR"),
        "id": 202,
        "opponent": "JAX",
        "fact_digest": "fact-sha-202",
    }
    first = _forecast()
    second = _forecast("2025_01_CAR_JAX", "CAR")
    artifact_a = _artifact([first, second], [first_fact, second_fact])
    artifact_b = _artifact([second, first], [second_fact, first_fact])
    assert artifact_a["artifact_digest"] == artifact_b["artifact_digest"]
    assert artifact_a["run_id"] == artifact_b["run_id"]
