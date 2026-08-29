"""Versioned persistence contract for roster-aware V2 team opportunity forecasts.

This module intentionally does not fit or synthesize forecast values. V2-008
supplies calibrated distributions; V2-007 validates their identity and
provenance against the immutable V2-003 facts and persists them idempotently.
"""

from __future__ import annotations

import hashlib
import json
import math
import uuid
from datetime import date, datetime
from typing import Any, Iterable, Mapping, Protocol, Sequence

from psycopg2.extras import Json


CONTRACT_VERSION = "ff-v2-team-opportunity-contract-v1"
FORECAST_NAMESPACE = uuid.UUID("5a5f48cd-34ee-49b9-860a-0bd9514fdd46")
REQUIRED_FORECAST_POOLS = frozenset(
    {
        "plays",
        "pass_attempts",
        "allocatable_targets",
        "rb_carries",
        "rb_targets",
        "pass_touchdowns",
        "rush_touchdowns",
    }
)
ALLOWED_FORECAST_POOLS = REQUIRED_FORECAST_POOLS | {"rush_attempts"}


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"Unsupported canonical JSON value: {type(value)!r}")


def canonical_digest(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def load_team_week_facts(conn: Any, context_run_id: str) -> list[dict[str, Any]]:
    """Load the V2-003 facts that establish valid forecast identities."""
    cursor = conn.cursor()
    cursor.execute(
        """SELECT id, season, week, game_id, game_date, team, opponent,
                  fact_digest, source_snapshot_ids
             FROM ff_v2_team_week_facts
            WHERE run_id=%s
            ORDER BY season, week, game_id, team""",
        (context_run_id,),
    )
    return [dict(row) for row in cursor.fetchall()]


def _fact_index(facts: Iterable[Mapping[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in facts:
        row = dict(raw)
        identity = (str(row.get("game_id") or ""), str(row.get("team") or ""))
        if not all(identity):
            raise ValueError("Every context fact must have game_id and team")
        if identity in result:
            raise ValueError(f"Duplicate context fact identity: {identity[0]}/{identity[1]}")
        if not row.get("id") or not row.get("fact_digest"):
            raise ValueError(f"Context fact {identity[0]}/{identity[1]} lacks persisted identity")
        result[identity] = row
    return result


def _distribution(pool: str, raw: Mapping[str, Any]) -> dict[str, Any]:
    required = ("expected_value", "dispersion", "p10", "p50", "p90", "distribution_family")
    missing = [field for field in required if raw.get(field) is None]
    if missing:
        raise ValueError(f"{pool} distribution is missing {', '.join(missing)}")
    values = {field: float(raw[field]) for field in required[:-1]}
    if not all(math.isfinite(value) and value >= 0 for value in values.values()):
        raise ValueError(f"{pool} distribution contains a negative or non-finite value")
    if not values["p10"] <= values["p50"] <= values["p90"]:
        raise ValueError(f"{pool} quantiles are not monotonic")
    family = str(raw["distribution_family"]).strip()
    if not family:
        raise ValueError(f"{pool} distribution_family is required")
    result = {
        **values,
        "distribution_family": family,
        "parameters": dict(raw.get("parameters") or {}),
    }
    result["distribution_digest"] = canonical_digest({"opportunity_type": pool, **result})
    return result


def prepare_forecast_artifact(
    *,
    context_run_id: str,
    model_version: str,
    calibration_version: str,
    as_of_at: datetime,
    source_snapshot_ids: Sequence[int],
    facts: Iterable[Mapping[str, Any]],
    forecasts: Iterable[Mapping[str, Any]],
    model_config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate externally supplied distributions and build an immutable artifact.

    No forecast values are derived here. A forecast must resolve to exactly one
    persisted V2-003 game/team fact, and all required pools must be supplied.
    """
    if as_of_at.tzinfo is None:
        raise ValueError("as_of_at must be timezone-aware")
    if not model_version.strip() or not calibration_version.strip():
        raise ValueError("model_version and calibration_version are required")
    snapshots = sorted({int(value) for value in source_snapshot_ids})
    if not snapshots:
        raise ValueError("At least one eligible feature source snapshot is required")

    indexed_facts = _fact_index(facts)
    prepared: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for raw in forecasts:
        game_id = str(raw.get("game_id") or "")
        team = str(raw.get("team") or "")
        identity = (game_id, team)
        if not all(identity):
            raise ValueError("Every forecast must have game_id and team")
        if identity in seen:
            raise ValueError(f"Duplicate forecast identity: {game_id}/{team}")
        seen.add(identity)
        fact = indexed_facts.get(identity)
        if fact is None:
            raise ValueError(f"Forecast references missing context game: {game_id}/{team}")

        raw_distributions = dict(raw.get("distributions") or {})
        unknown = set(raw_distributions) - ALLOWED_FORECAST_POOLS
        missing = REQUIRED_FORECAST_POOLS - set(raw_distributions)
        if unknown:
            raise ValueError(f"Unknown forecast pools for {game_id}/{team}: {sorted(unknown)}")
        if missing:
            raise ValueError(f"Missing forecast pools for {game_id}/{team}: {sorted(missing)}")
        distributions = {
            pool: _distribution(pool, raw_distributions[pool])
            for pool in sorted(raw_distributions)
        }
        fallback_tier = str(raw.get("fallback_tier") or "")
        confidence = float(raw.get("confidence_multiplier", 0))
        if fallback_tier not in {"A", "B", "C"}:
            raise ValueError(f"Invalid fallback tier for {game_id}/{team}")
        if not math.isfinite(confidence) or not 0 < confidence <= 1:
            raise ValueError(f"Invalid confidence multiplier for {game_id}/{team}")
        row_snapshots = sorted({int(value) for value in raw.get("source_snapshot_ids", snapshots)})
        if not row_snapshots or not set(row_snapshots).issubset(snapshots):
            raise ValueError(f"Forecast source snapshots are not declared by the run: {game_id}/{team}")
        feature_provenance = dict(raw.get("feature_provenance") or {})
        if not feature_provenance:
            raise ValueError(f"Feature provenance is required for {game_id}/{team}")

        row = {
            "context_fact_id": int(fact["id"]),
            "context_fact_digest": str(fact["fact_digest"]),
            "season": int(fact["season"]),
            "week": int(fact["week"]),
            "game_id": game_id,
            "game_date": str(fact["game_date"]),
            "team": team,
            "opponent": str(fact["opponent"]),
            "fallback_tier": fallback_tier,
            "confidence_multiplier": confidence,
            "source_snapshot_ids": row_snapshots,
            "feature_provenance": feature_provenance,
            "as_of_at": as_of_at.isoformat(),
            "distributions": distributions,
        }
        row["forecast_digest"] = canonical_digest(row)
        prepared.append(row)

    prepared.sort(key=lambda row: (row["season"], row["week"], row["game_id"], row["team"]))
    body = {
        "contract_version": CONTRACT_VERSION,
        "context_run_id": str(context_run_id),
        "model_version": model_version,
        "calibration_version": calibration_version,
        "as_of_at": as_of_at.isoformat(),
        "source_snapshot_ids": snapshots,
        "model_config": dict(model_config or {}),
        "forecasts": prepared,
    }
    digest = canonical_digest(body)
    return {
        **body,
        "forecast_count": len(prepared),
        "artifact_digest": digest,
        "run_id": str(uuid.uuid5(FORECAST_NAMESPACE, digest)),
    }


class ForecastRepository(Protocol):
    def get_run_digest(self, run_id: str) -> str | None: ...
    def insert_run(self, artifact: Mapping[str, Any]) -> bool: ...
    def get_forecast(self, run_id: str, game_id: str, team: str) -> tuple[int, str] | None: ...
    def insert_forecast(self, run_id: str, row: Mapping[str, Any]) -> int: ...
    def get_distribution_digests(self, forecast_id: int) -> dict[str, str]: ...
    def insert_distribution(self, forecast_id: int, pool: str, row: Mapping[str, Any]) -> bool: ...


def _verify_artifact_identity(artifact: Mapping[str, Any]) -> None:
    forecasts = list(artifact.get("forecasts") or [])
    if int(artifact.get("forecast_count", -1)) != len(forecasts):
        raise ValueError("Forecast artifact row count does not match its payload")
    body = {
        "contract_version": artifact.get("contract_version"),
        "context_run_id": artifact.get("context_run_id"),
        "model_version": artifact.get("model_version"),
        "calibration_version": artifact.get("calibration_version"),
        "as_of_at": artifact.get("as_of_at"),
        "source_snapshot_ids": artifact.get("source_snapshot_ids"),
        "model_config": artifact.get("model_config"),
        "forecasts": forecasts,
    }
    digest = canonical_digest(body)
    run_id = str(uuid.uuid5(FORECAST_NAMESPACE, digest))
    if artifact.get("contract_version") != CONTRACT_VERSION:
        raise ValueError("Unsupported team opportunity forecast contract version")
    if artifact.get("artifact_digest") != digest or str(artifact.get("run_id")) != run_id:
        raise ValueError("Forecast artifact identity does not match its payload")


def persist_forecast_artifact(repository: ForecastRepository, artifact: Mapping[str, Any]) -> dict[str, int]:
    """Persist once and verify exact identity on every idempotent refresh."""
    _verify_artifact_identity(artifact)
    run_id = str(artifact["run_id"])
    digest = str(artifact["artifact_digest"])
    existing_run = repository.get_run_digest(run_id)
    if existing_run is not None and existing_run != digest:
        raise RuntimeError(f"Forecast run identity collision: {run_id}")
    inserted_run = int(existing_run is None and repository.insert_run(artifact))
    inserted_forecasts = 0
    inserted_distributions = 0
    for row in artifact["forecasts"]:
        existing = repository.get_forecast(run_id, row["game_id"], row["team"])
        if existing is not None:
            forecast_id, stored_digest = existing
            if stored_digest != row["forecast_digest"]:
                raise RuntimeError(f"Forecast identity collision: {row['game_id']}/{row['team']}")
        else:
            forecast_id = repository.insert_forecast(run_id, row)
            inserted_forecasts += 1
        stored_distributions = repository.get_distribution_digests(forecast_id)
        expected = row["distributions"]
        extra = set(stored_distributions) - set(expected)
        if extra:
            raise RuntimeError(f"Unexpected stored forecast pools: {sorted(extra)}")
        for pool, distribution in expected.items():
            stored_digest = stored_distributions.get(pool)
            if stored_digest is not None and stored_digest != distribution["distribution_digest"]:
                raise RuntimeError(f"Distribution identity collision: {row['game_id']}/{row['team']}/{pool}")
            if stored_digest is None and repository.insert_distribution(forecast_id, pool, distribution):
                inserted_distributions += 1
    return {
        "inserted_run": inserted_run,
        "inserted_forecasts": inserted_forecasts,
        "inserted_distributions": inserted_distributions,
    }


class PostgresForecastRepository:
    """PostgreSQL adapter; callers own the surrounding transaction."""

    def __init__(self, conn: Any) -> None:
        self.conn = conn

    def get_run_digest(self, run_id: str) -> str | None:
        cursor = self.conn.cursor()
        cursor.execute("SELECT artifact_digest FROM ff_v2_team_opportunity_forecast_runs WHERE run_id=%s", (run_id,))
        row = cursor.fetchone()
        return str(row["artifact_digest"]) if row else None

    def insert_run(self, artifact: Mapping[str, Any]) -> bool:
        cursor = self.conn.cursor()
        cursor.execute(
            """INSERT INTO ff_v2_team_opportunity_forecast_runs
               (run_id,contract_version,context_run_id,model_version,calibration_version,
                as_of_at,source_snapshot_ids,model_config,forecast_count,artifact_digest)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT(run_id) DO NOTHING RETURNING run_id""",
            (
                artifact["run_id"], artifact["contract_version"], artifact["context_run_id"],
                artifact["model_version"], artifact["calibration_version"], artifact["as_of_at"],
                Json(artifact["source_snapshot_ids"]), Json(artifact["model_config"]),
                artifact["forecast_count"], artifact["artifact_digest"],
            ),
        )
        return cursor.fetchone() is not None

    def get_forecast(self, run_id: str, game_id: str, team: str) -> tuple[int, str] | None:
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT id, forecast_digest FROM ff_v2_team_opportunity_forecasts
                WHERE forecast_run_id=%s AND game_id=%s AND team=%s""",
            (run_id, game_id, team),
        )
        row = cursor.fetchone()
        return (int(row["id"]), str(row["forecast_digest"])) if row else None

    def insert_forecast(self, run_id: str, row: Mapping[str, Any]) -> int:
        cursor = self.conn.cursor()
        cursor.execute(
            """INSERT INTO ff_v2_team_opportunity_forecasts
               (forecast_run_id,context_fact_id,context_fact_digest,season,week,game_id,game_date,
                team,opponent,fallback_tier,confidence_multiplier,source_snapshot_ids,
                feature_provenance,forecast_digest,as_of_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               RETURNING id""",
            (
                run_id, row["context_fact_id"], row["context_fact_digest"], row["season"],
                row["week"], row["game_id"], row["game_date"], row["team"], row["opponent"],
                row["fallback_tier"], row["confidence_multiplier"], Json(row["source_snapshot_ids"]),
                Json(row["feature_provenance"]), row["forecast_digest"], row["as_of_at"],
            ),
        )
        return int(cursor.fetchone()["id"])

    def get_distribution_digests(self, forecast_id: int) -> dict[str, str]:
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT opportunity_type, distribution_digest
                 FROM ff_v2_team_opportunity_distributions WHERE forecast_id=%s""",
            (forecast_id,),
        )
        return {str(row["opportunity_type"]): str(row["distribution_digest"]) for row in cursor.fetchall()}

    def insert_distribution(self, forecast_id: int, pool: str, row: Mapping[str, Any]) -> bool:
        cursor = self.conn.cursor()
        cursor.execute(
            """INSERT INTO ff_v2_team_opportunity_distributions
               (forecast_id,opportunity_type,expected_value,dispersion,p10,p50,p90,
                distribution_family,parameters,distribution_digest)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT(forecast_id,opportunity_type) DO NOTHING RETURNING id""",
            (
                forecast_id, pool, row["expected_value"], row["dispersion"], row["p10"],
                row["p50"], row["p90"], row["distribution_family"], Json(row["parameters"]),
                row["distribution_digest"],
            ),
        )
        return cursor.fetchone() is not None


def persist_with_database(database: Any, artifact: Mapping[str, Any]) -> dict[str, int]:
    """Persist a prepared artifact in one DatabaseManager transaction."""
    with database.connect() as conn:
        return persist_forecast_artifact(PostgresForecastRepository(conn), artifact)
