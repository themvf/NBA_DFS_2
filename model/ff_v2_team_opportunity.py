"""Interpretable rolling-origin V2 team-opportunity shadow model.

The model consumes only prior-season V2-003 facts. It creates correlated game
scripts and persists their summaries through the V2-007 artifact contract. It
does not feed live redraft, Best Ball, rankings, or advisor paths.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from config import AppConfig
from db.database import DatabaseManager
from ingest.ff_v2_team_opportunity import (
    canonical_digest,
    persist_with_database,
    prepare_forecast_artifact,
)
from model.ff_v2_backtest import DEFAULT_SEED


MODEL_VERSION = "ff-v2-team-opportunity-v1"
CALIBRATION_VERSION = "uncalibrated-shadow-v1"
DEFAULT_DRAWS = 4000
FALLBACK_UNCERTAINTY = {"A": 1.0, "B": 1.25, "C": 1.7}
FALLBACK_CONFIDENCE = {"A": 1.0, "B": 0.8, "C": 0.6}
SCENARIO_PROBABILITIES = {"leading": 0.25, "neutral": 0.5, "trailing": 0.25}
FALLBACK_ORDER = {"A": 0, "B": 1, "C": 2}


@dataclass(frozen=True)
class ContextFeature:
    value: str | None
    available_at: datetime | None


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        result = datetime.fromisoformat(str(value))
    if result.tzinfo is None:
        raise ValueError("Feature and observation timestamps must be timezone-aware")
    return result


def _eligible_context(feature: ContextFeature | None, cutoff: datetime, name: str) -> str | None:
    if feature is None or feature.value is None:
        return None
    if feature.available_at is None:
        raise ValueError(f"{name} has a value without an availability timestamp")
    available_at = _as_datetime(feature.available_at)
    if available_at > cutoff:
        raise ValueError(f"{name} became available after the simulated cutoff")
    return str(feature.value)


def _value(row: Mapping[str, Any], parameter: str) -> float | None:
    plays = float(row.get("plays") or 0)
    pass_attempts = float(row.get("pass_attempts") or 0)
    sacks = float(row.get("sacks") or 0)
    rush_attempts = float(row.get("rush_attempts") or 0)
    targets = float(row.get("allocatable_targets") or 0)
    if parameter == "plays":
        return plays
    if parameter == "dropback_share":
        return (pass_attempts + sacks) / plays if plays else None
    if parameter == "sack_share":
        dropbacks = pass_attempts + sacks
        return sacks / dropbacks if dropbacks else None
    if parameter == "target_share":
        return targets / pass_attempts if pass_attempts else None
    if parameter == "rb_carry_share":
        return float(row.get("rb_carries") or 0) / rush_attempts if rush_attempts else None
    if parameter == "rb_target_share":
        return float(row.get("rb_targets") or 0) / targets if targets else None
    if parameter == "touchdowns":
        return float(row.get("pass_touchdowns") or 0) + float(row.get("rush_touchdowns") or 0)
    if parameter == "pass_td_share":
        touchdowns = float(row.get("pass_touchdowns") or 0) + float(row.get("rush_touchdowns") or 0)
        return float(row.get("pass_touchdowns") or 0) / touchdowns if touchdowns else None
    raise KeyError(parameter)


PARAMETERS = (
    "plays",
    "dropback_share",
    "sack_share",
    "target_share",
    "rb_carry_share",
    "rb_target_share",
    "touchdowns",
    "pass_td_share",
)
PRIOR_STRENGTH = {"team": 8.0, "opponent": 12.0, "quarterback": 20.0, "play_caller": 28.0}
EFFECT_WEIGHT = {"team": 1.0, "opponent": 0.35, "quarterback": 0.25, "play_caller": 0.2}


def _season_weight(row: Mapping[str, Any], evaluation_season: int) -> float:
    distance = evaluation_season - int(row["season"])
    return 0.72 ** max(0, distance - 1)


def _weighted_values(
    rows: Iterable[Mapping[str, Any]], parameter: str, evaluation_season: int
) -> tuple[list[float], list[float]]:
    values: list[float] = []
    weights: list[float] = []
    for row in rows:
        value = _value(row, parameter)
        if value is None or not math.isfinite(value):
            continue
        values.append(value)
        weights.append(_season_weight(row, evaluation_season))
    return values, weights


def _weighted_mean(values: Sequence[float], weights: Sequence[float]) -> float:
    return float(np.average(np.asarray(values, dtype=float), weights=np.asarray(weights, dtype=float)))


def _shrunk_component(
    rows: Sequence[Mapping[str, Any]],
    *,
    parameter: str,
    evaluation_season: int,
    league_mean: float,
    group: str,
) -> tuple[float, dict[str, Any]]:
    values, weights = _weighted_values(rows, parameter, evaluation_season)
    effective_n = float(sum(weights))
    strength = PRIOR_STRENGTH[group]
    raw_mean = _weighted_mean(values, weights) if values else league_mean
    shrunk = (raw_mean * effective_n + league_mean * strength) / (effective_n + strength)
    return shrunk, {
        "rows": len(values),
        "effective_sample": round(effective_n, 6),
        "prior_strength": strength,
        "reliability": round(effective_n / (effective_n + strength), 6),
        "raw_mean": round(raw_mean, 6),
        "league_prior": round(league_mean, 6),
        "shrunk_mean": round(shrunk, 6),
    }


def _clamp_parameter(parameter: str, value: float) -> float:
    bounds = {
        "plays": (45.0, 85.0),
        "dropback_share": (0.35, 0.82),
        "sack_share": (0.015, 0.2),
        "target_share": (0.65, 1.0),
        "rb_carry_share": (0.25, 0.98),
        "rb_target_share": (0.03, 0.6),
        "touchdowns": (0.2, 5.5),
        "pass_td_share": (0.2, 0.95),
    }
    low, high = bounds[parameter]
    return min(high, max(low, value))


def estimate_parameters(
    training_rows: Sequence[Mapping[str, Any]],
    *,
    evaluation_season: int,
    team: str,
    opponent: str,
    quarterback_id: str | None,
    play_caller_id: str | None,
) -> tuple[dict[str, float], dict[str, Any], str]:
    """Estimate parameters with explicit sample-size shrinkage to league priors."""
    if not training_rows:
        raise ValueError("At least one prior-season training row is required")
    team_rows = [row for row in training_rows if str(row.get("team")) == team]
    opponent_rows = [row for row in training_rows if str(row.get("opponent")) == opponent]
    quarterback_rows = [
        row for row in training_rows
        if quarterback_id and str(row.get("quarterback_gsis_id") or "") == quarterback_id
    ]
    caller_rows = [
        row for row in training_rows
        if play_caller_id and str(row.get("play_caller_id") or "") == play_caller_id
    ]
    tier = "C" if len(team_rows) < 4 or len(opponent_rows) < 4 else (
        "A" if quarterback_id and play_caller_id and quarterback_rows and caller_rows else "B"
    )

    estimates: dict[str, float] = {}
    evidence: dict[str, Any] = {"fallback_tier": tier, "parameters": {}}
    for parameter in PARAMETERS:
        league_values, league_weights = _weighted_values(training_rows, parameter, evaluation_season)
        if not league_values:
            raise ValueError(f"No eligible training values for {parameter}")
        league_mean = _weighted_mean(league_values, league_weights)
        components: dict[str, Any] = {}
        estimate = league_mean
        for group, rows in (
            ("team", team_rows),
            ("opponent", opponent_rows),
            ("quarterback", quarterback_rows),
            ("play_caller", caller_rows),
        ):
            if group in {"quarterback", "play_caller"} and not rows:
                components[group] = {
                    "rows": 0,
                    "effective_sample": 0.0,
                    "prior_strength": PRIOR_STRENGTH[group],
                    "reliability": 0.0,
                    "raw_mean": None,
                    "league_prior": round(league_mean, 6),
                    "shrunk_mean": round(league_mean, 6),
                    "missing": True,
                }
                continue
            shrunk, component = _shrunk_component(
                rows,
                parameter=parameter,
                evaluation_season=evaluation_season,
                league_mean=league_mean,
                group=group,
            )
            components[group] = component
            estimate += EFFECT_WEIGHT[group] * (shrunk - league_mean)
        estimates[parameter] = _clamp_parameter(parameter, estimate)
        evidence["parameters"][parameter] = {
            "league_prior": round(league_mean, 6),
            "estimate": round(estimates[parameter], 6),
            "components": components,
        }
    play_values, play_weights = _weighted_values(training_rows, "plays", evaluation_season)
    mean_play = _weighted_mean(play_values, play_weights)
    variance = float(np.average((np.asarray(play_values) - mean_play) ** 2, weights=play_weights))
    estimates["plays_sd"] = max(4.0, math.sqrt(variance))
    evidence["training_rows"] = len(training_rows)
    evidence["team_rows"] = len(team_rows)
    evidence["opponent_rows"] = len(opponent_rows)
    evidence["quarterback_rows"] = len(quarterback_rows)
    evidence["play_caller_rows"] = len(caller_rows)
    return estimates, evidence, tier


def _identity_seed(root_seed: int, game_id: str, team: str) -> int:
    token = f"team-opportunity:{game_id}:{team}"
    return int(hashlib.sha256(f"{int(root_seed)}|{token}".encode("utf-8")).hexdigest()[:16], 16)


def build_game_latents(root_seed: int, game_id: str, draws: int) -> dict[str, np.ndarray]:
    """Create game-level state shared by both teams, independent of row order."""
    game_seed = _identity_seed(root_seed, game_id, "GAME")
    rng = np.random.default_rng(game_seed)
    return {
        "scenario": rng.choice(3, size=draws, p=[0.25, 0.5, 0.25]),
        "volume_z": rng.normal(0, 1, size=draws),
        "scoring_z": rng.normal(0, 1, size=draws),
    }


def simulate_game_scripts(
    parameters: Mapping[str, float],
    *,
    tier: str,
    seed: int,
    draws: int,
    game_latents: Mapping[str, np.ndarray] | None = None,
    complement_scenario: bool = False,
) -> dict[str, np.ndarray]:
    """Draw one coherent hierarchy rather than independent opportunity counts."""
    if draws < 100:
        raise ValueError("At least 100 draws are required")
    uncertainty = FALLBACK_UNCERTAINTY[tier]
    rng = np.random.default_rng(seed)
    if game_latents is None:
        scenarios = rng.choice(3, size=draws, p=[0.25, 0.5, 0.25])
        volume_z = rng.normal(0, 1, size=draws)
        independent_scoring = rng.normal(0, 1, size=draws)
    else:
        scenarios = np.asarray(game_latents["scenario"], dtype=int)
        volume_z = np.asarray(game_latents["volume_z"], dtype=float)
        independent_scoring = np.asarray(game_latents["scoring_z"], dtype=float)
        if any(len(values) != draws for values in (scenarios, volume_z, independent_scoring)):
            raise ValueError("Game latent draw counts do not match")
    if complement_scenario:
        scenarios = 2 - scenarios
    script_pass_shift = np.choose(scenarios, [-0.10, 0.0, 0.12])
    script_play_shift = np.choose(scenarios, [-1.5, 0.0, 1.5])
    scoring_z = 0.35 * volume_z + math.sqrt(1 - 0.35**2) * independent_scoring
    plays = np.rint(
        parameters["plays"] + script_play_shift + volume_z * parameters["plays_sd"] * uncertainty
    ).clip(30, 100).astype(int)
    dropback_probability = np.clip(parameters["dropback_share"] + script_pass_shift, 0.2, 0.9)
    dropbacks = rng.binomial(plays, dropback_probability)
    sacks = rng.binomial(dropbacks, parameters["sack_share"])
    pass_attempts = dropbacks - sacks
    rush_attempts = plays - dropbacks
    allocatable_targets = rng.binomial(pass_attempts, parameters["target_share"])
    rb_targets = rng.binomial(allocatable_targets, parameters["rb_target_share"])
    rb_carries = rng.binomial(rush_attempts, parameters["rb_carry_share"])

    touchdown_lambda = np.clip(
        parameters["touchdowns"] * np.exp(0.20 * scoring_z * uncertainty), 0.05, 8.0
    )
    total_touchdowns = rng.poisson(touchdown_lambda)
    total_touchdowns = np.minimum(total_touchdowns, pass_attempts + rush_attempts)
    pass_td_probability = np.clip(
        parameters["pass_td_share"] + np.choose(scenarios, [-0.08, 0.0, 0.08]), 0.05, 0.98
    )
    pass_td_probability = np.where(pass_attempts == 0, 0.0, pass_td_probability)
    pass_td_probability = np.where((rush_attempts == 0) & (pass_attempts > 0), 1.0, pass_td_probability)
    raw_pass_touchdowns = rng.binomial(total_touchdowns, pass_td_probability)
    pass_touchdowns = np.minimum(raw_pass_touchdowns, pass_attempts)
    rush_touchdowns = np.minimum(total_touchdowns - raw_pass_touchdowns, rush_attempts)

    # Preserve the sampled offensive TD total when the other opportunity type
    # has spare capacity; only discard impossible excess when neither passing
    # nor rushing attempts can support it.
    unallocated = total_touchdowns - pass_touchdowns - rush_touchdowns
    pass_add = np.minimum(unallocated, pass_attempts - pass_touchdowns)
    pass_touchdowns = pass_touchdowns + pass_add
    unallocated = unallocated - pass_add
    rush_add = np.minimum(unallocated, rush_attempts - rush_touchdowns)
    rush_touchdowns = rush_touchdowns + rush_add
    return {
        "plays": plays,
        "pass_attempts": pass_attempts,
        "rush_attempts": rush_attempts,
        "sacks": sacks,
        "allocatable_targets": allocatable_targets,
        "rb_carries": rb_carries,
        "rb_targets": rb_targets,
        "pass_touchdowns": pass_touchdowns,
        "rush_touchdowns": rush_touchdowns,
        "scenario": scenarios,
    }


def _distribution(draws: np.ndarray) -> dict[str, Any]:
    p10, p50, p90 = np.quantile(draws, [0.1, 0.5, 0.9], method="linear")
    return {
        "expected_value": round(float(np.mean(draws)), 6),
        "dispersion": round(float(np.std(draws, ddof=0)), 6),
        "p10": round(float(p10), 6),
        "p50": round(float(p50), 6),
        "p90": round(float(p90), 6),
        "distribution_family": "seeded_hierarchical_empirical",
        "parameters": {"draw_count": int(len(draws))},
    }


def forecast_team_week(
    training_rows: Sequence[Mapping[str, Any]],
    evaluation_identity: Mapping[str, Any],
    *,
    cutoff: datetime,
    root_seed: int,
    draws: int = DEFAULT_DRAWS,
    quarterback: ContextFeature | None = None,
    play_caller: ContextFeature | None = None,
    source_snapshot_ids: Sequence[int],
    game_latents: Mapping[str, np.ndarray] | None = None,
    complement_scenario: bool = False,
    minimum_fallback_tier: str | None = None,
    declared_missing_sources: Sequence[str] = (),
) -> tuple[dict[str, Any], dict[str, np.ndarray]]:
    evaluation_season = int(evaluation_identity["season"])
    eligible_training = [
        dict(row)
        for row in training_rows
        if int(row["season"]) < evaluation_season and _as_datetime(row["observed_at"]) <= cutoff
    ]
    if len(eligible_training) != len(training_rows):
        raise ValueError("Training rows include a future-season or post-cutoff observation")
    quarterback_id = _eligible_context(quarterback, cutoff, "quarterback")
    play_caller_id = _eligible_context(play_caller, cutoff, "play_caller")
    team = str(evaluation_identity["team"])
    opponent = str(evaluation_identity["opponent"])
    parameters, evidence, estimated_tier = estimate_parameters(
        eligible_training,
        evaluation_season=evaluation_season,
        team=team,
        opponent=opponent,
        quarterback_id=quarterback_id,
        play_caller_id=play_caller_id,
    )
    if minimum_fallback_tier not in {None, "A", "B", "C"}:
        raise ValueError(f"Unknown minimum fallback tier: {minimum_fallback_tier}")
    tier = estimated_tier
    if minimum_fallback_tier and FALLBACK_ORDER[minimum_fallback_tier] > FALLBACK_ORDER[tier]:
        tier = minimum_fallback_tier
    evidence["estimated_fallback_tier"] = estimated_tier
    evidence["enforced_minimum_fallback_tier"] = minimum_fallback_tier
    evidence["declared_missing_sources"] = sorted(set(str(value) for value in declared_missing_sources))
    identity_seed = _identity_seed(root_seed, str(evaluation_identity["game_id"]), team)
    simulations = simulate_game_scripts(
        parameters,
        tier=tier,
        seed=identity_seed,
        draws=draws,
        game_latents=game_latents,
        complement_scenario=complement_scenario,
    )
    distributions = {
        pool: _distribution(simulations[pool])
        for pool in (
            "plays", "pass_attempts", "rush_attempts", "allocatable_targets",
            "rb_carries", "rb_targets", "pass_touchdowns", "rush_touchdowns",
        )
    }
    realized_scenarios = {
        name: round(float(np.mean(simulations["scenario"] == index)), 6)
        for index, name in enumerate(("leading", "neutral", "trailing"))
    }
    row_snapshots = sorted({int(value) for value in source_snapshot_ids})
    forecast = {
        "game_id": str(evaluation_identity["game_id"]),
        "team": team,
        "fallback_tier": tier,
        "confidence_multiplier": FALLBACK_CONFIDENCE[tier],
        "source_snapshot_ids": row_snapshots,
        "feature_provenance": {
            "model_version": MODEL_VERSION,
            "cutoff": cutoff.isoformat(),
            "evaluation_identity_fields": ["season", "week", "game_id", "game_date", "team", "opponent"],
            "held_out_outcomes_used_as_features": False,
            "fallback_tier_basis": {
                "estimated": estimated_tier,
                "enforced_minimum": minimum_fallback_tier,
                "effective": tier,
                "declared_missing_sources": sorted(
                    set(str(value) for value in declared_missing_sources)
                ),
            },
            "training_seasons": sorted({int(row["season"]) for row in eligible_training}),
            "training_digest": canonical_digest([
                {"id": row["id"], "fact_digest": row["fact_digest"]}
                for row in sorted(eligible_training, key=lambda item: int(item["id"]))
            ]),
            "identity_seed": identity_seed,
            "seed_token": f"team-opportunity:{evaluation_identity['game_id']}:{team}",
            "feature_audit": {
                "prior_team_week_facts": {
                    "value": canonical_digest([
                        {"id": row["id"], "fact_digest": row["fact_digest"]}
                        for row in sorted(eligible_training, key=lambda item: int(item["id"]))
                    ]),
                    "availableAt": cutoff.isoformat(),
                    "sourceDataset": "ff_v2_team_week_facts_prior_seasons",
                    "sourceSeason": max(int(row["season"]) for row in eligible_training),
                    "featureGroup": "football_performance",
                    "eligible": True,
                    "missingReason": None,
                },
                "schedule_context": {
                    "value": {"season": evaluation_season, "week": evaluation_identity["week"], "team": team, "opponent": opponent},
                    "availableAt": cutoff.isoformat(),
                    "sourceDataset": "nflverse_historical_schedule_as_of",
                    "sourceSeason": evaluation_season,
                    "featureGroup": "schedule_context",
                    "eligible": True,
                    "missingReason": None,
                },
                "quarterback": {
                    "value": quarterback_id,
                    "availableAt": cutoff.isoformat(),
                    "sourceDataset": "independent_quarterback_context_as_of",
                    "sourceSeason": evaluation_season,
                    "featureGroup": "football_performance",
                    "eligible": quarterback_id is not None,
                    "missingReason": None if quarterback_id is not None else "no_eligible_as_of_source",
                },
                "play_caller": {
                    "value": play_caller_id,
                    "availableAt": cutoff.isoformat(),
                    "sourceDataset": "attributable_play_caller_history",
                    "sourceSeason": evaluation_season,
                    "featureGroup": "football_performance",
                    "eligible": play_caller_id is not None,
                    "missingReason": None if play_caller_id is not None else "no_eligible_as_of_source",
                },
            },
            "game_script": {
                "scenario_probabilities": SCENARIO_PROBABILITIES,
                "realized_scenario_probabilities": realized_scenarios,
                "hierarchy": "plays -> dropbacks/sacks/rushes -> targets/RB work; total TDs -> pass/rush TDs",
                "uncertainty_multiplier": FALLBACK_UNCERTAINTY[tier],
                "interval_scale_floor": FALLBACK_UNCERTAINTY[tier],
                "opponent_scenario_is_complement": game_latents is not None,
            },
            "context": {
                "quarterback_id": quarterback_id,
                "quarterback_missing": quarterback_id is None,
                "quarterback_missing_reason": "no_eligible_as_of_source" if quarterback_id is None else None,
                "play_caller_id": play_caller_id,
                "play_caller_missing": play_caller_id is None,
                "play_caller_missing_reason": "no_eligible_as_of_source" if play_caller_id is None else None,
            },
            "shrinkage": evidence,
        },
        "distributions": distributions,
    }
    return forecast, simulations


def _snapshot_ids(rows: Iterable[Mapping[str, Any]]) -> list[int]:
    result: set[int] = set()
    for row in rows:
        values = row.get("source_snapshot_ids") or {}
        if isinstance(values, Mapping):
            result.update(int(value) for value in values.values())
        else:
            result.update(int(value) for value in values)
    return sorted(result)


def validate_snapshot_availability(
    source_snapshots: Sequence[Mapping[str, Any]],
    required_snapshot_ids: Sequence[int],
    cutoff: datetime,
) -> None:
    """Reject historical fits whose exact consumed bytes postdate the cutoff."""
    by_id = {int(row["id"]): row for row in source_snapshots}
    missing = sorted(set(int(value) for value in required_snapshot_ids) - set(by_id))
    if missing:
        raise ValueError(f"Missing source snapshot provenance for IDs: {missing}")
    late: list[int] = []
    ineligible: list[int] = []
    for snapshot_id in sorted(set(int(value) for value in required_snapshot_ids)):
        row = by_id[snapshot_id]
        if not bool(row.get("model_eligible", False)):
            ineligible.append(snapshot_id)
            continue
        available_at = row.get("available_at")
        if available_at is None or _as_datetime(available_at) > cutoff:
            late.append(snapshot_id)
    if ineligible:
        raise ValueError(f"Model-ineligible source snapshots: {ineligible}")
    if late:
        raise ValueError(
            f"Exact source snapshots were unavailable by the simulated cutoff: {late}"
        )


def build_historical_artifact(
    facts: Sequence[Mapping[str, Any]],
    *,
    context_run_id: str,
    evaluation_season: int,
    cutoff: datetime,
    source_snapshots: Sequence[Mapping[str, Any]],
    training_facts: Sequence[Mapping[str, Any]] | None = None,
    training_context_run_id: str | None = None,
    minimum_fallback_tier: str | None = None,
    declared_missing_sources: Sequence[str] = (),
    seed: int = DEFAULT_SEED,
    draws: int = DEFAULT_DRAWS,
) -> dict[str, Any]:
    training_source = facts if training_facts is None else training_facts
    training = [dict(row) for row in training_source if int(row["season"]) < evaluation_season]
    evaluation = [dict(row) for row in facts if int(row["season"]) == evaluation_season]
    if not training or not evaluation:
        raise ValueError("Historical artifact requires prior training and held-out evaluation rows")
    eligible_training = [row for row in training if _as_datetime(row["observed_at"]) <= cutoff]
    if len(eligible_training) != len(training):
        raise ValueError("A prior-season training fact was not available by the cutoff")
    snapshots = _snapshot_ids(eligible_training)
    validate_snapshot_availability(source_snapshots, snapshots, cutoff)
    forecasts: list[dict[str, Any]] = []
    by_game: dict[str, list[dict[str, Any]]] = {}
    for identity in evaluation:
        by_game.setdefault(str(identity["game_id"]), []).append(identity)
    for game_id in sorted(by_game):
        sides = sorted(by_game[game_id], key=lambda row: str(row["team"]))
        if len(sides) != 2:
            raise ValueError(f"Evaluation game {game_id} does not have exactly two team facts")
        game_latents = build_game_latents(seed, game_id, draws)
        for side_index, identity in enumerate(sides):
            # Historical QB/play-caller identities in V2-003 were observed postgame,
            # so the representative preseason reconstruction fails closed to tier B.
            forecast, _ = forecast_team_week(
                eligible_training,
                identity,
                cutoff=cutoff,
                root_seed=seed,
                draws=draws,
                quarterback=None,
                play_caller=None,
                source_snapshot_ids=snapshots,
                game_latents=game_latents,
                complement_scenario=side_index == 1,
                minimum_fallback_tier=minimum_fallback_tier,
                declared_missing_sources=declared_missing_sources,
            )
            forecasts.append(forecast)
    return prepare_forecast_artifact(
        context_run_id=context_run_id,
        model_version=MODEL_VERSION,
        calibration_version=CALIBRATION_VERSION,
        as_of_at=cutoff,
        source_snapshot_ids=snapshots,
        facts=evaluation,
        forecasts=forecasts,
        model_config={
            "seed": seed,
            "draws": draws,
            "evaluation_season": evaluation_season,
            "training_seasons": sorted({int(row["season"]) for row in eligible_training}),
            "training_context_run_id": training_context_run_id or context_run_id,
            "training_source_mode": "separate_archived_context" if training_facts is not None else "evaluation_context_prior_seasons",
            "minimum_fallback_tier": minimum_fallback_tier,
            "declared_missing_sources": sorted(set(str(value) for value in declared_missing_sources)),
            "rolling_origin": True,
            "retrospective_reconstruction": True,
            "outcome_features": False,
            "fallback_uncertainty": FALLBACK_UNCERTAINTY,
            "scenario_probabilities": SCENARIO_PROBABILITIES,
        },
    )


def load_model_facts(conn: Any, context_run_id: str) -> list[dict[str, Any]]:
    cursor = conn.cursor()
    cursor.execute(
        """SELECT id, season, week, game_id, game_date, team, opponent, plays,
                  pass_attempts, sacks, allocatable_targets, rush_attempts, rb_carries,
                  rb_targets, pass_touchdowns, rush_touchdowns, quarterback_gsis_id,
                  play_caller_id, source_snapshot_ids, fact_digest, observed_at
             FROM ff_v2_team_week_facts WHERE run_id=%s
            ORDER BY season, week, game_id, team""",
        (context_run_id,),
    )
    return [dict(row) for row in cursor.fetchall()]


def load_source_snapshots(conn: Any, snapshot_ids: Sequence[int]) -> list[dict[str, Any]]:
    if not snapshot_ids:
        return []
    cursor = conn.cursor()
    cursor.execute(
        """SELECT id, available_at, source_published_at, fetched_at, model_eligible,
                  eligibility_reason
             FROM ff_source_snapshots WHERE id = ANY(%s)
            ORDER BY id""",
        (list(sorted(set(int(value) for value in snapshot_ids))),),
    )
    return [dict(row) for row in cursor.fetchall()]


def _cutoff_from_backtest(path: Path, evaluation_season: int) -> datetime:
    artifact = json.loads(path.read_text(encoding="utf-8"))
    fold = next(
        (item for item in artifact["splits"] if int(item["evaluationSeason"]) == evaluation_season),
        None,
    )
    if fold is None:
        raise ValueError(f"Backtest artifact has no {evaluation_season} fold")
    return _as_datetime(fold["preseasonCutoff"])


def _archived_training_bundle(path: Path, evaluation_season: int) -> dict[str, Any]:
    artifact = json.loads(path.read_text(encoding="utf-8"))
    bundle_key = "2021-cutoff" if evaluation_season == 2021 else "2022-cutoff"
    bundle = next(
        (item for item in artifact["bundles"] if item["bundleKey"] == bundle_key),
        None,
    )
    if bundle is None:
        raise ValueError(f"Archived context artifact has no {bundle_key} bundle")
    return bundle


def run(args: argparse.Namespace) -> dict[str, Any]:
    config = AppConfig.from_env()
    if not config.database_url:
        raise RuntimeError("DATABASE_URL is required")
    database = DatabaseManager(config.database_url)
    cutoff = _cutoff_from_backtest(Path(args.backtest_artifact), args.evaluation_season)
    training_bundle = _archived_training_bundle(
        Path(args.archived_context_artifact), args.evaluation_season
    )
    training_context_run_id = str(training_bundle["runId"])
    with database.connect() as conn:
        facts = load_model_facts(conn, args.context_run_id)
        training_rows = load_model_facts(conn, training_context_run_id)
        source_snapshots = load_source_snapshots(conn, _snapshot_ids(training_rows))
    artifact = build_historical_artifact(
        facts,
        context_run_id=args.context_run_id,
        evaluation_season=args.evaluation_season,
        cutoff=cutoff,
        source_snapshots=source_snapshots,
        training_facts=training_rows,
        training_context_run_id=training_context_run_id,
        minimum_fallback_tier=str(training_bundle["coverage"]["fallback_tier"]),
        declared_missing_sources=training_bundle["coverage"]["declared_missing_sources"],
        seed=args.seed,
        draws=args.draws,
    )
    path = Path(args.artifact)
    if args.verify:
        stored = json.loads(path.read_text(encoding="utf-8"))
        if stored != artifact:
            raise RuntimeError("Team opportunity artifact replay mismatch")
        with database.connect() as conn:
            counts = conn.cursor()
            counts.execute(
                """SELECT r.artifact_digest, r.forecast_count,
                          COUNT(DISTINCT f.id) AS stored_forecasts,
                          COUNT(d.id) AS stored_distributions
                     FROM ff_v2_team_opportunity_forecast_runs r
                     LEFT JOIN ff_v2_team_opportunity_forecasts f ON f.forecast_run_id=r.run_id
                     LEFT JOIN ff_v2_team_opportunity_distributions d ON d.forecast_id=f.id
                    WHERE r.run_id=%s GROUP BY r.run_id""",
                (artifact["run_id"],),
            )
            row = counts.fetchone()
        expected_distributions = sum(len(item["distributions"]) for item in artifact["forecasts"])
        if not row or str(row["artifact_digest"]) != artifact["artifact_digest"]:
            raise RuntimeError("Persisted forecast run is missing or has a different digest")
        if int(row["stored_forecasts"]) != artifact["forecast_count"] or int(row["stored_distributions"]) != expected_distributions:
            raise RuntimeError("Persisted forecast row counts do not match replay")
        return {"status": "verified", "runId": artifact["run_id"], "artifactDigest": artifact["artifact_digest"]}
    persistence = persist_with_database(database, artifact)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"status": "persisted", "runId": artifact["run_id"], "artifactDigest": artifact["artifact_digest"], **persistence}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context-run-id", default="9077ad91-e258-5e47-beb8-f41b68c6651b")
    parser.add_argument("--evaluation-season", type=int, default=2025)
    parser.add_argument("--backtest-artifact", default="artifacts/ff_v2_backtest_harness_2020_2025.json")
    parser.add_argument(
        "--archived-context-artifact",
        default="artifacts/ff_v2_archived_team_context.json",
    )
    parser.add_argument("--artifact", default="artifacts/ff_v2_team_opportunity_2025.json")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--draws", type=int, default=DEFAULT_DRAWS)
    parser.add_argument("--verify", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    print(json.dumps(run(parse_args()), indent=2, sort_keys=True))
