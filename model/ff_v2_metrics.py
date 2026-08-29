"""Preregistered metric suite for Fantasy Football roster-aware V2.

The module is deliberately model-agnostic.  A rolling-origin fold supplies
frozen predictions and held-out outcomes as row dictionaries; this scorer
applies one immutable policy to every champion, baseline, challenger, and
ablation.  Missing historical cohorts remain explicit zero-sample results.
"""

from __future__ import annotations

import hashlib
import math
import random
from collections import defaultdict
from typing import Any, Callable, Mapping, Sequence


METRIC_SUITE_VERSION = "ff-v2-metrics-v1"
DEFAULT_BOOTSTRAP_DRAWS = 2000
DEFAULT_BOOTSTRAP_SEED = 20260829

TEAM_OPPORTUNITY_STATS = (
    "plays",
    "pass_attempts",
    "allocatable_targets",
    "rush_attempts",
    "rb_carries",
    "rb_targets",
    "pass_touchdowns",
    "rush_touchdowns",
)
POSITIONS = ("QB", "RB", "WR", "TE")
SPIKE_THRESHOLDS = {"QB": 25.0, "RB": 20.0, "WR": 20.0, "TE": 15.0}
SPIKE_PROBABILITY_CUTOFF = 0.30
SMALL_SAMPLE_MAX_PRIOR_GAMES = 5
PRODUCT_WEEK_MIN = 1
PRODUCT_WEEK_MAX = 17
INTERVAL_ALPHA = 0.20

REQUIRED_MODEL_LABELS = (
    "champion",
    "simple_baseline",
    "simple_baseline:rolling_average",
    "simple_baseline:league_average",
    "market_baseline",
    "challenger",
    "ablation:schedule",
    "ablation:correlation",
    "ablation:availability",
    "ablation:roster_fit",
)
COHORTS = (
    "overall",
    "position:QB",
    "position:RB",
    "position:WR",
    "position:TE",
    "rookie",
    "changed_team",
    "injury",
    "small_sample",
)

FROZEN_METRIC_POLICY = {
    "version": METRIC_SUITE_VERSION,
    "frozenBeforeFit": True,
    "distributionScore": "weighted_interval_score_p10_p50_p90_alpha_0.20",
    "coverageDefinition": "empirical_fraction_actual_lte_quantile",
    "topN": 12,
    "top12Universe": "draftable_QB_RB_WR_TE_by_evaluation_season_and_position",
    "top12Scoring": "PPR_points_Weeks_1_17; top_min(12,n); ties_by_entity_id",
    "spikeThresholds": SPIKE_THRESHOLDS,
    "spikeProbabilityCutoff": SPIKE_PROBABILITY_CUTOFF,
    "smallSampleDefinition": f"priorGames <= {SMALL_SAMPLE_MAX_PRIOR_GAMES}",
    "productWeeks": [PRODUCT_WEEK_MIN, PRODUCT_WEEK_MAX],
    "missingCohortBehavior": "emit_ineligible_zero_sample_result",
    "marketEligibility": (
        "immutable_preseason_market_snapshot_available_by_fold_cutoff; "
        "distribution_metrics_require_actual_market_p10_p50_p90"
    ),
    "calibrationTolerances": {
        "coverageP10AbsoluteError": 0.05,
        "coverageP50AbsoluteError": 0.05,
        "coverageP90AbsoluteError": 0.05,
        "seasonTotalBiasPoints": 5.0,
    },
    "materialDegradationBounds": {
        "teamOpportunityMaeOrWisRelative": 0.02,
        "seasonTotalMaeRelative": 0.01,
        "top12PrecisionOrRecallAbsolute": 0.02,
        "spikeRecallAbsolute": 0.02,
        "bestBallCountedPointsRelative": -0.01,
        "draftDecisionRegretRelative": 0.01,
    },
    "comparison": "challenger_minus_declared_comparator_on_identity_intersection",
    "confidenceInterval": "deterministic_paired_percentile_bootstrap_95pct",
    "resamplingUnits": {
        "team_week": "game",
        "player_week": "player_week",
        "season_total": "player_season",
        "top12": "evaluation_season",
        "roster_simulation": "draft_instance",
    },
}


def _number(row: Mapping[str, Any], key: str) -> float:
    value = float(row[key])
    if not math.isfinite(value):
        raise ValueError(f"{key} must be finite")
    return value


def _quantile(values: Sequence[float], probability: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("Cannot take a quantile of an empty sequence")
    index = (len(ordered) - 1) * probability
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def weighted_interval_score(row: Mapping[str, Any]) -> float:
    """Return a proper score from the frozen P10/P50/P90 forecast."""

    actual = _number(row, "actualValue")
    lower = _number(row, "p10")
    median = _number(row, "p50")
    upper = _number(row, "p90")
    if lower > median or median > upper:
        raise ValueError("Distribution quantiles must satisfy p10 <= p50 <= p90")
    interval_score = upper - lower
    if actual < lower:
        interval_score += (2.0 / INTERVAL_ALPHA) * (lower - actual)
    elif actual > upper:
        interval_score += (2.0 / INTERVAL_ALPHA) * (actual - upper)
    weight_median = 0.5
    weight_interval = INTERVAL_ALPHA / 2.0
    return (
        weight_median * abs(actual - median) + weight_interval * interval_score
    ) / (weight_median + weight_interval)


def _is_product_week(row: Mapping[str, Any]) -> bool:
    try:
        week = int(row["week"])
    except (KeyError, TypeError, ValueError):
        return False
    return PRODUCT_WEEK_MIN <= week <= PRODUCT_WEEK_MAX


def _is_week_1_17_season_total(row: Mapping[str, Any]) -> bool:
    return (
        str(row.get("scoringFormat", "")).lower() == "ppr"
        and int(row.get("weekStart", -1)) == PRODUCT_WEEK_MIN
        and int(row.get("weekEnd", -1)) == PRODUCT_WEEK_MAX
    )


def _in_cohort(row: Mapping[str, Any], cohort: str) -> bool:
    if cohort == "overall":
        return True
    if cohort.startswith("position:"):
        return str(row.get("position", "")).upper() == cohort.split(":", 1)[1]
    if cohort == "rookie":
        return row.get("isRookie") is True
    if cohort == "changed_team":
        return row.get("changedTeam") is True
    if cohort == "injury":
        return row.get("injuryAffected") is True
    if cohort == "small_sample":
        return row.get("smallSample") is True or (
            row.get("priorGames") is not None
            and int(row["priorGames"]) <= SMALL_SAMPLE_MAX_PRIOR_GAMES
        )
    raise ValueError(f"Unknown cohort: {cohort}")


def _row_identity(row: Mapping[str, Any]) -> str:
    return "|".join(
        str(row.get(key, ""))
        for key in ("artifactKind", "season", "week", "entityId", "statName")
    )


def _unit(row: Mapping[str, Any], metric: str) -> str:
    kind = str(row["artifactKind"])
    if metric.startswith("top12_"):
        return str(row["season"])
    if kind == "team_week":
        return str(row.get("gameId") or row["entityId"])
    if kind == "player_week":
        return _row_identity(row)
    if kind == "season_total":
        return f"{row['season']}|{row['entityId']}"
    if kind == "roster_simulation":
        return str(row.get("draftId") or row["entityId"])
    raise ValueError(f"Unsupported artifact kind: {kind}")


def _mean(values: Sequence[float]) -> float:
    return sum(values) / len(values)


def _top12_by_season(rows: Sequence[Mapping[str, Any]], *, recall: bool) -> dict[str, float]:
    grouped: dict[tuple[int, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(int(row["season"]), str(row["position"]).upper())].append(row)
    by_season: dict[str, list[float]] = defaultdict(list)
    for (season, _position), group in grouped.items():
        count = min(12, len(group))
        predicted = {
            str(row["entityId"])
            for row in sorted(group, key=lambda item: (-_number(item, "mean"), str(item["entityId"])))[:count]
        }
        actual = {
            str(row["entityId"])
            for row in sorted(group, key=lambda item: (-_number(item, "actualValue"), str(item["entityId"])))[:count]
        }
        denominator = len(actual if recall else predicted)
        by_season[str(season)].append(len(predicted & actual) / denominator if denominator else 0.0)
    return {season: _mean(values) for season, values in by_season.items()}


def _unit_values(metric: str, rows: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    if metric in ("top12_precision", "top12_recall"):
        return _top12_by_season(rows, recall=metric.endswith("recall"))

    contributions: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        actual = _number(row, "actualValue")
        if metric == "mae":
            value = abs(_number(row, "mean") - actual)
        elif metric == "bias":
            value = _number(row, "mean") - actual
        elif metric == "weighted_interval_score":
            value = weighted_interval_score(row)
        elif metric.startswith("coverage_p"):
            value = float(actual <= _number(row, metric.removeprefix("coverage_")))
        elif metric == "spike_recall":
            position = str(row.get("position", "")).upper()
            if position not in SPIKE_THRESHOLDS or actual < SPIKE_THRESHOLDS[position]:
                continue
            value = float(_number(row, "spikeProbability") >= SPIKE_PROBABILITY_CUTOFF)
        elif metric == "best_ball_counted_points":
            value = actual
        elif metric == "draft_decision_regret":
            value = _number(row, "oracleActualValue") - actual
            if value < 0:
                raise ValueError("oracleActualValue cannot be below the chosen actualValue")
        else:
            raise ValueError(f"Unknown metric: {metric}")
        contributions[_unit(row, metric)].append(value)

    # Best Ball product rows are weekly: sum Weeks 1-17 within each draft.
    if metric in ("best_ball_counted_points", "draft_decision_regret"):
        return {unit: sum(values) for unit, values in contributions.items()}
    return {unit: _mean(values) for unit, values in contributions.items()}


def _metric_definitions() -> list[tuple[str, str, str | None]]:
    definitions: list[tuple[str, str, str | None]] = []
    for stat in TEAM_OPPORTUNITY_STATS:
        for metric in (
            "mae",
            "bias",
            "weighted_interval_score",
            "coverage_p10",
            "coverage_p50",
            "coverage_p90",
        ):
            definitions.append(("team_week", metric, stat))
    definitions.extend(
        ("season_total", metric, None)
        for metric in ("mae", "bias", "top12_precision", "top12_recall")
    )
    definitions.append(("player_week", "spike_recall", None))
    definitions.extend(
        ("roster_simulation", metric, None)
        for metric in ("best_ball_counted_points", "draft_decision_regret")
    )
    return definitions


def _eligible_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    model_label: str,
    artifact_kind: str,
    stat_name: str | None,
    cohort: str,
) -> list[Mapping[str, Any]]:
    selected = [
        row
        for row in rows
        if row.get("modelLabel") == model_label
        and row.get("artifactKind") == artifact_kind
        and (stat_name is None or row.get("statName") == stat_name)
        and _in_cohort(row, cohort)
    ]
    if artifact_kind in ("team_week", "player_week", "roster_simulation"):
        selected = [row for row in selected if _is_product_week(row)]
    elif artifact_kind == "season_total":
        selected = [row for row in selected if _is_week_1_17_season_total(row)]
    return selected


def _ready_rows(metric: str, rows: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    required = {
        "mae": ("actualValue", "mean"),
        "bias": ("actualValue", "mean"),
        "weighted_interval_score": ("actualValue", "p10", "p50", "p90"),
        "coverage_p10": ("actualValue", "p10"),
        "coverage_p50": ("actualValue", "p50"),
        "coverage_p90": ("actualValue", "p90"),
        "top12_precision": ("actualValue", "mean", "position", "season"),
        "top12_recall": ("actualValue", "mean", "position", "season"),
        "spike_recall": ("actualValue", "spikeProbability", "position"),
        "best_ball_counted_points": ("actualValue",),
        "draft_decision_regret": ("actualValue", "oracleActualValue"),
    }[metric]
    return [row for row in rows if all(row.get(key) is not None for key in required)]


def _result(
    *,
    model_label: str,
    artifact_kind: str,
    metric: str,
    stat_name: str | None,
    cohort: str,
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    market_ineligible = model_label == "market_baseline" and any(
        row.get("marketEligible") is not True for row in rows
    )
    eligible_market_rows = (
        [row for row in rows if row.get("marketEligible") is True]
        if model_label == "market_baseline"
        else list(rows)
    )
    ready = _ready_rows(metric, eligible_market_rows)
    values = _unit_values(metric, ready)
    eligible = bool(values)
    contributing_n = len(ready)
    if metric == "spike_recall":
        contributing_n = sum(
            1
            for row in ready
            if str(row.get("position", "")).upper() in SPIKE_THRESHOLDS
            and _number(row, "actualValue")
            >= SPIKE_THRESHOLDS[str(row["position"]).upper()]
        )
    reason = None
    if not eligible:
        if market_ineligible and not eligible_market_rows:
            reason = "market_snapshot_ineligible"
        elif metric == "spike_recall" and ready:
            reason = "zero_actual_spike_events"
        else:
            reason = "missing_required_prediction_fields" if rows else "zero_sample_cohort"
    return {
        "modelLabel": model_label,
        "artifactKind": artifact_kind,
        "metric": metric,
        "statName": stat_name,
        "cohort": cohort,
        "eligible": eligible,
        "exclusionReason": reason,
        "candidateN": len(rows),
        "n": contributing_n,
        "resamplingUnit": FROZEN_METRIC_POLICY["resamplingUnits"][
            "top12" if metric.startswith("top12_") else artifact_kind
        ],
        "resamplingUnitCount": len(values),
        "value": round(_mean(list(values.values())), 10) if eligible else None,
    }


def _paired_interval(
    challenger_values: Mapping[str, float],
    comparator_values: Mapping[str, float],
    *,
    seed: int,
    draws: int,
) -> tuple[float | None, float | None, int, str | None]:
    units = sorted(challenger_values.keys() & comparator_values.keys())
    if not units:
        return None, None, 0, "no_paired_resampling_units"
    deltas = [challenger_values[unit] - comparator_values[unit] for unit in units]
    if len(units) < 2:
        return None, None, len(units), "fewer_than_two_paired_resampling_units"
    rng = random.Random(seed)
    estimates = [
        _mean([deltas[rng.randrange(len(deltas))] for _ in units])
        for _ in range(draws)
    ]
    return _quantile(estimates, 0.025), _quantile(estimates, 0.975), len(units), None


def evaluate_metric_suite(
    rows: Sequence[Mapping[str, Any]],
    *,
    model_labels: Sequence[str] = REQUIRED_MODEL_LABELS,
    bootstrap_draws: int = DEFAULT_BOOTSTRAP_DRAWS,
    seed: int = DEFAULT_BOOTSTRAP_SEED,
) -> dict[str, Any]:
    """Score all preregistered metrics and challenger comparisons.

    Rows are intentionally generic dictionaries so the same function accepts
    every rolling-origin artifact.  Required labels and cohorts are emitted
    even when absent; an absent market or historical roster cohort therefore
    cannot masquerade as a successful score.
    """

    if bootstrap_draws < 100:
        raise ValueError("bootstrap_draws must be at least 100")
    unknown = set(model_labels) - set(REQUIRED_MODEL_LABELS)
    if unknown:
        raise ValueError(f"Unregistered model labels: {sorted(unknown)}")
    identities: set[tuple[str, str]] = set()
    for row in rows:
        label = str(row.get("modelLabel"))
        if label not in model_labels:
            raise ValueError(f"Row uses undeclared model label: {label}")
        identity = (label, _row_identity(row))
        if identity in identities:
            raise ValueError(f"Duplicate model/artifact identity: {identity}")
        identities.add(identity)

    results: list[dict[str, Any]] = []
    comparisons: list[dict[str, Any]] = []
    for artifact_kind, metric, stat_name in _metric_definitions():
        for cohort in COHORTS:
            by_label: dict[str, list[Mapping[str, Any]]] = {}
            for label in model_labels:
                selected = _eligible_rows(
                    rows,
                    model_label=label,
                    artifact_kind=artifact_kind,
                    stat_name=stat_name,
                    cohort=cohort,
                )
                eligible_market_rows = (
                    [row for row in selected if row.get("marketEligible") is True]
                    if label == "market_baseline"
                    else selected
                )
                by_label[label] = _ready_rows(metric, eligible_market_rows)
                results.append(
                    _result(
                        model_label=label,
                        artifact_kind=artifact_kind,
                        metric=metric,
                        stat_name=stat_name,
                        cohort=cohort,
                        rows=selected,
                    )
                )

            challenger_map = {
                _row_identity(row): row for row in by_label.get("challenger", [])
            }
            for comparator in model_labels:
                if comparator == "challenger":
                    continue
                comparator_map = {_row_identity(row): row for row in by_label[comparator]}
                paired_ids = sorted(challenger_map.keys() & comparator_map.keys())
                challenger_paired = [challenger_map[key] for key in paired_ids]
                comparator_paired = [comparator_map[key] for key in paired_ids]
                challenger_values = _unit_values(metric, challenger_paired)
                comparator_values = _unit_values(metric, comparator_paired)
                token = f"{artifact_kind}|{metric}|{stat_name}|{cohort}|{comparator}"
                local_seed = seed ^ int(hashlib.sha256(token.encode()).hexdigest()[:8], 16)
                lower, upper, unit_count, reason = _paired_interval(
                    challenger_values,
                    comparator_values,
                    seed=local_seed,
                    draws=bootstrap_draws,
                )
                shared_units = sorted(challenger_values.keys() & comparator_values.keys())
                delta = (
                    _mean([challenger_values[key] - comparator_values[key] for key in shared_units])
                    if shared_units
                    else None
                )
                comparisons.append(
                    {
                        "challengerLabel": "challenger",
                        "comparatorLabel": comparator,
                        "artifactKind": artifact_kind,
                        "metric": metric,
                        "statName": stat_name,
                        "cohort": cohort,
                        "eligible": reason is None,
                        "exclusionReason": reason,
                        "pairedN": len(paired_ids),
                        "resamplingUnit": FROZEN_METRIC_POLICY["resamplingUnits"][
                            "top12" if metric.startswith("top12_") else artifact_kind
                        ],
                        "resamplingUnitCount": unit_count,
                        "deltaChallengerMinusComparator": round(delta, 10) if delta is not None else None,
                        "ci95Lower": round(lower, 10) if lower is not None else None,
                        "ci95Upper": round(upper, 10) if upper is not None else None,
                        "bootstrapDraws": bootstrap_draws,
                        "seed": local_seed,
                    }
                )

    return {
        "metricSuiteVersion": METRIC_SUITE_VERSION,
        "policy": FROZEN_METRIC_POLICY,
        "modelLabels": list(model_labels),
        "results": results,
        "pairedComparisons": comparisons,
    }
