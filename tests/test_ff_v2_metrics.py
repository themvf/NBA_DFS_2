from __future__ import annotations

import pytest

from model.ff_v2_backtest import score_definitive_metrics
from model.ff_v2_metrics import (
    FROZEN_METRIC_POLICY,
    REQUIRED_MODEL_LABELS,
    weighted_interval_score,
)


def _result(report, model, metric, cohort="overall", stat=None):
    return next(
        row
        for row in report["results"]
        if row["modelLabel"] == model
        and row["metric"] == metric
        and row["cohort"] == cohort
        and row["statName"] == stat
    )


def test_weighted_interval_score_is_proper_and_rejects_crossed_quantiles() -> None:
    centered = {"actualValue": 10, "p10": 8, "p50": 10, "p90": 12}
    missed = {"actualValue": 20, "p10": 8, "p50": 10, "p90": 12}
    assert weighted_interval_score(centered) < weighted_interval_score(missed)
    with pytest.raises(ValueError, match="p10 <= p50 <= p90"):
        weighted_interval_score({"actualValue": 10, "p10": 11, "p50": 10, "p90": 12})


def test_team_metrics_cover_errors_distribution_coverage_and_deterministic_paired_ci() -> None:
    rows = []
    for model, offset in (("champion", 2.0), ("challenger", 1.0)):
        for week in (1, 2, 18):
            actual = 60.0 + week
            rows.append(
                {
                    "artifactKind": "team_week",
                    "modelLabel": model,
                    "entityId": f"A:{week}",
                    "gameId": f"game:{week}",
                    "season": 2025,
                    "week": week,
                    "statName": "plays",
                    "actualValue": actual,
                    "mean": actual + offset,
                    "p10": actual - 3,
                    "p50": actual + offset,
                    "p90": actual + 3,
                }
            )
    first = score_definitive_metrics(rows, model_labels=("champion", "challenger"), bootstrap_draws=200)
    second = score_definitive_metrics(rows, model_labels=("champion", "challenger"), bootstrap_draws=200)

    assert _result(first, "challenger", "mae", stat="plays")["value"] == 1.0
    assert _result(first, "challenger", "mae", stat="plays")["n"] == 2
    assert _result(first, "challenger", "coverage_p10", stat="plays")["value"] == 0.0
    assert _result(first, "challenger", "coverage_p90", stat="plays")["value"] == 1.0
    comparison = next(
        row
        for row in first["pairedComparisons"]
        if row["metric"] == "mae"
        and row["statName"] == "plays"
        and row["cohort"] == "overall"
    )
    assert comparison["resamplingUnit"] == "game"
    assert comparison["deltaChallengerMinusComparator"] == -1.0
    assert comparison["ci95Lower"] == comparison["ci95Upper"] == -1.0
    assert first["pairedComparisons"] == second["pairedComparisons"]


def test_season_metrics_use_ppr_weeks_1_17_and_report_required_cohorts() -> None:
    rows = []
    for model, reverse in (("champion", True), ("challenger", False)):
        for index in range(13):
            actual = float(100 - index)
            predicted = float(index if reverse else actual)
            rows.append(
                {
                    "artifactKind": "season_total",
                    "modelLabel": model,
                    "entityId": f"wr-{index}",
                    "season": 2025,
                    "position": "WR",
                    "scoringFormat": "PPR",
                    "weekStart": 1,
                    "weekEnd": 17,
                    "actualValue": actual,
                    "mean": predicted,
                    "isRookie": index == 0,
                    "changedTeam": index == 1,
                    "injuryAffected": index == 2,
                    "priorGames": 3 if index == 3 else 20,
                }
            )
        rows.append(
            {
                "artifactKind": "season_total",
                "modelLabel": model,
                "entityId": "week18-contaminated",
                "season": 2025,
                "position": "WR",
                "scoringFormat": "PPR",
                "weekStart": 1,
                "weekEnd": 18,
                "actualValue": 999,
                "mean": 0,
            }
        )
    report = score_definitive_metrics(rows, model_labels=("champion", "challenger"), bootstrap_draws=100)
    assert _result(report, "challenger", "mae")["n"] == 13
    assert _result(report, "challenger", "top12_precision")["value"] == 1.0
    assert _result(report, "challenger", "top12_recall", "position:WR")["value"] == 1.0
    assert _result(report, "challenger", "mae", "rookie")["n"] == 1
    assert _result(report, "challenger", "mae", "changed_team")["n"] == 1
    assert _result(report, "challenger", "mae", "injury")["n"] == 1
    assert _result(report, "challenger", "mae", "small_sample")["n"] == 1


def test_position_aware_spike_recall_and_best_ball_metrics_filter_week_18() -> None:
    rows = []
    for model in ("champion", "challenger"):
        rows.extend(
            [
                {
                    "artifactKind": "player_week",
                    "modelLabel": model,
                    "entityId": "qb:1",
                    "season": 2025,
                    "week": 1,
                    "position": "QB",
                    "actualValue": 24.0,
                    "spikeProbability": 0.9,
                },
                {
                    "artifactKind": "player_week",
                    "modelLabel": model,
                    "entityId": "te:1",
                    "season": 2025,
                    "week": 1,
                    "position": "TE",
                    "actualValue": 16.0,
                    "spikeProbability": 0.4 if model == "challenger" else 0.1,
                },
            ]
        )
        for week, points in ((1, 10.0), (2, 15.0), (18, 1000.0)):
            rows.append(
                {
                    "artifactKind": "roster_simulation",
                    "modelLabel": model,
                    "entityId": f"draft-1:{week}",
                    "draftId": "draft-1",
                    "season": 2025,
                    "week": week,
                    "actualValue": points,
                    "oracleActualValue": points + 2,
                }
            )
    report = score_definitive_metrics(rows, model_labels=("champion", "challenger"), bootstrap_draws=100)
    assert _result(report, "challenger", "spike_recall")["value"] == 1.0
    assert _result(report, "challenger", "spike_recall")["n"] == 1
    assert _result(report, "challenger", "spike_recall", "position:QB")["eligible"] is False
    assert _result(report, "challenger", "best_ball_counted_points")["value"] == 25.0
    assert _result(report, "challenger", "draft_decision_regret")["value"] == 4.0


def test_spike_recall_reports_actual_positive_count_and_zero_event_reason() -> None:
    rows = [
        {
            "artifactKind": "player_week",
            "modelLabel": "challenger",
            "entityId": f"wr:{week}",
            "season": 2025,
            "week": week,
            "position": "WR",
            "actualValue": actual,
            "spikeProbability": 0.4,
        }
        for week, actual in ((1, 10.0), (2, 20.0), (3, 12.0))
    ]
    report = score_definitive_metrics(
        rows,
        model_labels=("challenger",),
        bootstrap_draws=100,
    )
    overall = _result(report, "challenger", "spike_recall")
    assert overall["candidateN"] == 3
    assert overall["n"] == 1
    assert overall["resamplingUnitCount"] == 1

    no_events = score_definitive_metrics(
        [dict(row, actualValue=10.0) for row in rows],
        model_labels=("challenger",),
        bootstrap_draws=100,
    )
    result = _result(no_events, "challenger", "spike_recall")
    assert result["eligible"] is False
    assert result["n"] == 0
    assert result["exclusionReason"] == "zero_actual_spike_events"


def test_missing_market_distributions_and_roster_history_are_explicitly_ineligible() -> None:
    report = score_definitive_metrics([], bootstrap_draws=100)
    assert report["modelLabels"] == list(REQUIRED_MODEL_LABELS)
    market_wis = _result(report, "market_baseline", "weighted_interval_score", stat="plays")
    roster = _result(report, "challenger", "best_ball_counted_points")
    assert market_wis == {**market_wis, "eligible": False, "exclusionReason": "zero_sample_cohort"}
    assert market_wis["n"] == 0
    assert roster["eligible"] is False
    assert roster["n"] == 0


def test_market_point_rows_do_not_fabricate_distribution_quantiles() -> None:
    row = {
        "artifactKind": "team_week",
        "modelLabel": "market_baseline",
        "entityId": "A:1",
        "gameId": "g1",
        "season": 2025,
        "week": 1,
        "statName": "plays",
        "actualValue": 60,
        "mean": 62,
        "marketEligible": True,
    }
    report = score_definitive_metrics([row], model_labels=("market_baseline",), bootstrap_draws=100)
    assert _result(report, "market_baseline", "mae", stat="plays")["eligible"] is True
    wis = _result(report, "market_baseline", "weighted_interval_score", stat="plays")
    assert wis["eligible"] is False
    assert wis["candidateN"] == 1
    assert wis["exclusionReason"] == "missing_required_prediction_fields"


def test_market_baseline_fails_closed_without_preseason_eligibility() -> None:
    row = {
        "artifactKind": "season_total",
        "modelLabel": "market_baseline",
        "entityId": "wr-1",
        "season": 2025,
        "position": "WR",
        "scoringFormat": "PPR",
        "weekStart": 1,
        "weekEnd": 17,
        "actualValue": 200,
        "mean": 190,
        "marketEligible": False,
    }
    report = score_definitive_metrics([row], model_labels=("market_baseline",), bootstrap_draws=100)
    result = _result(report, "market_baseline", "mae")
    assert result["eligible"] is False
    assert result["exclusionReason"] == "market_snapshot_ineligible"


def test_frozen_policy_declares_thresholds_tolerances_and_resampling_units() -> None:
    assert FROZEN_METRIC_POLICY["frozenBeforeFit"] is True
    assert FROZEN_METRIC_POLICY["productWeeks"] == [1, 17]
    assert FROZEN_METRIC_POLICY["spikeThresholds"] == {"QB": 25.0, "RB": 20.0, "WR": 20.0, "TE": 15.0}
    assert FROZEN_METRIC_POLICY["calibrationTolerances"]["seasonTotalBiasPoints"] == 5.0
    assert FROZEN_METRIC_POLICY["resamplingUnits"]["roster_simulation"] == "draft_instance"
