"""Chronological market-relative validation for Tennis surface Elo (SCRUM-27)."""

from __future__ import annotations

import json
import math
import random
from collections import defaultdict
from typing import Iterable

from psycopg2.extras import execute_values

from config import load_config
from db.database import DatabaseManager
from ingest.tennis_foundation import stable_json
from model.tennis_surface_elo import ALGORITHM_VERSION

EVALUATION_VERSION = "tennis-surface-elo-eval-v1"
BOOTSTRAP_REPS = 2000
BOOTSTRAP_SEED = 20260713
MIN_SAMPLE = 200
MAX_ECE_DEGRADATION = 0.01
MODELS = ("market", "overall_elo", "surface_elo", "blended_surface_elo")


def _clip(p: float) -> float:
    return min(max(float(p), 1e-9), 1.0 - 1e-9)


def _loss(p: float, y: int) -> float:
    p = _clip(p)
    return -(y * math.log(p) + (1 - y) * math.log(1 - p))


def _brier(p: float, y: int) -> float:
    return (float(p) - y) ** 2


def _ece(rows: list[dict], model: str, bins: int = 10) -> float:
    total = len(rows)
    if not total:
        return float("nan")
    grouped: dict[int, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[min(int(float(row[model]) * bins), bins - 1)].append(row)
    return sum(
        len(bucket) / total
        * abs(sum(float(row[model]) for row in bucket) / len(bucket)
              - sum(int(row["outcome"]) for row in bucket) / len(bucket))
        for bucket in grouped.values()
    )


def metrics(rows: list[dict], model: str) -> dict:
    if not rows:
        return {"n": 0, "brier": None, "log_loss": None, "ece": None}
    return {
        "n": len(rows),
        "brier": sum(_brier(row[model], row["outcome"]) for row in rows) / len(rows),
        "log_loss": sum(_loss(row[model], row["outcome"]) for row in rows) / len(rows),
        "ece": _ece(rows, model),
    }


def _quantile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    lo = int(math.floor(position))
    hi = int(math.ceil(position))
    if lo == hi:
        return ordered[lo]
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (position - lo)


def cluster_bootstrap_delta(rows: list[dict], seed: int) -> tuple[float, float]:
    clusters: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        clusters[row["tournament"]].append(row)
    names = sorted(clusters)
    rng = random.Random(seed)
    deltas: list[float] = []
    for _ in range(BOOTSTRAP_REPS):
        sample = [row for _name in names for row in clusters[rng.choice(names)]]
        delta = sum(
            _loss(row["blended_surface_elo"], row["outcome"])
            - _loss(row["overall_elo"], row["outcome"])
            for row in sample
        ) / len(sample)
        deltas.append(delta)
    return _quantile(deltas, 0.025), _quantile(deltas, 0.975)


def _load_rows(db: DatabaseManager, run_id: int) -> list[dict]:
    raw = db.execute(
        """
        SELECT e.tour,e.match_date,hm.tournament,e.surface_bucket AS surface,
               e.is_winner AS outcome,e.expected_overall AS overall_elo,
               e.expected_surface AS surface_elo,e.expected_blended AS blended_surface_elo,
               hm.winner_decimal_odds,hm.loser_decimal_odds
        FROM tennis_elo_rating_events e
        JOIN tennis_historical_matches hm ON hm.id=e.historical_match_id
        WHERE e.run_id=%s
          AND e.player_id=LEAST(hm.winner_player_id,hm.loser_player_id)
          AND hm.winner_decimal_odds > 1 AND hm.loser_decimal_odds > 1
        ORDER BY e.tour,e.match_date,hm.id
        """,
        (run_id,),
    )
    rows = []
    for row in raw:
        winner_imp = 1.0 / float(row["winner_decimal_odds"])
        loser_imp = 1.0 / float(row["loser_decimal_odds"])
        winner_fair = winner_imp / (winner_imp + loser_imp)
        year = row["match_date"].year
        period = {2023: "burn_in", 2024: "development", 2025: "validation", 2026: "final_test"}.get(year)
        if not period:
            continue
        rows.append({
            **dict(row),
            "outcome": int(row["outcome"]),
            "market": winner_fair if row["outcome"] else 1.0 - winner_fair,
            "period": period,
        })
    return rows


def evaluate(db: DatabaseManager) -> dict:
    elo_run = db.execute_one(
        """
        SELECT * FROM tennis_elo_runs
        WHERE algorithm_version=%s AND status='complete'
        ORDER BY completed_at DESC,id DESC LIMIT 1
        """,
        (ALGORITHM_VERSION,),
    )
    if not elo_run:
        raise RuntimeError("No complete current Tennis Elo run")
    config = {
        "periods": {"2023": "burn_in", "2024": "development", "2025": "validation", "2026": "final_test"},
        "bootstrap_reps": BOOTSTRAP_REPS,
        "bootstrap_seed": BOOTSTRAP_SEED,
        "bootstrap_cluster": "tournament",
        "min_sample": MIN_SAMPLE,
        "promotion_rule": "validation_ci_high<0 and validation_ece_delta<=0.01 and final_delta<0 and final_ece_delta<=0.01",
    }
    eval_run = db.execute_one(
        """
        INSERT INTO tennis_elo_evaluation_runs (
            elo_run_id,evaluation_version,config,source_checksum,status
        ) VALUES (%s,%s,%s::jsonb,%s,'running')
        ON CONFLICT (elo_run_id,evaluation_version) DO UPDATE SET
            status=CASE WHEN tennis_elo_evaluation_runs.status='complete' THEN 'complete' ELSE 'running' END,
            error_message=NULL
        RETURNING id
        """,
        (elo_run["id"], EVALUATION_VERSION, stable_json(config), elo_run["source_checksum"]),
    )
    evaluation_run_id = eval_run["id"]
    rows = _load_rows(db, elo_run["id"])

    metric_rows = []
    metric_report = []
    for tour in ("ATP", "WTA"):
        for period in ("burn_in", "development", "validation", "final_test"):
            period_rows = [row for row in rows if row["tour"] == tour and row["period"] == period]
            for surface in ("all", "hard", "clay", "grass"):
                subset = period_rows if surface == "all" else [row for row in period_rows if row["surface"] == surface]
                for model in MODELS:
                    result = metrics(subset, model)
                    metric_rows.append((evaluation_run_id, tour, period, surface, model,
                                        result["n"], result["brier"], result["log_loss"], result["ece"]))
                    metric_report.append({"tour": tour, "period": period, "surface": surface,
                                          "model": model, **result})

    gates = []
    gate_rows = []
    for tour_index, tour in enumerate(("ATP", "WTA")):
        validation = [row for row in rows if row["tour"] == tour and row["period"] == "validation"]
        final_test = [row for row in rows if row["tour"] == tour and row["period"] == "final_test"]
        val_overall = metrics(validation, "overall_elo")
        val_blended = metrics(validation, "blended_surface_elo")
        final_overall = metrics(final_test, "overall_elo")
        final_blended = metrics(final_test, "blended_surface_elo")
        val_delta = val_blended["log_loss"] - val_overall["log_loss"]
        val_ece_delta = val_blended["ece"] - val_overall["ece"]
        final_delta = final_blended["log_loss"] - final_overall["log_loss"]
        final_ece_delta = final_blended["ece"] - final_overall["ece"]
        ci_low, ci_high = cluster_bootstrap_delta(validation, BOOTSTRAP_SEED + tour_index)
        reasons = {
            "sample_gate": len(validation) >= MIN_SAMPLE and len(final_test) >= MIN_SAMPLE,
            "validation_incremental_value": ci_high < 0,
            "validation_calibration": val_ece_delta <= MAX_ECE_DEGRADATION,
            "final_direction": final_delta < 0,
            "final_calibration": final_ece_delta <= MAX_ECE_DEGRADATION,
        }
        gate_status = "PASS" if all(reasons.values()) else "FAIL"
        gate = {
            "tour": tour, "validation_n": len(validation), "validation_logloss_delta": val_delta,
            "bootstrap_ci": [ci_low, ci_high], "validation_ece_delta": val_ece_delta,
            "final_n": len(final_test), "final_logloss_delta": final_delta,
            "final_ece_delta": final_ece_delta, "status": gate_status, "reasons": reasons,
        }
        gates.append(gate)
        gate_rows.append((evaluation_run_id, tour, len(validation), val_delta, ci_low, ci_high,
                          val_ece_delta, len(final_test), final_delta, final_ece_delta,
                          gate_status, stable_json(reasons)))

    with db.connect() as conn:
        cur = conn.cursor()
        execute_values(
            cur,
            """
            INSERT INTO tennis_elo_evaluation_metrics (
                evaluation_run_id,tour,period,surface,model,sample_size,brier,log_loss,calibration_error
            ) VALUES %s ON CONFLICT DO NOTHING
            """,
            metric_rows,
        )
        execute_values(
            cur,
            """
            INSERT INTO tennis_elo_promotion_gates (
                evaluation_run_id,tour,validation_sample_size,validation_logloss_delta,
                bootstrap_ci_low,bootstrap_ci_high,validation_ece_delta,final_test_sample_size,
                final_logloss_delta,final_ece_delta,gate_status,reasons
            ) VALUES %s ON CONFLICT DO NOTHING
            """,
            gate_rows,
            template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)",
        )
        cur.execute(
            "UPDATE tennis_elo_evaluation_runs SET status='complete',completed_at=NOW(),error_message=NULL WHERE id=%s",
            (evaluation_run_id,),
        )

    report = {"evaluation_run_id": evaluation_run_id, "elo_run_id": elo_run["id"],
              "evaluation_version": EVALUATION_VERSION, "rows": len(rows),
              "gates": gates, "metrics": metric_report}
    print(json.dumps(report, indent=2))
    return report


if __name__ == "__main__":
    evaluate(DatabaseManager(load_config().database_url))
