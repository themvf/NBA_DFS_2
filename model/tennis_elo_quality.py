"""PASS/FAIL verification for immutable Tennis surface Elo snapshots."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from config import load_config
from db.database import DatabaseManager
from model.tennis_surface_elo import (
    ALGORITHM_VERSION,
    FEATURE_VERSION_BASE,
    _load_matches,
    _source_checksum,
)

QUALITY_VERSION = "tennis-elo-quality-v1"


def _check(name: str, passed: bool, evidence, remedy: str) -> dict:
    return {"check": name, "result": "PASS" if passed else "FAIL",
            "evidence": evidence, "remedy": None if passed else remedy}


def audit(db: DatabaseManager) -> dict:
    checks: list[dict] = []
    source_rows = _load_matches(db)
    current_checksum = _source_checksum(source_rows)
    run = db.execute_one(
        """
        SELECT * FROM tennis_elo_runs
        WHERE algorithm_version=%s AND status='complete'
        ORDER BY completed_at DESC,id DESC LIMIT 1
        """,
        (ALGORITHM_VERSION,),
    )
    current_run = bool(run and run["source_checksum"] == current_checksum)
    checks.append(_check(
        "current_source_run",
        current_run,
        {"run_id": run["id"] if run else None,
         "run_source_checksum": run["source_checksum"] if run else None,
         "current_source_checksum": current_checksum,
         "source_matches": len(source_rows),
         "run_status": run["status"] if run else "missing"},
        "Rebuild python -m model.tennis_surface_elo from the current canonical source rows.",
    ))
    if not run:
        return {"quality_version": QUALITY_VERSION, "generated_at": datetime.now(timezone.utc).isoformat(),
                "result": "FAIL", "checks": checks}
    run_id = run["id"]
    feature_version = f"{FEATURE_VERSION_BASE}:{run['source_checksum'][:12]}"

    counts = db.execute_one(
        """
        SELECT COUNT(*) AS events, COUNT(DISTINCT historical_match_id) AS matches,
               COUNT(*) FILTER (WHERE eligible) AS eligible_events,
               COUNT(*) FILTER (WHERE NOT eligible) AS excluded_events,
               COUNT(*) FILTER (WHERE match_date < DATE '2023-01-01') AS pre_2023,
               COUNT(*) FILTER (WHERE stats_through_at >= cutoff_at) AS leaked,
               COUNT(*) FILTER (WHERE expected_overall NOT BETWEEN 0 AND 1 OR
                   expected_surface NOT BETWEEN 0 AND 1 OR expected_blended NOT BETWEEN 0 AND 1) AS bad_probability,
               COUNT(*) FILTER (WHERE NOT eligible AND
                   (ABS(overall_delta) > 1e-12 OR ABS(surface_delta) > 1e-12)) AS excluded_updates,
               COUNT(*) FILTER (WHERE same_day_match_count > 1) AS ambiguous_same_day_events
        FROM tennis_elo_rating_events WHERE run_id=%s
        """,
        (run_id,),
    )
    counts_ok = bool(
        counts and counts["events"] == len(source_rows) * 2 and
        counts["matches"] == len(source_rows) and counts["pre_2023"] == 0 and
        counts["leaked"] == 0 and counts["bad_probability"] == 0 and
        counts["excluded_updates"] == 0 and
        counts["eligible_events"] == run["eligible_match_count"] * 2 and
        counts["excluded_events"] == run["excluded_match_count"] * 2
    )
    checks.append(_check(
        "event_population_and_exclusions", counts_ok, counts,
        "Rebuild event rows; require exactly two events per source match and zero excluded-result updates/leakage.",
    ))

    pairs = db.execute_one(
        """
        SELECT COUNT(*) AS bad_match_pairs FROM (
            SELECT historical_match_id
            FROM tennis_elo_rating_events WHERE run_id=%s
            GROUP BY historical_match_id
            HAVING COUNT(*) <> 2
               OR ABS(SUM(expected_overall)-1) > 1e-9
               OR ABS(SUM(expected_surface)-1) > 1e-9
               OR ABS(SUM(expected_blended)-1) > 1e-9
               OR ABS(SUM(overall_delta)) > 1e-9
               OR ABS(SUM(surface_delta)) > 1e-9
        ) bad
        """,
        (run_id,),
    )
    checks.append(_check(
        "paired_zero_sum_math", bool(pairs and pairs["bad_match_pairs"] == 0), pairs,
        "Fix probability symmetry or zero-sum Elo deltas for the affected match pairs.",
    ))

    batch = db.execute_one(
        """
        WITH per_day AS (
          SELECT player_id,match_date,
                 MAX(overall_before)-MIN(overall_before) AS before_spread,
                 MAX(overall_after)-MIN(overall_after) AS after_spread,
                 MAX(overall_after) - (MAX(overall_before)+SUM(overall_delta)) AS overall_error
          FROM tennis_elo_rating_events WHERE run_id=%s
          GROUP BY player_id,match_date
        ), per_surface_day AS (
          SELECT player_id,match_date,surface_bucket,
                 MAX(surface_before)-MIN(surface_before) AS before_spread,
                 MAX(surface_after)-MIN(surface_after) AS after_spread,
                 MAX(surface_after) - (MAX(surface_before)+SUM(surface_delta)) AS surface_error
          FROM tennis_elo_rating_events WHERE run_id=%s
          GROUP BY player_id,match_date,surface_bucket
        )
        SELECT
          (SELECT COUNT(*) FROM per_day WHERE ABS(before_spread)>1e-9 OR ABS(after_spread)>1e-9 OR ABS(overall_error)>1e-8) AS bad_overall_batches,
          (SELECT COUNT(*) FROM per_surface_day WHERE ABS(before_spread)>1e-9 OR ABS(after_spread)>1e-9 OR ABS(surface_error)>1e-8) AS bad_surface_batches
        """,
        (run_id, run_id),
    )
    checks.append(_check(
        "same_day_batch_math",
        bool(batch and batch["bad_overall_batches"] == 0 and batch["bad_surface_batches"] == 0),
        batch,
        "Recompute same-day batches from one frozen start-of-day rating and apply summed deltas once.",
    ))

    continuity = db.execute_one(
        """
        WITH overall_dates AS (
          SELECT player_id,match_date,MAX(overall_before) AS before,MAX(overall_after) AS after
          FROM tennis_elo_rating_events WHERE run_id=%s GROUP BY player_id,match_date
        ), overall_lag AS (
          SELECT *,LAG(after) OVER (PARTITION BY player_id ORDER BY match_date) AS previous_after
          FROM overall_dates
        ), surface_dates AS (
          SELECT player_id,surface_bucket,match_date,MAX(surface_before) AS before,MAX(surface_after) AS after
          FROM tennis_elo_rating_events WHERE run_id=%s GROUP BY player_id,surface_bucket,match_date
        ), surface_lag AS (
          SELECT *,LAG(after) OVER (PARTITION BY player_id,surface_bucket ORDER BY match_date) AS previous_after
          FROM surface_dates
        )
        SELECT
          (SELECT COUNT(*) FROM overall_lag WHERE previous_after IS NOT NULL AND ABS(before-previous_after)>1e-8) AS bad_overall_links,
          (SELECT COUNT(*) FROM surface_lag WHERE previous_after IS NOT NULL AND ABS(before-previous_after)>1e-8) AS bad_surface_links
        """,
        (run_id, run_id),
    )
    checks.append(_check(
        "chronological_continuity",
        bool(continuity and continuity["bad_overall_links"] == 0 and continuity["bad_surface_links"] == 0),
        continuity,
        "Fix chronological state propagation between match dates; do not use future rating state.",
    ))

    features = db.execute_one(
        """
        SELECT COUNT(*) AS snapshots,
               COUNT(*) FILTER (WHERE fs.stats_through_at >= fs.cutoff_at) AS leaked,
               COUNT(*) FILTER (WHERE fs.cutoff_at < TIMESTAMPTZ '2023-01-01') AS pre_2023,
               COUNT(*) FILTER (WHERE ABS(fs.overall_elo-e.overall_before)>1e-9 OR
                   ABS(fs.surface_elo-e.surface_before)>1e-9) AS rating_mismatch,
               COUNT(*) FILTER (WHERE e.tour='WTA' AND
                   (fs.serve_points_won_pct IS NOT NULL OR fs.return_points_won_pct IS NOT NULL)) AS fabricated_wta_stats,
               COUNT(*) FILTER (WHERE e.tour='ATP' AND fs.serve_points_won_pct IS NOT NULL
                   AND fs.return_points_won_pct IS NOT NULL) AS atp_with_performance,
               COUNT(*) FILTER (WHERE e.tour='ATP' AND fs.source_availability->>'serve_return'='tml_no_prior_365d_sample') AS atp_without_prior_sample
        FROM tennis_player_feature_snapshots fs
        JOIN tennis_elo_rating_events e ON e.historical_match_id=fs.historical_match_id
             AND e.player_id=fs.player_id AND e.run_id=%s
        WHERE fs.feature_version=%s
        """,
        (run_id, feature_version),
    )
    features_ok = bool(features and features["snapshots"] == len(source_rows) * 2 and
                       features["leaked"] == 0 and features["pre_2023"] == 0 and
                       features["rating_mismatch"] == 0 and features["fabricated_wta_stats"] == 0 and
                       features["atp_with_performance"] > 0)
    checks.append(_check(
        "feature_snapshot_leakage", features_ok, features,
        "Rebuild point-in-time snapshots from the matching Elo run; stats_through must precede cutoff and WTA unavailable stats stay NULL.",
    ))

    result = "PASS" if all(check["result"] == "PASS" for check in checks) else "FAIL"
    return {"quality_version": QUALITY_VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "run_id": run_id, "feature_version": feature_version,
            "result": result, "checks": checks}


if __name__ == "__main__":
    report = audit(DatabaseManager(load_config().database_url))
    print(json.dumps(report, indent=2, default=str))
    raise SystemExit(0 if report["result"] == "PASS" else 1)
