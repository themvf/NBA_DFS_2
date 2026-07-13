"""PASS/FAIL audit for the immutable 2023+ Tennis data foundation."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from config import load_config
from db.database import DatabaseManager

QUALITY_VERSION = "tennis-data-quality-v1"


def _check(name: str, passed: bool, evidence, remedy: str | None = None) -> dict:
    return {
        "check": name,
        "result": "PASS" if passed else "FAIL",
        "evidence": evidence,
        "remedy": None if passed else remedy,
    }


def audit(db: DatabaseManager, through_year: int | None = None) -> dict:
    audit_at = datetime.now(timezone.utc)
    through_year = through_year or audit_at.year
    checks: list[dict] = []

    latest_partitions = db.execute(
        """
        SELECT DISTINCT ON (provider, dataset, tour, season)
            provider, dataset, tour, season, status, row_count, accepted_count,
            rejected_count, min_match_date, max_match_date, missingness,
            retrieval_completed_at, error_message, run_id
        FROM tennis_source_partitions
        WHERE season BETWEEN 2023 AND %s
        ORDER BY provider, dataset, tour, season, created_at DESC, id DESC
        """,
        (through_year,),
    )
    by_partition = {
        (r["provider"], r["dataset"], r["tour"], r["season"]): r
        for r in latest_partitions
    }
    expected = []
    for year in range(2023, through_year + 1):
        expected.extend((
            ("tml_database", "historical_matches_stats", "ATP", year),
            ("tennis_data", "historical_odds_rank", "ATP", year),
            ("tennis_data", "historical_matches_odds_rank", "ATP", year),
            ("tennis_data", "historical_matches_odds_rank", "WTA", year),
        ))
    partition_evidence = []
    partition_ok = True
    for key in expected:
        row = by_partition.get(key)
        ok = bool(row and row["status"] == "pass" and row["accepted_count"] > 0)
        partition_ok &= ok
        partition_evidence.append({
            "provider": key[0], "dataset": key[1], "tour": key[2], "season": key[3],
            "status": row["status"] if row else "missing",
            "accepted": row["accepted_count"] if row else 0,
            "retrieved_at": row["retrieval_completed_at"] if row else None,
            "age_hours": (
                round((audit_at - row["retrieval_completed_at"]).total_seconds() / 3600, 2)
                if row and row["retrieval_completed_at"] else None
            ),
            "error": row["error_message"] if row else "partition has never run",
        })
    checks.append(_check(
        "required_source_partitions",
        partition_ok,
        partition_evidence,
        "Run python -m ingest.tennis_foundation --from-year 2023 and repair every missing/failed partition.",
    ))

    coverage = db.execute(
        """
        SELECT tour, EXTRACT(YEAR FROM match_date)::int AS match_year, surface,
               COUNT(*) AS matches,
               COUNT(*) FILTER (WHERE winner_rank IS NULL OR loser_rank IS NULL) AS missing_rank,
               COUNT(*) FILTER (WHERE winner_decimal_odds IS NULL OR loser_decimal_odds IS NULL) AS missing_odds,
               COUNT(*) FILTER (WHERE start_time IS NULL) AS missing_start_time
        FROM tennis_historical_matches
        WHERE match_date >= DATE '2023-01-01' AND is_current AND source='tennis_data'
        GROUP BY tour, match_year, surface
        ORDER BY tour, match_year, surface
        """
    )
    coverage_years = {(r["tour"], r["match_year"]) for r in coverage if r["matches"] > 0}
    expected_years = {(tour, year) for tour in ("ATP", "WTA") for year in range(2023, through_year + 1)}
    checks.append(_check(
        "historical_tour_year_coverage",
        coverage_years == expected_years,
        {"rows": coverage, "missing_tour_years": sorted(expected_years - coverage_years)},
        "Populate the missing ATP/WTA match-year window; zero-row years are blocking.",
    ))

    core = db.execute_one(
        """
        SELECT COUNT(*) AS matches,
               COUNT(*) FILTER (WHERE match_date IS NULL OR tour IS NULL OR
                   winner_player_id IS NULL OR loser_player_id IS NULL) AS missing_core,
               COUNT(*) FILTER (WHERE surface IS NULL) AS missing_surface,
               COUNT(*) FILTER (WHERE match_date < DATE '2023-01-01') AS pre_2023,
               COUNT(*) FILTER (WHERE winner_player_id=loser_player_id) AS self_matches
        FROM tennis_historical_matches
        WHERE is_current AND source='tennis_data'
        """
    )
    checks.append(_check(
        "historical_core_identity",
        bool(core and core["matches"] > 0 and core["missing_core"] == 0 and core["pre_2023"] == 0 and core["self_matches"] == 0),
        core,
        "Reject or resolve rows missing date/tour/player/surface; remove any pre-2023 dependency.",
    ))

    duplicates = db.execute_one(
        """
        SELECT
          (SELECT COUNT(*) FROM (
             SELECT source, source_match_key
             FROM tennis_historical_matches
             WHERE is_current
             GROUP BY source, source_match_key HAVING COUNT(*) > 1
           ) current_dupes) AS duplicate_current_source_keys,
          (SELECT COUNT(*) FROM (
             SELECT source, source_match_key, raw_checksum, transformation_version
             FROM tennis_historical_matches
             GROUP BY source, source_match_key, raw_checksum, transformation_version
             HAVING COUNT(*) > 1
           ) transform_dupes) AS duplicate_transformations,
          COUNT(*) FILTER (WHERE correction_of_id IS NOT NULL) AS immutable_corrections
        FROM tennis_historical_matches
        """
    )
    checks.append(_check(
        "idempotent_historical_rows",
        bool(duplicates and duplicates["duplicate_current_source_keys"] == 0 and
             duplicates["duplicate_transformations"] == 0),
        duplicates,
        "Fix the source-key/checksum uniqueness contract and replay ingestion.",
    ))

    stats = db.execute_one(
        """
        SELECT
            COUNT(*) FILTER (WHERE hm.tour='ATP') AS atp_stat_rows,
            COUNT(*) FILTER (WHERE hm.tour='ATP' AND ps.stats_available) AS atp_available,
            COUNT(*) FILTER (WHERE hm.tour='WTA') AS wta_stat_rows,
            COUNT(*) FILTER (WHERE hm.tour='WTA' AND ps.stats_available) AS wta_available,
            COUNT(*) FILTER (WHERE hm.tour='WTA' AND
                (ps.missing_reason IS NULL OR ps.missing_reason <> 'source_unavailable')) AS wta_missing_reason_errors
        FROM tennis_player_match_stats ps
        JOIN tennis_historical_matches hm ON hm.id=ps.historical_match_id
        WHERE hm.is_current
        """
    )
    checks.append(_check(
        "performance_availability_semantics",
        bool(stats and stats["atp_stat_rows"] > 0 and stats["atp_available"] > 0 and
             stats["wta_stat_rows"] > 0 and stats["wta_available"] == 0 and
             stats["wta_missing_reason_errors"] == 0),
        stats,
        "Populate ATP raw stats and record WTA source_unavailable explicitly; never zero-fill missing performance.",
    ))

    identity = db.execute_one(
        """
        SELECT COUNT(*) AS players,
               COUNT(*) FILTER (WHERE canonical_name IS NULL OR norm_name IS NULL OR norm_name='') AS invalid_players,
               (SELECT COUNT(*) FROM tennis_identity_reviews WHERE status='open') AS open_reviews
        FROM tennis_players
        """
    )
    checks.append(_check(
        "canonical_player_identity",
        bool(identity and identity["players"] > 0 and identity["invalid_players"] == 0 and identity["open_reviews"] == 0),
        identity,
        "Resolve open identity reviews and invalid canonical player keys before downstream joins.",
    ))

    leakage = db.execute_one(
        """
        SELECT COUNT(*) AS snapshots,
               COUNT(*) FILTER (WHERE stats_through_at >= cutoff_at) AS leaked_snapshots,
               COUNT(*) FILTER (WHERE cutoff_at < TIMESTAMPTZ '2023-01-01 00:00:00+00') AS pre_2023_cutoffs
        FROM tennis_player_feature_snapshots
        """
    )
    # Empty is acceptable at the foundation stage; SCRUM-27 must populate it.
    checks.append(_check(
        "feature_snapshot_leakage",
        bool(leakage and leakage["leaked_snapshots"] == 0 and leakage["pre_2023_cutoffs"] == 0),
        leakage,
        "Rebuild snapshots so stats_through_at is strictly earlier than the decision cutoff.",
    ))

    quotes = db.execute_one(
        """
        SELECT COUNT(*) AS quotes,
               COUNT(*) FILTER (WHERE validation_status='valid' AND NOT is_prestart) AS valid_post_start,
               COUNT(*) FILTER (WHERE validation_status='valid' AND market='moneyline' AND
                   (line_value IS NOT NULL OR paired_line_value IS NOT NULL OR selection_player_id IS NULL OR paired_player_id IS NULL)) AS bad_moneyline,
               COUNT(*) FILTER (WHERE validation_status='valid' AND market='total' AND
                   (line_value IS NULL OR paired_line_value IS NULL OR line_value <> paired_line_value)) AS bad_total_pair,
               COUNT(*) FILTER (WHERE validation_status='valid' AND market='spread' AND
                   (line_value IS NULL OR paired_line_value IS NULL OR ABS(line_value + paired_line_value) > 0.000001)) AS bad_spread_pair,
               COUNT(*) FILTER (WHERE validation_status='valid' AND
                   (bookmaker_updated_at >= commence_time_at_capture OR captured_at >= commence_time_at_capture)) AS bad_time
        FROM tennis_exact_quotes
        """
    )
    quote_ok = bool(quotes and quotes["valid_post_start"] == 0 and quotes["bad_moneyline"] == 0 and
                    quotes["bad_total_pair"] == 0 and quotes["bad_spread_pair"] == 0 and quotes["bad_time"] == 0)
    checks.append(_check(
        "exact_quote_contract",
        quote_ok,
        quotes,
        "Quarantine incomplete/mismatched/post-start quote pairs and rerun exact quote ingestion.",
    ))

    result = "PASS" if all(c["result"] == "PASS" for c in checks) else "FAIL"
    return {
        "quality_version": QUALITY_VERSION,
        "generated_at": audit_at.isoformat(),
        "through_year": through_year,
        "result": result,
        "checks": checks,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Audit the immutable Tennis data foundation")
    parser.add_argument("--through-year", type=int)
    parser.add_argument("--compact", action="store_true")
    args = parser.parse_args()
    cfg = load_config()
    report = audit(DatabaseManager(cfg.database_url), args.through_year)
    print(json.dumps(report, indent=None if args.compact else 2, default=str))
    raise SystemExit(0 if report["result"] == "PASS" else 1)
