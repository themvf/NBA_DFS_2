"""Reusable MLB pitcher lineup report.

Builds a historical report for DraftKings MLB starting pitchers and ranks the
latest loaded slate's active SPs using those historical cohort rates.

Usage:
    python -m model.mlb_pitcher_lineup_report
    python -m model.mlb_pitcher_lineup_report --slate-date 2026-04-15
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from statistics import mean
from typing import Any

from config import DATA_DIR, load_config
from db.database import DatabaseManager

REPORT_DIR = DATA_DIR / "reports"
DEFAULT_OUTPUT = REPORT_DIR / "mlb_pitcher_lineup_report.json"
DEFAULT_SMASH_THRESHOLD = 20.0
DEFAULT_ELITE_THRESHOLD = 25.0
DEFAULT_UNDEROWNED_THRESHOLD = 5.0
DEFAULT_MIN_SAMPLE = 8
DEFAULT_TOP_CANDIDATES = 10
DEFAULT_PIVOT_MAX_OWN = 12.0


BUCKET_SPECS = {
    "projection_bucket": {
        "label": "Projection",
        "order": ["<10", "10-13.9", "14-17.9", "18-21.9", "22+", "unknown"],
    },
    "value_bucket": {
        "label": "Projection per $1k",
        "order": ["<1.4x", "1.4-1.79x", "1.8-2.19x", "2.2x+", "unknown"],
    },
    "projected_own_bucket": {
        "label": "Projected ownership",
        "order": ["<5", "5-9.9", "10-14.9", "15-19.9", "20+", "unknown"],
    },
    "opp_implied_bucket": {
        "label": "Opponent implied total",
        "order": ["<3.2", "3.2-3.79", "3.8-4.49", "4.5+", "unknown"],
    },
    "moneyline_bucket": {
        "label": "Team moneyline",
        "order": ["fav160+", "fav120-159", "pickem", "dog110+", "unknown"],
    },
    "salary_bucket": {
        "label": "Salary",
        "order": ["<7k", "7k-7.9k", "8k-8.9k", "9k+", "unknown"],
    },
}


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result


def _avg(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [_safe_float(row.get(key)) for row in rows]
    valid = [value for value in values if value is not None]
    if not valid:
        return None
    return round(mean(valid), 2)


def _pct(count: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((count / total) * 100.0, 2)


def _weighted_mean(values: list[tuple[float | None, float]]) -> float | None:
    weighted_total = 0.0
    total_weight = 0.0
    for value, weight in values:
        if value is None or weight <= 0:
            continue
        weighted_total += value * weight
        total_weight += weight
    if total_weight <= 0:
        return None
    return round(weighted_total / total_weight, 2)


def bucket_projection(projection: float | None) -> str:
    if projection is None or projection <= 0:
        return "unknown"
    if projection < 10:
        return "<10"
    if projection < 14:
        return "10-13.9"
    if projection < 18:
        return "14-17.9"
    if projection < 22:
        return "18-21.9"
    return "22+"


def bucket_value(projection: float | None, salary: int | None) -> str:
    if projection is None or projection <= 0 or not salary:
        return "unknown"
    value = projection / (salary / 1000.0)
    if value < 1.4:
        return "<1.4x"
    if value < 1.8:
        return "1.4-1.79x"
    if value < 2.2:
        return "1.8-2.19x"
    return "2.2x+"


def bucket_projected_own(projected_own_pct: float | None) -> str:
    if projected_own_pct is None:
        return "unknown"
    if projected_own_pct < 5:
        return "<5"
    if projected_own_pct < 10:
        return "5-9.9"
    if projected_own_pct < 15:
        return "10-14.9"
    if projected_own_pct < 20:
        return "15-19.9"
    return "20+"


def bucket_opp_implied(opp_implied: float | None) -> str:
    if opp_implied is None:
        return "unknown"
    if opp_implied < 3.2:
        return "<3.2"
    if opp_implied < 3.8:
        return "3.2-3.79"
    if opp_implied < 4.5:
        return "3.8-4.49"
    return "4.5+"


def bucket_moneyline(team_ml: float | None) -> str:
    if team_ml is None:
        return "unknown"
    if team_ml <= -160:
        return "fav160+"
    if team_ml <= -120:
        return "fav120-159"
    if team_ml < 110:
        return "pickem"
    return "dog110+"


def bucket_salary(salary: int | None) -> str:
    if not salary:
        return "unknown"
    if salary < 7000:
        return "<7k"
    if salary < 8000:
        return "7k-7.9k"
    if salary < 9000:
        return "8k-8.9k"
    return "9k+"


def enrich_pitcher_row(row: dict[str, Any]) -> dict[str, Any]:
    projection = _safe_float(row.get("projection"))
    salary = int(row["salary"]) if row.get("salary") is not None else None
    projected_own_pct = _safe_float(row.get("projected_own_pct"))
    opp_implied = _safe_float(row.get("opp_implied"))
    team_ml = _safe_float(row.get("team_ml"))
    projected_value_x = None
    if projection is not None and projection > 0 and salary:
        projected_value_x = round(projection / (salary / 1000.0), 2)

    row["projection"] = projection
    row["salary"] = salary
    row["projected_own_pct"] = projected_own_pct
    row["actual_fpts"] = _safe_float(row.get("actual_fpts"))
    row["actual_own_pct"] = _safe_float(row.get("actual_own_pct"))
    row["linestar_proj"] = _safe_float(row.get("linestar_proj"))
    row["our_proj"] = _safe_float(row.get("our_proj"))
    row["our_own_pct"] = _safe_float(row.get("our_own_pct"))
    row["opp_implied"] = opp_implied
    row["team_ml"] = team_ml
    row["projected_value_x"] = projected_value_x
    row["projection_bucket"] = bucket_projection(projection)
    row["value_bucket"] = bucket_value(projection, salary)
    row["projected_own_bucket"] = bucket_projected_own(projected_own_pct)
    row["opp_implied_bucket"] = bucket_opp_implied(opp_implied)
    row["moneyline_bucket"] = bucket_moneyline(team_ml)
    row["salary_bucket"] = bucket_salary(salary)
    return row


def summarize_bucket_rows(
    rows: list[dict[str, Any]],
    bucket_key: str,
    smash_threshold: float,
    elite_threshold: float,
    underowned_threshold: float,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(bucket_key) or "unknown")].append(row)

    summaries: list[dict[str, Any]] = []
    for bucket, bucket_rows in grouped.items():
        row_count = len(bucket_rows)
        hit20 = sum(1 for row in bucket_rows if (row.get("actual_fpts") or 0.0) >= smash_threshold)
        hit25 = sum(1 for row in bucket_rows if (row.get("actual_fpts") or 0.0) >= elite_threshold)
        under20 = sum(
            1
            for row in bucket_rows
            if (row.get("actual_fpts") or 0.0) >= smash_threshold
            and (row.get("actual_own_pct") or 999.0) < underowned_threshold
        )
        under25 = sum(
            1
            for row in bucket_rows
            if (row.get("actual_fpts") or 0.0) >= elite_threshold
            and (row.get("actual_own_pct") or 999.0) < underowned_threshold
        )
        summaries.append(
            {
                "bucket": bucket,
                "rows": row_count,
                "avg_actual_fpts": _avg(bucket_rows, "actual_fpts"),
                "avg_projection": _avg(bucket_rows, "projection"),
                "avg_projected_own_pct": _avg(bucket_rows, "projected_own_pct"),
                "avg_salary": _avg(bucket_rows, "salary"),
                "hit20_rate": _pct(hit20, row_count),
                "hit25_rate": _pct(hit25, row_count),
                "underowned_hit20_rate": _pct(under20, row_count),
                "underowned_hit25_rate": _pct(under25, row_count),
            }
        )

    order = BUCKET_SPECS[bucket_key]["order"]
    order_index = {label: idx for idx, label in enumerate(order)}
    summaries.sort(key=lambda row: order_index.get(row["bucket"], len(order)))
    return summaries


def build_bucket_lookup(bucket_summaries: dict[str, list[dict[str, Any]]]) -> dict[str, dict[str, dict[str, Any]]]:
    return {
        bucket_key: {row["bucket"]: row for row in rows}
        for bucket_key, rows in bucket_summaries.items()
    }


def build_sample_summary(
    rows: list[dict[str, Any]],
    smash_threshold: float,
    elite_threshold: float,
    underowned_threshold: float,
) -> dict[str, Any]:
    row_count = len(rows)
    return {
        "rows": row_count,
        "slates": len({row["slate_id"] for row in rows}),
        "avg_actual_fpts": _avg(rows, "actual_fpts"),
        "avg_projection": _avg(rows, "projection"),
        "avg_projected_own_pct": _avg(rows, "projected_own_pct"),
        "hit20_rate": _pct(sum(1 for row in rows if (row.get("actual_fpts") or 0.0) >= smash_threshold), row_count),
        "hit25_rate": _pct(sum(1 for row in rows if (row.get("actual_fpts") or 0.0) >= elite_threshold), row_count),
        "underowned_hit20_rate": _pct(
            sum(
                1
                for row in rows
                if (row.get("actual_fpts") or 0.0) >= smash_threshold
                and (row.get("actual_own_pct") or 999.0) < underowned_threshold
            ),
            row_count,
        ),
        "context_coverage": {
            "opp_implied_known_rows": sum(1 for row in rows if row.get("opp_implied") is not None),
            "moneyline_known_rows": sum(1 for row in rows if row.get("team_ml") is not None),
        },
    }


def generate_findings(
    bucket_summaries: dict[str, list[dict[str, Any]]],
    min_sample: int,
) -> list[str]:
    findings: list[str] = []

    def top_bucket(bucket_key: str, metric: str) -> dict[str, Any] | None:
        rows = [row for row in bucket_summaries[bucket_key] if row["rows"] >= min_sample]
        if not rows:
            return None
        return max(rows, key=lambda row: (row.get(metric) or 0.0, row["rows"]))

    ceiling_bucket = top_bucket("projection_bucket", "hit20_rate")
    if ceiling_bucket:
        findings.append(
            f"Projection bucket {ceiling_bucket['bucket']} has the best 20+ DK rate at "
            f"{ceiling_bucket['hit20_rate']}% ({ceiling_bucket['rows']} rows)."
        )

    value_bucket = top_bucket("value_bucket", "hit25_rate")
    if value_bucket:
        findings.append(
            f"Value bucket {value_bucket['bucket']} leads 25+ DK outcomes at "
            f"{value_bucket['hit25_rate']}% ({value_bucket['rows']} rows)."
        )

    own_bucket = top_bucket("projected_own_bucket", "underowned_hit20_rate")
    if own_bucket:
        findings.append(
            f"Projected-own bucket {own_bucket['bucket']} is the best contrarian lane: "
            f"{own_bucket['underowned_hit20_rate']}% under-owned 20+ games ({own_bucket['rows']} rows)."
        )

    ml_bucket = top_bucket("moneyline_bucket", "hit20_rate")
    if ml_bucket:
        findings.append(
            f"Moneyline bucket {ml_bucket['bucket']} has the strongest known-context 20+ DK rate at "
            f"{ml_bucket['hit20_rate']}% ({ml_bucket['rows']} rows)."
        )

    opp_bucket = top_bucket("opp_implied_bucket", "underowned_hit20_rate")
    if opp_bucket:
        findings.append(
            f"Opponent-implied bucket {opp_bucket['bucket']} best supports contrarian ceiling: "
            f"{opp_bucket['underowned_hit20_rate']}% under-owned 20+ games ({opp_bucket['rows']} rows)."
        )

    return findings


def top_historical_smashes(
    rows: list[dict[str, Any]],
    smash_threshold: float,
    underowned_threshold: float,
    limit: int = 15,
) -> list[dict[str, Any]]:
    filtered = [
        row for row in rows
        if (row.get("actual_fpts") or 0.0) >= smash_threshold
        and (row.get("actual_own_pct") or 999.0) < underowned_threshold
    ]
    filtered.sort(
        key=lambda row: (
            row.get("actual_fpts") or 0.0,
            -1.0 * (row.get("actual_own_pct") or 0.0),
        ),
        reverse=True,
    )

    result: list[dict[str, Any]] = []
    for row in filtered[:limit]:
        result.append(
            {
                "slate_date": row["slate_date"],
                "name": row["name"],
                "team_abbrev": row["team_abbrev"],
                "salary": row["salary"],
                "projection": row["projection"],
                "projected_own_pct": row["projected_own_pct"],
                "actual_fpts": row["actual_fpts"],
                "actual_own_pct": row["actual_own_pct"],
                "opp_implied": row["opp_implied"],
                "team_ml": row["team_ml"],
                "projected_value_x": row["projected_value_x"],
            }
        )
    return result


def score_pitcher_candidate(
    row: dict[str, Any],
    bucket_lookup: dict[str, dict[str, dict[str, Any]]],
    min_sample: int,
) -> dict[str, Any]:
    projection_summary = bucket_lookup["projection_bucket"].get(row["projection_bucket"])
    value_summary = bucket_lookup["value_bucket"].get(row["value_bucket"])
    own_summary = bucket_lookup["projected_own_bucket"].get(row["projected_own_bucket"])
    opp_summary = bucket_lookup["opp_implied_bucket"].get(row["opp_implied_bucket"])
    ml_summary = bucket_lookup["moneyline_bucket"].get(row["moneyline_bucket"])

    def usable(summary: dict[str, Any] | None) -> bool:
        return bool(summary and summary["rows"] >= min_sample)

    projection_score = _weighted_mean([
        ((projection_summary or {}).get("hit20_rate"), 0.55) if usable(projection_summary) else (None, 0),
        ((value_summary or {}).get("hit20_rate"), 0.45) if usable(value_summary) else (None, 0),
    ])
    ceiling_score = _weighted_mean([
        ((projection_summary or {}).get("hit25_rate"), 0.55) if usable(projection_summary) else (None, 0),
        ((value_summary or {}).get("hit25_rate"), 0.45) if usable(value_summary) else (None, 0),
    ])
    contrarian_score = _weighted_mean([
        ((own_summary or {}).get("underowned_hit20_rate"), 0.45) if usable(own_summary) else (None, 0),
        ((value_summary or {}).get("underowned_hit20_rate"), 0.20) if usable(value_summary) else (None, 0),
        ((opp_summary or {}).get("underowned_hit20_rate"), 0.20) if usable(opp_summary) else (None, 0),
        ((ml_summary or {}).get("underowned_hit20_rate"), 0.15) if usable(ml_summary) else (None, 0),
    ])
    lineup_score = _weighted_mean([
        (projection_score, 0.45),
        (ceiling_score, 0.35),
        (contrarian_score, 0.20),
    ])

    notes: list[str] = []
    if usable(projection_summary):
        notes.append(
            f"Projection {row['projection_bucket']}: {projection_summary['hit20_rate']}% hit 20+"
            f" ({projection_summary['rows']} rows)"
        )
    if usable(value_summary):
        notes.append(
            f"Value {row['value_bucket']}: {value_summary['hit25_rate']}% hit 25+"
            f" ({value_summary['rows']} rows)"
        )
    if usable(own_summary):
        notes.append(
            f"Projected own {row['projected_own_bucket']}: {own_summary['underowned_hit20_rate']}% under-owned 20+"
            f" ({own_summary['rows']} rows)"
        )
    if usable(opp_summary) and row["opp_implied_bucket"] != "unknown":
        notes.append(
            f"Opp implied {row['opp_implied_bucket']}: {opp_summary['underowned_hit20_rate']}% under-owned 20+"
            f" ({opp_summary['rows']} rows)"
        )
    if usable(ml_summary) and row["moneyline_bucket"] != "unknown":
        notes.append(
            f"Moneyline {row['moneyline_bucket']}: {ml_summary['hit20_rate']}% hit 20+"
            f" ({ml_summary['rows']} rows)"
        )

    return {
        "name": row["name"],
        "team_abbrev": row["team_abbrev"],
        "salary": row["salary"],
        "projection": row["projection"],
        "linestar_proj": row["linestar_proj"],
        "our_proj": row["our_proj"],
        "projected_own_pct": row["projected_own_pct"],
        "our_own_pct": row["our_own_pct"],
        "projected_value_x": row["projected_value_x"],
        "opp_implied": row["opp_implied"],
        "team_ml": row["team_ml"],
        "is_home": row.get("is_home"),
        "projection_bucket": row["projection_bucket"],
        "value_bucket": row["value_bucket"],
        "projected_own_bucket": row["projected_own_bucket"],
        "opp_implied_bucket": row["opp_implied_bucket"],
        "moneyline_bucket": row["moneyline_bucket"],
        "projection_score": projection_score,
        "ceiling_score": ceiling_score,
        "contrarian_score": contrarian_score,
        "lineup_score": lineup_score,
        "notes": notes,
        "actual_fpts": row.get("actual_fpts"),
        "actual_own_pct": row.get("actual_own_pct"),
    }


def serialize_report(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: serialize_report(val) for key, val in value.items()}
    if isinstance(value, list):
        return [serialize_report(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def fetch_historical_pitcher_rows(
    db: DatabaseManager,
    contest_type: str,
    contest_format: str,
    exclude_slate_id: int | None,
) -> list[dict[str, Any]]:
    sql = """
        SELECT
            ds.id AS slate_id,
            ds.slate_date,
            dp.name,
            dp.team_abbrev,
            dp.salary,
            COALESCE(dp.live_proj, dp.our_proj, dp.linestar_proj) AS projection,
            dp.linestar_proj,
            dp.our_proj,
            COALESCE(dp.live_own_pct, dp.proj_own_pct, dp.our_own_pct) AS projected_own_pct,
            dp.our_own_pct,
            dp.actual_fpts,
            dp.actual_own_pct,
            CASE
                WHEN dp.mlb_team_id = mm.home_team_id THEN mm.away_implied
                WHEN dp.mlb_team_id = mm.away_team_id THEN mm.home_implied
                ELSE NULL
            END AS opp_implied,
            CASE
                WHEN dp.mlb_team_id = mm.home_team_id THEN mm.home_ml
                WHEN dp.mlb_team_id = mm.away_team_id THEN mm.away_ml
                ELSE NULL
            END AS team_ml,
            CASE
                WHEN dp.mlb_team_id = mm.home_team_id THEN TRUE
                WHEN dp.mlb_team_id = mm.away_team_id THEN FALSE
                ELSE NULL
            END AS is_home
        FROM dk_players dp
        JOIN dk_slates ds ON ds.id = dp.slate_id
        LEFT JOIN mlb_matchups mm ON mm.id = dp.matchup_id
        WHERE ds.sport = 'mlb'
          AND ds.contest_type = %s
          AND ds.contest_format = %s
          AND dp.actual_fpts IS NOT NULL
          AND dp.actual_own_pct IS NOT NULL
          AND COALESCE(dp.is_out, false) = false
          AND dp.eligible_positions ILIKE '%%SP%%'
    """
    params: list[Any] = [contest_type, contest_format]
    if exclude_slate_id is not None:
        sql += " AND ds.id <> %s"
        params.append(exclude_slate_id)
    rows = db.execute(sql, params)
    return [enrich_pitcher_row(dict(row)) for row in rows]


def resolve_target_slate(
    db: DatabaseManager,
    contest_type: str,
    contest_format: str,
    slate_date: str | None,
) -> dict[str, Any] | None:
    if slate_date:
        rows = db.execute(
            """
            SELECT
                ds.id,
                ds.slate_date,
                ds.contest_type,
                ds.contest_format,
                COUNT(*) FILTER (
                    WHERE COALESCE(dp.is_out, false) = false
                      AND dp.eligible_positions ILIKE '%%SP%%'
                )::int AS active_sp_count,
                COUNT(*) FILTER (WHERE dp.actual_fpts IS NULL)::int AS pending_actual_rows
            FROM dk_slates ds
            JOIN dk_players dp ON dp.slate_id = ds.id
            WHERE ds.sport = 'mlb'
              AND ds.contest_type = %s
              AND ds.contest_format = %s
              AND ds.slate_date = %s
            GROUP BY ds.id, ds.slate_date, ds.contest_type, ds.contest_format
            HAVING COUNT(*) FILTER (
                WHERE COALESCE(dp.is_out, false) = false
                  AND dp.eligible_positions ILIKE '%%SP%%'
            ) > 0
            ORDER BY ds.id DESC
            LIMIT 1
            """,
            [contest_type, contest_format, slate_date],
        )
        return dict(rows[0]) if rows else None

    rows = db.execute(
        """
        SELECT
            ds.id,
            ds.slate_date,
            ds.contest_type,
            ds.contest_format,
            COUNT(*) FILTER (
                WHERE COALESCE(dp.is_out, false) = false
                  AND dp.eligible_positions ILIKE '%%SP%%'
            )::int AS active_sp_count,
            COUNT(*) FILTER (WHERE dp.actual_fpts IS NULL)::int AS pending_actual_rows
        FROM dk_slates ds
        JOIN dk_players dp ON dp.slate_id = ds.id
        WHERE ds.sport = 'mlb'
          AND ds.contest_type = %s
          AND ds.contest_format = %s
        GROUP BY ds.id, ds.slate_date, ds.contest_type, ds.contest_format
        HAVING COUNT(*) FILTER (
            WHERE COALESCE(dp.is_out, false) = false
              AND dp.eligible_positions ILIKE '%%SP%%'
        ) > 0
        ORDER BY
            CASE WHEN COUNT(*) FILTER (WHERE dp.actual_fpts IS NULL) > 0 THEN 0 ELSE 1 END,
            ds.slate_date DESC,
            ds.id DESC
        LIMIT 1
        """,
        [contest_type, contest_format],
    )
    return dict(rows[0]) if rows else None


def fetch_current_pitchers(db: DatabaseManager, slate_id: int) -> list[dict[str, Any]]:
    rows = db.execute(
        """
        SELECT
            ds.id AS slate_id,
            ds.slate_date,
            dp.name,
            dp.team_abbrev,
            dp.salary,
            COALESCE(dp.live_proj, dp.our_proj, dp.linestar_proj) AS projection,
            dp.linestar_proj,
            dp.our_proj,
            COALESCE(dp.live_own_pct, dp.proj_own_pct, dp.our_own_pct) AS projected_own_pct,
            dp.our_own_pct,
            dp.actual_fpts,
            dp.actual_own_pct,
            CASE
                WHEN dp.mlb_team_id = mm.home_team_id THEN mm.away_implied
                WHEN dp.mlb_team_id = mm.away_team_id THEN mm.home_implied
                ELSE NULL
            END AS opp_implied,
            CASE
                WHEN dp.mlb_team_id = mm.home_team_id THEN mm.home_ml
                WHEN dp.mlb_team_id = mm.away_team_id THEN mm.away_ml
                ELSE NULL
            END AS team_ml,
            CASE
                WHEN dp.mlb_team_id = mm.home_team_id THEN TRUE
                WHEN dp.mlb_team_id = mm.away_team_id THEN FALSE
                ELSE NULL
            END AS is_home
        FROM dk_players dp
        JOIN dk_slates ds ON ds.id = dp.slate_id
        LEFT JOIN mlb_matchups mm ON mm.id = dp.matchup_id
        WHERE ds.id = %s
          AND COALESCE(dp.is_out, false) = false
          AND dp.eligible_positions ILIKE '%%SP%%'
        ORDER BY COALESCE(dp.live_proj, dp.our_proj, dp.linestar_proj) DESC NULLS LAST, dp.salary DESC
        """,
        [slate_id],
    )
    return [enrich_pitcher_row(dict(row)) for row in rows]


def build_report(
    db: DatabaseManager,
    contest_type: str,
    contest_format: str,
    slate_date: str | None,
    smash_threshold: float,
    elite_threshold: float,
    underowned_threshold: float,
    min_sample: int,
    top_candidates: int,
    pivot_max_own: float,
) -> dict[str, Any]:
    target_slate = resolve_target_slate(db, contest_type, contest_format, slate_date)
    exclude_slate_id = int(target_slate["id"]) if target_slate else None

    historical_rows = fetch_historical_pitcher_rows(db, contest_type, contest_format, exclude_slate_id)
    if not historical_rows:
        raise RuntimeError("No historical MLB SP rows found for the requested report settings.")

    bucket_summaries = {
        bucket_key: summarize_bucket_rows(
            historical_rows,
            bucket_key=bucket_key,
            smash_threshold=smash_threshold,
            elite_threshold=elite_threshold,
            underowned_threshold=underowned_threshold,
        )
        for bucket_key in BUCKET_SPECS
    }
    bucket_lookup = build_bucket_lookup(bucket_summaries)

    current_section: dict[str, Any] | None = None
    if target_slate:
        current_rows = fetch_current_pitchers(db, int(target_slate["id"]))
        scored_rows = [
            score_pitcher_candidate(row, bucket_lookup, min_sample=min_sample)
            for row in current_rows
        ]
        scored_rows.sort(
            key=lambda row: (
                row.get("lineup_score") or -1.0,
                row.get("projection") or -1.0,
                -1.0 * (row.get("projected_own_pct") or 0.0),
            ),
            reverse=True,
        )
        contrarian_rows = [
            row
            for row in scored_rows
            if row.get("projected_own_pct") is None or row["projected_own_pct"] <= pivot_max_own
        ]
        contrarian_rows.sort(
            key=lambda row: (
                row.get("contrarian_score") or -1.0,
                row.get("ceiling_score") or -1.0,
                row.get("projection") or -1.0,
            ),
            reverse=True,
        )
        current_section = {
            "slate": {
                "id": int(target_slate["id"]),
                "slate_date": target_slate["slate_date"],
                "contest_type": target_slate["contest_type"],
                "contest_format": target_slate["contest_format"],
                "active_sp_count": int(target_slate["active_sp_count"]),
                "pending_actual_rows": int(target_slate["pending_actual_rows"]),
            },
            "pitchers": scored_rows[:top_candidates],
            "contrarian_pitchers": contrarian_rows[:top_candidates],
        }

    return {
        "generated_at": datetime.now(UTC),
        "settings": {
            "contest_type": contest_type,
            "contest_format": contest_format,
            "slate_date": slate_date,
            "smash_threshold": smash_threshold,
            "elite_threshold": elite_threshold,
            "underowned_threshold": underowned_threshold,
            "min_sample": min_sample,
            "top_candidates": top_candidates,
            "pivot_max_own": pivot_max_own,
        },
        "historical": {
            "sample": build_sample_summary(
                historical_rows,
                smash_threshold=smash_threshold,
                elite_threshold=elite_threshold,
                underowned_threshold=underowned_threshold,
            ),
            "findings": generate_findings(bucket_summaries, min_sample=min_sample),
            "bucket_summaries": bucket_summaries,
            "top_underowned_smashes": top_historical_smashes(
                historical_rows,
                smash_threshold=smash_threshold,
                underowned_threshold=underowned_threshold,
            ),
        },
        "current_slate": current_section,
    }


def print_report(report: dict[str, Any]) -> None:
    historical = report["historical"]
    settings = report["settings"]
    sample = historical["sample"]

    print("MLB PITCHER LINEUP REPORT")
    print(
        f"Historical sample: {sample['rows']} active SP rows across {sample['slates']} "
        f"{settings['contest_type']} {settings['contest_format']} slates"
    )
    print(
        f"Targets: {settings['smash_threshold']:.0f}+ DK, {settings['elite_threshold']:.0f}+ DK elite, "
        f"<{settings['underowned_threshold']:.0f}% actual own contrarian"
    )
    print(
        f"Historical hit rates: 20+ DK {sample['hit20_rate']}% | "
        f"25+ DK {sample['hit25_rate']}% | "
        f"under-owned 20+ DK {sample['underowned_hit20_rate']}%"
    )

    findings = historical.get("findings") or []
    if findings:
        print("\nKey findings")
        for finding in findings:
            print(f"- {finding}")

    current_slate = report.get("current_slate")
    if current_slate:
        slate = current_slate["slate"]
        print(
            f"\nCurrent slate target: {slate['slate_date']} {slate['contest_type']} "
            f"(slate {slate['id']}) | active SP: {slate['active_sp_count']}"
        )
        print("Top lineup candidates")
        for idx, row in enumerate(current_slate["pitchers"], start=1):
            proj = f"{row['projection']:.2f}" if row.get("projection") is not None else "NA"
            own = f"{row['projected_own_pct']:.1f}%" if row.get("projected_own_pct") is not None else "NA"
            value = f"{row['projected_value_x']:.2f}x" if row.get("projected_value_x") is not None else "NA"
            score = f"{row['lineup_score']:.2f}" if row.get("lineup_score") is not None else "NA"
            print(
                f"{idx:>2}. {row['name']} ({row['team_abbrev']}) ${row['salary']} | "
                f"proj {proj} | own {own} | value {value} | lineup score {score}"
            )
            for note in row.get("notes", [])[:3]:
                print(f"    - {note}")

        contrarian_rows = current_slate.get("contrarian_pitchers") or []
        if contrarian_rows:
            print(f"\nContrarian pivots (<={report['settings']['pivot_max_own']:.0f}% projected own)")
            for idx, row in enumerate(contrarian_rows[:5], start=1):
                proj = f"{row['projection']:.2f}" if row.get("projection") is not None else "NA"
                own = f"{row['projected_own_pct']:.1f}%" if row.get("projected_own_pct") is not None else "NA"
                cscore = f"{row['contrarian_score']:.2f}" if row.get("contrarian_score") is not None else "NA"
                print(
                    f"{idx:>2}. {row['name']} ({row['team_abbrev']}) ${row['salary']} | "
                    f"proj {proj} | own {own} | contrarian score {cscore}"
                )
                for note in row.get("notes", [])[:3]:
                    print(f"    - {note}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build MLB pitcher lineup report from historical DK slates.")
    parser.add_argument("--contest-type", default="main", help="Contest type to analyze (default: main)")
    parser.add_argument("--contest-format", default="gpp", help="Contest format to analyze (default: gpp)")
    parser.add_argument("--slate-date", help="Optional slate date to score in YYYY-MM-DD format")
    parser.add_argument(
        "--smash-threshold",
        type=float,
        default=DEFAULT_SMASH_THRESHOLD,
        help=f"DK points threshold for a usable smash (default: {DEFAULT_SMASH_THRESHOLD})",
    )
    parser.add_argument(
        "--elite-threshold",
        type=float,
        default=DEFAULT_ELITE_THRESHOLD,
        help=f"DK points threshold for an elite smash (default: {DEFAULT_ELITE_THRESHOLD})",
    )
    parser.add_argument(
        "--underowned-threshold",
        type=float,
        default=DEFAULT_UNDEROWNED_THRESHOLD,
        help=f"Actual ownership threshold for contrarian hits (default: {DEFAULT_UNDEROWNED_THRESHOLD})",
    )
    parser.add_argument(
        "--min-sample",
        type=int,
        default=DEFAULT_MIN_SAMPLE,
        help=f"Minimum bucket rows before using that cohort in candidate scoring (default: {DEFAULT_MIN_SAMPLE})",
    )
    parser.add_argument(
        "--top-candidates",
        type=int,
        default=DEFAULT_TOP_CANDIDATES,
        help=f"How many current pitchers to include (default: {DEFAULT_TOP_CANDIDATES})",
    )
    parser.add_argument(
        "--pivot-max-own",
        type=float,
        default=DEFAULT_PIVOT_MAX_OWN,
        help=f"Maximum projected ownership for contrarian SP pivots (default: {DEFAULT_PIVOT_MAX_OWN})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output JSON path (default: {DEFAULT_OUTPUT})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config()
    if not cfg.database_url:
        raise RuntimeError("DATABASE_URL is required.")

    db = DatabaseManager(cfg.database_url)
    report = build_report(
        db=db,
        contest_type=args.contest_type,
        contest_format=args.contest_format,
        slate_date=args.slate_date,
        smash_threshold=args.smash_threshold,
        elite_threshold=args.elite_threshold,
        underowned_threshold=args.underowned_threshold,
        min_sample=args.min_sample,
        top_candidates=args.top_candidates,
        pivot_max_own=args.pivot_max_own,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    serialized = serialize_report(report)
    args.output.write_text(json.dumps(serialized, indent=2), encoding="utf-8")
    print_report(serialized)
    print(f"\nSaved report to {args.output}")


if __name__ == "__main__":
    main()
