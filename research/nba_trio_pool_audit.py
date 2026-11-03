"""Postgame attribution of the best-of-five scoring gap to pool and selection."""
from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path

from research.nba_points_forward import aware, write_new
from research.nba_trio_math_runner import checked, grade, read, signed

VERSION = "nba-trio-pool-audit-v1"


def player_id(row):
    return f"{row['event_id']}:{row['player_key']}"


def benchmark(ids, points, statuses, names):
    leaders = sorted((key for key in ids if key in points), key=lambda key: (-points[key], key))
    trio = leaders[:3] if len(leaders) >= 3 else []
    return {
        "eligible": len(ids),
        "verified": len(leaders),
        "unresolved": dict(Counter(statuses[key] for key in ids if key not in points)),
        "best_total": sum(points[key] for key in trio) if trio else None,
        "trio": [{"player": names[key], "points": points[key]} for key in trio],
    }


def top_three_capture(trio, points):
    if not trio or len(points) < 3:
        return None, None
    boundary = sorted(points.values(), reverse=True)[2]
    above = sum(value > boundary for value in points.values())
    tied = sum(value == boundary for value in points.values())
    fractional = sum(
        1 if points[key] > boundary else (3 - above) / tied if points[key] == boundary else 0
        for key in trio
    )
    exact = (
        sum(points[key] > boundary for key in trio) == above
        and all(points[key] >= boundary for key in trio)
    )
    return fractional, exact


def build(record, forecast, diagnostics, math_grade):
    for artifact in (record, forecast, math_grade):
        checked(artifact)
    if (
        forecast["sha256"] != record["forecast_sha256"]
        or forecast["model_sha256"] != record["base_model_sha256"]
        or (record["mode"] == "prospective" and forecast["mode"] != "prospective")
        or any(forecast[key] != record[key] for key in ("source_snapshot_sha256", "game_date", "lock_at"))
    ):
        raise ValueError("forecast does not match the frozen math record")
    if any(
        math_grade[key] != record[key]
        for key in ("source_snapshot_sha256", "game_date", "mode", "protocol_sha256", "model_sha256")
    ) or math_grade["record_sha256"] != record["sha256"]:
        raise ValueError("math grade does not match the frozen record")

    # Reuse the original grader's joins and DNP rules for the restricted pool.
    replay = grade(record, diagnostics)
    if any(replay[key] != math_grade[key] for key in ("players", "portfolios")):
        raise ValueError("diagnostics disagree with the supplied math grade")

    rows = forecast["rows"]
    names = {player_id(row): row["player"] for row in rows}
    if len(names) != len(rows) or len(names) != record["modeled_pool_n"] or len(names) != forecast["modeled_pool_n"]:
        raise ValueError("invalid frozen v3 membership")
    restricted = [row["id"] for row in record["players"]]
    ranked = sorted(rows, key=lambda row: (-row["market_line"], row["player_key"], row["event_id"]))
    if restricted != [player_id(row) for row in ranked[:12]] or len(restricted) != record["pool_n"]:
        raise ValueError("restricted membership does not match the original top 12")

    outcomes = {player_id(row): row for row in diagnostics["rows"]}
    schedule = {str(game["event_id"]) for game in record["slate_schedule"]}
    points, statuses = {}, {}
    for key in names:
        row = outcomes.get(key, {})
        statuses[key] = row.get("status", "missing")
        if statuses[key] == "verified":
            value = row["actual"]["points"]
            if str(row["espn_event_id"]) not in schedule or type(value) not in (int, float) or not (0 <= value < float("inf")):
                raise ValueError("invalid verified outcome for frozen eligible player")
            points[key] = value

    full = benchmark(names, points, statuses, names)
    narrow = benchmark(restricted, points, statuses, names)
    full_best, narrow_best = full["best_total"], narrow["best_total"]
    provisional = not math_grade["full_slate_final"] or bool(full["unresolved"])
    portfolios = {}
    for key, result in math_grade["portfolios"].items():
        best = result["best_of_five"]
        entries = record["portfolios"][key]["trios"]
        winning_entry = entries[result["trio_totals"].index(best)] if best is not None else []
        capture, exact = top_three_capture(winning_entry, points)
        portfolios[key] = {
            "best_entry": best,
            "selection_gap": narrow_best - best if narrow_best is not None and best is not None else None,
            "total_gap": full_best - best if full_best is not None and best is not None else None,
            "fractional_top_three": capture,
            "tie_compatible_top_three": exact,
        }

    return signed({
        "version": VERSION,
        "record_sha256": record["sha256"],
        "math_grade_sha256": math_grade["sha256"],
        "forecast_sha256": forecast["sha256"],
        "game_date": record["game_date"],
        "mode": record["mode"],
        "graded_at": math_grade["graded_at"],
        "full_slate_final": math_grade["full_slate_final"],
        "provisional": provisional,
        "full_pool": full,
        "top_12": narrow,
        "pool_gap": full_best - narrow_best if full_best is not None and narrow_best is not None else None,
        "portfolios": portfolios,
    })


def publish(audit, record, forecast):
    if (
        audit["mode"] != "prospective"
        or not aware(forecast["created_at"]) <= aware(record["created_at"]) < aware(record["lock_at"])
        or aware(record["completed_at"]) >= aware(record["lock_at"])
    ):
        raise ValueError("publication requires original pregame records")
    import psycopg2
    from psycopg2.extras import Json
    from config import load_config

    with psycopg2.connect(load_config().database_url) as conn, conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nba_trio_pool_audits (
                grade_sha256 TEXT PRIMARY KEY,
                record_sha256 TEXT NOT NULL REFERENCES nba_trio_math_snapshots(record_sha256),
                audit JSONB NOT NULL
            )
        """)
        cursor.execute("""
            SELECT 1 FROM nba_trio_math_grades
            WHERE record_sha256 = %s AND grade->>'sha256' = %s
        """, (audit["record_sha256"], audit["math_grade_sha256"]))
        if cursor.fetchone() is None:
            raise ValueError("matching published math grade is required")
        cursor.execute("""
            INSERT INTO nba_trio_pool_audits (grade_sha256, record_sha256, audit)
            VALUES (%s, %s, %s)
            ON CONFLICT (grade_sha256) DO NOTHING
        """, (audit["math_grade_sha256"], audit["record_sha256"], Json(audit)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("record", "forecast", "diagnostics", "math-grade", "output"):
        parser.add_argument(f"--{flag}", required=True, type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    record, forecast = read(args.record), read(args.forecast)
    audit = build(record, forecast, read(args.diagnostics), read(args.math_grade))
    write_new(args.output, audit)
    if args.apply:
        publish(audit, record, forecast)
    print(f"{args.output} | pool gap {audit['pool_gap']} | provisional {audit['provisional']}")


if __name__ == "__main__":
    main()
