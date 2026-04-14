"""Ingest DraftKings results → actual_fpts + actual_own_pct in dk_players.

Supports two file formats:

1. DK Salary-style results CSV (simple):
   Columns: Position, Name+ID, Name, ID, ..., Salary, ..., FPTS
   Usage: python -m ingest.dk_results --results DKResults_3_24_2026.csv

2. DK Contest Standings CSV (richer — includes actual ownership):
   Columns: Rank, EntryId, ..., Lineup, , Player, Roster Position, %Drafted, FPTS
   Usage: python -m ingest.dk_results --results contest-standings-189023744.csv

The standings format is preferred — it includes actual %Drafted for ownership
model calibration.

After updating actuals, prints:
  - FPTS accuracy (MAE + bias for our_proj vs linestar_proj vs actual)
  - Ownership accuracy (MAE, bias, correlation)
  - Strategy comparison (avg proj vs avg actual per lineup strategy)
  - Optimizer feature comparison for MLB durable jobs (if present)
  - Value misses (high FPTS + low proj ownership that we missed entirely)
"""

from __future__ import annotations

import argparse
import csv
import io
import logging

from rapidfuzz import fuzz, process

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)


def detect_format(content: str) -> str:
    """Return 'standings' or 'results' based on CSV header."""
    first_line = content.split("\n")[0]
    if "EntryId" in first_line or "%Drafted" in first_line:
        return "standings"
    return "results"


def parse_contest_standings_csv(content: str) -> list[dict]:
    """Parse DK post-contest standings CSV.

    The right-side columns (8-10) contain per-player data sorted by ownership:
      Player | Roster Position | %Drafted | FPTS
    """
    reader  = csv.reader(io.StringIO(content))
    next(reader)
    players: dict[str, dict] = {}
    for row in reader:
        if len(row) < 11:
            continue
        name    = row[7].strip()
        own_str = row[9].strip().replace("%", "")
        fpts_str = row[10].strip()
        if not name:
            continue
        try:
            actual_fpts    = float(fpts_str)
            actual_own_pct = float(own_str)
            if name not in players:
                players[name] = {
                    "name":           name,
                    "actual_fpts":    actual_fpts,
                    "actual_own_pct": actual_own_pct,
                }
        except ValueError:
            pass
    return list(players.values())


def parse_dk_results_csv(content: str) -> list[dict]:
    """Parse DK results CSV.

    Handles FPTS column name variants: FPTS / Total Points / ActualFpts.
    """
    reader  = csv.DictReader(io.StringIO(content))
    players = []
    for row in reader:
        name = (row.get("Name") or "").strip()
        if not name:
            continue
        salary_str = (row.get("Salary") or "0").replace("$", "").replace(",", "").strip()
        salary = int(salary_str) if salary_str.isdigit() else 0
        fpts_str = (
            row.get("FPTS") or row.get("Total Points") or
            row.get("ActualFpts") or row.get("Actual FPTS") or "0"
        ).strip()
        try:
            actual_fpts = float(fpts_str) if fpts_str else None
        except ValueError:
            actual_fpts = None
        if actual_fpts is not None:
            players.append({"name": name, "salary": salary, "actual_fpts": actual_fpts})
    return players


def run(results_path: str, slate_date: str | None = None) -> None:
    config = load_config()
    db     = DatabaseManager(config.database_url)

    with open(results_path, encoding="utf-8-sig") as f:
        content = f.read()

    fmt = detect_format(content)
    if fmt == "standings":
        result_players = parse_contest_standings_csv(content)
        print("Detected format: contest standings (includes actual ownership)")
    else:
        result_players = parse_dk_results_csv(content)
        print("Detected format: results CSV")

    if not result_players:
        print("ERROR: No players with FPTS found.")
        return
    print(f"Parsed {len(result_players)} players from results CSV")

    # Target slate
    if slate_date:
        slate = db.execute_one("SELECT id, slate_date FROM dk_slates WHERE slate_date = %s", (slate_date,))
    else:
        slate = db.execute_one("SELECT id, slate_date FROM dk_slates ORDER BY slate_date DESC LIMIT 1")

    if not slate:
        print("ERROR: No slate found. Run ingest.dk_slate first.")
        return
    slate_id = slate["id"]
    print(f"Targeting slate {slate_id} ({slate['slate_date']})")

    pool       = db.execute("SELECT id, name, salary FROM dk_players WHERE slate_id = %s", (slate_id,))
    pool_names = [p["name"] for p in pool]

    updated   = 0
    unmatched = []

    for result_p in result_players:
        own_pct  = result_p.get("actual_own_pct")
        r_salary = result_p.get("salary")

        exact = next(
            (p for p in pool if p["name"] == result_p["name"]
             and (r_salary is None or p["salary"] == r_salary)),
            None,
        )
        if exact:
            db.execute(
                "UPDATE dk_players SET actual_fpts = %s, actual_own_pct = %s WHERE id = %s",
                (result_p["actual_fpts"], own_pct, exact["id"]),
            )
            updated += 1
            continue

        has_salary    = result_p.get("salary") is not None
        same_salary   = [p for p in pool if p["salary"] == result_p.get("salary")] if has_salary else []
        candidates    = same_salary if same_salary else pool
        cand_names    = [p["name"] for p in candidates]

        match = process.extractOne(
            result_p["name"], cand_names,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=80,
        )
        if match:
            player = candidates[cand_names.index(match[0])]
            db.execute(
                "UPDATE dk_players SET actual_fpts = %s, actual_own_pct = %s WHERE id = %s",
                (result_p["actual_fpts"], own_pct, player["id"]),
            )
            updated += 1
        else:
            unmatched.append(result_p["name"])

    n = len(result_players)
    print(f"Updated: {updated}/{n} ({100 * updated // n if n else 0}%)")
    if unmatched:
        print(f"Unmatched ({len(unmatched)}): {', '.join(unmatched[:10])}")

    # FPTS accuracy
    stats = db.execute_one(
        """
        SELECT
            COUNT(*) FILTER (WHERE actual_fpts IS NOT NULL AND our_proj IS NOT NULL) AS n_our,
            AVG(ABS(our_proj - actual_fpts)) FILTER (WHERE actual_fpts IS NOT NULL AND our_proj IS NOT NULL) AS our_mae,
            AVG(our_proj - actual_fpts) FILTER (WHERE actual_fpts IS NOT NULL AND our_proj IS NOT NULL) AS our_bias,
            COUNT(*) FILTER (WHERE actual_fpts IS NOT NULL AND linestar_proj IS NOT NULL) AS n_ls,
            AVG(ABS(linestar_proj - actual_fpts)) FILTER (WHERE actual_fpts IS NOT NULL AND linestar_proj IS NOT NULL) AS ls_mae,
            AVG(linestar_proj - actual_fpts) FILTER (WHERE actual_fpts IS NOT NULL AND linestar_proj IS NOT NULL) AS ls_bias
        FROM dk_players WHERE slate_id = %s
        """,
        (slate_id,),
    )
    if stats and stats["n_our"]:
        print(f"\n-- FPTS Accuracy (n={stats['n_our']}) --------")
        print(f"  Our model  -- MAE: {stats['our_mae']:.2f}  Bias: {stats['our_bias']:+.2f}")
        if stats["n_ls"]:
            print(f"  LineStar   -- MAE: {stats['ls_mae']:.2f}  Bias: {stats['ls_bias']:+.2f}")
            winner = "Our model" if stats["our_mae"] < stats["ls_mae"] else "LineStar"
            print(f"  Winner: {winner} by {abs(stats['our_mae'] - stats['ls_mae']):.2f} pts/player")

    # Ownership accuracy
    own_stats = db.execute_one(
        """
        SELECT
            COUNT(*) FILTER (WHERE actual_own_pct IS NOT NULL AND proj_own_pct IS NOT NULL) AS n,
            AVG(ABS(proj_own_pct - actual_own_pct)) FILTER (WHERE actual_own_pct IS NOT NULL AND proj_own_pct IS NOT NULL) AS mae,
            AVG(proj_own_pct - actual_own_pct) FILTER (WHERE actual_own_pct IS NOT NULL AND proj_own_pct IS NOT NULL) AS bias,
            CORR(proj_own_pct, actual_own_pct) FILTER (WHERE actual_own_pct IS NOT NULL AND proj_own_pct IS NOT NULL) AS corr
        FROM dk_players WHERE slate_id = %s
        """,
        (slate_id,),
    )
    if own_stats and own_stats["n"]:
        print(f"\n-- Ownership Accuracy (n={own_stats['n']}) ---")
        print(f"  MAE:  {own_stats['mae']:.2f}%  Bias: {own_stats['bias']:+.2f}%")
        print(f"  Corr: {own_stats['corr']:.3f}  (1.0 = perfect rank-order)")

    update_lineup_actuals(db, slate_id)
    update_optimizer_job_lineup_actuals(db, slate_id)
    print_optimizer_feature_comparison(db)
    print_value_misses(db, slate_id)

    # Fetch per-stat actual lines (pts/reb/ast/stl/blk/tov/3pm) from NBA Stats API
    try:
        from ingest.nba_schedule import fetch_player_stats
        fetch_player_stats(db, slate["slate_date"])
    except Exception as exc:
        print(f"Player stat fetch skipped: {exc}")


def print_value_misses(db, slate_id: int, fpts_threshold: float = 30.0, own_threshold: float = 15.0) -> None:
    """Report high-scoring low-owned players we missed entirely.

    Flags players where actual_fpts >= threshold AND proj_own_pct < threshold
    AND not in ANY of our saved dk_lineups.
    """
    lineup_rows = db.execute("SELECT player_ids FROM dk_lineups WHERE slate_id = %s", (slate_id,))
    if not lineup_rows:
        return

    our_ids: set[int] = set()
    for row in lineup_rows:
        for pid_str in (row["player_ids"] or "").split(","):
            if pid_str.strip().isdigit():
                our_ids.add(int(pid_str.strip()))

    misses = db.execute(
        """
        SELECT name, team_abbrev, salary, our_proj, linestar_proj,
               proj_own_pct, actual_fpts, actual_own_pct, our_leverage, id
        FROM dk_players
        WHERE slate_id = %s
          AND actual_fpts >= %s
          AND (proj_own_pct IS NULL OR proj_own_pct < %s)
          AND actual_fpts IS NOT NULL
        ORDER BY actual_fpts DESC
        """,
        (slate_id, fpts_threshold, own_threshold),
    )
    if not misses:
        print(f"\n-- Value Misses: none (no player scored ≥{fpts_threshold} with <{own_threshold}% proj own)")
        return

    missed   = [p for p in misses if p["id"] not in our_ids]
    captured = [p for p in misses if p["id"] in our_ids]

    print(f"\n-- Value Misses (actual ≥{fpts_threshold:.0f} FPTS, proj own <{own_threshold:.0f}%) --")
    print(f"  {len(captured)} captured in our lineups, {len(missed)} missed entirely\n")

    if missed:
        print(f"  {'Name':<22} {'Team':>5}  {'Sal':>6}  {'OurProj':>8}  {'LSProj':>8}  {'ProjOwn':>8}  {'ActOwn':>7}  {'ActFPTS':>8}  {'Leverage':>9}")
        print("  " + "-" * 95)
        for p in missed:
            our_proj_s = f"{p['our_proj']:.1f}"     if p["our_proj"]     is not None else "  N/A"
            ls_proj_s  = f"{p['linestar_proj']:.1f}" if p["linestar_proj"] is not None else "  N/A"
            proj_own_s = f"{p['proj_own_pct']:.1f}%" if p["proj_own_pct"] is not None else "  N/A"
            act_own_s  = f"{p['actual_own_pct']:.1f}%" if p["actual_own_pct"] is not None else "  N/A"
            lev_s      = f"{p['our_leverage']:.2f}"  if p["our_leverage"] is not None else "  N/A"
            print(
                f"  {p['name']:<22} {p['team_abbrev']:>5}  ${p['salary']:>5}  "
                f"{our_proj_s:>8}  {ls_proj_s:>8}  {proj_own_s:>8}  {act_own_s:>7}  "
                f"{p['actual_fpts']:>8.1f}  {lev_s:>9}"
            )
    if captured:
        print(f"\n  Captured: {', '.join(p['name'] for p in captured)}")


def update_lineup_actuals(db, slate_id: int) -> None:
    """Sum actual_fpts for each saved lineup and update dk_lineups.actual_fpts."""
    lineups = db.execute("SELECT id, player_ids FROM dk_lineups WHERE slate_id = %s", (slate_id,))
    if not lineups:
        return

    updated = 0
    for lineup in lineups:
        ids = [int(x) for x in lineup["player_ids"].split(",") if x.strip()]
        if not ids:
            continue
        placeholders = ",".join(["%s"] * len(ids))
        result = db.execute_one(
            f"SELECT SUM(actual_fpts) AS total, "
            f"COUNT(*) FILTER (WHERE actual_fpts IS NOT NULL) AS actual_count "
            f"FROM dk_players WHERE id IN ({placeholders})",
            ids,
        )
        if result and result["total"] is not None and result["actual_count"] == len(ids):
            db.execute(
                "UPDATE dk_lineups SET actual_fpts = %s WHERE id = %s",
                (result["total"], lineup["id"]),
            )
            updated += 1
        else:
            db.execute(
                "UPDATE dk_lineups SET actual_fpts = NULL WHERE id = %s",
                (lineup["id"],),
            )

    print(f"Lineup actuals updated: {updated}/{len(lineups)}")

    comparison = db.execute(
        """
        SELECT strategy, COUNT(*) AS n, AVG(proj_fpts) AS avg_proj, AVG(actual_fpts) AS avg_actual
        FROM dk_lineups
        WHERE slate_id = %s AND actual_fpts IS NOT NULL
        GROUP BY strategy ORDER BY avg_actual DESC NULLS LAST
        """,
        (slate_id,),
    )
    if comparison:
        print(f"\n-- Strategy Comparison (slate_id={slate_id}) --")
        print(f"  {'Strategy':<12}  {'N':>4}  {'AvgProj':>8}  {'AvgActual':>10}")
        print("  " + "-" * 38)
        for row in comparison:
            avg_actual = f"{row['avg_actual']:.1f}" if row["avg_actual"] else "pending"
            print(f"  {row['strategy']:<12}  {row['n']:>4}  {row['avg_proj']:>8.1f}  {avg_actual:>10}")


def update_optimizer_job_lineup_actuals(db, slate_id: int) -> None:
    """Update durable optimizer job lineups with summed actual FPTS for a slate."""
    db.execute(
        """
        WITH lineup_totals AS (
            SELECT
                ojl.id AS lineup_id,
                COUNT(pid.player_id_text)::int AS player_count,
                COUNT(dp.actual_fpts)::int AS actual_count,
                SUM(dp.actual_fpts) AS total_fpts
            FROM optimizer_job_lineups ojl
            JOIN optimizer_jobs oj
              ON oj.id = ojl.job_id
            LEFT JOIN LATERAL jsonb_array_elements_text(ojl.player_ids_json) AS pid(player_id_text)
              ON TRUE
            LEFT JOIN dk_players dp
              ON dp.id = pid.player_id_text::INTEGER
             AND dp.slate_id = oj.slate_id
            WHERE oj.slate_id = %s
            GROUP BY ojl.id
        )
        UPDATE optimizer_job_lineups ojl
        SET actual_fpts = CASE
            WHEN lt.player_count > 0 AND lt.actual_count = lt.player_count THEN lt.total_fpts
            ELSE NULL
        END
        FROM lineup_totals lt
        WHERE ojl.id = lt.lineup_id
        """,
        (slate_id,),
    )

    counts = db.execute_one(
        """
        SELECT
            COUNT(*)::int AS total,
            COUNT(*) FILTER (WHERE ojl.actual_fpts IS NOT NULL)::int AS updated
        FROM optimizer_job_lineups ojl
        JOIN optimizer_jobs oj
          ON oj.id = ojl.job_id
        WHERE oj.slate_id = %s
        """,
        (slate_id,),
    )
    if counts and counts["total"]:
        print(f"Optimizer lineup actuals updated: {counts['updated']}/{counts['total']}")


def print_optimizer_feature_comparison(db) -> None:
    """Summarize MLB optimizer lineup results by feature toggle settings."""
    rows = db.execute(
        """
        WITH base AS (
            SELECT
                COALESCE((oj.settings_json ->> 'hrCorrelation')::boolean, false) AS hr_correlation,
                CASE
                    WHEN COALESCE((oj.settings_json ->> 'hrCorrelation')::boolean, false)
                    THEN (oj.settings_json ->> 'hrCorrelationThreshold')::double precision
                    ELSE NULL::double precision
                END AS hr_threshold,
                COALESCE((oj.settings_json ->> 'pitcherCeilingBoost')::boolean, false) AS pitcher_ceiling_boost,
                CASE
                    WHEN COALESCE((oj.settings_json ->> 'pitcherCeilingBoost')::boolean, false)
                    THEN (oj.settings_json ->> 'pitcherCeilingCount')::integer
                    ELSE NULL::integer
                END AS pitcher_ceiling_count,
                COALESCE(
                    (oj.effective_settings_json ->> 'antiCorrMax')::integer,
                    (oj.settings_json ->> 'antiCorrMax')::integer,
                    10
                ) AS effective_anti_corr_max,
                ojl.proj_fpts,
                ojl.actual_fpts
            FROM optimizer_job_lineups ojl
            JOIN optimizer_jobs oj
              ON oj.id = ojl.job_id
            WHERE oj.sport = 'mlb'
              AND ojl.actual_fpts IS NOT NULL
        )
        SELECT
            hr_correlation,
            hr_threshold,
            pitcher_ceiling_boost,
            pitcher_ceiling_count,
            effective_anti_corr_max,
            COUNT(*)::int AS n,
            AVG(proj_fpts) AS avg_proj,
            AVG(actual_fpts) AS avg_actual,
            AVG(actual_fpts - proj_fpts) AS avg_beat
        FROM base
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY hr_correlation DESC, pitcher_ceiling_boost DESC, effective_anti_corr_max ASC, hr_threshold NULLS FIRST
        """
    )
    if not rows:
        return

    print("\n-- MLB Optimizer Feature Comparison --")
    print(f"  {'HR Corr':<8} {'HR Thr':>6}  {'PitchCeil':<10} {'TopN':>4}  {'Anti':>4}  {'N':>4}  {'AvgProj':>8}  {'AvgActual':>10}  {'AvgBeat':>8}")
    print("  " + "-" * 82)
    for row in rows:
        hr_thr = f"{row['hr_threshold']:.2f}" if row["hr_threshold"] is not None else "  --"
        pitch_n = f"{row['pitcher_ceiling_count']:>4}" if row["pitcher_ceiling_count"] is not None else "  --"
        print(
            f"  {('on' if row['hr_correlation'] else 'off'):<8} {hr_thr:>6}  "
            f"{('on' if row['pitcher_ceiling_boost'] else 'off'):<10} {pitch_n}  "
            f"{row['effective_anti_corr_max']:>4}  {row['n']:>4}  "
            f"{row['avg_proj']:>8.1f}  {row['avg_actual']:>10.1f}  {row['avg_beat']:>8.2f}"
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Ingest DK results CSV → actual_fpts")
    parser.add_argument("--results",    required=True, help="Path to DK results CSV")
    parser.add_argument("--slate-date", help="Slate date (YYYY-MM-DD), defaults to most recent")
    args = parser.parse_args()
    run(args.results, args.slate_date)
