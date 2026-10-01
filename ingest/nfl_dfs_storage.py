"""NFL DFS storage maintenance: deduplicate, retain, compact.

Why this exists (measured 2026-09-30): the database reached 10 GB and was
growing ~1 GB a day. 1.6 GB of the 2.0 GB `nfl_dfs_player_projections` table
was one game-level matchup evidence object copied onto every player row of
every projection run (~70 players a game, ~30 runs a day), and the weekly
report card wrote a ~7 MB row on every research run even when nothing
changed. The projection writer now stores evidence once
(`nfl_dfs_matchup_evidence`, #327); this module repairs what was already
written and keeps growth bounded. Approved by the owner on 2026-09-30.

Three passes, each a dry run unless `--apply`:

  evidence    Move embedded `feature_snapshot.matchup.evidence` into
              `nfl_dfs_matchup_evidence`, leaving the digest and the small
              shadow result on each row. Nothing is lost: every row can still
              be joined to its exact evidence.
  retention   For COMPLETED weeks only (every regular-season game final and
              the last kickoff more than RETENTION_GRACE ago), delete
              projection runs nothing can read. A run is kept when any of:
                - a column names it (slate uploads, optimizer runs, prelock
                  manifests, matchup forecast/report baselines, specials runs);
                - its id appears in any NFL table's JSON, or in a tracked
                  file under docs/, artifacts/ or research/ (study pins);
                - it supplies the forecast the weekly report card grades for
                  some player (last capture before kickoff), so re-running a
                  report card gives the same answer;
                - it is the last run before some game's kickoff, or the
                  week's last run.
  reportcards Keep the newest REPORT_CARDS_KEPT report cards per week. Every
              reader (web, review digest) uses only the newest.

`--vacuum-full` rewrites the two tables afterwards so the freed space leaves
the database. It takes an exclusive lock for the duration; run it by hand at
a quiet time, never from a scheduled job.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

from psycopg2.extras import Json, execute_values

from config import load_config
from ingest.nfl_dfs_weekly import PipelineDatabase, target_season
from model.nfl_dfs_historical import artifact_digest

ROOT = Path(__file__).resolve().parents[1]
UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
RETENTION_GRACE = timedelta(days=2)
REPORT_CARDS_KEPT = 2
REFERENCE_COLUMNS = (
    ("nfl_dfs_slate_uploads", "projection_run_id"),
    ("nfl_dfs_optimizer_runs", "projection_run_id"),
    ("nfl_availability_prelock_manifests", "projection_run_id"),
    ("nfl_matchup_forecast_runs", "baseline_run_id"),
    ("nfl_matchup_research_reports", "baseline_run_id"),
    ("nfl_specials_runs", "projection_run_id"),
)


def _rows(db, sql, params=()):
    return [dict(r) for r in db.execute(sql, params)]


# ── evidence ─────────────────────────────────────────────────────────────
def migrate_evidence(db, apply: bool) -> dict:
    runs = [r["run_id"] for r in _rows(db, """SELECT DISTINCT run_id::text run_id FROM nfl_dfs_player_projections
        WHERE feature_snapshot->'matchup' ? 'evidence'""")]
    moved = 0
    for run_id in runs:
        distinct = _rows(db, """SELECT md5((feature_snapshot->'matchup'->'evidence')::text) m5,
                (array_agg(feature_snapshot->'matchup'->'evidence'))[1] evidence, count(*) n
            FROM nfl_dfs_player_projections WHERE run_id=%s::uuid AND feature_snapshot->'matchup' ? 'evidence'
            GROUP BY 1""", (run_id,))
        mapping = [(d["m5"], artifact_digest(d["evidence"]), d["evidence"]) for d in distinct]
        moved += sum(d["n"] for d in distinct)
        if not apply:
            continue
        with db.connect() as conn:
            cur = conn.cursor()
            execute_values(cur, """INSERT INTO nfl_dfs_matchup_evidence (evidence_digest,evidence)
                VALUES %s ON CONFLICT (evidence_digest) DO NOTHING""", [(dg, Json(ev)) for _, dg, ev in mapping])
            cur.execute("""UPDATE nfl_dfs_player_projections p SET feature_snapshot =
                    jsonb_set(p.feature_snapshot, '{matchup}', jsonb_build_object(
                        'evidence_digest', m.digest, 'shadow', p.feature_snapshot->'matchup'->'shadow'))
                FROM jsonb_to_recordset(%s) m(m5 text, digest text)
                WHERE p.run_id=%s::uuid AND p.feature_snapshot->'matchup' ? 'evidence'
                  AND md5((p.feature_snapshot->'matchup'->'evidence')::text)=m.m5""",
                (Json([{"m5": m5, "digest": dg} for m5, dg, _ in mapping]), run_id))
            cur.execute("""SELECT count(*) n FROM nfl_dfs_player_projections
                WHERE run_id=%s::uuid AND feature_snapshot->'matchup' ? 'evidence'""", (run_id,))
            left = cur.fetchone()["n"]
            if left:
                raise RuntimeError(f"run {run_id}: {left} rows still embed evidence after the update")
    return {"runs": len(runs), "rows": moved, "applied": apply}


# ── retention ────────────────────────────────────────────────────────────
def repo_uuids(root: Path = ROOT) -> set[str]:
    """Run ids pinned by text in tracked docs, artifacts and research files."""
    files = subprocess.run(["git", "ls-files", "docs", "artifacts", "research"], cwd=root,
                           capture_output=True, text=True, check=True).stdout.split("\n")
    found: set[str] = set()
    for name in filter(None, files):
        try:
            found.update(UUID_RE.findall((root / name).read_text(encoding="utf-8", errors="ignore").lower()))
        except OSError:
            continue
    return found


def json_uuids(db) -> set[str]:
    """Run ids named anywhere in NFL tables' JSON, except the projection table itself."""
    columns = _rows(db, """SELECT table_name, column_name FROM information_schema.columns
        WHERE table_schema='public' AND data_type='jsonb' AND table_name LIKE 'nfl%%'
          AND table_name NOT IN ('nfl_dfs_player_projections','nfl_dfs_matchup_evidence')""")
    run_ids = {r["run_id"] for r in _rows(db, "SELECT run_id::text run_id FROM nfl_dfs_projection_runs")}
    found: set[str] = set()
    for col in columns:
        for row in _rows(db, f"""SELECT DISTINCT m[1] id FROM "{col['table_name']}",
                regexp_matches("{col['column_name']}"::text, %s, 'g') m""", (UUID_RE.pattern,)):
            if row["id"] in run_ids:
                found.add(row["id"])
    return found


def completed_weeks(db, season: int, now: datetime) -> list[int]:
    return [r["week"] for r in _rows(db, """SELECT week FROM nfl_season_games
        WHERE season=%s AND game_type='REG' GROUP BY week
        HAVING bool_and(completed) AND max(kickoff) < %s ORDER BY week""", (season, now - RETENTION_GRACE))]


def week_keep_set(db, season: int, week: int) -> tuple[list[str], set[str]]:
    runs = [r["run_id"] for r in _rows(db, """SELECT run_id::text run_id FROM nfl_dfs_projection_runs
        WHERE season=%s AND week=%s""", (season, week))]
    keep: set[str] = set()
    # Report-card picks: same rule as ingest.nfl_dfs_reportcard.inputs.
    keep |= {r["run_id"] for r in _rows(db, """WITH g AS (SELECT g.id game_id,g.kickoff,h.abbreviation home_team,a.abbreviation away_team
            FROM nfl_season_games g JOIN nfl_teams h ON h.team_id=g.home_team_id JOIN nfl_teams a ON a.team_id=g.away_team_id
            WHERE g.season=%(s)s AND g.week=%(w)s AND g.game_type='REG'),
        f AS (SELECT p.id,p.run_id,p.player_id,p.team,GREATEST(p.created_at,r.created_at,r.as_of_at) captured_at
            FROM nfl_dfs_player_projections p JOIN nfl_dfs_projection_runs r ON r.run_id=p.run_id
            WHERE r.season=%(s)s AND r.week=%(w)s AND p.player_id IS NOT NULL),
        m AS (SELECT f.*,g.game_id,g.kickoff FROM f JOIN LATERAL (
            SELECT * FROM g WHERE f.team IN (g.home_team,g.away_team) ORDER BY g.kickoff,g.game_id LIMIT 1) g ON TRUE)
        SELECT DISTINCT ON (player_id,game_id) run_id::text run_id FROM m WHERE captured_at < kickoff
        ORDER BY player_id,game_id,captured_at DESC,id::text COLLATE "C" DESC""", {"s": season, "w": week})}
    # Last run before each kickoff, and the week's last run.
    keep |= {r["run_id"] for r in _rows(db, """SELECT DISTINCT ON (g.id) r.run_id::text run_id
        FROM nfl_season_games g JOIN nfl_dfs_projection_runs r ON r.season=g.season AND r.week=g.week
            AND r.created_at < g.kickoff
        WHERE g.season=%s AND g.week=%s AND g.game_type='REG' ORDER BY g.id, r.created_at DESC""", (season, week))}
    keep |= {r["run_id"] for r in _rows(db, """SELECT run_id::text run_id FROM nfl_dfs_projection_runs
        WHERE season=%s AND week=%s ORDER BY created_at DESC LIMIT 1""", (season, week))}
    return runs, keep


def retention(db, season: int, now: datetime, apply: bool, pinned: set[str]) -> dict:
    referenced: set[str] = set()
    for table, column in REFERENCE_COLUMNS:
        if _rows(db, "SELECT to_regclass(%s)::text t", (table,))[0]["t"]:
            referenced |= {r["id"] for r in _rows(db, f'SELECT DISTINCT "{column}"::text id FROM "{table}" WHERE "{column}" IS NOT NULL')}
    protected = referenced | pinned | json_uuids(db)
    report = {"protected_total": len(protected), "weeks": {}, "applied": apply}
    for week in completed_weeks(db, season, now):
        runs, keep = week_keep_set(db, season, week)
        keep |= protected & set(runs)
        drop = sorted(set(runs) - keep)
        report["weeks"][week] = {"runs": len(runs), "kept": len(runs) - len(drop), "deleted": len(drop)}
        if apply and drop:
            with db.connect() as conn:
                cur = conn.cursor()
                # CASCADE removes their player rows. Any other referencing row
                # was protected above; a missed one makes this raise, not orphan.
                cur.execute("DELETE FROM nfl_dfs_projection_runs WHERE run_id = ANY(%s::uuid[])", (drop,))
                if cur.rowcount != len(drop):
                    raise RuntimeError(f"week {week}: deleted {cur.rowcount} of {len(drop)} runs")
    return report


# ── report cards ─────────────────────────────────────────────────────────
def prune_report_cards(db, season: int, apply: bool) -> dict:
    stale = _rows(db, """SELECT report_digest FROM (SELECT report_digest,
            row_number() OVER (PARTITION BY week ORDER BY created_at DESC, report_digest DESC) n
        FROM nfl_dfs_weekly_report_cards WHERE season=%s) x WHERE n > %s""", (season, REPORT_CARDS_KEPT))
    if apply and stale:
        with db.connect() as conn:
            conn.cursor().execute("DELETE FROM nfl_dfs_weekly_report_cards WHERE report_digest = ANY(%s)",
                                  ([r["report_digest"] for r in stale],))
    return {"deleted": len(stale), "kept_per_week": REPORT_CARDS_KEPT, "applied": apply}


def sizes(db) -> dict:
    out = {r["t"]: r["size"] for r in _rows(db, """SELECT relname t, pg_size_pretty(pg_total_relation_size(oid)) size
        FROM pg_class WHERE relname IN ('nfl_dfs_player_projections','nfl_dfs_weekly_report_cards',
            'nfl_dfs_matchup_evidence')""")}
    out["database"] = _rows(db, "SELECT pg_size_pretty(pg_database_size(current_database())) s")[0]["s"]
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("passes", nargs="*", choices=["evidence", "retention", "reportcards"],
                        help="default: all three")
    parser.add_argument("--season", type=int)
    parser.add_argument("--apply", action="store_true", help="write; without it every pass only reports")
    parser.add_argument("--vacuum-full", action="store_true", help="rewrite the tables afterwards (exclusive lock)")
    args = parser.parse_args()
    args.passes = args.passes or ["evidence", "retention", "reportcards"]
    now = datetime.now(timezone.utc)
    season = target_season(args.season, now)
    db = PipelineDatabase(load_config().database_url, initialize_schema=args.apply)
    out = {"season": season, "before": sizes(db)}
    if "evidence" in args.passes:
        out["evidence"] = migrate_evidence(db, args.apply)
    if "retention" in args.passes:
        out["retention"] = retention(db, season, now, args.apply, repo_uuids())
    if "reportcards" in args.passes:
        out["reportcards"] = prune_report_cards(db, season, args.apply)
    if args.vacuum_full and args.apply:
        with db.connect() as conn:
            conn.autocommit = True
            for table in ("nfl_dfs_player_projections", "nfl_dfs_weekly_report_cards"):
                conn.cursor().execute(f"VACUUM (FULL, ANALYZE) {table}")
    out["after"] = sizes(db)
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
