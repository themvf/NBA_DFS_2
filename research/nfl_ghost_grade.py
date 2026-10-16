"""Tuesday grading for the ghost-lineup factorial (docs/nfl-ghost-factorial-study.md).

    python -m research.nfl_ghost_grade                     # every frozen slate not yet graded
    python -m research.nfl_ghost_grade --season 2026 --report-only
    python -m research.nfl_ghost_grade --dry-run

For each frozen slate it finds the flagship contest that slate fed (stat-api
import, complete standings, same format, at least 90% of the contest's
players present in the frozen pool), scores the house, every arm and every
owner build saved for the same slate with the contest's own player points
(then our weekly results by player id), places them against the score curve,
pays them from the payout table, and stores one row per subject. Then it
writes one report per format; factor effects appear only at the registered
milestone (model/nfl_ghost_grade.py). A slate with no gradable contest is
reported, never dropped.
"""

from __future__ import annotations

import argparse
import sys

from psycopg2.extras import Json

from config import load_config
from db.database import DatabaseManager
from model import nfl_ghost_grade as g
from model.nfl_dfs_field_audit import normalize_name

DESIGN_VERSION = "nfl-ghost-factorial-v1"
MIN_OVERLAP = 0.90


def contest_for(db: DatabaseManager, freeze: dict) -> tuple[dict | None, str]:
    names = {normalize_name(p["name"]) for p in freeze["input_snapshot"]}
    best, reason = None, "no complete flagship import with a payout table after this slate's kickoff"
    for c in db.execute(
            """SELECT contest_id, contest_name, entry_count, score_curve, payout_curve, entry_fee
                 FROM nfl_dfs_field_contests
                WHERE format = %s AND file_name LIKE 'stat-api:%%' AND score_curve IS NOT NULL AND payout_curve IS NOT NULL
                  AND imported_at > %s AND imported_at < %s + interval '10 days'""",
            (freeze["format"], freeze["first_kickoff"], freeze["first_kickoff"])):
        own = [r["normalized_name"] for r in db.execute("SELECT normalized_name FROM nfl_dfs_field_ownership WHERE contest_id=%s", (c["contest_id"],))]
        if not own:
            continue
        overlap = sum(1 for n in own if n in names) / len(own)
        if overlap >= MIN_OVERLAP and (best is None or overlap > best["overlap"] or (overlap == best["overlap"] and c["entry_count"] > best["entries"])):
            best = {"contest_id": c["contest_id"], "name": c["contest_name"], "entries": int(c["entry_count"]),
                    "score_curve": c["score_curve"], "payout_curve": c["payout_curve"], "entry_fee": c["entry_fee"], "overlap": round(overlap, 3)}
    if best is None and db.execute("""SELECT 1 FROM nfl_dfs_field_contests WHERE format=%s AND file_name LIKE 'stat-api:%%'
            AND imported_at > %s AND imported_at < %s + interval '10 days' LIMIT 1""", (freeze["format"], freeze["first_kickoff"], freeze["first_kickoff"])):
        reason = f"contests were imported but none shares {MIN_OVERLAP:.0%} of its players with this slate, or none has a payout table"
    return best, reason


def points_lookup(db: DatabaseManager, contest_id: str, season: int, week: int):
    by_name: dict[str, float] = {}
    for r in db.execute("SELECT normalized_name, fpts FROM nfl_dfs_field_ownership WHERE contest_id=%s AND fpts IS NOT NULL", (contest_id,)):
        by_name[r["normalized_name"]] = float(r["fpts"])
    for r in db.execute("SELECT lineups FROM nfl_dfs_field_user_builds WHERE contest_id=%s", (contest_id,)):
        for lineup in r["lineups"] or []:
            for seat in lineup.get("roster") or []:
                if seat[5] is not None:
                    by_name.setdefault(normalize_name(seat[1]), round(float(seat[5]) / (1.5 if seat[0] == "CPT" else 1), 2))
    by_ff = {int(r["player_id"]): float(r["actual_dk_fpts"]) for r in db.execute(
        """SELECT DISTINCT ON (player_id) player_id, actual_dk_fpts FROM nfl_dfs_player_week_results
            WHERE season=%s AND week=%s AND actual_dk_fpts IS NOT NULL ORDER BY player_id, scoring_version DESC""", (season, week))}
    return by_name, by_ff


def grade_freeze(db: DatabaseManager, freeze: dict, *, dry_run: bool) -> dict:
    contest, reason = contest_for(db, freeze)
    if contest is None:
        return {"freeze_id": str(freeze["freeze_id"]), "graded": 0, "reason": reason}
    by_name, by_ff = points_lookup(db, contest["contest_id"], freeze["season"], freeze["week"])

    def points_from(snapshot_by_id: dict):
        def points_for(slot: dict):
            name = normalize_name(slot.get("name") or "")
            if name in by_name:
                return by_name[name]
            ff = (snapshot_by_id.get(slot.get("dkPlayerId")) or {}).get("ffPlayerId")
            return by_ff.get(int(ff)) if ff is not None else None
        return points_for

    ghost_ids = {p["dkPlayerId"]: p for p in freeze["input_snapshot"]}
    subjects = []
    for a in db.execute("""SELECT a.arm, a.run_id, a.requested, a.levels FROM nfl_dfs_ghost_arms a WHERE a.freeze_id=%s ORDER BY a.arm""", (freeze["freeze_id"],)):
        lineups = [r["slots"] for r in db.execute("SELECT slots FROM nfl_dfs_lineups WHERE run_id=%s ORDER BY lineup_number", (a["run_id"],))]
        subjects.append((f"arm:{a['arm']}", lineups, int(a["requested"]), points_from(ghost_ids), a["levels"]))
    for o in db.execute("""SELECT o.run_id, o.requested_lineups, o.input_snapshot FROM nfl_dfs_optimizer_runs o
            JOIN nfl_dfs_slate_uploads u ON u.upload_id = o.upload_id
           WHERE u.origin = 'upload' AND u.slate_signature = %s AND o.status IN ('complete','partial')
             AND NOT (o.settings ? 'researchStudy') AND o.generated_lineups > 0 AND o.created_at < %s""",
                        (freeze["owner_signature"], freeze["first_kickoff"])):
        lineups = [r["slots"] for r in db.execute("SELECT slots FROM nfl_dfs_lineups WHERE run_id=%s ORDER BY lineup_number", (o["run_id"],))]
        snap = {p["dkPlayerId"]: p for p in (o["input_snapshot"] or [])}
        subjects.append((f"owner:{o['run_id']}", lineups, int(o["requested_lineups"]), points_from(snap), None))
    results = []
    for subject, lineups, requested, points_for, levels in subjects:
        m = g.grade_subject(lineups, requested, points_for, contest)
        results.append({"subject": subject, "levels": levels, **m})
        if not dry_run:
            db.execute(
                """INSERT INTO nfl_dfs_ghost_results (freeze_id, subject, contest_id, grader_version, lineups, scored, top1_hits, top01_hits,
                        cashes, payout, entry_fee, best_score, mean_score, detail)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (freeze_id, subject, contest_id, grader_version) DO UPDATE SET lineups=EXCLUDED.lineups, scored=EXCLUDED.scored,
                     top1_hits=EXCLUDED.top1_hits, top01_hits=EXCLUDED.top01_hits, cashes=EXCLUDED.cashes, payout=EXCLUDED.payout,
                     entry_fee=EXCLUDED.entry_fee, best_score=EXCLUDED.best_score, mean_score=EXCLUDED.mean_score, detail=EXCLUDED.detail, graded_at=NOW()
                   RETURNING freeze_id""",
                (freeze["freeze_id"], subject, contest["contest_id"], g.GRADER_VERSION, m["lineups"], m["scored"], m["top1_hits"],
                 m["top01_hits"], m["cashes"], m["payout"], m["entry_fee"], m["best_score"], m["mean_score"],
                 Json({**m["detail"], "built": m["built"], "contest_overlap": contest["overlap"]})))
    return {"freeze_id": str(freeze["freeze_id"]), "contest": contest["name"], "graded": len(results), "results": results}


def season_report(db: DatabaseManager, season: int, fmt: str, *, dry_run: bool) -> dict:
    rows = db.execute(
        """SELECT r.freeze_id, a.arm, a.levels, r.lineups, r.scored, r.top1_hits, r.payout, r.entry_fee, (r.detail->>'built')::int AS built
             FROM nfl_dfs_ghost_results r
             JOIN nfl_dfs_ghost_freezes f ON f.freeze_id = r.freeze_id
             JOIN nfl_dfs_ghost_arms a ON a.freeze_id = r.freeze_id AND r.subject = 'arm:' || a.arm
            WHERE f.design_version = %s AND f.season = %s AND f.format = %s AND r.grader_version = %s""",
        (DESIGN_VERSION, season, fmt, g.GRADER_VERSION))
    graded = [dict(r) for r in rows]
    house = next((r["levels"] for r in graded if r["arm"] == 0), {})
    rep = g.report(fmt, graded, house)
    if not dry_run:
        db.execute("""INSERT INTO nfl_dfs_ghost_reports (design_version, format, grader_version, slates, milestone, report)
                      VALUES (%s,%s,%s,%s,%s,%s) RETURNING id""",
                   (DESIGN_VERSION, fmt, g.GRADER_VERSION, rep["slates"], "screening" if rep["milestone_reached"] else None, Json(rep)))
    return rep


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--season", type=int, default=None)
    ap.add_argument("--report-only", action="store_true")
    ap.add_argument("--regrade", action="store_true", help="grade frozen slates that already have results")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    if not args.report_only:
        freezes = db.execute(
            """SELECT f.* FROM nfl_dfs_ghost_freezes f
                WHERE f.design_version = %s AND f.first_kickoff < NOW() - interval '6 hours'
                  AND (%s OR NOT EXISTS (SELECT 1 FROM nfl_dfs_ghost_results r WHERE r.freeze_id = f.freeze_id AND r.grader_version = %s))
                ORDER BY f.first_kickoff""", (DESIGN_VERSION, args.regrade, g.GRADER_VERSION))
        for f in freezes:
            out = grade_freeze(db, dict(f), dry_run=args.dry_run)
            line = f"{f['season']} w{f['week']} {f['format']} {f['draft_group_id']}: "
            if not out["graded"]:
                print(line + f"NOT GRADED - {out['reason']}")
                continue
            house = next((r for r in out["results"] if r["subject"] == "arm:0"), None)
            arms = [r for r in out["results"] if r["subject"].startswith("arm:")]
            owners = [r for r in out["results"] if r["subject"].startswith("owner:")]
            print(line + f"{out['contest']}: {len(arms)} arms, {len(owners)} owner build(s); house top-1% {house['top1_hits'] if house else '-'}"
                  f"/{house['scored'] if house else '-'}, payout {house['payout'] if house else '-'}")
    seasons = [args.season] if args.season else sorted({int(r["season"]) for r in db.execute(
        "SELECT DISTINCT season FROM nfl_dfs_ghost_freezes WHERE design_version=%s", (DESIGN_VERSION,))})
    for season in seasons:
        for fmt in ("classic", "showdown"):
            rep = season_report(db, season, fmt, dry_run=args.dry_run)
            print(f"{season} {fmt}: {rep['slates']} graded slate(s); " + (rep.get("note") or "screening computed"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
