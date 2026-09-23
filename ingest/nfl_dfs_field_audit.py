"""Import a DraftKings contest standings export and audit it against our board.

The export is the only read we ever get on what the field believed, and it
arrives after the slate is over. Too late to pick a lineup with; exactly in
time to show where our information was behind the market.

Ownership is persisted because it is the durable asset -- each contest file is
one more labelled slate, and the question this answers ("are the players the
field ignores our blind spots or our edges?") cannot be settled in one week.
The audit itself is recomputed on demand from that store, so a threshold change
never rewrites history.

Usage:
    python -m ingest.nfl_dfs_field_audit --contest path/to/contest-standings-NNN.csv
    python -m ingest.nfl_dfs_field_audit --contest FILE --upload <slate-upload-id>
    python -m ingest.nfl_dfs_field_audit --report          # every imported contest
    python -m ingest.nfl_dfs_field_audit --report --season 2026 --week 2
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import sys
from pathlib import Path

from psycopg2.extras import Json, execute_values

from config import load_config
from ingest.nfl_dfs_weekly import PipelineDatabase
from model.nfl_dfs_field_audit import (
    VERSION, audit_slate, is_showdown, normalize_name, parse_contest_export, pooled_summary,
)

# A standings lineup cell can be long; the default field limit rejects them.
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

CONTEST_ID_PATTERN = "contest-standings-"


def contest_id_from_path(path: Path) -> str:
    stem = path.stem
    if CONTEST_ID_PATTERN in stem:
        return stem.split(CONTEST_ID_PATTERN, 1)[1]
    return stem


def read_export(path: Path) -> tuple[dict, str]:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        next(reader, None)                      # header
        parsed = parse_contest_export(reader)
    return parsed, digest


def resolve_upload(db: PipelineDatabase, parsed: dict, fmt: str, upload_id: str | None) -> dict | None:
    """Find the slate this contest was played on.

    Matched by overlap between the contest's player set and each upload's, not
    by date: two slates on one day (a main slate and a showdown) share a date
    but share almost no players. An ambiguous match returns nothing rather than
    guessing -- attributing ownership to the wrong board would poison the audit
    silently.
    """
    if upload_id:
        rows = db.execute(
            """SELECT u.upload_id, u.format, r.season, r.week FROM nfl_dfs_slate_uploads u
               JOIN nfl_dfs_projection_runs r ON r.run_id = u.projection_run_id
               WHERE u.upload_id = %s""", (upload_id,))
        return dict(rows[0]) if rows else None

    names = set(parsed["players"])
    candidates = []
    for row in db.execute(
        """SELECT u.upload_id, u.format, u.created_at, r.season, r.week,
                  array_agg(p.normalized_name) AS names
           FROM nfl_dfs_slate_uploads u
           JOIN nfl_dfs_projection_runs r ON r.run_id = u.projection_run_id
           JOIN nfl_dfs_slate_players p ON p.upload_id = u.upload_id
           WHERE u.format = %s GROUP BY 1,2,3,4,5""", (fmt,)):
        slate = {normalize_name(n) for n in (row["names"] or [])}
        if not slate:
            continue
        overlap = len(names & slate) / max(1, len(names))
        if overlap >= 0.80:
            candidate = dict(row)
            candidate.pop("names", None)
            candidate["overlap"] = round(overlap, 3)
            candidates.append(candidate)
    if not candidates:
        return None
    # The same slate is often uploaded several times as news lands, and the
    # re-uploads differ by a player or two -- not enough for overlap to
    # separate them. The LAST upload is the board we actually built from, so
    # overlap picks the slate and recency picks which build of it.
    return max(candidates, key=lambda c: (round(c["overlap"], 2), c["created_at"]))


def persist(db: PipelineDatabase, contest_id: str, parsed: dict, fmt: str,
            path: Path, digest: str, upload: dict | None) -> None:
    with db.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO nfl_dfs_field_contests
                     (contest_id, contest_name, format, season, week, slate_upload_id,
                      entry_count, winning_score, median_score, min_score, file_name, file_digest)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (contest_id) DO UPDATE SET
                     slate_upload_id = EXCLUDED.slate_upload_id,
                     season = EXCLUDED.season, week = EXCLUDED.week,
                     entry_count = EXCLUDED.entry_count,
                     winning_score = EXCLUDED.winning_score,
                     median_score = EXCLUDED.median_score,
                     min_score = EXCLUDED.min_score""",
                (contest_id, path.stem, fmt,
                 (upload or {}).get("season"), (upload or {}).get("week"),
                 (upload or {}).get("upload_id"), parsed["entry_count"],
                 parsed["winning_score"], parsed["median_score"], parsed["min_score"],
                 path.name, digest))
            execute_values(
                cursor,
                """INSERT INTO nfl_dfs_field_ownership
                     (contest_id, player_name, normalized_name, drafted_pct, drafted_by_slot, fpts)
                   VALUES %s
                   ON CONFLICT (contest_id, normalized_name) DO UPDATE SET
                     drafted_pct = EXCLUDED.drafted_pct,
                     drafted_by_slot = EXCLUDED.drafted_by_slot,
                     fpts = EXCLUDED.fpts""",
                [(contest_id, p["name"], p["normalized_name"], p["drafted_pct"],
                  Json(p["drafted_by_slot"]), p["fpts"]) for p in parsed["players"].values()],
                page_size=500)


def slate_rows(db: PipelineDatabase, upload_id: str) -> list[dict]:
    return [dict(r) for r in db.execute(
        """SELECT dk_player_id, name, position, salary, our_proj, is_out, projection_status
           FROM nfl_dfs_slate_players WHERE upload_id = %s""", (upload_id,))]


def field_rows(db: PipelineDatabase, contest_id: str) -> dict[str, dict]:
    return {r["normalized_name"]: {"drafted_pct": float(r["drafted_pct"]),
                                   "fpts": None if r["fpts"] is None else float(r["fpts"])}
            for r in db.execute(
                "SELECT normalized_name, drafted_pct, fpts FROM nfl_dfs_field_ownership WHERE contest_id = %s",
                (contest_id,))}


def contests(db: PipelineDatabase, season: int | None, week: int | None) -> list[dict]:
    return [dict(r) for r in db.execute(
        """SELECT contest_id, contest_name, format, season, week, slate_upload_id,
                  entry_count, winning_score, median_score
           FROM nfl_dfs_field_contests
           WHERE slate_upload_id IS NOT NULL
             AND (%s::int IS NULL OR season = %s) AND (%s::int IS NULL OR week = %s)
           ORDER BY season, week, contest_id""", (season, season, week, week))]


def print_audit(contest: dict, audit: dict) -> None:
    s = audit["summary"]
    head = (f"{contest['season']}w{contest['week']} {contest['format']:<8} "
            f"contest {contest['contest_id']}  {contest['entry_count']:,} entries  "
            f"win {contest['winning_score']:.2f}  median {contest['median_score']:.2f}")
    print("\n" + head)
    print("  considered %d  flagged %d  ->  market knew %d, real edge %d"
          % (s["considered"], s["flagged"], s["market_knew"], s["real_edge"]))
    if not audit["flagged"]:
        print("  nothing flagged: no player we ranked highly was ignored by the field.")
        return
    print("  %-24s %-4s %6s %7s %7s %7s  %s"
          % ("PLAYER", "POS", "SAL", "PROJ", "FIELD%", "ACTUAL", "VERDICT"))
    for r in audit["flagged"]:
        actual = "  -  " if r["actual"] is None else f"{r['actual']:5.1f}"
        print("  %-24s %-4s %6d %7.1f %6.2f%% %7s  %s"
              % (r["name"], r["position"], r["salary"], r["our_proj"],
                 r["field_pct"], actual, r["verdict"]))
    print("  projected points assigned to players the field had written off "
          "who then produced nothing: %.1f" % s["projected_points_on_market_knew"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--contest", help="path to a DraftKings contest-standings CSV")
    parser.add_argument("--upload", help="slate upload id, when auto-matching cannot resolve it")
    parser.add_argument("--report", action="store_true", help="audit every imported contest")
    parser.add_argument("--season", type=int)
    parser.add_argument("--week", type=int)
    args = parser.parse_args()
    if not args.contest and not args.report:
        parser.error("pass --contest to import one, or --report to audit what is imported")

    db = PipelineDatabase(load_config().database_url)

    if args.contest:
        path = Path(args.contest)
        parsed, digest = read_export(path)
        fmt = "showdown" if is_showdown(parsed) else "classic"
        upload = resolve_upload(db, parsed, fmt, args.upload)
        contest_id = contest_id_from_path(path)
        persist(db, contest_id, parsed, fmt, path, digest, upload)
        print(f"imported contest {contest_id}: {parsed['entry_count']:,} entries, "
              f"{len(parsed['players'])} players with ownership, format={fmt}")
        if upload:
            print(f"  matched slate {upload['upload_id']} "
                  f"({upload['season']} week {upload['week']}"
                  + (f", {upload['overlap']:.0%} player overlap" if "overlap" in upload else "")
                  + ")")
        else:
            print("  NO slate matched. Ownership is stored and the audit is skipped; "
                  "re-run with --upload <id> to attach it.")

    if args.report or args.contest:
        audits, rows = [], contests(db, args.season, args.week)
        for contest in rows:
            audit = audit_slate(slate_rows(db, contest["slate_upload_id"]),
                                field_rows(db, contest["contest_id"]))
            audits.append(audit)
            print_audit(contest, audit)
        if audits:
            pooled = pooled_summary(audits)
            print(f"\n=== across {pooled['slates']} slate(s), {VERSION} ===")
            print(f"  flagged {pooled['flagged']}  ->  market knew {pooled['market_knew']}, "
                  f"real edge {pooled['real_edge']}")
            if pooled["market_knew_share"] is not None:
                print(f"  share that were our blind spots: {pooled['market_knew_share']:.0%}")
            print(f"  projected points spent on them: {pooled['projected_points_lost']}")
            if pooled["descriptive_only"]:
                print("  DESCRIPTIVE ONLY: under 30 flagged players, this split cannot be "
                      "told apart from luck. Keep importing contests.")
        elif args.report:
            print("no imported contest is attached to a slate yet.")


if __name__ == "__main__":
    main()
