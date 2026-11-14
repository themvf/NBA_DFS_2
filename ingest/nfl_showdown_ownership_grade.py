"""Grade stored Showdown ownership estimates against the real field.

`nfl-showdown-ownership-grade-v1` (model/nfl_showdown_ownership_grade.py; owner's
decision 2026-11-13). For every Showdown slate of the season that has been played:

1. the forecast: the last saved build or automatic freeze at least 20 minutes
   before kickoff that stored a captain and flex split;
2. the real field: the largest contest imported for that slate whose captain
   and flex ownership is usable (complete first), classified by the same rules
   the ownership scorecard uses (model/nfl_ownership_provenance.py);
3. the grade: the miss of every estimate stored in that forecast (the one the
   build read, our own model when stat-api replaced it, the comparison model),
   on the same players.

One row per slate and grade input goes to `nfl_showdown_ownership_grades`
(append-only; a slate whose inputs have not changed is skipped without reading
its payloads), and one row per run to `nfl_showdown_ownership_grade_runs`, which
names every recent slate still waiting and why. Real ownership arrives on its own
for the Thursday and Monday flagships (ingest/nfl_dfs_flagship_review.py, earlier
in the same workflow); any other Showdown is graded once its contest is imported.

    python -m ingest.nfl_showdown_ownership_grade [--season 2026] [--dry-run]

Reads only what is stored; calls no outside service.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from psycopg2.extras import Json

from config import load_config
from db.database import DatabaseManager
from db.nfl_showdown_ownership_grade_schema import STATEMENTS
from model import nfl_ownership_provenance as prov
from model import nfl_showdown_ownership_grade as g
from model.nfl_dfs_field_audit import normalize_name
from research.nfl_ownership_inventory import ET, Slate, attach_contests, load_slates, rows
from research.nfl_ownership_provenance import load_record

#: Newest pre-kickoff captures tried, newest first, until one holds a captain and flex split.
MAX_CANDIDATES = 5
#: A slate still waiting is named in the run row for this long after its kickoff.
WAITING_WINDOW = timedelta(days=30)
#: A contest is matched to a slate by its game only when imported within this long after kickoff.
MAX_IMPORT_LAG = timedelta(days=14)
_CONTEST_GAME = re.compile(r"\(([A-Z]{2,3})\s*(?:@|vs\.?)\s*([A-Z]{2,3})\)")

NO_FORECAST = "no saved build or automatic freeze from 20 minutes or more before kickoff"
NO_SPLIT = "the forecast saved before kickoff has no captain and flex split"
NO_CONTEST = "no contest ownership imported for this slate yet"
NO_LABELS = "no imported contest has usable captain and flex ownership"

_SNAPSHOT_PLAYER = "jsonb_build_object('name', e.p->'name', 'isOut', e.p->'isOut', 'ownership', e.p->'ownership')"
_SNAPSHOT_SQL = {
    "optimizer_run": f"""
        SELECT COALESCE(jsonb_agg({_SNAPSHOT_PLAYER}), '[]'::jsonb) AS players
        FROM nfl_dfs_optimizer_runs r
        CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(r.input_snapshot) = 'array'
                                                     THEN r.input_snapshot ELSE '[]'::jsonb END) AS e(p)
        WHERE r.run_id = %s::uuid""",
    "ghost_freeze": f"""
        SELECT COALESCE(jsonb_agg({_SNAPSHOT_PLAYER}), '[]'::jsonb) AS players
        FROM nfl_dfs_ghost_freezes f
        CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(f.input_snapshot) = 'array'
                                                     THEN f.input_snapshot ELSE '[]'::jsonb END) AS e(p)
        WHERE f.freeze_id = %s::uuid""",
}


def ensure_tables(conn) -> None:
    """Create the two tables once. Takes no lock when they are already there."""
    with conn.cursor() as cur:
        cur.execute("""SELECT to_regclass('public.nfl_showdown_ownership_grades') IS NOT NULL AS grades,
                              to_regclass('public.nfl_showdown_ownership_grade_runs') IS NOT NULL AS runs""")
        present = cur.fetchone()
        cur.execute("SELECT 1 FROM pg_trigger WHERE tgname = 'nfl_showdown_ownership_grades_immutable'")
        if present["grades"] and present["runs"] and cur.fetchone():
            return
        for statement in STATEMENTS:
            cur.execute(statement)
    conn.commit()


def current_season(conn) -> int:
    found = rows(conn, "SELECT max(season) AS season FROM nfl_season_games WHERE kickoff <= now()")
    if not found or found[0]["season"] is None:
        raise SystemExit("no played game on the schedule; pass --season")
    return int(found[0]["season"])


def showdown_slates(conn, season: int) -> list[Slate]:
    """Every Showdown slate of the NFL season with its imported contests attached."""
    slates, upload_slate = load_slates(conn, season)
    # load_slates filters on the calendar year of the kickoff; January and February belong to the same NFL season.
    late, _ = load_slates(conn, season + 1)
    for key, s in late.items():
        if key not in slates and s.first_kickoff and s.first_kickoff.astimezone(ET).month <= 2:
            slates[key] = s
            for upload in s.uploads:
                upload_slate[upload] = key
    contests = attach_contests(conn, slates, upload_slate, season)
    showdowns = sorted((s for s in slates.values() if s.fmt == "showdown" and s.first_kickoff), key=lambda s: s.first_kickoff)
    attach_by_game(showdowns, contests)
    return showdowns


def attach_by_game(slates: list[Slate], contests: list[dict]) -> int:
    """Attach Showdown contests `attach_contests` left without a slate, by the game in the contest's name.

    `attach_contests` matches an unlinked contest through the schedule, whose team codes differ
    from DraftKings' for some teams (the schedule says WSH, DraftKings says WAS), so a Washington
    game never matched. A Showdown slate's own game string is DraftKings' spelling, as the contest
    name is. The teams meet at most twice a season, so the slate is the latest one for that game
    that kicked off before the contest was imported, and no more than `MAX_IMPORT_LAG` before.
    """
    attached = 0
    for contest in contests:
        if contest.get("slate_key") or contest["format"] != "showdown":
            continue
        m = _CONTEST_GAME.search(contest["contest_name"] or "")
        if not m:
            continue
        games = {(f"{m.group(1)}@{m.group(2)}",), (f"{m.group(2)}@{m.group(1)}",)}
        candidates = [s for s in slates if s.games in games
                      and s.first_kickoff <= contest["imported_at"] <= s.first_kickoff + MAX_IMPORT_LAG]
        if not candidates:
            continue
        slate = max(candidates, key=lambda s: s.first_kickoff)
        contest["slate_key"], contest["link"] = slate.key, "matched_on_slate_game"
        slate.contests.append(contest)
        attached += 1
    return attached


def capture_candidates(conn, s: Slate) -> list[dict]:
    """The owner's saved builds and the automatic freezes of this slate, newest first, up to the cutoff."""
    uploads = sorted(s.uploads)
    return rows(conn, """
        SELECT source, id, at FROM (
          SELECT 'optimizer_run' AS source, r.run_id::text AS id, r.created_at AS at
          FROM nfl_dfs_optimizer_runs r JOIN nfl_dfs_slate_uploads u ON u.upload_id = r.upload_id
          WHERE r.upload_id = ANY(%s::uuid[]) AND NOT (r.settings ? 'researchStudy')
            AND r.status NOT LIKE 'research%%' AND u.origin = 'upload' AND r.created_at <= %s
          UNION ALL
          SELECT 'ghost_freeze', f.freeze_id::text, f.frozen_at
          FROM nfl_dfs_ghost_freezes f WHERE f.upload_id = ANY(%s::uuid[]) AND f.frozen_at <= %s) c
        ORDER BY at DESC LIMIT %s""", (uploads, s.cutoff, uploads, s.cutoff, MAX_CANDIDATES))


def load_snapshot(conn, capture: dict) -> list[dict]:
    """The frozen players of one capture, trimmed in the database to the fields the grade reads."""
    found = rows(conn, _SNAPSHOT_SQL[capture["source"]], (capture["id"],))
    players = found[0]["players"] if found else []
    return players if isinstance(players, list) else []


def forecast_for(conn, s: Slate) -> tuple[dict | None, list[dict], str | None]:
    """(capture, its players, reason when there is none)."""
    candidates = capture_candidates(conn, s)
    if not candidates:
        return None, [], NO_FORECAST
    for capture in candidates:
        players = load_snapshot(conn, capture)
        if g.snapshot_models(players)[1]:
            return capture, players, None
    return None, [], NO_SPLIT


def contest_signatures(conn, contest_ids: list[str]) -> list[list]:
    """A cheap fingerprint of what is stored for each contest, so an unchanged slate is skipped unread."""
    found = rows(conn, """
        SELECT c.contest_id, c.entry_count, c.score_curve IS NOT NULL AS full_standings,
               (SELECT count(*) FROM nfl_dfs_field_ownership o WHERE o.contest_id = c.contest_id) AS players,
               (SELECT COALESCE(sum(o.drafted_pct), 0) FROM nfl_dfs_field_ownership o
                 WHERE o.contest_id = c.contest_id) AS drafted,
               (SELECT COALESCE(sum((o.drafted_by_slot->>'CPT')::double precision), 0) FROM nfl_dfs_field_ownership o
                 WHERE o.contest_id = c.contest_id) AS captain,
               (SELECT count(*) FROM nfl_dfs_field_user_builds b WHERE b.contest_id = c.contest_id) AS users
        FROM nfl_dfs_field_contests c WHERE c.contest_id = ANY(%s) ORDER BY c.contest_id""", (contest_ids,))
    return [[r["contest_id"], int(r["entry_count"] or 0), bool(r["full_standings"]), int(r["players"]),
             round(float(r["drafted"]), 3), round(float(r["captain"]), 3), int(r["users"])] for r in found]


def input_key(capture: dict, signatures: list[list]) -> str:
    text = json.dumps([capture["source"], capture["id"], signatures], sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]


def labels_of(provenance: prov.ContestProvenance) -> dict[str, dict[str, float]]:
    """Usable real ownership by player and slot: observed, or a zero reconciled against a complete contest."""
    out: dict[str, dict[str, float]] = {}
    for label in provenance.labels:
        if (label.slot in g.SLOTS and label.label_class in prov.GATE_CLASSES and label.label_pct is not None
                and label.normalized_name):
            out.setdefault(label.normalized_name, {})[label.slot] = float(label.label_pct)
    return out


def pick_reference(conn, contests: list[dict], snapshot_names: frozenset[str]):
    """(contest, provenance, labels, complete) of the largest contest with complete real ownership,
    else the largest with any usable label.

    A contest whose stored split is the whole field's (model `whole_field_split`) is read
    value for value. Any other contest keeps the scorecard's label rules.
    """
    fallback = None
    for contest in sorted(contests, key=lambda c: -(c["entry_count"] or 0)):
        record, _ = load_record(conn, contest)
        if record.salary_pool is None and snapshot_names:
            # A contest matched to the slate by its name has no linked salary file; the frozen pool is the slate's.
            record = replace(record, salary_pool=snapshot_names)
        provenance = prov.classify_contest(record, mass_floor=prov.DEFAULT_MASS_FLOOR)
        whole = g.whole_field_split(record.ownership)
        labels = g.whole_field_labels(record.ownership, record.salary_pool) if whole else labels_of(provenance)
        if not labels:
            continue
        found = (contest, provenance, labels, bool(whole or provenance.complete), g.slot_sums(record.ownership))
        if found[3]:
            return found
        fallback = fallback or found
    return fallback


def season_week(conn, s: Slate) -> tuple[int | None, int | None]:
    if s.week is not None:
        return s.season, s.week
    found = rows(conn, "SELECT season, week FROM nfl_season_games WHERE kickoff = %s LIMIT 1", (s.first_kickoff,))
    return (int(found[0]["season"]), int(found[0]["week"])) if found else (s.season, None)


def _line(model: dict) -> str:
    def part(m):
        return "      -" if not m or m["miss"] is None else f"{m['miss']:5.2f} ({m['n']})"
    role = {"shown": "the build read", "ours": "ours, replaced", "shadow": "comparison"}.get(model["role"], model["role"])
    return f"    {model['version']:<38} {role:<15} captain {part(model['captain'])}  flex {part(model['flex'])}"


def run(conn, season: int, *, dry_run: bool) -> dict:
    now = datetime.now(timezone.utc)
    slates = [s for s in showdown_slates(conn, season) if s.first_kickoff <= now]
    existing: set[tuple[str, str]] = set()
    if rows(conn, "SELECT to_regclass('public.nfl_showdown_ownership_grades') IS NOT NULL AS present")[0]["present"]:
        existing = {(r["slate_key"], r["input_key"]) for r in rows(
            conn, "SELECT slate_key, input_key FROM nfl_showdown_ownership_grades WHERE grade_version = %s", (g.VERSION,))}
    graded_slates = {key for key, _ in existing}
    considered = new = 0
    waiting: list[dict] = []

    def wait(s: Slate, reason: str) -> None:
        print(f"  waiting  {'+'.join(s.games):<10} {s.first_kickoff.astimezone(ET):%a %Y-%m-%d}  {reason}")
        if s.key not in graded_slates and now - s.first_kickoff <= WAITING_WINDOW:
            waiting.append({"game": "+".join(s.games), "kickoff": s.first_kickoff.isoformat(), "reason": reason})

    print(f"{g.VERSION}: season {season}, {len(slates)} Showdown slates played")
    for s in slates:
        capture, players, reason = forecast_for(conn, s)
        if capture is None:
            if s.contests:
                considered += 1
                wait(s, reason or NO_FORECAST)
            continue
        considered += 1
        if not s.contests:
            wait(s, NO_CONTEST)
            continue
        key = input_key(capture, contest_signatures(conn, [c["contest_id"] for c in s.contests]))
        if (s.key, key) in existing:
            continue
        names = frozenset(n for n in (normalize_name(p.get("name")) for p in players) if n)
        reference = pick_reference(conn, s.contests, names)
        if reference is None:
            wait(s, NO_LABELS)
            continue
        contest, provenance, labels, complete, sums = reference
        result = g.grade_slate(players, labels)
        if not any((m["flex"] or {}).get("n") for m in result["models"]):
            wait(s, NO_LABELS)
            continue
        slate_season, week = season_week(conn, s)
        detail = {**result["detail"], "contest_path": provenance.path, "contest_link": contest.get("link"),
                  "real_totals": {"captain": round(sums["CPT"], 2), "flex": round(sums["FLEX"], 2)},
                  "contests_imported": len(s.contests), "minutes_before_kickoff": round(
                      (s.first_kickoff - capture["at"]).total_seconds() / 60, 1)}
        print(f"  graded   {'+'.join(s.games):<10} {s.first_kickoff.astimezone(ET):%a %Y-%m-%d}  {capture['source']} "
              f"{capture['id'][:8]} vs contest {contest['contest_id']} ({contest['entry_count']:,} entries"
              f"{'' if complete else ', ownership incomplete'})")
        for model in result["models"]:
            print(_line(model))
        new += 1
        graded_slates.add(s.key)
        if dry_run:
            continue
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO nfl_showdown_ownership_grades
                    (grade_version, slate_key, input_key, season, week, game, first_kickoff, capture_source, capture_id,
                     captured_at, contest_id, contest_name, entry_count, labels_complete, models, detail)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (grade_version, slate_key, input_key) DO NOTHING""",
                        (g.VERSION, s.key, key, slate_season, week, "+".join(s.games), s.first_kickoff, capture["source"],
                         capture["id"], capture["at"], contest["contest_id"], contest["contest_name"],
                         contest["entry_count"], complete, Json(result["models"]), Json(detail)))
        conn.commit()
    graded = sum(1 for s in slates if s.key in graded_slates)
    summary = {"season": season, "slates": considered, "graded": graded, "new_grades": new, "waiting": waiting}
    print(f"{considered} slates with a forecast or a contest: {graded} graded ({new} new this run), "
          f"{len(waiting)} recent slates waiting")
    if not dry_run:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO nfl_showdown_ownership_grade_runs (grade_version, season, slates, graded, new_grades, waiting)
                           VALUES (%s,%s,%s,%s,%s,%s)""", (g.VERSION, season, considered, graded, new, Json(waiting)))
        conn.commit()
    return summary


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--season", type=int, help="NFL season; default is the latest season with a played game")
    parser.add_argument("--dry-run", action="store_true", help="grade and print, write nothing")
    args = parser.parse_args(argv)
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    with db.connect() as conn:
        if not args.dry_run:
            ensure_tables(conn)
        run(conn, args.season or current_season(conn), dry_run=args.dry_run)


if __name__ == "__main__":
    main()
