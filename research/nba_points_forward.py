"""Freeze research baselines at one explicit cutoff, then grade a separate file.

This records market-pool benchmarks, not legal contest entries or payout odds.
No model is fitted and no database writes or Odds API requests are made.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

from config import load_config
from model.nba_points_calibration import (
    BoxGame, Capture, grade_captures, load_box_scores, select_pretip_rows,
)
from model.nba_points_ladder import LADDER_VERSION, name_key, player_key, read_player

VERSION = "nba-points-forward-v1"


def aware(value: str | datetime) -> datetime:
    at = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    if at.tzinfo is None or at.utcoffset() is None:
        raise ValueError("timestamps must include a UTC offset")
    return at.astimezone(timezone.utc)


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def build_snapshot(raw: list[dict[str, Any]], target: date, lock: datetime,
                   created: datetime, games: list[BoxGame] | None = None) -> dict[str, Any]:
    lock, created = aware(lock), aware(created)
    cutoff = min(lock, created)
    rows = []
    for r in raw:
        item = dict(r)
        item["captured_at"] = aware(item["captured_at"])
        item["commence_time"] = aware(item["commence_time"])
        if item["captured_at"] >= cutoff:
            continue
        if str(item["game_date"]) != target.isoformat():
            raise ValueError("mixed game dates in source rows")
        rows.append(item)
    mains = [r for r in rows if r["market"] == "player_points"]
    if not mains:
        raise ValueError("no main-line captures before cutoff")
    if lock > min(r["commence_time"] for r in mains):
        raise ValueError("an all-day pool requires a cutoff no later than its earliest market start")
    selected = select_pretip_rows(rows, games or [])
    ladders = {(str(r["event_id"]), name_key(r["player"])): r for r in selected if r["market"] == "player_points_alternate"}
    candidates = []
    identities = set()
    for r in selected:
        if r["market"] != "player_points":
            continue
        identity = (str(r["event_id"]), player_key(r["player"]))
        if identity in identities:
            raise ValueError(f"ambiguous player identity in snapshot: {identity}")
        identities.add(identity)
        alt = ladders.get((str(r["event_id"]), name_key(r["player"])))
        reading = read_player(r["books"], alt["books"] if alt else {})
        if reading is None:
            continue
        dist = reading.distribution
        candidates.append({
            "event_id":str(r["event_id"]), "player":r["player"], "player_key":identity[1],
            "home_team":r["home_team_name"], "away_team":r["away_team_name"],
            "commence_time":r["commence_time"].isoformat(), "captured_at":r["captured_at"].isoformat(),
            "line":reading.line, "p_over":reading.p_over,
            "ceiling":dist.ceiling if dist else None,
            "p30":dist.at_least[30] if dist else None,
            "main_books":r["books"], "ladder_books":alt["books"] if alt else {},
            "ladder_captured_at":alt["captured_at"].isoformat() if alt else None,
        })
    candidates.sort(key=lambda r:(r["event_id"], r["player_key"]))
    if len(candidates) < 3:
        raise ValueError("fewer than three candidates; cannot record a trio baseline")
    selections = {}
    for label, field in [("highest_lines", "line"), ("highest_ceilings", "ceiling"), ("highest_p30", "p30")]:
        pool = sorted([r for r in candidates if r[field] is not None],
                      key=lambda r:(-r[field], -r["line"], -(r["p_over"] or 0), r["player_key"], r["event_id"]))
        picks = pool[:3] if len(pool) >= 3 else []
        selections[label] = {
            "picks":[{"event_id":r["event_id"], "player_key":r["player_key"], "player":r["player"]} for r in picks],
            "eligible_candidates":len(pool),
            "boundary_tied_candidates":sum(r[field] == picks[-1][field] for r in pool) if picks else 0,
        }
    result = {
        "version":VERSION, "ladder_version":LADDER_VERSION, "game_date":target.isoformat(),
        "created_at":created.isoformat(), "lock_at":lock.isoformat(), "as_of":cutoff.isoformat(),
        "mode":"prospective" if created < lock else "retrospective_reconstruction",
        "scope":"unverified_all_day_market_pool", "contest_eligibility_verified":False,
        "availability_verified":False, "pool_complete":False,
        "espn_scheduled_starts":[{"event_id":g.espn_event_id,"home_team":g.home_team,
                                  "away_team":g.away_team,"start_time":g.start_time.isoformat()}
                                 for g in (games or []) if g.start_time],
        "tie_rule":"metric descending, line descending, p_over descending, canonical name, event id",
        "limitations":["Contest games, eligibility, roster constraints, field entries and payout rules are unverified.",
                        "Missing or pulled quotes can omit eligible players; this is only the observed market pool.",
                        "Ceiling and p30 benchmarks are unvalidated market interpretations, not optimized trios."],
        "candidates":candidates, "selections":selections,
    }
    result["sha256"] = digest(result)
    return result


def grade_snapshot(snapshot: dict[str, Any], games: list[BoxGame]) -> dict[str, Any]:
    frozen = {k:v for k,v in snapshot.items() if k != "sha256"}
    if snapshot.get("version") != VERSION or digest(frozen) != snapshot.get("sha256"):
        raise ValueError("snapshot version or digest mismatch")
    outcomes = {}
    for r in snapshot["candidates"]:
        cap = Capture(r["event_id"], aware(r["commence_time"]), aware(r["captured_at"]),
                      r["home_team"], r["away_team"], r["player"], r["main_books"])
        grades = grade_captures(date.fromisoformat(snapshot["game_date"]), [cap], {}, games)
        if len(grades) != 1:
            raise ValueError("frozen candidate no longer readable")
        outcomes[(r["event_id"], r["player_key"])] = grades[0]
    selections = {}
    for label, entry in snapshot["selections"].items():
        picks = [outcomes[(r["event_id"],r["player_key"])] for r in entry["picks"]]
        complete = len(picks) == 3 and all(r.status in ("graded", "push") and r.actual_points is not None for r in picks)
        selections[label] = {"points":sum(r.actual_points for r in picks) if complete else None,
                             "complete":complete, "outcomes":[{
                                 "player":r.player, "status":r.status, "actual_points":r.actual_points,
                                 "team":r.team, "espn_event_id":r.espn_event_id,
                             } for r in picks]}
    return {"version":VERSION, "snapshot_sha256":snapshot["sha256"], "snapshot_mode":snapshot["mode"],
            "graded_at":datetime.now(timezone.utc).isoformat(), "scope":snapshot["scope"],
            "selections":selections, "contest_finish":None, "payout":None,
            "limitations":["DNP/unmatched outcomes leave trio totals unresolved; no contest DNP rule is assumed.",
                            "This compares frozen market-pool baselines; it does not measure contest profitability."]}


def write_new(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as f:
        json.dump(value, f, sort_keys=True, indent=2, default=str)
        f.write("\n")


def main() -> int:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    freeze = sub.add_parser("freeze")
    freeze.add_argument("--date", required=True, type=date.fromisoformat)
    freeze.add_argument("--lock-at", required=True, type=aware)
    freeze.add_argument("--output", required=True, type=Path)
    grade = sub.add_parser("grade")
    grade.add_argument("--snapshot", required=True, type=Path)
    grade.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    # A read-only connection avoids schema initialization and accidental regrades.
    from db.database import DatabaseManager
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    with psycopg2.connect(db.database_url, cursor_factory=RealDictCursor) as conn:
        conn.set_session(readonly=True)
        db._shared_connection = conn
        try:
            if args.action == "freeze":
                created = datetime.now(timezone.utc)
                with conn.cursor() as cur:
                    cur.execute("""SELECT event_id,game_date,commence_time,captured_at,home_team_name,away_team_name,player,books,market
                        FROM prop_odds_history WHERE sport='nba' AND game_date=%s
                        AND market IN ('player_points','player_points_alternate') AND captured_at < %s""",
                        (args.date, min(created,args.lock_at)))
                    raw = [dict(r) for r in cur.fetchall()]
                result = build_snapshot(raw,args.date,args.lock_at,created,load_box_scores(db,args.date))
            else:
                snapshot = json.loads(args.snapshot.read_text(encoding="utf-8"))
                result = grade_snapshot(snapshot,load_box_scores(db,date.fromisoformat(snapshot["game_date"])))
            write_new(args.output,result)
        finally:
            db._shared_connection = None
    print(json.dumps({"output":str(args.output), "mode":result.get("mode",result.get("snapshot_mode")),
                      "scope":result["scope"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
