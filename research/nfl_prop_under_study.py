"""Grade `nfl-prop-under-v1` (docs/nfl-prop-under-study.md).

Under bets on DraftKings player lines where the game-leaders model's P(over) is
at least 10 points below DraftKings' de-vigged P(over), graded weekly from the
nflverse player file. The registration JSON holds every constant; this module
only reads it.

    python -m research.nfl_prop_under_study grade [--write-db] [--out FILE]
    python -m research.nfl_prop_under_study grade --discovery   # pre-registration games, report only
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
REGISTRATION_PATH = ROOT / "docs" / "nfl-prop-under-study.json"
REGISTRATION = json.loads(REGISTRATION_PATH.read_text())
STUDY = REGISTRATION["study"]
STAT_COLUMN = {"rushing_yards": "rushing_yards", "receiving_yards": "receiving_yards", "receptions": "receptions"}


def file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def pins_hold(registration: Mapping[str, Any] = REGISTRATION, root: Path = ROOT) -> dict[str, bool]:
    """True per pinned file when the current file matches the latest pin for it."""
    pins = dict(registration["model_pins"])
    for amendment in registration.get("amendments", []):
        pins.update(amendment.get("model_pins", {}))
    return {name: file_digest(root / name) == digest for name, digest in pins.items()}


def cohort_window(registration: Mapping[str, Any] = REGISTRATION) -> tuple[datetime, datetime | None]:
    """Rows priced in [registered_at, first model amendment) belong to this version."""
    start = datetime.fromisoformat(registration["registered_at"])
    ends = [datetime.fromisoformat(a["effective_at"]) for a in registration.get("amendments", []) if a.get("model_pins")]
    return start, (min(ends) if ends else None)


def american_to_decimal(price: float) -> float:
    return 1 + 100 / abs(price) if price < 0 else 1 + price / 100


def select_rows(rows: Iterable[Mapping[str, Any]], kickoffs: Mapping[str, datetime],
                start: datetime | None, end: datetime | None, *, before_start: bool = False,
                registration: Mapping[str, Any] = REGISTRATION) -> list[dict]:
    """Latest eligible pregame row per (game, metric, player)."""
    latest: dict[tuple, dict] = {}
    for row in rows:
        kickoff = kickoffs.get(row["game_id"])
        if kickoff is None or row["metric"] not in registration["markets"]:
            continue
        if row["version"] != registration["market_version"] or row["book"] != registration["book"]:
            continue
        if str(row["identity"]).startswith("OTHER") or any(row.get(k) is None for k in
                                                            ("line", "over_price", "under_price", "fair_over", "model_p_over", "prop_captured_at")):
            continue
        if not (row["computed_at"] < kickoff and row["prop_captured_at"] < kickoff):
            continue
        captured = row["prop_captured_at"]
        if before_start:
            if start is not None and captured >= start:
                continue
        else:
            if start is not None and (captured < start or kickoff <= start):
                continue
            if end is not None and row["computed_at"] >= end:
                continue
        key = (row["game_id"], row["metric"], row["identity"])
        if key not in latest or row["computed_at"] > latest[key]["computed_at"]:
            latest[key] = dict(row)
    return list(latest.values())


def flagged(row: Mapping[str, Any], registration: Mapping[str, Any] = REGISTRATION) -> bool:
    return row["model_p_over"] - row["fair_over"] <= registration["edge_threshold"]


def settle(line: float, actual: float | None, under_price: float) -> tuple[str, float | None]:
    """Under bet, $1 stake. None actual means the player has no stat row: void."""
    if actual is None:
        return "void", None
    if actual == line:
        return "push", 0.0
    if actual < line:
        return "win", american_to_decimal(under_price) - 1
    return "loss", -1.0


def clustered_interval(bets: list[dict], resamples: int, seed: int, level: float = 0.95) -> tuple[float, float] | None:
    by_week: dict[int, list[float]] = defaultdict(list)
    for b in bets:
        by_week[b["week"]].append(b["pnl"])
    weeks = list(by_week)
    if len(weeks) < 2:
        return None
    rng = np.random.default_rng(seed)
    sums = np.array([sum(by_week[w]) for w in weeks])
    counts = np.array([len(by_week[w]) for w in weeks])
    draws = rng.integers(0, len(weeks), size=(resamples, len(weeks)))
    rois = sums[draws].sum(axis=1) / counts[draws].sum(axis=1)
    tail = (1 - level) / 2
    return float(np.quantile(rois, tail)), float(np.quantile(rois, 1 - tail))


def summarize(bets: list[dict], registration: Mapping[str, Any] = REGISTRATION, with_interval: bool = True) -> dict:
    graded = [b for b in bets if b["result"] != "void"]
    n = len(graded)
    pnl = sum(b["pnl"] for b in graded)
    out = {"bets": n, "voids": len(bets) - n,
           "wins": sum(b["result"] == "win" for b in graded), "losses": sum(b["result"] == "loss" for b in graded),
           "pushes": sum(b["result"] == "push" for b in graded),
           "pnl_units": round(pnl, 4), "roi": round(pnl / n, 4) if n else None,
           "games": len({b["game_id"] for b in graded}), "game_weeks": len({b["week"] for b in graded})}
    if with_interval and n:
        boot = registration["bootstrap"]
        interval = clustered_interval(graded, boot["resamples"], boot["seed"], boot["interval"])
        out["roi_interval"] = [round(x, 4) for x in interval] if interval else None
    return out


def grade(rows: list[dict], kickoffs: Mapping[str, datetime], weeks: Mapping[str, int],
          stats: Mapping[tuple[int, str], Mapping[str, float]], covered_games: set[str],
          *, discovery: bool = False, registration: Mapping[str, Any] = REGISTRATION) -> dict:
    """Pure grading. `stats[(week, gsis_id)]` holds the player's stat columns;
    `covered_games` are final games whose nflverse rows exist for both teams."""
    start, end = cohort_window(registration)
    chosen = select_rows(rows, kickoffs, start, end, before_start=discovery, registration=registration)
    all_lines, pending = [], 0
    for row in chosen:
        if row["game_id"] not in covered_games:
            pending += 1
            continue
        week = weeks[row["game_id"]]
        stat = stats.get((week, row["identity"]))
        actual = None if stat is None else float(stat.get(STAT_COLUMN[row["metric"]]) or 0.0)
        result, pnl = settle(row["line"], actual, row["under_price"])
        all_lines.append({"game_id": row["game_id"], "week": week, "metric": row["metric"], "player": row["player"],
                          "identity": row["identity"], "line": row["line"], "under_price": row["under_price"],
                          "edge": round(row["model_p_over"] - row["fair_over"], 4), "flagged": flagged(row, registration),
                          "actual": actual, "result": result, "pnl": pnl})
    bets = [b for b in all_lines if b["flagged"]]
    primary = summarize(bets, registration)
    floors = registration["floors"]
    floors_met = primary["bets"] >= floors["graded_bets"] and primary["game_weeks"] >= floors["game_weeks"]
    verdict = None
    if floors_met and not discovery and primary.get("roi_interval"):
        verdict = "PASS" if primary["roi_interval"][0] > 0 else "FAIL"
    by_game = defaultdict(int)
    for b in bets:
        if b["result"] != "void":
            by_game[b["game_id"]] += 1
    return {
        "study": registration["study"], "population": "discovery (pre-registration, cannot confirm)" if discovery else "confirmation",
        "cohort": {"start": start.isoformat(), "end": end.isoformat() if end else None},
        "primary": primary, "floors": floors, "floors_met": floors_met, "verdict_if_first": verdict,
        "status": "descriptive until floors are met" if not floors_met else "floors met",
        "controls": {"every_under": summarize(all_lines, registration, with_interval=False),
                     "unflagged_unders": summarize([b for b in all_lines if not b["flagged"]], registration, with_interval=False)},
        "by_market": {m: summarize([b for b in bets if b["metric"] == m], registration, with_interval=False)
                      for m in registration["markets"]},
        "largest_game_share": round(max(by_game.values()) / primary["bets"], 4) if primary["bets"] else None,
        "pending_lines": pending, "bets": bets,
    }


# --- database and nflverse ----------------------------------------------------------------------

def _connect():
    import os
    import psycopg2
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    return psycopg2.connect(os.environ["DATABASE_URL"])


def load_inputs(cur, season: int) -> tuple[list[dict], dict, dict, set]:
    cur.execute("""SELECT nflverse_game_id, week, kickoff, completed FROM nfl_season_games
                   WHERE season = %s AND game_type = 'REG' AND nflverse_game_id IS NOT NULL""", (season,))
    games = cur.fetchall()
    kickoffs = {g[0]: g[2] for g in games}
    weeks = {g[0]: int(g[1]) for g in games}
    completed = {g[0] for g in games if g[3]}
    cur.execute("""SELECT game_id, metric, identity, player, book, line, over_price, under_price, fair_over,
                          model_p_over, prop_captured_at, computed_at, version
                   FROM nfl_game_leader_market WHERE game_id LIKE %s""", (f"{season}_%",))
    cols = ("game_id", "metric", "identity", "player", "book", "line", "over_price", "under_price", "fair_over",
            "model_p_over", "prop_captured_at", "computed_at", "version")
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return rows, kickoffs, weeks, completed


def load_stats(season: int, weeks: Mapping[str, int], completed: set[str]) -> tuple[dict, set, dict]:
    import pandas as pd
    import requests
    from ingest.ff_independent import NFLVERSE_WEEKLY_STATS_URL
    url = NFLVERSE_WEEKLY_STATS_URL.format(season=season)
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    frame = pd.read_csv(io.BytesIO(response.content), low_memory=False)
    frame = frame[(frame["season"] == season) & (frame["season_type"] == "REG")]
    stats = {(int(r.week), str(r.player_id)): {c: r[c] for c in STAT_COLUMN.values()}
             for _, r in frame.iterrows() if isinstance(r.player_id, str)}
    teams_by_week = defaultdict(set)
    for week, team in zip(frame["week"], frame["team"]):
        teams_by_week[int(week)].add(str(team))
    covered = set()
    for game_id in completed:
        away, home = game_id.split("_")[2:4]
        if {away, home} <= teams_by_week[weeks[game_id]]:
            covered.add(game_id)
    source = {"url": url, "sha256": hashlib.sha256(response.content).hexdigest(), "rows": int(len(frame)),
              "fetched_at": datetime.now(timezone.utc).isoformat()}
    return stats, covered, source


def ensure_tables() -> None:
    """Apply db/schema.py through the standard path (a no-op when unchanged), so
    the study tables exist before the first write."""
    import os
    from db.database import DatabaseManager
    DatabaseManager(os.environ["DATABASE_URL"])


def write_run(conn, report: dict) -> None:
    ensure_tables()
    cur = conn.cursor()
    primary = report["primary"]
    cur.execute("""INSERT INTO nfl_prop_under_study_runs (study, evaluated_at, graded_bets, game_weeks, roi, report)
                   VALUES (%s, now(), %s, %s, %s, %s) RETURNING id""",
                (report["study"], primary["bets"], primary["game_weeks"], primary["roi"], json.dumps(report, default=str)))
    run_id = cur.fetchone()[0]
    if report["verdict_if_first"]:
        cur.execute("""INSERT INTO nfl_prop_under_study_verdicts (study, verdict, run_id, decided_at)
                       VALUES (%s, %s, %s, now()) ON CONFLICT (study) DO NOTHING""",
                    (report["study"], report["verdict_if_first"], run_id))
    conn.commit()
    cur.execute("SELECT verdict, decided_at FROM nfl_prop_under_study_verdicts WHERE study = %s", (report["study"],))
    report["frozen_verdict"] = cur.fetchone()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    g = sub.add_parser("grade")
    g.add_argument("--write-db", action="store_true")
    g.add_argument("--discovery", action="store_true", help="grade the pre-registration games instead (report only)")
    g.add_argument("--out", type=Path)
    args = parser.parse_args()
    held = pins_hold()
    if not all(held.values()) and not args.discovery:
        raise SystemExit(f"Model pins changed without an amendment in {REGISTRATION_PATH.name}: "
                         f"{[k for k, v in held.items() if not v]}")
    conn = _connect()
    cur = conn.cursor()
    season = REGISTRATION["season"]
    rows, kickoffs, weeks, completed = load_inputs(cur, season)
    conn.rollback()
    stats, covered, source = load_stats(season, weeks, completed)
    report = grade(rows, kickoffs, weeks, stats, covered, discovery=args.discovery)
    report.update(evaluated_at=datetime.now(timezone.utc).isoformat(), nflverse=source,
                  registration_sha256=file_digest(REGISTRATION_PATH), pins_hold=held)
    if args.write_db and not args.discovery:
        write_run(conn, report)
    conn.close()
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=1, default=str))
    summary = {k: report[k] for k in ("study", "population", "primary", "floors_met", "status", "controls",
                                      "by_market", "largest_game_share", "pending_lines")}
    summary["frozen_verdict"] = report.get("frozen_verdict")
    print(json.dumps(summary, indent=1, default=str))


if __name__ == "__main__":
    main()
