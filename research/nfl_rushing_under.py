"""Forward test of every DraftKings NFL rushing-yards under (`nfl-rushing-under-v1`).

docs/nfl-rushing-under-study.md. The population is the closing pregame DraftKings
rushing-yards line of every listed player in a regular-season game that kicks off
after the registration (2026 weeks 7-18). Each is bet under at DraftKings' price,
$1 flat, and settled from the nflverse weekly player file with the parent study's
rules (research/nfl_prop_under_study.py, imported unchanged): no stat row is a void,
a push returns 0. No model is involved. With --write-db it appends one run row a
week and, at the single look after week 18, the one verdict (both append-only).

    python -m research.nfl_rushing_under sizes --week 6      # line counts only, no results
    python -m research.nfl_rushing_under grade [--write-db] [--out path]
"""
from __future__ import annotations

import argparse
import io
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np

from model.nfl_dfs_dk_pool_match import normalize_name
from model.nfl_prop_implied import p_over as fair_p_over
from research import nfl_prop_under_study as under

ROOT = Path(__file__).resolve().parents[1]
REGISTRATION_PATH = ROOT / "docs" / "nfl-rushing-under-study.json"
REGISTRATION = json.loads(REGISTRATION_PATH.read_text())


def _ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def complete_quote(quote: Any) -> dict | None:
    """A DraftKings quote with a line and both prices, else None."""
    if not isinstance(quote, Mapping):
        return None
    if any(quote.get(k) is None for k in ("line", "over", "under")):
        return None
    return {"line": float(quote["line"]), "over": float(quote["over"]), "under": float(quote["under"])}


def match_game(capture: Mapping[str, Any], games: Iterable[Mapping[str, Any]], window_minutes: float) -> dict | None:
    """The schedule game a prop event belongs to: same home and away team, kickoff within the window."""
    hits = [g for g in games if g["home_name"] == capture["home_team_name"] and g["away_name"] == capture["away_team_name"]
            and abs((_ts(g["kickoff"]) - _ts(capture["commence_time"])).total_seconds()) <= window_minutes * 60]
    return hits[0] if len(hits) == 1 else None


def closing_lines(captures: Iterable[Mapping[str, Any]], games: list[Mapping[str, Any]],
                  registration: Mapping[str, Any] = REGISTRATION, *, cohort_only: bool = True) -> list[dict]:
    """The latest pregame capture per (game, player) carrying a complete DraftKings quote.

    `captures` rows: event_id, player, dk (the DraftKings quote or None), captured_at,
    commence_time, home_team_name, away_team_name. With `cohort_only` only games that kick
    off after registered_at, in the registered weeks, are kept.
    """
    first, last = registration["weeks"]
    start = _ts(registration["registered_at"]) if registration.get("registered_at") else None
    window = registration["event_match_window_minutes"]
    final_capture: dict[str, datetime] = {}
    latest: dict[tuple[str, str], dict] = {}
    for cap in captures:
        game = match_game(cap, games, window)
        if game is None:
            continue
        kickoff, captured = _ts(game["kickoff"]), _ts(cap["captured_at"])
        if not (captured < kickoff and captured < _ts(cap["commence_time"])):
            continue
        if cohort_only and (not first <= game["week"] <= last or start is None or kickoff <= start):
            continue
        gid = game["game_id"]
        final_capture[gid] = max(final_capture.get(gid, captured), captured)
        quote = complete_quote(cap.get("dk"))
        if quote is None:
            continue
        key = (gid, normalize_name(cap["player"]))
        if key not in latest or captured > latest[key]["captured_at"]:
            latest[key] = {"game_id": gid, "week": game["week"], "kickoff": kickoff, "player": cap["player"],
                           "key": key[1], "line": quote["line"], "over_price": quote["over"],
                           "under_price": quote["under"], "captured_at": captured}
    for row in latest.values():
        row["fair_over"] = round(fair_p_over(row["over_price"], row["under_price"]), 6)
        row["in_final_capture"] = row["captured_at"] == final_capture[row["game_id"]]
    return sorted(latest.values(), key=lambda r: (r["kickoff"], r["game_id"], r["key"]))


def settle_lines(lines: Iterable[Mapping[str, Any]], names: Mapping[tuple[str, str], list[str]],
                 stats: Mapping[tuple[int, str], Mapping[str, float]], covered: set[str]) -> tuple[list[dict], int]:
    """Bets on every closing line whose game is graded, and the count still pending.

    `names[(game_id, normalized name)]` lists the nflverse player ids with a stat row in that
    game. None is a void (no stat row, as in the parent study); two is a void too (ambiguous).
    """
    bets, pending = [], 0
    for line in lines:
        if line["game_id"] not in covered:
            pending += 1
            continue
        ids = names.get((line["game_id"], line["key"]), [])
        identity, reason, actual = None, None, None
        if len(ids) == 1:
            identity = ids[0]
            stat = stats.get((line["week"], identity))
            if stat is None:
                reason = "no_stat_row"
            else:
                actual = float(stat.get("rushing_yards") or 0.0)
        else:
            reason = "no_stat_row" if not ids else "ambiguous_name"
        result, pnl = under.settle(line["line"], actual, line["under_price"])
        bets.append({"game_id": line["game_id"], "week": line["week"], "player": line["player"],
                     "identity": identity, "line": line["line"], "over_price": line["over_price"],
                     "under_price": line["under_price"], "fair_over": line["fair_over"],
                     "captured_at": line["captured_at"], "in_final_capture": line["in_final_capture"],
                     "actual": actual, "result": result, "pnl": pnl, "void_reason": reason})
    return bets, pending


def calibration(bets: list[dict]) -> dict | None:
    """Over hit rate against the de-vigged implied rate (pushes and voids excluded)."""
    rows = [b for b in bets if b["result"] in ("win", "loss")]
    if not rows:
        return None
    over = float(np.mean([b["result"] == "loss" for b in rows]))
    implied = float(np.mean([b["fair_over"] for b in rows]))
    return {"lines": len(rows), "over_hit": round(over, 4), "implied_over": round(implied, 4),
            "gap": round(over - implied, 4)}


def calibration_interval(bets: list[dict], resamples: int, seed: int, level: float = 0.95) -> tuple[float, float] | None:
    """Week-clustered bootstrap of the pooled over-hit minus implied gap (descriptive)."""
    by_week: dict[int, list[float]] = defaultdict(list)
    for b in bets:
        if b["result"] in ("win", "loss"):
            by_week[b["week"]].append((b["result"] == "loss") - b["fair_over"])
    weeks = list(by_week)
    if len(weeks) < 2:
        return None
    sums = np.array([sum(by_week[w]) for w in weeks])
    counts = np.array([len(by_week[w]) for w in weeks])
    draws = np.random.default_rng(seed).integers(0, len(weeks), size=(resamples, len(weeks)))
    gaps = sums[draws].sum(axis=1) / counts[draws].sum(axis=1)
    tail = (1 - level) / 2
    return float(np.quantile(gaps, tail)), float(np.quantile(gaps, 1 - tail))


def verdict(primary: Mapping[str, Any], floors_met: bool) -> str:
    interval = primary.get("roi_interval")
    if not floors_met or not interval:
        return "INCONCLUSIVE"
    if interval[0] > 0:
        return "PASS"
    if interval[1] < 0:
        return "LOSES"
    return "NO_EDGE"


def summarize(bets: list[dict], registration: Mapping[str, Any] = REGISTRATION, final: bool = False) -> dict:
    """`final` is True only once every cohort game through the last week is graded: the single look."""
    primary = under.summarize(bets, registration=registration)
    floors = registration["floors"]
    floors_met = primary["bets"] >= floors["graded_bets"] and primary["game_weeks"] >= floors["game_weeks"]
    boot = registration["bootstrap"]
    gap_ci = calibration_interval(bets, boot["resamples"], boot["seed"], boot["interval"])
    graded = [b for b in bets if b["result"] != "void"]
    void_reasons = defaultdict(int)
    for b in bets:
        if b["result"] == "void":
            void_reasons[b["void_reason"]] += 1
    return {
        "study": registration["study"],
        "primary": primary,
        "floors": floors, "floors_met": floors_met,
        "verdict_if_first": verdict(primary, floors_met) if final else None,
        "status": "final" if final else "descriptive until the final look",
        "calibration": {"pooled": calibration(bets),
                        "pooled_gap_interval": [round(x, 4) for x in gap_ci] if gap_ci else None,
                        "by_week": {w: calibration([b for b in bets if b["week"] == w])
                                    for w in sorted({b["week"] for b in bets})}},
        "by_week": {w: under.summarize([b for b in bets if b["week"] == w], registration, with_interval=False)
                    for w in sorted({b["week"] for b in bets})},
        "voids": dict(void_reasons),
        "closed_before_final_capture": sum(not b["in_final_capture"] for b in graded),
    }


# --- database and nflverse ----------------------------------------------------------------------

def load_inputs(cur, registration: Mapping[str, Any] = REGISTRATION) -> tuple[list[dict], list[dict]]:
    season = registration["season"]
    cur.execute("""SELECT g.nflverse_game_id, g.week, g.kickoff, g.completed, h.odds_api_name, a.odds_api_name
                   FROM nfl_season_games g
                   JOIN nfl_teams h ON h.team_id = g.home_team_id JOIN nfl_teams a ON a.team_id = g.away_team_id
                   WHERE g.season = %s AND g.game_type = 'REG' AND g.nflverse_game_id IS NOT NULL""", (season,))
    games = [{"game_id": r[0], "week": int(r[1]), "kickoff": r[2], "completed": bool(r[3]),
              "home_name": r[4], "away_name": r[5]} for r in cur.fetchall()]
    cur.execute("""SELECT event_id, player, books->%s, captured_at, commence_time, home_team_name, away_team_name
                   FROM prop_odds_history
                   WHERE sport = 'nfl' AND market = %s AND captured_at < commence_time
                     AND commence_time >= %s AND commence_time < %s""",
                (registration["book"], registration["market"], f"{season}-08-01", f"{season + 1}-03-01"))
    cols = ("event_id", "player", "dk", "captured_at", "commence_time", "home_team_name", "away_team_name")
    return games, [dict(zip(cols, r)) for r in cur.fetchall()]


def load_names(season: int) -> tuple[dict[tuple[str, str], list[str]], dict]:
    """(nflverse game_id, normalized display name) -> player ids with a stat row in that game."""
    import hashlib
    import pandas as pd
    import requests
    from ingest.ff_independent import NFLVERSE_WEEKLY_STATS_URL
    url = NFLVERSE_WEEKLY_STATS_URL.format(season=season)
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    frame = pd.read_csv(io.BytesIO(response.content), low_memory=False)
    frame = frame[(frame["season"] == season) & (frame["season_type"] == "REG")]
    names: dict[tuple[str, str], list[str]] = defaultdict(list)
    for gid, name, pid in zip(frame["game_id"], frame["player_display_name"], frame["player_id"]):
        if isinstance(pid, str) and isinstance(gid, str):
            ids = names[(gid, normalize_name(name))]
            if pid not in ids:
                ids.append(pid)
    return dict(names), {"url": url, "sha256": hashlib.sha256(response.content).hexdigest(), "rows": int(len(frame))}


def write_run(out: Mapping[str, Any], full: Mapping[str, Any]) -> tuple | None:
    """Append this run; write the verdict only at the final look, once."""
    under.ensure_tables()
    conn = under._connect()
    try:
        cur = conn.cursor()
        primary = out["primary"]
        cur.execute("""INSERT INTO nfl_rushing_under_runs (study, evaluated_at, graded_bets, game_weeks, roi, report)
                       VALUES (%s, now(), %s, %s, %s, %s) RETURNING id""",
                    (out["study"], primary["bets"], primary["game_weeks"], primary["roi"],
                     json.dumps(full, default=str)))
        run_id = cur.fetchone()[0]
        if out["verdict_if_first"]:
            cur.execute("""INSERT INTO nfl_rushing_under_verdicts (study, verdict, run_id, decided_at)
                           VALUES (%s, %s, %s, now()) ON CONFLICT (study) DO NOTHING""",
                        (out["study"], out["verdict_if_first"], run_id))
        conn.commit()
        cur.execute("SELECT verdict, decided_at FROM nfl_rushing_under_verdicts WHERE study = %s", (out["study"],))
        return cur.fetchone()
    finally:
        conn.close()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)
    s = sub.add_parser("sizes", help="count one week's DraftKings closing lines; no results are read")
    s.add_argument("--week", type=int, required=True)
    g = sub.add_parser("grade")
    g.add_argument("--out", type=Path)
    g.add_argument("--write-db", action="store_true")
    a = p.parse_args()
    conn = under._connect()
    cur = conn.cursor()
    games, captures = load_inputs(cur)
    conn.rollback()
    conn.close()
    if a.command == "sizes":
        lines = [r for r in closing_lines(captures, games, cohort_only=False) if r["week"] == a.week]
        print(json.dumps({"week": a.week, "lines": len(lines), "games": len({r["game_id"] for r in lines}),
                          "scheduled_games": sum(g["week"] == a.week for g in games),
                          "closed_before_final_capture": sum(not r["in_final_capture"] for r in lines)}, indent=1))
        return
    if REGISTRATION.get("status") != "registered" or not REGISTRATION.get("registered_at"):
        raise SystemExit("DRAFT: the study is not registered; it has no cohort yet")
    season = REGISTRATION["season"]
    weeks = {g["game_id"]: g["week"] for g in games}
    completed = {g["game_id"] for g in games if g["completed"]}
    stats, covered, nfl_source = under.load_stats(season, weeks, completed)
    names, names_source = load_names(season)
    lines = closing_lines(captures, games)
    bets, pending = settle_lines(lines, names, stats, covered)
    first, last = REGISTRATION["weeks"]
    start = _ts(REGISTRATION["registered_at"])
    cohort_games = [g["game_id"] for g in games if first <= g["week"] <= last and _ts(g["kickoff"]) > start]
    final = bool(cohort_games) and all(gid in covered for gid in cohort_games)
    out = summarize(bets, final=final)
    out.update(evaluated_at=datetime.now(timezone.utc).isoformat(), pending_lines=pending,
               cohort_games={"scheduled": len(cohort_games), "graded": sum(gid in covered for gid in cohort_games),
                             "with_lines": len({b["game_id"] for b in bets})},
               nflverse_results=nfl_source, nflverse_names=names_source,
               registration_sha256=under.file_digest(REGISTRATION_PATH))
    full = {**out, "bets": bets}
    if a.write_db:
        out["frozen_verdict"] = write_run(out, full)
    if a.out:
        a.out.parent.mkdir(parents=True, exist_ok=True)
        a.out.write_text(json.dumps(full, indent=1, default=str))
    print(json.dumps(out, indent=1, default=str))


if __name__ == "__main__":
    main()
