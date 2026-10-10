"""CFB pattern-watch live board - frozen selections, quote freshness, drift.

Companion to `model/cfb_pattern_watch.py`. The scan grades games AFTER they
finish at the verified close; this board is what a reader sees BEFORE kickoff,
and it must never show a stale or moved line as a current selection.

Rules (frozen with the study; see docs/cfb-early-season-patterns-study.md):

* A quote is STALE when its newest capture is older than `STALE_INSIDE_MIN`
  inside `NEAR_KICKOFF_HOURS` of kickoff, or `STALE_OUTSIDE_MIN` otherwise.
  A stale quote cannot create or refresh a selection; the card reads
  "line unavailable, last seen X".
* A SELECTION is frozen the first time a (trigger, game) fires on a fresh
  quote: line, consensus price, best retail book/price, capture time. It is
  append-only (`selections.jsonl`) and never rewritten. Everything after that
  is drift shown next to it.
* At `CUTOFF_MIN` before kickoff the selection is FINAL: the publication
  record the ledger can be held to. Drift is still displayed.
* A LATE MOVE (spread >= `LATE_SPREAD_PTS`, total >= `LATE_TOTAL_PTS`, or
  implied probability >= `LATE_PROB_PP` points, measured from the capture
  nearest `LATE_WINDOW_HOURS` before kickoff) flags the card WATCH: the
  market is telling us something we cannot see. No injury feed exists for
  college football in this repo, so the line is the injury feed.
* DROPPED: the trigger no longer fires at the current line (e.g. the spread
  left the band). The frozen selection stays in the file; the card says so.

Usage:
    python -m model.cfb_pattern_board            # write board.md, append new selections
    python -m model.cfb_pattern_board --no-write # print only
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config import load_config
from db.database import DatabaseManager
from model import cfb_pattern_watch as pw

BOARD_VERSION = "cfb-pattern-board-v1"
STALE_INSIDE_MIN = 60
STALE_OUTSIDE_MIN = 360
NEAR_KICKOFF_HOURS = 6
CUTOFF_MIN = 90
LATE_WINDOW_HOURS = 2
LATE_SPREAD_PTS = 1.5
LATE_TOTAL_PTS = 2.0
LATE_PROB_PP = 4.0
LOOKAHEAD_HOURS = 36
RETAIL_BOOKS = ("draftkings", "fanduel", "betmgm", "williamhill_us", "fanatics", "espnbet", "betrivers", "hardrockbet")
DEFAULT_SELECTIONS = Path("artifacts") / "cfb_pattern_watch" / "selections.jsonl"
DEFAULT_BOARD = Path("artifacts") / "cfb_pattern_watch" / "board.md"

_GAMES_SQL = """
SELECT m.id AS matchup_id, m.week, m.commence_time, m.home_spread, m.vegas_total, m.home_ml, m.away_ml,
       m.odds_fetched_at, m.completed,
       ht.name AS home, at.name AS away, ht.conference AS hconf, at.conference AS aconf,
       ht.classification AS hclass, at.classification AS aclass
FROM cfb_matchups m
JOIN cfb_teams ht ON ht.team_id = m.home_team_id
JOIN cfb_teams at ON at.team_id = m.away_team_id
WHERE m.season = %s AND m.commence_time >= %s AND m.commence_time < %s
ORDER BY m.commence_time, m.id
"""
_HIST_SQL = """
SELECT matchup_id, captured_at, home_spread, vegas_total, home_ml, away_ml, books
FROM game_odds_history WHERE sport = 'cfb' AND matchup_id = ANY(%s) ORDER BY matchup_id, captured_at
"""


# ── Pure rules ───────────────────────────────────────────────────────────────
def stale_limit_minutes(commence: datetime, now: datetime) -> int:
    hours_to_kick = (commence - now).total_seconds() / 3600
    return STALE_INSIDE_MIN if hours_to_kick <= NEAR_KICKOFF_HOURS else STALE_OUTSIDE_MIN


def is_stale(captured_at: datetime | None, commence: datetime, now: datetime) -> bool:
    if captured_at is None:
        return True
    return (now - captured_at).total_seconds() / 60 > stale_limit_minutes(commence, now)


def late_move(current: dict, reference: dict | None) -> list[str]:
    """Return the reasons a move since `reference` qualifies as a late move."""
    if not reference:
        return []
    reasons = []
    if current.get("home_spread") is not None and reference.get("home_spread") is not None:
        d = current["home_spread"] - reference["home_spread"]
        if abs(d) >= LATE_SPREAD_PTS:
            reasons.append(f"spread {reference['home_spread']:+g} -> {current['home_spread']:+g}")
    if current.get("vegas_total") is not None and reference.get("vegas_total") is not None:
        d = current["vegas_total"] - reference["vegas_total"]
        if abs(d) >= LATE_TOTAL_PTS:
            reasons.append(f"total {reference['vegas_total']:g} -> {current['vegas_total']:g}")
    if all(current.get(k) is not None and reference.get(k) is not None for k in ("home_ml", "away_ml")):
        d = (pw.vig_free_home(current["home_ml"], current["away_ml"]) - pw.vig_free_home(reference["home_ml"], reference["away_ml"])) * 100
        if abs(d) >= LATE_PROB_PP:
            reasons.append(f"home win prob {d:+.1f}pp")
    return reasons


def best_retail(trigger: str, game: dict, books: dict | None) -> dict | None:
    """Best same-proposition retail quote for the trigger's bet; None if no retail book quotes it."""
    if not books:
        return None
    sp = game["home_spread"]
    home_fav = sp < 0 if sp != 0 else game["home_ml"] < game["away_ml"]
    cands = []
    for b, v in books.items():
        if b not in RETAIL_BOOKS:
            continue
        if trigger in ("T1_g5_mid_fav_ml", "T5_sat_evening_g5_fav_ml"):
            k = "ml_home" if home_fav else "ml_away"
            if v.get(k) is not None:
                cands.append((pw.decimal_odds(v[k]), 0.0, b, None, v[k]))
        elif trigger in ("T3_small_fav_dog_spread",):
            k = "spread_away" if home_fav else "spread_home"
            if v.get(k) is not None and v.get(k + "_price") is not None:
                cands.append((pw.decimal_odds(v[k + "_price"]), v[k], b, v[k], v[k + "_price"]))  # more points better
        elif trigger in ("T2_fcs_fav_spread", "T6_mid_fav_spread"):
            k = "spread_home" if home_fav else "spread_away"
            if v.get(k) is not None and v.get(k + "_price") is not None:
                cands.append((pw.decimal_odds(v[k + "_price"]), v[k], b, v[k], v[k + "_price"]))  # less negative better
        elif trigger == "T4_big_fav_over":
            if v.get("total_line") is not None and v.get("over") is not None:
                cands.append((pw.decimal_odds(v["over"]), -v["total_line"], b, v["total_line"], v["over"]))  # lower total better
    if not cands:
        return None
    best = max(cands, key=lambda c: (c[1], c[0]))
    return {"book": best[2], "line": best[3], "price": best[4], "decimal": round(best[0], 4)}


def classify_status(game: dict, trigger: str, now: datetime, latest_capture: datetime | None,
                    frozen: dict | None, late_reasons: list[str], fires_now: bool) -> str:
    if game.get("completed") or game["commence_time"] <= now:
        return "started"
    if is_stale(latest_capture, game["commence_time"], now):
        return "stale"
    if not fires_now:
        return "dropped"
    if late_reasons:
        return "watch"
    if frozen and (game["commence_time"] - now).total_seconds() / 60 <= CUTOFF_MIN:
        return "final"
    return "current"


# ── Persistence ──────────────────────────────────────────────────────────────
def read_selections(path: Path) -> dict[tuple[str, int], dict]:
    out = {}
    if path.exists():
        for line in path.open(encoding="utf-8"):
            if line.strip():
                r = json.loads(line)
                out[(r["trigger"], int(r["matchup_id"]))] = r
    return out


def append_selections(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, default=str, sort_keys=True) + "\n")


# ── Board ────────────────────────────────────────────────────────────────────
def build_board(games: list[dict], history: dict[int, list[dict]], frozen: dict[tuple[str, int], dict], now: datetime) -> tuple[list[dict], list[dict]]:
    """Return (cards, new_selections). Pure given its inputs."""
    cards, new_rows = [], []
    for g in games:
        h = history.get(int(g["matchup_id"]), [])
        latest = h[-1] if h else None
        latest_capture = latest["captured_at"] if latest else g.get("odds_fetched_at")
        current = {k: (latest[k] if latest else g.get(k)) for k in ("home_spread", "vegas_total", "home_ml", "away_ml")}
        gcur = dict(g, **current)
        if gcur["home_spread"] is None or gcur["home_ml"] is None or gcur["away_ml"] is None:
            continue
        ref_time = g["commence_time"] - timedelta(hours=LATE_WINDOW_HOURS)
        reference = max((x for x in h if x["captured_at"] <= ref_time), key=lambda x: x["captured_at"], default=None)
        late = late_move(current, reference) if (now >= ref_time and reference is not None and latest is not None and latest is not reference) else []
        fires = set(pw.classify(gcur))
        stale = is_stale(latest_capture, g["commence_time"], now)
        for trig in sorted(fires | {k[0] for k in frozen if k[1] == int(g["matchup_id"])}):
            key = (trig, int(g["matchup_id"]))
            fz = frozen.get(key)
            status = classify_status(gcur, trig, now, latest_capture, fz, late, trig in fires)
            if fz is None and status in ("current", "watch") and trig in fires and now >= pw.trigger_registered_at(trig) and g["commence_time"] >= pw.trigger_registered_at(trig):
                grade_like = pw.grade(trig, dict(gcur, home_score=0, away_score=0))  # selection text + consensus price only
                fz = {
                    "board_version": BOARD_VERSION, "study_version": pw.STUDY_VERSION, "trigger": trig,
                    "matchup_id": int(g["matchup_id"]), "home": g["home"], "away": g["away"],
                    "commence_time": g["commence_time"].isoformat(), "frozen_at": now.isoformat(),
                    "captured_at": latest_capture.isoformat() if latest_capture else None,
                    "home_spread": current["home_spread"], "vegas_total": current["vegas_total"],
                    "home_ml": current["home_ml"], "away_ml": current["away_ml"],
                    "selection": grade_like["selection"], "consensus_decimal": grade_like["price"],
                    "best_retail": best_retail(trig, gcur, latest["books"] if latest else None),
                    "late_move_at_freeze": late,
                }
                new_rows.append(fz)
                frozen[key] = fz
                status = classify_status(gcur, trig, now, latest_capture, fz, late, True)
            drift = None
            if fz is not None:
                drift = {
                    "spread": None if current["home_spread"] is None or fz["home_spread"] is None else round(current["home_spread"] - fz["home_spread"], 1),
                    "total": None if current["vegas_total"] is None or fz["vegas_total"] is None else round(current["vegas_total"] - fz["vegas_total"], 1),
                    "fav_prob_pp": round((pw.vig_free_home(current["home_ml"], current["away_ml"]) - pw.vig_free_home(fz["home_ml"], fz["away_ml"])) * 100, 1),
                }
            cards.append({
                "trigger": trig, "matchup_id": int(g["matchup_id"]), "home": g["home"], "away": g["away"],
                "commence_time": g["commence_time"], "status": status, "stale": stale,
                "minutes_since_capture": None if latest_capture is None else round((now - latest_capture).total_seconds() / 60),
                "current": current, "frozen": fz, "drift": drift, "late_reasons": late,
                "best_retail_now": best_retail(trig, gcur, latest["books"] if latest else None) if not stale else None,
            })
    return cards, new_rows


def render(cards: list[dict], now: datetime) -> str:
    et = lambda ts: (ts - timedelta(hours=4)).strftime("%a %H:%M ET")
    lines = [f"# CFB pattern board - {BOARD_VERSION} / {pw.STUDY_VERSION}", f"generated {now.isoformat()}", "",
             "Status: current = fresh quote, trigger fires, selection frozen; final = inside the 90-minute cutoff; "
             "watch = late move since T-2h (treat as news); dropped = trigger no longer fires at the current line; "
             "stale = no fresh quote, no selection shown; started = kicked off.", "",
             "| kick | game | trigger | status | frozen selection | frozen best retail | now (spread/total) | drift | quote age | note |",
             "|---|---|---|---|---|---|---|---|---|---|"]
    for c in sorted(cards, key=lambda c: (c["commence_time"], c["trigger"])):
        fz = c["frozen"]; cur = c["current"]
        sel = fz["selection"] if fz else "-"
        br = fz["best_retail"] if fz else None
        brs = f"{br['book']} {br['line'] if br['line'] is not None else ''} ({br['price']:+d})" if br else "-"
        drift = "-"
        if c["drift"]:
            d = c["drift"]
            drift = f"sp {d['spread']:+g} / tot {d['total'] if d['total'] is None else format(d['total'], '+g')} / fav {d['fav_prob_pp']:+.1f}pp"
        note = "; ".join(c["late_reasons"]) if c["late_reasons"] else ""
        if c["status"] == "stale":
            note = f"line unavailable, last seen {c['minutes_since_capture']} min ago"
        lines.append(f"| {et(c['commence_time'])} | {c['away']} @ {c['home']} | {c['trigger']} | {c['status']} | {sel} | {brs} | {cur['home_spread']:+g} / {cur['vegas_total']} | {drift} | {c['minutes_since_capture']} min | {note} |")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-write", action="store_true")
    ap.add_argument("--selections", default=str(DEFAULT_SELECTIONS))
    ap.add_argument("--board", default=str(DEFAULT_BOARD))
    args = ap.parse_args(argv)
    now = datetime.now(timezone.utc)
    cfg = load_config()
    db = DatabaseManager(cfg.database_url, initialize_schema=False)
    games = [dict(r) for r in db.execute(_GAMES_SQL, (pw.SEASON, now - timedelta(hours=1), now + timedelta(hours=LOOKAHEAD_HOURS)))]
    hist: dict[int, list[dict]] = {}
    if games:
        for r in db.execute(_HIST_SQL, ([int(g["matchup_id"]) for g in games],)):
            hist.setdefault(int(r["matchup_id"]), []).append(dict(r))
    frozen = read_selections(Path(args.selections))
    cards, new_rows = build_board(games, hist, frozen, now)
    text = render(cards, now)
    print(text)
    counts = {}
    for c in cards:
        counts[c["status"]] = counts.get(c["status"], 0) + 1
    print(f"cards {len(cards)} {counts}; new selections frozen {len(new_rows)}")
    if not args.no_write:
        append_selections(Path(args.selections), new_rows)
        Path(args.board).parent.mkdir(parents=True, exist_ok=True)
        Path(args.board).write_text(text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
