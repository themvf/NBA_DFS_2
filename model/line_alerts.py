"""Sharp line-movement alerts — detect, notify, and AUDIT (Edge-Finding P2).

Runs after every odds capture. Two detectors over game_odds_history:

  * **pinnacle_divergence** — Pinnacle's vig-free probability sits >=
    _PIN_GAP_MIN_PP off the retail-consensus probability on some side of an
    upcoming game. Pinnacle is the sharp reference; the side it prices HIGHER
    than retail is the sharp side, and retail is offering a stale price on it.
  * **steam** — between the last two captures, >= _STEAM_MIN_BOOKS books moved
    the same side by >= _STEAM_MIN_MOVE_PP. Synchronized moves are informed
    money; solo moves are book position management.

Every alert is an IMMUTABLE ledger row frozen at trigger time (first breach
only — re-scans never rewrite it), then graded by ``settle``:

    clv_pp  — close_prob − alert_prob: did the market close toward the flagged
              side? The primary audit metric; converges in days.
    outcome — did the flagged side win the game (soccer graded on the
              90-minute regulation score, matching betting convention).

``report`` prints the backtest by sport × alert type. If an alert type shows
no positive CLV, its thresholds are noise — retire or retune them; that is the
whole point of making alerts a ledger instead of a toast.

Notifications: if TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID are set, new alerts
are pushed via the Telegram bot API. Absent secrets = silent skip; the ledger
row is written regardless, so the audit never depends on delivery.

Usage:
    python -m model.line_alerts --sport mlb          # scan + settle + notify
    python -m model.line_alerts --sport soccer
    python -m model.line_alerts --report             # backtest, all sports
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

import requests

from config import load_config
from db.database import DatabaseManager
from model.line_movement import _MATCHUP_TBL, _book_fair_home

logger = logging.getLogger(__name__)

_PIN_GAP_MIN_PP = 2.0     # Pinnacle vs retail consensus, probability points
_STEAM_MIN_BOOKS = 3      # books moving together between consecutive captures
_STEAM_MIN_MOVE_PP = 1.5  # per-book move threshold, probability points

# Grading sources per sport: (home score col, away score col). Soccer uses the
# 90-minute regulation score — a knockout tie decided in extra time is a DRAW
# for market purposes (the Belgium 3-2 aet lesson). Tennis uses the winner col.
_SCORE_COLS = {
    "mlb": ("home_score", "away_score"),
    "nba": ("home_score", "away_score"),
    "soccer": ("COALESCE(reg_home_score, home_score)", "COALESCE(reg_away_score, away_score)"),
}


def _book_fair_side(book: dict, side: str) -> float | None:
    """Vig-free P(side) from one book's (2- or 3-way) moneyline."""
    try:
        from model.soccer_bet_rating import american_to_prob
        legs = {}
        for leg, key in (("home", "ml_home"), ("away", "ml_away"), ("draw", "ml_draw")):
            v = book.get(key)
            if v is not None:
                legs[leg] = american_to_prob(int(v))
        if side not in legs:
            return None
        total = sum(legs.values())
        return legs[side] / total if total > 0 else None
    except (TypeError, ValueError):
        return None


def _retail_fair_side(books: dict, side: str) -> float | None:
    """Mean vig-free P(side) across all non-Pinnacle books."""
    probs = [p for key, b in books.items() if key != "pinnacle"
             for p in [_book_fair_side(b, side)] if p is not None]
    return sum(probs) / len(probs) if probs else None


def _sides(books: dict) -> list[str]:
    has_draw = any("ml_draw" in b for b in books.values())
    return ["home", "away", "draw"] if has_draw else ["home", "away"]


def _notify(alerts: list[dict]) -> None:
    """Push new alerts via Telegram if configured; silent no-op otherwise."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id or not alerts:
        return
    for a in alerts:
        text = (f"🚨 {a['sport'].upper()} {a['alert_type']}: {a['matchup']}\n"
                f"side={a['side']}  retail={a['alert_prob']*100:.1f}%  "
                f"sharp={a['sharp_prob']*100:.1f}%" if a.get("sharp_prob") is not None else
                f"🚨 {a['sport'].upper()} {a['alert_type']}: {a['matchup']} side={a['side']}")
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": text},
                timeout=10,
            )
        except requests.RequestException as e:
            logger.warning("Telegram notify failed: %s", e)


def scan(db: DatabaseManager, sport: str) -> int:
    """Detect breaches on UPCOMING games' latest captures. Returns new alerts."""
    matchup_tbl = _MATCHUP_TBL[sport]
    rows = db.execute(
        f"""
        SELECT DISTINCT ON (h.matchup_id)
               h.matchup_id, h.game_date, h.home_team_name, h.away_team_name,
               h.captured_at, h.capture_key, h.books, m.commence_time
        FROM game_odds_history h
        JOIN {matchup_tbl} m ON m.id = h.matchup_id
        WHERE h.sport = %s AND h.books IS NOT NULL
          AND m.commence_time > NOW()
        ORDER BY h.matchup_id, h.captured_at DESC
        """,
        (sport,),
    )
    new_alerts: list[dict] = []
    for r in rows:
        books = r["books"] or {}
        label = f"{r['away_team_name']} @ {r['home_team_name']}"
        # ── Pinnacle divergence ──
        pin = books.get("pinnacle")
        if pin:
            for side in _sides(books):
                sharp = _book_fair_side(pin, side)
                retail = _retail_fair_side(books, side)
                if sharp is None or retail is None:
                    continue
                gap_pp = (sharp - retail) * 100
                if gap_pp >= _PIN_GAP_MIN_PP:
                    new_alerts.extend(_insert(
                        db, sport=sport, r=r, label=label,
                        alert_type="pinnacle_divergence", side=side,
                        alert_prob=retail, sharp_prob=sharp,
                        details={"gap_pp": round(gap_pp, 2),
                                 "n_books": len(books)},
                    ))
        # ── Steam (needs the previous capture) ──
        prev = db.execute_one(
            """
            SELECT books FROM game_odds_history
            WHERE sport = %s AND matchup_id = %s AND captured_at < %s
              AND books IS NOT NULL
            ORDER BY captured_at DESC LIMIT 1
            """,
            (sport, r["matchup_id"], r["captured_at"]),
        )
        if prev and prev["books"]:
            pb = prev["books"]
            for side in _sides(books):
                moves = []
                for key in set(pb) & set(books):
                    p0 = _book_fair_side(pb[key], side)
                    p1 = _book_fair_side(books[key], side)
                    if p0 is not None and p1 is not None:
                        moves.append((p1 - p0) * 100)
                movers = [m for m in moves if m >= _STEAM_MIN_MOVE_PP]
                if len(movers) >= _STEAM_MIN_BOOKS:
                    retail = _retail_fair_side(books, side)
                    pin_p = _book_fair_side(books["pinnacle"], side) if "pinnacle" in books else None
                    new_alerts.extend(_insert(
                        db, sport=sport, r=r, label=label,
                        alert_type="steam", side=side,
                        alert_prob=retail, sharp_prob=pin_p,
                        details={"books_moved": len(movers),
                                 "avg_move_pp": round(sum(movers) / len(movers), 2)},
                    ))
    if new_alerts:
        print(f"Line alerts ({sport}): {len(new_alerts)} new — "
              + ", ".join(f"{a['alert_type']}:{a['matchup']}/{a['side']}" for a in new_alerts))
        _notify(new_alerts)
    return len(new_alerts)


def _insert(db, *, sport, r, label, alert_type, side, alert_prob, sharp_prob, details) -> list[dict]:
    """First-breach insert; returns [alert] only when a NEW row was created."""
    rows = db.execute(
        """
        INSERT INTO line_alerts (sport, matchup_id, game_date, matchup, commence_time,
                                 alert_type, side, capture_key, alert_prob, sharp_prob, details_json)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (sport, matchup_id, alert_type, side) DO NOTHING
        RETURNING id
        """,
        (sport, r["matchup_id"], r["game_date"], label, r["commence_time"],
         alert_type, side, r["capture_key"], alert_prob, sharp_prob, json.dumps(details)),
    )
    if not rows:
        return []
    return [{"sport": sport, "matchup": label, "alert_type": alert_type,
             "side": side, "alert_prob": alert_prob, "sharp_prob": sharp_prob}]


def settle(db: DatabaseManager, sport: str) -> int:
    """Grade alerts whose games have started: CLV always, outcome when scored."""
    matchup_tbl = _MATCHUP_TBL[sport]
    open_alerts = db.execute(
        "SELECT * FROM line_alerts WHERE sport = %s AND settled_at IS NULL "
        "AND commence_time IS NOT NULL AND commence_time <= NOW()",
        (sport,),
    )
    graded = 0
    for a in open_alerts:
        # CLV: vig-free P(side) at the last pre-commence per-book capture.
        close = db.execute_one(
            """
            SELECT books FROM game_odds_history
            WHERE sport = %s AND matchup_id = %s AND books IS NOT NULL
              AND captured_at <= %s
            ORDER BY captured_at DESC LIMIT 1
            """,
            (sport, a["matchup_id"], a["commence_time"]),
        )
        close_prob = None
        if close and close["books"]:
            close_prob = _retail_fair_side(close["books"], a["side"])
        clv_pp = ((close_prob - float(a["alert_prob"])) * 100
                  if close_prob is not None and a["alert_prob"] is not None else None)

        # Outcome from the final score / winner (soccer: 90' regulation score).
        outcome = None
        if sport == "tennis":
            m = db.execute_one(f"SELECT winner FROM {matchup_tbl} WHERE id = %s", (a["matchup_id"],))
            if m and m["winner"] in ("home", "away"):
                outcome = "won" if m["winner"] == a["side"] else "lost"
            elif m and m["winner"] == "retired":
                outcome = "void"
        else:
            hs_col, as_col = _SCORE_COLS[sport]
            m = db.execute_one(
                f"SELECT {hs_col} AS hs, {as_col} AS as_ FROM {matchup_tbl} WHERE id = %s",
                (a["matchup_id"],),
            )
            if m and m["hs"] is not None and m["as_"] is not None:
                hs, as_ = int(m["hs"]), int(m["as_"])
                winner = "home" if hs > as_ else "away" if as_ > hs else "draw"
                outcome = "won" if winner == a["side"] else "lost"

        # Settle once we have at least the CLV grade; outcome may lag scores
        # and is filled in the same pass on a later run if still NULL then.
        if clv_pp is None and outcome is None:
            continue
        db.execute(
            "UPDATE line_alerts SET close_prob = %s, clv_pp = %s, outcome = %s, "
            "settled_at = CASE WHEN %s::text IS NOT NULL THEN NOW() ELSE settled_at END "
            "WHERE id = %s",
            (close_prob, clv_pp, outcome, outcome, a["id"]),
        )
        graded += 1
    if graded:
        print(f"Line alerts ({sport}): {graded} graded")
    return graded


def report(db: DatabaseManager) -> None:
    """The audit: does each alert type beat the close, and win at the flagged rate?"""
    rows = db.execute(
        """
        SELECT sport, alert_type, COUNT(*) n,
               COUNT(*) FILTER (WHERE clv_pp IS NOT NULL) n_clv,
               ROUND(AVG(clv_pp)::numeric, 2) avg_clv_pp,
               ROUND(AVG((clv_pp > 0)::int)::numeric, 2) beat_close,
               COUNT(*) FILTER (WHERE outcome IN ('won','lost')) n_out,
               ROUND(AVG((outcome = 'won')::int)
                     FILTER (WHERE outcome IN ('won','lost'))::numeric, 3) win_rate,
               ROUND(AVG(alert_prob)
                     FILTER (WHERE outcome IN ('won','lost'))::numeric, 3) implied_rate
        FROM line_alerts
        GROUP BY sport, alert_type ORDER BY sport, alert_type
        """
    )
    print("\n=== Line-alert backtest — CLV (beat the close?) + outcomes (win at the flagged rate?) ===")
    if not rows:
        print("  (no alerts recorded yet)")
    for r in rows:
        print(f"  {r['sport']:<8}{r['alert_type']:<22} n={r['n']:>4}  "
              f"CLV: n={r['n_clv']} avg={r['avg_clv_pp'] or 0:+}pp beat-close={r['beat_close'] or 0}  "
              f"outcomes: n={r['n_out']} win={r['win_rate']} implied={r['implied_rate']}")
    print()


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")  # Windows cp1252 console
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Sharp line-movement alerts + audit")
    parser.add_argument("--sport", choices=sorted(_MATCHUP_TBL), help="Scan + settle one sport")
    parser.add_argument("--report", action="store_true", help="Print the backtest")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    if args.sport:
        scan(db, args.sport)
        settle(db, args.sport)
    if args.report or not args.sport:
        report(db)
