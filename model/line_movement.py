"""Line movement report — open → close sharp-movement detection (Edge-Finding P2).

Reads the game_odds_history capture trail (30-min cadence in season) and, per
game, reports how the market moved between our first pre-game capture (open)
and the last capture before first pitch (close — captures stop at commence
per the in-play guard):

  * consensus vig-free P(home) open → close (probability points)
  * total line open → close (uses the unrounded vegas_total_raw when present;
    the 0.5-rounded column hides half-point moves across key numbers)
  * max 30-min jump — the largest single-interval consensus move; fast
    synchronized moves are the steam signature, slow drift is position
    balancing
  * Pinnacle vs retail gap at the close (needs the per-book `books` JSONB,
    captured from 2026-07-02 on) — when the sharp book sits off retail
    consensus, the gap side is the sharp side
  * steam flag — a single interval where >= _STEAM_MIN_BOOKS books moved the
    same direction by >= _STEAM_MIN_MOVE probability points (per-book data
    required)

Consensus-only rows (history before 2026-07-02) still get open/close movement
and max-jump; the per-book columns show "—".

Usage:
    python -m model.line_movement                     # mlb, last 7 days
    python -m model.line_movement --sport nba --days 30
    python -m model.line_movement --date 2026-07-02
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict

from config import load_config
from db.database import DatabaseManager
from model.soccer_bet_rating import american_to_prob

logger = logging.getLogger(__name__)

_STEAM_MIN_BOOKS = 3     # books moving together within one capture interval
_STEAM_MIN_MOVE = 0.015  # >= 1.5 prob points per book, same direction

# sport -> matchup table (for commence_time; captures already stop at commence
# via the in-play guard, but the join keeps the report honest if old in-play
# rows exist from before the guard).
_MATCHUP_TBL = {
    "mlb": "mlb_matchups",
    "nba": "nba_matchups",
    "soccer": "soccer_matchups",
    "tennis": "tennis_matches",
}


def _book_fair_home(book: dict) -> float | None:
    """Vig-free P(home) from one book's moneyline (3-way when ml_draw present)."""
    h, a = book.get("ml_home"), book.get("ml_away")
    if h is None or a is None:
        return None
    try:
        ph, pa = american_to_prob(int(h)), american_to_prob(int(a))
        d = book.get("ml_draw")
        pd = american_to_prob(int(d)) if d is not None else 0.0
    except (TypeError, ValueError):
        return None
    total = ph + pa + pd
    return ph / total if total > 0 else None


def _steam_scan(snaps: list[dict]) -> tuple[int, float] | None:
    """Largest synchronized per-book move across consecutive captures.

    Returns (n_books_moved_together, avg_move) for the strongest interval that
    clears the steam thresholds, or None. Requires per-book data on both ends
    of an interval.
    """
    best: tuple[int, float] | None = None
    for prev, cur in zip(snaps, snaps[1:]):
        pb, cb = prev.get("books"), cur.get("books")
        if not pb or not cb:
            continue
        moves = []
        for key in set(pb) & set(cb):
            p0, p1 = _book_fair_home(pb[key]), _book_fair_home(cb[key])
            if p0 is not None and p1 is not None:
                moves.append(p1 - p0)
        up = [m for m in moves if m >= _STEAM_MIN_MOVE]
        down = [m for m in moves if m <= -_STEAM_MIN_MOVE]
        side = up if len(up) >= len(down) else down
        if len(side) >= _STEAM_MIN_BOOKS:
            cand = (len(side), sum(side) / len(side))
            if best is None or len(side) > best[0]:
                best = cand
    return best


def report(db: DatabaseManager, sport: str, days: int, date_filter: str | None) -> None:
    matchup_tbl = _MATCHUP_TBL[sport]
    where = "h.game_date = %s" if date_filter else f"h.game_date >= CURRENT_DATE - INTERVAL '{int(days)} days'"
    params: tuple = (date_filter,) if date_filter else ()
    rows = db.execute(
        f"""
        SELECT h.matchup_id, h.game_date, h.home_team_name, h.away_team_name,
               h.captured_at, h.vegas_prob_home,
               COALESCE(h.vegas_total_raw, h.vegas_total) AS total,
               h.books, m.commence_time
        FROM game_odds_history h
        LEFT JOIN {matchup_tbl} m ON m.id = h.matchup_id
        WHERE h.sport = %s AND {where}
          AND h.vegas_prob_home IS NOT NULL
          AND (m.commence_time IS NULL OR h.captured_at <= m.commence_time)
        ORDER BY h.matchup_id, h.captured_at
        """,
        (sport, *params),
    )
    by_game: dict[int, list[dict]] = defaultdict(list)
    for r in rows:
        by_game[r["matchup_id"]].append(r)

    games = []
    for mid, snaps in by_game.items():
        if len(snaps) < 2:
            continue
        o, c = snaps[0], snaps[-1]
        move = float(c["vegas_prob_home"]) - float(o["vegas_prob_home"])
        max_jump = max(
            (abs(float(b["vegas_prob_home"]) - float(a["vegas_prob_home"]))
             for a, b in zip(snaps, snaps[1:])),
            default=0.0,
        )
        total_move = (float(c["total"]) - float(o["total"])
                      if o["total"] is not None and c["total"] is not None else None)
        pin_gap = None
        if c["books"] and "pinnacle" in c["books"]:
            pin_fair = _book_fair_home(c["books"]["pinnacle"])
            retail = [_book_fair_home(b) for k, b in c["books"].items() if k != "pinnacle"]
            retail = [p for p in retail if p is not None]
            if pin_fair is not None and retail:
                pin_gap = pin_fair - sum(retail) / len(retail)
        games.append({
            "date": str(o["game_date"]), "label": f"{c['away_team_name']} @ {c['home_team_name']}",
            "n": len(snaps), "open_p": float(o["vegas_prob_home"]),
            "close_p": float(c["vegas_prob_home"]), "move": move, "max_jump": max_jump,
            "open_total": o["total"], "total_move": total_move,
            "pin_gap": pin_gap, "steam": _steam_scan(snaps),
        })

    games.sort(key=lambda g: abs(g["move"]), reverse=True)
    print(f"\n=== {sport.upper()} line movement — {len(games)} games, "
          f"{'date ' + date_filter if date_filter else f'last {days} days'} "
          f"(sorted by |open→close move|) ===")
    print("    NOTE: consensus history before 2026-07-02 carries the arithmetic-"
          "American-averaging bug — apparent 20-40pp 'moves' on those dates are "
          "capture noise, not market movement. Trust movement (and all per-book "
          "columns) from 2026-07-02 on.\n")
    print(f"{'date':<11}{'game':<28}{'caps':>4}  {'P(home) open→close':<22}"
          f"{'move':>7}{'maxjump':>8}  {'total o→Δ':<12}{'pin gap':>8}  steam")
    for g in games:
        total_str = (f"{float(g['open_total']):.2f} {g['total_move']:+.2f}"
                     if g["total_move"] is not None else "—")
        pin_str = f"{g['pin_gap']*100:+.1f}pp" if g["pin_gap"] is not None else "—"
        steam_str = (f"YES ({g['steam'][0]} books {g['steam'][1]*100:+.1f}pp)"
                     if g["steam"] else "—")
        print(f"{g['date']:<11}{g['label']:<28}{g['n']:>4}  "
              f"{g['open_p']*100:5.1f}% → {g['close_p']*100:5.1f}%      "
              f"{g['move']*100:+6.1f}pp{g['max_jump']*100:+7.1f}pp  "
              f"{total_str:<12}{pin_str:>8}  {steam_str}")
    if not games:
        print("  (no games with >= 2 pre-game captures in the window)")
    print()


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")  # Windows cp1252 console
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Open→close line movement report")
    parser.add_argument("--sport", choices=sorted(_MATCHUP_TBL), default="mlb")
    parser.add_argument("--days", type=int, default=7, help="Look-back window (days)")
    parser.add_argument("--date", help="Single game date YYYY-MM-DD")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    report(db, args.sport, args.days, args.date)
