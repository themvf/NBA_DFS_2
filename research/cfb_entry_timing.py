"""CFB entry-timing study: OPEN vs TRIGGER (recorded) vs CLOSE for each pick.

For every settled prospective CFB line alert this season we replay the SAME
selection (side/market) at three entry points and grade it against the final
score:

  OPEN    — the line/price in the opening game_odds_history snapshot
            (opening_history_id) the alert stored at creation.
  TRIGGER — the entry the paper record actually used (details_json exec_line /
            exec_decimal at the moment the alert fired).
  CLOSE   — the verified closing line/price (verified_clv_closes). This is the
            CLV benchmark: "what if we always just took the number the market
            settled on."

Spread/total outcome depends on the ENTRY LINE (the number you're locked into),
so open/trigger/close can produce different W/L/push on the same game. Moneyline
outcome is fixed by who won; only the PRICE (and thus P&L) changes with entry.

Read-only. Not a betting recommendation.
"""

from __future__ import annotations

import argparse
from collections import defaultdict

from config import load_config
from db.database import DatabaseManager
from model.line_alerts import (
    _cfb_market_snapshot,
    _nfl_line_outcome,
    _game_side_outcome,
)


def _american_to_decimal(american):
    try:
        price = float(american)
    except (TypeError, ValueError):
        return None
    if price == 0:
        return None
    return 1 + (100 / abs(price) if price < 0 else price / 100)


def _pct(n, d):
    return f"{n / d * 100:.1f}%" if d else "n/a"


def _load_books(db, history_id):
    if history_id is None:
        return None
    row = db.execute_one(
        "SELECT books FROM game_odds_history WHERE id = %s", (history_id,)
    )
    return row["books"] if row and row["books"] else None


def _verified_close_books(db, matchup_id):
    row = db.execute_one(
        """
        SELECT h.books FROM verified_clv_closes c
        JOIN game_odds_history h ON h.id = c.history_id
        WHERE c.sport = 'cfb' AND c.matchup_id = %s AND h.books IS NOT NULL
        """,
        (matchup_id,),
    )
    return row["books"] if row and row["books"] else None


def _line_entry(books, market, side, exec_book):
    """Return (user_line, home_line, decimal_odds) for a spread/total from a books snapshot.

    Line: prefer the exec book's exact quote; fall back to the deterministic
    lower-median market snapshot (same logic settlement uses).
    """
    home_line = user_line = decimal = None
    quote = (books or {}).get(exec_book) if exec_book else None
    if isinstance(quote, dict):
        if market == "spread":
            lk = "spread_home" if side == "home" else "spread_away"
            pk = "spread_home_price" if side == "home" else "spread_away_price"
            if quote.get(lk) is not None:
                user_line = float(quote[lk])
                home_line = user_line if side == "home" else -user_line
            decimal = _american_to_decimal(quote.get(pk))
        else:
            if quote.get("total_line") is not None:
                user_line = home_line = float(quote["total_line"])
            decimal = _american_to_decimal(quote.get("over" if side == "over" else "under"))
    if home_line is None:
        snap = _cfb_market_snapshot(books or {}, market)
        if snap:
            home_line = float(snap["line"])
            user_line = -home_line if (market == "spread" and side == "away") else home_line
    return user_line, home_line, decimal


def _ml_entry(books, side, exec_book):
    """Return decimal odds for a moneyline side from a books snapshot."""
    quote = (books or {}).get(exec_book) if exec_book else None
    if not isinstance(quote, dict):
        # fall back to any book carrying the ML
        for q in (books or {}).values():
            if isinstance(q, dict) and q.get("ml_home") is not None and q.get("ml_away") is not None:
                quote = q
                break
    if not isinstance(quote, dict):
        return None
    american = quote.get("ml_home") if side == "home" else quote.get("ml_away")
    return _american_to_decimal(american)


def _grade_pnl(market, side, entry_home_line, entry_decimal, hs, as_):
    """Return (outcome, pnl_units) for one entry. None if not gradable."""
    if entry_decimal is None:
        return None, None
    if market == "moneyline":
        outcome = _game_side_outcome("cfb", hs, as_, side)
    else:
        if entry_home_line is None:
            return None, None
        outcome = _nfl_line_outcome(market, side, entry_home_line, hs, as_)
    if outcome == "won":
        return outcome, entry_decimal - 1
    if outcome == "lost":
        return outcome, -1.0
    return outcome, 0.0  # push / void


class Tally:
    __slots__ = ("w", "l", "p", "v", "pnl", "stake", "n_priced")

    def __init__(self):
        self.w = self.l = self.p = self.v = 0
        self.pnl = 0.0
        self.stake = 0
        self.n_priced = 0

    def add(self, outcome, pnl):
        if outcome is None or pnl is None:
            return
        self.n_priced += 1
        if outcome == "won":
            self.w += 1
        elif outcome == "lost":
            self.l += 1
        elif outcome == "push":
            self.p += 1
        else:
            self.v += 1
        self.pnl += pnl
        if outcome in ("won", "lost", "push"):
            self.stake += 1

    def line(self, label):
        return (f"  {label:<9} {self.w}-{self.l}-{self.p}"
                f"{('/' + str(self.v) + 'v') if self.v else '':<4}"
                f"  win {_pct(self.w, self.w + self.l):>6}"
                f"  {self.pnl:+7.2f}u  ROI {_pct(self.pnl, self.stake):>7}"
                f"  (n={self.n_priced})")


def run(db, season):
    rows = db.execute(
        """
        SELECT a.id, a.alert_type, a.side, a.opening_history_id, a.trigger_history_id,
               a.details_json, a.matchup_id,
               m.home_score, m.away_score,
               COALESCE(a.details_json->>'market',
                        CASE WHEN a.alert_type LIKE 'spread%%' THEN 'spread'
                             WHEN a.alert_type LIKE 'total%%' THEN 'total'
                             ELSE 'moneyline' END) AS market
        FROM line_alerts a
        JOIN cfb_matchups m ON m.id = a.matchup_id
        WHERE a.sport = 'cfb' AND a.origin = 'prospective' AND m.season = %s
          AND m.completed = TRUE AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
        ORDER BY a.id
        """,
        (season,),
    )

    # market -> regime -> Tally
    tallies = defaultdict(lambda: {"open": Tally(), "trigger": Tally(), "close": Tally()})
    # count how often each entry beat the others on line value
    line_moves = defaultdict(lambda: {"open_vs_trigger": [], "trigger_vs_close": []})
    missing = defaultdict(lambda: {"open": 0, "close": 0})

    # favorite/underdog split for SPREAD, keyed by regime -> role -> Tally.
    # role is decided by the entry line of THAT regime (a side can be a
    # favorite at the open and a dog by the close if the line crosses zero).
    fav_split = {
        regime: {"favorite": Tally(), "underdog": Tally(), "pickem": Tally()}
        for regime in ("open", "trigger", "close")
    }

    def _role(market, side, home_line):
        """Favorite/underdog of the BET SIDE from its entry line (spread only)."""
        if market != "spread" or home_line is None:
            return None
        user_line = home_line if side == "home" else -home_line
        if abs(user_line) < 1e-9:
            return "pickem"
        return "favorite" if user_line < 0 else "underdog"

    for r in rows:
        market = r["market"]
        side = r["side"]
        hs, as_ = int(r["home_score"]), int(r["away_score"])
        details = r["details_json"] or {}
        exec_book = details.get("exec_book") or "draftkings"

        # --- TRIGGER (the recorded entry) ---
        if market == "moneyline":
            trig_dec = details.get("exec_decimal") or details.get("dk_decimal")
            trig_dec = float(trig_dec) if trig_dec is not None else None
            trig_home_line = None
        else:
            trig_dec = details.get("exec_decimal") or details.get("dk_decimal")
            trig_dec = float(trig_dec) if trig_dec is not None else None
            trig_home_line = details.get("entry_home_line")
            if trig_home_line is None and details.get("exec_line") is not None:
                el = float(details["exec_line"])
                trig_home_line = el if side in ("home", "over", "under") else -el
            trig_home_line = float(trig_home_line) if trig_home_line is not None else None
        o, p = _grade_pnl(market, side, trig_home_line, trig_dec, hs, as_)
        tallies[market]["trigger"].add(o, p)
        role = _role(market, side, trig_home_line)
        if role:
            fav_split["trigger"][role].add(o, p)

        # --- OPEN ---
        open_books = _load_books(db, r["opening_history_id"])
        if market == "moneyline":
            open_dec = _ml_entry(open_books, side, exec_book)
            open_home_line = None
        else:
            _, open_home_line, open_dec = _line_entry(open_books, market, side, exec_book)
        if open_dec is None:
            missing[market]["open"] += 1
        o, p = _grade_pnl(market, side, open_home_line, open_dec, hs, as_)
        tallies[market]["open"].add(o, p)
        role = _role(market, side, open_home_line)
        if role:
            fav_split["open"][role].add(o, p)

        # --- CLOSE (CLV benchmark) ---
        close_books = _verified_close_books(db, r["matchup_id"])
        if market == "moneyline":
            close_dec = _ml_entry(close_books, side, exec_book)
            close_home_line = None
        else:
            _, close_home_line, close_dec = _line_entry(close_books, market, side, exec_book)
        if close_dec is None:
            missing[market]["close"] += 1
        o, p = _grade_pnl(market, side, close_home_line, close_dec, hs, as_)
        tallies[market]["close"].add(o, p)
        role = _role(market, side, close_home_line)
        if role:
            fav_split["close"][role].add(o, p)

        # line movement diagnostics (spread/total only)
        if market in ("spread", "total"):
            if open_home_line is not None and trig_home_line is not None:
                line_moves[market]["open_vs_trigger"].append(trig_home_line - open_home_line)
            if trig_home_line is not None and close_home_line is not None:
                line_moves[market]["trigger_vs_close"].append(close_home_line - trig_home_line)

    print("=" * 82)
    print(f"CFB ENTRY-TIMING STUDY — season {season}  (settled prospective picks)")
    print("=" * 82)
    print("Same selection, three entry points. OPEN = opening snapshot,")
    print("TRIGGER = recorded paper entry, CLOSE = verified closing number (CLV).")
    print("Flat 1u per settled decision. 'v' = void (no-contest / tie).\n")

    for market in ("moneyline", "spread", "total"):
        t = tallies.get(market)
        if not t or t["trigger"].n_priced == 0:
            continue
        print(f"{market.upper()}")
        print(t["open"].line("OPEN"))
        print(t["trigger"].line("TRIGGER"))
        print(t["close"].line("CLOSE"))
        m = missing[market]
        if m["open"] or m["close"]:
            print(f"    (no priced entry: open={m['open']}, close={m['close']} — "
                  f"excluded from that regime's totals)")
        lm = line_moves.get(market)
        if lm and lm["open_vs_trigger"]:
            ot = lm["open_vs_trigger"]
            print(f"    home-line drift open->trigger: {sum(ot)/len(ot):+.2f} avg (n={len(ot)})")
        if lm and lm["trigger_vs_close"]:
            tc = lm["trigger_vs_close"]
            print(f"    home-line drift trigger->close: {sum(tc)/len(tc):+.2f} avg (n={len(tc)})")
        print()

    print("=" * 82)
    print("SPREAD picks split by FAVORITE vs UNDERDOG (role set by each regime's entry line)")
    print("=" * 82)
    print("Answers 'is betting the open on a favorite the best cell?' directly.\n")
    for regime in ("open", "trigger", "close"):
        print(f"{regime.upper()} entry")
        for role in ("favorite", "underdog", "pickem"):
            t = fav_split[regime][role]
            if t.n_priced:
                print(t.line(role))
        print()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=int, default=None)
    args = ap.parse_args()
    db = DatabaseManager(load_config().database_url or "", initialize_schema=False)
    season = args.season
    if season is None:
        row = db.execute_one("SELECT MAX(season) AS s FROM cfb_matchups")
        season = row["s"] if row else None
    if season is None:
        print("No CFB data found.")
        return
    run(db, season)


if __name__ == "__main__":
    main()
