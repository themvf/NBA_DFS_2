"""Best-price grading overlay (D1, Edge-Finding: execution accounting).

Grades settled ledger bets at the BEST captured per-book closing price
instead of the frozen consensus, answering: "what would these bets have
returned with disciplined line shopping?" Line shopping is worth 1-3%/bet
with zero predictive skill (the `sports-betting` package's market_maximum
convention; D-series review 2026-07-08) — this makes every edge study's
accounting realistic without touching the immutable ledger rows.

Non-negotiables (inherited from the standing disciplines):
  * The ledger is NEVER mutated. market_decimal stays the frozen consensus;
    best price is computed on the fly from game_odds_history.books.
  * Same-proposition only: a total is matched at the EXACT captured line
    (book total_line == bet line) — a different line is a different bet
    (the Herrera / comparison_status lesson). Moneylines match by side.
  * Same-subset comparison: frozen ROI and best-price ROI are both computed
    over the COVERED bets only (those with >= 1 matching per-book quote at
    the close). Uncovered bets (pre-2026-07-02 captures, moved lines,
    missing books) are counted and reported separately, never silently
    folded into either number.
  * Close convention matches model/clv_report.py: the LAST books-bearing
    capture at/before commence (the in-play guards freeze captures at first
    pitch/kickoff, so the last capture IS the close).

Covered bet types: moneyline (home/away/draw) and total (over/under at an
exact line) for mlb, soccer, tennis. draw_no_bet and first_scorer are out of
scope (no per-book DNB prices are captured; first-scorer already records the
best offered price at rating time).

Usage:
    python -m model.best_price                 # all three sports
    python -m model.best_price --sport mlb
    python -m model.best_price --since 2026-07-02
"""

from __future__ import annotations

import argparse
import logging
from collections import defaultdict

from config import load_config
from db.database import DatabaseManager
from model.soccer_bet_rating import american_to_decimal

logger = logging.getLogger(__name__)

_MATCHUP_TBL = {"mlb": "mlb_matchups", "soccer": "soccer_matchups", "tennis": "tennis_matches"}
_BETS_TBL = {"mlb": "mlb_bets", "soccer": "soccer_bets", "tennis": "tennis_bets"}
# game_odds_history.books side keys per moneyline side.
_ML_KEY = {"home": "ml_home", "away": "ml_away", "draw": "ml_draw"}

# Betting EXCHANGES are excluded from "best" everywhere: their feed odds
# exclude the 2-5% commission on winnings, so treating them as sportsbook
# prices overstates obtainable value (they dominated the naive best-book
# tally: matchbook won 78/175 MLB bets before this filter).
_EXCHANGES = {"matchbook", "smarkets", "betfair", "betfair_ex_eu", "betfair_ex_uk",
              "betfair_ex_au", "betfair_sb_uk"}
# Realistic execution tier: mainstream US-licensed retail books (what a US
# bettor can actually click). Everything else (EU/offshore like onexbet,
# betanysports, unibet_*) is reported only in the any-book upper bound.
_US_RETAIL = {"draftkings", "fanduel", "betmgm", "williamhill_us", "caesars",
              "betrivers", "fanatics", "espnbet", "hardrockbet", "ballybet",
              "betparx", "bovada"}


def _close_books_bulk(db: DatabaseManager, sport: str,
                      matchup_ids: list[int]) -> dict[int, dict]:
    """{matchup_id: books} of the last books-bearing capture at/before
    commence, for all requested games in ONE set-based query.

    Bulk on purpose: a per-matchup query loop both crawls over the network
    and repeatedly acquires locks while the 30-min capture cron's own
    schema/ingest transactions run — observed deadlocking live (2026-07-08).
    One DISTINCT ON pass takes the locks once."""
    if not matchup_ids:
        return {}
    rows = db.execute(
        f"""
        SELECT DISTINCT ON (h.matchup_id) h.matchup_id, h.books
        FROM game_odds_history h
        JOIN {_MATCHUP_TBL[sport]} m ON m.id = h.matchup_id
        WHERE h.sport = %s AND h.matchup_id = ANY(%s) AND h.books IS NOT NULL
          AND (m.commence_time IS NULL OR h.captured_at <= m.commence_time)
        ORDER BY h.matchup_id, h.captured_at DESC
        """,
        (sport, matchup_ids),
    )
    return {r["matchup_id"]: r["books"] for r in rows if r["books"]}


def best_close_price(books: dict, bet_type: str, side: str | None,
                     line: float | None, book_filter: set | None = None) -> dict | None:
    """Best (max-decimal) same-proposition price across the captured books.

    Exchanges are always excluded (see _EXCHANGES). Pass book_filter to
    additionally restrict to a tier (e.g. _US_RETAIL). Returns
    {book, american, decimal} or None when no book carries the exact
    proposition (side missing, or no book's total_line equals the bet line).
    """
    if not books or side is None:
        return None
    best: dict | None = None
    for book_key, b in books.items():
        if not isinstance(b, dict):
            continue
        if book_key in _EXCHANGES:
            continue
        if book_filter is not None and book_key not in book_filter:
            continue
        price = None
        if bet_type == "moneyline":
            price = b.get(_ML_KEY.get(side, ""))
        elif bet_type == "total":
            if line is None or b.get("total_line") is None:
                continue
            try:
                if float(b["total_line"]) != float(line):
                    continue  # different line = different proposition
            except (TypeError, ValueError):
                continue
            price = b.get(side)  # 'over' / 'under'
        if price is None:
            continue
        try:
            dec = american_to_decimal(int(price))
        except (TypeError, ValueError):
            continue
        if best is None or dec > best["decimal"]:
            best = {"book": book_key, "american": int(price), "decimal": dec}
    return best


def _bet_side(sport: str, bet: dict) -> str | None:
    """Derive the bet's side in books-JSONB terms from the ledger row."""
    label = (bet.get("selection_label") or "").strip()
    if bet["bet_type"] == "total":
        low = label.lower()
        side = bet.get("ij_side")
        if side in ("over", "under"):
            return side
        if low.startswith("over"):
            return "over"
        if low.startswith("under"):
            return "under"
        return None
    # moneyline
    if sport == "tennis":
        return bet.get("side") if bet.get("side") in ("home", "away") else None
    if label.lower() == "draw":
        return "draw"
    ij_side = bet.get("ij_side")
    if ij_side in ("home", "away", "draw"):
        return ij_side
    subj, home = bet.get("subject_team_id"), bet.get("home_team_id")
    if subj is not None and home is not None:
        return "home" if subj == home else "away"
    return None


def load_settled_bets(db: DatabaseManager, sport: str, since: str | None) -> list[dict]:
    """Settled won/lost moneyline + total bets with the fields side-derivation needs."""
    bets_tbl = _BETS_TBL[sport]
    matchup_col = "match_id" if sport == "tennis" else "matchup_id"
    side_col = "tb.side," if sport == "tennis" else "NULL AS side,"
    subj_col = "NULL AS subject_team_id," if sport == "tennis" else "tb.subject_team_id,"
    home_col = ("NULL AS home_team_id" if sport == "tennis"
                else "m.home_team_id")
    since_sql = "AND tb.created_at >= %s" if since else ""
    params: tuple = (since,) if since else ()
    return db.execute(
        f"""
        SELECT tb.id, tb.{matchup_col} AS matchup_id, tb.bet_type, tb.selection_label,
               tb.market_decimal, tb.status, tb.stars, tb.model_version,
               {side_col} {subj_col} {home_col},
               tb.inputs_json->>'side' AS ij_side,
               (tb.inputs_json->>'line')::float AS line
        FROM {bets_tbl} tb
        JOIN {_MATCHUP_TBL[sport]} m ON m.id = tb.{matchup_col}
        WHERE tb.bet_type IN ('moneyline', 'total')
          AND tb.status IN ('won', 'lost')
          AND tb.market_decimal IS NOT NULL
          {since_sql}
        ORDER BY tb.id
        """,
        params,
    )


def grade_sport(db: DatabaseManager, sport: str, since: str | None) -> dict:
    """Compute frozen vs best-price ROI on the covered subset; count the rest."""
    bets = load_settled_bets(db, sport, since)
    books_by_mid = _close_books_bulk(db, sport, sorted({b["matchup_id"] for b in bets}))
    covered: list[dict] = []
    n_no_books = 0      # no per-book close exists (e.g. pre-2026-07-02)
    n_no_match = 0      # books exist but no same-proposition quote (moved line)
    n_no_side = 0       # side underivable from the ledger row

    for b in bets:
        side = _bet_side(sport, b)
        if side is None:
            n_no_side += 1
            continue
        books = books_by_mid.get(b["matchup_id"])
        if books is None:
            n_no_books += 1
            continue
        # Coverage = a same-proposition quote exists at a US-retail book (the
        # realistic execution tier). The any-book upper bound is computed on
        # the SAME subset so the two ROIs stay comparable.
        retail = best_close_price(books, b["bet_type"], side, b.get("line"),
                                  book_filter=_US_RETAIL)
        if retail is None:
            n_no_match += 1
            continue
        anybook = best_close_price(books, b["bet_type"], side, b.get("line")) or retail
        covered.append({**b, "retail": retail, "anybook": anybook})

    def _roi(key_dec) -> tuple[float, float] | None:
        if not covered:
            return None
        profits = [(key_dec(c) - 1.0) if c["status"] == "won" else -1.0 for c in covered]
        return sum(profits), sum(profits) / len(profits)

    frozen = _roi(lambda c: float(c["market_decimal"]))
    retail = _roi(lambda c: c["retail"]["decimal"])
    anybook = _roi(lambda c: c["anybook"]["decimal"])
    # Execution uplift independent of outcomes: how much better was the best
    # US-retail book than the frozen consensus, per bet, at the close?
    uplift = ([c["retail"]["decimal"] / float(c["market_decimal"]) - 1.0 for c in covered]
              if covered else [])
    uplift.sort()
    by_type: dict[str, list[dict]] = defaultdict(list)
    for c in covered:
        by_type[c["bet_type"]].append(c)

    return {
        "sport": sport, "n_bets": len(bets), "covered": covered,
        "n_covered": len(covered), "n_no_books": n_no_books,
        "n_no_match": n_no_match, "n_no_side": n_no_side,
        "frozen": frozen, "retail": retail, "anybook": anybook,
        "avg_uplift": (sum(uplift) / len(uplift)) if uplift else None,
        "med_uplift": (uplift[len(uplift) // 2] if uplift else None),
        "by_type": by_type,
    }


def report(db: DatabaseManager, sports: list[str], since: str | None) -> None:
    import time as _time

    import psycopg2.errors

    print("=== Best-price grading (D1) — frozen consensus vs best captured book at the close ===")
    print("    Same covered subset for both ROIs; uncovered bets reported separately, never folded in.\n")
    for sport in sports:
        # The 30-min capture cron's ingest/schema transactions can deadlock a
        # concurrent reader; one short-backoff retry clears it (read-only job,
        # safe to re-run).
        try:
            r = grade_sport(db, sport, since)
        except psycopg2.errors.DeadlockDetected:
            logger.warning("deadlock with concurrent capture — retrying %s in 10s", sport)
            _time.sleep(10)
            r = grade_sport(db, sport, since)
        print(f"--- {sport.upper()} — {r['n_bets']} settled ML/total bets"
              + (f" since {since}" if since else "") + " ---")
        print(f"    covered={r['n_covered']} (US-retail quote exists)  "
              f"no-books={r['n_no_books']} (pre-per-book capture)  "
              f"no-retail-quote/line-moved={r['n_no_match']}  side-underivable={r['n_no_side']}")
        if not r["n_covered"]:
            print("    (no covered bets — nothing to grade)\n")
            continue
        f_tot, f_avg = r["frozen"]
        rt_tot, rt_avg = r["retail"]
        ab_tot, ab_avg = r["anybook"]
        n = r["n_covered"]
        print(f"    frozen consensus ROI: {f_tot:+.2f}u over {n} bets ({f_avg*100:+.2f}%/bet)")
        print(f"    best US-retail  ROI: {rt_tot:+.2f}u over {n} bets ({rt_avg*100:+.2f}%/bet)   <- realistic")
        print(f"    best any-book   ROI: {ab_tot:+.2f}u over {n} bets ({ab_avg*100:+.2f}%/bet)   <- upper bound (excl. exchanges)")
        print(f"    US-retail execution uplift vs frozen: mean {r['avg_uplift']*100:+.2f}%/bet, "
              f"median {r['med_uplift']*100:+.2f}%/bet")
        for bt, rows in sorted(r["by_type"].items()):
            fp = [(float(c["market_decimal"]) - 1.0) if c["status"] == "won" else -1.0 for c in rows]
            rp = [(c["retail"]["decimal"] - 1.0) if c["status"] == "won" else -1.0 for c in rows]
            print(f"      {bt:10} n={len(rows):4}  frozen={sum(fp)/len(fp)*100:+.2f}%/bet  "
                  f"retail-best={sum(rp)/len(rp)*100:+.2f}%/bet")
        print()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Best-price grading overlay (D1)")
    parser.add_argument("--sport", choices=["mlb", "soccer", "tennis", "all"], default="all")
    parser.add_argument("--since", default=None, help="Only bets created on/after this date")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    sports = ["mlb", "soccer", "tennis"] if args.sport == "all" else [args.sport]
    report(db, sports, args.since)
