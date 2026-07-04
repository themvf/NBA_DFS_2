"""Settle tennis bets from tennis-data.co.uk results (the only tennis source that
carries set/game scores AND covers both tours).

The Odds API provides odds but NOT tennis results (verified: /scores stays null
even hours after a match ends, and completed matches drop off). tennis-data.co.uk
publishes weekly post-event Excel files with Winner/Loser, sets, per-set games,
and closing odds — everything needed to settle moneyline now and totals/handicap
later.  It LAGS (results appear a few days after a match), so bets settle late,
never instantly; that's the documented tradeoff of a fully-automated, no-manual
pipeline.

Name matching: tennis-data uses "Surname Initial." (``Marozsan F.``) while the
Odds API uses "First Last" (``Fabian Marozsan``) — matched on
(normalized-surname, first-initial), which handles multi-word surnames
(``Davidovich Fokina A.`` ↔ ``Alejandro Davidovich Fokina``) and accents.

Usage:
    python -m ingest.tennis_results               # settle current-year ATP+WTA
    python -m ingest.tennis_results --year 2026
"""

from __future__ import annotations

import argparse
import io
import logging
import unicodedata
from datetime import date, datetime, timedelta

import requests

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0"}
# (tour, url-template). WTA lives under /{year}w/.
_TOURS = {
    "ATP": "http://www.tennis-data.co.uk/{year}/{year}.xlsx",
    "WTA": "http://www.tennis-data.co.uk/{year}w/{year}.xlsx",
}


def _norm(text: str) -> str:
    t = unicodedata.normalize("NFKD", str(text or "")).encode("ascii", "ignore").decode("ascii")
    return "".join(ch for ch in t.lower() if ch.isalnum())


def _key_oddsapi(name: str) -> tuple[str, str] | None:
    """(surname_norm, first_initial) from an Odds-API "First [Middle] Last" name."""
    parts = [p for p in str(name or "").split() if p]
    if len(parts) < 2:
        return None
    initial = _norm(parts[0])[:1]
    surname = _norm("".join(parts[1:]))
    return (surname, initial) if surname and initial else None


def _key_tennisdata(name: str) -> tuple[str, str] | None:
    """(surname_norm, first_initial) from a tennis-data "Surname [Parts] X." name."""
    parts = [p for p in str(name or "").split() if p]
    if len(parts) < 2:
        return None
    initial = _norm(parts[-1])[:1]            # trailing "F." → f
    surname = _norm("".join(parts[:-1]))
    return (surname, initial) if surname and initial else None


def _games(row, prefix: str, best_of: int) -> int:
    """Sum a player's games across sets (prefix 'W' or 'L'). NaN sets skipped."""
    total = 0
    for i in range(1, 6):
        v = row.get(f"{prefix}{i}")
        try:
            if v is not None and v == v:  # not NaN
                total += int(v)
        except (TypeError, ValueError):
            continue
    return total


def settle_tour(db: DatabaseManager, tour: str, year: int) -> tuple[int, int]:
    """Fetch one tour's results, write match results + settle moneyline bets.
    Returns (matches_updated, bets_settled)."""
    import pandas as pd

    # Index this tour's unsettled matches first — skip the ~230KB xlsx download
    # entirely once nothing is pending (this now runs every 15 min via
    # refresh_tennis_settlement.yml, so avoiding needless fetches matters).
    rows = db.execute(
        """SELECT id, match_date, home_player, away_player
           FROM tennis_matches WHERE tour = %s AND winner IS NULL""",
        (tour,),
    )
    if not rows:
        return 0, 0

    url = _TOURS[tour].format(year=year)
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=90)
        resp.raise_for_status()
        df = pd.read_excel(io.BytesIO(resp.content))
    except Exception as exc:  # noqa: BLE001 — network/parse both non-fatal
        logger.warning("tennis-data %s fetch failed: %s", tour, exc)
        return 0, 0

    index: dict[frozenset, list[dict]] = {}
    for m in rows:
        kh, ka = _key_oddsapi(m["home_player"]), _key_oddsapi(m["away_player"])
        if not kh or not ka:
            continue
        index.setdefault(frozenset((kh, ka)), []).append(m)

    matches_updated = bets_settled = 0
    for _, r in df.iterrows():
        if not str(r.get("Tournament", "")).strip():
            continue
        kw, kl = _key_tennisdata(r.get("Winner")), _key_tennisdata(r.get("Loser"))
        if not kw or not kl:
            continue
        cands = index.get(frozenset((kw, kl)))
        if not cands:
            continue

        # HARD date window — not just disambiguation. The same two players meet
        # across tournaments (Bencic–Kalinskaya at Eastbourne one week, Wimbledon
        # the next), and the results file carries the whole season: without this
        # guard a single-candidate pair match grabbed a WEEKS-OLD result and
        # wrote a winner onto a match that hadn't been played yet (2026-07-02).
        # A result may only settle a match dated within ±2 days of it; undated
        # result rows are skipped entirely.
        rd = r.get("Date")
        rd = rd.date() if isinstance(rd, datetime) else rd
        if not isinstance(rd, date):
            continue
        cands = [m for m in cands if abs((m["match_date"] - rd).days) <= 2]
        if not cands:
            continue
        match = min(cands, key=lambda m: abs((m["match_date"] - rd).days))

        # Orientation: is the home_player the Winner?
        home_is_winner = _key_oddsapi(match["home_player"]) == kw
        best_of = int(r.get("Best of") or 3)
        w_sets, l_sets = int(r.get("Wsets") or 0), int(r.get("Lsets") or 0)
        w_games, l_games = _games(r, "W", best_of), _games(r, "L", best_of)
        comment = str(r.get("Comment", "") or "")

        if home_is_winner:
            home_sets, away_sets, home_games, away_games, winner = w_sets, l_sets, w_games, l_games, "home"
        else:
            home_sets, away_sets, home_games, away_games, winner = l_sets, w_sets, l_games, w_games, "away"
        # Retirement/walkover: the advancer is still the Winner column, so
        # moneyline settles normally by `winner`. Totals/handicap would void on a
        # retirement — handled when those markets are rated (not yet).
        _ = comment

        db.execute(
            """UPDATE tennis_matches
               SET home_sets=%s, away_sets=%s, home_games=%s, away_games=%s, winner=%s
               WHERE id=%s""",
            (home_sets, away_sets, home_games, away_games, winner, match["id"]),
        )
        matches_updated += 1

        # Settle pending moneyline bets on this match.
        bets = db.execute(
            "SELECT id, side FROM tennis_bets WHERE match_id=%s AND status='pending' AND bet_type='moneyline'",
            (match["id"],),
        )
        for b in bets:
            status = "won" if b["side"] == winner else "lost"
            detail = f"{r.get('Winner')} d. {r.get('Loser')} {w_sets}-{l_sets}"
            db.execute(
                "UPDATE tennis_bets SET status=%s, result_detail=%s, settled_at=NOW() WHERE id=%s",
                (status, detail, b["id"]),
            )
            bets_settled += 1

    print(f"Tennis {tour}: {matches_updated} matches resulted, {bets_settled} moneyline bets settled")
    return matches_updated, bets_settled


def settle(db: DatabaseManager, year: int) -> None:
    total_m = total_b = 0
    for tour in _TOURS:
        m, b = settle_tour(db, tour, year)
        total_m += m
        total_b += b
    # Loud unsettled-but-completed signal: matches with bets still pending whose
    # kickoff is > 3 days past (results should have published by now).
    stale = db.execute_one(
        """SELECT COUNT(DISTINCT tm.id) c
           FROM tennis_matches tm JOIN tennis_bets tb ON tb.match_id=tm.id
           WHERE tb.status='pending' AND tm.match_date < CURRENT_DATE - 3"""
    )
    if stale and stale["c"]:
        print(f"  [!] {stale['c']} matches >3d old still have pending bets "
              f"(tennis-data lag, or a name-match miss — check)")
    print(f"Tennis results: {total_m} matches resulted, {total_b} bets settled total")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Settle tennis bets from tennis-data.co.uk")
    parser.add_argument("--year", type=int, default=date.today().year)
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    settle(db, args.year)
