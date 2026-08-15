"""Settle tennis bets from tennisexplorer.com — the same-day results source.

Both other settlement sources have proven too slow for a live Slam:
tennis-data.co.uk stalled 6+ days into Wimbledon 2026 (memory
tennis-data-sources), and TheSportsDB has zero Wimbledon 2026 events in its
feed at all (verified 2026-07-04). tennisexplorer.com's `/results/` page is
plain server-rendered HTML (robots.txt allows `/results/`), and carries
same-day completed match results — verified 2026-07-04 against real Wimbledon
2026 R1-R3 matches (e.g. Fery A. d. Bergs Z. 3-2, completed same day).

Page structure: one long stream of `<tr>` rows. A `<tr class="head flags">`
row starts a new tournament section (name in `td.t-name a`); each match is two
consecutive rows (id ends in "b" for the second) each with a player name
(`td.t-name a`, e.g. "De Minaur A." — surname [+ optional multi-word] + initial,
matching tennis-data.co.uk's convention) and a `td.result` cell = sets won by
that player. Whichever row in the pair has more sets is the winner. A single
day's page can include matches attributed to that page's date even if
originally scheduled earlier (rain delays / rescheduling), so multiple recent
days are scanned and matched via the existing ±2-day window, same as the
other two sources.

Usage:
    python -m ingest.tennis_results_tennisexplorer               # both tours, last 7 days
    python -m ingest.tennis_results_tennisexplorer --days-back 3
"""

from __future__ import annotations

import argparse
import logging
import unicodedata
from datetime import date, timedelta

import requests
from bs4 import BeautifulSoup

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0"}
_TOUR_TYPE = {"ATP": "atp-single", "WTA": "wta-single"}
_DEFAULT_DAYS_BACK = 7


def _norm(text: str) -> str:
    t = unicodedata.normalize("NFKD", str(text or "")).encode("ascii", "ignore").decode("ascii")
    return "".join(ch for ch in t.lower() if ch.isalnum())


def _surname_keys(parts: list[str], initial: str) -> set[tuple[str, str]]:
    """Return compound-surname and last-token variants for one player."""
    if not parts or not initial:
        return set()
    surnames = {_norm("".join(parts)), _norm(parts[-1])}
    return {(surname, initial) for surname in surnames if surname}


def _keys_surname_initial(name: str) -> set[tuple[str, str]]:
    """Keys from tennisexplorer's ``Surname [Parts] I.`` player format.

    The final-token variant covers feeds that abbreviate a compound surname;
    the full variant preserves matching for names such as Davidovich Fokina.
    """
    parts = [p for p in str(name or "").split() if p]
    if len(parts) < 2:
        return set()
    return _surname_keys(parts[:-1], _norm(parts[-1])[:1])


def _keys_full_name(name: str) -> set[tuple[str, str]]:
    """Keys from stored ``First [Middle] Last`` player names.

    The last-token fallback matches sources which omit a middle given name
    (``Thiago Agustin Tirante`` → ``Tirante T.``), while the compound form
    remains available for genuine compound surnames.
    """
    parts = [p for p in str(name or "").split() if p]
    if len(parts) < 2:
        return set()
    return _surname_keys(parts[1:], _norm(parts[0])[:1])


def _fetch_day(tour: str, d: date) -> list[dict]:
    """Completed singles matches on tennisexplorer's results page for one day.
    Returns player-key variants and set scores for each completed match; the
    winner is whichever side has more sets.
    """
    url = (f"https://www.tennisexplorer.com/results/?type={_TOUR_TYPE[tour]}"
           f"&year={d.year}&month={d.month}&day={d.day}")
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning("tennisexplorer %s %s fetch failed: %s", tour, d, e)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    matches: list[dict] = []
    pending: tuple[str, int] | None = None

    for tr in soup.find_all("tr"):
        classes = tr.get("class") or []
        if "head" in classes and "flags" in classes:
            pending = None
            continue

        name_a = tr.select_one("td.t-name a")
        result_td = tr.select_one("td.result")
        if not name_a or not result_td:
            continue
        name = name_a.get_text(strip=True)
        try:
            result = int(result_td.get_text(strip=True))
        except ValueError:
            continue

        row_id = tr.get("id") or ""
        if row_id.endswith("b") and pending is not None:
            a_name, a_sets = pending
            pending = None
            if a_sets != result:
                a_keys = _keys_surname_initial(a_name)
                b_keys = _keys_surname_initial(name)
                if a_keys and b_keys:
                    matches.append({"a_keys": a_keys, "a_sets": a_sets,
                                    "b_keys": b_keys, "b_sets": result})
        else:
            pending = (name, result)

    return matches


def settle_tour(db: DatabaseManager, tour: str, days_back: int) -> tuple[int, int]:
    rows = db.execute(
        """SELECT id, match_date, home_player, away_player
           FROM tennis_matches WHERE tour = %s AND winner IS NULL""",
        (tour,),
    )
    if not rows:
        return 0, 0
    index: dict[frozenset, list[dict]] = {}
    for m in rows:
        home_keys = _keys_full_name(m["home_player"])
        away_keys = _keys_full_name(m["away_player"])
        for kh in home_keys:
            for ka in away_keys:
                index.setdefault(frozenset((kh, ka)), []).append(m)
    if not index:
        return 0, 0

    today = date.today()
    matches_updated = bets_settled = 0
    for offset in range(days_back + 1):
        d = today - timedelta(days=offset)
        for scraped in _fetch_day(tour, d):
            cands_by_id: dict[int, dict] = {}
            for ka in scraped["a_keys"]:
                for kb in scraped["b_keys"]:
                    for candidate in index.get(frozenset((ka, kb)), []):
                        cands_by_id[candidate["id"]] = candidate
            cands = [m for m in cands_by_id.values()
                     if abs((m["match_date"] - d).days) <= 2]
            if not cands:
                continue
            match = min(cands, key=lambda m: abs((m["match_date"] - d).days))

            home_is_a = bool(_keys_full_name(match["home_player"]) & scraped["a_keys"])
            away_is_a = bool(_keys_full_name(match["away_player"]) & scraped["a_keys"])
            if home_is_a == away_is_a:
                logger.warning("Skipping ambiguous tennisexplorer match orientation for id=%s", match["id"])
                continue
            home_sets, away_sets = (
                (scraped["a_sets"], scraped["b_sets"]) if home_is_a
                else (scraped["b_sets"], scraped["a_sets"])
            )
            winner = "home" if home_sets > away_sets else "away"

            db.execute(
                """UPDATE tennis_matches
                   SET home_sets=%s, away_sets=%s, winner=%s,
                       completion_status='completed', retired=FALSE, walkover=FALSE,
                       result_source='tennisexplorer', result_comment=NULL
                   WHERE id=%s""",
                (home_sets, away_sets, winner, match["id"]),
            )
            matches_updated += 1

            bets = db.execute(
                "SELECT id, side FROM tennis_bets WHERE match_id=%s AND status='pending' AND bet_type='moneyline'",
                (match["id"],),
            )
            for b in bets:
                bet_status = "won" if b["side"] == winner else "lost"
                detail = f"{match['home_player']} {home_sets}-{away_sets} {match['away_player']} (tennisexplorer)"
                db.execute(
                    "UPDATE tennis_bets SET status=%s, result_detail=%s, settled_at=NOW() WHERE id=%s",
                    (bet_status, detail, b["id"]),
                )
                bets_settled += 1

    return matches_updated, bets_settled


def settle(db: DatabaseManager, days_back: int = _DEFAULT_DAYS_BACK) -> None:
    total_m = total_b = 0
    for tour in _TOUR_TYPE:
        m, b = settle_tour(db, tour, days_back)
        total_m += m
        total_b += b
        print(f"tennisexplorer {tour}: {m} matches resulted, {b} moneyline bets settled")
    print(f"tennisexplorer tennis results: {total_m} matches resulted, {total_b} bets settled total")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Settle tennis bets from tennisexplorer.com (same-day results)"
    )
    parser.add_argument("--days-back", type=int, default=_DEFAULT_DAYS_BACK)
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    settle(db, args.days_back)
