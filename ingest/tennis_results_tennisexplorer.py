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
import re
import unicodedata
from datetime import date, timedelta

import requests
from bs4 import BeautifulSoup

from config import load_config
from db.database import DatabaseManager
from ingest.tennis_result_settlement import (
    ResultObservation,
    fail_provider_run_if_open,
    finish_provider_run,
    record_observation_and_settle,
    start_provider_run,
)

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0"}
_TOUR_TYPE = {"ATP": "atp-single", "WTA": "wta-single"}
_DEFAULT_DAYS_BACK = 7
_PARSER_VERSION = "tennisexplorer-v4-completion"


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
    keys = _surname_keys(parts[1:], _norm(parts[0])[:1])
    # Provider and Odds-API display names do not use one universal order.
    # ``Wu Yibing`` is surname-first, ``Martin Damm Jr.`` has a suffix, and
    # ``Maria Camila Osorio Serrano`` may be shortened to ``Osorio C.``.
    # Generate adjacent-token and suffix variants; the opposing player plus
    # the tight date window still has to identify one unique stored fixture.
    for index, token in enumerate(parts):
        surname = _norm(token)
        if index > 0:
            initial = _norm(parts[index - 1])[:1]
            if surname and initial:
                keys.add((surname, initial))
        if index + 1 < len(parts):
            initial = _norm(parts[index + 1])[:1]
            if surname and initial:
                keys.add((surname, initial))
    first_initial = _norm(parts[0])[:1]
    for split in range(2, len(parts)):
        compound = _norm("".join(parts[split:]))
        if compound and first_initial:
            keys.add((compound, first_initial))
    return keys


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
        raise RuntimeError(f"tennisexplorer {tour} {d} fetch failed: {e}") from e

    return _parse_day(resp.text)


def _parse_day(html: str) -> list[dict]:
    """Retain set-level and status evidence; match paired row IDs exactly."""
    soup = BeautifulSoup(html, "html.parser")
    matches: list[dict] = []
    pending = None

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
            pending = None
            continue

        row_id = tr.get("id") or ""
        if row_id.endswith("b") and pending is not None:
            a_name, a_sets, a_id, a_row = pending
            pending = None
            if row_id != a_id + "b":
                continue
            if a_sets != result:
                a_keys = _keys_surname_initial(a_name)
                b_keys = _keys_surname_initial(name)
                if a_keys and b_keys:
                    def scores(row):
                        values = []
                        for cell in row.select("td.score"):
                            # Superscript is tiebreak points, not games.
                            text = "".join(str(t) for t in cell.find_all(string=True)
                                           if t.parent.name != "sup").strip()
                            values.append(int(text) if text.isdigit() else None)
                        return values
                    detail = a_row.select_one('a[href*="match-detail"]')
                    match_id = re.search(r"[?&]id=(\d+)", detail.get("href", "")) if detail else None
                    matches.append({"a_keys": a_keys, "a_sets": a_sets,
                                    "b_keys": b_keys, "b_sets": result,
                                    "a_games": scores(a_row), "b_games": scores(tr),
                                    "source_match_id": match_id.group(1) if match_id else None,
                                    "source_rows": str(a_row) + str(tr)})
        else:
            pending = (name, result, row_id, tr)

    return matches


def _completion_evidence(scraped: dict, best_of: int | None) -> bool:
    """Confirm a normal finish only from a complete, coherent best-of score.

    Any exception marker, incomplete set, missing format, or mismatching set
    tally fails closed. A leading player alone is not a completed-match winner.
    """
    if best_of not in (3, 5):
        return False
    markup = scraped.get("source_rows", "").lower()
    if re.search(r"retir|walkover|w\.o\.|\bret\b|abandon|suspend|postpon|cancel|awarded|disqual", markup):
        return False
    ag, bg = scraped.get("a_games", []), scraped.get("b_games", [])
    if not ag or len(ag) != len(bg):
        return False
    pairs = [(a, b) for a, b in zip(ag, bg) if a is not None or b is not None]
    needed = best_of // 2 + 1
    wins = [0, 0]
    # No played sets may follow a missing score column.
    if pairs != list(zip(ag, bg))[:len(pairs)]:
        return False
    for a, b in pairs:
        if a is None or b is None or max(wins) >= needed:
            return False
        hi, lo = max(a, b), min(a, b)
        if not ((hi == 6 and 0 <= lo <= 4) or (hi == 7 and lo in (5, 6))
                or (hi > 7 and hi - lo == 2)):
            return False
        wins[0 if a > b else 1] += 1
    return wins == [scraped["a_sets"], scraped["b_sets"]] and max(wins) == needed


def settle_tour(db: DatabaseManager, tour: str, days_back: int) -> tuple[int, int]:
    run_id = start_provider_run(db, "tennisexplorer", tour, _PARSER_VERSION)
    try:
        return _settle_tour(db, tour, days_back, run_id)
    except Exception as exc:
        fail_provider_run_if_open(db, run_id, exc, status="parse_error")
        raise


def _settle_tour(
    db: DatabaseManager, tour: str, days_back: int, run_id: int,
) -> tuple[int, int]:
    rows = db.execute(
        """SELECT m.id, m.match_date, m.home_player, m.away_player, e.best_of
           FROM tennis_matches m LEFT JOIN tennis_events e ON e.id=m.canonical_event_id
           WHERE m.tour=%s AND (m.winner IS NULL OR m.completion_status='unknown')
             AND m.match_date <= CURRENT_DATE""",
        (tour,),
    )
    if not rows:
        finish_provider_run(db, run_id, status="empty")
        return 0, 0

    index: dict[frozenset, list[dict]] = {}
    for match in rows:
        for home_key in _keys_full_name(match["home_player"]):
            for away_key in _keys_full_name(match["away_player"]):
                index.setdefault(frozenset((home_key, away_key)), []).append(match)

    today = date.today()
    recent_dates = {today - timedelta(days=offset) for offset in range(days_back + 1)}
    stale_dates = {row["match_date"] for row in rows}
    scan_dates = sorted(recent_dates | stale_dates, reverse=True)[:45]
    matches_updated = bets_settled = parsed = ambiguous = fetched = 0
    processed_ids: set[int] = set()
    try:
        for result_date in scan_dates:
            scraped_rows = _fetch_day(tour, result_date)
            fetched += 1
            parsed += len(scraped_rows)
            for scraped in scraped_rows:
                candidates: dict[int, dict] = {}
                for key_a in scraped["a_keys"]:
                    for key_b in scraped["b_keys"]:
                        for candidate in index.get(frozenset((key_a, key_b)), []):
                            if abs((candidate["match_date"] - result_date).days) <= 2:
                                candidates[candidate["id"]] = candidate
                if not candidates:
                    continue
                min_distance = min(abs((m["match_date"] - result_date).days)
                                   for m in candidates.values())
                best = [m for m in candidates.values()
                        if abs((m["match_date"] - result_date).days) == min_distance]
                if len(best) != 1 or best[0]["id"] in processed_ids:
                    ambiguous += len(best) != 1
                    continue
                match = best[0]
                best_of = match.get("best_of") or (3 if tour == "WTA" else None)
                if not _completion_evidence(scraped, best_of):
                    continue  # no invented completion or winner from a partial score
                home_is_a = bool(_keys_full_name(match["home_player"]) & scraped["a_keys"])
                away_is_a = bool(_keys_full_name(match["away_player"]) & scraped["a_keys"])
                if home_is_a == away_is_a:
                    ambiguous += 1
                    continue
                home_sets, away_sets = (
                    (scraped["a_sets"], scraped["b_sets"]) if home_is_a
                    else (scraped["b_sets"], scraped["a_sets"])
                )
                winner = "home" if home_sets > away_sets else "away"
                result = record_observation_and_settle(db, ResultObservation(
                    match_id=match["id"], provider="tennisexplorer",
                    winner_side=winner, completion_status="completed",
                    status_evidence=True, observed_match_date=result_date,
                    provider_event_id=scraped.get("source_match_id"),
                    home_sets=home_sets, away_sets=away_sets,
                    source_url=(f"https://www.tennisexplorer.com/results/?type={_TOUR_TYPE[tour]}"
                                f"&year={result_date.year}&month={result_date.month}&day={result_date.day}"),
                    parser_version=_PARSER_VERSION,
                    raw_payload={"a_keys": sorted(scraped["a_keys"]),
                                 "a_sets": scraped["a_sets"],
                                 "b_keys": sorted(scraped["b_keys"]),
                                 "b_sets": scraped["b_sets"],
                                 "a_games": scraped["a_games"], "b_games": scraped["b_games"],
                                 "best_of": best_of, "source_rows": scraped["source_rows"]},
                    match_method="surname_initial_date", match_confidence=0.9,
                    reason="Complete set-level score matches best-of format; no exception marker",
                ))
                if result["state"] == "resolved":
                    matches_updated += 1
                    bets_settled += int(result["bets"])
                    processed_ids.add(match["id"])
        finish_provider_run(
            db, run_id, status="success" if parsed else "empty", fetched=fetched,
            parsed=parsed, matched=matches_updated, ambiguous=ambiguous,
        )
    except Exception as exc:
        cause = getattr(exc, "__cause__", None)
        status = "fetch_error" if isinstance(exc, requests.RequestException) or isinstance(
            cause, requests.RequestException
        ) else "parse_error"
        fail_provider_run_if_open(
            db, run_id, exc, status=status, fetched=fetched, parsed=parsed,
            matched=matches_updated, ambiguous=ambiguous,
        )
        raise
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
    parser.add_argument("--existing-schema", action="store_true",
                        help="Skip schema initialization on deployed databases")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url, initialize_schema=not args.existing_schema)
    settle(db, args.days_back)
