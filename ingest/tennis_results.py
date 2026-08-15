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
from ingest.tennis_result_semantics import classify_completion
from ingest.tennis_result_settlement import (
    ResultObservation,
    fail_provider_run_if_open,
    finish_provider_run,
    record_observation_and_settle,
    start_provider_run,
)

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0"}
_PARSER_VERSION = "tennis-data-v2"
# (tour, url-template). WTA lives under /{year}w/.
_TOURS = {
    "ATP": "http://www.tennis-data.co.uk/{year}/{year}.xlsx",
    "WTA": "http://www.tennis-data.co.uk/{year}w/{year}.xlsx",
}


def _norm(text: str) -> str:
    t = unicodedata.normalize("NFKD", str(text or "")).encode("ascii", "ignore").decode("ascii")
    return "".join(ch for ch in t.lower() if ch.isalnum())


def _surname_keys(parts: list[str], initial: str) -> set[tuple[str, str]]:
    """Return compound-surname and last-token variants for one player."""
    if not parts or not initial:
        return set()
    surnames = {_norm("".join(parts)), _norm(parts[-1])}
    return {(surname, initial) for surname in surnames if surname}


def _keys_oddsapi(name: str) -> set[tuple[str, str]]:
    """Keys from an Odds-API ``First [Middle] Last`` player name."""
    parts = [p for p in str(name or "").split() if p]
    if len(parts) < 2:
        return set()
    return _surname_keys(parts[1:], _norm(parts[0])[:1])


def _keys_tennisdata(name: str) -> set[tuple[str, str]]:
    """Keys from tennis-data's ``Surname [Parts] X.`` player name."""
    parts = [p for p in str(name or "").split() if p]
    if len(parts) < 2:
        return set()
    return _surname_keys(parts[:-1], _norm(parts[-1])[:1])


def _int_or_zero(value) -> int:
    """Convert numeric result fields while treating blanks/NaN as an absent score."""
    try:
        return int(value) if value is not None and value == value else 0
    except (TypeError, ValueError, OverflowError):
        return 0


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
    """Append structured results and atomically publish accepted resolutions."""
    run_id = start_provider_run(db, "tennis_data", tour, _PARSER_VERSION)
    try:
        return _settle_tour(db, tour, year, run_id)
    except Exception as exc:
        fail_provider_run_if_open(db, run_id, exc, status="parse_error")
        raise


def _settle_tour(
    db: DatabaseManager, tour: str, year: int, run_id: int,
) -> tuple[int, int]:
    import pandas as pd

    rows = db.execute(
        """SELECT id, match_date, home_player, away_player, winner,
                  completion_status, home_games, away_games
           FROM tennis_matches
           WHERE tour=%s AND EXTRACT(YEAR FROM match_date)=%s
             AND (winner IS NULL OR completion_status IN ('scheduled','unknown')
                  OR home_games IS NULL OR away_games IS NULL)""",
        (tour, year),
    )
    if not rows:
        finish_provider_run(db, run_id, status="empty")
        return 0, 0

    url = _TOURS[tour].format(year=year)
    try:
        response = requests.get(url, headers=_HEADERS, timeout=90)
        response.raise_for_status()
        frame = pd.read_excel(io.BytesIO(response.content))
    except Exception as exc:  # noqa: BLE001 - persisted and re-raised for health visibility
        status = "fetch_error" if isinstance(exc, requests.RequestException) else "parse_error"
        finish_provider_run(
            db, run_id, status=status,
            http_status=getattr(getattr(exc, "response", None), "status_code", None),
            error=str(exc),
        )
        raise

    index: dict[frozenset, list[dict]] = {}
    for match in rows:
        for home_key in _keys_oddsapi(match["home_player"]):
            for away_key in _keys_oddsapi(match["away_player"]):
                index.setdefault(frozenset((home_key, away_key)), []).append(match)

    matches_updated = bets_settled = parsed = ambiguous = 0
    for _, source_row in frame.iterrows():
        if not str(source_row.get("Tournament", "")).strip():
            continue
        result_date = source_row.get("Date")
        result_date = result_date.date() if isinstance(result_date, datetime) else result_date
        if not isinstance(result_date, date):
            continue
        winner_keys = _keys_tennisdata(source_row.get("Winner"))
        loser_keys = _keys_tennisdata(source_row.get("Loser"))
        if not winner_keys or not loser_keys:
            continue
        parsed += 1
        candidates: dict[int, dict] = {}
        for winner_key in winner_keys:
            for loser_key in loser_keys:
                for candidate in index.get(frozenset((winner_key, loser_key)), []):
                    if abs((candidate["match_date"] - result_date).days) <= 2:
                        candidates[candidate["id"]] = candidate
        if not candidates:
            continue
        min_distance = min(abs((m["match_date"] - result_date).days)
                           for m in candidates.values())
        best = [m for m in candidates.values()
                if abs((m["match_date"] - result_date).days) == min_distance]
        if len(best) != 1:
            ambiguous += 1
            continue
        match = best[0]
        home_is_winner = bool(_keys_oddsapi(match["home_player"]) & winner_keys)
        away_is_winner = bool(_keys_oddsapi(match["away_player"]) & winner_keys)
        if home_is_winner == away_is_winner:
            ambiguous += 1
            continue

        winner_sets = _int_or_zero(source_row.get("Wsets"))
        loser_sets = _int_or_zero(source_row.get("Lsets"))
        winner_games = _games(source_row, "W", _int_or_zero(source_row.get("Best of")) or 3)
        loser_games = _games(source_row, "L", _int_or_zero(source_row.get("Best of")) or 3)
        comment = str(source_row.get("Comment", "") or "")
        completion_status, _retired, _walkover = classify_completion(comment)
        home_sets, away_sets = ((winner_sets, loser_sets) if home_is_winner
                                else (loser_sets, winner_sets))
        home_games, away_games = ((winner_games, loser_games) if home_is_winner
                                  else (loser_games, winner_games))
        winner_side = "home" if home_is_winner else "away"
        result = record_observation_and_settle(db, ResultObservation(
            match_id=match["id"], provider="tennis_data", winner_side=winner_side,
            completion_status=completion_status, status_evidence=True,
            observed_match_date=result_date, home_sets=home_sets, away_sets=away_sets,
            home_games=home_games, away_games=away_games, source_url=url,
            parser_version=_PARSER_VERSION,
            raw_payload={key: (None if value != value else value)
                         for key, value in source_row.to_dict().items()},
            match_method="surname_initial_date", match_confidence=0.98,
            reason=comment or "Structured tennis-data result",
        ))
        if result["state"] == "resolved":
            matches_updated += 1
            bets_settled += int(result["bets"])

    finish_provider_run(
        db, run_id, status="success" if parsed else "empty", fetched=len(frame),
        parsed=parsed, matched=matches_updated, ambiguous=ambiguous,
    )
    print(f"Tennis {tour}: {matches_updated} matches published, {bets_settled} moneyline bets settled")
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
