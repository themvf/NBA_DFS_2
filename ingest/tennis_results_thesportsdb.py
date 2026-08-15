"""Settle tennis bets from TheSportsDB as a fallback when tennis-data.co.uk lags.

tennis-data.co.uk is the primary settlement source (ingest/tennis_results.py) —
it carries full set/game scores and closing odds. But it has stalled before
during a Slam (Wimbledon 2026: Last-Modified stuck at 2026-06-28, six days
into the tournament, contradicting its own previously-observed near-daily
cadence). TheSportsDB has full ATP/WTA event coverage (leagues 4464/4517,
verified 2026-06-30) as a second, independent source.

**Verified 2026-07-04 against the real premium key**: unlike soccer,
TheSportsDB's bulk schedule/league endpoint does NOT populate strHomeTeam /
strAwayTeam / intHomeScore / intAwayScore for tennis (individual-sport events
apparently aren't modeled the same way team sports are) — those stay null even
on strStatus='FT' events. The real result lives in free text on the
**single-event lookup** (`lookup/event/{id}`) as `strResult`, e.g.
`"Keys  beat Maria  2-0\r\nKeys : 7 6 \r\nMaria : 5 4"`. So this module:
  1. pulls the bulk schedule just to find candidate FT events by date/tournament
     (strEvent is free text like "Eastbourne Open Keys vs Maria" — no
     structured player fields to match on directly),
  2. single-event-looks-up only events whose date falls near a still-pending
     match in our own ledger (bounded call count — not all ~1300+ FT events),
  3. parses the winner from strResult's "X beat Y" line and matches by
     surname substring against our stored home_player/away_player.

**Also verified 2026-07-04**: TheSportsDB has zero Wimbledon 2026 events in
this feed right now (same gap as tennis-data.co.uk — neither source has
caught up yet). This module will start settling automatically the moment
either source's Wimbledon coverage lands; it does nothing harmful in the
meantime (continue-on-error in the workflow, zero DB writes if nothing matches).

Usage:
    python -m ingest.tennis_results_thesportsdb               # both tours, current season
    python -m ingest.tennis_results_thesportsdb --debug        # dump raw feed state, no writes
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import unicodedata
from datetime import datetime, timedelta, timezone

import requests

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

_TSDB_KEY = os.getenv("THESPORTSDB_API_KEY", "123")
TSDB_V2_BASE = "https://www.thesportsdb.com/api/v2/json"
_TSDB_HEADERS = {"X-API-KEY": _TSDB_KEY}
_PARSER_VERSION = "thesportsdb-v2"

# TheSportsDB league IDs (verified 2026-06-30 — see memory thesportsdb-premium-api).
_LEAGUES = {"ATP": 4464, "WTA": 4517}
_SEASON = "2026"

# strResult observed format: "{Winner} beat {Loser} {wsets}-{lsets}\r\n...".
# Winner/loser names here are TheSportsDB's free-text surnames (possibly
# multi-word, e.g. "Davidovich Fokina") — matched by substring below, not
# equality, since we don't get a first initial like tennis-data.co.uk gives.
_RESULT_RE = re.compile(r"^(?P<winner>.+?)\s+beat\s+(?P<loser>.+?)\s+(?P<wsets>\d+)-(?P<lsets>\d+)", re.IGNORECASE)

# Bulk schedule doesn't expose strStatus reliably for tennis in all cases, so
# the elapsed-time guard is the primary backstop (mirrors soccer's live-score
# lesson). Best-of-5 Slam matches can run 4-5 hours; give more buffer than
# soccer's 3h so a marathon match's in-progress state is never mistaken final.
_MIN_ELAPSED = timedelta(hours=6)


def _norm(name: str) -> str:
    text = unicodedata.normalize("NFKD", str(name or "")).encode("ascii", "ignore").decode("ascii")
    return "".join(ch for ch in text.lower() if ch.isalnum())


def _fetch_season(tour: str) -> list[dict]:
    """All events for one tour/season in a single v2 schedule call; [] on failure."""
    league_id = _LEAGUES[tour]
    try:
        r = requests.get(
            f"{TSDB_V2_BASE}/schedule/league/{league_id}/{_SEASON}",
            headers=_TSDB_HEADERS,
            timeout=20,
        )
        r.raise_for_status()
        data = r.json() or {}
        return data.get("schedule") or data.get("events") or []
    except requests.RequestException as e:
        raise RuntimeError(f"TheSportsDB v2 schedule fetch failed for {tour}: {e}") from e


def _fetch_event(event_id) -> dict | None:
    """Single-event v2 lookup — needed for strResult, which the bulk schedule omits."""
    if not event_id:
        return None
    try:
        r = requests.get(
            f"{TSDB_V2_BASE}/lookup/event/{event_id}",
            headers=_TSDB_HEADERS,
            timeout=20,
        )
        r.raise_for_status()
        data = r.json() or {}
        events = data.get("lookup") or data.get("events") or data.get("event") or []
        return events[0] if events else None
    except (requests.RequestException, IndexError) as e:
        logger.warning("TheSportsDB v2 event %s lookup failed: %s", event_id, e)
        return None


def _parse_result(str_result: str | None) -> tuple[str, str, int, int] | None:
    """(winner_surname, loser_surname, winner_sets, loser_sets) from strResult, or None."""
    if not str_result:
        return None
    m = _RESULT_RE.match(str_result.strip())
    if not m:
        return None
    try:
        return (m.group("winner").strip(), m.group("loser").strip(),
                int(m.group("wsets")), int(m.group("lsets")))
    except (TypeError, ValueError):
        return None


def settle_tour(db: DatabaseManager, tour: str) -> tuple[int, int]:
    """Append fallback observations; ambiguous surname matches fail closed."""
    run_id = start_provider_run(db, "thesportsdb", tour, _PARSER_VERSION)
    try:
        return _settle_tour(db, tour, run_id)
    except Exception as exc:
        fail_provider_run_if_open(db, run_id, exc, status="parse_error")
        raise


def _settle_tour(db: DatabaseManager, tour: str, run_id: int) -> tuple[int, int]:
    rows = db.execute(
        """SELECT id, match_date, home_player, away_player
           FROM tennis_matches WHERE tour=%s AND winner IS NULL""",
        (tour,),
    )
    if not rows:
        finish_provider_run(db, run_id, status="empty")
        return 0, 0
    pending_dates = {match["match_date"] for match in rows}
    date_lo = min(pending_dates) - timedelta(days=2)
    date_hi = max(pending_dates) + timedelta(days=2)

    try:
        events = _fetch_season(tour)
    except Exception as exc:
        finish_provider_run(
            db, run_id, status="fetch_error",
            http_status=getattr(getattr(exc.__cause__, "response", None), "status_code", None),
            error=str(exc),
        )
        raise

    now = datetime.now(timezone.utc)
    matches_updated = bets_settled = parsed = ambiguous = 0
    processed_ids: set[int] = set()
    for event in events:
        event_date = event.get("dateEvent")
        try:
            result_date = datetime.strptime(event_date, "%Y-%m-%d").date()
        except (TypeError, ValueError):
            continue
        if not (date_lo <= result_date <= date_hi):
            continue
        status = (event.get("strStatus") or "").strip().upper()
        commence = datetime.strptime(event_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if status not in ("FT", "AET") and now - commence < _MIN_ELAPSED:
            continue
        full = _fetch_event(event.get("idEvent"))
        parsed_result = _parse_result(full.get("strResult") if full else None)
        if not parsed_result:
            continue
        parsed += 1
        winner_name, loser_name, winner_sets, loser_sets = parsed_result
        winner_key, loser_key = _norm(winner_name), _norm(loser_name)
        candidates = [
            match for match in rows
            if match["id"] not in processed_ids
            and abs((match["match_date"] - result_date).days) <= 2
            and ((winner_key in _norm(match["home_player"]) and loser_key in _norm(match["away_player"]))
                 or (winner_key in _norm(match["away_player"]) and loser_key in _norm(match["home_player"])))
        ]
        if not candidates:
            continue
        min_distance = min(abs((match["match_date"] - result_date).days) for match in candidates)
        best = [match for match in candidates
                if abs((match["match_date"] - result_date).days) == min_distance]
        if len(best) != 1:
            ambiguous += 1
            continue
        match = best[0]
        home_winner = winner_key in _norm(match["home_player"])
        away_winner = winner_key in _norm(match["away_player"])
        if home_winner == away_winner:
            ambiguous += 1
            continue
        winner_side = "home" if home_winner else "away"
        home_sets, away_sets = ((winner_sets, loser_sets) if home_winner
                                else (loser_sets, winner_sets))
        result = record_observation_and_settle(db, ResultObservation(
            match_id=match["id"], provider="thesportsdb", winner_side=winner_side,
            completion_status="unknown", status_evidence=False,
            observed_match_date=result_date, home_sets=home_sets, away_sets=away_sets,
            provider_event_id=str(event.get("idEvent") or "") or None,
            source_url=(f"{TSDB_V2_BASE}/lookup/event/{event.get('idEvent')}"),
            parser_version=_PARSER_VERSION, raw_payload=full or {},
            match_method="surname_substring_date", match_confidence=0.75,
            reason="Advancing player observed; completion semantics not supplied",
        ))
        if result["state"] == "resolved":
            matches_updated += 1
            bets_settled += int(result["bets"])
            processed_ids.add(match["id"])

    finish_provider_run(
        db, run_id, status="success" if parsed else "empty", fetched=len(events),
        parsed=parsed, matched=matches_updated, ambiguous=ambiguous,
    )
    return matches_updated, bets_settled


def settle(db: DatabaseManager) -> None:
    total_m = total_b = 0
    for tour in _LEAGUES:
        m, b = settle_tour(db, tour)
        total_m += m
        total_b += b
        print(f"TheSportsDB {tour}: {m} matches resulted, {b} moneyline bets settled")
    print(f"TheSportsDB tennis results: {total_m} matches resulted, {total_b} bets settled total")


def _debug_dump() -> None:
    """One-off diagnostic: print feed state per tour, no DB writes."""
    for tour in _LEAGUES:
        events = _fetch_season(tour)
        print(f"{tour}: {len(events)} raw events from schedule/league/{_LEAGUES[tour]}/{_SEASON}")
        wimb = [ev for ev in events if "wimbledon" in str(ev.get("strEvent", "")).lower()]
        print(f"  {len(wimb)} events with 'Wimbledon' in strEvent")
        ft_events = [ev for ev in events if (ev.get("strStatus") or "").upper() == "FT"]
        if ft_events:
            last = ft_events[-1]
            full = _fetch_event(last.get("idEvent"))
            print(f"  Most recent FT event: {last.get('dateEvent')} {last.get('strEvent')}")
            print(f"  strResult: {full.get('strResult') if full else None!r}")
            print(f"  Parsed: {_parse_result(full.get('strResult')) if full else None}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Fallback: settle tennis bets from TheSportsDB when tennis-data.co.uk lags"
    )
    parser.add_argument("--debug", action="store_true", help="Dump feed state, no DB writes")
    args = parser.parse_args()

    if args.debug:
        _debug_dump()
    else:
        config = load_config()
        db = DatabaseManager(config.database_url)
        settle(db)
