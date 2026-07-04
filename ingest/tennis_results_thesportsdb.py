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

logger = logging.getLogger(__name__)

_TSDB_KEY = os.getenv("THESPORTSDB_API_KEY", "123")
TSDB_V2_BASE = "https://www.thesportsdb.com/api/v2/json"
_TSDB_HEADERS = {"X-API-KEY": _TSDB_KEY}

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
        logger.warning("TheSportsDB v2 schedule fetch failed for %s: %s", tour, e)
        return []


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
    """Settle this tour's still-pending tennis_matches from TheSportsDB.
    Returns (matches_updated, bets_settled)."""
    rows = db.execute(
        """SELECT id, match_date, home_player, away_player
           FROM tennis_matches WHERE tour = %s AND winner IS NULL""",
        (tour,),
    )
    if not rows:
        return 0, 0
    pending_dates = {m["match_date"] for m in rows}
    date_lo, date_hi = min(pending_dates) - timedelta(days=2), max(pending_dates) + timedelta(days=2)

    now = datetime.now(timezone.utc)
    matches_updated = bets_settled = 0
    for ev in _fetch_season(tour):
        ev_date = ev.get("dateEvent")
        try:
            ev_date_d = datetime.strptime(ev_date, "%Y-%m-%d").date()
        except (TypeError, ValueError):
            continue
        # Bound single-event lookups to the window our pending matches actually
        # span — the full FT event count (1000+/tour) is too many calls otherwise.
        if not (date_lo <= ev_date_d <= date_hi):
            continue
        status = (ev.get("strStatus") or "").strip().upper()
        commence = datetime.strptime(ev_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if status not in ("FT", "AET") and now - commence < _MIN_ELAPSED:
            continue  # not confirmed finished and not old enough to assume so

        full = _fetch_event(ev.get("idEvent"))
        if not full:
            continue
        parsed = _parse_result(full.get("strResult"))
        if not parsed:
            continue
        winner_name, loser_name, wsets, lsets = parsed
        kw, kl = _norm(winner_name), _norm(loser_name)
        if not kw or not kl:
            continue

        # Surname substring match (TheSportsDB gives no first initial, unlike
        # tennis-data.co.uk) against our full stored player names, within the
        # ±2 day window per pair to avoid cross-tournament collisions.
        cands = [
            m for m in rows
            if abs((m["match_date"] - ev_date_d).days) <= 2
            and ((kw in _norm(m["home_player"]) and kl in _norm(m["away_player"]))
                 or (kw in _norm(m["away_player"]) and kl in _norm(m["home_player"])))
        ]
        if not cands:
            continue
        match = min(cands, key=lambda m: abs((m["match_date"] - ev_date_d).days))

        winner_is_home = kw in _norm(match["home_player"])
        winner_side = "home" if winner_is_home else "away"
        home_sets, away_sets = (wsets, lsets) if winner_is_home else (lsets, wsets)

        db.execute(
            """UPDATE tennis_matches
               SET home_sets=%s, away_sets=%s, winner=%s
               WHERE id=%s""",
            (home_sets, away_sets, winner_side, match["id"]),
        )
        matches_updated += 1

        bets = db.execute(
            "SELECT id, side FROM tennis_bets WHERE match_id=%s AND status='pending' AND bet_type='moneyline'",
            (match["id"],),
        )
        for b in bets:
            bet_status = "won" if b["side"] == winner_side else "lost"
            detail = f"{winner_name} d. {loser_name} {wsets}-{lsets} (TheSportsDB)"
            db.execute(
                "UPDATE tennis_bets SET status=%s, result_detail=%s, settled_at=NOW() WHERE id=%s",
                (bet_status, detail, b["id"]),
            )
            bets_settled += 1

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
