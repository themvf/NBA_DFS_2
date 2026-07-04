"""Settle tennis bets from TheSportsDB as a fallback when tennis-data.co.uk lags.

tennis-data.co.uk is the primary settlement source (ingest/tennis_results.py) —
it carries full set/game scores and closing odds. But it has stalled before
during a Slam (Wimbledon 2026: Last-Modified stuck at 2026-06-28, six days
into the tournament, contradicting its own previously-observed near-daily
cadence). TheSportsDB has full ATP/WTA event coverage (leagues 4464/4517,
verified 2026-06-30) including majors, so it fills the gap: no serve stats,
but settlement only needs winner + sets.

Mirrors ingest/soccer_backfill_results.py's v2 API pattern (schedule/league/{id}
/{season}, key in X-API-KEY header) — same live-score guards, adapted for
individual-player events instead of team pairs. Only touches matches still
unresolved after the primary source runs; never overwrites tennis-data.co.uk's
richer per-set data once it lands.

Usage:
    python -m ingest.tennis_results_thesportsdb               # both tours, current season
"""

from __future__ import annotations

import argparse
import logging
import os
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

# Same guard vocabulary as soccer_backfill_results.py: intHomeScore/intAwayScore
# populate LIVE during a match, so a bare "score present" check is not enough.
_LIVE_STATUSES = {
    "ns", "not started", "1st set", "2nd set", "3rd set", "4th set", "5th set",
    "live", "in play", "postponed", "postp", "susp", "suspended",
    "int", "interrupted", "rain delay",
}
# Best-of-5 Slam matches can run 4-5 hours; give more buffer than soccer's 3h
# so a marathon match's in-progress set score is never mistaken for final.
_MIN_ELAPSED = timedelta(hours=6)


def _norm(name: str) -> str:
    text = unicodedata.normalize("NFKD", str(name or "")).encode("ascii", "ignore").decode("ascii")
    return "".join(ch for ch in text.lower() if ch.isalnum())


def _commence(date_event: str | None, str_time: str | None):
    if not date_event:
        return None
    t = (str_time or "00:00:00")[:8]
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(f"{date_event} {t}", fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        return datetime.strptime(date_event, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _fetch_event(event_id) -> dict | None:
    """Single-event v2 lookup (diagnostic use only so far)."""
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
        events = data.get("events") or data.get("event") or []
        return events[0] if events else None
    except (requests.RequestException, IndexError) as e:
        logger.warning("TheSportsDB v2 event %s lookup failed: %s", event_id, e)
        return None


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
    index: dict[frozenset, list[dict]] = {}
    for m in rows:
        kh, ka = _norm(m["home_player"]), _norm(m["away_player"])
        if not kh or not ka:
            continue
        index.setdefault(frozenset((kh, ka)), []).append(m)
    if not index:
        return 0, 0

    now = datetime.now(timezone.utc)
    matches_updated = bets_settled = 0
    for ev in _fetch_season(tour):
        hs, as_ = ev.get("intHomeScore"), ev.get("intAwayScore")
        if hs is None or as_ is None:
            continue  # not played (or not scored) yet
        try:
            hs, as_ = int(hs), int(as_)
        except (TypeError, ValueError):
            continue
        if hs == as_:
            continue  # tennis always has a decisive winner; a tied set-count means mid-match

        status = (ev.get("strStatus") or "").strip().lower()
        if status in _LIVE_STATUSES:
            continue
        kickoff = _commence(ev.get("dateEvent"), ev.get("strTime"))
        if kickoff is None or now - kickoff < _MIN_ELAPSED:
            continue

        kh, ka = _norm(ev.get("strHomeTeam", "")), _norm(ev.get("strAwayTeam", ""))
        if not kh or not ka:
            continue
        cands = index.get(frozenset((kh, ka)))
        if not cands:
            continue

        # Same hard date window as tennis_results.py: the same two players can
        # meet across different tournaments/weeks, so only let a result settle a
        # match dated within ±2 days of it.
        ev_date = ev.get("dateEvent")
        try:
            ev_date_d = datetime.strptime(ev_date, "%Y-%m-%d").date()
        except (TypeError, ValueError):
            continue
        cands = [m for m in cands if abs((m["match_date"] - ev_date_d).days) <= 2]
        if not cands:
            continue
        match = min(cands, key=lambda m: abs((m["match_date"] - ev_date_d).days))

        home_is_home = _norm(match["home_player"]) == kh
        home_sets, away_sets = (hs, as_) if home_is_home else (as_, hs)
        winner = "home" if home_sets > away_sets else "away"

        db.execute(
            """UPDATE tennis_matches
               SET home_sets=%s, away_sets=%s, winner=%s
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
            detail = f"{ev.get('strHomeTeam')} {home_sets}-{away_sets} {ev.get('strAwayTeam')} (TheSportsDB)"
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
    """One-off diagnostic: print raw event counts/samples per tour, no DB writes."""
    for tour in _LEAGUES:
        events = _fetch_season(tour)
        print(f"{tour}: {len(events)} raw events from schedule/league/{_LEAGUES[tour]}/{_SEASON}")
        ft_events = [ev for ev in events if (ev.get("strStatus") or "").upper() == "FT"]
        print(f"  {len(ft_events)} events with strStatus == 'FT'")
        if ft_events:
            last_ft = ft_events[-1]
            print(f"  Last FT event (bulk): {last_ft}")
            event_id = last_ft.get("idEvent")
            try:
                r = requests.get(
                    f"{TSDB_V2_BASE}/lookup/event/{event_id}", headers=_TSDB_HEADERS, timeout=20,
                )
                print(f"  Single-event lookup RAW status={r.status_code} body={r.text[:1000]}")
            except requests.RequestException as e:
                print(f"  Single-event lookup RAW request failed: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Fallback: settle tennis bets from TheSportsDB when tennis-data.co.uk lags"
    )
    parser.add_argument("--debug", action="store_true", help="Dump raw events per tour, no DB writes")
    args = parser.parse_args()

    if args.debug:
        _debug_dump()
    else:
        config = load_config()
        db = DatabaseManager(config.database_url)
        settle(db)
