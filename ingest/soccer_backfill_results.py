"""Backfill completed World Cup fixtures the live feeds can't recover.

The schedule ingest (Odds API ``/odds``) only carries **upcoming** games, and
``soccer_results.fetch_scores`` only UPDATEs fixtures that already exist and
reaches back just ``daysFrom=3``.  So any match that had already kicked off
before our first ingest is never captured at all — e.g. USA 4-1 Paraguay
(2026-06-13), which left USA invisible to the group-winner model even though
they top the group.

This module backfills those gaps from **TheSportsDB**, which keeps the full
historical fixture list with final scores.  It uses the **v2 API**
(``schedule/league/{id}/{season}``) which returns the entire tournament
(~89 group + knockout events) in a single call.  The old ``eventsround.php``
(v1) path is deprecated and 404s for every round even with a premium key.

For each completed event it either UPDATEs the score of an existing fixture
(matched by team pair, orientation-aware) or INSERTs a new fixture row with a
``tsdb-<idEvent>`` game_id.  No odds are written — backfilled games are for
standings/settlement and the results-aware group sim, not betting.

Usage:
    python -m ingest.soccer_backfill_results                # full tournament (v2 schedule)
"""

from __future__ import annotations

import argparse
import logging
import os
import unicodedata
from datetime import datetime, timezone

import requests

from config import load_config
from db.database import DatabaseManager
from db.queries import build_soccer_team_name_cache

logger = logging.getLogger(__name__)

# Use the real key from the env (the workflow passes THESPORTSDB_API_KEY); fall
# back to the free '123' tier locally. Hardcoding '123' here meant CI ran on the
# rate-limited free tier even when a paid key was configured — 429 responses then
# made _fetch_round return [] and knockout results silently never landed.
_TSDB_KEY = os.getenv("THESPORTSDB_API_KEY", "123")
# v2 API: key goes in the X-API-KEY header, NOT the path. The single
# schedule/league/{id}/{season} call returns the whole tournament at once,
# replacing the deprecated (404) v1 eventsround.php per-round loop.
TSDB_V2_BASE = "https://www.thesportsdb.com/api/v2/json"
_TSDB_HEADERS = {"X-API-KEY": _TSDB_KEY}
WC_LEAGUE_ID = 4429          # FIFA World Cup on TheSportsDB
WC_SEASON = "2026"

# TheSportsDB nation names that differ from our soccer_teams.name beyond what
# accent/punctuation normalization already collapses.
_TSDB_ALIASES = {
    "bosniaherzegovina": "bosniaandherzegovina",  # both normalize away the &/-
}


def _norm(name: str) -> str:
    """Casefold + strip accents/punctuation for robust nation matching."""
    text = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode("ascii")
    key = "".join(ch for ch in text.lower() if ch.isalnum())
    return _TSDB_ALIASES.get(key, key)


def _commence(date_event: str | None, str_time: str | None):
    """Best-effort UTC kickoff datetime from TheSportsDB date + time fields."""
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


def _fetch_season() -> list[dict]:
    """All events for the whole tournament in one v2 schedule call; [] on failure."""
    try:
        r = requests.get(
            f"{TSDB_V2_BASE}/schedule/league/{WC_LEAGUE_ID}/{WC_SEASON}",
            headers=_TSDB_HEADERS,
            timeout=20,
        )
        r.raise_for_status()
        data = r.json() or {}
        # v2 returns {"schedule": [...]}; tolerate the legacy {"events": [...]} too.
        return data.get("schedule") or data.get("events") or []
    except requests.RequestException as e:
        logger.warning("TheSportsDB v2 schedule fetch failed: %s", e)
        return []


def _fetch_event(event_id) -> dict | None:
    """Single-event v2 lookup — needed for the penalty Extra fields the bulk
    schedule omits. Returns the event dict or None on any failure."""
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


def _winner_from_event(ev: dict, home_id: int, away_id: int, hs: int, as_: int) -> int | None:
    """Determine winning team_id from a TheSportsDB event dict.

    Regular-time / ET winner is clear from the score. For penalty shootouts
    (hs == as_ after 90+ET), TheSportsDB encodes the result in intHomeScoreExtra /
    intAwayScoreExtra (NOT intScoreHomeShootout, which is null). Returns None if
    the game is tied and no penalty data. The Extra fields are only present on
    the v2 single-event lookup (`_fetch_event`), not the bulk schedule, so the
    caller fetches the full event before passing it here on a draw.
    """
    if hs > as_:
        return home_id
    if as_ > hs:
        return away_id
    # Draw after 90+ET — read the penalty shootout score from the Extra fields.
    try:
        pens_h = ev.get("intHomeScoreExtra")
        pens_a = ev.get("intAwayScoreExtra")
        if pens_h not in (None, "") and pens_a not in (None, ""):
            ph, pa = int(pens_h), int(pens_a)
            if ph > pa:
                return home_id
            if pa > ph:
                return away_id
    except (TypeError, ValueError):
        pass
    return None


def backfill(db: DatabaseManager) -> int:
    """Insert/update completed WC fixtures from TheSportsDB.  Returns rows touched.

    Pulls the entire tournament (group stage + all knockout rounds) via one v2
    schedule call. Writes home_score, away_score, and winner_team_id (handles
    penalty shootouts via a single-event lookup for the intHomeScoreExtra /
    intAwayScoreExtra fields the bulk schedule omits).
    """
    norm_cache = {_norm(name): tid for name, tid in build_soccer_team_name_cache(db).items()}

    inserted = updated = skipped = unresolved = 0
    for ev in _fetch_season():
        hs, as_ = ev.get("intHomeScore"), ev.get("intAwayScore")
        if hs is None or as_ is None:
            continue  # not played yet
        try:
            hs, as_ = int(hs), int(as_)
        except (TypeError, ValueError):
            continue

        home_id = norm_cache.get(_norm(ev.get("strHomeTeam", "")))
        away_id = norm_cache.get(_norm(ev.get("strAwayTeam", "")))
        if not home_id or not away_id:
            unresolved += 1
            logger.warning("Unresolved teams: %r vs %r",
                           ev.get("strHomeTeam"), ev.get("strAwayTeam"))
            continue

        # Derive winner. The bulk schedule lacks the penalty Extra fields, so on a
        # draw fetch the single event to read intHomeScoreExtra / intAwayScoreExtra.
        winner_ev = ev
        if hs == as_:
            full = _fetch_event(ev.get("idEvent"))
            if full:
                winner_ev = full
        winner_id = _winner_from_event(winner_ev, home_id, away_id, hs, as_)
        # Stage label from the round number (groups are rounds 1-3, knockout 4+).
        try:
            round_no = int(ev.get("intRound") or 0)
        except (TypeError, ValueError):
            round_no = 0
        stage_label = "group" if 0 < round_no <= 3 else "knockout"

        date_event = ev.get("dateEvent")

        # Match existing fixture by team pair in either orientation.
        existing = db.execute_one(
            """
            SELECT id, home_team_id, away_team_id, home_score, away_score, winner_team_id
            FROM soccer_matchups
            WHERE (home_team_id = %s AND away_team_id = %s)
               OR (home_team_id = %s AND away_team_id = %s)
            ORDER BY game_date ASC
            LIMIT 1
            """,
            (home_id, away_id, away_id, home_id),
        )

        if existing:
            already_scored = (existing["home_score"] is not None
                              and existing["away_score"] is not None)
            already_winnered = existing.get("winner_team_id") is not None
            if already_scored and already_winnered:
                skipped += 1
                continue
            # Orient the score to the stored row's home/away.
            if existing["home_team_id"] == home_id:
                row_hs, row_as = hs, as_
                row_winner = winner_id
            else:
                row_hs, row_as = as_, hs
                # Flip winner if orientation is reversed.
                if winner_id == home_id:
                    row_winner = existing["away_team_id"]
                elif winner_id == away_id:
                    row_winner = existing["home_team_id"]
                else:
                    row_winner = None
            db.execute(
                """UPDATE soccer_matchups
                   SET home_score = %s, away_score = %s,
                       winner_team_id = COALESCE(%s, winner_team_id)
                   WHERE id = %s""",
                (row_hs, row_as, row_winner, existing["id"]),
            )
            updated += 1
        else:
            # New fixture — insert with score in TheSportsDB's orientation.
            db.execute(
                """
                INSERT INTO soccer_matchups
                    (game_date, game_id, commence_time, home_team_id, away_team_id,
                     stage, home_score, away_score, winner_team_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_date, home_team_id, away_team_id) DO UPDATE SET
                    home_score     = COALESCE(soccer_matchups.home_score, EXCLUDED.home_score),
                    away_score     = COALESCE(soccer_matchups.away_score, EXCLUDED.away_score),
                    winner_team_id = COALESCE(soccer_matchups.winner_team_id, EXCLUDED.winner_team_id),
                    game_id        = COALESCE(soccer_matchups.game_id, EXCLUDED.game_id)
                """,
                (date_event, f"tsdb-{ev.get('idEvent')}", _commence(date_event, ev.get("strTime")),
                 home_id, away_id, stage_label, hs, as_, winner_id),
            )
            inserted += 1
            logger.info("Backfilled: %s %d-%d %s (%s)",
                        ev.get("strHomeTeam"), hs, as_, ev.get("strAwayTeam"), date_event)

    print(f"Backfill: {inserted} inserted, {updated} score-updated, "
          f"{skipped} already complete, {unresolved} unresolved")
    return inserted + updated


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    argparse.ArgumentParser(description="Backfill completed WC fixtures from TheSportsDB").parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    backfill(db)
