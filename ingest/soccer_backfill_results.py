"""Backfill completed World Cup fixtures the live feeds can't recover.

The schedule ingest (Odds API ``/odds``) only carries **upcoming** games, and
``soccer_results.fetch_scores`` only UPDATEs fixtures that already exist and
reaches back just ``daysFrom=3``.  So any match that had already kicked off
before our first ingest is never captured at all — e.g. USA 4-1 Paraguay
(2026-06-13), which left USA invisible to the group-winner model even though
they top the group.

This module backfills those gaps from **TheSportsDB** (free tier, key "123",
already used for first-scorer settlement), which keeps the full historical
fixture list with final scores.  Round-based queries return complete matchdays
(``eventsround.php`` → 24 games/round) without the 15-row ``eventsseason`` cap.

For each completed event it either UPDATEs the score of an existing fixture
(matched by team pair, orientation-aware) or INSERTs a new fixture row with a
``tsdb-<idEvent>`` game_id.  No odds are written — backfilled games are for
standings/settlement and the results-aware group sim, not betting.

Usage:
    python -m ingest.soccer_backfill_results                # group stage (rounds 1-3)
    python -m ingest.soccer_backfill_results --rounds 1 2 3 4
"""

from __future__ import annotations

import argparse
import logging
import unicodedata
from datetime import datetime, timezone

import requests

from config import load_config
from db.database import DatabaseManager
from db.queries import build_soccer_team_name_cache

logger = logging.getLogger(__name__)

TSDB_BASE = "https://www.thesportsdb.com/api/v1/json/123"
WC_LEAGUE_ID = 4429          # FIFA World Cup on TheSportsDB
WC_SEASON = "2026"
GROUP_STAGE_ROUNDS = (1, 2, 3)

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


def _fetch_round(round_no: int) -> list[dict]:
    """All events for one matchday round; [] on any failure."""
    try:
        r = requests.get(
            f"{TSDB_BASE}/eventsround.php",
            params={"id": WC_LEAGUE_ID, "r": round_no, "s": WC_SEASON},
            timeout=20,
        )
        r.raise_for_status()
        return r.json().get("events") or []
    except requests.RequestException as e:
        logger.warning("TheSportsDB round %s fetch failed: %s", round_no, e)
        return []


def backfill(db: DatabaseManager, rounds=GROUP_STAGE_ROUNDS) -> int:
    """Insert/update completed WC fixtures from TheSportsDB.  Returns rows touched."""
    norm_cache = {_norm(name): tid for name, tid in build_soccer_team_name_cache(db).items()}

    inserted = updated = skipped = unresolved = 0
    for round_no in rounds:
        for ev in _fetch_round(round_no):
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

            date_event = ev.get("dateEvent")

            # Existing fixture for this pair, in either orientation (group-stage
            # pairs are unique, so we don't need a date match).
            existing = db.execute_one(
                """
                SELECT id, home_team_id, away_team_id, home_score, away_score
                FROM soccer_matchups
                WHERE (home_team_id = %s AND away_team_id = %s)
                   OR (home_team_id = %s AND away_team_id = %s)
                ORDER BY game_date ASC
                LIMIT 1
                """,
                (home_id, away_id, away_id, home_id),
            )

            if existing:
                if existing["home_score"] is not None and existing["away_score"] is not None:
                    skipped += 1
                    continue
                # Orient the score to the stored row's home/away.
                if existing["home_team_id"] == home_id:
                    row_hs, row_as = hs, as_
                else:
                    row_hs, row_as = as_, hs
                db.execute(
                    "UPDATE soccer_matchups SET home_score = %s, away_score = %s WHERE id = %s",
                    (row_hs, row_as, existing["id"]),
                )
                updated += 1
            else:
                # New fixture — insert with score in TheSportsDB's orientation.
                db.execute(
                    """
                    INSERT INTO soccer_matchups
                        (game_date, game_id, commence_time, home_team_id, away_team_id,
                         stage, home_score, away_score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (game_date, home_team_id, away_team_id) DO UPDATE SET
                        home_score = COALESCE(soccer_matchups.home_score, EXCLUDED.home_score),
                        away_score = COALESCE(soccer_matchups.away_score, EXCLUDED.away_score),
                        game_id    = COALESCE(soccer_matchups.game_id, EXCLUDED.game_id)
                    """,
                    (date_event, f"tsdb-{ev.get('idEvent')}", _commence(date_event, ev.get("strTime")),
                     home_id, away_id, "group", hs, as_),
                )
                inserted += 1
                logger.info("Backfilled: %s %d-%d %s (%s)",
                            ev.get("strHomeTeam"), hs, as_, ev.get("strAwayTeam"), date_event)

    print(f"Backfill: {inserted} inserted, {updated} score-updated, "
          f"{skipped} already complete, {unresolved} unresolved")
    return inserted + updated


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Backfill completed WC fixtures from TheSportsDB")
    parser.add_argument("--rounds", type=int, nargs="+", default=list(GROUP_STAGE_ROUNDS),
                        help="Matchday rounds to backfill (default: 1 2 3)")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    backfill(db, rounds=args.rounds)
