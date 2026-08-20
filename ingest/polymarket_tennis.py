"""Capture tennis odds from Polymarket's Gamma API (free, no auth).

Polymarket surfaces two valuable tennis signal types:

1. **Match markets** (short-lived, hours before match): 2-way moneyline on
   individual matches. Stored into game_odds_history with sport='tennis' so the
   existing pinnacle_polymarket_delta alert detector fires automatically.

2. **Futures markets** (long-lived): tournament winners, year-end rankings with
   many player sub-markets. Stored in polymarket_tennis_futures for tracking.

Discovery uses the Gamma API tag system -- and the two capture paths use
DIFFERENT tags, verified live 2026-08-19 (see MATCH_TAG_ID below for why):
  - Match markets: MATCH_TAG_ID = 864 (generic "Tennis"), tour inferred from
    the event slug ("atp-..." / "wta-...").
  - Futures markets: ATP_TAG_ID = 101232 / WTA_TAG_ID = 102123 (unchanged --
    these ARE where tournament futures/props live).

Bug fixed 2026-08-19: capture_matches() previously queried ATP_TAG_ID /
WTA_TAG_ID for match discovery too. Live verification found those two tags
are almost entirely tournament futures/props (scanning 500 closed ATP-tag
events by volume surfaced exactly 1 real head-to-head match) and had ZERO
live match events at verification time, despite real matches actively
trading on Polymarket. This is the most likely root cause of
pinnacle_polymarket_delta firing zero tennis alerts ever (see memory:
detector-health-check.md) -- capture_matches() was querying an
essentially-empty tag. See ingest/polymarket_tennis_wallet_pilot.py's
discovery comment for the investigation that found this.

Usage:
    python -m ingest.polymarket_tennis                    # both match + futures
    python -m ingest.polymarket_tennis --matches-only     # just match capture
    python -m ingest.polymarket_tennis --futures-only     # just futures
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any

import requests

from config import load_config
from db.database import DatabaseManager
from db.queries import insert_game_odds_history_rows
from ingest.tennis_foundation import normalize_name
from model.soccer_bet_rating import prob_to_american

logger = logging.getLogger(__name__)

GAMMA_BASE = "https://gamma-api.polymarket.com"
# Futures/outrights only -- see the module docstring for why match discovery
# does NOT use these two.
ATP_TAG_ID = 101232
WTA_TAG_ID = 102123
# Real head-to-head match events live here (generic "Tennis" tag). Verified
# live 2026-08-19: 99.6% match-event density (498/500 closed events sampled)
# vs ~0.2% under the tour tags above.
MATCH_TAG_ID = 864
# Patterns to identify match events (2-player head-to-head)
_VS_PATTERN = re.compile(r"\bvs\.?\b|:", re.IGNORECASE)
# Singles match event slugs are "atp-<players>-<date>" / "wta-...";
# "atp-doubles-..." is doubles (out of this project's tennis scope --
# tennis_matches only carries ATP/WTA singles) and everything else under
# MATCH_TAG_ID that isn't slugged this way is a futures/prop event that
# slipped in under the same generic tag.
_SINGLES_SLUG_RE = re.compile(r"^(atp|wta)-(?!doubles-)")


# ── Helpers ───────────────────────────────────────────────────────────────────


def _fetch_events(tag_id: int) -> list[dict]:
    """Fetch active, unclosed events for a given tag_id from Gamma API."""
    url = f"{GAMMA_BASE}/events"
    params = {
        "tag_id": tag_id,
        "active": "true",
        "closed": "false",
        "limit": 100,
    }
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.warning("Gamma API fetch failed for tag_id=%s: %s", tag_id, e)
        return []


def _parse_outcomes(market: dict) -> tuple[list[str], list[float]] | None:
    """Parse outcomes and outcomePrices from a Polymarket market dict.

    Both fields are JSON-encoded strings: '["Player A", "Player B"]'.
    Returns (names, prices) or None if unparseable.
    """
    raw_outcomes = market.get("outcomes")
    raw_prices = market.get("outcomePrices")
    if not raw_outcomes or not raw_prices:
        return None
    try:
        names = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes
        prices = json.loads(raw_prices) if isinstance(raw_prices, str) else raw_prices
        prices = [float(p) for p in prices]
    except (json.JSONDecodeError, ValueError, TypeError):
        return None
    if len(names) != len(prices):
        return None
    return names, prices


def _is_match_market(names: list[str]) -> bool:
    """Check if a market's outcomes represent a 2-player match (not O/U, Yes/No)."""
    if len(names) != 2:
        return False
    skip_patterns = {"over", "under", "yes", "no", "draw"}
    for name in names:
        if name.strip().lower() in skip_patterns:
            return False
    return True


def _is_match_event(event: dict) -> bool:
    """Determine if an event is a match (2-player head-to-head) vs a futures event."""
    title = event.get("title", "")
    # Check title for "vs" or ":" separating two names
    if _VS_PATTERN.search(title):
        return True
    # Check if the primary market has exactly 2 player-name outcomes
    markets = event.get("markets", [])
    if markets:
        parsed = _parse_outcomes(markets[0])
        if parsed and _is_match_market(parsed[0]):
            return True
    return False


def _normalize_poly_name(name: str) -> str:
    """Normalize a Polymarket player name for fuzzy matching.

    Strips accents, lowercases, removes punctuation — same approach as
    ingest/tennis_foundation.py's normalize_name.
    """
    return normalize_name(name)


def _find_match_market(event: dict) -> dict | None:
    """Find the match-winner market in an event (2-player outcomes, not O/U)."""
    for market in event.get("markets", []):
        parsed = _parse_outcomes(market)
        if parsed and _is_match_market(parsed[0]):
            return market
    return None


def _fuzzy_match_to_tennis_matches(
    db: DatabaseManager,
    player1: str,
    player2: str,
    tour: str,
) -> dict | None:
    """Try to match a Polymarket event to an existing tennis_matches row.

    Uses normalized name comparison. Returns the tennis_matches row dict or None.
    """
    norm1 = _normalize_poly_name(player1)
    norm2 = _normalize_poly_name(player2)

    # Look for upcoming matches in this tour
    rows = db.execute(
        """
        SELECT id, home_player, away_player, match_date, tour, game_id
        FROM tennis_matches
        WHERE tour = %s
          AND winner IS NULL
          AND match_date >= CURRENT_DATE
        ORDER BY match_date ASC
        LIMIT 200
        """,
        (tour,),
    )
    if not rows:
        return None

    for row in rows:
        home_norm = _normalize_poly_name(row["home_player"])
        away_norm = _normalize_poly_name(row["away_player"])

        # Try both orderings (Polymarket doesn't guarantee home/away order)
        if (norm1 == home_norm and norm2 == away_norm) or \
           (norm2 == home_norm and norm1 == away_norm):
            return row
        # Substring match for partial names (e.g. "Sinner" matching "Jannik Sinner")
        if (_name_contains(home_norm, norm1) and _name_contains(away_norm, norm2)) or \
           (_name_contains(home_norm, norm2) and _name_contains(away_norm, norm1)):
            return row

    return None


def _name_contains(full_norm: str, partial_norm: str) -> bool:
    """Check if partial_norm is a substantial substring of full_norm."""
    if not partial_norm or len(partial_norm) < 3:
        return False
    return partial_norm in full_norm or full_norm in partial_norm


# ── Match capture ─────────────────────────────────────────────────────────────


def capture_matches(db: DatabaseManager) -> int:
    """Capture Polymarket match-winner prices into game_odds_history."""
    now = datetime.now(timezone.utc)
    captured_at = now.replace(microsecond=0)
    capture_key = f"polymarket_{captured_at.isoformat()}"
    history_rows: list[dict] = []
    matched = 0
    unmatched = 0

    events = _fetch_events(MATCH_TAG_ID)
    logger.info("Polymarket tennis matches (tag=%s): %d events fetched", MATCH_TAG_ID, len(events))

    for event in events:
        slug_match = _SINGLES_SLUG_RE.match(event.get("slug", "") or "")
        if not slug_match:
            continue  # doubles / ITF / futures-under-same-tag -- out of scope
        tour = slug_match.group(1).upper()

        if not _is_match_event(event):
            continue

        market = _find_match_market(event)
        if not market:
            continue

        parsed = _parse_outcomes(market)
        if not parsed:
            continue

        names, prices = parsed
        if len(names) != 2 or len(prices) != 2:
            continue

        # Player 1 = first listed (home), Player 2 = second (away)
        player1, player2 = names[0], names[1]
        prob1, prob2 = prices[0], prices[1]

        # Skip markets with zero or near-zero prices (stale/resolved)
        if prob1 < 0.01 or prob2 < 0.01:
            continue

        # Convert probabilities to American odds
        try:
            ml_home = prob_to_american(prob1)
            ml_away = prob_to_american(prob2)
        except (ValueError, ZeroDivisionError):
            continue

        # Try to match to an existing tennis_matches row
        match_row = _fuzzy_match_to_tennis_matches(db, player1, player2, tour)

        if match_row:
            # Determine which Polymarket player maps to home/away
            home_norm = _normalize_poly_name(match_row["home_player"])
            p1_norm = _normalize_poly_name(player1)

            if p1_norm == home_norm or _name_contains(home_norm, p1_norm):
                home_ml, away_ml = ml_home, ml_away
                home_prob, away_prob = prob1, prob2
            else:
                # Swap: player1 is actually the away player
                home_ml, away_ml = ml_away, ml_home
                home_prob, away_prob = prob2, prob1

            history_rows.append({
                "sport": "tennis",
                "matchup_id": match_row["id"],
                "event_id": match_row.get("game_id"),
                "game_date": str(match_row["match_date"]),
                "home_team_name": match_row["home_player"],
                "away_team_name": match_row["away_player"],
                "bookmaker_count": 1,
                "home_ml": home_ml,
                "away_ml": away_ml,
                "vegas_prob_home": round(home_prob, 4),
                "capture_key": capture_key,
                "captured_at": captured_at,
                "books": {
                    "polymarket": {
                        "ml_home": home_ml,
                        "ml_away": away_ml,
                    }
                },
            })
            matched += 1
            logger.debug(
                "Matched: %s vs %s → tennis_matches id=%d (%s)",
                player1, player2, match_row["id"], tour,
            )
        else:
            unmatched += 1
            logger.warning(
                "Polymarket match not found in tennis_matches: %s vs %s (%s)",
                player1, player2, tour,
            )

    if history_rows:
        insert_game_odds_history_rows(db, history_rows)

    print(
        f"Polymarket tennis matches: {matched} matched + {unmatched} unmatched "
        f"({matched + unmatched} total match events)"
    )
    return matched


# ── Futures capture ───────────────────────────────────────────────────────────


def capture_futures(db: DatabaseManager) -> int:
    """Capture Polymarket futures/outright markets into polymarket_tennis_futures."""
    now = datetime.now(timezone.utc)
    captured_at = now.replace(microsecond=0)
    total_stored = 0

    for tag_id, tour in [(ATP_TAG_ID, "ATP"), (WTA_TAG_ID, "WTA")]:
        events = _fetch_events(tag_id)
        logger.info("Polymarket %s futures: %d events fetched", tour, len(events))

        for event in events:
            if _is_match_event(event):
                continue  # Skip match events — those go to capture_matches

            title = event.get("title", "")
            slug = event.get("slug", "")
            markets = event.get("markets", [])

            for market in markets:
                parsed = _parse_outcomes(market)
                if not parsed:
                    continue

                names, prices = parsed
                question = market.get("question", title)
                total_markets = len(markets)

                for i, (player_name, prob) in enumerate(zip(names, prices)):
                    if prob < 0.001:
                        continue  # Skip negligible probabilities

                    # bestAsk/bestBid available but outcomePrices is the simpler signal
                    yes_price = prob
                    no_price = 1.0 - prob if prob < 1.0 else None

                    db.execute(
                        """
                        INSERT INTO polymarket_tennis_futures (
                            tour, event_title, event_slug, market_question,
                            player_name, poly_prob, poly_yes_price, poly_no_price,
                            total_markets, captured_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (event_slug, player_name, captured_at) DO NOTHING
                        """,
                        (
                            tour, title, slug, question,
                            player_name, prob, yes_price, no_price,
                            total_markets, captured_at,
                        ),
                    )
                    total_stored += 1

    print(f"Polymarket tennis futures: {total_stored} player-outcomes stored")
    return total_stored


# ── CLI ───────────────────────────────────────────────────────────────────────


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Capture tennis data from Polymarket's Gamma API"
    )
    parser.add_argument(
        "--matches-only", action="store_true",
        help="Only capture match-winner prices (for frequent cadence)",
    )
    parser.add_argument(
        "--futures-only", action="store_true",
        help="Only capture futures/outright markets (less frequent)",
    )
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)

    if args.futures_only:
        capture_futures(db)
    elif args.matches_only:
        capture_matches(db)
    else:
        capture_matches(db)
        capture_futures(db)


if __name__ == "__main__":
    main()
