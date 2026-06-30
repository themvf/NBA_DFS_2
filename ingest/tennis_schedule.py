"""Fetch Wimbledon match schedule + Vegas odds into tennis_matches.

MVP scope: **odds only**, from The Odds API (already integrated for NBA/MLB/soccer).
Both Wimbledon draws are live during the tournament:

    tennis_atp_wimbledon   (men's, best-of-5)
    tennis_wta_wimbledon   (women's, best-of-3)

The feed carries fixtures (commence time, both player names) alongside three
markets, so one bulk call per tour seeds both the schedule and the lines:

  * ``h2h``     — 2-way moneyline (no draw).  Vig removed across the two sides.
  * ``totals``  — total games O/U (e.g. 22.5).
  * ``spreads`` — game/set handicap for the favorite (e.g. -4.5 / -1.5).

Consensus is computed by averaging in IMPLIED-PROBABILITY space across all books
(averaging American odds arithmetically is invalid).  Player names are stored
inline from the feed — no separate players table for the odds-only MVP.

Usage:
    python -m ingest.tennis_schedule                    # all upcoming, both tours
    python -m ingest.tennis_schedule --tour atp         # one tour
    python -m ingest.tennis_schedule --date 2026-06-29  # one match-day (UTC)
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone

import requests

from config import load_config
from db.database import DatabaseManager
from model.soccer_bet_rating import american_to_prob, prob_to_american

logger = logging.getLogger(__name__)

ODDS_BASE = "https://api.the-odds-api.com/v4"
REGIONS = "us,uk,eu"
TOURS = {
    "atp": ("ATP", "tennis_atp_wimbledon"),
    "wta": ("WTA", "tennis_wta_wimbledon"),
}


def _consensus_american(prices: list[int]) -> int | None:
    """Consensus American odds by averaging in implied-probability space."""
    if not prices:
        return None
    avg_prob = sum(american_to_prob(p) for p in prices) / len(prices)
    return prob_to_american(avg_prob)


def _two_way_probs(home_ml: int | None, away_ml: int | None) -> tuple[float | None, float | None]:
    """Vig-removed home/away probabilities from a 2-way moneyline."""
    if home_ml is None or away_ml is None:
        return None, None
    rh, ra = american_to_prob(home_ml), american_to_prob(away_ml)
    total = rh + ra
    if total <= 0:
        return None, None
    return round(rh / total, 4), round(ra / total, 4)


def _consensus_handicap_line(points: list[float]) -> float | None:
    """Most common handicap line across books (tennis spreads cluster tightly)."""
    if not points:
        return None
    # Round to nearest 0.5 and take the mode; ties → median-ish first.
    from collections import Counter
    rounded = [round(p * 2) / 2 for p in points]
    return Counter(rounded).most_common(1)[0][0]


def fetch_tour(db: DatabaseManager, api_key: str, tour_key: str, game_date: str | None) -> int:
    """Fetch one tour's fixtures + odds, upsert into tennis_matches. Returns count."""
    tour_label, sport_key = TOURS[tour_key]
    try:
        resp = requests.get(
            f"{ODDS_BASE}/sports/{sport_key}/odds/",
            params={
                "apiKey": api_key,
                "regions": REGIONS,
                "markets": "h2h,totals,spreads",
                "oddsFormat": "american",
                "dateFormat": "iso",
            },
            timeout=20,
        )
        resp.raise_for_status()
        events = resp.json()
    except requests.RequestException as e:
        logger.warning("Odds API %s request failed: %s", sport_key, e)
        return 0

    upserted = 0
    for ev in events:
        commence_iso = ev.get("commence_time")
        home = ev.get("home_team")
        away = ev.get("away_team")
        if not commence_iso or not home or not away:
            continue
        try:
            commence_dt = datetime.fromisoformat(commence_iso.replace("Z", "+00:00"))
        except ValueError:
            continue
        ev_date = commence_dt.astimezone(timezone.utc).date().isoformat()
        if game_date and ev_date != game_date:
            continue

        home_prices: list[int] = []
        away_prices: list[int] = []
        total_points: list[float] = []
        over_prices: list[int] = []
        under_prices: list[int] = []
        hcap_points: list[float] = []
        hcap_home_prices: list[int] = []
        hcap_away_prices: list[int] = []
        books = ev.get("bookmakers") or []

        for bm in books:
            for market in bm.get("markets", []):
                key = market.get("key")
                outs = market.get("outcomes", [])
                if key == "h2h":
                    for o in outs:
                        if o.get("name") == home:
                            home_prices.append(o["price"])
                        elif o.get("name") == away:
                            away_prices.append(o["price"])
                elif key == "totals":
                    over = next((o for o in outs if o.get("name") == "Over"), None)
                    under = next((o for o in outs if o.get("name") == "Under"), None)
                    if over and over.get("point") is not None:
                        total_points.append(float(over["point"]))
                        over_prices.append(over["price"])
                        if under:
                            under_prices.append(under["price"])
                elif key == "spreads":
                    h = next((o for o in outs if o.get("name") == home), None)
                    a = next((o for o in outs if o.get("name") == away), None)
                    if h and h.get("point") is not None:
                        hcap_points.append(float(h["point"]))
                        hcap_home_prices.append(h["price"])
                        if a:
                            hcap_away_prices.append(a["price"])

        home_ml = _consensus_american(home_prices)
        away_ml = _consensus_american(away_prices)
        p_home, p_away = _two_way_probs(home_ml, away_ml)
        total_line = round(sum(total_points) / len(total_points) * 2) / 2 if total_points else None
        over_odds = _consensus_american(over_prices)
        under_odds = _consensus_american(under_prices)
        set_handicap = _consensus_handicap_line(hcap_points)
        hcap_home = _consensus_american(hcap_home_prices)
        hcap_away = _consensus_american(hcap_away_prices)

        db.execute(
            """
            INSERT INTO tennis_matches (
                game_id, tour, tournament, match_date, commence_time,
                home_player, away_player, home_ml, away_ml,
                home_win_prob, away_win_prob, total_games_line, over_odds, under_odds,
                set_handicap, handicap_home_odds, handicap_away_odds, n_books, fetched_at
            ) VALUES (
                %s, %s, 'Wimbledon', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, NOW()
            )
            ON CONFLICT (tour, match_date, home_player, away_player) DO UPDATE SET
                game_id = EXCLUDED.game_id,
                commence_time = EXCLUDED.commence_time,
                home_ml = EXCLUDED.home_ml,
                away_ml = EXCLUDED.away_ml,
                home_win_prob = EXCLUDED.home_win_prob,
                away_win_prob = EXCLUDED.away_win_prob,
                total_games_line = EXCLUDED.total_games_line,
                over_odds = EXCLUDED.over_odds,
                under_odds = EXCLUDED.under_odds,
                set_handicap = EXCLUDED.set_handicap,
                handicap_home_odds = EXCLUDED.handicap_home_odds,
                handicap_away_odds = EXCLUDED.handicap_away_odds,
                n_books = EXCLUDED.n_books,
                fetched_at = NOW()
            """,
            (
                ev.get("id"), tour_label, ev_date, commence_dt, home, away,
                home_ml, away_ml, p_home, p_away, total_line, over_odds, under_odds,
                set_handicap, hcap_home, hcap_away, len(books),
            ),
        )
        upserted += 1

    print(f"Tennis {tour_label}: {upserted} matches upserted with Vegas lines"
          + (f" for {game_date}" if game_date else ""))
    return upserted


def fetch_schedule_and_odds(db: DatabaseManager, api_key: str,
                            tour: str | None = None, game_date: str | None = None) -> int:
    if not api_key:
        logger.warning("ODDS_API_KEY not set — cannot fetch tennis schedule")
        return 0
    keys = [tour] if tour else list(TOURS.keys())
    return sum(fetch_tour(db, api_key, k, game_date) for k in keys)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Fetch Wimbledon schedule + odds")
    parser.add_argument("--tour", choices=["atp", "wta"], help="One tour only (default: both)")
    parser.add_argument("--date", help="Kickoff date YYYY-MM-DD (UTC). Default: all upcoming")
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    fetch_schedule_and_odds(db, config.odds_api.api_key, args.tour, args.date)
