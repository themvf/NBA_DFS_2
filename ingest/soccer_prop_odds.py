"""Capture World Cup anytime-goalscorer odds per book (DK outlier detection).

Pinnacle does NOT post World Cup player props in the Odds API feed (probed
2026-07-02), so the sharp-anchor design used for MLB props is impossible here.
The honest substitute is cross-book outlier detection: capture every book's
anytime-goalscorer price per player and flag where DraftKings pays materially
more than the market median — still model-free, still audited.

Stored in prop_odds_history (sport='soccer', market='player_goal_scorer_anytime')
as books = {book_key: {yes: american_price, last_update}} — ATGS is a one-sided
market (Yes price only). Soccer game_id IS the Odds API event id, so matchup
linkage is a direct lookup (no name matching).

Settlement note (in model/line_alerts.py): DK settles soccer player props on
the 90-minute match only, so grading uses soccer_match_goals with minute <= 90.

Cost: 1 market x 3 regions = 3 credits per event; ~6 upcoming WC games on the
3h cadence is trivial. Tournament ends 2026-07-19 — this is a seasonal module.

Usage:
    python -m ingest.soccer_prop_odds
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime, timezone

import requests

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)

ODDS_BASE = "https://api.the-odds-api.com/v4"
SPORT_KEY = "soccer_fifa_world_cup"
MARKET = "player_goal_scorer_anytime"
REGIONS = "us,uk,eu"  # maximize book count — the median IS the anchor here
SLEEP_BETWEEN_CALLS = 0.5


def fetch_props(db: DatabaseManager, api_key: str) -> int:
    """Capture anytime-scorer odds for upcoming WC games. Returns rows written."""
    if not api_key:
        logger.warning("ODDS_API_KEY not set — skipping WC props fetch")
        return 0
    try:
        r = requests.get(f"{ODDS_BASE}/sports/{SPORT_KEY}/events",
                         params={"apiKey": api_key}, timeout=20)
        r.raise_for_status()
        events = r.json()
    except requests.RequestException as e:
        logger.warning("Odds API events request failed: %s", e)
        return 0

    now = datetime.now(timezone.utc)
    captured_at = now.replace(microsecond=0)
    capture_key = captured_at.isoformat()

    written = 0
    skipped_live = 0
    for ev in events:
        commence_iso = ev.get("commence_time", "")
        try:
            commence_dt = datetime.fromisoformat(commence_iso.replace("Z", "+00:00"))
        except ValueError:
            continue
        if commence_dt <= now:
            skipped_live += 1
            continue

        m = db.execute_one("SELECT id FROM soccer_matchups WHERE game_id = %s", (ev["id"],))
        matchup_id = m["id"] if m else None

        try:
            r2 = requests.get(
                f"{ODDS_BASE}/sports/{SPORT_KEY}/events/{ev['id']}/odds",
                params={"apiKey": api_key, "regions": REGIONS, "markets": MARKET,
                        "oddsFormat": "american", "dateFormat": "iso"},
                timeout=25,
            )
            r2.raise_for_status()
            data = r2.json()
        except requests.RequestException as e:
            logger.warning("WC props fetch failed for %s: %s", ev.get("id"), e)
            continue
        time.sleep(SLEEP_BETWEEN_CALLS)

        # {player: {book: {yes, last_update}}}. ATGS outcomes: name='Yes',
        # description=player, price. Some books use name=player directly.
        rows: dict[str, dict] = {}
        for bm in data.get("bookmakers", []):
            for mk in bm.get("markets", []):
                if mk.get("key") != MARKET:
                    continue
                for o in mk.get("outcomes", []):
                    player = o.get("description") or o.get("name")
                    if not player or player.lower() in ("yes", "no") or o.get("price") is None:
                        continue
                    if (o.get("name") or "").lower() == "no":
                        continue  # only the Yes side is comparable across books
                    rows.setdefault(player, {})[bm["key"]] = {
                        "yes": o["price"], "last_update": bm.get("last_update")}

        game_date = commence_dt.astimezone(timezone.utc).date().isoformat()
        with db.connect() as conn:
            cur = conn.cursor()
            for player, books in rows.items():
                cur.execute(
                    """
                    INSERT INTO prop_odds_history
                        (sport, event_id, matchup_id, game_date, commence_time,
                         home_team_name, away_team_name, market, player, books,
                         capture_key, captured_at)
                    VALUES ('soccer', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (sport, event_id, market, player, capture_key) DO NOTHING
                    """,
                    (ev["id"], matchup_id, game_date, commence_dt,
                     ev.get("home_team"), ev.get("away_team"), MARKET, player,
                     json.dumps(books), capture_key, captured_at),
                )
                written += 1

    msg = f"WC props: {written} anytime-scorer rows captured"
    if skipped_live:
        msg += f" ({skipped_live} in-play games skipped)"
    print(msg)
    return written


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    argparse.ArgumentParser(description="Capture WC anytime-scorer odds").parse_args()
    config = load_config()
    db = DatabaseManager(config.database_url)
    fetch_props(db, config.odds_api.api_key)
