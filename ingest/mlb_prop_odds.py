"""Capture MLB player-prop odds per book (D4, MLB prop-market expansion).

P0-verified (2026-07-02, pitcher_strikeouts + batter_total_bases; expanded
2026-07-08): each market below was checked against 5 real upcoming events for
BOTH DraftKings and Pinnacle presence (the detector requires both -- Pinnacle
alone can't anchor a DK-vs-Pinnacle value scan). One row per (event, market,
player, capture) with the full per-book detail:

    books = {book_key: {line, over, under, last_update}}

This feeds the dk_prop_value / prop_line_gap detectors in model/line_alerts.py
— the props analog of the game-line DK-vs-Pinnacle value scan, where the edge
thesis is strongest: prop lines are algorithmic, thin, and slow.

Markets (5, all DK+Pinnacle confirmed 4-5/5 events, 2026-07-08 probe):
  pitcher_strikeouts, batter_total_bases (original)
  pitcher_hits_allowed, pitcher_earned_runs, pitcher_outs (added)
NOT added: batter_home_runs -- Pinnacle posts it but DraftKings does NOT
(0/5 events), so it can never feed the DK-vs-Pinnacle detector regardless of
cost. Capturing it would burn credits for an unusable signal.

Cost discipline: props are PER-EVENT calls. Uses BOOKMAKERS=draftkings,pinnacle
instead of REGIONS=us,eu (2026-07-08 change) -- the Odds API prices bookmakers
requests at markets×1 credit regardless of book count, vs markets×regions for
a regions request. Same two books (the only ones the detector reads), HALF
the cost for the original 2 markets, and the 3-market expansion is markets×1
instead of markets×2 (verified via the x-requests-last response header: 4
credits/event old pattern -> 2 credits for identical 2-market data via
bookmakers param -> 5 credits for all 5 markets). Runs on the 3×/day
refresh_mlb_vegas cadence (user-approved net +45 credits/day for the
expansion, 2026-07-08), NOT the 30-minute game-line capture. Started games
are skipped (closing snapshots freeze at first pitch, and the feed serves
live props in-play).

Usage:
    python -m ingest.mlb_prop_odds
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timezone

import requests

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)

ODDS_BASE = "https://api.the-odds-api.com/v4"
MARKETS = (
    "pitcher_strikeouts,batter_total_bases,"
    "pitcher_hits_allowed,pitcher_earned_runs,pitcher_outs"
)
# The exact two books the detector reads. Costs markets×1 credit regardless of
# book count -- strictly cheaper than the old regions=us,eu (markets×regions)
# for identical data. Do not switch back to `regions` without re-checking cost.
BOOKMAKERS = "draftkings,pinnacle"
SLEEP_BETWEEN_CALLS = 0.5


def fetch_props(db: DatabaseManager, api_key: str) -> int:
    """Capture prop odds for all not-yet-started MLB games. Returns rows written."""
    if not api_key:
        logger.warning("ODDS_API_KEY not set — skipping MLB props fetch")
        return 0
    try:
        r = requests.get(f"{ODDS_BASE}/sports/baseball_mlb/events",
                         params={"apiKey": api_key}, timeout=20)
        r.raise_for_status()
        events = r.json()
    except requests.RequestException as e:
        logger.warning("Odds API events request failed: %s", e)
        return 0

    now = datetime.now(timezone.utc)
    captured_at = now.replace(microsecond=0)
    capture_key = captured_at.isoformat()

    # event home-team name -> ALL matchup rows, for settlement joins. A split
    # doubleheader is two rows for the same home team (game_id-first identity,
    # 2026-07-07) — each event resolves to the row with the nearest commence.
    matchups_by_home: dict[str, list[dict]] = {}
    for r_ in db.execute(
        """SELECT m.id, m.commence_time, t.name AS home_name FROM mlb_matchups m
           JOIN mlb_teams t ON t.team_id = m.home_team_id
           WHERE m.game_date >= CURRENT_DATE - 1"""):
        matchups_by_home.setdefault(r_["home_name"], []).append(r_)

    def _resolve_matchup_id(home: str, ev_dt: datetime) -> int | None:
        cands = matchups_by_home.get(home)
        if not cands:
            return None
        if len(cands) == 1:
            return cands[0]["id"]
        def _dist(c: dict) -> float:
            ct = c["commence_time"]
            return abs((ct - ev_dt).total_seconds()) if ct is not None else float("inf")
        return min(cands, key=_dist)["id"]

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

        try:
            r2 = requests.get(
                f"{ODDS_BASE}/sports/baseball_mlb/events/{ev['id']}/odds",
                params={"apiKey": api_key, "bookmakers": BOOKMAKERS, "markets": MARKETS,
                        "oddsFormat": "american", "dateFormat": "iso"},
                timeout=25,
            )
            r2.raise_for_status()
            data = r2.json()
        except requests.RequestException as e:
            logger.warning("Props fetch failed for %s: %s", ev.get("id"), e)
            continue
        time.sleep(SLEEP_BETWEEN_CALLS)

        # {(market, player): {book: {line, over, under, last_update}}}
        rows: dict[tuple[str, str], dict] = {}
        for bm in data.get("bookmakers", []):
            for mk in bm.get("markets", []):
                for o in mk.get("outcomes", []):
                    player = o.get("description")
                    if not player or o.get("point") is None:
                        continue
                    book = rows.setdefault((mk["key"], player), {}).setdefault(
                        bm["key"], {"last_update": bm.get("last_update")})
                    side = (o.get("name") or "").lower()
                    if side in ("over", "under"):
                        book["line"] = float(o["point"])
                        book[side] = o.get("price")

        matchup_id = _resolve_matchup_id(ev.get("home_team", ""), commence_dt)
        game_date = commence_dt.astimezone(timezone.utc).date().isoformat()
        with db.connect() as conn:
            cur = conn.cursor()
            for (market, player), books in rows.items():
                import json as _json
                cur.execute(
                    """
                    INSERT INTO prop_odds_history
                        (sport, event_id, matchup_id, game_date, commence_time,
                         home_team_name, away_team_name, market, player, books,
                         capture_key, captured_at)
                    VALUES ('mlb', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (sport, event_id, market, player, capture_key) DO NOTHING
                    """,
                    (ev["id"], matchup_id, game_date, commence_dt,
                     ev.get("home_team"), ev.get("away_team"), market, player,
                     _json.dumps(books), capture_key, captured_at),
                )
                written += 1

    msg = f"MLB props: {written} player-market rows captured"
    if skipped_live:
        msg += f" ({skipped_live} in-play games skipped)"
    print(msg)
    return written


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    argparse.ArgumentParser(description="Capture MLB prop odds (K + TB)").parse_args()
    config = load_config()
    db = DatabaseManager(config.database_url)
    fetch_props(db, config.odds_api.api_key)
