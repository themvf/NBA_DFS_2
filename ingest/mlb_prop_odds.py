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

Markets: 16 (see MARKETS). Books: 10 (see BOOKMAKERS). Both were re-derived
empirically on 2026-08-15 -- see those constants for the full coverage matrix
and the reason each slot exists.

Cost discipline: props are PER-EVENT calls priced at

    credits = n_markets x ceil(n_books / 10)

MEASURED on 2026-08-15, not taken from docs: 8/9/10 books all cost markets x 1,
while 11/12/16 books cost markets x 2. This corrects the earlier note here that
book count was free "regardless" -- it is free only to 10, and the 11th book
doubles the bill. Hence exactly 10 books. Never use REGIONS= (markets x
n_regions, strictly worse).

At 16 markets x 10 books x ~15 events x 3 captures/day this is ~720 credits/day
(~21.6k/30d) on a shared key -- budget it deliberately; exhaustion silently
degrades every other sport's capture too.

Runs on the 3x/day refresh_mlb_vegas cadence, NOT the 30-minute game-line
capture. Started games are skipped (closing snapshots freeze at first pitch,
and the feed serves live props in-play).

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
# ── Markets (expanded 2026-08-15) ────────────────────────────────────────────
# Re-probed empirically against ALL 15 upcoming events x 8 books (234 credits),
# counting only PAIRED quotes (a player+point with BOTH over and under), since
# a one-sided quote cannot anchor a same-proposition comparison.
#
# Kept: every market with >=1 BETTABLE book posting paired quotes on >=50% of
# events. "Bettable" excludes pinnacle (sharp reference, user's jurisdiction
# cannot bet it) and polymarket (prediction market, different settlement).
#
# NOT added: batter_home_runs / batter_first_home_run -- these are one-sided
# "to hit a HR" (yes-only) markets, so no bettable book offers a paired quote
# (the earlier unpaired probe made them look available; they are not usable).
# batter_strikeouts: no book posts it at all.
MARKETS = ",".join((
    "pitcher_strikeouts",    # Pinnacle same-line 100% of events
    "batter_total_bases",    # 100%
    "pitcher_outs",          #  93%
    "pitcher_hits_allowed",  #  80%
))
# ── Why only four (2026-08-15 anchor census, 16 markets x 15 events) ─────────
# Both detectors need a SAME-LINE Pinnacle quote. Exactly four markets clear a
# 60% coverage gate; the other twelve fail in three distinct ways:
#   Pinnacle absent entirely  pitcher_earned_runs (was 98% in July, decayed
#     98->51->1->0 over weeks), pitcher_walks, pitcher_record_a_win, and every
#     batter market except total_bases.
#   Anchor present, never comparable  batter_runs_scored -- Pinnacle posts 87%
#     of the time but at a DIFFERENT line than DK every single time. An anchor
#     that never matches the proposition is not an anchor.
#   No execution side  batter_home_runs -- DraftKings posts 0% (Pinnacle 100%).
#
# Capturing an unanchored market buys rows that can never produce a verdict.
# The census answered that question for 223 credits; a continuous exploratory
# tranche would have cost ~7,200/month to answer it again. Coverage does move,
# so re-run the census QUARTERLY (223 credits) -- a market that becomes
# anchored is a legitimate future candidate and gets added then.
#
# Cost at four markets: 4 x ceil(10 books/10) x ~15 events x 3 passes/day
#   = 180 credits/day = ~5,400/30d, against a ~20,000/month shared key.

# ── Books (expanded 2026-08-15) ──────────────────────────────────────────────
# COST RULE, measured not assumed (x-requests-last, one event, 4 markets):
#     books  8 -> 4 credits    books 11 -> 8 credits
#     books  9 -> 4 credits    books 12 -> 8 credits
#     books 10 -> 4 credits    books 16 -> 8 credits
#   => cost = markets x ceil(n_books / 10)
#
# This CORRECTS the earlier note (and CLAUDE.md) claiming book count is free
# "regardless" -- it is free only up to 10. The 11th book DOUBLES the bill.
# So the book list is capped at exactly 10 and every slot has to earn itself.
# Never switch back to `regions=` (markets x n_regions, strictly worse).
#
# Roles differ and the consumer MUST respect them -- a price at a book the user
# cannot bet is a reference, never a recommendation:
#   execution  draftkings betmgm fanatics williamhill_us fanduel betrivers
#   reference  pinnacle, and (until jurisdiction is confirmed) espnbet
#              hardrockbet fliff
#
# 2026-08-15 coverage probe, share of 15 events with a paired quote:
#   pitcher_strikeouts     DK 100 FD 100 MGM 80 BR 87 FAN 100 | PIN 67
#   pitcher_outs           DK 100 FD  93 MGM100 CZR100 FAN 100 | PIN  0
#   pitcher_hits_allowed   DK  80        MGM 67       FAN 100 | PIN  0
#   pitcher_earned_runs    DK  80        MGM100       FAN 100 | PIN  0
#   batter_total_bases     DK 100        MGM 80 CZR100 FAN 100 | PIN 67
#   batter_hits/rbis/1B    DK 100        MGM 80       FAN 100 | PIN  0
#
# NOTE the Pinnacle column: it has COLLAPSED on the pitcher markets. Stored
# history shows pitcher_earned_runs going 98% -> 51% -> 1% -> 0% across recent
# weeks, and a direct 6-event check found 0/6 for outs/hits_allowed/earned_runs.
# Both detectors in model/line_alerts.py anchor on Pinnacle, so those markets
# now produce NO alerts. Expanding the book set is what makes a replacement
# anchor (bettable-book consensus) possible -- see CLAUDE.md.
#
# Full-market survey 2026-08-15 (17 markets x 3 events x regions=us,us2,eu,uk,
# 204 credits) found 15 books posting MLB props. Paired-market coverage:
#   draftkings 15 | fliff 15 | espnbet 13 | hardrockbet 13 | hardrockbet_oh 12
#   betmgm 12 | fanatics 11 | bovada 7 | betparx 6 | pinnacle 5
#   betonlineag 4 | williamhill_us 4 | fanduel 2 | betrivers 1 | ballybet 0
#
# The 10 slots, and why each is here:
#   6 currently-executable books (draftkings betmgm fanatics williamhill_us
#     fanduel betrivers) -- needed for best-price execution even where their
#     market coverage is thin, because a price you cannot bet is not a price.
#   pinnacle -- the fair-value REFERENCE. Not bettable in this jurisdiction;
#     both detectors anchor on it, so it is non-negotiable despite 5 markets.
#   espnbet, hardrockbet, fliff -- the three highest-coverage books not already
#     included (13/13/15 markets). Whether they are EXECUTABLE depends on the
#     user's jurisdiction; until confirmed they are reference-only and must not
#     be offered as a bet (see the execution/reference split in CLAUDE.md).
#
# Cut, with reasons: hardrockbet_oh (Ohio-only duplicate of hardrockbet),
# bovada + betonlineag (offshore), betparx (regional, 6 markets),
# ballybet (one-sided quotes only), polymarket (posts ZERO MLB player props --
# 0/15 events across all 16 markets on the 2026-08-15 probe).
BOOKMAKERS = ",".join((
    # executable
    "draftkings", "betmgm", "fanatics", "williamhill_us", "fanduel", "betrivers",
    # reference (NOT bettable here)
    "pinnacle",
    # high-coverage; executability unconfirmed -> treat as reference until known
    "espnbet", "hardrockbet", "fliff",
))
assert len(BOOKMAKERS.split(",")) <= 10, "11th book doubles the credit cost"
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
