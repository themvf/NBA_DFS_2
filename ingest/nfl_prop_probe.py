"""Probe NFL player-prop market coverage on The Odds API. READ-ONLY.

Answers the P0 gate in docs/nfl-dfs-spec.md section 2: a market key
existing in the provider's documentation is NOT evidence that books post
it. This repo has been burned by that distinction twice -- batter_home_runs
(DraftKings posted nothing) and batter_runs_scored (Pinnacle posted a
different line every time, so it could never anchor a comparison).

Writes nothing to the database. Prints a coverage matrix and the exact
credit cost, then exits.

WHAT IT MEASURES, per market:

  events     events where ANY probed book posted the market
  DK         events where DraftKings posted it   (the execution book)
  PIN        events where Pinnacle posted it     (the fair-value anchor)
  paired     events where DK posted a PAIRED quote -- a player+point
             carrying BOTH over and under
  same-line  events where DK and Pinnacle posted the SAME point for at
             least one shared player

The paired/one-sided split is load-bearing and the two consumers of this
data want opposite things:

  * A DK-vs-Pinnacle VALUE DETECTOR needs paired same-line quotes, because
    a one-sided price cannot be de-vigged against its own other side. This
    is why ingest/mlb_prop_odds.py rejects batter_home_runs.
  * A DFS PROJECTION does not. `player_anytime_td` is a yes-only market by
    nature, and it is the single most valuable NFL projection input (a
    touchdown is 6 of the ~15 points a typical flex player scores). It is
    de-vigged ACROSS players in the market, the same power method already
    used in model/soccer_first_scorer.py.

So do not copy MLB's "reject one-sided markets" rule here without asking
which consumer is being served. Report both, decide per consumer.

COST, measured not assumed (see docs/the-odds-api.md and the note in
ingest/mlb_prop_odds.py):

    credits = n_markets x ceil(n_books / 10)   PER EVENT

8/9/10 books all cost markets x 1; the 11th book doubles the bill. Never
use regions= (markets x n_regions, strictly worse). At 7 markets, <=10
books and the default 5 events this probe costs 35 credits against a
20,000/month key shared by every sport.

Usage:
    python -m ingest.nfl_prop_probe [--events 5] [--markets a,b,c]
"""

from __future__ import annotations

import argparse
import logging
import math
import time
from collections import defaultdict
from typing import Any

import requests

from config import load_config

logger = logging.getLogger(__name__)

ODDS_BASE = "https://api.the-odds-api.com/v4"
SPORT = "americanfootball_nfl"

# The 7 documented NFL player-prop markets. Confirmed to EXIST in The Odds
# API market documentation 2026-09-01; whether books post them is exactly
# what this probe measures.
MARKETS: tuple[str, ...] = (
    "player_pass_yds",
    "player_pass_tds",
    "player_pass_interceptions",
    "player_rush_yds",
    "player_receptions",
    "player_reception_yds",
    "player_anytime_td",
)

# Same 10-book slate ingest/mlb_prop_odds.py settled on. Ten is the cap:
# an 11th book doubles the credit cost for zero extra markets.
BOOKMAKERS: tuple[str, ...] = (
    # executable
    "draftkings", "betmgm", "fanatics", "williamhill_us", "fanduel", "betrivers",
    # reference (NOT bettable here)
    "pinnacle",
    # high-coverage; executability unconfirmed -> treat as reference until known
    "espnbet", "hardrockbet", "fliff",
)
assert len(BOOKMAKERS) <= 10, "an 11th book doubles the credit cost"

EXECUTION_BOOK = "draftkings"
REFERENCE_BOOK = "pinnacle"
SLEEP_BETWEEN_CALLS = 0.5


def _quota(response: requests.Response) -> str:
    head = response.headers
    return (
        f"last={head.get('x-requests-last', '?')} "
        f"used={head.get('x-requests-used', '?')} "
        f"remaining={head.get('x-requests-remaining', '?')}"
    )


def fetch_events(api_key: str, limit: int) -> list[dict[str, Any]]:
    """Upcoming NFL events. The /events endpoint is FREE."""
    response = requests.get(
        f"{ODDS_BASE}/sports/{SPORT}/events",
        params={"apiKey": api_key},
        timeout=20,
    )
    response.raise_for_status()
    events = response.json()
    print(f"/events returned {len(events)} upcoming NFL games ({_quota(response)}, free endpoint)")
    return events[:limit]


def probe(api_key: str, markets: tuple[str, ...], event_limit: int) -> int:
    if not api_key:
        print("ODDS_API_KEY not set - cannot probe. Nothing was spent.")
        return 1

    events = fetch_events(api_key, event_limit)
    if not events:
        print("No upcoming NFL events. Nothing to probe, nothing spent.")
        return 0

    per_event_cost = len(markets) * math.ceil(len(BOOKMAKERS) / 10)
    print(
        f"\nProbing {len(markets)} markets x {len(BOOKMAKERS)} books x {len(events)} events "
        f"= {per_event_cost} credits/event, {per_event_cost * len(events)} total.\n"
    )

    # market -> counters
    seen: dict[str, set[str]] = defaultdict(set)
    dk_seen: dict[str, set[str]] = defaultdict(set)
    pin_seen: dict[str, set[str]] = defaultdict(set)
    dk_paired: dict[str, set[str]] = defaultdict(set)
    same_line: dict[str, set[str]] = defaultdict(set)
    books_seen: dict[str, set[str]] = defaultdict(set)
    dk_players: dict[str, int] = defaultdict(int)
    outcome_shapes: dict[str, set[str]] = defaultdict(set)

    last_response: requests.Response | None = None

    for event in events:
        event_id = event["id"]
        label = f"{event.get('away_team')} @ {event.get('home_team')}"
        try:
            response = requests.get(
                f"{ODDS_BASE}/sports/{SPORT}/events/{event_id}/odds",
                params={
                    "apiKey": api_key,
                    "markets": ",".join(markets),
                    "bookmakers": ",".join(BOOKMAKERS),
                    "oddsFormat": "american",
                },
                timeout=30,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            print(f"  {label}: FAILED ({type(exc).__name__}: {exc})")
            continue
        last_response = response
        payload = response.json()

        # book -> market -> player -> {point -> set(over/under-ish names)}
        by_book: dict[str, dict[str, dict[str, dict[Any, set[str]]]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
        )
        for bookmaker in payload.get("bookmakers", []):
            book = bookmaker.get("key", "")
            for market in bookmaker.get("markets", []):
                key = market.get("key", "")
                if key not in markets:
                    continue
                seen[key].add(event_id)
                books_seen[key].add(book)
                for outcome in market.get("outcomes", []):
                    # For over/under props the player is in `description`
                    # and the side in `name`. For yes-only markets the
                    # provider may put the player in either field, so
                    # record the shape rather than assuming one.
                    player = outcome.get("description") or outcome.get("name") or ""
                    side = (outcome.get("name") or "").lower()
                    point = outcome.get("point")
                    outcome_shapes[key].add(
                        f"name={'<player>' if not outcome.get('description') else side}"
                        f",description={'yes' if outcome.get('description') else 'no'}"
                        f",point={'yes' if point is not None else 'no'}"
                    )
                    by_book[book][key][player][point].add(side)

        for key in markets:
            dk = by_book.get(EXECUTION_BOOK, {}).get(key, {})
            pin = by_book.get(REFERENCE_BOOK, {}).get(key, {})
            if dk:
                dk_seen[key].add(event_id)
                dk_players[key] += len(dk)
            if pin:
                pin_seen[key].add(event_id)
            # Paired = some player+point carries BOTH over and under.
            for points in dk.values():
                for point, sides in points.items():
                    if point is not None and {"over", "under"} <= sides:
                        dk_paired[key].add(event_id)
            # Same-line = a shared player quoted at an identical point.
            for player, dk_points in dk.items():
                pin_points = pin.get(player)
                if not pin_points:
                    continue
                if set(dk_points) & set(pin_points):
                    same_line[key].add(event_id)

        print(f"  {label}: {_quota(response)}")
        time.sleep(SLEEP_BETWEEN_CALLS)

    n = len(events)
    print(f"\n{'market':<28} {'events':>7} {'DK':>4} {'PIN':>4} {'DK+PIN':>7} {'paired':>7} {'same-line':>10} {'DK players':>11}")
    print("-" * 84)
    for key in markets:
        both = len(dk_seen[key] & pin_seen[key])
        print(
            f"{key:<28} {len(seen[key]):>4}/{n:<2} {len(dk_seen[key]):>4} {len(pin_seen[key]):>4} "
            f"{both:>7} {len(dk_paired[key]):>7} {len(same_line[key]):>10} {dk_players[key]:>11}"
        )

    print("\nOutcome shape per market (how the provider labels players/sides):")
    for key in markets:
        shapes = ", ".join(sorted(outcome_shapes[key])) or "(market absent)"
        print(f"  {key:<28} {shapes}")

    print("\nBooks posting each market:")
    for key in markets:
        found = ", ".join(sorted(books_seen[key])) or "(none)"
        print(f"  {key:<28} {found}")

    if last_response is not None:
        print(f"\nFinal quota: {_quota(last_response)}")
    print(
        "\nREAD THIS BEFORE ACTING ON THE TABLE:\n"
        "  * 'paired' and 'same-line' gate a DK-vs-Pinnacle VALUE DETECTOR.\n"
        "  * A DFS PROJECTION does not need either -- a yes-only market like\n"
        "    player_anytime_td is de-vigged across players, not against its\n"
        "    own other side. Judge each market against the consumer that\n"
        "    will actually use it."
    )
    return 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=5, help="events to probe (default 5)")
    parser.add_argument("--markets", type=str, default=",".join(MARKETS))
    args = parser.parse_args()

    markets = tuple(m.strip() for m in args.markets.split(",") if m.strip())
    config = load_config()
    return probe(config.odds_api.api_key, markets, max(1, args.events))


if __name__ == "__main__":
    raise SystemExit(main())
