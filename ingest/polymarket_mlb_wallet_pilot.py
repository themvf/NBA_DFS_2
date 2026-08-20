#!/usr/bin/env python3
"""MLB Polymarket wallet-intelligence PILOT -- read-only, no DB, no UI.

Same shared engine as ingest/polymarket_tennis_wallet_pilot.py (see
ingest/polymarket_wallet_pilot_common.py's module docstring), pointed at
MLB single-game moneyline markets instead of tennis matches. This file
supplies MLB-specific discovery only.

Discovery tag: live-verified 2026-08-19 -- MLB game markets carry a
dedicated tag_id=100381 ("MLB"), distinct from the generic "baseball"
(id=678) and "Sports" (id=1) tags. Scanning 600 closed events under this
tag by volume, 595 (99.2%) matched the real game-slug pattern
"mlb-<away>-<home>-<date>" (e.g. "mlb-lad-col-2026-08-18"); scanning open
events found 95 of 183 currently live. This tag needed no separate
"futures vs match" disambiguation the way tennis did -- World Series/
division-winner futures use different slugs (e.g.
"world-series-champion-...") that the game-slug regex naturally excludes,
so a single filter does both jobs tennis needed two tags for.

Structural differences from tennis this pilot's numbers should be read
against, not silently assumed away:
  - MLB plays ~15 games/day across a ~180-day season vs tennis's handful
    of concurrent matches -- far more repeat opportunities per wallet per
    unit time, which should mean faster archetype-band/walk-forward
    seasoning if the same wallets are actually active across many games.
  - Games resolve same-day (hours), not multi-day -- fill-tape "early
    entry" therefore means something closer to "bet well before first
    pitch" than tennis's "bet well before the match", not directly
    comparable across sports without re-deriving bands (which this pilot
    does, per-sport, exactly for this reason).
  - Team markets (not individual competitors) mean a "wallet with an edge
    on the Dodgers" is a plausible, checkable hypothesis this pilot's
    output doesn't yet slice for -- left as a follow-up, not built here.

Read-only against public, no-auth Polymarket endpoints. No DB, no UI.
Research context only -- not investment or betting advice.

Usage:
    python -m ingest.polymarket_mlb_wallet_pilot [--max-markets 400]
        [--min-markets 5] [--out FILE]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from typing import Any, Dict, List, Optional

from ingest.polymarket_wallet_pilot_common import (
    GAMMA,
    analyze_and_partition,
    analyze_resolved_markets,
    api_get,
    classify_with_bands,
    compare_dev_holdout,
    derive_archetype_bands,
    fetch_wallet_open_positions,
    paginate_events,
    print_leaderboard,
    print_roi_leaderboard,
    rank_wallets,
    rank_wallets_by_roi,
    select_balanced_dev_holdout,
)

MLB_TAG_ID = 100381
_GAME_SLUG_RE = re.compile(r"^mlb-[a-z]{2,3}-[a-z]{2,3}-\d{4}-\d{2}-\d{2}$")


def _parse_outcomes(market: dict) -> Optional[tuple]:
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


def _is_game_market(names: List[str]) -> bool:
    return len(names) == 2 and all(n.strip().lower() not in {"yes", "no"} for n in names)


def fetch_all_resolved_game_markets() -> List[Dict[str, Any]]:
    """ALL discoverable resolved MLB game (moneyline/spread/total) markets
    (no volume truncation -- selection happens afterward, per-half, via
    select_balanced_dev_holdout)."""
    events = paginate_events(MLB_TAG_ID, closed="true", max_pages=20)  # ~2000-event safe ceiling
    markets: List[Dict[str, Any]] = []
    for event in events:
        if not _GAME_SLUG_RE.match(event.get("slug", "") or ""):
            continue  # futures / props / postseason specials -- out of scope
        for market in event.get("markets") or []:
            parsed = _parse_outcomes(market)
            if not parsed:
                continue
            names, prices = parsed
            if not _is_game_market(names):
                continue
            condition_id = market.get("conditionId")
            if not condition_id:
                continue
            winners = [n for n, p in zip(names, prices) if p > 0.99]
            if len(winners) != 1:
                continue  # unresolved / voided / ambiguous -- skip
            markets.append({
                "condition_id": condition_id,
                "question": str(market.get("question") or event.get("title") or ""),
                "winner": winners[0],
                "teams": names,
                "volume": float(market.get("volume") or 0),
                "end_date": str(event.get("endDate") or ""),
            })
    return markets


def fetch_open_game_markets() -> Dict[str, Dict[str, str]]:
    open_markets: Dict[str, Dict[str, str]] = {}
    events = api_get(f"{GAMMA}/events", {"tag_id": MLB_TAG_ID, "closed": "false", "limit": 100})
    for event in events or []:
        if not _GAME_SLUG_RE.match(event.get("slug", "") or ""):
            continue
        for market in event.get("markets") or []:
            parsed = _parse_outcomes(market)
            if not parsed or not _is_game_market(parsed[0]):
                continue
            condition_id = market.get("conditionId")
            if not condition_id:
                continue
            open_markets[condition_id] = {"question": str(market.get("question") or event.get("title") or "")}
    return open_markets


def _label(market: Dict[str, Any]) -> str:
    return market["question"][:40]


def run(max_markets: int, min_markets: int) -> Dict[str, Any]:
    all_markets = fetch_all_resolved_game_markets()
    print(f"resolved MLB game markets discovered: {len(all_markets)}", file=sys.stderr)

    # Split the FULL pool by date first, then take top-volume per half --
    # see select_balanced_dev_holdout's docstring. This specifically fixes
    # the 2026-08-19 run's bug: picking one global top-400-by-volume set
    # BEFORE splitting by date left only 3 of the top 30 dev-period wallets
    # with any activity at all in the holdout half, because MLB volume is
    # concentrated in specific high-profile games rather than spread evenly
    # across the season -- a coverage artifact, not a persistence finding.
    markets, dev_ids, holdout_ids, dev_range, holdout_range, excluded_undated = select_balanced_dev_holdout(
        all_markets, max_markets
    )
    print(f"selected for analysis: {len(markets)} ({len(dev_ids)} dev + {len(holdout_ids)} holdout)", file=sys.stderr)

    wallet_stats, total_fills = analyze_resolved_markets(markets, _label)
    qualified = rank_wallets(wallet_stats, min_markets)
    bands = derive_archetype_bands(qualified)
    for row in qualified:
        row["archetype"] = classify_with_bands(row, bands)
    roi_leaderboard = rank_wallets_by_roi(qualified)

    walkforward: Dict[str, Any] = {"status": "skipped", "reason": "not enough dated markets to split"}
    if dev_ids and holdout_ids:
        dev_stats, holdout_stats, wf_fills = analyze_and_partition(markets, dev_ids, holdout_ids, _label)
        dev_qualified = rank_wallets(dev_stats, max(3, min_markets // 2))
        dev_bands = derive_archetype_bands(dev_qualified)
        for row in dev_qualified:
            row["archetype"] = classify_with_bands(row, dev_bands)
        persistence = compare_dev_holdout(dev_qualified, holdout_stats)
        walkforward = {
            "status": "ok",
            "dev_date_range": dev_range,
            "holdout_date_range": holdout_range,
            "dev_markets": len(dev_ids),
            "holdout_markets": len(holdout_ids),
            "excluded_undated_markets": excluded_undated,
            "dev_bands": dev_bands,
            "dev_wallets_qualified": len(dev_qualified),
            "persistence": persistence,
        }

    print("fetching open game markets + top-wallet positioning...", file=sys.stderr)
    open_markets = fetch_open_game_markets()
    for row in qualified[:10]:
        row["open_mlb_positions"] = fetch_wallet_open_positions(row["wallet"], open_markets)

    return {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "markets_analyzed": len(markets),
        "fills_processed": total_fills,
        "wallets_seen": len(wallet_stats),
        "wallets_qualified": len(qualified),
        "min_markets": min_markets,
        "archetype_bands": bands,
        "open_game_markets": len(open_markets),
        "leaderboard": qualified[:50],
        "roi_leaderboard": roi_leaderboard[:50],
        "walkforward": walkforward,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--max-markets", type=int, default=400)
    parser.add_argument("--min-markets", type=int, default=5)
    parser.add_argument("--out", default="polymarket_mlb_wallet_pilot_output.json")
    args = parser.parse_args()

    output = run(args.max_markets, args.min_markets)
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(output, handle, indent=1)

    print_leaderboard(f"MLB game-market wallet leaderboard by PnL (min {args.min_markets} markets)", output["leaderboard"])
    print_roi_leaderboard("MLB game-market wallet leaderboard by ROI (min $1000 cost)", output["roi_leaderboard"])

    wf = output["walkforward"]
    print(f"\n=== Walk-forward persistence check ===")
    if wf["status"] != "ok":
        print(f"  skipped: {wf['reason']}")
    else:
        print(f"  dev period:     {wf['dev_date_range']} ({wf['dev_markets']} markets)")
        print(f"  holdout period: {wf['holdout_date_range']} ({wf['holdout_markets']} markets)")
        print(f"  dev-qualified wallets: {wf['dev_wallets_qualified']}  |  dev bands: {wf['dev_bands']}")
        rows = wf["persistence"]["wallets"]
        n_persisted = sum(1 for r in rows if r["holdout_status"] == "persisted")
        n_reversed = sum(1 for r in rows if r["holdout_status"] == "reversed")
        n_no_activity = sum(1 for r in rows if r["holdout_status"] == "no_holdout_activity")
        print(f"  of top {len(rows)} dev wallets: {n_persisted} persisted, {n_reversed} reversed, {n_no_activity} no holdout activity")
        for arch, stats in wf["persistence"]["by_archetype"].items():
            print(f"    {arch:<12} n={stats['n_with_holdout_activity']:>3}  avg holdout pnl=${stats['avg_holdout_pnl']:>9.2f}  avg holdout win%={stats['avg_holdout_win_rate']*100:.0f}%  persisted={stats['persisted_count']}")

    print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
