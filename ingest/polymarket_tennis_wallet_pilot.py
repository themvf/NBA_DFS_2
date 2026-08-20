#!/usr/bin/env python3
"""Tennis Polymarket wallet-intelligence PILOT -- read-only, no DB, no UI.

v2 (2026-08-19): scaled discovery depth, added a chronological walk-forward
split, and replaced the earnings-pilot's copied archetype bands with ones
derived from tennis's own qualified-wallet distribution. See
ingest/polymarket_wallet_pilot_common.py's module docstring for the
shared-engine rationale -- this file supplies tennis-specific discovery only.

Question this pilot exists to answer, before any DB/UI work: does
Polymarket tennis match-market fill-tape volume support wallet-level
signal, AND does any apparent "skill" survive a look at a later period the
wallet wasn't selected on? v1 (60 markets, single sample) found deep
participation (23,883 distinct wallets, 6,632 qualified at >=3 markets) but
couldn't say anything about persistence. v2 adds that check.

Discovery tag: NOT ingest/polymarket_tennis.py's ATP_TAG_ID (101232) /
WTA_TAG_ID (102123) -- those are almost entirely tournament futures/props
(live-verified 2026-08-18: scanning 500 closed ATP-tag events by volume
surfaced exactly 1 real head-to-head match, and 0 live matches existed
under either tag at capture time, despite real matches actively trading).
Real match events instead carry the generic tag_id=864 ("Tennis") --
99.6% match-event density (498/500 scanned), 89 live matches at capture
time. This was very likely the root cause of pinnacle_polymarket_delta
firing zero tennis alerts ever (memory: detector-health-check.md);
ingest/polymarket_tennis.py's capture_matches() has been fixed to use the
same tag=864 discovery this pilot uses (see that file's docstring).

Read-only against public, no-auth Polymarket endpoints. No DB, no UI.
Research context only -- not investment or betting advice.

Usage:
    python -m ingest.polymarket_tennis_wallet_pilot [--max-markets 400]
        [--min-markets 5] [--tour ATP|WTA|both] [--out FILE]
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
    print_confidence_leaderboard,
    print_leaderboard,
    print_roi_leaderboard,
    rank_wallets,
    rank_wallets_by_confidence,
    rank_wallets_by_roi,
    select_balanced_dev_holdout,
)

# Real match events are slugged "atp-<players>-<date>" / "wta-..." for
# singles, "atp-doubles-..." for doubles, "itf-..." for lower-tier
# futures/challenger events. Doubles and ITF are out of this project's
# existing tennis scope (tennis_matches only carries ATP/WTA singles).
SINGLES_TAG_ID = 864
_SINGLES_SLUG_RE = re.compile(r"^(atp|wta)-(?!doubles-)")
_SKIP_OUTCOMES = {"over", "under", "yes", "no", "draw"}


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


def _is_match_market(names: List[str]) -> bool:
    if len(names) != 2:
        return False
    return all(n.strip().lower() not in _SKIP_OUTCOMES for n in names)


def _singles_tour(slug: str) -> Optional[str]:
    m = _SINGLES_SLUG_RE.match(slug or "")
    return m.group(1).upper() if m else None


def fetch_all_resolved_match_markets() -> List[Dict[str, Any]]:
    """ALL discoverable resolved ATP/WTA singles match-winner markets (no
    volume truncation -- selection happens afterward, per-half, via
    select_balanced_dev_holdout so the dev/holdout split doesn't bias which
    markets survive it)."""
    events = paginate_events(SINGLES_TAG_ID, closed="true", max_pages=20)  # ~2000 events safe ceiling
    markets: List[Dict[str, Any]] = []
    for event in events:
        tour = _singles_tour(event.get("slug", ""))
        if tour is None:
            continue  # doubles / ITF / non-match event -- out of scope
        for market in event.get("markets") or []:
            parsed = _parse_outcomes(market)
            if not parsed:
                continue
            names, prices = parsed
            if not _is_match_market(names):
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
                "tour": tour,
                "winner": winners[0],
                "players": names,
                "volume": float(market.get("volume") or 0),
                "end_date": str(event.get("endDate") or ""),
            })
    return markets


def fetch_open_match_markets() -> Dict[str, Dict[str, str]]:
    open_markets: Dict[str, Dict[str, str]] = {}
    events = api_get(f"{GAMMA}/events", {"tag_id": SINGLES_TAG_ID, "closed": "false", "limit": 100})
    for event in events or []:
        tour = _singles_tour(event.get("slug", ""))
        if tour is None:
            continue
        for market in event.get("markets") or []:
            parsed = _parse_outcomes(market)
            if not parsed or not _is_match_market(parsed[0]):
                continue
            condition_id = market.get("conditionId")
            if not condition_id:
                continue
            open_markets[condition_id] = {
                "question": str(market.get("question") or event.get("title") or ""),
                "tour": tour,
            }
    return open_markets


def _label(market: Dict[str, Any]) -> str:
    return market["question"][:40]


def run(max_markets: int, min_markets: int, tour_filter: Optional[str]) -> Dict[str, Any]:
    all_markets = fetch_all_resolved_match_markets()
    if tour_filter:
        all_markets = [m for m in all_markets if m["tour"] == tour_filter]
    print(f"resolved singles match markets discovered: {len(all_markets)}", file=sys.stderr)

    # Split the FULL pool by date first, then take top-volume per half --
    # see select_balanced_dev_holdout's docstring for why (fixes the MLB
    # coverage bug from the 2026-08-19 run).
    markets, dev_ids, holdout_ids, dev_range, holdout_range, excluded_undated = select_balanced_dev_holdout(
        all_markets, max_markets
    )
    print(f"selected for analysis: {len(markets)} ({len(dev_ids)} dev + {len(holdout_ids)} holdout)", file=sys.stderr)

    # ── Full-sample leaderboard (dev+holdout combined) ──────────────────────
    wallet_stats, total_fills = analyze_resolved_markets(markets, _label)
    qualified = rank_wallets(wallet_stats, min_markets)
    bands = derive_archetype_bands(qualified)
    for row in qualified:
        row["archetype"] = classify_with_bands(row, bands)
    roi_leaderboard = rank_wallets_by_roi(qualified)
    confidence_leaderboard = rank_wallets_by_confidence(qualified)

    # ── Walk-forward: does dev-period edge persist into holdout? ───────────
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

    print("fetching open match markets + top-wallet positioning...", file=sys.stderr)
    open_markets = fetch_open_match_markets()
    if tour_filter:
        open_markets = {k: v for k, v in open_markets.items() if v["tour"] == tour_filter}
    for row in qualified[:10]:
        row["open_tennis_positions"] = fetch_wallet_open_positions(row["wallet"], open_markets)

    return {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "tour_filter": tour_filter or "both",
        "markets_analyzed": len(markets),
        "fills_processed": total_fills,
        "wallets_seen": len(wallet_stats),
        "wallets_qualified": len(qualified),
        "min_markets": min_markets,
        "archetype_bands": bands,
        "open_match_markets": len(open_markets),
        "leaderboard": qualified[:50],
        "roi_leaderboard": roi_leaderboard[:50],
        "confidence_leaderboard": confidence_leaderboard[:50],
        "walkforward": walkforward,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--max-markets", type=int, default=400)
    parser.add_argument("--min-markets", type=int, default=5)
    parser.add_argument("--tour", choices=["ATP", "WTA", "both"], default="both")
    parser.add_argument("--out", default="polymarket_tennis_wallet_pilot_output.json")
    args = parser.parse_args()

    tour_filter = None if args.tour == "both" else args.tour
    output = run(args.max_markets, args.min_markets, tour_filter)
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(output, handle, indent=1)

    print_confidence_leaderboard(
        f"Tennis match-market wallet leaderboard by Wilson-confident win rate (min {args.min_markets} markets)",
        output["confidence_leaderboard"],
    )
    print_leaderboard(f"Tennis match-market wallet leaderboard by PnL (min {args.min_markets} markets)", output["leaderboard"])
    print_roi_leaderboard("Tennis match-market wallet leaderboard by ROI (min $1000 cost)", output["roi_leaderboard"])

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
