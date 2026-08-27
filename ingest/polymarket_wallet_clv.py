#!/usr/bin/env python3
"""Polymarket wallet screen v2 -- rank on CLOSING-LINE VALUE, not entry price.

Read-only research tooling. No DB writes, no schedule, no UI.

WHY THIS EXISTS
---------------
v1 (ingest/polymarket_{tennis,mlb}_wallet_pilot.py) ranked wallets on

    edge = Wilson win-rate floor - average entry price

and the answer, documented in full in docs/polymarket-wallet-tracker.md, was
that the metric measured TRADING STYLE rather than skill. 52% of the top-50
"edge" leaderboard turned out to be automated, and the non-automated,
sports-focused remainder lost 2.5% across 460 markets. The failure was not
"we picked the wrong wallets" -- it was that a metric built for directional
bettors returns a large, confident, meaningless number when fed
market-making flow, because for a quoter "average entry price" is simply
wherever the book happened to sit.

This module changes the question from

    did they buy cheap relative to how often they won?      (v1)

to

    after they bought, did the market move toward them?     (v2)

That is closing-line value, and it is the right instrument for one specific
reason: a market maker cannot systematically win it. A quoter earns the
spread by providing liquidity at the prevailing price; it is not
anticipating a move, so its average CLV is ~0 by construction. Only a wallet
trading ahead of information wins CLV repeatedly. This project already
established CLV is the faster instrument -- CLAUDE.md's Edge-Finding Roadmap
P1 notes it "converges ~10x faster than ROI (every bet scores, win or
lose)". We built that harness for our own bets and never pointed it at
wallets.

CLV also kills v1's single worst artifact for free. The wallet with 101
markets, a 100% win rate, $7.5M of cost and $8,103 of profit was buying
near-certainties at near-certain prices: bought at 0.99, closed at 0.99,
CLV ~= 0. Under v1 that wallet topped the leaderboard.

WHAT "THE CLOSE" MEANS HERE -- the load-bearing design decision
---------------------------------------------------------------
It is NOT the last trade before resolution. By then the market has already
absorbed the outcome, so "CLV" would collapse into a restatement of the
result and lose every property that makes it useful.

The close is the last pre-match price, exactly as in sportsbook CLV. That
requires a match start time, and Gamma supplies one: real head-to-head
markets carry `gameStartTime`.

Verified live 2026-08-27 across 946 closed markets, and the separation is
total: of 232 tennis singles markets, 159 carry gameStartTime and ALL 159
are "-vs-" head-to-head matches; the 73 without are ALL tournament-winner
futures (atp-winston-salem-winner, atp-cincinnati-winner, atp-finals-winner).
Zero misclassifications either way. So requiring gameStartTime is
simultaneously the CLV anchor AND a strictly better match filter than v1's
two-outcome-name heuristic. MLB shows the same shape (470/714).

Fills at or after gameStartTime are IN-PLAY. They are excluded from CLV --
we have no post-start benchmark to score them against -- but they are still
counted for the behavioural signals below, because heavy in-play
round-tripping is itself informative about what a wallet is.

BINARY-MARKET PRICE UNIFICATION
-------------------------------
A two-outcome market's prices sum to ~1, so a trade on outcome B at price p
is information about outcome A at 1-p. Every trade is therefore mapped into
a single reference-outcome series before the close is computed. This is not
a convenience: it roughly doubles the observations available for the close
and is exactly correct for a binary market.

ELIGIBILITY GATES COME BEFORE RANKING, NOT AFTER
------------------------------------------------
This is the second thing v1 got wrong. Its sport-focus filter was applied to
an already-ranked list, which let a 9-market wallet through and single
handedly flipped the survivor cohort's ROI from -2.5% to +14.6% -- the same
small-sample artifact the Wilson bound exists to suppress, reappearing on a
different axis.

Here the gates run first and a wallet that fails any one of them is never
ranked at all.

Critically, the automation screen is computed FROM THE SAME FILL TAPE we
already fetched, not from a separate per-wallet API sweep. v1's forensics
needed ~2 minutes of extra calls for 50 wallets and could therefore only be
run on a shortlist -- which is precisely why it ran after ranking. Here it
costs nothing and runs on every wallet.

WHAT THIS DOES NOT SOLVE
------------------------
Identification is not actionability. Following a wallet requires seeing its
fill and acting before the market absorbs it; the Data API reports fills
after the fact and our capture cadence is measured in hours. That latency
gap is independent of wallet quality and is a harder problem than this one.
A positive result here would be evidence worth acting on, not a strategy.

The honest prior remains negative: six documented no-edge results in this
project, and a base rate saying the top of this very leaderboard is mostly
machines. What is different is that the failure mode is now understood and
the fix is specific. If CLV comes back flat, that is a conclusive answer
rather than another metric artifact -- which is the point of using a measure
a market maker cannot fake.

Usage:
    python -m ingest.polymarket_wallet_clv --sport tennis [--max-markets 400]
    python -m ingest.polymarket_wallet_clv --sport mlb --out report.json
"""

from __future__ import annotations

import argparse
import json
import math
import random
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from ingest.polymarket_wallet_pilot_common import (
    fetch_market_fills,
    paginate_events,
    select_balanced_dev_holdout,
)

TENNIS_TAG_ID = 864
MLB_TAG_ID = 100381
_TENNIS_SLUG_RE = re.compile(r"^(atp|wta)-(?!doubles-)")

# --- close definition -------------------------------------------------------
# The close is volume-weighted over the final window before first serve /
# first pitch rather than a single last trade, because one odd-lot fill at a
# stale price should not define the benchmark every wallet is scored against.
# If the window is too thin we widen to the last N pregame trades; if even
# that fails the market is ineligible for CLV and is reported as such, never
# silently dropped.
CLOSE_WINDOW_S = 3600
CLOSE_MIN_TRADES = 3
CLOSE_FALLBACK_TRADES = 5

# --- eligibility gates (applied BEFORE ranking) -----------------------------
MIN_CLV_MARKETS = 30          # sample floor, fixed before looking at results
MIN_PREGAME_STAKE = 1000.0    # lottery-ticket filter, carried over from v1
MIN_HOLD_RATIO = 0.5          # directional bettor vs scalper
MIN_BUY_DOMINANCE = 0.6       # carried over from v1 bug #5
MAX_DUST_SHARE = 0.10         # sub-$1 notional -- market-maker odd lots
MAX_SAME_SECOND_SHARE = 0.05  # two fills in one second is not a human
DUST_NOTIONAL = 1.0

BOOTSTRAP_ROUNDS = 2000


# ---------------------------------------------------------------------------
# discovery
# ---------------------------------------------------------------------------

def _parse_outcomes(market: dict) -> Optional[Tuple[List[str], List[float]]]:
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
    if len(names) != len(prices) or len(names) != 2:
        return None
    return names, prices


def parse_ts(raw: Any) -> Optional[int]:
    """Gamma serves gameStartTime as '2024-11-11 19:30:00+00'. Returns a UNIX
    timestamp so it compares directly against the Data API's integer fill
    timestamps, with no timezone ambiguity in between."""
    if not raw:
        return None
    text = str(raw).strip().replace(" ", "T", 1)
    if text.endswith("+00"):
        text = text[:-3] + "+00:00"
    text = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def discover_markets(sport: str) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Resolved head-to-head markets that carry a usable gameStartTime.

    Returns (markets, rejection_counts). Rejections are counted by reason and
    printed, so a shrinking sample always surfaces as a reason rather than as
    an unexplained smaller number."""
    tag = TENNIS_TAG_ID if sport == "tennis" else MLB_TAG_ID
    events = paginate_events(tag, closed="true", max_pages=20)
    markets: List[Dict[str, Any]] = []
    rejects: Counter = Counter()
    for event in events:
        slug = event.get("slug") or ""
        if sport == "tennis":
            matched = _TENNIS_SLUG_RE.match(slug)
            if not matched:
                rejects["not_atp_wta_singles"] += 1
                continue
            tour = matched.group(1).upper()
        else:
            tour = "MLB"
        for market in event.get("markets") or []:
            parsed = _parse_outcomes(market)
            if not parsed:
                rejects["not_two_outcome"] += 1
                continue
            names, prices = parsed
            start_ts = parse_ts(market.get("gameStartTime"))
            if start_ts is None:
                # Futures / tournament-winner market, or a match whose start
                # time Gamma never recorded. Either way there is no pre-match
                # close to score against.
                rejects["no_game_start_time"] += 1
                continue
            condition_id = market.get("conditionId")
            if not condition_id:
                rejects["no_condition_id"] += 1
                continue
            winners = [n for n, p in zip(names, prices) if p > 0.99]
            if len(winners) != 1:
                rejects["unresolved_or_voided"] += 1
                continue
            markets.append({
                "condition_id": condition_id,
                "question": str(market.get("question") or event.get("title") or ""),
                "tour": tour,
                "winner": winners[0],
                "outcomes": names,
                "game_start_ts": start_ts,
                "volume": float(market.get("volume") or 0),
                "end_date": str(event.get("endDate") or ""),
            })
    return markets, dict(rejects)


# ---------------------------------------------------------------------------
# closing price
# ---------------------------------------------------------------------------

def in_reference_terms(outcome: str, price: float, reference: str) -> float:
    """Express a trade in reference-outcome terms. A binary market's two
    prices sum to ~1, so a trade on the other side at p is the reference side
    at 1-p."""
    return price if outcome == reference else 1.0 - price


def closing_price(
    fills: List[Dict[str, Any]], reference: str, start_ts: int, outcomes: List[str]
) -> Optional[float]:
    """Volume-weighted reference-outcome price over the final pregame window.

    Returns None when the market never traded before its start time, which
    makes it ineligible for CLV rather than defaulting to something."""
    valid = set(outcomes)
    pregame: List[Tuple[int, float, float]] = []
    for fill in fills:
        outcome = str(fill.get("outcome") or "")
        if outcome not in valid:
            continue
        try:
            ts = int(fill.get("timestamp") or 0)
            size = float(fill.get("size") or 0)
            price = float(fill.get("price") or 0)
        except (TypeError, ValueError):
            continue
        if ts <= 0 or ts >= start_ts or size <= 0 or not 0.0 < price < 1.0:
            continue
        pregame.append((ts, size, in_reference_terms(outcome, price, reference)))
    if not pregame:
        return None
    pregame.sort(key=lambda row: row[0])
    window = [row for row in pregame if row[0] >= start_ts - CLOSE_WINDOW_S]
    if len(window) < CLOSE_MIN_TRADES:
        window = pregame[-CLOSE_FALLBACK_TRADES:]
    total_size = sum(row[1] for row in window)
    if total_size <= 0:
        return None
    return sum(row[1] * row[2] for row in window) / total_size


# ---------------------------------------------------------------------------
# per-market, per-wallet measurement
# ---------------------------------------------------------------------------

def measure_market(
    fills: List[Dict[str, Any]], market: Dict[str, Any]
) -> Tuple[Dict[str, Dict[str, Any]], Optional[float]]:
    """Per-wallet CLV plus the behavioural signals the eligibility gates need.

    Everything here comes from one fill tape we already had to fetch for
    settlement, so the automation screen is free -- which is what lets it run
    as a gate on every wallet instead of a post-filter on a shortlist."""
    reference = market["winner"]
    start_ts = market["game_start_ts"]
    close_ref = closing_price(fills, reference, start_ts, market["outcomes"])

    per_wallet: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "clv_num": 0.0, "clv_stake": 0.0,
        "pregame_buy_cash": 0.0, "pregame_trades": 0, "inplay_trades": 0,
        "buy_cash": 0.0, "sell_cash": 0.0,
        "gross_size": 0.0, "trades": 0, "dust": 0,
        "net_by_outcome": defaultdict(float),
        "seconds": Counter(),
        "cash": 0.0, "cost": 0.0, "net_win": 0.0,
        "name": "",
    })

    for fill in fills:
        wallet = str(fill.get("proxyWallet") or "")
        outcome = str(fill.get("outcome") or "")
        side = str(fill.get("side") or "").upper()
        try:
            size = float(fill.get("size") or 0)
            price = float(fill.get("price") or 0)
            ts = int(fill.get("timestamp") or 0)
        except (TypeError, ValueError):
            continue
        if not wallet or size <= 0 or side not in ("BUY", "SELL"):
            continue
        entry = per_wallet[wallet]
        name = str(fill.get("name") or fill.get("pseudonym") or "")
        if name:
            entry["name"] = name

        notional = size * price
        entry["trades"] += 1
        entry["gross_size"] += size
        entry["seconds"][ts] += 1
        if notional < DUST_NOTIONAL:
            entry["dust"] += 1
        if ts >= start_ts:
            entry["inplay_trades"] += 1
        else:
            entry["pregame_trades"] += 1

        signed = size if side == "BUY" else -size
        entry["net_by_outcome"][outcome] += signed
        entry["cash"] += -notional if side == "BUY" else notional
        if side == "BUY":
            entry["cost"] += notional
            entry["buy_cash"] += notional
        else:
            entry["sell_cash"] += notional
        if outcome == reference:
            entry["net_win"] += signed

        # CLV scores PREGAME BUYS only. A sell is an exit, not a position
        # taken at a price; scoring it would credit a scalper for unwinding
        # into a move it did not predict.
        if side == "BUY" and ts < start_ts and close_ref is not None:
            close_for_outcome = close_ref if outcome == reference else 1.0 - close_ref
            entry["clv_num"] += notional * (close_for_outcome - price)
            entry["clv_stake"] += notional
            entry["pregame_buy_cash"] += notional

    out: Dict[str, Dict[str, Any]] = {}
    for wallet, entry in per_wallet.items():
        net_abs = sum(abs(v) for v in entry["net_by_outcome"].values())
        same_second = sum(c for c in entry["seconds"].values() if c > 1)
        out[wallet] = {
            "clv_num": entry["clv_num"],
            "clv_stake": entry["clv_stake"],
            "pregame_buy_cash": entry["pregame_buy_cash"],
            "pregame_trades": entry["pregame_trades"],
            "inplay_trades": entry["inplay_trades"],
            "buy_cash": entry["buy_cash"],
            "sell_cash": entry["sell_cash"],
            "gross_size": entry["gross_size"],
            "net_abs_size": net_abs,
            "trades": entry["trades"],
            "dust": entry["dust"],
            "same_second": same_second,
            "pnl": entry["cash"] + max(entry["net_win"], 0.0),
            "cost": entry["cost"],
            "name": entry["name"],
            "clv_market": (entry["clv_num"] / entry["clv_stake"]) if entry["clv_stake"] > 0 else None,
        }
    return out, close_ref


# ---------------------------------------------------------------------------
# aggregation + gates
# ---------------------------------------------------------------------------

_SUM_FLOAT_KEYS = (
    "clv_num", "clv_stake", "pregame_buy_cash", "buy_cash",
    "sell_cash", "gross_size", "net_abs_size",
)
_SUM_INT_KEYS = ("pregame_trades", "inplay_trades", "trades", "dust", "same_second")


def new_wallet() -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "name": "", "clv_markets": 0, "obs": [],
        "pnl": 0.0, "cost": 0.0, "markets": 0, "wins": 0,
    }
    for key in _SUM_FLOAT_KEYS:
        base[key] = 0.0
    for key in _SUM_INT_KEYS:
        base[key] = 0
    return base


def accumulate(
    markets: List[Dict[str, Any]], progress_every: int = 25
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, int]]:
    wallets: Dict[str, Dict[str, Any]] = defaultdict(new_wallet)
    counts = {"markets": 0, "fills": 0, "clv_eligible": 0, "no_pregame_trades": 0}
    for i, market in enumerate(markets, 1):
        try:
            fills = fetch_market_fills(market["condition_id"])
        except Exception as exc:  # noqa: BLE001 - one bad market must not end the run
            print(f"  ! {market['question'][:40]}: {exc}", file=sys.stderr)
            continue
        counts["markets"] += 1
        counts["fills"] += len(fills)
        measured, close_ref = measure_market(fills, market)
        if close_ref is None:
            counts["no_pregame_trades"] += 1
        else:
            counts["clv_eligible"] += 1
        for wallet, stats in measured.items():
            agg = wallets[wallet]
            if stats["name"]:
                agg["name"] = stats["name"]
            agg["markets"] += 1
            agg["pnl"] += stats["pnl"]
            agg["cost"] += stats["cost"]
            if stats["pnl"] > 0:
                agg["wins"] += 1
            for key in _SUM_FLOAT_KEYS + _SUM_INT_KEYS:
                agg[key] += stats[key]
            if stats["clv_market"] is not None and stats["clv_stake"] > 0:
                agg["clv_markets"] += 1
                agg["obs"].append((market["condition_id"], stats["clv_stake"], stats["clv_num"]))
        if i % progress_every == 0:
            print(f"  processed {i}/{len(markets)} markets ({counts['fills']} fills)", file=sys.stderr)
    return wallets, counts


def gate(wallet: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Every gate a wallet fails, not just the first -- a wallet rejected for
    one reason and a wallet rejected for four are different objects, and the
    distribution of failure reasons IS the base-rate result."""
    fails: List[str] = []
    if wallet["clv_markets"] < MIN_CLV_MARKETS:
        fails.append("sample")
    if wallet["pregame_buy_cash"] < MIN_PREGAME_STAKE:
        fails.append("stake")
    gross = wallet["gross_size"]
    hold = (wallet["net_abs_size"] / gross) if gross > 0 else 0.0
    if hold < MIN_HOLD_RATIO:
        fails.append("scalper")
    total_cash = wallet["buy_cash"] + wallet["sell_cash"]
    dominance = (wallet["buy_cash"] / total_cash) if total_cash > 0 else 0.0
    if dominance < MIN_BUY_DOMINANCE:
        fails.append("sell_side")
    trades = wallet["trades"]
    if trades > 0 and wallet["dust"] / trades > MAX_DUST_SHARE:
        fails.append("dust")
    if trades > 0 and wallet["same_second"] / trades > MAX_SAME_SECOND_SHARE:
        fails.append("same_second")
    return (not fails), fails


def bootstrap_clv_ci(
    obs: List[Tuple[str, float, float]], rounds: int = BOOTSTRAP_ROUNDS, seed: int = 12345
) -> Tuple[float, float]:
    """95% CI on dollar-weighted CLV, resampling MARKETS with replacement.

    Clustered by market because a wallet's fills inside one market are not
    independent observations -- the same rule every other study in this
    project follows (date-clustered there, market-clustered here)."""
    if len(obs) < 2:
        return (float("nan"), float("nan"))
    rng = random.Random(seed)
    n = len(obs)
    means: List[float] = []
    for _ in range(rounds):
        picks = [obs[rng.randrange(n)] for _ in range(n)]
        stake = sum(p[1] for p in picks)
        if stake <= 0:
            continue
        means.append(sum(p[2] for p in picks) / stake)
    if len(means) < 2:
        return (float("nan"), float("nan"))
    means.sort()
    lo = means[int(0.025 * len(means))]
    hi = means[min(int(0.975 * len(means)), len(means) - 1)]
    return (lo, hi)


def rank_by_clv(wallets: Dict[str, Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Counter]:
    rows: List[Dict[str, Any]] = []
    reasons: Counter = Counter()
    for address, wallet in wallets.items():
        ok, fails = gate(wallet)
        if not ok:
            for fail in fails:
                reasons[fail] += 1
            reasons["__rejected__"] += 1
            continue
        clv = wallet["clv_num"] / wallet["clv_stake"] if wallet["clv_stake"] > 0 else 0.0
        lo, hi = bootstrap_clv_ci(wallet["obs"])
        gross = wallet["gross_size"] or 1.0
        total_cash = (wallet["buy_cash"] + wallet["sell_cash"]) or 1.0
        rows.append({
            "wallet": address,
            "name": wallet["name"],
            "clv": clv,
            "clv_lo": lo,
            "clv_hi": hi,
            "clv_markets": wallet["clv_markets"],
            "pregame_stake": wallet["pregame_buy_cash"],
            "hold_ratio": wallet["net_abs_size"] / gross,
            "buy_dominance": wallet["buy_cash"] / total_cash,
            "inplay_share": wallet["inplay_trades"] / max(wallet["trades"], 1),
            "trades": wallet["trades"],
            "markets": wallet["markets"],
            "roi": (wallet["pnl"] / wallet["cost"]) if wallet["cost"] > 0 else 0.0,
            "pnl": wallet["pnl"],
            "cost": wallet["cost"],
            "win_rate": wallet["wins"] / max(wallet["markets"], 1),
        })
    rows.sort(key=lambda r: -r["clv"])
    return rows, reasons


def cohort_clv(
    wallets: Dict[str, Dict[str, Any]], rows: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Pooled CLV across every eligible wallet.

    This -- not the top of the leaderboard -- is the headline. A leaderboard
    always has a top; the question is whether the eligible population as a
    whole beats the close. Ranking a population whose pooled CLV is zero just
    re-finds noise, which is exactly how v1 failed."""
    obs: List[Tuple[str, float, float]] = []
    for row in rows:
        obs.extend(wallets[row["wallet"]]["obs"])
    stake = sum(o[1] for o in obs)
    if stake <= 0:
        return {"n_wallets": len(rows), "n_obs": 0, "clv": None, "ci": (None, None)}
    lo, hi = bootstrap_clv_ci(obs)
    return {
        "n_wallets": len(rows),
        "n_obs": len(obs),
        "stake": stake,
        "clv": sum(o[2] for o in obs) / stake,
        "ci": (lo, hi),
    }


# ---------------------------------------------------------------------------
# reporting
# ---------------------------------------------------------------------------

def print_leaderboard(title: str, rows: List[Dict[str, Any]], limit: int = 20) -> None:
    print()
    print("=" * 108)
    print(title)
    print("=" * 108)
    if not rows:
        print("  (no wallet cleared the eligibility gates)")
        return
    print(f"{'#':>3} {'wallet':<14} {'CLV':>9} {'95% CI':>19} {'mkts':>5} "
          f"{'stake':>11} {'hold':>5} {'buy%':>5} {'inpl':>5} {'ROI':>8}")
    print("-" * 108)
    for i, row in enumerate(rows[:limit], 1):
        ident = (row["name"] or row["wallet"])[:14]
        ci = ("n/a" if math.isnan(row["clv_lo"])
              else f"[{row['clv_lo']:+.3f},{row['clv_hi']:+.3f}]")
        print(f"{i:>3} {ident:<14} {row['clv']:>+9.4f} {ci:>19} {row['clv_markets']:>5} "
              f"${row['pregame_stake']:>10,.0f} {row['hold_ratio']:>5.2f} "
              f"{row['buy_dominance']:>5.2f} {row['inplay_share']:>5.2f} {row['roi']:>+8.1%}")


def print_funnel(total: int, reasons: Counter, eligible: int) -> None:
    print()
    print("=" * 108)
    print("ELIGIBILITY FUNNEL (gates applied BEFORE ranking)")
    print("=" * 108)
    print(f"  wallets seen                          {total:>9,}")
    print(f"  rejected                              {reasons.get('__rejected__', 0):>9,}")
    labels = [
        ("sample", f"under {MIN_CLV_MARKETS} CLV-scored markets"),
        ("stake", f"under ${MIN_PREGAME_STAKE:,.0f} pregame stake"),
        ("scalper", f"hold ratio under {MIN_HOLD_RATIO}"),
        ("sell_side", f"buy dominance under {MIN_BUY_DOMINANCE}"),
        ("dust", f"over {MAX_DUST_SHARE:.0%} sub-$1 trades"),
        ("same_second", f"over {MAX_SAME_SECOND_SHARE:.0%} same-second trades"),
    ]
    for key, desc in labels:
        print(f"    - {desc:<45} {reasons.get(key, 0):>9,}")
    print(f"  ELIGIBLE                              {eligible:>9,}")


def run(sport: str, max_markets: int, out_path: Optional[str]) -> Dict[str, Any]:
    all_markets, rejects = discover_markets(sport)
    print(f"{sport}: head-to-head markets with gameStartTime: {len(all_markets)}", file=sys.stderr)
    for reason, count in sorted(rejects.items(), key=lambda kv: -kv[1]):
        print(f"  discovery reject {reason}: {count}", file=sys.stderr)
    if not all_markets:
        print("no markets discovered -- nothing to do", file=sys.stderr)
        return {}

    markets, dev_ids, holdout_ids, dev_range, holdout_range, undated = select_balanced_dev_holdout(
        all_markets, max_markets
    )
    if not markets:
        markets = sorted(all_markets, key=lambda m: -m["volume"])[:max_markets]
        dev_ids, holdout_ids = set(), set()
    print(f"selected {len(markets)} markets "
          f"({len(dev_ids)} dev + {len(holdout_ids)} holdout, {undated} undated excluded)",
          file=sys.stderr)

    wallets, counts = accumulate(markets)
    rows, reasons = rank_by_clv(wallets)
    pooled = cohort_clv(wallets, rows)

    print()
    print(f"markets processed {counts['markets']}, fills {counts['fills']:,}, "
          f"CLV-eligible markets {counts['clv_eligible']} "
          f"(no pregame trades: {counts['no_pregame_trades']})")
    print_funnel(len(wallets), reasons, len(rows))

    print()
    print("=" * 108)
    print("HEADLINE -- pooled CLV across ALL eligible wallets")
    print("=" * 108)
    if pooled.get("clv") is None:
        print("  no eligible observations")
    else:
        lo, hi = pooled["ci"]
        if lo > 0:
            verdict = "BEATS the close"
        elif hi < 0:
            verdict = "LOSES to the close"
        else:
            verdict = "is INDISTINGUISHABLE from the close"
        print(f"  wallets {pooled['n_wallets']:,} | market-observations {pooled['n_obs']:,} | "
              f"stake ${pooled['stake']:,.0f}")
        print(f"  dollar-weighted CLV {pooled['clv']:+.4f}  95% CI [{lo:+.4f}, {hi:+.4f}]")
        print(f"  VERDICT: the eligible population {verdict}.")

    print_leaderboard(f"{sport.upper()} -- eligible wallets ranked by CLV", rows)

    report = {
        "sport": sport,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "markets_selected": len(markets),
        "counts": counts,
        "discovery_rejects": rejects,
        "dev_range": dev_range,
        "holdout_range": holdout_range,
        "gates": {
            "min_clv_markets": MIN_CLV_MARKETS,
            "min_pregame_stake": MIN_PREGAME_STAKE,
            "min_hold_ratio": MIN_HOLD_RATIO,
            "min_buy_dominance": MIN_BUY_DOMINANCE,
            "max_dust_share": MAX_DUST_SHARE,
            "max_same_second_share": MAX_SAME_SECOND_SHARE,
        },
        "funnel": dict(reasons),
        "pooled": pooled,
        "leaderboard": rows[:50],
    }
    if out_path:
        with open(out_path, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, default=str)
        print(f"wrote {out_path}", file=sys.stderr)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Polymarket wallet CLV screen (v2)")
    parser.add_argument("--sport", choices=["tennis", "mlb"], default="tennis")
    parser.add_argument("--max-markets", type=int, default=400)
    parser.add_argument("--out")
    args = parser.parse_args()
    run(args.sport, args.max_markets, args.out)


if __name__ == "__main__":
    main()
