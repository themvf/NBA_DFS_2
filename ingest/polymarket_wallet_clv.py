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
from typing import Any, Dict, List, Optional, Set, Tuple

from ingest.polymarket_wallet_pilot_common import (
    DATA,
    TRADES_PAGE,
    api_get,
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

# --- fill tape --------------------------------------------------------------
# The Data API serves /trades NEWEST-FIRST and ignores every sort parameter
# tried (order=timestamp, ascending=true, sortDirection=ASC all return the
# same descending page -- verified live 2026-08-27). Truncation therefore
# eats the OLDEST trades, which for a sports market is exactly the pregame
# window CLV is measured in. That makes the depth limit load-bearing here in
# a way it never was for v1's settlement, which only needed net position and
# is indifferent to which end of the tape is missing.
#
# The hard ceiling is 10,000: offset 10,000 returns a row, 10,500 returns
# HTTP 400 (verified against an $18M market). The shared engine caps at
# 6,000, leaving 40% of the reachable tape -- the oldest 40% -- unfetched.
MAX_TRADES_PER_MARKET = 10000

# --- market volume band -----------------------------------------------------
# Selecting the highest-volume markets -- inherited from v1, where it was
# correct -- is actively hostile to CLV. Measured 2026-08-27 with a
# one-call-per-market probe (read the trade at offset 9,500 and ask whether
# it predates the match) over a volume-stratified sample of all 24,567
# tennis markets:
#
#   decile 1-9  ($10 .. $10,266 median volume)   pregame reachable  12/12 each
#   decile 10   ($575,560 median)                pregame reachable  10/12
#
# Reachability is essentially total everywhere EXCEPT the extreme top, which
# is exactly where a top-volume selection puts every market it picks. That is
# why the first full run lost 36% of its sample: not a property of the
# market, a property of how the sample was chosen.
#
# The band's upper bound sits at the point where reachability starts to fail;
# the lower bound drops markets too thin to carry wallets (decile 1 markets
# have single-digit dollars of volume). Mid-volume markets are also far
# cheaper to fetch -- a short tape is one page, not twenty -- so the band
# buys sample size and speed at once.
MIN_MARKET_VOLUME = 1000.0
MAX_MARKET_VOLUME = 350000.0

BOOTSTRAP_ROUNDS = 2000


# ---------------------------------------------------------------------------
# fill tape
# ---------------------------------------------------------------------------

def fetch_market_fills(condition_id: str) -> Tuple[List[Dict[str, Any]], bool]:
    """Newest-first fill tape. Returns (fills, hit_ceiling).

    hit_ceiling is reported rather than swallowed: a truncated tape may have
    lost its entire pregame window, and a market silently contributing no
    CLV observations is indistinguishable from one where nobody traded."""
    fills: List[Dict[str, Any]] = []
    offset = 0
    while offset < MAX_TRADES_PER_MARKET:
        try:
            page = api_get(f"{DATA}/trades", {
                "market": condition_id, "limit": TRADES_PAGE,
                "offset": offset, "takerOnly": "false",
            })
        except Exception:  # noqa: BLE001 - offset ceiling surfaces as HTTP 400
            return fills, True
        if not isinstance(page, list) or not page:
            return fills, False
        fills.extend(page)
        if len(page) < TRADES_PAGE:
            return fills, False
        offset += TRADES_PAGE
    return fills, True


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
    counts = {
        "markets": 0, "fills": 0, "clv_eligible": 0, "no_pregame_trades": 0,
        "truncated": 0, "truncated_and_lost_pregame": 0,
    }
    for i, market in enumerate(markets, 1):
        try:
            fills, hit_ceiling = fetch_market_fills(market["condition_id"])
        except Exception as exc:  # noqa: BLE001 - one bad market must not end the run
            print(f"  ! {market['question'][:40]}: {exc}", file=sys.stderr)
            continue
        counts["markets"] += 1
        counts["fills"] += len(fills)
        if hit_ceiling:
            counts["truncated"] += 1
        measured, close_ref = measure_market(fills, market)
        if close_ref is None:
            counts["no_pregame_trades"] += 1
            if hit_ceiling:
                # The tape ran out before reaching pregame -- this market was
                # lost TO truncation, not to an absence of pregame trading.
                counts["truncated_and_lost_pregame"] += 1
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


def walk_forward(
    wallets: Dict[str, Dict[str, Any]],
    rows: List[Dict[str, Any]],
    dev_ids: Set[str],
    holdout_ids: Set[str],
    top_n: int = 20,
) -> Dict[str, Any]:
    """Does dev-period CLV rank predict holdout-period CLV?

    Selection uses the dev half ONLY; the score is the holdout half, which
    those wallets were not chosen on. This is the check v1 ran honestly and
    still failed -- 6/14 and 3/6 persisted, a coin flip -- and the reason it
    failed was the metric, not the split. So the split is kept unchanged and
    pointed at the new metric.

    No re-fetching: every observation already carries its condition_id, so
    the two halves are a partition of data we have.

    The eligible-population baseline is reported alongside, because "the top
    20 beat the close in holdout" means nothing if every eligible wallet did.
    What matters is whether the SELECTED wallets beat the rest."""
    if not dev_ids or not holdout_ids:
        return {"available": False, "reason": "no chronological split available"}

    def split(address: str) -> Tuple[List[Any], List[Any]]:
        obs = wallets[address]["obs"]
        return ([o for o in obs if o[0] in dev_ids],
                [o for o in obs if o[0] in holdout_ids])

    def clv_of(obs: List[Any]) -> Optional[float]:
        stake = sum(o[1] for o in obs)
        return (sum(o[2] for o in obs) / stake) if stake > 0 else None

    scored = []
    for row in rows:
        dev_obs, hold_obs = split(row["wallet"])
        dev_clv, hold_clv = clv_of(dev_obs), clv_of(hold_obs)
        if dev_clv is None or hold_clv is None:
            continue
        scored.append({
            "wallet": row["wallet"], "name": row["name"],
            "dev_clv": dev_clv, "dev_markets": len(dev_obs),
            "holdout_clv": hold_clv, "holdout_markets": len(hold_obs),
        })
    if not scored:
        return {"available": False, "reason": "no wallet is active in both halves"}

    scored.sort(key=lambda r: -r["dev_clv"])
    # Never select the entire population. Asking for the top 20 out of 7
    # wallets leaves no unselected remainder, which silently disables the
    # selection-gap comparison -- the sharpest part of the test -- while
    # still printing a confident-looking holdout number. Observed live on
    # the first complete tennis run (7 wallets, top_n=20).
    effective_n = min(top_n, len(scored) // 2)
    if effective_n < 1:
        return {
            "available": False,
            "reason": f"only {len(scored)} wallet(s) active in both halves -- "
                      "too few to split into selected and unselected groups",
        }
    selected = scored[:effective_n]
    rest = scored[effective_n:]

    sel_obs: List[Any] = []
    for row in selected:
        sel_obs.extend(split(row["wallet"])[1])
    rest_obs: List[Any] = []
    for row in rest:
        rest_obs.extend(split(row["wallet"])[1])

    lo, hi = bootstrap_clv_ci(sel_obs) if len(sel_obs) > 1 else (float("nan"), float("nan"))
    return {
        "available": True,
        "n_both_halves": len(scored),
        "top_n": len(selected),
        "top_n_requested": top_n,
        "selected_holdout_clv": clv_of(sel_obs),
        "selected_holdout_ci": (lo, hi),
        "selected_holdout_obs": len(sel_obs),
        "rest_holdout_clv": clv_of(rest_obs),
        "rest_holdout_obs": len(rest_obs),
        "persisted": sum(1 for r in selected if r["holdout_clv"] > 0),
        "rows": selected,
    }


# ---------------------------------------------------------------------------
# reporting
# ---------------------------------------------------------------------------

def print_walk_forward(wf: Dict[str, Any]) -> None:
    print()
    print("=" * 108)
    print("WALK-FORWARD -- selected on the DEV half, scored on the HOLDOUT half")
    print("=" * 108)
    if not wf.get("available"):
        print(f"  unavailable: {wf.get('reason')}")
        return
    lo, hi = wf["selected_holdout_ci"]
    ci = "n/a" if math.isnan(lo) else f"[{lo:+.4f}, {hi:+.4f}]"
    print(f"  wallets active in both halves      {wf['n_both_halves']:>9,}")
    note = ("" if wf["top_n"] == wf.get("top_n_requested")
            else f" -- reduced from {wf['top_n_requested']} to keep a comparison group")
    print(f"  selected (top {wf['top_n']} by dev CLV){note}")
    print(f"    holdout CLV                      {wf['selected_holdout_clv']:>+9.4f}  95% CI {ci}"
          f"  (n={wf['selected_holdout_obs']:,})")
    if wf["rest_holdout_clv"] is not None:
        print(f"  everyone else")
        print(f"    holdout CLV                      {wf['rest_holdout_clv']:>+9.4f}"
              f"  (n={wf['rest_holdout_obs']:,})")
        gap = wf["selected_holdout_clv"] - wf["rest_holdout_clv"]
        print(f"  selection gap                      {gap:>+9.4f}"
              f"   <- this, not the absolute level, is the test")
    print(f"  selected wallets with positive holdout CLV: {wf['persisted']}/{wf['top_n']}")


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


def run(
    sport: str,
    max_markets: int,
    out_path: Optional[str],
    min_volume: float = MIN_MARKET_VOLUME,
    max_volume: float = MAX_MARKET_VOLUME,
) -> Dict[str, Any]:
    all_markets, rejects = discover_markets(sport)
    print(f"{sport}: head-to-head markets with gameStartTime: {len(all_markets)}", file=sys.stderr)
    for reason, count in sorted(rejects.items(), key=lambda kv: -kv[1]):
        print(f"  discovery reject {reason}: {count}", file=sys.stderr)
    if not all_markets:
        print("no markets discovered -- nothing to do", file=sys.stderr)
        return {}

    discovered = len(all_markets)
    banded = [m for m in all_markets if min_volume <= m["volume"] <= max_volume]
    print(f"  volume band ${min_volume:,.0f}-${max_volume:,.0f}: "
          f"{len(banded)} of {discovered} markets "
          f"({discovered - len(banded)} outside the band)", file=sys.stderr)
    if banded:
        all_markets = banded
    else:
        print("  band empty -- falling back to the full pool", file=sys.stderr)

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
    wf = walk_forward(wallets, rows, dev_ids, holdout_ids)

    print()
    print(f"markets processed {counts['markets']}, fills {counts['fills']:,}")
    print(f"  CLV-eligible markets      {counts['clv_eligible']}")
    print(f"  no pregame trades reached {counts['no_pregame_trades']} "
          f"(of which {counts['truncated_and_lost_pregame']} lost to tape truncation)")
    print(f"  tapes hitting the {MAX_TRADES_PER_MARKET:,}-trade ceiling: {counts['truncated']}")
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
    print_walk_forward(wf)

    report = {
        "sport": sport,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "markets_selected": len(markets),
        "markets_discovered": discovered,
        "volume_band": [min_volume, max_volume],
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
        "walk_forward": wf,
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
    parser.add_argument("--min-volume", type=float, default=MIN_MARKET_VOLUME)
    parser.add_argument("--max-volume", type=float, default=MAX_MARKET_VOLUME)
    parser.add_argument("--out")
    args = parser.parse_args()
    run(args.sport, args.max_markets, args.out, args.min_volume, args.max_volume)


if __name__ == "__main__":
    main()
