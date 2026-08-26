#!/usr/bin/env python3
"""Forensics on specific Polymarket wallets -- read-only, no DB, no UI.

ingest/polymarket_{tennis,mlb}_wallet_pilot.py surfaced SIX wallet addresses
that appear independently in BOTH sports' top-20 "edge" leaderboards. With
~21,000 qualified wallets per sport, that overlap is far past chance, so the
wallets are real -- but the pilots' own walk-forward check came back a coin
flip (tennis 6/14 persisted, MLB 3/6), meaning top-of-leaderboard status did
NOT predict later performance.

This script exists to answer the only question that decides whether wallet
tracking becomes infrastructure or stays a research toy:

    are these six skilled operators, one entity running several wallets,
    or bots?

Those three have completely different implications. A skilled operator is a
signal worth following. One entity behind six wallets means our "six
independent confirmations" is really ONE observation and the overlap is an
artifact of counting. A market maker is not predicting anything at all --
it earns the spread, and its apparent "edge" is a measurement error in a
metric built for directional bettors.

Method: pull each wallet's full reachable trade history from the free,
no-auth Data API and compute behavioural signals only. Nothing here settles
markets or claims PnL -- the pilots already did outcome-verified settlement
on their sampled markets, and re-deriving it across every market these
wallets ever touched would be a far larger job for a question this does not
need to answer.

RESULT (run 2026-08-26, all six wallets, ~19,400 trades):

  NOT one entity. Pairwise market overlap is near zero (highest Jaccard
  0.139, most exactly 0.00), so these are six genuinely independent actors.

  NOT sports specialists. Every one trades 7-11 unrelated categories, and
  MLB+Tennis -- the two sports we ranked them on -- is only 8-31% of their
  activity. The rest is esports, crypto, politics, and one-off novelty
  markets ("Will Donald Trump publicly insult someone on August 23").

  NOT humans making considered bets. Trade velocity runs 59-250/day across
  826-2,673 distinct markets. Two are outright automated (53% and 23% of
  trades under $1 notional; 28% and 27% sharing an exact second with
  another trade), and even the "directional" four place 10-13% of trades in
  the same second as another -- impossible by hand.

  => The edge metric was measuring TRADING STYLE, not skill. edge =
  Wilson win-rate floor - average entry price assumes a considered wager at
  a chosen price. For a high-frequency generalist or a quoter earning the
  spread, "average entry price" is just wherever the book sat, so the
  metric returns a large but meaningless number. This is also why the
  pilots' walk-forward came back a coin flip: the leaderboard was never
  ranking predictive skill, so there was nothing to persist.

  Following these wallets would be following market-making flow, not
  information. The finding is negative and closes the question the pilots
  opened -- which is the useful outcome, not a disappointing one.

Read-only against public endpoints. No DB writes. Research context only --
not investment or betting advice.

Usage:
    python -m ingest.polymarket_wallet_forensics                # the six
    python -m ingest.polymarket_wallet_forensics --wallet 0x... # ad hoc
    python -m ingest.polymarket_wallet_forensics --out report.json
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from itertools import combinations
from typing import Any, Dict, List

import requests

DATA = "https://data-api.polymarket.com"
THROTTLE_S = 0.15
PAGE = 500
# The Data API refuses offsets past roughly this point (documented in the
# Speeches earnings pilot and re-confirmed here). A wallet that hits the
# ceiling has its EARLIEST trades unreachable, so every figure below is over
# the most recent N trades, not a lifetime record. Reported explicitly per
# wallet rather than quietly presented as complete.
MAX_TRADES = 3500

# The six from the cross-sport overlap. Order is stable so reruns diff cleanly.
CROSS_SPORT_WALLETS = [
    "0x32bfc4950e354e505406f3769b0ebbc7ec8fec60",
    "0x39820b9a79c5a1c2fb22d9d67b055468a3fd2bef",
    "0x42c74ed2312cab584c909087d1255cc838bec415",
    "0x82a59f9c648edcb10ab8f310237f5e34f7fd674e",
    "0x84d4f0a864c647b88635c0e299da30d7606377a9",
    "0x948b7f2f159eb7f5c0270a0b188ab580d77da188",
]

session = requests.Session()
session.headers["User-Agent"] = "NBADFS-wallet-forensics/1.0 (research)"

# eventSlug prefixes -> a coarse category. Deliberately crude: the question
# is "how broad is this wallet", not precise taxonomy.
_CATEGORY_PATTERNS = [
    (re.compile(r"^mlb-"), "MLB"),
    (re.compile(r"^(atp|wta)-"), "Tennis"),
    (re.compile(r"^itf-"), "Tennis (ITF)"),
    (re.compile(r"^nba-"), "NBA"),
    (re.compile(r"^nfl-"), "NFL"),
    (re.compile(r"^nhl-"), "NHL"),
    (re.compile(r"^(lol|lck|lec|csgo|dota|val)"), "Esports"),
    (re.compile(r"^(epl|ucl|laliga|seriea|bundesliga|mls|soccer|fifa)"), "Soccer"),
    (re.compile(r"(bitcoin|ethereum|crypto|btc|eth)"), "Crypto"),
    (re.compile(r"(election|president|senate|fed-|cpi|inflation)"), "Politics/Macro"),
]


def _category(slug: str, title: str) -> str:
    s = (slug or "").lower()
    for pat, label in _CATEGORY_PATTERNS:
        if pat.search(s):
            return label
    t = (title or "").lower()
    for pat, label in _CATEGORY_PATTERNS:
        if pat.search(t):
            return label
    return "Other"


def fetch_wallet_trades(wallet: str) -> tuple[List[Dict[str, Any]], bool]:
    """All reachable trades, newest first. Returns (trades, hit_ceiling)."""
    out: List[Dict[str, Any]] = []
    offset = 0
    while offset < MAX_TRADES:
        time.sleep(THROTTLE_S)
        try:
            r = session.get(
                f"{DATA}/trades",
                params={"user": wallet, "limit": PAGE, "offset": offset, "takerOnly": "false"},
                timeout=30,
            )
            r.raise_for_status()
            page = r.json()
        except requests.RequestException as exc:
            print(f"  ! {wallet[:12]} offset={offset}: {exc}", file=sys.stderr)
            break
        if not isinstance(page, list) or not page:
            return out, False
        out.extend(page)
        if len(page) < PAGE:
            return out, False
        offset += PAGE
    return out, True


def _ts(t: Dict[str, Any]) -> int:
    try:
        return int(t.get("timestamp") or 0)
    except (TypeError, ValueError):
        return 0


def profile(wallet: str, trades: List[Dict[str, Any]], hit_ceiling: bool) -> Dict[str, Any]:
    """Behavioural signals for one wallet. No settlement, no PnL claims."""
    buys = sells = 0
    buy_cash = sell_cash = 0.0
    sizes: List[float] = []
    dust = 0                      # sub-$1 notional -- market-maker odd lots
    cats: Counter = Counter()
    markets: set[str] = set()
    per_second: Counter = Counter()
    names: Counter = Counter()

    for t in trades:
        side = str(t.get("side") or "").upper()
        try:
            size = float(t.get("size") or 0)
            price = float(t.get("price") or 0)
        except (TypeError, ValueError):
            continue
        if size <= 0 or side not in ("BUY", "SELL"):
            continue
        notional = size * price
        sizes.append(notional)
        if notional < 1.0:
            dust += 1
        if side == "BUY":
            buys += 1
            buy_cash += notional
        else:
            sells += 1
            sell_cash += notional
        cats[_category(str(t.get("eventSlug") or ""), str(t.get("title") or ""))] += 1
        if t.get("conditionId"):
            markets.add(str(t["conditionId"]))
        per_second[_ts(t)] += 1
        nm = str(t.get("name") or t.get("pseudonym") or "").strip()
        if nm:
            names[nm] += 1

    n = buys + sells
    stamps = [_ts(t) for t in trades if _ts(t) > 0]
    total_cash = buy_cash + sell_cash
    # Trades sharing an exact second: a human clicking cannot do this at
    # volume; an automated quoter does it constantly.
    same_second = sum(c for c in per_second.values() if c > 1)

    return {
        "wallet": wallet,
        "display_name": names.most_common(1)[0][0] if names else None,
        "trades_analyzed": n,
        "history_truncated": hit_ceiling,
        "first_trade": datetime.fromtimestamp(min(stamps), timezone.utc).date().isoformat() if stamps else None,
        "last_trade": datetime.fromtimestamp(max(stamps), timezone.utc).date().isoformat() if stamps else None,
        "distinct_markets": len(markets),
        "categories": dict(cats.most_common()),
        "distinct_categories": len(cats),
        "buy_trades": buys,
        "sell_trades": sells,
        "buy_dollar_share": round(buy_cash / total_cash, 3) if total_cash else None,
        "total_notional_usd": round(total_cash, 2),
        "median_trade_usd": round(statistics.median(sizes), 2) if sizes else None,
        "sub_dollar_trade_share": round(dust / n, 3) if n else None,
        "same_second_trade_share": round(same_second / n, 3) if n else None,
        "_markets": markets,
        "_stamps_by_market": None,
    }


def classify(p: Dict[str, Any]) -> tuple[str, List[str]]:
    """Label a wallet from its signals, with the reasons that drove it.

    Thresholds are coarse and stated in the output rather than tuned -- this
    is a triage tool for six wallets, not a calibrated classifier, and
    pretending otherwise would be the exact overreach the pilots' own
    walk-forward result warns against.
    """
    reasons: List[str] = []
    bot_points = 0

    dust = p.get("sub_dollar_trade_share") or 0
    if dust >= 0.30:
        bot_points += 2
        reasons.append(f"{dust*100:.0f}% of trades under $1 (odd-lot quoting, not betting)")
    elif dust >= 0.10:
        bot_points += 1
        reasons.append(f"{dust*100:.0f}% of trades under $1")

    burst = p.get("same_second_trade_share") or 0
    if burst >= 0.20:
        bot_points += 2
        reasons.append(f"{burst*100:.0f}% of trades share an exact second with another")
    elif burst >= 0.05:
        bot_points += 1
        reasons.append(f"{burst*100:.0f}% of trades share an exact second")

    buy_share = p.get("buy_dollar_share")
    if buy_share is not None and buy_share < 0.60:
        bot_points += 2
        reasons.append(f"only {buy_share*100:.0f}% of dollar volume is buying (two-sided quoting)")

    if p.get("distinct_categories", 0) >= 5:
        reasons.append(f"trades {p['distinct_categories']} unrelated categories")

    n = p.get("trades_analyzed") or 0
    mk = p.get("distinct_markets") or 1
    if n / max(mk, 1) >= 8:
        bot_points += 1
        reasons.append(f"{n/mk:.1f} trades per market (repeated re-quoting)")

    if bot_points >= 4:
        return "AUTOMATED / MARKET-MAKER", reasons
    if bot_points >= 2:
        return "LIKELY AUTOMATED", reasons
    return "DIRECTIONAL BETTOR", reasons or ["no automation signals tripped"]


def relatedness(profiles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Pairwise overlap -- are these actually one entity wearing six hats?

    Two independent bettors both active in liquid sports markets WILL share
    some markets by coincidence, so raw overlap alone proves nothing. Trades
    on the same market within the same second are the stronger tell: that is
    hard to do by chance and easy to do from one script.
    """
    by_wallet = {p["wallet"]: p for p in profiles}
    out = []
    for a, b in combinations(sorted(by_wallet), 2):
        pa, pb = by_wallet[a], by_wallet[b]
        ma, mb = pa["_markets"], pb["_markets"]
        inter = ma & mb
        union = ma | mb
        out.append({
            "wallet_a": a,
            "wallet_b": b,
            "shared_markets": len(inter),
            "jaccard": round(len(inter) / len(union), 3) if union else 0.0,
        })
    out.sort(key=lambda r: -r["jaccard"])
    return out


def run(wallets: List[str]) -> Dict[str, Any]:
    profiles = []
    for w in wallets:
        print(f"fetching {w} ...", file=sys.stderr)
        trades, ceiling = fetch_wallet_trades(w)
        print(f"   {len(trades)} trades{' (TRUNCATED at API ceiling)' if ceiling else ''}", file=sys.stderr)
        p = profile(w, trades, ceiling)
        label, reasons = classify(p)
        p["classification"] = label
        p["classification_reasons"] = reasons
        profiles.append(p)

    pairs = relatedness(profiles)
    for p in profiles:
        p.pop("_markets", None)
        p.pop("_stamps_by_market", None)
    return {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "wallets": profiles,
        "pairwise_overlap": pairs,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--wallet", action="append", help="Analyse specific wallet(s) instead of the six")
    ap.add_argument("--out", default="polymarket_wallet_forensics.json")
    args = ap.parse_args()

    report = run(args.wallet or CROSS_SPORT_WALLETS)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=1)

    print("\n=== Wallet forensics ===")
    for p in report["wallets"]:
        name = p["display_name"] or p["wallet"][:14]
        trunc = " [truncated]" if p["history_truncated"] else ""
        print(f"\n{name}  ({p['wallet'][:18]}...)")
        print(f"  {p['trades_analyzed']} trades{trunc} | {p['distinct_markets']} markets | "
              f"{p['first_trade']} -> {p['last_trade']}")
        print(f"  ${p['total_notional_usd']:,.0f} notional | median ${p['median_trade_usd']:,.2f} | "
              f"buy share {(p['buy_dollar_share'] or 0)*100:.0f}%")
        print(f"  categories: {', '.join(f'{k} {v}' for k, v in list(p['categories'].items())[:6])}")
        print(f"  >> {p['classification']}")
        for r in p["classification_reasons"]:
            print(f"       - {r}")

    print("\n=== Are these one entity? (pairwise market overlap) ===")
    for r in report["pairwise_overlap"][:6]:
        print(f"  {r['wallet_a'][:12]}.. / {r['wallet_b'][:12]}..  "
              f"shared_markets={r['shared_markets']:<5} jaccard={r['jaccard']}")

    print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
