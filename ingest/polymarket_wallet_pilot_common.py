"""Shared Polymarket wallet-intelligence pilot engine -- read-only, no DB, no UI.

Sport-agnostic core reused by ingest/polymarket_tennis_wallet_pilot.py and
ingest/polymarket_mlb_wallet_pilot.py:

  1. Outcome-verified per-wallet P&L reconstruction, ported from themvf/
     Speeches' polymarket_pilot.py (the SEC-25 earnings-market pilot). Per
     wallet per market: net position + cash flow per outcome from fills
     (BUY: +size, -size*price cash; SELL: -size, +size*price cash); at
     resolution the winning outcome's tokens redeem at $1, losers at $0, so
     pnl = cash + max(net_winner, 0). Negative net positions (possible via
     on-chain split/merge, invisible in the fill tape) clamp to 0 payout --
     a small, documented approximation carried over unchanged.
  2. A chronological walk-forward split: does a wallet that looked skilled
     in an earlier "development" window still look skilled in a later
     "holdout" window it never trained on? Same discipline this whole repo
     applies everywhere else (never trust a same-sample result).
  3. Archetype bands derived empirically from each sport's OWN qualified-
     wallet distribution (percentiles of entry price / win rate), instead
     of reusing the earnings pilot's fixed numeric bands verbatim. Still a
     first-pass, descriptive calibration -- not a validated predictive
     rule until it's tested on its own held-out sample.
  4. Generic Gamma /events pagination, including a live-verified quirk:
     both the tennis (tag=864) and MLB (tag=100381) tags return a
     malformed {"type":"error",...} object (not a list) once the offset
     passes ~2100 -- treated as "end of data", not a crash.

Every sport-specific module supplies only discovery: a function returning
resolved markets shaped {"condition_id","question","winner","volume",
"end_date", ...} and one returning currently-open markets shaped
{condition_id: {...}}. Everything downstream -- fills, settlement, ranking,
walk-forward, bands -- lives here so it's written and reviewed once.

Read-only against public, no-auth Polymarket endpoints (gamma-api.
polymarket.com, data-api.polymarket.com). No DB, no UI. Research context
only -- not investment or betting advice.
"""

from __future__ import annotations

import sys
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import requests

GAMMA = "https://gamma-api.polymarket.com"
DATA = "https://data-api.polymarket.com"
THROTTLE_S = 0.15
TRADES_PAGE = 500
MAX_TRADES_PER_MARKET = 6000

session = requests.Session()
session.headers["User-Agent"] = "NBADFS-polymarket-wallet-pilot/1.0 (research)"


def api_get(url: str, params: Dict[str, Any]) -> Any:
    time.sleep(THROTTLE_S)
    resp = session.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def paginate_events(
    tag_id: int, closed: str, order: str = "volume", ascending: str = "false", max_pages: int = 20
) -> List[Dict[str, Any]]:
    """Generic /events pager. Stops cleanly (not a crash) on empty results
    OR the malformed non-list response Gamma returns past its own
    ~2100-offset ceiling -- verified live 2026-08-19 for both the tennis and
    MLB tags, so treated as a platform quirk, not sport-specific."""
    events: List[Dict[str, Any]] = []
    for page in range(max_pages):
        data = api_get(f"{GAMMA}/events", {
            "tag_id": tag_id, "closed": closed, "order": order,
            "ascending": ascending, "limit": 100, "offset": page * 100,
        })
        if not isinstance(data, list) or not data:
            break
        events.extend(data)
    return events


def fetch_market_fills(condition_id: str) -> List[Dict[str, Any]]:
    """Newest-first fill tape; the Data API 400s past offset ~3500 -- keep
    the partial tape rather than discarding the market (documented caveat
    carried over from the source earnings pilot: on the highest-volume
    markets the earliest fills are unreachable via this endpoint, which
    biases "early entry price" toward more-recent trades)."""
    fills: List[Dict[str, Any]] = []
    offset = 0
    while offset < MAX_TRADES_PER_MARKET:
        try:
            page = api_get(f"{DATA}/trades", {
                "market": condition_id, "limit": TRADES_PAGE, "offset": offset,
                "takerOnly": "false",
            })
        except requests.HTTPError:
            break  # offset ceiling reached -- return what we have
        if not isinstance(page, list) or not page:
            break
        fills.extend(page)
        if len(page) < TRADES_PAGE:
            break
        offset += TRADES_PAGE
    return fills


def settle_market(fills: List[Dict[str, Any]], winner: str) -> Dict[str, Dict[str, Any]]:
    """Per-wallet P&L for one resolved market. Outcome-name agnostic --
    works identically for tennis player names, MLB team names, or an
    earnings ticker's Yes/No -- no sport-specific change needed.

    Tracks TWO separate entry-price averages, and the distinction matters:
    `entry_avg` (all buys, any outcome) is the wallet's real average price
    paid in this market -- the correct break-even bar for judging whether
    a high win rate reflects skill or just paying full price for
    near-certainty. `win_entry_avg` (winning buys only) is kept for
    backward-compat display but is a biased, optimistic proxy -- it's
    silent on what a wallet paid for the bets it LOST, so a wallet that
    lost occasionally at a different price than it won at would look
    better on win_entry_avg than it should. Found live 2026-08-19: sorting
    purely by win-rate confidence surfaced wallets with ~99-100% win rates
    and near-zero-to-NEGATIVE PnL on hundreds of thousands to millions of
    dollars in cost (e.g. one wallet: 101 markets, 100% win, $7.5M cost,
    $8,103 profit) -- entry_avg for those wallets sits at 0.99+, i.e. they
    were simply buying near-certain favorites at near-certain prices. A
    high win rate is not skill unless it exceeds what the price paid
    already implied."""
    positions: Dict[str, Dict[str, float]] = defaultdict(
        lambda: {
            "cash": 0.0, "cost": 0.0, "net_win": 0.0,
            "buy_size": 0.0, "buy_cash": 0.0,
            "win_buy_size": 0.0, "win_buy_cash": 0.0,
        }
    )
    names: Dict[str, str] = {}
    for fill in fills:
        wallet = str(fill.get("proxyWallet") or "")
        outcome = str(fill.get("outcome") or "")
        side = str(fill.get("side") or "").upper()
        try:
            size = float(fill.get("size") or 0)
            price = float(fill.get("price") or 0)
        except (TypeError, ValueError):
            continue
        if not wallet or size <= 0 or side not in ("BUY", "SELL"):
            continue
        pos = positions[wallet]
        name = str(fill.get("name") or fill.get("pseudonym") or "")
        if name:
            names[wallet] = name
        signed = size if side == "BUY" else -size
        cash = -size * price if side == "BUY" else size * price
        pos["cash"] += cash
        if side == "BUY":
            pos["cost"] += size * price
            pos["buy_size"] += size
            pos["buy_cash"] += size * price
        if outcome == winner:
            pos["net_win"] += signed
            if side == "BUY":
                pos["win_buy_size"] += size
                pos["win_buy_cash"] += size * price
    out: Dict[str, Dict[str, Any]] = {}
    for wallet, pos in positions.items():
        payout = max(pos["net_win"], 0.0)
        out[wallet] = {
            "pnl": pos["cash"] + payout,
            "cost": pos["cost"],
            "entry_avg": (pos["buy_cash"] / pos["buy_size"]) if pos["buy_size"] > 0 else None,
            "win_entry_avg": (pos["win_buy_cash"] / pos["win_buy_size"]) if pos["win_buy_size"] > 0 else None,
            "name": names.get(wallet, ""),
        }
    return out


def _new_agg() -> Dict[str, Any]:
    return {"markets": 0, "wins": 0, "pnl": 0.0, "cost": 0.0, "entries": [], "win_entries": [], "name": ""}


def analyze_resolved_markets(
    markets: List[Dict[str, Any]], label_market: Callable[[Dict[str, Any]], str]
) -> Tuple[Dict[str, Any], int]:
    """Single-sample-set version (no walk-forward split)."""
    wallet_stats: Dict[str, Any] = defaultdict(_new_agg)
    total_fills = 0
    for i, market in enumerate(markets, 1):
        try:
            fills = fetch_market_fills(market["condition_id"])
        except Exception as exc:
            print(f"  ! {label_market(market)}: {exc}", file=sys.stderr)
            continue
        total_fills += len(fills)
        settled = settle_market(fills, market["winner"])
        for wallet, stats in settled.items():
            agg = wallet_stats[wallet]
            agg["markets"] += 1
            agg["pnl"] += stats["pnl"]
            agg["cost"] += stats["cost"]
            if stats["pnl"] > 0:
                agg["wins"] += 1
            if stats["entry_avg"] is not None:
                agg["entries"].append(stats["entry_avg"])
            if stats["win_entry_avg"] is not None:
                agg["win_entries"].append(stats["win_entry_avg"])
            if stats["name"]:
                agg["name"] = stats["name"]
        if i % 25 == 0:
            print(f"  processed {i}/{len(markets)} markets ({total_fills} fills)", file=sys.stderr)
    return wallet_stats, total_fills


def _split_dated_markets_by_median(
    markets: List[Dict[str, Any]], date_key: str, min_dated: int
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Optional[Tuple[str, str]], Optional[Tuple[str, str]], int]:
    """Chronological median split of a market pool into (dev_pool,
    holdout_pool) market-dict lists (not ids -- callers decide what to keep
    from each half). Markets with no usable date are EXCLUDED from both
    halves and counted, never guessed into one."""
    dated: List[Tuple[datetime, Dict[str, Any]]] = []
    excluded = 0
    for m in markets:
        raw = m.get(date_key)
        if not raw:
            excluded += 1
            continue
        try:
            dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except ValueError:
            excluded += 1
            continue
        dated.append((dt, m))
    if len(dated) < min_dated:
        return [], [], None, None, excluded
    dated.sort(key=lambda x: x[0])
    mid = len(dated) // 2
    dev = dated[:mid]
    holdout = dated[mid:]
    dev_range = (dev[0][0].date().isoformat(), dev[-1][0].date().isoformat())
    holdout_range = (holdout[0][0].date().isoformat(), holdout[-1][0].date().isoformat())
    return [m for _, m in dev], [m for _, m in holdout], dev_range, holdout_range, excluded


def select_balanced_dev_holdout(
    all_markets: List[Dict[str, Any]], max_markets: int, date_key: str = "end_date", min_dated: int = 20
) -> Tuple[List[Dict[str, Any]], Set[str], Set[str], Optional[Tuple[str, str]], Optional[Tuple[str, str]], int]:
    """Split the FULL discovered market pool chronologically FIRST, then
    take the top-volume markets independently within each half.

    This replaces the earlier (and wrong) approach of picking one global
    top-N-by-volume set and only THEN splitting it by date. For a daily
    sport like MLB that produced a real bug, not just a suboptimal design:
    the 2026-08-19 pilot run picked its 400 markets by volume across the
    WHOLE history first, and because MLB volume isn't spread evenly across
    a season, the chronological split of that pre-filtered set left the two
    halves with almost no shared wallet population -- only 3 of the top 30
    dev-period wallets by PnL had traded in ANY of the 200 holdout-half
    markets at all. Splitting the full pool by date before selecting by
    volume guarantees both halves independently prioritize their own best
    markets, so wallet overlap reflects real repeat behavior instead of an
    artifact of which markets happened to survive one global volume cut.

    Returns (selected_markets, dev_ids, holdout_ids, dev_range,
    holdout_range, excluded_undated_count). selected_markets is
    dev_selected + holdout_selected, sized up to max_markets total
    (max_markets // 2 from each half)."""
    dev_pool, holdout_pool, dev_range, holdout_range, excluded = _split_dated_markets_by_median(
        all_markets, date_key, min_dated
    )
    if not dev_pool or not holdout_pool:
        return [], set(), set(), None, None, excluded
    half = max_markets // 2
    dev_selected = sorted(dev_pool, key=lambda m: -m["volume"])[:half]
    holdout_selected = sorted(holdout_pool, key=lambda m: -m["volume"])[:half]
    dev_ids = {m["condition_id"] for m in dev_selected}
    holdout_ids = {m["condition_id"] for m in holdout_selected}
    return dev_selected + holdout_selected, dev_ids, holdout_ids, dev_range, holdout_range, excluded


def analyze_and_partition(
    markets: List[Dict[str, Any]],
    dev_ids: Set[str],
    holdout_ids: Set[str],
    label_market: Callable[[Dict[str, Any]], str],
) -> Tuple[Dict[str, Any], Dict[str, Any], int]:
    """One fetch pass: each market's fills are pulled exactly once and its
    settled per-wallet result routed into dev_stats or holdout_stats
    depending on which half its market landed in (markets outside both
    halves -- the ones partition_by_date excluded -- are skipped)."""
    dev_stats: Dict[str, Any] = defaultdict(_new_agg)
    holdout_stats: Dict[str, Any] = defaultdict(_new_agg)
    total_fills = 0
    for i, market in enumerate(markets, 1):
        cid = market["condition_id"]
        if cid in dev_ids:
            target = dev_stats
        elif cid in holdout_ids:
            target = holdout_stats
        else:
            continue
        try:
            fills = fetch_market_fills(cid)
        except Exception as exc:
            print(f"  ! {label_market(market)}: {exc}", file=sys.stderr)
            continue
        total_fills += len(fills)
        settled = settle_market(fills, market["winner"])
        for wallet, stats in settled.items():
            agg = target[wallet]
            agg["markets"] += 1
            agg["pnl"] += stats["pnl"]
            agg["cost"] += stats["cost"]
            if stats["pnl"] > 0:
                agg["wins"] += 1
            if stats["entry_avg"] is not None:
                agg["entries"].append(stats["entry_avg"])
            if stats["win_entry_avg"] is not None:
                agg["win_entries"].append(stats["win_entry_avg"])
            if stats["name"]:
                agg["name"] = stats["name"]
        if i % 25 == 0:
            print(f"  processed {i}/{len(markets)} markets ({total_fills} fills)", file=sys.stderr)
    return dev_stats, holdout_stats, total_fills


def rank_wallets(wallet_stats: Dict[str, Any], min_markets: int) -> List[Dict[str, Any]]:
    """Qualified (>=min_markets) wallets as ranked rows, PnL desc. No
    archetype attached here -- bands are sport-specific and derived
    separately (see derive_archetype_bands), then applied with
    classify_with_bands."""
    qualified = []
    for wallet, agg in wallet_stats.items():
        if agg["markets"] < min_markets:
            continue
        qualified.append({
            "wallet": wallet,
            "name": agg["name"],
            "markets": agg["markets"],
            "wins": agg["wins"],
            "win_rate": round(agg["wins"] / agg["markets"], 3),
            "pnl_usd": round(agg["pnl"], 2),
            "cost_usd": round(agg["cost"], 2),
            "roi": round(agg["pnl"] / agg["cost"], 3) if agg["cost"] > 0 else None,
            "avg_entry_price": (
                round(sum(agg["entries"]) / len(agg["entries"]), 4)
                if agg["entries"] else None
            ),
            "avg_winner_entry_price": (
                round(sum(agg["win_entries"]) / len(agg["win_entries"]), 3)
                if agg["win_entries"] else None
            ),
        })
    qualified.sort(key=lambda w: -w["pnl_usd"])
    return qualified


# Sample-size guard: one lucky market must not mint a "sharp" wallet. Same
# constant and rationale as the earnings pilot's ARCH_MIN_MARKETS.
ARCH_MIN_MARKETS = 8


def derive_archetype_bands(qualified: List[Dict[str, Any]]) -> Dict[str, float]:
    """Terciles of avg_entry_price and win_rate among wallets that clear
    ARCH_MIN_MARKETS, computed from THIS sport's own qualified-wallet
    sample. Uses avg_entry_price (all buys), NOT avg_winner_entry_price
    (winning buys only, a biased proxy -- see settle_market's docstring for
    why) -- switched 2026-08-19 alongside rank_wallets_by_edge for the same
    reason. Empirical, first-pass, descriptive -- NOT validated as
    predictive until tested on its own held-out sample (that's what
    compare_dev_holdout's by_archetype breakdown is a first look at).
    Returns {} if there aren't enough eligible wallets to derive bands
    honestly (min 9, so each tercile has >=3)."""
    eligible = [r for r in qualified if r["markets"] >= ARCH_MIN_MARKETS and r.get("avg_entry_price") is not None]
    if len(eligible) < 9:
        return {}
    entries = sorted(r["avg_entry_price"] for r in eligible)
    win_rates = sorted(r["win_rate"] for r in eligible)

    def pct(seq: List[float], p: float) -> float:
        idx = min(len(seq) - 1, max(0, round(p * (len(seq) - 1))))
        return seq[idx]

    return {
        "entry_p33": round(pct(entries, 0.33), 3),
        "entry_p67": round(pct(entries, 0.67), 3),
        "win_p33": round(pct(win_rates, 0.33), 3),
        "win_p67": round(pct(win_rates, 0.67), 3),
        "n_eligible": len(eligible),
    }


def classify_with_bands(row: Dict[str, Any], bands: Dict[str, float]) -> str:
    """early_sharp / late_closer / longshot / unclassified, using bands
    derived from THIS sport's own distribution (see derive_archetype_bands)
    rather than the earnings pilot's fixed numbers."""
    if not bands or row["markets"] < ARCH_MIN_MARKETS:
        return "unclassified"
    entry = row.get("avg_entry_price")
    if entry is None:
        return "unclassified"
    win = row["win_rate"]
    roi = row.get("roi")
    if entry <= bands["entry_p33"] and win >= bands["win_p67"] and row["pnl_usd"] > 0:
        return "early_sharp"
    if entry >= bands["entry_p67"] and win >= bands["win_p67"]:
        return "late_closer"
    if entry <= bands["entry_p33"] and win <= bands["win_p33"] and roi is not None and roi > 1:
        return "longshot"
    return "unclassified"


def compare_dev_holdout(
    dev_qualified: List[Dict[str, Any]], holdout_stats: Dict[str, Any], top_n: int = 30
) -> Dict[str, Any]:
    """For the top-N dev-period wallets by PnL (each already tagged with an
    'archetype' key by the caller), check whether their edge persisted into
    the holdout period. Also buckets by dev-period archetype label against
    mean holdout PnL/win-rate -- a first, purely descriptive look at
    whether the label carries any out-of-sample information. Small samples
    are expected and reported honestly, not hidden."""
    rows = []
    for row in dev_qualified[:top_n]:
        wallet = row["wallet"]
        h = holdout_stats.get(wallet)
        entry = {
            "wallet": wallet, "name": row["name"], "archetype": row.get("archetype", "unclassified"),
            "dev_markets": row["markets"], "dev_win_rate": row["win_rate"], "dev_pnl": row["pnl_usd"],
        }
        if h is None or h["markets"] == 0:
            entry["holdout_status"] = "no_holdout_activity"
        else:
            holdout_win_rate = round(h["wins"] / h["markets"], 3)
            holdout_pnl = round(h["pnl"], 2)
            persisted = holdout_pnl > 0 and holdout_win_rate >= 0.5
            entry.update({
                "holdout_status": "persisted" if persisted else "reversed",
                "holdout_markets": h["markets"],
                "holdout_win_rate": holdout_win_rate,
                "holdout_pnl": holdout_pnl,
            })
        rows.append(entry)

    by_archetype: Dict[str, Dict[str, Any]] = {}
    for entry in rows:
        if entry["holdout_status"] == "no_holdout_activity":
            continue
        bucket = by_archetype.setdefault(
            entry["archetype"], {"n": 0, "pnl_sum": 0.0, "win_rate_sum": 0.0, "persisted": 0}
        )
        bucket["n"] += 1
        bucket["pnl_sum"] += entry["holdout_pnl"]
        bucket["win_rate_sum"] += entry["holdout_win_rate"]
        bucket["persisted"] += 1 if entry["holdout_status"] == "persisted" else 0
    archetype_summary = {
        arch: {
            "n_with_holdout_activity": b["n"],
            "avg_holdout_pnl": round(b["pnl_sum"] / b["n"], 2),
            "avg_holdout_win_rate": round(b["win_rate_sum"] / b["n"], 3),
            "persisted_count": b["persisted"],
        }
        for arch, b in by_archetype.items()
    }
    return {"wallets": rows, "by_archetype": archetype_summary}


def rank_wallets_by_roi(qualified: List[Dict[str, Any]], min_cost: float = 1000.0) -> List[Dict[str, Any]]:
    """Same qualified rows, re-sorted by ROI instead of raw PnL.

    Raw PnL rewards bet SIZE, not skill -- a wallet that put $2M on one
    -105 favorite and won looks identical to a genuinely sharp wallet on
    the PnL leaderboard. ROI is closer to a size-independent skill proxy,
    but only once a minimum total cost floor keeps a $10 lucky trade from
    producing a meaningless 2000% ROI headline (already observed in v1/v2
    output, e.g. a single tiny fill posting roi=2697.03)."""
    eligible = [r for r in qualified if r.get("roi") is not None and r["cost_usd"] >= min_cost]
    return sorted(eligible, key=lambda r: -r["roi"])


def print_roi_leaderboard(title: str, rows: List[Dict[str, Any]], limit: int = 20) -> None:
    print(f"\n=== {title} ===")
    print(f"{'wallet/name':<28} {'mkts':>4} {'win%':>5} {'cost $':>10} {'pnl $':>10} {'roi':>7} {'archetype':<12}")
    for row in rows[:limit]:
        label = (row["name"] or row["wallet"][:10] + "...")[:27]
        print(
            f"{label:<28} {row['markets']:>4} {row['win_rate']*100:>4.0f}% "
            f"{row['cost_usd']:>10.2f} {row['pnl_usd']:>10.2f} {row['roi']:>7.2f} {row.get('archetype', '-'):<12}"
        )


def wilson_lower_bound(wins: int, n: int, z: float = 1.96) -> float:
    """95% Wilson score interval LOWER bound on a win rate.

    Neither raw PnL nor raw win% is a sound skill ranking on its own, and
    they fail in opposite directions: PnL rewards one huge bet regardless
    of accuracy ("bet size"), raw win% rewards a tiny sample that got lucky
    and stopped ("5-for-5 beats 200-for-280" -- observed live in this
    pilot's own output: with ~20k qualified wallets, dozens will hit a
    5-for-5 streak on pure chance at ARCH_MIN_MARKETS-floor sample sizes).
    Wilson's lower bound answers "what win rate can I be 95% confident this
    wallet is AT LEAST this good, given how many bets it's actually
    placed" -- it converges toward the raw win rate as n grows and shrinks
    toward 50% as n shrinks, so a 5-for-5 wallet (lower bound ~0.57 -- a
    coin flip is well within its plausible range) no longer outranks a
    200-for-280 wallet (lower bound ~0.66, genuinely, provably good). This
    is the same family of fix
    as the "confidence-discounted edge" idea already discussed elsewhere in
    this project (shrink thin estimates before trusting them), applied
    here to a ranking metric instead of a star-rating gate."""
    if n <= 0:
        return 0.0
    phat = wins / n
    denom = 1 + z * z / n
    center = phat + z * z / (2 * n)
    margin = z * ((phat * (1 - phat) + z * z / (4 * n)) / n) ** 0.5
    return max(0.0, (center - margin) / denom)


def rank_wallets_by_confidence(qualified: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """All qualified wallets, ranked by Wilson-lower-bound win rate instead
    of raw PnL or raw win%.

    IMPORTANT, found live 2026-08-19: this fixes the small-sample problem
    (a 5-for-5 wallet no longer outranks a 200-for-280 one) but is NOT a
    skill ranking on its own -- it says nothing about the PRICE paid for
    those wins. Sorting MLB wallets by this alone surfaced several with
    ~99-100% win rates and 40-287 markets whose PnL was near zero or
    NEGATIVE on huge cost (e.g. 101 markets, 100% win, $7.5M cost, $8,103
    profit) -- they were simply buying near-certain favorites at
    near-certain prices (avg entry ~0.99+). A high win rate earned by
    paying full price for it is not edge. Use rank_wallets_by_edge for the
    actual skill-shaped ranking; this function is kept because "who wins
    almost every bet" is still a legitimate, distinct question (e.g.
    identifying favorite-only bettors) -- it just isn't the same question
    as "who has an edge"."""
    ranked = []
    for row in qualified:
        wlb = wilson_lower_bound(row["wins"], row["markets"])
        ranked.append({**row, "wilson_lower_bound": round(wlb, 4)})
    ranked.sort(key=lambda r: -r["wilson_lower_bound"])
    return ranked


def print_confidence_leaderboard(title: str, rows: List[Dict[str, Any]], limit: int = 20) -> None:
    print(f"\n=== {title} ===")
    print(f"{'wallet/name':<28} {'mkts':>4} {'wins':>4} {'win%':>5} {'wilson_lb':>9} {'pnl $':>10} {'archetype':<12}")
    for row in rows[:limit]:
        label = (row["name"] or row["wallet"][:10] + "...")[:27]
        print(
            f"{label:<28} {row['markets']:>4} {row['wins']:>4} {row['win_rate']*100:>4.0f}% "
            f"{row['wilson_lower_bound']*100:>8.1f}% {row['pnl_usd']:>10.2f} {row.get('archetype', '-'):<12}"
        )


def rank_wallets_by_edge(qualified: List[Dict[str, Any]], min_cost: float = 1000.0) -> List[Dict[str, Any]]:
    """All qualified wallets with a usable entry price and at least
    min_cost total stake, ranked by edge_lower_bound =
    Wilson-lower-bound(win rate) - avg_entry_price.

    This is the actual skill-shaped ranking, and the reason both prior
    metrics needed it: PnL rewards bet SIZE, raw/confidence-adjusted win
    rate rewards betting only cheap favorites-into-certainty. Comparing a
    CONSERVATIVE win-rate estimate against the AVERAGE PRICE PAID (not just
    the price paid on winners -- see settle_market's entry_avg vs
    win_entry_avg distinction) answers the actual question: even being
    skeptical about this wallet's win rate, does it still beat what the
    market was charging it? Same edge = our_prob - reference_prob
    convention used throughout this project's bet-rating code
    (model/soccer_bet_rating.py etc.), applied here with the wallet's own
    historical entry price as the reference. A wallet buying at 0.99 and
    winning 99% of the time has edge ~0; a wallet buying at 0.55 and
    winning 70% of the time (even at its conservative Wilson floor of,
    say, 63%) has real edge (~8pp).

    min_cost matters here for the SAME reason it matters for ROI (see
    rank_wallets_by_roi), and this function initially shipped without it --
    a real bug, caught live 2026-08-19: the unfiltered version's #1 MLB
    wallet had 51 markets, a 96% win rate, and a $29.95 total cost. Deep-
    longshot lottery-ticket buyers (average entry price a few cents, so ANY
    win at all produces a large numeric edge) dominate the unfiltered
    ranking exactly the way a $10 lucky trade dominated the unfiltered ROI
    leaderboard before that fix."""
    eligible = [r for r in qualified if r.get("avg_entry_price") is not None and r["cost_usd"] >= min_cost]
    ranked = []
    for row in eligible:
        wlb = wilson_lower_bound(row["wins"], row["markets"])
        edge = wlb - row["avg_entry_price"]
        ranked.append({**row, "wilson_lower_bound": round(wlb, 4), "edge_lower_bound": round(edge, 4)})
    ranked.sort(key=lambda r: -r["edge_lower_bound"])
    return ranked


def print_edge_leaderboard(title: str, rows: List[Dict[str, Any]], limit: int = 20) -> None:
    print(f"\n=== {title} ===")
    print(f"{'wallet/name':<28} {'mkts':>4} {'win%':>5} {'avg entry':>9} {'wilson_lb':>9} {'edge_lb':>8} {'pnl $':>10} {'archetype':<12}")
    for row in rows[:limit]:
        label = (row["name"] or row["wallet"][:10] + "...")[:27]
        print(
            f"{label:<28} {row['markets']:>4} {row['win_rate']*100:>4.0f}% "
            f"{row['avg_entry_price']*100:>8.1f}% {row['wilson_lower_bound']*100:>8.1f}% "
            f"{row['edge_lower_bound']*100:>+7.1f}% {row['pnl_usd']:>10.2f} {row.get('archetype', '-'):<12}"
        )


def fetch_wallet_open_positions(wallet: str, open_markets: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Net stance per currently-open market from the wallet's recent fills."""
    try:
        fills = api_get(f"{DATA}/trades", {"user": wallet, "limit": 500, "takerOnly": "false"})
    except Exception:
        return []
    stance: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for fill in fills or []:
        condition_id = str(fill.get("conditionId") or "")
        if condition_id not in open_markets:
            continue
        outcome = str(fill.get("outcome") or "")
        side = str(fill.get("side") or "").upper()
        try:
            size = float(fill.get("size") or 0)
        except (TypeError, ValueError):
            continue
        stance[condition_id][outcome] += size if side == "BUY" else -size
    out = []
    for condition_id, outcomes in stance.items():
        held = {o: round(v, 2) for o, v in outcomes.items() if abs(v) > 0.5}
        if held:
            out.append({**open_markets[condition_id], "net_shares": held})
    return out


def print_leaderboard(title: str, rows: List[Dict[str, Any]], limit: int = 20) -> None:
    print(f"\n=== {title} ===")
    print(f"{'wallet/name':<28} {'mkts':>4} {'win%':>5} {'pnl $':>10} {'roi':>6} {'entry':>6} {'archetype':<12}")
    for row in rows[:limit]:
        label = (row["name"] or row["wallet"][:10] + "...")[:27]
        roi = f"{row['roi']:.2f}" if row.get("roi") is not None else "-"
        entry = f"{row['avg_winner_entry_price']:.2f}" if row.get("avg_winner_entry_price") is not None else "-"
        print(
            f"{label:<28} {row['markets']:>4} {row['win_rate']*100:>4.0f}% "
            f"{row['pnl_usd']:>10.2f} {roi:>6} {entry:>6} {row.get('archetype', '-'):<12}"
        )
