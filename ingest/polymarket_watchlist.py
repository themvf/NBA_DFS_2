#!/usr/bin/env python3
"""Frozen watchlist of Polymarket wallets -- the instrument for the forward test.

This is NOT a "follow these wallets" product, and the distinction is the whole
reason it is built this way.

docs/polymarket-wallet-tracker.md §11 records one unconfirmed positive: MLB
wallets selected on early-period closing-line value beat the close in a later
period by +0.0084 CLV, 95% CI [+0.0046, +0.0127], surviving concentration,
favourite-longshot and self-impact checks. Tennis, run identically, failed.
One sport out of two, from an exploratory scan, on a metric refined in the
same session that produced the result.

The honest next step for a result like that is a PRE-REGISTERED FORWARD TEST:
freeze the list now, then score it only on markets that resolve afterwards.
Freezing is what separates that from a rolling leaderboard that quietly
improves as losers are dropped -- the failure this project's specs exist to
prevent. So:

  * The cohort is written ONCE with a freeze timestamp and a cohort version.
    Re-running does not add, drop or re-rank wallets. Changing the membership
    means a NEW cohort version, leaving the old one intact and scoreable.
  * Forward CLV is computed only over markets whose game start is strictly
    AFTER the freeze. Markets that resolved before it built the hypothesis and
    can never confirm it.

TWO SCOPE TRAPS THIS GUARDS AGAINST
-----------------------------------
Verified live 2026-08-27 while designing this, and both would mislead badly if
left implicit:

1. These wallets are NOT MLB specialists. Of their recent positions, lottobot
   is mlb 192 / wnba 39 / nfl 24, 0ev is mlb 375 / wnba 53 / nba 41, and
   Bagwall is mlb 137 / fifwc 74 / wnba 28 / bra2 22 / kbo 11. A watchlist
   that just lists "their open trades" shows you a wallet's NFL bets on the
   strength of MLB evidence.

2. Their open positions are mostly SPREADS and TOTALS. The CLV screen scored
   only head-to-head match markets. Every position is therefore tagged
   in_scope (the sport AND market type the wallet was actually validated on)
   or out_of_scope, and the page must show it.

Read-only against the free, no-auth Data API. Writes only its own two tables.

Usage:
    python -m ingest.polymarket_watchlist --freeze-from report.json --sport mlb
    python -m ingest.polymarket_watchlist --refresh-positions
    python -m ingest.polymarket_watchlist --score-forward --sport mlb
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from config import load_config
from db.database import DatabaseManager
from ingest.polymarket_wallet_clv import (
    MIN_MARKET_VOLUME,
    MAX_MARKET_VOLUME,
    accumulate,
    discover_markets,
    parse_ts,
)

DATA = "https://data-api.polymarket.com"
THROTTLE_S = 0.15
POSITIONS_PAGE = 500

COHORT_VERSION = "mlb-clv-v2-2026-08-28"

session = requests.Session()
session.headers["User-Agent"] = "NBADFS-polymarket-watchlist/1.0 (research)"

# eventSlug prefix -> sport. Deliberately coarse; the question is "is this the
# sport we validated them on", not precise taxonomy.
_SPORT_PATTERNS = [
    (re.compile(r"^mlb-"), "mlb"),
    (re.compile(r"^(atp|wta)-"), "tennis"),
    (re.compile(r"^itf-"), "tennis"),
    (re.compile(r"^nba-"), "nba"),
    (re.compile(r"^wnba-"), "wnba"),
    (re.compile(r"^nfl-"), "nfl"),
    (re.compile(r"^nhl-"), "nhl"),
    (re.compile(r"^(kbo|npb|cpbl)-"), "baseball-intl"),
    (re.compile(r"^(fifwc|epl|ucl|laliga|seriea|bundesliga|mls)"), "soccer"),
    (re.compile(r"(bitcoin|ethereum|crypto)"), "crypto"),
    (re.compile(r"(election|president|senate|fed-|cpi)"), "politics"),
]

# Market type, from the market title. The CLV screen scored head-to-head
# winner markets only -- a spread or a total is a different proposition and
# was never validated, however good the wallet looks.
_TOTAL_RE = re.compile(r"\bO/U\b|\bover/under\b|total", re.I)
_SPREAD_RE = re.compile(r"\bspread\b|\([-+]\d", re.I)
_PROP_RE = re.compile(r"\bafter \d|\binning|\bfirst \d|\bto (score|hit|record)", re.I)


def classify_sport(event_slug: str) -> str:
    slug = (event_slug or "").lower()
    for pattern, label in _SPORT_PATTERNS:
        if pattern.search(slug):
            return label
    return "other"


def classify_market_type(title: str) -> str:
    text = title or ""
    if _TOTAL_RE.search(text):
        return "total"
    if _SPREAD_RE.search(text):
        return "spread"
    if _PROP_RE.search(text):
        return "prop"
    return "moneyline"


def in_scope(sport: str, market_type: str, validated_sport: str) -> bool:
    """A position counts as in-scope only when BOTH the sport and the market
    type match what the wallet was actually validated on. Anything else is
    the wallet doing something we have no evidence about."""
    return sport == validated_sport and market_type == "moneyline"


# ---------------------------------------------------------------------------
# schema (self-provisioning, same pattern as other experimental tables)
# ---------------------------------------------------------------------------

_DDL = [
    """CREATE TABLE IF NOT EXISTS polymarket_watchlist_wallets (
        id SERIAL PRIMARY KEY,
        cohort_version TEXT NOT NULL,
        wallet TEXT NOT NULL,
        display_name TEXT,
        validated_sport TEXT NOT NULL,
        model_version TEXT NOT NULL,
        dev_clv DOUBLE PRECISION,
        dev_markets INTEGER,
        holdout_clv_at_freeze DOUBLE PRECISION,
        holdout_markets_at_freeze INTEGER,
        rank_at_freeze INTEGER,
        cohort_group TEXT NOT NULL DEFAULT 'selected',
        frozen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (cohort_version, wallet)
    )""",
    """CREATE TABLE IF NOT EXISTS polymarket_watchlist_positions (
        id SERIAL PRIMARY KEY,
        cohort_version TEXT NOT NULL,
        wallet TEXT NOT NULL,
        condition_id TEXT,
        event_slug TEXT,
        title TEXT,
        outcome TEXT,
        sport TEXT,
        market_type TEXT,
        is_in_scope BOOLEAN NOT NULL DEFAULT FALSE,
        size DOUBLE PRECISION,
        avg_price DOUBLE PRECISION,
        cur_price DOUBLE PRECISION,
        current_value DOUBLE PRECISION,
        cash_pnl DOUBLE PRECISION,
        percent_pnl DOUBLE PRECISION,
        end_date TIMESTAMPTZ,
        captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (cohort_version, wallet, condition_id, outcome, captured_at)
    )""",
    """CREATE TABLE IF NOT EXISTS polymarket_watchlist_forward (
        id SERIAL PRIMARY KEY,
        cohort_version TEXT NOT NULL,
        wallet TEXT NOT NULL,
        scored_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        markets INTEGER NOT NULL,
        stake DOUBLE PRECISION NOT NULL,
        clv DOUBLE PRECISION,
        window_start TIMESTAMPTZ,
        window_end TIMESTAMPTZ,
        UNIQUE (cohort_version, wallet, scored_at)
    )""",
    """CREATE INDEX IF NOT EXISTS idx_pm_watchlist_pos_lookup
         ON polymarket_watchlist_positions(cohort_version, captured_at DESC)""",
]


def ensure_schema(db: DatabaseManager) -> None:
    for ddl in _DDL:
        db.execute(ddl)
    db.execute("ALTER TABLE polymarket_watchlist_wallets "
               "ADD COLUMN IF NOT EXISTS cohort_group TEXT NOT NULL DEFAULT 'selected'")


# ---------------------------------------------------------------------------
# freeze
# ---------------------------------------------------------------------------

def freeze_cohort(db: DatabaseManager, report_path: str, sport: str, cohort: str) -> int:
    """Write the cohort ONCE. Re-running is a no-op by design.

    A watchlist that silently re-ranks is not a forward test -- it is a
    leaderboard that always looks good because the losers keep falling off."""
    with open(report_path, encoding="utf-8") as handle:
        report = json.load(handle)
    wf = report.get("walk_forward") or {}
    rows = wf.get("rows") or []
    rest = wf.get("rest_rows") or []
    if not rows:
        print("report has no walk-forward rows -- nothing to freeze", file=sys.stderr)
        return 0
    if not rest:
        print("WARNING: report carries no unselected control group. The forward "
              "test will only be able to score the selected group's absolute "
              "CLV, not the selection gap -- which is the actual statistic. "
              "Re-run the scan with a build that emits rest_rows.",
              file=sys.stderr)

    existing = db.execute_one(
        "SELECT COUNT(*) AS n FROM polymarket_watchlist_wallets WHERE cohort_version = %s",
        (cohort,),
    )
    if existing and int(existing["n"]) > 0:
        print(f"cohort {cohort} already frozen with {existing['n']} wallets -- "
              f"refusing to modify it. Use a new --cohort to change membership.",
              file=sys.stderr)
        return 0

    written = 0
    for group, group_rows in (("selected", rows), ("control", rest)):
        for rank, row in enumerate(sorted(group_rows, key=lambda r: -r["dev_clv"]), 1):
            db.execute(
                """INSERT INTO polymarket_watchlist_wallets
                     (cohort_version, wallet, display_name, validated_sport, model_version,
                      dev_clv, dev_markets, holdout_clv_at_freeze, holdout_markets_at_freeze,
                      rank_at_freeze, cohort_group)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (cohort_version, wallet) DO NOTHING""",
                (cohort, row["wallet"], row.get("name") or None, sport, "clv-v2",
                 row.get("dev_clv"), row.get("dev_markets"),
                 row.get("holdout_clv"), row.get("holdout_markets"), rank, group),
            )
            written += 1
    print(f"  selected={len(rows)} control={len(rest)}", file=sys.stderr)
    print(f"froze {written} wallets as cohort {cohort} (validated sport: {sport})",
          file=sys.stderr)
    return written


def cohort_wallets(
    db: DatabaseManager, cohort: str, group: Optional[str] = None
) -> List[Dict[str, Any]]:
    if group:
        return db.execute(
            """SELECT wallet, display_name, validated_sport, frozen_at, cohort_group
                 FROM polymarket_watchlist_wallets
                WHERE cohort_version = %s AND cohort_group = %s
                ORDER BY rank_at_freeze""",
            (cohort, group),
        )
    return db.execute(
        """SELECT wallet, display_name, validated_sport, frozen_at, cohort_group
             FROM polymarket_watchlist_wallets
            WHERE cohort_version = %s
            ORDER BY cohort_group, rank_at_freeze""",
        (cohort,),
    )


# ---------------------------------------------------------------------------
# positions
# ---------------------------------------------------------------------------

def fetch_positions(wallet: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    offset = 0
    while True:
        time.sleep(THROTTLE_S)
        try:
            resp = session.get(f"{DATA}/positions", params={
                "user": wallet, "limit": POSITIONS_PAGE, "offset": offset,
            }, timeout=30)
            resp.raise_for_status()
            page = resp.json()
        except Exception as exc:  # noqa: BLE001
            print(f"  ! positions {wallet[:12]}: {exc}", file=sys.stderr)
            break
        if not isinstance(page, list) or not page:
            break
        out.extend(page)
        if len(page) < POSITIONS_PAGE:
            break
        offset += POSITIONS_PAGE
        if offset >= 2000:
            break
    return out


def is_open(position: Dict[str, Any]) -> bool:
    """Open means still trading. A resolved-but-unclaimed position reports
    redeemable=true and curPrice=0 -- listing those as 'open trades' would
    show stale bets as live ones."""
    if position.get("redeemable"):
        return False
    try:
        return float(position.get("curPrice") or 0) > 0
    except (TypeError, ValueError):
        return False


def refresh_positions(db: DatabaseManager, cohort: str) -> Tuple[int, int]:
    # SELECTED only. The 161-wallet control group exists to be scored, not
    # displayed -- pulling its positions daily would be ~8x the requests for
    # rows nobody looks at, and the forward test never reads position
    # snapshots anyway (it rebuilds from the fill tape).
    wallets = cohort_wallets(db, cohort, group="selected")
    if not wallets:
        print(f"no wallets in cohort {cohort}", file=sys.stderr)
        return (0, 0)
    captured_at = datetime.now(timezone.utc)
    total_open = 0
    for entry in wallets:
        wallet = entry["wallet"]
        validated = entry["validated_sport"]
        positions = fetch_positions(wallet)
        open_positions = [p for p in positions if is_open(p)]
        for position in open_positions:
            slug = str(position.get("eventSlug") or "")
            title = str(position.get("title") or "")
            sport = classify_sport(slug)
            market_type = classify_market_type(title)
            db.execute(
                """INSERT INTO polymarket_watchlist_positions
                     (cohort_version, wallet, condition_id, event_slug, title, outcome,
                      sport, market_type, is_in_scope, size, avg_price, cur_price,
                      current_value, cash_pnl, percent_pnl, end_date, captured_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT DO NOTHING""",
                (cohort, wallet, position.get("conditionId"), slug, title,
                 position.get("outcome"), sport, market_type,
                 in_scope(sport, market_type, validated),
                 position.get("size"), position.get("avgPrice"), position.get("curPrice"),
                 position.get("currentValue"), position.get("cashPnl"),
                 position.get("percentPnl"), position.get("endDate"), captured_at),
            )
        total_open += len(open_positions)
        breakdown = Counter(classify_sport(str(p.get("eventSlug") or "")) for p in positions)
        name = entry["display_name"] or wallet[:12]
        print(f"  {name:<16} {len(open_positions):>3} open / {len(positions):>4} total  "
              f"{dict(breakdown.most_common(5))}", file=sys.stderr)
    return (len(wallets), total_open)


# ---------------------------------------------------------------------------
# forward scoring
# ---------------------------------------------------------------------------

def score_forward(db: DatabaseManager, cohort: str, sport: str, max_markets: int) -> int:
    """CLV on markets starting strictly AFTER the freeze.

    This is the number that will eventually mean something. Everything before
    the freeze built the hypothesis and cannot confirm it, so it is excluded
    by construction rather than by remembering to."""
    wallets = cohort_wallets(db, cohort)
    if not wallets:
        print(f"no wallets in cohort {cohort}", file=sys.stderr)
        return 0
    frozen_at = min(w["frozen_at"] for w in wallets)
    if frozen_at.tzinfo is None:
        frozen_at = frozen_at.replace(tzinfo=timezone.utc)
    freeze_ts = int(frozen_at.timestamp())
    tracked = {w["wallet"] for w in wallets}

    all_markets, _ = discover_markets(sport)
    forward = [m for m in all_markets
               if MIN_MARKET_VOLUME <= m["volume"] <= MAX_MARKET_VOLUME
               and m["game_start_ts"] > freeze_ts]
    forward.sort(key=lambda m: -m["volume"])
    forward = forward[:max_markets]
    print(f"forward markets (start after {frozen_at.date()}): {len(forward)}", file=sys.stderr)
    if not forward:
        print("  none yet -- the forward test has not accrued any data. "
              "This is the expected state immediately after freezing.", file=sys.stderr)
        return 0

    aggregated, counts = accumulate(forward)
    scored_at = datetime.now(timezone.utc)
    groups = {w["wallet"]: w["cohort_group"] for w in wallets}
    totals: Dict[str, List[float]] = {"selected": [0.0, 0.0], "control": [0.0, 0.0]}
    written = 0
    for wallet in tracked:
        stats = aggregated.get(wallet)
        if not stats or stats["clv_stake"] <= 0:
            continue
        db.execute(
            """INSERT INTO polymarket_watchlist_forward
                 (cohort_version, wallet, scored_at, markets, stake, clv,
                  window_start, window_end)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT DO NOTHING""",
            (cohort, wallet, scored_at, stats["clv_markets"], stats["clv_stake"],
             stats["clv_num"] / stats["clv_stake"], frozen_at, scored_at),
        )
        bucket = totals.get(groups.get(wallet, "selected"))
        if bucket is not None:
            bucket[0] += stats["clv_stake"]
            bucket[1] += stats["clv_num"]
        written += 1
    print(f"scored {written} of {len(tracked)} tracked wallets over "
          f"{counts['clv_eligible']} eligible forward markets", file=sys.stderr)

    # The selection gap is the test statistic; the absolute level moves with
    # whatever the market did in the window, so it is reported alongside but
    # never alone.
    sel_stake, sel_num = totals["selected"]
    ctl_stake, ctl_num = totals["control"]
    if sel_stake > 0 and ctl_stake > 0:
        sel = sel_num / sel_stake
        ctl = ctl_num / ctl_stake
        print(f"  forward CLV  selected {sel:+.4f}  control {ctl:+.4f}  "
              f"GAP {sel - ctl:+.4f}", file=sys.stderr)
    else:
        print("  gap not computable yet -- one group has no scored forward "
              "stake. This is expected early in the window.", file=sys.stderr)
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description="Polymarket frozen watchlist")
    parser.add_argument("--cohort", default=COHORT_VERSION)
    parser.add_argument("--sport", default="mlb", choices=["mlb", "tennis"])
    parser.add_argument("--freeze-from", help="path to a clv report JSON")
    parser.add_argument("--refresh-positions", action="store_true")
    parser.add_argument("--score-forward", action="store_true")
    parser.add_argument("--max-markets", type=int, default=1200)
    args = parser.parse_args()

    config = load_config()
    if not config.database_url:
        raise SystemExit("DATABASE_URL not set")
    db = DatabaseManager(config.database_url)
    ensure_schema(db)
    if args.freeze_from:
        freeze_cohort(db, args.freeze_from, args.sport, args.cohort)
    if args.refresh_positions:
        wallets, opened = refresh_positions(db, args.cohort)
        print(f"refreshed {wallets} wallets, {opened} open positions", file=sys.stderr)
    if args.score_forward:
        score_forward(db, args.cohort, args.sport, args.max_markets)
    if not (args.freeze_from or args.refresh_positions or args.score_forward):
        parser.error("pick at least one of --freeze-from / --refresh-positions / --score-forward")


if __name__ == "__main__":
    main()
