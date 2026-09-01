"""Closing Line Value (CLV) report — the edge-finding instrument (P1).

ROI needs ~1,000+ settled bets to separate skill from noise; CLV scores every
bet the moment it closes, win or lose. For each bet with >= 2 pre-kickoff
snapshots in the append-only *_bet_snapshots audit trail:

    entry = FIRST snapshot  (when the model first rated the selection)
    close = LAST snapshot at/before the frozen event close when available;
            otherwise LAST snapshot at/before kickoff (legacy fallback)

    delta_prob = close.market_prob - entry.market_prob
        + = the market moved TOWARD our selection after we rated it (we beat
        the close); sustained positive delta_prob in a slice is edge evidence
        even while its win/loss record is still statistical noise.

    clv_ev = close.market_prob * decimal(entry.market_odds) - 1
        EV of the entry-price ticket judged by the closing vig-free number.
        Includes the vig, so ~ -2..-4% is par for a no-signal bettor.
        Soccer only: MLB snapshot odds recorded before 2026-07-02 carry the
        arithmetic-American-averaging corruption (the bets table was repaired;
        the append-only snapshots were left as-recorded), so MLB reports
        probability movement only.

The headline science per slice: corr(entry_edge, delta_prob), where
entry_edge = entry.our_prob - entry.market_prob. If the market systematically
closes toward the side our model liked early, the model is early — that is an
exploitable edge even though it loses to the CLOSING line (see the
Edge-Finding Roadmap in CLAUDE.md).

Exclusions:
  * MLB totals — market_prob is the constant 0.5 vig-free reference (priced at
    a standard -110), so there is no line movement to measure.
  * Bets with a single pre-kickoff snapshot (no movement window).

Usage:
    python -m model.clv_report                 # all sports, all history
    python -m model.clv_report --sport soccer
    python -m model.clv_report --since 2026-06-20
"""

from __future__ import annotations

import argparse
import logging
import math
import sys
from collections import defaultdict

from config import load_config
from db.database import DatabaseManager
from model.soccer_bet_rating import american_to_decimal

logger = logging.getLogger(__name__)

# sport -> (bets table, snapshots table, bet-id FK column, matchup FK column)
_SOURCES = {
    "soccer": ("soccer_bets", "soccer_bet_snapshots", "bet_id", None),
    "mlb": ("mlb_bets", "mlb_bet_snapshots", "bet_id", "matchup_id"),
    # tennis snapshots exist from 2026-07-02 (post-Wimbledon upgrade); tennis
    # market_odds are prob-space consensus from day one, so clv_ev is valid.
    "tennis": ("tennis_bets", "tennis_bet_snapshots", "bet_id", "match_id"),
}


def _collect(
    db: DatabaseManager, sport: str, since: str | None, *, include_legacy: bool = False,
) -> list[dict]:
    """One record per bet: entry/close snapshot pair + bet metadata."""
    bets_tbl, snaps_tbl, fk, matchup_fk = _SOURCES[sport]
    since_clause = "AND b.event_commence >= %s" if since else ""
    params: tuple = (since,) if since else ()
    if sport in ("mlb", "tennis"):
        close_relation = "event_closing_lines" if include_legacy else "verified_clv_closes"
        join_kind = "LEFT JOIN" if include_legacy else "JOIN"
        close_join = (
            f"{join_kind} {close_relation} c "
            f"ON c.sport = %s AND c.matchup_id = b.{matchup_fk}"
        )
    else:
        close_join = "LEFT JOIN event_closing_lines c ON FALSE"
    query_params: tuple = ((sport,) + params) if sport in ("mlb", "tennis") else params
    rows = db.execute(
        f"""
        SELECT b.id AS bet_id, b.bet_type, b.model_version, b.selection_label,
               b.event_commence, b.status, b.stars AS final_stars,
               s.captured_at, s.market_prob, s.market_odds, s.our_prob, s.stars,
               c.captured_at AS frozen_close_at, c.quality AS close_quality,
               c.boundary_source AS close_boundary_source,
               c.clv_cohort AS clv_cohort,
               c.verification_level AS verification_level
        FROM {bets_tbl} b
        JOIN {snaps_tbl} s ON s.{fk} = b.id
        {close_join}
        WHERE b.event_commence IS NOT NULL
          AND s.market_prob IS NOT NULL
          AND s.captured_at <= COALESCE(c.captured_at, b.event_commence)
          {since_clause}
        ORDER BY b.id, s.captured_at
        """,
        query_params,
    )
    by_bet: dict[int, list[dict]] = defaultdict(list)
    meta: dict[int, dict] = {}
    for r in rows:
        by_bet[r["bet_id"]].append(r)
        meta[r["bet_id"]] = r

    records = []
    for bet_id, snaps in by_bet.items():
        if len(snaps) < 2:
            continue
        entry, close = snaps[0], snaps[-1]
        if entry["captured_at"] >= close["captured_at"]:
            continue
        m = meta[bet_id]
        if sport == "mlb" and m["bet_type"] == "total":
            continue  # market_prob is a constant 0.5 reference — no movement
        rec = {
            "sport": sport,
            "bet_type": m["bet_type"],
            "model_version": m["model_version"],
            "entry_stars": entry["stars"],
            "delta_prob": float(close["market_prob"]) - float(entry["market_prob"]),
            "entry_edge": (float(entry["our_prob"]) - float(entry["market_prob"]))
                          if entry["our_prob"] is not None else None,
            "hours_held": (close["captured_at"] - entry["captured_at"]).total_seconds() / 3600,
            "close_source": (
                "verified_clv_v1"
                if close.get("clv_cohort") == "verified_clv_v1" else
                "non_primary_frozen"
                if close.get("frozen_close_at") else
                "legacy_last_observed"
            ),
            "close_quality": close.get("close_quality"),
            "verification_level": close.get("verification_level"),
            "clv_ev": None,
        }
        # Entry-price EV at the closing number — soccer only (see module doc).
        if sport == "soccer" and entry["market_odds"] is not None:
            try:
                rec["clv_ev"] = (float(close["market_prob"])
                                 * american_to_decimal(int(entry["market_odds"])) - 1)
            except (ValueError, ZeroDivisionError):
                pass
        records.append(rec)
    return records


def _mean(xs: list[float]) -> float:
    return sum(xs) / len(xs)


def _t_stat(xs: list[float]) -> float | None:
    """t statistic for mean != 0 (the 'is this movement real' number)."""
    n = len(xs)
    if n < 3:
        return None
    mu = _mean(xs)
    var = sum((x - mu) ** 2 for x in xs) / (n - 1)
    if var == 0:
        return None
    return mu / math.sqrt(var / n)


def _corr_slope(pairs: list[tuple[float, float]]) -> tuple[float, float] | None:
    """(pearson r, OLS slope) of y on x; None if degenerate."""
    n = len(pairs)
    if n < 10:
        return None
    xs = [p[0] for p in pairs]
    ys = [p[1] for p in pairs]
    mx, my = _mean(xs), _mean(ys)
    cov = sum((x - mx) * (y - my) for x, y in pairs)
    vx = sum((x - mx) ** 2 for x in xs)
    vy = sum((y - my) ** 2 for y in ys)
    if vx == 0 or vy == 0:
        return None
    return cov / math.sqrt(vx * vy), cov / vx


def _fmt_slice(label: str, recs: list[dict]) -> str:
    d = [r["delta_prob"] for r in recs]
    pos = sum(1 for x in d if x > 0)
    neu = sum(1 for x in d if x == 0)
    t = _t_stat(d)
    evs = [r["clv_ev"] for r in recs if r["clv_ev"] is not None]
    hours = _mean([r["hours_held"] for r in recs])
    line = (f"  {label:<42} n={len(recs):>5}  "
            f"avg Δprob={_mean(d)*100:+.2f}pp  beat-close={pos/len(recs)*100:.0f}%"
            f" (flat {neu/len(recs)*100:.0f}%)  t={t:+.1f}" if t is not None else
            f"  {label:<42} n={len(recs):>5}  avg Δprob={_mean(d)*100:+.2f}pp")
    if evs:
        line += f"  clv_ev={_mean(evs)*100:+.1f}%"
    line += f"  held={hours:.0f}h"
    return line


def report(
    db: DatabaseManager, sports: list[str], since: str | None, *, include_legacy: bool = False,
) -> None:
    for sport in sports:
        recs = _collect(db, sport, since, include_legacy=include_legacy)
        cohort_label = (
            "including non-primary/legacy" if include_legacy else "verified_clv_v1 only"
        ) if sport in ("mlb", "tennis") else "legacy methodology; no v1 adapter"
        print(
            f"\n=== {sport.upper()} — {len(recs)} bets with a measurable entry→close window "
            f"({cohort_label}) ==="
        )
        if not recs:
            continue
        verified = [r for r in recs if r["close_source"] == "verified_clv_v1"]
        quality_counts = {
            quality: sum(1 for r in verified if r["close_quality"] == quality)
            for quality in ("A", "B", "C", "stale")
        }
        print(
            f"    close source: verified={len(verified)} non-primary/legacy={len(recs)-len(verified)} "
            f"quality={quality_counts}"
        )
        verification_counts = {
            level: sum(1 for r in verified if r["verification_level"] == level)
            for level in ("actual_start", "scheduled_boundary")
        }
        print(f"    boundary verification: {verification_counts}")

        by_type: dict[str, list[dict]] = defaultdict(list)
        for r in recs:
            by_type[r["bet_type"]].append(r)

        for bt, rs in sorted(by_type.items()):
            print(_fmt_slice(f"{bt} (all)", rs))
            # Entry-star tiers: did our early conviction predict movement?
            by_star: dict[int, list[dict]] = defaultdict(list)
            for r in rs:
                if r["entry_stars"] is not None:
                    by_star[r["entry_stars"]].append(r)
            for star in sorted(by_star):
                if len(by_star[star]) >= 10:
                    print(_fmt_slice(f"    {star}★ at entry", by_star[star]))

            # The headline science: does entry-time model disagreement predict
            # the direction the market closes?
            pairs = [(r["entry_edge"], r["delta_prob"]) for r in rs
                     if r["entry_edge"] is not None]
            cs = _corr_slope(pairs)
            if cs:
                r_, slope = cs
                print(f"    corr(entry model edge, close movement): r={r_:+.3f}  "
                      f"slope={slope:+.3f}  (n={len(pairs)}; + = market closes toward our side)")
        print()


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")  # Windows cp1252 console
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Closing Line Value report")
    parser.add_argument("--sport", choices=sorted(_SOURCES), help="Limit to one sport")
    parser.add_argument("--since", help="Only events commencing on/after YYYY-MM-DD")
    parser.add_argument(
        "--include-legacy", action="store_true",
        help="Explicitly include stale, non-primary frozen, and legacy last-observed closes",
    )
    args = parser.parse_args()

    config = load_config()
    db = DatabaseManager(config.database_url)
    report(
        db, [args.sport] if args.sport else sorted(_SOURCES), args.since,
        include_legacy=args.include_legacy,
    )
