"""Pre-registered MLB prop-value measurement program (registered 2026-08-15).

THE QUESTION, stated honestly and narrowly:

    Does `dk_prop_value` produce capturable closing-line value at all?

NOT "which of N markets is most exploitable" -- that question is not answerable
with the data this system can generate. Projected season sample is ~180-220
effective observations TOTAL; split across four markets that is 45-90 per cell,
far below any defensible floor. So the four anchored markets are evaluated as
ONE POOLED CELL and the test family is exactly 1.

Why a family of 1 matters: 10 markets x 2 detectors = 20 tests carries a 64%
chance of at least one spurious "winner" under a pure null, and weekly peeking
pushes that above 95%. This system has already produced exactly that failure
once -- the soccer anytime-scorer detector had six memorable winners inside a
sample whose CI ultimately sat entirely below zero. A family of 1 needs no
multiplicity correction; the count is reported anyway so silent expansion is
visible.

EVERYTHING BELOW IS FROZEN AT REGISTRATION. The single permitted revision is
the Day-14 blinded variance re-estimation, which changes only the arithmetic n
implied by an UNCHANGED power target. Adding a market, a detector, a threshold
variant or a sub-slice creates a NEW program version with its own registration
-- it never joins this family retroactively.

Usage:
    python -m model.mlb_prop_program            # status report
    python -m model.mlb_prop_program --gate     # run the Day-14 blinded gate
"""

from __future__ import annotations

import argparse
import collections
import json
import logging
import math
import random
import statistics
from datetime import date

from config import load_config
from db.database import DatabaseManager

logger = logging.getLogger(__name__)

# ── Frozen registration ──────────────────────────────────────────────────────
PROGRAM_VERSION = "mlb-prop-program-v1"
REGISTERED_AT = "2026-08-15"

LIVE_DETECTOR = "prop-value-v3-dk-trigger-best-exec"
CONTROL_DETECTOR = "prop_line_gap"          # frozen v1 trigger, zero credits
ANCHORED_MARKETS = (
    "pitcher_strikeouts", "batter_total_bases", "pitcher_outs",
    "pitcher_hits_allowed",
)
TEST_FAMILY_SIZE = 1

# Primary metric: mean reference CLV in percentage points, date-clustered.
# MDE is 1.0pp, NOT the 0.5pp originally drafted. At $100/bet and this volume,
# +0.5pp CLV converts to roughly +1% ROI ~ $650/yr, which does not exceed the
# cost of running the program. An effect not worth acting on is not worth
# powering for. This was set on economics confirmed AFTER the first design, and
# it is the last time it moves.
MDE_PP = 1.0
Z_80_POWER = 2.80                            # 80% power, alpha .05 two-sided
PLANNING_SD_PP = 4.12                        # from v1's CI, effect-blind

# Conjunctive sample floors -- ALL must hold before any verdict.
FLOOR_SETTLED = 30                           # matches _MIN_SETTLED_FOR_CI
FLOOR_DISTINCT_DATES = 25                    # clusters, not observations. This
                                             # cannot be bought with volume: a
                                             # date-clustered bootstrap resamples
                                             # CLUSTERS, so 10 dates gives a
                                             # meaningless CI at any n.

# Execution-book concentration (C1-C4). Uniform share across 6 books is 16.7%,
# so the MLB-underdog spec's 25% team bar would be nearly non-binding here.
# One book's pricing quirk is also more dangerous than one team's lucky season:
# it is a systematic artifact the book will actively remove by limiting you.
CONCENTRATION_DISCLOSE = 0.40                # C1: >40% => SINGLE-BOOK FINDING
LOBO_MIN_MAGNITUDE_RETAINED = 0.50           # C2: leave-one-book-out must keep
                                             #     >=50% of the point estimate

DAY14_GATE_DATE = "2026-09-01"
VERDICT_TRIGGER_DATE = "2026-10-04"          # regular season close
BOOTSTRAP_ITERS = 10000
_SEED = 20260815


def floor_n_eff(sd_pp: float = PLANNING_SD_PP) -> int:
    """Pre-registered power floor. Formula frozen; only sd_pp is re-estimated."""
    return int(math.ceil((Z_80_POWER * sd_pp / MDE_PP) ** 2))


def _decimal(american: float) -> float:
    a = float(american)
    return 1 + 100 / abs(a) if a < 0 else 1 + a / 100


def load_clv_observations(db: DatabaseManager, *, detector_version: str | None,
                          control: bool = False) -> list[dict]:
    """Same-book same-line CLV per settled alert.

    Entry is the price frozen at trigger; close is the last PRE-COMMENCE capture
    for the identical (market, player, line, side) at the SAME book the entry
    came from. A moved line is a different proposition and is dropped, never
    graded -- that is the Herrera rule, and it is why `n_comparable` is reported
    separately from `n_settled`.
    """
    where_ver = (
        "AND a.details_json->>'detector_version' = %s" if detector_version
        else "AND NOT (a.details_json ? 'detector_version')"
    )
    params: list = ["mlb", CONTROL_DETECTOR if control else "dk_prop_value"]
    if detector_version:
        params.append(detector_version)

    rows = db.execute(
        f"""
        WITH a AS (
          SELECT a.id, a.game_date, a.matchup_id,
                 a.details_json->>'market'  AS mk,
                 a.details_json->>'player'  AS pl,
                 (a.details_json->>'line')::float AS ln,
                 lower(a.details_json->>'bet')    AS side,
                 (a.details_json->>'dk_decimal')::float AS entry_dec,
                 COALESCE(a.details_json->>'clv_book',
                          a.details_json->>'exec_book',
                          'draftkings')     AS clv_book,
                 a.details_json->>'exec_book' AS exec_book,
                 a.outcome
          FROM line_alerts a
          WHERE a.sport = %s AND a.alert_type = %s
            AND a.outcome IN ('won','lost')
            AND a.details_json ? 'dk_decimal'
            AND a.details_json ? 'line'
            AND a.details_json ? 'player'
            {where_ver}
        ),
        close AS (
          SELECT DISTINCT ON (p.matchup_id, p.market, p.player)
                 p.matchup_id, p.market, p.player, p.books
          FROM prop_odds_history p
          JOIN mlb_matchups m ON m.id = p.matchup_id
          WHERE p.sport = 'mlb' AND m.commence_time IS NOT NULL
            AND p.captured_at < m.commence_time
          ORDER BY p.matchup_id, p.market, p.player, p.captured_at DESC
        )
        SELECT a.*, c.books
        FROM a JOIN close c
          ON c.matchup_id = a.matchup_id AND c.market = a.mk AND c.player = a.pl
        """,
        tuple(params),
    )

    out: list[dict] = []
    for r in rows:
        books = r["books"]
        if isinstance(books, str):
            books = json.loads(books)
        q = (books or {}).get(r["clv_book"])
        if not isinstance(q, dict) or q.get("line") is None:
            continue
        if float(q["line"]) != float(r["ln"]):
            continue                       # different proposition, not gradable
        px = q.get(r["side"])
        if px is None or not r["entry_dec"]:
            continue
        out.append({
            "date": str(r["game_date"]),
            "market": r["mk"],
            "exec_book": r["exec_book"] or "draftkings",
            "clv_pp": 100.0 * (float(r["entry_dec"]) / _decimal(px) - 1.0),
            "outcome": r["outcome"],
        })
    return out


def date_clustered_ci(obs: list[dict], iters: int = BOOTSTRAP_ITERS,
                      seed: int = _SEED) -> tuple[float, float]:
    """Resample DATES, not observations. Alerts on one slate share a pitcher,
    a park, a weather system and an umpire; treating them as independent is how
    a CI ends up 30-40% too narrow."""
    by_date: dict[str, list[float]] = collections.defaultdict(list)
    for o in obs:
        by_date[o["date"]].append(o["clv_pp"])
    keys = list(by_date)
    rng = random.Random(seed)
    means = []
    for _ in range(iters):
        s: list[float] = []
        for _ in range(len(keys)):
            s.extend(by_date[keys[rng.randrange(len(keys))]])
        means.append(sum(s) / len(s))
    means.sort()
    return means[int(0.025 * iters)], means[int(0.975 * iters)]


def design_effect(obs: list[dict]) -> float:
    """DEFF = 1 + (m_bar - 1) * rho. n_eff = n / DEFF."""
    by_date: dict[str, list[float]] = collections.defaultdict(list)
    for o in obs:
        by_date[o["date"]].append(o["clv_pp"])
    sizes = [len(v) for v in by_date.values()]
    if not sizes:
        return 1.0
    m_bar = sum(sizes) / len(sizes)
    if m_bar <= 1 or len(sizes) < 2:
        return 1.0
    grand = statistics.fmean(o["clv_pp"] for o in obs)
    between = sum(len(v) * (statistics.fmean(v) - grand) ** 2 for v in by_date.values())
    within = sum((x - statistics.fmean(v)) ** 2 for v in by_date.values() for x in v)
    total = between + within
    rho = 0.0 if total <= 0 else max(0.0, min(1.0, between / total))
    return 1.0 + (m_bar - 1.0) * rho


def concentration(obs: list[dict]) -> tuple[str, float, dict]:
    """C1 disclosure. Returns (top_book, top_share, full distribution)."""
    c = collections.Counter(o["exec_book"] for o in obs)
    if not c:
        return "", 0.0, {}
    top, n = c.most_common(1)[0]
    return top, n / len(obs), dict(c)


def _fmt_under_floor(obs: list[dict], n_eff: float, floor: int, dates: int) -> str:
    """Under-floor rows show RAW COUNTS ONLY. No rate, no interval, no verdict,
    no colour, no rank. This is where the leak actually happens -- a percentage
    in a weekly table is read as a finding no matter what the caption says."""
    won = sum(1 for o in obs if o["outcome"] == "won")
    lost = sum(1 for o in obs if o["outcome"] == "lost")
    return (f"  DESCRIPTIVE ONLY - NOT A FINDING\n"
            f"  n_eff {n_eff:.0f} / {floor} ({n_eff / floor:.0%})  ·  "
            f"{dates} clusters  ·  {len(obs)} settled\n"
            f"  Record: {won} W - {lost} L\n"
            f"  No rate, no interval and no verdict may be computed for this cell.")


def report(db: DatabaseManager, *, sd_pp: float = PLANNING_SD_PP) -> dict:
    """Status report. Computes a VERDICT only when every floor is met."""
    live = [o for o in load_clv_observations(db, detector_version=LIVE_DETECTOR)
            if o["market"] in ANCHORED_MARKETS]
    ctrl = [o for o in load_clv_observations(db, detector_version=None, control=True)
            if o["market"] in ANCHORED_MARKETS]

    floor = floor_n_eff(sd_pp)
    deff = design_effect(live)
    n_eff = len(live) / deff if deff else 0.0
    dates = len({o["date"] for o in live})

    print(f"\n{'=' * 72}")
    print(f"MLB PROP-VALUE PROGRAM  ·  {PROGRAM_VERSION}  ·  registered {REGISTERED_AT}")
    print(f"{'=' * 72}")
    print(f"question      does {LIVE_DETECTOR} produce capturable CLV at all?")
    print(f"population    ONE pooled cell over {len(ANCHORED_MARKETS)} anchored markets")
    print(f"test family   {TEST_FAMILY_SIZE} declared / 0 resolved  "
          f"(no multiplicity correction needed at family=1)")
    print(f"floors        n_eff>={floor} (SD {sd_pp:.2f}pp, MDE {MDE_PP}pp) "
          f"AND >={FLOOR_DISTINCT_DATES} dates AND >={FLOOR_SETTLED} settled")
    print(f"verdict due   {VERDICT_TRIGGER_DATE}   ·   Day-14 gate {DAY14_GATE_DATE}")

    print(f"\n-- LIVE CELL ({LIVE_DETECTOR}) --")
    meets = (len(live) >= FLOOR_SETTLED and dates >= FLOOR_DISTINCT_DATES
             and n_eff >= floor)
    if not live:
        print("  no settled observations yet - cohort opened 2026-08-15")
    elif not meets:
        print(_fmt_under_floor(live, n_eff, floor, dates))
    else:
        mean = statistics.fmean(o["clv_pp"] for o in live)
        lo, hi = date_clustered_ci(live)
        top, share, dist = concentration(live)
        print(f"  n={len(live)}  n_eff={n_eff:.0f}  DEFF={deff:.2f}  dates={dates}")
        print(f"  mean CLV {mean:+.2f}pp   95% CI [{lo:+.2f}, {hi:+.2f}]")
        print(f"  execution books: {dist}")
        if share > CONCENTRATION_DISCLOSE:
            print(f"  ** SINGLE-BOOK FINDING - {top}, {share:.0%} of alerts **")
            rest = [o for o in live if o["exec_book"] != top]
            if len(rest) >= FLOOR_SETTLED and len({o['date'] for o in rest}) >= FLOOR_DISTINCT_DATES:
                r_mean = statistics.fmean(o["clv_pp"] for o in rest)
                r_lo, r_hi = date_clustered_ci(rest)
                ok = r_lo > 0 and abs(r_mean) >= LOBO_MIN_MAGNITUDE_RETAINED * abs(mean)
                print(f"  C2 leave-one-book-out: n={len(rest)} mean {r_mean:+.2f}pp "
                      f"CI [{r_lo:+.2f}, {r_hi:+.2f}] -> {'PASSES' if ok else 'FAILS'}")
                verdict = "PROMOTE (market finding)" if ok else "RETIRE"
            else:
                print(f"  C3 leave-one-book-out underpowered (n={len(rest)}) "
                      f"-> SINGLE-BOOK - UNVERIFIABLE")
                verdict = ("SINGLE-BOOK - UNVERIFIABLE (counterparty finding only; "
                           "assume it DECAYS, not compounds)")
        else:
            verdict = ("PROMOTE (market finding)" if lo > 0
                       else "RETIRE" if hi < 0 else "EXTEND (one only)")
        print(f"  VERDICT: {verdict}")

    print(f"\n-- CONTROL ({CONTROL_DETECTOR}, frozen v1 trigger, zero credits) --")
    if ctrl:
        c_mean = statistics.fmean(o["clv_pp"] for o in ctrl)
        print(f"  n={len(ctrl)}  mean CLV {c_mean:+.2f}pp   (baseline: -0.13pp)")
        if abs(c_mean - (-0.13)) > 0.30:
            print("  ** INSTRUMENT ALARM - control drifted from its baseline. "
                  "Suspect cadence, close definition or reference drift, "
                  "NOT a finding in the live cell. **")
        print("  Both arms share pipeline, grading, latency and voids, so the "
              "DIFFERENCE (live - control) cancels systematic error.")
        if live and meets:
            print(f"  live - control = "
                  f"{statistics.fmean(o['clv_pp'] for o in live) - c_mean:+.2f}pp")
    else:
        print("  no settled control observations")

    print(f"\n-- PROGRAM KILL (pre-registered) --")
    print("  If the live cell fails its verdict at floor - CLV CI includes or")
    print("  sits below zero - that is the FIFTH confirmed negative and the")
    print("  edge-finding program TERMINATES. Declared before the data.")
    print()
    return {"n": len(live), "n_eff": n_eff, "floor": floor, "dates": dates,
            "meets_floor": meets}


def day14_gate(db: DatabaseManager) -> dict:
    """Blinded variance re-estimation. Decides IN-SEASON vs 2027, once.

    Computes nuisance parameters ONLY -- SD, alert rate, DEFF. The CLV sign and
    magnitude are never printed here, so the decision to evaluate this season
    cannot be contaminated by knowing which way the effect points.
    """
    live = [o for o in load_clv_observations(db, detector_version=LIVE_DETECTOR)
            if o["market"] in ANCHORED_MARKETS]
    print(f"\nDAY-14 BLINDED GATE  ({DAY14_GATE_DATE})")
    print("  effect sign and magnitude deliberately NOT computed")
    if len(live) < 2:
        print(f"  n={len(live)} - insufficient to estimate variance.")
        print("  DECISION: defer to 2027 cohort unless data arrives before the gate date.")
        return {"decision": "insufficient", "n": len(live)}

    # Sign-stripped: variance of |deviations|, never the mean itself.
    sd = statistics.stdev([o["clv_pp"] for o in live])
    deff = design_effect(live)
    dates = len({o["date"] for o in live})
    rate = len(live) / max(dates, 1)
    days_left = (date.fromisoformat(VERDICT_TRIGGER_DATE)
                 - date.fromisoformat(DAY14_GATE_DATE)).days
    projected = len(live) + rate * days_left
    n_eff_proj = projected / deff if deff else 0.0
    floor = floor_n_eff(sd)
    ok = n_eff_proj >= floor
    print(f"  SD {sd:.2f}pp   DEFF {deff:.2f}   rate {rate:.1f}/day   "
          f"dates {dates}")
    print(f"  projected n {projected:.0f} -> n_eff {n_eff_proj:.0f}  vs floor {floor}")
    # Hoisted out of the f-string: an implicit string concatenation split across
    # lines INSIDE a replacement field is a SyntaxError before Python 3.12 (PEP
    # 701 legalised it later), and CI pins 3.11 -- so this module could never be
    # imported and the program's own status reporter had never once run.
    decision = ("EVALUATE IN 2026" if ok else
                "DEFER - 2027 COHORT, 2026 becomes parameter-estimation only")
    print(f"  DECISION: {decision}")
    print("  Decided once, on nuisance parameters only. Not revisited.\n")
    return {"decision": "2026" if ok else "2027", "sd_pp": sd,
            "n_eff_projected": n_eff_proj, "floor": floor}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--gate", action="store_true",
                    help="run the Day-14 blinded variance gate")
    args = ap.parse_args()
    _db = DatabaseManager(load_config().database_url)
    day14_gate(_db) if args.gate else report(_db)
