"""How the books' NFL lines for rushing yards, receiving yards and receptions did against results.

For every closing line in `nfl_prop_implied_lines` (DraftKings first) whose game
is final, the player's actual stat (nflverse weekly stats) grades the over and
the under: hit rate against the de-vigged chance the price implied, the miss
(actual minus line), and the return of betting every over or every under at
the posted price. Uncertainty is a game-clustered bootstrap (one game's lines
move together). Exploratory and descriptive: several slices are read at once,
so a lone "significant" cell is weak evidence.

A listed player with no stat row is either inactive (books void the bet) or
played and recorded nothing (the under wins). The weekly stats cannot tell
those apart, so they are excluded and COUNTED, and the over rate is re-read as
if every one had been an active zero (the worst case for the over).

    python -m research.nfl_prop_outcomes --season 2026 --weeks 1-5
"""

from __future__ import annotations

import argparse
import collections
import random
import statistics as st
from datetime import datetime, timedelta, timezone

from config import load_config
from db.database import DatabaseManager
from model import nfl_prop_implied as pim
from research.nfl_prop_implied import parse_weeks, player_meta

MARKETS = {"player_rush_yds": "rushing_yards", "player_reception_yds": "receiving_yards", "player_receptions": "receptions"}
LABEL = {"player_rush_yds": "Rushing yards", "player_reception_yds": "Receiving yards", "player_receptions": "Receptions"}


def decimal(american: float) -> float:
    return 1 + (100 / -american if american < 0 else american / 100)


def load_rows(db, season: int, weeks: list[int]) -> tuple[list[dict], collections.Counter]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=5)  # a game this long after kickoff is final
    lines = db.execute(
        """SELECT week, player_key, player_name, team, position, market, line, over_price, under_price, event_id, commence_time, book
           FROM nfl_prop_implied_lines WHERE season = %s AND week = ANY(%s) AND capture_role = 'close'
           AND conversion_version = %s AND market = ANY(%s) AND commence_time < %s""",
        (season, weeks, pim.VERSION, list(MARKETS), cutoff))
    metas = {w: player_meta(db, season, w) for w in weeks}
    out, missing = [], collections.Counter()
    for r in lines:
        if r["line"] is None or r["over_price"] is None or r["under_price"] is None:
            continue
        stats = (metas[r["week"]].get(r["player_key"]) or {}).get("stats")
        base = {**r, "p_over": pim.p_over(r["over_price"], r["under_price"])}
        if not stats:
            missing[r["market"]] += 1
            out.append({**base, "actual": None})
            continue
        out.append({**base, "actual": float(stats.get(MARKETS[r["market"]]) or 0)})
    return out, missing


def boot(groups: dict, fn, n: int = 2000, seed: int = 11):
    rng = random.Random(seed); keys = list(groups); vals = []
    for _ in range(n):
        sample = [x for k in rng.choices(keys, k=len(keys)) for x in groups[k]]
        v = fn(sample)
        if v is not None:
            vals.append(v)
    vals.sort()
    return (vals[int(0.025 * len(vals))], vals[int(0.975 * len(vals)) - 1]) if vals else (float("nan"), float("nan"))


def summarize(rows: list[dict]) -> dict | None:
    graded = [r for r in rows if r["actual"] is not None and r["actual"] != r["line"]]
    if len(graded) < 10:
        return None
    over = [r["actual"] > r["line"] for r in graded]
    roi_over = st.mean((decimal(r["over_price"]) - 1) if r["actual"] > r["line"] else -1 for r in graded)
    roi_under = st.mean((decimal(r["under_price"]) - 1) if r["actual"] < r["line"] else -1 for r in graded)
    return {"n": len(graded), "over_rate": st.mean(over), "implied_over": st.mean(r["p_over"] for r in graded),
            "miss_mean": st.mean(r["actual"] - r["line"] for r in graded), "miss_median": st.median(r["actual"] - r["line"] for r in graded),
            "roi_over": roi_over, "roi_under": roi_under}


def report(rows: list[dict], missing: collections.Counter, weeks: list[int]) -> None:
    print(f"Closing lines graded, weeks {weeks[0]}-{weeks[-1]} (final games only; DraftKings first, else the next book)\n")
    for market, stat in MARKETS.items():
        mrows = [r for r in rows if r["market"] == market]
        played = [r for r in mrows if r["actual"] is not None]
        pushes = sum(1 for r in played if r["actual"] == r["line"])
        s = summarize(mrows)
        if not s:
            continue
        groups = collections.defaultdict(list)
        for r in played:
            if r["actual"] != r["line"]:
                groups[r["event_id"]].append(r)
        gap_ci = boot(groups, lambda xs: st.mean(x["actual"] > x["line"] for x in xs) - st.mean(x["p_over"] for x in xs))
        under_ci = boot(groups, lambda xs: st.mean((decimal(x["under_price"]) - 1) if x["actual"] < x["line"] else -1 for x in xs))
        over_ci = boot(groups, lambda xs: st.mean((decimal(x["over_price"]) - 1) if x["actual"] > x["line"] else -1 for x in xs))
        m = missing.get(market, 0)
        worst = (s["over_rate"] * s["n"]) / (s["n"] + m) if m else s["over_rate"]
        print(f"== {LABEL[market]}: {s['n']} graded lines in {len(groups)} games ({pushes} pushes; {m} listed players with no stat row)")
        print(f"   over hit {s['over_rate']:.1%} vs {s['implied_over']:.1%} implied by the price: gap {s['over_rate'] - s['implied_over']:+.1%} "
              f"(95% CI {gap_ci[0]:+.1%} to {gap_ci[1]:+.1%}); if every no-row player was an active zero: over {worst:.1%}")
        print(f"   actual minus line: mean {s['miss_mean']:+.1f}, median {s['miss_median']:+.1f}")
        print(f"   bet every over: {s['roi_over']:+.1%} per bet (CI {over_ci[0]:+.1%} to {over_ci[1]:+.1%}); "
              f"every under: {s['roi_under']:+.1%} (CI {under_ci[0]:+.1%} to {under_ci[1]:+.1%})")
        # Slices: position, line size (thirds), week.
        print(f"   {'slice':22}{'n':>5}{'over hit':>10}{'implied':>9}{'mean miss':>11}{'under ROI':>11}")
        slices = [(f"pos {p}", [r for r in mrows if r["position"] == p]) for p in ("QB", "RB", "WR", "TE")]
        lines_sorted = sorted(r["line"] for r in played)
        if lines_sorted:
            t1, t2 = lines_sorted[len(lines_sorted) // 3], lines_sorted[2 * len(lines_sorted) // 3]
            slices += [(f"line <= {t1:g}", [r for r in mrows if r["line"] <= t1]),
                       (f"line {t1:g}-{t2:g}", [r for r in mrows if t1 < r["line"] <= t2]),
                       (f"line > {t2:g}", [r for r in mrows if r["line"] > t2])]
        slices += [(f"week {w}", [r for r in mrows if r["week"] == w]) for w in weeks]
        for label, sub in slices:
            ss = summarize(sub)
            if ss:
                print(f"   {label:22}{ss['n']:>5}{ss['over_rate']:>10.1%}{ss['implied_over']:>9.1%}{ss['miss_mean']:>+11.1f}{ss['roi_under']:>+11.1%}")
        print()


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--weeks", default="1-5")
    args = parser.parse_args(argv)
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    weeks = parse_weeks(args.weeks)
    rows, missing = load_rows(db, args.season, weeks)
    report(rows, missing, weeks)


if __name__ == "__main__":
    main()
