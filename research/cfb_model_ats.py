"""ATS and over/under record of the frozen CFB score forecasts (read-only).

For every frozen pregame forecast (research.cfb_prospective_evaluation.load_rows),
take the side the model prefers against the market quote frozen with it, then
grade it at that frozen line and again at the verified close. Flat 1 unit at
an assumed -110. Pushes are 0. Descriptive only; nothing is written.

    python -m research.cfb_model_ats --season 2026
"""

from __future__ import annotations

import argparse
import json
import random
from collections import defaultdict

from config import load_config
from db.database import DatabaseManager
from research.cfb_prospective_evaluation import FORECAST_VERSIONS, _close_values, _final, _forecast_value, load_rows

WIN = 100 / 110
BUCKETS = ((0, "all"), (3, ">=3 pts"), (5, ">=5 pts"), (7, ">=7 pts"))


def grade(pick_sign: float, actual: float, line: float) -> float:
    edge = (actual - line) * pick_sign
    return WIN if edge > 0 else (-1.0 if edge < 0 else 0.0)


def ci(rows: list[dict], key: str, iters: int = 4000) -> tuple[float, float] | None:
    by = defaultdict(list)
    for r in rows:
        by[r["date"]].append(r[key])
    dates = list(by)
    if len(dates) < 3:
        return None
    rng = random.Random(20261017)
    vals = []
    for _ in range(iters):
        s = [v for d in rng.choices(dates, k=len(dates)) for v in by[d]]
        vals.append(sum(s) / len(s))
    vals.sort()
    return vals[int(.025 * iters)], vals[int(.975 * iters) - 1]


def line(label: str, rows: list[dict], key: str) -> str:
    n = len(rows)
    if not n:
        return f"    {label:9s} n=0"
    w = sum(r[key] > 0 for r in rows); l = sum(r[key] < 0 for r in rows)
    u = sum(r[key] for r in rows)
    c = ci(rows, key)
    cs = f"[{c[0]:+.1%},{c[1]:+.1%}]" if c else "[n/a]"
    return (f"    {label:9s} n={n:3d} {w}-{l}-{n-w-l} cover {w/max(1,w+l):5.1%} "
            f"units {u:+6.2f} ROI {u/n:+6.1%} CI{cs} dates={len({r['date'] for r in rows})}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, default=2026)
    args = ap.parse_args(argv)
    db = DatabaseManager(load_config().database_url)
    out = defaultdict(lambda: defaultdict(list))
    for row in load_rows(db, args.season):
        if not _final(row):
            continue
        ev = row["evidence_json"]
        ev = json.loads(ev) if isinstance(ev, str) else ev
        close = _close_values(row)
        margin = float(row["home_score"] - row["away_score"])
        total = float(row["home_score"] + row["away_score"])
        for market, actual in (("spread", margin), ("total", total)):
            q = ev["markets"][market]
            if not q["eligible"]:
                continue
            frozen = float(q["value"])          # spread: expected home margin (= -home spread)
            model = _forecast_value(ev, market)
            gap = model - frozen
            if gap == 0:
                continue
            sign = 1.0 if gap > 0 else -1.0       # spread: home side; total: over
            rec = {"date": str(row["game_date"]), "gap": abs(gap),
                   "game": f"{row['away_name']} @ {row['home_name']}",
                   "frozen": grade(sign, actual, frozen)}
            if close[market] is not None:
                rec["close"] = grade(sign, actual, close[market])
            out[row["version"]][market].append(rec)
    print(f"# CFB model ATS / O-U record, season {args.season} (frozen pregame forecasts, -110, read-only)")
    print("Side = the model's side vs the market quote frozen with the forecast. Graded at that line and at the verified close.")
    for version in FORECAST_VERSIONS:
        print(f"\n== {version} ==")
        for market in ("spread", "total"):
            rows = out[version][market]
            print(f"  {market.upper()} (model side {'home/away ATS' if market == 'spread' else 'over/under'})")
            for floor, lab in BUCKETS:
                sub = [r for r in rows if r["gap"] >= floor]
                print(line(lab + " @fz", sub, "frozen"))
            closed = [r for r in rows if "close" in r]
            for floor, lab in BUCKETS[:2]:
                print(line(lab + " @cl", [r for r in closed if r["gap"] >= floor], "close"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
