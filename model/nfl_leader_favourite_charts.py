"""Reader chart: is the favourite to lead a game overrated?

For every game-leader market we priced (rushing yards, receiving yards, receptions, rush + receiving yards),
the favourite by DraftKings' prop-implied share, and how often he actually led, beside our model's share and
the market-anchored blend for the same player. Descriptive; nothing here claims an edge.

  nfl_take_leader_favourites.png

What this is NOT: DraftKings' own "most yards" leader odds are not captured anywhere (The Odds API does not
offer that market), so this cannot test a leader-market price like "+156". The DraftKings number here is the
leader share implied by its over/under player props (``nfl_game_leader_anchored.dk_implied_share``). Real
leader prices enter through the advice pick log (``research.nfl_advice_log``) from now on.

Sources (SELECT only): ``nfl_game_leader_anchored`` (prospective rows preferred, discovery rows re-created
after the game from pregame lines otherwise), ``nfl_season_games`` for kickoffs, nflverse weekly stats for
who led (ties split, as the anchored test grades them).

    python -m model.nfl_leader_favourite_charts --out-dir DIR
"""

from __future__ import annotations

import argparse
import math
import statistics as st
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from model.cfb_pattern_charts import DPI, INK, INK_2, SERIES, SURFACE  # noqa: E402
from model.story_charts import WIDTH_IN, _axes, _finish  # noqa: E402

SEASON = 2026
DEFAULT_DIR = Path("artifacts") / "nfl_market_scenarios"
METRIC_NAMES = {"rushing_yards": "Rushing yards", "receiving_yards": "Receiving yards",
                "receptions": "Receptions", "total_yards": "Rush + rec yards"}
READINGS = (("dk", "DraftKings props", SERIES[0]), ("ours", "Our model", SERIES[1]),
            ("anchored", "Anchored blend", SERIES[2]))
BINS = ((0.0, 0.30, "under 30%"), (0.30, 0.45, "30-45%"), (0.45, 0.60, "45-60%"), (0.60, 1.01, "60%+"))


# ── pure computations ────────────────────────────────────────────────────────
def latest_pregame(rows: list[dict], kickoffs: dict) -> dict[str, list[dict]]:
    """game_id -> the rows of its latest pregame-priced computation (prospective preferred over discovery)."""
    by = defaultdict(list)
    for r in rows:
        k = kickoffs.get(r["game_id"])
        if k is None or r["prop_captured_at"] >= k:
            continue
        if r["origin"] == "prospective" and r["computed_at"] >= k:
            continue
        by[(r["game_id"], r["origin"])].append(r)
    out = {}
    for (gid, origin), rs in by.items():
        if gid in out and origin != "prospective":
            continue
        last = max(r["computed_at"] for r in rs)
        out[gid] = [r for r in rs if r["computed_at"] == last]
    return out


def favourites(games: dict[str, list[dict]], credit_for) -> list[dict]:
    """One row per (game, metric): DraftKings' prop-implied favourite, his three shares, and his lead credit."""
    out = []
    for gid, rows in games.items():
        for metric in METRIC_NAMES:
            mrows = [r for r in rows if r["metric"] == metric and r["dk_implied_share"] is not None
                     and not str(r["identity"]).startswith("OTHER")]
            if not mrows:
                continue
            fav = max(mrows, key=lambda r: r["dk_implied_share"])
            credit = credit_for(gid, metric, mrows)
            if credit is None:
                continue
            out.append({"game_id": gid, "metric": metric, "identity": fav["identity"], "dk": fav["dk_implied_share"],
                        "ours": fav["our_share"], "anchored": fav["anchored_share"],
                        "led": credit.get(fav["identity"], 0.0)})
    return out


def wilson(k: float, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return 0.0, 0.0
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return max(0.0, c - h), min(1.0, c + h)


def summary(rows: list[dict]) -> dict:
    n = len(rows)
    led = sum(r["led"] for r in rows)
    return {"n": n, "led": led, "rate": led / n if n else None, "ci": wilson(led, n),
            **{key: st.mean(r[key] for r in rows if r[key] is not None) if n else None for key, *_ in READINGS}}


# ── chart ────────────────────────────────────────────────────────────────────
def chart(rows: list[dict], path: Path, weeks: str) -> bool:
    if not rows:
        return False
    height = 6.4
    fig, (left, right) = plt.subplots(1, 2, figsize=(WIDTH_IN, height), dpi=DPI, gridspec_kw={"width_ratios": [3, 2]})
    fig.patch.set_facecolor(SURFACE)
    groups = [(METRIC_NAMES[m], [r for r in rows if r["metric"] == m]) for m in METRIC_NAMES] + [("All", rows)]
    groups = [g for g in groups if g[1]]
    width = 0.19
    _axes(left, xgrid=False)
    for i, (label, rs) in enumerate(groups):
        s = summary(rs)
        if not s["n"]:
            continue
        for j, (key, name, colour) in enumerate(READINGS):
            left.bar(i + (j - 1.5) * width, s[key], width=width * 0.95, color=colour, label=name if i == 0 else None)
        x = i + 1.5 * width
        left.bar(x, s["rate"], width=width * 0.95, color=INK, label="Actually led" if i == 0 else None)
        left.errorbar(x, s["rate"], yerr=[[s["rate"] - s["ci"][0]], [s["ci"][1] - s["rate"]]], color=INK_2,
                      linewidth=0.8, capsize=2)
    left.set_xticks(range(len(groups)), [f"{g[0]}\nn={len(g[1])}" for g in groups], fontsize=7)
    left.set_ylim(0, 1)
    left.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}"))
    left.set_ylabel("Chance the favourite leads the game", fontsize=7.5, color=INK_2)
    left.set_title("By market", fontsize=9, color=INK, loc="left", fontweight="bold")
    left.legend(fontsize=6.6, frameon=False, ncol=2, loc="upper right")

    _axes(right)
    xs, ys, ns = [], [], []
    for lo, hi, label in BINS:
        b = [r for r in rows if lo <= r["dk"] < hi]
        if not b:
            continue
        s = summary(b)
        right.errorbar(s["dk"], s["rate"], yerr=[[s["rate"] - s["ci"][0]], [s["ci"][1] - s["rate"]]], fmt="o",
                       color=SERIES[0], ecolor=INK_2, capsize=2, markersize=6)
        right.scatter([s["dk"]], [s["ours"]], marker="x", color=SERIES[1], s=36, zorder=4)
        right.annotate(f"{label}\n{s['led']:.0f}/{s['n']}", (s["dk"], s["rate"]), fontsize=6.2, color=INK,
                       xytext=(6, -10), textcoords="offset points")
    right.plot([0, 1], [0, 1], color=INK_2, linewidth=0.8, linestyle="--")
    right.set_xlim(0, 1)
    right.set_ylim(0, 1)
    for ax in (right.xaxis, right.yaxis):
        ax.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}"))
    right.set_xlabel("DraftKings prop-implied share (favourite)", fontsize=7.5, color=INK_2)
    right.set_ylabel("How often he led", fontsize=7.5, color=INK_2)
    right.set_title("Calibration by DraftKings share", fontsize=9, color=INK, loc="left", fontweight="bold")
    right.scatter([], [], color=SERIES[0], label="Actually led (95% interval)")
    right.scatter([], [], marker="x", color=SERIES[1], label="Our model's average share")
    right.legend(fontsize=6.4, frameon=False, loc="upper left")
    fig.tight_layout(w_pad=2.2)
    s = summary(rows)
    sub = (f"Every game-leader market we priced, {weeks}: the favourite is the player DraftKings' over/under props "
           f"make likeliest to lead. Bars: his average share by each reading, and how\noften he actually led "
           f"(black, ties split, 95% interval). All markets: DraftKings {s['dk']:.0%}, ours {s['ours']:.0%}, "
           f"anchored {s['anchored']:.0%}, actually led {s['rate']:.0%} ({s['led']:.0f} of {s['n']}). Below the "
           f"dashed line\non the right: favourites at that share led less often than the props implied. This is "
           f"prop-implied, not DraftKings' own leader-market odds, which are not captured. Small sample.")
    return _finish(fig, path, "Is the favourite to lead the game overrated?", sub, height)


# ── data ─────────────────────────────────────────────────────────────────────
def load(season: int = SEASON) -> tuple[list[dict], str]:
    from research.nfl_game_leaders_anchored import _connect, load_grade_inputs, winners_for

    conn = _connect()
    try:
        stored, kickoffs, weeks, frame = load_grade_inputs(conn.cursor(), season)
    finally:
        conn.close()
    games = latest_pregame(stored, kickoffs)
    rows = favourites(games, lambda gid, metric, mrows: winners_for(gid, weeks[gid], mrows, frame, metric)
                      if gid in weeks else None)
    ws = sorted({weeks[r["game_id"]] for r in rows})
    return rows, (f"weeks {ws[0]}-{ws[-1]}" if ws else "no weeks")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--season", type=int, default=SEASON)
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    args = ap.parse_args(argv)
    rows, weeks = load(args.season)
    path = Path(args.out_dir) / "nfl_take_leader_favourites.png"
    print(f"{path.name}: {'written' if chart(rows, path, weeks) else 'skipped, no data'} ({len(rows)} markets)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
