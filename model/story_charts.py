"""Reader charts for the betting-market story: the same games, shown nine ways.

Shared by the NFL (model/nfl_market_scenarios.py) and college
(model/cfb_pattern_charts.py) chart exports. Every function takes the graded
game records both sports already build (``r["g"]`` is the game, plus
``home_fav``, ``won``, ``price``, ``dog``, ``imp``, ``cover``, ``ou``), so the
numbers match the scenario charts. Descriptive, season to date; nothing here
claims an edge.

1. margin_chart / total_chart  every game: the favorite's margin against its line,
                               and points scored against the total
2. line_miss_chart             how far off the spread was, and final margins
3. bankroll_chart              $100 a game on each side, week by week
4. slope_chart                 the biggest changes from the prior seasons
5. calibration_chart           do favorites win as often as their price says?
6. team_calendar_chart         every team's cover result, week by week
7. team_quadrant_chart         cover rate against over rate, by team
8. outcomes_chart              favorite won and covered / won, did not cover / lost
9. late_flips_chart            games whose spread result changed in the last five minutes
"""

from __future__ import annotations

import math
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

from model.cfb_pattern_charts import DPI, GRID, INK, INK_2, SERIES, SURFACE  # noqa: E402
from model.cfb_scenario_charts import _heading, _save  # noqa: E402
from model.cfb_team_charts import _pick_labels, _place_labels  # noqa: E402

WIDTH_IN = 9.6
GOOD, BAD, NEUTRAL = "#1baf7a", "#e34948", "#b9b7b1"
WIN_U = 100 / 110   # a winning -110 bet returns 0.909 units


# ── shared helpers ───────────────────────────────────────────────────────────
def teams(g: dict) -> tuple[str, str]:
    """(home, away) names; the NFL games carry ``home``/``away``, the college ones ``hname``/``aname``."""
    return (g.get("home") or g.get("hname") or "?"), (g.get("away") or g.get("aname") or "?")


def fav_view(r: dict) -> dict:
    """The moneyline favorite's margin, the points it gave (its line, sign flipped) and the names."""
    g = r["g"]
    home, away = teams(g)
    margin = g["home_score"] - g["away_score"]
    line = float(g["home_spread"]) if r["home_fav"] else -float(g["home_spread"])
    return {"fav": home if r["home_fav"] else away, "dog": away if r["home_fav"] else home,
            "margin": margin if r["home_fav"] else -margin, "given": -line,
            "label": f"{away} @ {home}", "week": g["week"]}


def _axes(ax, xgrid=True, ygrid=True):
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=INK_2, labelsize=7.5, length=0)
    if xgrid:
        ax.grid(axis="x", color=GRID, linewidth=0.6)
    if ygrid:
        ax.grid(axis="y", color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)


def _fig(height: float, ncols: int = 1, **kw):
    fig, axes = plt.subplots(1, ncols, figsize=(WIDTH_IN, height), dpi=DPI, **kw)
    fig.patch.set_facecolor(SURFACE)
    return fig, axes


def _finish(fig, path: Path, title: str, subtitle: str, height: float) -> bool:
    _heading(fig, title, subtitle)
    _save(fig, path, top=1 - (0.5 + 0.15 * (subtitle.count("\n") + 1)) / height)
    return True


def _dollars(v: float) -> str:
    return f"{'+' if v > 0 else '-' if v < 0 else ''}${abs(v):,.0f}"


def spread_labels(ys: list[float], gap: float) -> list[float]:
    """Label positions for sorted ``ys``: as close to each value as possible, at least ``gap`` apart."""
    out = list(ys)
    for _ in range(200):
        moved = False
        for i in range(1, len(out)):
            short = gap - (out[i] - out[i - 1])
            if short > 1e-9:
                out[i - 1] -= short / 2
                out[i] += short / 2
                moved = True
        if not moved:
            break
    return out


def _season_note(records: list[dict]) -> str:
    weeks = [r["g"]["week"] for r in records]
    return f"{len(records)} games, weeks {min(weeks)}-{max(weeks)}"


# ── 1. every game against its line ───────────────────────────────────────────
def margin_chart(records: list[dict], path: Path, sport: str, n_labels: int = 8) -> bool:
    """One dot per game: points the favorite gave (x) against its winning margin (y)."""
    if len(records) < 5:
        return False
    rows = [fav_view(r) | {"cover": r["cover"]} for r in records]
    height = 6.2
    fig, ax = _fig(height)
    _axes(ax)
    lo = min(-3, min(v["given"] for v in rows) - 1)
    hi = max(v["given"] for v in rows) + 2
    ax.fill_between([lo, hi], [lo, hi], [80, 80], color=GOOD, alpha=0.05, linewidth=0)
    ax.fill_between([lo, hi], [-80, -80], [lo, hi], color=BAD, alpha=0.05, linewidth=0)
    ax.plot([lo, hi], [lo, hi], color=INK_2, linewidth=1, linestyle="--")
    ax.axhline(0, color=INK_2, linewidth=0.8)
    colour = {1: GOOD, -1: BAD, 0: NEUTRAL}
    ax.scatter([v["given"] for v in rows], [v["margin"] for v in rows], s=20,
               c=[colour[v["cover"]] for v in rows], alpha=0.8, edgecolor=SURFACE, linewidth=0.5, zorder=3)
    ys = [v["margin"] for v in rows]
    ax.set_xlim(lo, hi)
    ax.set_ylim(min(ys) - 4, max(ys) + 4)
    ax.set_xlabel("Points the favorite gave (closing spread)", color=INK_2, fontsize=8)
    ax.set_ylabel("Favorite's winning margin (negative: lost)", color=INK_2, fontsize=8)
    ax.text(0.01, 0.98, "covered", color=GOOD, fontsize=9, ha="left", va="top", fontweight="bold", transform=ax.transAxes)
    ax.text(0.99, 0.02, "did not cover", color=BAD, fontsize=9, ha="right", va="bottom", fontweight="bold",
            transform=ax.transAxes)
    covers = sum(v["cover"] > 0 for v in rows)
    misses = sum(v["cover"] < 0 for v in rows)
    title = f"Every {sport} game against the spread"
    sub = (f"{_season_note(records)}. Each dot is one game. Dots above the dashed line: the favorite covered "
           f"({covers}); below: it did not ({misses}); grey: push.\nLabelled: the games furthest from their line.")
    fig.tight_layout(rect=(0, 0, 1, 1 - 0.85 / height))
    pts = [dict(v, miss=v["margin"] - v["given"]) for v in rows]
    for v in pts:
        v["abs_miss"] = abs(v["miss"])
    top = sorted(pts, key=lambda v: -v["abs_miss"])[:n_labels]
    _place_labels(ax, [(v["given"], v["margin"], f"{v['fav']} ({v['margin']:+d})") for v in top],
                  [(v["given"], v["margin"]) for v in rows])
    return _finish(fig, path, title, sub, height)


def total_chart(records: list[dict], path: Path, sport: str, n_labels: int = 8) -> bool:
    """One dot per game: the closing total (x) against points scored (y)."""
    rows = [r for r in records if r["g"].get("vegas_total") is not None]
    if len(rows) < 5:
        return False
    xs = [float(r["g"]["vegas_total"]) for r in rows]
    ys = [r["g"]["home_score"] + r["g"]["away_score"] for r in rows]
    height = 6.2
    fig, ax = _fig(height)
    _axes(ax)
    lo, hi = min(xs) - 2, max(xs) + 2
    ax.plot([lo, hi], [lo, hi], color=INK_2, linewidth=1, linestyle="--")
    colour = {1: SERIES[0], -1: SERIES[1], 0: NEUTRAL}
    ax.scatter(xs, ys, s=20, c=[colour[r["ou"]] for r in rows], alpha=0.8, edgecolor=SURFACE, linewidth=0.5, zorder=3)
    ax.set_xlim(lo, hi)
    ax.set_xlabel("Closing total", color=INK_2, fontsize=8)
    ax.set_ylabel("Points scored", color=INK_2, fontsize=8)
    overs = sum(r["ou"] > 0 for r in rows)
    unders = sum(r["ou"] < 0 for r in rows)
    sub = (f"{_season_note(rows)}. Each dot is one game. Blue, above the dashed line: over ({overs}); "
           f"orange, below: under ({unders}); grey: push.\nLabelled: the games furthest from their total.")
    fig.tight_layout(rect=(0, 0, 1, 1 - 0.85 / height))
    order = sorted(range(len(rows)), key=lambda i: -abs(ys[i] - xs[i]))[:n_labels]
    labels = []
    for i in order:
        home, away = teams(rows[i]["g"])
        labels.append((xs[i], ys[i], f"{away} @ {home} ({ys[i] - xs[i]:+.0f})"))
    _place_labels(ax, labels, list(zip(xs, ys)))
    return _finish(fig, path, f"Every {sport} game against the total", sub, height)


# ── 2. how far off the line was ──────────────────────────────────────────────
def line_miss_chart(records: list[dict], path: Path, sport: str, key_numbers=(3, 7)) -> bool:
    """Left: the favorite's margin minus the points it gave. Right: final margins, key numbers marked."""
    if len(records) < 5:
        return False
    rows = [fav_view(r) for r in records]
    miss = [v["margin"] - v["given"] for v in rows]
    height = 4.6
    fig, (a1, a2) = _fig(height, 2)
    for ax in (a1, a2):
        _axes(ax, xgrid=False)
    # Round away from zero, so a half-point cover lands on +1 and a half-point miss on -1; 0 is pushes only.
    buckets = [math.ceil(m) if m > 0 else math.floor(m) if m < 0 else 0 for m in miss]
    centres = list(range(min(buckets), max(buckets) + 1))
    counts = [buckets.count(c) for c in centres]
    a1.bar(centres, counts, width=0.9, color=[GOOD if c > 0 else BAD if c < 0 else NEUTRAL for c in centres],
           linewidth=0)
    a1.axvline(0, color=INK_2, linewidth=0.8)
    hook_lost = sum(1 for m in miss if m == -0.5)
    hook_won = sum(1 for m in miss if m == 0.5)
    a1.set_title("Favorite's margin minus the spread", loc="left", fontsize=9, color=INK)
    a1.set_xlabel("Points, half points rounded out: right of 0 covered, left did not, 0 push", color=INK_2, fontsize=7.5)
    within3 = sum(abs(m) <= 3 for m in miss)
    a1.set_ylim(0, max(counts) * 1.4 + 1)
    a1.text(0.02, 0.97, f"Within 3 points of the line: {within3} of {len(miss)}\n"
            f"Decided by half a point: favorites {hook_won} covered, {hook_lost} did not",
            transform=a1.transAxes, fontsize=7, color=INK_2, va="top")
    margins = [abs(r["g"]["home_score"] - r["g"]["away_score"]) for r in records]
    top = min(max(margins), 40)
    xs = list(range(1, top + 1))
    cnt = [sum(1 for m in margins if m == x) for x in xs]
    a2.bar(xs, cnt, width=0.85, color=[SERIES[3] if x in key_numbers else SERIES[0] for x in xs], linewidth=0)
    for k in key_numbers:
        if k <= top:
            share = sum(1 for m in margins if m == k) / len(margins)
            a2.text(k, cnt[k - 1] + 0.3, f"{k}: {share:.0%}", ha="center", va="bottom", fontsize=7, color=INK)
    a2.set_title("Final winning margins", loc="left", fontsize=9, color=INK)
    a2.set_xlabel(f"Points (margins above {top} not shown)" if max(margins) > top else "Points", color=INK_2, fontsize=7.5)
    sub = (f"{_season_note(records)}. Left: how far each favorite finished from its line. "
           f"Right: winning margins, with {' and '.join(map(str, key_numbers))} in gold, the most common in football.")
    return _finish(fig, path, f"How close the {sport} spread came", sub, height)


# ── 3. the bankroll race ─────────────────────────────────────────────────────
BETS = (("Favorites, moneyline", SERIES[0], "-"), ("Underdogs, moneyline", SERIES[1], "-"),
        ("Favorites, spread", SERIES[0], "--"), ("Underdogs, spread", SERIES[1], "--"),
        ("Overs", SERIES[2], "-"), ("Unders", SERIES[6], "-"))


def bet_units(r: dict) -> dict:
    """Units won by one unit on each of the six bets in one game (spread and total at -110, pushes 0)."""
    won = r["won"]
    out = {"Favorites, moneyline": (r["price"] - 1) if won else -1.0,
           "Underdogs, moneyline": -1.0 if won else (r["dog"] - 1),
           "Favorites, spread": WIN_U if r["cover"] > 0 else -1.0 if r["cover"] < 0 else 0.0,
           "Underdogs, spread": WIN_U if r["cover"] < 0 else -1.0 if r["cover"] > 0 else 0.0}
    if r["ou"] is not None:
        out["Overs"] = WIN_U if r["ou"] > 0 else -1.0 if r["ou"] < 0 else 0.0
        out["Unders"] = WIN_U if r["ou"] < 0 else -1.0 if r["ou"] > 0 else 0.0
    return out


def bankroll(records: list[dict]) -> tuple[list[int], dict[str, list[float]]]:
    """(weeks, running dollars per bet at $100 a game) after each week."""
    weeks = sorted({r["g"]["week"] for r in records})
    run = {b[0]: 0.0 for b in BETS}
    series = {b[0]: [] for b in BETS}
    for w in weeks:
        for r in records:
            if r["g"]["week"] == w:
                for k, v in bet_units(r).items():
                    run[k] += 100 * v
        for k in run:
            series[k].append(run[k])
    return weeks, series


def bankroll_chart(records: list[dict], path: Path, sport: str) -> bool:
    if len(records) < 5:
        return False
    weeks, series = bankroll(records)
    if len(weeks) < 2:
        return False
    height = 4.8
    fig, (a1, a2) = _fig(height, 2, sharey=True)
    for ax, names, title in ((a1, BETS[:2], "Moneyline"), (a2, BETS[2:], "Spread and total at -110")):
        _axes(ax, xgrid=False)
        ax.axhline(0, color=INK_2, linewidth=0.8)
        ends = []
        for name, colour, style in names:
            ax.plot(weeks, series[name], color=colour, linestyle=style, linewidth=2, marker="o", markersize=2.5)
            ends.append((series[name][-1], name, colour))
        ends.sort()
        last_y = None
        span = max(abs(v) for s in series.values() for v in s) or 1
        for y, name, colour in ends:   # keep end labels from overlapping
            y_text = y if last_y is None else max(y, last_y + span * 0.09)
            ax.text(weeks[-1] + 0.25, y_text, f"{name} {_dollars(y)}", color=colour, fontsize=7, va="center")
            last_y = y_text
        ax.set_xlim(weeks[0] - 0.3, weeks[-1] + (2.6 if len(weeks) < 8 else 4))
        ax.set_xticks(weeks)
        ax.set_title(title, loc="left", fontsize=9, color=INK)
        ax.set_xlabel("Week", color=INK_2, fontsize=7.5)
    a1.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: _dollars(v)))
    sub = (f"{_season_note(records)}. Running profit from $100 on every game, by bet. Moneyline at the closing price; "
           "spread and total at -110, pushes returned.")
    return _finish(fig, path, f"The {sport} bankroll race", sub, height)


# ── 4. the biggest changes from the prior seasons ────────────────────────────
SLOPE_MARKETS = (("Favorites, moneyline", "ROI (%)", "Games"), ("Favorites, spread", "Fav ATS ROI (%)", "Games"),
                 ("Overs", "Over ROI (%)", "Games with total"))


def slope_rows(rows: list[dict], prior_rows: list[dict], market: tuple, exclude=(), min_games: int = 20,
               n: int = 8) -> list[dict]:
    """Single-cut groups with enough games now and a prior value, biggest change first."""
    _, col, n_col = market
    prior = {(r["Level name"], r["Split"]): r for r in prior_rows if not r.get("Grid")}
    out = []
    for r in rows:
        if r.get("Grid") or r.get("Level name") in exclude or r.get("Level name") == "All games":
            continue
        p = prior.get((r["Level name"], r["Split"]))
        now, then = r.get(col), p.get(col) if p else None
        if now is None or then is None or (r.get(n_col) or 0) < min_games:
            continue
        out.append({"group": r["Split"], "now": now, "then": then, "games": r.get(n_col)})
    out.sort(key=lambda x: -abs(x["now"] - x["then"]))
    return out[:n]


def slope_chart(rows: list[dict], prior_rows: list[dict], path: Path, sport: str, prior_label: str,
                exclude=(), min_games: int = 20) -> bool:
    panels = [(m, slope_rows(rows, prior_rows, m, exclude, min_games)) for m in SLOPE_MARKETS]
    if not any(p for _, p in panels):
        return False
    height = 5.6
    fig, axes = _fig(height, 3, sharey=True)
    for ax, (market, data) in zip(axes, panels):
        _axes(ax, xgrid=False)
        ax.axhline(0, color=INK_2, linewidth=0.8)
        for d in data:
            colour = GOOD if d["now"] > d["then"] else BAD
            ax.plot([0, 1], [d["then"], d["now"]], color=colour, linewidth=1.6, marker="o", markersize=3.5)
        # End labels spread apart, each tied to its point by a short leader.
        span = max([abs(v) for _, d in panels for x in d for v in (x["now"], x["then"])] + [10])
        ordered = sorted(data, key=lambda x: x["now"])
        for d, y in zip(ordered, spread_labels([d["now"] for d in ordered], gap=span * 0.11)):
            ax.plot([1.0, 1.05], [d["now"], y], color=INK_2, linewidth=0.5)
            ax.text(1.07, y, f"{d['group']} ({d['games']})", fontsize=6.3, color=INK, va="center")
        ax.set_xlim(-0.15, 2.4)
        ax.set_xticks([0, 1], [prior_label, "This season"], fontsize=7)
        ax.set_title(market[0], loc="left", fontsize=9, color=INK)
        if not data:
            ax.text(0.5, 0.5, "no group has enough games yet", transform=ax.transAxes, ha="center", fontsize=7, color=INK_2)
    axes[0].yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:+.0f}%"))
    sub = (f"Return per unit for the eight groups that moved most since {prior_label}, among groups with at least "
           f"{min_games} games this season.\nGreen: better than before, red: worse. Moneyline at the closing price, "
           "spread and total at -110.")
    return _finish(fig, path, f"What changed in the {sport} market this season", sub, height)


# ── 5. do favorites win as often as their price says? ────────────────────────
CAL_BINS = (0.5, 0.6, 0.7, 0.8, 0.9, 1.0001)


def calibration_rows(records: list[dict], bins=CAL_BINS) -> list[dict]:
    out = []
    for lo, hi in zip(bins, bins[1:]):
        rs = [r for r in records if lo <= r["imp"] < hi]
        if not rs:
            continue
        n = len(rs)
        p = sum(r["won"] for r in rs) / n
        z = 1.96   # Wilson interval
        centre = (p + z * z / (2 * n)) / (1 + z * z / n)
        half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / (1 + z * z / n)
        out.append({"lo": lo, "hi": min(hi, 1.0), "n": n, "implied": sum(r["imp"] for r in rs) / n,
                    "actual": p, "ci": (centre - half, centre + half)})
    return out


def calibration_chart(records: list[dict], prior: list[dict], path: Path, sport: str, prior_label: str) -> bool:
    now = calibration_rows(records)
    if not now:
        return False
    then = calibration_rows(prior)
    height = 5.4
    fig, ax = _fig(height)
    _axes(ax)
    ax.plot([0.5, 1], [0.5, 1], color=INK_2, linestyle="--", linewidth=1, label="Price exactly right")
    if then:
        ax.scatter([t["implied"] for t in then], [t["actual"] for t in then], s=[12 + t["n"] / 6 for t in then],
                   facecolor="none", edgecolor=INK_2, linewidth=1, zorder=3, label=prior_label)
    for t in now:
        ax.plot([t["implied"]] * 2, t["ci"], color=SERIES[0], alpha=0.35, linewidth=3, solid_capstyle="round")
    ax.scatter([t["implied"] for t in now], [t["actual"] for t in now], s=[25 + t["n"] * 2 for t in now],
               color=SERIES[0], zorder=4, label="This season")
    for t in now:
        ax.text(t["implied"] + 0.008, t["actual"], f"{t['lo']:.0%}-{t['hi']:.0%}: {t['actual']:.0%} of {t['n']}",
                fontsize=6.8, color=INK, va="center")
    ax.set_xlim(0.5, 1.0)
    ax.set_ylim(0.3, 1.02)
    pct = plt.FuncFormatter(lambda v, _: f"{v:.0%}")
    ax.xaxis.set_major_formatter(pct)
    ax.yaxis.set_major_formatter(pct)
    ax.set_xlabel("Win chance the closing moneyline gave the favorite (vig removed)", color=INK_2, fontsize=8)
    ax.set_ylabel("How often those favorites won", color=INK_2, fontsize=8)
    ax.legend(loc="lower right", fontsize=7, frameon=False)
    sub = (f"{_season_note(records)}, grouped by the favorite's price. Dots on the dashed line mean the market priced "
           "those games right.\nBlue bars: the range the true rate plausibly sits in, given how few games each group has "
           f"(95% interval). Hollow circles: {prior_label}.")
    return _finish(fig, path, f"Do {sport} favorites win as often as their price says?", sub, height)


# ── 6 and 7. teams ───────────────────────────────────────────────────────────
def team_games(records: list[dict]) -> dict[str, list[dict]]:
    """{team: [{week, opp, home, cover, ou}]}, from the team's own side of the line."""
    out: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        g = r["g"]
        home, away = teams(g)
        margin = g["home_score"] - g["away_score"]
        hs = float(g["home_spread"])
        for name, opp, is_home, m, line in ((home, away, True, margin, hs), (away, home, False, -margin, -hs)):
            c = m + line
            out[name].append({"week": g["week"], "opp": opp, "home": is_home,
                              "cover": (c > 0) - (c < 0), "ou": r["ou"]})
    return out


def team_rows(records: list[dict], min_games: int = 3) -> list[dict]:
    rows = []
    for team, gs in team_games(records).items():
        decided = [x for x in gs if x["cover"] != 0]
        totals = [x for x in gs if x["ou"] not in (None, 0)]
        if len(gs) < min_games or not decided or not totals:
            continue
        rows.append({"team": team, "games": len(gs), "covers": sum(x["cover"] > 0 for x in decided),
                     "cover_rate": sum(x["cover"] > 0 for x in decided) / len(decided),
                     "over_rate": sum(x["ou"] > 0 for x in totals) / len(totals), "list": gs})
    rows.sort(key=lambda x: (-x["cover_rate"], -x["covers"], x["team"]))
    return rows


def _calendar_panel(ax, rows: list[dict], weeks: list[int], short_names: bool) -> None:
    ax.set_facecolor(SURFACE)
    for s in ax.spines.values():
        s.set_visible(False)
    ax.tick_params(colors=INK_2, labelsize=7, length=0)
    n = len(rows)
    for i, t in enumerate(rows):
        y = n - 1 - i
        by_week = {x["week"]: x for x in t["list"]}
        for j, w in enumerate(weeks):
            x = by_week.get(w)
            colour = "#f1f0ec" if x is None else GOOD if x["cover"] > 0 else BAD if x["cover"] < 0 else NEUTRAL
            ax.add_patch(plt.Rectangle((j, y), 0.92, 0.86, color=colour, linewidth=0))
            if x is not None and short_names:
                ax.text(j + 0.46, y + 0.43, ("" if x["home"] else "@") + x["opp"], ha="center", va="center",
                        fontsize=5.2, color="white" if x["cover"] else INK)
    ax.set_xlim(0, len(weeks))
    ax.set_ylim(0, n)
    ax.set_xticks([j + 0.46 for j in range(len(weeks))], [str(w) for w in weeks], fontsize=6.5)
    ax.set_yticks([n - 1 - i + 0.43 for i in range(n)],
                  [f"{t['team']}  {t['covers']}-{sum(x['cover'] < 0 for x in t['list'])}" for t in rows], fontsize=6.5)
    ax.xaxis.set_ticks_position("top")


def team_calendar_chart(records: list[dict], path: Path, sport: str, max_rows: int = 40, split: int = 25,
                        extra: str = "") -> bool:
    rows = team_rows(records, min_games=1)
    if not rows:
        return False
    weeks = sorted({r["g"]["week"] for r in records})
    short = all(len(t["team"]) <= 4 for t in rows)
    if len(rows) <= max_rows:
        height = 0.2 * len(rows) + 1.6
        fig, ax = _fig(height)
        _calendar_panel(ax, rows, weeks, short)
        note = "Teams sorted by cover rate; record is covers-misses. Cells name the opponent (@ = away)." if short else \
            "Teams sorted by cover rate; record is covers-misses."
    else:
        top, bottom = rows[:split], rows[-split:]
        height = 0.2 * split + 1.8
        fig, (a1, a2) = _fig(height, 2)
        _calendar_panel(a1, top, weeks, short)
        _calendar_panel(a2, bottom, weeks, short)
        a1.set_title(f"Best {split} cover rates", loc="left", fontsize=8.5, color=INK, pad=16)
        a2.set_title(f"Worst {split} cover rates", loc="left", fontsize=8.5, color=INK, pad=16)
        note = (f"The {split} best and worst of {len(rows)} teams by cover rate; record is covers-misses.")
    sub = f"Green: covered the closing spread; red: did not; grey: push; blank: no game. {note}"
    if extra:
        sub += f"\n{extra}"
    return _finish(fig, path, f"Every {sport} team against the spread, week by week", sub, height)


def team_quadrant_chart(records: list[dict], path: Path, sport: str, min_games: int = 3, n_labels: int | None = None) -> bool:
    rows = team_rows(records, min_games)
    if len(rows) < 4:
        return False
    height = 6.4
    fig, ax = _fig(height)
    _axes(ax)
    xs = [t["cover_rate"] for t in rows]
    ys = [t["over_rate"] for t in rows]
    ax.axvline(0.5, color=INK_2, linewidth=0.8)
    ax.axhline(0.5, color=INK_2, linewidth=0.8)
    # Teams on the same spot are drawn as one bigger dot with one label.
    spot_of: dict = defaultdict(list)
    for t in rows:
        spot_of[(round(t["cover_rate"], 3), round(t["over_rate"], 3))].append(t)
    spots = list(spot_of.items())
    ax.scatter([k[0] for k, _ in spots], [k[1] for k, _ in spots], s=[24 + 16 * (len(v) - 1) for _, v in spots],
               color=SERIES[0], alpha=0.8, edgecolor=SURFACE, linewidth=0.5, zorder=3)
    ax.set_xlim(-0.05, 1.05)
    ax.set_ylim(-0.05, 1.05)
    pct = plt.FuncFormatter(lambda v, _: f"{v:.0%}")
    ax.xaxis.set_major_formatter(pct)
    ax.yaxis.set_major_formatter(pct)
    ax.set_xlabel("Covered the spread (pushes left out)", color=INK_2, fontsize=8)
    ax.set_ylabel("Their games went over the total", color=INK_2, fontsize=8)
    for x, y, text, ha in ((1.03, 1.03, "Covers, and shootouts", "right"), (-0.03, 1.03, "Misses, and shootouts", "left"),
                           (1.03, -0.03, "Covers, and grinders", "right"), (-0.03, -0.03, "Misses, and grinders", "left")):
        ax.text(x, y, text, ha=ha, va="top" if y > 0.5 else "bottom", fontsize=8, color=INK_2, fontweight="bold")
    sub = (f"{len(rows)} teams with {min_games}+ games. Each dot is a team: right of centre it covers more often than not; "
           "above centre its games go over more often.\nSmall samples swing hard: one game moves a team several points.")
    fig.tight_layout(rect=(0, 0, 1, 1 - 0.85 / height))
    picked = rows if n_labels is None or len(rows) <= n_labels else _pick_labels(rows, "cover_rate", "over_rate", n_labels)
    keep = {id(t) for t in picked}
    labels = [(k[0], k[1], ", ".join(sorted(t["team"] for t in v))) for k, v in spots if any(id(t) in keep for t in v)]
    _place_labels(ax, labels, [k for k, _ in spots])
    return _finish(fig, path, f"{sport} teams: who covers, and whose games go over", sub, height)


# ── 8. one bet, three outcomes ───────────────────────────────────────────────
OUTCOMES = (("Won and covered", GOOD), ("Won, did not cover", SERIES[3]), ("Won, pushed", NEUTRAL), ("Lost", BAD))


def outcome_rows(records: list[dict], bands) -> list[dict]:
    """Per spread band: how the moneyline favorite's game ended against both bets."""
    out = []
    for label, rule in bands:
        rs = [r for r in records if rule(abs(float(r["g"]["home_spread"])))]
        counts = {"Won and covered": sum(r["won"] and r["cover"] > 0 for r in rs),
                  "Won, did not cover": sum(r["won"] and r["cover"] < 0 for r in rs),
                  "Won, pushed": sum(r["won"] and r["cover"] == 0 for r in rs),
                  "Lost": sum(not r["won"] for r in rs)}
        out.append({"band": label, "n": len(rs), **counts})
    return out


def outcomes_chart(records: list[dict], bands, path: Path, sport: str) -> bool:
    rows = [r for r in outcome_rows(records, bands) if r["n"]]
    if not rows:
        return False
    height = 0.5 * len(rows) + 2.2
    fig, ax = _fig(height)
    _axes(ax, ygrid=False)
    ys = list(range(len(rows)))[::-1]
    for y, r in zip(ys, rows):
        left = 0.0
        for name, colour in OUTCOMES:
            share = r[name] / r["n"]
            ax.barh(y, share, left=left, color=colour, height=0.62, linewidth=0)
            if share >= 0.07:
                ax.text(left + share / 2, y, f"{share:.0%}", ha="center", va="center", fontsize=7.5, color="white",
                        fontweight="bold")
            left += share
    ax.set_yticks(ys, [f"{r['band']} ({r['n']})" for r in rows], fontsize=8, color=INK)
    ax.set_xlim(0, 1)
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}"))
    x = 0.0
    for name, colour in OUTCOMES:
        ax.text(x, 1.0 + 0.4 / height, f"■ {name}", color=colour, fontsize=7.5, transform=ax.transAxes)
        x += 0.25
    sub = (f"{_season_note(records)}, by the size of the closing spread (games in brackets). The moneyline favorite pays "
           "whenever it wins;\nthe spread bet pays only when it wins by more than the line. Gold is the gap between the two.")
    return _finish(fig, path, f"{sport} favorites: winning is not covering", sub, height + 0.3)


# ── 9. decided late ──────────────────────────────────────────────────────────
def late_flip(game: dict) -> dict | None:
    """Spread and total status with five minutes left against the final.

    ``game``: home, away, week, home_spread, vegas_total, home_score, away_score
    (final, overtime included) and ``late_home``/``late_away``, the score with
    5:00 left in regulation. The spread favorite is the side giving points; a
    pick'em has none and is skipped for the spread.
    """
    hs = game.get("home_spread")
    if hs is None or game.get("late_home") is None:
        return None
    out = {"label": f"{game['away']} @ {game['home']}", "week": game["week"]}
    if hs != 0:
        home_fav = hs < 0
        given = abs(float(hs))
        sign = 1 if home_fav else -1
        late = sign * (game["late_home"] - game["late_away"]) - given
        final = sign * (game["home_score"] - game["away_score"]) - given
        out.update(fav=game["home"] if home_fav else game["away"], given=given, spread_late=late, spread_final=final,
                   spread_flip=(late > 0) != (final > 0) and late != 0 and final != 0)
    total = game.get("vegas_total")
    if total is not None:
        late_t = game["late_home"] + game["late_away"] - float(total)
        final_t = game["home_score"] + game["away_score"] - float(total)
        # Points only go up, so an over at 5:00 stays an over; the flip runs under -> over.
        out.update(total_late=late_t, total_final=final_t, total_flip=late_t < 0 < final_t)
    return out


def late_flips_chart(games: list[dict], path: Path, sport: str, n: int = 14) -> bool:
    rows = [x for x in (late_flip(g) for g in games) if x]
    spread = [x for x in rows if "spread_flip" in x]
    flips = [x for x in spread if x["spread_flip"]]
    if not spread:
        return False
    flips.sort(key=lambda x: -abs(x["spread_final"] - x["spread_late"]))
    shown = flips[:n]
    totals = [x for x in rows if "total_flip" in x]
    t_flips = sum(x["total_flip"] for x in totals)
    height = 0.36 * max(len(shown), 3) + 2.4
    fig, ax = _fig(height)
    _axes(ax, ygrid=False)
    ax.axvline(0, color=INK_2, linewidth=0.8)
    ys = list(range(len(shown)))[::-1]
    for y, x in zip(ys, shown):
        colour = GOOD if x["spread_final"] > 0 else BAD
        ax.annotate("", xy=(x["spread_final"], y), xytext=(x["spread_late"], y),
                    arrowprops={"arrowstyle": "-|>", "color": colour, "lw": 1.8, "shrinkA": 0, "shrinkB": 0})
        ax.scatter([x["spread_late"]], [y], s=18, color=INK_2, zorder=3)
        ax.text(max(x["spread_final"], x["spread_late"]) + 0.6, y,
                f"{x['fav']} -{x['given']:g}: {x['spread_late']:+g} → {x['spread_final']:+g}", fontsize=6.5,
                color=INK, va="center")
    lim = max([abs(v) for x in shown for v in (x["spread_late"], x["spread_final"])] + [7]) + 1
    ax.set_xlim(-lim, lim * 1.55)
    ax.set_yticks(ys, [f"Wk {x['week']}  {x['label']}" for x in shown], fontsize=7, color=INK)
    ax.set_xlabel("Favorite's margin minus the spread: right of 0 it was covering", color=INK_2, fontsize=7.5)
    if not shown:
        ax.text(0.5, 0.5, "no spread result changed in the last five minutes", transform=ax.transAxes,
                ha="center", fontsize=8, color=INK_2)
    to_cover = sum(x["spread_final"] > 0 for x in flips)
    sub = (f"{len(flips)} of {len(spread)} games ({len(flips) / len(spread):.0%}) changed spread result after 5:00 left in "
           f"the fourth quarter: {to_cover} late covers, {len(flips) - to_cover} late misses. "
           f"{t_flips} of {len(totals)} totals went from under to over.\n"
           "Grey dot: where the favorite stood against the line with 5:00 left; arrow: the final (overtime included). "
           f"{'Largest swings shown.' if len(flips) > n else ''}")
    return _finish(fig, path, f"Decided late: {sport} spreads that changed in the last five minutes", sub, height)
