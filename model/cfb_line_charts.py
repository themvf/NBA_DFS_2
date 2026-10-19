"""Line-versus-result charts for the CFB pattern-watch page and Substack posts.

Descriptive only: what happened this season, with the sample sizes shown. None
of these charts is a betting signal, and none claims a book or a side can be
beaten.

  spread_vs_result   each game's closing spread (favorite's side) against the
                     favorite's final margin, one week or the whole season
  total_vs_result    closing total against the combined points
  cumulative_upsets  running count of outright upsets vs the number the
                     closing moneylines predicted
  calibration        favorites' vig-free win probability, bucketed, against how
                     often they won: 2023-25 pooled and 2026 to date
  line_move_hist     how far the consensus spread moved, open to close
  book_outliers      how often each sportsbook's closing spread sat at the top
                     or bottom of the market (ties shared, exchanges excluded)

    python -m model.cfb_line_charts [--out-dir artifacts/cfb_pattern_watch/charts/lines]
"""
from __future__ import annotations

import argparse
import math
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

from model import cfb_pattern_watch as pw  # noqa: E402
from model.cfb_pattern_charts import GRID, INK, INK_2, SERIES, SURFACE, _figure, _save, _style  # noqa: E402

DEFAULT_DIR = Path("artifacts") / "cfb_pattern_watch" / "charts" / "lines"
GREEN, RED, GREY = "#1baf7a", "#e34948", "#bdbcb8"
CAL_BUCKETS = ((0.5, 0.6), (0.6, 0.7), (0.7, 0.8), (0.8, 0.9), (0.9, 1.0001))
MOVE_BIN = 0.5
MOVE_CAP = 7.0             # moves of 7+ points share the last bar
MIN_BOOK_GAMES = 30
PARTIAL_WEEK_SHARE = 0.5   # a latest week with under half the previous week's games is still in progress


# ── pure helpers ─────────────────────────────────────────────────────────────
def _short(name: str | None) -> str:
    n = (name or "?").replace(" State", " St.").replace("University", "U")
    return n if len(n) <= 16 else n[:15] + "."


def _fbs_regular(g: dict) -> bool:
    return (g.get("hclass") == "fbs" and g.get("aclass") == "fbs"
            and (g.get("season_type") or "regular") == "regular"
            and g.get("home_score") is not None and g.get("away_score") is not None)


def spread_points(games: list[dict], week: int | None = None) -> list[dict]:
    """FBS-vs-FBS games with a closing spread: points the favorite gave and its final margin.

    The favorite is the side giving points; a pick'em uses the shorter moneyline,
    and a pick'em with no moneyline is left out (it has no favorite).
    """
    out = []
    for g in games:
        if not _fbs_regular(g) or g.get("home_spread") is None:
            continue
        if week is not None and int(g["week"]) != week:
            continue
        sp = float(g["home_spread"])
        if sp != 0:
            home_fav = sp < 0
        elif g.get("home_ml") is not None and g.get("away_ml") is not None and g["home_ml"] != g["away_ml"]:
            home_fav = pw.decimal_odds(g["home_ml"]) < pw.decimal_odds(g["away_ml"])
        else:
            continue
        margin = g["home_score"] - g["away_score"]
        fav_margin = margin if home_fav else -margin
        line = abs(sp)
        fav, dog = (g["hname"], g["aname"]) if home_fav else (g["aname"], g["hname"])
        out.append({"week": int(g["week"]), "line": line, "margin": fav_margin,
                    "result": (fav_margin > line) - (fav_margin < line), "miss": fav_margin - line,
                    "fav": fav, "dog": dog})
    return out


def total_points(games: list[dict], week: int | None = None) -> list[dict]:
    """FBS-vs-FBS games with a closing total: the total and the combined points."""
    out = []
    for g in games:
        if not _fbs_regular(g) or g.get("vegas_total") is None:
            continue
        if week is not None and int(g["week"]) != week:
            continue
        line = float(g["vegas_total"])
        pts = g["home_score"] + g["away_score"]
        out.append({"week": int(g["week"]), "line": line, "points": pts,
                    "result": (pts > line) - (pts < line), "miss": pts - line,
                    "home": g["hname"], "away": g["aname"]})
    return out


def latest_completed_week(games: list[dict]) -> int | None:
    """The latest week with games, unless it is a partial week (Thursday games of the next week)."""
    counts: dict[int, int] = defaultdict(int)
    for g in games:
        if _fbs_regular(g):
            counts[int(g["week"])] += 1
    if not counts:
        return None
    weeks = sorted(counts)
    last = weeks[-1]
    if len(weeks) > 1 and counts[last] < PARTIAL_WEEK_SHARE * counts[weeks[-2]]:
        return weeks[-2]
    return last


def upsets_by_week(records: list[dict]) -> list[dict]:
    """Per week: actual underdog wins and the number the closing prices predicted.

    `records` are `cfb_pattern_measures._ml_eligible` records. Expected upsets are
    the sum of every underdog's vig-free win probability. (Same rule as
    cfb_week_story.upsets_by_week.)
    """
    weeks: dict[int, list[dict]] = {}
    for r in records:
        weeks.setdefault(int(r["g"]["week"]), []).append(r)
    return [{"week": w, "games": len(rs), "upsets": sum(1 for r in rs if not r["won"]),
             "expected": sum(1 - r["imp"] for r in rs)} for w, rs in sorted(weeks.items())]


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return (0.0, 1.0)
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (c - h, c + h)


def calibration_buckets(records: list[dict], buckets=CAL_BUCKETS) -> list[dict]:
    """Favorites grouped by vig-free implied win probability: games, mean implied, win rate, 95% interval."""
    out = []
    for lo, hi in buckets:
        rs = [r for r in records if lo <= r["imp"] < hi]
        n = len(rs)
        wins = sum(1 for r in rs if r["won"])
        lo_ci, hi_ci = wilson(wins, n)
        out.append({"label": f"{lo * 100:.0f}-{min(hi, 1.0) * 100:.0f}%", "lo": lo, "hi": min(hi, 1.0), "games": n,
                    "wins": wins, "implied": statistics.mean(r["imp"] for r in rs) if rs else None,
                    "won": wins / n if n else None, "ci": (lo_ci, hi_ci) if n else None})
    return out


def move_histogram(line_moves: list[dict], width: float = MOVE_BIN, cap: float = MOVE_CAP) -> dict:
    """Absolute open-to-close spread moves in bins of `width`, the last bin holding `cap`+."""
    moves = [abs(r["Spread move (pts)"]) for r in line_moves
             if r.get("Spread move (pts)") is not None and r.get("Open hours before kickoff") is not None]
    nbins = int(round(cap / width)) + 1
    counts = [0] * nbins
    for m in moves:
        i = min(int(m / width + 1e-9), nbins - 1)
        counts[i] += 1
    return {"moves": moves, "counts": counts, "width": width, "cap": cap,
            "median": statistics.median(moves) if moves else None,
            "share_2plus": (sum(1 for m in moves if m >= 2.0) / len(moves)) if moves else None,
            "unmoved": sum(1 for m in moves if m == 0)}


def book_outliers(games_quotes: list[list[dict]], min_games: int = MIN_BOOK_GAMES) -> dict:
    """How often each sportsbook's closing spread was the market's most or fewest points on the favorite.

    `games_quotes` holds one list of quotes per game, each `{"book", "spread"}`
    with `spread` the home spread (exchanges must already be removed; any key in
    `cfb_book_disagreement.EXCHANGES` is dropped again here). The favorite is the
    side the median quote favors; a game whose median is a pick'em is counted as
    quoted but credits nobody. When k books tie at an end, each gets 1/k. A game
    where every book agrees credits nobody either.
    """
    from model.cfb_book_disagreement import EXCHANGES

    quoted: dict[str, int] = defaultdict(int)
    most: dict[str, float] = defaultdict(float)
    fewest: dict[str, float] = defaultdict(float)
    for quotes in games_quotes:
        qs = [(q["book"], float(q["spread"])) for q in quotes
              if q.get("spread") is not None and str(q.get("book", "")).lower() not in EXCHANGES]
        # One quote per book per game.
        seen: dict[str, float] = {}
        for b, s in qs:
            seen.setdefault(b, s)
        if len(seen) < 2:
            continue
        for b in seen:
            quoted[b] += 1
        med = statistics.median(seen.values())
        if med == 0:
            continue
        # Points the median favorite lays at each book (home favored when the median home spread is negative).
        lays = {b: (-s if med < 0 else s) for b, s in seen.items()}
        hi, lo = max(lays.values()), min(lays.values())
        if hi == lo:
            continue
        top = [b for b, v in lays.items() if v == hi]
        bot = [b for b, v in lays.items() if v == lo]
        for b in top:
            most[b] += 1 / len(top)
        for b in bot:
            fewest[b] += 1 / len(bot)
    rows, dropped = [], []
    for b, n in quoted.items():
        if n < min_games:
            dropped.append(b)
            continue
        rows.append({"book": b, "games": n, "most": most[b], "fewest": fewest[b],
                     "most_share": most[b] / n, "fewest_share": fewest[b] / n,
                     "either_share": (most[b] + fewest[b]) / n})
    rows.sort(key=lambda r: -r["either_share"])
    return {"rows": rows, "dropped": sorted(dropped), "games": len(games_quotes)}


# ── chart helpers ────────────────────────────────────────────────────────────
LABEL_PT = 6.8


def _crosses(a, b) -> bool:
    """Whether segments a and b properly intersect."""
    def orient(p, q, r):
        return (q[0] - p[0]) * (r[1] - p[1]) - (q[1] - p[1]) * (r[0] - p[0])
    (p1, p2), (p3, p4) = a, b
    d1, d2 = orient(p3, p4, p1), orient(p3, p4, p2)
    d3, d4 = orient(p1, p2, p3), orient(p1, p2, p4)
    return d1 * d2 < 0 and d3 * d4 < 0


def _place_labels(fig, ax, all_xy: list[tuple[float, float]], targets: list[tuple[float, float, str]],
                  avoid=()) -> None:
    """Label `targets` where each label covers the fewest data points and no earlier label.

    Greedy, in display pixels: each label tries a ring of offsets around its
    point and keeps the one with the lowest cost (points under the box, overlap
    with placed labels, leaving the axes). Long offsets get a leader line.
    """
    fig.tight_layout()
    to_px = ax.transData.transform
    pts = [tuple(to_px(xy)) for xy in all_xy]
    bbox = ax.get_window_extent()
    px_per_pt = fig.dpi / 72
    h = LABEL_PT * 1.35 * px_per_pt
    renderer = fig.canvas.get_renderer()
    placed: list[tuple[float, float, float, float]] = []
    leaders: list[tuple[tuple[float, float], tuple[float, float]]] = []
    for artist in avoid:   # e.g. the corner notes: treated as labels already placed
        e = artist.get_window_extent(renderer)
        placed.append((e.x0, e.y0, e.x1, e.y1))
    # Beside the point first (no leader), then further out on a ring (with a leader line).
    rings = [(8.0, 0.0), (-8.0, 0.0)] + [(r * math.cos(a), r * math.sin(a)) for r in (28, 44, 60)
                                          for a in [math.radians(d) for d in (0, 180, 35, 145, -35, -145, 65, 115,
                                                                               -65, -115)]]
    for x, y, text in targets:
        w = len(text) * LABEL_PT * 0.56 * px_per_pt
        px, py = to_px((x, y))
        best = None
        for i, (dx, dy) in enumerate(rings):
            left = dx >= 0
            x0 = px + dx * px_per_pt if left else px + dx * px_per_pt - w
            y0 = py + dy * px_per_pt - h / 2
            box = (x0, y0, x0 + w, y0 + h)
            cost = sum(1 for qx, qy in pts if box[0] - 3 <= qx <= box[2] + 3 and box[1] - 3 <= qy <= box[3] + 3)
            cost += 50 * sum(1 for b in placed if not (box[2] < b[0] or box[0] > b[2] or box[3] < b[1] or box[1] > b[3]))
            if box[0] < bbox.x0 or box[2] > bbox.x1 or box[1] < bbox.y0 or box[3] > bbox.y1:
                cost += 100
            anchor = (box[0] if left else box[2], (box[1] + box[3]) / 2)
            if math.hypot(dx, dy) > 10:
                cost += 50 * sum(1 for seg in leaders if _crosses(seg, ((px, py), anchor)))
            cost += i * 0.01   # prefer close offsets
            if best is None or cost < best[0]:
                best = (cost, box, dx, dy, left, anchor)
        _, box, dx, dy, left, anchor = best
        placed.append(box)
        leaders.append(((px, py), anchor))
        far = math.hypot(dx, dy) > 10
        ax.annotate(text, (x, y), xytext=(dx, dy), textcoords="offset points", fontsize=LABEL_PT, color=INK,
                    ha="left" if left else "right", va="center",
                    arrowprops={"arrowstyle": "-", "color": INK_2, "linewidth": 0.7, "shrinkA": 1, "shrinkB": 2}
                    if far else None, zorder=4)


# ── charts ───────────────────────────────────────────────────────────────────
def _line_scatter(pts: list[dict], y_key: str, classes, week: int | None, labeler, xlabel: str, ylabel: str,
                  above: str, below: str, title: str, subtitle: str, path: Path, zero_line: bool) -> None:
    """Shared layout for the line-vs-result scatters: diagonal, colored classes, labelled misses, legend below."""
    fig, ax = _figure(5.6 if week is None else 5.2)
    xs = [p["line"] for p in pts]
    ys = [p[y_key] for p in pts]
    lo = min(min(xs), min(ys)) - 4
    hi = max(max(xs), max(ys)) + 4
    if zero_line:
        ax.axhline(0, color=GRID, linewidth=1, zorder=0)
    x_lo, x_hi = (min(xs) - 1.5, max(xs) + 2.5)
    ax.plot([x_lo, x_hi], [x_lo, x_hi], color=INK_2, linewidth=1, linestyle="--", zorder=1)
    ax.set_xlim(x_lo, x_hi)
    ax.set_ylim(lo, hi)
    size = 14 if week is None else 34
    for res, color, label in classes:
        sel = [p for p in pts if p["result"] == res]
        if sel:
            ax.scatter([p["line"] for p in sel], [p[y_key] for p in sel], s=size, color=color, alpha=0.9,
                       edgecolor=SURFACE, linewidth=0.6, zorder=3, label=f"{label} ({len(sel)})")
    # Say which side of the dashed line is which, in the plot's two open corners.
    notes = [ax.text(0.015, 0.97, above, transform=ax.transAxes, fontsize=7, color=INK_2, ha="left", va="top")]
    notes += [ax.text(0.985, 0.03, below, transform=ax.transAxes, fontsize=7, color=INK_2, ha="right", va="bottom")]
    ax.set_xlabel(xlabel, color=INK_2, fontsize=8)
    ax.set_ylabel(ylabel, color=INK_2, fontsize=8)
    ax.grid(axis="x", color=GRID, linewidth=0.6)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.11), ncol=3, fontsize=7.5, frameon=False, labelcolor=INK)
    _style(ax, title, subtitle)
    n_label = 4 if week is None else 6
    misses = sorted(pts, key=lambda p: -abs(p["miss"]))[:n_label]
    _place_labels(ax.figure, ax, list(zip(xs, ys)), [(p["line"], p[y_key], labeler(p)) for p in misses],
                  avoid=notes)
    _save(fig, path)


def spread_vs_result_chart(games: list[dict], week: int | None, path: Path) -> bool:
    pts = spread_points(games, week)
    if not pts:
        return False
    covered = sum(1 for p in pts if p["result"] == 1)
    decided = sum(1 for p in pts if p["result"] != 0)
    scope = "season to date" if week is None else f"Week {week}"
    title = "Favorites against the spread, season to date" if week is None else f"Week {week}: who beat the number?"
    _line_scatter(
        pts, "margin",
        ((1, GREEN, "Favorite covered"), (-1, RED, "Favorite did not cover"), (0, GREY, "Landed on the number")),
        week, lambda p: f"{_short(p['fav'])} vs {_short(p['dog'])} ({p['margin']:+d})",
        "Points the favorite was giving at the close", "Favorite's final margin",
        "Above the dashed line: favorite covered", "Below: favorite fell short",
        title, f"FBS vs FBS, {scope}: {len(pts)} games. Favorites covered {covered} of {decided} "
               f"({100 * covered / decided:.0f}%, pushes out).",
        path, zero_line=True)
    return True


def total_vs_result_chart(games: list[dict], week: int | None, path: Path) -> bool:
    pts = total_points(games, week)
    if not pts:
        return False
    overs = sum(1 for p in pts if p["result"] == 1)
    decided = sum(1 for p in pts if p["result"] != 0)
    avg_miss = statistics.mean(p["miss"] for p in pts)
    scope = "season to date" if week is None else f"Week {week}"
    title = "Totals against the final score, season to date" if week is None else f"Week {week}: overs and unders"
    _line_scatter(
        pts, "points",
        ((1, SERIES[0], "Over"), (-1, SERIES[1], "Under"), (0, GREY, "Landed on the number")),
        week, lambda p: f"{_short(p['away'])} @ {_short(p['home'])} ({p['points']} vs {p['line']:g})",
        "Closing total", "Points scored, both teams",
        "Above the dashed line: over", "Below: under",
        title, f"FBS vs FBS, {scope}, {len(pts)} games: {overs} of {decided} over "
               f"({100 * overs / decided:.0f}%); scoring {avg_miss:+.1f} vs the total.",
        path, zero_line=False)
    return True


def cumulative_upsets_chart(records: list[dict], path: Path) -> bool:
    """`records` is the output of upsets_by_week."""
    if not records:
        return False
    xs = [r["week"] for r in records]
    actual, expected = [], []
    a = e = 0.0
    for r in records:
        a += r["upsets"]
        e += r["expected"]
        actual.append(a)
        expected.append(e)
    fig, ax = _figure(4.0)
    ax.fill_between(xs, actual, expected, color=SERIES[1], alpha=0.12, linewidth=0)
    ax.plot(xs, expected, color=INK_2, linewidth=2, linestyle="--", marker="o", markersize=4,
            label="Expected from the closing moneylines")
    ax.plot(xs, actual, color=SERIES[1], linewidth=2, marker="o", markersize=4, label="Actual upsets")
    ax.annotate(f"{actual[-1]:.0f} actual", (xs[-1], actual[-1]), xytext=(6, -2 if actual[-1] < expected[-1] else 4),
                textcoords="offset points", fontsize=7.5, color=INK, va="top" if actual[-1] < expected[-1] else "bottom")
    ax.annotate(f"{expected[-1]:.1f} expected", (xs[-1], expected[-1]),
                xytext=(6, 4 if actual[-1] < expected[-1] else -2), textcoords="offset points", fontsize=7.5,
                color=INK, va="bottom" if actual[-1] < expected[-1] else "top")
    ax.set_xticks(xs, [f"Wk {x}" for x in xs])
    ax.set_xlim(min(xs) - 0.3, max(xs) + 1.4)
    ax.set_ylim(0, max(max(actual), max(expected)) * 1.12)
    ax.set_ylabel("upsets, season to date", color=INK_2, fontsize=8)
    ax.legend(loc="upper left", fontsize=7.5, frameon=False, labelcolor=INK)
    games = sum(r["games"] for r in records)
    gap = actual[-1] - expected[-1]
    _style(ax, "Fewer upsets than the odds expected" if gap < 0 else "More upsets than the odds expected",
           f"FBS vs FBS regular season, {games} games with a closing moneyline, season to date. "
           f"Gap: {gap:+.1f} upsets.")
    _save(fig, path)
    return True


def calibration_chart(prior_games: list[dict], current_games: list[dict], path: Path,
                      prior_label: str = "2023-25", current_label: str = f"{pw.SEASON} to date") -> bool:
    from model.cfb_pattern_measures import _ml_eligible

    prior = calibration_buckets(_ml_eligible(prior_games)) if prior_games else []
    cur = calibration_buckets(_ml_eligible(current_games)) if current_games else []
    if not any(b["games"] for b in prior + cur):
        return False
    fig, ax = _figure(5.0)
    ax.plot([0.5, 1.0], [0.5, 1.0], color=INK_2, linewidth=1, linestyle="--", zorder=1,
            label="Honest odds: won exactly as often as implied")
    ax.set_xlim(0.5, 1.0)
    ax.set_ylim(0.4, 1.02)
    for buckets, color, label, off, side in ((prior, SERIES[0], prior_label, -0.004, -1),
                                             (cur, SERIES[1], current_label, 0.004, 1)):
        pts = [b for b in buckets if b["games"]]
        if not pts:
            continue
        n = sum(b["games"] for b in pts)
        x = [b["implied"] + off for b in pts]
        y = [b["won"] for b in pts]
        ax.vlines(x, [b["ci"][0] for b in pts], [min(b["ci"][1], 1.02) for b in pts], color=color, alpha=0.3,
                  linewidth=2.2, zorder=2)
        ax.plot(x, y, color=color, linewidth=2, marker="o", markersize=5, zorder=3, label=f"{label} ({n} games)")
        for xi, yi in zip(x, y):
            ax.annotate(f"{100 * yi:.0f}%", (xi, yi), xytext=(8 * side, 0), textcoords="offset points",
                        fontsize=7, color=INK, ha="left" if side > 0 else "right", va="center", zorder=5,
                        bbox={"boxstyle": "square,pad=0.15", "facecolor": SURFACE, "edgecolor": "none"})
    # Band labels on the axis carry each band's game count for both series.
    centers, labels = [], []
    for i, (lo, hi) in enumerate(CAL_BUCKETS):
        pn = prior[i]["games"] if prior else 0
        cn = cur[i]["games"] if cur else 0
        centers.append((lo + min(hi, 1.0)) / 2)
        labels.append(f"{lo * 100:.0f}-{min(hi, 1.0) * 100:.0f}%\n{pn} | {cn} games")
    ax.set_xticks(centers, labels, fontsize=7)
    ax.grid(axis="x", visible=False)
    for lo, _hi in CAL_BUCKETS[1:]:
        ax.axvline(lo, color=GRID, linewidth=0.6, zorder=0)
    yt = [0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    ax.set_yticks(yt, [f"{t * 100:.0f}%" for t in yt])
    ax.set_xlabel(f"Favorite's win chance implied by the closing moneyline, margin removed "
                  f"(games: {prior_label} | {current_label})", color=INK_2, fontsize=8)
    ax.set_ylabel("How often the favorite won", color=INK_2, fontsize=8)
    ax.legend(loc="upper left", fontsize=7.5, frameon=False, labelcolor=INK)
    _style(ax, "Are the odds honest?",
           "FBS vs FBS regular-season favorites in 10-point bands. Shaded bars: 95% range for each band.")
    _save(fig, path)
    return True


def line_move_histogram_chart(line_moves: list[dict], path: Path) -> bool:
    h = move_histogram(line_moves)
    if not h["moves"]:
        return False
    fig, ax = _figure(3.9)
    w = h["width"]
    idx = list(range(len(h["counts"])))
    starts = [i * w for i in idx]
    colors = [SERIES[0] if s < 2.0 else SERIES[1] for s in starts]
    ax.bar(idx, h["counts"], width=0.82, color=colors)
    for i, c in zip(idx, h["counts"]):
        if c:
            ax.annotate(str(c), (i, c), xytext=(0, 2), textcoords="offset points", ha="center",
                        fontsize=6.5, color=INK_2)
    labels = [f"{s:g}" for s in starts]
    labels[-1] = f"{h['cap']:g}+"
    ax.set_xticks(idx, labels, fontsize=7)
    ax.set_xlim(-0.6, len(idx) - 0.4)
    ax.set_ylim(0, max(h["counts"]) * 1.12)
    ax.set_xlabel("How far the consensus spread moved, open to close (points either way, half-point steps)",
                  color=INK_2, fontsize=8)
    ax.set_ylabel("games", color=INK_2, fontsize=8)
    ax.grid(axis="y", color=GRID, linewidth=0.6)
    _style(ax, "Most lines barely move",
           f"{len(h['moves'])} games with an open 12h+ out. Median move "
           f"{h['median']:g} pts; {h['unmoved']} never moved; {100 * h['share_2plus']:.0f}% moved 2+.")
    _save(fig, path)
    return True


def book_outlier_chart(outliers: dict, path: Path) -> bool:
    """`outliers` is the output of book_outliers."""
    rows = outliers.get("rows") or []
    if not rows:
        return False
    rows = rows[::-1]
    fig, ax = _figure(max(3.4, 0.36 * len(rows) + 1.9))
    ys = list(range(len(rows)))
    most = [100 * r["most_share"] for r in rows]
    fewest = [100 * r["fewest_share"] for r in rows]
    ax.barh(ys, most, color=SERIES[0], height=0.6, edgecolor=SURFACE, linewidth=1, label="Most points on the favorite")
    ax.barh(ys, fewest, left=most, color=SERIES[1], height=0.6, edgecolor=SURFACE, linewidth=1, label="Fewest points on the favorite")
    for y, m, f, r in zip(ys, most, fewest, rows):
        ax.text(m + f + 0.8, y, f"{m + f:.0f}%  ({r['games']} games)", va="center", fontsize=7, color=INK)
    ax.set_yticks(ys, [r["book"] for r in rows], fontsize=7.5, color=INK)
    ax.set_xlim(0, max(m + f for m, f in zip(most, fewest)) * 1.3)
    dropped = outliers.get("dropped") or []
    left_out = (f"; {len(dropped)} quoting under {MIN_BOOK_GAMES} games left out" if dropped
                else f"; books quoting under {MIN_BOOK_GAMES} games would be left out")
    ax.set_xlabel(f"share of the games each book quoted, %{left_out}", color=INK_2, fontsize=8)
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", color=GRID, linewidth=0.6)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.11), ncol=2, fontsize=7.5, frameon=False, labelcolor=INK)
    _style(ax, "Which books sit at the edge of the market?",
           "Closing spreads before kickoff, exchanges out, ties shared. Describes pricing, not value.")
    _save(fig, path)
    return True


# ── loader ───────────────────────────────────────────────────────────────────
def build_charts(db, out_dir: Path) -> dict[str, bool]:
    """Load each input once and write every chart into `out_dir`."""
    from model import cfb_book_disagreement as books
    from model import cfb_pattern_history as history
    from model import cfb_pattern_measures as measures

    out = Path(out_dir)
    games = measures.load_base_rate_games(db)
    made: dict[str, bool] = {}
    week = latest_completed_week(games)
    if week is not None:
        made[f"spread_vs_result_week_{week}.png"] = spread_vs_result_chart(games, week, out / f"spread_vs_result_week_{week}.png")
        made[f"total_vs_result_week_{week}.png"] = total_vs_result_chart(games, week, out / f"total_vs_result_week_{week}.png")
    made["spread_vs_result_season.png"] = spread_vs_result_chart(games, None, out / "spread_vs_result_season.png")
    made["total_vs_result_season.png"] = total_vs_result_chart(games, None, out / "total_vs_result_season.png")
    made["cumulative_upsets.png"] = cumulative_upsets_chart(upsets_by_week(measures._ml_eligible(games)),
                                                            out / "cumulative_upsets.png")
    made["calibration.png"] = calibration_chart(history.load_prior_games(db), games, out / "calibration.png")

    # One capture read serves both the line moves and the per-book outliers.
    by_id = {g["matchup_id"]: dict(g) for g in db.execute(books._GAMES_SQL, (books.SEASON,))}
    captures = [dict(c) for c in db.execute(books._CAPTURES_SQL, (books.SEASON,))]
    made["line_move_histogram.png"] = line_move_histogram_chart(books.line_moves(captures, by_id),
                                                                out / "line_move_histogram.png")
    last: dict[int, dict] = {}
    for c in captures:   # ordered by matchup, captured_at: the last row per game carries the books
        last[c["matchup_id"]] = c
    quotes = [books.book_quotes(c.get("books")) for c in last.values()]
    made["book_outliers.png"] = book_outlier_chart(book_outliers(quotes), out / "book_outliers.png")
    return made


def main(argv: list[str] | None = None) -> int:
    from config import load_config
    from db.database import DatabaseManager

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    args = ap.parse_args(argv)
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    out = Path(args.out_dir)
    for name, ok in build_charts(db, out).items():
        print(f"{out / name}: {'written' if ok else 'skipped, no data'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
