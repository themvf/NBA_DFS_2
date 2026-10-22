"""Reader charts: DraftKings player-prop lines against each player's own game history.

Descriptive, season to date. Nothing here claims a betting edge.

  nfl_take_prop_line_vs_average.png    each closing line against the player's 2026 average going in
  nfl_take_prop_over_rate_by_gap.png   how often the over hit, by how far the line sat from that average
  nfl_take_prop_line_follows.png       which history the line tracks, and which history predicts the result
  nfl_take_prop_after_big_game.png     how much the next line moved after a player beat or missed his last one
  nfl_take_prop_player_paths.png       game by game: line, result and last season's average for the most-bet players

Sources (SELECT only):

* ``nfl_prop_implied_lines``  closing DraftKings lines (capture_role 'close', book 'draftkings')
* ``ff_player_week_stats``    nflverse weekly stats, 2025 and 2026 (a row exists only for games he played)

History is strictly before the line's week. "Average going in" needs at least ``MIN_PRIOR`` 2026 games.
A listed player with no stat row in the line's week (inactive, or not loaded) has no result and is left
out of every outcome measure, never scored 0. Pushes are left out of over rates.

    python -m model.nfl_prop_history_charts --out-dir DIR
"""

from __future__ import annotations

import argparse
import collections
import json
import statistics as st
from datetime import datetime, timedelta, timezone
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from model.cfb_pattern_charts import DPI, GRID, INK, INK_2, SERIES, SURFACE  # noqa: E402
from model.story_charts import BAD, GOOD, NEUTRAL, WIDTH_IN, _axes, _finish  # noqa: E402

SEASON = 2026
DEFAULT_DIR = Path("artifacts") / "nfl_market_scenarios"
MIN_PRIOR = 2            # 2026 games a player needs before a line for "average going in"
MIN_LAST_SEASON = 4      # 2025 games for "last season's average"
MARKETS = {               # market -> (stat in nflverse weekly row, label)
    "player_pass_yds": ("passing_yards", "Passing yards"),
    "player_rush_yds": ("rushing_yards", "Rushing yards"),
    "player_receptions": ("receptions", "Receptions"),
    "player_reception_yds": ("receiving_yards", "Receiving yards"),
}
MIN_BIN = 5              # a gap group needs this many graded lines to draw a bar
GAP_BINS = ((-9.0, -0.30, "30%+ below"), (-0.30, -0.15, "15-30% below"), (-0.15, 0.0, "0-15% below"),
            (0.0, 0.15, "0-15% above"), (0.15, 9.0, "15%+ above"))


# ── pure computations ────────────────────────────────────────────────────────
def mean(xs):
    xs = [x for x in xs if x is not None]
    return st.mean(xs) if xs else None


def history_features(games: dict[int, float], last_season: list[float], week: int) -> dict:
    """Season average, last-3 average, last game, game count (2026, weeks before ``week``), last season's average."""
    prior = [games[w] for w in sorted(games) if w < week]
    return {"n_prior": len(prior),
            "season_avg": mean(prior) if len(prior) >= MIN_PRIOR else None,
            "last3": mean(prior[-3:]) if len(prior) >= MIN_PRIOR else None,
            "last_game": prior[-1] if prior else None,
            "last_season": mean(last_season) if len(last_season) >= MIN_LAST_SEASON else None}


def result(actual: float | None, line: float) -> str | None:
    if actual is None:
        return None
    return "over" if actual > line else "under" if actual < line else "push"


def gap_bin(line: float, avg: float | None) -> str | None:
    if avg is None or avg <= 0:
        return None
    g = (line - avg) / avg
    for lo, hi, label in GAP_BINS:
        if lo <= g < hi:
            return label
    return None


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return 0.0, 0.0
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5) / d
    return c - h, c + h


def pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 3:
        return None
    mx, my = st.mean(xs), st.mean(ys)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    return sxy / (sxx * syy) ** 0.5 if sxx > 0 and syy > 0 else None


def slope(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 3:
        return None
    mx, my = st.mean(xs), st.mean(ys)
    sxx = sum((x - mx) ** 2 for x in xs)
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx if sxx > 0 else None


def line_changes(rows: list[dict]) -> list[dict]:
    """For a player's consecutive lines in one market: last game minus last line, and this line minus last line."""
    by = collections.defaultdict(list)
    for r in rows:
        by[(r["market"], r["player_key"])].append(r)
    out = []
    for rs in by.values():
        rs.sort(key=lambda r: r["week"])
        for prev, cur in zip(rs, rs[1:]):
            if prev["actual"] is None:
                continue
            out.append({"market": cur["market"], "beat": prev["actual"] - prev["line"], "move": cur["line"] - prev["line"],
                        "gap_weeks": cur["week"] - prev["week"]})
    return out


# ── charts ───────────────────────────────────────────────────────────────────
def _grid(height: float, sharey=False):
    fig, axes = plt.subplots(2, 2, figsize=(WIDTH_IN, height), dpi=DPI, sharey=sharey)
    fig.patch.set_facecolor(SURFACE)
    return fig, axes


def _title(ax, market):
    ax.set_title(MARKETS[market][1], fontsize=9.5, color=INK, loc="left", fontweight="bold")


def line_vs_average_chart(rows: list[dict], path: Path, weeks: str) -> bool:
    pts = [r for r in rows if r["season_avg"] is not None]
    if not pts:
        return False
    height = 8.2
    fig, axes = _grid(height)
    notes = []
    for ax, market in zip(axes.flat, MARKETS):
        ps = [r for r in pts if r["market"] == market]
        _axes(ax)
        if not ps:
            ax.set_visible(False)
            continue
        xs, ys = [r["season_avg"] for r in ps], [r["line"] for r in ps]
        hi = max(xs + ys) * 1.05
        ax.plot([0, hi], [0, hi], color=INK_2, linewidth=0.8, linestyle="--")
        ax.scatter(xs, ys, s=11, color=SERIES[0], alpha=0.55, edgecolor="none")
        ratio = st.median(y / x for x, y in zip(xs, ys) if x > 0)
        below = sum(1 for x, y in zip(xs, ys) if y < x) / len(ps)
        far, seen = [], set()
        for r in sorted(ps, key=lambda r: abs(r["line"] - r["season_avg"]), reverse=True):
            if r["name"] not in seen:
                far.append(r)
                seen.add(r["name"])
            if len(far) == 3:
                break
        for j, r in enumerate(far):
            ax.annotate(f"{r['name']} wk {r['week']}", (r["season_avg"], r["line"]), fontsize=6, color=INK,
                        xytext=(-80 if r["line"] < r["season_avg"] else 8, 30 - 24 * j), textcoords="offset points",
                        arrowprops={"arrowstyle": "-", "color": INK_2, "linewidth": 0.5})
        ax.set_xlim(0, hi)
        ax.set_ylim(0, hi)
        ax.set_xlabel("His 2026 average going in", fontsize=7.5, color=INK_2)
        ax.set_ylabel("Closing line", fontsize=7.5, color=INK_2)
        _title(ax, market)
        ax.text(0.03, 0.96, f"{len(ps)} lines\nline / average: median {ratio:.2f}\nline below average: {below:.0%}",
                transform=ax.transAxes, fontsize=7, color=INK, va="top")
        notes.append(f"{MARKETS[market][1].lower()} {ratio:.2f}")
    fig.tight_layout(h_pad=2.0, w_pad=2.0)
    sub = (f"Every closing DraftKings line, {weeks}, against the player's average in his 2026 games before that week "
           f"(at least {MIN_PRIOR} games). Dashed line: the line\nequals his average; points below it are lines set under "
           f"what he had been doing.\nMedian line as a share of the average: " + ", ".join(notes) + ".\n"
           f"A line is a median and a yardage average is pulled up by big games, so a line somewhat below the "
           f"average is expected.\nNamed: the three players whose line sat furthest from their average.")
    return _finish(fig, path, "Where the book sets a player's line, against what he has averaged", sub, height)


def over_rate_by_gap_chart(rows: list[dict], path: Path, weeks: str) -> bool:
    pts = [r for r in rows if r["season_avg"] is not None and r["result"] in ("over", "under")]
    if not pts:
        return False
    height = 7.8
    fig, axes = _grid(height, sharey=True)
    labels = [b[2] for b in GAP_BINS]
    for ax, market in zip(axes.flat, MARKETS):
        _axes(ax, xgrid=False)
        ps = [r for r in pts if r["market"] == market]
        for i, label in enumerate(labels):
            b = [r for r in ps if r["gap"] == label]
            if len(b) < MIN_BIN:
                ax.text(i, 0.04, f"{len(b)} line{'s' if len(b) != 1 else ''}\ntoo few", ha="center", fontsize=6.3,
                        color=INK_2)
                continue
            k = sum(1 for r in b if r["result"] == "over")
            p = k / len(b)
            lo, hi = wilson(k, len(b))
            ax.bar(i, p, width=0.66, color=GOOD if p >= 0.5 else BAD, alpha=0.8)
            ax.errorbar(i, p, yerr=[[p - lo], [hi - p]], color=INK_2, linewidth=0.8, capsize=2.5)
            priced = st.mean(r["p_over"] for r in b)
            ax.scatter([i], [priced], marker="_", s=260, color=INK, linewidth=1.6, zorder=4)
            ax.text(i, hi + 0.02, f"{p:.0%}", ha="center", fontsize=7, color=INK, fontweight="bold")
            ax.text(i, -0.03, f"{k}/{len(b)}", ha="center", va="top", fontsize=6.3, color=INK_2,
                    transform=ax.get_xaxis_transform())
        ax.axhline(0.5, color=INK_2, linewidth=0.8, linestyle="--")
        ax.set_ylim(0, 1)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}"))
        ax.set_xticks(range(len(labels)), labels, fontsize=7)
        ax.tick_params(axis="x", pad=12)
        _title(ax, market)
    for ax in axes[:, 0]:
        ax.set_ylabel("Over hit", fontsize=7.5, color=INK_2)
    fig.tight_layout(h_pad=2.2)
    sub = (f"Closing DraftKings lines, {weeks}, grouped by where the line sat against the player's 2026 average going "
           f"in. Bar: how often he went over, with a 95% interval;\nblack tick: the over chance the prices implied "
           f"(vig removed). Under each bar: overs / lines graded. Pushes and players with no stat row that week are "
           f"left out.\nGroups under {MIN_BIN} lines get no bar. Small groups and several markets read at once: a pattern here is a question, not a finding.")
    return _finish(fig, path, "Did the over hit more when the line sat below a player's average?", sub, height)


def line_follows_chart(rows: list[dict], path: Path, weeks: str) -> bool:
    feats = (("season_avg", "2026 average"), ("last3", "Last 3 games"), ("last_game", "Last game"),
             ("last_season", "2025 average"))
    height = 6.4
    fig, axes = plt.subplots(1, 2, figsize=(WIDTH_IN, height), dpi=DPI, sharey=True)
    fig.patch.set_facecolor(SURFACE)
    width = 0.8 / (len(feats) + 1)
    drew = False
    for ax, (target, heading) in zip(axes, (("line", "What the line follows"),
                                             ("actual", "What predicted the result"))):
        _axes(ax, xgrid=False)
        counts: dict[str, int] = {}
        for mi, market in enumerate(MARKETS):
            ps = [r for r in rows if r["market"] == market and all(r[f] is not None for f, _ in feats)
                  and r[target] is not None]
            if len(ps) < 10:
                continue
            series = list(feats) + ([("line", "The line")] if target == "actual" else [])
            for fi, (f, label) in enumerate(series):
                c = pearson([r[f] for r in ps], [r[target] for r in ps])
                if c is None:
                    continue
                x = mi + (fi - (len(series) - 1) / 2) * width
                ax.bar(x, c, width=width * 0.92, color=SERIES[fi % len(SERIES)] if f != "line" else INK,
                       label=label if mi == 0 else None)
                ax.text(x, c + 0.015, f"{c:.2f}", ha="center", fontsize=5.6, color=INK, rotation=90, va="bottom")
                drew = True
            counts[market] = len(ps)
        ax.set_xticks(range(len(MARKETS)), [f"{MARKETS[m][1]}\nn={counts.get(m, 0)}" for m in MARKETS], fontsize=7.5)
        ax.set_ylim(0, 1.08)
        ax.set_title(heading, fontsize=9.5, color=INK, loc="left", fontweight="bold")
        ax.legend(fontsize=6.8, frameon=False, loc="upper right")
    if not drew:
        plt.close(fig)
        return False
    axes[0].set_ylabel("Correlation", fontsize=7.5, color=INK_2)
    fig.tight_layout(w_pad=2.0)
    sub = (f"Closing DraftKings lines, {weeks}, for players with at least {MIN_PRIOR} 2026 games before the line and "
           f"{MIN_LAST_SEASON}+ games in 2025. Left: how closely the line moved\nwith each piece of his history "
           f"(1 = in lockstep). Right: how closely each piece, and the line itself, moved with what he then did. "
           f"Same players in every bar of a market.")
    return _finish(fig, path, "Which part of a player's history does the line follow, and which predicts the game?",
                   sub, height)


def after_big_game_chart(changes: list[dict], path: Path, weeks: str) -> bool:
    if not changes:
        return False
    height = 8.0
    fig, axes = _grid(height)
    notes = []
    for ax, market in zip(axes.flat, MARKETS):
        cs = [c for c in changes if c["market"] == market]
        _axes(ax)
        if len(cs) < 5:
            ax.set_visible(False)
            continue
        xs, ys = [c["beat"] for c in cs], [c["move"] for c in cs]
        ax.scatter(xs, ys, s=11, color=[GOOD if x > 0 else BAD for x in xs], alpha=0.55, edgecolor="none")
        b = slope(xs, ys)
        lo, hi = min(xs), max(xs)
        mx, my = st.mean(xs), st.mean(ys)
        ax.plot([lo, hi], [my + b * (lo - mx), my + b * (hi - mx)], color=INK, linewidth=1.2)
        ax.axhline(0, color=INK_2, linewidth=0.7)
        ax.axvline(0, color=INK_2, linewidth=0.7)
        unit = "catch" if market == "player_receptions" else "yard"
        ax.text(0.03, 0.96, f"{len(cs)} line pairs\nslope {b:.2f}: each {unit} he beat his last line by moved\n"
                f"the next line {b:.2f}", transform=ax.transAxes, fontsize=7, color=INK, va="top")
        ax.set_xlabel("Last game minus his last line", fontsize=7.5, color=INK_2)
        ax.set_ylabel("This line minus last line", fontsize=7.5, color=INK_2)
        _title(ax, market)
        notes.append(f"{MARKETS[market][1].lower()} {b:.2f}")
    fig.tight_layout(h_pad=2.0, w_pad=2.0)
    sub = (f"Each point: a player's line in one week against his previous line in the same market, {weeks}. Across: "
           f"how far his last game beat (green) or missed (red) that\nprevious line. Up: how far the book then moved "
           f"his line. Black: the straight-line fit. Slopes: " + ", ".join(notes) + ".\nA slope near 0 means the book "
           f"mostly ignored the last game; near 1 would mean it chased it. Weeks between the two lines can differ "
           f"(bye, a missed game).")
    return _finish(fig, path, "After a player beats or misses his line, how far does the next one move?", sub, height)


def pick_players(rows: list[dict], market: str, n: int, min_lines: int = 4) -> list[str]:
    by = collections.defaultdict(list)
    for r in rows:
        if r["market"] == market:
            by[r["player_key"]].append(r)
    ok = [(st.mean(r["line"] for r in rs), k) for k, rs in by.items()
          if sum(1 for r in rs if r["actual"] is not None) >= min_lines]
    return [k for _, k in sorted(ok, reverse=True)[:n]]


def player_paths_chart(rows: list[dict], path: Path, weeks: str, per_market: int = 4) -> bool:
    picks = [("player_reception_yds", k) for k in pick_players(rows, "player_reception_yds", per_market)] + \
            [("player_rush_yds", k) for k in pick_players(rows, "player_rush_yds", per_market)]
    if not picks:
        return False
    ncols = 4
    nrows = (len(picks) + ncols - 1) // ncols
    height = 2.55 * nrows + 2.2
    fig, axes = plt.subplots(nrows, ncols, figsize=(WIDTH_IN, height), dpi=DPI, squeeze=False)
    fig.patch.set_facecolor(SURFACE)
    for ax, (market, key) in zip(axes.flat, picks):
        rs = sorted((r for r in rows if r["market"] == market and r["player_key"] == key), key=lambda r: r["week"])
        _axes(ax)
        ws = [r["week"] for r in rs]
        ax.plot(ws, [r["line"] for r in rs], color=INK, linewidth=1.3, marker="_", markersize=9, label="Line")
        for r in rs:
            if r["actual"] is not None:
                ax.scatter([r["week"]], [r["actual"]], s=24, zorder=3,
                           color=GOOD if r["actual"] > r["line"] else BAD if r["actual"] < r["line"] else NEUTRAL)
        last = rs[-1]["last_season"]
        if last is not None:
            ax.axhline(last, color=SERIES[2], linewidth=1.0, linestyle="--")
        overs = sum(1 for r in rs if r["result"] == "over")
        unders = sum(1 for r in rs if r["result"] == "under")
        short = {"player_reception_yds": "rec yds", "player_rush_yds": "rush yds"}.get(market, MARKETS[market][1])
        ax.set_title(f"{rs[-1]['name']}, {short} ({overs}-{unders})", fontsize=7.2, color=INK, loc="left",
                     fontweight="bold")
        ax.set_xticks(sorted(set(ws)))
        ax.tick_params(labelsize=6.5)
        ax.set_ylim(bottom=0)
    for ax in list(axes.flat)[len(picks):]:
        ax.set_visible(False)
    fig.tight_layout(h_pad=1.6, w_pad=1.0)
    sub = (f"Game by game, {weeks}, for the four players with the highest average receiving-yards line and the four "
           f"with the highest rushing-yards line\n(each with {4}+ graded games). Black: his closing DraftKings line. "
           f"Dots: what he did (green over, red under). Dashed: his 2025 average. Title: overs-unders.")
    return _finish(fig, path, "The most-bet players: their line each week against what they did", sub, height)


# ── data ─────────────────────────────────────────────────────────────────────
def _db():
    from config import load_config
    from db.database import DatabaseManager
    return DatabaseManager(load_config().database_url, initialize_schema=False)


def load_history(db, season: int) -> dict[str, dict[int, dict]]:
    """normalized name -> {week: nflverse weekly row} for one season."""
    from model.nfl_dfs_dk_pool_match import normalize_name

    out: dict[str, dict[int, dict]] = collections.defaultdict(dict)
    for r in db.execute(
            """SELECT f.canonical_name, s.week, s.source_row FROM ff_player_week_stats s
               JOIN ff_players f ON f.id = s.player_id WHERE s.season = %s AND s.week <= 18""", (season,)):
        row = r["source_row"] if isinstance(r["source_row"], dict) else json.loads(r["source_row"] or "{}")
        out[normalize_name(r["canonical_name"])].setdefault(r["week"], row)
    return out


def load_rows(db, season: int = SEASON) -> list[dict]:
    from model import nfl_prop_implied as pim

    cutoff = datetime.now(timezone.utc) - timedelta(hours=5)
    lines = db.execute(
        """SELECT week, player_key, player_name, market, line, over_price, under_price, event_id
           FROM nfl_prop_implied_lines
           WHERE season = %s AND capture_role = 'close' AND book = 'draftkings' AND conversion_version = %s
             AND market = ANY(%s) AND commence_time < %s AND line IS NOT NULL
             AND over_price IS NOT NULL AND under_price IS NOT NULL""",
        (season, pim.VERSION, list(MARKETS), cutoff))
    now, prev = load_history(db, season), load_history(db, season - 1)
    rows = []
    for r in lines:
        stat = MARKETS[r["market"]][0]
        mine = now.get(r["player_key"], {})
        games = {w: float(row.get(stat) or 0) for w, row in mine.items()}
        last_season = [float(row.get(stat) or 0) for row in prev.get(r["player_key"], {}).values()]
        actual = games.get(r["week"])
        h = history_features(games, last_season, r["week"])
        line = float(r["line"])
        rows.append({"week": r["week"], "player_key": r["player_key"], "name": r["player_name"] or r["player_key"],
                     "market": r["market"], "line": line, "event_id": r["event_id"],
                     "p_over": pim.p_over(r["over_price"], r["under_price"]), "actual": actual,
                     "result": result(actual, line), "gap": gap_bin(line, h["season_avg"]), **h})
    return rows


def build_all(out_dir: Path, season: int = SEASON, db=None) -> list[tuple[str, bool]]:
    db = db or _db()
    rows = load_rows(db, season)
    ws = sorted({r["week"] for r in rows})
    weeks = f"weeks {ws[0]}-{ws[-1]}" if ws else "no weeks"
    out_dir = Path(out_dir)
    jobs = [("nfl_take_prop_line_vs_average.png", lambda p: line_vs_average_chart(rows, p, weeks)),
            ("nfl_take_prop_over_rate_by_gap.png", lambda p: over_rate_by_gap_chart(rows, p, weeks)),
            ("nfl_take_prop_line_follows.png", lambda p: line_follows_chart(rows, p, weeks)),
            ("nfl_take_prop_after_big_game.png", lambda p: after_big_game_chart(line_changes(rows), p, weeks)),
            ("nfl_take_prop_player_paths.png", lambda p: player_paths_chart(rows, p, weeks))]
    return [(name, fn(out_dir / name)) for name, fn in jobs]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--season", type=int, default=SEASON)
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    args = ap.parse_args(argv)
    for name, ok in build_all(Path(args.out_dir), args.season):
        print(f"{name}: {'written' if ok else 'skipped, no data'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
