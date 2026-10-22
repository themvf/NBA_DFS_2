"""Reader charts: do the defenses that hold each position down also sack, intercept and keep points off the board?

Descriptive, season to date; nothing here claims an edge. Builds on ``model.nfl_defense_qb_charts`` (QB
points allowed against each QB's usual) and ``model.nfl_defense_position_charts`` (the same for RB, WR, TE).

  nfl_take_def_correlations.png   correlation of each position's points-vs-usual with sacks, interceptions and
                                  points allowed per game, with a 95% interval
  nfl_take_def_corr_scatter.png   the same twelve pairs as scatter plots

Defense stats come from nflverse play-by-play: sacks and interceptions per game, and points allowed per game
from each game's final score (overtime included). 32 defenses, five or six games each: a correlation needs
to be large to mean anything, and the interval says how large.

    python -m model.nfl_defense_correlation_charts --out-dir DIR
"""

from __future__ import annotations

import argparse
import collections
import math
import statistics as st
import time
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from model import nfl_defense_position_charts as pos_charts  # noqa: E402
from model import nfl_defense_qb_charts as qb_charts  # noqa: E402
from model.cfb_pattern_charts import DPI, INK, INK_2, SERIES, SURFACE  # noqa: E402
from model.nfl_defense_qb_charts import _flag, _team, pearson  # noqa: E402
from model.story_charts import BAD, GOOD, WIDTH_IN, _axes, _finish  # noqa: E402

SEASON = 2026
DEFAULT_DIR = Path("artifacts") / "nfl_market_scenarios"
ROWS = (("QB", "Quarterbacks"), ("RB", "Running backs"), ("WR", "Wide receivers"), ("TE", "Tight ends"))
COLS = (("sacks", "Sacks / game"), ("ints", "Interceptions / game"), ("points", "Points allowed / game"))
PBP_COLS = ("game_id", "week", "season_type", "defteam", "home_team", "away_team", "home_score", "away_score",
            "sack", "interception")


# ── pure computations ────────────────────────────────────────────────────────
def defense_stats(pbp: list[dict], weeks: set[int]) -> dict[str, dict]:
    """defense -> games, sacks, interceptions and points allowed per game (regular season, ``weeks`` only)."""
    sacks, ints = collections.Counter(), collections.Counter()
    finals: dict[str, tuple] = {}
    for p in pbp:
        if p.get("season_type") != "REG" or int(p["week"]) not in weeks:
            continue
        finals.setdefault(p["game_id"], (_team(p["home_team"]), _team(p["away_team"]),
                                          p.get("home_score"), p.get("away_score")))
        d = _team(p.get("defteam"))
        if not d:
            continue
        sacks[d] += _flag(p.get("sack"))
        ints[d] += _flag(p.get("interception"))
    games, allowed = collections.Counter(), collections.Counter()
    for home, away, hs, as_ in finals.values():
        if hs is None or as_ is None or hs != hs or as_ != as_:
            continue
        games[home] += 1
        games[away] += 1
        allowed[home] += float(as_)
        allowed[away] += float(hs)
    return {t: {"games": n, "sacks": sacks[t] / n, "ints": ints[t] / n, "points": allowed[t] / n}
            for t, n in games.items() if n}


def fisher_ci(r: float | None, n: int, z: float = 1.96) -> tuple[float, float] | None:
    if r is None or n < 4 or abs(r) >= 1:
        return None
    f = math.atanh(r)
    h = z / math.sqrt(n - 3)
    return math.tanh(f - h), math.tanh(f + h)


def correlations(effects: dict[str, dict[str, float]], stats: dict[str, dict]) -> dict[tuple[str, str], dict]:
    """(position, stat) -> r, its 95% interval and n, over defenses present in both."""
    out = {}
    for pos, _ in ROWS:
        teams = [t for t in effects.get(pos, {}) if t in stats]
        for key, _ in COLS:
            xs = [stats[t][key] for t in teams]
            ys = [effects[pos][t] for t in teams]
            r = pearson(xs, ys)
            out[(pos, key)] = {"r": r, "ci": fisher_ci(r, len(teams)), "n": len(teams), "xs": xs, "ys": ys,
                               "teams": teams}
    return out


# ── charts ───────────────────────────────────────────────────────────────────
def matrix_chart(corr: dict, path: Path, weeks: str) -> bool:
    if not any(c["r"] is not None for c in corr.values()):
        return False
    height = 5.4
    fig, ax = plt.subplots(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    from matplotlib.colors import LinearSegmentedColormap

    # Negative r: the defense that does more of the column held the position further below its usual.
    cmap = LinearSegmentedColormap.from_list("corr", [GOOD, "#f4f2ec", BAD])
    for y, (pos, _) in enumerate(ROWS):
        for x, (key, _) in enumerate(COLS):
            c = corr[(pos, key)]
            if c["r"] is None:
                continue
            ax.add_patch(plt.Rectangle((x, y), 1, 1, facecolor=cmap((c["r"] + 1) / 2), linewidth=0.8,
                                       edgecolor=SURFACE))
            ci = c["ci"]
            sig = ci is not None and (ci[0] > 0 or ci[1] < 0)
            text = f"{c['r']:+.2f}" + ("" if ci is None else f"\n[{ci[0]:+.2f}, {ci[1]:+.2f}]")
            ax.text(x + 0.5, y + 0.5, text, ha="center", va="center", fontsize=8.6 if sig else 7.6, color=INK,
                    fontweight="bold" if sig else "normal")
    ax.set_xlim(0, len(COLS))
    ax.set_ylim(len(ROWS), 0)
    ax.set_xticks([i + 0.5 for i in range(len(COLS))], [c[1] for c in COLS], fontsize=8)
    ax.xaxis.tick_top()
    ax.set_yticks([i + 0.5 for i in range(len(ROWS))], [f"{r[1]} vs usual" for r in ROWS], fontsize=8)
    for side in ax.spines.values():
        side.set_visible(False)
    ax.tick_params(length=0, colors=INK_2)
    fig.tight_layout()
    n = max(c["n"] for c in corr.values())
    sub = (f"Across {n} defenses, {weeks}. Rows: how far opposing players at the position scored above or below "
           f"their usual against the defense (from the earlier charts).\nColumns: the defense's own sacks, "
           f"interceptions and points allowed per game. Each cell: the correlation and its 95% interval; bold when "
           f"the interval\nexcludes zero. Green: defenses with more of the column held the position further below "
           f"its usual; red: the reverse. Several cells read at once,\nso one bold cell among twelve could be chance. "
           f"Points allowed is partly the same thing measured twice (these players' touchdowns are the points), and "
           f"an interception\ncosts the QB a DraftKings point, so those columns are expected to line up; sacks are "
           f"the cleaner test.")
    return _finish(fig, path, "Do defenses that hold positions down also sack, intercept and stop scoring?", sub,
                   height)


def scatter_chart(corr: dict, path: Path, weeks: str) -> bool:
    if not any(c["r"] is not None for c in corr.values()):
        return False
    height = 9.6
    fig, axes = plt.subplots(len(ROWS), len(COLS), figsize=(WIDTH_IN, height), dpi=DPI, sharex="col")
    fig.patch.set_facecolor(SURFACE)
    for i, (pos, pname) in enumerate(ROWS):
        for j, (key, cname) in enumerate(COLS):
            ax = axes[i][j]
            c = corr[(pos, key)]
            _axes(ax)
            if c["r"] is None:
                continue
            xs, ys = c["xs"], c["ys"]
            ax.scatter(xs, ys, s=12, color=SERIES[0], alpha=0.75, edgecolor="none")
            mx, my = st.mean(xs), st.mean(ys)
            sxx = sum((x - mx) ** 2 for x in xs)
            if sxx > 0:
                b = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx
                lo, hi = min(xs), max(xs)
                ax.plot([lo, hi], [my + b * (lo - mx), my + b * (hi - mx)], color=INK, linewidth=1.0)
            ax.axhline(0, color=INK_2, linewidth=0.5)
            # Name the two defenses furthest from the line of best fit, the most interesting exceptions.
            if sxx > 0:
                resid = sorted(zip(c["teams"], xs, ys), key=lambda t: -abs(t[2] - (my + b * (t[1] - mx))))[:2]
                for t, x, y in resid:
                    ax.annotate(t, (x, y), fontsize=5.6, color=INK, xytext=(2, 2), textcoords="offset points")
            ax.text(0.03, 0.95, f"r {c['r']:+.2f}", transform=ax.transAxes, fontsize=6.8, color=INK, va="top",
                    fontweight="bold")
            ax.tick_params(labelsize=6)
            if i == len(ROWS) - 1:
                ax.set_xlabel(cname, fontsize=7, color=INK_2)
            if j == 0:
                ax.set_ylabel(f"{pos} vs usual", fontsize=7, color=INK_2)
    fig.tight_layout(h_pad=0.8, w_pad=0.8)
    sub = (f"The twelve pairs from the correlation table, {weeks}, one dot per defense. Up: how far opposing players "
           f"at the position scored above (up) or below (down) their usual,\nper game. Across: the defense's "
           f"sacks, interceptions or points allowed per game. Black: straight-line fit. Named: the two defenses "
           f"furthest from it.")
    return _finish(fig, path, "Defenses' effect on each position, against their sacks, picks and points allowed",
                   sub, height)


# ── data ─────────────────────────────────────────────────────────────────────
def load_pbp(season: int = SEASON) -> list[dict]:
    import pandas as pd

    from model.nfl_market_scenarios import pbp_url

    last = None
    for attempt in range(3):
        try:
            frame = pd.read_csv(pbp_url(season), usecols=lambda c: c in PBP_COLS, compression="gzip",
                                low_memory=False)
            missing = set(PBP_COLS) - set(frame.columns)
            if missing:
                raise RuntimeError(f"play-by-play is missing columns: {sorted(missing)}")
            return frame.to_dict("records")
        except RuntimeError:
            raise
        except Exception as exc:  # noqa: BLE001 - retried, then raised
            last = exc
            time.sleep(2 ** attempt)
    raise RuntimeError(f"nflverse play-by-play {season} download failed: {last}")


def build_all(out_dir: Path, season: int = SEASON, db=None) -> list[tuple[str, bool]]:
    db = db or qb_charts._db()
    out_dir = Path(out_dir)
    starts = qb_charts.defense_effects(qb_charts.load_starts(db, season))
    rows = pos_charts.with_usual(pos_charts.load_player_games(db, season))
    ws = sorted({s["week"] for s in starts} | {r["week"] for r in rows})
    weeks = f"weeks {ws[0]}-{ws[-1]}" if ws else "no weeks"
    effects = {"QB": {t: v["over"] for t, v in qb_charts.by_defense(starts).items() if v["over"] is not None}}
    for pos in pos_charts.POSITIONS:
        effects[pos] = {t: v["over"] for t, v in pos_charts.by_defense(pos_charts.defense_games(rows, pos)).items()}
    corr = correlations(effects, defense_stats(load_pbp(season), set(ws)))
    return [("nfl_take_def_correlations.png", matrix_chart(corr, out_dir / "nfl_take_def_correlations.png", weeks)),
            ("nfl_take_def_corr_scatter.png", scatter_chart(corr, out_dir / "nfl_take_def_corr_scatter.png", weeks))]


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
