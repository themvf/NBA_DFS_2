"""Reader chart: each pass catcher's receiving yards game by game, from worst game to best, with the average.

Descriptive, season to date; nothing here claims an edge.

  nfl_take_rec_yards_range_<teams>.png   one row per player who caught 3+ passes: a thin line from his fewest to
                                         his most receiving yards in a game, a thick band over his middle half of
                                         games, a dot for every game and a diamond at his average

Source: nflverse weekly player stats, regular season. A game counts when the player has a stat line for it; a game
he played without touching the ball can be missing from the feed, so the bottom of a range may sit slightly high.
Quarterbacks are left out.

    python -m model.nfl_receiving_yards_range_charts --teams PIT,NO --out-dir DIR
"""

from __future__ import annotations

import argparse
import statistics as st
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402

from model.cfb_pattern_charts import DPI, INK, INK_2, SURFACE  # noqa: E402
from model.nfl_team_receiving_charts import TEAM_NAMES, _n  # noqa: E402
from model.story_charts import WIDTH_IN, _axes, _finish  # noqa: E402

SEASON = 2026
DEFAULT_DIR = Path("artifacts") / "nfl_market_scenarios"
MIN_RECEPTIONS = 3
RANGE_COLOUR = "#7fb2ec"
BAND_COLOUR = "#2a78d6"
AVG_COLOUR = "#e3a33b"


# ── pure computations ────────────────────────────────────────────────────────
def player_ranges(rows: list[dict], team: str) -> list[dict]:
    """Per non-QB on ``team`` with MIN_RECEPTIONS+ catches: game yards, low, high, middle half and average."""
    games: dict[str, dict] = {}
    for r in rows:
        if r.get("team") != team or r.get("position") == "QB":
            continue
        p = games.setdefault(r["player_id"], {"name": r["player_display_name"], "position": r["position"],
                                              "weeks": [], "yards": [], "catches": [], "receptions": 0.0})
        p["weeks"].append(int(r["week"]))
        p["yards"].append(_n(r.get("receiving_yards")))
        p["catches"].append(int(_n(r.get("receptions"))))
        p["receptions"] += _n(r.get("receptions"))
    out = []
    for p in games.values():
        if p["receptions"] < MIN_RECEPTIONS:
            continue
        ys = p["yards"]
        q1, q3 = (st.quantiles(ys, n=4, method="inclusive")[0::2] if len(ys) >= 2 else (ys[0], ys[0]))
        out.append({"name": p["name"], "position": p["position"], "games": len(ys), "yards": ys,
                    "weeks": p["weeks"], "catches": p["catches"], "low": min(ys), "high": max(ys), "q1": q1, "q3": q3,
                    "avg": sum(ys) / len(ys), "total": sum(ys)})
    return sorted(out, key=lambda p: (-p["avg"], p["name"]))


# ── chart ────────────────────────────────────────────────────────────────────
def range_chart(teams: dict[str, list[dict]], path: Path, weeks: dict[str, str]) -> bool:
    teams = {t: ps for t, ps in teams.items() if ps}
    if not teams:
        return False
    n_rows = sum(len(ps) for ps in teams.values())
    height = 0.42 * n_rows + 1.0 * len(teams) + 2.4
    fig, axes = plt.subplots(len(teams), 1, figsize=(WIDTH_IN, height), dpi=DPI, sharex=True,
                             gridspec_kw={"height_ratios": [len(ps) for ps in teams.values()]})
    fig.patch.set_facecolor(SURFACE)
    axes = axes if len(teams) > 1 else [axes]
    top = max(p["high"] for ps in teams.values() for p in ps)
    for ax, (team, players) in zip(axes, teams.items()):
        _axes(ax, ygrid=False)
        for y, p in enumerate(players):
            ax.plot([p["low"], p["high"]], [y, y], color=RANGE_COLOUR, linewidth=1.4, zorder=1)
            ax.plot([p["q1"], p["q3"]], [y, y], color=BAND_COLOUR, linewidth=7, solid_capstyle="butt", zorder=2,
                    alpha=0.85)
            # One dot per game with that game's catches inside it. Dots closer than a dot's width are nudged
            # up and down in turn so every number stays readable.
            last_x, flip = None, 1
            for yds, rec in sorted(zip(p["yards"], p.get("catches", [None] * len(p["yards"])))):
                dy = 0.0
                if last_x is not None and yds - last_x < top * 0.028:
                    dy, flip = 0.27 * flip, -flip
                else:
                    flip = 1
                last_x = yds
                ax.scatter(yds, y + dy, s=62, facecolor=SURFACE, edgecolor=INK_2, linewidth=0.8, zorder=5)
                if rec is not None:
                    ax.text(yds, y + dy, str(rec), ha="center", va="center", fontsize=5.4, color=INK,
                            fontweight="bold", zorder=6)
            ax.scatter(p["avg"], y, marker="D", s=40, color=AVG_COLOUR, edgecolor=INK, linewidth=0.6, zorder=4)
            ax.text(p["low"] - top * 0.026, y, f"{p['low']:.0f}", ha="right", va="center", fontsize=6.4,
                    color=INK_2)
            ax.text(p["high"] + top * 0.026, y, f"{p['high']:.0f}", ha="left", va="center", fontsize=6.4,
                    color=INK_2)
            ax.text(1.01, y, f"avg {p['avg']:.0f}  ({p['total']:.0f} in {p['games']} g)", ha="left",
                    va="center", fontsize=6.6, color=INK, transform=ax.get_yaxis_transform(), clip_on=False)
        ax.set_yticks(range(len(players)), [f"{p['name']} ({p['position']})" for p in players], fontsize=6.9)
        ax.set_ylim(len(players) - 0.4, -0.6)
        ax.set_xlim(min(0.0, min(p["low"] for p in players)) - top * 0.09, top * 1.08)
        ax.set_title(f"{TEAM_NAMES.get(team, team)}, {weeks[team]}", loc="left", fontsize=9, color=INK,
                     fontweight="bold")
        ax.tick_params(axis="x", labelsize=7)
        ax.axvline(100, color=INK_2, linewidth=0.6, linestyle=":", zorder=0)
    axes[-1].set_xlabel("Receiving yards in a game", fontsize=8, color=INK_2)
    axes[0].legend(handles=[Line2D([], [], color=RANGE_COLOUR, linewidth=1.4, label="Fewest to most in a game"),
                            Patch(facecolor=BAND_COLOUR, alpha=0.85, label="Middle half of games"),
                            Line2D([], [], marker="o", linestyle="", markerfacecolor=SURFACE,
                                   markeredgecolor=INK_2, label="Each game (catches inside)"),
                            Line2D([], [], marker="D", linestyle="", color=AVG_COLOUR, markeredgecolor=INK,
                                   label="Average")],
                   loc="lower right", bbox_to_anchor=(1.0, 1.0), ncol=4, fontsize=6.4, frameon=False,
                   handletextpad=0.4, columnspacing=1.0)
    fig.tight_layout(h_pad=1.2, rect=(0, 0, 0.85, 1))
    names = " and ".join(TEAM_NAMES.get(t, t) for t in teams)
    sub = (f"{names}: every non-quarterback with {MIN_RECEPTIONS}+ catches, sorted by average receiving yards.\n"
           f"Thin line: his worst to best game, with the number at each end. Thick band: the middle half of his "
           f"games.\nDots: each game, with that game's catches inside. Diamond: his average. Dotted line: 100 yards, the DraftKings bonus. Five or "
           f"six games each,\nso the band is only a rough guide. A game he played without touching the ball can be "
           f"missing from the data.")
    return _finish(fig, path, "Receiving yards: worst game, best game and the average", sub, height)


# ── data ─────────────────────────────────────────────────────────────────────
def build_all(out_dir: Path, teams: list[str], season: int = SEASON) -> list[tuple[str, bool]]:
    from model.nfl_game_leader_history_charts import load

    rows = load(season)
    weeks = {}
    for t in teams:
        ws = sorted({int(r["week"]) for r in rows if r.get("team") == t})
        weeks[t] = f"weeks {ws[0]}-{ws[-1]}" if ws else "no games"
    name = f"nfl_take_rec_yards_range_{'_'.join(t.lower() for t in teams)}.png"
    data = {t: player_ranges(rows, t) for t in teams}
    return [(name, range_chart(data, Path(out_dir) / name, weeks))]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--teams", default="PIT,NO")
    ap.add_argument("--season", type=int, default=SEASON)
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    args = ap.parse_args(argv)
    teams = [t.strip().upper() for t in args.teams.split(",") if t.strip()]
    for name, ok in build_all(Path(args.out_dir), teams, args.season):
        print(f"{name}: {'written' if ok else 'skipped, no data'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
