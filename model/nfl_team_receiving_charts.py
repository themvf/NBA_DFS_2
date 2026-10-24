"""Reader chart: how much of each pass catcher's DraftKings points came from catches, yards and touchdowns.

Descriptive, season to date; nothing here claims an edge.

  nfl_take_receiving_<teams>.png   one panel per team, one bar per player who caught 3+ passes: DraftKings points
                                   split into receptions (1 each), receiving yards (1 per 10), receiving
                                   touchdowns (6 each), rushing (yards and touchdowns) and everything else
                                   (100-yard bonuses, two-point conversions, return touchdowns, lost fumbles)

Source: nflverse weekly player stats, regular season. DraftKings points use the same scoring as the DFS model
(``model.nfl_dfs_historical.draftkings_points``). Quarterbacks are left out.

    python -m model.nfl_team_receiving_charts --teams CHI,NE --out-dir DIR
"""

from __future__ import annotations

import argparse
import collections
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402

from model.cfb_pattern_charts import DPI, INK, INK_2, SURFACE  # noqa: E402
from model.nfl_dfs_historical import draftkings_points  # noqa: E402
from model.story_charts import WIDTH_IN, _axes, _finish  # noqa: E402

SEASON = 2026
DEFAULT_DIR = Path("artifacts") / "nfl_market_scenarios"
MIN_RECEPTIONS = 3
TEAM_NAMES = {"CHI": "Chicago Bears", "NE": "New England Patriots"}
PARTS = (("rec", "Receptions (1 pt each)", "#2a78d6"),
         ("yds", "Receiving yards (1 pt per 10)", "#7fb2ec"),
         ("td", "Receiving touchdowns (6 each)", "#1baf7a"),
         ("rush", "Rushing yards and touchdowns", "#e3a33b"),
         ("other", "Bonuses and other", "#b9b7b1"))


def _n(v) -> float:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if f != f else f


# ── pure computations ────────────────────────────────────────────────────────
def player_totals(rows: list[dict], team: str) -> list[dict]:
    """Season totals for every non-QB on ``team`` with MIN_RECEPTIONS+ catches, sorted by DraftKings points."""
    agg: dict[str, dict] = {}
    for r in rows:
        if r.get("team") != team or r.get("position") == "QB":
            continue
        a = agg.setdefault(r["player_id"], collections.defaultdict(float))
        a["name"] = r["player_display_name"]
        a["position"] = r["position"]
        a["games"] += 1
        a["receptions"] += _n(r.get("receptions"))
        a["rec_yards"] += _n(r.get("receiving_yards"))
        a["rec_tds"] += _n(r.get("receiving_tds"))
        rush = _n(r.get("rushing_yards")) / 10 + 6 * _n(r.get("rushing_tds"))
        a["rush"] += rush
        a["dk"] += draftkings_points(r["position"], r)
    out = []
    for a in agg.values():
        if a["receptions"] < MIN_RECEPTIONS:
            continue
        parts = {"rec": a["receptions"], "yds": a["rec_yards"] / 10, "td": 6 * a["rec_tds"], "rush": a["rush"]}
        parts["other"] = a["dk"] - sum(parts.values())
        out.append({"name": a["name"], "position": a["position"], "games": int(a["games"]),
                    "receptions": int(a["receptions"]), "rec_yards": int(a["rec_yards"]),
                    "rec_tds": int(a["rec_tds"]), "dk": a["dk"], "parts": parts})
    return sorted(out, key=lambda p: -p["dk"])


# ── chart ────────────────────────────────────────────────────────────────────
def receiving_chart(teams: dict[str, list[dict]], path: Path, weeks: str) -> bool:
    teams = {t: ps for t, ps in teams.items() if ps}
    if not teams:
        return False
    n_rows = sum(len(ps) for ps in teams.values())
    height = 0.3 * n_rows + 1.0 * len(teams) + 2.4
    fig, axes = plt.subplots(len(teams), 1, figsize=(WIDTH_IN, height), dpi=DPI,
                             gridspec_kw={"height_ratios": [len(ps) for ps in teams.values()]})
    fig.patch.set_facecolor(SURFACE)
    axes = axes if len(teams) > 1 else [axes]
    top = max(p["dk"] for ps in teams.values() for p in ps)
    for ax, (team, players) in zip(axes, teams.items()):
        _axes(ax, ygrid=False)
        for y, p in enumerate(players):
            left_pos = left_neg = 0.0
            for key, _, colour in PARTS:
                v = p["parts"][key]
                if v >= 0:
                    ax.barh(y, v, left=left_pos, color=colour, height=0.72)
                    if v >= 6:
                        ax.text(left_pos + v / 2, y, f"{v:.0f}", ha="center", va="center", fontsize=6,
                                color="white" if key in ("rec", "td") else INK)
                    left_pos += v
                else:
                    ax.barh(y, v, left=left_neg, color=colour, height=0.72)
                    left_neg += v
            ax.text(left_pos + top * 0.01, y, f"{p['dk']:.1f} pts in {p['games']} g ({p['dk'] / p['games']:.1f}/g)",
                    va="center", fontsize=6.4, color=INK_2)
        ax.set_yticks(range(len(players)),
                      [f"{p['name']} ({p['position']})  {p['receptions']} rec, {p['rec_yards']} yds, "
                       f"{p['rec_tds']} TD" for p in players], fontsize=6.8)
        ax.set_ylim(len(players) - 0.4, -0.6)
        ax.set_xlim(min(0.0, min(p["parts"]["other"] for p in players)) - 1, top * 1.28)
        ax.set_title(TEAM_NAMES.get(team, team), loc="left", fontsize=9, color=INK, fontweight="bold")
        ax.tick_params(axis="x", labelsize=7)
    axes[-1].set_xlabel("DraftKings points, season total", fontsize=8, color=INK_2)
    axes[0].legend(handles=[Patch(facecolor=c, label=lab) for _, lab, c in PARTS], loc="lower right",
                   fontsize=6.6, frameon=False)
    fig.tight_layout(h_pad=1.2)
    names = " and ".join(TEAM_NAMES.get(t, t) for t in teams)
    sub = (f"{names}, {weeks}: every non-quarterback with {MIN_RECEPTIONS}+ catches, sorted by DraftKings points. "
           f"Each bar splits the points into\ncatches (1 each), receiving yards (1 per 10), receiving touchdowns "
           f"(6 each), rushing, and the rest (100-yard bonuses, two-point conversions, lost fumbles).\nLabels: "
           f"catches, receiving yards and receiving touchdowns. Season totals, so a player who missed games sits "
           f"lower; the per-game figure is on the right.")
    return _finish(fig, path, "Where the fantasy points came from: catches, yards or touchdowns", sub, height)


# ── data ─────────────────────────────────────────────────────────────────────
def build_all(out_dir: Path, teams: list[str], season: int = SEASON) -> list[tuple[str, bool]]:
    from model.nfl_game_leader_history_charts import load

    rows = load(season)
    ws = sorted({int(r["week"]) for r in rows})
    weeks = f"weeks {ws[0]}-{ws[-1]}" if ws else "no weeks"
    name = f"nfl_take_receiving_{'_'.join(t.lower() for t in teams)}.png"
    data = {t: player_totals(rows, t) for t in teams}
    return [(name, receiving_chart(data, Path(out_dir) / name, weeks))]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--teams", default="CHI,NE")
    ap.add_argument("--season", type=int, default=SEASON)
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    args = ap.parse_args(argv)
    teams = [t.strip().upper() for t in args.teams.split(",") if t.strip()]
    for name, ok in build_all(Path(args.out_dir), teams, args.season):
        print(f"{name}: {'written' if ok else 'skipped, no data'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
