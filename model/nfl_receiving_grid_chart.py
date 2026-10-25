"""Reader chart: each pass catcher's receiving yards week by week, with that game's catches.

Descriptive, season to date; nothing here claims an edge.

  nfl_take_rec_grid_<teams>.png   one row per non-quarterback with 10+ catches, one cell per week: yards in the
                                  cell, catches under them, shaded by yards (gold at 100+, the DraftKings bonus).
                                  A week the team did not play reads BYE; a week the team played but the player
                                  has no stat line reads "no line". Right: average yards and catches per game.

Source: nflverse weekly player stats, regular season. A game counts when the player has a stat line for it; a game
he played without touching the ball can be missing from the feed, so "no line" is not proof he sat.

    python -m model.nfl_receiving_grid_chart --teams PIT,NO --out-dir DIR
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyBboxPatch  # noqa: E402

from model.cfb_pattern_charts import DPI, INK, INK_2, SURFACE  # noqa: E402
from model.nfl_team_receiving_charts import TEAM_NAMES, _n  # noqa: E402
from model.story_charts import WIDTH_IN, _finish  # noqa: E402

SEASON = 2026
DEFAULT_DIR = Path("artifacts") / "nfl_market_scenarios"
MIN_RECEPTIONS = 10
BONUS = 100
# Light to dark blue by yards; 100+ is gold so a bonus game stands out from a merely good one.
SHADES = ((20, "#eaf2fb"), (40, "#cfe1f6"), (60, "#a3c6ee"), (80, "#5f9de2"), (BONUS, "#2a78d6"))
GOLD = "#eda100"
EMPTY = "#efeeea"
BAD = "#c8352f"


# ── pure computations ────────────────────────────────────────────────────────
def shade(yards: float) -> str:
    if yards >= BONUS:
        return GOLD
    for limit, colour in SHADES:
        if yards < limit:
            return colour
    return SHADES[-1][1]


def team_weeks(rows: list[dict], team: str) -> tuple[list[int], dict[int, str]]:
    """All weeks played league-wide, and each week's opponent for ``team`` (absent = bye)."""
    weeks = sorted({int(r["week"]) for r in rows})
    opp = {}
    for r in rows:
        if r.get("team") == team and r.get("opponent_team"):
            opp[int(r["week"])] = str(r["opponent_team"])
    return weeks, opp


def player_grid(rows: list[dict], team: str, min_rec: int = MIN_RECEPTIONS) -> list[dict]:
    """Per non-QB on ``team`` with ``min_rec``+ catches: {week: (yards, catches)} plus averages."""
    players: dict[str, dict] = {}
    for r in rows:
        if r.get("team") != team or r.get("position") == "QB":
            continue
        p = players.setdefault(r["player_id"], {"name": r["player_display_name"], "position": r["position"],
                                                "weeks": {}})
        p["weeks"][int(r["week"])] = (_n(r.get("receiving_yards")), int(_n(r.get("receptions"))))
    out = []
    for p in players.values():
        games = list(p["weeks"].values())
        rec = sum(c for _, c in games)
        if rec < min_rec:
            continue
        yds = sum(y for y, _ in games)
        out.append({**p, "games": len(games), "yards": yds, "receptions": rec,
                    "avg": yds / len(games), "rec_pg": rec / len(games)})
    return sorted(out, key=lambda p: (-p["avg"], p["name"]))


# ── chart ────────────────────────────────────────────────────────────────────
def grid_chart(teams: dict[str, tuple[list[dict], list[int], dict[int, str]]], path: Path) -> bool:
    teams = {t: v for t, v in teams.items() if v[0]}
    if not teams:
        return False
    # Columns run from the first to the last week any shown team has played, so a week only other teams have
    # reached (a Thursday game) is not drawn as a bye.
    played = {w for _, _, opp in teams.values() for w in opp}
    weeks = list(range(min(played), max(played) + 1))
    n_rows = sum(len(ps) for ps, _, _ in teams.values())
    row_h, head_h = 0.46, 0.62
    body = 0.3 + row_h * n_rows + (head_h + 0.12) * len(teams) + 0.45
    height = body + 1.25
    fig = plt.figure(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    ax = fig.add_axes((0, 0.05 / height, 1, body / height))
    ax.set_xlim(0, WIDTH_IN)
    ax.set_ylim(body, 0)
    ax.axis("off")
    name_w, cell_w, gap = 2.15, 0.84, 0.06
    x0 = 0.25 + name_w
    sum_x = x0 + len(weeks) * cell_w + 0.25
    y = 0.0
    ax.text(0.25, y + 0.15, "Player", fontsize=7, color=INK_2, va="center", fontweight="bold")
    for i, w in enumerate(weeks):
        ax.text(x0 + (i + 0.5) * cell_w, y + 0.15, f"Week {w}", fontsize=7, color=INK_2, ha="center",
                va="center", fontweight="bold")
    ax.text(sum_x + 0.55, y + 0.15, "Per game", fontsize=7, color=INK_2, ha="center", va="center",
            fontweight="bold")
    y += 0.3
    for team, (players, _, opp) in teams.items():
        ax.plot([0.25, WIDTH_IN - 0.2], [y + 0.05, y + 0.05], color="#d8d6d0", linewidth=0.7)
        ax.text(0.25, y + 0.32, TEAM_NAMES.get(team, team), fontsize=9.5, color=INK, va="center",
                fontweight="bold")
        for i, w in enumerate(weeks):
            ax.text(x0 + (i + 0.5) * cell_w, y + 0.32, f"vs {opp[w]}" if w in opp else "BYE", fontsize=6.5,
                    color=INK_2, ha="center", va="center")
        y += head_h
        for p in players:
            cy = y + row_h / 2
            ax.text(0.25, cy, p["name"], fontsize=8, color=INK, va="center", fontweight="bold")
            ax.text(0.25 + name_w - 0.15, cy, p["position"], fontsize=6.5, color=INK_2, va="center", ha="right")
            for i, w in enumerate(weeks):
                left = x0 + i * cell_w + gap / 2
                if w not in opp:
                    continue
                cell = p["weeks"].get(w)
                if cell is None:
                    ax.add_patch(FancyBboxPatch((left, y + 0.04), cell_w - gap, row_h - 0.08,
                                                boxstyle="round,pad=0,rounding_size=0.05", facecolor=EMPTY,
                                                edgecolor="none"))
                    ax.text(left + (cell_w - gap) / 2, cy, "no line", fontsize=6, color=INK_2, ha="center",
                            va="center")
                    continue
                yds, rec = cell
                fill = shade(yds)
                dark = fill in (SHADES[-1][1], SHADES[-2][1])
                ax.add_patch(FancyBboxPatch((left, y + 0.04), cell_w - gap, row_h - 0.08,
                                            boxstyle="round,pad=0,rounding_size=0.05", facecolor=fill,
                                            edgecolor="none"))
                ink = "white" if dark else (BAD if yds < 0 else INK)
                ax.text(left + (cell_w - gap) / 2, cy - 0.065, f"{yds:.0f}", fontsize=8.6, color=ink, ha="center",
                        va="center", fontweight="bold")
                ax.text(left + (cell_w - gap) / 2, cy + 0.115, f"{rec} rec", fontsize=5.6,
                        color="white" if dark else (INK if fill == GOLD else INK_2), ha="center", va="center")
            ax.text(sum_x + 0.55, cy - 0.06, f"{p['avg']:.0f} yds", fontsize=8.6, color=INK, ha="center",
                    va="center", fontweight="bold")
            ax.text(sum_x + 0.55, cy + 0.115, f"{p['rec_pg']:.1f} rec · {p['games']} g", fontsize=5.8,
                    color=INK_2, ha="center", va="center")
            y += row_h
        y += 0.12
    # Legend: the shading steps.
    ly = y + 0.2
    ax.text(0.25, ly, "Yards in the game:", fontsize=6.6, color=INK_2, va="center")
    lx = 1.4
    labels = ["<20", "20-39", "40-59", "60-79", "80-99", "100+"]
    for (limit, colour), lab in zip(list(SHADES) + [(None, GOLD)], labels):
        ax.add_patch(FancyBboxPatch((lx, ly - 0.09), 0.26, 0.18, boxstyle="round,pad=0,rounding_size=0.03",
                                    facecolor=colour, edgecolor="none"))
        ax.text(lx + 0.31, ly, lab, fontsize=6.4, color=INK_2, va="center")
        lx += 0.82
    names = " and ".join(TEAM_NAMES.get(t, t) for t in teams)
    sub = (f"{names}: every non-quarterback with {MIN_RECEPTIONS}+ catches, sorted by yards per game. "
           f"Each cell: yards,\nthen catches. Gold is a 100-yard game, the DraftKings bonus. Under each week "
           f"header: that week's opponent.\n\"no line\": the team played but he has no stat line (often "
           f"inactive, not proven). Per game counts only games with a line.")
    return _finish(fig, path, "Receiving yards, week by week", sub, height)


# ── data ─────────────────────────────────────────────────────────────────────
def build_all(out_dir: Path, teams: list[str], season: int = SEASON) -> list[tuple[str, bool]]:
    from model.nfl_game_leader_history_charts import load

    rows = load(season)
    data = {}
    for t in teams:
        weeks, opp = team_weeks(rows, t)
        data[t] = (player_grid(rows, t), weeks, opp)
    name = f"nfl_take_rec_grid_{'_'.join(t.lower() for t in teams)}.png"
    return [(name, grid_chart(data, Path(out_dir) / name))]


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
