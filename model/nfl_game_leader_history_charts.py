"""Reader charts: which players have led their games in rushing yards and receiving yards most often.

Season to date, descriptive. A "game leader" is the player with the most of the stat among BOTH teams in that
game; a tie for the lead splits the credit (two tied players get half a game each).

  nfl_take_game_leaders_count.png   the players who led the most games, rushing and receiving side by side
  nfl_take_game_leaders_rate.png    the same players by share of their games led, with the yards they led with

Source: nflverse weekly player stats (``ingest.ff_independent.NFLVERSE_WEEKLY_STATS_URL``), regular season.
A game counts once both teams' rows are in the file.

    python -m model.nfl_game_leader_history_charts --out-dir DIR
"""

from __future__ import annotations

import argparse
import io
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
METRICS = (("rushing_yards", "Rushing yards"), ("receiving_yards", "Receiving yards"))
TOP = 20
MIN_GAMES_FOR_RATE = 3


# ── pure computations ────────────────────────────────────────────────────────
def games(rows: list[dict]) -> dict[tuple, list[dict]]:
    """(week, team_a, team_b) -> player rows from both teams; games missing one team are dropped."""
    by = defaultdict(list)
    for r in rows:
        if not r.get("team") or not r.get("opponent_team"):
            continue
        by[(int(r["week"]), *sorted((r["team"], r["opponent_team"])))].append(r)
    return {k: v for k, v in by.items() if {r["team"] for r in v} == set(k[1:])}


def _val(r: dict, stat: str) -> float:
    v = r.get(stat)
    return 0.0 if v is None or v != v else float(v)


def leaders(gs: dict[tuple, list[dict]], stat: str) -> dict[str, dict]:
    """player_id -> name, team, games played, games led (ties split), and the yards of each game he led."""
    out: dict[str, dict] = {}
    for key, rows in gs.items():
        for r in rows:
            p = out.setdefault(r["player_id"], {"name": r.get("player_display_name") or r.get("player_name"),
                                                "team": r["team"], "position": r.get("position"), "played": 0,
                                                "led": 0.0, "lead_yards": []})
            p["played"] += 1
            p["team"] = r["team"]
        best = max(_val(r, stat) for r in rows)
        if best <= 0:
            continue
        tops = [r for r in rows if _val(r, stat) == best]
        for r in tops:
            out[r["player_id"]]["led"] += 1 / len(tops)
            out[r["player_id"]]["lead_yards"].append(best)
    return out


def ranked(table: dict[str, dict], n: int = TOP) -> list[dict]:
    rows = [p | {"id": pid} for pid, p in table.items() if p["led"] > 0]
    return sorted(rows, key=lambda p: (-p["led"], p["played"], p["name"]))[:n]


def _fmt(x: float) -> str:
    return f"{x:g}" if x == int(x) else f"{x:.1f}"


# ── charts ───────────────────────────────────────────────────────────────────
def count_chart(tables: dict[str, dict], path: Path, weeks: str, n_games: int) -> bool:
    if not any(tables.values()):
        return False
    height = 0.30 * TOP + 2.8
    fig, axes = plt.subplots(1, 2, figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    for ax, (stat, label), colour in zip(axes, METRICS, (SERIES[0], SERIES[1])):
        rows = ranked(tables[stat])[::-1]
        _axes(ax, ygrid=False)
        ys = range(len(rows))
        ax.barh(list(ys), [p["played"] for p in rows], color="#e4e1da", height=0.72, label="Games played")
        ax.barh(list(ys), [p["led"] for p in rows], color=colour, height=0.72, label="Games led")
        for y, p in zip(ys, rows):
            ax.text(p["played"] + 0.1, y, f"{_fmt(p['led'])} of {p['played']}", va="center", fontsize=6.4, color=INK)
        ax.set_yticks(list(ys), [f"{p['name']} {p['team']}" for p in rows], fontsize=6.8)
        ax.set_xlim(0, max(p["played"] for p in rows) + 1.6)
        ax.set_title(label, fontsize=9.5, color=INK, loc="left", fontweight="bold")
        ax.set_xlabel("Games", fontsize=7, color=INK_2)
        ax.tick_params(axis="x", labelsize=6.6)
        ax.legend(fontsize=6.6, frameon=False, loc="lower right")
    fig.tight_layout(w_pad=2.0)
    sub = (f"{weeks}, {n_games} games. A game's leader had the most of the stat among both teams; a tie splits the "
           f"credit. Coloured bar: games he led; grey: games he played.\nTop {TOP} by games led, fewer games played "
           f"first on a tie.")
    return _finish(fig, path, "Who has led the most games in rushing and receiving yards?", sub, height)


def rate_chart(tables: dict[str, dict], path: Path, weeks: str) -> bool:
    if not any(tables.values()):
        return False
    height = 0.30 * TOP + 3.0
    fig, axes = plt.subplots(1, 2, figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    for ax, (stat, label), colour in zip(axes, METRICS, (SERIES[0], SERIES[1])):
        rows = [p | {"rate": p["led"] / p["played"]} for p in tables[stat].values()
                if p["led"] > 0 and p["played"] >= MIN_GAMES_FOR_RATE]
        rows = sorted(rows, key=lambda p: (-p["rate"], -p["led"]))[:TOP][::-1]
        _axes(ax, ygrid=False)
        ys = range(len(rows))
        ax.barh(list(ys), [p["rate"] for p in rows], color=colour, height=0.72)
        for y, p in zip(ys, rows):
            ax.text(p["rate"] + 0.01, y, f"{p['rate']:.0%}  ({_fmt(p['led'])}/{p['played']}, "
                    f"avg {st.mean(p['lead_yards']):.0f} yds when leading)", va="center", fontsize=6.0, color=INK)
        ax.set_yticks(list(ys), [f"{p['name']} {p['team']}" for p in rows], fontsize=6.8)
        ax.set_xlim(0, 1.75)
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}" if v <= 1 else ""))
        ax.set_title(label, fontsize=9.5, color=INK, loc="left", fontweight="bold")
        ax.tick_params(axis="x", labelsize=6.6)
    fig.tight_layout(w_pad=2.0)
    sub = (f"{weeks}: share of the games he played in which he led both teams, players with "
           f"{MIN_GAMES_FOR_RATE}+ games. Text: games led of games played, and the average yards the game's lead\n"
           f"took (the leader's total in the games he led). A few games is a small base, so a 100% can be two of "
           f"three.")
    return _finish(fig, path, "Who leads his games most reliably?", sub, height)


# ── data ─────────────────────────────────────────────────────────────────────
def load(season: int = SEASON) -> list[dict]:
    import pandas as pd
    import requests

    from ingest.ff_independent import NFLVERSE_WEEKLY_STATS_URL
    content = requests.get(NFLVERSE_WEEKLY_STATS_URL.format(season=season), timeout=120).content
    frame = pd.read_csv(io.BytesIO(content), low_memory=False)
    frame = frame[(frame["season"] == season) & (frame["season_type"] == "REG")]
    return frame.to_dict("records")


def build_all(out_dir: Path, season: int = SEASON) -> list[tuple[str, bool]]:
    gs = games(load(season))
    ws = sorted({k[0] for k in gs})
    weeks = f"Weeks {ws[0]}-{ws[-1]}" if ws else "No weeks"
    tables = {stat: leaders(gs, stat) for stat, _ in METRICS}
    out_dir = Path(out_dir)
    return [("nfl_take_game_leaders_count.png",
             count_chart(tables, out_dir / "nfl_take_game_leaders_count.png", weeks, len(gs))),
            ("nfl_take_game_leaders_rate.png", rate_chart(tables, out_dir / "nfl_take_game_leaders_rate.png", weeks))]


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
