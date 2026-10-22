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


# ── defenses ─────────────────────────────────────────────────────────────────
def defense_allowed(gs: dict[tuple, list[dict]], stat: str) -> dict[str, dict]:
    """defense -> games and the stat allowed per game (every opposing player's total), with a rank (1 = fewest)."""
    acc = defaultdict(list)
    for key, rows in gs.items():
        for team in key[1:]:
            opp = next(t for t in key[1:] if t != team)
            acc[opp].append(sum(_val(r, stat) for r in rows if r["team"] == team))
    table = {d: {"games": len(v), "per_game": st.mean(v)} for d, v in acc.items()}
    for rank, d in enumerate(sorted(table, key=lambda d: table[d]["per_game"]), start=1):
        table[d]["rank"] = rank
    return table


def player_weeks(gs: dict[tuple, list[dict]], stat: str, pid: str) -> dict[int, dict]:
    """week -> his opponent, his yards and his lead credit in that game."""
    out = {}
    for key, rows in gs.items():
        mine = [r for r in rows if r["player_id"] == pid]
        if not mine:
            continue
        best = max(_val(r, stat) for r in rows)
        tops = [r for r in rows if _val(r, stat) == best] if best > 0 else []
        me = mine[0]
        out[key[0]] = {"opp": me["opponent_team"], "yards": _val(me, stat),
                       "led": (1 / len(tops)) if me in tops else 0.0}
    return out


def defense_leaders(gs: dict[tuple, list[dict]], stat: str) -> dict[str, dict]:
    """defense -> games, how often the game's leader came from the offense it faced (ties split), his yards."""
    acc = defaultdict(lambda: {"games": 0, "against": 0.0, "yards": []})
    for key, rows in gs.items():
        best = max(_val(r, stat) for r in rows)
        tops = [r for r in rows if _val(r, stat) == best] if best > 0 else []
        for team in key[1:]:
            defense = next(t for t in key[1:] if t != team)
            a = acc[defense]
            a["games"] += 1
            for r in tops:
                if r["team"] == team:
                    a["against"] += 1 / len(tops)
                    a["yards"].append(best)
    return dict(acc)


def grid_chart(gs: dict[tuple, list[dict]], tables: dict[str, dict], path: Path, weeks: list[int],
               n: int = 12) -> bool:
    if not any(tables.values()):
        return False
    from matplotlib.colors import LinearSegmentedColormap

    cmap = LinearSegmentedColormap.from_list("defrank", ["#1baf7a", "#f4f2ec", "#e34948"])
    height = 2 * (0.34 * n + 1.0) + 2.6
    fig, axes = plt.subplots(2, 1, figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    for ax, (stat, label) in zip(axes, METRICS):
        allowed = defense_allowed(gs, stat)
        n_def = len(allowed)
        players = ranked(tables[stat], n)
        ax.set_facecolor(SURFACE)
        for y, p in enumerate(players):
            pw = player_weeks(gs, stat, p["id"])
            for x, w in enumerate(weeks):
                cell = pw.get(w)
                if cell is None:
                    ax.add_patch(plt.Rectangle((x, y), 1, 1, facecolor="#ebe8e1", edgecolor=SURFACE, linewidth=1.2))
                    ax.text(x + 0.5, y + 0.5, "did not play", ha="center", va="center", fontsize=5.4, color=INK_2)
                    continue
                d = allowed.get(cell["opp"])
                # Rank 1 = allowed the fewest: dark green is a tough defense for this stat, red a soft one.
                colour = cmap((d["rank"] - 1) / max(n_def - 1, 1)) if d else "#ebe8e1"
                ax.add_patch(plt.Rectangle((x, y), 1, 1, facecolor=colour, edgecolor=SURFACE, linewidth=1.2))
                led = cell["led"] > 0
                text = f"{'LED ' if led else ''}{cell['opp']}\n{cell['yards']:.0f} yds" + (f" (#{d['rank']})" if d else "")
                ax.text(x + 0.5, y + 0.5, text, ha="center", va="center", fontsize=5.8, color=INK,
                        fontweight="bold" if led else "normal")
                if led:
                    ax.add_patch(plt.Rectangle((x + 0.04, y + 0.06), 0.92, 0.88, fill=False, edgecolor=INK,
                                               linewidth=1.3))
        ax.set_xlim(0, len(weeks))
        ax.set_ylim(len(players), 0)
        ax.set_xticks([i + 0.5 for i in range(len(weeks))], [f"Week {w}" for w in weeks], fontsize=7)
        ax.xaxis.tick_top()
        ax.set_yticks([i + 0.5 for i in range(len(players))],
                      [f"{p['name']} {p['team']} ({_fmt(p['led'])}/{p['played']})" for p in players], fontsize=6.8)
        for side in ax.spines.values():
            side.set_visible(False)
        ax.tick_params(length=0, colors=INK_2)
        ax.set_title(label, fontsize=9.5, color=INK, loc="left", fontweight="bold", pad=18)
    fig.tight_layout(h_pad=2.0)
    sub = (f"The top {n} game leaders in each stat, week by week. Cell: his opponent, his yards, and that defense's "
           f"rank in the stat allowed per game this season (#1 allowed the fewest).\nColour: green is a tough "
           f"defense for the stat, red a soft one. Boxed and 'LED': he led both teams that game. Ranks use the whole "
           f"season to date, including\nthe game shown, and reflect the offenses each defense faced as much as the "
           f"defense itself.")
    return _finish(fig, path, "Game leaders, week by week, against the defenses they faced", sub, height)


def defense_chart(gs: dict[tuple, list[dict]], path: Path, weeks: str) -> bool:
    if not gs:
        return False
    height = 0.25 * 32 + 3.0
    fig, axes = plt.subplots(1, 2, figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    for ax, (stat, label), colour in zip(axes, METRICS, (SERIES[0], SERIES[1])):
        dl = defense_leaders(gs, stat)
        allowed = defense_allowed(gs, stat)
        rows = sorted(dl.items(), key=lambda kv: (kv[1]["against"] / kv[1]["games"], kv[1]["against"]))
        _axes(ax, ygrid=False)
        ys = range(len(rows))
        shares = [v["against"] / v["games"] for _, v in rows]
        ax.barh(list(ys), shares, color=colour, height=0.7)
        ax.axvline(0.5, color=INK_2, linewidth=0.8, linestyle="--")
        for y, (d, v), s in zip(ys, rows, shares):
            yds = f", {st.mean(v['yards']):.0f} yds" if v["yards"] else ""
            ax.text(s + 0.015, y, f"{_fmt(v['against'])}/{v['games']}{yds}  ({allowed[d]['per_game']:.0f}/g allowed, "
                    f"#{allowed[d]['rank']})", va="center", fontsize=5.8, color=INK)
        ax.set_yticks(list(ys), [d for d, _ in rows], fontsize=6.8)
        ax.set_xlim(0, 1.6)
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}" if v <= 1 else ""))
        ax.set_title(label, fontsize=9.5, color=INK, loc="left", fontweight="bold")
        ax.tick_params(axis="x", labelsize=6.6)
    fig.tight_layout(w_pad=2.0)
    sub = (f"{weeks}. Bar: share of each defense's games in which the game's leader in the stat played for the "
           f"offense it faced (dashed: half). Text: those games of all games, the leader's\naverage yards in them, "
           f"and the defense's yards allowed per game and rank (#1 allowed the fewest). Ties split the credit. "
           f"Top of each panel: the defenses whose\nopponent most often produced the game's top rusher or receiver. "
           f"Each game has one leader, so a defense whose OWN offense has a dominant player\nrarely gives it up "
           f"(Kansas City's rushing 0/5 is mostly Kenneth Walker leading KC's own games): this mixes the defense "
           f"with its own offense.")
    return _finish(fig, path, "Which defenses gave up the game's leading rusher and receiver?", sub, height)


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
            ("nfl_take_game_leaders_rate.png", rate_chart(tables, out_dir / "nfl_take_game_leaders_rate.png", weeks)),
            ("nfl_take_game_leaders_vs_defense.png",
             grid_chart(gs, tables, out_dir / "nfl_take_game_leaders_vs_defense.png", ws)),
            ("nfl_take_defense_gave_leader.png",
             defense_chart(gs, out_dir / "nfl_take_defense_gave_leader.png", weeks))]


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
