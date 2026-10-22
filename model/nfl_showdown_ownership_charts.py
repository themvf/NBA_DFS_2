"""Reader charts: DraftKings NFL Showdown ownership against points, every primetime flagship this season.

Descriptive, season to date; nothing here claims an edge.

  nfl_take_sd_own_by_game.png    one panel per game: each player's ownership against his DraftKings points
  nfl_take_sd_own_pooled.png     every game together: chalk that delivered, chalk that busted, low-owned smashes
  nfl_take_sd_captains.png       per game: captain ownership, and how many lineups had the best captain

Words used on the images:

* ownership: share of lineups that drafted the player, captain plus flex (``drafted_pct``); a player fills
  one slot per lineup, so it tops out at 100%
* captain ownership: share of lineups with him at captain (``drafted_by_slot.CPT``)
* points: his DraftKings points (``fpts``, flex scoring; a captain scores 1.5x)

Sources (SELECT only): ``nfl_dfs_field_contests`` / ``nfl_dfs_field_ownership``, the stat-api flagship of each
primetime game (the contest whose name carries the matchup, biggest field per game). dfsdb mirrors are left
out: their showdown ownership is flex only. A contest whose ownership adds to under 99.5% of the 600% six
slots hold is left out, since missing players could not be read as 0% owned.

    python -m model.nfl_showdown_ownership_charts --out-dir DIR
"""

from __future__ import annotations

import argparse
import json
import re
import statistics as st
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from model.cfb_pattern_charts import DPI, GRID, INK, INK_2, SERIES, SURFACE  # noqa: E402
from model.story_charts import BAD, GOOD, NEUTRAL, WIDTH_IN, _axes, _finish  # noqa: E402

SEASON = 2026
DEFAULT_DIR = Path("artifacts") / "nfl_market_scenarios"
FULL_MASS = 600.0 * 0.995
CHALK = 40.0          # ownership at or above this is chalk
LOW = 10.0            # ownership below this is low-owned
MID = 30.0            # the "under the radar" band runs up to this
_MATCHUP = re.compile(r"\(([A-Z]{2,3}) @ ([A-Z]{2,3})\)")
_DAY = re.compile(r"(Wednesday|Thursday|Friday|Saturday|Sunday|Monday|Tuesday)", re.I)


# ── pure computations ────────────────────────────────────────────────────────
def pick_flagships(contests: list[dict]) -> list[dict]:
    """The biggest full-ownership stat-api contest per (week, matchup), oldest week first."""
    best: dict[tuple, dict] = {}
    for c in contests:
        m = _MATCHUP.search(c.get("contest_name") or "")
        if not m or str(c["contest_id"]).startswith("dfsdb-") or (c.get("mass") or 0) < FULL_MASS:
            continue
        key = (c["week"], m.group(1), m.group(2))
        if key not in best or (c.get("entry_count") or 0) > (best[key].get("entry_count") or 0):
            day = _DAY.search(c["contest_name"])
            best[key] = c | {"matchup": f"{m.group(1)} @ {m.group(2)}",
                             "label": f"Wk {c['week']} {day.group(1)[:3] if day else ''} {m.group(1)} @ {m.group(2)}"
                             .replace("  ", " ")}
    return sorted(best.values(), key=lambda c: (c["week"], c["contest_id"]))


def players(rows: list[dict]) -> list[dict]:
    """Ownership rows with points, captain share and flex share; one row per player name (largest kept)."""
    out: dict[str, dict] = {}
    for r in rows:
        if r.get("fpts") is None or r.get("drafted_pct") is None:
            continue
        slots = r.get("drafted_by_slot") or {}
        if isinstance(slots, str):
            slots = json.loads(slots)
        p = {"name": r["player_name"], "own": float(r["drafted_pct"]), "cpt": float(slots.get("CPT") or 0),
             "flex": float(slots.get("FLEX") or 0), "pts": float(r["fpts"])}
        if p["name"] not in out or p["own"] > out[p["name"]]["own"]:
            out[p["name"]] = p
    return sorted(out.values(), key=lambda p: -p["own"])


def game_summary(ps: list[dict]) -> dict:
    """Best captain (top scorer), how many lineups had him at captain, and the chalk captain."""
    top = max(ps, key=lambda p: p["pts"])
    chalk = max(ps, key=lambda p: p["cpt"])
    return {"best": top, "chalk_cpt": chalk, "cpt_mass": sum(p["cpt"] for p in ps)}


def short(name: str) -> str:
    parts = name.split()
    return name if len(parts) < 2 or len(name) <= 14 else f"{parts[0][0]}. {' '.join(parts[1:])}"


# ── charts ───────────────────────────────────────────────────────────────────
def by_game_chart(games: list[dict], path: Path) -> bool:
    if not games:
        return False
    ncols = 2
    nrows = (len(games) + 1) // 2
    height = 3.0 * nrows + 2.2
    fig, axes = plt.subplots(nrows, ncols, figsize=(WIDTH_IN, height), dpi=DPI, squeeze=False)
    fig.patch.set_facecolor(SURFACE)
    for ax, g in zip(axes.flat, games):
        ps = g["players"]
        _axes(ax)
        colours = [GOOD if p["pts"] >= 15 else BAD if p["own"] >= CHALK else NEUTRAL for p in ps]
        ax.scatter([p["own"] for p in ps], [p["pts"] for p in ps], s=[18 + p["cpt"] * 2.2 for p in ps],
                   color=colours, alpha=0.8, edgecolor="white", linewidth=0.5)
        named = {p["name"] for p in sorted(ps, key=lambda p: -p["own"])[:3]} | \
                {p["name"] for p in sorted(ps, key=lambda p: -p["pts"])[:3]}
        for p in ps:
            if p["name"] in named:
                ax.annotate(short(p["name"]), (p["own"], p["pts"]), fontsize=5.8, color=INK,
                            xytext=(3, 2), textcoords="offset points")
        ax.axvline(CHALK, color=GRID, linewidth=0.8, linestyle="--")
        ax.set_xlim(-3, 103)
        ax.set_ylim(min(-2, min(p["pts"] for p in ps) - 2), max(p["pts"] for p in ps) * 1.18 + 2)
        ax.set_title(f"{g['label']}  ({g['entry_count']:,} entries)", fontsize=7.8, color=INK, loc="left",
                     fontweight="bold")
        ax.tick_params(labelsize=6.3)
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}%"))
    for ax in list(axes.flat)[len(games):]:
        ax.set_visible(False)
    for ax in axes[-1]:
        ax.set_xlabel("Ownership (captain + flex)", fontsize=7, color=INK_2)
    for ax in axes[:, 0]:
        ax.set_ylabel("DraftKings points", fontsize=7, color=INK_2)
    fig.tight_layout(h_pad=1.4, w_pad=1.2)
    sub = ("Every primetime DraftKings Showdown flagship this season, one dot per player. Across: share of lineups "
           "that drafted him (captain plus flex). Up: his DraftKings points\n(flex scoring). Dot size: captain "
           f"ownership. Green: 15+ points; red: {CHALK:.0f}%+ owned and under 15; grey: the rest. Dashed: "
           f"{CHALK:.0f}% ownership. Named: the three\nmost-owned players and the three top scorers.")
    return _finish(fig, path, "Showdown ownership against points, game by game", sub, height)


def pooled_chart(games: list[dict], path: Path) -> bool:
    pts = [p | {"game": g["label"]} for g in games for p in g["players"]]
    if not pts:
        return False
    height = 7.6
    fig, ax = plt.subplots(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    _axes(ax)
    chalk = [p for p in pts if p["own"] >= CHALK]
    low = [p for p in pts if p["own"] < LOW]
    ax.scatter([p["own"] for p in pts], [p["pts"] for p in pts], s=16, color=SERIES[0], alpha=0.45,
               edgecolor="none")
    ax.axvline(CHALK, color=INK_2, linewidth=0.8, linestyle="--")
    ax.axvline(LOW, color=INK_2, linewidth=0.8, linestyle=":")
    ax.axhline(15, color=GRID, linewidth=0.8)
    smash = sorted((p for p in pts if p["own"] < MID), key=lambda p: -p["pts"])[:5]
    busts = sorted((p for p in chalk if p["pts"] < 10), key=lambda p: -p["own"])[:5]
    hits = sorted((p for p in chalk if p["pts"] >= 20), key=lambda p: -p["pts"])[:5]
    for group, colour in ((smash, GOOD), (busts, BAD), (hits, SERIES[1])):
        for j, p in enumerate(group):
            ax.scatter([p["own"]], [p["pts"]], s=30, color=colour, zorder=3)
            ax.annotate(f"{short(p['name'])} ({p['game'].split(' ', 2)[-1]}, wk {p['game'].split()[1]})",
                        (p["own"], p["pts"]), fontsize=6.2, color=INK, xytext=(4, 2), textcoords="offset points")
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.set_xlim(-3, 103)
    ax.set_xlabel("Ownership (captain + flex)", fontsize=7.5, color=INK_2)
    ax.set_ylabel("DraftKings points", fontsize=7.5, color=INK_2)
    fig.tight_layout()
    chalk_hit = sum(1 for p in chalk if p["pts"] >= 15) / len(chalk) if chalk else 0
    low_hit = sum(1 for p in low if p["pts"] >= 15) / len(low) if low else 0
    mid = [p for p in pts if LOW <= p["own"] < MID]
    mid_hit = sum(1 for p in mid if p["pts"] >= 15) / len(mid) if mid else 0
    sub = (f"{len(pts)} players across {len(games)} primetime Showdowns, weeks {games[0]['week']}-{games[-1]['week']}. "
           f"Of the {len(chalk)} players {CHALK:.0f}%+ owned (right of dashed), {chalk_hit:.0%} scored 15+; of the "
           f"{len(low)} under\n{LOW:.0f}% owned (left of dotted), {low_hit:.0%} did; of the {len(mid)} at {LOW:.0f}-"
           f"{MID:.0f}%, {mid_hit:.0%} did. Green: the top scorers under {MID:.0f}% owned; orange: chalk that "
           f"delivered (20+);\nred: the most-owned chalk that scored under 10. Under {LOW:.0f}% are mostly backups "
           f"and kickers who rarely score, which is why they are low-owned.")
    return _finish(fig, path, "Did the Showdown chalk deliver?", sub, height)


def captain_chart(games: list[dict], path: Path, n: int = 6) -> bool:
    if not games:
        return False
    ncols = 2
    nrows = (len(games) + 1) // 2
    height = 2.5 * nrows + 2.2
    fig, axes = plt.subplots(nrows, ncols, figsize=(WIDTH_IN, height), dpi=DPI, squeeze=False)
    fig.patch.set_facecolor(SURFACE)
    for ax, g in zip(axes.flat, games):
        ps = g["players"]
        best = max(ps, key=lambda p: p["pts"])
        rows = sorted(ps, key=lambda p: -p["cpt"])[:n]
        if best not in rows:
            rows = rows[:n - 1] + [best]
        rows = rows[::-1]
        _axes(ax, ygrid=False)
        ys = range(len(rows))
        ax.barh(list(ys), [p["cpt"] for p in rows], height=0.66,
                color=[GOOD if p is best else NEUTRAL for p in rows])
        top = max(p["cpt"] for p in rows)
        for y, p in zip(ys, rows):
            ax.text(p["cpt"] + top * 0.02, y, f"{p['cpt']:.1f}%  ->  {1.5 * p['pts']:.1f} pts", va="center",
                    fontsize=5.9, color=INK)
        ax.set_yticks(list(ys), [short(p["name"]) for p in rows], fontsize=6.3)
        ax.set_xlim(0, top * 1.75)
        ax.set_title(f"{g['label']}: best CPT {short(best['name'])} at {best['cpt']:.1f}%", fontsize=7.4,
                     color=INK, loc="left", fontweight="bold")
        ax.tick_params(axis="x", labelsize=6)
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}%"))
    for ax in list(axes.flat)[len(games):]:
        ax.set_visible(False)
    fig.tight_layout(h_pad=1.4, w_pad=1.2)
    share = st.mean(max(g["players"], key=lambda p: p["pts"])["cpt"] for g in games)
    sub = (f"Each primetime Showdown: the {n} most popular captains (share of lineups), and what each scored at "
           f"captain (1.5x his points). Green: the game's top scorer, the best\ncaptain in hindsight, added if he "
           f"was not among the {n}. On average {share:.1f}% of lineups had the best captain.")
    return _finish(fig, path, "Showdown captains: who the field picked, and who it should have", sub, height)


# ── data ─────────────────────────────────────────────────────────────────────
def _db():
    from config import load_config
    from db.database import DatabaseManager
    return DatabaseManager(load_config().database_url, initialize_schema=False)


def load_games(db, season: int = SEASON) -> list[dict]:
    contests = [dict(r) for r in db.execute(
        """SELECT c.contest_id, c.contest_name, c.week, c.entry_count, SUM(o.drafted_pct) AS mass
           FROM nfl_dfs_field_contests c JOIN nfl_dfs_field_ownership o ON o.contest_id = c.contest_id
           WHERE c.season = %s AND c.format = 'showdown'
           GROUP BY c.contest_id, c.contest_name, c.week, c.entry_count""", (season,))]
    games = pick_flagships(contests)
    for g in games:
        g["players"] = players([dict(r) for r in db.execute(
            """SELECT player_name, drafted_pct, drafted_by_slot, fpts FROM nfl_dfs_field_ownership
               WHERE contest_id = %s""", (g["contest_id"],))])
    return [g for g in games if g["players"]]


def build_all(out_dir: Path, season: int = SEASON, db=None) -> list[tuple[str, bool]]:
    games = load_games(db or _db(), season)
    out_dir = Path(out_dir)
    return [("nfl_take_sd_own_by_game.png", by_game_chart(games, out_dir / "nfl_take_sd_own_by_game.png")),
            ("nfl_take_sd_own_pooled.png", pooled_chart(games, out_dir / "nfl_take_sd_own_pooled.png")),
            ("nfl_take_sd_captains.png", captain_chart(games, out_dir / "nfl_take_sd_captains.png"))]


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
