"""Reader charts from the NFL DFS contest and player-prop data, for a newsletter.

Descriptive storytelling, season to date. Nothing here claims an edge.

  nfl_take_crowd_vs_results.png  the latest DraftKings Millionaire: how many lineups
                                 drafted each player against what he scored
  nfl_take_chalk_report.png      every week: the 10 most-drafted players against the rest
  nfl_take_winning_lineups.png   each week's first-place Millionaire lineup, slot by slot
  nfl_take_winners_history.png   how much of the crowd those winners shared, 2023-25 vs 2026
  nfl_take_prop_results.png      DraftKings player-prop overs: how often they hit against
                                 the rate their prices implied

Sources (see CLAUDE.md "stat-api.com", "Published Millionaire winners" and
"Reading the books' player lines"):

* ``nfl_dfs_field_contests``     the flagship Classic Millionaire per week, stored from stat-api
* ``nfl_dfs_field_ownership``    one row per player: share of lineups that drafted him and his
                                 DK points in that contest (``fpts``)
* ``nfl_dfs_field_user_builds``  every lineup of the top users, with salary, team, position and
                                 points per seat (the first-place roster comes from here)
* ``nfl_dfs_field_top_entries``  the top finishers' entries and ranks (ties are visible here)
* ``nfl_dfs_published_winners``  Hello Rookie's rank-1 Millionaire lineups, 2023-2025
* ``nfl_prop_implied_lines``     closing DraftKings prop lines; results from nflverse weekly stats

A player score comes from the contest's own per-player data, then from the
stat-api builds; a player with neither is left out and counted, never scored 0.

    python -m model.nfl_dfs_story_charts --out-dir DIR
"""

from __future__ import annotations

import argparse
import collections
import json
import random
import statistics as st
from datetime import datetime, timedelta, timezone
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from model.cfb_pattern_charts import GRID, INK, INK_2, SERIES, SURFACE  # noqa: E402
from model.story_charts import BAD, GOOD, NEUTRAL, _axes, _fig, _finish  # noqa: E402

SEASON = 2026
DEFAULT_DIR = Path("artifacts") / "nfl_dfs_story" / "charts"
OWN_NOTE = "Ownership = the share of lineups in the contest that drafted the player."
CLASSIC_SLOTS = ("QB", "RB", "RB", "WR", "WR", "WR", "TE", "FLEX", "DST")
POS_COLOUR = {"QB": SERIES[0], "RB": SERIES[1], "WR": SERIES[2], "TE": SERIES[4], "DST": SERIES[6]}
CHALK_N = 10            # "the chalk": the 10 most-drafted players
REST_MIN_OWN = 1.0      # the comparison group: everyone else drafted in at least 1% of lineups


# ── pure computations ────────────────────────────────────────────────────────
def scored_players(own_rows: list[dict], builds_points: dict[str, dict] | None = None) -> tuple[list[dict], int]:
    """(players with a score, count left out).

    ``own_rows``: name, normalized_name, drafted_pct, fpts. ``builds_points``:
    normalized name -> {points, position, team, salary} from the stat-api builds.
    The contest's own ``fpts`` wins; the builds fill a gap; no score means the
    player is dropped and counted.
    """
    extra = builds_points or {}
    out, missing = [], 0
    for r in own_rows:
        info = extra.get(r.get("normalized_name") or "", {})
        pts = r.get("fpts")
        if pts is None:
            pts = info.get("points")
        if pts is None or r.get("drafted_pct") is None:
            missing += 1
            continue
        out.append({"name": r["player_name"], "own": float(r["drafted_pct"]), "points": float(pts),
                    "position": info.get("position"), "team": info.get("team"), "salary": info.get("salary")})
    return out, missing


def chalk_split(players: list[dict], n: int = CHALK_N, rest_min_own: float = REST_MIN_OWN) -> dict | None:
    """Average points of the ``n`` most-drafted players against everyone else drafted in at least
    ``rest_min_own``% of lineups. None when there are not enough players for both groups."""
    ranked = sorted(players, key=lambda p: (-p["own"], p["name"]))
    chalk = ranked[:n]
    rest = [p for p in ranked[n:] if p["own"] >= rest_min_own]
    if len(chalk) < n or not rest:
        return None
    return {"chalk_avg": st.mean(p["points"] for p in chalk), "rest_avg": st.mean(p["points"] for p in rest),
            "chalk_n": len(chalk), "rest_n": len(rest),
            "chalk_20": sum(p["points"] >= 20 for p in chalk),
            "chalk_own": sum(p["own"] for p in chalk) / len(chalk),
            "best": max(chalk, key=lambda p: p["points"]), "worst": min(chalk, key=lambda p: p["points"])}


def crowd_labels(players: list[dict], n_busts: int = 5, n_smashes: int = 5, chalk_pool: int = 15,
                 bust_below: float = 10.0, bust_min_own: float = 10.0, smash_own: float = 5.0,
                 smash_min: float = 20.0) -> tuple[list[dict], list[dict]]:
    """(chalk busts, low-owned smashes): busts are among the ``chalk_pool`` most-drafted players, in at
    least ``bust_min_own``% of lineups, and scored under ``bust_below``; smashes were drafted in under
    ``smash_own``% of lineups and scored the most (at least ``smash_min``)."""
    ranked = sorted(players, key=lambda p: (-p["own"], p["name"]))
    busts = [p for p in ranked[:chalk_pool] if p["points"] < bust_below and p["own"] >= bust_min_own][:n_busts]
    smashes = sorted((p for p in players if p["own"] < smash_own and p["points"] >= smash_min),
                     key=lambda p: (-p["points"], p["name"]))[:n_smashes]
    return busts, smashes


def first_place(entries: list[dict]) -> dict | None:
    """The best-ranked entry and how many entries share that rank.

    ``entries``: rank, points, username, lineup_text. Returns the entry (the
    alphabetically first username when tied, so the pick is stable) plus
    ``tied`` (entries at that rank) and ``distinct_lineups`` among them.
    """
    ranked = [e for e in entries if e.get("rank") is not None]
    if not ranked:
        return None
    best = min(e["rank"] for e in ranked)
    top = sorted((e for e in ranked if e["rank"] == best), key=lambda e: (str(e.get("username") or ""),
                                                                           str(e.get("entry_id") or "")))
    return {**top[0], "tied": len(top), "distinct_lineups": len({e.get("lineup_text") for e in top})}


def lineup_summary(roster: list[dict], stack: str | None = None) -> dict:
    """Total salary, ownership and points of a roster, and its QB stack.

    ``roster``: slot, name, team, position, salary, points, own. The stack is
    the QB plus same-team WR/TE/RB teammates; ``stack`` is stat-api's own label
    (``QB/WR/BB``), read only for its bring-back flag.
    """
    qb = next((p for p in roster if p.get("position") == "QB" or p.get("slot") == "QB"), None)
    mates = []
    if qb and qb.get("team"):
        mates = [p["position"] for p in roster if p is not qb and p.get("team") == qb["team"]
                 and p.get("position") in ("WR", "TE", "RB")]
    order = {"WR": 0, "TE": 1, "RB": 2}
    mates.sort(key=lambda x: order[x])
    if not qb or not qb.get("team"):
        stack_text = "QB team unknown"
    elif mates:
        stack_text = f"{qb['team']} QB + {' + '.join(mates)}"
    else:
        stack_text = "no QB stack"
    if stack and "BB" in stack and mates:
        stack_text += ", bring-back"

    def total(key):
        vals = [p.get(key) for p in roster]
        return None if any(v is None for v in vals) else sum(vals)

    return {"salary": total("salary"), "own": total("own"), "points": total("points"), "stack": stack_text,
            "stacked": len(mates)}


def published_summary(rows: list[dict]) -> list[dict]:
    """Season, week and total ownership of each published winner, skipping rows flagged as a
    duplicate of another week (the page repeated a table) and rows with a missing ownership."""
    out = []
    for r in rows:
        flags = r.get("flags") or []
        if any(str(f).startswith("duplicate") for f in flags):
            continue
        pcts = [p.get("draft_pct") for p in r.get("players") or []]
        if not pcts or any(v is None for v in pcts):
            continue
        out.append({"season": r["season"], "week": r["week"], "own": float(sum(pcts)),
                    "points": r.get("total_points"), "flags": list(flags)})
    return out


def boot_ci(groups: dict, fn, n: int = 2000, seed: int = 11) -> tuple[float, float]:
    """95% interval of ``fn`` over resamples of whole groups (games): one game's lines move together."""
    rng = random.Random(seed)
    keys = list(groups)
    vals = []
    for _ in range(n):
        sample = [x for k in rng.choices(keys, k=len(keys)) for x in groups[k]]
        v = fn(sample)
        if v is not None:
            vals.append(v)
    vals.sort()
    if not vals:
        return float("nan"), float("nan")
    return vals[int(0.025 * len(vals))], vals[int(0.975 * len(vals)) - 1]


def over_summary(rows: list[dict], n_boot: int = 2000) -> dict | None:
    """Over hit rate against the implied rate for one market's closing lines.

    ``rows``: line, actual (None = no stat row), p_over (de-vigged), event_id.
    Pushes (actual == line) and missing stats are excluded and counted.
    """
    graded = [r for r in rows if r.get("actual") is not None and r["actual"] != r["line"]]
    pushes = sum(1 for r in rows if r.get("actual") is not None and r["actual"] == r["line"])
    missing = sum(1 for r in rows if r.get("actual") is None)
    if not graded:
        return None
    groups = collections.defaultdict(list)
    for r in graded:
        groups[r["event_id"]].append(r)
    over = st.mean(r["actual"] > r["line"] for r in graded)
    implied = st.mean(r["p_over"] for r in graded)
    lo, hi = boot_ci(groups, lambda xs: st.mean(x["actual"] > x["line"] for x in xs), n_boot)
    glo, ghi = boot_ci(groups, lambda xs: st.mean(x["actual"] > x["line"] for x in xs) - st.mean(x["p_over"] for x in xs),
                       n_boot)
    return {"n": len(graded), "games": len(groups), "pushes": pushes, "missing": missing, "over": over,
            "implied": implied, "ci": (lo, hi), "gap": over - implied, "gap_ci": (glo, ghi)}


# ── chart helpers ────────────────────────────────────────────────────────────
def _short(name: str, width: int = 15) -> str:
    """'Christian McCaffrey' -> 'C. McCaffrey'; team defenses and short names stay whole."""
    parts = name.split()
    if len(name) <= width or len(parts) < 2:
        return name
    out = f"{parts[0][0]}. {' '.join(parts[1:])}"
    return out if len(out) <= width else out[: width - 1] + "."


# ── 1a. the crowd against the results ────────────────────────────────────────
def crowd_chart(players: list[dict], path: Path, week: int, entries: int | None = None, missing: int = 0) -> bool:
    if len(players) < 10:
        return False
    height = 6.2
    fig, ax = _fig(height)
    _axes(ax)
    busts, smashes = crowd_labels(players)
    for pos, colour in POS_COLOUR.items():
        pts = [p for p in players if p.get("position") == pos]
        if pts:
            ax.scatter([p["own"] for p in pts], [p["points"] for p in pts], s=22, color=colour, alpha=0.75,
                       edgecolor=SURFACE, linewidth=0.5, zorder=3, label=pos)
    other = [p for p in players if p.get("position") not in POS_COLOUR]
    if other:
        ax.scatter([p["own"] for p in other], [p["points"] for p in other], s=22, color=NEUTRAL, alpha=0.75,
                   edgecolor=SURFACE, linewidth=0.5, zorder=3, label="position unknown")
    for p in busts:
        ax.scatter([p["own"]], [p["points"]], s=70, facecolor="none", edgecolor=BAD, linewidth=1.3, zorder=4)
    for p in smashes:
        ax.scatter([p["own"]], [p["points"]], s=70, facecolor="none", edgecolor=GOOD, linewidth=1.3, zorder=4)
    xs = [p["own"] for p in players]
    ys = [p["points"] for p in players]
    ax.set_xlim(-1, max(xs) * 1.12 + 1)
    ax.set_ylim(min(-2, min(ys) - 2), max(ys) * 1.12 + 2)
    ax.axhline(20, color=INK_2, linewidth=0.7, linestyle=":")
    ax.text(max(xs) * 1.1, 20.5, "20 points", fontsize=6.8, color=INK_2, ha="right", va="bottom")
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.set_xlabel("Ownership: share of lineups that drafted the player", color=INK_2, fontsize=8)
    ax.set_ylabel("DraftKings points he scored", color=INK_2, fontsize=8)
    ax.legend(loc="upper right", fontsize=7, frameon=False, ncol=6, bbox_to_anchor=(1, 1.0))
    field = f"{entries:,} lineups" if entries else "the contest"
    sub = (f"Week {week} DraftKings Millionaire, {field}. Each dot is a player someone drafted ({len(players)}). "
           f"{OWN_NOTE}\nRed rings: crowd favorites that scored under 10 points. "
           "Green rings: players in under 5% of lineups who scored 20 or more.")
    if missing:
        sub += f" {missing} drafted players had no score and are left out."
    # Rings are numbered; the names sit in two lists in the open space, since the low-owned corner is crowded.
    marked = [(p, BAD) for p in busts] + [(p, GOOD) for p in smashes]
    for i, (p, colour) in enumerate(marked, 1):
        ax.annotate(str(i), (p["own"], p["points"]), xytext=(6, 4), textcoords="offset points", fontsize=7.5,
                    color=colour, fontweight="bold")
    blocks = [("Low-owned smashes", GOOD, [(i, p) for i, (p, c) in enumerate(marked, 1) if c == GOOD]),
              ("Crowd favorites that busted", BAD, [(i, p) for i, (p, c) in enumerate(marked, 1) if c == BAD])]
    y = 0.88
    for head, colour, items in blocks:
        if not items:
            continue
        ax.text(0.6, y, head, transform=ax.transAxes, fontsize=8, color=colour, fontweight="bold", va="top")
        y -= 0.045
        for i, p in items:
            ax.text(0.6, y, f"{i}. {p['name']}: {p['own']:.1f}% owned, {p['points']:.1f} pts",
                    transform=ax.transAxes, fontsize=7.3, color=INK, va="top", zorder=6,
                    bbox={"facecolor": SURFACE, "edgecolor": "none", "pad": 0.4, "alpha": 0.9})
            y -= 0.033
        y -= 0.02
    return _finish(fig, path, f"What the crowd drafted, and what it got: week {week}", sub, height)


# ── 1b. the chalk report ─────────────────────────────────────────────────────
def chalk_report_chart(weeks: list[dict], path: Path) -> bool:
    """``weeks``: week plus the chalk_split() fields."""
    weeks = [w for w in weeks if w]
    if not weeks:
        return False
    height = 5.0
    fig, ax = _fig(height)
    _axes(ax, xgrid=False)
    xs = list(range(len(weeks)))
    w_ = 0.36
    ax.bar([x - w_ / 2 for x in xs], [w["chalk_avg"] for w in weeks], w_, color=SERIES[1], linewidth=0,
           label=f"The {CHALK_N} most-drafted players")
    ax.bar([x + w_ / 2 for x in xs], [w["rest_avg"] for w in weeks], w_, color=SERIES[0], linewidth=0,
           label=f"Everyone else drafted in {REST_MIN_OWN:.0f}%+ of lineups")
    top = max(max(w["chalk_avg"], w["rest_avg"]) for w in weeks)
    for x, w in zip(xs, weeks):
        ax.text(x - w_ / 2, w["chalk_avg"] + top * 0.015, f"{w['chalk_avg']:.1f}", ha="center", va="bottom",
                fontsize=7.5, color=INK)
        ax.text(x + w_ / 2, w["rest_avg"] + top * 0.015, f"{w['rest_avg']:.1f}", ha="center", va="bottom",
                fontsize=7.5, color=INK)
        ax.text(x, -top * 0.13, f"{w['chalk_20']} of {CHALK_N} hit 20+\nbest: {_short(w['best']['name'], 14)} "
                f"{w['best']['points']:.0f}\nworst: {_short(w['worst']['name'], 14)} {w['worst']['points']:.0f}",
                ha="center", va="top", fontsize=6.4, color=INK_2)
    ax.set_xticks(xs, [f"Week {w['week']}" for w in weeks], fontsize=8, color=INK)
    ax.tick_params(axis="x", pad=2)
    ax.set_ylim(0, top * 1.25)
    ax.set_ylabel("Average DraftKings points per player", color=INK_2, fontsize=8)
    ax.legend(loc="upper left", fontsize=7.5, frameon=False, ncol=2)
    beat = sum(w["chalk_avg"] > w["rest_avg"] for w in weeks)
    sub = (f"DraftKings Millionaire, weeks {weeks[0]['week']}-{weeks[-1]['week']} ({len(weeks)} contests). "
           f"The chalk outscored the rest in {beat} of {len(weeks)} weeks. {OWN_NOTE}\n"
           "The chalk is mostly expensive stars, so it should score more per player; the question is by how much. "
           "Five weeks is a small sample.")
    fig.tight_layout(rect=(0, 0.11, 1, 1 - 0.95 / height))
    return _finish(fig, path, "Did the crowd's favorites deliver?", sub, height)


# ── 2a. first-place lineups ──────────────────────────────────────────────────
def winning_lineups_chart(weeks: list[dict], path: Path) -> bool:
    """``weeks``: week, entries, roster (lineup_summary() input, in slot order), summary, tied,
    distinct_lineups, username."""
    weeks = [w for w in weeks if w and w.get("roster")]
    if not weeks:
        return False
    n = len(weeks)
    height = 1.05 * n + 1.7
    fig, ax = _fig(height)
    ax.set_facecolor(SURFACE)
    ax.axis("off")
    label_w = 1.75   # in column units; each slot is one unit
    ncols = len(CLASSIC_SLOTS)
    ax.set_xlim(0, label_w + ncols)
    ax.set_ylim(0, n + 0.45)
    for j, slot in enumerate(CLASSIC_SLOTS):
        ax.text(label_w + j + 0.48, n + 0.2, slot, ha="center", va="center", fontsize=8, color=INK_2, fontweight="bold")

    def shade(own):
        if own is None:
            return "#f1f0ec"
        if own < 5:
            return "#d8f1e6"
        if own >= 20:
            return "#fde3d6"
        return "#f1f0ec"

    for i, w in enumerate(weeks):
        y = n - 1 - i
        s = w["summary"]
        tie = f", tied with {w['tied'] - 1} other" + ("s" if w["tied"] > 2 else "") if w["tied"] > 1 else ""
        ax.text(0.02, y + 0.78, f"Week {w['week']}", fontsize=9, color=INK, fontweight="bold", va="center")
        lines = [f"{s['points']:.1f} pts{tie}" if s["points"] is not None else f"score n/a{tie}",
                 f"Ownership total {s['own']:.0f}%" if s["own"] is not None else "ownership n/a",
                 s["stack"]]
        if w.get("entries"):
            lines.append(f"of {w['entries']:,} lineups")
        ax.text(0.02, y + 0.58, "\n".join(lines), fontsize=6.4, color=INK_2, va="top", linespacing=1.25)
        for j, p in enumerate(w["roster"][:ncols]):
            x = label_w + j
            ax.add_patch(plt.Rectangle((x + 0.02, y + 0.04), 0.92, 0.88, color=shade(p.get("own")), linewidth=0))
            ax.text(x + 0.48, y + 0.74, _short(p["name"], 13), ha="center", va="center", fontsize=6.6, color=INK,
                    fontweight="bold")
            meta = []
            if p.get("position") and p["position"] != p.get("slot"):
                meta.append(p["position"])
            if p.get("team"):
                meta.append(p["team"])
            if p.get("salary") is not None:
                meta.append(f"${p['salary'] / 1000:.1f}K")
            ax.text(x + 0.48, y + 0.50, " ".join(meta), ha="center", va="center", fontsize=6, color=INK_2)
            own = f"{p['own']:.0f}%" if p.get("own") is not None else "?"
            pts = f"{p['points']:.1f}" if p.get("points") is not None else "?"
            ax.text(x + 0.48, y + 0.25, f"{own} own · {pts} pts", ha="center", va="center", fontsize=6.2, color=INK)
    ties = [w for w in weeks if w["tied"] > 1]
    tie_note = (" Ties at first: " + ", ".join(f"week {w['week']} ({w['tied']} entries)" for w in ties) + "."
                if ties else " No week had a tie at first.")
    sub = (f"The rank-1 lineup of each week's DraftKings Millionaire, {len(weeks)} weeks. Each cell: player, team, "
           f"salary, ownership and points.\n{OWN_NOTE} Green cells: under 5% owned; orange: 20% or more.\n"
           f"Stack = the quarterback plus his own teammates in the lineup; bring-back = an opponent of the QB too."
           f"{tie_note}")
    fig.tight_layout(rect=(0, 0, 1, 1 - 1.1 / height))
    return _finish(fig, path, "How the Millionaire was won, week by week", sub, height)


# ── 2b. winners then and now ─────────────────────────────────────────────────
def winners_history_chart(published: list[dict], current: list[dict], path: Path) -> bool:
    """``published``: published_summary() rows (2023-25); ``current``: {season, week, own} for this season."""
    rows = [r for r in published if r.get("own") is not None] + [r for r in current if r.get("own") is not None]
    seasons = sorted({r["season"] for r in rows})
    if len(rows) < 3 or len(seasons) < 2:
        return False
    height = 4.6
    fig, ax = _fig(height)
    _axes(ax, ygrid=False)
    rng = random.Random(3)
    for i, s in enumerate(seasons):
        vals = [r["own"] for r in rows if r["season"] == s]
        y = len(seasons) - 1 - i
        colour = SERIES[1] if s == max(seasons) and current else SERIES[0]
        ax.scatter(vals, [y + rng.uniform(-0.14, 0.14) for _ in vals], s=30, color=colour, alpha=0.8,
                   edgecolor=SURFACE, linewidth=0.5, zorder=3)
        med = st.median(vals)
        ax.plot([med, med], [y - 0.3, y + 0.3], color=INK, linewidth=1.6, zorder=4)
        ax.text(med, y + 0.34, f"median {med:.0f}%", ha="center", va="bottom", fontsize=7, color=INK)
    ax.set_yticks([len(seasons) - 1 - i for i in range(len(seasons))],
                  [f"{s} ({sum(r['season'] == s for r in rows)} weeks)" for s in seasons], fontsize=8, color=INK)
    ax.set_ylim(-0.6, len(seasons) - 0.2)
    xs = [r["own"] for r in rows]
    ax.set_xlim(min(xs) - 10, max(xs) + 10)
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.set_xlabel("Ownership total: the nine players' ownership added together", color=INK_2, fontsize=8)
    lo = min(xs)
    hi = max(xs)
    sub = (f"Each dot is one week's first-place DraftKings Millionaire lineup. Totals run from {lo:.0f}% to {hi:.0f}%: "
           "a lineup of nine players each in 10% of lineups adds to 90%.\n2023-25 from Hello Rookie's published "
           "winners (a week the page repeated is left out); 2026 from the contest results. "
           "One lineup a week is a small, hand-picked sample.")
    return _finish(fig, path, "How contrarian were Millionaire winners?", sub, height)


# ── 3. player-prop overs ─────────────────────────────────────────────────────
PROP_MARKETS = {  # market -> (label, nflverse stat field)
    "player_receptions": ("Receptions", "receptions"),
    "player_reception_yds": ("Receiving yards", "receiving_yards"),
    "player_rush_yds": ("Rushing yards", "rushing_yards"),
    "player_pass_yds": ("Passing yards", "passing_yards"),
    "player_pass_tds": ("Passing touchdowns", "passing_tds"),
    "player_pass_interceptions": ("Interceptions thrown", "passing_interceptions"),
}
SMALL_N = 200


def prop_chart(summaries: dict[str, dict], path: Path, weeks: tuple[int, int] | None = None) -> bool:
    """``summaries``: market -> over_summary() result."""
    rows = [(m, s) for m, s in summaries.items() if s]
    if not rows:
        return False
    rows.sort(key=lambda ms: -ms[1]["n"])
    height = 0.62 * len(rows) + 2.4
    fig, ax = _fig(height)
    _axes(ax, ygrid=False)
    ax.axvline(0.5, color=GRID, linewidth=1)
    ys = list(range(len(rows)))[::-1]
    for y, (m, s) in zip(ys, rows):
        lo, hi = s["ci"]
        ax.plot([lo, hi], [y, y], color=SERIES[0], alpha=0.35, linewidth=5, solid_capstyle="round")
        ax.scatter([s["over"]], [y], s=46, color=SERIES[0], zorder=4)
        ax.scatter([s["implied"]], [y], s=44, marker="D", facecolor=SURFACE, edgecolor=INK, linewidth=1.1, zorder=5)
        flag = "  small sample" if s["n"] < SMALL_N else ""
        glo, ghi = s["gap_ci"]
        ax.text(0.715, y + 0.12, f"Over {s['over']:.1%} vs {s['implied']:.1%} priced{flag}", fontsize=7, color=INK,
                va="center")
        ax.text(0.715, y - 0.16, f"gap {s['gap']:+.1%} (95% range {glo:+.1%} to {ghi:+.1%})", fontsize=6.6,
                color=INK_2, va="center")
    ax.set_yticks(ys, [f"{PROP_MARKETS.get(m, (m,))[0]}\n{s['n']} lines, {s['games']} games" for m, s in rows],
                  fontsize=7.5, color=INK)
    ax.set_xlim(0.28, 0.98)
    ax.set_xticks([0.3, 0.4, 0.5, 0.6, 0.7])
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}"))
    ax.set_xlabel("How often the over hit", color=INK_2, fontsize=8)
    ax.set_ylim(-0.6, len(rows) - 0.4)
    ax.scatter([], [], s=46, color=SERIES[0], label="Over hit rate (bar: 95% range, games resampled)")
    ax.scatter([], [], s=44, marker="D", facecolor=SURFACE, edgecolor=INK, label="Rate the price implied (vig removed)")
    ax.legend(loc="upper right", fontsize=7, frameon=False, bbox_to_anchor=(1, 1.0 + 0.55 / height))
    pushes = sum(s["pushes"] for _, s in rows)
    missing = sum(s["missing"] for _, s in rows)
    wk = f"weeks {weeks[0]}-{weeks[1]}" if weeks else "this season"
    rush = summaries.get("player_rush_yds")
    if rush and rush["gap_ci"][1] < 0:
        note = ("Rushing yards overs hit less often than their prices said, but six markets were read at once, "
                "so one gap like that is exploratory, not a finding.")
    else:
        note = f"{len(rows)} markets read at once: a lone gap is exploratory, not a finding."
    sub = (f"Closing DraftKings player-prop lines, {wk}, final games. {pushes} pushes and {missing} listed players with "
           f"no stat row (inactive or no touches) are left out.\n{note}")
    fig.tight_layout(rect=(0, 0, 1, 1 - 1.05 / height))
    return _finish(fig, path, "Did the player-prop overs hit as often as priced?", sub, height)


# ── loaders (SELECT only) ────────────────────────────────────────────────────
def _plain(v):
    if isinstance(v, str):
        try:
            return json.loads(v)
        except ValueError:
            return v
    return v


def load_millionaires(db, season: int = SEASON) -> list[dict]:
    """One flagship Classic Millionaire per week, stored from stat-api (the largest field if several)."""
    rows = db.execute(
        """SELECT DISTINCT ON (week) contest_id, contest_name, week, entry_count
           FROM nfl_dfs_field_contests
           WHERE season = %s AND format = 'classic' AND contest_name ILIKE '%%millionaire%%'
             AND file_name LIKE 'stat-api:%%'
           ORDER BY week, entry_count DESC NULLS LAST""", (season,))
    return [dict(r) for r in rows]


def load_build_points(db, contest_id: str) -> dict[str, dict]:
    """normalized name -> position, team, salary, points from the stat-api builds' exposure lists."""
    out: dict[str, dict] = {}
    for r in db.execute("SELECT exposure FROM nfl_dfs_field_user_builds WHERE contest_id = %s", (contest_id,)):
        for e in _plain(r["exposure"]) or []:
            key = "".join(ch for ch in str(e.get("name", "")).lower() if ch.isalnum())
            if key and key not in out:
                out[key] = {"position": e.get("position"), "team": e.get("team"), "salary": e.get("salary"),
                            "points": e.get("fantasy_points")}
    return out


def load_players(db, contest_id: str) -> tuple[list[dict], int]:
    own = [dict(r) for r in db.execute(
        "SELECT player_name, normalized_name, drafted_pct, fpts FROM nfl_dfs_field_ownership WHERE contest_id = %s",
        (contest_id,))]
    return scored_players(own, load_build_points(db, contest_id))


def load_winner(db, contest: dict) -> dict | None:
    entries = [dict(r) for r in db.execute(
        """SELECT entry_id, rank, username, points, lineup_text, players FROM nfl_dfs_field_top_entries
           WHERE contest_id = %s ORDER BY rank LIMIT 200""", (contest["contest_id"],))]
    top = first_place(entries)
    if not top:
        return None
    roster, stack = None, None
    builds = db.execute("SELECT lineups FROM nfl_dfs_field_user_builds WHERE contest_id = %s AND username = %s",
                        (contest["contest_id"], top["username"]))
    for b in builds:
        for lu in _plain(b["lineups"]) or []:
            if lu.get("rank") == top["rank"] and abs((lu.get("points") or 0) - (top.get("points") or 0)) < 0.01:
                roster = [{"slot": s[0], "name": s[1], "team": s[2], "position": s[3], "salary": s[4], "points": s[5],
                           "own": s[6]} for s in lu["roster"]]
                stack = lu.get("stack")
                break
    if roster is None:   # fall back to the entry's own seats and the contest's per-player points
        pts = {r["normalized_name"]: r["fpts"] for r in db.execute(
            "SELECT normalized_name, fpts FROM nfl_dfs_field_ownership WHERE contest_id = %s", (contest["contest_id"],))}
        roster = [{"slot": p["slot"], "name": p["name"], "team": None, "position": p["slot"], "salary": None,
                   "points": pts.get(p.get("normalized_name")), "own": p.get("drafted_pct")}
                  for p in _plain(top["players"]) or []]
    return {"week": contest["week"], "entries": contest.get("entry_count"), "roster": roster,
            "summary": lineup_summary(roster, stack), "tied": top["tied"],
            "distinct_lineups": top["distinct_lineups"], "username": top["username"]}


def load_published(db) -> list[dict]:
    rows = db.execute("SELECT season, week, players, total_points, flags FROM nfl_dfs_published_winners "
                      "WHERE rank = 1 ORDER BY season, week")
    return published_summary([{**dict(r), "players": _plain(r["players"]), "flags": _plain(r["flags"])} for r in rows])


def load_prop_rows(db, season: int = SEASON) -> tuple[dict[str, list[dict]], tuple[int, int] | None]:
    """market -> closing DraftKings lines of final games, each with the player's actual stat (None = no row)."""
    from model import nfl_prop_implied as pim
    from research.nfl_prop_implied import player_meta

    cutoff = datetime.now(timezone.utc) - timedelta(hours=5)   # a game this long after kickoff is final
    lines = db.execute(
        """SELECT week, player_key, market, line, over_price, under_price, event_id FROM nfl_prop_implied_lines
           WHERE season = %s AND capture_role = 'close' AND book = 'draftkings' AND conversion_version = %s
             AND market = ANY(%s) AND commence_time < %s AND line IS NOT NULL
             AND over_price IS NOT NULL AND under_price IS NOT NULL""",
        (season, pim.VERSION, list(PROP_MARKETS), cutoff))
    weeks = sorted({r["week"] for r in lines})
    metas = {w: player_meta(db, season, w) for w in weeks}
    out: dict[str, list[dict]] = collections.defaultdict(list)
    for r in lines:
        stats = (metas[r["week"]].get(r["player_key"]) or {}).get("stats")
        field = PROP_MARKETS[r["market"]][1]
        actual = None if not stats else float(stats.get(field) or 0)
        out[r["market"]].append({"line": float(r["line"]), "actual": actual, "event_id": r["event_id"],
                                 "p_over": pim.p_over(r["over_price"], r["under_price"])})
    return dict(out), ((weeks[0], weeks[-1]) if weeks else None)


def build_charts(out_dir: Path, db=None, season: int = SEASON) -> dict[str, bool]:
    """Run every query and write every chart; returns {file name: written}."""
    if db is None:
        from config import load_config
        from db.database import DatabaseManager
        db = DatabaseManager(load_config().database_url, initialize_schema=False)
    out_dir = Path(out_dir)
    made: dict[str, bool] = {}
    contests = load_millionaires(db, season)
    per_week = []
    for c in contests:
        players, missing = load_players(db, c["contest_id"])
        per_week.append((c, players, missing))
    latest = per_week[-1] if per_week else None
    made["nfl_take_crowd_vs_results.png"] = (
        crowd_chart(latest[1], out_dir / "nfl_take_crowd_vs_results.png", latest[0]["week"],
                    latest[0].get("entry_count"), latest[2]) if latest else False)
    splits = []
    for c, players, _ in per_week:
        s = chalk_split(players)
        if s:
            splits.append({"week": c["week"], **s})
    made["nfl_take_chalk_report.png"] = chalk_report_chart(splits, out_dir / "nfl_take_chalk_report.png")
    winners = [w for w in (load_winner(db, c) for c in contests) if w]
    made["nfl_take_winning_lineups.png"] = winning_lineups_chart(winners, out_dir / "nfl_take_winning_lineups.png")
    current = [{"season": season, "week": w["week"], "own": w["summary"]["own"]} for w in winners]
    made["nfl_take_winners_history.png"] = winners_history_chart(load_published(db), current,
                                                                 out_dir / "nfl_take_winners_history.png")
    props, weeks = load_prop_rows(db, season)
    summaries = {m: over_summary(rows) for m, rows in props.items()}
    made["nfl_take_prop_results.png"] = prop_chart(summaries, out_dir / "nfl_take_prop_results.png", weeks)
    return made


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    ap.add_argument("--season", type=int, default=SEASON)
    args = ap.parse_args(argv)
    made = build_charts(Path(args.out_dir), season=args.season)
    for name, ok in made.items():
        print(f"{Path(args.out_dir) / name}: {'written' if ok else 'skipped, no data'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
