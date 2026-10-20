"""NFL play-by-play reader charts: eight season-to-date stories for a newsletter.

Built from nflverse's play-by-play file (the same release model/nfl_market_scenarios.py
reads for late scores) plus its schedule file for coaches. Descriptive only: these
charts tell the season so far, and none of them claims a betting edge.

  nfl_take_luck_vs_skill.png       play-by-play efficiency against the scoreboard
  nfl_take_wp_swings.png           the four wildest games by win-probability swing
  nfl_take_close_games.png         one-score games against everything else
  nfl_take_turnover_luck.png       turnover margin against fumble-recovery share
  nfl_take_fourth_down.png         who goes for it on 4th-and-short in plus territory
  nfl_take_pass_rate_oe.png        passing more or less than the situation suggests
  nfl_take_red_zone_two_minute.png points per red-zone trip, and late-half scoring
  nfl_take_usage_shifts.png        target and carry share, early weeks against recent weeks

Definitions (stated on each chart too):
  * A "play" for EPA and pass rate is a pass or run (penalty-only plays, kicks,
    kneels and spikes excluded). EPA (expected points added) is how much a play
    changed the points the offense could expect from that down, distance and spot.
  * Red-zone trip: a drive with any snap at or inside the opponent 20. Its points
    are the offense's score change over the drive, so a touchdown counts 6, 7 or 8.
  * Last two minutes: the second and fourth quarters with 2:00 or less on the half
    clock; overtime is excluded. An extra point is credited to its touchdown's time.

    python -m model.nfl_play_charts --out-dir artifacts/nfl_play_charts [--season 2026]
"""

from __future__ import annotations

import argparse
import textwrap
import time
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

from model.cfb_pattern_charts import DPI, GRID, INK, INK_2, SERIES, SURFACE  # noqa: E402
from model.cfb_scenario_charts import _heading  # noqa: E402
from model.story_charts import BAD, GOOD, NEUTRAL, WIDTH_IN, _axes, _fig, _finish  # noqa: E402

VERSION = "nfl-play-charts-v1"
DEFAULT_DIR = Path("artifacts") / "nfl_play_charts"
CLOSE_MARGIN = 8           # one-score game: final margin of 8 or fewer
TWO_MIN = 120              # seconds left in the half
USAGE_WINDOW = 3           # weeks in each usage window
MIN_TARGET_VOLUME = 15     # targets across both windows to be charted
MIN_CARRY_VOLUME = 20      # carries across both windows to be charted

PBP_COLUMNS = [
    "game_id", "play_id", "season_type", "week", "home_team", "away_team", "home_score", "away_score",
    "posteam", "defteam", "qtr", "down", "ydstogo", "yardline_100", "half_seconds_remaining",
    "game_seconds_remaining", "play_type", "desc", "epa", "wp", "home_wp", "home_wp_post", "pass_oe",
    "fumble", "fumbled_1_team", "fumble_recovery_1_team", "interception", "fourth_down_converted",
    "fixed_drive", "posteam_score", "posteam_score_post", "total_home_score",
    "total_away_score", "receiver_player_id", "receiver_player_name", "rusher_player_id",
    "rusher_player_name", "qb_scramble", "two_point_attempt", "home_coach", "away_coach",
]
SCRIMMAGE = ("pass", "run")
SWING_PLAYS = ("pass", "run", "field_goal", "punt")
# Snaps that put a drive in the red zone; tries (snapped from the 15 or 2) and kickoffs never count.
RZ_SNAPS = ("pass", "run", "field_goal", "punt", "qb_kneel", "qb_spike", "no_play")


# ── loading ──────────────────────────────────────────────────────────────────
def fetch_pbp(season: int, attempts: int = 3, source: str | None = None):
    """Regular-season play-by-play for ``season`` with only the columns these charts use."""
    import pandas as pd

    from model.nfl_market_scenarios import pbp_url

    url = source or pbp_url(season)
    last = None
    for attempt in range(attempts):
        try:
            head = pd.read_csv(url, nrows=0, compression="gzip")
            missing = [c for c in PBP_COLUMNS if c not in head.columns]
            if missing:
                raise SystemExit(f"play-by-play {season} lacks columns these charts need: {missing}")
            frame = pd.read_csv(url, usecols=PBP_COLUMNS, compression="gzip", low_memory=False)
            return frame[frame["season_type"] == "REG"].reset_index(drop=True)
        except SystemExit:
            raise
        except Exception as exc:  # noqa: BLE001 - retried, then raised
            last = exc
            time.sleep(2 ** attempt)
    raise RuntimeError(f"nflverse play-by-play {season} download failed: {last}")


def latest_season(schedule_rows: list[dict]) -> int:
    seasons = [int(r["season"]) for r in schedule_rows
               if r.get("game_type") == "REG" and r.get("home_score") == r.get("home_score")
               and r.get("home_score") is not None]
    return max(seasons)


def coaches_from_schedule(schedule_rows: list[dict], season: int) -> dict[str, str]:
    """Each team's head coach in its most recent completed regular-season game."""
    out: dict[str, tuple] = {}
    for r in schedule_rows:
        if int(r["season"]) != season or r.get("game_type") != "REG":
            continue
        hs = r.get("home_score")
        if hs is None or hs != hs:
            continue
        key = (int(r["week"]), str(r.get("gameday")))
        for side in ("home", "away"):
            team, coach = r.get(f"{side}_team"), r.get(f"{side}_coach")
            if isinstance(coach, str) and (team not in out or key >= out[team][0]):
                out[team] = (key, coach)
    return {t: c for t, (_, c) in out.items()}


# ── shared computations ──────────────────────────────────────────────────────
def game_results(pbp) -> list[dict]:
    """One row per game: teams, final score, week."""
    out = []
    for gid, g in pbp.groupby("game_id", sort=False):
        first = g.iloc[0]
        out.append({"game_id": gid, "week": int(first["week"]), "home": first["home_team"],
                    "away": first["away_team"], "home_score": int(first["home_score"]),
                    "away_score": int(first["away_score"])})
    return out


def team_game_counts(games: list[dict]) -> dict[str, int]:
    n: dict[str, int] = defaultdict(int)
    for g in games:
        n[g["home"]] += 1
        n[g["away"]] += 1
    return dict(n)


def _plays(pbp):
    return pbp[pbp["play_type"].isin(SCRIMMAGE)]


def _note(games: list[dict]) -> str:
    weeks = [g["week"] for g in games]
    return f"{len(games)} games, weeks {min(weeks)}-{max(weeks)}"


def _tie_break(rows: list[dict], key: str, reverse: bool = True) -> list[dict]:
    return sorted(rows, key=lambda r: ((-r[key]) if reverse else r[key], r["team"]))


# ── 1. luck vs skill ─────────────────────────────────────────────────────────
def epa_rows(pbp, games: list[dict]) -> list[dict]:
    """Offense EPA/play, EPA/play allowed, their difference, and point differential per game."""
    plays = _plays(pbp)
    plays = plays[plays["epa"].notna()]
    off = plays.groupby("posteam")["epa"].agg(["mean", "size"])
    deff = plays.groupby("defteam")["epa"].mean()
    n = team_game_counts(games)
    pdiff: dict[str, int] = defaultdict(int)
    for g in games:
        pdiff[g["home"]] += g["home_score"] - g["away_score"]
        pdiff[g["away"]] += g["away_score"] - g["home_score"]
    out = []
    for team in sorted(n):
        if team not in off.index or team not in deff.index:
            continue
        o, d = float(off.loc[team, "mean"]), float(deff.loc[team])
        out.append({"team": team, "off_epa": o, "def_epa": d, "epa_diff": o - d, "plays": int(off.loc[team, "size"]),
                    "games": n[team], "pd_pg": pdiff[team] / n[team]})
    return out


def _pre_layout(fig, sub: str, height: float, **kw) -> None:
    """Lay the figure out exactly as ``_finish`` will, so labels placed in between keep their spots."""
    fig.tight_layout(rect=(0, 0, 1, 1 - (0.5 + 0.15 * (sub.count("\n") + 1)) / height), **kw)


def _fit(xs, ys):
    slope, icept = np.polyfit(xs, ys, 1)
    return float(slope), float(icept)


_OFFSETS = [(7, 5), (7, -11), (-7, 5), (-7, -11), (0, 9), (0, -15), (12, 0), (-12, 0), (14, 14), (-14, 14),
            (14, -20), (-14, -20), (22, 6), (-22, 6), (22, -14), (-22, -14), (0, 22), (0, -28), (30, 0), (-30, 0)]


def label_all(ax, points: list[tuple[float, float, str]], dots=(), fontsize: float = 6.8) -> int:
    """Label every point: first collision-free nearby offset, else the least-crowded one.

    Returns how many labels needed the fallback (overlapping something)."""
    from matplotlib.transforms import Bbox

    fig = ax.figure
    renderer = fig.canvas.get_renderer()
    frame = ax.get_window_extent(renderer)
    r = 2.6 * fig.dpi / 72
    dot_boxes = []
    for dx_, dy_ in dots:
        px, py = ax.transData.transform((dx_, dy_))
        dot_boxes.append(((dx_, dy_), Bbox.from_extents(px - r, py - r, px + r, py + r)))
    placed = []
    forced = 0
    for x, y, text in points:
        best = None
        for dx, dy in _OFFSETS:
            ann = ax.annotate(text, (x, y), xytext=(dx, dy), textcoords="offset points", fontsize=fontsize,
                              color=INK, ha="left" if dx > 0 else "right" if dx < 0 else "center",
                              arrowprops={"arrowstyle": "-", "color": INK_2, "lw": 0.4, "shrinkA": 1, "shrinkB": 3})
            ann.set_in_layout(False)
            box = ann.get_window_extent(renderer).expanded(1.04, 1.1)
            inside = box.x0 >= frame.x0 and box.x1 <= frame.x1 and box.y0 >= frame.y0 and box.y1 <= frame.y1
            hits = sum(box.overlaps(b) for xy, b in dot_boxes if abs(xy[0] - x) > 1e-9 or abs(xy[1] - y) > 1e-9)
            hits += 3 * sum(box.overlaps(b) for b in placed)
            score = hits + (100 if not inside else 0)
            if score == 0:
                placed.append(box)
                best = None
                break
            if best is None or score < best[0]:
                best = (score, dx, dy, box)
            ann.remove()
        else:
            _, dx, dy, box = best
            ann = ax.annotate(text, (x, y), xytext=(dx, dy), textcoords="offset points", fontsize=fontsize,
                              color=INK, ha="left" if dx > 0 else "right" if dx < 0 else "center",
                              arrowprops={"arrowstyle": "-", "color": INK_2, "lw": 0.4, "shrinkA": 1, "shrinkB": 3})
            ann.set_in_layout(False)
            placed.append(box)
            forced += 1
    return forced


def luck_vs_skill_chart(rows: list[dict], path: Path, note: str) -> bool:
    if len(rows) < 3:
        return False
    xs = np.array([r["epa_diff"] for r in rows])
    ys = np.array([r["pd_pg"] for r in rows])
    slope, icept = _fit(xs, ys)
    resid = {r["team"]: r["pd_pg"] - (slope * r["epa_diff"] + icept) for r in rows}
    height = 7.2
    fig, ax = _fig(height)
    _axes(ax)
    ax.axhline(0, color=INK_2, linewidth=0.7)
    ax.axvline(0, color=INK_2, linewidth=0.7)
    gx = np.array([xs.min() - 0.02, xs.max() + 0.02])
    ax.plot(gx, slope * gx + icept, color=SERIES[1], linewidth=1.5, zorder=2)
    lucky = sorted(rows, key=lambda r: -resid[r["team"]])[:3]
    unlucky = sorted(rows, key=lambda r: resid[r["team"]])[:3]
    flagged = {r["team"]: GOOD for r in lucky} | {r["team"]: BAD for r in unlucky}
    ax.scatter(xs, ys, s=26, c=[flagged.get(r["team"], SERIES[0]) for r in rows], alpha=0.85, edgecolor=SURFACE,
               linewidth=0.6, zorder=3)
    ax.margins(x=0.08, y=0.08)
    ax.set_xlabel("Efficiency gap: offense EPA per play minus EPA per play allowed", color=INK_2, fontsize=8)
    ax.set_ylabel("Point differential per game", color=INK_2, fontsize=8)
    fmt = lambda rs: ", ".join(f"{r['team']} ({resid[r['team']]:+.1f})" for r in rs)  # noqa: E731
    corr = float(np.corrcoef(xs, ys)[0, 1])
    sub = (f"{note}. EPA (expected points added) grades every pass and run by how much it changed the points the "
           "offense could expect;\nthe gap is a team's offense per play minus what its defense allowed per play. "
           f"Orange line: the usual scoreboard for that gap (r = {corr:+.2f}).\nGreen, furthest above it: scoring "
           f"more than their play suggests (points a game above the line): {fmt(lucky)}.\nRed, furthest below: "
           f"playing better than their scoreboard: {fmt(unlucky)}.")
    _pre_layout(fig, sub, height)
    label_all(ax, [(r["epa_diff"], r["pd_pg"], r["team"]) for r in rows], list(zip(xs, ys)))
    return _finish(fig, path, "Who is better than their record?", sub, height)


# ── 2. win-probability swings ────────────────────────────────────────────────
def _elapsed(qtr, left) -> float:
    """Minutes since kickoff; overtime continues past 60."""
    return (3600 - left) / 60 if qtr <= 4 else (3600 + 600 - left) / 60


def _clock(qtr, half_left) -> str:
    q = int(qtr)
    left = half_left - (900 if q in (1, 3) else 0)
    left = max(0, int(round(left)))
    return f"{'OT' if q >= 5 else f'Q{q}'} {left // 60}:{left % 60:02d}"


def wp_swing_games(pbp, games: list[dict], n: int = 4, n_swings: int = 3) -> list[dict]:
    """The ``n`` games whose eventual winner fell lowest in win probability, with their biggest single plays."""
    results = {g["game_id"]: g for g in games}
    out = []
    for gid, g in pbp.groupby("game_id", sort=False):
        res = results.get(gid)
        if res is None or res["home_score"] == res["away_score"]:
            continue
        g = g[g["home_wp"].notna() & g["game_seconds_remaining"].notna()].sort_values("play_id")
        if g.empty:
            continue
        home_won = res["home_score"] > res["away_score"]
        wp = g["home_wp"].to_numpy(dtype=float)
        low = float(wp.min() if home_won else (1 - wp).min())
        t = [_elapsed(q, s) for q, s in zip(g["qtr"], g["game_seconds_remaining"])]
        swings = []
        sign = 1.0 if home_won else -1.0
        for i, row in enumerate(g.itertuples(index=False)):
            post = row.home_wp_post
            # Only real snaps and kicks from scrimmage; the model's jumps on tries and kickoffs are bookkeeping.
            if post != post or row.play_type not in SWING_PLAYS:
                continue
            delta = float(post) - float(row.home_wp)
            # The new value shows on the line from the next play, so the ring sits there.
            t_after = t[i + 1] if i + 1 < len(t) else t[i]
            swings.append({"t": t_after, "wp": float(post), "delta": delta, "winner_delta": sign * delta,
                           "clock": _clock(row.qtr, row.half_seconds_remaining), "desc": str(row.desc)})
        swings.sort(key=lambda s: -abs(s["delta"]))
        out.append({**res, "low": low, "winner": res["home"] if home_won else res["away"],
                    "t": t + [t[-1] if t else 0], "wp": list(wp) + [1.0 if home_won else 0.0],
                    "swings": swings[:n_swings]})
    out.sort(key=lambda r: (r["low"], r["game_id"]))
    return out[:n]


def _short_desc(desc: str, width: int = 70) -> str:
    """A play description trimmed for a caption: clock, formation, jersey numbers and kick-unit credits removed."""
    import re

    d = re.sub(r"\((Shotgun|No Huddle|No Huddle, Shotgun|Pistol)\)", "", desc)
    for _ in range(2):
        d = re.sub(r"^\s*\(\s*\d*:\d+\s*\)\s*", "", d)
    d = re.sub(r"TWO-POINT CONVERSION ATTEMPT\.\s*", "2-pt try: ", d)
    d = re.sub(r",?\s*Center-.*$", "", d)
    d = re.sub(r"\s*\*\*.*$", "", d)          # injury notes appended to the play
    d = re.sub(r"\s*\([^)]*\)", "", d)          # tacklers and other parentheticals
    d = re.sub(r"\b\d{1,2}-(?=[A-Z])", "", d)
    d = re.sub(r"\s+", " ", d).strip()
    return textwrap.shorten(d, width=width, placeholder="...")


def wp_swings_chart(games: list[dict], path: Path, note: str) -> bool:
    if not games:
        return False
    height = 8.6
    fig, axes = plt.subplots(2, 2, figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    flat = list(np.ravel(axes))
    for ax, g in zip(flat, games):
        _axes(ax, xgrid=False)
        ax.axhline(0.5, color=INK_2, linewidth=0.6, linestyle=":")
        ax.fill_between(g["t"], 0.5, g["wp"], where=[w >= 0.5 for w in g["wp"]], color=SERIES[0], alpha=0.12,
                        linewidth=0, step="post")
        ax.fill_between(g["t"], 0.5, g["wp"], where=[w < 0.5 for w in g["wp"]], color=SERIES[1], alpha=0.12,
                        linewidth=0, step="post")
        ax.step(g["t"], g["wp"], where="post", color=INK, linewidth=1.1)
        end = max(60, g["t"][-1])
        ax.set_xlim(0, end + 0.9)
        ax.set_ylim(-0.02, 1.02)
        ax.set_xticks([0, 15, 30, 45, 60] + ([end] if end > 61 else []))
        ax.set_xticklabels(["Kick", "Q2", "Half", "Q4", "60'"] + (["OT"] if end > 61 else []), fontsize=7)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}"))
        ax.set_yticks([0, 0.25, 0.5, 0.75, 1])
        ax.set_ylabel(f"{g['home']} win chance", color=INK_2, fontsize=7.5)
        ax.text(0.01, 0.97, g["home"], transform=ax.transAxes, fontsize=7, color=SERIES[0], va="top", fontweight="bold")
        ax.text(0.01, 0.03, g["away"], transform=ax.transAxes, fontsize=7, color=SERIES[1], va="bottom",
                fontweight="bold")
        title = (f"Week {g['week']}: {g['away']} {g['away_score']} at {g['home']} {g['home_score']}\n"
                 f"{g['winner']} won after falling to {g['low']:.1%}")
        ax.set_title(title, loc="left", fontsize=8.5, color=INK)
        lines = []
        for i, s in enumerate(g["swings"], 1):
            ax.scatter([s["t"]], [s["wp"]], s=60, facecolor=SURFACE, edgecolor=SERIES[7], linewidth=1.2, zorder=4)
            ax.text(s["t"], s["wp"], str(i), fontsize=6.5, ha="center", va="center", color=SERIES[7],
                    fontweight="bold", zorder=5)
            lines.append(f"{i}. {s['clock']}, {g['winner']} {s['winner_delta'] * 100:+.0f} pts: "
                         f"{_short_desc(s['desc'])}")
        ax.text(0, -0.13, "\n".join(lines), transform=ax.transAxes, fontsize=6.2, color=INK_2, va="top", ha="left")
    for ax in flat[len(games):]:
        ax.set_visible(False)
    sub = (f"{note}. The four games whose winner looked most beaten at some point. The line is the home team's win "
           "chance (nflverse's win-probability model) after each play.\nNumbered rings: the three single plays that "
           "moved it most, described below each game as the change in the winner's chance (percentage points).")
    fig.subplots_adjust(left=0.07, right=0.985, top=1 - 1.2 / height, bottom=0.75 / height, hspace=0.62, wspace=0.16)
    _heading(fig, "The season's wildest comebacks", sub)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)
    return True


# ── 3. close games ───────────────────────────────────────────────────────────
def close_game_rows(games: list[dict], margin: int = CLOSE_MARGIN) -> list[dict]:
    """Win share (ties count half) in games decided by ``margin`` or fewer points and in the rest."""
    acc: dict[str, dict] = defaultdict(lambda: {"cw": 0.0, "cn": 0, "ow": 0.0, "on": 0, "cl": 0, "ct": 0})
    for g in games:
        m = g["home_score"] - g["away_score"]
        for team, mine in ((g["home"], m), (g["away"], -m)):
            a = acc[team]
            won = 1.0 if mine > 0 else 0.5 if mine == 0 else 0.0
            if abs(m) <= margin:
                a["cn"] += 1
                a["cw"] += won
                a["cl"] += mine < 0
                a["ct"] += mine == 0
            else:
                a["on"] += 1
                a["ow"] += won
    out = []
    for team, a in sorted(acc.items()):
        out.append({"team": team, "close_n": a["cn"], "close_w": a["cw"], "close_l": a["cl"], "close_t": a["ct"],
                    "close_pct": a["cw"] / a["cn"] if a["cn"] else None,
                    "other_n": a["on"], "other_w": a["ow"], "other_pct": a["ow"] / a["on"] if a["on"] else None})
    return out


def _record(w: float, n: int, ties: int = 0) -> str:
    wins = int(w - ties / 2 + 1e-9)
    losses = n - wins - ties
    return f"{wins}-{losses}" + (f"-{ties}" if ties else "")


def close_games_chart(rows: list[dict], path: Path, note: str) -> bool:
    rows = [r for r in rows if r["close_n"] or r["other_n"]]
    if len(rows) < 2:
        return False
    rows = sorted(rows, key=lambda r: (-(r["close_pct"] if r["close_pct"] is not None else -1), -r["close_n"], r["team"]))
    n = len(rows)
    height = 0.215 * n + 1.9
    fig, ax = _fig(height)
    _axes(ax, ygrid=False)
    ys = np.arange(n)[::-1]
    for y, r in zip(ys, rows):
        c, o = r["close_pct"], r["other_pct"]
        if c is not None and o is not None:
            ax.plot([o, c], [y, y], color=GRID, linewidth=2.4, zorder=1, solid_capstyle="round")
        if o is not None:
            ax.scatter([o], [y], s=28, color=NEUTRAL, zorder=2)
        if c is not None:
            ax.scatter([c], [y], s=34, color=SERIES[0], zorder=3, alpha=1 if r["close_n"] >= 3 else 0.45)
        ct = _record(r["close_w"], r["close_n"], r["close_t"]) if r["close_n"] else "none"
        ot = _record(r["other_w"], r["other_n"]) if r["other_n"] else "none"
        ax.text(1.06, y, f"{ct}", fontsize=6.6, color=SERIES[0], va="center", ha="left")
        ax.text(1.2, y, f"{ot}", fontsize=6.6, color=INK_2, va="center", ha="left")
    ax.text(1.06, n - 0.2, "one-score", fontsize=6.6, color=SERIES[0], fontweight="bold", ha="left")
    ax.text(1.2, n - 0.2, "others", fontsize=6.6, color=INK_2, fontweight="bold", ha="left")
    ax.set_yticks(ys)
    ax.set_yticklabels([r["team"] for r in rows], fontsize=7)
    ax.set_xlim(-0.04, 1.34)
    ax.set_ylim(-0.8, n + 0.4)
    ax.set_xticks([0, 0.25, 0.5, 0.75, 1])
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}"))
    ax.axvline(0.5, color=INK_2, linewidth=0.6, linestyle=":")
    ax.set_xlabel("Win share (a tie counts as half a win)", color=INK_2, fontsize=8)
    close = sum(r["close_n"] for r in rows) // 2
    games = (sum(r["close_n"] + r["other_n"] for r in rows)) // 2
    sub = (f"{note}. Blue: win share in one-score games (final margin {CLOSE_MARGIN} points or fewer); grey: all "
           f"other games. {close} of {games} games were one-score games.\nRecords at right. Faded blue: fewer than 3 "
           "one-score games. A one-score game can turn on a single play.")
    return _finish(fig, path, "Close games: who has been winning the coin flips", sub, height)


# ── 4. turnover luck ─────────────────────────────────────────────────────────
def turnover_rows(pbp, games: list[dict]) -> list[dict]:
    """Takeaways minus giveaways per game, and the share of all recovered fumbles in a team's games it recovered.

    A giveaway is an interception thrown or a fumble recovered by the other team;
    penalty-nullified plays are left out.
    """
    live = pbp[pbp["play_type"] != "no_play"]
    give: dict[str, int] = defaultdict(int)
    take: dict[str, int] = defaultdict(int)
    for row in live[live["interception"] == 1].itertuples(index=False):
        give[row.posteam] += 1
        take[row.defteam] += 1
    fumbles = live[(live["fumble"] == 1) & live["fumble_recovery_1_team"].notna() & live["fumbled_1_team"].notna()]
    in_games: dict[str, int] = defaultdict(int)
    rec: dict[str, int] = defaultdict(int)
    for row in fumbles.itertuples(index=False):
        for t in (row.home_team, row.away_team):
            in_games[t] += 1
        rec[row.fumble_recovery_1_team] += 1
        if row.fumble_recovery_1_team != row.fumbled_1_team:
            give[row.fumbled_1_team] += 1
            take[row.fumble_recovery_1_team] += 1
    n = team_game_counts(games)
    out = []
    for team in sorted(n):
        out.append({"team": team, "games": n[team], "giveaways": give[team], "takeaways": take[team],
                    "margin_pg": (take[team] - give[team]) / n[team], "fumbles": in_games[team],
                    "recovered": rec[team], "rec_share": rec[team] / in_games[team] if in_games[team] else None})
    return out


def turnover_luck_chart(rows: list[dict], path: Path, note: str) -> bool:
    rows = [r for r in rows if r["rec_share"] is not None]
    if len(rows) < 3:
        return False
    xs = np.array([r["margin_pg"] for r in rows])
    ys = np.array([r["rec_share"] for r in rows])
    height = 6.8
    fig, ax = _fig(height)
    _axes(ax)
    ax.axhline(0.5, color=INK_2, linewidth=0.9, linestyle="--")
    ax.axvline(0, color=INK_2, linewidth=0.7)
    ax.scatter(xs, ys, s=26, c=[GOOD if y > 0.6 else BAD if y < 0.4 else SERIES[0] for y in ys], alpha=0.85,
               edgecolor=SURFACE, linewidth=0.6, zorder=3)
    ax.margins(x=0.08, y=0.08)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}"))
    ax.set_xlabel("Turnover margin per game (takeaways minus giveaways)", color=INK_2, fontsize=8)
    ax.set_ylabel("Share of all fumbles in its games that the team recovered", color=INK_2, fontsize=8)
    ax.text(0.99, 0.5, "50%: a coin flip", transform=ax.get_yaxis_transform(), fontsize=6.8, color=INK_2,
            ha="right", va="bottom")
    corr = float(np.corrcoef(xs, ys)[0, 1])
    total = sum(r["fumbles"] for r in rows) // 2
    lo, hi = min(r["fumbles"] for r in rows), max(r["fumbles"] for r in rows)
    sub = (f"{note}; {total} fumbles with a recovery, {lo}-{hi} in each team's games, its own and its opponents'. "
           "Who ends up on a loose ball is mostly\nchance, so a team far from 50% (green above 60%, red below 40%) "
           f"has had bounces feed its turnover margin. Correlation between the two: r = {corr:+.2f}.")
    _pre_layout(fig, sub, height)
    label_all(ax, [(r["margin_pg"], r["rec_share"], r["team"]) for r in rows], list(zip(xs, ys)))
    return _finish(fig, path, "Turnover margin and the bounce of the ball", sub, height)


# ── 5. fourth down ───────────────────────────────────────────────────────────
def fourth_down_chances(pbp):
    """4th-and-1 or -2 between the opponent 50 and 5, outside the last two minutes of each half, no penalty plays."""
    m = ((pbp["down"] == 4) & pbp["ydstogo"].isin([1, 2]) & (pbp["yardline_100"] <= 50) & (pbp["yardline_100"] >= 5)
         & (pbp["half_seconds_remaining"] > TWO_MIN) & (pbp["qtr"] <= 4)
         & pbp["play_type"].isin(["pass", "run", "punt", "field_goal"]))
    return pbp[m]


def fourth_down_rows(pbp, coaches: dict[str, str] | None = None) -> list[dict]:
    chances = fourth_down_chances(pbp)
    out = []
    for team, g in chances.groupby("posteam"):
        go = g[g["play_type"].isin(SCRIMMAGE)]
        conv = int((go["fourth_down_converted"] == 1).sum())
        out.append({"team": team, "chances": len(g), "go": len(go), "converted": conv,
                    "go_rate": len(go) / len(g), "conv_rate": conv / len(go) if len(go) else None,
                    "coach": (coaches or {}).get(team)})
    return out


def fourth_down_chart(rows: list[dict], path: Path, note: str, min_chances: int = 3,
                      teams: list[str] | None = None) -> bool:
    if len(rows) < 2:
        return False
    missing = sorted(set(teams or []) - {r["team"] for r in rows})
    rows = sorted(rows, key=lambda r: (-r["go_rate"], -r["chances"], r["team"]))
    n = len(rows)
    height = 0.215 * n + 1.9
    fig, ax = _fig(height)
    _axes(ax, ygrid=False)
    ys = np.arange(n)[::-1]
    league = sum(r["go"] for r in rows) / sum(r["chances"] for r in rows)
    for y, r in zip(ys, rows):
        alpha = 1 if r["chances"] >= min_chances else 0.4
        ax.barh(y, r["go_rate"], height=0.68, color=SERIES[0], alpha=alpha, linewidth=0)
        conv = f", converted {r['converted']}/{r['go']}" if r["go"] else ""
        ax.text(r["go_rate"] + 0.012, y, f"went {r['go']}/{r['chances']}{conv}", fontsize=6.4, color=INK_2,
                va="center", zorder=4, bbox={"facecolor": SURFACE, "edgecolor": "none", "pad": 0.6, "alpha": 0.9})
    ax.axvline(league, color=SERIES[1], linewidth=1.1, linestyle="--", zorder=1)
    ax.text(league, n - 0.1, f"league {league:.0%}", fontsize=6.8, color=SERIES[1], ha="center", va="bottom")
    ax.set_yticks(ys)
    ax.set_yticklabels([f"{r['team']}  {r['coach'] or ''}".rstrip() for r in rows], fontsize=6.8)
    ax.set_xlim(0, 1.32)
    ax.set_ylim(-0.8, n + 0.6)
    ax.set_xticks([0, 0.25, 0.5, 0.75, 1])
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}"))
    ax.set_xlabel("Share of chances where the offense ran a play instead of punting or kicking", color=INK_2,
                  fontsize=8)
    total = sum(r["chances"] for r in rows)
    went = sum(r["go"] for r in rows)
    conv = sum(r["converted"] for r in rows)
    none = f" {', '.join(missing)} had no such chance." if missing else ""
    sub = (f"{note}; {total} chances, {went} go-for-its, {conv} converted ({conv / went:.0%}).{none}\nA chance: 4th-and-1 or 4th-and-2 between the opponent's 50 and 5-yard lines, "
           "outside the last two minutes of each half; penalty-only plays left out.\nFaded: fewer than "
           f"{min_chances} chances. This shows what coaches chose, not what they should have done.")
    return _finish(fig, path, "Who goes for it on 4th-and-short", sub, height)


# ── 6. pass rate over expected ───────────────────────────────────────────────
def proe_filter(pbp):
    m = (pbp["down"].isin([1, 2, 3]) & (pbp["wp"] >= 0.2) & (pbp["wp"] <= 0.8)
         & (pbp["half_seconds_remaining"] > TWO_MIN) & (pbp["qtr"] <= 4) & pbp["pass_oe"].notna()
         & pbp["play_type"].isin(SCRIMMAGE))
    return pbp[m]


def proe_rows(pbp) -> list[dict]:
    plays = proe_filter(pbp)
    out = []
    for team, g in plays.groupby("posteam"):
        out.append({"team": team, "plays": len(g), "proe": float(g["pass_oe"].mean()),
                    "pass_rate": float((g["play_type"] == "pass").mean())})
    return out


def pass_rate_oe_chart(rows: list[dict], path: Path, note: str) -> bool:
    if len(rows) < 2:
        return False
    rows = sorted(rows, key=lambda r: (-r["proe"], r["team"]))
    n = len(rows)
    height = 0.215 * n + 1.9
    fig, ax = _fig(height)
    _axes(ax, ygrid=False)
    ys = np.arange(n)[::-1]
    lim = max(abs(r["proe"]) for r in rows) * 1.35 + 1
    for y, r in zip(ys, rows):
        v = r["proe"]
        ax.barh(y, v, height=0.68, color=SERIES[0] if v >= 0 else SERIES[1], linewidth=0)
        ax.text(v + (0.25 if v >= 0 else -0.25), y, f"{v:+.1f}  ({r['pass_rate']:.0%} passes)", fontsize=6.4,
                color=INK_2, va="center", ha="left" if v >= 0 else "right", zorder=4,
                bbox={"facecolor": SURFACE, "edgecolor": "none", "pad": 0.6, "alpha": 0.9})
    ax.axvline(0, color=INK_2, linewidth=0.8)
    league = sum(r["proe"] * r["plays"] for r in rows) / sum(r["plays"] for r in rows)
    ax.axvline(league, color=INK, linewidth=0.9, linestyle="--", zorder=3)
    ax.text(league, n - 0.35, f"league {league:+.1f}", fontsize=6.6, color=INK_2, ha="right", va="bottom")
    ax.set_yticks(ys)
    ax.set_yticklabels([r["team"] for r in rows], fontsize=7)
    ax.set_xlim(-lim, lim)
    ax.set_ylim(-0.8, n + 0.5)
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: "0" if abs(v) < 1e-9 else f"{v:+.0f}"))
    ax.set_xlabel("Pass rate over expected, percentage points", color=INK_2, fontsize=8)
    plays = sum(r["plays"] for r in rows)
    sub = (f"{note}; {plays:,} plays. For each play nflverse's model estimates how often teams pass in that exact "
           "spot\n(down, distance, field position, score, time). 'Over expected' is how much more (blue) or less "
           "(orange) often a team actually threw.\nDowns 1-3, game still in doubt (win chance 20-80%), outside the "
           "last two minutes of each half. Dashed: league average this season\n(the model learned from "
           "past seasons, so this year's league as a whole need not average zero).")
    return _finish(fig, path, "Who throws more than the situation calls for", sub, height)


# ── 7. red zone and two-minute scoring ───────────────────────────────────────
def red_zone_trips(pbp) -> list[dict]:
    """One row per drive with a snap at or inside the opponent 20: team and the points it scored on the drive."""
    plays = pbp[pbp["posteam"].notna() & pbp["fixed_drive"].notna()]
    out = []
    for (gid, drive, team), g in plays.groupby(["game_id", "fixed_drive", "posteam"], sort=False):
        snaps = g[g["play_type"].isin(RZ_SNAPS)]
        if not (snaps["yardline_100"] <= 20).any():
            continue
        before = g["posteam_score"].dropna()
        after = g["posteam_score_post"].dropna()
        if before.empty or after.empty:
            continue
        out.append({"game_id": gid, "drive": drive, "team": team, "points": int(after.max() - before.min())})
    return out


def two_minute_points(pbp) -> dict[str, int]:
    """Points each team scored in the last two minutes of the 2nd and 4th quarters (try credited to its touchdown)."""
    pts: dict[str, int] = defaultdict(int)
    for _, g in pbp.sort_values(["game_id", "play_id"]).groupby("game_id", sort=False):
        prev_h = prev_a = 0
        last_time = None
        for row in g.itertuples(index=False):
            h, a = row.total_home_score, row.total_away_score
            if h != h or a != a:
                continue
            dh, da = int(h) - prev_h, int(a) - prev_a
            prev_h, prev_a = int(h), int(a)
            is_try = row.play_type == "extra_point" or row.two_point_attempt == 1
            when = last_time if is_try and last_time is not None else (row.qtr, row.half_seconds_remaining)
            if dh or da:
                if not is_try:
                    last_time = (row.qtr, row.half_seconds_remaining)
                q, left = when
                if q in (2, 4) and left == left and left <= TWO_MIN:
                    pts[row.home_team] += max(dh, 0)
                    pts[row.away_team] += max(da, 0)
    return dict(pts)


def red_zone_rows(pbp, games: list[dict]) -> list[dict]:
    trips: dict[str, list[int]] = defaultdict(list)
    for t in red_zone_trips(pbp):
        trips[t["team"]].append(t["points"])
    late = two_minute_points(pbp)
    n = team_game_counts(games)
    out = []
    for team in sorted(n):
        p = trips.get(team, [])
        out.append({"team": team, "games": n[team], "trips": len(p), "rz_points": sum(p),
                    "rz_ppt": sum(p) / len(p) if p else None, "td_trips": sum(1 for x in p if x >= 6),
                    "two_min": late.get(team, 0), "two_min_pg": late.get(team, 0) / n[team]})
    return out


def red_zone_two_minute_chart(rows: list[dict], path: Path, note: str) -> bool:
    rows = [r for r in rows if r["games"]]
    if len(rows) < 2:
        return False
    n = len(rows)
    height = 0.215 * n + 2.1
    fig, (a1, a2) = _fig(height, 2)
    ys = np.arange(n)[::-1]
    rz = sorted([r for r in rows if r["rz_ppt"] is not None], key=lambda r: (-r["rz_ppt"], r["team"]))
    lg_rz = sum(r["rz_points"] for r in rz) / sum(r["trips"] for r in rz)
    _axes(a1, ygrid=False)
    for y, r in zip(ys, rz):
        a1.barh(y, r["rz_ppt"], height=0.68, color=SERIES[0], linewidth=0)
        a1.text(r["rz_ppt"] + 0.08, y, f"{r['rz_ppt']:.1f} ({r['td_trips']} TD / {r['trips']})", fontsize=6.2,
                color=INK_2, va="center", zorder=4, bbox={"facecolor": SURFACE, "edgecolor": "none", "pad": 0.6, "alpha": 0.9})
    a1.axvline(lg_rz, color=SERIES[1], linewidth=1, linestyle="--", zorder=1)
    a1.text(lg_rz, n - 0.1, f"league {lg_rz:.1f}", fontsize=6.6, color=SERIES[1], ha="center", va="bottom")
    a1.set_yticks(ys[:len(rz)])
    a1.set_yticklabels([r["team"] for r in rz], fontsize=6.8)
    a1.set_xlim(0, max(r["rz_ppt"] for r in rz) * 1.45)
    a1.set_ylim(-0.8, n + 0.6)
    a1.set_title("Points per red-zone trip", loc="left", fontsize=9, color=INK)
    a1.set_xlabel("Points per drive reaching the opponent 20 (TD / trips)", color=INK_2, fontsize=7.5)
    late = sorted(rows, key=lambda r: (-r["two_min_pg"], r["team"]))
    lg_late = sum(r["two_min"] for r in late) / sum(r["games"] for r in late)
    _axes(a2, ygrid=False)
    for y, r in zip(ys, late):
        a2.barh(y, r["two_min_pg"], height=0.68, color=SERIES[2], linewidth=0)
        a2.text(r["two_min_pg"] + 0.08, y, f"{r['two_min_pg']:.1f} ({r['two_min']} in {r['games']})", fontsize=6.2,
                color=INK_2, va="center", zorder=4, bbox={"facecolor": SURFACE, "edgecolor": "none", "pad": 0.6, "alpha": 0.9})
    a2.axvline(lg_late, color=SERIES[1], linewidth=1, linestyle="--", zorder=1)
    a2.text(lg_late, n - 0.1, f"league {lg_late:.1f}", fontsize=6.6, color=SERIES[1], ha="center", va="bottom")
    a2.set_yticks(ys)
    a2.set_yticklabels([r["team"] for r in late], fontsize=6.8)
    a2.set_xlim(0, max(max(r["two_min_pg"] for r in late) * 1.5, 1))
    a2.set_ylim(-0.8, n + 0.6)
    a2.set_title("Points in the last two minutes of each half", loc="left", fontsize=9, color=INK)
    a2.set_xlabel("Points per game in the final 2:00 of each half (points / games)", color=INK_2, fontsize=7.5)
    trips = sum(r["trips"] for r in rz)
    sub = (f"{note}. Left: {trips} drives that reached the opponent 20; the drive's points include the extra point "
           "or two-point try, so a touchdown counts 6-8.\nRight: every point a team scored with 2:00 or less left in "
           "a half, defense and special teams included; overtime excluded. Dashed: league average.")
    return _finish(fig, path, "Finishing drives, and finishing halves", sub, height)


# ── 8. usage shifts ──────────────────────────────────────────────────────────
def usage_rows(pbp, kind: str = "targets", window: int = USAGE_WINDOW, min_volume: int | None = None) -> list[dict]:
    """Each player's share of his team's targets (or carries) in the first ``window`` weeks and the latest ``window``.

    Targets: passes with a listed receiver. Carries: designed runs (scrambles and
    kneels excluded). A share counts only the weeks the player touched the ball at
    least once for that team (a target or a carry), so an injury, a benching for a
    whole game or a bye is not read as a role change; a player needs
    ``min_active`` such weeks in each window to be included.
    """
    weeks = sorted(int(w) for w in pbp["week"].dropna().unique())
    if len(weeks) < 2:
        return []
    k = min(window, len(weeks) // 2)
    early, late = set(weeks[:k]), set(weeks[-k:])
    min_active = min(2, k)
    targets = pbp[(pbp["play_type"] == "pass") & pbp["receiver_player_id"].notna()]
    carries = pbp[(pbp["play_type"] == "run") & pbp["rusher_player_id"].notna() & (pbp["qb_scramble"] != 1)]
    active: dict[tuple, set] = defaultdict(set)
    for frame, col in ((targets, "receiver_player_id"), (carries, "rusher_player_id")):
        for team, player, week in zip(frame["posteam"], frame[col], frame["week"]):
            active[(team, player)].add(int(week))
    if kind == "targets":
        plays, pid, pname = targets, "receiver_player_id", "receiver_player_name"
        min_volume = MIN_TARGET_VOLUME if min_volume is None else min_volume
    else:
        plays, pid, pname = carries, "rusher_player_id", "rusher_player_name"
        min_volume = MIN_CARRY_VOLUME if min_volume is None else min_volume
    team_week = plays.groupby(["posteam", "week"]).size()
    out = []
    for (team, player), g in plays.groupby(["posteam", pid]):
        on = active[(team, player)]
        e_weeks, l_weeks = on & early, on & late
        if len(e_weeks) < min_active or len(l_weeks) < min_active:
            continue
        e = int(g["week"].isin(e_weeks).sum())
        l_ = int(g["week"].isin(l_weeks).sum())
        te = int(sum(team_week.get((team, w), 0) for w in e_weeks))
        tl = int(sum(team_week.get((team, w), 0) for w in l_weeks))
        if e + l_ < min_volume or not te or not tl:
            continue
        out.append({"team": team, "player": str(g[pname].mode().iloc[0]), "player_id": player,
                    "early_n": e, "late_n": l_, "early_games": len(e_weeks), "late_games": len(l_weeks),
                    "early_share": e / te, "late_share": l_ / tl,
                    "change": l_ / tl - e / te, "early_weeks": f"{min(early)}-{max(early)}",
                    "late_weeks": f"{min(late)}-{max(late)}"})
    return out


def _movers(rows: list[dict], n: int) -> list[dict]:
    up = sorted([r for r in rows if r["change"] > 0], key=lambda r: (-r["change"], r["player"]))[:n]
    down = sorted([r for r in rows if r["change"] < 0], key=lambda r: (r["change"], r["player"]))[:n]
    return up + down[::-1]


def _usage_panel(ax, rows: list[dict], title: str, unit: str) -> None:
    _axes(ax, ygrid=False)
    m = len(rows)
    ys = np.arange(m)[::-1]
    xmax = max(max(r["early_share"], r["late_share"]) for r in rows)
    for y, r in zip(ys, rows):
        c = GOOD if r["change"] > 0 else BAD
        ax.annotate("", xy=(r["late_share"], y), xytext=(r["early_share"], y),
                    arrowprops={"arrowstyle": "-|>", "color": c, "lw": 1.4, "shrinkA": 2, "shrinkB": 2,
                                "mutation_scale": 8})
        ax.scatter([r["early_share"]], [y], s=16, color=NEUTRAL, zorder=3)
        ax.scatter([r["late_share"]], [y], s=22, color=c, zorder=4)
        right = max(r["early_share"], r["late_share"])
        ax.text(right + 0.035 * xmax, y, f"{r['change'] * 100:+.0f}  ({r['early_n']} in {r['early_games']} → "
                f"{r['late_n']} in {r['late_games']} {unit})", fontsize=5.9, color=INK_2, va="center")
    ax.set_yticks(ys)
    ax.set_yticklabels([f"{r['player']} ({r['team']})" for r in rows], fontsize=6.6)
    ax.set_xlim(0, xmax * 1.6)
    ax.set_xticks([t for t in ax.get_xticks() if 0 <= t <= min(1.0, xmax * 1.6) + 1e-9])
    ax.set_ylim(-0.8, m - 0.2)
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}"))
    ax.set_title(title, loc="left", fontsize=9, color=INK)
    split = sum(r["change"] > 0 for r in rows)
    if 0 < split < m:
        ax.axhline(ys[split - 1] - 0.5, color=GRID, linewidth=0.8)


def usage_shifts_chart(targets: list[dict], carries: list[dict], path: Path, note: str, n: int = 10) -> bool:
    t, c = _movers(targets, n), _movers(carries, n)
    if not t and not c:
        return False
    m = max(len(t), len(c))
    height = 0.235 * m + 2.2
    panels = [(x, title, unit) for x, title, unit in ((t, "Share of team targets", "tgt"),
                                                       (c, "Share of team carries", "car")) if x]
    fig, axes = _fig(height, len(panels))
    axes = np.atleast_1d(axes)
    for ax, (rows, title, unit) in zip(axes, panels):
        _usage_panel(ax, rows, title, unit)
        ax.set_xlabel(f"Weeks {rows[0]['early_weeks']} (grey) to weeks {rows[0]['late_weeks']} (color)",
                      color=INK_2, fontsize=7.5)
    sub = (f"{note}. The {n} biggest risers (green, top) and fallers (red, bottom) in a player's share of his "
           "team's targets and carries,\nfirst three weeks against the latest three. Shares count only games in "
           "which the player touched the ball, so injuries and byes are not\nread as role changes; at least 2 such "
           f"games in each window and {MIN_TARGET_VOLUME}+ targets or {MIN_CARRY_VOLUME}+ carries in all. Carries "
           "exclude QB scrambles.\nNumbers: change in share points, then the player's count and games in each "
           "window.")
    return _finish(fig, path, "Changing roles: who is getting more of the ball", sub, height)


# ── entry point ──────────────────────────────────────────────────────────────
def build_charts(out_dir: Path, season: int | None = None, pbp=None, schedule_rows: list[dict] | None = None
                 ) -> dict[str, bool]:
    """Write the eight PNGs to ``out_dir``; returns {file name: written}."""
    from model import nfl_market_scenarios as ns

    if schedule_rows is None:
        schedule_rows = ns.fetch_schedule()
    if season is None:
        season = latest_season(schedule_rows)
    if pbp is None:
        pbp = fetch_pbp(season)
    games = game_results(pbp)
    if not games:
        return {f"nfl_take_{s}.png": False for s in SLUGS}
    note = _note(games)
    coaches = coaches_from_schedule(schedule_rows, season)
    out_dir = Path(out_dir)
    o = lambda s: out_dir / f"nfl_take_{s}.png"  # noqa: E731
    return {
        "nfl_take_luck_vs_skill.png": luck_vs_skill_chart(epa_rows(pbp, games), o("luck_vs_skill"), note),
        "nfl_take_wp_swings.png": wp_swings_chart(wp_swing_games(pbp, games), o("wp_swings"), note),
        "nfl_take_close_games.png": close_games_chart(close_game_rows(games), o("close_games"), note),
        "nfl_take_turnover_luck.png": turnover_luck_chart(turnover_rows(pbp, games), o("turnover_luck"), note),
        "nfl_take_fourth_down.png": fourth_down_chart(fourth_down_rows(pbp, coaches), o("fourth_down"), note,
                                                    teams=sorted(team_game_counts(games))),
        "nfl_take_pass_rate_oe.png": pass_rate_oe_chart(proe_rows(pbp), o("pass_rate_oe"), note),
        "nfl_take_red_zone_two_minute.png": red_zone_two_minute_chart(red_zone_rows(pbp, games),
                                                                     o("red_zone_two_minute"), note),
        "nfl_take_usage_shifts.png": usage_shifts_chart(usage_rows(pbp, "targets"), usage_rows(pbp, "carries"),
                                                        o("usage_shifts"), note),
    }


SLUGS = ("luck_vs_skill", "wp_swings", "close_games", "turnover_luck", "fourth_down", "pass_rate_oe",
         "red_zone_two_minute", "usage_shifts")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    ap.add_argument("--season", type=int, default=None, help="season to chart (default: the latest with results)")
    ap.add_argument("--pbp", default=None, help="local play-by-play .csv.gz instead of downloading")
    args = ap.parse_args(argv)
    from model import nfl_market_scenarios as ns

    schedule = ns.fetch_schedule()
    season = args.season or latest_season(schedule)
    pbp = fetch_pbp(season, source=args.pbp)
    games = game_results(pbp)
    if not games:
        raise SystemExit(f"{season}: no regular-season play-by-play yet; nothing to chart")
    print(f"{VERSION}: {season} play-by-play, {len(games)} games through week {max(g['week'] for g in games)}")
    out = Path(args.out_dir)
    for name, ok in build_charts(out, season, pbp=pbp, schedule_rows=schedule).items():
        print(f"{out / name}: {'written' if ok else 'skipped, no data'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
