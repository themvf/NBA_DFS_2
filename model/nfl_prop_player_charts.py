"""Reader charts about NFL player props and touchdowns, for a newsletter.

Descriptive, season to date. Nothing here claims a betting edge.

  nfl_take_receptions_vs_line.png        every closing DraftKings receptions line against the catches
  nfl_take_receptions_players.png        players with 4+ lines: who beat their receptions line most and least
  nfl_take_receiving_yards_vs_line.png   the same for receiving yards, biggest beats and misses named
  nfl_take_receiving_yards_players.png   ranked list with average yards above or below the line
  nfl_take_td_vs_line.png                anytime-touchdown prices against how often players scored
  nfl_take_longest_td.png                the season's longest touchdowns, and each game's longest

Sources (SELECT only):

* ``nfl_prop_implied_lines``  closing DraftKings lines (capture_role 'close'); the anytime-TD market has a
                              yes price only, so its implied chance still carries the book's margin
* ``ff_player_week_stats``    nflverse weekly stats via ``research.nfl_prop_implied.player_meta``
* nflverse play-by-play       ``model.nfl_market_scenarios.pbp_url`` for the touchdown plays
* ``artifacts/nfl-longest-td/grades/*.json``  our longest-TD model's graded weeks (subtitle only)

A listed player with no stat row that week (inactive, or the stats are not loaded yet) is left out and
counted, never scored 0. A game in which no listed player has a stat row is treated as not yet loaded
and dropped whole.

    python -m model.nfl_prop_player_charts --out-dir DIR
"""

from __future__ import annotations

import argparse
import collections
import json
import math
import random
import statistics as st
import textwrap
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from model.cfb_pattern_charts import INK, INK_2, SERIES, SURFACE  # noqa: E402
from model.story_charts import BAD, GOOD, NEUTRAL, _axes, _fig, _finish, spread_labels  # noqa: E402

SEASON = 2026
DEFAULT_DIR = Path("artifacts") / "nfl_dfs_story" / "charts"
GRADES_DIR = Path("artifacts") / "nfl-longest-td" / "grades"
MIN_LINES = 4            # a player needs this many graded lines to be ranked
RANK_N = 12              # players shown at each end of a ranked list
TD_BINS = (0.0, 0.10, 0.20, 0.30, 0.40, 0.50, 0.65, 1.0001)
ASSUMED_MARGIN = 1.08    # model.nfl_prop_implied.ANYTIME_TD_MARGIN, the margin our own reading assumes
YARD_MARKETS = {"player_receptions": "receptions", "player_reception_yds": "receiving_yards"}
TD_FIELDS = ("rushing_tds", "receiving_tds", "special_teams_tds", "fumble_recovery_tds")


# ── pure computations ────────────────────────────────────────────────────────
def classify(actual: float | None, line: float) -> str | None:
    """'over', 'under' or 'push' against the line; None when there is no stat row."""
    if actual is None:
        return None
    if actual > line:
        return "over"
    if actual < line:
        return "under"
    return "push"


def drop_unloaded_games(rows: list[dict]) -> tuple[list[dict], int]:
    """Drop games in which no listed player has a stat row (the game's stats are not loaded).

    Returns (kept rows, number of games dropped). Within a loaded game, a player with no row stays,
    with actual None, so he is counted as missing rather than scored.
    """
    by_game = collections.defaultdict(list)
    for r in rows:
        by_game[r["event_id"]].append(r)
    kept, dropped = [], 0
    for rs in by_game.values():
        if any(r.get("actual") is not None for r in rs):
            kept.extend(rs)
        else:
            dropped += 1
    return kept, dropped


def tally(rows: list[dict]) -> dict:
    """Counts for one market's lines: over, under, push, missing, games, and the priced over rate."""
    out = collections.Counter(classify(r.get("actual"), r["line"]) or "missing" for r in rows)
    graded = [r for r in rows if classify(r.get("actual"), r["line"]) in ("over", "under")]
    weeks = sorted({r["week"] for r in rows})
    return {"over": out["over"], "under": out["under"], "push": out["push"], "missing": out["missing"],
            "graded": len(graded), "games": len({r["event_id"] for r in rows if r.get("actual") is not None}),
            "over_rate": (out["over"] / len(graded)) if graded else None,
            "priced": st.mean(r["p_over"] for r in graded) if graded and all("p_over" in r for r in graded) else None,
            "weeks": (weeks[0], weeks[-1]) if weeks else None}


def player_table(rows: list[dict], min_lines: int = MIN_LINES) -> list[dict]:
    """Per player with at least ``min_lines`` graded lines (pushes and missing stat rows do not count).

    Over rate is overs over (overs + unders). ``diff`` is the average of actual minus line over the same lines.
    Team is the one on his latest line. Sorted by over rate, then average difference, then name.
    """
    by = collections.defaultdict(list)
    for r in rows:
        if classify(r.get("actual"), r["line"]) in ("over", "under"):
            by[r["player_key"]].append(r)
    out = []
    for key, rs in by.items():
        if len(rs) < min_lines:
            continue
        latest = max(rs, key=lambda r: r["week"])
        overs = sum(r["actual"] > r["line"] for r in rs)
        out.append({"player_key": key, "name": latest["name"], "team": latest.get("team") or "?",
                    "n": len(rs), "overs": overs, "rate": overs / len(rs),
                    "diff": st.mean(r["actual"] - r["line"] for r in rs),
                    "avg_line": st.mean(r["line"] for r in rs)})
    out.sort(key=lambda p: (-p["rate"], -p["diff"], p["name"]))
    return out


def ends(table: list[dict], n: int = RANK_N) -> tuple[list[dict], list[dict]]:
    """The top ``n`` and bottom ``n`` (worst last) without repeating a player when the list is short."""
    top = table[:n]
    rest = table[len(top):]
    return top, rest[-n:]


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """95% Wilson interval for k successes in n trials."""
    if n == 0:
        return float("nan"), float("nan")
    p = k / n
    den = 1 + z * z / n
    mid = (p + z * z / (2 * n)) / den
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / den
    return max(0.0, mid - half), min(1.0, mid + half)


def is_team_defense(name: str | None) -> bool:
    """'Chicago Bears D/ST' and the like: a team market listed under the anytime-TD key."""
    n = (name or "").upper()
    return n.endswith("D/ST") or n.endswith(" DST") or n.endswith("DEFENSE")


def implied_yes(american: float) -> float:
    return -american / (-american + 100) if american < 0 else 100 / (american + 100)


def td_bins(rows: list[dict], bins=TD_BINS) -> list[dict]:
    """``rows``: implied (raw yes-price chance) and scored (bool). One row per non-empty bin."""
    out = []
    for lo, hi in zip(bins, bins[1:]):
        xs = [r for r in rows if lo <= r["implied"] < hi]
        if not xs:
            continue
        k = sum(r["scored"] for r in xs)
        ci = wilson(k, len(xs))
        out.append({"lo": lo, "hi": hi, "n": len(xs), "scored": k, "rate": k / len(xs),
                    "implied": st.mean(r["implied"] for r in xs), "ci": ci})
    return out


def long_shots(rows: list[dict], n: int = 10) -> list[dict]:
    """Scorers with the lowest priced chance (raw yes price), longest odds first."""
    hits = [r for r in rows if r["scored"]]
    hits.sort(key=lambda r: (r["implied"], r["week"], r["name"]))
    return hits[:n]


def _num(v) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else f


def _text(v) -> str | None:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    s = str(v).strip()
    return s or None


def _last(name: str | None) -> str:
    return (name or "?").split(".")[-1].strip() or "?"


def td_plays(plays: list[dict]) -> list[dict]:
    """Every touchdown that counted, from nflverse play-by-play records.

    Penalty-nullified plays (play_type 'no_play'), two-point tries and rows without a scoring team are left
    out. Length is the return distance for returns (kicks, punts, interceptions, fumbles), else yards gained.
    """
    out = []
    for p in plays:
        if _num(p.get("touchdown")) != 1 or _text(p.get("play_type")) in (None, "no_play"):
            continue
        if _num(p.get("two_point_attempt")) == 1:
            continue
        team = _text(p.get("td_team"))
        if not team:
            continue
        ptype = _text(p.get("play_type"))
        if _num(p.get("return_touchdown")) == 1:
            if _num(p.get("interception")) == 1:
                kind = "interception return"
            else:
                kind = {"kickoff": "kick return", "punt": "punt return",
                        "field_goal": "missed FG return"}.get(ptype, "fumble return")
            yards = _num(p.get("return_yards"))
        elif _num(p.get("pass_touchdown")) == 1:
            kind = f"pass from {_last(_text(p.get('passer_player_name')))}"
            yards = _num(p.get("yards_gained"))
        elif _num(p.get("rush_touchdown")) == 1:
            kind = "rush"
            yards = _num(p.get("yards_gained"))
        else:
            kind = "fumble recovery"
            yards = _num(p.get("return_yards")) or _num(p.get("yards_gained"))
        if yards is None:
            continue
        home, away = _text(p.get("home_team")), _text(p.get("away_team"))
        opp = away if team == home else home
        player = (_text(p.get("td_player_name")) or _text(p.get("receiver_player_name"))
                  or _text(p.get("rusher_player_name")) or "?")
        out.append({"game_id": p.get("game_id"), "week": int(_num(p.get("week")) or 0), "team": team, "opp": opp,
                    "player": player, "player_id": _text(p.get("td_player_id")), "yards": int(round(yards)),
                    "kind": kind})
    out.sort(key=lambda t: (-t["yards"], t["week"], t["player"]))
    return out


def with_full_names(tds: list[dict], names: dict[str, str]) -> list[dict]:
    """Swap play-by-play short names ('D.Samuel') for full names where the player id is known."""
    return [t | {"player": names.get(t.get("player_id") or "", t["player"])} for t in tds]


def final_games(plays: list[dict]) -> set:
    """Game ids whose play-by-play includes the 'END GAME' row."""
    return {p.get("game_id") for p in plays if (_text(p.get("desc")) or "").upper().startswith("END GAME")}


def game_longest(tds: list[dict], games: set) -> dict:
    """game id -> the game's longest touchdown in yards (0 when nobody scored one), finished games only."""
    out = {g: 0 for g in games}
    for t in tds:
        if t["game_id"] in out:
            out[t["game_id"]] = max(out[t["game_id"]], t["yards"])
    return out


def grade_summary(grades: list[dict]) -> dict | None:
    """Top-pick hit rate of our longest-TD model over graded games; ``grades`` are (week, game rows)."""
    games, weeks = [], set()
    for week, rows in grades:
        for g in rows:
            if g.get("status") == "graded" and g.get("top_hit") is not None:
                games.append(g)
                weeks.add(week)
    if not games:
        return None
    chance = st.mean(1 / g["outcomes"] for g in games if g.get("outcomes"))
    return {"games": len(games), "hits": sum(bool(g["top_hit"]) for g in games), "weeks": sorted(weeks),
            "chance": chance}


# ── chart helpers ────────────────────────────────────────────────────────────
def _wrap(text: str, width: int = 158) -> str:
    """Wrap each subtitle line so nothing runs off the image."""
    return "\n".join(textwrap.fill(line, width) for line in text.split("\n"))


def _short(name: str, width: int = 18) -> str:
    parts = name.split()
    if len(name) <= width or len(parts) < 2:
        return name
    out = f"{parts[0][0]}. {' '.join(parts[1:])}"
    return out if len(out) <= width else out[: width - 1] + "."


def _weeks(w: tuple[int, int] | None) -> str:
    if not w:
        return "this season"
    return f"week {w[0]}" if w[0] == w[1] else f"weeks {w[0]}-{w[1]}"


def _missing_note(t: dict, dropped: int) -> str:
    bits = []
    if t["missing"]:
        bits.append(f"{t['missing']} listed players with no stat row (inactive) are left out")
    if dropped:
        bits.append(f"{dropped} game{'s' if dropped != 1 else ''} whose stats are not loaded yet "
                    f"{'are' if dropped != 1 else 'is'} left out")
    return ("; ".join(bits) + ".") if bits else ""


# ── 1/2. lines against results ───────────────────────────────────────────────
def line_scatter_chart(rows: list[dict], path: Path, market: str, dropped: int = 0, n_labels: int = 0,
                       seed: int = 7) -> bool:
    """One dot per player-game: closing line (x) against the actual stat (y), coloured over or under."""
    t = tally(rows)
    graded = [r for r in rows if r.get("actual") is not None]
    if len(graded) < 20:
        return False
    yards = market == "player_reception_yds"
    unit = "receiving yards" if yards else "catches"
    rng = random.Random(seed)
    jx, jy = (0.6, 0.0) if yards else (0.16, 0.22)
    height = 6.4
    fig, ax = _fig(height)
    _axes(ax)
    colour = {"over": GOOD, "under": BAD, "push": NEUTRAL}
    pts = []
    for r in graded:
        c = classify(r["actual"], r["line"])
        x = r["line"] + rng.uniform(-jx, jx)
        y = r["actual"] + (rng.uniform(-jy, jy) if not yards else 0)
        pts.append((x, y, c, r))
    for c in ("under", "over", "push"):
        sel = [p for p in pts if p[2] == c]
        if sel:
            ax.scatter([p[0] for p in sel], [p[1] for p in sel], s=11 if yards else 9, color=colour[c],
                       alpha=0.5, edgecolor="none", zorder=3)
    lo = min(0, min(r["line"] for r in graded) - 1)
    hi = max(max(r["line"] for r in graded), max(r["actual"] for r in graded)) + (28 if yards else 1)
    xhi = max(r["line"] for r in graded) + (6 if yards else 0.8)
    ax.plot([lo, xhi], [lo, xhi], color=INK_2, linewidth=1, linestyle="--", zorder=4)
    # median actual at each line (receptions) or line band (yards): a calibrated line sits on the diagonal
    groups = collections.defaultdict(list)
    for r in graded:
        key = r["line"] if not yards else 10 * math.floor(r["line"] / 10) + 5
        groups[key].append(r["actual"])
    meds = sorted((k, st.median(v), len(v)) for k, v in groups.items() if len(v) >= 15)
    if meds:
        ax.plot([m[0] for m in meds], [m[1] for m in meds], color=INK, linewidth=1.4, marker="D", markersize=4.5,
                markerfacecolor=SURFACE, zorder=5)
    ax.set_xlim(lo, xhi)
    ax.set_ylim(-1 if not yards else -6, hi)
    if not yards:
        ax.yaxis.set_major_locator(plt.MultipleLocator(2))
        ax.xaxis.set_major_locator(plt.MultipleLocator(1))
    label = "Closing receptions line" if not yards else "Closing receiving-yards line"
    ax.set_xlabel(f"{label} (DraftKings)", color=INK_2, fontsize=8)
    ax.set_ylabel(f"Actual {unit}", color=INK_2, fontsize=8)
    ax.scatter([], [], s=24, color=GOOD, label=f"Over: beat the line ({t['over']})")
    ax.scatter([], [], s=24, color=BAD, label=f"Under: fell short ({t['under']})")
    if t["push"]:
        ax.scatter([], [], s=24, color=NEUTRAL, label=f"Push: exactly the line ({t['push']})")
    ax.plot([], [], color=INK_2, linestyle="--", label="Actual = line")
    ax.plot([], [], color=INK, marker="D", markersize=4.5, markerfacecolor=SURFACE,
            label="Median actual at that line" + (" (10-yard bands)" if yards else ""))
    ax.legend(loc="upper left", fontsize=7, frameon=False)
    if n_labels:
        diffs = sorted(pts, key=lambda p: p[3]["actual"] - p[3]["line"])
        marked = [(p, GOOD) for p in diffs[-n_labels:][::-1]] + [(p, BAD) for p in diffs[:n_labels]]
        for i, (p, colour) in enumerate(marked, 1):
            ax.scatter([p[0]], [p[1]], s=46, facecolor="none", edgecolor=colour, linewidth=1.2, zorder=6)
            crowded = any(0 < q[0] - p[0] <= 4 and abs(q[1] - p[1]) < 15 for q, _ in marked)
            ax.annotate(str(i), (p[0], p[1]), xytext=(-6 if crowded else 5, 3), textcoords="offset points",
                        fontsize=7.5, color=colour, fontweight="bold", zorder=7, ha="right" if crowded else "left")
        y = 0.79
        for head, colour in (("Biggest beats", GOOD), ("Biggest misses", BAD)):
            ax.text(0.015, y, head, transform=ax.transAxes, fontsize=7.8, color=colour, fontweight="bold",
                    va="top", zorder=8, bbox={"facecolor": SURFACE, "edgecolor": "none", "pad": 0.3})
            y -= 0.036
            for i, (p, c) in enumerate(marked, 1):
                if c != colour:
                    continue
                r = p[3]
                ax.text(0.015, y, f"{i}. {r['name']} ({r.get('team') or '?'}), wk {r['week']}: {r['actual']:.0f} "
                        f"yds on a {r['line']:g} line ({r['actual'] - r['line']:+.0f})", transform=ax.transAxes,
                        fontsize=7.1, color=INK, va="top", zorder=8,
                        bbox={"facecolor": SURFACE, "edgecolor": "none", "pad": 0.3, "alpha": 0.9})
                y -= 0.032
            y -= 0.015
    rate = f"{t['over_rate']:.1%}" if t["over_rate"] is not None else "n/a"
    priced = f", against {t['priced']:.1%} the prices implied once the book's margin is removed" if t["priced"] else ""
    what = "receiving yards" if yards else "receptions"
    sub = (f"Closing DraftKings {what} lines, {_weeks(t['weeks'])}, final games: {t['graded']} player-games in "
           f"{t['games']} games. Each dot is one player in one game{' (dots nudged so equal values do not stack)' if not yards else ''}.\n"
           f"The over hit {rate}{priced}. {_missing_note(t, dropped)}")
    if n_labels:
        sub += f"\nNumbered rings: the {n_labels} biggest beats and {n_labels} biggest misses (actual minus line)."
    title = ("Receptions: the line against the catches" if not yards
             else "Receiving yards: the line against the yards")
    return _finish(fig, path, title, _wrap(sub), height)


def players_chart(table: list[dict], path: Path, market: str, weeks: tuple[int, int] | None = None,
                  n: int = RANK_N, min_lines: int = MIN_LINES) -> bool:
    """Top and bottom players by how often they beat their line."""
    if len(table) < 4:
        return False
    top, bottom = ends(table, n)
    picked = top + bottom
    yards = market == "player_reception_yds"
    height = 0.235 * (len(picked) + 1) + 2.1
    fig, ax = _fig(height)
    _axes(ax, ygrid=False)
    ys, y = [], len(picked) + (1 if bottom else 0)
    for i in range(len(picked)):
        if i == len(top) and bottom:
            y -= 1
        y -= 1
        ys.append(y)
    colours = [GOOD] * len(top) + [BAD] * len(bottom)
    ax.barh(ys, [100 * p["rate"] for p in picked], color=colours, height=0.64, alpha=0.85)
    unit = "yds" if yards else "catches"
    for yy, p in zip(ys, picked):
        diff = f"{p['diff']:+.1f}" if not yards else f"{p['diff']:+.0f}"
        ax.text(100 * p["rate"] + 1.2, yy, f"{p['overs']} of {p['n']} over ({p['rate']:.0%}), "
                f"{diff} {unit} vs line a game (avg line {p['avg_line']:.1f})", va="center", fontsize=6.9, color=INK, zorder=10,
                bbox={"facecolor": SURFACE, "edgecolor": "none", "pad": 0.3})
    ax.set_yticks(ys, [f"{p['name']} ({p['team']})" for p in picked], fontsize=7.3, color=INK)
    ax.set_xlim(0, 168)
    ax.set_xticks([0, 25, 50, 75, 100])
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.set_xlabel("How often he beat his closing line (pushes left out)", color=INK_2, fontsize=8)
    ax.set_ylim(min(ys) - 0.7, max(ys) + 0.7)
    what = "receiving-yards" if yards else "receptions"
    nlines = [p["n"] for p in table]
    sub = (f"Closing DraftKings {what} lines, {_weeks(weeks)}, final games. {len(table)} players had {min_lines}+ "
           f"graded lines ({min(nlines)}-{max(nlines)} each); the {len(top)} most and {len(bottom)} least often over "
           f"are shown.\nSmall samples: a coin flip comes up 4 of 4 one time in 16, so a name near either end says "
           f"more about a few games than about the player. Ties are ordered by the average {'yards' if yards else 'catches'} above the line.")
    title = ("Who beat their receptions line, and who did not" if not yards
             else "Who beat their receiving-yards line, and who did not")
    return _finish(fig, path, title, _wrap(sub), height)


# ── 3. anytime touchdown ─────────────────────────────────────────────────────
def td_chart(rows: list[dict], path: Path, dropped: int = 0, missing: int = 0) -> bool:
    """Priced anytime-TD chance (raw yes price) against how often listed players scored."""
    bins = td_bins(rows)
    if len(rows) < 50 or len(bins) < 3:
        return False
    height = 6.0
    fig, (ax, side) = _fig(height, ncols=2, gridspec_kw={"width_ratios": [1.55, 1]})
    _axes(ax)
    ax.plot([0, 1], [0, 1], color=INK_2, linewidth=1, linestyle="--", label="Scored exactly as often as priced")
    ax.plot([0, 1], [0, 1 / ASSUMED_MARGIN], color=INK_2, linewidth=1, linestyle=":",
            label=f"Priced chance with an assumed {ASSUMED_MARGIN - 1:.0%} margin removed")
    for b in bins:
        lo, hi = b["ci"]
        ax.plot([b["implied"], b["implied"]], [lo, hi], color=SERIES[0], alpha=0.35, linewidth=5,
                solid_capstyle="round")
    ax.plot([b["implied"] for b in bins], [b["rate"] for b in bins], color=SERIES[0], marker="o", markersize=6,
            linewidth=1.2, zorder=4, label="How often players in that price band scored")
    labs = spread_labels([b["rate"] for b in bins], 0.035)
    for b, ly in zip(bins, labs):
        flag = " (small sample)" if b["n"] < 30 else ""
        ax.text(b["implied"] + 0.018, ly, f"{b['scored']}/{b['n']}{flag}", fontsize=6.8, color=INK, va="center")
    top = max(max(b["ci"][1] for b in bins), max(b["implied"] for b in bins)) + 0.06
    ax.set_xlim(0, max(b["implied"] for b in bins) + 0.17)
    ax.set_ylim(0, top)
    for a in (ax.xaxis, ax.yaxis):
        a.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}"))
    ax.set_xlabel("Chance the DraftKings yes price implied (book's margin still in)", color=INK_2, fontsize=8)
    ax.set_ylabel("Share of those players who scored", color=INK_2, fontsize=8)
    ax.legend(loc="upper left", fontsize=6.8, frameon=False)
    side.axis("off")
    side.set_facecolor(SURFACE)
    shots = long_shots(rows, 10)
    side.text(0.0, 1.0, "Longest-priced scorers", fontsize=8.5, color=INK, fontweight="bold", va="top",
              transform=side.transAxes)
    yy = 0.93
    for r in shots:
        price = f"+{r['price']:.0f}" if r["price"] > 0 else f"{r['price']:.0f}"
        side.text(0.0, yy, f"{r['name']} ({r['team']}), wk {r['week']}", fontsize=7.3, color=INK, va="top",
                  transform=side.transAxes)
        side.text(1.0, yy, f"{price}  ({r['implied']:.0%})", fontsize=7.3, color=GOOD, va="top", ha="right",
                  transform=side.transAxes)
        yy -= 0.072
    n, k = len(rows), sum(r["scored"] for r in rows)
    priced = st.mean(r["implied"] for r in rows)
    weeks = sorted({r["week"] for r in rows})
    left = []
    if dropped:
        left.append(f"{dropped} game{'s' if dropped != 1 else ''} whose stats are not loaded yet")
    if missing:
        left.append(f"{missing} listed players with no stat row (inactive, no touches, or a name we could not "
                    f"match). A touchdown leaves a stat row, so few if any of them scored; counting them would pull "
                    f"the low-priced bands down")
    sub = (f"Closing DraftKings anytime-touchdown prices, {_weeks((weeks[0], weeks[-1]))}, final games: {n} player-games, "
           f"{k} scored ({k / n:.1%}) against {priced:.1%} the prices implied.\nThe market has a yes price only, so "
           f"the implied chance includes the book's margin and runs above the true chance; a fair price would sit "
           f"near the dotted line.\nCounts any rushing, receiving or return touchdown (not touchdown passes); team "
           f"defense markets are left out. Bars: 95% range treating each line as independent."
           f"{' Left out: ' + '; '.join(left) + '.' if left else ''}")
    fig.tight_layout(rect=(0, 0, 1, 1 - 1.25 / height))
    return _finish(fig, path, "Anytime touchdowns: priced chance against who scored", _wrap(sub), height)


# ── 4. longest touchdowns ────────────────────────────────────────────────────
KIND_COLOUR = {"rush": SERIES[1], "pass": SERIES[0], "return": SERIES[6]}


def _kind_colour(kind: str) -> str:
    if kind == "rush":
        return KIND_COLOUR["rush"]
    if kind.startswith("pass"):
        return KIND_COLOUR["pass"]
    return KIND_COLOUR["return"]


def longest_td_chart(tds: list[dict], per_game: dict, path: Path, grades: dict | None = None, n: int = 15) -> bool:
    if len(tds) < 5 or not per_game:
        return False
    top = tds[:n]
    height = 6.2
    fig, (ax, hx) = _fig(height, ncols=2, gridspec_kw={"width_ratios": [1.7, 1]})
    _axes(ax, ygrid=False)
    ys = list(range(len(top)))[::-1]
    ax.barh(ys, [t["yards"] for t in top], color=[_kind_colour(t["kind"]) for t in top], height=0.66)
    for y, t in zip(ys, top):
        ax.text(t["yards"] - 1.5, y, f"{t['yards']}", va="center", ha="right", fontsize=7, color=SURFACE,
                fontweight="bold")
        ax.text(t["yards"] + 1.5, y, f"{t['kind']}, {t['team']} vs {t['opp']}, wk {t['week']}", va="center",
                fontsize=6.8, color=INK_2)
    ax.set_yticks(ys, [_short(t["player"], 20) for t in top], fontsize=7.3, color=INK)
    ax.set_xlim(0, 175)
    ax.set_xticks([0, 25, 50, 75, 100])
    ax.set_xlabel("Touchdown length (yards)", color=INK_2, fontsize=8)
    ax.set_ylim(-0.7, len(top) - 0.3)
    from matplotlib.patches import Patch
    ax.legend(handles=[Patch(color=KIND_COLOUR["rush"], label="Rush"), Patch(color=KIND_COLOUR["pass"], label="Pass"),
                       Patch(color=KIND_COLOUR["return"], label="Return")], loc="lower right", fontsize=7,
              frameon=False, ncol=1)
    _axes(hx, xgrid=False)
    vals = list(per_game.values())
    edges = list(range(0, 110, 10))
    scored = [v for v in vals if v > 0]
    none = len(vals) - len(scored)
    counts = [sum(lo <= v < lo + 10 or (lo == 90 and v == 100) for v in scored) for lo in edges[:-1]]
    hx.bar([e + 5 for e in edges[:-1]], counts, width=9, color=SERIES[0], alpha=0.85)
    if none:
        hx.bar([-8], [none], width=7, color=NEUTRAL)
        hx.text(-8, none + 0.3, "no TD", fontsize=6.5, color=INK_2, ha="center", va="bottom")
    med = st.median(vals)
    hx.axvline(med, color=INK, linewidth=1.1, linestyle="--")
    hx.text(med + 2, max(counts + [none]) * 0.98, f"median\n{med:.0f} yds", fontsize=7, color=INK, va="top")
    hx.set_xlim(-14, 102)
    hx.set_xticks([0, 20, 40, 60, 80, 100])
    hx.set_xlabel("Each game's longest touchdown (yards)", color=INK_2, fontsize=8)
    hx.set_ylabel("Games", color=INK_2, fontsize=8)
    weeks = sorted({t["week"] for t in tds})
    sub = (f"Every touchdown that counted, {_weeks((weeks[0], weeks[-1]))} ({len(tds)} touchdowns in {len(vals)} "
           f"finished games; penalty-wiped plays left out). Return length is the return itself.\n"
           f"Right: the longest touchdown in each game; half the games topped out at {med:.0f} yards or less. "
           f"Our data has no sportsbook longest-touchdown line, so nothing here is compared with a price.")
    if grades:
        wk = ", ".join(str(w) for w in grades["weeks"])
        sub += (f"\nOur longest-TD model's top pick scored the game's longest touchdown in {grades['hits']} of "
                f"{grades['games']} graded games (week{'s' if len(grades['weeks']) > 1 else ''} {wk} only; "
                f"a random pick would hit about {grades['chance']:.0%} of the time).")
    fig.tight_layout(rect=(0, 0, 1, 1 - (0.5 + 0.15 * (sub.count(chr(10)) + 1)) / height))
    return _finish(fig, path, "The season's longest touchdowns", _wrap(sub), height)


# ── loaders (SELECT only) ────────────────────────────────────────────────────
def _db():
    from config import load_config
    from db.database import DatabaseManager
    return DatabaseManager(load_config().database_url, initialize_schema=False)


def load_lines(db, season: int = SEASON) -> dict[str, list[dict]]:
    """market -> closing DraftKings rows of finished games, each with the player's actual stat (None = no row)."""
    from model import nfl_prop_implied as pim
    from research.nfl_prop_implied import player_meta

    cutoff = datetime.now(timezone.utc) - timedelta(hours=5)   # a game this long after kickoff is final
    markets = list(YARD_MARKETS) + ["player_anytime_td"]
    lines = db.execute(
        """SELECT week, player_key, player_name, team, market, line, over_price, under_price, yes_price, event_id
           FROM nfl_prop_implied_lines
           WHERE season = %s AND capture_role = 'close' AND book = 'draftkings' AND conversion_version = %s
             AND market = ANY(%s) AND commence_time < %s""",
        (season, pim.VERSION, markets, cutoff))
    weeks = sorted({r["week"] for r in lines})
    metas = {w: player_meta(db, season, w) for w in weeks}
    out: dict[str, list[dict]] = collections.defaultdict(list)
    for r in lines:
        stats = (metas[r["week"]].get(r["player_key"]) or {}).get("stats")
        base = {"week": r["week"], "player_key": r["player_key"], "name": r["player_name"] or r["player_key"],
                "team": r["team"], "event_id": r["event_id"]}
        if r["market"] == "player_anytime_td":
            if r["yes_price"] is None or is_team_defense(base["name"]):
                continue   # a team defense/special-teams touchdown market is not a player prop
            scored = None if not stats else sum(float(stats.get(f) or 0) for f in TD_FIELDS) > 0
            out["player_anytime_td"].append(base | {"price": float(r["yes_price"]),
                                                    "implied": implied_yes(float(r["yes_price"])),
                                                    "actual": scored})
        else:
            if r["line"] is None or r["over_price"] is None or r["under_price"] is None:
                continue
            actual = None if not stats else float(stats.get(YARD_MARKETS[r["market"]]) or 0)
            out[r["market"]].append(base | {"line": float(r["line"]), "actual": actual,
                                            "p_over": pim.p_over(r["over_price"], r["under_price"])})
    return dict(out)


PBP_COLS = ("game_id", "week", "season_type", "home_team", "away_team", "play_type", "desc", "touchdown",
            "pass_touchdown", "rush_touchdown", "return_touchdown", "two_point_attempt", "td_team",
            "td_player_name", "td_player_id", "interception", "passer_player_name", "receiver_player_name",
            "rusher_player_name", "yards_gained", "return_yards")


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
            frame = frame[frame["season_type"] == "REG"]
            return frame.to_dict("records")
        except RuntimeError:
            raise
        except Exception as exc:  # noqa: BLE001 - retried, then raised
            last = exc
            time.sleep(2 ** attempt)
    raise RuntimeError(f"nflverse play-by-play {season} download failed: {last}")


def load_grades(season: int = SEASON, grades_dir: Path = GRADES_DIR) -> dict | None:
    grades = []
    for f in sorted(Path(grades_dir).glob(f"{season}-week*.json")):
        try:
            week = int(f.stem.split("week")[-1])
            grades.append((week, json.loads(f.read_text(encoding="utf-8"))))
        except (ValueError, OSError):
            continue
    return grade_summary(grades)


def build_charts(out_dir: Path, db=None, season: int = SEASON) -> dict[str, bool]:
    """Run every query and write every chart; returns {file name: written}."""
    db = db or _db()
    out_dir = Path(out_dir)
    made: dict[str, bool] = {}
    lines = load_lines(db, season)
    for market, slug in (("player_receptions", "receptions"), ("player_reception_yds", "receiving_yards")):
        rows, dropped = drop_unloaded_games(lines.get(market, []))
        made[f"nfl_take_{slug}_vs_line.png"] = line_scatter_chart(
            rows, out_dir / f"nfl_take_{slug}_vs_line.png", market, dropped,
            n_labels=5 if market == "player_reception_yds" else 0)
        made[f"nfl_take_{slug}_players.png"] = players_chart(
            player_table(rows), out_dir / f"nfl_take_{slug}_players.png", market, tally(rows)["weeks"])
    tds, dropped = drop_unloaded_games(lines.get("player_anytime_td", []))
    graded = [r | {"scored": bool(r["actual"])} for r in tds if r["actual"] is not None]
    made["nfl_take_td_vs_line.png"] = td_chart(graded, out_dir / "nfl_take_td_vs_line.png", dropped,
                                               sum(r["actual"] is None for r in tds))
    plays = load_pbp(season)
    games = final_games(plays)
    names = {r["gsis_id"]: r["canonical_name"] for r in db.execute(
        "SELECT DISTINCT ON (gsis_id) gsis_id, canonical_name FROM ff_players WHERE gsis_id IS NOT NULL "
        "ORDER BY gsis_id, season DESC")}
    touchdowns = with_full_names([t for t in td_plays(plays) if t["game_id"] in games], names)
    made["nfl_take_longest_td.png"] = longest_td_chart(touchdowns, game_longest(touchdowns, games),
                                                       out_dir / "nfl_take_longest_td.png", load_grades(season))
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
