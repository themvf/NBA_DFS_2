"""Team charts for the CFB pattern-watch page, season to date (Substack PNGs).

Descriptive only: "what happened this season", every chart with its sample
sizes. Nothing here is a signal, a pick or evidence of a betting edge.

  market_ratings.png       market power ratings by week, from closing spreads
  top_ratings.png          top 25 market ratings now, with the change since last week
  turnovers_vs_ats.png     turnover margin per game against cover rate
  fourth_down_coaches.png  who goes for it on 4th and short
  red_zone_teams.png       red-zone touchdown rate by offense
  biggest_comebacks.png    the largest deficits overcome by the eventual winner
  comeback_flow.png        the score margin path of the biggest comeback
  explosive_vs_points.png  share of 20+ yard plays against points per drive

Definitions worth knowing before quoting a number:

* **Market ratings** are fitted to closing spreads, not results. For each week
  w, every FBS-vs-FBS regular-season game through week w enters a weighted ridge
  least-squares fit of ``-home_spread = r_home - r_away + hfa * (not neutral)``.
  Weights halve every ``HALF_LIFE_WEEKS`` (2) weeks back: a team plays about
  once a week, so a short half-life keeps the latest one or two lines in charge
  (the closing line already prices everything known then) while older lines
  still tie the schedule together, which is what lets a Big Ten team be
  compared with a Sun Belt team at all. The ridge penalty (``RIDGE``, in
  weighted games) pulls each rating toward 0 and keeps teams with a single
  game from running away; the solution has mean 0, so a rating is points
  better than an average FBS team on a neutral field. Home edge is fitted,
  pulled toward 3 points with the weight of 10 games: in week 1 each team has
  one game, and without that prior the fit read a 17-point home edge (home
  teams in early FBS-vs-FBS games are mostly the stronger side).
* **Turnovers** are interceptions and lost fumbles on scrimmage plays, by the
  team with the ball at the snap (CFBD's offense), returns for touchdowns
  included. Cover % leaves pushes out.
* **Fourth and short**: 4th and 1-3, regulation, the offense at the opponent's
  side of its own 40 or better (59 or fewer yards to goal). A run or pass
  (fakes included) is a go; a punt or field-goal try is the alternative;
  penalties and timeouts are not decisions. Same rules as cfb_drive_stories.
* **Drives** follow cfb_drive_stories: regulation, a start spot 1-99 yards out,
  end-of-half/game and uncategorized drives left out; points are the offense's
  score change over the drive (a TD includes its try). A red-zone trip is a
  drive with a scrimmage snap at the opponent's 20 or closer.
* **Comebacks** come from scoring plays only, regulation only; an overtime win
  counts as a win. The running score CFBD carries on ordinary plays is not
  trustworthy (a 7-0 game read 25-0 for three minutes in week 6, which would
  have been the "biggest comeback"), so a scoring play is accepted only when
  each team's score rises by a legal amount, and a game whose accepted scores
  do not add up to its quarter line scores is left out and counted.

Every query aggregates in Postgres (the database bills by egress); only
scoring plays (a few thousand a season) are fetched row by row.

    python -m model.cfb_team_charts [--out-dir artifacts/cfb_pattern_watch/charts]
"""
from __future__ import annotations

import argparse
import math
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

from model.cfb_drive_stories import (  # noqa: E402
    EXCLUDED_RESULTS, FG_TYPES, GO_TYPES, PUNT_TYPES, SCRIMMAGE_TYPES, classify_fourth_down, go_converted,
)
from model.cfb_pattern_charts import GRID, INK, INK_2, SERIES, SURFACE, _figure, _save, _style  # noqa: E402

SEASON = 2026
DEFAULT_DIR = Path("artifacts") / "cfb_pattern_watch" / "charts"
HALF_LIFE_WEEKS = 2.0
RIDGE = 0.5
HFA_PRIOR, HFA_PRIOR_GAMES = 3.0, 10.0
DEFAULT_TEAMS = ("Georgia", "Texas", "Alabama", "Oklahoma", "Notre Dame")
MIN_GAMES = 4
MIN_FOURTH_DOWNS = 8
MIN_RED_ZONE_TRIPS = 15
GREEN, RED = "#1baf7a", "#e34948"

# Scrimmage snaps (runs, passes, sacks, turnovers on them), as in cfb_week_story.
SCRIMMAGE = ("Rush", "Pass Reception", "Pass Incompletion", "Sack", "Rushing Touchdown", "Passing Touchdown",
             "Interception", "Pass Interception Return", "Interception Return Touchdown", "Fumble Recovery (Own)",
             "Fumble Recovery (Opponent)", "Fumble Return Touchdown", "Fumble", "Safety")
TURNOVER_PLAYS = ("Interception", "Pass Interception Return", "Interception Return Touchdown",
                  "Fumble Recovery (Opponent)", "Fumble Return Touchdown")


def _style_lines(ax, title: str, subtitle: str, bars: bool = False) -> None:
    """`_style` with the title lifted clear of a subtitle of any number of lines.
    ``bars``: horizontal bars, so the grid runs vertically only."""
    _style(ax, title, subtitle)
    ax.set_title(title, loc="left", fontsize=12, fontweight="bold", color=INK, pad=12 + 11 * (subtitle.count("\n") + 1))
    if bars:
        ax.grid(axis="y", visible=False)
        ax.grid(axis="x", color=GRID, linewidth=0.6)


# ── 1. market power ratings ──────────────────────────────────────────────────
def _rating_games(games: list[dict]) -> list[dict]:
    return [g for g in games
            if g.get("hclass") == "fbs" and g.get("aclass") == "fbs" and g.get("home_spread") is not None
            and (g.get("season_type") or "regular") == "regular" and g.get("week") is not None]


def fit_ratings(games: list[dict], week: int, half_life: float = HALF_LIFE_WEEKS, ridge: float = RIDGE) -> dict:
    """One week's fit: {'ratings': {team_id: points}, 'hfa', 'mae', 'n', 'names', 'games_by_team'}.

    ``games`` must already be FBS vs FBS with a closing spread; those after
    ``week`` are ignored. MAE is unweighted, fitted vs closing spread, over the
    games in the fit.
    """
    used = [g for g in games if int(g["week"]) <= week]
    if not used:
        return {"ratings": {}, "hfa": None, "mae": None, "mae_latest": None, "n": 0, "names": {}, "games_by_team": {}}
    ids = sorted({g["home_team_id"] for g in used} | {g["away_team_id"] for g in used})
    col = {t: i for i, t in enumerate(ids)}
    k = len(ids)
    x = np.zeros((len(used), k + 1))
    y = np.zeros(len(used))
    w = np.zeros(len(used))
    names, counts = {}, defaultdict(int)
    for row, g in enumerate(used):
        x[row, col[g["home_team_id"]]] = 1.0
        x[row, col[g["away_team_id"]]] = -1.0
        x[row, k] = 0.0 if g.get("neutral_site") else 1.0
        y[row] = -float(g["home_spread"])
        w[row] = 0.5 ** ((week - int(g["week"])) / half_life)
        names[g["home_team_id"]], names[g["away_team_id"]] = g.get("hname"), g.get("aname")
        counts[g["home_team_id"]] += 1
        counts[g["away_team_id"]] += 1
    # Ridge toward 0 for every rating; the home edge is pulled toward HFA_PRIOR
    # with the weight of HFA_PRIOR_GAMES games, which only matters in the first
    # weeks (week 1 alone cannot separate home edge from the home teams' strength).
    penalty = np.diag([ridge] * k + [HFA_PRIOR_GAMES])
    rhs = x.T @ (w * y)
    rhs[k] += HFA_PRIOR_GAMES * HFA_PRIOR
    beta = np.linalg.solve(x.T @ (x * w[:, None]) + penalty, rhs)
    miss = np.abs(x @ beta - y)
    latest = np.array([int(g["week"]) == week for g in used])
    return {
        "ratings": {t: float(beta[col[t]]) for t in ids},
        "hfa": float(beta[k]),
        "mae": float(np.mean(miss)),
        "mae_latest": float(np.mean(miss[latest])) if latest.any() else None,
        "n": len(used),
        "names": names,
        "games_by_team": dict(counts),
    }


def market_rating_fits(games: list[dict], **kw) -> list[dict]:
    """Per-week fit diagnostics: week, games in fit, home edge, MAE vs closing spread over
    every game in the fit and over that week's games alone (which carry the most weight)."""
    gs = _rating_games(games)
    out = []
    for week in sorted({int(g["week"]) for g in gs}):
        f = fit_ratings(gs, week, **kw)
        out.append({"week": week, "games": f["n"], "hfa": round(f["hfa"], 2), "mae": round(f["mae"], 2),
                    "mae_latest": None if f["mae_latest"] is None else round(f["mae_latest"], 2)})
    return out


def market_ratings(games: list[dict], **kw) -> list[dict]:
    """One row per (week, FBS team): week, team_id, team, rating (points vs average FBS team), games."""
    gs = _rating_games(games)
    out = []
    for week in sorted({int(g["week"]) for g in gs}):
        f = fit_ratings(gs, week, **kw)
        for t, r in f["ratings"].items():
            out.append({"week": week, "team_id": t, "team": f["names"].get(t), "rating": r,
                        "games": f["games_by_team"].get(t, 0)})
    return out


def _by_team(ratings: list[dict]) -> dict[str, dict[int, float]]:
    out: dict[str, dict[int, float]] = defaultdict(dict)
    for r in ratings:
        out[r["team"]][r["week"]] = r["rating"]
    return out


def biggest_risers(ratings: list[dict], n: int = 3, since_week: int | None = None, exclude=()) -> list[str]:
    """Teams whose rating rose most from ``since_week`` (default: the second fitted week) to the latest week."""
    weeks = sorted({r["week"] for r in ratings})
    if len(weeks) < 2:
        return []
    start = since_week if since_week is not None else weeks[1]
    last = weeks[-1]
    by = _by_team(ratings)
    deltas = [(v[last] - v[start], t) for t, v in by.items() if start in v and last in v and t not in exclude]
    return [t for _, t in sorted(deltas, reverse=True)[:n]]


def _end_labels(ax, ends: list[tuple[float, str, str]], x: float, gap: float) -> None:
    """Label line ends, nudging labels apart vertically so none overlap."""
    ends = sorted(ends)
    ys = [e[0] for e in ends]
    for i in range(1, len(ys)):
        ys[i] = max(ys[i], ys[i - 1] + gap)
    shift = max(0.0, ys[-1] - max(e[0] for e in ends)) / 2 if ys else 0.0
    for (y0, text, colour), y in zip(ends, ys):
        ax.annotate(text, (x, y0), xytext=(x + 0.15, y - shift), textcoords="data", fontsize=7.5, color=INK,
                    va="center", arrowprops={"arrowstyle": "-", "color": colour, "lw": 0.6})


def ratings_chart(ratings: list[dict], path: Path, teams=None, fits: list[dict] | None = None) -> bool:
    if not ratings:
        return False
    by = _by_team(ratings)
    weeks = sorted({r["week"] for r in ratings})
    start = weeks[1] if len(weeks) > 1 else weeks[0]
    if teams is None:
        teams = [t for t in DEFAULT_TEAMS if t in by]
        teams += biggest_risers(ratings, 3, exclude=teams)
    teams = [t for t in teams if t in by][:len(SERIES)]
    if not teams:
        return False
    fig, ax = _figure(4.4)
    ax.axhline(0, color=INK_2, linewidth=0.8)
    ends = []
    for i, t in enumerate(teams):
        pts = sorted((wk, v) for wk, v in by[t].items() if wk >= start)
        if not pts:
            continue
        xs, ys = zip(*pts)
        ax.plot(xs, ys, color=SERIES[i], linewidth=2, marker="o", markersize=3.5, label=t)
        ends.append((ys[-1], f"{t} {ys[-1]:+.1f}", SERIES[i]))
    lo, hi = ax.get_ylim()
    _end_labels(ax, ends, weeks[-1], (hi - lo) * 0.055)
    ax.set_xticks(weeks[weeks.index(start):], [f"Wk {w}" for w in weeks[weeks.index(start):]])
    ax.set_xlim(start - 0.2, weeks[-1] + 1.3)
    ax.set_ylabel("points better than an average FBS team", color=INK_2, fontsize=8)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.08), ncol=4, fontsize=7, frameon=False, labelcolor=INK)
    fit_note = ""
    if fits:
        f = fits[-1]
        fit_note = f" Fit through week {f['week']}: {f['games']} games, home edge {f['hfa']:.1f} pts;\nfitted spreads miss week {f['week']}'s closing lines by {f['mae_latest']:.1f} pts on average ({f['mae']:.1f} across all weeks)."
    _style_lines(ax, "How the betting market rates the contenders, week by week",
                 "Ratings fitted to every FBS-vs-FBS closing spread, recent weeks weighted most, season to date.\n"
                 f"Includes the three biggest risers since week {start}.{fit_note}")
    _save(fig, path)
    return True


def top_ratings_chart(ratings: list[dict], path: Path, week: int | None = None, n: int = 25) -> bool:
    if not ratings:
        return False
    weeks = sorted({r["week"] for r in ratings})
    week = weeks[-1] if week is None else week
    prev = max((w for w in weeks if w < week), default=None)
    now = sorted((r for r in ratings if r["week"] == week), key=lambda r: -r["rating"])[:n]
    if not now:
        return False
    before = {r["team"]: r["rating"] for r in ratings if r["week"] == prev}
    rank_before = {t: i + 1 for i, (t, _) in enumerate(sorted(before.items(), key=lambda kv: -kv[1]))}
    fig, ax = _figure(0.26 * len(now) + 1.5)
    ys = list(range(len(now)))[::-1]
    vals = [r["rating"] for r in now]
    ax.barh(ys, vals, color=SERIES[0], height=0.62)
    span = max(vals)
    for y, r in zip(ys, now):
        delta = r["rating"] - before[r["team"]] if r["team"] in before else None
        note = f"{r['rating']:.1f}" + (f"   {delta:+.1f} since wk {prev}" if delta is not None else "   new")
        ax.text(r["rating"] + 0.01 * span, y, note, va="center", fontsize=7, color=INK)
    labels = []
    for i, r in enumerate(now):
        rb = rank_before.get(r["team"])
        move = "" if rb is None else ("  (=)" if rb == i + 1 else f"  ({'↑' if rb > i + 1 else '↓'}{abs(rb - i - 1)})")
        labels.append(f"{i + 1}. {r['team']}{move}")
    ax.set_yticks(ys, labels, fontsize=7.5, color=INK)
    ax.set_xlim(0, span * 1.32)
    ax.set_xlabel("points better than an average FBS team, neutral field", color=INK_2, fontsize=8)
    _style_lines(ax, f"The market's top {len(now)} after week {week}",
                 f"Fitted to FBS-vs-FBS closing spreads through week {week}, season to date.\n"
                 f"Rank change since week {prev} in brackets; rating change in points beside each bar.", bars=True)
    _save(fig, path)
    return True


# ── shared scatter ───────────────────────────────────────────────────────────
def _corr(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 3:
        return None
    r = np.corrcoef(xs, ys)[0, 1]
    return None if math.isnan(r) else float(r)


def _pick_labels(rows: list[dict], xk: str, yk: str, n: int) -> list[dict]:
    xs = np.array([r[xk] for r in rows], dtype=float)
    ys = np.array([r[yk] for r in rows], dtype=float)
    zx = (xs - xs.mean()) / (xs.std() or 1)
    zy = (ys - ys.mean()) / (ys.std() or 1)
    order = np.argsort(-(zx ** 2 + zy ** 2))
    return [rows[i] for i in order[:n]]


def _scatter(rows: list[dict], xk: str, yk: str, path: Path, title: str, subtitle: str,
             xlabel: str, ylabel: str, xfmt, n_labels: int = 8, height: float = 4.6) -> bool:
    if len(rows) < 3:
        return False
    xs = [r[xk] for r in rows]
    ys = [r[yk] for r in rows]
    fig, ax = _figure(height)
    ax.scatter(xs, ys, s=22, color=SERIES[0], alpha=0.75, edgecolor=SURFACE, linewidth=0.6, zorder=3)
    slope, icept = np.polyfit(xs, ys, 1)
    gx = np.array([min(xs), max(xs)])
    ax.plot(gx, slope * gx + icept, color=SERIES[1], linewidth=1.6, zorder=2)
    r = _corr(xs, ys)
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: xfmt(v)))
    ax.grid(axis="x", color=GRID, linewidth=0.6)
    ax.set_xlabel(xlabel, color=INK_2, fontsize=8)
    ax.set_ylabel(ylabel, color=INK_2, fontsize=8)
    ax.margins(x=0.1, y=0.12)
    rtxt = f"Orange line: straight-line trend. Correlation r = {r:+.2f} across {len(rows)} teams." if r is not None else ""
    _style_lines(ax, title, f"{subtitle}\n{rtxt}")
    fig.tight_layout()
    # Teams on the same spot share one label.
    groups: dict = defaultdict(list)
    for p in _pick_labels(rows, xk, yk, n_labels):
        groups[(round(p[xk], 6), round(p[yk], 6))].append(p["team"])
    _place_labels(ax, [(x, y, ", ".join(sorted(ts))) for (x, y), ts in groups.items()], list(zip(xs, ys)))
    _save(fig, path)
    return True


_OFFSETS = [(9, 8), (9, -14), (-9, 8), (-9, -14), (0, 12), (0, -18), (18, 18), (-18, 18), (18, -24), (-18, -24),
            (26, -3), (-26, -3)]


def _place_labels(ax, points: list[tuple[float, float, str]], dots: list[tuple[float, float]] = ()) -> None:
    """Annotate each point at the first nearby offset whose text box overlaps no
    label already placed and no other dot, and stays inside the axes; a thin
    leader line ties each label to its dot."""
    from matplotlib.transforms import Bbox

    fig = ax.figure
    renderer = fig.canvas.get_renderer()
    frame = ax.get_window_extent(renderer)
    r = 2.5 * fig.dpi / 72   # dot radius in pixels
    placed = []
    dot_boxes = []
    for dx_, dy_ in dots:
        px, py = ax.transData.transform((dx_, dy_))
        dot_boxes.append(((dx_, dy_), Bbox.from_extents(px - r, py - r, px + r, py + r)))
    for x, y, text in points:
        for dx, dy in _OFFSETS:   # no room anywhere: the point stays unlabelled
            ann = ax.annotate(text, (x, y), xytext=(dx, dy), textcoords="offset points", fontsize=7, color=INK,
                              ha="left" if dx > 0 else "right" if dx < 0 else "center",
                              arrowprops={"arrowstyle": "-", "color": INK_2, "lw": 0.5, "shrinkA": 1, "shrinkB": 3})
            ann.set_in_layout(False)
            box = ann.get_window_extent(renderer).expanded(1.05, 1.15)
            inside = (box.x0 >= frame.x0 and box.x1 <= frame.x1 and box.y0 >= frame.y0 and box.y1 <= frame.y1)
            hits_dot = any(box.overlaps(b) for xy, b in dot_boxes
                           if abs(xy[0] - x) > 1e-6 or abs(xy[1] - y) > 1e-6)
            if inside and not hits_dot and not any(box.overlaps(b) for b in placed):
                placed.append(box)
                break
            ann.remove()


# ── 2. turnovers vs ATS ──────────────────────────────────────────────────────
def turnover_ats_rows(turnovers: list[dict], games: list[dict], min_games: int = MIN_GAMES) -> list[dict]:
    """Join per-team turnover counts (team_id, team, games, giveaways, takeaways) with season ATS.

    Returns rows with margin_pg (takeaways minus giveaways per game), cover_pct,
    ats record pieces. Teams with fewer than ``min_games`` play-by-play games or
    no decided ATS game are left out.
    """
    from model.cfb_team_ats import team_games

    ats: dict = defaultdict(lambda: [0, 0, 0])
    for r in team_games(games):
        a = ats[r["team_id"]]
        a[{"W": 0, "L": 1, "P": 2}[r["ats"]]] += 1
    out = []
    for t in turnovers:
        if int(t["games"]) < min_games:
            continue
        w, lost, p = ats.get(t["team_id"], [0, 0, 0])
        if w + lost == 0:
            continue
        out.append({"team": t["team"], "team_id": t["team_id"], "games": int(t["games"]),
                    "giveaways": int(t["giveaways"]), "takeaways": int(t["takeaways"]),
                    "margin_pg": (int(t["takeaways"]) - int(t["giveaways"])) / int(t["games"]),
                    "cover_pct": 100.0 * w / (w + lost), "ats": f"{w}-{lost}-{p}"})
    return out


def turnovers_vs_ats_chart(rows: list[dict], path: Path) -> bool:
    return _scatter(rows, "margin_pg", "cover_pct", path,
                    "Win the turnover battle, cover the spread?",
                    f"Each dot is an FBS team with {MIN_GAMES}+ games, season to date. Turnovers: interceptions\n"
                    "and lost fumbles on scrimmage plays. Cover % leaves pushes out.",
                    "turnover margin per game (takeaways minus giveaways)", "covered the spread, % of decided games",
                    lambda v: f"{v:+.1f}", n_labels=10)


# ── 3. fourth down ───────────────────────────────────────────────────────────
def fourth_down_teams(rows: list[dict], min_decisions: int = MIN_FOURTH_DOWNS) -> list[dict]:
    """Per team go rate on 4th and short. Rows: team_id, team, play_type, gained_distance, n."""
    acc: dict = defaultdict(lambda: {"decisions": 0, "go": 0, "conv": 0})
    names = {}
    for r in rows:
        decision = classify_fourth_down(r.get("play_type"))
        if decision is None:
            continue
        a, n = acc[r["team_id"]], int(r.get("n", 1) or 0)
        names[r["team_id"]] = r["team"]
        a["decisions"] += n
        if decision == "go":
            a["go"] += n
            a["conv"] += n if go_converted(r.get("play_type"), bool(r.get("gained_distance"))) else 0
    out = [{"team": names[t], "team_id": t, **a, "go_pct": 100.0 * a["go"] / a["decisions"]}
           for t, a in acc.items() if a["decisions"] >= min_decisions]
    out.sort(key=lambda r: (-r["go_pct"], -r["decisions"], r["team"]))
    return out


def _ranked_bars(top: list[dict], bottom: list[dict], value, label, path: Path, title: str, subtitle: str,
                 xlabel: str, xmax: float = 100.0, groups: tuple[str, str] = ("Top", "Bottom")) -> bool:
    picked = top + [r for r in bottom if r not in top]
    if not picked:
        return False
    fig, ax = _figure(0.25 * len(picked) + 1.6)
    ys = []
    y = len(picked) + (1 if bottom and len(top) < len(picked) else 0)
    for i, _ in enumerate(picked):
        if i == len(top) and len(top) < len(picked):
            y -= 1   # a gap between the top and the bottom group
        y -= 1
        ys.append(y)
    colours = [SERIES[0]] * len(top) + [SERIES[1]] * (len(picked) - len(top))
    vals = [value(r) for r in picked]
    ax.barh(ys, vals, color=colours, height=0.62)
    for yy, r, v in zip(ys, picked, vals):
        ax.text(v + xmax * 0.01, yy, label(r), va="center", fontsize=7, color=INK)
    ax.set_yticks(ys, [r["team"] for r in picked], fontsize=7.5, color=INK)
    ax.set_xlim(0, xmax * 1.42)
    ax.set_xticks([t for t in ax.get_xticks() if 0 <= t <= xmax])
    ax.set_xlabel(xlabel, color=INK_2, fontsize=8)
    if len(top) < len(picked):
        from matplotlib.patches import Patch
        ax.legend(handles=[Patch(color=SERIES[0], label=groups[0]), Patch(color=SERIES[1], label=groups[1])],
                  loc="lower right", fontsize=7.5, frameon=False, labelcolor=INK)
    _style_lines(ax, title, subtitle, bars=True)
    _save(fig, path)
    return True


def fourth_down_coaches_chart(rows: list[dict], path: Path, top_n: int = 15, bottom_n: int = 5) -> bool:
    if not rows:
        return False
    top = rows[:top_n]
    bottom = rows[-bottom_n:] if len(rows) > top_n else []
    return _ranked_bars(
        top, bottom, lambda r: r["go_pct"],
        lambda r: f"{r['go_pct']:.0f}%  went {r['go']} of {r['decisions']}, converted {r['conv']}",
        path, "Who keeps the offense on the field on fourth and short",
        f"4th and 1-3 from their own 41 or beyond, regulation, season to date. Go = run or pass; the alternative\n"
        f"is a punt or field goal. Top {len(top)} and bottom {len(bottom)} of {len(rows)} FBS teams with "
        f"{MIN_FOURTH_DOWNS}+ such decisions.",
        "went for it, % of decisions", xmax=max(r["go_pct"] for r in rows),
        groups=(f"Most aggressive {len(top)}", f"Least aggressive {len(bottom)}"))


# ── 4. red zone ──────────────────────────────────────────────────────────────
def drive_team_rows(rows: list[dict]) -> list[dict]:
    """Per offense: drives, points, red-zone trips and TDs. Rows: team_id, team, drive_result,
    reached_red_zone, n, offense_points (already restricted to included drives)."""
    acc: dict = defaultdict(lambda: {"drives": 0, "points": 0.0, "trips": 0, "rz_td": 0})
    names = {}
    for r in rows:
        if (r.get("drive_result") or "Uncategorized") in EXCLUDED_RESULTS:
            continue
        a, n = acc[r["team_id"]], int(r.get("n", 1) or 0)
        names[r["team_id"]] = r["team"]
        a["drives"] += n
        a["points"] += float(r.get("offense_points") or 0)
        if r.get("reached_red_zone"):
            a["trips"] += n
            a["rz_td"] += n if r.get("drive_result") == "TD" else 0
    out = []
    for t, a in acc.items():
        out.append({"team": names[t], "team_id": t, **a,
                    "points_per_drive": a["points"] / a["drives"] if a["drives"] else None,
                    "rz_td_pct": 100.0 * a["rz_td"] / a["trips"] if a["trips"] else None})
    return out


def red_zone_teams_chart(rows: list[dict], path: Path, top_n: int = 12, bottom_n: int = 8,
                         min_trips: int = MIN_RED_ZONE_TRIPS) -> bool:
    rows = sorted((r for r in rows if r["trips"] >= min_trips and r["rz_td_pct"] is not None),
                  key=lambda r: (-r["rz_td_pct"], -r["trips"], r["team"]))
    if not rows:
        return False
    top = rows[:top_n]
    bottom = rows[-bottom_n:] if len(rows) > top_n else []
    return _ranked_bars(
        top, bottom, lambda r: r["rz_td_pct"],
        lambda r: f"{r['rz_td_pct']:.0f}%  {r['rz_td']} TDs in {r['trips']} trips",
        path, "Finishing drives: red-zone touchdown rate",
        f"Offense, regulation, season to date. A trip is a drive with a snap at the opponent's 20 or closer.\n"
        f"Best {len(top)} and worst {len(bottom)} of {len(rows)} FBS teams with {min_trips}+ trips.",
        "trips ending in a touchdown, %", xmax=max(r["rz_td_pct"] for r in rows),
        groups=(f"Best {len(top)}", f"Worst {len(bottom)}"))


# ── 5. comebacks ─────────────────────────────────────────────────────────────
LEGAL_SCORE_STEPS = frozenset({0, 1, 2, 3, 6, 7, 8})


def score_steps(plays: list[dict], team_id: int) -> tuple[list[tuple[float, int, int]], int]:
    """(minute, team score, opponent score) after each accepted scoring play in regulation, and
    the number of scoring plays rejected.

    CFBD's running score on plays is unreliable: whole drives can carry a
    score that never happened (a 7-0 game read 25-0 for three minutes in
    week 6). Only plays flagged ``scoring`` are read, and one is accepted only
    when neither team's score goes down and each rises by a legal amount
    (0, 1, 2, 3, 6, 7 or 8) from the last accepted score.
    """
    def key(p):
        return (p["period"], -(p["game_seconds_remaining"] or 0), p.get("drive_number") or 0, p.get("play_number") or 0)

    mine = theirs = 0
    steps, rejected = [], 0
    for p in sorted(plays, key=key):
        if not p.get("scoring", True) or p["period"] > 4 or p.get("game_seconds_remaining") is None:
            continue
        if p.get("offense_score") is None or p.get("defense_score") is None:
            continue
        a, b = ((p["offense_score"], p["defense_score"]) if p["offense_team_id"] == team_id
                else (p["defense_score"], p["offense_score"]))
        if (a, b) == (mine, theirs):
            continue
        if a - mine in LEGAL_SCORE_STEPS and b - theirs in LEGAL_SCORE_STEPS:
            mine, theirs = a, b
            steps.append(((3600 - p["game_seconds_remaining"]) / 60, mine, theirs))
        else:
            rejected += 1
    return steps, rejected


def margin_path(plays: list[dict], team_id: int, final: tuple[int, int]) -> list[tuple[float, int]]:
    """(minutes elapsed, team margin) through regulation from accepted scoring plays, ending on ``final``."""
    steps, _ = score_steps(plays, team_id)
    path = [(0.0, 0)] + [(m, a - b) for m, a, b in steps]
    path.append((60.0, final[0] - final[1]))
    # No despike: accepted steps are already checked, and a real score answered
    # within half a minute (a kickoff return) would be erased by it.
    return path


def biggest_deficit(path: list[tuple[float, int]]) -> tuple[int, float | None]:
    """(largest deficit, minute it was reached) along a margin path; (0, None) if never behind."""
    worst, at = 0, None
    for minute, m in path:
        if -m > worst:
            worst, at = -m, minute
    return worst, at


def _steps_match_lines(steps: list[tuple[float, int, int]], g: dict, home_won: bool, reg: tuple[int, int]) -> bool:
    """Accepted scores agree with the regulation score and, when quarter line scores
    are given (home_line / away_line), with the running score at the end of each quarter."""
    end = (steps[-1][1], steps[-1][2]) if steps else (0, 0)
    if end != reg:
        return False
    hl, al = g.get("home_line"), g.get("away_line")
    if not hl or not al or len(hl) < 4 or len(al) < 4:
        return True
    mine_l, theirs_l = (hl, al) if home_won else (al, hl)
    for q in range(1, 4):
        at = [(a, b) for m, a, b in steps if m <= 15 * q]
        score = at[-1] if at else (0, 0)
        if score != (sum(mine_l[:q]), sum(theirs_l[:q])):
            return False
    return True


def find_comebacks(games: list[dict], plays: list[dict], n: int = 10, audit: dict | None = None) -> list[dict]:
    """The ``n`` largest deficits overcome by the eventual winner.

    ``games``: game_id, week, home_team_id, away_team_id, home, away, home_score,
    away_score (completed, not tied), optional reg_home/reg_away (regulation
    score from the line scores) and overtime. ``plays``: scoring plays with
    game_id and the score_steps fields. A game whose accepted scoring plays do
    not add up to its regulation score is left out (counted in ``audit``),
    because a missing score could hide or invent a deficit. Ties on deficit
    break by later minute.
    """
    by_game: dict = defaultdict(list)
    for p in plays:
        by_game[p["game_id"]].append(p)
    out = []
    checked = excluded = 0
    for g in games:
        if g["home_score"] == g["away_score"]:
            continue
        checked += 1
        home_won = g["home_score"] > g["away_score"]
        winner_id = g["home_team_id"] if home_won else g["away_team_id"]
        final = (g["home_score"], g["away_score"]) if home_won else (g["away_score"], g["home_score"])
        reg = (g.get("reg_home"), g.get("reg_away"))
        if reg[0] is None or reg[1] is None:
            if g.get("overtime"):
                excluded += 1
                continue
            reg = (g["home_score"], g["away_score"])
        reg = reg if home_won else (reg[1], reg[0])
        steps, _ = score_steps(by_game.get(g["game_id"], []), winner_id)
        if not _steps_match_lines(steps, g, home_won, tuple(reg)):
            excluded += 1
            continue
        path = margin_path(by_game.get(g["game_id"], []), winner_id, reg)
        deficit, minute = biggest_deficit(path)
        if deficit <= 0:
            continue
        out.append({"game_id": g["game_id"], "week": g["week"], "deficit": deficit, "minute": minute,
                    "winner": g["home"] if home_won else g["away"], "loser": g["away"] if home_won else g["home"],
                    "winner_score": final[0], "loser_score": final[1], "overtime": bool(g.get("overtime")),
                    "path": path})
    out.sort(key=lambda r: (-r["deficit"], -(r["minute"] or 0)))
    if audit is not None:
        audit.update({"games": checked, "excluded": excluded})
    return out[:n]


def biggest_comebacks_chart(rows: list[dict], path: Path, audit: dict | None = None) -> bool:
    if not rows:
        return False
    fig, ax = _figure(0.34 * len(rows) + 1.5)
    ys = list(range(len(rows)))[::-1]
    vals = [r["deficit"] for r in rows]
    ax.barh(ys, vals, color=SERIES[2], height=0.62)
    span = max(vals)
    for y, r in zip(ys, rows):
        q = min(4, max(0, int(((r["minute"] or 0) - 1e-9) // 15)) + 1)
        ax.text(r["deficit"] + span * 0.01, y,
                f"vs {r['loser']}: down {r['deficit']} in Q{q}, won {r['winner_score']}-{r['loser_score']}"
                + (" in OT" if r["overtime"] else "") + f" (wk {r['week']})",
                va="center", fontsize=7, color=INK)
    ax.set_yticks(ys, [r["winner"] for r in rows], fontsize=7.5, color=INK)
    ax.set_xlim(0, span * 2.0)
    ax.set_xticks([t for t in ax.get_xticks() if 0 <= t <= span])
    ax.set_xlabel("largest deficit overcome, points", color=INK_2, fontsize=8)
    _style_lines(ax, "The season's biggest comebacks",
                 "Largest deficit the eventual winner climbed out of in regulation, from scoring plays,\n"
                 "season to date."
                 + (f" {audit['games']} games checked; {audit['excluded']} left out because their scoring\n"
                    "plays did not add up to the quarter-by-quarter score." if audit else ""), bars=True)
    _save(fig, path)
    return True


def comeback_flow_chart(row: dict | None, path: Path) -> bool:
    if not row or not row.get("path"):
        return False
    fig, ax = _figure(3.6)
    xs, ys = zip(*row["path"])
    ax.step(xs, ys, where="post", color=INK, linewidth=1.3)
    ax.fill_between(xs, ys, 0, step="post", where=[y >= 0 for y in ys], color=GREEN, alpha=0.25, linewidth=0)
    ax.fill_between(xs, ys, 0, step="post", where=[y < 0 for y in ys], color=RED, alpha=0.25, linewidth=0)
    ax.axhline(0, color=INK_2, linewidth=0.7)
    for q in (15, 30, 45):
        ax.axvline(q, color=GRID, linewidth=0.6)
    if row.get("minute") is not None:
        ax.annotate(f"down {row['deficit']}", (row["minute"], -row["deficit"]), xytext=(8, -4),
                    textcoords="offset points", fontsize=7.5, color=INK, va="top")
    ax.set_xlim(0, 60)
    ax.set_xticks([0, 15, 30, 45, 60], ["Kickoff", "End Q1", "Half", "End Q3", "Final"])
    ax.set_ylabel(f"{row['winner']} margin, points", color=INK_2, fontsize=8)
    ax.grid(axis="y", color=GRID, linewidth=0.6)
    lo, hi = ax.get_ylim()
    ax.set_ylim(lo - 2, hi + 2)
    ot = " Tied after regulation; won in overtime." if row["overtime"] else ""
    _style_lines(ax, f"Anatomy of a comeback: {row['winner']} {row['winner_score']}, {row['loser']} {row['loser_score']}",
                 f"Week {row['week']}: {row['winner']}'s score margin through regulation, from scoring plays. "
                 f"Red: behind. Green: ahead.{ot}")
    _save(fig, path)
    return True


# ── 6. explosive plays vs points ─────────────────────────────────────────────
def explosive_rows(plays: list[dict], drives: list[dict], min_games: int = MIN_GAMES) -> list[dict]:
    """Join per-team explosive counts (team_id, team, games, snaps, explosive) with drive_team_rows output."""
    ppd = {d["team_id"]: d for d in drives}
    out = []
    for p in plays:
        d = ppd.get(p["team_id"])
        if d is None or int(p["games"]) < min_games or not p["snaps"] or d["points_per_drive"] is None:
            continue
        out.append({"team": p["team"], "team_id": p["team_id"], "games": int(p["games"]),
                    "snaps": int(p["snaps"]), "explosive": int(p["explosive"]),
                    "explosive_pct": 100.0 * int(p["explosive"]) / int(p["snaps"]),
                    "points_per_drive": d["points_per_drive"], "drives": d["drives"]})
    return out


def explosive_vs_points_chart(rows: list[dict], path: Path) -> bool:
    return _scatter(rows, "explosive_pct", "points_per_drive", path,
                    "Big plays, big scoring",
                    f"Each dot is an FBS offense with {MIN_GAMES}+ games, season to date. Explosive: a scrimmage play\n"
                    "gaining 20+ yards. Points per drive in regulation, a touchdown counting its try.",
                    "share of scrimmage plays gaining 20+ yards", "points per drive", lambda v: f"{v:.0f}%",
                    n_labels=10)


# ── loaders ──────────────────────────────────────────────────────────────────
_TURNOVER_SQL = """
WITH s AS (
    SELECT p.game_id, p.offense_team_id, p.defense_team_id, (p.play_type = ANY(%(turnovers)s)) AS turnover
    FROM cfb_plays p
    WHERE p.season = %(season)s AND p.play_type = ANY(%(scrimmage)s) AND p.down BETWEEN 1 AND 4
),
o AS (SELECT offense_team_id AS team_id, COUNT(DISTINCT game_id) AS games,
             COUNT(*) FILTER (WHERE turnover) AS giveaways FROM s GROUP BY 1),
d AS (SELECT defense_team_id AS team_id, COUNT(*) FILTER (WHERE turnover) AS takeaways FROM s GROUP BY 1)
SELECT t.team_id, t.name AS team, o.games, o.giveaways, COALESCE(d.takeaways, 0) AS takeaways
FROM cfb_teams t JOIN o ON o.team_id = t.team_id LEFT JOIN d ON d.team_id = t.team_id
WHERE t.classification = 'fbs'
"""

_FOURTH_SQL = """
SELECT p.offense_team_id AS team_id, t.name AS team, p.play_type,
       (p.yards_gained >= p.distance) AS gained_distance, COUNT(*) AS n
FROM cfb_plays p JOIN cfb_teams t ON t.team_id = p.offense_team_id AND t.classification = 'fbs'
WHERE p.season = %(season)s AND p.down = 4 AND p.distance BETWEEN 1 AND 3
  AND p.yards_to_goal BETWEEN 1 AND 59 AND p.period BETWEEN 1 AND 4
  AND p.play_type = ANY(%(types)s)
GROUP BY 1, 2, 3, 4
"""

_DRIVE_SQL = """
WITH rz AS (
    SELECT cfbd_drive_id, MIN(yards_to_goal) AS min_ytg
    FROM cfb_plays
    WHERE season = %(season)s AND play_type = ANY(%(scrimmage)s) AND yards_to_goal BETWEEN 1 AND 99
    GROUP BY cfbd_drive_id
)
SELECT d.offense_team_id AS team_id, t.name AS team, d.drive_result,
       (COALESCE(rz.min_ytg, 100) <= 20) AS reached_red_zone,
       COUNT(*) AS n,
       SUM(GREATEST(COALESCE(d.end_offense_score - d.start_offense_score, 0), 0)) AS offense_points
FROM cfb_drives d
JOIN cfb_teams t ON t.team_id = d.offense_team_id AND t.classification = 'fbs'
LEFT JOIN rz ON rz.cfbd_drive_id = d.cfbd_drive_id
WHERE d.season = %(season)s AND d.start_period BETWEEN 1 AND 4
  AND d.start_yards_to_goal BETWEEN 1 AND 99
  AND NOT (d.drive_result = ANY(%(excluded)s))
GROUP BY 1, 2, 3, 4
"""

_EXPLOSIVE_SQL = """
SELECT p.offense_team_id AS team_id, t.name AS team, COUNT(DISTINCT p.game_id) AS games,
       COUNT(*) AS snaps, COUNT(*) FILTER (WHERE p.yards_gained >= 20) AS explosive
FROM cfb_plays p JOIN cfb_teams t ON t.team_id = p.offense_team_id AND t.classification = 'fbs'
WHERE p.season = %(season)s AND p.play_type = ANY(%(scrimmage)s) AND p.down BETWEEN 1 AND 4
  AND p.period BETWEEN 1 AND 4
GROUP BY 1, 2
"""

# Every completed game the play-by-play covers, with its regulation score from
# the quarter line scores; only scoring plays are fetched (a few thousand rows).
_COMEBACK_GAMES_SQL = """
SELECT g.id AS game_id, g.week, g.home_team_id, g.away_team_id, ht.name AS home, at.name AS away,
       g.home_score, g.away_score, COALESCE(g.went_to_overtime, false) AS overtime,
       g.home_line_scores AS home_line, g.away_line_scores AS away_line,
       CASE WHEN jsonb_array_length(g.home_line_scores) >= 4 THEN
           (g.home_line_scores->>0)::int + (g.home_line_scores->>1)::int
         + (g.home_line_scores->>2)::int + (g.home_line_scores->>3)::int END AS reg_home,
       CASE WHEN jsonb_array_length(g.away_line_scores) >= 4 THEN
           (g.away_line_scores->>0)::int + (g.away_line_scores->>1)::int
         + (g.away_line_scores->>2)::int + (g.away_line_scores->>3)::int END AS reg_away
FROM cfb_matchups g
JOIN cfb_teams ht ON ht.team_id = g.home_team_id
JOIN cfb_teams at ON at.team_id = g.away_team_id
WHERE g.id IN (SELECT DISTINCT game_id FROM cfb_plays WHERE season = %(season)s)
  AND g.completed AND g.home_score IS NOT NULL AND g.away_score IS NOT NULL AND g.home_score <> g.away_score
"""

_COMEBACK_PLAYS_SQL = """
SELECT game_id, period, game_seconds_remaining, drive_number, play_number, offense_team_id,
       offense_score, defense_score, scoring
FROM cfb_plays
WHERE season = %(season)s AND scoring AND period BETWEEN 1 AND 4
"""


def _plain(rows) -> list[dict]:
    out = []
    for r in rows:
        d = dict(r)
        for k, v in d.items():
            if v is not None and not isinstance(v, (bool, int, float, str, list, dict)):
                d[k] = float(v)
        out.append(d)
    return out


def load_turnovers(db, season: int = SEASON) -> list[dict]:
    rows = _plain(db.execute(_TURNOVER_SQL, {"season": season, "turnovers": list(TURNOVER_PLAYS),
                                             "scrimmage": list(SCRIMMAGE)}))
    return [r for r in rows if r["games"]]


def load_fourth_downs(db, season: int = SEASON) -> list[dict]:
    return _plain(db.execute(_FOURTH_SQL, {"season": season, "types": sorted(GO_TYPES | PUNT_TYPES | FG_TYPES)}))


def load_drives(db, season: int = SEASON) -> list[dict]:
    return _plain(db.execute(_DRIVE_SQL, {"season": season, "scrimmage": sorted(SCRIMMAGE_TYPES),
                                          "excluded": sorted(EXCLUDED_RESULTS)}))


def load_explosive(db, season: int = SEASON) -> list[dict]:
    return _plain(db.execute(_EXPLOSIVE_SQL, {"season": season, "scrimmage": list(SCRIMMAGE)}))


def load_comebacks(db, season: int = SEASON, n: int = 10) -> tuple[list[dict], dict]:
    """(top ``n`` comebacks, audit {'games': checked, 'excluded': scores did not add up})."""
    games = _plain(db.execute(_COMEBACK_GAMES_SQL, {"season": season}))
    audit: dict = {"games": 0, "excluded": 0}
    if not games:
        return [], audit
    plays = _plain(db.execute(_COMEBACK_PLAYS_SQL, {"season": season}))
    for g in games:
        for k in ("home_score", "away_score", "home_team_id", "away_team_id", "week", "reg_home", "reg_away"):
            g[k] = None if g[k] is None else int(g[k])
        for k in ("home_line", "away_line"):
            g[k] = [int(v) for v in g[k]] if isinstance(g.get(k), list) else None
    return find_comebacks(games, plays, n, audit), audit


def build_charts(db, out_dir: Path, season: int = SEASON) -> dict[str, bool]:
    """Run every query once and write every chart; returns {file name: written}."""
    from model.cfb_pattern_measures import load_base_rate_games

    out_dir = Path(out_dir)
    games = load_base_rate_games(db)
    ratings = market_ratings(games)
    fits = market_rating_fits(games)
    drives = drive_team_rows(load_drives(db, season))
    comebacks, audit = load_comebacks(db, season)
    return {
        "market_ratings.png": ratings_chart(ratings, out_dir / "market_ratings.png", fits=fits),
        "top_ratings.png": top_ratings_chart(ratings, out_dir / "top_ratings.png"),
        "turnovers_vs_ats.png": turnovers_vs_ats_chart(turnover_ats_rows(load_turnovers(db, season), games),
                                                       out_dir / "turnovers_vs_ats.png"),
        "fourth_down_coaches.png": fourth_down_coaches_chart(fourth_down_teams(load_fourth_downs(db, season)),
                                                             out_dir / "fourth_down_coaches.png"),
        "red_zone_teams.png": red_zone_teams_chart(drives, out_dir / "red_zone_teams.png"),
        "biggest_comebacks.png": biggest_comebacks_chart(comebacks, out_dir / "biggest_comebacks.png", audit),
        "comeback_flow.png": comeback_flow_chart(comebacks[0] if comebacks else None, out_dir / "comeback_flow.png"),
        "explosive_vs_points.png": explosive_vs_points_chart(explosive_rows(load_explosive(db, season), drives),
                                                             out_dir / "explosive_vs_points.png"),
    }


def main(argv: list[str] | None = None) -> int:
    from config import load_config
    from db.database import DatabaseManager

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    args = ap.parse_args(argv)
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    made = build_charts(db, Path(args.out_dir))
    for name, ok in made.items():
        print(f"{Path(args.out_dir) / name}: {'written' if ok else 'skipped, no data'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
