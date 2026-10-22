"""Reader charts: which offenses gain the most yards for the points they score, and where the yards go.

Descriptive, season to date; nothing here claims an edge.

  nfl_take_yards_vs_points.png    yards per game against points per game, one dot per team, with the fitted
                                  line; teams far below it gain yards that do not turn into points
  nfl_take_yards_per_point.png    teams ranked by yards per point, with what usually explains the gap:
                                  turnovers, red-zone touchdown rate and field goals per game

Source: nflverse play-by-play, regular season. Yards are offensive scrimmage yards (``yards_gained`` on pass and
run plays, sacks included as lost yards; penalties excluded). Points are the team's final score, so they include
defensive and special-teams touchdowns, which the offense did not gain yards for. A red-zone trip is a drive
with at least one snap inside the opponent's 20; it ends in a touchdown when that team scored one on the drive.

    python -m model.nfl_yards_points_charts --out-dir DIR
"""

from __future__ import annotations

import argparse
import collections
import statistics as st
import time
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from model.cfb_pattern_charts import DPI, INK, INK_2, SERIES, SURFACE  # noqa: E402
from model.nfl_defense_qb_charts import _flag, _num, _team  # noqa: E402
from model.story_charts import BAD, GOOD, NEUTRAL, WIDTH_IN, _axes, _finish  # noqa: E402

SEASON = 2026
DEFAULT_DIR = Path("artifacts") / "nfl_market_scenarios"
PBP_COLS = ("game_id", "week", "season_type", "posteam", "home_team", "away_team", "home_score", "away_score",
            "play_type", "yards_gained", "interception", "fumble_lost", "field_goal_attempt", "yardline_100",
            "fixed_drive", "touchdown", "td_team")


# ── pure computations ────────────────────────────────────────────────────────
def team_offense(pbp: list[dict]) -> dict[str, dict]:
    """team -> games, yards, points, turnovers, field goals and red-zone trips per game (regular season)."""
    yards, tos, fgs = collections.Counter(), collections.Counter(), collections.Counter()
    finals: dict[str, tuple] = {}
    drives: dict[tuple, dict] = {}
    for p in pbp:
        if p.get("season_type") != "REG":
            continue
        finals.setdefault(p["game_id"], (_team(p["home_team"]), _team(p["away_team"]),
                                          _num(p.get("home_score")), _num(p.get("away_score"))))
        t = _team(p.get("posteam"))
        if not t:
            continue
        if p.get("play_type") in ("pass", "run"):
            yards[t] += _num(p.get("yards_gained")) or 0.0
        tos[t] += _flag(p.get("interception")) + _flag(p.get("fumble_lost"))
        fgs[t] += _flag(p.get("field_goal_attempt"))
        drive = _num(p.get("fixed_drive"))
        if drive is None:
            continue
        d = drives.setdefault((p["game_id"], drive, t), {"rz": False, "td": False})
        yl = _num(p.get("yardline_100"))
        if yl is not None and yl <= 20:
            d["rz"] = True
        if _flag(p.get("touchdown")) and _team(p.get("td_team")) == t:
            d["td"] = True
    games, points = collections.Counter(), collections.Counter()
    for home, away, hs, as_ in finals.values():
        if hs is None or as_ is None:
            continue
        games[home] += 1
        games[away] += 1
        points[home] += hs
        points[away] += as_
    trips, trip_tds = collections.Counter(), collections.Counter()
    for (_, _, t), d in drives.items():
        if d["rz"]:
            trips[t] += 1
            trip_tds[t] += d["td"]
    out = {}
    for t, n in games.items():
        if not n:
            continue
        out[t] = {"games": n, "yards": yards[t] / n, "points": points[t] / n, "turnovers": tos[t] / n,
                  "fgs": fgs[t] / n, "rz_trips": trips[t] / n,
                  "rz_td": trip_tds[t] / trips[t] if trips[t] else None,
                  "ypp": yards[t] / points[t] if points[t] else None}
    return out


def fit(teams: dict[str, dict]) -> tuple[float, float]:
    """Least-squares points = a + b * yards across teams."""
    xs = [v["yards"] for v in teams.values()]
    ys = [v["points"] for v in teams.values()]
    mx, my = st.mean(xs), st.mean(ys)
    sxx = sum((x - mx) ** 2 for x in xs)
    b = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx if sxx else 0.0
    return my - b * mx, b


def gaps(teams: dict[str, dict]) -> dict[str, float]:
    """team -> points per game above (+) or below (-) what its yards per game predict."""
    a, b = fit(teams)
    return {t: v["points"] - (a + b * v["yards"]) for t, v in teams.items()}


# ── charts ───────────────────────────────────────────────────────────────────
def scatter_chart(teams: dict[str, dict], path: Path, weeks: str) -> bool:
    if len(teams) < 5:
        return False
    a, b = fit(teams)
    gap = gaps(teams)
    height = 7.4
    fig, ax = plt.subplots(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    _axes(ax)
    xs = [v["yards"] for v in teams.values()]
    lo, hi = min(xs) - 8, max(xs) + 8
    ax.plot([lo, hi], [a + b * lo, a + b * hi], color=INK_2, linewidth=1.0, linestyle="--", zorder=1)
    for t, v in teams.items():
        g = gap[t]
        colour = GOOD if g >= 2.5 else BAD if g <= -2.5 else NEUTRAL
        ax.scatter(v["yards"], v["points"], s=34, color=colour, edgecolor="white", linewidth=0.6, zorder=3)
        ax.annotate(t, (v["yards"], v["points"]), fontsize=7, color=INK, xytext=(4, 2), textcoords="offset points",
                    fontweight="bold" if abs(g) >= 2.5 else "normal")
    ax.set_xlim(lo, hi)
    ax.set_xlabel("Offensive yards per game", fontsize=8.5, color=INK_2)
    ax.set_ylabel("Points scored per game", fontsize=8.5, color=INK_2)
    ax.tick_params(labelsize=7.5)
    fig.tight_layout()
    worst = sorted(gap, key=gap.get)[:3]
    best = sorted(gap, key=gap.get, reverse=True)[:3]
    w = ", ".join(f"{t} {gap[t]:+.1f}" for t in worst)
    bst = ", ".join(f"{t} {gap[t]:+.1f}" for t in best)
    sub = (f"All 32 teams, {weeks}. Dashed line: the points per game a team's yards usually produce this season "
           f"(about {b * 10:.1f} points for every 10 yards).\nRed: 2.5+ points a game below the line, yards that are "
           f"not turning into points ({w}). Green: 2.5+ above ({bst}).\nPoints are the final score, so defensive and "
           f"special-teams touchdowns count; yards are offensive scrimmage yards with sacks, penalties excluded.")
    return _finish(fig, path, "Yards gained against points scored", sub, height)


def ranked_chart(teams: dict[str, dict], path: Path, weeks: str, n: int = 32) -> bool:
    rows = sorted((t for t in teams if teams[t]["ypp"] is not None), key=lambda t: -teams[t]["ypp"])[:n]
    if len(rows) < 5:
        return False
    height = 0.25 * len(rows) + 2.6
    fig, (ax, tbl) = plt.subplots(1, 2, figsize=(WIDTH_IN, height), dpi=DPI, gridspec_kw={"width_ratios": [1.5, 1]})
    fig.patch.set_facecolor(SURFACE)
    _axes(ax, ygrid=False)
    vals = [teams[t]["ypp"] for t in rows]
    league = sum(v["yards"] * v["games"] for v in teams.values()) / sum(v["points"] * v["games"] for v in teams.values())
    ys = list(range(len(rows)))
    colours = [BAD if v >= league * 1.15 else GOOD if v <= league * 0.87 else SERIES[0] for v in vals]
    ax.barh(ys, vals, color=colours, height=0.7)
    ax.axvline(league, color=INK_2, linewidth=0.9, linestyle="--")
    ax.text(league, -1.1, f"league {league:.1f}", fontsize=6.8, color=INK_2, ha="center")
    for y, t, v in zip(ys, rows, vals):
        ax.text(v + 0.2, y, f"{v:.1f}", va="center", fontsize=6.6, color=INK)
    ax.set_yticks(ys, [f"{t}  {teams[t]['yards']:.0f} yds, {teams[t]['points']:.1f} pts" for t in rows], fontsize=6.8)
    ax.set_ylim(len(rows) - 0.4, -1.6)
    ax.set_xlim(0, max(vals) * 1.12)
    ax.set_xlabel("Yards per point scored", fontsize=8, color=INK_2)
    ax.tick_params(axis="x", labelsize=7)

    # The usual reasons, per team: league averages give the reader a reference for each column.
    cols = (("turnovers", "Turnovers\n/ game", "{:.1f}", True), ("rz_td", "Red-zone\nTD rate", "{:.0%}", False),
            ("fgs", "FG tries\n/ game", "{:.1f}", True))
    tbl.set_xlim(0, len(cols))
    tbl.set_ylim(len(rows) - 0.4, -1.6)
    tbl.axis("off")
    for j, (key, label, fmt, high_bad) in enumerate(cols):
        vs = [teams[t][key] for t in teams if teams[t][key] is not None]
        avg = st.mean(vs)
        sd = st.pstdev(vs) or 1.0
        tbl.text(j + 0.5, -1.2, f"{label}\n(avg {fmt.format(avg)})", ha="center", va="center", fontsize=6.6,
                 color=INK_2)
        for y, t in enumerate(rows):
            v = teams[t][key]
            if v is None:
                tbl.text(j + 0.5, y, "-", ha="center", va="center", fontsize=6.6, color=INK_2)
                continue
            z = (v - avg) / sd * (1 if high_bad else -1)
            colour = BAD if z >= 1 else GOOD if z <= -1 else INK
            tbl.text(j + 0.5, y, fmt.format(v), ha="center", va="center", fontsize=6.8, color=colour,
                     fontweight="bold" if abs(z) >= 1 else "normal")
    fig.tight_layout(w_pad=0.5)
    sub = (f"Offensive yards divided by points scored, {weeks}; top: the most yards needed for each point. Red bars: "
           f"15%+ above the league rate; green: 13%+ below.\nRight: the usual reasons a team's yards do not become "
           f"points. Red when a team is one standard deviation worse than average (more turnovers,\nfewer red-zone "
           f"trips ending in a touchdown, more field-goal tries instead of touchdowns), green when one better. "
           f"Five or six games each:\na single fluky game moves these numbers a lot.")
    return _finish(fig, path, "Which offenses need the most yards for every point?", sub, height)


# ── data ─────────────────────────────────────────────────────────────────────
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
            return frame.to_dict("records")
        except RuntimeError:
            raise
        except Exception as exc:  # noqa: BLE001 - retried, then raised
            last = exc
            time.sleep(2 ** attempt)
    raise RuntimeError(f"nflverse play-by-play {season} download failed: {last}")


def build_all(out_dir: Path, season: int = SEASON) -> list[tuple[str, bool]]:
    out_dir = Path(out_dir)
    pbp = load_pbp(season)
    ws = sorted({int(p["week"]) for p in pbp if p.get("season_type") == "REG"})
    weeks = f"weeks {ws[0]}-{ws[-1]}" if ws else "no weeks"
    teams = team_offense(pbp)
    return [("nfl_take_yards_vs_points.png", scatter_chart(teams, out_dir / "nfl_take_yards_vs_points.png", weeks)),
            ("nfl_take_yards_per_point.png", ranked_chart(teams, out_dir / "nfl_take_yards_per_point.png", weeks))]


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
