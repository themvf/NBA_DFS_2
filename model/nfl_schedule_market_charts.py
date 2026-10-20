"""NFL schedule and betting-market charts for readers: six images, season to date.

1. nfl_take_travel          miles each team travels over the regular-season schedule,
                            time zones crossed and short weeks
2. nfl_take_weather         outdoor games: points scored against the closing total, by
                            game-time wind and temperature
3. nfl_take_referees        by referee: penalties, points and how often games went over
4. nfl_take_market_ratings  power ratings implied by the closing spreads, week by week,
                            by division
5. nfl_take_line_moves      games whose spread moved 1.5+ points between our first and last
                            pre-kickoff capture, and whether the side it moved toward covered
6. nfl_take_upsets          every outright underdog win, by week and payout

Sources: nflverse's schedule file (via ``model.nfl_market_scenarios.fetch_schedule``)
for games, closing lines, rest, venue, weather and referee; nflverse play-by-play for
penalties; and this project's ``game_odds_history`` (sport 'nfl', consensus spread per
capture, joined to games through ``nfl_season_games.matchup_id``) for line movement.

Descriptive storytelling only: nothing here claims a betting edge, and small groups are
faded or flagged rather than hidden. Every chart is a pure function (rows in, path in,
returns False when there is nothing to draw).

    python -m model.nfl_schedule_market_charts --out-dir artifacts/nfl_schedule_market_charts
"""

from __future__ import annotations

import argparse
import math
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap, Normalize  # noqa: E402

from model.cfb_pattern_charts import GRID, INK, INK_2, SERIES, SURFACE  # noqa: E402
from model.cfb_team_charts import _place_labels  # noqa: E402
from model.nfl_market_scenarios import _num, fetch_schedule, pbp_url  # noqa: E402
from model.story_charts import BAD, GOOD, NEUTRAL, _axes, _fig, _finish, spread_labels  # noqa: E402

VERSION = "nfl-schedule-market-charts-v1"
DEFAULT_DIR = Path("artifacts") / "nfl_schedule_market_charts"
POOL_SEASONS = (2023, 2024, 2025, 2026)
SHORT_WEEK_DAYS = 5
MIN_REF_GAMES = 20          # referees with fewer pooled games are faded
LINE_MOVE_POINTS = 1.5
RATING_HALF_LIFE = 4.0      # weeks; older closing spreads count half as much every 4 weeks
RATING_RIDGE = 0.5
HFA_PRIOR, HFA_PRIOR_GAMES = 1.5, 10.0
EARTH_MILES = 3958.8

# Home stadium per team: (lat, lon, IANA zone, country), 2026 venues.
TEAM_SITES = {
    "ARI": (33.5276, -112.2626, "America/Phoenix", "US"),
    "ATL": (33.7554, -84.4009, "America/New_York", "US"),
    "BAL": (39.2780, -76.6227, "America/New_York", "US"),
    "BUF": (42.7738, -78.7870, "America/New_York", "US"),
    "CAR": (35.2258, -80.8528, "America/New_York", "US"),
    "CHI": (41.8623, -87.6167, "America/Chicago", "US"),
    "CIN": (39.0955, -84.5161, "America/New_York", "US"),
    "CLE": (41.5061, -81.6995, "America/New_York", "US"),
    "DAL": (32.7473, -97.0945, "America/Chicago", "US"),
    "DEN": (39.7439, -105.0201, "America/Denver", "US"),
    "DET": (42.3400, -83.0456, "America/New_York", "US"),
    "GB": (44.5013, -88.0622, "America/Chicago", "US"),
    "HOU": (29.6847, -95.4107, "America/Chicago", "US"),
    "IND": (39.7601, -86.1639, "America/New_York", "US"),
    "JAX": (30.3239, -81.6373, "America/New_York", "US"),
    "KC": (39.0489, -94.4839, "America/Chicago", "US"),
    "LV": (36.0909, -115.1833, "America/Los_Angeles", "US"),
    "LAC": (33.9535, -118.3392, "America/Los_Angeles", "US"),
    "LA": (33.9535, -118.3392, "America/Los_Angeles", "US"),
    "MIA": (25.9580, -80.2389, "America/New_York", "US"),
    "MIN": (44.9737, -93.2577, "America/Chicago", "US"),
    "NE": (42.0909, -71.2643, "America/New_York", "US"),
    "NO": (29.9511, -90.0812, "America/Chicago", "US"),
    "NYG": (40.8135, -74.0745, "America/New_York", "US"),
    "NYJ": (40.8135, -74.0745, "America/New_York", "US"),
    "PHI": (39.9008, -75.1675, "America/New_York", "US"),
    "PIT": (40.4468, -80.0158, "America/New_York", "US"),
    "SF": (37.4033, -121.9694, "America/Los_Angeles", "US"),
    "SEA": (47.5952, -122.3316, "America/Los_Angeles", "US"),
    "TB": (27.9759, -82.5033, "America/New_York", "US"),
    "TEN": (36.1665, -86.7713, "America/Chicago", "US"),
    "WAS": (38.9077, -76.8645, "America/New_York", "US"),
}

# Neutral-site venues by the schedule's stadium name.
NEUTRAL_SITES = {
    "Melbourne Cricket Ground": (-37.8200, 144.9834, "Australia/Melbourne", "AU"),
    "Maracana Stadium": (-22.9122, -43.2302, "America/Sao_Paulo", "BR"),
    "Arena Corinthians": (-23.5453, -46.4742, "America/Sao_Paulo", "BR"),
    "Tottenham Hotspur Stadium": (51.6043, -0.0664, "Europe/London", "GB"),
    "Tottenham Stadium": (51.6043, -0.0664, "Europe/London", "GB"),
    "Wembley Stadium": (51.5560, -0.2795, "Europe/London", "GB"),
    "Stade de France": (48.9244, 2.3601, "Europe/Paris", "FR"),
    "Bernabeu": (40.4531, -3.6883, "Europe/Madrid", "ES"),
    "FC Bayern Munich Stadium": (48.2188, 11.6247, "Europe/Berlin", "DE"),
    "Allianz Arena": (48.2188, 11.6247, "Europe/Berlin", "DE"),
    "Deutsche Bank Park": (50.0686, 8.6455, "Europe/Berlin", "DE"),
    "Estadio Banorte": (19.3029, -99.1505, "America/Mexico_City", "MX"),
    "Estadio Azteca": (19.3029, -99.1505, "America/Mexico_City", "MX"),
}

DIVISIONS = {
    "AFC East": ("BUF", "MIA", "NE", "NYJ"), "AFC North": ("BAL", "CIN", "CLE", "PIT"),
    "AFC South": ("HOU", "IND", "JAX", "TEN"), "AFC West": ("DEN", "KC", "LAC", "LV"),
    "NFC East": ("DAL", "NYG", "PHI", "WAS"), "NFC North": ("CHI", "DET", "GB", "MIN"),
    "NFC South": ("ATL", "CAR", "NO", "TB"), "NFC West": ("ARI", "LA", "SF", "SEA"),
}


# ── shared helpers ───────────────────────────────────────────────────────────
def _reg(rows: list[dict], season: int | None = None) -> list[dict]:
    return [r for r in rows if r.get("game_type") == "REG" and (season is None or int(r["season"]) == season)]


def _played(r: dict) -> bool:
    return _num(r.get("home_score")) is not None and _num(r.get("away_score")) is not None


def latest_season(rows: list[dict]) -> int | None:
    seasons = [int(r["season"]) for r in _reg(rows) if _played(r)]
    return max(seasons) if seasons else None


def _parse_day(v) -> date | None:
    try:
        return datetime.strptime(str(v)[:10], "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None


def _seasons_note(seasons: list[int]) -> str:
    s = sorted(set(seasons))
    return str(s[0]) if len(s) == 1 else f"{s[0]}-{s[-1]}"


# ── 1. travel ────────────────────────────────────────────────────────────────
def haversine_miles(a: tuple[float, float], b: tuple[float, float]) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 2 * EARTH_MILES * math.asin(math.sqrt(h))


def utc_offset_hours(zone: str, day: date) -> float:
    dt = datetime(day.year, day.month, day.day, 12, tzinfo=ZoneInfo(zone))
    return dt.utcoffset().total_seconds() / 3600


def zones_between(zone_a: str, zone_b: str, day: date) -> int:
    """Clock hours between two zones on that day, the short way round (LA to Melbourne is 7, not 17)."""
    d = abs(utc_offset_hours(zone_a, day) - utc_offset_hours(zone_b, day)) % 24
    return int(round(min(d, 24 - d)))


def game_site(r: dict, team: str, unmatched: set | None = None) -> tuple:
    """Where ``team`` plays this game: its own stadium at home, the host's on the road, the
    venue by name at a neutral site (unmatched names fall back to the listed home team's stadium)."""
    if r.get("location") == "Neutral":
        site = NEUTRAL_SITES.get(str(r.get("stadium") or "").strip())
        if site is not None:
            return site
        if unmatched is not None:
            unmatched.add(str(r.get("stadium") or "unnamed venue"))
    return TEAM_SITES[r["home_team"]]


def team_travel(rows: list[dict], season: int) -> tuple[list[dict], list[str]]:
    """One row per team over the whole regular-season schedule: miles, time zones crossed, short weeks.

    Every road or neutral-site game is a round trip from home, except that two games in a
    row outside the team's own country are one trip (a London double header does not fly home
    in between). Short week: the team had ``SHORT_WEEK_DAYS`` or fewer days of rest.
    """
    unmatched: set = set()
    by_team: dict[str, list[dict]] = defaultdict(list)
    for r in _reg(rows, season):
        for team, rest in ((r["home_team"], r.get("home_rest")), (r["away_team"], r.get("away_rest"))):
            if team not in TEAM_SITES:
                continue
            by_team[team].append({"day": _parse_day(r.get("gameday")), "week": int(r["week"]),
                                  "site": game_site(r, team, unmatched), "rest": _num(rest),
                                  "played": _played(r)})
    out = []
    for team, games in by_team.items():
        games.sort(key=lambda g: g["week"])
        home = TEAM_SITES[team]
        here = home
        miles = 0.0
        zones = 0
        road = 0
        for i, g in enumerate(games):
            day = g["day"] or date(season, 10, 1)
            site = g["site"]
            if site[:2] != home[:2]:
                road += 1
            miles += haversine_miles(here[:2], site[:2])
            zones += zones_between(here[2], site[2], day)
            here = site
            nxt = games[i + 1]["site"] if i + 1 < len(games) else None
            abroad_again = (site[3] != home[3] and nxt is not None and nxt[3] != home[3])
            if here[:2] != home[:2] and not abroad_again:
                miles += haversine_miles(here[:2], home[:2])
                zones += zones_between(here[2], home[2], day)
                here = home
        out.append({"team": team, "miles": miles, "zones": zones, "games": len(games), "road": road,
                    "short_weeks": sum(1 for g in games if g["rest"] is not None and g["rest"] <= SHORT_WEEK_DAYS),
                    "played": sum(g["played"] for g in games)})
    out.sort(key=lambda t: -t["miles"])
    return out, sorted(unmatched)


def travel_chart(teams: list[dict], path: Path, season: int, unmatched: list[str] | None = None) -> bool:
    if not teams:
        return False
    teams = sorted(teams, key=lambda t: t["miles"])
    height = 9.4
    fig, ax = _fig(height)
    _axes(ax, ygrid=False)
    cmap = LinearSegmentedColormap.from_list("zones", ["#cfe0f4", SERIES[0], "#0d2f5c"])
    hi_z = max(t["zones"] for t in teams)
    norm = Normalize(vmin=0, vmax=max(hi_z, 1))
    ys = np.arange(len(teams))
    ax.barh(ys, [t["miles"] for t in teams], color=[cmap(norm(t["zones"])) for t in teams], height=0.72)
    top = max(t["miles"] for t in teams)
    for y, t in zip(ys, teams):
        note = f"{t['miles']:,.0f} mi   {t['zones']} zones"
        ax.text(t["miles"] + top * 0.01, y, note, va="center", fontsize=7, color=INK)
        if t["short_weeks"]:
            ax.text(top * 1.36, y, "●" * t["short_weeks"], va="center", ha="left", fontsize=7, color=SERIES[1])
    ax.set_yticks(ys, [t["team"] for t in teams], fontsize=7.5, color=INK)
    ax.set_xlim(0, top * 1.5)
    ax.set_xticks(np.arange(0, top + 1, 10000 if top > 20000 else 5000))
    ax.set_ylim(-0.7, len(teams) - 0.3)
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v / 1000:.0f}k" if v else "0"))
    ax.set_xlabel("Miles travelled over the season (to every road or neutral-site game and back)", color=INK_2,
                  fontsize=8)
    ax.text(top * 1.36, len(teams) - 0.1, "short weeks", fontsize=7, color=SERIES[1], ha="left", va="bottom",
            fontweight="bold")
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    cb = fig.colorbar(sm, ax=ax, orientation="horizontal", fraction=0.025, pad=0.07, aspect=45, shrink=0.45,
                      anchor=(1.0, 0.0))
    cb.set_label("time zones crossed over the season (out and back)", color=INK_2, fontsize=7)
    cb.ax.tick_params(colors=INK_2, labelsize=6.5, length=0)
    cb.outline.set_visible(False)
    most, least = teams[-1], teams[0]
    zones_top = max(teams, key=lambda t: t["zones"])
    short = sum(t["short_weeks"] for t in teams)
    fallback = (f"\nNeutral venue not recognised, placed at the listed home stadium: {', '.join(unmatched)}."
                if unmatched else "")
    sub = (f"The full {season} regular-season schedule, all 32 teams. {most['team']} travel the most "
           f"({most['miles']:,.0f} miles), {least['team']} the least ({least['miles']:,.0f}); "
           f"{zones_top['team']} cross the most time zones ({zones_top['zones']}).\n"
           f"Straight-line miles, every road game counted as a round trip from home (two games abroad in a row are "
           f"one trip). Orange dots: short weeks, {SHORT_WEEK_DAYS} or fewer days of rest ({short} in all).{fallback}")
    return _finish(fig, path, f"Who racks up the miles: {season} NFL travel", sub, height)


# ── 2. weather ───────────────────────────────────────────────────────────────
def weather_games(rows: list[dict], seasons) -> list[dict]:
    """Completed outdoor (roof outdoors or open) games with a closing total and game-time temp and wind."""
    out = []
    for r in _reg(rows):
        if int(r["season"]) not in seasons or not _played(r) or r.get("roof") not in ("outdoors", "open"):
            continue
        total, temp, wind = _num(r.get("total_line")), _num(r.get("temp")), _num(r.get("wind"))
        if total is None or temp is None or wind is None:
            continue
        pts = _num(r["home_score"]) + _num(r["away_score"])
        out.append({"season": int(r["season"]), "week": int(r["week"]), "temp": temp, "wind": wind,
                    "total": total, "points": pts, "diff": pts - total,
                    "label": f"{r['away_team']} @ {r['home_team']}"})
    return out


def outdoor_coverage(rows: list[dict], seasons) -> tuple[int, int]:
    """(outdoor completed games, of which weather is filled)."""
    outdoor = [r for r in _reg(rows) if int(r["season"]) in seasons and _played(r)
               and r.get("roof") in ("outdoors", "open")]
    filled = [r for r in outdoor if _num(r.get("temp")) is not None and _num(r.get("wind")) is not None]
    return len(outdoor), len(filled)


WIND_CAP = 30                # mph; the odd gale is drawn at the edge of the axis
WIND_BINS = (("0-5 mph", 0, 5), ("6-10", 6, 10), ("11-15", 11, 15), ("16+", 16, 999))
TEMP_BINS = (("under 40°F", -99, 39), ("40-59", 40, 59), ("60-79", 60, 79), ("80+", 80, 999))


def bin_means(games: list[dict], key: str, bins) -> list[dict]:
    out = []
    for label, lo, hi in bins:
        g = [x for x in games if lo <= x[key] <= hi]
        out.append({"label": label, "lo": lo, "hi": hi, "n": len(g),
                    "mean": float(np.mean([x["diff"] for x in g])) if g else None,
                    "over": (sum(x["diff"] > 0 for x in g) / sum(x["diff"] != 0 for x in g))
                    if any(x["diff"] != 0 for x in g) else None})
    return out


def weather_chart(games: list[dict], path: Path, current: int, coverage: tuple[int, int] | None = None) -> bool:
    if len(games) < 10:
        return False
    height = 5.4
    fig, axes = _fig(height, ncols=2, sharey=True)
    seasons = sorted({g["season"] for g in games})
    now = [g for g in games if g["season"] == current]
    caps = {"wind": WIND_CAP, "temp": None}
    beyond = [g for g in games if g["wind"] > WIND_CAP]
    ylo = min(-40.0, min(g["diff"] for g in games) - 3)
    yhi = max(40.0, max(g["diff"] for g in games) + 3) + 10
    for ax, key, bins, xlabel in ((axes[0], "wind", WIND_BINS, "Wind at kickoff (mph)"),
                                  (axes[1], "temp", TEMP_BINS, "Temperature at kickoff (°F)")):
        _axes(ax)
        cap = caps[key]
        xv = (lambda g: min(g[key], cap)) if cap else (lambda g: g[key])
        old = [g for g in games if g["season"] != current]
        ax.axhline(0, color=INK_2, linewidth=0.9)
        ax.scatter([xv(g) for g in old], [g["diff"] for g in old], s=11, color=NEUTRAL, alpha=0.55,
                   edgecolor="none", label=f"{_seasons_note([g['season'] for g in old])}" if old else None)
        ax.scatter([xv(g) for g in now], [g["diff"] for g in now], s=18, color=SERIES[0], alpha=0.85,
                   edgecolor=SURFACE, linewidth=0.4, label=str(current), zorder=3)
        lo_x = min(xv(g) for g in games)
        hi_x = max(xv(g) for g in games)
        for b in bin_means(games, key, bins):
            if not b["n"]:
                continue
            x0, x1 = max(b["lo"], lo_x), min(b["hi"], hi_x)
            faded = b["n"] < 20
            ax.plot([x0, x1], [b["mean"]] * 2, color=SERIES[1], linewidth=2.4, alpha=0.45 if faded else 1, zorder=4)
            ax.text((x0 + x1) / 2, yhi - 1.5, f"{b['label']}\n{b['mean']:+.1f}\nn={b['n']}", ha="center", va="top",
                    fontsize=6.5, color=INK_2 if faded else INK, linespacing=1.15)
        ax.set_xlabel(xlabel, color=INK_2, fontsize=8)
        ax.set_ylim(ylo, yhi)
    if beyond:
        axes[0].text(WIND_CAP, ylo + 2, f"{len(beyond)} game{'s' if len(beyond) > 1 else ''} above {WIND_CAP} mph\n"
                     f"drawn at the edge", fontsize=6, color=INK_2, ha="right", va="bottom")
    axes[0].set_ylabel("Points scored minus the closing total", color=INK_2, fontsize=8)
    axes[0].legend(loc="lower left", fontsize=7, frameon=False, labelcolor=INK)
    windy = [g for g in games if g["wind"] >= 16]
    calm = [g for g in games if g["wind"] <= 5]
    cov = ""
    if coverage:
        cov = f" Weather is filled for {coverage[1]} of {coverage[0]} completed outdoor games."
    pooled = f"pooled over {_seasons_note(seasons)} because one season is too few games" if len(seasons) > 1 else \
        f"{current} only"
    windy_txt = f"{np.mean([g['diff'] for g in windy]):+.1f}" if windy else "n/a"
    calm_txt = f"{np.mean([g['diff'] for g in calm]):+.1f}" if calm else "n/a"
    sub = (f"{len(games)} outdoor games (roof open or no roof), {pooled}; blue dots are {current}.\n"
           f"{cov.strip()} Orange bars: average points against the total in each band (faded under 20 games).\n"
           f"Calm games (5 mph or less) finished {calm_txt} points against the total on average, windy games "
           f"(16+ mph) {windy_txt}.")
    return _finish(fig, path, "Does weather drag scoring under the total?", sub, height)


# ── 3. referees ──────────────────────────────────────────────────────────────
def penalties_from_pbp(rows: list[dict]) -> dict[str, int]:
    """{game_id: plays with a penalty flag}."""
    out: dict[str, int] = defaultdict(int)
    for r in rows:
        gid = r.get("game_id")
        if gid and _num(r.get("penalty")) == 1:
            out[gid] += 1
        elif gid and gid not in out:
            out[gid] = 0
    return dict(out)


def fetch_penalties(seasons, attempts: int = 3, cache_dir: Path | None = None) -> dict[str, int]:
    import pandas as pd

    out: dict[str, int] = {}
    for season in seasons:
        cached = cache_dir / f"pbp_penalty_{season}.csv" if cache_dir else None
        frame = None
        if cached is not None and cached.exists():
            frame = pd.read_csv(cached)
        last = None
        for attempt in range(attempts if frame is None else 0):
            try:
                frame = pd.read_csv(pbp_url(season), usecols=["game_id", "penalty"], compression="gzip",
                                    low_memory=False)
                break
            except Exception as exc:  # noqa: BLE001 - retried, then reported
                last = exc
                time.sleep(2 ** attempt)
        if frame is None:
            print(f"play-by-play {season}: download failed ({last}); its games have no penalty count")
            continue
        if cached is not None and not cached.exists():
            cached.parent.mkdir(parents=True, exist_ok=True)
            frame.to_csv(cached, index=False)
        out.update(penalties_from_pbp(frame.to_dict("records")))
    return out


def referee_rows(rows: list[dict], penalties: dict[str, int], seasons) -> list[dict]:
    """One row per referee over completed games: games, penalties per game (games with play-by-play),
    points per game, over rate (pushes left out)."""
    acc: dict[str, dict] = defaultdict(lambda: {"games": 0, "points": 0.0, "pen": 0, "pen_games": 0,
                                                "over": 0, "decided": 0, "seasons": set()})
    for r in _reg(rows):
        ref = str(r.get("referee") or "").strip()
        if int(r["season"]) not in seasons or not _played(r) or not ref or ref.lower() == "nan":
            continue
        a = acc[ref]
        pts = _num(r["home_score"]) + _num(r["away_score"])
        a["games"] += 1
        a["points"] += pts
        a["seasons"].add(int(r["season"]))
        if r.get("game_id") in penalties:
            a["pen"] += penalties[r["game_id"]]
            a["pen_games"] += 1
        total = _num(r.get("total_line"))
        if total is not None and pts != total:
            a["decided"] += 1
            a["over"] += pts > total
    out = []
    for ref, a in acc.items():
        out.append({"referee": ref, "games": a["games"], "ppg": a["points"] / a["games"],
                    "pen_pg": a["pen"] / a["pen_games"] if a["pen_games"] else None, "pen_games": a["pen_games"],
                    "over_rate": a["over"] / a["decided"] if a["decided"] else None, "decided": a["decided"]})
    out.sort(key=lambda x: -(x["pen_pg"] or 0))
    return out


def missing_referees(rows: list[dict], seasons) -> int:
    return sum(1 for r in _reg(rows) if int(r["season"]) in seasons and _played(r)
               and str(r.get("referee") or "").strip().lower() in ("", "nan"))


def referee_chart(refs: list[dict], path: Path, seasons, min_games: int = MIN_REF_GAMES, no_ref: int = 0) -> bool:
    refs = [r for r in refs if r["pen_pg"] is not None]
    if len(refs) < 3:
        return False
    refs = sorted(refs, key=lambda r: r["pen_pg"])
    height = 1.6 + 0.24 * len(refs)
    fig, axes = _fig(height, ncols=3, sharey=True, gridspec_kw={"width_ratios": [1.2, 1, 1]})
    ys = np.arange(len(refs))
    games = sum(r["games"] for r in refs)
    avg_pen = sum(r["pen_pg"] * r["pen_games"] for r in refs) / sum(r["pen_games"] for r in refs)
    avg_ppg = sum(r["ppg"] * r["games"] for r in refs) / games
    avg_over = sum(r["over_rate"] * r["decided"] for r in refs if r["over_rate"] is not None) / \
        sum(r["decided"] for r in refs if r["over_rate"] is not None)
    panels = (("pen_pg", "Penalty flags per game", avg_pen, lambda v: f"{v:.1f}"),
              ("ppg", "Points per game", avg_ppg, lambda v: f"{v:.1f}"),
              ("over_rate", "Games over the total", avg_over, lambda v: f"{v:.0%}"))
    for ax, (key, label, avg, fmt) in zip(axes, panels):
        _axes(ax, ygrid=False)
        ax.axvline(avg, color=INK_2, linewidth=0.8, linestyle="--")
        vals = [r[key] if r[key] is not None else avg for r in refs]
        thin = [r["games"] < min_games for r in refs]
        for y, v, t in zip(ys, vals, thin):
            ax.plot([avg, v], [y, y], color=GRID, linewidth=1.4, zorder=1)
            ax.scatter([v], [y], s=26, color=NEUTRAL if t else (SERIES[1] if v > avg else SERIES[0]),
                       zorder=3, edgecolor=SURFACE, linewidth=0.5)
        ax.set_title(f"{label}\n(dashed: all games, {fmt(avg)})", fontsize=8, color=INK, loc="left")
        spread = max(abs(v - avg) for v in vals) or 1
        ax.set_xlim(avg - spread * 1.25, avg + spread * 1.25)
        if key == "over_rate":
            ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}"))
    axes[0].set_yticks(ys, [f"{r['referee']} ({r['games']})" for r in refs], fontsize=7, color=INK)
    for lab, t in zip(axes[0].get_yticklabels(), [r["games"] < min_games for r in refs]):
        if t:
            lab.set_color(INK_2)
    axes[0].set_ylim(-0.7, len(refs) - 0.3)
    most, least = refs[-1], refs[0]
    note = _seasons_note(list(seasons))
    pooled = f"pooled over {note} because one season gives each crew only a handful of games" \
        if len(set(seasons)) > 1 else note
    missing = f" ({no_ref} more have no referee listed yet)" if no_ref else ""
    sub = (f"{games} completed regular-season games by head referee{missing}, {pooled}.\n"
           f"Games in brackets; orange is above the all-games average, blue below, grey fewer than {min_games} games. "
           f"Flags are plays with a penalty in nflverse play-by-play.\n"
           f"{most['referee']}'s games carry the most flags ({most['pen_pg']:.1f} a game), {least['referee']}'s the "
           f"fewest ({least['pen_pg']:.1f}).")
    return _finish(fig, path, "Referee crews: flags, points and overs", sub, height)


# ── 4. market power ratings ─────────────────────────────────────────────────
def fit_week(games: list[dict], week: int, half_life: float = RATING_HALF_LIFE, ridge: float = RATING_RIDGE) -> dict:
    """Ratings from closing spreads of games in weeks <= ``week``: spread = home edge + r(home) - r(away).

    ``games``: dicts with week, home, away, neutral (bool) and ``margin``, the home team's
    closing expected margin (nflverse ``spread_line``). Mirrors model/cfb_team_charts.fit_ratings:
    ridge toward 0, home edge pulled toward ``HFA_PRIOR`` with ``HFA_PRIOR_GAMES`` games of weight.
    """
    used = [g for g in games if g["week"] <= week]
    if not used:
        return {"ratings": {}, "hfa": None, "mae": None, "n": 0}
    teams = sorted({g["home"] for g in used} | {g["away"] for g in used})
    col = {t: i for i, t in enumerate(teams)}
    k = len(teams)
    x = np.zeros((len(used), k + 1))
    y = np.zeros(len(used))
    w = np.zeros(len(used))
    for row, g in enumerate(used):
        x[row, col[g["home"]]] = 1.0
        x[row, col[g["away"]]] = -1.0
        x[row, k] = 0.0 if g.get("neutral") else 1.0
        y[row] = g["margin"]
        w[row] = 0.5 ** ((week - g["week"]) / half_life)
    penalty = np.diag([ridge] * k + [HFA_PRIOR_GAMES])
    rhs = x.T @ (w * y)
    rhs[k] += HFA_PRIOR_GAMES * HFA_PRIOR
    beta = np.linalg.solve(x.T @ (x * w[:, None]) + penalty, rhs)
    return {"ratings": {t: float(beta[col[t]]) for t in teams}, "hfa": float(beta[k]),
            "mae": float(np.mean(np.abs(x @ beta - y))), "n": len(used)}


def rating_games(rows: list[dict], season: int) -> list[dict]:
    """Completed games of the season with a closing spread, in fit_week's shape."""
    out = []
    for r in _reg(rows, season):
        margin = _num(r.get("spread_line"))
        if margin is None or not _played(r):
            continue
        out.append({"week": int(r["week"]), "home": r["home_team"], "away": r["away_team"],
                    "neutral": r.get("location") == "Neutral", "margin": margin})
    return out


def weekly_ratings(games: list[dict], **kw) -> tuple[list[dict], list[dict]]:
    """(rows of week/team/rating, per-week fit notes)."""
    rows, fits = [], []
    for week in sorted({g["week"] for g in games}):
        f = fit_week(games, week, **kw)
        fits.append({"week": week, "n": f["n"], "hfa": f["hfa"], "mae": f["mae"]})
        rows += [{"week": week, "team": t, "rating": v} for t, v in f["ratings"].items()]
    return rows, fits


def ratings_chart(ratings: list[dict], path: Path, season: int, fits: list[dict] | None = None) -> bool:
    if not ratings:
        return False
    by: dict[str, dict[int, float]] = defaultdict(dict)
    for r in ratings:
        by[r["team"]][r["week"]] = r["rating"]
    weeks = sorted({r["week"] for r in ratings})
    last = weeks[-1]
    lo = min(r["rating"] for r in ratings)
    hi = max(r["rating"] for r in ratings)
    pad = (hi - lo) * 0.08
    height = 7.6
    fig, axes = plt.subplots(2, 4, figsize=(9.6, height), dpi=150, sharex=True, sharey=True)
    fig.patch.set_facecolor(SURFACE)
    for ax, (div, members) in zip(axes.flat, DIVISIONS.items()):
        _axes(ax)
        ax.axhline(0, color=INK_2, linewidth=0.7)
        ends = []
        for i, team in enumerate(members):
            pts = sorted(by.get(team, {}).items())
            if not pts:
                continue
            xs, ys = zip(*pts)
            colour = SERIES[[0, 1, 2, 6][i]]
            ax.plot(xs, ys, color=colour, linewidth=1.8, marker="o", markersize=2.5)
            ends.append((ys[-1], f"{team} {ys[-1]:+.1f}", colour))
        ends.sort()
        placed = spread_labels([e[0] for e in ends], (hi - lo + 2 * pad) * 0.075)
        for (y0, text, colour), yl in zip(ends, placed):
            ax.text(last + 0.25, yl, text, fontsize=6.5, color=colour, va="center", fontweight="bold")
        ax.set_title(div, fontsize=8.5, color=INK, loc="left", fontweight="bold")
        ax.set_xlim(weeks[0] - 0.3, last + (last - weeks[0] + 1) * 0.55)
        ax.set_ylim(lo - pad, hi + pad)
        ax.set_xticks(weeks, [str(w) for w in weeks])
    for ax in axes[1]:
        ax.set_xlabel("going into week", color=INK_2, fontsize=7.5)
    for ax in axes[:, 0]:
        ax.set_ylabel("points vs average team", color=INK_2, fontsize=7.5)
    top = max(by, key=lambda t: by[t].get(last, -99))
    bottom = min(by, key=lambda t: by[t].get(last, 99))
    rise = max(by, key=lambda t: by[t].get(last, 0) - by[t].get(weeks[0], 0))
    fall = min(by, key=lambda t: by[t].get(last, 0) - by[t].get(weeks[0], 0))
    fit = fits[-1] if fits else None
    fit_note = (f" Fit through week {fit['week']}: {fit['n']} games, home edge {fit['hfa']:.1f} pts, "
                f"fitted spreads within {fit['mae']:.1f} pts of the real ones on average.") if fit else ""
    sub = (f"Each team's rating is how many points better than an average team the {season} closing spreads make it, "
           f"fitted every week (recent weeks count most).\nWeek N uses week N's lines, set before kickoff, so it is "
           f"the market's view going into that week. Top now: {top} ({by[top][last]:+.1f}); bottom: {bottom} "
           f"({by[bottom][last]:+.1f}).\nBiggest climb since week {weeks[0]}: {rise} "
           f"({by[rise][last] - by[rise][weeks[0]]:+.1f}); biggest drop: {fall} "
           f"({by[fall][last] - by[fall][weeks[0]]:+.1f}).{fit_note}")
    return _finish(fig, path, f"How the betting market rates every team, week by week ({season})", sub, height)


# ── 5. line moves ────────────────────────────────────────────────────────────
_LINE_MOVE_SQL = """
WITH caps AS (
    SELECT g.nflverse_game_id AS game_id, g.week, g.kickoff, h.captured_at, h.home_spread
    FROM nfl_season_games g
    JOIN game_odds_history h ON h.sport = 'nfl' AND h.matchup_id = g.matchup_id
    WHERE g.season = %s AND g.game_type = 'REG' AND h.home_spread IS NOT NULL
      AND g.kickoff IS NOT NULL AND h.captured_at < g.kickoff
)
SELECT game_id, week, kickoff, count(*) AS captures,
       min(captured_at) AS first_at, max(captured_at) AS last_at,
       (array_agg(home_spread ORDER BY captured_at))[1] AS open_spread,
       (array_agg(home_spread ORDER BY captured_at DESC))[1] AS close_spread
FROM caps GROUP BY game_id, week, kickoff
"""


def load_line_history(db, season: int) -> list[dict]:
    """First and last pre-kickoff consensus spread (home team's own line, negative = home favored) per game."""
    return [dict(r) for r in db.execute(_LINE_MOVE_SQL, (season,))]


def line_moves(history: list[dict], rows: list[dict], min_move: float = LINE_MOVE_POINTS) -> list[dict]:
    """Completed games whose spread moved ``min_move``+ points, from the side it moved toward.

    The line moved toward the home team when its own spread fell (it had to give more points).
    ``cover``: 1 if that side covered the last pre-kickoff spread, -1 if not, 0 for a push.
    """
    sched = {r.get("game_id"): r for r in _reg(rows)}
    out = []
    for h in history:
        r = sched.get(h["game_id"])
        if r is None or not _played(r) or h.get("open_spread") is None or h.get("close_spread") is None:
            continue
        o, c = float(h["open_spread"]), float(h["close_spread"])
        if abs(c - o) < min_move - 1e-9:
            continue
        toward_home = c < o
        margin = int(_num(r["home_score"]) - _num(r["away_score"]))
        side_margin = margin if toward_home else -margin
        side_open, side_close = (o, c) if toward_home else (-o, -c)
        res = side_margin + side_close
        out.append({"game_id": h["game_id"], "week": int(r["week"]),
                    "team": r["home_team"] if toward_home else r["away_team"],
                    "opp": r["away_team"] if toward_home else r["home_team"],
                    "home": toward_home, "open": side_open, "close": side_close, "move": abs(c - o),
                    "margin": side_margin, "cover": 1 if res > 0 else -1 if res < 0 else 0,
                    "first_at": h.get("first_at"), "last_at": h.get("last_at"), "kickoff": h.get("kickoff"),
                    "captures": h.get("captures")})
    out.sort(key=lambda m: (-m["move"], m["week"]))
    return out


def _days(a, b) -> float | None:
    try:
        return (b - a).total_seconds() / 86400
    except (TypeError, AttributeError):
        return None


def line_moves_chart(moves: list[dict], path: Path, season: int, history: list[dict] | None = None,
                     n_games: int | None = None) -> bool:
    if not moves:
        return False
    moves = sorted(moves, key=lambda m: (m["move"], -m["week"]))
    height = 1.9 + 0.26 * len(moves)
    fig, ax = _fig(height)
    _axes(ax, ygrid=False)
    colour = {1: GOOD, -1: BAD, 0: NEUTRAL}
    ys = np.arange(len(moves))
    for y, m in zip(ys, moves):
        ax.annotate("", xy=(m["close"], y), xytext=(m["open"], y),
                    arrowprops={"arrowstyle": "-|>", "color": colour[m["cover"]], "lw": 1.8, "mutation_scale": 9})
        ax.scatter([m["open"]], [y], s=16, color=SURFACE, edgecolor=colour[m["cover"]], linewidth=1.2, zorder=3)
        result = {1: "covered", -1: "did not cover", 0: "push"}[m["cover"]]
        ax.text(max(m["open"], m["close"]) + 0.4, y, f"won by {m['margin']}" if m["margin"] > 0 else
                (f"lost by {-m['margin']}" if m["margin"] < 0 else "tied"), fontsize=6.5, color=INK_2,
                va="center")
        ax.text(min(m["open"], m["close"]) - 0.4, y, result, fontsize=6.5, color=colour[m["cover"]], va="center",
                ha="right")
    ax.axvline(0, color=INK_2, linewidth=0.7)
    labels = [f"{m['team']} {'vs' if m['home'] else 'at'} {m['opp']}, wk {m['week']}" for m in moves]
    ax.set_yticks(ys, labels, fontsize=7, color=INK)
    lo = min(min(m["open"], m["close"]) for m in moves)
    hi = max(max(m["open"], m["close"]) for m in moves)
    ax.set_xlim(lo - 4.5, hi + 3.5)
    ax.set_ylim(-0.7, len(moves) - 0.3)
    ax.set_xlabel("Spread for the team the line moved toward (negative: favored by that many). "
                  "Hollow dot: first capture, arrow head: last before kickoff", color=INK_2, fontsize=7.5)
    covered = sum(m["cover"] > 0 for m in moves)
    decided = sum(m["cover"] != 0 for m in moves)
    window = ""
    if history:
        lead = [d for d in (_days(h.get("first_at"), h.get("kickoff")) for h in history) if d is not None]
        last = [d for d in (_days(h.get("last_at"), h.get("kickoff")) for h in history) if d is not None]
        firsts = [h["first_at"] for h in history if h.get("first_at")]
        if lead and last and firsts:
            close_h = float(np.median(last)) * 24
            close_txt = f"{close_h * 60:.0f} minutes" if close_h < 1 else f"{close_h:.1f} hours"
            window = (f"\nThe first capture of a game came a median {np.median(lead):.0f} days before kickoff (usually "
                      f"when the previous week's games end), the last a median {close_txt} before.\nSpreads are the "
                      f"consensus of the books we capture, so they can sit between half points.")
    of = f" of {n_games} completed {season} games with captures" if n_games else f" {season} games"
    share = f"{covered / decided:.0%}" if decided else "n/a"
    sub = (f"{len(moves)}{of} moved {LINE_MOVE_POINTS:g}+ points between our first and last pre-kickoff "
           f"capture.\nThe team the line moved toward covered the last spread in {covered} of {decided} "
           f"({share}; pushes left out).{window}")
    return _finish(fig, path, "When the line moved, did the move turn out right?", sub, height)


# ── 6. upsets ────────────────────────────────────────────────────────────────
def payout(ml: float) -> float:
    """Profit on a $100 bet at American odds ``ml``."""
    return ml if ml > 0 else 100 * 100 / -ml


def upsets(rows: list[dict], season: int) -> tuple[list[dict], int]:
    """(outright underdog wins, games with both moneylines and a winner). The underdog is the longer price."""
    out, games = [], 0
    for r in _reg(rows, season):
        hml, aml = _num(r.get("home_moneyline")), _num(r.get("away_moneyline"))
        if not _played(r) or hml is None or aml is None or hml == aml:
            continue
        margin = _num(r["home_score"]) - _num(r["away_score"])
        if margin == 0:
            continue
        games += 1
        home_dog = payout(hml) > payout(aml)
        dog_won = (margin > 0) == home_dog
        if not dog_won:
            continue
        ml = hml if home_dog else aml
        out.append({"week": int(r["week"]), "dog": r["home_team"] if home_dog else r["away_team"],
                    "fav": r["away_team"] if home_dog else r["home_team"], "home": home_dog, "ml": int(ml),
                    "payout": payout(ml), "score": f"{int(max(_num(r['home_score']), _num(r['away_score'])))}-"
                                                  f"{int(min(_num(r['home_score']), _num(r['away_score'])))}"})
    out.sort(key=lambda u: -u["payout"])
    return out, games


def upsets_chart(rows: list[dict], path: Path, season: int, games: int | None = None, n_labels: int = 8) -> bool:
    if not rows:
        return False
    height = 5.6
    fig, ax = _fig(height)
    _axes(ax)
    rng = np.random.default_rng(7)
    pts = []
    for u in rows:
        x = u["week"] + rng.uniform(-0.18, 0.18)
        pts.append({**u, "x": x, "label": f"{u['dog']} {u['ml']:+d} over {u['fav']}"})
    xs = [p["x"] for p in pts]
    ys = [p["payout"] for p in pts]
    sizes = [18 + p["payout"] / 3 for p in pts]
    ax.scatter(xs, ys, s=sizes, color=SERIES[1], alpha=0.75, edgecolor=SURFACE, linewidth=0.7, zorder=3)
    weeks = sorted({u["week"] for u in rows})
    ax.set_xticks(range(min(weeks), max(weeks) + 1))
    ax.set_xlim(min(weeks) - 0.7, max(weeks) + 0.7)
    ax.set_ylim(0, max(ys) * 1.18)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"${v:,.0f}"))
    ax.set_xlabel("Week", color=INK_2, fontsize=8)
    ax.set_ylabel("Profit on a $100 moneyline bet on the underdog", color=INK_2, fontsize=8)
    fig.tight_layout()
    labelled = sorted(pts, key=lambda p: -p["payout"])[:n_labels]
    _place_labels(ax, [(p["x"], p["payout"], p["label"]) for p in labelled], list(zip(xs, ys)))
    big = rows[0]
    per_week = defaultdict(int)
    for u in rows:
        per_week[u["week"]] += 1
    busiest = max(per_week, key=per_week.get)
    rate = f" of {games} decided games ({len(rows) / games:.0%})" if games else ""
    sub = (f"{len(rows)} outright underdog wins{rate} in {season}, at the closing moneyline. Bigger dot: bigger payout.\n"
           f"Biggest: {big['dog']} at {big['ml']:+d} over {big['fav']} in week {big['week']} ({big['score']}). "
           f"Most in one week: {per_week[busiest]} in week {busiest}.")
    return _finish(fig, path, f"Every {season} NFL upset, and what it paid", sub, height)


# ── build ────────────────────────────────────────────────────────────────────
def _db():
    from config import load_config
    from db.database import DatabaseManager

    return DatabaseManager(load_config().database_url, initialize_schema=False)


def build_charts(out_dir: Path, db=None, season: int | None = None, rows: list[dict] | None = None,
                 cache_dir: Path | None = None) -> dict[str, bool]:
    out_dir = Path(out_dir)
    rows = rows if rows is not None else fetch_schedule()
    season = season or latest_season(rows)
    if season is None:
        raise SystemExit("no completed regular-season games in the schedule")
    made: dict[str, bool] = {}

    teams, unmatched = team_travel(rows, season)
    made["nfl_take_travel.png"] = travel_chart(teams, out_dir / "nfl_take_travel.png", season, unmatched)

    pool = tuple(s for s in POOL_SEASONS if s <= season) or (season,)
    current_weather = weather_games(rows, (season,))
    seasons_w = (season,) if len(current_weather) >= 80 else pool
    made["nfl_take_weather.png"] = weather_chart(weather_games(rows, seasons_w), out_dir / "nfl_take_weather.png",
                                                 season, outdoor_coverage(rows, seasons_w))

    penalties = fetch_penalties(pool, cache_dir=cache_dir)
    made["nfl_take_referees.png"] = referee_chart(referee_rows(rows, penalties, pool),
                                                  out_dir / "nfl_take_referees.png", pool,
                                                  no_ref=missing_referees(rows, pool))

    ratings, fits = weekly_ratings(rating_games(rows, season))
    made["nfl_take_market_ratings.png"] = ratings_chart(ratings, out_dir / "nfl_take_market_ratings.png", season,
                                                        fits)

    try:
        db = db or _db()
        history = load_line_history(db, season)
    except Exception as exc:  # noqa: BLE001 - the other five charts still stand
        print(f"line history unavailable ({exc}); nfl_take_line_moves skipped")
        history = []
    played_ids = {r["game_id"] for r in _reg(rows, season) if _played(r)}
    done = [h for h in history if h["game_id"] in played_ids]
    made["nfl_take_line_moves.png"] = line_moves_chart(line_moves(done, rows), out_dir / "nfl_take_line_moves.png",
                                                       season, done, len(done))

    ups, decided = upsets(rows, season)
    made["nfl_take_upsets.png"] = upsets_chart(ups, out_dir / "nfl_take_upsets.png", season, decided)
    return made


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    ap.add_argument("--season", type=int, default=None, help="season to chart (default: latest with results)")
    ap.add_argument("--cache-dir", default=None, help="keep the play-by-play penalty columns here between runs")
    args = ap.parse_args(argv)
    out = Path(args.out_dir)
    made = build_charts(out, season=args.season, cache_dir=Path(args.cache_dir) if args.cache_dir else None)
    for name, ok in made.items():
        print(f"{out / name}: {'written' if ok else 'skipped, no data'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
