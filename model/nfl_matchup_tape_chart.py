"""Reader chart: two teams side by side before they meet, season to date.

Descriptive, no edge claimed. One chart in two parts:

  nfl_take_tape_<away>_<home>.png   top: a butterfly of ten season stats, each team's bar sized by its league rank
                                    (longer = better), value and rank printed at the bar end; bottom: each team's
                                    games, one bar per game for the final margin, opponent underneath

Stats (regular season, games before the matchup): record, points scored and allowed a game, offensive yards gained
and allowed a game, offensive touchdowns a game, EPA a play on offense and allowed on defense (pass and run plays),
turnover margin a game, and red-zone touchdown rate (drives that reached the 20 and ended in an offensive
touchdown). Ranks are among all 32 teams. The subtitle adds the market's line now and at its first capture this
week, and any player ruled out or on injured reserve who has 10%+ of his team's targets.

    python -m model.nfl_matchup_tape_chart --away IND --home MIN --out-dir DIR
"""

from __future__ import annotations

import argparse
import collections
import math
import textwrap
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from model.cfb_pattern_charts import DPI, INK, INK_2, SURFACE  # noqa: E402
from model.nfl_team_receiving_charts import TEAM_NAMES  # noqa: E402
from model.story_charts import BAD, GOOD, WIDTH_IN, _finish  # noqa: E402

SEASON = 2026
DEFAULT_DIR = Path("artifacts") / "nfl_market_scenarios"
AWAY_COLOUR, HOME_COLOUR = "#e3833b", "#2a78d6"
PBP_COLS = ("game_id", "week", "season_type", "posteam", "play_type", "yards_gained", "epa", "touchdown", "td_team",
            "interception", "fumble_lost", "yardline_100", "fixed_drive")
OFFENSE_PLAYS = ("pass", "run")
# key, label, higher is better, formatter
METRICS = (
    ("pf", "Points scored a game", True, lambda v: f"{v:.1f}"),
    ("pa", "Points allowed a game", False, lambda v: f"{v:.1f}"),
    ("off_yds", "Yards gained a game", True, lambda v: f"{v:.0f}"),
    ("def_yds", "Yards allowed a game", False, lambda v: f"{v:.0f}"),
    ("off_td", "Offensive TDs a game", True, lambda v: f"{v:.1f}"),
    ("off_epa", "Offense EPA a play", True, lambda v: f"{v:+.2f}"),
    ("def_epa", "Defense EPA a play allowed", False, lambda v: f"{v:+.2f}"),
    ("to_margin", "Turnover margin a game", True, lambda v: f"{v:+.1f}"),
    ("rz_td", "Red-zone TD rate", True, lambda v: f"{v:.0%}"),
)


def _num(v) -> float | None:
    return float(v) if isinstance(v, (int, float)) and not math.isnan(v) else None


# ── pure computations ────────────────────────────────────────────────────────
def team_table(pbp: list[dict], schedule: list[dict], before_week: int) -> dict[str, dict]:
    """team -> season stats over regular-season games played before ``before_week``, plus the game log."""
    games = [g for g in schedule if g["week"] < before_week and g.get("home_score") is not None]
    ids = {g["game_id"] for g in games}
    log: dict[str, list] = collections.defaultdict(list)
    pts = collections.defaultdict(lambda: [0.0, 0.0])
    for g in sorted(games, key=lambda g: g["week"]):
        for team, opp, f, a, home in ((g["home_team"], g["away_team"], g["home_score"], g["away_score"], True),
                                      (g["away_team"], g["home_team"], g["away_score"], g["home_score"], False)):
            log[team].append({"week": g["week"], "opp": opp, "home": home, "for": f, "against": a})
            pts[team][0] += f
            pts[team][1] += a
    acc = collections.defaultdict(lambda: collections.Counter())
    drives: dict[tuple, dict] = {}
    for p in pbp:
        if p.get("season_type") != "REG" or p["game_id"] not in ids or not isinstance(p.get("posteam"), str):
            continue
        t = p["posteam"]
        opp = None
        giveaway = (_num(p.get("interception")) or 0) + (_num(p.get("fumble_lost")) or 0)
        acc[t]["giveaways"] += giveaway
        if p.get("play_type") not in OFFENSE_PLAYS:
            continue
        y, e = _num(p.get("yards_gained")) or 0.0, _num(p.get("epa"))
        acc[t]["off_yds"] += y
        if e is not None:
            acc[t]["off_epa"] += e
            acc[t]["off_plays"] += 1
        td = _num(p.get("touchdown")) == 1.0 and p.get("td_team") == t
        acc[t]["off_td"] += td
        drive = _num(p.get("fixed_drive"))
        yl = _num(p.get("yardline_100"))
        if drive is not None:
            d = drives.setdefault((p["game_id"], drive, t), {"rz": False, "td": False})
            d["rz"] |= yl is not None and yl <= 20
            d["td"] |= td
        del opp
    # Defense and takeaways are the opponent's offense in the same games.
    opp_of = {}
    for g in games:
        opp_of[(g["game_id"], g["home_team"])] = g["away_team"]
        opp_of[(g["game_id"], g["away_team"])] = g["home_team"]
    per_game = collections.defaultdict(lambda: collections.Counter())
    for p in pbp:
        if p.get("season_type") != "REG" or p["game_id"] not in ids or not isinstance(p.get("posteam"), str):
            continue
        d = opp_of.get((p["game_id"], p["posteam"]))
        if not d:
            continue
        per_game[d]["takeaways"] += (_num(p.get("interception")) or 0) + (_num(p.get("fumble_lost")) or 0)
        if p.get("play_type") in OFFENSE_PLAYS:
            per_game[d]["def_yds"] += _num(p.get("yards_gained")) or 0.0
            e = _num(p.get("epa"))
            if e is not None:
                per_game[d]["def_epa"] += e
                per_game[d]["def_plays"] += 1
    trips, trip_tds = collections.Counter(), collections.Counter()
    for (_, _, t), d in drives.items():
        if d["rz"]:
            trips[t] += 1
            trip_tds[t] += d["td"]
    out = {}
    for t, games_t in log.items():
        n = len(games_t)
        a, dd = acc[t], per_game[t]
        out[t] = {
            "games": n, "wins": sum(g["for"] > g["against"] for g in games_t),
            "losses": sum(g["for"] < g["against"] for g in games_t),
            "ties": sum(g["for"] == g["against"] for g in games_t), "log": games_t,
            "pf": pts[t][0] / n, "pa": pts[t][1] / n,
            "off_yds": a["off_yds"] / n, "def_yds": dd["def_yds"] / n, "off_td": a["off_td"] / n,
            "off_epa": a["off_epa"] / a["off_plays"] if a["off_plays"] else None,
            "def_epa": dd["def_epa"] / dd["def_plays"] if dd["def_plays"] else None,
            "to_margin": (dd["takeaways"] - a["giveaways"]) / n, "to_total": dd["takeaways"] - a["giveaways"],
            "rz_td": trip_tds[t] / trips[t] if trips[t] else None,
        }
    return out


def ranks(table: dict[str, dict]) -> dict[str, dict[str, int]]:
    """team -> {metric: league rank}, 1 = best, ties share the better rank."""
    out: dict[str, dict[str, int]] = collections.defaultdict(dict)
    for key, _, higher, _ in METRICS:
        vals = [(t, v[key]) for t, v in table.items() if v.get(key) is not None]
        for t, v in vals:
            out[t][key] = 1 + sum((o > v) if higher else (o < v) for _, o in vals)
    return out


# ── chart ────────────────────────────────────────────────────────────────────
def _name(team: str) -> str:
    return TEAM_NAMES.get(team, team).split()[-1]


def tape_chart(away: str, home: str, table: dict, rk: dict, path: Path, context: str) -> bool:
    if away not in table or home not in table:
        return False
    teams_n = len(table)
    a, h = table[away], table[home]
    height = 9.4
    fig = plt.figure(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    top = fig.add_axes((0.04, 0.35, 0.92, 0.53))
    top.set_axis_off()
    top.set_xlim(-1.45, 1.45)
    n = len(METRICS) + 1
    top.set_ylim(n - 0.4, -1.05)
    top.text(-0.55, -0.85, f"{_name(away)} (away)", ha="center", va="center", fontsize=10, color=AWAY_COLOUR,
             fontweight="bold")
    top.text(0.55, -0.85, f"{_name(home)} (home)", ha="center", va="center", fontsize=10, color=HOME_COLOUR,
             fontweight="bold")
    rec = lambda t: f"{t['wins']}-{t['losses']}" + (f"-{t['ties']}" if t["ties"] else "")  # noqa: E731
    top.text(0, 0, "Record", ha="center", va="center", fontsize=8, color=INK_2)
    top.text(-0.2, 0, rec(a), ha="right", va="center", fontsize=9, color=INK, fontweight="bold")
    top.text(0.2, 0, rec(h), ha="left", va="center", fontsize=9, color=INK, fontweight="bold")
    for y, (key, label, _, fmt) in enumerate(METRICS, start=1):
        top.text(0, y - 0.38, label, ha="center", va="center", fontsize=7.6, color=INK_2)
        for team, sign, colour in ((away, -1, AWAY_COLOUR), (home, 1, HOME_COLOUR)):
            v, r = table[team].get(key), rk[team].get(key)
            if v is None or r is None:
                continue
            strength = (teams_n + 1 - r) / teams_n
            mine, theirs = rk[team].get(key), rk[home if team == away else away].get(key)
            better = theirs is not None and mine < theirs
            top.barh(y + 0.08, sign * strength, left=sign * 0.06, height=0.42, color=colour,
                     alpha=0.95 if better else 0.35)
            end = sign * (0.06 + strength)
            top.text(end + sign * 0.03, y + 0.08, f"{fmt(v)}  ({_ord(r)})", ha="left" if sign > 0 else "right",
                     va="center", fontsize=7.6, color=INK, fontweight="bold" if better else "normal")

    for i, (team, colour) in enumerate(((away, AWAY_COLOUR), (home, HOME_COLOUR))):
        ax = fig.add_axes((0.07 + i * 0.48, 0.06, 0.42, 0.22))
        ax.set_facecolor(SURFACE)
        lg = table[team]["log"]
        margins = [g["for"] - g["against"] for g in lg]
        xs = range(len(lg))
        ax.bar(xs, margins, color=[GOOD if m > 0 else BAD if m < 0 else INK_2 for m in margins], width=0.62)
        for x, g, m in zip(xs, lg, margins):
            ax.text(x, m + (1.2 if m >= 0 else -1.2), f"{g['for']:.0f}-{g['against']:.0f}", ha="center",
                    va="bottom" if m >= 0 else "top", fontsize=6.4, color=INK)
        ax.set_xticks(list(xs), [f"W{g['week']}\n{'vs' if g['home'] else '@'} {g['opp']}" for g in lg],
                     fontsize=6.4, color=INK_2)
        ax.axhline(0, color=INK_2, linewidth=0.6)
        lim = max(10, max(abs(m) for m in margins) + 6)
        ax.set_ylim(-lim, lim)
        ax.tick_params(axis="y", labelsize=6.4, colors=INK_2)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        for side in ("left", "bottom"):
            ax.spines[side].set_color("#d8d6d0")
        ax.set_title(f"{_name(team)}: each game's final margin", loc="left", fontsize=8.4, color=colour,
                     fontweight="bold")
    sub = "\n".join(textwrap.wrap(
        f"Season to date, {table[away]['games']} and {table[home]['games']} games. Bars sized by league rank among "
        f"{teams_n} teams (longer = better); the darker bar is the better of the two. {context}", 125))
    return _finish(fig, path, f"{_name(away)} at {_name(home)}: the tale of the tape", sub, height)


def _ord(n: int) -> str:
    return f"{n}{'th' if 11 <= n % 100 <= 13 else {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')}"


# ── data ─────────────────────────────────────────────────────────────────────
def load_pbp(season: int) -> list[dict]:
    import pandas as pd

    from model.nfl_market_scenarios import pbp_url

    frame = pd.read_csv(pbp_url(season), usecols=lambda c: c in PBP_COLS, compression="gzip", low_memory=False)
    missing = set(PBP_COLS) - set(frame.columns)
    if missing:
        raise RuntimeError(f"play-by-play {season} is missing columns: {sorted(missing)}")
    return frame.to_dict("records")


def load_schedule(season: int) -> list[dict]:
    import pandas as pd

    from ingest.ff_independent import NFLVERSE_SCHEDULE_URL

    f = pd.read_csv(NFLVERSE_SCHEDULE_URL)
    f = f[(f.season == season) & (f.game_type == "REG")]
    rows = f[["game_id", "week", "gameday", "home_team", "away_team", "home_score", "away_score"]].to_dict("records")
    for r in rows:
        for k in ("home_score", "away_score"):
            if isinstance(r[k], float) and math.isnan(r[k]):
                r[k] = None
    return rows


def context_line(away: str, home: str, game: dict, schedule: list[dict], season: int) -> str:
    """Market line now vs its first capture this week, any bye, and key players out (10%+ of team targets)."""
    import datetime as dt

    from model.nfl_game_leader_history_charts import load
    from model.nfl_win_prob_chart import ET, game_rows, load_games

    parts = []
    day = dt.date.fromisoformat(game["gameday"])
    alias = {"LA": "LAR"}
    for g in game_rows(load_games(day)):
        if (g["away"], g["home"]) == (alias.get(away, away), alias.get(home, home)):
            fav = g["home"] if g["p_home"] > 0.5 else g["away"]
            p = max(g["p_home"], g["p_away"])
            text = f"Market now: {fav} {p:.0%}"
            if g.get("spread") is not None:
                text += f", {fav} -{abs(g['spread']):g}"
            if g.get("move") is not None and g.get("opened_at") is not None:
                opened = g["opened_at"].astimezone(ET).strftime("%b %d").replace(" 0", " ")
                text += f" ({fav} {'up' if (g['move'] > 0) == (fav == g['home']) else 'down'} " \
                        f"{abs(g['move']) * 100:.0f} points of win chance since {opened})"
            parts.append(text + ".")
    for team in (away, home):
        weeks = sorted(g["week"] for g in schedule if team in (g["home_team"], g["away_team"]))
        if game["week"] - 1 >= 1 and game["week"] - 1 not in weeks:
            parts.append(f"{_name(team)} come off a bye.")
    rows = [r for r in load(season) if r.get("team") in (away, home)]
    share = collections.Counter()
    tot = collections.Counter()
    for r in rows:
        tg = float(r.get("targets") or 0)
        share[(r["team"], r["player_display_name"])] += tg
        tot[r["team"]] += tg
    out = _players_out(away, home)
    missing = [f"{n} ({t}, {share[(t, n)] / tot[t]:.0%} of targets, {s})" for t, n, s in out
               if tot[t] and share[(t, n)] / tot[t] >= 0.10]
    if missing:
        parts.append("Out: " + "; ".join(missing) + ".")
    return " ".join(parts)


def _players_out(away: str, home: str) -> list[tuple[str, str, str]]:
    import os

    import psycopg2
    from dotenv import load_dotenv

    load_dotenv(".env")
    with psycopg2.connect(os.environ["DATABASE_URL"]) as conn, conn.cursor() as cur:
        cur.execute("""SELECT team_abbrev, canonical_name, injury_status FROM ff_players
                       WHERE season = %s AND team_abbrev IN (%s, %s) AND injury_status IN ('Out', 'IR', 'Doubtful')""",
                    (SEASON, away, home))
        return cur.fetchall()


def build_all(out_dir: Path, away: str, home: str, season: int = SEASON) -> list[tuple[str, bool]]:
    schedule = load_schedule(season)
    game = next((g for g in schedule if (g["away_team"], g["home_team"]) == (away, home)), None)
    if game is None:
        raise SystemExit(f"no {season} regular-season game {away} at {home}")
    table = team_table(load_pbp(season), schedule, game["week"])
    name = f"nfl_take_tape_{away.lower()}_{home.lower()}.png"
    ctx = context_line(away, home, game, schedule, season)
    return [(name, tape_chart(away, home, table, ranks(table), Path(out_dir) / name, ctx))]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--away", required=True)
    ap.add_argument("--home", required=True)
    ap.add_argument("--season", type=int, default=SEASON)
    ap.add_argument("--out-dir", default=str(DEFAULT_DIR))
    args = ap.parse_args(argv)
    for name, ok in build_all(Path(args.out_dir), args.away.upper(), args.home.upper(), args.season):
        print(f"{name}: {'written' if ok else 'skipped, no data'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
