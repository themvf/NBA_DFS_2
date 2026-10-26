"""Reader chart: two teams side by side before they meet, season to date.

Descriptive, no edge claimed. One chart in two parts:

  nfl_take_tape_<away>_<home>.png   top: six small panels, one per unit (points, yards, touchdowns, EPA, turnovers,
                                    red zone), each with an offense row and a defense row; the bars show the real
                                    values on that panel's own scale, a grey tick marks the league average, and the
                                    league rank is printed beside each value. Bottom: each team's games, one bar per
                                    game for the final margin, opponent underneath

Stats (regular season, games before the matchup): points scored and allowed a game; offensive yards gained and
allowed a game; offensive touchdowns scored and allowed a game (touchdowns on pass and run plays); EPA a play on
offense and allowed on defense; turnovers forced and committed a game; red-zone touchdown rate on offense and allowed
(drives that reached the 20 and ended in an offensive touchdown). Ranks are among all 32 teams, 1st = best, so on a
row where lower is better the shortest bar ranks highest. The subtitle adds the market's line now and at its first
capture this week, any bye, and any player ruled out or on injured reserve who has 10%+ of his team's targets.

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
# Panel title, value format, and its two rows: (key, row label, higher is better).
PANELS = (
    ("Points a game", "{:.1f}", (("pf", "Scored", True), ("pa", "Allowed", False))),
    ("Yards a game", "{:.0f}", (("off_yds", "Gained", True), ("def_yds", "Allowed", False))),
    ("Offensive touchdowns a game", "{:.1f}", (("off_td", "Scored", True), ("def_td", "Allowed", False))),
    ("EPA a play", "{:+.2f}", (("off_epa", "Offense", True), ("def_epa", "Allowed", False))),
    ("Turnovers a game", "{:.1f}", (("takeaways", "Forced", True), ("giveaways", "Committed", False))),
    ("Red-zone touchdown rate", "{:.0%}", (("rz_td", "Offense", True), ("def_rz_td", "Allowed", False))),
)
METRICS = tuple((key, higher) for _, _, rows in PANELS for key, _, higher in rows)


def _num(v) -> float | None:
    return float(v) if isinstance(v, (int, float)) and not math.isnan(v) else None


# ── pure computations ────────────────────────────────────────────────────────
def team_table(pbp: list[dict], schedule: list[dict], before_week: int) -> dict[str, dict]:
    """team -> season stats over regular-season games played before ``before_week``, plus the game log.

    A team's defensive numbers are its opponents' offensive numbers in the same games."""
    games = [g for g in schedule if g["week"] < before_week and g.get("home_score") is not None]
    ids = {g["game_id"] for g in games}
    opp_of = {}
    log: dict[str, list] = collections.defaultdict(list)
    pts = collections.defaultdict(lambda: [0.0, 0.0])
    for g in sorted(games, key=lambda g: g["week"]):
        opp_of[(g["game_id"], g["home_team"])] = g["away_team"]
        opp_of[(g["game_id"], g["away_team"])] = g["home_team"]
        for team, opp, f, a, home in ((g["home_team"], g["away_team"], g["home_score"], g["away_score"], True),
                                      (g["away_team"], g["home_team"], g["away_score"], g["home_score"], False)):
            log[team].append({"week": g["week"], "opp": opp, "home": home, "for": f, "against": a})
            pts[team][0] += f
            pts[team][1] += a
    off = collections.defaultdict(collections.Counter)   # by the team with the ball
    drives: dict[tuple, dict] = {}
    for p in pbp:
        if p.get("season_type") != "REG" or p["game_id"] not in ids or not isinstance(p.get("posteam"), str):
            continue
        t = p["posteam"]
        off[(p["game_id"], t)]["giveaways"] += (_num(p.get("interception")) or 0) + (_num(p.get("fumble_lost")) or 0)
        if p.get("play_type") not in OFFENSE_PLAYS:
            continue
        c = off[(p["game_id"], t)]
        c["yds"] += _num(p.get("yards_gained")) or 0.0
        e = _num(p.get("epa"))
        if e is not None:
            c["epa"] += e
            c["plays"] += 1
        td = _num(p.get("touchdown")) == 1.0 and p.get("td_team") == t
        c["td"] += td
        drive, yl = _num(p.get("fixed_drive")), _num(p.get("yardline_100"))
        if drive is not None:
            d = drives.setdefault((p["game_id"], drive, t), {"rz": False, "td": False})
            d["rz"] |= yl is not None and yl <= 20
            d["td"] |= td
    for (gid, _, t), d in drives.items():
        if d["rz"]:
            off[(gid, t)]["trips"] += 1
            off[(gid, t)]["trip_tds"] += d["td"]
    mine = collections.defaultdict(collections.Counter)
    theirs = collections.defaultdict(collections.Counter)
    for (gid, t), c in off.items():
        mine[t].update(c)
        if (gid, t) in opp_of:
            theirs[opp_of[(gid, t)]].update(c)
    out = {}
    for t, games_t in log.items():
        n = len(games_t)
        o, d = mine[t], theirs[t]
        out[t] = {
            "games": n, "wins": sum(g["for"] > g["against"] for g in games_t),
            "losses": sum(g["for"] < g["against"] for g in games_t),
            "ties": sum(g["for"] == g["against"] for g in games_t), "log": games_t,
            "pf": pts[t][0] / n, "pa": pts[t][1] / n,
            "off_yds": o["yds"] / n, "def_yds": d["yds"] / n,
            "off_td": o["td"] / n, "def_td": d["td"] / n,
            "off_epa": o["epa"] / o["plays"] if o["plays"] else None,
            "def_epa": d["epa"] / d["plays"] if d["plays"] else None,
            "takeaways": d["giveaways"] / n, "giveaways": o["giveaways"] / n,
            "rz_td": o["trip_tds"] / o["trips"] if o["trips"] else None,
            "def_rz_td": d["trip_tds"] / d["trips"] if d["trips"] else None,
        }
    return out


def ranks(table: dict[str, dict]) -> dict[str, dict[str, int]]:
    """team -> {metric: league rank}, 1 = best, ties share the better rank."""
    out: dict[str, dict[str, int]] = collections.defaultdict(dict)
    for key, higher in METRICS:
        vals = [(t, v[key]) for t, v in table.items() if v.get(key) is not None]
        for t, v in vals:
            out[t][key] = 1 + sum((o > v) if higher else (o < v) for _, o in vals)
    return out


def league_average(table: dict[str, dict], key: str) -> float | None:
    vals = [v[key] for v in table.values() if v.get(key) is not None]
    return sum(vals) / len(vals) if vals else None


# ── chart ────────────────────────────────────────────────────────────────────
def _name(team: str) -> str:
    return TEAM_NAMES.get(team, team).split()[-1]


def _panel(ax, title: str, fmt: str, rows, away: str, home: str, table: dict, rk: dict) -> None:
    ax.set_facecolor(SURFACE)
    vals = [table[t][k] for k, _, _ in rows for t in (away, home) if table[t].get(k) is not None]
    avgs = [league_average(table, k) for k, _, _ in rows]
    span = [v for v in vals + avgs if v is not None]
    if not span:
        ax.set_axis_off()
        ax.set_title(f"{title}: no data yet", loc="left", fontsize=8.4, color=INK_2)
        return
    lo, hi = min(0.0, min(span)), max(0.0, max(span))
    pad = (hi - lo) * 0.62 or 1.0
    ax.set_xlim(lo - (pad if lo < 0 else 0), hi + pad)
    for y, (key, label, higher) in enumerate(rows):
        a_rank, h_rank = rk[away].get(key), rk[home].get(key)
        for team, dy, colour in ((away, -0.19, AWAY_COLOUR), (home, 0.19, HOME_COLOUR)):
            v, r = table[team].get(key), rk[team].get(key)
            if v is None:
                continue
            other = h_rank if team == away else a_rank
            better = other is not None and r < other
            ax.barh(y + dy, v, height=0.34, color=colour, alpha=0.95 if better else 0.4)
            avg = league_average(table, key)
            at = v
            # Start the label past the league-average line when the line would cut through it.
            if avg is not None and v >= 0 and v <= avg < v + (hi - lo) * 0.3:
                at = avg
            elif avg is not None and v < 0 and v - (hi - lo) * 0.3 < avg <= v:
                at = avg
            ax.text(at + (hi - lo) * 0.03 * (1 if v >= 0 else -1), y + dy, f"{fmt.format(v)}  {_ord(r)}",
                    ha="left" if v >= 0 else "right", va="center", fontsize=6.9, color=INK, zorder=5,
                    fontweight="bold" if better else "normal",
                    bbox=dict(facecolor=SURFACE, edgecolor="none", pad=0.4))
        avg = league_average(table, key)
        if avg is not None:
            ax.plot([avg, avg], [y - 0.42, y + 0.42], color=INK_2, linewidth=1.2, linestyle=(0, (2, 1.5)),
                    zorder=3)
    ax.axvline(0, color="#cfcdc7", linewidth=0.7)
    ax.set_yticks(range(len(rows)), [f"{lab}\n{'higher' if hi_ else 'lower'} is better"
                                     for _, lab, hi_ in rows], fontsize=6.8, color=INK)
    ax.set_ylim(len(rows) - 0.5, -0.55)
    ax.set_xticks([])
    for side in ax.spines.values():
        side.set_visible(False)
    ax.tick_params(axis="y", length=0)
    ax.set_title(title, loc="left", fontsize=8.4, color=INK, fontweight="bold")


def tape_chart(away: str, home: str, table: dict, rk: dict, path: Path, context: str) -> bool:
    if away not in table or home not in table:
        return False
    teams_n = len(table)
    a, h = table[away], table[home]
    height = 10.6
    fig = plt.figure(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    rec = lambda t: f"{t['wins']}-{t['losses']}" + (f"-{t['ties']}" if t["ties"] else "")  # noqa: E731
    fig.text(0.09, 0.87, f"{_name(away)} (away) {rec(a)}", fontsize=10, color=AWAY_COLOUR, fontweight="bold")
    fig.text(0.40, 0.87, f"{_name(home)} (home) {rec(h)}", fontsize=10, color=HOME_COLOUR, fontweight="bold")
    fig.text(0.70, 0.87, "- - league average", fontsize=7.6, color=INK_2)
    for i, (title, fmt, rows) in enumerate(PANELS):
        col, row = i % 2, i // 2
        ax = fig.add_axes((0.13 + col * 0.48, 0.715 - row * 0.16, 0.33, 0.12))
        _panel(ax, title, fmt, rows, away, home, table, rk)

    for i, (team, colour) in enumerate(((away, AWAY_COLOUR), (home, HOME_COLOUR))):
        ax = fig.add_axes((0.07 + i * 0.48, 0.05, 0.42, 0.24))
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
        lim = max(10, max(abs(m) for m in margins) + 7)
        ax.set_ylim(-lim, lim)
        ax.tick_params(axis="y", labelsize=6.4, colors=INK_2)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        for side in ("left", "bottom"):
            ax.spines[side].set_color("#d8d6d0")
        ax.set_title(f"{_name(team)}: each game's final margin", loc="left", fontsize=8.4, color=colour,
                     fontweight="bold")
    sub = "\n".join(textwrap.wrap(
        f"Season to date, {a['games']} and {h['games']} games. Bars show the actual numbers; the rank beside each is "
        f"among {teams_n} teams (1st = best), and the darker bar is the better of the two. {context}", 125))
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
                       WHERE season = %s AND team_abbrev IN (%s, %s) AND upper(injury_status) IN ('OUT', 'IR', 'DOUBTFUL')""",
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
