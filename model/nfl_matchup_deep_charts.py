"""Reader charts: a deeper look at two teams before they meet, season to date. Companion to the tale of the tape.

Descriptive, no edge claimed: none of these numbers has been shown to predict better than the betting line.

  nfl_take_tape_luck_<away>_<home>.png   "Is the record real?": win rate against the rate the points scored and
                                         allowed would predict, one-score games, the share of fumbles each team
                                         recovered, schedule strength, and recent form against the season
  nfl_take_tape_edge_<away>_<home>.png   "Where the edge is": each offense against the other defense (passing,
                                         rushing, explosive plays, third down, sacks), the two quarterbacks, the
                                         line through the week, and players listed out or doubtful

Every bar is the actual value; each row is drawn on its own scale (rows mix units), so compare bars within a row.
The rank beside each value is among all 32 teams, 1st = best for that side of the ball; the darker bar in a row is
the better-ranked of the two. A grey tick marks the league average.

Definitions (regular season, games before the matchup's week; pass and run plays unless stated):
  expected win rate   points for^2.37 / (points for^2.37 + points against^2.37)
  one-score game      final margin of 8 points or fewer
  fumbles recovered   share of every fumble in the team's games (by either side) that the team recovered
  schedule strength   mean, over the team's games, of the opponent's net EPA a play (offense minus defense allowed),
                      from all of that opponent's games in the window; higher = tougher
  explosive play      a gain of 20+ yards
  third down          third-down plays converted / third-down plays with a conversion result
  sack rate           sacks / dropbacks
  quarterback         the passer with most dropbacks in the team's latest game, his season numbers;
                      CPOE = completion percentage above expected (nflverse)

    python -m model.nfl_matchup_deep_charts --away IND --home MIN --out-dir DIR
"""

from __future__ import annotations

import argparse
import collections
import math
import re
import textwrap
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402

from model.cfb_pattern_charts import DPI, INK, INK_2, SURFACE  # noqa: E402
from model.nfl_matchup_tape_chart import AWAY_COLOUR, HOME_COLOUR, _name, _ord, load_schedule  # noqa: E402
from model.story_charts import WIDTH_IN, _finish  # noqa: E402

SEASON = 2026
DEFAULT_DIR = Path("artifacts") / "nfl_market_scenarios"
PBP_COLS = ("game_id", "week", "season_type", "posteam", "defteam", "play_type", "yards_gained", "epa",
            "qb_dropback", "sack", "interception", "fumble", "fumble_recovery_1_team", "third_down_converted",
            "third_down_failed", "down", "cpoe", "passer_player_name")
PYTH_EXP = 2.37
ONE_SCORE = 8
EXPLOSIVE = 20
LAST_N = 3
OFFENSE_PLAYS = ("pass", "run")
DB_TEAM = {"LA": "LAR"}

# metric -> (higher is better, value format)
METRICS = {
    "win_pct": (True, "{:.0%}"), "pyth_pct": (True, "{:.0%}"), "one_score_pct": (True, "{:.0%}"),
    "fum_rec": (True, "{:.0%}"), "sos": (True, "{:+.3f}"),
    "off_epa": (True, "{:+.2f}"), "off_epa_last": (True, "{:+.2f}"),
    "def_epa": (False, "{:+.2f}"), "def_epa_last": (False, "{:+.2f}"),
    "pass_off": (True, "{:+.2f}"), "rush_off": (True, "{:+.2f}"),
    "pass_def": (False, "{:+.2f}"), "rush_def": (False, "{:+.2f}"),
    "expl_off": (True, "{:.1%}"), "expl_def": (False, "{:.1%}"),
    "third_off": (True, "{:.0%}"), "third_def": (False, "{:.0%}"),
    "sack_off": (False, "{:.1%}"), "sack_def": (True, "{:.1%}"),
    "qb_epa": (True, "{:+.2f}"), "qb_cpoe": (True, "{:+.1f}"), "qb_int": (False, "{:.1%}"),
    "qb_sack": (False, "{:.1%}"),
}


def _num(v) -> float | None:
    return float(v) if isinstance(v, (int, float)) and not math.isnan(v) else None


def _ratio(a: float, b: float) -> float | None:
    return a / b if b else None


# ── pure computations ────────────────────────────────────────────────────────
def team_stats(pbp: list[dict], schedule: list[dict], before_week: int) -> dict[str, dict]:
    """team -> every metric above, over completed regular-season games before ``before_week``."""
    games = [g for g in schedule if g["week"] < before_week and g.get("home_score") is not None]
    ids = {g["game_id"] for g in games}
    log: dict[str, list] = collections.defaultdict(list)
    for g in sorted(games, key=lambda g: g["week"]):
        for team, opp, f, a in ((g["home_team"], g["away_team"], g["home_score"], g["away_score"]),
                                (g["away_team"], g["home_team"], g["away_score"], g["home_score"])):
            log[team].append({"game_id": g["game_id"], "week": g["week"], "opp": opp, "for": f, "against": a})

    c = collections.defaultdict(collections.Counter)            # team -> season counters
    by_game = collections.defaultdict(collections.Counter)      # (team, game) -> offense/defense EPA sums
    qb = collections.defaultdict(collections.Counter)           # (team, passer) -> passer counters
    qb_game = collections.defaultdict(collections.Counter)      # (team, game) -> passer -> dropbacks
    for p in pbp:
        if p.get("season_type") != "REG" or p["game_id"] not in ids:
            continue
        t, d = p.get("posteam"), p.get("defteam")
        if not isinstance(t, str) or not isinstance(d, str):
            continue
        g = p["game_id"]
        if _num(p.get("fumble")) == 1.0:
            rec = p.get("fumble_recovery_1_team")
            for side in (t, d):
                c[side]["fumbles"] += 1
                c[side]["fum_rec"] += rec == side
        if p.get("play_type") not in OFFENSE_PLAYS:
            continue
        e = _num(p.get("epa"))
        y = _num(p.get("yards_gained")) or 0.0
        dropback = _num(p.get("qb_dropback")) == 1.0
        sack = _num(p.get("sack")) == 1.0
        if e is not None:
            c[t]["off_epa"] += e
            c[t]["off_n"] += 1
            c[d]["def_epa"] += e
            c[d]["def_n"] += 1
            by_game[(t, g)]["off_epa"] += e
            by_game[(t, g)]["off_n"] += 1
            by_game[(d, g)]["def_epa"] += e
            by_game[(d, g)]["def_n"] += 1
            unit = "pass" if dropback else "rush"
            c[t][f"{unit}_off"] += e
            c[t][f"{unit}_off_n"] += 1
            c[d][f"{unit}_def"] += e
            c[d][f"{unit}_def_n"] += 1
        if not sack:
            c[t]["plays"] += 1
            c[d]["plays_faced"] += 1
            c[t]["expl"] += y >= EXPLOSIVE
            c[d]["expl_allowed"] += y >= EXPLOSIVE
        if dropback:
            c[t]["dropbacks"] += 1
            c[d]["opp_dropbacks"] += 1
            c[t]["sacked"] += sack
            c[d]["sacks"] += sack
            name = p.get("passer_player_name")
            if isinstance(name, str):
                qb_game[(t, g)][name] += 1
                q = qb[(t, name)]
                q["dropbacks"] += 1
                q["sacks"] += sack
                q["ints"] += _num(p.get("interception")) == 1.0
                if e is not None:
                    q["epa"] += e
                    q["epa_n"] += 1
                cp = _num(p.get("cpoe"))
                if cp is not None:
                    q["cpoe"] += cp
                    q["cpoe_n"] += 1
        if _num(p.get("down")) == 3.0:
            conv, fail = _num(p.get("third_down_converted")) == 1.0, _num(p.get("third_down_failed")) == 1.0
            if conv or fail:
                c[t]["third_n"] += 1
                c[t]["third_conv"] += conv
                c[d]["third_def_n"] += 1
                c[d]["third_def_conv"] += conv

    out: dict[str, dict] = {}
    for t, lg in log.items():
        n = len(lg)
        k = c[t]
        pf, pa = sum(x["for"] for x in lg), sum(x["against"] for x in lg)
        wins = sum(x["for"] > x["against"] for x in lg)
        ties = sum(x["for"] == x["against"] for x in lg)
        close = [x for x in lg if abs(x["for"] - x["against"]) <= ONE_SCORE]
        cw = sum(x["for"] > x["against"] for x in close)
        cl = sum(x["for"] < x["against"] for x in close)
        ct = len(close) - cw - cl
        last = lg[-LAST_N:]
        lo = [by_game[(t, x["game_id"])] for x in last]
        # The quarterback is whoever took the most dropbacks in the team's latest game (so an injured
        # starter's replacement is the one shown); his numbers are his whole season with this team.
        latest = qb_game.get((t, lg[-1]["game_id"])) if lg else None
        name = max(latest, key=latest.get) if latest else None
        if name is None:
            passers = [(nm, q) for (team, nm), q in qb.items() if team == t]
            name = max(passers, key=lambda nq: nq[1]["dropbacks"])[0] if passers else None
        q = qb[(t, name)] if name else collections.Counter()
        out[t] = {
            "games": n, "wins": wins, "losses": n - wins - ties, "ties": ties, "log": lg,
            "pf": pf, "pa": pa, "win_pct": (wins + 0.5 * ties) / n,
            "pyth_pct": pf ** PYTH_EXP / (pf ** PYTH_EXP + pa ** PYTH_EXP) if pf + pa else None,
            "one_score": (cw, cl, ct), "one_score_pct": _ratio(cw + 0.5 * ct, len(close)),
            "fum_rec": _ratio(k["fum_rec"], k["fumbles"]), "fumbles": (k["fum_rec"], k["fumbles"]),
            "off_epa": _ratio(k["off_epa"], k["off_n"]), "def_epa": _ratio(k["def_epa"], k["def_n"]),
            "off_epa_last": _ratio(sum(x["off_epa"] for x in lo), sum(x["off_n"] for x in lo)),
            "def_epa_last": _ratio(sum(x["def_epa"] for x in lo), sum(x["def_n"] for x in lo)),
            "last_n": len(last),
            "pass_off": _ratio(k["pass_off"], k["pass_off_n"]), "rush_off": _ratio(k["rush_off"], k["rush_off_n"]),
            "pass_def": _ratio(k["pass_def"], k["pass_def_n"]), "rush_def": _ratio(k["rush_def"], k["rush_def_n"]),
            "expl_off": _ratio(k["expl"], k["plays"]), "expl_def": _ratio(k["expl_allowed"], k["plays_faced"]),
            "third_off": _ratio(k["third_conv"], k["third_n"]),
            "third_def": _ratio(k["third_def_conv"], k["third_def_n"]),
            "sack_off": _ratio(k["sacked"], k["dropbacks"]), "sack_def": _ratio(k["sacks"], k["opp_dropbacks"]),
            "qb_name": name, "qb_dropbacks": q["dropbacks"], "qb_epa": _ratio(q["epa"], q["epa_n"]), "qb_cpoe": _ratio(q["cpoe"], q["cpoe_n"]),
            "qb_int": _ratio(q["ints"], q["dropbacks"]), "qb_sack": _ratio(q["sacks"], q["dropbacks"]),
        }
    for t, row in out.items():
        net = [out[x["opp"]]["off_epa"] - out[x["opp"]]["def_epa"] for x in row["log"]
               if x["opp"] in out and out[x["opp"]]["off_epa"] is not None and out[x["opp"]]["def_epa"] is not None]
        row["sos"] = sum(net) / len(net) if net else None
    return out


def ranks(table: dict[str, dict]) -> dict[str, dict[str, int]]:
    """team -> {metric: league rank}, 1 = best (toughest for schedule strength), ties share the better rank."""
    out: dict[str, dict[str, int]] = collections.defaultdict(dict)
    for key, (higher, _) in METRICS.items():
        vals = [(t, v[key]) for t, v in table.items() if v.get(key) is not None]
        for t, v in vals:
            out[t][key] = 1 + sum((o > v) if higher else (o < v) for _, o in vals)
    return out


def league_average(table: dict[str, dict], key: str) -> float | None:
    vals = [v[key] for v in table.values() if v.get(key) is not None]
    return sum(vals) / len(vals) if vals else None


# ── chart ────────────────────────────────────────────────────────────────────
def _panel(ax, title: str, rows, table: dict, rk: dict, away: str) -> None:
    """``rows``: (label, note, [(team, key), (team, key)]). Each row is scaled on its own; values are actual."""
    ax.set_facecolor(SURFACE)
    neg = any((table[t].get(k) or 0) < 0 for _, _, items in rows for t, k in items)
    ax.set_xlim(-1.75 if neg else -0.02, 1.75)
    for y, (_, _, items) in enumerate(rows):
        vals = [table[t].get(k) for t, k in items] + [league_average(table, k) for _, k in items]
        scale = max((abs(v) for v in vals if v is not None), default=0) or 1.0
        rs = [rk[t].get(k) for t, k in items]
        for i, ((team, key), dy) in enumerate(zip(items, (-0.19, 0.19))):
            v = table[team].get(key)
            colour = AWAY_COLOUR if team == away else HOME_COLOUR
            if v is None:
                ax.text(0.03, y + dy, "n/a", ha="left", va="center", fontsize=6.6, color=INK_2)
                continue
            other = rs[1 - i]
            better = rs[i] is not None and other is not None and rs[i] < other
            x = v / scale
            ax.barh(y + dy, x, height=0.34, color=colour, alpha=0.95 if better else 0.4)
            avg = league_average(table, key)
            if avg is not None:
                ax.plot([avg / scale] * 2, [y + dy - 0.2, y + dy + 0.2], color=INK_2, linewidth=1.2, zorder=3)
            at = max(x, avg / scale) if x >= 0 and avg is not None else (min(x, avg / scale) if avg is not None else x)
            ax.text(at + (0.05 if x >= 0 else -0.05), y + dy, f"{METRICS[key][1].format(v)}  {_ord(rs[i])}",
                    ha="left" if x >= 0 else "right", va="center", fontsize=6.7, color=INK, zorder=5,
                    fontweight="bold" if better else "normal")
    ax.axvline(0, color="#cfcdc7", linewidth=0.7)
    ax.set_yticks(range(len(rows)), [lab + (f"\n{note}" if note else "") for lab, note, _ in rows], fontsize=6.8,
                  color=INK)
    ax.set_ylim(len(rows) - 0.5, -0.55)
    ax.set_xticks([])
    for side in ax.spines.values():
        side.set_visible(False)
    ax.tick_params(axis="y", length=0)
    ax.set_title(title, loc="left", fontsize=8.4, color=INK, fontweight="bold")


def _header(fig, away: str, home: str, table: dict, y: float) -> None:
    rec = lambda t: f"{t['wins']}-{t['losses']}" + (f"-{t['ties']}" if t["ties"] else "")  # noqa: E731
    fig.text(0.09, y, f"{_name(away)} (away) {rec(table[away])}", fontsize=10, color=AWAY_COLOUR, fontweight="bold")
    fig.text(0.42, y, f"{_name(home)} (home) {rec(table[home])}", fontsize=10, color=HOME_COLOUR, fontweight="bold")
    fig.text(0.74, y, "| league average", fontsize=7.6, color=INK_2)


def _wrap(text: str) -> str:
    return "\n".join(textwrap.wrap(text, 125))


def luck_chart(away: str, home: str, table: dict, rk: dict, path: Path) -> bool:
    if away not in table or home not in table:
        return False
    a, h = table[away], table[home]
    height = 9.6
    fig = plt.figure(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    _header(fig, away, home, table, 0.845)
    pair = lambda k: [(away, k), (home, k)]  # noqa: E731
    rec = lambda r: f"{r[0]}-{r[1]}" + (f"-{r[2]}" if r[2] else "")  # noqa: E731
    panels = [
        ("Win rate: actual vs expected from points", [("Actual", "", pair("win_pct")),
                                                       ("Expected", "from points for and against", pair("pyth_pct"))]),
        ("One-score games (8 points or fewer)",
         [("Win rate", f"{_name(away)} {rec(a['one_score'])}, {_name(home)} {rec(h['one_score'])}",
           pair("one_score_pct"))]),
        ("Fumbles recovered (about half is normal)",
         [("Share recovered", f"{_name(away)} {a['fumbles'][0]} of {a['fumbles'][1]}, "
           f"{_name(home)} {h['fumbles'][0]} of {h['fumbles'][1]}", pair("fum_rec"))]),
        ("Schedule strength", [("Opponents' net EPA a play", "higher = tougher schedule", pair("sos"))]),
        ("Offense form: EPA a play", [("Season", "", pair("off_epa")),
                                      (f"Last {LAST_N} games", "", pair("off_epa_last"))]),
        ("Defense form: EPA a play allowed", [("Season", "lower is better", pair("def_epa")),
                                              (f"Last {LAST_N} games", "lower is better", pair("def_epa_last"))]),
    ]
    for i, (title, rows) in enumerate(panels):
        col, row = i % 2, i // 2
        ax = fig.add_axes((0.22 + col * 0.49, 0.62 - row * 0.265, 0.25, 0.17))
        _panel(ax, title, rows, table, rk, away)
    exp_a, exp_h = a["pyth_pct"] * a["games"], h["pyth_pct"] * h["games"]
    sub = _wrap(
        f"Season to date, {a['games']} and {h['games']} games. {_name(away)} have won {a['wins']}; their points "
        f"say about {exp_a:.1f}. {_name(home)} have won {h['wins']}; their points say about {exp_h:.1f}. Close-game "
        f"records and fumble recoveries mostly even out over a season. Each row has its own scale; the darker bar is "
        f"the better-ranked team, rank among 32. Descriptive only: none of this beats the betting line.")
    return _finish(fig, path, f"{_name(away)} at {_name(home)}: is the record real?", sub, height)


def edge_chart(away: str, home: str, table: dict, rk: dict, path: Path, line: list[tuple], injuries: list[str],
               line_fav: str) -> bool:
    if away not in table or home not in table:
        return False
    height = 11.2
    fig = plt.figure(figsize=(WIDTH_IN, height), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    _header(fig, away, home, table, 0.87)

    def cross(off: str, dfn: str):
        return [("Passing", "EPA a dropback", [(off, "pass_off"), (dfn, "pass_def")]),
                ("Rushing", "EPA a carry", [(off, "rush_off"), (dfn, "rush_def")]),
                ("Explosive plays", "share of plays gaining 20+", [(off, "expl_off"), (dfn, "expl_def")]),
                ("Third down", "conversion rate", [(off, "third_off"), (dfn, "third_def")]),
                ("Sacks", "share of dropbacks", [(off, "sack_off"), (dfn, "sack_def")])]

    for i, (off, dfn) in enumerate(((away, home), (home, away))):
        ax = fig.add_axes((0.2 + i * 0.49, 0.53, 0.27, 0.29))
        _panel(ax, f"{_name(off)} offense vs {_name(dfn)} defense", cross(off, dfn), table, rk, away)
    qa, qh = table[away]["qb_name"] or "?", table[home]["qb_name"] or "?"
    ax = fig.add_axes((0.2, 0.22, 0.27, 0.22))
    _panel(ax, f"Quarterbacks: {qa} vs {qh}",
           [("EPA a dropback", "", [(away, "qb_epa"), (home, "qb_epa")]),
            ("Completion above expected", "points of percentage", [(away, "qb_cpoe"), (home, "qb_cpoe")]),
            ("Interceptions", "share of dropbacks", [(away, "qb_int"), (home, "qb_int")]),
            ("Sacked", "share of dropbacks", [(away, "qb_sack"), (home, "qb_sack")])], table, rk, away)

    ax = fig.add_axes((0.64, 0.24, 0.32, 0.18))
    ax.set_facecolor(SURFACE)
    if line:
        ts, ys = zip(*line)
        ax.plot(ts, ys, color=INK, linewidth=1.4, drawstyle="steps-post")
        ax.scatter([ts[0], ts[-1]], [ys[0], ys[-1]], color=INK, s=14, zorder=3)
        ax.text(ts[0], ys[0], f" {ys[0]:g}", ha="left", va="bottom", fontsize=6.8, color=INK_2)
        ax.text(ts[-1], ys[-1], f"{ys[-1]:g} ", ha="right", va="bottom", fontsize=6.8, color=INK)
        lo, hi = min(ys), max(ys)
        ax.set_ylim(lo - 1.2, hi + 1.2)
        ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%b %d"))
        ax.set_title(f"The line this week: {_name(line_fav)} favored by", loc="left", fontsize=8.4, color=INK,
                     fontweight="bold")
    else:
        ax.text(0.5, 0.5, "No line captures for this game", ha="center", va="center", fontsize=7.4, color=INK_2,
                transform=ax.transAxes)
        ax.set_title("The line this week", loc="left", fontsize=8.4, color=INK, fontweight="bold")
    ax.tick_params(labelsize=6.6, colors=INK_2)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)

    lines = injuries or ["None listed with 10%+ of their team's targets or carries."]
    fig.text(0.04, 0.135, "Out, doubtful or questionable (players with 10%+ of their team's targets or carries)",
             fontsize=8.4, color=INK, fontweight="bold")
    fig.text(0.04, 0.12, "\n".join(lines[:8]), fontsize=7.2, color=INK, va="top", linespacing=1.6)
    sub = _wrap(
        "Each panel pairs one team's offense with the other team's defense on the same play type. The darker bar is "
        "the better-ranked side, rank among 32 (1st = best for that side). Each row has its own scale. "
        "Quarterback = the passer with most dropbacks in each team's latest game. "
        "Descriptive only: none of this beats the betting line.")
    return _finish(fig, path, f"{_name(away)} at {_name(home)}: where the edge is", sub, height)


# ── data ─────────────────────────────────────────────────────────────────────
def load_pbp(season: int) -> list[dict]:
    import pandas as pd

    from model.nfl_market_scenarios import pbp_url

    frame = pd.read_csv(pbp_url(season), usecols=lambda c: c in PBP_COLS, compression="gzip", low_memory=False)
    missing = set(PBP_COLS) - set(frame.columns)
    if missing:
        raise RuntimeError(f"play-by-play {season} is missing columns: {sorted(missing)}")
    return frame.to_dict("records")


def _connect():
    import os

    import psycopg2
    from dotenv import load_dotenv

    load_dotenv(".env")
    return psycopg2.connect(os.environ["DATABASE_URL"])


def load_line(game_id: str) -> list[tuple]:
    """(captured_at, home team favored by) for every pregame capture of the game."""
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("""SELECT h.captured_at, h.home_spread FROM game_odds_history h
                       JOIN nfl_season_games s ON s.matchup_id = h.matchup_id
                       WHERE h.sport = 'nfl' AND s.nflverse_game_id = %s AND h.captured_at < s.kickoff
                         AND h.home_spread IS NOT NULL
                       ORDER BY h.captured_at""", (game_id,))
        return [(t, -float(s)) for t, s in cur.fetchall()]


def _norm(name: str) -> str:
    return re.sub(r"[^a-z]", "", name.lower()) if isinstance(name, str) else ""


def load_injuries(away: str, home: str, season: int) -> list[str]:
    """Players listed out, doubtful or questionable who carry 10%+ of their team's targets or carries."""
    from model.nfl_game_leader_history_charts import load

    share: dict = {}
    for team in (away, home):
        rows = [r for r in load(season) if r.get("team") == team]
        tg = collections.Counter()
        ca = collections.Counter()
        names = {}
        for r in rows:
            k = _norm(r.get("player_display_name"))
            if not k:
                continue
            names[k] = r["player_display_name"]
            tg[k] += float(r.get("targets") or 0)
            ca[k] += float(r.get("carries") or 0)
        T, C = sum(tg.values()), sum(ca.values())
        for k in names:
            share[(team, k)] = (tg[k] / T if T else 0.0, ca[k] / C if C else 0.0)
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("""SELECT team_abbrev, canonical_name, position, injury_status FROM ff_players
                       WHERE season = %s AND team_abbrev IN (%s, %s)
                         AND upper(injury_status) IN ('OUT', 'IR', 'DOUBTFUL', 'QUESTIONABLE', 'PUP')""",
                    (season, DB_TEAM.get(away, away), DB_TEAM.get(home, home)))
        rows = cur.fetchall()
    back = {v: k for k, v in DB_TEAM.items()}
    out = []
    for team, name, pos, status in rows:
        team = back.get(team, team)
        t, c = share.get((team, _norm(name)), (0.0, 0.0))
        if max(t, c) < 0.10:
            continue
        parts = [f"{t:.0%} of targets" if t >= 0.10 else "", f"{c:.0%} of carries" if c >= 0.10 else ""]
        label = status if status.upper() in ("IR", "PUP") else status.title()
        out.append((max(t, c), f"{_name(team)}: {name} ({pos}), {label}: {', '.join(p for p in parts if p)}"))
    return [s for _, s in sorted(out, reverse=True)]


def build_all(out_dir: Path, away: str, home: str, season: int = SEASON) -> list[tuple[str, bool]]:
    schedule = load_schedule(season)
    game = next((g for g in schedule if (g["away_team"], g["home_team"]) == (away, home)), None)
    if game is None:
        raise SystemExit(f"no {season} regular-season game {away} at {home}")
    table = team_stats(load_pbp(season), schedule, game["week"])
    rk = ranks(table)
    line = load_line(game["game_id"])
    fav = home
    if line and line[-1][1] < 0:
        fav, line = away, [(t, -y) for t, y in line]
    stem = f"{away.lower()}_{home.lower()}"
    out_dir = Path(out_dir)
    return [(f"nfl_take_tape_luck_{stem}.png", luck_chart(away, home, table, rk, out_dir / f"nfl_take_tape_luck_{stem}.png")),
            (f"nfl_take_tape_edge_{stem}.png",
             edge_chart(away, home, table, rk, out_dir / f"nfl_take_tape_edge_{stem}.png", line,
                        load_injuries(away, home, season), fav))]


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
