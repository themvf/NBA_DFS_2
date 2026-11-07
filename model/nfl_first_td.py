"""NFL first touchdown scorer: the college model (model/cfb_first_td.py) on NFL play-by-play.

UNVALIDATED. Version ``nfl-first-td-v1``; ``nfl-first-td-lines-v1`` additionally drops players
DraftKings leaves off its anytime-TD list (prices are never read).

Same three steps as college football:
  1. a share PHI of first touchdowns are returns or defensive scores (no player);
  2. which team: implied totals from the closing spread and total, H^K / (H^K + A^K);
  3. which player: this season's rush+receiving TDs plus KAPPA x league TDs-per-opportunity x
     (carries + targets), last season weighted W_PRIOR, among players with a carry or target for
     the team in either of its last two games; THETA of the team's share left to everyone else.
Volume is carries + targets (the play-by-play records targets, not only catches).

Data: nflverse play-by-play in ``nfl_pbp_archetypes`` / ``nfl_pbp_play_participants`` (2016+),
which carries the closing spread and total on every play and the credited touchdown scorer.
Two-point tries are excluded. A touchdown is offensive when its scorer is the play's rusher or
receiver; anything else (returns, defense, a teammate's fumble recovery) is non-offense.

K and PHI are fitted on 2016-2022, the player settings on 2023-2024, and 2025-2026 are held out.

    python -m model.nfl_first_td backtest --json F
    python -m model.nfl_first_td slate --date 2026-11-08 [--lines]
    python -m model.nfl_first_td publish
    python -m model.nfl_first_td grade
"""
from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from dataclasses import replace
from pathlib import Path

from config import load_config
from db.database import DatabaseManager
from model import cfb_first_td as cf
from model.cfb_first_td import Params

VERSION = "nfl-first-td-v1"
VERSION_LINES = "nfl-first-td-lines-v1"
FIT_SEASONS_TEAM = tuple(range(2016, 2023))
FIT_SEASONS_PLAYER = (2023, 2024)
TEST_SEASONS = (2025, 2026)
PUBLISH_HOURS = 48
PUBLISH_TOP = 10
# Filled from ``backtest`` (artifacts/nfl_first_td/backtest_v1.json).
FITTED = Params(k=1.4, phi=0.05910290237467018, theta=0.05, kappa=4.0, w_prior=0.0, tau=0.9)

CHIP_GOAL_LINE_SHARE, CHIP_GOAL_LINE_MIN_TEAM = 0.4, 3
CHIP_NEW_ROLE_RATIO, CHIP_NEW_ROLE_MIN = 1.5, 10.0
CHIP_QB_PASS_ATT, CHIP_QB_RUSH_TD = 30, 2
CHIP_INSIDE_TEN_SHARE, CHIP_INSIDE_TEN_MIN_TEAM = 0.35, 12
CHIP_BIG_FAVOURITE = 7.0

DDL = [
    """CREATE TABLE IF NOT EXISTS nfl_first_td_runs (
        id BIGSERIAL PRIMARY KEY, version TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        params JSONB NOT NULL, games INTEGER NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS nfl_first_td_predictions (
        run_id BIGINT NOT NULL REFERENCES nfl_first_td_runs(id), game_id TEXT NOT NULL,
        season INTEGER NOT NULL, week INTEGER, kickoff TIMESTAMPTZ NOT NULL,
        away TEXT NOT NULL, home TEXT NOT NULL, away_implied DOUBLE PRECISION NOT NULL,
        home_implied DOUBLE PRECISION NOT NULL, p_home_first DOUBLE PRECISION NOT NULL,
        other_offense DOUBLE PRECISION NOT NULL, non_offense DOUBLE PRECISION NOT NULL,
        players JSONB NOT NULL, dist JSONB NOT NULL, availability JSONB, context JSONB,
        PRIMARY KEY (run_id, game_id))""",
    "CREATE INDEX IF NOT EXISTS idx_nfl_first_td_predictions_game ON nfl_first_td_predictions (game_id, run_id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_nfl_first_td_predictions_kickoff ON nfl_first_td_predictions (kickoff)",
    """CREATE TABLE IF NOT EXISTS nfl_first_td_grades (
        run_id BIGINT NOT NULL REFERENCES nfl_first_td_runs(id), game_id TEXT NOT NULL,
        version TEXT NOT NULL, season INTEGER NOT NULL, week INTEGER, kickoff TIMESTAMPTZ NOT NULL,
        outcome TEXT NOT NULL, scorer TEXT, prob DOUBLE PRECISION NOT NULL, top_pick_hit BOOLEAN NOT NULL,
        graded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY (run_id, game_id))""",
]


# ── data ─────────────────────────────────────────────────────────────────────
def load_box(db: DatabaseManager) -> list[dict]:
    """One row per (game, team, player): carries, targets, passes, touches inside the 10, rush/rec TDs.

    Field names follow the college box score (rush_att = carries, receptions = targets) so the
    shared candidate and scoring code reads both sports."""
    rows = db.execute("""
        WITH pl AS (SELECT game_id, play_id, season, week, yardline_100 FROM nfl_pbp_archetypes
                    WHERE two_point_result IS NULL)
        SELECT r.game_id, pl.season, pl.week, r.team, r.player_id AS pid, max(r.player_name) AS name,
               count(*) FILTER (WHERE r.role = 'rusher') AS rush_att,
               count(*) FILTER (WHERE r.role = 'receiver') AS receptions,
               count(*) FILTER (WHERE r.role = 'passer') AS pass_att,
               count(*) FILTER (WHERE r.role IN ('rusher', 'receiver') AND pl.yardline_100 <= 10) AS inside10,
               count(*) FILTER (WHERE r.role = 'rusher' AND td.player_id IS NOT NULL) AS rush_td,
               count(*) FILTER (WHERE r.role = 'receiver' AND td.player_id IS NOT NULL) AS rec_td
        FROM nfl_pbp_play_participants r
        JOIN pl ON pl.game_id = r.game_id AND pl.play_id = r.play_id
        LEFT JOIN nfl_pbp_play_participants td ON td.game_id = r.game_id AND td.play_id = r.play_id
             AND td.player_id = r.player_id AND td.role = 'touchdown'
        WHERE r.role IN ('rusher', 'receiver', 'passer') AND r.player_id IS NOT NULL AND r.team IS NOT NULL
        GROUP BY r.game_id, pl.season, pl.week, r.team, r.player_id""")
    out = []
    for r in rows:
        d = dict(r)
        d["cfbd_game_id"] = d.pop("game_id")
        d["season_type"] = "regular"
        for k in ("rush_att", "receptions", "pass_att", "inside10", "rush_td", "rec_td"):
            d[k] = int(d[k])
        out.append(d)
    return out


def load_games(db: DatabaseManager) -> list[dict]:
    """Every game with play-by-play: teams, closing implied totals and its first touchdown."""
    rows = db.execute("""
        WITH g AS (
            SELECT game_id, min(season) AS season, min(week) AS week, min(home_team) AS home,
                   min(away_team) AS away, max(spread_line) AS spread, max(total_line) AS total
            FROM nfl_pbp_archetypes GROUP BY game_id),
        td AS (
            SELECT DISTINCT ON (a.game_id) a.game_id, a.posteam, t.player_id, t.team AS scorer_team,
                   EXISTS (SELECT 1 FROM nfl_pbp_play_participants x WHERE x.game_id = a.game_id
                           AND x.play_id = a.play_id AND x.player_id = t.player_id
                           AND x.role IN ('rusher', 'receiver')) AS offensive
            FROM nfl_pbp_archetypes a
            JOIN nfl_pbp_play_participants t ON t.game_id = a.game_id AND t.play_id = a.play_id
                 AND t.role = 'touchdown'
            WHERE a.two_point_result IS NULL
            ORDER BY a.game_id, a.quarter, a.game_seconds_remaining DESC, a.play_id)
        SELECT g.*, td.posteam, td.player_id AS scorer, td.scorer_team, td.offensive
        FROM g LEFT JOIN td ON td.game_id = g.game_id""")
    games = []
    for r in rows:
        g = dict(r)
        g["cfbd_game_id"] = g["game_id"]
        g["season_type"] = "regular"
        if g["spread"] is not None and g["total"] is not None:
            g["home_implied"] = (float(g["total"]) + float(g["spread"])) / 2
            g["away_implied"] = (float(g["total"]) - float(g["spread"])) / 2
        else:
            g["home_implied"] = g["away_implied"] = None
        games.append(g)
    return games


def label(g: dict) -> dict:
    if not g.get("scorer_team"):
        return {"outcome": "no_td"}
    side = "home" if g["scorer_team"] == g["home"] else "away" if g["scorer_team"] == g["away"] else None
    if g["offensive"] and g["scorer_team"] == g["posteam"]:
        return {"outcome": "offense", "side": side, "pid": g["scorer"], "method": "pbp"}
    return {"outcome": "non_offense", "side": side}


# ── backtest ─────────────────────────────────────────────────────────────────
def scored_games(games: list[dict], history: cf.History, seasons) -> list[dict]:
    out = []
    for g in games:
        if g["season"] not in seasons or not (g["home_implied"] and g["away_implied"]):
            continue
        lab = g["_label"]
        if lab["outcome"] == "no_td" or lab.get("side") is None:
            continue
        before = (g["season"], g["week"])
        home, away = history.candidates(g["home"], before), history.candidates(g["away"], before)
        if not home or not away:
            continue
        out.append({**g, "_home": home, "_away": away, "_before": before})
    return out


def backtest(db: DatabaseManager) -> dict:
    history = cf.History(load_box(db))
    games = load_games(db)
    for g in games:
        g["_label"] = label(g)
    phi_fit = [g for g in games if g["season"] in FIT_SEASONS_TEAM and g["_label"]["outcome"] != "no_td"]
    phi = sum(g["_label"]["outcome"] == "non_offense" for g in phi_fit) / len(phi_fit)
    k, _ = cf.fit_team_power(games, FIT_SEASONS_TEAM)
    seasons = sorted({g["season"] for g in games})
    rho_by_season = {s: history.rho((s, 0)) for s in seasons}
    base = Params(k=k, phi=phi)
    fit_rows = scored_games(games, history, FIT_SEASONS_PLAYER)
    best = None
    for theta in (0.01, 0.02, 0.03, 0.05, 0.08, 0.12):
        for kappa in (0.0, 0.5, 1.0, 2.0, 3.0, 4.0, 6.0, 8.0, 16.0):
            for w in (0.0, 0.05, 0.1, 0.2, 0.5):
                for tau in (0.5, 0.6, 0.7, 0.8, 0.9, 1.0):
                    p = replace(base, theta=theta, kappa=kappa, w_prior=w, tau=tau)
                    ll, _ = cf.log_loss(fit_rows, p, rho_by_season)
                    if best is None or ll < best[0]:
                        best = (ll, p)
    fitted = best[1]
    report = {"version": VERSION, "phi": phi,
              "team": {"k": k, "fit": cf.team_report(games, FIT_SEASONS_TEAM, k),
                       "test": cf.team_report(games, TEST_SEASONS, k)},
              "params": fitted.__dict__ | {"rho": rho_by_season}}
    for name, seas in (("fit", FIT_SEASONS_PLAYER), ("test_2025", (2025,)), ("test_2026", (2026,))):
        rows = scored_games(games, history, seas)
        if not rows:
            continue
        ll, n = cf.log_loss(rows, fitted, rho_by_season)
        vol, _ = cf.log_loss(rows, replace(fitted, kappa=1000.0), rho_by_season)
        flat, _ = cf.log_loss(rows, replace(fitted, tau=1.0), rho_by_season)
        report[name] = {"games": n, "log_loss": ll, "volume_only_shares": vol, "tau_1": flat,
                        "model_minus_tau_1": cf.paired_week_bootstrap(
                            rows, cf.game_losses(rows, fitted, rho_by_season),
                            cf.game_losses(rows, replace(fitted, tau=1.0), rho_by_season)),
                        "model_minus_volume_only": cf.paired_week_bootstrap(
                            rows, cf.game_losses(rows, fitted, rho_by_season),
                            cf.game_losses(rows, replace(fitted, kappa=1000.0), rho_by_season)),
                        "coin_and_uniform": cf.uniform_log_loss(rows, fitted),
                        **cf.top_pick_rate(rows, fitted, rho_by_season)}
    return report


# ── chips ────────────────────────────────────────────────────────────────────
def player_chips(history: cf.History, pid: str, team: str, before: tuple[int, int]) -> list[dict]:
    season = before[0]
    team_games = [gid for order, gid in history.games_by_team.get(team, []) if order < before and order[0] == season]
    last2 = team_games[-2:]
    mine = [r for r in history.rows_by_player.get(pid, []) if r["order"] < before and r["season"] == season
            and r["team"] == team]
    by_game = {r["cfbd_game_id"]: r for r in mine}
    team_rows = [r for gid in team_games for r in history.rows_by_game_team[(gid, team)]]
    chips = []
    team_rush_td, my_rush_td = sum(r["rush_td"] for r in team_rows), sum(r["rush_td"] for r in mine)
    if team_rush_td >= CHIP_GOAL_LINE_MIN_TEAM and my_rush_td / team_rush_td >= CHIP_GOAL_LINE_SHARE:
        chips.append({"code": "GOAL_LINE", "label": f"{my_rush_td} of {team_rush_td} team rush TDs"})
    team_in10, my_in10 = sum(r["inside10"] for r in team_rows), sum(r["inside10"] for r in mine)
    if team_in10 >= CHIP_INSIDE_TEN_MIN_TEAM and my_in10 / team_in10 >= CHIP_INSIDE_TEN_SHARE:
        chips.append({"code": "INSIDE_TEN", "label": f"{my_in10 / team_in10:.0%} of team touches inside the 10"})
    if len(last2) == 2 and all(gid in by_game and by_game[gid]["rush_td"] + by_game[gid]["rec_td"] > 0
                               for gid in last2):
        chips.append({"code": "HOT", "label": "TD in each of last 2"})
    earlier = [by_game[gid] for gid in team_games[:-2] if gid in by_game]
    if len(last2) == 2 and len(earlier) >= 2:
        recent = sum(by_game[g]["rush_att"] + by_game[g]["receptions"] if g in by_game else 0 for g in last2) / 2
        before_avg = sum(r["rush_att"] + r["receptions"] for r in earlier) / len(earlier)
        if recent >= CHIP_NEW_ROLE_MIN and recent >= CHIP_NEW_ROLE_RATIO * max(before_avg, 1.0):
            chips.append({"code": "NEW_ROLE",
                          "label": f"{recent:.0f} carries + targets a game, last 2 (was {before_avg:.0f})"})
    if sum(r["pass_att"] for r in mine) >= CHIP_QB_PASS_ATT and my_rush_td >= CHIP_QB_RUSH_TD:
        chips.append({"code": "RUNNING_QB", "label": f"QB, {my_rush_td} rush TDs"})
    return chips


def team_chips(g: dict, firsts: dict[str, list], before: tuple[int, int]) -> dict[str, list[dict]]:
    out = {}
    gap = float(g["home_implied"]) - float(g["away_implied"])
    for side, sign in (("home", 1), ("away", -1)):
        chips = []
        if sign * gap >= CHIP_BIG_FAVOURITE:
            chips.append({"code": "BIG_FAVOURITE", "label": f"Favoured by {sign * gap:.1f}".replace(".0", "")})
        past = [hit for order, hit in firsts.get(g[side], []) if order < before and order[0] == before[0]]
        if past:
            chips.append({"code": "SCORED_FIRST", "label": f"Scored first TD in {sum(past)} of {len(past)}"})
        out[side] = chips
    return out


def first_scorers(games: list[dict]) -> dict[str, list]:
    out: dict[str, list] = defaultdict(list)
    for g in games:
        lab = label(g)
        if lab["outcome"] == "no_td" or lab.get("side") is None:
            continue
        for side in ("home", "away"):
            out[g[side]].append(((g["season"], g["week"]), lab["side"] == side))
    return out


# ── slate ────────────────────────────────────────────────────────────────────
_UPCOMING = """
    SELECT g.nflverse_game_id AS game_id, g.season, g.week, g.kickoff AS commence_time,
           COALESCE(g.market_spread_line, g.quoted_spread_line) AS spread,
           COALESCE(g.market_total_line, g.quoted_total_line) AS total,
           ht.odds_api_name AS home_odds_name, at.odds_api_name AS away_odds_name
    FROM nfl_season_games g
    JOIN nfl_teams ht ON ht.team_id = g.home_team_id JOIN nfl_teams at ON at.team_id = g.away_team_id
    WHERE g.nflverse_game_id IS NOT NULL AND {where}
    ORDER BY g.kickoff, g.nflverse_game_id"""


def upcoming(db: DatabaseManager, where: str, args: tuple) -> list[dict]:
    games = []
    for r in db.execute(_UPCOMING.format(where=where), args):
        g = dict(r)
        if g["spread"] is None or g["total"] is None:
            continue
        _, _, g["away"], g["home"] = g["game_id"].split("_")  # play-by-play team codes (LA, WAS)
        g["home_implied"] = (float(g["total"]) + float(g["spread"])) / 2
        g["away_implied"] = (float(g["total"]) - float(g["spread"])) / 2
        games.append(g)
    return games


def load_lines(db: DatabaseManager, games: list[dict]) -> dict[str, dict]:
    """The latest DraftKings anytime-TD list per game (matched on team names and kickoff).

    Only captures in the 30 hours before kickoff count: the midweek main-line reads are too early
    to say who is playing."""
    out: dict[str, dict] = {}
    for g in games:
        rows = db.execute("""
            WITH ev AS (
                SELECT event_id, max(capture_key) AS capture_key FROM prop_odds_history
                WHERE sport = 'nfl' AND market = 'player_anytime_td' AND home_team_name = %s
                  AND away_team_name = %s AND commence_time BETWEEN %s - INTERVAL '6 hours' AND %s + INTERVAL '6 hours'
                  AND captured_at < NOW() AND captured_at >= %s - INTERVAL '30 hours'
                GROUP BY event_id ORDER BY max(capture_key) DESC LIMIT 1)
            SELECT p.player, p.captured_at FROM prop_odds_history p
            JOIN ev ON ev.event_id = p.event_id AND ev.capture_key = p.capture_key
            WHERE p.sport = 'nfl' AND p.market = 'player_anytime_td' AND p.books ? 'draftkings'""",
            (g["home_odds_name"], g["away_odds_name"], g["commence_time"], g["commence_time"], g["commence_time"]))
        if rows:
            out[g["game_id"]] = {"names": {k for k in (cf.name_key(r["player"]) for r in rows) if k},
                                 "captured_at": rows[0]["captured_at"].isoformat()}
    return out


def display_names(db: DatabaseManager) -> dict[str, str]:
    """Full names by GSIS id from the fantasy roster (play-by-play has 'J.Allen')."""
    rows = db.execute("""SELECT DISTINCT ON (gsis_id) gsis_id, canonical_name FROM ff_players
                         WHERE gsis_id IS NOT NULL ORDER BY gsis_id, season DESC""")
    return {r["gsis_id"]: r["canonical_name"] for r in rows}


def predict(games: list[dict], params: Params, history: cf.History, firsts: dict, names: dict[str, str],
            lines: dict[str, dict] | None = None, filter_lines: bool = True, top: int = PUBLISH_TOP) -> list[dict]:
    out = []
    for g in games:
        before = (g["season"], g["week"])
        p = replace(params, rho=history.rho((g["season"], 0)))
        home, away = history.candidates(g["home"], before), history.candidates(g["away"], before)
        for cands in (home, away):
            for pid, c in cands.items():
                c["name"] = names.get(pid, c["name"])
        listed = lines.get(g["game_id"]) if lines is not None else None
        availability = None
        if lines is not None and filter_lines:
            home, away, availability = cf.apply_lines(home, away, listed["names"] if listed else None)
            if listed:
                availability["captured_at"] = listed["captured_at"]
        p_home = cf.team_probability(g["home_implied"], g["away_implied"], p.k)
        dist = cf.game_distribution(p_home, home, away, p)
        cands = {**home, **away}
        players = sorted(((pid, pr) for pid, pr in dist.items() if pid in cands), key=lambda t: -t[1])
        out.append({
            "game_id": g["game_id"], "season": g["season"], "week": g["week"], "away": g["away"],
            "home": g["home"], "commence_time": g["commence_time"], "game": f"{g['away']} @ {g['home']}",
            "implied": [g["away_implied"], g["home_implied"]], "p_home_first": p_home,
            "players": [{"name": cands[pid]["name"], "team": cands[pid]["team"], "prob": pr,
                         "fair": cf.fair_american(pr), "td": cands[pid]["td"], "touches": cands[pid]["vol"],
                         "chips": player_chips(history, pid, cands[pid]["team"], before),
                         "listed": None if not listed else cf.name_key(cands[pid]["name"]) in listed["names"]}
                        for pid, pr in players[:top]],
            "other_offense": dist["other:home"] + dist["other:away"], "non_offense": dist["non_offense"],
            "dist": dist, "availability": availability,
            "context": {"team_chips": team_chips(g, firsts, before)},
        })
    return out


def _save_run(cur, version: str, params: Params, rows: list[dict]) -> int:
    cur.execute("INSERT INTO nfl_first_td_runs (version, params, games) VALUES (%s, %s, %s) RETURNING id",
                (version, json.dumps(params.__dict__), len(rows)))
    run_id = cur.fetchone()["id"]
    for r in rows:
        cur.execute("""
            INSERT INTO nfl_first_td_predictions (run_id, game_id, season, week, kickoff, away, home,
                away_implied, home_implied, p_home_first, other_offense, non_offense, players, dist,
                availability, context)
            SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s WHERE %s > NOW()""",
            (run_id, r["game_id"], r["season"], r["week"], r["commence_time"], r["away"], r["home"],
             r["implied"][0], r["implied"][1], r["p_home_first"], r["other_offense"], r["non_offense"],
             json.dumps(r["players"]), json.dumps(r["dist"]),
             json.dumps(r["availability"]) if r["availability"] is not None else None,
             json.dumps(r["context"]), r["commence_time"]))
    return run_id


def _inputs(db: DatabaseManager):
    games = load_games(db)
    return cf.History(load_box(db)), first_scorers(games), display_names(db)


def publish(db: DatabaseManager, params: Params = FITTED, hours: int = PUBLISH_HOURS) -> list[tuple[str, int, int]]:
    """Save both versions for games kicking off in the next ``hours``; started games are never written."""
    games = upcoming(db, "g.kickoff > NOW() AND g.kickoff <= NOW() + %s * INTERVAL '1 hour'", (hours,))
    rows_base, rows_lines = [], []
    if games:
        history, firsts, names = _inputs(db)
        lines = load_lines(db, games)
        rows_base = predict(games, params, history, firsts, names, lines, filter_lines=False)
        rows_lines = predict(games, params, history, firsts, names, lines)
    with db.connect() as conn:
        cur = conn.cursor()
        for statement in DDL:
            cur.execute(statement)
        return [(VERSION, _save_run(cur, VERSION, params, rows_base), len(rows_base)),
                (VERSION_LINES, _save_run(cur, VERSION_LINES, params, rows_lines), len(rows_lines))]


def grade(db: DatabaseManager) -> int:
    with db.connect() as conn:
        cur = conn.cursor()
        for statement in DDL:
            cur.execute(statement)
    pending = db.execute("""
        SELECT DISTINCT ON (r.version, p.game_id) p.run_id, p.game_id, r.version, p.season, p.week,
               p.kickoff, p.dist
        FROM nfl_first_td_predictions p JOIN nfl_first_td_runs r ON r.id = p.run_id
        WHERE r.created_at < p.kickoff AND p.kickoff < NOW() - INTERVAL '5 hours'
          AND NOT EXISTS (SELECT 1 FROM nfl_first_td_grades x WHERE x.game_id = p.game_id AND x.version = r.version)
        ORDER BY r.version, p.game_id, r.created_at DESC""")
    if not pending:
        return 0
    games = {g["game_id"]: g for g in load_games(db)}
    written = 0
    with db.connect() as conn:
        cur = conn.cursor()
        for r in pending:
            g = games.get(r["game_id"])
            if not g:
                continue  # play-by-play not stored yet
            lab = label(g)
            prob = cf.dist_probability(r["dist"], lab)
            if prob is None:
                continue
            players = {k: v for k, v in r["dist"].items() if not k.startswith("other") and k != "non_offense"}
            best = max(players, key=players.get) if players else None
            cur.execute("""
                INSERT INTO nfl_first_td_grades (run_id, game_id, version, season, week, kickoff, outcome,
                    scorer, prob, top_pick_hit) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING""",
                (r["run_id"], r["game_id"], r["version"], r["season"], r["week"], r["kickoff"],
                 lab["outcome"], lab.get("pid"), prob, best is not None and best == lab.get("pid")))
            written += 1
    return written


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("backtest")
    b.add_argument("--json", type=Path)
    s = sub.add_parser("slate")
    s.add_argument("--date", required=True)
    s.add_argument("--lines", action="store_true")
    s.add_argument("--top", type=int, default=6)
    pub = sub.add_parser("publish")
    pub.add_argument("--hours", type=int, default=PUBLISH_HOURS)
    sub.add_parser("grade")
    args = ap.parse_args(argv)
    db = DatabaseManager(load_config().database_url, initialize_schema=False)
    if args.cmd == "backtest":
        report = backtest(db)
        text = json.dumps(report, indent=2, default=str)
        if args.json:
            args.json.write_text(text, encoding="utf-8")
        print(text)
        return 0
    if args.cmd == "publish":
        for version, run_id, n in publish(db, FITTED, args.hours):
            print(f"run {run_id}: {n} games saved ({version})")
        return 0
    if args.cmd == "grade":
        print(f"{grade(db)} forecasts graded")
        return 0
    games = upcoming(db, "(g.kickoff AT TIME ZONE 'America/New_York')::date = %s", (args.date,))
    history, firsts, names = _inputs(db)
    lines = load_lines(db, games) if args.lines else None
    for g in predict(games, FITTED, history, firsts, names, lines, top=args.top):
        print(f"\n{g['game']}  implied {g['implied'][0]:.1f}-{g['implied'][1]:.1f}  P(home first) {g['p_home_first']:.0%}"
              f"  {g['context']['team_chips']}")
        for p in g["players"]:
            print(f"  {p['prob']:5.1%} {p['fair']:>+6d} {p['name']} ({p['team']}) {p['td']:.0f} TD / "
                  f"{p['touches']:.0f} opp  {[c['label'] for c in p['chips']]}")
        if g["availability"]:
            print(f"  lines: {g['availability']['status']}, dropped "
                  f"{', '.join(d['name'] for d in g['availability']['dropped']) or 'nobody'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
