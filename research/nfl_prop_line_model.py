"""Reverse-engineering the books' NFL lines, step 2: model the line, not the result.

For each market, the book's implied mean (closing line read by
`model/nfl_prop_implied.py`) is regressed on what was public before the game:

    this season's per-game average (and how many games it rests on)
    last season's per-game average
    the last game, and the last three
    the team's implied points and the spread (from the game's market line)
    what the opponent allowed to the position this season
    home or away

Fitted weights describe how the market behaves; each week is predicted from a
fit on the other weeks, so R^2 is out of sample. The same table is fitted for
our projection's stat means, so the two weightings can be set side by side,
and a last section regresses (book - ours) on the same features to say which
inputs the gap follows. Exploratory; not a pre-registered test.

    python -m research.nfl_prop_line_model --season 2026 --weeks 1-4
"""

from __future__ import annotations

import argparse
import collections
import json
import statistics as st

import numpy as np

from config import load_config
from db.database import DatabaseManager
from model import nfl_prop_implied as pim
from model.nfl_dfs_dk_pool_match import normalize_name
from research.nfl_prop_implied import load, our_stat_means, parse_weeks

TARGETS = {
    "player_pass_yds": ("passing_yards", ("QB",)),
    "player_pass_tds": ("passing_tds", ("QB",)),
    "player_rush_yds": ("rushing_yards", ("RB", "QB")),
    "player_receptions": ("receptions", ("WR", "TE", "RB")),
    "player_reception_yds": ("receiving_yards", ("WR", "TE", "RB")),
}
FEATURES = ("season_avg", "season_games", "last_season_avg", "last_game", "last3_avg",
            "team_implied", "team_spread", "opp_allowed", "home")
# nflverse codes vs the schedule table's codes.
ALIAS = {"WAS": "WSH", "LA": "LAR", "JAC": "JAX", "LV": "LV", "OAK": "LV", "SD": "LAC", "STL": "LAR"}


def team_code(t: str | None) -> str | None:
    return ALIAS.get(t, t) if t else t


def history(db, season: int) -> dict:
    """player key -> list of (season, week, team, opponent, position, stats) for regular-season games, oldest first."""
    out = collections.defaultdict(list)
    for r in db.execute(
            """SELECT f.canonical_name, s.season, s.week, s.team, s.opponent, s.source_row FROM ff_player_week_stats s
               JOIN ff_players f ON f.id = s.player_id
               WHERE s.season IN (%s, %s) AND COALESCE(s.season_type, 'REG') = 'REG'
               ORDER BY s.season, s.week""", (season - 1, season)):
        row = r["source_row"] if isinstance(r["source_row"], dict) else json.loads(r["source_row"] or "{}")
        key = normalize_name(r["canonical_name"])
        if out[key] and out[key][-1][:2] == (r["season"], r["week"]):
            continue
        out[key].append((r["season"], r["week"], team_code(r["team"] or row.get("team")),
                         team_code(r["opponent"] or row.get("opponent_team")), row.get("position"), row))
    return out


def game_env(db, season: int) -> dict:
    """(week, team) -> (implied points, spread from the team's side, home flag, opponent)."""
    out = {}
    for r in db.execute(
            """SELECT g.week, h.abbreviation AS home, a.abbreviation AS away,
                      COALESCE(g.market_total_line, g.quoted_total_line) AS total,
                      COALESCE(g.market_spread_line, g.quoted_spread_line) AS spread
               FROM nfl_season_games g JOIN nfl_teams h ON h.team_id = g.home_team_id JOIN nfl_teams a ON a.team_id = g.away_team_id
               WHERE g.season = %s""", (season,)):
        if r["total"] is None or r["spread"] is None:
            continue
        # spread_line is the home margin the market expects (positive = home favored).
        home_pts = (r["total"] + r["spread"]) / 2
        out[(r["week"], r["home"])] = (home_pts, r["spread"], 1, r["away"])
        out[(r["week"], r["away"])] = (r["total"] - home_pts, -r["spread"], 0, r["home"])
    return out


def opp_allowed(hist: dict, season: int) -> dict:
    """(opponent, position, stat) -> list of (week, per-game total allowed), this season."""
    per_game = collections.defaultdict(float)
    for games in hist.values():
        for s, w, team, opp, pos, row in games:
            if s != season or not opp or not pos:
                continue
            for stat in ("passing_yards", "passing_tds", "rushing_yards", "receptions", "receiving_yards"):
                per_game[(opp, pos, stat, w)] += row.get(stat) or 0
    out = collections.defaultdict(list)
    for (opp, pos, stat, w), v in per_game.items():
        out[(opp, pos, stat)].append((w, v))
    return out


def features(key: str, week: int, season: int, stat: str, pos: str, hist: dict, env: dict, allowed: dict,
             league: dict, team_hint: str | None) -> dict | None:
    games = hist.get(key, [])
    before = [g for g in games if (g[0], g[1]) < (season, week)]
    this = [g[5].get(stat) or 0 for g in before if g[0] == season]
    last = [g[5].get(stat) or 0 for g in before if g[0] == season - 1]
    if not before:
        return None
    team = team_code(team_hint) or before[-1][2]
    e = env.get((week, team))
    if e is None:
        return None
    opp_team = e[3]
    allowed_rows = [v for w, v in allowed.get((opp_team, pos, stat), []) if w < week]
    recent = [g[5].get(stat) or 0 for g in before[-3:]]
    last_season = st.mean(last) if last else None
    season_avg = st.mean(this) if this else (last_season if last_season is not None else st.mean(recent))
    return {"season_avg": season_avg, "season_games": len(this),
            "last_season_avg": last_season if last_season is not None else season_avg,
            "last_game": before[-1][5].get(stat) or 0, "last3_avg": st.mean(recent),
            "team_implied": e[0], "team_spread": e[1],
            "opp_allowed": (st.mean(allowed_rows) - league.get((pos, stat), 0)) if allowed_rows else 0.0,
            "home": e[2]}


def fit(X: np.ndarray, y: np.ndarray, ridge: float = 1e-3):
    Xc = np.column_stack([np.ones(len(X)), X])
    reg = ridge * np.eye(Xc.shape[1]); reg[0, 0] = 0
    return np.linalg.solve(Xc.T @ Xc + reg, Xc.T @ y)


def predict(beta, X):
    return np.column_stack([np.ones(len(X)), X]) @ beta


def lowo_r2(rows: list[dict], target: str, weeks: list[int]) -> float:
    preds, actual = [], []
    for w in weeks:
        train = [r for r in rows if r["w"] != w]; test = [r for r in rows if r["w"] == w]
        if len(train) < 20 or not test:
            continue
        beta = fit(np.array([[r[f] for f in FEATURES] for r in train]), np.array([r[target] for r in train]))
        preds += list(predict(beta, np.array([[r[f] for f in FEATURES] for r in test]))); actual += [r[target] for r in test]
    if not actual:
        return float("nan")
    a, p = np.array(actual), np.array(preds)
    return 1 - ((a - p) ** 2).sum() / ((a - a.mean()) ** 2).sum()


def standardized(rows, target):
    X = np.array([[r[f] for f in FEATURES] for r in rows]); y = np.array([r[target] for r in rows])
    sd = X.std(axis=0); sd[sd == 0] = 1
    beta = fit((X - X.mean(axis=0)) / sd, y)
    return dict(zip(FEATURES, beta[1:])), beta


def study(db, season: int, weeks: list[int]) -> None:
    lines = load(db, season, weeks)
    ours = our_stat_means(db, season, weeks)
    hist = history(db, season)
    env = game_env(db, season)
    allowed = opp_allowed(hist, season)
    league = {}
    for (opp, pos, stat), vals in allowed.items():
        league.setdefault((pos, stat), []).extend(v for _, v in vals)
    league = {k: st.mean(v) for k, v in league.items()}

    for market, (stat, positions) in TARGETS.items():
        rows = []
        for (w, key), d in lines.items():
            if d["position"] not in positions or market not in d["lines"]:
                continue
            q = d["lines"][market]
            book = pim.market_mean(market, q["line"], q["over"], q["under"])
            mine = (ours.get((w, key)) or {}).get(stat)
            f = features(key, w, season, stat, d["position"], hist, env, allowed, league, d["team"])
            if book is None or f is None:
                continue
            rows.append({"w": w, "key": key, "book": book, "ours": mine, **f})
        both = [r for r in rows if r["ours"] is not None]
        if len(both) < 30:
            print(f"\n{market}: only {len(both)} rows, skipped")
            continue
        for r in both:
            r["gap"] = r["book"] - r["ours"]
        print(f"\n== {market} ({stat}; {', '.join(positions)}): {len(both)} player-weeks")
        print(f"   averages: book {st.mean(r['book'] for r in both):.2f}, ours {st.mean(r['ours'] for r in both):.2f}, "
              f"this-season avg {st.mean(r['season_avg'] for r in both):.2f}, last-season avg {st.mean(r['last_season_avg'] for r in both):.2f}")
        print(f"   out-of-sample R^2 from public inputs: book line {lowo_r2(both, 'book', weeks):.2f}, our projection {lowo_r2(both, 'ours', weeks):.2f}")
        cb, _ = standardized(both, "book"); co, _ = standardized(both, "ours"); cg, _ = standardized(both, "gap")
        print(f"   {'input (+1 SD)':18}{'book':>9}{'ours':>9}{'book-ours':>11}")
        for f in FEATURES:
            print(f"   {f:18}{cb[f]:>+9.2f}{co[f]:>+9.2f}{cg[f]:>+11.2f}")


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--weeks", default="1-4")
    args = parser.parse_args(argv)
    study(DatabaseManager(load_config().database_url, initialize_schema=False), args.season, parse_weeks(args.weeks))


if __name__ == "__main__":
    main()
