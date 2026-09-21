"""Pre-registered study: does a player's RED-ZONE TOUCH SHARE forecast his
touchdowns better than his own touchdown history?

Registered 2026-09-21, BEFORE any variant was scored. Nothing below was tuned.
Follows `model/nfl_dfs_redzone_trips.py`, where TEAM red-zone trips failed to
beat team touchdown history. This is a different claim: allocation WITHIN a
team. A back with 60% of the goal-line carries who has not scored in a month
is the case a touchdown-rate history handles worst.

Identity: `nfl_pbp_play_participants.player_id` is the GSIS id, the same key
`ff_players.gsis_id` and the weekly stats use. No crosswalk.

Target
------
A player's rushing + receiving touchdowns in a game, from the nflverse weekly
stats the production projection already scores against. RB / WR / TE only.

Baseline (production-style)
---------------------------
    td_hat = shrunk EWMA of the player's own TD per game
             (half-life 6, 17-game window, 4 prior games toward the
              position's league mean TD per game)

One variant, fixed in advance -- the test family is exactly 1
-------------------------------------------------------------
V1 `team_budget_x_rz_share`:
    team_td   = shrunk EWMA of the team's own rush+rec TDs per game
                (all positions, the same budget the trips study called baseline)
    rz_share  = shrunk EWMA of (player red-zone touches / team red-zone touches)
                over games where the team had >= 1 red-zone touch, shrunk with
                4 prior games toward the position's league-mean share
    v1        = team_td * rz_share
A red-zone touch is a rusher or receiver credit on a scrimmage play snapped at
or inside the opponent's 20 (`yardline_100 <= 20`).

Population
----------
Player-games from 2024 week 1 onward, RB/WR/TE, regular season, where the
player has >= 2 prior games of red-zone share evidence and the team has prior
touchdown history. Strictly earlier weeks only. Features from 2022 onward.

Primary metric and kill rule
----------------------------
Paired MAE difference vs the baseline on the same player-games, bootstrap CI
resampling (season, week). Survives only if the 95% CI lies entirely below
zero. Includes zero: dead. No re-slicing by position, no share prior re-fit.

Interpretation guard
--------------------
A survivor is a measurement on player TD per game. It is not a projection
change. `--tonight` writes nothing.
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict

import numpy as np

from config import load_config
from ingest.nfl_dfs_weekly import PipelineDatabase
from model.nfl_dfs_workload import CONFIG, weighted_mean

VERSION = "nfl-dfs-redzone-share-study-v1"
POSITIONS = ("RB", "WR", "TE")
FIRST_SEASON = 2022
START = (2024, 1)
MIN_SHARE_GAMES = 2
ALIASES = {"LA": "LAR", "WAS": "WSH", "AZ": "ARI", "JAC": "JAX"}
SCRIMMAGE = "a.play_archetype NOT IN ('SPECIAL_TEAMS','NON_PLAY','KNEEL','SPIKE','TWO_POINT') AND a.down IS NOT NULL"


def load(db):
    touches = db.execute(f"""
        SELECT a.season, a.week, a.posteam team, p.player_id, count(*) n
        FROM nfl_pbp_archetypes a
        JOIN nfl_pbp_play_participants p ON p.game_id=a.game_id AND p.play_id=a.play_id
          AND p.role IN ('rusher','receiver') AND p.side='offense'
        WHERE a.season_type='REG' AND a.season >= {FIRST_SEASON} AND {SCRIMMAGE} AND a.yardline_100 <= 20
        GROUP BY 1,2,3,4""")
    rz = {}  # (season, week, team) -> {player: touches}
    for r in touches:
        rz.setdefault((r["season"], r["week"], ALIASES.get(r["team"], r["team"])), {})[r["player_id"]] = float(r["n"])
    stats = db.execute(f"""
        SELECT p.gsis_id, p.position, w.season, w.week, w.team,
               COALESCE((w.source_row->>'rushing_tds')::float,0) + COALESCE((w.source_row->>'receiving_tds')::float,0) td
        FROM ff_player_week_stats w JOIN ff_players p ON p.id=w.player_id
        WHERE w.season_type='REG' AND w.source='nflverse' AND w.season >= {FIRST_SEASON}
          AND p.position IN ('QB','RB','WR','TE') AND p.gsis_id IS NOT NULL""")
    players = [{"id": r["gsis_id"], "pos": r["position"], "season": r["season"], "week": r["week"],
                "team": ALIASES.get(r["team"], r["team"]), "td": float(r["td"])} for r in stats]
    return rz, players


def shrunk(values, prior_mean, config=CONFIG):
    values = values[-config["max_games"]:]
    if not values:
        return None, 0
    w = len(values) / (len(values) + config["prior_games"])
    return w * weighted_mean(values, config["half_life_games"], config["max_games"]) + (1 - w) * prior_mean, len(values)


class Prior:
    def __init__(self, rz, players, cutoff):
        rows = sorted((r for r in players if (r["season"], r["week"]) < cutoff), key=lambda r: (r["season"], r["week"]))
        self.by_player, team_td, self.pos_td = defaultdict(list), defaultdict(lambda: defaultdict(float)), defaultdict(list)
        for r in rows:
            self.by_player[r["id"]].append(r)
            team_td[r["team"]][(r["season"], r["week"])] += r["td"]
            self.pos_td[r["pos"]].append(r["td"])
        self.team_td = {t: [v for _, v in sorted(g.items())] for t, g in team_td.items()}
        self.league_team_td = float(np.mean([v for g in self.team_td.values() for v in g]))
        # Per-game shares, only where the team had a red-zone touch that game.
        self.shares, pos_shares = defaultdict(list), defaultdict(list)
        pos_of = {r["id"]: r["pos"] for r in rows}
        for key in sorted(k for k in rz if (k[0], k[1]) < cutoff):
            team_total = sum(rz[key].values())
            if team_total <= 0:
                continue
            for pid, n in rz[key].items():
                self.shares[pid].append(n / team_total)
                if pid in pos_of:
                    pos_shares[pos_of[pid]].append(n / team_total)
        self.pos_mean_share = {p: float(np.mean(v)) for p, v in pos_shares.items()}
        self.pos_mean_td = {p: float(np.mean(v)) for p, v in self.pos_td.items()}

    def forecast(self, pid, pos, team):
        own = self.by_player.get(pid, [])
        base, n_own = shrunk([r["td"] for r in own], self.pos_mean_td.get(pos, 0.0))
        team_hist = self.team_td.get(team, [])
        team_td, n_team = shrunk(team_hist, self.league_team_td)
        share, n_share = shrunk(self.shares.get(pid, []), self.pos_mean_share.get(pos, 0.0))
        if base is None or team_td is None or share is None or n_share < MIN_SHARE_GAMES:
            return None
        return {"baseline": base, "v1": team_td * share, "own_games": n_own, "share": share,
                "share_games": n_share, "team_td": team_td, "team_games": n_team}


def backtest(rz, players):
    out, cache = [], {}
    for t in players:
        cutoff = (t["season"], t["week"])
        if cutoff < START or t["pos"] not in POSITIONS:
            continue
        if cutoff not in cache:
            cache[cutoff] = Prior(rz, players, cutoff)
        f = cache[cutoff].forecast(t["id"], t["pos"], t["team"])
        if not f:
            continue
        out.append({"season": t["season"], "week": t["week"], "pos": t["pos"], "id": t["id"], "actual": t["td"],
                    "baseline": f["baseline"], "v1": f["v1"]})
    return out


def paired_ci(rows, draws=4000, seed=20260921):
    weeks = defaultdict(list)
    for r in rows:
        weeks[(r["season"], r["week"])].append(abs(r["v1"] - r["actual"]) - abs(r["baseline"] - r["actual"]))
    keys = list(weeks)
    rng = np.random.default_rng(seed)
    means = [np.concatenate([weeks[keys[i]] for i in rng.choice(len(keys), size=len(keys))]).mean() for _ in range(draws)]
    lo, hi = np.quantile(means, [0.025, 0.975])
    return {"point": float(np.mean(np.concatenate([weeks[k] for k in keys]))), "ci95": [float(lo), float(hi)],
            "weeks": len(keys), "verdict": "SURVIVES" if hi < 0 else "DEAD"}


def metrics(rows):
    m = {"n": len(rows),
         "baseline_mae": float(np.mean([abs(r["baseline"] - r["actual"]) for r in rows])),
         "v1_mae": float(np.mean([abs(r["v1"] - r["actual"]) for r in rows])),
         "baseline_bias": float(np.mean([r["baseline"] - r["actual"] for r in rows])),
         "v1_bias": float(np.mean([r["v1"] - r["actual"] for r in rows])),
         "v1_delta": paired_ci(rows)}
    # Descriptive only, not gating: the pre-registered verdict is the pooled one.
    m["by_position_descriptive"] = {p: {"n": len(s), "baseline_mae": float(np.mean([abs(r["baseline"] - r["actual"]) for r in s])),
                                        "v1_mae": float(np.mean([abs(r["v1"] - r["actual"]) for r in s]))}
                                    for p in POSITIONS if (s := [r for r in rows if r["pos"] == p])}
    return m


def tonight(db, rz, players, season, week, teams):
    prior = Prior(rz, players, (season, week))
    roster = db.execute("""SELECT gsis_id, canonical_name, position, team_abbrev FROM ff_players
        WHERE season=%s AND active AND team_abbrev=ANY(%s) AND position=ANY(%s) AND gsis_id IS NOT NULL""", (season, teams, list(POSITIONS)))
    out = []
    for r in roster:
        f = prior.forecast(r["gsis_id"], r["position"], r["team_abbrev"])
        if f:
            out.append({"team": r["team_abbrev"], "name": r["canonical_name"], "pos": r["position"],
                        **{k: round(v, 3) if isinstance(v, float) else v for k, v in f.items()}})
    return sorted(out, key=lambda x: (x["team"], -x["v1"]))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--tonight", default="", help="comma list of teams; research only, writes nothing")
    p.add_argument("--season", type=int, default=2026)
    p.add_argument("--week", type=int, default=2)
    a = p.parse_args()
    db = PipelineDatabase(load_config().database_url)
    rz, players = load(db)
    out = {"version": VERSION, "metrics": metrics(backtest(rz, players))}
    if a.tonight:
        out["tonight"] = tonight(db, rz, players, a.season, a.week, a.tonight.split(","))
        out["tonight_status"] = "RESEARCH ONLY -- player TD per game, not written, not a projection change"
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
