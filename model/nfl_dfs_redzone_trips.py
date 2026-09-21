"""Pre-registered study: do RED-ZONE TRIPS forecast a team's touchdowns better
than its own touchdown history?

Registered 2026-09-21, BEFORE any variant was scored. Nothing below was tuned.

Why this and not the workload backtest
--------------------------------------
`model/nfl_dfs_workload.py` budgets attempts / carries / targets. Red-zone
trips do not map to any of those; what they bear on is the TOUCHDOWN budget,
the highest-variance component of a DFS projection and one the production
model derives from season touchdown rates. So this study grades a team
touchdown budget with the same machinery: production shrinkage (half-life 6,
17-game window, 4 prior games toward the league mean), walk-forward, paired
MAE against a production-style baseline, weeks-clustered bootstrap.

Structural premise, checked before registration (not a result): ~28-30% of
drives reach the 20; those convert at a stable 55-58%; drives that never
reach the 20 convert at ~7%. So trips are the bulk of touchdowns, and the
question is only whether trips are MORE persistent than the touchdowns
themselves -- the textbook "touchdown regression" claim.

Target
------
Offensive touchdown DRIVES per team-game (`drive_archetype = 'TOUCHDOWN'`)
from `nfl_pbp_archetypes`, regular season. One source for target and
features, 2022 onward, so nothing is stitched across feeds.

Baseline (production-style)
---------------------------
    td_hat = shrunk EWMA of the team's own touchdown drives per game

Two variants, fixed in advance -- the test family is exactly 2
----------------------------------------------------------------
V1 `trips_x_conversion`:
    trips   = shrunk EWMA of the team's own red-zone trips per game
    v1      = trips * (league touchdown drives / league red-zone trips)
    League conversion is computed over the SAME prior window, never fitted.

V2 `trips_with_opponent`:
    trips   = own trips (as V1) + 0.5 * (opponent's allowed trips - league trips)
    v2      = trips * league conversion
    The 0.5 is the same stated prior the opponent workload study used.

Population
----------
Every team-game from 2024 week 1 to the latest labelled week with the team's
own prior history present and the red-zone flag present on the game's drives.
Strictly earlier weeks only.

Primary metric and kill rule
----------------------------
Per variant: paired MAE difference vs the baseline on the same rows,
bootstrap CI resampling (season, week). Survives only if the 95% CI lies
entirely below zero. CI includes zero: dead. No re-slicing, no re-fitting the
0.5, no third variant bolted on afterwards.

Interpretation guard
--------------------
A survivor is a measurement on TEAM touchdowns. It is not a player TD share,
not a projection change, and `--tonight` writes nothing.
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict

import numpy as np

from config import load_config
from ingest.nfl_dfs_weekly import PipelineDatabase
from model.nfl_dfs_workload import CONFIG, weighted_mean

VERSION = "nfl-dfs-redzone-trips-study-v1"
V2_WEIGHT = 0.5
FIRST_SEASON = 2022
START = (2024, 1)
PBP_TEAM_ALIASES = {"LA": "LAR", "WAS": "WSH", "AZ": "ARI", "JAC": "JAX"}


def team_games(db):
    """One row per team-game: touchdown drives, red-zone trips, drive count."""
    rows = db.execute(f"""
        WITH d AS (
          SELECT DISTINCT season, week, game_id, posteam, defteam, drive, drive_archetype, drive_inside_twenty rz
          FROM nfl_pbp_archetypes
          WHERE season_type='REG' AND season >= {FIRST_SEASON} AND drive IS NOT NULL
            AND posteam IS NOT NULL AND defteam IS NOT NULL
            AND drive_archetype NOT IN ('KNEEL_DOWN','CLOCK_EXPIRED'))
        SELECT season, week, game_id, posteam, defteam,
               count(*) drives,
               sum((drive_archetype='TOUCHDOWN')::int) td,
               sum((rz = 1)::int) trips,
               sum((rz IS NULL)::int) rz_null
        FROM d GROUP BY 1,2,3,4,5 ORDER BY 1,2,3,4""")
    out = []
    for r in rows:
        out.append({"season": r["season"], "week": r["week"], "game_id": r["game_id"],
                    "team": PBP_TEAM_ALIASES.get(r["posteam"], r["posteam"]),
                    "opponent": PBP_TEAM_ALIASES.get(r["defteam"], r["defteam"]),
                    "drives": float(r["drives"]), "td": float(r["td"]), "trips": float(r["trips"]),
                    "rz_known": r["rz_null"] == 0})
    return out


def shrunk(values, league_mean, config=CONFIG):
    values = values[-config["max_games"]:]
    if not values:
        return None, 0
    ewma = weighted_mean(values, config["half_life_games"], config["max_games"])
    w = len(values) / (len(values) + config["prior_games"])
    return w * ewma + (1 - w) * league_mean, len(values)


class Prior:
    def __init__(self, rows, cutoff):
        self.rows = sorted((r for r in rows if (r["season"], r["week"]) < cutoff), key=lambda r: (r["season"], r["week"]))
        self.by_team, self.allowed = defaultdict(list), defaultdict(list)
        for r in self.rows:
            self.by_team[r["team"]].append(r)
            self.allowed[r["opponent"]].append(r)
        self.league_td = float(np.mean([r["td"] for r in self.rows]))
        self.league_trips = float(np.mean([r["trips"] for r in self.rows]))
        self.conversion = sum(r["td"] for r in self.rows) / max(1.0, sum(r["trips"] for r in self.rows))

    def forecast(self, team, opponent):
        own = self.by_team[team]
        if not own:
            return None
        base, n = shrunk([r["td"] for r in own], self.league_td)
        trips, _ = shrunk([r["trips"] for r in own], self.league_trips)
        allowed, n_allowed = shrunk([r["trips"] for r in self.allowed[opponent]], self.league_trips)
        v1 = trips * self.conversion
        v2 = (trips + V2_WEIGHT * (allowed - self.league_trips)) * self.conversion if allowed is not None else None
        return {"baseline": base, "v1": v1, "v2": v2, "own_games": n, "trips": trips,
                "opp_allowed_trips": allowed, "opp_games": n_allowed, "league_conversion": self.conversion}


def backtest(rows):
    out, cache = [], {}
    for t in rows:
        cutoff = (t["season"], t["week"])
        if cutoff < START or not t["rz_known"]:
            continue
        if cutoff not in cache:
            cache[cutoff] = Prior(rows, cutoff)
        f = cache[cutoff].forecast(t["team"], t["opponent"])
        if not f or f["v2"] is None:
            continue
        out.append({"season": t["season"], "week": t["week"], "team": t["team"], "opponent": t["opponent"],
                    "actual": t["td"], "baseline": f["baseline"], "v1": f["v1"], "v2": f["v2"]})
    return out


def paired_ci(rows, variant, draws=4000, seed=20260921):
    weeks = defaultdict(list)
    for r in rows:
        weeks[(r["season"], r["week"])].append(abs(r[variant] - r["actual"]) - abs(r["baseline"] - r["actual"]))
    keys = list(weeks)
    rng = np.random.default_rng(seed)
    means = [np.concatenate([weeks[keys[i]] for i in rng.choice(len(keys), size=len(keys))]).mean() for _ in range(draws)]
    point = float(np.mean(np.concatenate([weeks[k] for k in keys])))
    lo, hi = np.quantile(means, [0.025, 0.975])
    return {"point": point, "ci95": [float(lo), float(hi)], "weeks": len(keys), "verdict": "SURVIVES" if hi < 0 else "DEAD"}


def metrics(rows):
    m = {"n": len(rows), "baseline_mae": float(np.mean([abs(r["baseline"] - r["actual"]) for r in rows])),
         "baseline_bias": float(np.mean([r["baseline"] - r["actual"] for r in rows]))}
    for v in ("v1", "v2"):
        m[f"{v}_mae"] = float(np.mean([abs(r[v] - r["actual"]) for r in rows]))
        m[f"{v}_bias"] = float(np.mean([r[v] - r["actual"] for r in rows]))
        m[f"{v}_delta"] = paired_ci(rows, v)
    return m


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--tonight", default="", help="comma list of TEAM:OPP pairs, e.g. NYG:LAR,LAR:NYG; research only, writes nothing")
    p.add_argument("--season", type=int, default=2026)
    p.add_argument("--week", type=int, default=2)
    a = p.parse_args()
    db = PipelineDatabase(load_config().database_url)
    rows = team_games(db)
    res = backtest(rows)
    out = {"version": VERSION, "v2_weight": V2_WEIGHT, "metrics": metrics(res)}
    if a.tonight:
        prior = Prior(rows, (a.season, a.week))
        out["tonight"] = {pair: prior.forecast(*pair.split(":")) for pair in a.tonight.split(",")}
        out["tonight_status"] = "RESEARCH ONLY -- team touchdown drives, not written, not a projection change"
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
