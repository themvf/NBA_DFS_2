"""Pre-registered study: does the OPPONENT improve the team workload budget?

`model/nfl_dfs_workload.py` forecasts a team's attempts / carries / targets
from that team's own history alone, shrunk toward the league. It never asks
who the team is playing. This study tests whether adding the opponent helps,
against the same walk-forward backtest that already grades that forecast.

Registered 2026-09-21, BEFORE any variant was scored. Nothing below was tuned.

Hypothesis
----------
A defence's tendency to allow volume is persistent enough that adding it to
the team's own forecast lowers out-of-sample error on the three budgets.

Two variants, fixed in advance -- the test family is exactly 2
----------------------------------------------------------------
V1 `allowed_volume` (same source as the budgets, no archetype data):
    allowed  = EWMA over the opponent's prior games of what THEIR opponents
               produced in this field, shrunk toward the league mean with the
               production `prior_games` (4), same half-life and window.
    v1       = own_forecast + 0.5 * (allowed - league_mean)
    0.5 is the textbook equal-weight offence/defence blend. It is a stated
    prior, not a fitted constant, and it is not tuned here.

V2 `pace_allowed` (from `nfl_pbp_archetypes`, the reason this was asked):
    plays_allowed = EWMA over the opponent's prior games of scrimmage plays
                    they faced, shrunk toward the league mean the same way.
    v2            = own_forecast * (plays_allowed / league_plays) ** 0.5
    Exponent 0.5 mirrors V1's half weight. Also a stated prior.

Population
----------
Every team-game from 2024 week 1 onward that the production backtest already
scores (its own `backtest()` population: actual present, own history
present). Strictly earlier weeks only, same as production. The Rams are `LAR`
in the DFS tables and `LA` in play-by-play; mapped here, never guessed.

Primary metric and kill rule
----------------------------
Per field: MAE of the variant vs MAE of the production candidate on the SAME
rows (paired). Bootstrap CI on the paired MAE difference, resampling WEEKS
(season, week) so the 32 games of one weekend are not 32 independent draws.
A variant survives only if its CI lies entirely below zero for a field. Any
field where the CI includes zero: the variant is dead for that field. No
re-slicing by season, team or favourite/underdog to rescue it.

Interpretation guard
--------------------
Passing this is a MEASUREMENT on team volume, not permission to change a
displayed projection. `--tonight` prints research budgets for one game so the
numbers can be looked at; it writes nothing and is labelled as such.
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone

import numpy as np

from config import load_config
from ingest.nfl_dfs_weekly import PipelineDatabase
from ingest.nfl_dfs_workload import raw_history, inputs
from model.nfl_dfs_study_provenance import provenance
from model.nfl_dfs_workload import CONFIG, TEAM_FIELDS, allocate, team_forecast, value, weighted_mean

VERSION = "nfl-dfs-workload-opponent-study-v1"
V1_WEIGHT = 0.5
V2_EXPONENT = 0.5
PBP_TEAM_ALIASES = {"LA": "LAR", "WAS": "WSH", "AZ": "ARI", "JAC": "JAX"}
SCRIMMAGE = "play_archetype NOT IN ('SPECIAL_TEAMS','NON_PLAY','KNEEL','SPIKE','TWO_POINT') AND down IS NOT NULL"


def shrunk_ewma(values, league_mean, config=CONFIG):
    """Production shrinkage applied to an opponent's allowed series."""
    values = values[-config["max_games"]:]
    if not values:
        return None, 0
    ewma = weighted_mean(values, config["half_life_games"], config["max_games"])
    weight = len(values) / (len(values) + config["prior_games"])
    return weight * ewma + (1 - weight) * league_mean, len(values)


def plays_faced(db):
    """Scrimmage plays each defence faced, per game, from the archetype table."""
    rows = db.execute(f"""SELECT season, week, defteam, count(*) plays FROM nfl_pbp_archetypes
        WHERE season_type='REG' AND {SCRIMMAGE} AND defteam IS NOT NULL GROUP BY 1,2,3""")
    return {(r["season"], r["week"], PBP_TEAM_ALIASES.get(r["defteam"], r["defteam"])): float(r["plays"]) for r in rows}


class Prior:
    """Everything strictly before one (season, week), indexed once per week."""

    def __init__(self, team_rows, plays, cutoff):
        self.rows = [r for r in team_rows if (r["season"], r["week"]) < cutoff]
        self.rows.sort(key=lambda r: (r["season"], r["week"]))
        self.by_team = defaultdict(list)
        self.allowed_by_opp = defaultdict(list)
        for r in self.rows:
            self.by_team[r["team"]].append(r)
            self.allowed_by_opp[r["opponent"]].append(r)
        self.league_mean = {f: float(np.mean([v for r in self.rows if (v := value(r, f)) is not None])) for f in TEAM_FIELDS}
        faced = sorted(((k, v) for k, v in plays.items() if (k[0], k[1]) < cutoff), key=lambda kv: kv[0][:2])
        self.plays_faced = defaultdict(list)
        for (season, week, team), n in faced:
            self.plays_faced[team].append(n)
        self.league_plays = float(np.mean([n for _, n in faced])) if faced else None

    def own(self, team, field):
        expanded = self.by_team[team] + [{**r, "scope": "league"} for r in self.rows]
        return team_forecast(expanded, field)

    def allowed(self, opponent, field):
        series = [v for r in self.allowed_by_opp[opponent] if (v := value(r, field)) is not None]
        return shrunk_ewma(series, self.league_mean[field])

    def pace(self, opponent):
        if self.league_plays is None:
            return None, 0
        return shrunk_ewma(self.plays_faced[opponent], self.league_plays)


def variants(prior, team, opponent, field):
    own = prior.own(team, field)
    if not own:
        return None
    allowed, n_allowed = prior.allowed(opponent, field)
    pace, n_pace = prior.pace(opponent)
    out = {"candidate": own["mean"], "own_games": own["games"]}
    if allowed is not None:
        out["v1"] = own["mean"] + V1_WEIGHT * (allowed - prior.league_mean[field])
        out["v1_allowed"] = allowed
        out["v1_games"] = n_allowed
    if pace is not None and prior.league_plays:
        out["v2"] = own["mean"] * (pace / prior.league_plays) ** V2_EXPONENT
        out["v2_pace"] = pace
        out["v2_games"] = n_pace
    return out


def backtest(team_rows, plays, start=(2024, 1)):
    output = []
    targets = sorted((r for r in team_rows if (r["season"], r["week"]) >= start), key=lambda r: (r["season"], r["week"], r["team"]))
    prior_cache = {}
    for target in targets:
        cutoff = (target["season"], target["week"])
        if cutoff not in prior_cache:
            prior_cache[cutoff] = Prior(team_rows, plays, cutoff)
        prior = prior_cache[cutoff]
        for field in TEAM_FIELDS:
            actual = value(target, field)
            if actual is None or not prior.by_team[target["team"]]:
                continue
            v = variants(prior, target["team"], target["opponent"], field)
            if not v or "v1" not in v or "v2" not in v:
                continue
            output.append({"season": target["season"], "week": target["week"], "team": target["team"],
                           "opponent": target["opponent"], "field": field, "actual": actual,
                           "candidate": v["candidate"], "v1": v["v1"], "v2": v["v2"]})
    return output


def paired_ci(rows, variant, draws=4000, seed=20260921):
    """Bootstrap CI on mean(|variant-actual| - |candidate-actual|), resampling weeks."""
    weeks = defaultdict(list)
    for r in rows:
        weeks[(r["season"], r["week"])].append(abs(r[variant] - r["actual"]) - abs(r["candidate"] - r["actual"]))
    keys = list(weeks)
    rng = np.random.default_rng(seed)
    means = []
    for _ in range(draws):
        pick = rng.choice(len(keys), size=len(keys))
        sample = np.concatenate([weeks[keys[i]] for i in pick])
        means.append(sample.mean())
    point = float(np.mean(np.concatenate([weeks[k] for k in keys])))
    lo, hi = np.quantile(means, [0.025, 0.975])
    return point, float(lo), float(hi), len(keys)


def metrics(rows):
    result = []
    for field in TEAM_FIELDS:
        sample = [r for r in rows if r["field"] == field]
        if not sample:
            continue
        item = {"field": field, "n": len(sample),
                "candidate_mae": float(np.mean([abs(r["candidate"] - r["actual"]) for r in sample]))}
        for variant in ("v1", "v2"):
            item[f"{variant}_mae"] = float(np.mean([abs(r[variant] - r["actual"]) for r in sample]))
            point, lo, hi, weeks = paired_ci(sample, variant)
            item[f"{variant}_delta"] = {"point": point, "ci95": [lo, hi], "weeks": weeks,
                                        "verdict": "SURVIVES" if hi < 0 else "DEAD"}
        result.append(item)
    return result


def tonight(db, team_rows, players, plays, season, week, teams):
    """Research budgets for one slate. Writes nothing."""
    now = datetime.now(timezone.utc)
    games, roster = inputs(db, season, week, now)
    prior = Prior(team_rows, plays, (season, week))
    player_past = [r for r in players if (r["season"], r["week"]) < (season, week)]
    report = []
    for game in games:
        for team, opponent in ((game["home_team"], game["away_team"]), (game["away_team"], game["home_team"])):
            if teams and team not in teams:
                continue
            budgets, detail = {}, {}
            for field in TEAM_FIELDS:
                v = variants(prior, team, opponent, field)
                detail[field] = v
                if not v:
                    budgets[field] = None
                    continue
                # Only the CARRIES opponent term survived its kill test; attempts
                # and targets stay on the production candidate. Applying v1 to
                # the dead fields here (as the first version did) would print a
                # budget the study itself rejected.
                use_v1 = field == "carries" and "v1" in v
                budgets[field] = {"mean": v["v1"] if use_v1 else v["candidate"],
                                  "basis": "v1_allowed_carries" if use_v1 else "candidate"}
            # Same coherence cap production applies in model.nfl_dfs_workload.build().
            if budgets["attempts"] and budgets["targets"] and budgets["targets"]["mean"] > budgets["attempts"]["mean"]:
                budgets["targets"]["mean"] = budgets["attempts"]["mean"]
                budgets["targets"]["constraint"] = "targets_capped_at_attempts"
            current = [p for p in roster if p["team"] == team]
            alloc = allocate(team, current, player_past, prior.rows, budgets)
            report.append({"game_id": game["game_id"], "team": team, "opponent": opponent, "budgets": detail,
                           "players": [{"name": p["name"], "position": p["position"],
                                        "v1_means": {k: round(c["mean"], 1) for k, c in p["components"].items() if "mean" in c}}
                                       for p in alloc if p["components"]]})
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tonight", action="store_true", help="print research budgets for the next slate; writes nothing")
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--week", type=int, default=2)
    parser.add_argument("--teams", default="", help="comma list, e.g. NYG,LAR")
    args = parser.parse_args()
    db = PipelineDatabase(load_config().database_url)
    players, team_rows = raw_history(db)
    plays = plays_faced(db)
    rows = backtest(team_rows, plays)
    out = {"version": VERSION, "v1_weight": V1_WEIGHT, "v2_exponent": V2_EXPONENT, "rows": len(rows), "metrics": metrics(rows),
           "kill_tests_in_family": 6, "provenance": provenance(team_rows)}
    if args.tonight:
        out["tonight"] = tonight(db, team_rows, players, plays, args.season, args.week, [t for t in args.teams.split(",") if t])
        out["tonight_status"] = "RESEARCH ONLY -- not written, not a projection change"
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
