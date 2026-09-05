"""Transparent workload candidate. Research only; no fantasy-point output."""
from collections import defaultdict
from math import isfinite
import numpy as np

VERSION = "nfl-dfs-workload-v1"
CONFIG = {"half_life_games": 6.0, "max_games": 17, "prior_games": 4.0}
TEAM_FIELDS = ("attempts", "carries", "targets")
PLAYER_FIELDS = {"QB": ("attempts", "carries"), "RB": ("carries", "targets"),
                 "WR": ("targets",), "TE": ("targets",)}


def value(row, key):
    item = (row.get("stats") or {}).get(key)
    return float(item) if isinstance(item, (int, float)) and not isinstance(item, bool) and isfinite(item) and item >= 0 else None


def weighted_mean(values, half_life=6.0, max_games=CONFIG["max_games"]):
    if not values:
        return None
    x = np.asarray(values[-max_games:], dtype=float)
    weights = .5 ** (np.arange(len(x)-1, -1, -1) / half_life)
    return float(np.dot(x, weights) / weights.sum())


def team_forecast(prior_rows, field, config=CONFIG):
    ordered = sorted(prior_rows, key=lambda r: (r["season"], r["week"]))
    valid = [value(r, field) for r in ordered if r.get("scope") != "league"]
    valid = [v for v in valid if v is not None]
    valid = valid[-config["max_games"]:]
    if not valid:
        return None
    league = [value(r, field) for r in ordered if r.get("scope") == "league"]
    league = [v for v in league if v is not None]
    player = weighted_mean(valid, config["half_life_games"], config["max_games"])
    prior = float(np.mean(league)) if league else player
    weight = len(valid) / (len(valid) + config["prior_games"])
    return {"mean": weight*player + (1-weight)*prior, "history_mean": player, "prior": prior,
            "games": len(valid), "weight": weight, "unit": "team game count"}


def player_shares(history, team_rows, player, field, team):
    budgets = {(r["season"], r["week"], r["team"]): value(r, field) for r in team_rows}
    observations = []
    for r in sorted(history, key=lambda r: (r["season"], r["week"])):
        if r["identity"] != player["identity"] or r["team"] != team:
            continue
        numerator, denominator = value(r, field), budgets.get((r["season"], r["week"], team))
        if numerator is not None and denominator and numerator <= denominator:
            observations.append({"season": r["season"], "week": r["week"], "actual": numerator, "share": numerator/denominator})
    return observations


def allocate(team, roster, history, team_rows, budgets, config=CONFIG):
    players = []
    for player in roster:
        components = {}
        for field in PLAYER_FIELDS.get(player["position"], ()):
            observations = player_shares(history, team_rows, player, field, team)
            if observations:
                used = observations[-config["max_games"]:]
                components[field] = {"raw_share": weighted_mean([o["share"] for o in used], config["half_life_games"], config["max_games"]),
                                     "games": len(used), "recorded_games_available": len(observations), "recent": used[-8:]}
        players.append({**player, "components": components})
    for field, budget in budgets.items():
        if not budget:
            continue
        eligible = [(p, p["components"].get(field)) for p in players if p["components"].get(field)]
        total = sum(c["raw_share"] for _, c in eligible)
        scale = max(1.0, total)
        for _, component in eligible:
            component["share"] = component["raw_share"] / scale
            component["mean"] = budget["mean"] * component["share"]
            component["normalization"] = "scaled_to_team_budget" if total > 1 else "none"
        allocated = min(1.0, max(0.0, sum(c["share"] for _, c in eligible)))
        budgets[field]["allocated_share"] = allocated
        budgets[field]["unallocated_share"] = 1 - allocated
    return players


def build(team_history, player_history, games, rosters, as_of, config=CONFIG):
    forecasts = []
    for game in games:
        for team, opponent in ((game["home_team"], game["away_team"]), (game["away_team"], game["home_team"])):
            past = [r for r in team_history if (r["season"], r["week"]) < (game["season"], game["week"])]
            own = [r for r in past if r["team"] == team]
            # Attach an explicitly marked league population for shrinkage.
            expanded = own + [{**r, "scope": "league"} for r in past]
            budgets = {f: team_forecast(expanded, f, config) for f in TEAM_FIELDS}
            if budgets["attempts"] and budgets["targets"] and budgets["targets"]["mean"] > budgets["attempts"]["mean"]:
                budgets["targets"]["mean"] = budgets["attempts"]["mean"]
                budgets["targets"]["constraint"] = "targets_capped_at_attempts"
            current = [p for p in rosters if p["team"] == team and p["position"] in PLAYER_FIELDS]
            player_past = [r for r in player_history if (r["season"], r["week"]) < (game["season"], game["week"])]
            players = allocate(team, current, player_past, past, budgets, config)
            forecasts.append({"game_id": game["game_id"], "season": game["season"], "week": game["week"],
                              "kickoff": game["kickoff"], "team": team, "opponent": opponent,
                              "budgets": budgets, "players": players, "as_of": as_of})
    return forecasts


def backtest(team_history, start=(2024, 1)):
    output = []
    for target in sorted(team_history, key=lambda r: (r["season"], r["week"], r["team"])):
        if (target["season"], target["week"]) < start:
            continue
        prior = [r for r in team_history if (r["season"], r["week"]) < (target["season"], target["week"])]
        own = [r for r in prior if r["team"] == target["team"]]
        expanded = own + [{**r, "scope": "league"} for r in prior]
        for field in TEAM_FIELDS:
            actual, candidate = value(target, field), team_forecast(expanded, field)
            baseline = weighted_mean([v for r in own if (v := value(r, field)) is not None])
            if actual is not None and candidate and baseline is not None:
                output.append({"season": target["season"], "week": target["week"], "team": target["team"],
                               "field": field, "actual": actual, "candidate": candidate["mean"], "baseline": baseline})
    return output


def metrics(rows):
    result = []
    for field in TEAM_FIELDS:
        sample = [r for r in rows if r["field"] == field]
        result.append({"field": field, "n": len(sample),
            "candidate_mae": float(np.mean([abs(r["candidate"]-r["actual"]) for r in sample])) if sample else None,
            "baseline_mae": float(np.mean([abs(r["baseline"]-r["actual"]) for r in sample])) if sample else None,
            "candidate_bias_actual_minus_projected": float(np.mean([r["actual"]-r["candidate"] for r in sample])) if sample else None})
    return result
