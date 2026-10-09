"""Context-bearing shadow variants for the environment and interval studies.

Registered in docs/nfl-dfs-environment-and-interval-studies.md (WP6, WP7).
The shadow ledger froze every baseline with `ProjectionContext()` -- no team
environment at all -- so no environment change could ever be graded there.
This module computes, for one player at freeze time, a small set of frozen
alternative forecasts that differ from production v3 in exactly one stated
way each. They are stored in the shadow payload and graded later; nothing
here touches production.

    env_baseline   v3 with the team's implied total (what production uses)
    env_trailing   v3 with the team factor relative to the team's own
                   trailing scoring instead of the 22.5 league constant,
                   clamp widened to [0.7, 1.3]
    opp_carries    env_baseline plus an opponent term on RUSHING lines only:
                   rushing yards/TDs scaled by the surviving allowed-carries
                   mechanism from the opponent workload study (weight 0.5,
                   not re-fitted)
    interval_rq    env_baseline mean; p10/p90 from residual quantiles of the
                   recency-weighted own-mean estimator, by position x history
                   bucket, computed walk-forward inside the frozen history
    prior8         v3 with prior_equivalent_games = 8 (env_baseline context)

v3 (`model/nfl_dfs_historical.py`) is imported, never edited.
"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from typing import Iterable, Mapping, Sequence

import numpy as np

from model.nfl_dfs_historical import (
    BOOM_THRESHOLDS, MODEL_CONFIG, HistoricalWeek, ProjectionContext, _environment_factors, _peer_rows,
    _recency_weights, _weighted_mean, adjust_stat_line, before_cutoff, draftkings_points, project_player,
)
from model.nfl_dfs_workload import CONFIG as WORKLOAD_CONFIG, weighted_mean

VARIANTS_VERSION = "nfl-dfs-context-variants-v1"
OPP_CARRIES_WEIGHT = 0.5             # the opponent study's stated prior; not re-fitted
TRAILING_CLAMP = (0.70, 1.30)
PRIOR8 = 8.0
HISTORY_BUCKETS = ((2, 5, "hist_2_5"), (6, 16, "hist_6_16"), (17, 10**6, "hist_17_plus"))
RUSH_FIELDS = ("rushing_yards", "rushing_tds")


def trailing_points(games: Iterable[Mapping], team: str, cutoff: tuple[int, int],
                    config: Mapping = WORKLOAD_CONFIG) -> dict | None:
    """Shrunk EWMA of points the team scored in completed games before cutoff.

    games: {season, week, home_team, away_team, home_score, away_score, completed}
    Shrinks toward the league mean over the same prior window with the
    workload study's constants (half-life 6, 17 games, 4 prior games).
    """
    prior = sorted((g for g in games if g.get("completed") and g.get("home_score") is not None
                    and (int(g["season"]), int(g["week"])) < cutoff), key=lambda g: (int(g["season"]), int(g["week"])))
    own = [float(g["home_score"] if g["home_team"] == team else g["away_score"])
           for g in prior if team in (g["home_team"], g["away_team"])]
    if not own or not prior:
        return None
    league = float(np.mean([float(g["home_score"]) for g in prior] + [float(g["away_score"]) for g in prior]))
    ewma = weighted_mean(own, config["half_life_games"], config["max_games"])
    n = min(len(own), config["max_games"])
    w = n / (n + config["prior_games"])
    return {"trailing_ppg": w * ewma + (1 - w) * league, "own_games": n, "league_ppg": league}


def rush_factor(prior, team: str, opponent: str) -> dict | None:
    """v1 / candidate carries budget from the opponent workload study's Prior."""
    own = prior.own(team, "carries")
    if not own:
        return None
    allowed, n_allowed = prior.allowed(opponent, "carries")
    if allowed is None or own["mean"] <= 0:
        return None
    v1 = own["mean"] + OPP_CARRIES_WEIGHT * (allowed - prior.league_mean["carries"])
    return {"factor": max(0.5, min(1.5, v1 / own["mean"])), "own_carries": own["mean"],
            "opp_allowed_carries": allowed, "opp_games": n_allowed, "league_carries": prior.league_mean["carries"]}


def residual_quantiles(history: Sequence[HistoricalWeek], half_life: float = 6.0, max_games: int = 34) -> dict:
    """Walk-forward residuals of the recency-weighted own mean, by position x bucket.

    Cheap proxy for the model's residual distribution: for every player-game
    with >= 2 prior games, residual = actual DK points - recency-weighted mean
    of the prior games (the `baseline_fpts` estimator v3 already computes).
    """
    by_player: dict[int, list[HistoricalWeek]] = defaultdict(list)
    for r in sorted(history, key=lambda r: (r.chronological_key, r.player_id)):
        by_player[r.player_id].append(r)
    residuals: dict[str, list[float]] = defaultdict(list)
    for rows in by_player.values():
        for i in range(2, len(rows)):
            own = rows[max(0, i - max_games):i]
            est = _weighted_mean([r.dk_points for r in own], _recency_weights(own, half_life))
            bucket = next(name for lo, hi, name in HISTORY_BUCKETS if lo <= len(own) <= hi)
            residuals[f"{rows[i].position}:{bucket}"].append(rows[i].dk_points - est)
    return {key: {"n": len(v), "q10": float(np.quantile(v, 0.10)), "q90": float(np.quantile(v, 0.90))}
            for key, v in residuals.items() if len(v) >= 30}


def _bucket(n: int) -> str | None:
    return next((name for lo, hi, name in HISTORY_BUCKETS if lo <= n <= hi), None)


def _project_with_rush_factor(*, player_id, player_gsis_id, player_name, position, historical_rows,
                              cutoff_season, cutoff_week, context, seed, config, factor):
    """v3's draw loop with rushing yards/TDs scaled by `factor` after the
    environment adjustment. Only the two rushing fields differ from v3."""
    prior = sorted((r for r in historical_rows if before_cutoff(r, cutoff_season, cutoff_week)), key=lambda r: r.chronological_key)
    own = [r for r in prior if r.player_id == player_id][-int(config["max_player_games"]):]
    peers = _peer_rows(position, own, prior, int(config["max_prior_games"]))
    if len(own) < int(config["minimum_historical_games"]):
        return None
    own_weights = _recency_weights(own, float(config["player_half_life_games"]))
    strength = len(own) / (len(own) + float(config["prior_equivalent_games"]))
    identity_seed = int(hashlib.sha256(f"{seed}:{player_id}:{player_gsis_id}:{player_name}".encode()).hexdigest()[:16], 16)
    rng = np.random.default_rng(identity_seed)
    draws = int(config["draws"])
    choose = rng.random(draws) < strength
    own_idx = rng.choice(len(own), size=draws, p=own_weights)
    peer_idx = rng.choice(len(peers), size=draws) if peers else np.zeros(draws, dtype=int)
    scores = []
    for i in range(draws):
        row = own[int(own_idx[i])] if (choose[i] or not peers) else peers[int(peer_idx[i])]
        stats = adjust_stat_line(position, row.stats, context, config)
        for key in RUSH_FIELDS:
            if key in stats:
                stats[key] = stats[key] * factor
        scores.append(draftkings_points(position, stats))
    a = np.asarray(scores)
    return {"mean": round(float(a.mean()), 4), "p10": round(float(np.quantile(a, .1)), 4),
            "p90": round(float(np.quantile(a, .9)), 4),
            "boom_probability": round(float(np.mean(a >= BOOM_THRESHOLDS[position])), 6)}


def _pack(p) -> dict:
    return {"mean": p.model_proj_fpts, "p10": p.floor_fpts, "p90": p.ceiling_fpts, "boom_probability": p.boom_rate}


def context_variants(*, player_id, player_gsis_id, player_name, position, historical_rows, cutoff_season,
                     cutoff_week, seed, config, team_implied_total, trailing: dict | None,
                     rush: dict | None, quantiles: dict, history_games: int) -> dict:
    common = dict(player_id=player_id, player_gsis_id=player_gsis_id, player_name=player_name, position=position,
                  historical_rows=historical_rows, cutoff_season=cutoff_season, cutoff_week=cutoff_week, seed=seed)
    env = ProjectionContext(team_implied_total=team_implied_total)
    out: dict = {"version": VARIANTS_VERSION, "team_implied_total": team_implied_total}
    base = project_player(**common, context=env, config=config)
    out["env_baseline"] = _pack(base)
    if trailing and team_implied_total:
        cfg = {**config, "league_team_points": trailing["trailing_ppg"],
               "min_environment_factor": TRAILING_CLAMP[0], "max_environment_factor": TRAILING_CLAMP[1]}
        out["env_trailing"] = {**_pack(project_player(**common, context=env, config=cfg)), "inputs": trailing}
    else:
        out["env_trailing"] = None
    if rush and position in ("QB", "RB", "WR", "TE"):
        scaled = _project_with_rush_factor(**common, context=env, config=config, factor=rush["factor"])
        out["opp_carries"] = {**scaled, "inputs": rush} if scaled else None
    else:
        out["opp_carries"] = None
    q = quantiles.get(f"{position}:{_bucket(history_games)}") if _bucket(history_games) else None
    out["interval_rq"] = ({"mean": base.model_proj_fpts, "p10": round(base.model_proj_fpts + q["q10"], 4),
                           "p90": round(base.model_proj_fpts + q["q90"], 4), "boom_probability": base.boom_rate,
                           "bucket": f"{position}:{_bucket(history_games)}", "residual_n": q["n"]} if q else None)
    out["prior8"] = _pack(project_player(**common, context=env, config={**config, "prior_equivalent_games": PRIOR8}))
    return out
