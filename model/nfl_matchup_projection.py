"""Registered PFR challengers. Numerical output is shadow-only until qualified.

Baseline sampling is an adapter over v5's existing functions, with exactly
the same identities, RNG calls, priors and scoring. The protected model is
not edited to add research behavior.
"""
from __future__ import annotations

import hashlib
from copy import deepcopy

import numpy as np

from model.nfl_context_engine import stable_digest
from model.nfl_dfs_historical import (
    MODEL_CONFIG, BOOM_THRESHOLDS, ProjectionContext, before_cutoff,
    _peer_rows, _recency_weights, adjust_stat_line, draftkings_points,
)
from model.nfl_pfr_supplement import team_code

VERSION = "nfl-matchup-shadow-v1"
RIDGE = 20.0
MAX_EFFICIENCY_CHANGE = 0.10
MIN_TRAINING_ROWS = 100
REPRODUCTION_TOLERANCE = 0.00011  # Saved v5 statistics are rounded to four decimals.
FAMILIES = {"pressure": ("own_pressure", "opp_pressure"),
            "contact": ("own_ybc", "opp_ybc", "own_yac", "opp_yac")}


def sample_baseline_draws(player, history, config=None, seed=None):
    config = {**MODEL_CONFIG, **(config or {})}
    fs = player.get("feature_snapshot") or {}
    cutoff_season, cutoff_week = int(fs.get("cutoff_season", player.get("season", 2026))), fs.get("cutoff_week", player.get("week"))
    if cutoff_week is None:
        raise ValueError("an explicit baseline cutoff week is required")
    seed = int(seed if seed is not None else fs.get("seed", 20260902))
    player_id, gsis = player.get("player_id"), player.get("player_gsis_id")
    name, position = player["player_name"], player["position"]
    prior = sorted((r for r in history if before_cutoff(r, cutoff_season, cutoff_week)), key=lambda r: r.chronological_key)
    own = [r for r in prior if player_id is not None and r.player_id == player_id]
    if not own and gsis:
        own = [r for r in prior if r.player_gsis_id == gsis]
    own = own[-int(config["max_player_games"]):]
    peers = _peer_rows(position, own, prior, int(config["max_prior_games"]))
    if not peers and len(own) < int(config["minimum_historical_games"]):
        return []
    strength = len(own) / (len(own) + float(config["prior_equivalent_games"])) if len(own) >= int(config["minimum_historical_games"]) else 0.0
    identity_seed = int(hashlib.sha256(f"{seed}:{player_id}:{gsis}:{name}".encode()).hexdigest()[:16], 16)
    rng = np.random.default_rng(identity_seed)
    count = int(config["draws"])
    choose = rng.random(count) < strength
    own_indices = rng.choice(len(own), size=count, p=_recency_weights(own, float(config["player_half_life_games"]))) if own else np.zeros(count, dtype=int)
    peer_indices = rng.choice(len(peers), size=count) if peers else np.zeros(count, dtype=int)
    context = ProjectionContext(team_implied_total=fs.get("team_implied_total"), opponent_factor=fs.get("opponent_factor"))
    return [adjust_stat_line(position, (own[int(own_indices[i])] if own and (choose[i] or not peers) else peers[int(peer_indices[i])]).stats,
                             context, config) for i in range(count)]


def summarize_draws(position, draws):
    if not draws:
        return None
    scores = np.asarray([draftkings_points(position, d) for d in draws])
    keys = set().union(*(d.keys() for d in draws))
    return {"mean": float(scores.mean()), "p10": float(np.quantile(scores, .1)),
            "p50": float(np.quantile(scores, .5)), "p90": float(np.quantile(scores, .9)),
            "boom": float(np.mean(scores >= BOOM_THRESHOLDS[position])),
            "stat_means": {k: float(np.mean([d.get(k, 0) for d in draws])) for k in sorted(keys)}}


def saved_summary(player):
    """Retain the actual saved forecast even when its draws cannot be replayed."""
    fields = {"mean": "model_proj_fpts", "p10": "floor_fpts", "p50": "median_fpts",
              "p90": "ceiling_fpts", "boom": "boom_rate"}
    return {**{key: player.get(field) for key, field in fields.items()},
            "stat_means": deepcopy(player.get("stat_means") or {})}


def reproduction_check(player, summary):
    if summary is None:
        return {"passed": False, "reason": "no_draws", "tolerance": REPRODUCTION_TOLERANCE}
    saved = saved_summary(player)
    compared = {}
    for key in ("mean", "p10", "p50", "p90", "boom"):
        value = saved.get(key)
        if value is None or not np.isfinite(value):
            return {"passed": False, "reason": "missing_saved_" + key, "tolerance": REPRODUCTION_TOLERANCE}
        compared[key] = abs(summary[key] - value)
    if not saved["stat_means"] or set(saved["stat_means"]) != set(summary["stat_means"]):
        return {"passed": False, "reason": "saved_stat_fields_differ", "tolerance": REPRODUCTION_TOLERANCE}
    compared["stat_means"] = max(abs(summary["stat_means"][key] - value) for key, value in saved["stat_means"].items())
    passed = all(np.isfinite(value) and value <= REPRODUCTION_TOLERANCE for value in compared.values())
    return {"passed": passed, "reason": "matched_saved_distribution" if passed else "saved_distribution_differs",
            "tolerance": REPRODUCTION_TOLERANCE, "max_absolute_differences": compared}


def fit_family(family: str, rows: list[dict], source_manifest: dict) -> dict:
    """Fit only the supplied development population; this never grants approval."""
    names = FAMILIES[family]
    eligible = [r for r in rows if r.get("family") == family and all(r.get(k) is not None and np.isfinite(r[k]) for k in (*names, "residual"))]
    if len(eligible) < MIN_TRAINING_ROWS:
        return {"family": family, "status": "insufficient_development_data", "n": len(eligible)}
    x = np.array([[r[k] for k in names] for r in eligible], dtype=float)
    y = np.array([r["residual"] for r in eligible], dtype=float)
    center, scale = x.mean(axis=0), x.std(axis=0)
    scale = np.where(scale > 1e-9, scale, 1.)
    z = (x - center) / scale
    # No fitted intercept: this is an incremental matchup effect, not a new
    # unconditional baseline correction hidden inside PFR.
    beta = np.linalg.solve(z.T @ z + RIDGE * np.eye(len(names)), z.T @ y)
    artifact = {"version": VERSION, "family": family, "status": "research_fitted", "authority": "shadow_only",
                "features": list(names), "center": center.tolist(), "scale": scale.tolist(), "coefficients": beta.tolist(),
                "ridge": RIDGE, "max_efficiency_change": MAX_EFFICIENCY_CHANGE, "n": len(eligible),
                "training_rows_digest": stable_digest(eligible), "source_manifest": source_manifest,
                "target": "yards_per_attempt_residual" if family == "pressure" else "yards_per_carry_residual",
                "forward_weeks": 0, "production_qualified": False}
    artifact["artifact_hash"] = stable_digest(artifact)
    return artifact


def feature_vector(matchup, team, family):
    team = team_code(team)
    opponent = matchup["away"] if matchup["home"] == team else matchup["home"]
    own = matchup["teams"][team]["offense"]
    allowed = matchup["teams"][opponent]["defense"]
    if family == "pressure":
        if own.get("pressure_coverage_usable", own.get("pressure_coverage_complete")) is not True or allowed.get("pressure_coverage_usable", allowed.get("pressure_coverage_complete")) is not True:
            return None
        if min(own["pressure_games"], allowed["pressure_games"]) < 2:
            return None
        return {"own_pressure": own["pressure_pct"], "opp_pressure": allowed["pressure_pct"]}
    if own.get("contact_coverage_usable", own.get("contact_coverage_complete")) is not True or allowed.get("contact_coverage_usable", allowed.get("contact_coverage_complete")) is not True:
        return None
    if min(own.get("rb_carries") or 0, allowed.get("rb_carries") or 0) < 20:
        return None
    return {"own_ybc": own["rb_before_contact_per_carry"], "opp_ybc": allowed["rb_before_contact_per_carry"],
            "own_yac": own["rb_after_contact_per_carry"], "opp_yac": allowed["rb_after_contact_per_carry"]}


def shadow_projection(player, baseline_draws, matchup, fitted, *, include_scores=False):
    """Separate numerical challenger; never modifies caller's active projection."""
    position = player["position"]
    family = "pressure" if position == "QB" else "contact" if position == "RB" else None
    baseline = summarize_draws(position, baseline_draws)
    replay = reproduction_check(player, baseline)
    result = {"version": VERSION, "status": "not_applied", "authority": "shadow_only", "active_delta": 0.0,
              "baseline": baseline, "candidate": baseline, "delta": 0.0, "ledger": [],
              "matchup_manifest_hash": matchup["manifest_hash"], "model_artifact_hash": None}
    result["reproduction"] = replay
    reason = None
    artifact = fitted.get(family, {})
    x = feature_vector(matchup, player["team"], family) if family else None
    if player.get("projection_status") == "out" or player.get("is_out"):
        reason = "ineligible_or_out"
    elif not family:
        reason = "no_registered_effect_for_position"
    elif baseline is None:
        reason = "no_baseline_draws"
    elif artifact.get("status") != "research_fitted":
        reason = "no_fitted_research_model"
    elif not x or any(v is None for v in x.values()):
        reason = "insufficient_matching_pressure_or_rb_contact_history"
    # Availability-adjusted stat lines cannot be recreated by applying an
    # unrelated location shift to their original historical draws.
    elif not replay["passed"]:
        reason = "saved_baseline_not_reproduced_or_availability_adjusted"
    if reason:
        result["reason"] = reason
        result["baseline"] = result["candidate"] = saved_summary(player)
        result["distribution_available"] = replay["passed"] and reason != "ineligible_or_out"
        if include_scores and result["distribution_available"]:
            result["scores"] = [draftkings_points(position, d) for d in baseline_draws]
        return result
    field, exposure = ("passing_yards", "attempts") if family == "pressure" else ("rushing_yards", "carries")
    mean_units = baseline["stat_means"].get(exposure, 0)
    mean_yards = baseline["stat_means"].get(field, 0)
    if mean_units <= 0 or mean_yards <= 0:
        result["reason"] = "no_positive_baseline_efficiency"
        result["baseline"] = result["candidate"] = saved_summary(player)
        result["distribution_available"] = True
        if include_scores:
            result["scores"] = [draftkings_points(position, d) for d in baseline_draws]
        return result
    before = mean_yards / mean_units
    raw = float(np.dot((np.array([x[k] for k in artifact["features"]]) - artifact["center"]) / artifact["scale"], artifact["coefficients"]))
    delta_rate = float(np.clip(raw, -before * MAX_EFFICIENCY_CHANGE, before * MAX_EFFICIENCY_CHANGE))
    factor = (before + delta_rate) / before
    candidate_draws = deepcopy(baseline_draws)
    for draw in candidate_draws:
        draw[field] *= factor
    candidate = summarize_draws(position, candidate_draws)
    result.update(status="under_evaluation", candidate=candidate, delta=candidate["mean"]-baseline["mean"],
                  model_artifact_hash=artifact["artifact_hash"], reason="prospective_gate_not_yet_scorable", distribution_available=True)
    result["ledger"] = [{"family": family, "component": field, "unit": "yards_per_opportunity",
                         "before": before, "after": before+delta_rate, "raw_rate_delta": raw,
                         "factor": factor, "points_delta": result["delta"], "features": x,
                         "status": "shadow_only", "unchanged": "opportunity, touchdowns, all other stat fields"}]
    if include_scores:
        result["scores"] = [draftkings_points(position, d) for d in candidate_draws]
    return result
