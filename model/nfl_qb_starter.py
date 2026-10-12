"""Expected-starter quarterback construction, `nfl-qb-spot-start-v2` (docs/nfl-qb-spot-start-study.md).

An overlay on the historical DFS model, applied by the production writer after
the base projection, like the availability transfer: the base model file and
its version are untouched, the writer records the overlay in its configuration
(`qb_starter_mode`) and every replaced row carries `qb_starter_construction`.

For a quarterback the caller names the expected starter and whose starting
history is thin (mean attempts over his previous `trailing_games` below
`trailing_limit`, or fewer than two prior games):

* volume is the TEAM's expected pass attempts (`team_pass_attempts`);
* lines are drawn from his own games with at least `min_line_attempts`,
  blended by n/(n + prior_equivalent_games) with other quarterbacks' first
  `spot_start_prior_games` starts;
* each drawn line is rescaled so attempts equal the team volume (passing
  stats move with it, rushing stays his own), then the base model's
  environment adjustment and scoring apply.

Constants are the frozen development fit (artifacts/nfl-qb-spot-start/fit.json).
Blind 2016-2019: spot-start DK-points MAE 7.38 -> 5.79, 95% interval of the
change [-2.76, -0.53]; every other quarterback is projected exactly as before.
"""
from __future__ import annotations

import hashlib
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from model.nfl_dfs_historical import (BOOM_THRESHOLDS, MODEL_CONFIG, MODEL_VERSION, OFFENSE_FIELDS, HistoricalProjection,
                                      HistoricalWeek, ProjectionContext, _environment_factors, _number, _recency_weights,
                                      _weighted_mean, adjust_stat_line, before_cutoff, draftkings_points)

VERSION = "nfl-qb-spot-start-v2"
CONFIG: dict[str, Any] = {
    "team_volume_half_life": 8.0,
    "team_volume_prior_games": 2.0,
    "spot_start_prior_games": 16,
    "min_line_attempts": 10.0,
    "volume_scale_clip": (0.5, 2.0),
    "trailing_games": 6,
    "trailing_limit": 15.0,
}
PASS_VOLUME_FIELDS = ("passing_yards", "passing_tds", "passing_interceptions", "passing_2pt_conversions")


def team_pass_attempts(rows: Iterable[HistoricalWeek], team: str | None, config: Mapping[str, Any] = CONFIG) -> float | None:
    """A team's expected pass attempts: per-game attempts over prior games,
    recency-weighted (half-life in games), shrunk toward the league mean.
    `rows` must already be restricted to games before the cutoff."""
    from model.nfl_team_aliases import normalize_team
    if not team:
        return None
    wanted = normalize_team(team)
    per_game: dict[tuple[str, int, int], float] = {}
    for row in rows:
        if row.position != "QB" or not row.team:
            continue
        key = (normalize_team(row.team), row.season, row.week)
        per_game[key] = per_game.get(key, 0.0) + _number(row.stats, "attempts")
    if not per_game:
        return None
    league = float(np.mean(list(per_game.values())))
    own = sorted(((s, w), v) for (t, s, w), v in per_game.items() if t == wanted)
    if not own:
        return league
    values = np.asarray([v for _, v in own], dtype=float)
    weights = 0.5 ** (np.arange(len(values))[::-1] / float(config["team_volume_half_life"]))
    prior = float(config["team_volume_prior_games"])
    return float((np.dot(weights, values) + prior * league) / (weights.sum() + prior))


def spot_start_prior_rows(rows: Iterable[HistoricalWeek], config: Mapping[str, Any] = CONFIG) -> list[HistoricalWeek]:
    """Every quarterback's first S games with 15+ attempts, pooled: what a thin starting history looks like."""
    limit = int(config["spot_start_prior_games"])
    by_player: dict[int, list[HistoricalWeek]] = {}
    for row in rows:
        if row.position == "QB" and _number(row.stats, "attempts") >= 15:
            by_player.setdefault(row.player_id, []).append(row)
    out: list[HistoricalWeek] = []
    for lines in by_player.values():
        out.extend(sorted(lines, key=lambda r: r.chronological_key)[:limit])
    return out


def thin_starting_history(own: Sequence[HistoricalWeek], config: Mapping[str, Any] = CONFIG) -> bool:
    """The study's spot-start definition, from pregame history."""
    recent = list(own)[-int(config["trailing_games"]):]
    if len(recent) < 2:
        return True
    return float(np.mean([_number(r.stats, "attempts") for r in recent])) < float(config["trailing_limit"])


def rescale_to_team_volume(stats: Mapping[str, Any], target_attempts: float, config: Mapping[str, Any] = CONFIG) -> dict[str, float]:
    """Scale one drawn line so its attempts equal the team volume; passing moves with it, rushing stays."""
    line = {key: _number(stats, key) for key in OFFENSE_FIELDS}
    attempts = line["attempts"]
    if attempts <= 0:
        return line
    lo, hi = config["volume_scale_clip"]
    scale = float(np.clip(target_attempts / attempts, lo, hi))
    for key in PASS_VOLUME_FIELDS:
        line[key] *= scale
    line["attempts"] = attempts * scale
    return line


def project_expected_starter(
    *,
    player_id: int | None,
    player_gsis_id: str | None,
    player_name: str,
    historical_rows: Iterable[HistoricalWeek],
    cutoff_season: int,
    cutoff_week: int | None,
    context: ProjectionContext,
    team_pass_attempts: float | None,
    seed: int,
    config: Mapping[str, Any] = MODEL_CONFIG,
    overlay: Mapping[str, Any] = CONFIG,
) -> HistoricalProjection | None:
    """The construction for a named expected starter, or None when it does not apply
    (thick starting history, no team volume, no usable lines). Mirrors the base
    model's draw mechanics so the two are comparable draw for draw."""
    if team_pass_attempts is None:
        return None
    prior = sorted((row for row in historical_rows if before_cutoff(row, cutoff_season, cutoff_week)),
                   key=lambda row: row.chronological_key)
    own = [row for row in prior if player_id is not None and row.player_id == player_id]
    if not own and player_gsis_id:
        own = [row for row in prior if row.player_gsis_id == player_gsis_id]
    own = own[-int(config["max_player_games"]):]
    if not thin_starting_history(own, overlay):
        return None
    own = [row for row in own if _number(row.stats, "attempts") >= float(overlay["min_line_attempts"])]
    peers = spot_start_prior_rows(prior, overlay)[-int(config["max_prior_games"]):]
    status = "historical" if len(own) >= int(config["minimum_historical_games"]) else ("position_prior" if peers else None)
    if status is None:
        return None

    own_weights = _recency_weights(own, float(config["player_half_life_games"]))
    baseline = _weighted_mean([row.dk_points for row in own], own_weights) if own else None
    player_strength = len(own) / (len(own) + float(config["prior_equivalent_games"]))
    if status == "position_prior":
        player_strength = 0.0
    identity_seed = int(hashlib.sha256(f"{seed}:{player_id}:{player_gsis_id}:{player_name}".encode()).hexdigest()[:16], 16)
    rng = np.random.default_rng(identity_seed)
    draw_count = int(config["draws"])
    choose_player = rng.random(draw_count) < player_strength
    own_indices = rng.choice(len(own), size=draw_count, p=own_weights) if own else np.zeros(draw_count, dtype=int)
    peer_indices = rng.choice(len(peers), size=draw_count) if peers else np.zeros(draw_count, dtype=int)

    scores: list[float] = []
    stat_totals: dict[str, float] = {}
    for index in range(draw_count):
        row = own[int(own_indices[index])] if own and (choose_player[index] or not peers) else peers[int(peer_indices[index])]
        stats = adjust_stat_line("QB", rescale_to_team_volume(row.stats, float(team_pass_attempts), overlay), context, config)
        scores.append(draftkings_points("QB", stats))
        for key, value in stats.items():
            stat_totals[key] = stat_totals.get(key, 0.0) + float(value)
    array = np.asarray(scores, dtype=float)
    team_factor, yardage_factor, td_factor = _environment_factors(context, config)
    confidence = min(1.0, len(own) / 12.0) * (0.75 if context.team_implied_total is None else 1.0)
    return HistoricalProjection(
        model_version=MODEL_VERSION, projection_status=status, player_id=player_id, player_gsis_id=player_gsis_id,
        player_name=player_name, position="QB", history_games=len(own), prior_games=len(peers),
        model_proj_fpts=round(float(array.mean()), 4), baseline_fpts=None if baseline is None else round(baseline, 4),
        floor_fpts=round(float(np.quantile(array, 0.10)), 4), median_fpts=round(float(np.quantile(array, 0.50)), 4),
        ceiling_fpts=round(float(np.quantile(array, 0.90)), 4), boom_rate=round(float(np.mean(array >= BOOM_THRESHOLDS["QB"])), 6),
        confidence=round(confidence, 4),
        stat_means={key: round(value / draw_count, 4) for key, value in sorted(stat_totals.items())},
        feature_snapshot={
            "cutoff_season": cutoff_season, "cutoff_week": cutoff_week, "team_implied_total": context.team_implied_total,
            "opponent_factor": context.opponent_factor, "team_environment_factor": round(team_factor, 6),
            "yardage_factor": round(yardage_factor, 6), "touchdown_factor": round(td_factor, 6),
            "player_weight": round(player_strength, 6), "draws": draw_count, "seed": seed, "prop_inputs": [],
            "qb_starter_construction": {"version": VERSION, "team_pass_attempts": round(float(team_pass_attempts), 3),
                                        "own_starter_lines": len(own), "spot_start_prior_rows": len(peers),
                                        "config": {k: (list(v) if isinstance(v, tuple) else v) for k, v in overlay.items()}},
        },
    )
