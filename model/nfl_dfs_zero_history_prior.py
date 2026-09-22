"""nfl-dfs-historical-v4: the first-appearance cohort prior (research module).

Registered in docs/nfl-dfs-v4-zero-history-prior-study.md. v3 is not edited
here; its machinery (recency weights, environment adjustment, DK scoring,
draw mechanics) is imported and reused so the ONLY difference between v3 and
v4 is the peer population a player with fewer than six games draws from.

v3: peers = the 400 most recent stat rows of the position, i.e. players who
    recorded stats, i.e. starters. A player with 0-1 games is projected as an
    average starter (player_strength = 0).
v4: peers = the walk-forward first-appearance cohort -- every other player's
    chronologically FIRST recorded game before the cutoff. A player with 0
    games is projected as a typical first game; anyone with 1+ games
    delegates to v3 unchanged.

A first draft also shrank 1-5 game players toward the cohort with
w = n/(n+4). On the discovery weeks it made that cohort worse at every
position (+0.2 to +1.0 MAE) -- those players have real usage and a
first-game prior is too pessimistic for them -- so it was dropped before
registration. See docs/nfl-dfs-v4-zero-history-prior-study.md.
"""

from __future__ import annotations

import hashlib
from typing import Iterable, Mapping, Sequence

import numpy as np

from model.nfl_dfs_historical import (
    BOOM_THRESHOLDS, MODEL_CONFIG, SKILL_POSITIONS, HistoricalProjection, HistoricalWeek,
    ProjectionContext, _environment_factors, adjust_stat_line,
    before_cutoff, draftkings_points, project_player,
)

MODEL_VERSION = "nfl-dfs-historical-v4"
COHORT_LIMIT = 400              # same cap as v3's max_prior_games
DELEGATE_TO_V3_FROM_GAMES = 1   # hist_1_5 and hist_6_plus are identical to v3 by construction


def first_appearance_cohort(
    position: str,
    prior_rows: Sequence[HistoricalWeek],
    *,
    exclude_player_id: int | None,
    limit: int = COHORT_LIMIT,
) -> list[HistoricalWeek]:
    """One row per other player in the position: his first recorded game.

    `prior_rows` must already be restricted to rows before the cutoff, so the
    "first game" is the first game the model could have seen -- a player
    whose real first game falls after the cutoff is simply absent, which is
    exactly the walk-forward population.
    """
    first: dict[int, HistoricalWeek] = {}
    for row in sorted(prior_rows, key=lambda r: (r.chronological_key, r.player_id)):
        if row.position != position or row.player_id == exclude_player_id:
            continue
        if row.player_id not in first:
            first[row.player_id] = row
    cohort = sorted(first.values(), key=lambda r: (r.chronological_key, r.player_id), reverse=True)
    return cohort[:limit]


def project_player_v4(
    *,
    player_id: int | None,
    player_gsis_id: str | None,
    player_name: str,
    position: str,
    historical_rows: Iterable[HistoricalWeek],
    cutoff_season: int,
    cutoff_week: int | None,
    context: ProjectionContext = ProjectionContext(),
    seed: int = 20260902,
    config: Mapping[str, float] = MODEL_CONFIG,
) -> HistoricalProjection:
    rows = list(historical_rows)
    prior = sorted((r for r in rows if before_cutoff(r, cutoff_season, cutoff_week)), key=lambda r: r.chronological_key)
    own = [r for r in prior if player_id is not None and r.player_id == player_id]
    if not own and player_gsis_id:
        own = [r for r in prior if r.player_gsis_id == player_gsis_id]
    own = own[-int(config["max_player_games"]):]

    if position not in SKILL_POSITIONS or len(own) >= DELEGATE_TO_V3_FROM_GAMES:
        v3 = project_player(player_id=player_id, player_gsis_id=player_gsis_id, player_name=player_name,
                            position=position, historical_rows=rows, cutoff_season=cutoff_season,
                            cutoff_week=cutoff_week, context=context, seed=seed, config=config)
        return HistoricalProjection(**{**v3.as_dict(), "model_version": MODEL_VERSION,
                                       "feature_snapshot": {**v3.feature_snapshot, "v4_path": "delegated_to_v3"}})

    # From here on the player has NO pre-cutoff history (own is empty).
    cohort = first_appearance_cohort(position, prior, exclude_player_id=player_id)
    if not cohort:
        return HistoricalProjection(
            MODEL_VERSION, "unavailable", player_id, player_gsis_id, player_name, position,
            0, 0, None, None, None, None, None, None, 0.0, {},
            {"reason": "no pre-cutoff first-appearance cohort", "v4_path": "cohort"},
        )
    status = "cohort_prior"
    baseline = None
    player_strength = 0.0

    identity_seed = int(hashlib.sha256(f"{seed}:{player_id}:{player_gsis_id}:{player_name}".encode()).hexdigest()[:16], 16)
    rng = np.random.default_rng(identity_seed)
    draw_count = int(config["draws"])
    cohort_indices = rng.choice(len(cohort), size=draw_count)

    scores: list[float] = []
    stat_totals: dict[str, float] = {}
    for i in range(draw_count):
        row = cohort[int(cohort_indices[i])]
        stats = adjust_stat_line(position, row.stats, context, config)
        scores.append(draftkings_points(position, stats))
        for key, value in stats.items():
            stat_totals[key] = stat_totals.get(key, 0.0) + float(value)
    array = np.asarray(scores, dtype=float)
    team_factor, yardage_factor, td_factor = _environment_factors(context, config)
    confidence = min(1.0, len(own) / 12.0) * (0.75 if context.team_implied_total is None else 1.0)
    return HistoricalProjection(
        model_version=MODEL_VERSION, projection_status=status, player_id=player_id,
        player_gsis_id=player_gsis_id, player_name=player_name, position=position,
        history_games=len(own), prior_games=len(cohort),
        model_proj_fpts=round(float(array.mean()), 4),
        baseline_fpts=None if baseline is None else round(baseline, 4),
        floor_fpts=round(float(np.quantile(array, 0.10)), 4),
        median_fpts=round(float(np.quantile(array, 0.50)), 4),
        ceiling_fpts=round(float(np.quantile(array, 0.90)), 4),
        boom_rate=round(float(np.mean(array >= BOOM_THRESHOLDS[position])), 6),
        confidence=round(confidence, 4),
        stat_means={k: round(v / draw_count, 4) for k, v in sorted(stat_totals.items())},
        feature_snapshot={
            "cutoff_season": cutoff_season, "cutoff_week": cutoff_week,
            "team_implied_total": context.team_implied_total, "opponent_factor": context.opponent_factor,
            "team_environment_factor": round(team_factor, 6), "yardage_factor": round(yardage_factor, 6),
            "touchdown_factor": round(td_factor, 6), "player_weight": round(player_strength, 6),
            "draws": draw_count, "seed": seed, "prop_inputs": [],
            "v4_path": "cohort", "cohort_rows": len(cohort),
            "cohort_mean_dk_points": round(float(np.mean([r.dk_points for r in cohort])), 4) if cohort else None,
        },
    )
