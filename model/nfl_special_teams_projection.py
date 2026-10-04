"""Pregame DraftKings DST and Showdown kicker forecast candidate.

This module leaves the historical-v5 production baseline untouched. It draws
the same eligible whole-game histories, then conditions DST points allowed on
the opposing team's implied total and sack/turnover points on that offense's
pre-cutoff history. Kicker PAT points use the kicking team's implied total;
field-goal points keep their observed distance mix because a higher team total
does not by itself establish more stalled drives. The result is a separately
versioned candidate until paired forward and lineup grading qualifies it as a
production default.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping

import numpy as np

from model.nfl_dfs_historical import (
    BOOM_THRESHOLDS,
    MODEL_CONFIG,
    HistoricalWeek,
    _peer_rows,
    _recency_weights,
    before_cutoff,
    draftkings_points,
)
from model.nfl_team_aliases import normalize_team


VERSION = "nfl-special-teams-pregame-v1"
LEAGUE_TEAM_POINTS = 22.5
MIN_TOTAL_FACTOR = 0.70
MAX_TOTAL_FACTOR = 1.30
KICKER_PAT_TOTAL_WEIGHT = 0.50
OPPONENT_SHRINK_GAMES = 16.0
MIN_OPPONENT_FACTOR = 0.75
MAX_OPPONENT_FACTOR = 1.25
_DST_COMPONENTS = (
    "sacks", "interceptions", "fumble_recoveries", "dk_points_allowed",
    "points_allowed_fpts",
)
_KICK_FIELDS = (
    "pat_made", "fg_made_0_19", "fg_made_20_29", "fg_made_30_39",
    "fg_made_40_49", "fg_made_50_59", "fg_made_60_",
)


@dataclass(frozen=True)
class SpecialTeamsContext:
    team_implied_total: float | None = None
    opponent_implied_total: float | None = None
    opponent_team: str | None = None


@dataclass(frozen=True)
class SpecialTeamsProjection:
    version: str
    status: str
    position: str
    mean: float | None
    p10: float | None
    p50: float | None
    p90: float | None
    boom: float | None
    history_games: int
    prior_games: int
    feature_snapshot: Mapping[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _finite_positive(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value > 0


def _components(row: HistoricalWeek) -> Mapping[str, Any] | None:
    if row.position == "DST":
        values = row.stats.get("scoring_components")
        if not isinstance(values, Mapping) or not all(
            key in values and isinstance(values[key], (int, float)) and math.isfinite(values[key])
            for key in _DST_COMPONENTS
        ):
            return None
        return values
    if row.position == "K" and all(
        key in row.stats and isinstance(row.stats[key], (int, float)) and math.isfinite(row.stats[key])
        for key in _KICK_FIELDS
    ):
        return row.stats
    return None


def _dst_allowed_points(points: int) -> float:
    if points == 0:
        return 10.0
    if points <= 6:
        return 7.0
    if points <= 13:
        return 4.0
    if points <= 20:
        return 1.0
    if points <= 27:
        return 0.0
    if points <= 34:
        return -1.0
    return -4.0


def opponent_event_factors(
    history: Iterable[HistoricalWeek], opponent_team: str | None,
) -> tuple[float, float, int]:
    """Shrink this offense's past sacks/takeaways allowed toward the league.

    Rows must already be cut off before the target week. No target-game result
    or future-week observation may enter this calculation.
    """
    team = normalize_team(opponent_team)
    eligible = [(row, _components(row)) for row in history if row.position == "DST"]
    eligible = [(row, c) for row, c in eligible if c is not None]
    faced = [(row, c) for row, c in eligible if normalize_team(row.opponent) == team] if team else []
    if not faced or not eligible:
        return 1.0, 1.0, 0

    def shrunk_factor(key: str) -> float:
        league = float(np.mean([float(c[key]) for _, c in eligible]))
        if league <= 0:
            return 1.0
        observed = sum(float(c[key]) for _, c in faced)
        shrunk = (observed + OPPONENT_SHRINK_GAMES * league) / (len(faced) + OPPONENT_SHRINK_GAMES)
        return float(np.clip(shrunk / league, MIN_OPPONENT_FACTOR, MAX_OPPONENT_FACTOR))

    league_takeaways = [float(c["interceptions"]) + float(c["fumble_recoveries"]) for _, c in eligible]
    faced_takeaways = [float(c["interceptions"]) + float(c["fumble_recoveries"]) for _, c in faced]
    league_mean = float(np.mean(league_takeaways))
    takeaway_factor = 1.0 if league_mean <= 0 else float(np.clip(
        ((sum(faced_takeaways) + OPPONENT_SHRINK_GAMES * league_mean)
         / (len(faced) + OPPONENT_SHRINK_GAMES)) / league_mean,
        MIN_OPPONENT_FACTOR, MAX_OPPONENT_FACTOR,
    ))
    return shrunk_factor("sacks"), takeaway_factor, len(faced)


def conditioned_score(
    row: HistoricalWeek, context: SpecialTeamsContext,
    *, sack_factor: float = 1.0, takeaway_factor: float = 1.0,
) -> float:
    """Condition an exact historical DK line without changing scoring units."""
    values = _components(row)
    if values is None:
        raise ValueError("Exact special-teams scoring components are missing")
    if row.position == "K":
        if not _finite_positive(context.team_implied_total):
            raise ValueError("Kicker team implied total is unavailable")
        factor = float(np.clip(context.team_implied_total / LEAGUE_TEAM_POINTS, MIN_TOTAL_FACTOR, MAX_TOTAL_FACTOR))
        pat_points = float(values["pat_made"])
        fg_points = draftkings_points("K", values) - pat_points
        return pat_points * (1.0 + KICKER_PAT_TOTAL_WEIGHT * (factor - 1.0)) + fg_points
    if row.position == "DST":
        if not _finite_positive(context.opponent_implied_total):
            raise ValueError("DST opponent implied total is unavailable")
        old_allowed = float(values["dk_points_allowed"])
        new_allowed = max(0, round(old_allowed + context.opponent_implied_total - LEAGUE_TEAM_POINTS))
        old_pa_points = float(values["points_allowed_fpts"])
        sack_points = float(values["sacks"])
        takeaway_points = 2.0 * (float(values["interceptions"]) + float(values["fumble_recoveries"]))
        other_points = row.dk_points - old_pa_points - sack_points - takeaway_points
        return (other_points + _dst_allowed_points(new_allowed)
                + sack_points * sack_factor + takeaway_points * takeaway_factor)
    raise ValueError(f"Unsupported special-teams position: {row.position}")


def project_special_teams(
    *, position: str, player_id: int | None, player_gsis_id: str | None,
    player_name: str, historical_rows: Iterable[HistoricalWeek],
    cutoff_season: int, cutoff_week: int | None, context: SpecialTeamsContext,
    seed: int = 20260902, config: Mapping[str, float] = MODEL_CONFIG,
) -> SpecialTeamsProjection:
    if position not in {"DST", "K"}:
        raise ValueError("Special-teams projections support DST and K only")
    prior = sorted(
        (r for r in historical_rows if before_cutoff(r, cutoff_season, cutoff_week)),
        key=lambda r: r.chronological_key,
    )
    own = [r for r in prior if player_id is not None and r.player_id == player_id]
    if not own and player_gsis_id:
        own = [r for r in prior if r.player_gsis_id == player_gsis_id]
    own = own[-int(config["max_player_games"]):]
    peers = _peer_rows(position, own, prior, int(config["max_prior_games"]))
    snapshot: dict[str, Any] = {
        "cutoff_season": cutoff_season, "cutoff_week": cutoff_week,
        "team_implied_total": context.team_implied_total,
        "opponent_implied_total": context.opponent_implied_total,
        "opponent_team": normalize_team(context.opponent_team),
        "draws": int(config["draws"]), "seed": seed,
        "baseline_model": "nfl-dfs-historical-v5", "authority": "candidate_only",
    }

    def unavailable(reason: str) -> SpecialTeamsProjection:
        return SpecialTeamsProjection(VERSION, "unavailable", position, None, None, None, None,
                                      None, len(own), len(peers), {**snapshot, "reason": reason})

    if position == "K" and not _finite_positive(context.team_implied_total):
        return unavailable("Kicker team implied total is missing")
    if position == "DST" and not _finite_positive(context.opponent_implied_total):
        return unavailable("DST opponent implied total is missing")
    if position == "DST" and not normalize_team(context.opponent_team):
        return unavailable("DST opponent identity is missing")
    if not own and not peers:
        return unavailable("No eligible pre-cutoff special-teams history")
    if any(_components(row) is None for row in [*own, *peers]):
        return unavailable("Exact scoring components are missing from eligible history")

    strength = len(own) / (len(own) + float(config["prior_equivalent_games"])) if own else 0.0
    if len(own) < int(config["minimum_historical_games"]):
        strength = 0.0
    own_weights = _recency_weights(own, float(config["player_half_life_games"]))
    identity_seed = int(hashlib.sha256(
        f"{seed}:{player_id}:{player_gsis_id}:{player_name}".encode()).hexdigest()[:16], 16)
    rng = np.random.default_rng(identity_seed)
    draws = int(config["draws"])
    choose_player = rng.random(draws) < strength
    own_indices = rng.choice(len(own), size=draws, p=own_weights) if own else np.zeros(draws, dtype=int)
    peer_indices = rng.choice(len(peers), size=draws) if peers else np.zeros(draws, dtype=int)
    sack_factor, takeaway_factor, opponent_games = opponent_event_factors(prior, context.opponent_team)
    scores = np.asarray([
        conditioned_score(
            own[int(own_indices[i])] if own and (choose_player[i] or not peers) else peers[int(peer_indices[i])],
            context, sack_factor=sack_factor, takeaway_factor=takeaway_factor,
        )
        for i in range(draws)
    ], dtype=float)
    snapshot.update({
        "player_weight": round(strength, 6), "opponent_history_games": opponent_games,
        "opponent_sack_factor": round(sack_factor, 6),
        "opponent_takeaway_factor": round(takeaway_factor, 6),
        "conditioning": "historical whole-game draws; pregame implied total and shrunk opponent events",
    })
    return SpecialTeamsProjection(
        VERSION, "candidate", position, round(float(scores.mean()), 4),
        round(float(np.quantile(scores, .10)), 4),
        round(float(np.quantile(scores, .50)), 4),
        round(float(np.quantile(scores, .90)), 4),
        round(float(np.mean(scores >= BOOM_THRESHOLDS[position])), 6),
        len(own), len(peers), snapshot,
    )
