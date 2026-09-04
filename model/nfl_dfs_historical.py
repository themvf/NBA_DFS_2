"""Historical-first DraftKings NFL projection model.

The model deliberately contains no prop or third-party projection input.  It
uses completed player weeks available before the target kickoff, preserves the
observed relationship between a player's component stats by resampling whole
game lines, and scores every draw with DraftKings' non-linear bonus rules.

Props will later be an overlay that cites one immutable historical projection
row.  They must never rewrite the baseline produced here.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping, Sequence

import numpy as np


MODEL_VERSION = "nfl-dfs-historical-v2"
MODEL_CONFIG = {
    "player_half_life_games": 6.0,
    "prior_equivalent_games": 4.0,
    "max_player_games": 34,
    "max_prior_games": 400,
    "league_team_points": 22.5,
    "environment_yardage_exponent": 0.35,
    "environment_td_exponent": 1.0,
    "opponent_exponent": 0.35,
    "min_environment_factor": 0.80,
    "max_environment_factor": 1.20,
    "minimum_historical_games": 2,
    "draws": 2000,
}

SKILL_POSITIONS = ("QB", "RB", "WR", "TE")
SUPPORTED_POSITIONS = SKILL_POSITIONS + ("K", "DST")
BOOM_THRESHOLDS = {"QB": 30.0, "RB": 25.0, "WR": 25.0, "TE": 20.0, "K": 15.0, "DST": 15.0}

OFFENSE_FIELDS = (
    "passing_yards", "passing_tds", "passing_interceptions",
    "rushing_yards", "rushing_tds", "receiving_yards", "receiving_tds",
    "receptions", "passing_2pt_conversions", "rushing_2pt_conversions",
    "receiving_2pt_conversions", "special_teams_tds", "fumble_recovery_tds",
    "fumbles_lost_total",
)


def _number(row: Mapping[str, Any], key: str) -> float:
    value = row.get(key)
    if value is None:
        return 0.0
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if math.isnan(number) else number


def draftkings_points(position: str, row: Mapping[str, Any]) -> float:
    """Score a realized stat line using current DraftKings NFL rules."""
    if position == "DST":
        # DST history reaches the model only through the versioned exact-DK
        # result ledger. Its component evidence remains available separately
        # for custom redraft scoring.
        return _number(row, "fantasy_points")
    if position == "K":
        return (
            _number(row, "pat_made")
            + 3.0 * (_number(row, "fg_made_0_19") + _number(row, "fg_made_20_29") + _number(row, "fg_made_30_39"))
            + 4.0 * _number(row, "fg_made_40_49")
            + 5.0 * (_number(row, "fg_made_50_59") + _number(row, "fg_made_60_"))
        )

    passing_yards = _number(row, "passing_yards")
    rushing_yards = _number(row, "rushing_yards")
    receiving_yards = _number(row, "receiving_yards")
    points = (
        passing_yards / 25.0
        + 4.0 * _number(row, "passing_tds")
        - _number(row, "passing_interceptions")
        + rushing_yards / 10.0
        + 6.0 * _number(row, "rushing_tds")
        + receiving_yards / 10.0
        + 6.0 * _number(row, "receiving_tds")
        + _number(row, "receptions")
        + 2.0 * (
            _number(row, "passing_2pt_conversions")
            + _number(row, "rushing_2pt_conversions")
            + _number(row, "receiving_2pt_conversions")
        )
        + 6.0 * (_number(row, "special_teams_tds") + _number(row, "fumble_recovery_tds"))
        - _number(row, "fumbles_lost_total")
    )
    points += 3.0 if passing_yards >= 300 else 0.0
    points += 3.0 if rushing_yards >= 100 else 0.0
    points += 3.0 if receiving_yards >= 100 else 0.0
    return float(points)


@dataclass(frozen=True)
class HistoricalWeek:
    player_id: int
    player_gsis_id: str | None
    player_name: str
    position: str
    season: int
    week: int
    team: str | None
    opponent: str | None
    stats: Mapping[str, Any]

    @property
    def chronological_key(self) -> tuple[int, int]:
        return self.season, self.week

    @property
    def dk_points(self) -> float:
        return draftkings_points(self.position, self.stats)


@dataclass(frozen=True)
class ProjectionContext:
    team_implied_total: float | None = None
    opponent_factor: float | None = None


@dataclass(frozen=True)
class HistoricalProjection:
    model_version: str
    projection_status: str
    player_id: int | None
    player_gsis_id: str | None
    player_name: str
    position: str
    history_games: int
    prior_games: int
    model_proj_fpts: float | None
    baseline_fpts: float | None
    floor_fpts: float | None
    median_fpts: float | None
    ceiling_fpts: float | None
    boom_rate: float | None
    confidence: float
    stat_means: Mapping[str, float]
    feature_snapshot: Mapping[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def before_cutoff(row: HistoricalWeek, season: int, week: int | None) -> bool:
    if row.season < season:
        return True
    return week is not None and row.season == season and row.week < week


def _recency_weights(rows: Sequence[HistoricalWeek], half_life: float) -> np.ndarray:
    if not rows:
        return np.array([], dtype=float)
    ages = np.arange(len(rows) - 1, -1, -1, dtype=float)
    weights = np.power(0.5, ages / half_life)
    return weights / weights.sum()


def _weighted_mean(values: Sequence[float], weights: np.ndarray) -> float:
    return float(np.dot(np.asarray(values, dtype=float), weights))


def _environment_factors(context: ProjectionContext, config: Mapping[str, float]) -> tuple[float, float, float]:
    if context.team_implied_total is None or context.team_implied_total <= 0:
        team_factor = 1.0
    else:
        team_factor = context.team_implied_total / config["league_team_points"]
    team_factor = float(np.clip(team_factor, config["min_environment_factor"], config["max_environment_factor"]))
    opponent = float(np.clip(context.opponent_factor or 1.0, 0.80, 1.20))
    yardage = (team_factor ** config["environment_yardage_exponent"]) * (opponent ** config["opponent_exponent"])
    touchdowns = team_factor ** config["environment_td_exponent"]
    return team_factor, yardage, touchdowns


def adjust_stat_line(
    position: str,
    stats: Mapping[str, Any],
    context: ProjectionContext,
    config: Mapping[str, float] = MODEL_CONFIG,
) -> dict[str, float]:
    """Apply only pregame environment features; never outcome information."""
    if position in {"DST", "K"}:
        return {key: _number(stats, key) for key in stats}
    _, yardage_factor, td_factor = _environment_factors(context, config)
    adjusted = {key: _number(stats, key) for key in OFFENSE_FIELDS}
    for key in ("passing_yards", "rushing_yards", "receiving_yards", "receptions"):
        adjusted[key] *= yardage_factor
    for key in ("passing_tds", "rushing_tds", "receiving_tds"):
        adjusted[key] *= td_factor
    return adjusted


def _peer_rows(
    position: str,
    player_rows: Sequence[HistoricalWeek],
    all_prior_rows: Sequence[HistoricalWeek],
    limit: int,
) -> list[HistoricalWeek]:
    peers = [row for row in all_prior_rows if row.position == position and row.player_id != (player_rows[-1].player_id if player_rows else None)]
    if not peers:
        return []
    if player_rows:
        player_mean = np.mean([row.dk_points for row in player_rows])
        # Pick comparable PLAYERS by their earlier mean, then retain their
        # whole game lines. Picking individual games close to the mean would
        # manufacture an artificially narrow P10/P90 distribution.
        by_player: dict[int, list[HistoricalWeek]] = {}
        for row in peers:
            by_player.setdefault(row.player_id, []).append(row)
        peer_ids = sorted(
            by_player,
            key=lambda peer_id: (
                abs(float(np.mean([row.dk_points for row in by_player[peer_id]])) - player_mean),
                peer_id,
            ),
        )
        selected: list[HistoricalWeek] = []
        for peer_id in peer_ids:
            selected.extend(sorted(by_player[peer_id], key=lambda row: row.chronological_key, reverse=True))
            if len(selected) >= limit:
                break
        return selected[:limit]
    else:
        peers.sort(key=lambda row: (-row.season, -row.week, row.player_id))
    return peers[:limit]


def project_player(
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
    if position not in SUPPORTED_POSITIONS:
        raise ValueError(f"Unsupported NFL position: {position}")
    prior = sorted(
        (row for row in historical_rows if before_cutoff(row, cutoff_season, cutoff_week)),
        key=lambda row: row.chronological_key,
    )
    own = [row for row in prior if player_id is not None and row.player_id == player_id]
    if not own and player_gsis_id:
        own = [row for row in prior if row.player_gsis_id == player_gsis_id]
    own = own[-int(config["max_player_games"]):]
    peers = _peer_rows(position, own, prior, int(config["max_prior_games"]))

    status = "historical" if len(own) >= int(config["minimum_historical_games"]) else ("position_prior" if peers else "unavailable")
    if status == "unavailable":
        return HistoricalProjection(
            MODEL_VERSION, status, player_id, player_gsis_id, player_name, position,
            len(own), 0, None, None, None, None, None, None, 0.0, {},
            {"reason": "no eligible pre-cutoff player or position history"},
        )

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
        stats = adjust_stat_line(position, row.stats, context, config)
        scores.append(draftkings_points(position, stats))
        for key, value in stats.items():
            stat_totals[key] = stat_totals.get(key, 0.0) + float(value)

    array = np.asarray(scores, dtype=float)
    team_factor, yardage_factor, td_factor = _environment_factors(context, config)
    confidence = min(1.0, len(own) / 12.0) * (0.75 if context.team_implied_total is None else 1.0)
    if position == "DST":
        confidence *= 0.65
    return HistoricalProjection(
        model_version=MODEL_VERSION,
        projection_status=status,
        player_id=player_id,
        player_gsis_id=player_gsis_id,
        player_name=player_name,
        position=position,
        history_games=len(own),
        prior_games=len(peers),
        model_proj_fpts=round(float(array.mean()), 4),
        baseline_fpts=None if baseline is None else round(baseline, 4),
        floor_fpts=round(float(np.quantile(array, 0.10)), 4),
        median_fpts=round(float(np.quantile(array, 0.50)), 4),
        ceiling_fpts=round(float(np.quantile(array, 0.90)), 4),
        boom_rate=round(float(np.mean(array >= BOOM_THRESHOLDS[position])), 6),
        confidence=round(confidence, 4),
        stat_means={key: round(value / draw_count, 4) for key, value in sorted(stat_totals.items())},
        feature_snapshot={
            "cutoff_season": cutoff_season,
            "cutoff_week": cutoff_week,
            "team_implied_total": context.team_implied_total,
            "opponent_factor": context.opponent_factor,
            "team_environment_factor": round(team_factor, 6),
            "yardage_factor": round(yardage_factor, 6),
            "touchdown_factor": round(td_factor, 6),
            "player_weight": round(player_strength, 6),
            "draws": draw_count,
            "seed": seed,
            "prop_inputs": [],
        },
    )


def artifact_digest(payload: Mapping[str, Any] | Sequence[Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()
