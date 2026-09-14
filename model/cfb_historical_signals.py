"""Leakage-safe primitives for College Football historical research.

This module deliberately contains no database or network access.  Ingestion and
UI queries may share these definitions without making outcome math depend on a
provider payload or a browser implementation.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import date
from typing import Iterable, Mapping, Sequence


DEFINITION_VERSION = "cfb-history-v1"
PRIOR_STRENGTH = 20.0
MARKET_BREAK_EVEN_110 = 110 / 210

SPREAD_BUCKETS: tuple[tuple[float, float, str], ...] = (
    (0.5, 2.5, "Favorite 0.5-2.5"),
    (3.0, 6.5, "Favorite 3.0-6.5"),
    (7.0, 10.0, "Favorite 7.0-10.0"),
    (10.5, 13.5, "Favorite 10.5-13.5"),
    (14.0, 16.5, "Favorite 14.0-16.5"),
    (17.0, 20.5, "Favorite 17.0-20.5"),
    (21.0, 27.5, "Favorite 21.0-27.5"),
    (28.0, math.inf, "Favorite 28.0+"),
)


@dataclass(frozen=True)
class RecordSummary:
    n: int
    wins: int
    losses: int
    pushes: int
    rate: float | None
    ci_low: float | None
    ci_high: float | None


@dataclass(frozen=True)
class CohortSummary:
    definition_version: str
    exact_line: float | None
    bucket_low: float | None
    bucket_high: float | None
    bucket_label: str | None
    su: RecordSummary
    ats: RecordSummary
    seasons: tuple[int, ...]

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["seasons"] = list(self.seasons)
        return payload


def spread_bucket(home_spread: float) -> tuple[float, float, str] | None:
    """Return the favorite-size bucket for a non-pick'em spread."""
    favorite_size = abs(float(home_spread))
    if favorite_size == 0:
        return None
    for low, high, label in SPREAD_BUCKETS:
        if low <= favorite_size <= high:
            return low, high, label
    return None


def grade_home(home_score: int, away_score: int, home_spread: float) -> tuple[str, str]:
    """Return straight-up and ATS outcomes from the home-team perspective."""
    margin = int(home_score) - int(away_score)
    su = "win" if margin > 0 else "loss" if margin < 0 else "push"
    adjusted = margin + float(home_spread)
    ats = "win" if adjusted > 1e-9 else "loss" if adjusted < -1e-9 else "push"
    return su, ats


def wilson_interval(wins: int, trials: int, z: float = 1.959963984540054) -> tuple[float | None, float | None]:
    if trials <= 0:
        return None, None
    p = wins / trials
    denominator = 1 + z * z / trials
    center = (p + z * z / (2 * trials)) / denominator
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * trials)) / trials) / denominator
    return max(0.0, center - margin), min(1.0, center + margin)


def summarize_outcomes(outcomes: Iterable[str]) -> RecordSummary:
    values = tuple(outcomes)
    wins = values.count("win")
    losses = values.count("loss")
    pushes = values.count("push")
    decisions = wins + losses
    low, high = wilson_interval(wins, decisions)
    return RecordSummary(
        n=len(values), wins=wins, losses=losses, pushes=pushes,
        rate=wins / decisions if decisions else None, ci_low=low, ci_high=high,
    )


def cohort_summary(
    rows: Iterable[Mapping[str, object]], *, exact_home_spread: float | None = None,
    favorite_low: float | None = None, favorite_high: float | None = None,
    require_home_favorite: bool = True,
) -> CohortSummary:
    """Summarize a prefiltered FBS/non-neutral row set.

    Rows without a final score or canonical home spread are excluded.  Callers
    remain responsible for population filters that need database metadata.
    """
    accepted: list[tuple[str, str, int]] = []
    bucket_label = None
    if exact_home_spread is not None:
        bucket = spread_bucket(exact_home_spread)
        if bucket:
            _, _, bucket_label = bucket
    for row in rows:
        if row.get("home_score") is None or row.get("away_score") is None or row.get("home_spread") is None:
            continue
        spread = float(row["home_spread"])
        if require_home_favorite and spread >= 0:
            continue
        if exact_home_spread is not None and not math.isclose(spread, exact_home_spread, abs_tol=1e-9):
            continue
        favorite_size = abs(spread)
        if favorite_low is not None and favorite_size < favorite_low:
            continue
        if favorite_high is not None and favorite_size > favorite_high:
            continue
        su, ats = grade_home(int(row["home_score"]), int(row["away_score"]), spread)
        accepted.append((su, ats, int(row.get("season") or 0)))
    return CohortSummary(
        definition_version=DEFINITION_VERSION,
        exact_line=exact_home_spread,
        bucket_low=favorite_low,
        bucket_high=favorite_high,
        bucket_label=bucket_label,
        su=summarize_outcomes(row[0] for row in accepted),
        ats=summarize_outcomes(row[1] for row in accepted),
        seasons=tuple(sorted({row[2] for row in accepted if row[2]})),
    )


def shrunk_rate(
    team_wins: int, team_losses: int, cohort_rate: float | None,
    prior_strength: float = PRIOR_STRENGTH,
) -> float | None:
    decisions = team_wins + team_losses
    if cohort_rate is None:
        return team_wins / decisions if decisions else None
    if decisions == 0:
        return cohort_rate
    return (team_wins + prior_strength * cohort_rate) / (decisions + prior_strength)


def reliability_label(n: int) -> str:
    if n < 10:
        return "VERY LOW"
    if n < 25:
        return "LOW"
    if n < 50:
        return "MODERATE"
    return "HIGH"


def season_blend_weights(effective_games: float, prior_games: float = 4.0) -> tuple[float, float]:
    if effective_games < 0 or prior_games <= 0:
        raise ValueError("effective_games must be non-negative and prior_games positive")
    current = effective_games / (effective_games + prior_games)
    return current, 1 - current


def blend_feature(current: float | None, prior: float | None, effective_games: float) -> float | None:
    if current is None:
        return prior
    if prior is None:
        return current
    current_weight, prior_weight = season_blend_weights(effective_games)
    return current * current_weight + prior * prior_weight


@dataclass(frozen=True)
class OpponentAdjustment:
    """Simple Rating System output for one season of FBS-versus-FBS games.

    ``ratings`` is a points-per-game quality estimate centred on zero, so a
    rating of +7 means "about a touchdown better than the average team in this
    population".  ``strength_of_schedule`` is the mean rating of the opponents
    a team actually played, which is the quantity raw points-for/against hides.

    The only free parameters are numerical (iteration cap and convergence
    tolerance).  Home-field advantage is estimated from the supplied games
    rather than assumed, so this introduces no hand-set statistical constant.

    ``components`` counts the connected components of the schedule graph.
    Ratings are only comparable *within* a component: two teams that share no
    chain of common opponents have no observed link, and the zero-centring
    that makes the solve well posed assigns their relative level arbitrarily.
    Early in a season the graph is heavily fragmented, so a caller comparing
    two teams must check this rather than assume one national scale.
    """

    ratings: Mapping[int, float]
    strength_of_schedule: Mapping[int, float]
    home_field_advantage: float
    teams: int
    games: int
    components: int
    iterations: int
    converged: bool


def _schedule_components(schedule: Mapping[int, Sequence[tuple[int, float]]]) -> int:
    """Count connected components of the schedule graph via union-find."""
    parent = {team_id: team_id for team_id in schedule}

    def find(team_id: int) -> int:
        while parent[team_id] != team_id:
            parent[team_id] = parent[parent[team_id]]
            team_id = parent[team_id]
        return team_id

    for team_id, opponents in schedule.items():
        for opponent, _ in opponents:
            left, right = find(team_id), find(opponent)
            if left != right:
                parent[left] = right
    return len({find(team_id) for team_id in parent})


def opponent_adjusted_ratings(
    games: Iterable[Mapping[str, object]], *,
    max_iterations: int = 200, tolerance: float = 1e-6,
) -> OpponentAdjustment:
    """Solve an iterative SRS over completed games.

    Callers must pass only games that were final *before* the decision time and
    that belong to the intended population; this function applies no
    point-in-time or classification filter of its own.
    """
    played: list[tuple[int, int, float, bool]] = []
    for row in games:
        if row.get("home_score") is None or row.get("away_score") is None:
            continue
        home_id = int(row["home_team_id"])
        away_id = int(row["away_team_id"])
        margin = float(row["home_score"]) - float(row["away_score"])
        played.append((home_id, away_id, margin, bool(row.get("neutral_site"))))
    if not played:
        return OpponentAdjustment({}, {}, 0.0, 0, 0, 0, 0, True)

    sited = [margin for _, _, margin, neutral in played if not neutral]
    home_field_advantage = sum(sited) / len(sited) if sited else 0.0

    schedule: dict[int, list[tuple[int, float]]] = {}
    for home_id, away_id, margin, neutral in played:
        shift = 0.0 if neutral else home_field_advantage
        schedule.setdefault(home_id, []).append((away_id, margin - shift))
        schedule.setdefault(away_id, []).append((home_id, -margin + shift))

    ratings = {team_id: 0.0 for team_id in schedule}
    iterations = 0
    converged = False
    for iterations in range(1, max_iterations + 1):
        updated = {
            team_id: sum(
                adjusted + ratings[opponent] for opponent, adjusted in opponents
            ) / len(opponents)
            for team_id, opponents in schedule.items()
        }
        centre = sum(updated.values()) / len(updated)
        updated = {team_id: value - centre for team_id, value in updated.items()}
        shift = max(abs(updated[team_id] - ratings[team_id]) for team_id in updated)
        ratings = updated
        if shift < tolerance:
            converged = True
            break

    strength_of_schedule = {
        team_id: sum(ratings[opponent] for opponent, _ in opponents) / len(opponents)
        for team_id, opponents in schedule.items()
    }
    return OpponentAdjustment(
        ratings=ratings, strength_of_schedule=strength_of_schedule,
        home_field_advantage=home_field_advantage, teams=len(ratings),
        games=len(played), components=_schedule_components(schedule),
        iterations=iterations, converged=converged,
    )


def walk_forward_splits(seasons: Sequence[int], min_train_seasons: int = 4) -> list[tuple[tuple[int, ...], int]]:
    ordered = tuple(sorted(set(int(season) for season in seasons)))
    if min_train_seasons < 1:
        raise ValueError("min_train_seasons must be positive")
    return [
        (ordered[:index], ordered[index])
        for index in range(min_train_seasons, len(ordered))
    ]


def benjamini_hochberg(p_values: Sequence[float]) -> list[float]:
    """Return false-discovery-rate-adjusted q-values in input order."""
    if any(value < 0 or value > 1 for value in p_values):
        raise ValueError("p-values must be between zero and one")
    count = len(p_values)
    ordered = sorted(enumerate(p_values), key=lambda item: item[1])
    adjusted = [1.0] * count
    running = 1.0
    for rank_index in range(count - 1, -1, -1):
        original_index, value = ordered[rank_index]
        rank = rank_index + 1
        running = min(running, value * count / rank)
        adjusted[original_index] = min(1.0, running)
    return adjusted


def snapshot_is_point_in_time(available_at, captured_at, kickoff_at) -> bool:
    return available_at <= captured_at < kickoff_at


def promotion_eligible(
    *, status: str, definition_frozen: bool, holdout_passed: bool,
    leakage_findings: int, prospective_n: int, required_prospective_n: int,
    requires_clv: bool, avg_clv: float | None,
) -> bool:
    """Machine-check the final gate; it never changes state by itself."""
    return (
        status == "PROSPECTIVE_SHADOW"
        and definition_frozen
        and holdout_passed
        and leakage_findings == 0
        and prospective_n >= required_prospective_n
        and (not requires_clv or (avg_clv is not None and avg_clv > 0))
    )
